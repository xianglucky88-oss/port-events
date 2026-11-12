"""Snapshot retrieval with keyset pagination.

Contract
--------
A caller searches submitted events by container number, vessel/voyage, port,
event type and an inclusive UTC time range. Every page is returned from one
*read snapshot*: an independent read-only SQLite connection pinned by an
explicit read transaction (WAL). Writes landing between pages therefore
cannot change the visible rows -- no full-table materialization, no OFFSET,
and no fabricated continuity.

Continuation uses the complete ordering ``(event_time_utc, event_key)``. The
WHERE/ORDER BY/LIMIT work is pushed entirely into SQLite; each query fetches
at most ``page_size + 1`` rows.

Cursors are opaque, self-contained tokens signed with a per-snapshot secret.
They bind the filter fingerprint and the read context (snapshot id), carry an
expiry timestamp, and are rejected with an explicit reason when expired,
tampered with, used against a mismatched filter, or used after the snapshot
has been closed.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from .models import ShipmentEvent


class SnapshotError(Exception):
    """Base class for snapshot/contract failures."""

    reason = "snapshot_error"


class InvalidFilter(SnapshotError):
    reason = "invalid_filter"


class InvalidTimeRange(InvalidFilter):
    reason = "invalid_time_range"


class InvalidPageSize(InvalidFilter):
    reason = "invalid_page_size"


class CursorExpired(SnapshotError):
    reason = "cursor_expired"


class CursorInvalid(SnapshotError):
    reason = "cursor_invalid"


class SnapshotClosed(SnapshotError):
    reason = "snapshot_closed"


@dataclass(frozen=True)
class EventFilter:
    """Search criteria. ``None`` means the field is not constrained.

    Text fields follow the normalization applied at import time (uppercased,
    stripped). Time bounds are inclusive and must be timezone-aware ISO 8601
    strings; they are canonicalized to UTC for stable lexicographic comparison
    against ``event_time_utc``.
    """

    container: Optional[str] = None
    vessel: Optional[str] = None
    voyage: Optional[str] = None
    port: Optional[str] = None
    event_type: Optional[str] = None
    start_time_utc: Optional[str] = None
    end_time_utc: Optional[str] = None

    def __post_init__(self) -> None:
        # Container numbers and event-type vocabulary are case-sensitive; only
        # vessel/voyage/port follow the import-time uppercase normalization.
        object.__setattr__(self, "container", _strip_optional(self.container))
        object.__setattr__(self, "vessel", _normalize_optional(self.vessel))
        object.__setattr__(self, "voyage", _normalize_optional(self.voyage))
        object.__setattr__(self, "port", _normalize_optional(self.port))
        object.__setattr__(
            self, "event_type", _strip_optional(self.event_type)
        )
        start = None
        end = None
        if self.start_time_utc is not None:
            start = _canonical_utc(self.start_time_utc, "start_time_utc")
        if self.end_time_utc is not None:
            end = _canonical_utc(self.end_time_utc, "end_time_utc")
        object.__setattr__(self, "start_time_utc", start)
        object.__setattr__(self, "end_time_utc", end)
        if start is not None and end is not None and start > end:
            raise InvalidTimeRange(
                "start_time_utc must not be later than end_time_utc"
            )

    def fingerprint(self) -> str:
        """Stable identifier of the effective criteria, bound into cursors."""
        payload = json.dumps(
            {
                "container": self.container,
                "vessel": self.vessel,
                "voyage": self.voyage,
                "port": self.port,
                "event_type": self.event_type,
                "start_time_utc": self.start_time_utc,
                "end_time_utc": self.end_time_utc,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def where_clause(self) -> tuple[str, list]:
        """Build the parameterized predicate pushed down to SQLite."""
        clauses: list[str] = []
        params: list = []
        if self.container is not None:
            # Vessel-level rows store NULL and must not match a container query.
            clauses.append("container IS NOT NULL AND container = ?")
            params.append(self.container)
        if self.vessel is not None:
            clauses.append("vessel = ?")
            params.append(self.vessel)
        if self.voyage is not None:
            clauses.append("voyage = ?")
            params.append(self.voyage)
        if self.port is not None:
            clauses.append("port = ?")
            params.append(self.port)
        if self.event_type is not None:
            clauses.append("event_type = ?")
            params.append(self.event_type)
        if self.start_time_utc is not None:
            clauses.append("event_time_utc >= ?")
            params.append(self.start_time_utc)
        if self.end_time_utc is not None:
            clauses.append("event_time_utc <= ?")
            params.append(self.end_time_utc)
        sql = " WHERE " + " AND ".join(clauses) if clauses else ""
        return sql, params


@dataclass(frozen=True)
class EventPage:
    events: tuple[ShipmentEvent, ...]
    next_cursor: Optional[str]
    has_more: bool
    snapshot_id: str
    revision: int
    page_size: int


@dataclass(frozen=True)
class _CursorPayload:
    snapshot_id: str
    filter_fingerprint: str
    last_event_time_utc: str
    last_event_key: str
    expires_at: str


class EventSnapshot:
    """A pinned read context for iterating filtered event pages."""

    def __init__(self, store, event_filter: Optional[EventFilter] = None,
                 ttl_seconds: float = 300.0) -> None:
        if not getattr(store, "_supports_snapshots", False):
            raise InvalidFilter(
                "snapshots require a file-backed database; ':memory:' cannot "
                "share data across a separate read-only connection"
            )
        if ttl_seconds <= 0:
            raise InvalidFilter("ttl_seconds must be positive")
        self.filter = event_filter or EventFilter()
        self.store = store
        self.ttl_seconds = float(ttl_seconds)
        self.snapshot_id = secrets.token_hex(16)
        self._secret = secrets.token_bytes(32)
        self._closed = False
        # A private, read-only connection. The explicit read transaction pins
        # one SQLite snapshot for the whole lifetime of this object. The path
        # is URI-quoted so spaces and special characters are handled safely.
        from urllib.parse import quote

        uri = f"file:{quote(str(store.database_path.resolve()))}?mode=ro"
        self.connection = sqlite3.connect(uri, uri=True, check_same_thread=False)
        # Autocommit + explicit BEGIN gives a plain read transaction with no
        # implicit statement snapshots underneath.
        self.connection.isolation_level = None
        self.connection.execute("BEGIN")
        self.revision = self._read_revision()
        self.filter_fingerprint = self.filter.fingerprint()
        store._register_snapshot(self)

    # -- cursor tokens -------------------------------------------------------

    def _encode_cursor(self, last_event_time_utc: str, last_event_key: str) -> str:
        payload = {
            "snapshot_id": self.snapshot_id,
            "filter": self.filter_fingerprint,
            "time": last_event_time_utc,
            "key": last_event_key,
            "expires_at": (
                datetime.now(timezone.utc) + timedelta(seconds=self.ttl_seconds)
            ).isoformat(),
        }
        body = base64.urlsafe_b64encode(
            json.dumps(payload, separators=(",", ":")).encode("utf-8")
        )
        signature = hmac.new(self._secret, body, hashlib.sha256).digest()
        return (body + b"." + base64.urlsafe_b64encode(signature)).decode("ascii")

    def _decode_cursor(self, token: str) -> _CursorPayload:
        # Order of checks matters: prove authenticity before trusting any field.
        try:
            body_b64, sig_b64 = token.encode("ascii").split(b".", 1)
            body = base64.urlsafe_b64decode(body_b64)
            signature = base64.urlsafe_b64decode(sig_b64)
        except (ValueError, TypeError) as exc:
            raise CursorInvalid("malformed cursor token") from exc
        expected = hmac.new(self._secret, body_b64, hashlib.sha256).digest()
        if not hmac.compare_digest(signature, expected):
            raise CursorInvalid("cursor signature does not match this snapshot")
        try:
            payload = json.loads(body.decode("utf-8"))
            decoded = _CursorPayload(
                snapshot_id=payload["snapshot_id"],
                filter_fingerprint=payload["filter"],
                last_event_time_utc=payload["time"],
                last_event_key=payload["key"],
                expires_at=payload["expires_at"],
            )
        except (ValueError, KeyError, TypeError) as exc:
            raise CursorInvalid("cursor payload is incomplete") from exc
        if decoded.snapshot_id != self.snapshot_id:
            raise CursorInvalid("cursor belongs to a different read snapshot")
        if decoded.filter_fingerprint != self.filter_fingerprint:
            raise CursorInvalid(
                "cursor was created for different filter criteria"
            )
        try:
            expires_at = datetime.fromisoformat(decoded.expires_at)
        except ValueError as exc:
            raise CursorInvalid("cursor expiry is unreadable") from exc
        if expires_at <= datetime.now(timezone.utc):
            raise CursorExpired(
                f"cursor expired at {decoded.expires_at}; reopen a snapshot "
                "and restart pagination from the first page"
            )
        return decoded

    # -- pagination ----------------------------------------------------------

    def fetch_page(self, cursor: Optional[str] = None,
                   page_size: int = 100) -> EventPage:
        if self._closed:
            raise SnapshotClosed("this snapshot has been closed")
        if not isinstance(page_size, int) or page_size <= 0:
            raise InvalidPageSize("page_size must be a positive integer")

        where_sql, params = self.filter.where_clause()
        clauses = []
        if where_sql:
            clauses.append(where_sql[len(" WHERE "):])

        if cursor is not None:
            decoded = self._decode_cursor(cursor)
            # Strict total-order continuation on (event_time_utc, event_key).
            clauses.append(
                "(event_time_utc > ? OR "
                "(event_time_utc = ? AND event_key > ?))"
            )
            params.extend(
                [
                    decoded.last_event_time_utc,
                    decoded.last_event_time_utc,
                    decoded.last_event_key,
                ]
            )

        predicate = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = (
            "SELECT event_key, vessel, voyage, container, port, event_type, "
            "event_time_utc, status, sources, observed_at "
            "FROM shipment_events"
            f"{predicate} ORDER BY event_time_utc ASC, event_key ASC "
            "LIMIT ?"
        )
        rows = self.connection.execute(sql, (*params, page_size + 1)).fetchall()

        has_more = len(rows) > page_size
        page_rows = rows[:page_size]
        events = tuple(_row_to_event(row) for row in page_rows)
        next_cursor = None
        if has_more and page_rows:
            last = page_rows[-1]
            # Column indexes: 0 event_key, 6 event_time_utc.
            next_cursor = self._encode_cursor(last[6], last[0])
        return EventPage(
            events=events,
            next_cursor=next_cursor,
            has_more=has_more,
            snapshot_id=self.snapshot_id,
            revision=self.revision,
            page_size=page_size,
        )

    def iter_pages(self, page_size: int = 100):
        """Yield pages until the snapshot's visible result set is exhausted."""
        cursor = None
        while True:
            page = self.fetch_page(cursor=cursor, page_size=page_size)
            yield page
            if not page.has_more:
                return
            cursor = page.next_cursor

    def _read_revision(self) -> int:
        row = self.connection.execute(
            "SELECT revision FROM store_revision WHERE id = 1"
        ).fetchone()
        return int(row[0])

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self.connection.rollback()
        finally:
            self.connection.close()
            self.store._unregister_snapshot(self)

    def __enter__(self) -> "EventSnapshot":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


# -- helpers ------------------------------------------------------------------


def _normalize_optional(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    normalized = str(value).strip().upper()
    return normalized or None


def _strip_optional(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _canonical_utc(value: str, field_name: str) -> str:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise InvalidTimeRange(f"{field_name} is not a valid ISO 8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise InvalidTimeRange(
            f"{field_name} must be timezone-aware (use an explicit offset, e.g. +00:00)"
        )
    return parsed.astimezone(timezone.utc).isoformat()


def _row_to_event(row: tuple) -> ShipmentEvent:
    sources = tuple(part for part in row[8].split(",") if part)
    return ShipmentEvent(
        event_key=row[0],
        vessel=row[1],
        voyage=row[2],
        container=row[3],
        port=row[4],
        event_type=row[5],
        event_time_utc=row[6],
        status=row[7],
        sources=sources,
        observed_at=row[9],
    )
