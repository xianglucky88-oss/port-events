"""Offline replay: recompute events from a fixture and diff them against a read-only SQLite snapshot.

This module is the pre-go-live verification counterpart to ``PortEventsPipeline.run``. It reuses
``JsonEventSource``, ``normalize_event`` and ``merge_events`` to derive the expected event set from a
fixture and a timezone name, then compares that set against a full-table snapshot of
``shipment_events`` by exact ``event_key``.

Guarantees:

* The database is never written and never created. The snapshot is opened with ``mode=ro`` plus
  ``PRAGMA query_only = ON`` and read inside a single explicit transaction, so concurrent writers
  cannot produce a torn read. A missing path or an unsupported table structure fails clearly.
* Comparison is by complete ``event_key`` only. Keys present on just one side are reported as-is;
  no approximate matching, renaming or deletion inference is attempted.
* ``observed_at`` (from ``observed_now``) is operational noise, not business data, so it is never
  selected or compared. Time interpretation and status adjudication stay exactly as implemented in
  ``normalize_event`` / ``merge_events``; nothing is adjusted to manufacture stability.
* Fixture parse or normalization failures return an incomplete report carrying the failed stage and
  the reason. In that case the snapshot rows that could not be compared are *not* reported as diffs.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

from .connectors import JsonEventSource
from .correlate import merge_events
from .models import ShipmentEvent
from .normalize import normalize_event

# Columns of the schema written by the pipeline's event store. A snapshot missing any of them,
# or one where event_key is not the primary key, is an unsupported structure and fails clearly.
_REQUIRED_COLUMNS = frozenset(
    {
        "event_key",
        "vessel",
        "voyage",
        "container",
        "port",
        "event_type",
        "event_time_utc",
        "status",
        "sources",
        "observed_at",
    }
)


class SnapshotError(Exception):
    """The database snapshot is missing, unreadable, or has an unsupported structure."""


@dataclass(frozen=True)
class SnapshotRow:
    event_key: str
    status: str
    sources: tuple[str, ...]


@dataclass(frozen=True)
class KeyChange:
    """Same event_key on both sides, but the status or the source set differs."""

    event_key: str
    replay_status: str
    snapshot_status: str
    replay_sources: tuple[str, ...]
    snapshot_sources: tuple[str, ...]


@dataclass(frozen=True)
class ReplayReport:
    """Outcome of one offline replay.

    ``complete`` is False when a stage failed; ``stage`` then names the failed stage
    ("fixture", "normalize" or "snapshot") and ``error`` carries the reason. Incomplete
    reports never contain diff entries. Complete reports have stage "compare" and hold the
    full, deterministically ordered diff.
    """

    complete: bool
    stage: str
    error: str | None = None
    replayed: int = 0
    snapshot_rows: int = 0
    replay_only: tuple[str, ...] = ()
    snapshot_only: tuple[str, ...] = ()
    changed: tuple[KeyChange, ...] = ()


def _reason(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"


def _split_sources(raw: str) -> tuple[str, ...]:
    return tuple(sorted({part for part in raw.split(",") if part}))


def _replay_events(fixture: str | Path, timezone_name: str) -> tuple[list[ShipmentEvent] | None, str | None, str | None]:
    """Run the exact pipeline transforms (read -> normalize -> merge) without any store.

    Returns (events, None, None) on success, or (None, failed_stage, reason) on failure.
    """
    try:
        raw_items = JsonEventSource(fixture).read()
    except Exception as exc:
        return None, "fixture", _reason(exc)
    try:
        events = merge_events([normalize_event(item, timezone_name) for item in raw_items])
    except Exception as exc:
        return None, "normalize", _reason(exc)
    return events, None, None


def _read_snapshot(database: str | Path) -> tuple[SnapshotRow, ...]:
    """Read every shipment_events row from a strictly read-only, transactionally consistent snapshot."""
    path = Path(database)
    if not path.is_file():
        raise SnapshotError(f"database does not exist: {path}")
    uri = "file:" + quote(str(path.resolve()), safe="/") + "?mode=ro"
    connection = sqlite3.connect(uri, uri=True, isolation_level=None)
    try:
        connection.execute("PRAGMA query_only = ON")
        connection.execute("BEGIN")
        try:
            info = connection.execute("PRAGMA table_info(shipment_events)").fetchall()
            if not info:
                raise SnapshotError("table 'shipment_events' not found in snapshot")
            columns = {row[1] for row in info}
            missing = sorted(_REQUIRED_COLUMNS - columns)
            if missing:
                raise SnapshotError("unsupported shipment_events structure; missing columns: " + ", ".join(missing))
            if not any(row[1] == "event_key" and row[5] for row in info):
                raise SnapshotError("unsupported shipment_events structure; event_key is not the primary key")
            rows = connection.execute("SELECT event_key, status, sources FROM shipment_events ORDER BY event_key").fetchall()
        finally:
            connection.execute("ROLLBACK")
    finally:
        connection.close()
    return tuple(SnapshotRow(event_key=key, status=status, sources=_split_sources(sources)) for key, status, sources in rows)


def _diff_events(events: list[ShipmentEvent], rows: tuple[SnapshotRow, ...]) -> tuple[tuple[str, ...], tuple[str, ...], tuple[KeyChange, ...]]:
    replay_map = {event.event_key: event for event in events}
    snapshot_map = {row.event_key: row for row in rows}
    replay_keys = set(replay_map)
    snapshot_keys = set(snapshot_map)
    replay_only = tuple(sorted(replay_keys - snapshot_keys))
    snapshot_only = tuple(sorted(snapshot_keys - replay_keys))
    changed = []
    for key in sorted(replay_keys & snapshot_keys):
        event = replay_map[key]
        row = snapshot_map[key]
        replay_sources = tuple(sorted(set(event.sources)))
        if event.status != row.status or replay_sources != row.sources:
            changed.append(
                KeyChange(
                    event_key=key,
                    replay_status=event.status,
                    snapshot_status=row.status,
                    replay_sources=replay_sources,
                    snapshot_sources=row.sources,
                )
            )
    return replay_only, snapshot_only, tuple(changed)


def replay_fixture(fixture: str | Path, database: str | Path, timezone_name: str = "Asia/Shanghai") -> ReplayReport:
    """Replay a fixture offline and diff the expected events against the SQLite snapshot.

    Never writes to or creates the database. Failures in any stage return an incomplete
    ReplayReport naming the stage and the reason, with no diff entries.
    """
    events, stage, error = _replay_events(fixture, timezone_name)
    if events is None:
        return ReplayReport(complete=False, stage=stage or "fixture", error=error)
    try:
        rows = _read_snapshot(database)
    except (SnapshotError, sqlite3.Error, OSError) as exc:
        return ReplayReport(complete=False, stage="snapshot", error=_reason(exc), replayed=len(events))
    replay_only, snapshot_only, changed = _diff_events(events, rows)
    return ReplayReport(
        complete=True,
        stage="compare",
        replayed=len(events),
        snapshot_rows=len(rows),
        replay_only=replay_only,
        snapshot_only=snapshot_only,
        changed=changed,
    )


def _report_payload(report: ReplayReport) -> dict:
    return {
        "complete": report.complete,
        "stage": report.stage,
        "error": report.error,
        "replayed": report.replayed,
        "snapshot_rows": report.snapshot_rows,
        "replay_only": list(report.replay_only),
        "snapshot_only": list(report.snapshot_only),
        "changed": [
            {
                "event_key": change.event_key,
                "replay_status": change.replay_status,
                "snapshot_status": change.snapshot_status,
                "replay_sources": list(change.replay_sources),
                "snapshot_sources": list(change.snapshot_sources),
            }
            for change in report.changed
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="port-events-replay",
        description="Replay a fixture offline and diff it against a read-only SQLite snapshot. Exit 0: in sync; 1: differences; 2: replay incomplete.",
    )
    parser.add_argument("--fixture", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--timezone", default="Asia/Shanghai")
    args = parser.parse_args()
    report = replay_fixture(args.fixture, args.database, args.timezone)
    print(json.dumps(_report_payload(report), indent=2, sort_keys=True))
    if not report.complete:
        sys.exit(2)
    sys.exit(1 if (report.replay_only or report.snapshot_only or report.changed) else 0)


if __name__ == "__main__":
    main()
