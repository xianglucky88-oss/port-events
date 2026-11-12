from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from typing import Optional

from .models import (
    STATE_ABANDONED,
    STATE_DELIVERED,
    STATE_QUEUED,
    STATE_REJECTED,
    STATE_RETRYABLE,
    STATE_SENDING,
    Acceptance,
    DeliveryOutcome,
    DeliveryRequest,
    EnvelopeRecord,
    Progress,
    RevisionConflictError,
    delivery_identity,
)

_TERMINAL_STATES = (STATE_DELIVERED, STATE_ABANDONED)
_DISPATCHABLE_STATES = (STATE_QUEUED, STATE_RETRYABLE)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS envelopes (
    delivery_id      TEXT PRIMARY KEY,
    recipient        TEXT NOT NULL,
    event_key        TEXT NOT NULL,
    revision         INTEGER NOT NULL,
    payload          BLOB NOT NULL,
    state            TEXT NOT NULL,
    attempt          INTEGER NOT NULL DEFAULT 0,
    not_before       REAL NOT NULL DEFAULT 0,
    reject_reason    TEXT NOT NULL DEFAULT '',
    abandoned_reason TEXT NOT NULL DEFAULT '',
    owner            TEXT,
    lease_expires    REAL,
    created_epoch    INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_envelopes_triple
    ON envelopes (recipient, event_key, revision);
CREATE INDEX IF NOT EXISTS idx_envelopes_dispatch
    ON envelopes (state, not_before);

CREATE TABLE IF NOT EXISTS streams (
    recipient       TEXT NOT NULL,
    event_key       TEXT NOT NULL,
    high_water_mark INTEGER NOT NULL DEFAULT 0,
    blocked_reason  TEXT,
    PRIMARY KEY (recipient, event_key)
);

CREATE TABLE IF NOT EXISTS attempts_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    delivery_id TEXT NOT NULL,
    attempt     INTEGER NOT NULL,
    epoch       INTEGER NOT NULL,
    kind        TEXT NOT NULL,
    reason      TEXT NOT NULL DEFAULT '',
    at_epoch_of_sender INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);
"""


class DeliveryStore:
    """Independent durable store for accepted envelopes and delivery progress.

    It owns a dedicated SQLite database that is never shared with the event
    store; the only way an envelope enters the database is :meth:`accept`.
    """

    def __init__(self, database: str | Path) -> None:
        self.database = str(database)
        self.connection = sqlite3.connect(
            self.database, timeout=30.0, check_same_thread=False
        )
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.executescript(_SCHEMA)
        self.connection.execute(
            "INSERT OR IGNORE INTO meta (key, value) VALUES ('epoch', 0)"
        )
        self.connection.commit()
        # SQLite serializes writes itself, but a single connection shared by
        # worker threads still needs an application lock around transactions
        # so nested BEGIN/COMMIT statements cannot interleave.
        self._tx_lock = threading.RLock()

    def close(self) -> None:
        self.connection.close()

    # ------------------------------------------------------------------ meta
    def get_epoch(self) -> int:
        row = self.connection.execute(
            "SELECT value FROM meta WHERE key='epoch'"
        ).fetchone()
        return int(row["value"])

    # ---------------------------------------------------------------- accept
    def accept(
        self,
        recipient: str,
        event_key: str,
        revision: int,
        payload: bytes,
    ) -> Acceptance:
        """Persist an envelope. Returns after durable commit.

        Re-submission of the same triple is idempotent; identical bytes yield
        the original identity, different bytes raise ``RevisionConflictError``.
        """
        delivery_id = delivery_identity(recipient, event_key, revision)
        with self._tx_lock, self.connection:
            self.connection.execute(
                "INSERT OR IGNORE INTO streams (recipient, event_key) VALUES (?, ?)",
                (recipient, event_key),
            )
            existing = self.connection.execute(
                "SELECT payload, state FROM envelopes WHERE delivery_id=?",
                (delivery_id,),
            ).fetchone()
            if existing is not None:
                if bytes(existing["payload"]) != bytes(payload):
                    raise RevisionConflictError(
                        f"revision {revision} of {recipient}/{event_key} was "
                        f"already accepted with different payload bytes"
                    )
                return Acceptance(
                    delivery_id=delivery_id,
                    recipient=recipient,
                    event_key=event_key,
                    revision=revision,
                    is_duplicate=True,
                    state=existing["state"],
                )
            self.connection.execute(
                """INSERT INTO envelopes
                   (delivery_id, recipient, event_key, revision, payload,
                    state, attempt, not_before, created_epoch)
                   VALUES (?, ?, ?, ?, ?, ?, 0, 0, ?)""",
                (
                    delivery_id,
                    recipient,
                    event_key,
                    revision,
                    bytes(payload),
                    STATE_QUEUED,
                    self.get_epoch(),
                ),
            )
            self._refresh_stream(recipient, event_key)
        return Acceptance(
            delivery_id=delivery_id,
            recipient=recipient,
            event_key=event_key,
            revision=revision,
            is_duplicate=False,
            state=STATE_QUEUED,
        )

    # ---------------------------------------------------------------- dispatch
    def claim_due(
        self, now: float, lease_seconds: float, owner: str
    ) -> Optional[DeliveryRequest]:
        """Atomically claim the next deliverable envelope.

        An envelope is deliverable only when every earlier revision of its
        stream is delivered or explicitly abandoned; a missing predecessor
        therefore blocks later revisions inside the query itself.
        """
        conn = self.connection
        with self._tx_lock, conn:
            epoch = self.get_epoch()
            row = conn.execute(
                """
                SELECT e.* FROM envelopes e
                WHERE e.state IN (?, ?) AND e.not_before <= ?
                  AND (
                      e.revision = 1
                      OR EXISTS (
                          SELECT 1 FROM envelopes prev
                          WHERE prev.recipient = e.recipient
                            AND prev.event_key = e.event_key
                            AND prev.revision = e.revision - 1
                            AND prev.state IN (?, ?)
                      )
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM envelopes p
                      WHERE p.recipient = e.recipient
                        AND p.event_key = e.event_key
                        AND p.revision < e.revision
                        AND p.state NOT IN (?, ?)
                  )
                ORDER BY e.recipient, e.event_key, e.revision
                LIMIT 1
                """,
                (
                    STATE_QUEUED,
                    STATE_RETRYABLE,
                    now,
                    STATE_DELIVERED,
                    STATE_ABANDONED,
                    STATE_DELIVERED,
                    STATE_ABANDONED,
                ),
            ).fetchone()
            if row is None:
                return None
            cursor = conn.execute(
                """UPDATE envelopes
                   SET state=?, attempt=attempt+1, owner=?, lease_expires=?
                   WHERE delivery_id=? AND state IN (?, ?)""",
                (
                    STATE_SENDING,
                    owner,
                    now + lease_seconds,
                    row["delivery_id"],
                    STATE_QUEUED,
                    STATE_RETRYABLE,
                ),
            )
            if cursor.rowcount != 1:  # pragma: no cover - defensive
                return None
            self._refresh_stream(row["recipient"], row["event_key"])
            return DeliveryRequest(
                delivery_id=row["delivery_id"],
                recipient=row["recipient"],
                event_key=row["event_key"],
                revision=row["revision"],
                payload=bytes(row["payload"]),
                attempt=row["attempt"] + 1,
                epoch=epoch,
            )

    def record_outcome(
        self,
        delivery_id: str,
        owner: str,
        outcome: DeliveryOutcome,
        now: float,
        base_backoff: float,
        max_backoff: float,
    ) -> str:
        """Apply an adapter outcome.

        Returns ``"applied"`` or ``"stale"``.  A stale outcome (wrong owner,
        or the envelope is no longer in flight for this sender) is ignored:
        late receipts after takeover can never advance progress.
        """
        conn = self.connection
        with self._tx_lock, conn:
            row = conn.execute(
                "SELECT state, owner, attempt, recipient, event_key FROM envelopes "
                "WHERE delivery_id=?",
                (delivery_id,),
            ).fetchone()
            if row is None:
                return "stale"
            if row["state"] != STATE_SENDING or row["owner"] != owner:
                return "stale"

            epoch = self.get_epoch()
            kind = outcome.kind
            if kind == "acked":
                conn.execute(
                    """UPDATE envelopes SET state=?, owner=NULL, lease_expires=NULL,
                       reject_reason='' WHERE delivery_id=?""",
                    (STATE_DELIVERED, delivery_id),
                )
            elif kind == "retryable":
                delay = min(
                    max_backoff, base_backoff * (2 ** max(0, row["attempt"] - 1))
                )
                conn.execute(
                    """UPDATE envelopes SET state=?, owner=NULL, lease_expires=NULL,
                       not_before=? WHERE delivery_id=?""",
                    (STATE_RETRYABLE, now + delay, delivery_id),
                )
            elif kind == "rejected":
                conn.execute(
                    """UPDATE envelopes SET state=?, owner=NULL, lease_expires=NULL,
                       reject_reason=? WHERE delivery_id=?""",
                    (STATE_REJECTED, outcome.reason, delivery_id),
                )
            else:  # fenced: another owner leads; requeue for the new owner
                conn.execute(
                    """UPDATE envelopes SET state=?, owner=NULL, lease_expires=NULL,
                       not_before=? WHERE delivery_id=?""",
                    (STATE_QUEUED, now, delivery_id),
                )
            conn.execute(
                """INSERT INTO attempts_log
                   (delivery_id, attempt, epoch, kind, reason, at_epoch_of_sender)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    delivery_id,
                    row["attempt"],
                    epoch,
                    kind,
                    outcome.reason,
                    epoch,
                ),
            )
            self._refresh_stream(row["recipient"], row["event_key"])
        return "applied"

    # ------------------------------------------------------------- ownership
    def recover_expired_leases(self, now: float) -> int:
        """Requeue in-flight envelopes whose lease expired (sender crash)."""
        with self._tx_lock, self.connection:
            affected_streams = self._streams_of_sending()
            cursor = self.connection.execute(
                """UPDATE envelopes SET state=?, owner=NULL, lease_expires=NULL
                   WHERE state=? AND lease_expires IS NOT NULL AND lease_expires <= ?""",
                (STATE_QUEUED, STATE_SENDING, now),
            )
            count = cursor.rowcount
            for recipient, event_key in affected_streams:
                self._refresh_stream(recipient, event_key)
        return count

    def takeover(self, now: float) -> int:
        """Take leadership: reclaim every in-flight envelope and bump epoch.

        The epoch bump fences the old sender: any request it still sends
        carries the old epoch, and any receipt it returns is rejected by the
        owner guard in :meth:`record_outcome`.
        """
        conn = self.connection
        with self._tx_lock, conn:
            affected_streams = self._streams_of_sending()
            conn.execute(
                """UPDATE envelopes SET state=?, owner=NULL, lease_expires=NULL
                   WHERE state=?""",
                (STATE_QUEUED, STATE_SENDING),
            )
            conn.execute(
                "UPDATE meta SET value=value+1 WHERE key='epoch'"
            )
            for recipient, event_key in affected_streams:
                self._refresh_stream(recipient, event_key)
            epoch = self.get_epoch()
        return epoch

    def _streams_of_sending(self) -> list[tuple[str, str]]:
        rows = self.connection.execute(
            "SELECT DISTINCT recipient, event_key FROM envelopes "
            "WHERE state=?",
            (STATE_SENDING,),
        ).fetchall()
        return [(r["recipient"], r["event_key"]) for r in rows]

    # ------------------------------------------------------ human decisions
    def retry_rejected(self, delivery_id: str) -> None:
        """Explicitly retry a permanently rejected envelope."""
        with self._tx_lock, self.connection:
            row = self._require_row(delivery_id)
            if row["state"] != STATE_REJECTED:
                raise RevisionConflictError(
                    f"envelope {delivery_id} is in state {row['state']}, "
                    "only rejected envelopes can be retried"
                )
            self.connection.execute(
                """UPDATE envelopes SET state=?, reject_reason='', owner=NULL,
                   lease_expires=NULL, not_before=0 WHERE delivery_id=?""",
                (STATE_QUEUED, delivery_id),
            )
            self._refresh_stream(row["recipient"], row["event_key"])

    def abandon(self, delivery_id: str, reason: str) -> None:
        """Give up a rejected envelope, unblocking later revisions."""
        with self._tx_lock, self.connection:
            row = self._require_row(delivery_id)
            if row["state"] != STATE_REJECTED:
                raise RevisionConflictError(
                    f"envelope {delivery_id} is in state {row['state']}, "
                    "only rejected envelopes can be abandoned"
                )
            self.connection.execute(
                """UPDATE envelopes SET state=?, abandoned_reason=?, owner=NULL,
                   lease_expires=NULL WHERE delivery_id=?""",
                (STATE_ABANDONED, reason, delivery_id),
            )
            self._refresh_stream(row["recipient"], row["event_key"])

    def _require_row(self, delivery_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM envelopes WHERE delivery_id=?", (delivery_id,)
        ).fetchone()
        if row is None:
            raise RevisionConflictError(f"unknown delivery identity {delivery_id}")
        return row

    # -------------------------------------------------------------- queries
    def get_envelope(self, delivery_id: str) -> EnvelopeRecord:
        row = self._require_row(delivery_id)
        return EnvelopeRecord(
            delivery_id=row["delivery_id"],
            recipient=row["recipient"],
            event_key=row["event_key"],
            revision=row["revision"],
            payload=bytes(row["payload"]),
            state=row["state"],
            attempt=row["attempt"],
            not_before=row["not_before"],
            reject_reason=row["reject_reason"],
            abandoned_reason=row["abandoned_reason"],
        )

    def get_identity(self, recipient: str, event_key: str, revision: int) -> str:
        return delivery_identity(recipient, event_key, revision)

    def progress(self, recipient: str, event_key: str) -> Progress:
        stream = self.connection.execute(
            "SELECT * FROM streams WHERE recipient=? AND event_key=?",
            (recipient, event_key),
        ).fetchone()
        if stream is None:
            return Progress(
                recipient=recipient,
                event_key=event_key,
                high_water_mark=0,
                delivered_revisions=(),
                blocked_reason=None,
            )
        delivered = tuple(
            r["revision"]
            for r in self.connection.execute(
                "SELECT revision FROM envelopes WHERE recipient=? AND event_key=? "
                "AND state=? ORDER BY revision",
                (recipient, event_key, STATE_DELIVERED),
            )
        )
        return Progress(
            recipient=recipient,
            event_key=event_key,
            high_water_mark=stream["high_water_mark"],
            delivered_revisions=delivered,
            blocked_reason=stream["blocked_reason"],
        )

    def attempt_count(self, delivery_id: str) -> int:
        row = self.connection.execute(
            "SELECT COUNT(*) AS n FROM attempts_log WHERE delivery_id=?",
            (delivery_id,),
        ).fetchone()
        return int(row["n"])

    # ------------------------------------------------------------- internals
    def _refresh_stream(self, recipient: str, event_key: str) -> None:
        rows = self.connection.execute(
            "SELECT revision, state, reject_reason FROM envelopes "
            "WHERE recipient=? AND event_key=? ORDER BY revision",
            (recipient, event_key),
        ).fetchall()
        high_water_mark = 0
        expected = 1
        blocked_reason: Optional[str] = None
        for row in rows:
            if row["revision"] != expected:
                blocked_reason = f"missing revision {expected}"
                break
            if row["state"] in _TERMINAL_STATES:
                high_water_mark = row["revision"]
            elif row["state"] == STATE_REJECTED:
                blocked_reason = f"revision {row['revision']} rejected: {row['reject_reason']}"
                break
            else:
                blocked_reason = f"revision {row['revision']} pending ({row['state']})"
                break
            expected += 1
        self.connection.execute(
            """INSERT INTO streams (recipient, event_key, high_water_mark, blocked_reason)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(recipient, event_key) DO UPDATE SET
                   high_water_mark=excluded.high_water_mark,
                   blocked_reason=excluded.blocked_reason""",
            (recipient, event_key, high_water_mark, blocked_reason),
        )
