from __future__ import annotations
import sqlite3
from pathlib import Path
from .models import ShipmentEvent


class EventStore:
    """SQLite-backed event store.

    Import semantics are unchanged (``INSERT OR REPLACE`` keyed by
    ``event_key``). A monotonic ``store_revision`` is bumped in the same
    transaction as every write so that read contexts can identify which
    revision they are looking at.
    """

    def __init__(self, database: str | Path) -> None:
        self.database_path = Path(database)
        self.connection = sqlite3.connect(database)
        # WAL lets a long-lived read transaction keep a stable snapshot while
        # writers keep committing. In-memory databases cannot share data across
        # connections, so WAL and snapshots are only supported for file paths.
        self._supports_snapshots = database != ":memory:"
        if self._supports_snapshots:
            self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("""CREATE TABLE IF NOT EXISTS shipment_events (
            event_key TEXT PRIMARY KEY, vessel TEXT NOT NULL, voyage TEXT NOT NULL,
            container TEXT, port TEXT NOT NULL, event_type TEXT NOT NULL,
            event_time_utc TEXT NOT NULL, status TEXT NOT NULL, sources TEXT NOT NULL,
            observed_at TEXT NOT NULL
        )""")
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS store_revision ("
            "id INTEGER PRIMARY KEY CHECK (id = 1), revision INTEGER NOT NULL)"
        )
        self.connection.execute(
            "INSERT OR IGNORE INTO store_revision (id, revision) VALUES (1, 0)"
        )
        self.connection.commit()
        self._snapshots: set = set()

    def upsert_many(self, events: list[ShipmentEvent]) -> None:
        # Preserve the original import semantics: replace any row sharing the
        # same event_key. The revision bump is part of the same transaction.
        if not events:
            return
        rows = [
            (
                e.event_key, e.vessel, e.voyage, e.container, e.port,
                e.event_type, e.event_time_utc, e.status,
                ",".join(e.sources), e.observed_at,
            )
            for e in events
        ]
        with self.connection:
            self.connection.executemany(
                "INSERT OR REPLACE INTO shipment_events "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows
            )
            self.connection.execute(
                "UPDATE store_revision SET revision = revision + 1 WHERE id = 1"
            )

    def current_revision(self) -> int:
        row = self.connection.execute(
            "SELECT revision FROM store_revision WHERE id = 1"
        ).fetchone()
        return int(row[0])

    def open_snapshot(self, event_filter, ttl_seconds: float = 300.0):
        # Local import to avoid a circular import at module load time.
        from .snapshot import EventSnapshot

        return EventSnapshot(self, event_filter, ttl_seconds=ttl_seconds)

    def _register_snapshot(self, snapshot) -> None:
        self._snapshots.add(snapshot)

    def _unregister_snapshot(self, snapshot) -> None:
        self._snapshots.discard(snapshot)

    def close(self) -> None:
        for snapshot in list(self._snapshots):
            snapshot.close()
        self.connection.close()

    def __enter__(self) -> "EventStore":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
