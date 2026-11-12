from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Callable

from .models import ShipmentEvent
from .schema import (
    EVENTS_TABLE,
    MIGRATIONS_TABLE,
    QUARANTINE_TABLE,
    REGISTRY,
    SchemaError,
    SchemaLockError,
    SchemaManager,
)

_EVENT_COLUMNS = (
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
)


class EventStore:
    """Event persistence gated on the schema-version lifecycle.

    Opening a store recognises the database (fresh, current, adoptable,
    migratable) and drives it to the current version before any normal
    write is accepted.  Unknown or newer structures raise a diagnostic and
    leave the file untouched.

    ``checkpoint_observer`` is an optional callable invoked with checkpoint
    names (e.g. ``"rebuild.committing"``, ``"rebuild.committed"``,
    ``"upsert.committing"``); it exists for observability and fault
    injection in tests.
    """

    def __init__(
        self,
        database: str | Path,
        *,
        lock_timeout: float = 5.0,
        checkpoint_observer: Callable[[str], None] | None = None,
    ) -> None:
        self.database = str(database)
        self.connection = sqlite3.connect(
            self.database, timeout=lock_timeout, isolation_level=None
        )
        self._observer = checkpoint_observer
        self._ready = False
        try:
            SchemaManager(self.connection, REGISTRY, observer=checkpoint_observer).ensure_ready()
        except BaseException:
            self.connection.close()
            raise
        self._ready = True

    @property
    def ready(self) -> bool:
        return self._ready

    @property
    def schema_version(self) -> int | None:
        row = self.connection.execute("SELECT version FROM schema_meta WHERE id = 1").fetchone()
        return row[0] if row else None

    def upsert_many(self, events: list[ShipmentEvent]) -> None:
        if not self._ready:
            raise SchemaError("EventStore is not ready; schema lifecycle did not complete")
        sql = (
            f"INSERT OR REPLACE INTO {EVENTS_TABLE} ({', '.join(_EVENT_COLUMNS)}) "
            f"VALUES ({', '.join('?' for _ in _EVENT_COLUMNS)})"
        )
        params = [
            (
                e.event_key,
                e.vessel,
                e.voyage,
                e.container,
                e.port,
                e.event_type,
                e.event_time_utc,
                e.status,
                ",".join(e.sources),
                e.observed_at,
            )
            for e in events
        ]
        try:
            self.connection.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as exc:
            raise SchemaLockError(f"cannot start event write: {exc}") from exc
        try:
            self.connection.executemany(sql, params)
            self._observe("upsert.committing")
        except BaseException:
            self.connection.execute("ROLLBACK")
            raise
        self.connection.execute("COMMIT")

    def quarantined_rows(self) -> list[dict]:
        if not self._has_table(QUARANTINE_TABLE):
            return []
        cursor = self.connection.execute(
            f"SELECT quarantine_id, source_version, reason, row_payload, quarantined_at "
            f"FROM {QUARANTINE_TABLE} ORDER BY quarantine_id"
        )
        return [
            {
                "quarantine_id": row[0],
                "source_version": row[1],
                "reason": row[2],
                "row": json.loads(row[3]),
                "quarantined_at": row[4],
            }
            for row in cursor.fetchall()
        ]

    def migration_history(self) -> list[dict]:
        if not self._has_table(MIGRATIONS_TABLE):
            return []
        cursor = self.connection.execute(
            f"SELECT step, from_version, to_version, detail, recorded_at "
            f"FROM {MIGRATIONS_TABLE} ORDER BY seq"
        )
        return [
            {
                "step": row[0],
                "from_version": row[1],
                "to_version": row[2],
                "detail": row[3],
                "recorded_at": row[4],
            }
            for row in cursor.fetchall()
        ]

    def _has_table(self, name: str) -> bool:
        return (
            self.connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (name,)
            ).fetchone()
            is not None
        )

    def _observe(self, checkpoint: str) -> None:
        if self._observer is not None:
            self._observer(checkpoint)

    def close(self) -> None:
        self.connection.close()
