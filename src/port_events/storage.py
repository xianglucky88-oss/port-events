from __future__ import annotations
import sqlite3
from pathlib import Path
from .models import ShipmentEvent

# Interpretations are not persisted: the table shape is unchanged, so existing
# rows and databases keep working and no migration is introduced.
_INSERT = "INSERT OR REPLACE INTO shipment_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"


class EventStore:
    def __init__(self, database: str | Path) -> None:
        self.connection = sqlite3.connect(database)
        self.connection.execute("""CREATE TABLE IF NOT EXISTS shipment_events (
            event_key TEXT PRIMARY KEY, vessel TEXT NOT NULL, voyage TEXT NOT NULL,
            container TEXT, port TEXT NOT NULL, event_type TEXT NOT NULL,
            event_time_utc TEXT NOT NULL, status TEXT NOT NULL, sources TEXT NOT NULL,
            observed_at TEXT NOT NULL
        )""")
        self.connection.commit()

    def upsert_many(self, events: list[ShipmentEvent]) -> None:
        self.connection.executemany(_INSERT, [(e.event_key, e.vessel, e.voyage, e.container, e.port, e.event_type, e.event_time_utc, e.status, ",".join(e.sources), e.observed_at) for e in events])
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()
