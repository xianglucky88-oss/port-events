from __future__ import annotations
import sqlite3
from pathlib import Path
from .models import ShipmentEvent


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
        self.connection.executemany("INSERT OR REPLACE INTO shipment_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [(e.event_key, e.vessel, e.voyage, e.container, e.port, e.event_type, e.event_time_utc, e.status, ",".join(e.sources), e.observed_at) for e in events])
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()


def load_shipment_events_readonly(database: str | Path) -> list[ShipmentEvent]:
    """只读加载已提交的 shipment_events 行。

    以 SQLite 只读 URI 打开：不建表、不采集、不恢复、不分页、不写回；
    库或表为空时返回空列表，而不是尝试修复任何数据。
    """
    uri = Path(database).expanduser().resolve().as_uri() + "?mode=ro"
    try:
        connection = sqlite3.connect(uri, uri=True)
        try:
            cursor = connection.execute(
                "SELECT event_key, vessel, voyage, container, port, event_type, "
                "event_time_utc, status, sources, observed_at FROM shipment_events"
            )
            rows = cursor.fetchall()
        finally:
            connection.close()
    except sqlite3.OperationalError:
        # 空库或缺失库（尚无 shipment_events 表）：空输入，不做任何恢复动作。
        return []
    return [
        ShipmentEvent(
            event_key=row[0],
            vessel=row[1],
            voyage=row[2],
            container=row[3],
            port=row[4],
            event_type=row[5],
            event_time_utc=row[6],
            status=row[7],
            sources=tuple(row[8].split(",")) if row[8] else (),
            observed_at=row[9],
        )
        for row in rows
    ]
