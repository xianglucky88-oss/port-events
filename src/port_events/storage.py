from __future__ import annotations
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from .correlate import merge_events
from .models import ShipmentEvent, observed_now

# 批次状态机：pending（已认领、结果未知）→ committed（终态）/ failed（可重试）
BATCH_STATES = ("pending", "failed", "committed")


@dataclass(frozen=True)
class BatchRecord:
    source_id: str
    batch_id: str
    state: str
    attempts: int
    last_error: str | None
    updated_at: str


class EventStore:
    def __init__(self, database: str | Path, timeout: float = 5.0) -> None:
        # isolation_level=None：事务由 _transaction 显式管理，保证
        # “事件写入 + 批次确认” 落在同一个 SQLite 事务里。
        self.connection = sqlite3.connect(database, timeout=timeout, isolation_level=None)
        self.connection.execute("""CREATE TABLE IF NOT EXISTS shipment_events (
            event_key TEXT PRIMARY KEY, vessel TEXT NOT NULL, voyage TEXT NOT NULL,
            container TEXT, port TEXT NOT NULL, event_type TEXT NOT NULL,
            event_time_utc TEXT NOT NULL, status TEXT NOT NULL, sources TEXT NOT NULL,
            observed_at TEXT NOT NULL
        )""")
        self.connection.execute("""CREATE TABLE IF NOT EXISTS event_batches (
            source_id TEXT NOT NULL, batch_id TEXT NOT NULL,
            state TEXT NOT NULL CHECK (state IN ('pending', 'failed', 'committed')),
            attempts INTEGER NOT NULL DEFAULT 0, last_error TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (source_id, batch_id)
        )""")

    @contextmanager
    def _transaction(self):
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            yield
        except Exception:
            self.connection.rollback()
            raise
        try:
            self.connection.commit()
        except Exception:
            # 提交结果不确定时尽力回滚；日志保持非 committed，下轮重试
            self.connection.rollback()
            raise

    # ---- 事件读写：跨批精确键按 merge_events 既有规则合并 ----

    def upsert_many(self, events: list[ShipmentEvent]) -> None:
        with self._transaction():
            self._merge_upsert(events)

    def _merge_upsert(self, events: list[ShipmentEvent]) -> None:
        if not events:
            return
        keys = sorted({event.event_key for event in events})
        placeholders = ", ".join("?" for _ in keys)
        rows = self.connection.execute(
            "SELECT event_key, vessel, voyage, container, port, event_type, "
            "event_time_utc, status, sources, observed_at "
            f"FROM shipment_events WHERE event_key IN ({placeholders})",
            keys,
        ).fetchall()
        merged = merge_events([self._row_to_event(row) for row in rows] + list(events))
        self.connection.executemany(
            "INSERT OR REPLACE INTO shipment_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    e.event_key, e.vessel, e.voyage, e.container, e.port, e.event_type,
                    e.event_time_utc, e.status, ",".join(e.sources), e.observed_at,
                )
                for e in merged
            ],
        )

    def fetch_events(self) -> list[ShipmentEvent]:
        rows = self.connection.execute(
            "SELECT event_key, vessel, voyage, container, port, event_type, "
            "event_time_utc, status, sources, observed_at "
            "FROM shipment_events ORDER BY event_time_utc, event_key"
        ).fetchall()
        return [self._row_to_event(row) for row in rows]

    @staticmethod
    def _row_to_event(row: tuple) -> ShipmentEvent:
        (event_key, vessel, voyage, container, port, event_type,
         event_time_utc, status, sources, observed_at) = row
        return ShipmentEvent(
            event_key, vessel, voyage, container, port, event_type, event_time_utc,
            status, tuple(sources.split(",")) if sources else (), observed_at,
        )

    # ---- 批次日志：待处理 / 失败 / 已提交 ----

    def batch_state(self, source_id: str, batch_id: str) -> str | None:
        row = self.connection.execute(
            "SELECT state FROM event_batches WHERE source_id = ? AND batch_id = ?",
            (source_id, batch_id),
        ).fetchone()
        return row[0] if row else None

    def begin_attempt(self, source_id: str, batch_id: str) -> bool:
        """认领一个待执行批次；已提交的同批重放返回 False（跳过）。"""
        with self._transaction():
            row = self.connection.execute(
                "SELECT state FROM event_batches WHERE source_id = ? AND batch_id = ?",
                (source_id, batch_id),
            ).fetchone()
            if row and row[0] == "committed":
                return False
            self.connection.execute(
                """INSERT INTO event_batches
                       (source_id, batch_id, state, attempts, last_error, updated_at)
                   VALUES (?, ?, 'pending', 1, NULL, ?)
                   ON CONFLICT(source_id, batch_id) DO UPDATE SET
                       state = 'pending', attempts = attempts + 1,
                       last_error = NULL, updated_at = excluded.updated_at""",
                (source_id, batch_id, observed_now()),
            )
            return True

    def mark_failed(self, source_id: str, batch_id: str, error: str) -> None:
        # 不降级已提交批次：提交结果不确定时，以日志中的 committed 为准
        with self._transaction():
            self.connection.execute(
                "UPDATE event_batches SET state = 'failed', last_error = ?, updated_at = ? "
                "WHERE source_id = ? AND batch_id = ? AND state != 'committed'",
                (error, observed_now(), source_id, batch_id),
            )

    def commit_batch(self, source_id: str, batch_id: str, events: list[ShipmentEvent]) -> None:
        """来源原子提交：事件写入与批次确认同事务，故障后二者必然对应。"""
        with self._transaction():
            self._merge_upsert(events)
            self.connection.execute(
                """INSERT INTO event_batches
                       (source_id, batch_id, state, attempts, last_error, updated_at)
                   VALUES (?, ?, 'committed', 0, NULL, ?)
                   ON CONFLICT(source_id, batch_id) DO UPDATE SET
                       state = 'committed', last_error = NULL,
                       updated_at = excluded.updated_at""",
                (source_id, batch_id, observed_now()),
            )

    def retryable_batches(self) -> list[BatchRecord]:
        """重启后待执行的批次：pending（上次中断）与 failed（上次失败）。"""
        rows = self.connection.execute(
            "SELECT source_id, batch_id, state, attempts, last_error, updated_at "
            "FROM event_batches WHERE state IN ('pending', 'failed') "
            "ORDER BY source_id, batch_id"
        ).fetchall()
        return [BatchRecord(*row) for row in rows]

    def close(self) -> None:
        self.connection.close()
