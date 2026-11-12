from __future__ import annotations
import sqlite3
from typing import Iterable
from .connectors import EventSource, SourceBatch
from .correlate import merge_events
from .models import BatchOutcome, ShipmentEvent
from .normalize import normalize_event
from .storage import EventStore


class BatchRunReport:
    """一轮多来源批次的执行汇总。"""

    def __init__(self, outcomes: Iterable[BatchOutcome]) -> None:
        self.outcomes = tuple(outcomes)

    def with_status(self, status: str) -> tuple[BatchOutcome, ...]:
        return tuple(outcome for outcome in self.outcomes if outcome.status == status)

    @property
    def committed(self) -> tuple[BatchOutcome, ...]:
        return self.with_status("committed")

    @property
    def failed(self) -> tuple[BatchOutcome, ...]:
        return self.with_status("failed")

    @property
    def skipped(self) -> tuple[BatchOutcome, ...]:
        return self.with_status("skipped")


class PortEventsPipeline:
    def __init__(self, source: EventSource | None, store: EventStore) -> None:
        self.source = source
        self.store = store

    def run(self, timezone_name: str = "Asia/Shanghai") -> list[ShipmentEvent]:
        if self.source is None:
            raise ValueError("run() 需要单一来源；多来源批次模式请用 run_batches()")
        normalized = [normalize_event(item, timezone_name) for item in self.source.read()]
        events = merge_events(normalized)
        self.store.upsert_many(events)
        return events

    def run_batches(
        self,
        batches: Iterable[SourceBatch],
        timezone_name: str = "Asia/Shanghai",
    ) -> BatchRunReport:
        """一次接收多组独立来源批次；单批失败不影响其他来源。

        已提交批次跳过（同批重放），待处理/失败批次重试（含重启后恢复）。
        """
        return BatchRunReport(self._run_batch(batch, timezone_name) for batch in batches)

    def _run_batch(self, batch: SourceBatch, timezone_name: str) -> BatchOutcome:
        try:
            claimed = self.store.begin_attempt(batch.source_id, batch.batch_id)
        except sqlite3.OperationalError as exc:
            # 锁冲突：本批留待下轮重试，其他来源继续
            return BatchOutcome(batch.source_id, batch.batch_id, "failed", 0,
                                f"journal locked, retry later: {exc}")
        if not claimed:
            return BatchOutcome(batch.source_id, batch.batch_id, "skipped")
        try:
            raw = batch.source.read()
        except Exception as exc:
            # 读取失败（如承运人文件暂不可读）：本批失败可重试，其他来源继续
            return self._fail(batch, f"read failed: {exc}")
        try:
            normalized = [normalize_event(item, timezone_name) for item in raw]
        except Exception as exc:
            # 非法记录：整批失败，不写入任何一条
            return self._fail(batch, f"invalid record, whole batch rejected: {exc}")
        events = merge_events(normalized)
        try:
            self.store.commit_batch(batch.source_id, batch.batch_id, events)
        except sqlite3.OperationalError as exc:
            # 提交时锁冲突：事务未生效，下轮重试
            return self._fail(batch, f"commit failed (locked), retryable: {exc}")
        except Exception as exc:
            # 提交结果不确定：以批次日志为准，committed 跳过、否则重试
            return self._fail(batch, f"commit outcome uncertain, journal decides on restart: {exc}")
        return BatchOutcome(batch.source_id, batch.batch_id, "committed", len(events))

    def _fail(self, batch: SourceBatch, error: str) -> BatchOutcome:
        try:
            self.store.mark_failed(batch.source_id, batch.batch_id, error)
        except sqlite3.Error:
            pass  # 日志不可用（如锁冲突）：批次保持 pending，下轮仍会重试
        return BatchOutcome(batch.source_id, batch.batch_id, "failed", 0, error)
