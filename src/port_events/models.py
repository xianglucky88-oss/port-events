from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(frozen=True)
class RawEvent:
    source: str
    vessel: str
    voyage: str
    container: str | None
    port: str
    event_code: str
    event_time: str
    status: str
    source_ref: str


@dataclass(frozen=True)
class ShipmentEvent:
    event_key: str
    vessel: str
    voyage: str
    container: str | None
    port: str
    event_type: str
    event_time_utc: str
    status: str
    sources: tuple[str, ...]
    observed_at: str


@dataclass(frozen=True)
class BatchOutcome:
    """单个批次本轮的执行结果：committed / failed / skipped。"""
    source_id: str
    batch_id: str
    status: str
    event_count: int = 0
    error: str | None = None


def observed_now() -> str:
    return datetime.now(timezone.utc).isoformat()
