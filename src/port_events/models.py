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


def observed_now() -> str:
    return datetime.now(timezone.utc).isoformat()
