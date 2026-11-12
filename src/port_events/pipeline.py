from __future__ import annotations
from .connectors import EventSource
from .correlate import merge_events
from .models import ShipmentEvent
from .normalize import normalize_event
from .storage import EventStore


class PortEventsPipeline:
    def __init__(self, source: EventSource, store: EventStore) -> None:
        self.source = source
        self.store = store

    def run(self, timezone_name: str = "Asia/Shanghai") -> list[ShipmentEvent]:
        normalized = [normalize_event(item, timezone_name) for item in self.source.read()]
        events = merge_events(normalized)
        self.store.upsert_many(events)
        return events
