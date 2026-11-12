from __future__ import annotations
from .models import ShipmentEvent


def merge_events(events: list[ShipmentEvent]) -> list[ShipmentEvent]:
    merged: dict[str, ShipmentEvent] = {}
    for event in events:
        previous = merged.get(event.event_key)
        if previous is None:
            merged[event.event_key] = event
        else:
            sources = tuple(sorted(set(previous.sources + event.sources)))
            status = "actual" if "actual" in (previous.status, event.status) else previous.status
            merged[event.event_key] = ShipmentEvent(event.event_key, event.vessel, event.voyage, event.container, event.port, event.event_type, event.event_time_utc, status, sources, max(previous.observed_at, event.observed_at))
    return sorted(merged.values(), key=lambda item: item.event_time_utc)
