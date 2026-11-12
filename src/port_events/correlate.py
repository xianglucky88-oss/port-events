from __future__ import annotations
from .models import ShipmentEvent
from .time_policy import (
    RULE_EXPLICIT_OFFSET,
    RULE_GLOBAL_DEFAULT,
    RULE_RECORD_DECLARATION,
    RULE_SOURCE_DECLARATION,
    RULE_SOURCE_MAPPING,
    TimeInterpretation,
)

# Merging keeps one interpretation for the merged row.  When records agree on
# the UTC instant but were interpreted differently, the strongest authority
# wins so the retained provenance is the most directly anchored one.
_RULE_STRENGTH = {
    RULE_EXPLICIT_OFFSET: 0,
    RULE_RECORD_DECLARATION: 1,
    RULE_SOURCE_DECLARATION: 2,
    RULE_SOURCE_MAPPING: 3,
    RULE_GLOBAL_DEFAULT: 4,
}


def _select_interpretation(
    previous: TimeInterpretation | None,
    current: TimeInterpretation | None,
) -> TimeInterpretation | None:
    if previous is None:
        return current
    if current is None:
        return previous
    previous_rank = (_RULE_STRENGTH.get(previous.rule, 9), previous.origin, previous.applied_timezone or "")
    current_rank = (_RULE_STRENGTH.get(current.rule, 9), current.origin, current.applied_timezone or "")
    return previous if previous_rank <= current_rank else current


def merge_events(events: list[ShipmentEvent]) -> list[ShipmentEvent]:
    merged: dict[str, ShipmentEvent] = {}
    for event in events:
        previous = merged.get(event.event_key)
        if previous is None:
            merged[event.event_key] = event
        else:
            sources = tuple(sorted(set(previous.sources + event.sources)))
            status = "actual" if "actual" in (previous.status, event.status) else previous.status
            merged[event.event_key] = ShipmentEvent(
                event.event_key,
                event.vessel,
                event.voyage,
                event.container,
                event.port,
                event.event_type,
                event.event_time_utc,
                status,
                sources,
                max(previous.observed_at, event.observed_at),
                _select_interpretation(
                    previous.time_interpretation, event.time_interpretation
                ),
            )
    return sorted(merged.values(), key=lambda item: item.event_time_utc)
