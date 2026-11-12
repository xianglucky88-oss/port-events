from __future__ import annotations
from typing import Mapping
from .connectors import EventSource
from .correlate import merge_events
from .models import ShipmentEvent
from .normalize import normalize_event
from .storage import EventStore
from .time_policy import (
    TimeZonePolicy,
    combine_time_zone_policies,
)


class PortEventsPipeline:
    def __init__(self, source: EventSource, store: EventStore) -> None:
        self.source = source
        self.store = store

    def run(
        self,
        timezone_name: str | None = "Asia/Shanghai",
        *,
        source_timezones: Mapping[str, str] | None = None,
        timezone_policy: TimeZonePolicy | None = None,
    ) -> list[ShipmentEvent]:
        """Normalize, correlate and persist all events from the source.

        ``timezone_name`` is the legacy global default for offset-less
        timestamps.  It ranks below any connector (``source_declaration``) or
        caller (``source_mapping``) declaration, and below a record-level
        declaration.  Pass ``None`` to disable it entirely.

        ``source_timezones`` lets the caller provide a per-source mapping;
        ``timezone_policy`` accepts an already-built policy.  Layers that
        disagree about a source raise :class:`TimeZoneDeclarationConflict`
        instead of silently preferring one.
        """

        caller_policy = TimeZonePolicy.from_mapping(
            source_timezones,
            origin="caller:run:source_timezones",
        )
        connector_policy = getattr(self.source, "timezone_policy", None)
        policy = combine_time_zone_policies(
            connector_policy, caller_policy, timezone_policy
        )

        normalized = [
            normalize_event(item, timezone_name, timezone_policy=policy)
            for item in self.source.read()
        ]
        events = merge_events(normalized)
        self.store.upsert_many(events)
        return events
