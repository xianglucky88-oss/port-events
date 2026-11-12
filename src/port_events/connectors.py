from __future__ import annotations
import json
from pathlib import Path
from typing import Mapping, Protocol
from .models import RawEvent
from .time_policy import (
    RULE_SOURCE_DECLARATION,
    TimeZonePolicy,
    combine_time_zone_policies,
)


class EventSource(Protocol):
    def read(self) -> list[RawEvent]: ...


class JsonEventSource:
    """JSON fixture connector.

    A connector may declare the time zone that governs offset-less timestamps
    for the sources it reads.  ``source_timezones`` maps a raw ``source`` value
    to an IANA zone and produces ``source_declaration`` entries.  Records may
    still declare their own ``timezone`` field; conflicting declarations are
    rejected during normalization rather than silently overridden.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        source_timezones: Mapping[str, str] | None = None,
        timezone_policy: TimeZonePolicy | None = None,
    ) -> None:
        self.path = Path(path)
        self.timezone_policy = self._build_policy(source_timezones, timezone_policy)

    @staticmethod
    def _build_policy(
        source_timezones: Mapping[str, str] | None,
        timezone_policy: TimeZonePolicy | None,
    ) -> TimeZonePolicy:
        declared = TimeZonePolicy.from_mapping(
            source_timezones,
            origin="connector:JsonEventSource:source_timezones",
            rule=RULE_SOURCE_DECLARATION,
        )
        # Merge connector-declared zones into one policy; a conflict here is
        # a connector programming error and must surface, not be hidden.
        return combine_time_zone_policies(declared, timezone_policy)

    def read(self) -> list[RawEvent]:
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        # Older fixtures may omit the "container" key entirely; a record with
        # no container number is valid, so normalize it to None.
        return [RawEvent(**{"container": None, **item}) for item in payload]
