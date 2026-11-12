from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - import cycle only matters for typing
    from .time_policy import TimeInterpretation


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
    # Optional source context added by the time contract.  Records and
    # connectors that do not provide it keep working unchanged.
    timezone: str | None = None
    fold: int | None = None


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
    time_interpretation: "TimeInterpretation | None" = None

    @property
    def applied_timezone(self) -> str | None:
        """IANA zone used for an offset-less value; ``None`` for explicit offsets."""
        return None if self.time_interpretation is None else self.time_interpretation.applied_timezone

    @property
    def time_rule(self) -> str | None:
        """Authority that supplied the zone (see ``time_policy`` rule constants)."""
        return None if self.time_interpretation is None else self.time_interpretation.rule

    @property
    def time_origin(self) -> str | None:
        """Human-readable origin of the zone declaration."""
        return None if self.time_interpretation is None else self.time_interpretation.origin

    @property
    def disambiguation(self) -> str | None:
        """Basis used to resolve the local time (explicit offset, unambiguous, fold)."""
        return None if self.time_interpretation is None else self.time_interpretation.disambiguation


def observed_now() -> str:
    return datetime.now(timezone.utc).isoformat()
