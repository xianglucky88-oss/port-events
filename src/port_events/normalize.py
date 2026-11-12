from __future__ import annotations
from datetime import timezone
from .models import RawEvent, ShipmentEvent, observed_now
from .time_policy import TimeZonePolicy, interpret_timestamp

_EVENT_CODES = {"ATA": "arrived", "BERTH": "berthed", "DEPART": "departed", "GATE_IN": "gate_in", "LOAD": "loaded"}
_STATUS = {"actual": "actual", "estimated": "estimated", "cancelled": "cancelled"}


def normalize_event(
    raw: RawEvent,
    local_timezone: str = "Asia/Shanghai",
    *,
    timezone_policy: TimeZonePolicy | None = None,
) -> ShipmentEvent:
    """Normalize a raw record into a shipment event.

    ``local_timezone`` is the legacy global default for offset-less timestamps.
    It applies only when neither the record nor ``timezone_policy`` declares a
    zone for the record's source; it is never combined with a machine-local
    default.  Explicit-offset timestamps ignore it entirely.

    Time zone declaration precedence and daylight-saving handling live in
    :mod:`port_events.time_policy`; the provenance is attached to the event as
    ``time_interpretation`` (also exposed via ``applied_timezone``,
    ``time_rule``, ``time_origin`` and ``disambiguation``).
    """

    if raw.event_code not in _EVENT_CODES:
        raise ValueError(f"unsupported event code: {raw.event_code}")

    utc_time, interpretation = interpret_timestamp(
        raw.event_time,
        source=raw.source,
        record_timezone=raw.timezone,
        fold=raw.fold,
        policy=timezone_policy,
        global_default=local_timezone,
    )
    utc_iso = utc_time.astimezone(timezone.utc).isoformat()
    event_type = _EVENT_CODES[raw.event_code]
    event_key = "|".join((raw.vessel.strip().upper(), raw.voyage.strip().upper(), raw.container or "", raw.port.strip().upper(), event_type, utc_iso))
    return ShipmentEvent(
        event_key,
        raw.vessel.strip().upper(),
        raw.voyage.strip().upper(),
        raw.container,
        raw.port.strip().upper(),
        event_type,
        utc_iso,
        _STATUS.get(raw.status.lower(), "unknown"),
        (raw.source_ref,),
        observed_now(),
        interpretation,
    )
