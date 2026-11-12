from __future__ import annotations
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from .models import RawEvent, ShipmentEvent, observed_now

_EVENT_CODES = {"ATA": "arrived", "BERTH": "berthed", "DEPART": "departed", "GATE_IN": "gate_in", "LOAD": "loaded"}
_STATUS = {"actual": "actual", "estimated": "estimated", "cancelled": "cancelled"}


def normalize_event(raw: RawEvent, local_timezone: str = "Asia/Shanghai") -> ShipmentEvent:
    if raw.event_code not in _EVENT_CODES:
        raise ValueError(f"unsupported event code: {raw.event_code}")
    local_time = datetime.fromisoformat(raw.event_time.replace("Z", "+00:00"))
    if local_time.tzinfo is None:
        local_time = local_time.replace(tzinfo=ZoneInfo(local_timezone))
    utc_time = local_time.astimezone(timezone.utc).isoformat()
    event_type = _EVENT_CODES[raw.event_code]
    event_key = "|".join((raw.vessel.strip().upper(), raw.voyage.strip().upper(), raw.container or "", raw.port.strip().upper(), event_type, utc_time))
    return ShipmentEvent(event_key, raw.vessel.strip().upper(), raw.voyage.strip().upper(), raw.container, raw.port.strip().upper(), event_type, utc_time, _STATUS.get(raw.status.lower(), "unknown"), (raw.source_ref,), observed_now())
