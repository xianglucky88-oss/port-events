import json

import pytest

from port_events import (
    AmbiguousLocalTimeError,
    InvalidFoldError,
    InvalidTimeZoneError,
    NonexistentLocalTimeError,
    RULE_EXPLICIT_OFFSET,
    RULE_GLOBAL_DEFAULT,
    RULE_RECORD_DECLARATION,
    RULE_SOURCE_DECLARATION,
    RULE_SOURCE_MAPPING,
    TimeZoneDeclarationConflict,
    TimeZonePolicy,
)
from port_events.connectors import JsonEventSource
from port_events.models import RawEvent, ShipmentEvent
from port_events.normalize import normalize_event
from port_events.pipeline import PortEventsPipeline
from port_events.storage import EventStore
from port_events.time_policy import interpret_timestamp


def _raw(**overrides):
    base = {
        "source": "s",
        "vessel": "V",
        "voyage": "1",
        "container": "C",
        "port": "P",
        "event_code": "ATA",
        "event_time": "2026-01-01T00:00:00",
        "status": "actual",
        "source_ref": "ref",
    }
    base.update(overrides)
    return RawEvent(**base)


def _write_events(tmp_path, payload):
    path = tmp_path / "events.json"
    path.write_text(json.dumps(payload))
    return path


def _run(tmp_path, payload, **kwargs):
    store = EventStore(tmp_path / "events.sqlite3")
    try:
        return PortEventsPipeline(JsonEventSource(_write_events(tmp_path, payload)), store).run(**kwargs)
    finally:
        store.close()


def test_events_are_correlated_and_actual_wins(tmp_path):
    source_path = tmp_path / "events.json"
    source_path.write_text('[{"source":"a","vessel":"V","voyage":"1","container":"C","port":"P","event_code":"ATA","event_time":"2026-01-01T00:00:00Z","status":"estimated","source_ref":"a1"},{"source":"b","vessel":"v","voyage":"1","container":"C","port":"p","event_code":"ATA","event_time":"2025-12-31T16:00:00-08:00","status":"actual","source_ref":"b1"}]')
    store = EventStore(tmp_path / "events.sqlite3")
    try:
        events = PortEventsPipeline(JsonEventSource(source_path), store).run()
        assert len(events) == 1
        assert events[0].status == "actual"
        assert events[0].sources == ("a1", "b1")
    finally:
        store.close()


# --- Explicit offsets keep their own meaning ------------------------------


def test_explicit_offset_ignores_declarations_and_default():
    raw = _raw(event_time="2026-11-10T08:00:00+08:00")
    event = normalize_event(
        raw,
        "Pacific/Kiritimati",
        timezone_policy=TimeZonePolicy.from_mapping(
            {"s": "America/New_York"}, origin="test"
        ),
    )
    assert event.event_time_utc == "2026-11-10T00:00:00+00:00"
    assert event.time_rule == RULE_EXPLICIT_OFFSET
    assert event.applied_timezone is None
    assert event.disambiguation == "explicit_offset"


def test_zulu_keeps_its_own_meaning():
    event = normalize_event(_raw(event_time="2026-11-10T00:00:00Z"), None)
    assert event.event_time_utc == "2026-11-10T00:00:00+00:00"
    assert event.time_rule == RULE_EXPLICIT_OFFSET


def test_equivalent_offsets_produce_same_event_key_and_utc():
    with_zone = normalize_event(_raw(event_time="2026-11-10T08:00:00+08:00"))
    with_zulu = normalize_event(_raw(event_time="2026-11-10T00:00:00Z"))
    assert with_zone.event_time_utc == with_zulu.event_time_utc
    assert with_zone.event_key == with_zulu.event_key


# --- Offset-less values: precedence and provenance ------------------------


def test_offset_less_uses_legacy_global_default_when_nothing_declared():
    event = normalize_event(_raw(event_time="2026-11-10T08:00:00"), "Asia/Shanghai")
    assert event.event_time_utc == "2026-11-10T00:00:00+00:00"
    assert event.time_rule == RULE_GLOBAL_DEFAULT
    assert event.applied_timezone == "Asia/Shanghai"


def test_source_mapping_beats_global_default():
    event = normalize_event(
        _raw(event_time="2026-11-10T08:00:00"),
        "Asia/Shanghai",
        timezone_policy=TimeZonePolicy.from_mapping({"s": "America/New_York"}, origin="caller"),
    )
    assert event.event_time_utc == "2026-11-10T13:00:00+00:00"
    assert event.time_rule == RULE_SOURCE_MAPPING
    assert event.applied_timezone == "America/New_York"


def test_connector_declaration_applies_over_global_default(tmp_path):
    payload = [
        {
            "source": "carrier",
            "vessel": "V",
            "voyage": "1",
            "container": "C",
            "port": "P",
            "event_code": "ATA",
            "event_time": "2026-11-10T08:00:00",
            "status": "actual",
            "source_ref": "r",
        }
    ]
    connector = JsonEventSource(
        _write_events(tmp_path, payload), source_timezones={"carrier": "America/New_York"}
    )
    store = EventStore(tmp_path / "e.sqlite3")
    try:
        events = PortEventsPipeline(connector, store).run("Asia/Shanghai")
    finally:
        store.close()
    assert events[0].event_time_utc == "2026-11-10T13:00:00+00:00"
    assert events[0].time_rule == RULE_SOURCE_DECLARATION
    assert events[0].applied_timezone == "America/New_York"


def test_connector_and_caller_declarations_that_disagree_conflict(tmp_path):
    payload = [
        {
            "source": "carrier",
            "vessel": "V",
            "voyage": "1",
            "container": "C",
            "port": "P",
            "event_code": "ATA",
            "event_time": "2026-11-10T08:00:00",
            "status": "actual",
            "source_ref": "r",
        }
    ]
    connector = JsonEventSource(
        _write_events(tmp_path, payload), source_timezones={"carrier": "America/New_York"}
    )
    store = EventStore(tmp_path / "e.sqlite3")
    try:
        with pytest.raises(TimeZoneDeclarationConflict):
            PortEventsPipeline(connector, store).run(
                "Asia/Shanghai", source_timezones={"carrier": "Asia/Tokyo"}
            )
    finally:
        store.close()


def test_record_declaration_beats_connector_declaration_when_they_agree():
    raw = _raw(event_time="2026-11-10T08:00:00", timezone="America/New_York")
    policy = TimeZonePolicy.from_mapping(
        {"s": "America/New_York"},
        origin="connector",
        rule=RULE_SOURCE_DECLARATION,
    )
    event = normalize_event(raw, "Asia/Shanghai", timezone_policy=policy)
    assert event.event_time_utc == "2026-11-10T13:00:00+00:00"
    assert event.time_rule == RULE_RECORD_DECLARATION
    assert event.applied_timezone == "America/New_York"


def test_record_and_source_declaration_conflict_is_rejected():
    raw = _raw(event_time="2026-11-10T08:00:00", timezone="America/New_York")
    policy = TimeZonePolicy.from_mapping({"s": "Asia/Tokyo"}, origin="connector")
    with pytest.raises(TimeZoneDeclarationConflict) as info:
        normalize_event(raw, "Asia/Shanghai", timezone_policy=policy)
    message = str(info.value)
    assert "America/New_York" in message and "Asia/Tokyo" in message


def test_same_source_with_two_caller_zones_conflicts(tmp_path):
    payload = [
        {
            "source": "carrier",
            "vessel": "V",
            "voyage": "1",
            "container": "C",
            "port": "P",
            "event_code": "ATA",
            "event_time": "2026-11-10T08:00:00",
            "status": "actual",
            "source_ref": "r",
        }
    ]
    connector = JsonEventSource(
        _write_events(tmp_path, payload), source_timezones={"carrier": "Asia/Tokyo"}
    )
    store = EventStore(tmp_path / "e.sqlite3")
    try:
        with pytest.raises(TimeZoneDeclarationConflict):
            PortEventsPipeline(connector, store).run(
                source_timezones={"carrier": "America/New_York"}
            )
    finally:
        store.close()


def test_invalid_timezone_is_rejected_not_fallen_back():
    with pytest.raises(InvalidTimeZoneError):
        normalize_event(_raw(event_time="2026-11-10T08:00:00"), "Not/AZone")
    # A bad source mapping fails when the policy is built, not silently
    # falling back to the global default.
    with pytest.raises(InvalidTimeZoneError):
        TimeZonePolicy.from_mapping({"s": "Not/AZone"}, origin="caller")


def test_offset_less_without_any_default_or_declaration_is_rejected():
    with pytest.raises(InvalidTimeZoneError):
        normalize_event(_raw(event_time="2026-11-10T08:00:00"), None)


def test_pipeline_can_disable_global_default_without_using_machine_local(tmp_path):
    payload = [
        {
            "source": "a",
            "vessel": "V",
            "voyage": "1",
            "container": "C",
            "port": "P",
            "event_code": "ATA",
            "event_time": "2026-11-10T08:00:00",
            "status": "actual",
            "source_ref": "a1",
        }
    ]
    with pytest.raises(InvalidTimeZoneError):
        _run(tmp_path, payload, timezone_name=None)


# --- Cross-source same literal, different zones ---------------------------


def test_same_literal_across_sources_uses_each_source_zone(tmp_path):
    payload = [
        {
            "source": "shanghai",
            "vessel": "V1",
            "voyage": "1",
            "container": "C",
            "port": "P",
            "event_code": "ATA",
            "event_time": "2026-11-10T08:00:00",
            "status": "actual",
            "source_ref": "s1",
        },
        {
            "source": "newyork",
            "vessel": "V2",
            "voyage": "1",
            "container": "C",
            "port": "P",
            "event_code": "ATA",
            "event_time": "2026-11-10T08:00:00",
            "status": "actual",
            "source_ref": "s2",
        },
    ]
    events = _run(
        tmp_path,
        payload,
        source_timezones={"shanghai": "Asia/Shanghai", "newyork": "America/New_York"},
    )
    by_source = {event.sources[0]: event for event in events}
    assert by_source["s1"].event_time_utc == "2026-11-10T00:00:00+00:00"
    assert by_source["s2"].event_time_utc == "2026-11-10T13:00:00+00:00"
    assert by_source["s1"].applied_timezone == "Asia/Shanghai"
    assert by_source["s2"].applied_timezone == "America/New_York"
    assert by_source["s1"].event_key != by_source["s2"].event_key


# --- Daylight-saving gaps and overlaps ------------------------------------


def test_nonexistent_local_time_is_rejected():
    with pytest.raises(NonexistentLocalTimeError) as info:
        normalize_event(
            _raw(event_time="2026-03-08T02:30:00"),
            "America/New_York",
        )
    assert "does not exist" in str(info.value)
    assert "event_key" in str(info.value)


def test_ambiguous_local_time_requires_disambiguation():
    with pytest.raises(AmbiguousLocalTimeError) as info:
        normalize_event(_raw(event_time="2026-11-01T01:30:00"), "America/New_York")
    assert "occurs twice" in str(info.value)
    assert "fold=0" in str(info.value) and "fold=1" in str(info.value)


def test_fold_selects_distinct_utc_and_event_key():
    first = normalize_event(
        _raw(event_time="2026-11-01T01:30:00", fold=0), "America/New_York"
    )
    second = normalize_event(
        _raw(event_time="2026-11-01T01:30:00", fold=1), "America/New_York"
    )
    assert first.event_time_utc == "2026-11-01T05:30:00+00:00"
    assert second.event_time_utc == "2026-11-01T06:30:00+00:00"
    assert first.disambiguation == "fold=0"
    assert second.disambiguation == "fold=1"
    assert first.event_key != second.event_key


def test_explicit_offset_resolves_ambiguous_literal_without_fold():
    event = normalize_event(_raw(event_time="2026-11-01T01:30:00-04:00"), "America/New_York")
    assert event.event_time_utc == "2026-11-01T05:30:00+00:00"
    assert event.time_rule == RULE_EXPLICIT_OFFSET


def test_invalid_fold_is_rejected():
    with pytest.raises(InvalidFoldError):
        normalize_event(_raw(event_time="2026-11-01T01:30:00", fold=2), "America/New_York")


def test_unambiguous_local_time_records_disambiguation():
    event = normalize_event(_raw(event_time="2026-11-01T00:30:00"), "America/New_York")
    assert event.event_time_utc == "2026-11-01T04:30:00+00:00"
    assert event.disambiguation == "unambiguous"


# --- Container-less records ----------------------------------------------


def test_container_less_records_are_handled(tmp_path):
    payload = [
        {
            "source": "a",
            "vessel": "V",
            "voyage": "1",
            "container": None,
            "port": "P",
            "event_code": "DEPART",
            "event_time": "2026-11-10T00:00:00Z",
            "status": "actual",
            "source_ref": "a1",
        },
        {
            "source": "b",
            "vessel": "V",
            "voyage": "1",
            "port": "P",
            "event_code": "DEPART",
            "event_time": "2026-11-10T00:00:00Z",
            "status": "estimated",
            "source_ref": "b1",
        },
    ]
    events = _run(tmp_path, payload)
    assert len(events) == 1
    assert events[0].container is None
    assert set(events[0].sources) == {"a1", "b1"}


# --- Backwards compatibility ---------------------------------------------


def test_legacy_call_signature_and_json_still_work(tmp_path):
    payload = [
        {
            "source": "a",
            "vessel": "V",
            "voyage": "1",
            "container": "C",
            "port": "P",
            "event_code": "ATA",
            "event_time": "2026-01-01T00:00:00",
            "status": "estimated",
            "source_ref": "a1",
        }
    ]
    events = _run(tmp_path, payload)
    assert events[0].event_time_utc == "2025-12-31T16:00:00+00:00"
    assert events[0].time_rule == RULE_GLOBAL_DEFAULT


def test_interpret_timestamp_exposes_provenance():
    utc, interpretation = interpret_timestamp(
        "2026-11-10T08:00:00", source="s", policy=None, global_default="Asia/Shanghai"
    )
    assert utc.isoformat() == "2026-11-10T00:00:00+00:00"
    assert interpretation.applied_timezone == "Asia/Shanghai"
    assert interpretation.rule == RULE_GLOBAL_DEFAULT
    assert interpretation.utc_offset == "+08:00"


def test_merged_event_keeps_strongest_interpretation(tmp_path):
    payload = [
        {
            "source": "a",
            "vessel": "V",
            "voyage": "1",
            "container": "C",
            "port": "P",
            "event_code": "ATA",
            "event_time": "2026-11-10T08:00:00+08:00",
            "status": "estimated",
            "source_ref": "a1",
        },
        {
            "source": "b",
            "vessel": "V",
            "voyage": "1",
            "container": "C",
            "port": "P",
            "event_code": "ATA",
            "event_time": "2026-11-10T00:00:00Z",
            "status": "actual",
            "source_ref": "b1",
        },
    ]
    events = _run(tmp_path, payload)
    assert len(events) == 1
    assert events[0].time_rule == RULE_EXPLICIT_OFFSET
