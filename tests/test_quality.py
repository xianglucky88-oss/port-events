from datetime import datetime, timezone
import sqlite3

import pytest

from port_events.models import ShipmentEvent, observed_now
from port_events.quality import (
    ISSUE_MISSING_EVIDENCE,
    ISSUE_OUT_OF_ORDER,
    ISSUE_ROLE_CONFLICT,
    check_database,
    check_sequence,
    load_window_events,
    open_readonly_connection,
)
from port_events.storage import EventStore

VESSEL = "Ocean Star"
VOYAGE = "OS202611A"
PORT = "CNSHA"
START = "2026-11-10T00:00:00+00:00"
END = "2026-11-13T00:00:00+00:00"


def make_event(
    event_type,
    event_time,
    container=None,
    status="actual",
    vessel=VESSEL,
    voyage=VOYAGE,
    port=PORT,
):
    if isinstance(event_time, datetime):
        event_time = event_time.astimezone(timezone.utc).isoformat()
    key = "|".join(
        (
            vessel.strip().upper(),
            voyage.strip().upper(),
            container or "",
            port.strip().upper(),
            event_type,
            event_time,
        )
    )
    return ShipmentEvent(
        event_key=key,
        vessel=vessel.strip().upper(),
        voyage=voyage.strip().upper(),
        container=container,
        port=port.strip().upper(),
        event_type=event_type,
        event_time_utc=event_time,
        status=status,
        sources=("test",),
        observed_at=observed_now(),
    )


def build_database(tmp_path, events):
    database = tmp_path / "shipment.sqlite3"
    store = EventStore(database)
    try:
        store.upsert_many(events)
    finally:
        store.close()
    return database


def normal_events():
    # gate_in <= loaded <= departed, one departed shared by two containers.
    return [
        make_event("arrived", "2026-11-10T00:00:00+00:00"),
        make_event("berthed", "2026-11-10T04:00:00+00:00"),
        make_event("departed", "2026-11-12T10:30:00+00:00"),
        make_event("gate_in", "2026-11-10T06:00:00+00:00", "CONT001"),
        make_event("loaded", "2026-11-11T02:00:00+00:00", "CONT001"),
        make_event("gate_in", "2026-11-10T08:00:00+00:00", "CONT002"),
        make_event("loaded", "2026-11-12T10:30:00+00:00", "CONT002"),
    ]


def test_normal_sequence_has_no_issues(tmp_path):
    database = build_database(tmp_path, normal_events())
    issues = check_database(database, VESSEL, VOYAGE, PORT, START, END)
    assert issues == []


def test_equal_timestamps_are_ordered(tmp_path):
    events = [
        make_event("arrived", "2026-11-10T00:00:00+00:00"),
        make_event("berthed", "2026-11-10T00:00:00+00:00"),
        make_event("departed", "2026-11-10T00:00:00+00:00"),
        make_event("gate_in", "2026-11-10T00:00:00+00:00", "C1"),
        make_event("loaded", "2026-11-10T00:00:00+00:00", "C1"),
    ]
    database = build_database(tmp_path, events)
    issues = check_database(database, VESSEL, VOYAGE, PORT, START, END)
    assert [issue.issue_type for issue in issues] == []


def test_out_of_order_vessel_and_container_nodes(tmp_path):
    events = [
        make_event("arrived", "2026-11-10T08:00:00+00:00"),
        make_event("berthed", "2026-11-10T04:00:00+00:00"),
        make_event("departed", "2026-11-10T06:00:00+00:00"),
        make_event("gate_in", "2026-11-10T09:00:00+00:00", "C1"),
        make_event("loaded", "2026-11-10T07:00:00+00:00", "C1"),
    ]
    database = build_database(tmp_path, events)
    issues = check_database(database, VESSEL, VOYAGE, PORT, START, END)

    vessel_roles = {issue.role for issue in issues if issue.scope == "vessel"}
    # berthed (04:00) <= departed (06:00) is in order; only arrivals violate.
    assert vessel_roles == {"arrived>berthed", "arrived>departed"}
    container_issues = [issue for issue in issues if issue.scope == "container"]
    assert {issue.role for issue in container_issues} == {"gate_in>loaded", "loaded>departed"}
    assert all(issue.issue_type == ISSUE_OUT_OF_ORDER for issue in issues)
    # Every ordering issue references both compared event keys.
    assert all(len(issue.event_keys) == 2 for issue in issues)


def test_missing_nodes_report_missing_evidence(tmp_path):
    events = [
        make_event("arrived", "2026-11-10T00:00:00+00:00"),
        make_event("loaded", "2026-11-11T00:00:00+00:00", "C1"),
    ]
    database = build_database(tmp_path, events)
    issues = check_database(database, VESSEL, VOYAGE, PORT, START, END)
    missing = [issue for issue in issues if issue.issue_type == ISSUE_MISSING_EVIDENCE]
    assert {(issue.scope, issue.role, issue.container) for issue in missing} == {
        ("vessel", "berthed", None),
        ("vessel", "departed", None),
        ("container", "gate_in", "C1"),
    }
    assert all(issue.event_keys == () for issue in missing)
    # Ordering is not judged while a required node is absent: only missing evidence.
    assert {issue.issue_type for issue in issues} == {ISSUE_MISSING_EVIDENCE}


def test_empty_window_reports_all_vessel_roles_missing(tmp_path):
    database = build_database(tmp_path, normal_events())
    issues = check_database(
        database, VESSEL, VOYAGE, PORT,
        "2026-12-01T00:00:00+00:00", "2026-12-02T00:00:00+00:00",
    )
    assert [issue.role for issue in issues] == ["arrived", "berthed", "departed"]
    assert all(issue.issue_type == ISSUE_MISSING_EVIDENCE for issue in issues)
    assert all(issue.scope == "vessel" for issue in issues)


def test_duplicate_roles_report_conflicts_with_event_keys(tmp_path):
    events = normal_events() + [
        make_event("arrived", "2026-11-10T01:00:00+00:00"),
        make_event("gate_in", "2026-11-10T07:00:00+00:00", "CONT001"),
    ]
    database = build_database(tmp_path, events)
    issues = check_database(database, VESSEL, VOYAGE, PORT, START, END)

    conflicts = [issue for issue in issues if issue.issue_type == ISSUE_ROLE_CONFLICT]
    assert {(issue.scope, issue.role, issue.container) for issue in conflicts} == {
        ("vessel", "arrived", None),
        ("container", "gate_in", "CONT001"),
    }
    # One stable finding per duplicated row, each carrying its own event_key.
    vessel_arrived = [issue for issue in conflicts if issue.role == "arrived"]
    assert len(vessel_arrived) == 2
    assert all(len(issue.event_keys) == 1 for issue in conflicts)

    # Duplicated roles must not participate in ordering checks.
    assert not any(issue.issue_type == ISSUE_OUT_OF_ORDER for issue in issues)
    # Unique nodes are still checked for missing evidence: all other roles exist.
    assert not any(issue.issue_type == ISSUE_MISSING_EVIDENCE for issue in issues)


def test_duplicate_departed_blocks_shared_departed_ordering(tmp_path):
    events = normal_events() + [
        make_event("departed", "2026-11-12T11:00:00+00:00"),
    ]
    database = build_database(tmp_path, events)
    issues = check_database(database, VESSEL, VOYAGE, PORT, START, END)
    conflicts = [issue for issue in issues if issue.issue_type == ISSUE_ROLE_CONFLICT]
    assert len(conflicts) == 2
    assert {issue.event_keys[0] for issue in conflicts} == {
        events[2].event_key,
        events[-1].event_key,
    }
    assert not any(issue.role == "loaded>departed" for issue in issues)


def test_window_is_half_open_and_actual_only(tmp_path):
    events = normal_events() + [
        # Exactly at window end: excluded.
        make_event("departed", "2026-11-13T00:00:00+00:00"),
        # Exactly at window start: included.
        make_event("berthed", "2026-11-10T00:00:00+00:00"),
        # Estimated rows never participate.
        make_event("arrived", "2026-11-11T00:00:00+00:00", status="estimated"),
        make_event("loaded", "2026-11-11T00:00:00+00:00", "CONT003", status="estimated"),
    ]
    database = build_database(tmp_path, events)

    loaded = load_window_events(
        open_readonly_connection(database), VESSEL, VOYAGE, PORT, START, END
    )
    assert all(event.status == "actual" for event in loaded)
    assert "2026-11-13T00:00:00+00:00" not in {event.event_time_utc for event in loaded}

    # The unique departed inside the window is the 10:30 one; berthed is duplicated.
    issues = check_database(database, VESSEL, VOYAGE, PORT, START, END)
    berthed_conflicts = [
        issue for issue in issues
        if issue.issue_type == ISSUE_ROLE_CONFLICT and issue.role == "berthed"
    ]
    assert len(berthed_conflicts) == 2
    assert not any(
        issue.issue_type == ISSUE_MISSING_EVIDENCE and issue.role == "departed"
        for issue in issues
    )
    # Estimated-only container is never seen, so no CONT003 findings.
    assert all(issue.container != "CONT003" for issue in issues)


def test_identity_filtering_keeps_other_calls_untouched(tmp_path):
    other_voyage = make_event(
        "arrived", "2026-11-11T00:00:00+00:00", voyage="OS202611B"
    )
    other_port = make_event(
        "arrived", "2026-11-11T00:00:00+00:00", port="CNNGB"
    )
    database = build_database(tmp_path, normal_events() + [other_voyage, other_port])
    issues = check_database(database, VESSEL, VOYAGE, PORT, START, END)
    assert issues == []


def test_check_is_readonly(tmp_path):
    database = build_database(tmp_path, normal_events())
    before = database.read_bytes()
    connection = open_readonly_connection(database)
    try:
        with pytest.raises(sqlite3.OperationalError):
            connection.execute("DELETE FROM shipment_events")
        with pytest.raises(sqlite3.OperationalError):
            connection.execute("CREATE TABLE tampered (id INTEGER)")
    finally:
        connection.close()
    assert database.read_bytes() == before
    assert check_database(database, VESSEL, VOYAGE, PORT, START, END) == []


def test_invalid_window_rejected(tmp_path):
    database = build_database(tmp_path, normal_events())
    with pytest.raises(ValueError):
        check_database(database, VESSEL, VOYAGE, PORT, END, START)


def test_output_is_deterministic_and_carries_event_keys(tmp_path):
    events = [
        make_event("arrived", "2026-11-10T08:00:00+00:00"),
        make_event("berthed", "2026-11-10T04:00:00+00:00"),
        make_event("departed", "2026-11-10T06:00:00+00:00"),
        make_event("gate_in", "2026-11-10T09:00:00+00:00", "C2"),
        make_event("loaded", "2026-11-10T07:00:00+00:00", "C2"),
        make_event("gate_in", "2026-11-10T09:00:00+00:00", "C1"),
        make_event("loaded", "2026-11-10T07:00:00+00:00", "C1"),
    ]
    database = build_database(tmp_path, events)
    first = check_database(database, VESSEL, VOYAGE, PORT, START, END)
    second = check_database(database, VESSEL, VOYAGE, PORT, START, END)
    assert first == second
    rendered = [
        (issue.issue_type, issue.scope, issue.container or "", issue.role, issue.event_keys)
        for issue in first
    ]
    assert rendered == sorted(rendered, key=lambda row: (row[1], row[2], row[0], row[3], row[4]))


def test_check_sequence_accepts_naive_utc_datetime_bounds(tmp_path):
    database = build_database(tmp_path, normal_events())
    issues = check_database(
        database, VESSEL, VOYAGE, PORT,
        datetime(2026, 11, 10, 0, 0, 0),
        datetime(2026, 11, 13, 0, 0, 0),
    )
    assert issues == []
