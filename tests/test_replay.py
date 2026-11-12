import hashlib
import inspect
import json
import sqlite3
import threading
import time

import pytest

import port_events.replay as replay_module
from port_events.connectors import JsonEventSource
from port_events.correlate import merge_events
from port_events.normalize import normalize_event
from port_events.pipeline import PortEventsPipeline
from port_events.replay import replay_fixture
from port_events.storage import EventStore

FIXTURE_ITEMS = [
    {"source": "carrier-api", "vessel": "Ocean Star", "voyage": "OS1", "container": "C1", "port": "CNSHA", "event_code": "ATA", "event_time": "2026-11-10T08:00:00+08:00", "status": "actual", "source_ref": "carrier-1"},
    {"source": "terminal-api", "vessel": "ocean star", "voyage": "OS1", "container": "C1", "port": "cnsha", "event_code": "ATA", "event_time": "2026-11-10T00:00:00Z", "status": "estimated", "source_ref": "terminal-7"},
    {"source": "carrier-api", "vessel": "Ocean Star", "voyage": "OS1", "container": "C1", "port": "CNSHA", "event_code": "DEPART", "event_time": "2026-11-12T18:30:00+08:00", "status": "estimated", "source_ref": "carrier-2"},
]


def write_fixture(tmp_path, items=FIXTURE_ITEMS, name="events.json"):
    path = tmp_path / name
    path.write_text(json.dumps(items), encoding="utf-8")
    return path


def run_pipeline(fixture, database, timezone="Asia/Shanghai"):
    store = EventStore(database)
    try:
        return PortEventsPipeline(JsonEventSource(fixture), store).run(timezone)
    finally:
        store.close()


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def sql(database, statement, parameters=()):
    connection = sqlite3.connect(database)
    try:
        connection.execute(statement, parameters)
        connection.commit()
    finally:
        connection.close()


def test_replay_matches_snapshot_and_never_writes(tmp_path):
    fixture = write_fixture(tmp_path)
    database = tmp_path / "events.sqlite3"
    events = run_pipeline(fixture, database)
    before = digest(database)

    report = replay_fixture(fixture, database)

    assert report.complete
    assert report.stage == "compare"
    assert report.error is None
    assert report.replayed == len(events) == 2
    assert report.snapshot_rows == 2
    assert report.replay_only == ()
    assert report.snapshot_only == ()
    assert report.changed == ()
    # No external writes happened, so the database bytes must be identical after replay.
    assert digest(database) == before

    # Regression ingest: the pipeline still runs cleanly against the replayed database.
    again = run_pipeline(fixture, database)
    assert [event.event_key for event in again] == [event.event_key for event in events]
    assert replay_fixture(fixture, database).changed == ()


def test_replay_module_has_no_write_path():
    # The replay module must not reach the store at all; keep it that way.
    source = inspect.getsource(replay_module)
    assert "EventStore" not in source
    assert "from .storage" not in source
    assert "import storage" not in source


def test_replay_reports_each_side_and_same_key_changes(tmp_path):
    fixture = write_fixture(tmp_path)
    database = tmp_path / "events.sqlite3"
    events = run_pipeline(fixture, database)
    ata_key, depart_key = (event.event_key for event in events)

    sql(database, "DELETE FROM shipment_events WHERE event_key = ?", (depart_key,))
    sql(database, "UPDATE shipment_events SET status = 'cancelled', sources = 'terminal-7' WHERE event_key = ?", (ata_key,))
    extra_key = "EXTRA|1|C|P|loaded|2026-11-11T00:00:00+00:00"
    sql(
        database,
        "INSERT INTO shipment_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (extra_key, "EXTRA", "1", "C", "P", "loaded", "2026-11-11T00:00:00+00:00", "actual", "x1", "2026-11-11T00:00:00+00:00"),
    )

    report = replay_fixture(fixture, database)

    assert report.complete
    # Keys unique to one side are listed as-is; no approximate match or deletion inference.
    assert report.replay_only == (depart_key,)
    assert report.snapshot_only == (extra_key,)
    assert len(report.changed) == 1
    change = report.changed[0]
    assert change.event_key == ata_key
    assert change.replay_status == "actual"
    assert change.snapshot_status == "cancelled"
    assert change.replay_sources == ("carrier-1", "terminal-7")
    assert change.snapshot_sources == ("terminal-7",)


def test_replay_ignores_observed_at_drift(tmp_path):
    fixture = write_fixture(tmp_path)
    database = tmp_path / "events.sqlite3"
    run_pipeline(fixture, database)
    sql(database, "UPDATE shipment_events SET observed_at = '2000-01-01T00:00:00+00:00'")

    report = replay_fixture(fixture, database)

    assert report.complete
    assert report.changed == ()
    assert report.replay_only == ()
    assert report.snapshot_only == ()


def test_replay_duplicate_sources_are_deduplicated(tmp_path):
    base = {"source": "a", "vessel": "V", "voyage": "1", "container": "C", "port": "P", "event_code": "ATA", "event_time": "2026-01-01T00:00:00Z", "status": "actual", "source_ref": "a1"}
    items = [base, dict(base), dict(base, source_ref="a2"), dict(base, source_ref="a2")]
    fixture = write_fixture(tmp_path, items)
    database = tmp_path / "events.sqlite3"

    events = run_pipeline(fixture, database)
    assert len(events) == 1
    assert events[0].sources == ("a1", "a2")

    report = replay_fixture(fixture, database)
    assert report.complete
    assert report.changed == ()
    assert report.replay_only == ()
    assert report.snapshot_only == ()


@pytest.mark.parametrize("statuses", [("estimated", "actual"), ("actual", "estimated")])
def test_replay_status_priority_is_stable_under_input_reorder(tmp_path, statuses):
    base = {"source": "a", "vessel": "V", "voyage": "1", "container": "C", "port": "P", "event_code": "ATA", "event_time": "2026-01-01T00:00:00Z", "source_ref": "r"}
    items = [dict(base, status=status, source_ref=f"r{index}") for index, status in enumerate(statuses)]
    fixture = write_fixture(tmp_path, items)
    database = tmp_path / "events.sqlite3"

    events = run_pipeline(fixture, database)
    assert len(events) == 1
    assert events[0].status == "actual"

    report = replay_fixture(fixture, database)
    assert report.complete
    assert report.changed == ()


def test_replay_preserves_pipeline_order_sensitive_adjudication(tmp_path):
    # estimated vs cancelled has no absolute priority: merge keeps the first-seen status.
    # Replay must reproduce that adjudication exactly, not smooth it over for stability.
    base = {"source": "a", "vessel": "V", "voyage": "1", "container": "C", "port": "P", "event_code": "ATA", "event_time": "2026-01-01T00:00:00Z", "source_ref": "r"}
    for index, (statuses, expected) in enumerate([(("estimated", "cancelled"), "estimated"), (("cancelled", "estimated"), "cancelled")]):
        items = [dict(base, status=status, source_ref=f"r{position}") for position, status in enumerate(statuses)]
        fixture = write_fixture(tmp_path, items, name=f"events{index}.json")
        database = tmp_path / f"events{index}.sqlite3"
        events = run_pipeline(fixture, database)
        assert events[0].status == expected
        report = replay_fixture(fixture, database)
        assert report.complete
        assert report.changed == ()


def test_replay_reads_consistent_snapshots_during_concurrent_writes(tmp_path):
    items = [
        {"source": "w", "vessel": "V", "voyage": f"N{index}", "container": "C", "port": "P", "event_code": "ATA", "event_time": f"2026-01-{index + 1:02d}T00:00:00Z", "status": "estimated", "source_ref": f"w{index}"}
        for index in range(25)
    ]
    fixture = write_fixture(tmp_path, items)
    database = tmp_path / "events.sqlite3"
    EventStore(database).close()
    expected = merge_events([normalize_event(item, "Asia/Shanghai") for item in JsonEventSource(fixture).read()])

    writer_errors = []

    def writer():
        store = EventStore(database)
        try:
            for event in expected:
                store.upsert_many([event])
                time.sleep(0.001)
        except Exception as exc:  # pragma: no cover - asserted below
            writer_errors.append(exc)
        finally:
            store.close()

    thread = threading.Thread(target=writer)
    thread.start()
    previous_replay_only = None
    try:
        for _ in range(40):
            report = replay_fixture(fixture, database)
            assert report.complete, report.error
            # Every read is a consistent snapshot: only whole fixture events appear,
            # they never disagree on status/sources, and the pending set only shrinks.
            assert report.snapshot_only == ()
            assert report.changed == ()
            if previous_replay_only is not None:
                assert set(report.replay_only) <= set(previous_replay_only)
            previous_replay_only = report.replay_only
    finally:
        thread.join()
    assert writer_errors == []

    final = replay_fixture(fixture, database)
    assert final.complete
    assert final.replay_only == ()
    assert final.snapshot_only == ()
    assert final.changed == ()


def test_replay_missing_database_fails_clearly_and_creates_nothing(tmp_path):
    fixture = write_fixture(tmp_path)
    database = tmp_path / "missing.sqlite3"

    report = replay_fixture(fixture, database)

    assert not report.complete
    assert report.stage == "snapshot"
    assert "does not exist" in report.error
    assert report.replay_only == ()
    assert report.snapshot_only == ()
    assert report.changed == ()
    assert not database.exists()


def test_replay_rejects_database_without_shipment_events_table(tmp_path):
    fixture = write_fixture(tmp_path)
    database = tmp_path / "other.sqlite3"
    sql(database, "CREATE TABLE other (id INTEGER)")
    before = digest(database)

    report = replay_fixture(fixture, database)

    assert not report.complete
    assert report.stage == "snapshot"
    assert "not found" in report.error
    assert digest(database) == before


def test_replay_rejects_database_with_unsupported_columns(tmp_path):
    fixture = write_fixture(tmp_path)
    database = tmp_path / "slim.sqlite3"
    sql(database, "CREATE TABLE shipment_events (event_key TEXT PRIMARY KEY, note TEXT)")
    before = digest(database)

    report = replay_fixture(fixture, database)

    assert not report.complete
    assert report.stage == "snapshot"
    assert "missing columns" in report.error
    assert digest(database) == before


def test_replay_rejects_table_without_event_key_primary_key(tmp_path):
    fixture = write_fixture(tmp_path)
    database = tmp_path / "nopk.sqlite3"
    sql(
        database,
        "CREATE TABLE shipment_events (event_key TEXT, vessel TEXT, voyage TEXT, container TEXT, port TEXT, event_type TEXT, event_time_utc TEXT, status TEXT, sources TEXT, observed_at TEXT)",
    )
    before = digest(database)

    report = replay_fixture(fixture, database)

    assert not report.complete
    assert report.stage == "snapshot"
    assert "primary key" in report.error
    assert digest(database) == before


def test_replay_rejects_non_sqlite_file(tmp_path):
    fixture = write_fixture(tmp_path)
    database = tmp_path / "junk.sqlite3"
    database.write_text("not a database", encoding="utf-8")

    report = replay_fixture(fixture, database)

    assert not report.complete
    assert report.stage == "snapshot"
    assert report.error
    assert database.read_bytes() == b"not a database"


def test_replay_fixture_parse_failure_is_incomplete_and_ignores_snapshot(tmp_path):
    fixture = write_fixture(tmp_path)
    database = tmp_path / "events.sqlite3"
    run_pipeline(fixture, database)
    fixture.write_text("[{broken json", encoding="utf-8")

    report = replay_fixture(fixture, database)

    assert not report.complete
    assert report.stage == "fixture"
    assert report.error
    # The remaining snapshot rows must not be reported as differences.
    assert report.snapshot_rows == 0
    assert report.snapshot_only == ()
    assert report.replay_only == ()
    assert report.changed == ()


def test_replay_normalize_failure_reports_stage_and_ignores_snapshot(tmp_path):
    good = dict(FIXTURE_ITEMS[0])
    bad = dict(FIXTURE_ITEMS[0], event_code="FLY", source_ref="carrier-9")
    fixture = write_fixture(tmp_path, [good, bad])
    database = tmp_path / "events.sqlite3"
    EventStore(database).close()

    report = replay_fixture(fixture, database)

    assert not report.complete
    assert report.stage == "normalize"
    assert "unsupported event code" in report.error
    assert report.snapshot_rows == 0
    assert report.snapshot_only == ()
    assert report.replay_only == ()
    assert report.changed == ()


def test_consecutive_replays_are_identical(tmp_path):
    fixture = write_fixture(tmp_path)
    database = tmp_path / "events.sqlite3"
    events = run_pipeline(fixture, database)
    sql(database, "UPDATE shipment_events SET status = 'cancelled' WHERE event_key = ?", (events[0].event_key,))
    sql(database, "DELETE FROM shipment_events WHERE event_key = ?", (events[1].event_key,))
    sql(
        database,
        "INSERT INTO shipment_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("EXTRA|1|C|P|loaded|2026-11-11T00:00:00+00:00", "EXTRA", "1", "C", "P", "loaded", "2026-11-11T00:00:00+00:00", "actual", "x1", "2026-11-11T00:00:00+00:00"),
    )
    before = digest(database)

    first = replay_fixture(fixture, database)
    second = replay_fixture(fixture, database)

    assert first.complete
    assert first == second
    assert first.replay_only and first.snapshot_only and first.changed
    assert digest(database) == before


def test_replay_timezone_name_changes_interpretation(tmp_path):
    naive = dict(FIXTURE_ITEMS[0], event_time="2026-01-01T08:00:00")
    fixture = write_fixture(tmp_path, [naive])
    database = tmp_path / "events.sqlite3"
    run_pipeline(fixture, database, timezone="Asia/Shanghai")

    aligned = replay_fixture(fixture, database, "Asia/Shanghai")
    assert aligned.complete
    assert aligned.replay_only == ()
    assert aligned.snapshot_only == ()

    shifted = replay_fixture(fixture, database, "UTC")
    assert shifted.complete
    assert len(shifted.replay_only) == 1
    assert len(shifted.snapshot_only) == 1
    assert shifted.changed == ()


def test_replay_cli_outputs_stable_json_and_exit_codes(tmp_path, capsys, monkeypatch):
    import sys

    from port_events.replay import main

    fixture = write_fixture(tmp_path)
    database = tmp_path / "events.sqlite3"
    run_pipeline(fixture, database)

    monkeypatch.setattr(sys, "argv", ["port-events-replay", "--fixture", str(fixture), "--database", str(database)])
    with pytest.raises(SystemExit) as excinfo:
        main()
    assert excinfo.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["complete"] is True
    assert payload["stage"] == "compare"
    assert payload["replay_only"] == []
    assert payload["snapshot_only"] == []
    assert payload["changed"] == []

    sql(database, "UPDATE shipment_events SET status = 'cancelled'")
    with pytest.raises(SystemExit) as excinfo:
        main()
    assert excinfo.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert len(payload["changed"]) == 2

    monkeypatch.setattr(sys, "argv", ["port-events-replay", "--fixture", str(fixture), "--database", str(tmp_path / "missing.sqlite3")])
    with pytest.raises(SystemExit) as excinfo:
        main()
    assert excinfo.value.code == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["complete"] is False
    assert payload["stage"] == "snapshot"
