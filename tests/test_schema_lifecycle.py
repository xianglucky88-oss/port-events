"""Schema-version lifecycle tests.

Covers: empty database, current structure without a version marker,
supported missing-column legacy database, duplicate-key legacy table,
unknown higher version, unknown structure, lock conflicts, concurrent
upgrades, two injected fault points, interrupted writes, field provenance
after recovery, and regression fixture imports.
"""

import json
import sqlite3
import threading
from pathlib import Path

import pytest

from port_events.connectors import JsonEventSource
from port_events.models import ShipmentEvent
from port_events.pipeline import PortEventsPipeline
from port_events.schema import (
    Migration,
    MigrationDefinitionError,
    MigrationError,
    SchemaLockError,
    TableSchema,
    UnknownSchemaError,
    UnsupportedVersionError,
)
from port_events.storage import EventStore

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "events.json"

CURRENT_DDL = """
CREATE TABLE shipment_events (
    event_key TEXT PRIMARY KEY, vessel TEXT NOT NULL, voyage TEXT NOT NULL,
    container TEXT, port TEXT NOT NULL, event_type TEXT NOT NULL,
    event_time_utc TEXT NOT NULL, status TEXT NOT NULL, sources TEXT NOT NULL,
    observed_at TEXT NOT NULL
)"""

V1_DDL = """
CREATE TABLE shipment_events (
    event_key TEXT PRIMARY KEY, vessel TEXT NOT NULL, voyage TEXT NOT NULL,
    port TEXT NOT NULL, event_type TEXT NOT NULL, event_time_utc TEXT NOT NULL,
    status TEXT NOT NULL, source TEXT NOT NULL, recorded_at TEXT NOT NULL
)"""

V1_DDL_UNCONSTRAINED = """
CREATE TABLE shipment_events (
    event_key TEXT, vessel TEXT, voyage TEXT, port TEXT, event_type TEXT,
    event_time_utc TEXT, status TEXT, source TEXT, recorded_at TEXT
)"""

META_DDL = """
CREATE TABLE schema_meta (
    id INTEGER PRIMARY KEY CHECK (id = 1), version INTEGER NOT NULL,
    upgraded_from INTEGER, updated_at TEXT NOT NULL
)"""

CURRENT_COLUMNS = [
    "event_key", "vessel", "voyage", "container", "port", "event_type",
    "event_time_utc", "status", "sources", "observed_at",
]


def v1_row(key, vessel="VESSEL", voyage="V1", port="CNSHA", event_type="arrived",
           event_time="2026-01-01T00:00:00+00:00", status="actual",
           source="legacy-ref", recorded="2026-01-01T01:00:00+00:00"):
    return (key, vessel, voyage, port, event_type, event_time, status, source, recorded)


def build_db(path, ddl, rows=(), meta_version=None):
    conn = sqlite3.connect(str(path))
    conn.execute(ddl)
    if rows:
        placeholders = ", ".join("?" for _ in rows[0])
        conn.executemany(f"INSERT INTO shipment_events VALUES ({placeholders})", rows)
    if meta_version is not None:
        conn.execute(META_DDL)
        conn.execute(
            "INSERT INTO schema_meta VALUES (1, ?, NULL, ?)",
            (meta_version, "2026-01-01T00:00:00+00:00"),
        )
    conn.commit()
    conn.close()


def fetch_events(store):
    cursor = store.connection.execute("SELECT * FROM shipment_events ORDER BY event_key")
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def run_fixture(store):
    return PortEventsPipeline(JsonEventSource(FIXTURE), store).run()


def make_event(key):
    return ShipmentEvent(
        key, "V", "1", None, "P", "arrived", "2026-01-01T00:00:00+00:00",
        "actual", ("r1",), "2026-01-01T01:00:00+00:00",
    )


class FailAt:
    """Fault injection: raise when the named checkpoint fires."""

    def __init__(self, checkpoint):
        self.checkpoint = checkpoint
        self.fired = []

    def __call__(self, name):
        self.fired.append(name)
        if name == self.checkpoint:
            raise RuntimeError(f"injected fault at {name}")


# ---------------------------------------------------------------------------
# Recognition: empty, current-unversioned, legacy
# ---------------------------------------------------------------------------


def test_empty_database_initializes_current_structure(tmp_path):
    path = tmp_path / "empty.sqlite3"
    path.touch()
    store = EventStore(path)
    try:
        assert store.ready
        assert store.schema_version == 2
        columns = [row[1] for row in store.connection.execute("PRAGMA table_info(shipment_events)")]
        assert columns == CURRENT_COLUMNS
        assert [entry["step"] for entry in store.migration_history()] == ["init"]
        # regression: fixture import works on a fresh database
        events = run_fixture(store)
        assert len(events) == 2
        merged = next(e for e in events if e.event_type == "arrived")
        assert merged.status == "actual"
        assert merged.sources == ("carrier-event-1", "terminal-event-7")
        assert len(fetch_events(store)) == 2
    finally:
        store.close()


def test_current_structure_without_version_marker_is_adopted(tmp_path):
    path = tmp_path / "adopt.sqlite3"
    build_db(
        path,
        CURRENT_DDL,
        rows=[("K1", "V", "1", None, "P", "arrived", "2026-01-01T00:00:00+00:00",
               "actual", "ref-1", "2026-01-01T01:00:00+00:00")],
    )
    store = EventStore(path)
    try:
        assert store.schema_version == 2
        assert [entry["step"] for entry in store.migration_history()] == ["adopt"]
        rows = fetch_events(store)
        assert len(rows) == 1
        assert rows[0]["event_key"] == "K1"
        assert rows[0]["sources"] == "ref-1"
        # normal writes continue after adoption
        run_fixture(store)
        assert len(fetch_events(store)) == 3
    finally:
        store.close()


def test_supported_legacy_database_migrates_losslessly(tmp_path):
    path = tmp_path / "legacy.sqlite3"
    build_db(path, V1_DDL, rows=[
        v1_row("K1", source="ref-a", recorded="2026-01-01T01:00:00+00:00"),
        v1_row("K2", vessel="OTHER", source="ref-b", recorded="2026-01-02T02:00:00+00:00"),
    ])
    store = EventStore(path)
    try:
        assert store.schema_version == 2
        assert [entry["step"] for entry in store.migration_history()] == ["rebuild", "register"]
        rows = {row["event_key"]: row for row in fetch_events(store)}
        assert set(rows) == {"K1", "K2"}
        # field provenance: renamed and copied columns carry the legacy values
        assert rows["K1"]["sources"] == "ref-a"
        assert rows["K1"]["observed_at"] == "2026-01-01T01:00:00+00:00"
        assert rows["K2"]["sources"] == "ref-b"
        assert rows["K2"]["observed_at"] == "2026-01-02T02:00:00+00:00"
        assert rows["K2"]["vessel"] == "OTHER"
        # container did not exist in v1: NULL, not invented
        assert rows["K1"]["container"] is None
        assert store.quarantined_rows() == []
        # regression: fixture import continues into the upgraded database
        run_fixture(store)
        assert len(fetch_events(store)) == 4
    finally:
        store.close()


# ---------------------------------------------------------------------------
# Unmappable rows: quarantine with original content and reason
# ---------------------------------------------------------------------------


def test_unmappable_rows_are_quarantined_with_original_content(tmp_path):
    path = tmp_path / "dup.sqlite3"
    build_db(path, V1_DDL_UNCONSTRAINED, rows=[
        v1_row("K1"),
        v1_row("K2"),
        v1_row("K1", vessel="OTHER", source="dup-ref"),  # duplicate key
        v1_row("K3", vessel=None),                        # null required column
        v1_row(None),                                     # null key
    ])
    store = EventStore(path)
    try:
        assert store.schema_version == 2
        rows = fetch_events(store)
        assert [row["event_key"] for row in rows] == ["K1", "K2"]
        quarantine = store.quarantined_rows()
        assert len(quarantine) == 3
        reasons = [entry["reason"] for entry in quarantine]
        assert any("duplicate event_key 'K1'" in reason for reason in reasons)
        assert any("null value in required column 'vessel'" in reason for reason in reasons)
        assert any("null value in required column 'event_key'" in reason for reason in reasons)
        # original content preserved verbatim, not repaired
        dup = next(entry for entry in quarantine if "duplicate" in entry["reason"])
        assert dup["source_version"] == 1
        assert dup["row"]["vessel"] == "OTHER"
        assert dup["row"]["source"] == "dup-ref"
        null_key = next(
            entry for entry in quarantine
            if "null value in required column 'event_key'" in entry["reason"]
        )
        assert null_key["row"]["event_key"] is None
        assert null_key["row"]["source"] == "legacy-ref"
    finally:
        store.close()


# ---------------------------------------------------------------------------
# Diagnostics: newer version, unknown structure, inconsistent state
# ---------------------------------------------------------------------------


def test_newer_version_is_rejected_with_diagnostic(tmp_path):
    path = tmp_path / "future.sqlite3"
    build_db(path, CURRENT_DDL, meta_version=99)
    with pytest.raises(UnsupportedVersionError) as excinfo:
        EventStore(path)
    message = str(excinfo.value)
    assert "99" in message
    assert "current version 2" in message
    # the rejected database is left untouched
    conn = sqlite3.connect(str(path))
    tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert tables == {"shipment_events", "schema_meta"}
    assert conn.execute("SELECT version FROM schema_meta").fetchone()[0] == 99
    conn.close()


def test_unknown_structure_is_diagnosed(tmp_path):
    path = tmp_path / "unknown.sqlite3"
    conn = sqlite3.connect(str(path))
    conn.execute("CREATE TABLE shipment_events (a TEXT, b TEXT)")
    conn.commit()
    conn.close()
    with pytest.raises(UnknownSchemaError) as excinfo:
        EventStore(path)
    assert "a, b" in str(excinfo.value)


def test_version_marker_ahead_of_structure_is_inconsistent(tmp_path):
    path = tmp_path / "inconsistent.sqlite3"
    build_db(path, V1_DDL, rows=[v1_row("K1")], meta_version=2)
    with pytest.raises(MigrationError, match="structure matches version 1"):
        EventStore(path)


# ---------------------------------------------------------------------------
# Locking and concurrency
# ---------------------------------------------------------------------------


def test_lock_conflict_is_reported_and_recovers(tmp_path):
    path = tmp_path / "locked.sqlite3"
    build_db(path, V1_DDL, rows=[v1_row("K1")])
    blocker = sqlite3.connect(str(path), isolation_level=None)
    blocker.execute("BEGIN EXCLUSIVE")
    try:
        with pytest.raises(SchemaLockError):
            EventStore(path, lock_timeout=0.1)
    finally:
        blocker.execute("ROLLBACK")
        blocker.close()
    store = EventStore(path, lock_timeout=0.1)
    try:
        assert store.ready
        assert store.schema_version == 2
        assert [row["event_key"] for row in fetch_events(store)] == ["K1"]
    finally:
        store.close()


def test_concurrent_openers_migrate_exactly_once(tmp_path):
    path = tmp_path / "concurrent.sqlite3"
    build_db(path, V1_DDL, rows=[v1_row("K1"), v1_row("K2"), v1_row("K3")])
    barrier = threading.Barrier(4)
    errors = []

    def open_store():
        try:
            barrier.wait(timeout=10)
            store = EventStore(path)
            assert store.schema_version == 2
            store.close()
        except Exception as exc:  # noqa: BLE001 - collected for assertion
            errors.append(exc)

    threads = [threading.Thread(target=open_store) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
    assert not errors
    store = EventStore(path)
    try:
        assert store.schema_version == 2
        assert len(fetch_events(store)) == 3
        steps = [entry["step"] for entry in store.migration_history()]
        assert steps.count("rebuild") == 1
        assert steps.count("register") == 1
    finally:
        store.close()


# ---------------------------------------------------------------------------
# Fault points and recovery
# ---------------------------------------------------------------------------


def test_fault_during_rebuild_rolls_back_completely(tmp_path):
    path = tmp_path / "fault1.sqlite3"
    build_db(path, V1_DDL, rows=[v1_row("K1"), v1_row("K2")])
    with pytest.raises(RuntimeError, match="injected fault"):
        EventStore(path, checkpoint_observer=FailAt("rebuild.committing"))
    # the whole step rolled back: still the plain legacy table, no aux tables
    conn = sqlite3.connect(str(path))
    tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert tables == {"shipment_events"}
    columns = [row[1] for row in conn.execute("PRAGMA table_info(shipment_events)")]
    assert len(columns) == 9
    conn.close()
    # restart migrates cleanly
    store = EventStore(path)
    try:
        assert store.schema_version == 2
        assert [row["event_key"] for row in fetch_events(store)] == ["K1", "K2"]
        assert [entry["step"] for entry in store.migration_history()] == ["rebuild", "register"]
    finally:
        store.close()


def test_fault_after_rebuild_recovers_by_registering_version(tmp_path):
    path = tmp_path / "fault2.sqlite3"
    build_db(path, V1_DDL, rows=[
        v1_row("K1", source="ref-a", recorded="2026-01-01T01:00:00+00:00"),
        v1_row("K2", vessel="OTHER", source="ref-b", recorded="2026-01-02T02:00:00+00:00"),
    ])
    with pytest.raises(RuntimeError, match="injected fault"):
        EventStore(path, checkpoint_observer=FailAt("rebuild.committed"))
    # intermediate state: DDL done, version unmarked
    conn = sqlite3.connect(str(path))
    columns = [row[1] for row in conn.execute("PRAGMA table_info(shipment_events)")]
    assert columns == CURRENT_COLUMNS
    assert conn.execute("SELECT version FROM schema_meta").fetchone()[0] == 1
    conn.close()
    # recovery completes registration only, then verifies field provenance
    store = EventStore(path)
    try:
        assert store.schema_version == 2
        steps = [entry["step"] for entry in store.migration_history()]
        assert steps == ["rebuild", "register"]
        rows = {row["event_key"]: row for row in fetch_events(store)}
        assert rows["K1"]["sources"] == "ref-a"
        assert rows["K1"]["observed_at"] == "2026-01-01T01:00:00+00:00"
        assert rows["K1"]["container"] is None
        assert rows["K2"]["sources"] == "ref-b"
        assert rows["K2"]["observed_at"] == "2026-01-02T02:00:00+00:00"
        assert store.quarantined_rows() == []
        # regression: fixture import continues after recovery
        run_fixture(store)
        assert len(fetch_events(store)) == 4
    finally:
        store.close()


def test_interrupted_write_rolls_back_and_store_recovers(tmp_path):
    path = tmp_path / "writes.sqlite3"
    store = EventStore(path, checkpoint_observer=FailAt("upsert.committing"))
    with pytest.raises(RuntimeError, match="injected fault"):
        store.upsert_many([make_event("W1"), make_event("W2")])
    store.close()
    # restart: the interrupted write left nothing behind, writes work again
    store = EventStore(path)
    try:
        assert fetch_events(store) == []
        store.upsert_many([make_event("W1")])
        assert [row["event_key"] for row in fetch_events(store)] == ["W1"]
    finally:
        store.close()


# ---------------------------------------------------------------------------
# Construction library validation
# ---------------------------------------------------------------------------


def test_migration_builder_enforces_lossless_definitions():
    source = (
        TableSchema.build("t", 1)
        .column("id", primary_key=True)
        .column("a")
        .column("dropped_col")
        .done()
    )
    target = TableSchema.build("t", 2).column("id", primary_key=True).column("a").done()
    with pytest.raises(MigrationDefinitionError, match="dropped"):
        Migration.define("bad-drop", source, target).copy("id", "a").done()
    with pytest.raises(MigrationDefinitionError, match="without mapping"):
        Migration.define("bad-missing", source, target).copy("id").done()

    nullable_source = (
        TableSchema.build("t", 3)
        .column("id", primary_key=True)
        .column("a", required=False)
        .done()
    )
    required_target = TableSchema.build("t", 4).column("id", primary_key=True).column("a").done()
    with pytest.raises(MigrationDefinitionError, match="nullable source"):
        Migration.define("bad-null", nullable_source, required_target).copy("id", "a").done()
    with pytest.raises(MigrationDefinitionError, match="filled with NULL"):
        Migration.define("bad-fill", nullable_source, required_target).copy("id").fill_null("a").done()

    with pytest.raises(MigrationDefinitionError, match="duplicate column"):
        TableSchema.build("t", 9).column("x").column("x").done()


def test_migration_mapping_is_verbatim_only():
    from port_events.schema import MIGRATION_V1_V2

    assert MIGRATION_V1_V2.from_version == 1
    assert MIGRATION_V1_V2.to_version == 2
    described = MIGRATION_V1_V2.describe_mapping()
    assert described["sources"] == "source"
    assert described["observed_at"] == "recorded_at"
    assert described["container"] == "<null>"
    projected = MIGRATION_V1_V2.project({
        "event_key": "K", "vessel": "V", "voyage": "1", "port": "P",
        "event_type": "arrived", "event_time_utc": "T", "status": "actual",
        "source": "s1", "recorded_at": "r1",
    })
    assert projected == {
        "event_key": "K", "vessel": "V", "voyage": "1", "container": None,
        "port": "P", "event_type": "arrived", "event_time_utc": "T",
        "status": "actual", "sources": "s1", "observed_at": "r1",
    }
