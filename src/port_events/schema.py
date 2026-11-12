"""Independent schema-version lifecycle for the port-events store.

The store recognises exactly the structures declared in this module:

* a fresh file (no user tables) is initialised at the current version;
* the current ten-column structure is accepted, and adopted when it carries
  no version marker yet (files written by older builds);
* a declared legacy structure is upgraded through the registered migration
  — field mappings are total and lossless by construction;
* anything else (unknown columns, newer version markers, inconsistent
  version/structure pairs) is rejected with an explicit diagnostic.

Migrations run as recovery checkpoints instead of one opaque transaction:
the rebuild transaction (staging table, copied rows, quarantined rows,
swap) commits first, the version registration commits second.  A crash
between the two leaves "DDL done, version unmarked", which the next open
detects and completes by registration only.  Every step is idempotent and
re-checked under a write lock, so concurrent openers serialise safely.
Normal writes are gated on the lifecycle having completed.

Rows that cannot be mapped losslessly (null in a required column,
duplicate key, constraint rejection) are never guessed at: timestamps are
not fabricated, keys are not recomputed, states are not adjudicated.  The
offending row is quarantined with its original content and the reason.
"""

from __future__ import annotations

import contextlib
import json
import sqlite3
from dataclasses import dataclass
from typing import Callable, Iterable, Iterator

from .models import observed_now

EVENTS_TABLE = "shipment_events"
QUARANTINE_TABLE = "shipment_events_quarantine"
META_TABLE = "schema_meta"
MIGRATIONS_TABLE = "schema_migrations"
AUX_TABLES = frozenset({META_TABLE, MIGRATIONS_TABLE, QUARANTINE_TABLE})

__all__ = [
    "EVENTS_TABLE",
    "QUARANTINE_TABLE",
    "Detection",
    "Migration",
    "MigrationDefinitionError",
    "MigrationError",
    "MIGRATION_V1_V2",
    "REGISTRY",
    "SCHEMA_V1",
    "SCHEMA_V2",
    "SchemaError",
    "SchemaLockError",
    "SchemaManager",
    "SchemaRegistry",
    "TableSchema",
    "UnknownSchemaError",
    "UnsupportedVersionError",
    "detect",
]


# ---------------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------------


class SchemaError(Exception):
    """Base class for schema lifecycle failures."""


class MigrationDefinitionError(SchemaError):
    """A schema or migration was declared inconsistently."""


class UnknownSchemaError(SchemaError):
    """The database structure matches no declared schema version."""


class UnsupportedVersionError(SchemaError):
    """The recorded version is newer than, or unreachable from, this build."""


class SchemaLockError(SchemaError):
    """The schema lock could not be acquired (concurrent upgrade or writer)."""


class MigrationError(SchemaError):
    """A migration step failed or the database is internally inconsistent."""


# ---------------------------------------------------------------------------
# Construction library: table schemas
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Column:
    name: str
    affinity: str = "TEXT"
    required: bool = True
    primary_key: bool = False

    def ddl(self) -> str:
        parts = [self.name, self.affinity]
        if self.primary_key:
            parts.append("PRIMARY KEY")
        elif self.required:
            parts.append("NOT NULL")
        return " ".join(parts)


@dataclass(frozen=True)
class TableSchema:
    """A declared, recognisable structure of one table at one version."""

    table: str
    version: int
    columns: tuple[Column, ...]

    @staticmethod
    def build(table: str, version: int) -> "TableSchemaBuilder":
        return TableSchemaBuilder(table, version)

    def column_names(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns)

    def signature(self) -> tuple[tuple[str, str], ...]:
        """Recognisable shape: ordered (name, affinity) pairs.

        Constraints are deliberately excluded so that dumps created without
        PRIMARY KEY / NOT NULL clauses still match a declared version; row
        level problems are then caught during the lossless copy and
        quarantined instead of blocking recognition.
        """
        return tuple((column.name, column.affinity.upper()) for column in self.columns)

    def matches(self, table_info: list) -> bool:
        found = tuple((row[1], (row[2] or "").strip().upper()) for row in table_info)
        return found == self.signature()

    def create_sql(self, name: str | None = None) -> str:
        cols = ", ".join(column.ddl() for column in self.columns)
        return f"CREATE TABLE {name or self.table} ({cols})"


class TableSchemaBuilder:
    def __init__(self, table: str, version: int) -> None:
        self._table = table
        self._version = version
        self._columns: list[Column] = []

    def column(
        self,
        name: str,
        affinity: str = "TEXT",
        required: bool = True,
        primary_key: bool = False,
    ) -> "TableSchemaBuilder":
        self._columns.append(Column(name, affinity, required, primary_key))
        return self

    def done(self) -> TableSchema:
        names = [column.name for column in self._columns]
        if not names:
            raise MigrationDefinitionError(f"{self._table} v{self._version}: no columns declared")
        if len(set(names)) != len(names):
            raise MigrationDefinitionError(f"{self._table} v{self._version}: duplicate column names")
        if sum(1 for column in self._columns if column.primary_key) > 1:
            raise MigrationDefinitionError(f"{self._table} v{self._version}: more than one primary key")
        return TableSchema(self._table, self._version, tuple(self._columns))


# ---------------------------------------------------------------------------
# Construction library: field mappings and migrations
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FieldMapping:
    """One target column filled from one source column (None = NULL fill).

    Only verbatim copies are expressible: there is no hook to fabricate
    timestamps, recompute keys or adjudicate statuses.
    """

    source: str | None
    target: str


@dataclass(frozen=True)
class Migration:
    """A declared upgrade step: verifiable source, target and version pair."""

    name: str
    source: TableSchema
    target: TableSchema
    mappings: tuple[FieldMapping, ...]

    @staticmethod
    def define(name: str, source: TableSchema, target: TableSchema) -> "MigrationBuilder":
        return MigrationBuilder(name, source, target)

    @property
    def from_version(self) -> int:
        return self.source.version

    @property
    def to_version(self) -> int:
        return self.target.version

    def project(self, source_row: dict) -> dict:
        return {
            mapping.target: (source_row[mapping.source] if mapping.source is not None else None)
            for mapping in self.mappings
        }

    def describe_mapping(self) -> dict:
        return {
            mapping.target: (mapping.source if mapping.source is not None else "<null>")
            for mapping in self.mappings
        }


class MigrationBuilder:
    def __init__(self, name: str, source: TableSchema, target: TableSchema) -> None:
        self._name = name
        self._source = source
        self._target = target
        self._sources: dict[str, str | None] = {}  # target column -> source column

    def copy(self, *names: str) -> "MigrationBuilder":
        """Copy same-named columns verbatim."""
        for name in names:
            self._sources[name] = name
        return self

    def rename(self, source: str, target: str) -> "MigrationBuilder":
        """Copy one source column verbatim into a differently named target."""
        self._sources[target] = source
        return self

    def fill_null(self, target: str) -> "MigrationBuilder":
        """Declare that a nullable target has no source and stays NULL."""
        self._sources[target] = None
        return self

    def done(self) -> Migration:
        target_cols = {column.name: column for column in self._target.columns}
        source_cols = {column.name: column for column in self._source.columns}
        missing = [name for name in target_cols if name not in self._sources]
        if missing:
            raise MigrationDefinitionError(f"{self._name}: target columns without mapping: {missing}")
        unknown = [name for name in self._sources if name not in target_cols]
        if unknown:
            raise MigrationDefinitionError(f"{self._name}: mappings to unknown target columns: {unknown}")
        used_sources: list[str] = []
        for target, source in self._sources.items():
            target_col = target_cols[target]
            if source is None:
                if target_col.required:
                    raise MigrationDefinitionError(
                        f"{self._name}: required target '{target}' cannot be filled with NULL"
                    )
                continue
            source_col = source_cols.get(source)
            if source_col is None:
                raise MigrationDefinitionError(f"{self._name}: unknown source column '{source}'")
            if target_col.required and not source_col.required:
                raise MigrationDefinitionError(
                    f"{self._name}: required target '{target}' cannot draw from nullable source '{source}'"
                )
            used_sources.append(source)
        dropped = [name for name in source_cols if name not in used_sources]
        if dropped:
            raise MigrationDefinitionError(
                f"{self._name}: source columns would be dropped (data loss): {dropped}"
            )
        mappings = tuple(
            FieldMapping(source=self._sources[column.name], target=column.name)
            for column in self._target.columns
        )
        return Migration(self._name, self._source, self._target, mappings)


class SchemaRegistry:
    """The declared current structure, legacy structures and migrations."""

    def __init__(
        self,
        current: TableSchema,
        legacy: Iterable[TableSchema],
        migrations: Iterable[Migration],
    ) -> None:
        self.current = current
        self.legacy = tuple(legacy)
        self._migrations: dict[int, Migration] = {}
        for migration in migrations:
            if migration.to_version != current.version:
                raise MigrationDefinitionError(
                    f"{migration.name}: migrations must target the current version "
                    f"{current.version}, got {migration.to_version}"
                )
            if migration.from_version in self._migrations:
                raise MigrationDefinitionError(
                    f"{migration.name}: duplicate migration from version {migration.from_version}"
                )
            self._migrations[migration.from_version] = migration
        for schema in self.legacy:
            if schema.version not in self._migrations:
                raise MigrationDefinitionError(
                    f"legacy version {schema.version} has no declared migration"
                )

    def migration_from(self, version: int) -> Migration | None:
        return self._migrations.get(version)

    def describe_supported(self) -> str:
        legacy = sorted(self._migrations)
        return f"current version {self.current.version}; declared legacy versions: {legacy or 'none'}"


# ---------------------------------------------------------------------------
# Declared structures and the registered migration
# ---------------------------------------------------------------------------

# Version 1 (legacy, supported): nine columns.  It predates `container`,
# carries a single `source` reference per row and records the observation
# time as `recorded_at`.
SCHEMA_V1 = (
    TableSchema.build(EVENTS_TABLE, 1)
    .column("event_key", primary_key=True)
    .column("vessel")
    .column("voyage")
    .column("port")
    .column("event_type")
    .column("event_time_utc")
    .column("status")
    .column("source")
    .column("recorded_at")
    .done()
)

# Version 2 (current): ten columns.
SCHEMA_V2 = (
    TableSchema.build(EVENTS_TABLE, 2)
    .column("event_key", primary_key=True)
    .column("vessel")
    .column("voyage")
    .column("container", required=False)
    .column("port")
    .column("event_type")
    .column("event_time_utc")
    .column("status")
    .column("sources")
    .column("observed_at")
    .done()
)

# v1 -> v2: every v1 column is consumed, every required v2 column has a
# lossless source.  `container` did not exist in v1 and stays NULL; the
# single `source` reference becomes `sources`; `recorded_at` is renamed to
# `observed_at`.  No timestamps are guessed, no keys recomputed, no
# statuses adjudicated.
MIGRATION_V1_V2 = (
    Migration.define("rebuild-v1-to-v2", SCHEMA_V1, SCHEMA_V2)
    .copy("event_key", "vessel", "voyage", "port", "event_type", "event_time_utc", "status")
    .rename("source", "sources")
    .rename("recorded_at", "observed_at")
    .fill_null("container")
    .done()
)

REGISTRY = SchemaRegistry(current=SCHEMA_V2, legacy=[SCHEMA_V1], migrations=[MIGRATION_V1_V2])


# ---------------------------------------------------------------------------
# Auxiliary tables
# ---------------------------------------------------------------------------

_META_DDL = f"""
CREATE TABLE IF NOT EXISTS {META_TABLE} (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    version INTEGER NOT NULL,
    upgraded_from INTEGER,
    updated_at TEXT NOT NULL
)"""

_MIGRATIONS_DDL = f"""
CREATE TABLE IF NOT EXISTS {MIGRATIONS_TABLE} (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    step TEXT NOT NULL,
    from_version INTEGER,
    to_version INTEGER NOT NULL,
    detail TEXT,
    recorded_at TEXT NOT NULL
)"""

_QUARANTINE_DDL = f"""
CREATE TABLE IF NOT EXISTS {QUARANTINE_TABLE} (
    quarantine_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_version INTEGER NOT NULL,
    reason TEXT NOT NULL,
    row_payload TEXT NOT NULL,
    quarantined_at TEXT NOT NULL
)"""


# ---------------------------------------------------------------------------
# Recognition
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Detection:
    state: str  # fresh | ready | adopt | register | migrate | unsupported-version | unknown | inconsistent
    meta_version: int | None
    columns: tuple[str, ...]
    migration: Migration | None = None
    detail: str = ""


def _table_names(connection: sqlite3.Connection) -> set:
    return {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def _table_info(connection: sqlite3.Connection, table: str) -> list:
    return list(connection.execute(f"PRAGMA table_info({table})"))


def detect(connection: sqlite3.Connection, registry: SchemaRegistry | None = None) -> Detection:
    """Classify the database without modifying it."""
    registry = registry or REGISTRY
    current = registry.current
    tables = _table_names(connection)
    meta_version = None
    if META_TABLE in tables:
        row = connection.execute(f"SELECT version FROM {META_TABLE} WHERE id = 1").fetchone()
        meta_version = row[0] if row else None
    if meta_version is not None and meta_version > current.version:
        return Detection(
            "unsupported-version",
            meta_version,
            (),
            detail=f"{META_TABLE} records version {meta_version}, newer than this build's "
            f"version {current.version}",
        )
    if EVENTS_TABLE not in tables:
        others = sorted(t for t in tables if t not in AUX_TABLES and not t.startswith("sqlite_"))
        if not others and META_TABLE not in tables:
            return Detection("fresh", meta_version, ())
        found = ", ".join(others) if others else "none"
        return Detection(
            "unknown",
            meta_version,
            (),
            detail=f"no {EVENTS_TABLE} table found; unrelated tables present: {found}",
        )
    info = _table_info(connection, EVENTS_TABLE)
    columns = tuple(row[1] for row in info)
    if current.matches(info):
        if meta_version is None:
            return Detection("adopt", None, columns)
        if meta_version == current.version:
            return Detection("ready", meta_version, columns)
        return Detection(
            "register",
            meta_version,
            columns,
            detail=f"structure already matches version {current.version} but {META_TABLE} "
            f"records {meta_version}; completing version registration",
        )
    for legacy in registry.legacy:
        if legacy.matches(info):
            migration = registry.migration_from(legacy.version)
            if migration is None:
                return Detection(
                    "unsupported-version",
                    meta_version,
                    columns,
                    detail=f"structure matches version {legacy.version} but no migration to "
                    f"{current.version} is declared",
                )
            if meta_version is None or meta_version == legacy.version:
                return Detection("migrate", meta_version, columns, migration=migration)
            return Detection(
                "inconsistent",
                meta_version,
                columns,
                detail=f"structure matches version {legacy.version} but {META_TABLE} records "
                f"{meta_version}",
            )
    return Detection(
        "unknown",
        meta_version,
        columns,
        detail=f"{EVENTS_TABLE} columns match no declared schema version",
    )


# ---------------------------------------------------------------------------
# Lifecycle runner
# ---------------------------------------------------------------------------


def _validate_row(migration: Migration, target_row: dict) -> list:
    reasons = []
    for column in migration.target.columns:
        if column.required and target_row[column.name] is None:
            reasons.append(f"null value in required column '{column.name}'")
    key = target_row.get("event_key")
    if isinstance(key, str) and not key.strip():
        reasons.append("blank event_key")
    return reasons


class SchemaManager:
    """Drives a database to the ready state: detect, act, re-check, repeat."""

    MAX_STEPS = 16

    def __init__(
        self,
        connection: sqlite3.Connection,
        registry: SchemaRegistry | None = None,
        observer: Callable[[str], None] | None = None,
    ) -> None:
        self._conn = connection
        self._registry = registry or REGISTRY
        self._observer = observer

    def ensure_ready(self) -> Detection:
        for _ in range(self.MAX_STEPS):
            det = self._detect()
            if det.state == "ready":
                self._observe("ready")
                return det
            if det.state == "fresh":
                self._init_fresh()
            elif det.state == "adopt":
                self._adopt()
            elif det.state == "register":
                self._register()
            elif det.state == "migrate":
                self._rebuild(det)
            else:
                raise self._diagnostic(det)
        raise MigrationError("schema lifecycle did not converge")

    # -- recognition under the lock -------------------------------------

    def _detect(self) -> Detection:
        try:
            return detect(self._conn, self._registry)
        except sqlite3.OperationalError as exc:
            if "locked" in str(exc):
                raise SchemaLockError(
                    f"cannot inspect database schema: {exc}; another process may be "
                    "upgrading or writing"
                ) from exc
            raise MigrationError(f"cannot inspect database schema: {exc}") from exc

    def _diagnostic(self, det: Detection) -> SchemaError:
        supported = self._registry.describe_supported()
        if det.state == "unsupported-version":
            return UnsupportedVersionError(f"cannot open event store: {det.detail}; {supported}")
        if det.state == "unknown":
            found = f"found columns: {', '.join(det.columns)}" if det.columns else det.detail
            return UnknownSchemaError(f"cannot open event store: {found}; {supported}")
        return MigrationError(f"cannot open event store: {det.detail}")

    # -- primitives ------------------------------------------------------

    def _observe(self, checkpoint: str) -> None:
        if self._observer is not None:
            self._observer(checkpoint)

    @contextlib.contextmanager
    def _write_txn(self, label: str) -> Iterator[None]:
        try:
            self._conn.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as exc:
            raise SchemaLockError(
                f"cannot start {label}: {exc}; another process may be upgrading or writing"
            ) from exc
        try:
            yield
        except BaseException:
            self._conn.execute("ROLLBACK")
            raise
        self._conn.execute("COMMIT")

    def _ensure_aux_tables(self) -> None:
        self._conn.execute(_META_DDL)
        self._conn.execute(_MIGRATIONS_DDL)
        self._conn.execute(_QUARANTINE_DDL)

    def _write_meta(self, version: int, upgraded_from: int | None) -> None:
        cursor = self._conn.execute(
            f"UPDATE {META_TABLE} SET version = ?, upgraded_from = ?, updated_at = ? WHERE id = 1",
            (version, upgraded_from, observed_now()),
        )
        if cursor.rowcount == 0:
            self._conn.execute(
                f"INSERT INTO {META_TABLE} (id, version, upgraded_from, updated_at) VALUES (1, ?, ?, ?)",
                (version, upgraded_from, observed_now()),
            )

    def _ensure_meta_row(self, version: int) -> None:
        self._conn.execute(
            f"INSERT OR IGNORE INTO {META_TABLE} (id, version, upgraded_from, updated_at) "
            "VALUES (1, ?, NULL, ?)",
            (version, observed_now()),
        )

    def _log(self, step: str, from_version: int | None, to_version: int, detail: str) -> None:
        self._conn.execute(
            f"INSERT INTO {MIGRATIONS_TABLE} (step, from_version, to_version, detail, recorded_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (step, from_version, to_version, detail, observed_now()),
        )

    def _quarantine(self, source_version: int, source_row: dict, reason: str) -> None:
        self._conn.execute(
            f"INSERT INTO {QUARANTINE_TABLE} (source_version, reason, row_payload, quarantined_at) "
            "VALUES (?, ?, ?, ?)",
            (
                source_version,
                reason,
                json.dumps(source_row, ensure_ascii=False, sort_keys=True),
                observed_now(),
            ),
        )

    # -- steps ------------------------------------------------------------

    def _init_fresh(self) -> None:
        current = self._registry.current
        with self._write_txn("schema initialisation"):
            if self._detect().state != "fresh":
                return  # a concurrent opener already initialised
            self._conn.execute(current.create_sql())
            self._ensure_aux_tables()
            self._write_meta(current.version, None)
            self._log("init", None, current.version, "fresh database initialised at current structure")
            self._observe("init.committing")

    def _adopt(self) -> None:
        current = self._registry.current
        with self._write_txn("schema adoption"):
            if self._detect().state != "adopt":
                return
            self._ensure_aux_tables()
            self._write_meta(current.version, None)
            self._log(
                "adopt",
                None,
                current.version,
                "existing shipment_events matches current structure; version registered",
            )
            self._observe("adopt.committing")

    def _register(self) -> None:
        current = self._registry.current
        with self._write_txn("version registration"):
            again = self._detect()
            if again.state != "register":
                return
            self._ensure_aux_tables()
            self._write_meta(current.version, again.meta_version)
            self._log(
                "register",
                again.meta_version,
                current.version,
                "structure already at current version; completing version registration",
            )
            self._observe("register.committing")

    def _rebuild(self, det: Detection) -> None:
        migration = det.migration
        if migration is None:
            raise MigrationError("internal error: migrate state without a migration")
        label = f"migration {migration.name}"
        with self._write_txn(label):
            again = self._detect()
            if again.state != "migrate" or again.migration is not migration:
                return  # a concurrent opener already rebuilt
            info = _table_info(self._conn, EVENTS_TABLE)
            if not migration.source.matches(info):
                raise MigrationError(
                    f"{label}: precondition failed; {EVENTS_TABLE} no longer matches "
                    f"declared version {migration.from_version}"
                )
            self._ensure_aux_tables()
            self._ensure_meta_row(migration.from_version)
            staging = f"{EVENTS_TABLE}__v{migration.to_version}"
            if staging in _table_names(self._conn):
                raise MigrationError(
                    f"{label}: staging table {staging} already exists; "
                    "inspect and drop it before retrying"
                )
            self._conn.execute(migration.target.create_sql(staging))
            source_cols = migration.source.column_names()
            target_cols = migration.target.column_names()
            insert_sql = (
                f"INSERT INTO {staging} ({', '.join(target_cols)}) "
                f"VALUES ({', '.join('?' for _ in target_cols)})"
            )
            rows = self._conn.execute(
                f"SELECT {', '.join(source_cols)} FROM {EVENTS_TABLE}"
            ).fetchall()
            copied = 0
            quarantined = 0
            seen_keys: set = set()
            for raw in rows:
                source_row = dict(zip(source_cols, raw))
                target_row = migration.project(source_row)
                reasons = _validate_row(migration, target_row)
                key = target_row.get("event_key")
                if key is not None and key in seen_keys:
                    reasons.append(f"duplicate event_key {key!r}")
                if not reasons:
                    try:
                        self._conn.execute(insert_sql, [target_row[column] for column in target_cols])
                    except sqlite3.IntegrityError as exc:
                        reasons.append(f"rejected by target constraint: {exc}")
                if reasons:
                    self._quarantine(migration.from_version, source_row, "; ".join(reasons))
                    quarantined += 1
                else:
                    seen_keys.add(key)
                    copied += 1
            self._conn.execute(f"DROP TABLE {EVENTS_TABLE}")
            self._conn.execute(f"ALTER TABLE {staging} RENAME TO {EVENTS_TABLE}")
            detail = json.dumps(
                {
                    "copied": copied,
                    "quarantined": quarantined,
                    "mapping": migration.describe_mapping(),
                },
                ensure_ascii=False,
                sort_keys=True,
            )
            self._log("rebuild", migration.from_version, migration.to_version, detail)
            self._observe("rebuild.committing")
        # Checkpoint committed: the structure is now current but the version
        # marker is not yet written.  A crash here is recovered by _register.
        self._observe("rebuild.committed")
