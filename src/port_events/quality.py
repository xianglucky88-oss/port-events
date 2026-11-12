"""Read-only single-operation sequence quality checks for shipment events.

The caller scopes a check to one vessel / voyage / port and a half-open UTC
window ``[window_start, window_end)``. Events belonging to repeated port calls
are never attributed to a specific operation: if more than one event with the
same role falls inside the window it is reported as a role conflict instead.

Only ``actual`` rows inside the window participate in the checks:

* vessel nodes must satisfy ``arrived <= berthed <= departed``;
* per container, ``gate_in <= loaded`` and ``loaded <= departed``; a single
  vessel ``departed`` node may be shared by every container.

Missing unique nodes are reported as missing evidence, duplicated roles as
role conflicts, and chronological order is only evaluated when both compared
nodes are unique. Nothing is ever written back to the database.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence

from .models import ShipmentEvent

VESSEL_ROLES: tuple[str, ...] = ("arrived", "berthed", "departed")
CONTAINER_ROLES: tuple[str, ...] = ("gate_in", "loaded")

ISSUE_MISSING_EVIDENCE = "missing_evidence"
ISSUE_ROLE_CONFLICT = "role_conflict"
ISSUE_OUT_OF_ORDER = "out_of_order"

_SCOPE_VESSEL = "vessel"
_SCOPE_CONTAINER = "container"


@dataclass(frozen=True)
class QualityIssue:
    """One stable quality finding tied to zero or more ``event_key`` values."""

    issue_type: str
    scope: str
    role: str
    detail: str
    event_keys: tuple[str, ...]
    container: str | None = None


def _to_utc(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        moment = value
    else:
        moment = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc)


def _sorted_nodes(nodes: Sequence[tuple[datetime, ShipmentEvent]]) -> list[tuple[datetime, ShipmentEvent]]:
    return sorted(nodes, key=lambda item: (item[0].isoformat(), item[1].event_key))


def check_sequence(
    events: Iterable[ShipmentEvent],
    vessel: str,
    voyage: str,
    port: str,
    window_start: datetime | str,
    window_end: datetime | str,
) -> list[QualityIssue]:
    """Check ``actual`` events of one vessel/voyage/port inside a UTC window.

    The window is half-open: ``window_start <= event_time < window_end``.
    Inputs are never mutated and the function has no side effects.
    """
    vessel_key = vessel.strip().upper()
    voyage_key = voyage.strip().upper()
    port_key = port.strip().upper()
    start = _to_utc(window_start)
    end = _to_utc(window_end)
    if not start < end:
        raise ValueError("window_start must be strictly before window_end")

    vessel_nodes: dict[str, list[tuple[datetime, ShipmentEvent]]] = {role: [] for role in VESSEL_ROLES}
    container_nodes: dict[str, dict[str, list[tuple[datetime, ShipmentEvent]]]] = {}

    for event in events:
        if event.status != "actual":
            continue
        if (event.vessel, event.voyage, event.port) != (vessel_key, voyage_key, port_key):
            continue
        moment = _to_utc(event.event_time_utc)
        if not (start <= moment < end):
            continue
        if event.event_type in vessel_nodes:
            vessel_nodes[event.event_type].append((moment, event))
        elif event.event_type in CONTAINER_ROLES and event.container:
            group = container_nodes.setdefault(
                event.container, {role: [] for role in CONTAINER_ROLES}
            )
            group[event.event_type].append((moment, event))

    issues: list[QualityIssue] = []
    identity = f"{vessel_key}|{voyage_key}|{port_key}"

    # Vessel-level nodes: missing evidence vs. role conflicts.
    for role in VESSEL_ROLES:
        nodes = _sorted_nodes(vessel_nodes[role])
        if not nodes:
            issues.append(
                QualityIssue(
                    ISSUE_MISSING_EVIDENCE,
                    _SCOPE_VESSEL,
                    role,
                    f"vessel operation {identity} has no actual {role} in window",
                    (),
                )
            )
        elif len(nodes) > 1:
            for _, event in nodes:
                issues.append(
                    QualityIssue(
                        ISSUE_ROLE_CONFLICT,
                        _SCOPE_VESSEL,
                        role,
                        f"vessel operation {identity} has {len(nodes)} actual {role} events",
                        (event.event_key,),
                    )
                )

    # Vessel ordering: only compare roles that resolved to a unique node.
    unique_vessel: dict[str, tuple[datetime, ShipmentEvent]] = {
        role: nodes[0] for role, nodes in vessel_nodes.items() if len(nodes) == 1
    }
    for earlier_role, later_role in (
        ("arrived", "berthed"),
        ("berthed", "departed"),
        ("arrived", "departed"),
    ):
        if earlier_role in unique_vessel and later_role in unique_vessel:
            earlier_time, earlier_event = unique_vessel[earlier_role]
            later_time, later_event = unique_vessel[later_role]
            if earlier_time > later_time:
                issues.append(
                    QualityIssue(
                        ISSUE_OUT_OF_ORDER,
                        _SCOPE_VESSEL,
                        f"{earlier_role}>{later_role}",
                        (
                            f"{earlier_role} at {earlier_time.isoformat()} is after "
                            f"{later_role} at {later_time.isoformat()}"
                        ),
                        (earlier_event.event_key, later_event.event_key),
                    )
                )

    # Container-level nodes, each checked against the shared vessel departed node.
    unique_departed = unique_vessel.get("departed")
    for container in sorted(container_nodes):
        groups = container_nodes[container]
        for role in CONTAINER_ROLES:
            nodes = _sorted_nodes(groups[role])
            if not nodes:
                issues.append(
                    QualityIssue(
                        ISSUE_MISSING_EVIDENCE,
                        _SCOPE_CONTAINER,
                        role,
                        f"container {container} of {identity} has no actual {role} in window",
                        (),
                        container,
                    )
                )
            elif len(nodes) > 1:
                for _, event in nodes:
                    issues.append(
                        QualityIssue(
                            ISSUE_ROLE_CONFLICT,
                            _SCOPE_CONTAINER,
                            role,
                            (
                                f"container {container} of {identity} has "
                                f"{len(nodes)} actual {role} events"
                            ),
                            (event.event_key,),
                            container,
                        )
                    )

        unique_container = {role: nodes[0] for role, nodes in groups.items() if len(nodes) == 1}
        if "gate_in" in unique_container and "loaded" in unique_container:
            gate_in_time, gate_in_event = unique_container["gate_in"]
            loaded_time, loaded_event = unique_container["loaded"]
            if gate_in_time > loaded_time:
                issues.append(
                    QualityIssue(
                        ISSUE_OUT_OF_ORDER,
                        _SCOPE_CONTAINER,
                        "gate_in>loaded",
                        (
                            f"container {container} gate_in at {gate_in_time.isoformat()} is after "
                            f"loaded at {loaded_time.isoformat()}"
                        ),
                        (gate_in_event.event_key, loaded_event.event_key),
                        container,
                    )
                )
        if "loaded" in unique_container and unique_departed is not None:
            loaded_time, loaded_event = unique_container["loaded"]
            departed_time, departed_event = unique_departed
            if loaded_time > departed_time:
                issues.append(
                    QualityIssue(
                        ISSUE_OUT_OF_ORDER,
                        _SCOPE_CONTAINER,
                        "loaded>departed",
                        (
                            f"container {container} loaded at {loaded_time.isoformat()} is after "
                            f"vessel departed at {departed_time.isoformat()}"
                        ),
                        (loaded_event.event_key, departed_event.event_key),
                        container,
                    )
                )

    issues.sort(
        key=lambda item: (
            item.scope,
            item.container or "",
            item.issue_type,
            item.role,
            item.event_keys,
        )
    )
    return issues


def open_readonly_connection(database: str | Path) -> sqlite3.Connection:
    """Open a SQLite database in immutable read-only mode (no writes possible)."""
    path = Path(database)
    if not path.is_file():
        raise FileNotFoundError(database)
    uri = f"file:{path.resolve()}?mode=ro"
    return sqlite3.connect(uri, uri=True)


def load_window_events(
    connection: sqlite3.Connection,
    vessel: str,
    voyage: str,
    port: str,
    window_start: datetime | str,
    window_end: datetime | str,
) -> list[ShipmentEvent]:
    """Load actual rows matching the identity and the half-open UTC window."""
    start_iso = _to_utc(window_start).isoformat()
    end_iso = _to_utc(window_end).isoformat()
    rows = connection.execute(
        """
        SELECT event_key, vessel, voyage, container, port, event_type,
               event_time_utc, status, sources, observed_at
        FROM shipment_events
        WHERE vessel = ? AND voyage = ? AND port = ?
          AND status = 'actual'
          AND event_time_utc >= ? AND event_time_utc < ?
        ORDER BY event_time_utc, event_key
        """,
        (vessel.strip().upper(), voyage.strip().upper(), port.strip().upper(), start_iso, end_iso),
    ).fetchall()
    return [
        ShipmentEvent(
            event_key=row[0],
            vessel=row[1],
            voyage=row[2],
            container=row[3],
            port=row[4],
            event_type=row[5],
            event_time_utc=row[6],
            status=row[7],
            sources=tuple(row[8].split(",")) if row[8] else (),
            observed_at=row[9],
        )
        for row in rows
    ]


def check_database(
    database: str | Path,
    vessel: str,
    voyage: str,
    port: str,
    window_start: datetime | str,
    window_end: datetime | str,
) -> list[QualityIssue]:
    """Run the sequence check against a SQLite database without writing to it."""
    connection = open_readonly_connection(database)
    try:
        events = load_window_events(
            connection, vessel, voyage, port, window_start, window_end
        )
        return check_sequence(events, vessel, voyage, port, window_start, window_end)
    finally:
        connection.close()
