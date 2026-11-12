"""Reproducible streaming export of a snapshot's filtered result set.

The export is a single JSON Lines document. The first line is a metadata
header recording the read context (snapshot id, store revision, filter,
ordering and total count); subsequent lines are one event per line.

Pages are streamed through the keyset cursor and event lines are spooled to a
temporary file, so the full result set is never held in memory at once while
the header can still carry the final ``event_count``. Re-opening a snapshot on
an unchanged database and exporting with the same filter reproduces the same
ordered rows.
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict
from typing import IO, Optional

from .snapshot import EventFilter, EventSnapshot

EXPORT_FORMAT = "port-events-snapshot-jsonl/1"
ORDERING = ["event_time_utc", "event_key"]


def _event_record(event) -> dict:
    return {
        "event_key": event.event_key,
        "vessel": event.vessel,
        "voyage": event.voyage,
        "container": event.container,
        "port": event.port,
        "event_type": event.event_type,
        "event_time_utc": event.event_time_utc,
        "status": event.status,
        "sources": list(event.sources),
        "observed_at": event.observed_at,
    }


def _build_header(snapshot: EventSnapshot, page_size: int, event_count: int) -> dict:
    return {
        "format": EXPORT_FORMAT,
        "snapshot_id": snapshot.snapshot_id,
        "revision": snapshot.revision,
        "ordering": ORDERING,
        "page_size": page_size,
        "event_count": event_count,
        "filter": asdict(snapshot.filter),
    }


def export_snapshot(snapshot: EventSnapshot, stream: IO[str],
                    page_size: int = 100) -> dict:
    """Write all visible pages of ``snapshot`` to ``stream`` as JSON Lines.

    Event rows are spooled to a temporary file while paging so memory use stays
    bounded. The metadata header (including ``event_count``) is returned and
    written as the first line.
    """
    spool = tempfile.SpooledTemporaryFile(mode="w+", encoding="utf-8")
    total = 0
    for page in snapshot.iter_pages(page_size=page_size):
        for event in page.events:
            spool.write(
                json.dumps(_event_record(event), sort_keys=True,
                           ensure_ascii=False) + "\n"
            )
            total += 1
    header = _build_header(snapshot, page_size, total)
    stream.write(json.dumps(header, sort_keys=True, ensure_ascii=False) + "\n")
    spool.seek(0)
    while True:
        chunk = spool.read(1024 * 1024)
        if not chunk:
            break
        stream.write(chunk)
    spool.close()
    return header


def export_filter(store, event_filter: Optional[EventFilter], path: str,
                  page_size: int = 100, ttl_seconds: float = 300.0) -> dict:
    """Open a snapshot, export it to ``path`` and close the snapshot.

    The output file is written atomically: a partial export never replaces an
    existing destination file.
    """
    destination = os.path.abspath(path)
    with store.open_snapshot(event_filter, ttl_seconds=ttl_seconds) as snapshot:
        directory = os.path.dirname(destination) or "."
        fd, tmp_path = tempfile.mkstemp(prefix=".export-", dir=directory,
                                        suffix=".jsonl")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                header = export_snapshot(snapshot, stream, page_size=page_size)
            os.replace(tmp_path, destination)
        except BaseException:
            try:
                os.unlink(tmp_path)
            except FileNotFoundError:
                pass
            raise
    return header
