"""Acceptance tests for snapshot retrieval and keyset pagination.

Covers: same-instant rows, vessel-level rows without container numbers, empty
results, illegal time ranges, cross-criteria cursors, cursor expiry, and
writes landing between pages -- proving legal continuation never loses or
duplicates rows and that invalid cursors fail with an explicit reason.
"""

from __future__ import annotations

import io
import json
import tempfile
import unittest
from pathlib import Path

from port_events.connectors import JsonEventSource
from port_events.export import EXPORT_FORMAT, export_snapshot
from port_events.models import ShipmentEvent
from port_events.pipeline import PortEventsPipeline
from port_events.snapshot import (
    CursorExpired,
    CursorInvalid,
    EventFilter,
    InvalidFilter,
    InvalidTimeRange,
    SnapshotClosed,
)
from port_events.storage import EventStore


def make_event(event_key, vessel="VESSEL A", voyage="V001", container="CONT-1",
               port="CNSHA", event_type="arrived",
               event_time_utc="2026-11-10T00:00:00+00:00", status="actual",
               sources=("ref-1",), observed_at="2026-11-01T00:00:00+00:00"):
    return ShipmentEvent(event_key, vessel, voyage, container, port, event_type,
                         event_time_utc, status, tuple(sources), observed_at)


# A deliberately varied dataset: same-instant rows, vessel-level NULL container
# rows, distinct vessels/ports/types and a wide time window.
SEED_EVENTS = [
    make_event("K01", event_time_utc="2026-11-01T00:00:00+00:00",
               container="CONT-1", event_type="arrived", port="CNSHA"),
    make_event("K02", event_time_utc="2026-11-01T00:00:00+00:00",
               container="CONT-2", event_type="arrived", port="CNSHA"),
    make_event("K03", event_time_utc="2026-11-01T00:00:00+00:00",
               container=None, event_type="berthed", port="CNSHA"),
    make_event("K04", event_time_utc="2026-11-02T08:30:00+00:00",
               container="CONT-1", event_type="loaded", port="CNNGB"),
    make_event("K05", event_time_utc="2026-11-03T12:00:00+00:00",
               container=None, event_type="departed", port="CNSHA"),
    make_event("K06", event_time_utc="2026-11-05T23:59:59+00:00",
               container="CONT-9", vessel="VESSEL B", voyage="V002",
               event_type="gate_in", port="HKHKG"),
]


class SnapshotTestBase(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "events.sqlite3"
        self.store = EventStore(self.db_path)
        self.store.upsert_many(SEED_EVENTS)

    def tearDown(self):
        self.store.close()
        self.tempdir.cleanup()

    def drain(self, snapshot, page_size):
        """Walk every page and assert no missing/duplicate keys."""
        seen = []
        for page in snapshot.iter_pages(page_size=page_size):
            self.assertEqual(page.page_size, page_size)
            seen.extend(event.event_key for event in page.events)
        self.assertEqual(len(seen), len(set(seen)), "duplicate keys across pages")
        return seen

    def expected_keys(self, event_filter):
        # Independent in-Python reference implementation of the filter.
        result = []
        for event in SEED_EVENTS:
            if event_filter.container is not None and event.container != event_filter.container:
                continue
            if event_filter.vessel is not None and event.vessel != event_filter.vessel:
                continue
            if event_filter.voyage is not None and event.voyage != event_filter.voyage:
                continue
            if event_filter.port is not None and event.port != event_filter.port:
                continue
            if event_filter.event_type is not None and event.event_type != event_filter.event_type:
                continue
            if (event_filter.start_time_utc is not None
                    and event.event_time_utc < event_filter.start_time_utc):
                continue
            if (event_filter.end_time_utc is not None
                    and event.event_time_utc > event_filter.end_time_utc):
                continue
            result.append(event.event_key)
        return sorted(
            result, key=lambda key: next(
                (e.event_time_utc, e.event_key) for e in SEED_EVENTS
                if e.event_key == key
            )
        )


class PaginationTests(SnapshotTestBase):
    def test_same_instant_rows_are_ordered_and_complete(self):
        event_filter = EventFilter(
            start_time_utc="2026-11-01T00:00:00Z",
            end_time_utc="2026-11-01T00:00:00Z",
        )
        with self.store.open_snapshot(event_filter) as snapshot:
            keys = self.drain(snapshot, page_size=1)
        self.assertEqual(keys, ["K01", "K02", "K03"])

    def test_pagination_never_loses_or_duplicates_for_every_page_size(self):
        event_filter = EventFilter()
        expected = self.expected_keys(event_filter)
        for page_size in range(1, len(SEED_EVENTS) + 2):
            with self.store.open_snapshot(event_filter) as snapshot:
                keys = self.drain(snapshot, page_size)
            self.assertEqual(keys, expected)

    def test_pages_carry_event_key_status_and_source_refs(self):
        with self.store.open_snapshot(EventFilter()) as snapshot:
            page = snapshot.fetch_page(page_size=2)
        self.assertEqual(len(page.events), 2)
        first = page.events[0]
        self.assertEqual(first.event_key, "K01")
        self.assertEqual(first.status, "actual")
        self.assertEqual(first.sources, ("ref-1",))
        self.assertTrue(page.has_more)
        self.assertIsNotNone(page.next_cursor)
        self.assertEqual(page.revision, 1)

    def test_last_page_reports_no_more_and_no_cursor(self):
        with self.store.open_snapshot(EventFilter()) as snapshot:
            page = snapshot.fetch_page(page_size=len(SEED_EVENTS))
        self.assertFalse(page.has_more)
        self.assertIsNone(page.next_cursor)
        self.assertEqual(len(page.events), len(SEED_EVENTS))

    def test_exact_multiple_page_size_emits_no_trailing_empty_page(self):
        with self.store.open_snapshot(EventFilter()) as snapshot:
            pages = list(snapshot.iter_pages(page_size=3))
        self.assertEqual(len(pages), 2)  # 6 rows / 3, not 3 pages
        self.assertFalse(pages[-1].has_more)
        self.assertIsNone(pages[-1].next_cursor)
        self.assertEqual(len(pages[-1].events), 3)


class FilterTests(SnapshotTestBase):
    def test_vessel_level_rows_without_container(self):
        # A container filter must exclude NULL-container vessel-level rows.
        with self.store.open_snapshot(EventFilter(container="CONT-1")) as snapshot:
            keys = self.drain(snapshot, page_size=2)
        self.assertEqual(keys, ["K01", "K04"])

        # Without a container filter the vessel-level rows are visible.
        with self.store.open_snapshot(
            EventFilter(event_type="departed")
        ) as snapshot:
            page = snapshot.fetch_page(page_size=10)
        self.assertEqual([e.event_key for e in page.events], ["K05"])
        self.assertIsNone(page.events[0].container)

    def test_all_filter_dimensions_combined(self):
        event_filter = EventFilter(
            container="CONT-1", vessel="VESSEL A", voyage="V001",
            port="CNNGB", event_type="loaded",
            start_time_utc="2026-11-02T00:00:00+00:00",
            end_time_utc="2026-11-02T23:59:59+00:00",
        )
        with self.store.open_snapshot(event_filter) as snapshot:
            keys = self.drain(snapshot, page_size=1)
        self.assertEqual(keys, ["K04"])
        self.assertEqual(keys, self.expected_keys(event_filter))

    def test_empty_result(self):
        event_filter = EventFilter(container="DOES-NOT-EXIST")
        with self.store.open_snapshot(event_filter) as snapshot:
            page = snapshot.fetch_page(page_size=10)
        self.assertEqual(page.events, ())
        self.assertFalse(page.has_more)
        self.assertIsNone(page.next_cursor)

    def test_inclusive_utc_time_bounds_and_offset_normalization(self):
        # Bounds given with a non-UTC offset are canonicalized to UTC.
        event_filter = EventFilter(
            start_time_utc="2026-11-02T16:30:00+08:00",  # 08:30 UTC
            end_time_utc="2026-11-03T20:00:00+08:00",    # 12:00 UTC
        )
        with self.store.open_snapshot(event_filter) as snapshot:
            keys = self.drain(snapshot, page_size=5)
        self.assertEqual(keys, ["K04", "K05"])

    def test_illegal_time_range_start_after_end(self):
        with self.assertRaises(InvalidTimeRange):
            EventFilter(
                start_time_utc="2026-11-10T00:00:00Z",
                end_time_utc="2026-11-09T00:00:00Z",
            )

    def test_illegal_time_range_naive_and_unparseable(self):
        with self.assertRaises(InvalidTimeRange):
            EventFilter(start_time_utc="2026-11-10T00:00:00")
        with self.assertRaises(InvalidTimeRange):
            EventFilter(end_time_utc="not-a-timestamp")

    def test_illegal_page_size(self):
        with self.store.open_snapshot(EventFilter()) as snapshot:
            for bad in (0, -1, 1.5, "10"):
                with self.assertRaises(InvalidFilter):
                    snapshot.fetch_page(page_size=bad)


class CursorContractTests(SnapshotTestBase):
    def test_cursor_rejected_across_filter_criteria(self):
        with self.store.open_snapshot(EventFilter(port="CNSHA")) as snapshot_a:
            page = snapshot_a.fetch_page(page_size=1)
            cursor = page.next_cursor
        with self.store.open_snapshot(EventFilter(port="HKHKG")) as snapshot_b:
            with self.assertRaises(CursorInvalid) as ctx:
                snapshot_b.fetch_page(cursor=cursor, page_size=1)
        self.assertEqual(ctx.exception.reason, "cursor_invalid")

    def test_tampered_and_foreign_cursors_are_rejected(self):
        with self.store.open_snapshot(EventFilter()) as snapshot:
            cursor = snapshot.fetch_page(page_size=1).next_cursor
            with self.assertRaises(CursorInvalid):
                snapshot.fetch_page(cursor="garbage-token")
            tampered = cursor[:-2] + ("AA" if not cursor.endswith("AA") else "BB")
            with self.assertRaises(CursorInvalid):
                snapshot.fetch_page(cursor=tampered)

    def test_expired_cursor_explains_reason(self):
        with self.store.open_snapshot(EventFilter(), ttl_seconds=300) as snapshot:
            cursor = snapshot.fetch_page(page_size=1).next_cursor
            # Rewrite the embedded expiry to the past without weakening the
            # contract test: expiry is enforced before the next fetch, and a
            # genuinely expired TTL is also covered by ttl_seconds boundary.
            import base64
            import hmac as hmac_mod
            import hashlib
            body_b64, _ = cursor.split(".", 1)
            payload = json.loads(base64.urlsafe_b64decode(body_b64))
            payload["expires_at"] = "2000-01-01T00:00:00+00:00"
            new_body = base64.urlsafe_b64encode(
                json.dumps(payload, separators=(",", ":")).encode()
            )
            sig = hmac_mod.new(snapshot._secret, new_body,
                               hashlib.sha256).digest()
            expired_token = (
                new_body + b"." + base64.urlsafe_b64encode(sig)
            ).decode()
            with self.assertRaises(CursorExpired) as ctx:
                snapshot.fetch_page(cursor=expired_token)
            self.assertEqual(ctx.exception.reason, "cursor_expired")
            self.assertIn("expired", str(ctx.exception))

    def test_zero_ttl_is_rejected(self):
        with self.assertRaises(InvalidFilter):
            self.store.open_snapshot(EventFilter(), ttl_seconds=0)

    def test_genuinely_expired_ttl_fails_on_next_fetch(self):
        snapshot = self.store.open_snapshot(EventFilter(), ttl_seconds=0.05)
        try:
            first = snapshot.fetch_page(page_size=2)
            token = snapshot._encode_cursor(
                first.events[-1].event_time_utc,
                first.events[-1].event_key,
            )
            import time
            time.sleep(0.08)
            with self.assertRaises(CursorExpired) as ctx:
                snapshot.fetch_page(cursor=token, page_size=2)
            self.assertEqual(ctx.exception.reason, "cursor_expired")
        finally:
            snapshot.close()

    def test_cursor_after_snapshot_closed(self):
        snapshot = self.store.open_snapshot(EventFilter())
        cursor = snapshot.fetch_page(page_size=1).next_cursor
        snapshot.close()
        with self.assertRaises(SnapshotClosed):
            snapshot.fetch_page(cursor=cursor)


class SnapshotConsistencyTests(SnapshotTestBase):
    def test_writes_between_pages_are_not_visible_to_open_snapshot(self):
        snapshot = self.store.open_snapshot(EventFilter())
        try:
            first = snapshot.fetch_page(page_size=2)
            self.assertEqual(len(first.events), 2)

            # Insert new rows and overwrite (INSERT OR REPLACE) an existing one
            # while pagination is in flight.
            inserted = make_event(
                "K07", event_time_utc="2026-10-31T00:00:00+00:00",
                container="CONT-7", event_type="arrived",
            )
            replaced = make_event(
                "K01", event_time_utc="2026-11-01T00:00:00+00:00",
                status="cancelled", sources=("ref-overwritten",),
            )
            self.store.upsert_many([inserted, replaced])

            # Continue from the cursor rather than restarting: the first page is
            # only counted once, proving continuation has no gap or duplicate.
            keys = [event.event_key for event in first.events]
            cursor = first.next_cursor
            while cursor is not None:
                page = snapshot.fetch_page(cursor=cursor, page_size=2)
                keys.extend(event.event_key for event in page.events)
                cursor = page.next_cursor
            # The pinned snapshot still sees the original six rows exactly once.
            self.assertEqual(sorted(keys), sorted(e.event_key for e in SEED_EVENTS))
            k01 = next(event for event in self._all_snapshot_events(snapshot)
                       if event.event_key == "K01")
            self.assertEqual(k01.status, "actual")
            self.assertEqual(k01.sources, ("ref-1",))
            self.assertEqual(snapshot.revision, 1)
        finally:
            snapshot.close()

        # A fresh snapshot reads revision 2 and observes the writes.
        with self.store.open_snapshot(EventFilter()) as fresh:
            self.assertEqual(fresh.revision, 2)
            keys = self.drain(fresh, page_size=3)
        self.assertIn("K07", keys)
        self.assertEqual(len(keys), len(SEED_EVENTS) + 1)

    def _all_snapshot_events(self, snapshot):
        events = []
        for page in snapshot.iter_pages(page_size=100):
            events.extend(page.events)
        return events

    def test_store_close_closes_snapshots(self):
        store = EventStore(self.db_path)
        snapshot = store.open_snapshot(EventFilter())
        snapshot.fetch_page(page_size=1)
        store.close()
        with self.assertRaises(SnapshotClosed):
            snapshot.fetch_page()

    def test_memory_database_rejects_snapshot(self):
        memory_store = EventStore(":memory:")
        try:
            with self.assertRaises(InvalidFilter):
                memory_store.open_snapshot(EventFilter())
        finally:
            memory_store.close()


class PushdownTests(SnapshotTestBase):
    def test_queries_are_limited_keyset_scans_not_full_materialization(self):
        traced = []
        with self.store.open_snapshot(
            EventFilter(container="CONT-1", port="CNSHA")
        ) as snapshot:
            snapshot.connection.set_trace_callback(
                lambda statement: traced.append(statement)
            )
            page = snapshot.fetch_page(page_size=2)

        select_sql = [
            statement for statement in traced
            if " FROM shipment_events" in statement
        ]
        self.assertEqual(len(select_sql), 1)
        sql = select_sql[0]
        self.assertIn("WHERE", sql)
        self.assertIn("container IS NOT NULL AND container = 'CONT-1'", sql)
        self.assertIn("port = 'CNSHA'", sql)
        self.assertIn("ORDER BY event_time_utc ASC, event_key ASC", sql)
        # The trace shows the bound LIMIT (page_size + 1) inlined; no OFFSET
        # and never an unbounded scan.
        self.assertIn("LIMIT 3", sql)
        self.assertNotIn("OFFSET", sql.upper())
        # At most page_size rows are returned to the caller.
        self.assertLessEqual(len(page.events), 2)

    def test_continuation_query_embeds_keyset_predicate(self):
        traced = []
        with self.store.open_snapshot(EventFilter()) as snapshot:
            first = snapshot.fetch_page(page_size=2)
            snapshot.connection.set_trace_callback(
                lambda statement: traced.append(statement)
            )
            second = snapshot.fetch_page(cursor=first.next_cursor, page_size=2)
        sql = next(s for s in traced if " FROM shipment_events" in s)
        self.assertIn("event_time_utc > '2026-11-01T00:00:00+00:00' OR "
                      "(event_time_utc = '2026-11-01T00:00:00+00:00' "
                      "AND event_key > 'K02')", sql)
        self.assertIn("LIMIT 3", sql)
        self.assertEqual(len(second.events), 2)
        self.assertNotIn(
            second.events[0].event_key,
            [event.event_key for event in first.events],
        )


class ExportTests(SnapshotTestBase):
    def test_export_is_streaming_and_reproducible(self):
        event_filter = EventFilter(vessel="VESSEL A")
        buffer = io.StringIO()
        with self.store.open_snapshot(event_filter) as snapshot:
            header = export_snapshot(snapshot, buffer, page_size=2)
        lines = buffer.getvalue().splitlines()
        self.assertEqual(json.loads(lines[0])["format"], EXPORT_FORMAT)
        records = [json.loads(line) for line in lines[1:]]
        self.assertEqual(header["event_count"], len(records))
        self.assertEqual(
            [record["event_key"] for record in records],
            ["K01", "K02", "K03", "K04", "K05"],
        )
        self.assertEqual(records[0]["sources"], ["ref-1"])

        # Re-export on the unchanged store reproduces the same ordered rows.
        again = io.StringIO()
        with self.store.open_snapshot(event_filter) as snapshot2:
            export_snapshot(snapshot2, again, page_size=4)
        self.assertEqual(
            [json.loads(line)["event_key"] for line in again.getvalue().splitlines()[1:]],
            [record["event_key"] for record in records],
        )

    def test_export_empty_result_has_header_only(self):
        buffer = io.StringIO()
        with self.store.open_snapshot(EventFilter(container="NOPE")) as snapshot:
            header = export_snapshot(snapshot, buffer)
        self.assertEqual(header["event_count"], 0)
        self.assertEqual(len(buffer.getvalue().splitlines()), 1)


class ImportSemanticsTests(unittest.TestCase):
    def test_existing_pipeline_import_remains_unchanged(self):
        with tempfile.TemporaryDirectory() as tmp:
            source_path = Path(tmp) / "events.json"
            source_path.write_text(
                '[{"source":"a","vessel":"V","voyage":"1","container":"C",'
                '"port":"P","event_code":"ATA","event_time":"2026-01-01T00:00:00Z",'
                '"status":"estimated","source_ref":"a1"},'
                '{"source":"b","vessel":"v","voyage":"1","container":"C",'
                '"port":"p","event_code":"ATA",'
                '"event_time":"2025-12-31T16:00:00-08:00","status":"actual",'
                '"source_ref":"b1"}]'
            )
            db_path = Path(tmp) / "events.sqlite3"
            store = EventStore(db_path)
            try:
                events = PortEventsPipeline(
                    JsonEventSource(source_path), store
                ).run()
                self.assertEqual(len(events), 1)
                self.assertEqual(events[0].status, "actual")
                self.assertEqual(events[0].sources, ("a1", "b1"))
                self.assertEqual(store.current_revision(), 1)
                # Re-importing the same data replaces, not duplicates.
                PortEventsPipeline(JsonEventSource(source_path), store).run()
                self.assertEqual(store.current_revision(), 2)
                with store.open_snapshot(EventFilter()) as snapshot:
                    page = snapshot.fetch_page(page_size=100)
                self.assertEqual(len(page.events), 1)
            finally:
                store.close()


class CliTests(SnapshotTestBase):
    def test_export_subcommand_writes_jsonl_and_rejects_bad_range(self):
        from port_events.cli import main

        out_path = Path(self.tempdir.name) / "export.jsonl"
        main([
            "export", "--database", str(self.db_path),
            "--out", str(out_path), "--page-size", "2",
            "--vessel", "vessel a",
        ])
        lines = out_path.read_text(encoding="utf-8").splitlines()
        header = json.loads(lines[0])
        self.assertEqual(header["format"], EXPORT_FORMAT)
        self.assertEqual(header["event_count"], 5)  # all VESSEL A rows
        self.assertEqual(header["page_size"], 2)
        self.assertEqual(
            [json.loads(line)["event_key"] for line in lines[1:]],
            ["K01", "K02", "K03", "K04", "K05"],
        )

        import contextlib
        import io as io_module
        stderr = io_module.StringIO()
        with contextlib.redirect_stderr(stderr):
            with self.assertRaises(SystemExit) as ctx:
                main([
                    "export", "--database", str(self.db_path),
                    "--out", str(Path(self.tempdir.name) / "x.jsonl"),
                    "--start-time-utc", "2026-11-12T00:00:00Z",
                    "--end-time-utc", "2026-11-10T00:00:00Z",
                ])
        self.assertEqual(ctx.exception.code, 2)
        self.assertIn("invalid_time_range", stderr.getvalue())

    def test_legacy_import_form_remains_accepted(self):
        from port_events.cli import main

        fixture_path = Path(self.tempdir.name) / "events.json"
        fixture_path.write_text(json.dumps([
            {
                "source": "carrier-api", "vessel": "Ocean Star",
                "voyage": "OS202611A", "container": "CONT001",
                "port": "CNSHA", "event_code": "ATA",
                "event_time": "2026-11-10T08:00:00+08:00",
                "status": "actual", "source_ref": "carrier-event-1",
            }
        ]))
        db_path = Path(self.tempdir.name) / "legacy.sqlite3"
        # Flat, pre-subcommand argument layout must keep working.
        main(["--fixture", str(fixture_path), "--database", str(db_path)])
        store = EventStore(db_path)
        try:
            with store.open_snapshot(EventFilter()) as snapshot:
                page = snapshot.fetch_page(page_size=10)
            self.assertEqual(len(page.events), 1)
            self.assertEqual(page.events[0].event_type, "arrived")
        finally:
            store.close()


if __name__ == "__main__":
    unittest.main()
