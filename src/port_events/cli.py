from __future__ import annotations

import argparse
import json
import sys

from .connectors import JsonEventSource
from .export import EXPORT_FORMAT, export_snapshot
from .pipeline import PortEventsPipeline
from .snapshot import EventFilter, SnapshotError
from .storage import EventStore


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Port events import and snapshot export."
    )
    sub = parser.add_subparsers(dest="command")

    import_parser = sub.add_parser("import", help="import events from a JSON fixture")
    import_parser.add_argument("--fixture", required=True)
    import_parser.add_argument("--database", default="port-events.sqlite3")
    import_parser.add_argument("--timezone", default="Asia/Shanghai")

    export_parser = sub.add_parser(
        "export", help="export a filtered, paginated read snapshot as JSON Lines"
    )
    export_parser.add_argument("--database", default="port-events.sqlite3")
    export_parser.add_argument("--out", required=True, help="output JSONL path")
    export_parser.add_argument("--container")
    export_parser.add_argument("--vessel")
    export_parser.add_argument("--voyage")
    export_parser.add_argument("--port")
    export_parser.add_argument("--event-type", dest="event_type")
    export_parser.add_argument("--start-time-utc", dest="start_time_utc")
    export_parser.add_argument("--end-time-utc", dest="end_time_utc")
    export_parser.add_argument("--page-size", dest="page_size", type=int, default=100)
    export_parser.add_argument("--ttl-seconds", dest="ttl_seconds", type=float,
                               default=300.0)
    return parser


def _legacy_argv(argv) -> list:
    """Translate the historical flat `--fixture ... --database ...` form."""
    return ["import", *argv]


def main(argv: list | None = None) -> None:
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    parser = _build_parser()
    if raw_argv and raw_argv[0] not in {"import", "export", "-h", "--help"}:
        raw_argv = _legacy_argv(raw_argv)
    args = parser.parse_args(raw_argv)

    if args.command == "export":
        try:
            event_filter = EventFilter(
                container=args.container,
                vessel=args.vessel,
                voyage=args.voyage,
                port=args.port,
                event_type=args.event_type,
                start_time_utc=args.start_time_utc,
                end_time_utc=args.end_time_utc,
            )
            store = EventStore(args.database)
            try:
                with store.open_snapshot(
                    event_filter, ttl_seconds=args.ttl_seconds
                ) as snapshot:
                    with open(args.out, "w", encoding="utf-8") as stream:
                        header = export_snapshot(
                            snapshot, stream, page_size=args.page_size
                        )
            finally:
                store.close()
        except (SnapshotError, OSError) as exc:
            print(json.dumps({"error": exc.reason, "detail": str(exc)}),
                  file=sys.stderr)
            raise SystemExit(2)
        print(json.dumps(header, sort_keys=True, ensure_ascii=False))
        return

    # command == "import": identical import behavior to the original CLI.
    store = EventStore(args.database)
    try:
        events = PortEventsPipeline(
            JsonEventSource(args.fixture), store
        ).run(args.timezone)
        print({"events": len(events), "database": args.database})
    finally:
        store.close()


if __name__ == "__main__":
    main()
