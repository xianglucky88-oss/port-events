from __future__ import annotations
import argparse
from .connectors import JsonEventSource
from .pipeline import PortEventsPipeline
from .storage import EventStore


def _parse_source_timezone(value: str) -> tuple[str, str]:
    if "=" not in value:
        raise argparse.ArgumentTypeError(
            f"expected SOURCE=TIMEZONE, got {value!r}"
        )
    source, _, timezone_name = value.partition("=")
    source = source.strip()
    timezone_name = timezone_name.strip()
    if not source or not timezone_name:
        raise argparse.ArgumentTypeError(
            f"expected SOURCE=TIMEZONE, got {value!r}"
        )
    return source, timezone_name


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture", required=True)
    parser.add_argument("--database", default="port-events.sqlite3")
    parser.add_argument(
        "--timezone",
        default="Asia/Shanghai",
        help="legacy global default for offset-less timestamps",
    )
    parser.add_argument(
        "--source-timezone",
        action="append",
        default=[],
        type=_parse_source_timezone,
        metavar="SOURCE=TIMEZONE",
        help="per-source IANA zone for offset-less timestamps (repeatable)",
    )
    args = parser.parse_args()
    source_timezones = dict(args.source_timezone)
    store = EventStore(args.database)
    try:
        events = PortEventsPipeline(JsonEventSource(args.fixture), store).run(
            args.timezone, source_timezones=source_timezones
        )
        print({"events": len(events), "database": args.database})
    finally:
        store.close()


if __name__ == "__main__":
    main()
