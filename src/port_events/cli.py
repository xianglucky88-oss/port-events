from __future__ import annotations

import argparse
import sys

from .connectors import JsonEventSource
from .pipeline import PortEventsPipeline
from .schema import SchemaError
from .storage import EventStore


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture", required=True)
    parser.add_argument("--database", default="port-events.sqlite3")
    parser.add_argument("--timezone", default="Asia/Shanghai")
    parser.add_argument("--lock-timeout", type=float, default=5.0)
    args = parser.parse_args()
    try:
        store = EventStore(args.database, lock_timeout=args.lock_timeout)
    except SchemaError as exc:
        print(f"port-events: cannot open {args.database}: {exc}", file=sys.stderr)
        raise SystemExit(2)
    try:
        events = PortEventsPipeline(JsonEventSource(args.fixture), store).run(args.timezone)
        report = {
            "events": len(events),
            "database": args.database,
            "schema_version": store.schema_version,
        }
        quarantined = len(store.quarantined_rows())
        if quarantined:
            report["quarantined"] = quarantined
        print(report)
    finally:
        store.close()


if __name__ == "__main__":
    main()
