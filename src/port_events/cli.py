from __future__ import annotations
import argparse
from .connectors import JsonEventSource
from .pipeline import PortEventsPipeline
from .storage import EventStore


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture", required=True)
    parser.add_argument("--database", default="port-events.sqlite3")
    parser.add_argument("--timezone", default="Asia/Shanghai")
    args = parser.parse_args()
    store = EventStore(args.database)
    try:
        events = PortEventsPipeline(JsonEventSource(args.fixture), store).run(args.timezone)
        print({"events": len(events), "database": args.database})
    finally:
        store.close()


if __name__ == "__main__":
    main()
