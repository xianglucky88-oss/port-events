from __future__ import annotations
import argparse
from .connectors import JsonEventSource, SourceBatch
from .pipeline import PortEventsPipeline
from .storage import EventStore


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture", help="单一 fixture 路径（旧的一次性模式）")
    parser.add_argument(
        "--batch",
        action="append",
        default=[],
        metavar="SOURCE_ID:BATCH_ID:PATH",
        help="声明一个来源批次，可重复；与 --fixture 互斥",
    )
    parser.add_argument("--database", default="port-events.sqlite3")
    parser.add_argument("--timezone", default="Asia/Shanghai")
    args = parser.parse_args()
    store = EventStore(args.database)
    try:
        if args.batch:
            batches = []
            for spec in args.batch:
                source_id, batch_id, path = spec.split(":", 2)
                batches.append(SourceBatch(source_id, batch_id, JsonEventSource(path)))
            report = PortEventsPipeline(None, store).run_batches(batches, args.timezone)
            print({
                "batches": {f"{o.source_id}/{o.batch_id}": o.status for o in report.outcomes},
                "retryable": [f"{b.source_id}/{b.batch_id}" for b in store.retryable_batches()],
                "database": args.database,
            })
        elif args.fixture:
            events = PortEventsPipeline(JsonEventSource(args.fixture), store).run(args.timezone)
            print({"events": len(events), "database": args.database})
        else:
            parser.error("provide --fixture or at least one --batch")
    finally:
        store.close()


if __name__ == "__main__":
    main()
