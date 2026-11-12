from __future__ import annotations
import argparse
import json
from dataclasses import asdict
from datetime import timedelta

from .candidates import find_node_candidates
from .connectors import JsonEventSource
from .pipeline import PortEventsPipeline
from .storage import EventStore, load_shipment_events_readonly


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture")
    parser.add_argument("--database", default="port-events.sqlite3")
    parser.add_argument("--timezone", default="Asia/Shanghai")
    parser.add_argument(
        "--candidates-report",
        action="store_true",
        help="只读输出近似节点候选报告，不采集、不写回、不合并",
    )
    parser.add_argument(
        "--candidate-window-hours",
        type=float,
        default=12.0,
        help="候选时间窗口（小时，含边界），默认 12",
    )
    args = parser.parse_args()

    if args.candidates_report:
        # 只读路径：以只读 URI 打开数据库，绝不写回。
        events = load_shipment_events_readonly(args.database)
        candidates = find_node_candidates(
            events, time_window=timedelta(hours=args.candidate_window_hours)
        )
        print(json.dumps(
            {
                "input_rows": len(events),
                "candidate_count": len(candidates),
                "candidates": [asdict(candidate) for candidate in candidates],
            },
            ensure_ascii=False,
            indent=2,
        ))
        return

    if not args.fixture:
        parser.error("--fixture is required unless --candidates-report is used")
    store = EventStore(args.database)
    try:
        events = PortEventsPipeline(JsonEventSource(args.fixture), store).run(args.timezone)
        print({"events": len(events), "database": args.database})
    finally:
        store.close()


if __name__ == "__main__":
    main()
