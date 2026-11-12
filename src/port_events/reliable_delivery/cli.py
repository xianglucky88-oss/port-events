from __future__ import annotations

import argparse
import json
from pathlib import Path

from .adapters import HttpDeliveryAdapter, RecordingAdapter
from .models import Envelope
from .receiver import OfflineHttpReceiver
from .service import ReliableDeliveryService
from .store import DeliveryStore


def _add_accept_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--recipient", required=True)
    parser.add_argument("--event-key", required=True)
    parser.add_argument("--revision", required=True, type=int)
    parser.add_argument(
        "--payload-file",
        help="file read as opaque payload bytes; defaults to UTF-8 of --payload",
    )
    parser.add_argument("--payload", default="")


def _payload(args: argparse.Namespace) -> bytes:
    if args.payload_file:
        return Path(args.payload_file).read_bytes()
    return args.payload.encode("utf-8")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="reliable-delivery",
        description="Ordered, at-least-once revision delivery with its own database",
    )
    parser.add_argument("--database", required=True, help="delivery sqlite database")
    sub = parser.add_subparsers(dest="command", required=True)

    receive = sub.add_parser("receive", help="run the offline HTTP receiver")
    receive.add_argument("--host", default="127.0.0.1")
    receive.add_argument("--port", type=int, default=0)
    receive.add_argument("--receiver-database", required=True)

    accept = sub.add_parser("accept", help="durably accept one immutable envelope")
    _add_accept_arguments(accept)

    send = sub.add_parser("send", help="drive due envelopes until idle")
    send.add_argument("--receiver-url", help="HTTP receiver; offline recorder if omitted")
    send.add_argument("--owner")
    send.add_argument("--lease-seconds", type=float, default=30.0)

    retry = sub.add_parser("retry-rejected", help="explicitly retry a rejected envelope")
    retry.add_argument("--delivery-id", required=True)

    abandon = sub.add_parser("abandon", help="abandon a rejected envelope")
    abandon.add_argument("--delivery-id", required=True)
    abandon.add_argument("--reason", required=True)

    progress = sub.add_parser("progress", help="show durable progress for a stream")
    progress.add_argument("--recipient", required=True)
    progress.add_argument("--event-key", required=True)

    args = parser.parse_args(argv)

    if args.command == "receive":
        receiver = OfflineHttpReceiver(
            args.receiver_database, host=args.host, port=args.port
        )
        receiver.serve_in_thread()
        print({"receiver_url": receiver.url, "database": args.receiver_database})
        try:
            import time

            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            receiver.close()
        return

    store = DeliveryStore(args.database)
    try:
        if args.command == "accept":
            acceptance = ReliableDeliveryService(
                store, RecordingAdapter()
            ).accept(
                Envelope(
                    args.recipient,
                    args.event_key,
                    args.revision,
                    _payload(args),
                )
            )
            print(json.dumps(acceptance.__dict__, ensure_ascii=False))
            return

        if args.command == "send":
            adapter = (
                HttpDeliveryAdapter(args.receiver_url)
                if args.receiver_url
                else RecordingAdapter()
            )
            service = ReliableDeliveryService(
                store,
                adapter,
                **({"owner": args.owner} if args.owner else {}),
                lease_seconds=args.lease_seconds,
            )
            dispatched = service.run_until_idle()
            if isinstance(adapter, RecordingAdapter):
                print({"dispatched": dispatched, "identities": adapter.identities()})
            else:
                print({"dispatched": dispatched})
            return

        if args.command == "retry-rejected":
            store.retry_rejected(args.delivery_id)
            print({"retried": args.delivery_id})
            return

        if args.command == "abandon":
            store.abandon(args.delivery_id, args.reason)
            print({"abandoned": args.delivery_id, "reason": args.reason})
            return

        if args.command == "progress":
            view = store.progress(args.recipient, args.event_key)
            print(json.dumps(view.__dict__, ensure_ascii=False))
    finally:
        store.close()


if __name__ == "__main__":
    main()
