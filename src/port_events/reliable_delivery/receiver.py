from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Callable, Optional


@dataclass(frozen=True)
class ReceivedDelivery:
    """Durable record kept by the offline receiver."""

    delivery_id: str
    recipient: str
    event_key: str
    revision: int
    epoch: int
    payload: bytes


class _ReceiverState:
    """SQLite-backed receiver state, independent of the sender's database."""

    def __init__(self, database: str | Path, policy: Optional[Callable[[ReceivedDelivery], tuple[int, str]]]) -> None:
        self.connection = sqlite3.connect(
            str(database), check_same_thread=False
        )
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS deliveries (
                delivery_id TEXT PRIMARY KEY,
                recipient   TEXT NOT NULL,
                event_key   TEXT NOT NULL,
                revision    INTEGER NOT NULL,
                epoch       INTEGER NOT NULL,
                payload     BLOB NOT NULL,
                accepted_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS stream_cursors (
                recipient TEXT NOT NULL,
                event_key TEXT NOT NULL,
                last_revision INTEGER NOT NULL,
                PRIMARY KEY (recipient, event_key)
            );
            """
        )
        self.connection.commit()
        self._lock = threading.Lock()
        self._policy = policy
        self.request_count = 0

    def handle(
        self,
        delivery_id: str,
        recipient: str,
        event_key: str,
        revision: int,
        epoch: int,
        payload: bytes,
        simulate: str,
    ) -> tuple[int, str, str]:
        """Return (http_status, reject_kind, message)."""
        with self._lock:
            self.request_count += 1
            delivery = ReceivedDelivery(
                delivery_id=delivery_id,
                recipient=recipient,
                event_key=event_key,
                revision=revision,
                epoch=epoch,
                payload=payload,
            )
            existing = self.connection.execute(
                "SELECT delivery_id FROM deliveries WHERE delivery_id=?",
                (delivery_id,),
            ).fetchone()
            if existing is not None:
                # Idempotent re-delivery of the same identity: never apply
                # twice, never let it regress ordering.
                return (
                    200,
                    "duplicate",
                    "duplicate delivery identity acknowledged",
                )

            stale = self.connection.execute(
                "SELECT MAX(epoch) AS e FROM deliveries "
                "WHERE recipient=? AND event_key=?",
                (recipient, event_key),
            ).fetchone()
            if stale["e"] is not None and epoch < int(stale["e"]):
                return (
                    409,
                    "stale-epoch",
                    f"epoch {epoch} is older than accepted epoch {stale['e']}",
                )

            cursor = self.connection.execute(
                "SELECT last_revision FROM stream_cursors "
                "WHERE recipient=? AND event_key=?",
                (recipient, event_key),
            ).fetchone()
            if cursor is not None and revision < int(cursor["last_revision"]):
                # Receiver-side guard: the sender promises ordering; a
                # backward revision for the same stream is a permanent refusal.
                return (
                    409,
                    "revision-regression",
                    f"revision {revision} behind cursor {cursor['last_revision']}",
                )

            status, kind, message = self._decide(delivery, simulate)
            if status != 200:
                return status, kind, message

            # Only a 200 decision is made durable, and the commit happens
            # before the response is sent.
            with self.connection:
                self.connection.execute(
                    """INSERT INTO deliveries
                       (delivery_id, recipient, event_key, revision, epoch,
                        payload, accepted_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        delivery_id,
                        recipient,
                        event_key,
                        revision,
                        epoch,
                        payload,
                        self.request_count,
                    ),
                )
                self.connection.execute(
                    """INSERT INTO stream_cursors (recipient, event_key, last_revision)
                       VALUES (?, ?, ?)
                       ON CONFLICT(recipient, event_key) DO UPDATE SET
                           last_revision=MAX(last_revision, excluded.last_revision)""",
                    (recipient, event_key, revision),
                )
            return 200, "", "accepted"

    def _decide(
        self, delivery: ReceivedDelivery, simulate: str
    ) -> tuple[int, str, str]:
        if simulate == "reject":
            return 422, "permanent-rejection", "simulated permanent rejection"
        if simulate == "retry":
            return 503, "retryable", "simulated retryable failure"
        if simulate not in ("", "ack"):
            return 400, "bad-simulation", f"unknown simulate outcome {simulate!r}"
        if self._policy is not None:
            status, message = self._policy(delivery)
            if status == 200:
                return 200, "", message
            if status in (408, 425, 429) or status >= 500:
                return status, "retryable", message
            return status, "permanent-rejection", message
        return 200, "", "accepted"

    def received(self) -> list[ReceivedDelivery]:
        rows = self.connection.execute(
            "SELECT * FROM deliveries ORDER BY accepted_at"
        ).fetchall()
        return [
            ReceivedDelivery(
                delivery_id=r["delivery_id"],
                recipient=r["recipient"],
                event_key=r["event_key"],
                revision=r["revision"],
                epoch=r["epoch"],
                payload=bytes(r["payload"]),
            )
            for r in rows
        ]

    def close(self) -> None:
        self.connection.close()


class _Handler(BaseHTTPRequestHandler):
    server_version = "OfflineReliableReceiver/1.0"

    def do_POST(self) -> None:  # noqa: N802 - http.server API
        length = int(self.headers.get("Content-Length", "0") or "0")
        body = self.rfile.read(length)
        try:
            delivery_id = self.headers["X-Delivery-Id"]
            recipient = self.headers["X-Recipient"]
            event_key = self.headers["X-Event-Key"]
            revision = int(self.headers["X-Revision"])
            epoch = int(self.headers["X-Epoch"])
        except (KeyError, TypeError, ValueError):
            self._write(400, "bad-request", "missing or malformed delivery headers")
            return
        simulate = self.headers.get("X-Simulate-Outcome", "")
        status, kind, message = self.server.receiver_state.handle(
            delivery_id,
            recipient,
            event_key,
            revision,
            epoch,
            body,
            simulate,
        )
        self._write(status, kind, message)

    def do_GET(self) -> None:  # noqa: N802 - http.server API
        if self.path == "/health":
            self._write(200, "", "ok")
        else:
            self._write(404, "not-found", "unknown path")

    def _write(self, status: int, kind: str, message: str) -> None:
        data = message.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        if kind:
            self.send_header("X-Reject-Kind", kind)
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args) -> None:  # silence stderr in tests
        pass


class OfflineHttpReceiver:
    """Threaded, SQLite-backed offline HTTP receiver with durable dedup.

    Start/stop is explicit so tests can run the full HTTP stack on
    localhost without any external service.
    """

    def __init__(
        self,
        database: str | Path,
        *,
        host: str = "127.0.0.1",
        port: int = 0,
        policy: Optional[Callable[[ReceivedDelivery], tuple[int, str]]] = None,
    ) -> None:
        self.state = _ReceiverState(database, policy)
        self._httpd = ThreadingHTTPServer((host, port), _Handler)
        self._httpd.receiver_state = self.state

    @property
    def url(self) -> str:
        host, port = self._httpd.server_address[:2]
        return f"http://{host}:{port}/deliver"

    def serve_in_thread(self) -> None:
        import threading

        thread = threading.Thread(
            target=self._httpd.serve_forever, name="offline-http-receiver", daemon=True
        )
        thread.start()

    def received(self) -> list[ReceivedDelivery]:
        return self.state.received()

    @property
    def request_count(self) -> int:
        return self.state.request_count

    def close(self) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()
        self.state.close()
