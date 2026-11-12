from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from port_events.reliable_delivery import (
    DeliveryStore,
    Envelope,
    HttpDeliveryAdapter,
    OfflineHttpReceiver,
    OUTCOME_ACKED,
    OUTCOME_FENCED,
    OUTCOME_REJECTED,
    ReliableDeliveryService,
    delivery_identity,
)

SRC = str(Path(__file__).resolve().parents[1] / "src")


@pytest.fixture()
def receiver(tmp_path):
    recv = OfflineHttpReceiver(tmp_path / "receiver.sqlite3")
    recv.serve_in_thread()
    try:
        yield recv
    finally:
        recv.close()


def service_with_http(tmp_path, receiver, *, simulate=None, clock=None, **kwargs):
    store = DeliveryStore(tmp_path / "sender.sqlite3")
    adapter = HttpDeliveryAdapter(receiver.url, timeout=5.0, simulate=simulate)
    service = ReliableDeliveryService(
        store, adapter, **({"clock": clock} if clock is not None else {}), **kwargs
    )
    return store, service


def envelope(revision=1, payload=b"payload", recipient="fact-db-1", key="EVT-1"):
    return Envelope(recipient, key, revision, payload)


def test_http_happy_path_persists_on_receiver_and_dedups_redelivery(
    tmp_path, receiver
):
    store, service = service_with_http(tmp_path, receiver)
    for revision in range(1, 4):
        service.accept(envelope(revision, f"r{revision}".encode()))
    assert service.run_until_idle() == 3

    received = receiver.received()
    assert [(r.recipient, r.event_key, r.revision) for r in received] == [
        ("fact-db-1", "EVT-1", 1),
        ("fact-db-1", "EVT-1", 2),
        ("fact-db-1", "EVT-1", 3),
    ]
    assert [r.payload for r in received] == [b"r1", b"r2", b"r3"]

    # Redrive with a fresh sender instance using the same stable identities.
    store.close()
    store2, service2 = service_with_http(tmp_path, receiver)
    # Force redelivery by requeuing one envelope directly at storage level.
    target = delivery_identity("fact-db-1", "EVT-1", 2)
    with store2.connection:
        store2.connection.execute(
            "UPDATE envelopes SET state='queued', owner=NULL, lease_expires=NULL "
            "WHERE delivery_id=?",
            (target,),
        )
    service2.run_until_idle()
    received = receiver.received()
    assert len(received) == 3  # redelivery was deduplicated, never applied twice
    assert receiver.request_count == 4  # but the HTTP attempt really happened
    store2.close()


def test_http_retryable_failure_then_lost_receipt_redelivers_same_identity(
    tmp_path, receiver
):
    calls = {"n": 0}

    def simulate(request):
        calls["n"] += 1
        if calls["n"] == 1:
            return "retry"  # receiver answers 503
        return "ack"

    clock = {"t": 500.0}
    store, service = service_with_http(
        tmp_path,
        receiver,
        simulate=simulate,
        base_backoff=10.0,
        max_backoff=10.0,
        clock=lambda: clock["t"],
    )
    accepted = service.accept(envelope())
    service.run_until_idle()
    assert service.envelope(accepted.delivery_id).state == "retryable"
    # Backoff window not elapsed: no second HTTP attempt yet.
    assert receiver.request_count == 1
    clock["t"] = 511.0
    service.run_until_idle()
    assert service.envelope(accepted.delivery_id).state == "delivered"
    ids = [r.delivery_id for r in receiver.received()]
    assert ids == [accepted.delivery_id]
    assert calls["n"] == 2
    store.close()


def test_http_permanent_rejection_blocks_and_retry_releases(tmp_path, receiver):
    def simulate(request):
        return "reject" if request.revision == 1 else "ack"

    store, service = service_with_http(tmp_path, receiver, simulate=simulate)
    r1 = service.accept(envelope(revision=1))
    r2 = service.accept(envelope(revision=2, payload=b"r2"))
    service.run_until_idle()
    record = service.envelope(r1.delivery_id)
    assert record.state == "rejected"
    assert "permanent" in record.reject_reason
    assert receiver.received() == []  # nothing durable on the receiver
    assert service.dispatch_once() is None  # rev 2 blocked

    def simulate2(request):
        return "ack"

    service.adapter = HttpDeliveryAdapter(receiver.url, simulate=simulate2)
    service.retry_rejected(r1.delivery_id)
    service.run_until_idle()
    assert service.envelope(r1.delivery_id).state == "delivered"
    assert service.envelope(r2.delivery_id).state == "delivered"
    assert [r.revision for r in receiver.received()] == [1, 2]
    store.close()


def test_http_stale_epoch_is_fenced_and_new_leader_redelivers(tmp_path, receiver):
    store = DeliveryStore(tmp_path / "sender.sqlite3")
    old_adapter = HttpDeliveryAdapter(receiver.url)
    old = ReliableDeliveryService(store, old_adapter, owner="old")
    accepted = old.accept(envelope())
    # Old sender claims rev 1 under epoch 0 but never gets to finish.
    store.claim_due(0.0, 30.0, "old")
    old.takeover()  # epoch -> 1

    new_adapter = HttpDeliveryAdapter(receiver.url)
    new = ReliableDeliveryService(store, new_adapter, owner="new")
    new.run_until_idle()
    assert store.get_envelope(accepted.delivery_id).state == "delivered"
    assert receiver.received()[0].epoch == 1

    # Replaying the old sender's exact request: the identity was already
    # durably accepted, so idempotent dedup wins and returns an ack that the
    # successor must simply ignore if it ever arrives late.
    from port_events.reliable_delivery import DeliveryRequest

    stale_request = DeliveryRequest(
        delivery_id=accepted.delivery_id,
        recipient="fact-db-1",
        event_key="EVT-1",
        revision=1,
        payload=b"payload",
        attempt=1,
        epoch=0,
    )
    duplicate = old_adapter.send(stale_request)
    assert duplicate.kind == OUTCOME_ACKED
    assert len(receiver.received()) == 1

    # A genuinely different identity carrying the stale epoch is fenced,
    # because the stream already accepted deliveries under epoch 1.
    stale_other = DeliveryRequest(
        delivery_id="00000000-0000-0000-0000-0000000000aa",
        recipient="fact-db-1",
        event_key="EVT-1",
        revision=1,
        payload=b"stale",
        attempt=1,
        epoch=0,
    )
    outcome = old_adapter.send(stale_other)
    assert outcome.kind == OUTCOME_FENCED
    # And no duplicate application occurred on the receiver.
    assert len(receiver.received()) == 1
    store.close()


def test_http_receiver_rejects_revision_regression(tmp_path, receiver):
    store = DeliveryStore(tmp_path / "sender.sqlite3")
    adapter = HttpDeliveryAdapter(receiver.url)
    service = ReliableDeliveryService(store, adapter)
    service.accept(envelope(revision=1))
    service.accept(envelope(revision=2, payload=b"r2"))
    service.run_until_idle()
    assert [r.revision for r in receiver.received()] == [1, 2]

    # A fabricated lower revision for the same stream is a permanent refusal.
    from port_events.reliable_delivery import DeliveryRequest

    forged = DeliveryRequest(
        delivery_id=delivery_identity("fact-db-1", "EVT-1", 1),
        recipient="fact-db-1",
        event_key="EVT-1",
        revision=1,
        payload=b"old-bytes",
        attempt=9,
        epoch=5,
    )
    # Same identity -> idempotent duplicate, never applied twice.
    duplicate = adapter.send(forged)
    assert duplicate.kind == OUTCOME_ACKED
    assert len(receiver.received()) == 2

    # A genuinely different identity that moves the revision backwards refuses.
    forged_back = DeliveryRequest(
        delivery_id="00000000-0000-0000-0000-000000000001",
        recipient="fact-db-1",
        event_key="EVT-1",
        revision=1,
        payload=b"regression",
        attempt=1,
        epoch=6,
    )
    outcome = adapter.send(forged_back)
    assert outcome.kind == OUTCOME_REJECTED
    assert "behind cursor" in outcome.reason
    assert len(receiver.received()) == 2
    store.close()


def test_http_aba_across_restart_with_real_subprocess_crash(tmp_path):
    """End-to-end: process crash after bytes are sent but before local ack."""
    db = tmp_path / "sender.sqlite3"
    receiver_db = tmp_path / "receiver.sqlite3"
    receiver = OfflineHttpReceiver(receiver_db)
    receiver.serve_in_thread()
    port = receiver._httpd.server_address[1]

    worker = textwrap.dedent(
        f"""
        import sys
        sys.path.insert(0, {SRC!r})
        from port_events.reliable_delivery import (
            DeliveryStore, HttpDeliveryAdapter,
            ReliableDeliveryService, Envelope, DeliveryCrash,
        )

        class CrashAdapter(HttpDeliveryAdapter):
            def send(self, request):
                outcome = super().send(request)
                if outcome.kind == "acked":
                    raise SystemExit(7)  # hard crash before local record_outcome
                return outcome

        store = DeliveryStore({str(db)!r})
        adapter = CrashAdapter("http://127.0.0.1:{port}/deliver", timeout=5.0)
        service = ReliableDeliveryService(store, adapter, owner="crashed")
        service.accept(Envelope("fact-db-1", "EVT-1", 1, b"crash-payload"))
        try:
            service.dispatch_once()
        except SystemExit:
            raise
        """
    )
    env = dict(os.environ, PYTHONPATH=SRC)
    completed = subprocess.run(
        [sys.executable, "-c", worker],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert completed.returncode == 7, completed.stderr

    # Receiver has the durable application exactly once.
    assert [(r.delivery_id, r.payload) for r in receiver.received()] == [
        (delivery_identity("fact-db-1", "EVT-1", 1), b"crash-payload")
    ]

    # Sender database: envelope still leased to the dead process.
    conn = sqlite3.connect(db)
    row = conn.execute(
        "SELECT state, owner FROM envelopes WHERE delivery_id=?",
        (delivery_identity("fact-db-1", "EVT-1", 1),),
    ).fetchone()
    conn.close()
    assert row == ("sending", "crashed")

    # Successor takes over and redelivers under the same stable identity;
    # the receiver deduplicates so it is still applied exactly once.
    store2 = DeliveryStore(db)
    successor = ReliableDeliveryService(
        store2,
        HttpDeliveryAdapter(receiver.url),
        owner="successor",
        clock=lambda: 10**9,
    )
    new_epoch = successor.takeover()
    assert new_epoch == 1
    successor.run_until_idle()
    target = delivery_identity("fact-db-1", "EVT-1", 1)
    assert store2.get_envelope(target).state == "delivered"
    assert len(receiver.received()) == 1
    assert receiver.request_count == 2  # initial + redelivery attempt
    store2.close()
    receiver.close()


def test_http_network_outage_is_retryable_and_resumes_after_recovery(
    tmp_path, receiver
):
    store = DeliveryStore(tmp_path / "sender.sqlite3")
    # Point first at a closed port: every attempt must classify as retryable.
    dead = HttpDeliveryAdapter("http://127.0.0.1:1/deliver", timeout=1.0)
    service = ReliableDeliveryService(
        store, dead, base_backoff=0.0, max_backoff=0.0
    )
    accepted = service.accept(envelope())
    service.dispatch_once()
    record = service.envelope(accepted.delivery_id)
    assert record.state == "retryable"
    assert record.attempt == 1

    service.adapter = HttpDeliveryAdapter(receiver.url)
    service.run_until_idle()
    assert service.envelope(accepted.delivery_id).state == "delivered"
    assert len(receiver.received()) == 1
    store.close()


def test_http_two_independent_databases_never_share_progress(
    tmp_path, receiver
):
    db_a = tmp_path / "a.sqlite3"
    db_b = tmp_path / "b.sqlite3"
    sa = ReliableDeliveryService(
        DeliveryStore(db_a), HttpDeliveryAdapter(receiver.url)
    )
    sb = ReliableDeliveryService(
        DeliveryStore(db_b), HttpDeliveryAdapter(receiver.url)
    )
    sa.accept(envelope(revision=1, payload=b"from-a"))
    sb.accept(envelope(revision=1, payload=b"from-b"))
    sa.run_until_idle()
    sb.run_until_idle()
    # Each sender has its own durable progress even though identities overlap.
    assert sa.progress("fact-db-1", "EVT-1").high_water_mark == 1
    assert sb.progress("fact-db-1", "EVT-1").high_water_mark == 1
    # Receiver dedups on the shared stable identity: one durable application.
    assert len(receiver.received()) == 1
    assert receiver.received()[0].payload == b"from-a"
    sa.store.close()
    sb.store.close()
