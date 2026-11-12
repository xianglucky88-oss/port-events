from __future__ import annotations

import threading

import pytest

from port_events.reliable_delivery import (
    Acceptance,
    DeliveryCrash,
    DeliveryOutcome,
    DeliveryRequest,
    DeliveryStore,
    Envelope,
    InvalidEnvelopeError,
    OUTCOME_ACKED,
    OUTCOME_FENCED,
    OUTCOME_REJECTED,
    OUTCOME_RETRYABLE,
    RecordingAdapter,
    ReliableDeliveryService,
    RevisionConflictError,
    STATE_DELIVERED,
    STATE_QUEUED,
    STATE_REJECTED,
    STATE_RETRYABLE,
    STATE_SENDING,
    delivery_identity,
)


def make_service(tmp_path, adapter=None, **kwargs):
    store = DeliveryStore(tmp_path / "delivery.sqlite3")
    service = ReliableDeliveryService(store, adapter or RecordingAdapter(), **kwargs)
    return store, service


def env(recipient="fact-db-1", event_key="EVT-1", revision=1, payload=b"v1"):
    return Envelope(recipient, event_key, revision, payload)


# --------------------------------------------------------------- validation
def test_accept_requires_immutable_envelope_shape(tmp_path):
    store, service = make_service(tmp_path)
    with pytest.raises(InvalidEnvelopeError):
        service.accept(Envelope("", "EVT-1", 1, b"x"))
    with pytest.raises(InvalidEnvelopeError):
        service.accept(Envelope("r", "", 1, b"x"))
    with pytest.raises(InvalidEnvelopeError):
        service.accept(Envelope("r", "k", 0, b"x"))
    with pytest.raises(InvalidEnvelopeError):
        service.accept(Envelope("r", "k", -1, b"x"))
    with pytest.raises(InvalidEnvelopeError):
        service.accept(Envelope("r", "k", 1, "not bytes"))  # type: ignore[arg-type]
    store.close()


# -------------------------------------------------------------- missing gap
def test_missing_predecessor_blocks_later_revisions(tmp_path):
    store, service = make_service(tmp_path)
    # Revisions 1 and 3 accepted; revision 2 missing.
    service.accept(env(revision=1, payload=b"r1"))
    service.accept(env(revision=3, payload=b"r3"))

    assert service.run_until_idle() == 1  # only revision 1 can leave
    assert adapter_of(service).identities() == [
        delivery_identity("fact-db-1", "EVT-1", 1)
    ]
    progress = service.progress("fact-db-1", "EVT-1")
    assert progress.high_water_mark == 1
    assert progress.blocked_reason == "missing revision 2"

    # Filling the gap unblocks revision 3 (which is then the next due one).
    service.accept(env(revision=2, payload=b"r2"))
    assert service.run_until_idle() == 2
    assert service.progress("fact-db-1", "EVT-1").high_water_mark == 3
    store.close()


def test_independent_streams_are_not_blocked_by_each_other(tmp_path):
    store, service = make_service(tmp_path)
    service.accept(env(recipient="A", event_key="X", revision=2, payload=b"x2"))
    service.accept(env(recipient="A", event_key="Y", revision=1, payload=b"y1"))
    service.accept(env(recipient="B", event_key="X", revision=1, payload=b"b1"))
    assert service.run_until_idle() == 2  # A/Y rev1 and B/X rev1; A/X blocked
    assert service.progress("A", "X").blocked_reason == "missing revision 1"
    assert service.progress("A", "Y").high_water_mark == 1
    assert service.progress("B", "X").high_water_mark == 1
    store.close()


# ------------------------------------------------------------ idempotency
def test_resubmission_returns_original_identity_and_does_not_resend(tmp_path):
    store, service = make_service(tmp_path)
    first = service.accept(env(payload=b"hello"))
    again = service.accept(env(payload=b"hello"))
    assert isinstance(first, Acceptance)
    assert first.is_duplicate is False
    assert again.is_duplicate is True
    assert again.delivery_id == first.delivery_id == delivery_identity(
        "fact-db-1", "EVT-1", 1
    )
    service.run_until_idle()
    assert len(adapter_of(service).sent) == 1

    # Re-submitting after delivery stays idempotent and adds no new attempt.
    third = service.accept(env(payload=b"hello"))
    assert third.is_duplicate is True
    assert third.state == STATE_DELIVERED
    service.run_until_idle()
    assert len(adapter_of(service).sent) == 1
    store.close()


def test_same_revision_with_different_bytes_is_rejected(tmp_path):
    store, service = make_service(tmp_path)
    service.accept(env(revision=2, payload=b"original"))
    with pytest.raises(RevisionConflictError):
        service.accept(env(revision=2, payload=b"tampered"))
    # The original bytes are untouched.
    record = service.envelope(delivery_identity("fact-db-1", "EVT-1", 2))
    assert record.payload == b"original"
    store.close()


def test_opaque_payload_is_stored_and_forwarded_byte_for_byte(tmp_path):
    store, service = make_service(tmp_path)
    weird = bytes([0x00, 0xFF, 0xFE, 0x80]) + "船期".encode("utf-8")
    service.accept(env(payload=weird))
    service.run_until_idle()
    request = adapter_of(service).sent[0]
    assert request.payload == weird
    assert service.envelope(request.delivery_id).payload == weird
    store.close()


def test_equal_payloads_at_different_revisions_are_still_separate_sends(tmp_path):
    store, service = make_service(tmp_path)
    service.accept(env(revision=1, payload=b"same"))
    service.accept(env(revision=2, payload=b"same"))
    service.run_until_idle()
    identities = adapter_of(service).identities()
    assert len(identities) == 2
    assert len(set(identities)) == 2
    assert all(request.payload == b"same" for request in adapter_of(service).sent)
    store.close()


# ------------------------------------------------------------- lost receipt
def test_lost_receipt_redispatches_with_stable_identity(tmp_path):
    outcomes = [
        DeliveryOutcome(OUTCOME_RETRYABLE, reason="receipt lost"),
        DeliveryOutcome(OUTCOME_ACKED),
    ]
    adapter = RecordingAdapter(lambda request: outcomes.pop(0))
    clock = {"t": 1000.0}
    store = DeliveryStore(tmp_path / "delivery.sqlite3")
    service = ReliableDeliveryService(
        store,
        adapter,
        base_backoff=10.0,
        max_backoff=10.0,
        clock=lambda: clock["t"],
    )
    accepted = service.accept(env())
    assert service.run_until_idle() == 1  # first attempt only: backoff pending
    record = service.envelope(accepted.delivery_id)
    assert record.state == STATE_RETRYABLE
    assert record.attempt == 1
    assert record.not_before == 1010.0
    # Still inside the backoff window: nothing is claimed.
    assert service.dispatch_once() is None
    assert len(adapter.sent) == 1
    # After the window elapses, the same identity is redelivered.
    clock["t"] = 1011.0
    service.run_until_idle()
    assert [request.delivery_id for request in adapter.sent] == [
        accepted.delivery_id,
        accepted.delivery_id,
    ]
    assert adapter.sent[1].attempt == 2
    assert service.envelope(accepted.delivery_id).state == STATE_DELIVERED
    assert store.attempt_count(accepted.delivery_id) == 2
    store.close()


# ----------------------------------------------------------------- crashes
def test_crash_before_persisted_accept_leaves_nothing_behind(tmp_path):
    db = tmp_path / "delivery.sqlite3"
    store = DeliveryStore(db)
    # Simulate a crash between intent-to-accept and the durable commit:
    # the INSERT is issued inside an explicit transaction that then rolls back.
    store.connection.execute("BEGIN")
    store.connection.execute(
        "INSERT OR IGNORE INTO streams (recipient, event_key) VALUES (?, ?)",
        ("fact-db-1", "EVT-1"),
    )
    store.connection.execute(
        """INSERT INTO envelopes
           (delivery_id, recipient, event_key, revision, payload,
            state, attempt, not_before, created_epoch)
           VALUES (?, ?, ?, ?, ?, 'queued', 0, 0, 0)""",
        (delivery_identity("fact-db-1", "EVT-1", 1), "fact-db-1", "EVT-1", 1, b"v1"),
    )
    store.connection.rollback()  # process vanished before commit
    store.close()

    fresh = DeliveryStore(db)
    assert fresh.progress("fact-db-1", "EVT-1").high_water_mark == 0
    # Re-accept on a brand new store connection works and is not a duplicate.
    service = ReliableDeliveryService(fresh, RecordingAdapter())
    accepted = service.accept(env())
    assert accepted.is_duplicate is False
    fresh.close()


def test_crash_after_send_before_ack_redelivers_same_identity(tmp_path):
    delivery_id = delivery_identity("fact-db-1", "EVT-1", 1)

    class CrashOnceAdapter(RecordingAdapter):
        def __init__(self):
            super().__init__()
            self.crashed = False

        def send(self, request: DeliveryRequest) -> DeliveryOutcome:
            self.sent.append(request)
            if not self.crashed:
                self.crashed = True
                raise DeliveryCrash("process vanished after transport accepted bytes")
            return DeliveryOutcome(OUTCOME_ACKED)

    adapter = CrashOnceAdapter()
    store = DeliveryStore(tmp_path / "delivery.sqlite3")
    service = ReliableDeliveryService(store, adapter, lease_seconds=30.0)
    service.accept(env())
    with pytest.raises(DeliveryCrash):
        service.dispatch_once()
    assert service.envelope(delivery_id).state == STATE_SENDING

    # A new service instance takes over after the old lease is considered dead.
    store2 = DeliveryStore(tmp_path / "delivery.sqlite3")
    successor = ReliableDeliveryService(
        store2,
        RecordingAdapter(),
        lease_seconds=30.0,
        clock=lambda: 10**9,
    )
    reclaimed = successor.recover_expired_leases()
    assert reclaimed == 1
    successor.run_until_idle()
    redelivered = adapter_of(successor).sent
    assert len(redelivered) == 1
    assert redelivered[0].delivery_id == delivery_id
    assert store2.get_envelope(delivery_id).state == STATE_DELIVERED
    store.close()
    store2.close()


# --------------------------------------------------------------- takeover
def test_takeover_bumps_epoch_and_late_receipt_cannot_advance(tmp_path):
    store = DeliveryStore(tmp_path / "delivery.sqlite3")
    old = ReliableDeliveryService(store, RecordingAdapter(), owner="old-owner")
    accepted = old.accept(env())
    request = store.claim_due(0.0, 30.0, "old-owner")
    assert request is not None and request.epoch == 0

    epoch = old.takeover()
    assert epoch == 1
    assert store.get_envelope(accepted.delivery_id).state == STATE_QUEUED

    # The old owner's late receipt for its in-flight envelope is rejected.
    late = DeliveryOutcome(OUTCOME_ACKED)
    result = store.record_outcome(
        accepted.delivery_id, "old-owner", late, 1.0, 1.0, 60.0
    )
    assert result == "stale"
    assert store.get_envelope(accepted.delivery_id).state == STATE_QUEUED

    # A brand new owner drives it under the new epoch.
    new_service = ReliableDeliveryService(store, RecordingAdapter(), owner="new-owner")
    new_service.run_until_idle()
    assert store.get_envelope(accepted.delivery_id).state == STATE_DELIVERED
    new_request = adapter_of(new_service).sent[0]
    assert new_request.epoch == 1
    store.close()


def test_two_senders_concurrent_only_the_leader_delivers(tmp_path):
    store = DeliveryStore(tmp_path / "delivery.sqlite3")
    leader = ReliableDeliveryService(store, RecordingAdapter(), owner="leader")
    for revision in range(1, 4):
        leader.accept(env(revision=revision, payload=f"r{revision}".encode()))
    leader.run_until_idle()
    assert len(adapter_of(leader).sent) == 3

    # A stale second sender tries to claim with its own owner id; the store
    # serializes claims, so it can never double-deliver a delivered revision.
    stale = ReliableDeliveryService(store, RecordingAdapter(), owner="stale")
    assert stale.dispatch_once() is None
    assert adapter_of(stale).sent == []
    store.close()


def test_takeover_during_inflight_redrives_without_double_ack(tmp_path):
    # Receiver side counts applications: duplicate identities must not be
    # applied twice even across an epoch change.
    applied: dict[str, int] = {}

    class ApplyingAdapter(RecordingAdapter):
        def send(self, request):
            self.sent.append(request)
            applied[request.delivery_id] = applied.get(request.delivery_id, 0) + 1
            return DeliveryOutcome(OUTCOME_ACKED)

    first_adapter = ApplyingAdapter()
    store = DeliveryStore(tmp_path / "delivery.sqlite3")
    old = ReliableDeliveryService(store, first_adapter, owner="old")
    accepted = old.accept(env())
    old.dispatch_once()  # delivered under epoch 0

    # Now an envelope 2 gets stuck in flight on the old sender.
    old.accept(env(revision=2, payload=b"r2"))
    claimed = store.claim_due(0.0, 30.0, "old")
    assert claimed is not None
    old.takeover()  # new leader takes over

    next_adapter = ApplyingAdapter()
    new = ReliableDeliveryService(store, next_adapter, owner="new")
    new.run_until_idle()
    # Rev 1 never resent; rev 2 delivered exactly once under the new epoch.
    assert [r.delivery_id for r in next_adapter.sent] == [
        delivery_identity("fact-db-1", "EVT-1", 2)
    ]
    assert applied[delivery_identity("fact-db-1", "EVT-1", 1)] == 1
    store.close()


# --------------------------------------------------------------- rejection
def test_permanent_rejection_stores_reason_and_blocks_successors(tmp_path):
    outcomes = [
        DeliveryOutcome(OUTCOME_REJECTED, reason="schema mismatch: missing field"),
    ]
    adapter = RecordingAdapter(lambda request: outcomes.pop(0) if outcomes
                               else DeliveryOutcome(OUTCOME_ACKED))
    store, service = make_service(tmp_path, adapter=adapter)
    r1 = service.accept(env(revision=1, payload=b"r1"))
    service.accept(env(revision=2, payload=b"r2"))
    service.run_until_idle()
    record = service.envelope(r1.delivery_id)
    assert record.state == STATE_REJECTED
    assert "schema mismatch" in record.reject_reason
    # Rev 2 remains blocked behind the rejected predecessor.
    assert service.dispatch_once() is None
    progress = service.progress("fact-db-1", "EVT-1")
    assert progress.high_water_mark == 0
    assert "rejected" in progress.blocked_reason
    store.close()


def test_explicit_retry_of_rejection_releases_the_stream(tmp_path):
    sequence = [
        DeliveryOutcome(OUTCOME_REJECTED, reason="nope"),
        DeliveryOutcome(OUTCOME_ACKED),
        DeliveryOutcome(OUTCOME_ACKED),
    ]
    adapter = RecordingAdapter(lambda request: sequence.pop(0))
    store, service = make_service(tmp_path, adapter=adapter)
    r1 = service.accept(env(revision=1))
    r2_id = service.accept(env(revision=2)).delivery_id
    service.run_until_idle()
    with pytest.raises(RevisionConflictError):
        service.retry_rejected(r2_id)  # not itself rejected
    service.retry_rejected(r1.delivery_id)
    assert service.envelope(r1.delivery_id).state == STATE_QUEUED
    service.run_until_idle()
    assert service.envelope(r1.delivery_id).state == STATE_DELIVERED
    assert service.envelope(r2_id).state == STATE_DELIVERED
    store.close()


def test_abandon_unblocks_successors_and_keeps_reason(tmp_path):
    adapter = RecordingAdapter(
        lambda request: DeliveryOutcome(OUTCOME_REJECTED, reason="poison")
        if request.revision == 1
        else DeliveryOutcome(OUTCOME_ACKED)
    )
    store, service = make_service(tmp_path, adapter=adapter)
    r1 = service.accept(env(revision=1))
    r2 = service.accept(env(revision=2))
    service.run_until_idle()
    with pytest.raises(InvalidEnvelopeError):
        service.abandon(r1.delivery_id, "")
    service.abandon(r1.delivery_id, "business decided to skip revision 1")
    record = service.envelope(r1.delivery_id)
    assert record.abandoned_reason.startswith("business decided")
    service.run_until_idle()
    assert service.envelope(r2.delivery_id).state == STATE_DELIVERED
    assert service.progress("fact-db-1", "EVT-1").high_water_mark == 2
    store.close()


# -------------------------------------------------------------------- A-B-A
def test_aba_late_receipts_and_old_requests_never_regress(tmp_path):
    store = DeliveryStore(tmp_path / "delivery.sqlite3")
    old = ReliableDeliveryService(store, RecordingAdapter(), owner="old")
    accepted = old.accept(env())
    old_request = store.claim_due(0.0, 30.0, "old")
    assert old_request.epoch == 0

    # A-B-A: old loses leadership, new takes over, then old's transport answers.
    old.takeover()
    new = ReliableDeliveryService(store, RecordingAdapter(), owner="new")
    new_request = store.claim_due(0.0, 30.0, "new")
    assert new_request is not None and new_request.epoch == 1
    store.record_outcome(
        new_request.delivery_id, "new", DeliveryOutcome(OUTCOME_ACKED), 1.0, 1.0, 60.0
    )
    # The old request's receipts arrive twice (the "A" again); both are stale.
    for _ in range(2):
        assert store.record_outcome(
            accepted.delivery_id,
            "old",
            DeliveryOutcome(OUTCOME_ACKED),
            2.0,
            1.0,
            60.0,
        ) == "stale"
        assert store.record_outcome(
            accepted.delivery_id,
            "old",
            DeliveryOutcome(OUTCOME_RETRYABLE, reason="late"),
            2.0,
            1.0,
            60.0,
        ) == "stale"
    assert store.get_envelope(accepted.delivery_id).state == STATE_DELIVERED
    assert store.get_epoch() == 1
    # Only the new owner's ack is present in the durable attempt log.
    kinds = [
        row["kind"]
        for row in store.connection.execute(
            "SELECT kind FROM attempts_log WHERE delivery_id=?",
            (accepted.delivery_id,),
        )
    ]
    assert kinds == ["acked"]
    store.close()


def test_fenced_outcome_requeues_for_current_leader(tmp_path):
    store, service = make_service(tmp_path)
    accepted = service.accept(env())
    adapter = RecordingAdapter(
        lambda request: DeliveryOutcome(OUTCOME_FENCED, reason="epoch behind")
    )
    service.adapter = adapter
    service.dispatch_once()
    assert service.envelope(accepted.delivery_id).state == STATE_QUEUED
    store.close()


def test_takeover_and_lease_recovery_refresh_stream_progress(tmp_path):
    store = DeliveryStore(tmp_path / "delivery.sqlite3")
    service = ReliableDeliveryService(
        store, RecordingAdapter(), owner="old", clock=lambda: 0.0
    )
    service.accept(env(revision=1))
    service.accept(env(revision=2, payload=b"r2"))
    # Rev 1 gets stuck in flight on the dying sender.
    stuck = store.claim_due(0.0, 30.0, "old")
    assert stuck is not None
    progress = service.progress("fact-db-1", "EVT-1")
    assert progress.blocked_reason == "revision 1 pending (sending)"

    # Takeover must recompute the stream view instead of leaving it stale.
    service.takeover()
    progress = service.progress("fact-db-1", "EVT-1")
    assert progress.blocked_reason == "revision 1 pending (queued)"

    # Same guarantee when a lease simply expires rather than an explicit takeover.
    stuck = store.claim_due(0.0, 30.0, "old")
    assert stuck is not None
    fresh = ReliableDeliveryService(
        store, RecordingAdapter(), owner="new", clock=lambda: 100.0
    )
    assert fresh.recover_expired_leases() == 1
    progress = fresh.progress("fact-db-1", "EVT-1")
    assert progress.blocked_reason == "revision 1 pending (queued)"
    fresh.run_until_idle()
    assert fresh.progress("fact-db-1", "EVT-1").high_water_mark == 2
    store.close()


# ----------------------------------------------------------- no source scan
def test_component_uses_independent_database_and_never_imports_event_store(tmp_path):
    # The delivery database is a separate file from any event database.
    event_db = tmp_path / "events.sqlite3"
    delivery_db = tmp_path / "delivery.sqlite3"
    event_db.write_bytes(b"")  # simulate an existing unrelated file
    store = DeliveryStore(delivery_db)
    service = ReliableDeliveryService(store, RecordingAdapter())
    service.accept(env())
    service.run_until_idle()
    store.close()
    # The event database is untouched: still empty, no tables added.
    import sqlite3

    conn = sqlite3.connect(event_db)
    try:
        assert conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall() == []
    finally:
        conn.close()

    # Package boundary: reliable_delivery must not depend on event modules.
    import inspect

    import port_events.reliable_delivery as rd
    source_files = inspect.getfile(rd)
    package_dir = source_files.rsplit("/", 1)[0]
    for path in __import__("pathlib").Path(package_dir).glob("*.py"):
        text = path.read_text(encoding="utf-8")
        assert "port_events.storage" not in text
        assert "port_events.models" not in text
        assert "EventStore" not in text
        assert "ShipmentEvent" not in text


def test_persistence_restarts_with_fresh_service_and_keeps_identity(tmp_path):
    db = tmp_path / "delivery.sqlite3"
    store = DeliveryStore(db)
    service = ReliableDeliveryService(store, RecordingAdapter())
    accepted = service.accept(env())
    service.run_until_idle()
    store.close()

    restarted_store = DeliveryStore(db)
    restarted = ReliableDeliveryService(restarted_store, RecordingAdapter())
    duplicate = restarted.accept(env())
    assert duplicate.is_duplicate is True
    assert duplicate.delivery_id == accepted.delivery_id
    assert restarted.progress("fact-db-1", "EVT-1").high_water_mark == 1
    assert restarted.dispatch_once() is None  # nothing left to send
    restarted_store.close()


# ------------------------------------------------------------- concurrency
def test_parallel_claims_are_serialized_and_ordered(tmp_path):
    store = DeliveryStore(tmp_path / "delivery.sqlite3")
    service = ReliableDeliveryService(store, RecordingAdapter())
    for revision in range(1, 6):
        service.accept(env(revision=revision, payload=f"p{revision}".encode()))
    claimed: list[str] = []
    barrier = threading.Barrier(4)

    def worker(owner: str) -> None:
        barrier.wait()
        for _ in range(5):
            request = store.claim_due(0.0, 30.0, owner)
            if request is None:
                return
            claimed.append(request.delivery_id)
            store.record_outcome(
                request.delivery_id,
                owner,
                DeliveryOutcome(OUTCOME_ACKED),
                0.0,
                1.0,
                60.0,
            )

    threads = [threading.Thread(target=worker, args=(f"w{i}",)) for i in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert len(claimed) == 5
    revisions = [
        store.get_envelope(delivery_id).revision for delivery_id in claimed
    ]
    assert revisions == [1, 2, 3, 4, 5]
    store.close()


def adapter_of(service: ReliableDeliveryService) -> RecordingAdapter:
    adapter = service.adapter
    assert isinstance(adapter, RecordingAdapter)
    return adapter
