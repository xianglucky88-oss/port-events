from __future__ import annotations

import time
import uuid
from typing import Callable, Optional

from .adapters import DeliveryCrash
from .models import (
    OUTCOME_FENCED,
    Acceptance,
    DeliveryAdapter,
    InvalidEnvelopeError,
    Progress,
    Envelope,
)
from .store import DeliveryStore


class ReliableDeliveryService:
    """Accepts immutable envelopes and delivers each stream at-least-once.

    Ordering is maintained per (recipient, event_key).  Acceptance is only
    acknowledged after the envelope is durably stored; nothing is ever
    discovered by scanning the event database.
    """

    def __init__(
        self,
        store: DeliveryStore,
        adapter: DeliveryAdapter,
        *,
        owner: Optional[str] = None,
        lease_seconds: float = 30.0,
        base_backoff: float = 1.0,
        max_backoff: float = 60.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.store = store
        self.adapter = adapter
        self.owner = owner or uuid.uuid4().hex
        self.lease_seconds = lease_seconds
        self.base_backoff = base_backoff
        self.max_backoff = max_backoff
        self._clock = clock

    # ------------------------------------------------------------- intake
    def accept(self, envelope: Envelope) -> Acceptance:
        """Validate and durably accept one immutable envelope."""
        if not isinstance(envelope, Envelope):
            raise InvalidEnvelopeError("accept() expects an Envelope instance")
        if not envelope.recipient or not isinstance(envelope.recipient, str):
            raise InvalidEnvelopeError("recipient must be a non-empty string")
        if not envelope.event_key or not isinstance(envelope.event_key, str):
            raise InvalidEnvelopeError("event_key must be a non-empty string")
        if (
            not isinstance(envelope.revision, int)
            or isinstance(envelope.revision, bool)
            or envelope.revision < 1
        ):
            raise InvalidEnvelopeError("revision must be an integer >= 1")
        if not isinstance(envelope.payload, bytes):
            raise InvalidEnvelopeError("payload must be raw bytes")
        return self.store.accept(
            envelope.recipient,
            envelope.event_key,
            envelope.revision,
            envelope.payload,
        )

    # ------------------------------------------------------------ driving
    def dispatch_once(self) -> Optional[str]:
        """Claim and deliver one due envelope, returning its delivery id.

        Returns ``None`` when nothing is currently deliverable (idle,
        blocked on a predecessor, or waiting on backoff).
        """
        request = self.store.claim_due(
            self._clock(), self.lease_seconds, self.owner
        )
        if request is None:
            return None
        outcome = self.adapter.send(request)
        result = self.store.record_outcome(
            request.delivery_id,
            self.owner,
            outcome,
            self._clock(),
            self.base_backoff,
            self.max_backoff,
        )
        if result == "stale":
            # Ownership was lost: the attempt had no authority to advance
            # anything, so there is nothing further for this owner to do.
            return request.delivery_id
        if outcome.is_fenced:
            # Receiver rejected this sender's epoch; stop leading so the
            # current owner redrives under the new epoch.
            return request.delivery_id
        return request.delivery_id

    def run_until_idle(self, *, max_rounds: Optional[int] = None) -> int:
        """Dispatch due envelopes until no more can make progress.

        Retryable backoff, missing predecessors and permanent rejections
        end the loop naturally. ``DeliveryCrash`` from the adapter is not
        swallowed: the envelope stays leased and is redriven after
        recovery/takeover.
        """
        rounds = 0
        while max_rounds is None or rounds < max_rounds:
            claimed = self.dispatch_once()
            if claimed is None:
                break
            rounds += 1
        return rounds

    # ------------------------------------------------------------ recovery
    def recover_expired_leases(self) -> int:
        """Requeue envelopes whose leases expired (sender crash handling)."""
        return self.store.recover_expired_leases(self._clock())

    def takeover(self) -> int:
        """Take leadership, bumping the fencing epoch and reclaiming leases."""
        return self.store.takeover(self._clock())

    # ----------------------------------------------- permanent dispositions
    def retry_rejected(self, delivery_id: str) -> None:
        """Explicitly retry an envelope that the receiver permanently refused."""
        self.store.retry_rejected(delivery_id)

    def abandon(self, delivery_id: str, reason: str) -> None:
        """Abandon a permanently rejected envelope, unblocking its successors."""
        if not reason:
            raise InvalidEnvelopeError("an abandon reason is required")
        self.store.abandon(delivery_id, reason)

    # -------------------------------------------------------------- views
    def progress(self, recipient: str, event_key: str) -> Progress:
        return self.store.progress(recipient, event_key)

    def envelope(self, delivery_id: str):
        return self.store.get_envelope(delivery_id)
