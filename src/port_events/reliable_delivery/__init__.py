"""Reliable, ordered revision delivery independent of the event store.

The component accepts immutable envelopes from callers and drives each
(recipient, event_key) revision stream to an offline receiver at-least-once,
in revision order.  It never discovers revisions from the event database:
progress is created exclusively through :meth:`ReliableDeliveryService.accept`.
"""
from .adapters import (
    DeliveryAdapter,
    DeliveryCrash,
    HttpDeliveryAdapter,
    RecordingAdapter,
)
from .models import (
    Acceptance,
    DeliveryOutcome,
    DeliveryRequest,
    Envelope,
    EnvelopeRecord,
    InvalidEnvelopeError,
    OUTCOME_ACKED,
    OUTCOME_FENCED,
    OUTCOME_REJECTED,
    OUTCOME_RETRYABLE,
    Progress,
    RevisionConflictError,
    STATE_ABANDONED,
    STATE_DELIVERED,
    STATE_QUEUED,
    STATE_REJECTED,
    STATE_RETRYABLE,
    STATE_SENDING,
    delivery_identity,
)
from .receiver import OfflineHttpReceiver, ReceivedDelivery
from .service import ReliableDeliveryService
from .store import DeliveryStore

__all__ = [
    "Acceptance",
    "DeliveryAdapter",
    "DeliveryCrash",
    "DeliveryOutcome",
    "DeliveryRequest",
    "DeliveryStore",
    "Envelope",
    "EnvelopeRecord",
    "HttpDeliveryAdapter",
    "InvalidEnvelopeError",
    "OfflineHttpReceiver",
    "OUTCOME_ACKED",
    "OUTCOME_FENCED",
    "OUTCOME_REJECTED",
    "OUTCOME_RETRYABLE",
    "Progress",
    "ReceivedDelivery",
    "RecordingAdapter",
    "ReliableDeliveryService",
    "RevisionConflictError",
    "STATE_ABANDONED",
    "STATE_DELIVERED",
    "STATE_QUEUED",
    "STATE_REJECTED",
    "STATE_SENDING",
]
