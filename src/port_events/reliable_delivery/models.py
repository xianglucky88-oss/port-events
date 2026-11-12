from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

# Persistent envelope states.
STATE_QUEUED = "queued"
STATE_SENDING = "sending"
STATE_DELIVERED = "delivered"
STATE_RETRYABLE = "retryable"
STATE_REJECTED = "rejected"
STATE_ABANDONED = "abandoned"

# Adapter outcomes for a single delivery attempt.
OUTCOME_ACKED = "acked"
OUTCOME_RETRYABLE = "retryable"
OUTCOME_REJECTED = "rejected"
OUTCOME_FENCED = "fenced"

# A fixed namespace makes the delivery identity deterministic across
# processes, restarts and independent databases (RFC 4122 v5).
DELIVERY_NAMESPACE = uuid.UUID("6f9d2c81-4b7e-4f2a-9c15-2d8e7a3f0b61")


class DeliveryError(Exception):
    """Base class for reliable-delivery errors."""


class InvalidEnvelopeError(DeliveryError):
    """The envelope itself is malformed (bad recipient, key or revision)."""


class RevisionConflictError(DeliveryError):
    """A revision already exists for the triple but carries different bytes."""


@dataclass(frozen=True)
class Envelope:
    """Immutable delivery input supplied by a caller.

    ``payload`` is opaque: the component stores and forwards the bytes
    without interpreting status, source or observed_at fields.
    """

    recipient: str
    event_key: str
    revision: int
    payload: bytes

    def triple(self) -> tuple[str, str, int]:
        return self.recipient, self.event_key, self.revision


@dataclass(frozen=True)
class Acceptance:
    """Result of an accept call; identical re-submission returns the same data."""

    delivery_id: str
    recipient: str
    event_key: str
    revision: int
    is_duplicate: bool
    state: str


@dataclass(frozen=True)
class DeliveryRequest:
    """What the adapter must send for one delivery attempt."""

    delivery_id: str
    recipient: str
    event_key: str
    revision: int
    payload: bytes
    attempt: int
    epoch: int


@dataclass(frozen=True)
class DeliveryOutcome:
    """Adapter classification of an attempt's result.

    ``acked``        -> durable receiver acknowledgement
    ``retryable``    -> transient failure; resend with the same identity
    ``rejected``     -> permanent refusal; reason is stored and blocks the stream
    ``fenced``       -> another owner has taken over; the attempt is void
    """

    kind: str
    reason: str = ""
    received_delivery_id: str | None = None

    @property
    def is_ack(self) -> bool:
        return self.kind == OUTCOME_ACKED

    @property
    def is_retryable(self) -> bool:
        return self.kind == OUTCOME_RETRYABLE

    @property
    def is_rejected(self) -> bool:
        return self.kind == OUTCOME_REJECTED

    @property
    def is_fenced(self) -> bool:
        return self.kind == OUTCOME_FENCED


@dataclass(frozen=True)
class EnvelopeRecord:
    """Persistent view of one accepted envelope."""

    delivery_id: str
    recipient: str
    event_key: str
    revision: int
    payload: bytes
    state: str
    attempt: int
    not_before: float
    reject_reason: str
    abandoned_reason: str


@dataclass(frozen=True)
class Progress:
    """Durable progress for one (recipient, event_key) stream."""

    recipient: str
    event_key: str
    high_water_mark: int
    delivered_revisions: tuple[int, ...]
    blocked_reason: str | None


def delivery_identity(recipient: str, event_key: str, revision: int) -> str:
    """Stable, deterministic delivery identity for an envelope triple."""
    source = f"{recipient}\x1f{event_key}\x1f{revision}"
    return str(uuid.uuid5(DELIVERY_NAMESPACE, source))


@runtime_checkable
class DeliveryAdapter(Protocol):
    """Replaceable transport used by the delivery service."""

    def send(self, request: DeliveryRequest) -> DeliveryOutcome:
        """Attempt one delivery.

        Implementations must never partially advance local state: any
        raised exception is treated as a retryable crash, and a returned
        outcome is authoritative for the attempt.
        """
        ...
