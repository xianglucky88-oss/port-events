from __future__ import annotations

import urllib.error
import urllib.request
from typing import Callable, Optional

from .models import (
    OUTCOME_ACKED,
    OUTCOME_FENCED,
    OUTCOME_REJECTED,
    OUTCOME_RETRYABLE,
    DeliveryAdapter,
    DeliveryOutcome,
    DeliveryRequest,
)


class DeliveryCrash(RuntimeError):
    """Raised by test adapters to simulate a sender process crash.

    A real process would disappear; tests use this to assert that envelopes
    already handed to the transport are still redelivered under the same
    stable identity.
    """


class RecordingAdapter:
    """In-memory adapter for tests and offline acceptance.

    Behaviour is driven by a script: a callable that receives each
    :class:`DeliveryRequest` and returns a :class:`DeliveryOutcome` (or
    raises :class:`DeliveryCrash`).  The default script acknowledges every
    attempt, which also makes redelivery visible because every attempt is
    recorded.
    """

    def __init__(
        self,
        script: Optional[Callable[[DeliveryRequest], DeliveryOutcome]] = None,
    ) -> None:
        self.sent: list[DeliveryRequest] = []
        self._script = script or (lambda request: DeliveryOutcome(OUTCOME_ACKED))

    def send(self, request: DeliveryRequest) -> DeliveryOutcome:
        self.sent.append(request)
        return self._script(request)

    def identities(self) -> list[str]:
        return [request.delivery_id for request in self.sent]


_RETRYABLE_STATUS = {408, 425, 429}
# 401/403 etc. are permanent for an offline receiver: credentials or routing
# do not heal themselves between blind retries.
_PERMANENT_4XX_RANGE = range(400, 500)


class HttpDeliveryAdapter:
    """Transport adapter posting envelopes to an offline HTTP receiver.

    The receiver distinguishes:

    * ``200``                 -> durable acknowledgement (acked)
    * ``409`` + reject-kind   -> permanent conflict (rejected), or a stale
      epoch (fenced) when ``X-Reject-Kind: stale-epoch`` is returned
    * other 4xx (408/425/429 aside) -> permanent refusal (rejected)
    * 5xx, 408, 425, 429 and every network/timeout error -> retryable
    """

    def __init__(
        self,
        url: str,
        *,
        timeout: float = 5.0,
        opener: Optional[urllib.request.OpenerDirector] = None,
        simulate: Optional[Callable[[DeliveryRequest], str]] = None,
    ) -> None:
        self.url = url
        self.timeout = timeout
        self._opener = opener or urllib.request.build_opener()
        self._simulate = simulate

    def send(self, request: DeliveryRequest) -> DeliveryOutcome:
        headers = {
            "Content-Type": "application/octet-stream",
            "X-Delivery-Id": request.delivery_id,
            "X-Recipient": request.recipient,
            "X-Event-Key": request.event_key,
            "X-Revision": str(request.revision),
            "X-Attempt": str(request.attempt),
            "X-Epoch": str(request.epoch),
        }
        if self._simulate is not None:
            outcome = self._simulate(request)
            if outcome:
                headers["X-Simulate-Outcome"] = outcome
        http_request = urllib.request.Request(
            self.url,
            data=bytes(request.payload),
            method="POST",
            headers=headers,
        )
        try:
            with self._opener.open(http_request, timeout=self.timeout) as response:
                body = response.read().decode("utf-8", "replace")
                received_id = response.headers.get("X-Delivery-Id")
                return DeliveryOutcome(
                    OUTCOME_ACKED, reason=body, received_delivery_id=received_id
                )
        except urllib.error.HTTPError as error:
            return self._classify_http_error(error)
        except (urllib.error.URLError, TimeoutError, OSError) as error:
            return DeliveryOutcome(OUTCOME_RETRYABLE, reason=f"network error: {error}")

    def _classify_http_error(self, error: urllib.error.HTTPError) -> DeliveryOutcome:
        status = error.code
        reason = error.read().decode("utf-8", "replace") or error.reason
        reject_kind = error.headers.get("X-Reject-Kind", "")
        if status == 409 and reject_kind == "stale-epoch":
            return DeliveryOutcome(OUTCOME_FENCED, reason=reason)
        if status in _RETRYABLE_STATUS or status >= 500:
            return DeliveryOutcome(OUTCOME_RETRYABLE, reason=f"HTTP {status}: {reason}")
        if status in _PERMANENT_4XX_RANGE:
            return DeliveryOutcome(OUTCOME_REJECTED, reason=f"HTTP {status}: {reason}")
        return DeliveryOutcome(OUTCOME_RETRYABLE, reason=f"HTTP {status}: {reason}")
