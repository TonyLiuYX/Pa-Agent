"""Typed, secret-safe errors for the Alpaca IEX stream.

The stream adapter deliberately does not expose provider error text.  Alpaca
messages are useful for classifying an outcome, but an error body can contain
connection details or a credential echoed by a test transport.  Every public
error therefore carries only a stable kind/code and a short fixed message.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


class AlpacaStreamError(Exception):
    """Base class for all typed stream failures."""

    kind = "stream_error"
    retryable = False

    def __init__(self, message: str = "alpaca stream error", *, code: int | None = None) -> None:
        self.code = int(code) if code is not None else None
        # Do not include provider text, URLs, payloads or exception reprs.
        self.message = message if isinstance(message, str) and message in _SAFE_MESSAGES else _SAFE_MESSAGES.get(self.kind, "alpaca stream error")
        super().__init__(self.message)


_SAFE_MESSAGES = {
    "stream_error": "alpaca stream error",
    "malformed_payload": "malformed Alpaca stream payload",
    "authentication": "Alpaca authentication failed",
    "entitlement": "Alpaca market-data entitlement rejected",
    "subscription": "Alpaca stream subscription failed",
    "timeout": "Alpaca stream timed out",
    "disconnect": "Alpaca stream disconnected",
    "protocol": "Alpaca stream protocol error",
    "server": "Alpaca stream server error",
    "subscription_mismatch": "Alpaca subscription acknowledgement did not match the requested symbol",
    "duplicate_trade": "conflicting duplicate Alpaca trade",
}


class MalformedPayloadError(AlpacaStreamError):
    kind = "malformed_payload"


class AuthenticationError(AlpacaStreamError):
    kind = "authentication"


class EntitlementError(AlpacaStreamError):
    kind = "entitlement"


class SubscriptionError(AlpacaStreamError):
    kind = "subscription"


class SubscriptionMismatchError(AlpacaStreamError):
    kind = "subscription_mismatch"


class StreamTimeoutError(AlpacaStreamError):
    kind = "timeout"
    retryable = True


class DisconnectError(AlpacaStreamError):
    kind = "disconnect"
    retryable = True


class ProtocolError(AlpacaStreamError):
    kind = "protocol"


class ServerError(AlpacaStreamError):
    kind = "server"
    retryable = True


class DuplicateTradeError(AlpacaStreamError):
    kind = "duplicate_trade"


@dataclass(frozen=True, slots=True)
class StreamOutcome:
    """A sanitized typed result for callers that do not want exceptions."""

    kind: str
    ok: bool
    code: int | None = None
    retryable: bool = False
    detail: str | None = None

    @property
    def category(self) -> str:
        return self.kind

    @property
    def classification(self) -> str:
        return self.kind

    @property
    def success(self) -> bool:
        return self.ok

    @property
    def terminal(self) -> bool:
        return not self.retryable

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "kind": self.kind,
            "ok": self.ok,
            "retryable": self.retryable,
        }
        if self.code is not None:
            result["code"] = self.code
        if self.detail is not None:
            # Detail is always a fixed safe label supplied by the adapter.
            result["detail"] = self.detail
        return result


def outcome_for_error(error: BaseException) -> StreamOutcome:
    """Convert a typed error into a stable result without leaking its text."""

    if isinstance(error, AlpacaStreamError):
        return StreamOutcome(error.kind, False, error.code, error.retryable, _SAFE_MESSAGES.get(error.kind))
    if isinstance(error, TimeoutError):
        return StreamOutcome("timeout", False, None, True, _SAFE_MESSAGES["timeout"])
    return StreamOutcome("disconnect", False, None, True, _SAFE_MESSAGES["disconnect"])


__all__ = [
    "AlpacaStreamError",
    "AuthenticationError",
    "DisconnectError",
    "DuplicateTradeError",
    "EntitlementError",
    "MalformedPayloadError",
    "ProtocolError",
    "ServerError",
    "StreamOutcome",
    "StreamTimeoutError",
    "SubscriptionError",
    "SubscriptionMismatchError",
    "outcome_for_error",
]
