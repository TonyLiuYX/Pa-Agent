"""Small standard-library transport seam for the Alpaca history adapter.

The history adapter deliberately accepts a transport object instead of using
an SDK.  Recorded tests can therefore provide a queue/callback while the
default implementation uses :mod:`urllib.request`.  Authentication headers
are held only on the in-memory request handed to the transport; none of the
serializers in this module include their values.
"""

from __future__ import annotations

import json
import socket
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Callable, Mapping, Protocol, Sequence


@dataclass(frozen=True, slots=True)
class TransportRequest:
    """A sanitized request description handed to an injected transport.

    ``headers`` contains the runtime authentication values so a real
    transport can send them.  ``to_dict`` intentionally reports only header
    names and never values.  ``params`` contains no credentials by contract.
    """

    method: str
    url: str
    params: Mapping[str, str]
    headers: Mapping[str, str]
    timeout: float

    @property
    def query(self) -> str:
        return urllib.parse.urlencode(list(self.params.items()))

    @property
    def query_params(self) -> Mapping[str, str]:
        return self.params

    @property
    def full_url(self) -> str:
        query = self.query
        return f"{self.url}?{query}" if query else self.url

    def to_dict(self) -> dict[str, Any]:
        safe_params = dict(self.params)
        if "page_token" in safe_params:
            safe_params["page_token"] = "<redacted>"
        return {
            "method": self.method,
            "url": self.url,
            "params": safe_params,
            "header_names": sorted(str(key) for key in self.headers),
            "timeout": self.timeout,
        }

    def __getitem__(self, key: str) -> Any:
        """Permit simple recorded transports to treat the request as a map."""

        if key == "headers":
            return self.headers
        if key == "params":
            return self.params
        if key == "url":
            return self.url
        if key == "method":
            return self.method
        if key == "timeout":
            return self.timeout
        raise KeyError(key)

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    def __repr__(self) -> str:
        # Keep this representation suitable for logs/debuggers.  The actual
        # ``params`` mapping is needed by the transport, but page tokens are
        # potentially sensitive and must never be echoed from repr surfaces.
        return f"TransportRequest({self.to_dict()!r})"


@dataclass(frozen=True, slots=True)
class TransportResponse:
    """Provider response normalized from an HTTP or recorded transport."""

    status_code: int
    body: Any
    headers: Mapping[str, str] = field(default_factory=dict)

    def json(self) -> Any:
        if isinstance(self.body, (Mapping, list)):
            return self.body
        if isinstance(self.body, bytes):
            return json.loads(self.body.decode("utf-8"), parse_float=Decimal, parse_int=int)
        if isinstance(self.body, str):
            return json.loads(self.body, parse_float=Decimal, parse_int=int)
        raise ValueError("response body is not JSON")

    def __repr__(self) -> str:
        return f"TransportResponse(status_code={self.status_code!r}, body=<redacted>, headers={sorted(str(key) for key in self.headers)!r})"


class MalformedTransportResponse(ValueError):
    """Raised internally when a transport response is not an HTTP response."""

    def __init__(self) -> None:
        # Keep the exception itself free of provider/body/query text.  The
        # adapter maps this marker to a stable malformed-response outcome.
        super().__init__("malformed transport response")


class HistoryTransport(Protocol):
    """Protocol implemented by the default and recorded transports."""

    def send(self, request: TransportRequest) -> TransportResponse:
        ...


class UrllibTransport:
    """Minimal HTTP GET transport with no third-party dependency."""

    def send(self, request: TransportRequest) -> TransportResponse:
        # Query construction happens here, immediately before the request is
        # sent.  Exceptions deliberately do not retain the URL or headers;
        # the adapter turns them into a stable sanitized outcome.
        url = request.full_url
        req = urllib.request.Request(url, method=request.method, headers=dict(request.headers))
        try:
            with urllib.request.urlopen(req, timeout=request.timeout) as response:
                body = response.read()
                headers = {str(k): str(v) for k, v in response.headers.items()}
                return TransportResponse(int(response.status), body, headers)
        except urllib.error.HTTPError as exc:
            # HTTPError is also a response.  Read no body here: provider error
            # text can echo a secret or a page token and is not needed for the
            # typed status classification.
            return TransportResponse(int(exc.code), b"", {})
        except (urllib.error.URLError, TimeoutError, socket.timeout, OSError) as exc:
            # Preserve a typed marker for the adapter without exposing the
            # exception text (which may contain a URL/query).
            raise TransportIOError("transport failed", cause=exc) from None


class TransportIOError(Exception):
    """Internal marker for an I/O failure whose message is safe to discard."""

    def __init__(self, message: str = "transport failed", *, cause: BaseException | None = None) -> None:
        self.cause_type = type(cause).__name__ if cause is not None else None
        reason = getattr(cause, "reason", None)
        self.is_timeout = (
            isinstance(cause, (TimeoutError, socket.timeout))
            or isinstance(reason, (TimeoutError, socket.timeout))
            or self.cause_type in {"TimeoutError", "socket.timeout"}
        )
        super().__init__(message)


class RecordedTransport:
    """Deterministic queue/callback transport used by offline tests.

    ``responses`` may contain :class:`TransportResponse`, ``(status, body)``
    pairs, mappings with ``status_code``/``body``, or exceptions to raise.
    A callback receives each :class:`TransportRequest` and may return any of
    those values.  Requests are retained in sanitized form for assertions;
    runtime header values are never copied into ``requests``.
    """

    def __init__(
        self,
        responses: Sequence[Any] | Callable[[TransportRequest], Any] = (),
    ) -> None:
        self._callback = responses if callable(responses) else None
        self._responses = list(responses) if not callable(responses) else []
        self.requests: list[TransportRequest] = []

    def send(self, request: TransportRequest) -> TransportResponse:
        self.requests.append(request)
        raw = self._callback(request) if self._callback is not None else self._next()
        if isinstance(raw, BaseException):
            raise raw
        return coerce_transport_response(raw)

    def _next(self) -> Any:
        if not self._responses:
            raise TransportIOError("recorded transport exhausted")
        return self._responses.pop(0)


# Friendly aliases used by callers writing recorded fixtures.
MockTransport = RecordedTransport
FixtureTransport = RecordedTransport


def coerce_transport_response(raw: Any) -> TransportResponse:
    """Normalize a recorded response without preserving arbitrary payloads."""

    if isinstance(raw, TransportResponse):
        response = raw
    elif isinstance(raw, Mapping):
        try:
            if "status_code" in raw:
                body = raw.get("body", raw.get("json", raw.get("data", raw.get("payload"))))
                response = TransportResponse(raw["status_code"], body, raw.get("headers") or {})
            elif "status" in raw:
                body = raw.get("body", raw.get("json", raw.get("data", raw.get("payload"))))
                response = TransportResponse(raw["status"], body, raw.get("headers") or {})
            else:
                raise MalformedTransportResponse
        except MalformedTransportResponse:
            raise
        except Exception:
            raise MalformedTransportResponse from None
    elif isinstance(raw, (tuple, list)) and len(raw) == 2:
        response = TransportResponse(raw[0], raw[1], {})
    elif isinstance(raw, (tuple, list)) and len(raw) == 3:
        response = TransportResponse(raw[0], raw[1], raw[2] or {})
    else:
        try:
            has_status = hasattr(raw, "status_code") or hasattr(raw, "status")
            if not has_status:
                raise MalformedTransportResponse
            status = getattr(raw, "status_code", getattr(raw, "status", 0))
            headers = getattr(raw, "headers", {}) or {}
            body = getattr(raw, "text", getattr(raw, "body", None))
            if body in (None, "") and hasattr(raw, "json"):
                body = raw.json()
            response = TransportResponse(status, body, headers)
        except MalformedTransportResponse:
            raise
        except Exception:
            raise MalformedTransportResponse from None

    status = response.status_code
    if isinstance(status, bool) or not isinstance(status, int) or not 100 <= status <= 599:
        raise MalformedTransportResponse
    return response


__all__ = [
    "FixtureTransport",
    "HistoryTransport",
    "MalformedTransportResponse",
    "MockTransport",
    "RecordedTransport",
    "TransportIOError",
    "TransportRequest",
    "TransportResponse",
    "UrllibTransport",
    "coerce_transport_response",
]
