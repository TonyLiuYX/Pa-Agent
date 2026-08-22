"""DeepSeek V4 Flash Chat Completions adapter for G022.

The adapter consumes one validated G017 ``normalized_model_request`` and
performs exactly one DeepSeek HTTP attempt.  It maps the provider response to
the G017 normalized response/call-metadata artifacts, while parsing and
validating the untrusted JSON candidate through the G018 schema first.

No provider SDK is required.  The default transport uses :mod:`urllib`; tests
can inject a callable with the same small ``(url, headers, payload, timeout)``
signature.  Runtime credentials are held only in an ephemeral
``RuntimeCredential`` or resolver and are never retained in artifacts,
payloads, reprs, or public exceptions.
"""

from __future__ import annotations

import copy
import json
import os
import socket
import threading
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol, TypeAlias

from pa_engine.strategy.decision import (
    make_model_output_ref,
    map_candidate_to_normalized_model_candidate,
    parse_candidate,
)

from ..base import (
    AdapterError,
    AdapterOutcome,
    CapabilityDescriptor,
    contains_secret_values,
    ContractValidationError,
    FailureClassification,
    MalformedRequestError,
    MalformedResponseError,
    RuntimeCredential,
    RuntimeCredentialResolver,
    cancellation_outcome,
    first_unsupported_capability,
    make_model_call_metadata,
    make_normalized_model_response,
    normalize_error,
    request_semantic_identity,
    unsupported_capability_outcome,
    validate_normalized_request,
)

from .config import (
    DEEPSEEK_ADAPTER_VERSION,
    DEEPSEEK_API_URL,
    DEEPSEEK_MODEL,
    DEEPSEEK_PROVIDER,
    DeepSeekConfig,
)


ADAPTER_VERSION = DEEPSEEK_ADAPTER_VERSION
BASE_URL = DEEPSEEK_API_URL
MODEL = DEEPSEEK_MODEL
PROVIDER = DEEPSEEK_PROVIDER


TransportCallable: TypeAlias = Callable[
    [str, Mapping[str, str], Mapping[str, Any], float],
    "DeepSeekTransportResponse",
]

_FINISH_REASONS = frozenset(
    {"stop", "length", "content_filter", "tool_calls", "insufficient_system_resource"}
)
_SUPPORTED_SUCCESS_FINISH_REASON = "stop"
_RESOURCE_INTERRUPTION_FINISH_REASON = "insufficient_system_resource"
_HTTP_CLASSIFICATIONS: dict[int, str] = {
    400: FailureClassification.INVALID_REQUEST.value,
    401: FailureClassification.AUTHENTICATION.value,
    402: FailureClassification.INSUFFICIENT_BALANCE.value,
    403: FailureClassification.AUTHENTICATION.value,
    408: FailureClassification.TIMEOUT.value,
    413: FailureClassification.INVALID_REQUEST.value,
    422: FailureClassification.INVALID_REQUEST.value,
    429: FailureClassification.RATE_LIMIT.value,
    500: FailureClassification.SERVER.value,
    502: FailureClassification.SERVER.value,
    503: FailureClassification.SERVER.value,
    504: FailureClassification.TIMEOUT.value,
}


def _classify_http_status(status: int) -> str:
    """Map one validated HTTP status consistently across transports."""

    classification = _HTTP_CLASSIFICATIONS.get(status)
    if classification is not None:
        return classification
    if status >= 500:
        return FailureClassification.SERVER.value
    if status >= 400:
        return FailureClassification.INVALID_REQUEST.value
    # 3xx redirects are deliberately terminal provider boundaries.  The
    # urllib transport never follows them, and injected transports must map
    # them identically rather than treating them as invalid requests.
    return FailureClassification.PROVIDER.value


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _safe_int(value: Any) -> int | None:
    if type(value) is int and value >= 0:
        return value
    return None


def _json_number(value: Any) -> int | float:
    """Convert a G017 canonical decimal parameter to a JSON API number."""

    if type(value) not in {str, int}:
        raise ValueError("model parameter is not a canonical number")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError("model parameter is not a canonical number") from None
    if not parsed.is_finite():
        raise ValueError("model parameter is not finite")
    if parsed == parsed.to_integral_value():
        return int(parsed)
    return float(parsed)


def _status_code(value: Any) -> int:
    """Accept only an actual integer transport status.

    A provider response object can be supplied by an injected transport, so
    its status field is still untrusted even when the object has the expected
    dataclass type.  In particular, converting arbitrary text with ``int``
    could retain a runtime secret in a leaked ``ValueError``.  The adapter
    boundary instead rejects non-integer status values generically.
    """

    if type(value) is not int or not 100 <= value <= 599:
        raise ValueError("DeepSeek transport status is malformed")
    return value


@dataclass(frozen=True, slots=True)
class DeepSeekTransportResponse:
    """Small transport result used by the default and test transports."""

    status_code: int
    body: Any = field(repr=False)
    headers: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class DeepSeekTransportRequest:
    """Recorded transport request with a sanitized diagnostic projection."""

    method: str
    url: str
    headers: Mapping[str, str] = field(repr=False)
    payload: Mapping[str, Any]
    timeout: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "method": self.method,
            "url": self.url,
            "header_names": sorted(str(key) for key in self.headers),
            "payload": copy.deepcopy(dict(self.payload)),
            "timeout": self.timeout,
        }


class DeepSeekHTTPError(Exception):
    """Internal HTTP status exception without retaining provider body text."""

    __slots__ = ("status_code", "headers")

    def __init__(self, status_code: int, headers: Mapping[str, str] | None = None) -> None:
        self.status_code = _status_code(status_code)
        self.headers = dict(headers or {})
        super().__init__(f"DeepSeek HTTP status {self.status_code}")


class DeepSeekTransport(Protocol):
    def __call__(
        self,
        url: str,
        headers: Mapping[str, str],
        payload: Mapping[str, Any],
        timeout: float,
    ) -> DeepSeekTransportResponse:
        ...


class DeepSeekNoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Refuse every HTTP redirect, including POST 301/302/303 rewrites."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        del req, fp, code, msg, headers, newurl
        return None


class UrllibDeepSeekTransport:
    """One-shot stdlib POST transport.

    HTTP errors become :class:`DeepSeekHTTPError` and their response body is
    discarded.  The body can contain arbitrary provider text and is not part
    of a normalized error or exception, which keeps the secret boundary
    narrow and deterministic.
    """

    def __init__(self, *, opener: Any | None = None) -> None:
        # ``build_opener`` retains urllib's normal proxy and HTTPS handlers;
        # replacing only the redirect handler keeps TLS/default-proxy behavior
        # while ensuring Authorization never crosses to a Location host.
        self._opener = opener if opener is not None else urllib.request.build_opener(DeepSeekNoRedirectHandler())

    def __call__(
        self,
        url: str,
        headers: Mapping[str, str],
        payload: Mapping[str, Any],
        timeout: float,
    ) -> DeepSeekTransportResponse:
        body = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        request = urllib.request.Request(url, data=body, headers=dict(headers), method="POST")
        pending_error: DeepSeekHTTPError | None = None
        response_result: DeepSeekTransportResponse | None = None
        try:
            with self._opener.open(request, timeout=timeout) as response:
                raw = response.read()
                response_headers = {str(key): str(value) for key, value in response.headers.items()}
                response_result = DeepSeekTransportResponse(int(response.status), raw, response_headers)
        except urllib.error.HTTPError as exc:
            # Do not read/retain ``exc``'s provider body, Location, or any
            # response headers.  In particular, a redirect Location may be a
            # different host and must never survive beside Authorization.
            try:
                exc.close()
            except Exception:
                pass
            pending_error = DeepSeekHTTPError(exc.code)
        if pending_error is not None:
            raise pending_error
        if response_result is None:
            raise ValueError("DeepSeek transport returned no response")
        return response_result


# Explicit alias used by callers that prefer the provider-neutral name.
DeepSeekHTTPTransport = UrllibDeepSeekTransport


@dataclass(frozen=True, slots=True)
class DeepSeekMappedResponse:
    """Validated mapping of one official DeepSeek completion object."""

    candidate: dict[str, Any] | None
    normalized_candidate: dict[str, Any] | None
    finish_reason: str | None
    provider_request_id: str | None
    generated_at: str | None
    usage: dict[str, int] | None
    reasoning_tokens: int | None
    prompt_cache_hit_tokens: int | None
    prompt_cache_miss_tokens: int | None
    system_fingerprint: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate": copy.deepcopy(self.candidate),
            "normalized_candidate": copy.deepcopy(self.normalized_candidate),
            "finish_reason": self.finish_reason,
            "provider_request_id": self.provider_request_id,
            "generated_at": self.generated_at,
            "usage": copy.deepcopy(self.usage),
            "reasoning_tokens": self.reasoning_tokens,
            "prompt_cache_hit_tokens": self.prompt_cache_hit_tokens,
            "prompt_cache_miss_tokens": self.prompt_cache_miss_tokens,
            "system_fingerprint": self.system_fingerprint,
        }

    as_dict = to_dict


def _body_mapping(body: Any) -> Mapping[str, Any]:
    if isinstance(body, Mapping):
        return body
    if not isinstance(body, (str, bytes, bytearray)):
        raise ValueError("DeepSeek response body is not JSON")
    try:
        decoded = body.decode("utf-8") if isinstance(body, (bytes, bytearray)) else body
        def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
            result: dict[str, Any] = {}
            for key, item in pairs:
                if key in result:
                    raise ValueError("duplicate response field")
                result[key] = item
            return result

        def reject_constant(token: str) -> None:
            del token
            raise ValueError("non-finite response value")

        value = json.loads(
            decoded,
            object_pairs_hook=reject_duplicates,
            parse_constant=reject_constant,
        )
    except (UnicodeError, TypeError, ValueError):
        raise ValueError("DeepSeek response body is not valid JSON") from None
    if not isinstance(value, Mapping):
        raise ValueError("DeepSeek response body is not a JSON object")
    return value


def _created_timestamp(value: Any) -> str | None:
    if type(value) is not int or value < 0:
        return None
    try:
        return _timestamp(datetime.fromtimestamp(value, tz=timezone.utc))
    except (OverflowError, OSError, ValueError):
        return None


def _usage_mapping(value: Any) -> tuple[dict[str, int] | None, int | None, int | None, int | None]:
    """Return core usage plus optional cache/reasoning counters.

    DeepSeek adds cache counters and ``completion_tokens_details`` to the
    documented usage object.  G014's frozen usage schema carries only the
    three core counters, so the extra values stay in call-metadata extensions.
    """

    if value is None:
        return None, None, None, None
    if not isinstance(value, Mapping):
        raise ValueError("DeepSeek usage is malformed")
    prompt = _safe_int(value.get("prompt_tokens"))
    completion = _safe_int(value.get("completion_tokens"))
    total = _safe_int(value.get("total_tokens"))
    if prompt is None or completion is None or total is None or total != prompt + completion:
        raise ValueError("DeepSeek usage is malformed")
    cache_hit = value.get("prompt_cache_hit_tokens")
    cache_miss = value.get("prompt_cache_miss_tokens")
    if cache_hit is not None and _safe_int(cache_hit) is None:
        raise ValueError("DeepSeek usage is malformed")
    if cache_miss is not None and _safe_int(cache_miss) is None:
        raise ValueError("DeepSeek usage is malformed")
    details = value.get("completion_tokens_details")
    reasoning: Any = None
    if details is not None:
        if not isinstance(details, Mapping):
            raise ValueError("DeepSeek usage is malformed")
        reasoning = details.get("reasoning_tokens")
        if reasoning is not None and _safe_int(reasoning) is None:
            raise ValueError("DeepSeek usage is malformed")
    return (
        {"prompt_tokens": prompt, "completion_tokens": completion, "total_tokens": total},
        _safe_int(cache_hit) if cache_hit is not None else None,
        _safe_int(cache_miss) if cache_miss is not None else None,
        _safe_int(reasoning) if reasoning is not None else None,
    )


def _response_telemetry(
    body: Any,
    secret_values: tuple[str, ...],
) -> tuple[str | None, str | None, str | None, dict[str, int] | None, int | None, int | None, int | None, str | None]:
    """Extract safe response telemetry even when candidate mapping fails."""

    try:
        value = _body_mapping(body)
        provider_request_id = _secret_safe_text(
            value.get("id") if isinstance(value.get("id"), str) else None,
            secret_values,
        )
        generated_at = _created_timestamp(value.get("created"))
        system_fingerprint = _secret_safe_text(
            value.get("system_fingerprint") if isinstance(value.get("system_fingerprint"), str) else None,
            secret_values,
        )
        usage, cache_hit, cache_miss, reasoning = _usage_mapping(value.get("usage"))
        choices = value.get("choices")
        finish_reason = None
        if type(choices) is list and choices and isinstance(choices[0], Mapping):
            raw_finish = choices[0].get("finish_reason")
            if (
                isinstance(raw_finish, str)
                and raw_finish in _FINISH_REASONS
                and not contains_secret_values(raw_finish, secret_values)
            ):
                finish_reason = raw_finish
        return (
            finish_reason,
            provider_request_id,
            generated_at,
            usage,
            reasoning,
            cache_hit,
            cache_miss,
            system_fingerprint,
        )
    except Exception:
        return (None, None, None, None, None, None, None, None)


def _is_resource_interruption_response(
    body: Any,
    secret_values: tuple[str, ...] = (),
) -> bool:
    """Recognize the official resource-interruption envelope safely.

    This narrow check mirrors the structural guards in
    :func:`map_deepseek_response` while allowing the documented interrupted
    message content to be empty or null.  It prevents a malformed object that
    merely mentions the finish reason from being promoted to a retryable
    server outcome.
    """

    try:
        value = _body_mapping(body)
        if contains_secret_values(value, secret_values):
            return False
        if value.get("object") != "chat.completion":
            return False
        if value.get("model") != DEEPSEEK_MODEL:
            return False
        choices = value.get("choices")
        if type(choices) is not list or len(choices) != 1 or not isinstance(choices[0], Mapping):
            return False
        choice = choices[0]
        if type(choice.get("index")) is not int or choice.get("index") != 0:
            return False
        if choice.get("finish_reason") != _RESOURCE_INTERRUPTION_FINISH_REASON:
            return False
        message = choice.get("message")
        if not isinstance(message, Mapping):
            return False
        if "role" in message and message.get("role") != "assistant":
            return False
        if "content" not in message:
            return False
        content = message.get("content")
        if content is not None and not isinstance(content, str):
            return False
        reasoning_content = message.get("reasoning_content")
        if "reasoning_content" in message and reasoning_content is not None and not isinstance(reasoning_content, str):
            return False
        return True
    except Exception:
        return False


def map_deepseek_response(
    body: Any,
    *,
    profile_ref: Mapping[str, Any] | None = None,
    secret_values: tuple[str, ...] = (),
) -> DeepSeekMappedResponse:
    """Parse one official non-streaming response and validate its G018 JSON.

    This function deliberately raises only a generic ``ValueError`` on any
    malformed/truncated/unsupported output.  The adapter converts that typed
    boundary failure into a terminal G017 ``invalid_model_response`` outcome;
    no candidate is ever returned unless G018 accepts it.
    """

    value = _body_mapping(body)
    if value.get("object") != "chat.completion":
        raise ValueError("DeepSeek response object is unsupported")
    if value.get("model") != DEEPSEEK_MODEL:
        raise ValueError("DeepSeek response model is unsupported")
    provider_request_id = _secret_safe_text(
        value.get("id") if isinstance(value.get("id"), str) else None,
        secret_values,
    )
    generated_at = _created_timestamp(value.get("created"))
    system_fingerprint = _secret_safe_text(
        value.get("system_fingerprint") if isinstance(value.get("system_fingerprint"), str) else None,
        secret_values,
    )
    usage, cache_hit, cache_miss, reasoning = _usage_mapping(value.get("usage"))
    choices = value.get("choices")
    if type(choices) is not list or len(choices) != 1 or not isinstance(choices[0], Mapping):
        raise ValueError("DeepSeek response choices are malformed")
    choice = choices[0]
    if type(choice.get("index")) is not int or choice.get("index") != 0:
        raise ValueError("DeepSeek response choice index is malformed")
    finish_reason = choice.get("finish_reason")
    if not isinstance(finish_reason, str) or finish_reason not in _FINISH_REASONS:
        raise ValueError("DeepSeek finish reason is unsupported")
    if contains_secret_values(finish_reason, secret_values):
        raise ValueError("DeepSeek finish reason contains runtime credential data")
    message = choice.get("message")
    if not isinstance(message, Mapping) or not isinstance(message.get("content"), str) or not message.get("content").strip():
        raise ValueError("DeepSeek message content is malformed")
    if "role" in message and message.get("role") != "assistant":
        raise ValueError("DeepSeek message role is malformed")
    if "reasoning_content" in message and message.get("reasoning_content") is not None and not isinstance(message.get("reasoning_content"), str):
        raise ValueError("DeepSeek reasoning content is malformed")
    content = message["content"]
    if contains_secret_values(content, secret_values):
        raise ValueError("DeepSeek response contains runtime credential data")
    # JSON output that hit max_tokens or a tool/content filter is not a safe
    # strategy candidate even if the prefix happens to parse as an object.
    if finish_reason != _SUPPORTED_SUCCESS_FINISH_REASON:
        raise ValueError("DeepSeek response did not finish with stop")
    try:
        candidate = parse_candidate(content)
        if contains_secret_values(candidate, secret_values):
            raise ValueError("DeepSeek response contains runtime credential data")
        normalized_candidate = map_candidate_to_normalized_model_candidate(candidate, profile_ref=profile_ref)
    except Exception:
        # Never retain the provider's candidate text or exception on the
        # caller-visible boundary.
        raise ValueError("DeepSeek candidate is invalid") from None
    return DeepSeekMappedResponse(
        candidate=copy.deepcopy(candidate),
        normalized_candidate=copy.deepcopy(normalized_candidate),
        finish_reason=finish_reason,
        provider_request_id=provider_request_id,
        generated_at=generated_at,
        usage=usage,
        reasoning_tokens=reasoning,
        prompt_cache_hit_tokens=cache_hit,
        prompt_cache_miss_tokens=cache_miss,
        system_fingerprint=system_fingerprint,
    )


# Mapper aliases make the small function easy to discover from integration
# code without introducing another module or registry entry.
map_response = map_deepseek_response
parse_deepseek_response = map_deepseek_response


class DeepSeekResponseMapper:
    """Stateless object form of :func:`map_deepseek_response`."""

    @staticmethod
    def map(
        body: Any,
        *,
        profile_ref: Mapping[str, Any] | None = None,
        secret_values: tuple[str, ...] = (),
    ) -> DeepSeekMappedResponse:
        return map_deepseek_response(body, profile_ref=profile_ref, secret_values=secret_values)

    __call__ = map


class DeepSeekRequestMapper:
    """Stateless object form of :func:`build_transport_payload`."""

    @staticmethod
    def map(request: Mapping[str, Any]) -> dict[str, Any]:
        return build_transport_payload(request)

    __call__ = map


def build_transport_payload(request: Mapping[str, Any]) -> dict[str, Any]:
    """Map a validated G017 request to the exact DeepSeek JSON body."""

    result = validate_normalized_request(request)
    if not result.valid:
        raise MalformedRequestError()
    if request.get("provider") != DEEPSEEK_PROVIDER or request.get("model") != DEEPSEEK_MODEL:
        raise ValueError("normalized request is not for DeepSeek V4 Flash")
    extensions = request.get("extensions")
    if not isinstance(extensions, Mapping) or extensions.get("reasoning_mode") not in {"enabled", "disabled"}:
        raise ValueError("normalized request reasoning mode is malformed")
    parameters = request.get("parameters")
    if not isinstance(parameters, Mapping):
        raise ValueError("normalized request parameters are malformed")
    payload: dict[str, Any] = {
        "model": DEEPSEEK_MODEL,
        "messages": [
            {"role": "system", "content": request["instructions"]},
            {"role": "user", "content": request["context_text"]},
        ],
        "thinking": {"type": extensions["reasoning_mode"]},
        "response_format": {"type": "json_object"},
        "max_tokens": int(parameters["max_output_tokens"]),
    }
    # DeepSeek thinking mode explicitly ignores temperature/top_p and the
    # penalty fields.  Keep the provider-neutral G017 temperature meaningful
    # for non-thinking requests, while omitting it entirely for enabled
    # thinking requests so the wire shape contains no documented no-op field.
    if extensions["reasoning_mode"] == "disabled":
        payload["temperature"] = _json_number(parameters["temperature"])
    # The payload intentionally contains no provider-neutral ``extensions``
    # object, profile packet, balance, or credential.
    return payload


build_request_payload = build_transport_payload
map_request_to_deepseek = build_transport_payload


def _header_value(headers: Mapping[str, Any] | None, name: str) -> str | None:
    if not isinstance(headers, Mapping):
        return None
    wanted = name.lower()
    for key, value in headers.items():
        if str(key).lower() == wanted and isinstance(value, str):
            return value
    return None


def _secret_safe_text(value: str | None, secret_values: tuple[str, ...]) -> str | None:
    """Keep provider identifiers out of artifacts when they echo a key."""

    if not isinstance(value, str) or contains_secret_values(value, secret_values):
        return None
    return value


def _coerce_transport_result(value: Any) -> DeepSeekTransportResponse:
    if isinstance(value, DeepSeekTransportResponse):
        return DeepSeekTransportResponse(
            _status_code(value.status_code),
            value.body,
            value.headers if isinstance(value.headers, Mapping) else {},
        )
    if isinstance(value, (tuple, list)) and len(value) in {2, 3}:
        status, body = value[0], value[1]
        headers = value[2] if len(value) == 3 else {}
        return DeepSeekTransportResponse(_status_code(status), body, headers if isinstance(headers, Mapping) else {})
    if isinstance(value, Mapping):
        if ("status_code" in value or "status" in value) and "body" in value:
            return DeepSeekTransportResponse(
                _status_code(value.get("status_code", value.get("status"))),
                value.get("body"),
                value.get("headers") if isinstance(value.get("headers"), Mapping) else {},
            )
        # A mapping is treated as a successful JSON body for terse test
        # transports; it is not copied into any artifact until validated.
        return DeepSeekTransportResponse(200, value, {})
    status = getattr(value, "status_code", getattr(value, "status", None))
    if status is not None:
        body = getattr(value, "body", None)
        if body is None and hasattr(value, "read"):
            body = value.read()
        headers = getattr(value, "headers", {})
        return DeepSeekTransportResponse(_status_code(status), body, headers if isinstance(headers, Mapping) else {})
    raise ValueError("DeepSeek transport returned an invalid response")


class RecordedTransport:
    """Queue/callback transport for offline DeepSeek conformance tests."""

    def __init__(self, responses: Any = ()) -> None:
        self._callback = responses if callable(responses) else None
        self._responses = list(responses) if not callable(responses) else []
        self.requests: list[DeepSeekTransportRequest] = []

    def send(self, request: DeepSeekTransportRequest) -> DeepSeekTransportResponse:
        self.requests.append(request)
        if self._callback is not None:
            raw = self._callback(request)
        elif self._responses:
            raw = self._responses.pop(0)
        else:
            raise DeepSeekHTTPError(599)
        if isinstance(raw, Exception):
            raise raw
        return _coerce_transport_result(raw)


MockTransport = RecordedTransport
FixtureTransport = RecordedTransport
TransportRequest = DeepSeekTransportRequest
TransportResponse = DeepSeekTransportResponse
UrllibTransport = UrllibDeepSeekTransport
DeepSeekTransportError = DeepSeekHTTPError


class DeepSeekAdapter:
    """One-attempt provider adapter implementing the G017 protocol."""

    _CAPABILITIES = CapabilityDescriptor(structured_json=True, reasoning_mode=True)

    def __init__(
        self,
        api_key: str | RuntimeCredential | None = None,
        *,
        credential: str | RuntimeCredential | None = None,
        credential_resolver: RuntimeCredentialResolver | Mapping[str, Any] | Callable[[str], Any] | None = None,
        resolver: RuntimeCredentialResolver | Mapping[str, Any] | Callable[[str], Any] | None = None,
        config: DeepSeekConfig | None = None,
        transport: DeepSeekTransport | TransportCallable | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if api_key is not None and credential is not None:
            raise ValueError("pass api_key or credential, not both")
        if credential_resolver is not None and resolver is not None:
            raise ValueError("pass credential_resolver or resolver, not both")
        supplied = credential if credential is not None else api_key
        if supplied is not None and not isinstance(supplied, (str, RuntimeCredential)):
            raise ValueError("DeepSeek credential must be a runtime key or handle")
        self._credential = self._credential_handle(supplied)
        supplied_resolver = credential_resolver if credential_resolver is not None else resolver
        if isinstance(supplied_resolver, Mapping) or callable(supplied_resolver):
            supplied_resolver = RuntimeCredentialResolver(supplied_resolver)
        self._resolver = supplied_resolver
        if self._resolver is not None and not isinstance(self._resolver, RuntimeCredentialResolver):
            raise ValueError("DeepSeek resolver must be RuntimeCredentialResolver or a mapping")
        self._config = config or DeepSeekConfig()
        self._transport: DeepSeekTransport | TransportCallable = transport or UrllibDeepSeekTransport()
        self._clock = clock or _utc_now
        self._attempt_lock = threading.Lock()
        self._attempt_count = 0

    @classmethod
    def from_env(
        cls,
        *,
        env_name: str = "DEEPSEEK_API_KEY",
        **kwargs: Any,
    ) -> "DeepSeekAdapter":
        """Resolve one ephemeral key from the process environment."""

        return cls(api_key=os.environ.get(env_name), **kwargs)

    @staticmethod
    def _credential_handle(value: str | RuntimeCredential | None) -> RuntimeCredential | None:
        if isinstance(value, RuntimeCredential):
            return value
        if isinstance(value, str) and value:
            return RuntimeCredential(DEEPSEEK_PROVIDER, {"api_key": value})
        return None

    @property
    def capabilities(self) -> CapabilityDescriptor:
        return self._CAPABILITIES

    @property
    def config(self) -> DeepSeekConfig:
        return self._config

    @property
    def attempt_count(self) -> int:
        with self._attempt_lock:
            return self._attempt_count

    @property
    def calls(self) -> int:
        return self.attempt_count

    def __repr__(self) -> str:
        return "DeepSeekAdapter(model='deepseek-v4-flash', credential=<runtime-only>)"

    __str__ = __repr__

    def _runtime_credential(self) -> RuntimeCredential | None:
        if self._resolver is not None:
            try:
                return self._resolver.resolve(DEEPSEEK_PROVIDER)
            except Exception:
                return None
        return self._credential

    @staticmethod
    def _api_key(credential: RuntimeCredential | None) -> tuple[str | None, tuple[str, ...]]:
        if credential is None:
            return None, ()
        value = credential.get("api_key")
        if value is None:
            value = credential.get("key")
        if value is None:
            runtime_value = credential.as_runtime_value()
            if isinstance(runtime_value, str):
                value = runtime_value
        if not isinstance(value, str) or not value:
            return None, ()
        # RuntimeCredential also exposes mapping *keys* in ``secret_values``
        # for generic recursive redaction.  They are not secrets here (and a
        # short key such as ``api_key`` would create false positives in normal
        # prompt text), so the adapter tracks only the actual injected value.
        return value, (value,)

    def _next_attempt(self) -> None:
        with self._attempt_lock:
            self._attempt_count += 1

    def _invoke_transport(
        self,
        payload: Mapping[str, Any],
        api_key: str,
    ) -> DeepSeekTransportResponse:
        headers = {
            "Authorization": "Bearer " + api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        request = DeepSeekTransportRequest("POST", self._config.endpoint, headers, payload, self._config.timeout_seconds)
        # ``transport`` is intentionally called once.  There is no retry loop
        # here; scheduler-owned G023 handles any returned retryable outcome.
        sender = getattr(self._transport, "send", None)
        if callable(sender):
            result = sender(request)
        else:
            result = self._transport(self._config.endpoint, headers, payload, self._config.timeout_seconds)
        return _coerce_transport_result(result)

    def _metadata_extensions(
        self,
        *,
        reasoning_tokens: int | None = None,
        prompt_cache_hit_tokens: int | None = None,
        prompt_cache_miss_tokens: int | None = None,
        system_fingerprint: str | None = None,
    ) -> dict[str, Any]:
        return {
            "deepseek": {
                "reasoning_tokens": reasoning_tokens,
                "prompt_cache_hit_tokens": prompt_cache_hit_tokens,
                "prompt_cache_miss_tokens": prompt_cache_miss_tokens,
                "system_fingerprint": system_fingerprint,
            }
        }

    def _outcome(
        self,
        request: Mapping[str, Any],
        *,
        classification: str,
        secret_values: tuple[str, ...],
        generation: int | None,
        started_at: str | None,
        completed_at: str | None,
        latency_ms: int | None,
        finish_reason: str | None = None,
        candidate: Mapping[str, Any] | None = None,
        normalized_candidate: Mapping[str, Any] | None = None,
        error_message: str | None = None,
        error_details: str | None = None,
        transient: bool | None = None,
        usage: Mapping[str, Any] | None = None,
        provider_request_id: str | None = None,
        generated_at: str | None = None,
        reasoning_tokens: int | None = None,
        prompt_cache_hit_tokens: int | None = None,
        prompt_cache_miss_tokens: int | None = None,
        system_fingerprint: str | None = None,
        profile_ref: Mapping[str, Any] | None = None,
    ) -> AdapterOutcome:
        normalized_classification = classification
        if normalized_classification == FailureClassification.SUCCESS.value:
            if not isinstance(candidate, Mapping) or not isinstance(normalized_candidate, Mapping):
                normalized_classification = FailureClassification.MALFORMED_RESPONSE.value
        error = None
        response_extensions: dict[str, Any] = {}
        if normalized_classification == FailureClassification.SUCCESS.value:
            response_extensions["g018_model_output_ref"] = make_model_output_ref(candidate or {})
            if isinstance(profile_ref, Mapping):
                response_extensions["strategy_profile_ref"] = copy.deepcopy(dict(profile_ref))
        else:
            error = normalize_error(
                normalized_classification,
                message=error_message,
                details=error_details,
                transient=transient,
                secret_values=secret_values,
            )
        response = make_normalized_model_response(
            request,
            status="completed" if normalized_classification == FailureClassification.SUCCESS.value else "error",
            candidate=normalized_candidate if normalized_classification == FailureClassification.SUCCESS.value else None,
            error=None if normalized_classification == FailureClassification.SUCCESS.value else error,
            finish_reason=finish_reason,
            generated_at=generated_at,
            extensions=response_extensions,
            secret_values=secret_values,
        )
        metadata = make_model_call_metadata(
            request,
            response,
            started_at=started_at,
            completed_at=completed_at,
            latency_ms=latency_ms,
            usage=usage,
            provider_request_id=provider_request_id,
            extensions=self._metadata_extensions(
                reasoning_tokens=reasoning_tokens,
                prompt_cache_hit_tokens=prompt_cache_hit_tokens,
                prompt_cache_miss_tokens=prompt_cache_miss_tokens,
                system_fingerprint=system_fingerprint,
            ),
            secret_values=secret_values,
        )
        return AdapterOutcome(
            response=response,
            call_metadata=metadata,
            classification=normalized_classification,
            request_semantic_hash=request_semantic_identity(request),
            retryable=bool(error["retryable"]) if isinstance(error, Mapping) else False,
            generation=generation,
        )

    def _timing(self, started: datetime) -> tuple[str, str, int]:
        try:
            completed = self._clock()
        except Exception:
            completed = _utc_now()
        if not isinstance(completed, datetime):
            completed = _utc_now()
        elapsed = max(0.0, (completed - started).total_seconds() * 1000)
        return _timestamp(started), _timestamp(completed), int(elapsed)

    def complete(
        self,
        request: Mapping[str, Any],
        *,
        cancelled: bool = False,
        generation: int | None = None,
    ) -> AdapterOutcome:
        request_result = validate_normalized_request(request)
        if not request_result.valid:
            raise MalformedRequestError()
        credential = self._runtime_credential()
        api_key, secret_values = self._api_key(credential)
        if contains_secret_values(request, secret_values):
            raise MalformedRequestError(secret_values=secret_values)
        request_hash = request_semantic_identity(request)
        if cancelled:
            return cancellation_outcome(request, secret_values=secret_values, generation=generation)
        capability = first_unsupported_capability(request, self.capabilities)
        if capability is not None:
            return unsupported_capability_outcome(request, capability, secret_values=secret_values, generation=generation)
        if request.get("provider") != DEEPSEEK_PROVIDER or request.get("model") != DEEPSEEK_MODEL:
            return self._outcome(
                request,
                classification=FailureClassification.INVALID_CONFIGURATION.value,
                secret_values=secret_values,
                generation=generation,
                started_at=None,
                completed_at=None,
                latency_ms=None,
                error_message="DeepSeek adapter configuration does not match the normalized request",
            )
        if api_key is None:
            return self._outcome(
                request,
                classification=FailureClassification.AUTHENTICATION.value,
                secret_values=secret_values,
                generation=generation,
                started_at=None,
                completed_at=None,
                latency_ms=None,
            )
        try:
            payload = build_transport_payload(request)
        except (MalformedRequestError, ContractValidationError, TypeError, ValueError):
            return self._outcome(
                request,
                classification=FailureClassification.INVALID_REQUEST.value,
                secret_values=secret_values,
                generation=generation,
                started_at=None,
                completed_at=None,
                latency_ms=None,
            )
        # Reject a credential-bearing prompt before it crosses the transport
        # boundary.  The check includes context and instructions via payload.
        if contains_secret_values(payload, secret_values):
            return self._outcome(
                request,
                classification=FailureClassification.INVALID_REQUEST.value,
                secret_values=secret_values,
                generation=generation,
                started_at=None,
                completed_at=None,
                latency_ms=None,
            )
        profile_ref = None
        extensions = request.get("extensions")
        if isinstance(extensions, Mapping) and isinstance(extensions.get("strategy_profile_ref"), Mapping):
            profile_ref = extensions["strategy_profile_ref"]
        try:
            started = self._clock()
        except Exception:
            started = _utc_now()
        if not isinstance(started, datetime):
            started = _utc_now()
        self._next_attempt()
        try:
            transport_result = self._invoke_transport(payload, api_key)
        except DeepSeekHTTPError as exc:
            start_text, end_text, latency = self._timing(started)
            status = int(exc.status_code)
            classification = _classify_http_status(status)
            transient = classification == FailureClassification.SERVER.value
            request_id = _secret_safe_text(_header_value(exc.headers, "x-request-id"), secret_values)
            return self._outcome(
                request,
                classification=classification,
                secret_values=secret_values,
                generation=generation,
                started_at=start_text,
                completed_at=end_text,
                latency_ms=latency,
                error_details=f"DeepSeek HTTP status {status}",
                transient=transient,
                provider_request_id=request_id,
            )
        except (socket.timeout, TimeoutError):
            start_text, end_text, latency = self._timing(started)
            return self._outcome(
                request,
                classification=FailureClassification.TIMEOUT.value,
                secret_values=secret_values,
                generation=generation,
                started_at=start_text,
                completed_at=end_text,
                latency_ms=latency,
            )
        except urllib.error.URLError as exc:
            start_text, end_text, latency = self._timing(started)
            is_timeout = isinstance(getattr(exc, "reason", None), (socket.timeout, TimeoutError))
            del exc
            return self._outcome(
                request,
                classification=FailureClassification.TIMEOUT.value if is_timeout else FailureClassification.TRANSPORT.value,
                secret_values=secret_values,
                generation=generation,
                started_at=start_text,
                completed_at=end_text,
                latency_ms=latency,
                transient=None if is_timeout else True,
            )
        except Exception as exc:
            # Transport implementations are test/runtime boundaries.  Their
            # exception text is intentionally discarded rather than redacted
            # into a contract, preventing a key-bearing traceback from being
            # retained by the outcome.
            del exc
            start_text, end_text, latency = self._timing(started)
            return self._outcome(
                request,
                classification=FailureClassification.TRANSPORT.value,
                secret_values=secret_values,
                generation=generation,
                started_at=start_text,
                completed_at=end_text,
                latency_ms=latency,
                transient=False,
            )

        start_text, end_text, latency = self._timing(started)
        # ``_coerce_transport_result`` normally validates this field.  Keep a
        # second narrow guard here for a custom response object that mutates
        # after coercion; malformed status is a terminal boundary failure and
        # must never escape with provider/key-bearing text.
        try:
            status_code = _status_code(transport_result.status_code)
        except Exception:
            return self._outcome(
                request,
                classification=FailureClassification.TRANSPORT.value,
                secret_values=secret_values,
                generation=generation,
                started_at=start_text,
                completed_at=end_text,
                latency_ms=latency,
                transient=False,
            )
        provider_request_id = _secret_safe_text(_header_value(transport_result.headers, "x-request-id"), secret_values)
        if status_code < 200 or status_code >= 300:
            classification = _classify_http_status(status_code)
            return self._outcome(
                request,
                classification=classification,
                secret_values=secret_values,
                generation=generation,
                started_at=start_text,
                completed_at=end_text,
                latency_ms=latency,
                error_details=f"DeepSeek HTTP status {status_code}",
                transient=classification == FailureClassification.SERVER.value,
                provider_request_id=provider_request_id,
            )
        try:
            mapped = map_deepseek_response(transport_result.body, profile_ref=profile_ref, secret_values=secret_values)
        except Exception:
            (
                failed_finish_reason,
                failed_provider_request_id,
                failed_generated_at,
                failed_usage,
                failed_reasoning_tokens,
                failed_cache_hit,
                failed_cache_miss,
                failed_system_fingerprint,
            ) = _response_telemetry(transport_result.body, secret_values)
            if (
                failed_finish_reason == _RESOURCE_INTERRUPTION_FINISH_REASON
                and _is_resource_interruption_response(transport_result.body, secret_values)
            ):
                return self._outcome(
                    request,
                    classification=FailureClassification.SERVER.value,
                    secret_values=secret_values,
                    generation=generation,
                    started_at=start_text,
                    completed_at=end_text,
                    latency_ms=latency,
                    finish_reason=failed_finish_reason,
                    usage=failed_usage,
                    provider_request_id=failed_provider_request_id or provider_request_id,
                    generated_at=failed_generated_at,
                    reasoning_tokens=failed_reasoning_tokens,
                    prompt_cache_hit_tokens=failed_cache_hit,
                    prompt_cache_miss_tokens=failed_cache_miss,
                    system_fingerprint=failed_system_fingerprint,
                    transient=True,
                )
            return self._outcome(
                request,
                classification=FailureClassification.MALFORMED_RESPONSE.value,
                secret_values=secret_values,
                generation=generation,
                started_at=start_text,
                completed_at=end_text,
                latency_ms=latency,
                finish_reason=failed_finish_reason,
                usage=failed_usage,
                provider_request_id=failed_provider_request_id or provider_request_id,
                generated_at=failed_generated_at,
                reasoning_tokens=failed_reasoning_tokens,
                prompt_cache_hit_tokens=failed_cache_hit,
                prompt_cache_miss_tokens=failed_cache_miss,
                system_fingerprint=failed_system_fingerprint,
            )
        return self._outcome(
            request,
            classification=FailureClassification.SUCCESS.value,
            secret_values=secret_values,
            generation=generation,
            started_at=start_text,
            completed_at=end_text,
            latency_ms=latency,
            finish_reason=mapped.finish_reason,
            candidate=mapped.candidate,
            normalized_candidate=mapped.normalized_candidate,
            usage=mapped.usage,
            provider_request_id=mapped.provider_request_id or provider_request_id,
            generated_at=mapped.generated_at,
            reasoning_tokens=mapped.reasoning_tokens,
            prompt_cache_hit_tokens=mapped.prompt_cache_hit_tokens,
            prompt_cache_miss_tokens=mapped.prompt_cache_miss_tokens,
            system_fingerprint=mapped.system_fingerprint,
            profile_ref=profile_ref,
        )

    call = complete


# Compatibility aliases for downstream integration naming.
DeepSeekV4FlashAdapter = DeepSeekAdapter
DeepSeekAdapterConfig = DeepSeekConfig


__all__ = [
    "ADAPTER_VERSION",
    "BASE_URL",
    "DeepSeekAdapter",
    "DeepSeekAdapterConfig",
    "DeepSeekHTTPError",
    "DeepSeekHTTPTransport",
    "DeepSeekMappedResponse",
    "DeepSeekNoRedirectHandler",
    "DeepSeekRequestMapper",
    "DeepSeekResponseMapper",
    "DeepSeekTransportRequest",
    "DeepSeekTransport",
    "DeepSeekTransportError",
    "DeepSeekTransportResponse",
    "DeepSeekV4FlashAdapter",
    "UrllibDeepSeekTransport",
    "build_request_payload",
    "build_transport_payload",
    "map_deepseek_response",
    "map_request_to_deepseek",
    "map_response",
    "parse_deepseek_response",
    "FixtureTransport",
    "MODEL",
    "MockTransport",
    "PROVIDER",
    "RecordedTransport",
    "TransportRequest",
    "TransportResponse",
    "UrllibTransport",
]
