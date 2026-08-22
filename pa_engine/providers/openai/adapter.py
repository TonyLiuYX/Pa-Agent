"""OpenAI Responses API adapter for the fixed G031 Luna configuration.

The adapter consumes one validated G017 normalized request and performs one
stateless ``POST /v1/responses`` attempt.  It uses only the Python standard
library, disables redirects, requests strict structured JSON, and sends the
selected service tier without retaining the runtime credential anywhere in a
contract, diagnostic projection, or exception.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
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
from pathlib import Path
from typing import Any, Protocol, TypeAlias

from pa_engine.strategy.decision import (
    make_model_output_ref,
    map_candidate_to_normalized_model_candidate,
    parse_candidate,
)
from pa_engine.strategy.decision.schema import SCHEMA_PATH

from ..base import (
    AdapterOutcome,
    CapabilityDescriptor,
    ContractValidationError,
    FailureClassification,
    MalformedRequestError,
    RuntimeCredential,
    RuntimeCredentialResolver,
    cancellation_outcome,
    contains_secret_values,
    first_unsupported_capability,
    make_model_call_metadata,
    make_normalized_model_response,
    normalize_error,
    request_semantic_identity,
    unsupported_capability_outcome,
    validate_normalized_request,
)

from .config import (
    OPENAI_ADAPTER_VERSION,
    OPENAI_API_URL,
    OPENAI_MODEL,
    OPENAI_PROVIDER,
    OPENAI_REASONING_EFFORT,
    OPENAI_SERVICE_TIERS,
    OPENAI_SPEEDS,
    OpenAIConfig,
)


ADAPTER_VERSION = OPENAI_ADAPTER_VERSION
BASE_URL = OPENAI_API_URL
MODEL = OPENAI_MODEL
PROVIDER = OPENAI_PROVIDER


TransportCallable: TypeAlias = Callable[
    [str, Mapping[str, str], Mapping[str, Any], float],
    "OpenAITransportResponse",
]

_EFFECTIVE_SERVICE_TIERS = frozenset({"default", "fast", "priority"})
_RESPONSE_STATUSES = frozenset({"completed", "failed", "incomplete", "cancelled", "queued", "in_progress"})
_HTTP_CLASSIFICATIONS: dict[int, str] = {
    400: FailureClassification.INVALID_REQUEST.value,
    401: FailureClassification.AUTHENTICATION.value,
    402: FailureClassification.INSUFFICIENT_BALANCE.value,
    403: FailureClassification.AUTHENTICATION.value,
    408: FailureClassification.TIMEOUT.value,
    409: FailureClassification.PROVIDER.value,
    413: FailureClassification.INVALID_REQUEST.value,
    422: FailureClassification.INVALID_REQUEST.value,
    429: FailureClassification.RATE_LIMIT.value,
    500: FailureClassification.SERVER.value,
    502: FailureClassification.SERVER.value,
    503: FailureClassification.SERVER.value,
    504: FailureClassification.TIMEOUT.value,
}


def _classify_http_status(status: int) -> str:
    classification = _HTTP_CLASSIFICATIONS.get(status)
    if classification is not None:
        return classification
    if status >= 500:
        return FailureClassification.SERVER.value
    if status >= 400:
        return FailureClassification.INVALID_REQUEST.value
    # Redirects are terminal provider boundaries.  The default transport
    # refuses them, and an injected transport gets the same classification.
    return FailureClassification.PROVIDER.value


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _safe_int(value: Any) -> int | None:
    if type(value) is int and value >= 0:
        return value
    return None


def _status_code(value: Any) -> int:
    if type(value) is not int or not 100 <= value <= 599:
        raise ValueError("OpenAI transport status is malformed")
    return value


def _body_mapping(body: Any) -> Mapping[str, Any]:
    """Decode one JSON object while rejecting duplicate keys and constants."""

    if isinstance(body, Mapping):
        return body
    if not isinstance(body, (str, bytes, bytearray)):
        raise ValueError("OpenAI response body is not JSON")

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

    try:
        decoded = body.decode("utf-8") if isinstance(body, (bytes, bytearray)) else body
        value = json.loads(
            decoded,
            object_pairs_hook=reject_duplicates,
            parse_constant=reject_constant,
        )
    except (UnicodeError, TypeError, ValueError):
        raise ValueError("OpenAI response body is not valid JSON") from None
    if not isinstance(value, Mapping):
        raise ValueError("OpenAI response body is not a JSON object")
    return value


def _created_timestamp(value: Any) -> str | None:
    if type(value) is not int or value < 0:
        return None
    try:
        return _timestamp(datetime.fromtimestamp(value, tz=timezone.utc))
    except (OverflowError, OSError, ValueError):
        return None


def _secret_safe_text(value: str | None, secret_values: tuple[str, ...]) -> str | None:
    if not isinstance(value, str) or contains_secret_values(value, secret_values):
        return None
    return value


def _header_value(headers: Mapping[str, Any] | None, name: str) -> str | None:
    if not isinstance(headers, Mapping):
        return None
    wanted = name.lower()
    for key, value in headers.items():
        if str(key).lower() == wanted and isinstance(value, str):
            return value
    return None


@dataclass(frozen=True, slots=True)
class OpenAITransportResponse:
    """Small transport result used by the default and test transports."""

    status_code: int
    body: Any = field(repr=False)
    headers: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class OpenAITransportRequest:
    """Recorded request with a diagnostic projection that hides headers."""

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


class OpenAIHTTPError(Exception):
    """Internal HTTP status exception that never retains provider body text."""

    __slots__ = ("status_code",)

    def __init__(self, status_code: int) -> None:
        self.status_code = _status_code(status_code)
        super().__init__(f"OpenAI HTTP status {self.status_code}")


class OpenAITransport(Protocol):
    def __call__(
        self,
        url: str,
        headers: Mapping[str, str],
        payload: Mapping[str, Any],
        timeout: float,
    ) -> OpenAITransportResponse:
        ...


class OpenAINoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Refuse every redirect so Authorization cannot cross hosts."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        del req, fp, code, msg, headers, newurl
        return None


class UrllibOpenAITransport:
    """One-shot standard-library POST transport."""

    def __init__(self, *, opener: Any | None = None) -> None:
        self._opener = opener if opener is not None else urllib.request.build_opener(OpenAINoRedirectHandler())

    def __call__(
        self,
        url: str,
        headers: Mapping[str, str],
        payload: Mapping[str, Any],
        timeout: float,
    ) -> OpenAITransportResponse:
        body = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        request = urllib.request.Request(url, data=body, headers=dict(headers), method="POST")
        try:
            with self._opener.open(request, timeout=timeout) as response:
                raw = response.read()
                response_headers = {str(key): str(value) for key, value in response.headers.items()}
                return OpenAITransportResponse(int(response.status), raw, response_headers)
        except urllib.error.HTTPError as exc:
            # Do not retain body, Location, or arbitrary headers from an HTTP
            # error.  A redirect can contain a different host and must never
            # survive beside the Authorization-bearing request.
            try:
                exc.close()
            except Exception:
                pass
            status = exc.code
            del exc
            raise OpenAIHTTPError(status) from None


OpenAIHTTPTransport = UrllibOpenAITransport
UrllibTransport = UrllibOpenAITransport


def _load_structured_schema() -> dict[str, Any]:
    try:
        value = json.loads(Path(SCHEMA_PATH).read_text(encoding="utf-8"))
    except Exception:
        raise RuntimeError("G018 structured output schema is unavailable") from None
    if not isinstance(value, dict):
        raise RuntimeError("G018 structured output schema is malformed")
    return value


_G018_SCHEMA = _load_structured_schema()


_OPENAI_UNSUPPORTED_SCHEMA_KEYWORDS = frozenset(
    {
        "allOf",
        "not",
        "dependentRequired",
        "dependentSchemas",
        "if",
        "then",
        "else",
        # The live Responses strict-schema boundary rejects uniqueItems for
        # Luna.  G018 still enforces uniqueness locally after parsing.
        "uniqueItems",
    }
)
_OPENAI_SCHEMA_METADATA = frozenset({"$schema", "$id", "title"})


def _materialize_derived_grounding_ref(schema: dict[str, Any]) -> None:
    definitions = schema.get("$defs")
    if not isinstance(definitions, dict):
        raise RuntimeError("G018 structured output schema definitions are unavailable")
    grounding_ref = definitions.get("groundingRef")
    if not isinstance(grounding_ref, dict):
        raise RuntimeError("G018 grounding reference definition is unavailable")
    derived_ref = copy.deepcopy(grounding_ref)
    properties = derived_ref.get("properties")
    if not isinstance(properties, dict):
        raise RuntimeError("G018 grounding reference properties are malformed")
    properties["declaration_type"] = {"const": "derived_interpretation"}
    definitions["derivedGroundingRef"] = derived_ref


def _remove_openai_unsupported_schema_keywords(value: Any) -> Any:
    if isinstance(value, Mapping):
        result = {
            str(key): _remove_openai_unsupported_schema_keywords(item)
            for key, item in value.items()
            if key not in _OPENAI_UNSUPPORTED_SCHEMA_KEYWORDS and key not in _OPENAI_SCHEMA_METADATA
        }
        if "type" not in result and ("const" in result or "enum" in result):
            literals = [result["const"]] if "const" in result else result["enum"]
            if isinstance(literals, list) and literals:
                types: list[str] = []
                for literal in literals:
                    literal_type = (
                        "null"
                        if literal is None
                        else "boolean"
                        if isinstance(literal, bool)
                        else "integer"
                        if isinstance(literal, int)
                        else "number"
                        if isinstance(literal, float)
                        else "string"
                        if isinstance(literal, str)
                        else None
                    )
                    if literal_type is not None and literal_type not in types:
                        types.append(literal_type)
                if types:
                    result["type"] = types[0] if len(types) == 1 else types
        return result
    if isinstance(value, list):
        return [_remove_openai_unsupported_schema_keywords(item) for item in value]
    return copy.deepcopy(value)


def _make_openai_structured_schema() -> dict[str, Any]:
    schema = copy.deepcopy(_G018_SCHEMA)
    # The branch conditionals are semantic constraints enforced by the local
    # G018 parser/finalizer.  Responses Structured Outputs receives the common
    # strict object shape, so a candidate cannot be rejected by unsupported
    # composition keywords before local validation runs.
    schema.pop("allOf", None)
    _materialize_derived_grounding_ref(schema)
    return _remove_openai_unsupported_schema_keywords(schema)


_OPENAI_STRUCTURED_SCHEMA = _make_openai_structured_schema()


def _structured_output_format() -> dict[str, Any]:
    return {
        "type": "json_schema",
        "name": "pa_decision_candidate",
        "strict": True,
        "schema": copy.deepcopy(_OPENAI_STRUCTURED_SCHEMA),
    }


def _service_tier(value: Any) -> str:
    if not isinstance(value, str) or value not in _EFFECTIVE_SERVICE_TIERS:
        raise ValueError("OpenAI service tier is malformed")
    return value


def _usage_mapping(value: Any) -> tuple[dict[str, int], int | None, int | None]:
    if not isinstance(value, Mapping):
        raise ValueError("OpenAI usage is malformed")
    prompt = _safe_int(value.get("input_tokens"))
    completion = _safe_int(value.get("output_tokens"))
    total = _safe_int(value.get("total_tokens"))
    if prompt is None or completion is None or total is None or total != prompt + completion:
        raise ValueError("OpenAI usage is malformed")

    input_details = value.get("input_tokens_details")
    cached_input: int | None = None
    if input_details is not None:
        if not isinstance(input_details, Mapping):
            raise ValueError("OpenAI usage is malformed")
        cached_raw = input_details.get("cached_tokens")
        if cached_raw is not None:
            cached_input = _safe_int(cached_raw)
            if cached_input is None:
                raise ValueError("OpenAI usage is malformed")

    output_details = value.get("output_tokens_details")
    reasoning: int | None = None
    if output_details is not None:
        if not isinstance(output_details, Mapping):
            raise ValueError("OpenAI usage is malformed")
        reasoning_raw = output_details.get("reasoning_tokens")
        if reasoning_raw is not None:
            reasoning = _safe_int(reasoning_raw)
            if reasoning is None:
                raise ValueError("OpenAI usage is malformed")

    return (
        {"prompt_tokens": prompt, "completion_tokens": completion, "total_tokens": total},
        reasoning,
        cached_input,
    )


def _extract_output_text(output: Any) -> str:
    """Select exactly one completed assistant output-text block.

    Reasoning items are allowed because the Responses API may return them
    beside the final message.  Any second assistant message, refusal, tool
    call, or second text block makes the candidate ambiguous and is rejected.
    """

    if type(output) is not list or not output:
        raise ValueError("OpenAI response output is malformed")
    assistant_messages: list[Mapping[str, Any]] = []
    for item in output:
        if not isinstance(item, Mapping):
            raise ValueError("OpenAI response output item is malformed")
        item_type = item.get("type")
        if item_type == "reasoning":
            continue
        if item_type != "message" or item.get("role") != "assistant":
            raise ValueError("OpenAI response contains unsupported output items")
        assistant_messages.append(item)
    if len(assistant_messages) != 1:
        raise ValueError("OpenAI response has ambiguous assistant messages")
    message = assistant_messages[0]
    if "status" in message and message.get("status") != "completed":
        raise ValueError("OpenAI assistant message is incomplete")
    content = message.get("content")
    if type(content) is not list or len(content) != 1:
        raise ValueError("OpenAI assistant content is ambiguous")
    block = content[0]
    if not isinstance(block, Mapping) or block.get("type") != "output_text":
        raise ValueError("OpenAI assistant output is filtered or unsupported")
    text = block.get("text")
    if not isinstance(text, str) or not text.strip():
        raise ValueError("OpenAI assistant output text is empty")
    return text


@dataclass(frozen=True, slots=True)
class OpenAIMappedResponse:
    """Validated mapping of one official OpenAI Responses object."""

    candidate: dict[str, Any] | None
    normalized_candidate: dict[str, Any] | None
    finish_reason: str | None
    provider_request_id: str | None
    generated_at: str | None
    usage: dict[str, int] | None
    reasoning_tokens: int | None
    cached_input_tokens: int | None
    effective_service_tier: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate": copy.deepcopy(self.candidate),
            "normalized_candidate": copy.deepcopy(self.normalized_candidate),
            "finish_reason": self.finish_reason,
            "provider_request_id": self.provider_request_id,
            "generated_at": self.generated_at,
            "usage": copy.deepcopy(self.usage),
            "reasoning_tokens": self.reasoning_tokens,
            "cached_input_tokens": self.cached_input_tokens,
            "effective_service_tier": self.effective_service_tier,
        }

    as_dict = to_dict


def _response_telemetry(
    body: Any,
    secret_values: tuple[str, ...],
) -> tuple[str | None, str | None, str | None, dict[str, int] | None, int | None, int | None, str | None]:
    """Extract only safe telemetry when candidate extraction fails."""

    try:
        value = _body_mapping(body)
        if contains_secret_values(value, secret_values):
            return (None, None, None, None, None, None, None)
        status = value.get("status")
        finish_reason = status if isinstance(status, str) and status in _RESPONSE_STATUSES else None
        provider_request_id = _secret_safe_text(
            value.get("id") if isinstance(value.get("id"), str) else None,
            secret_values,
        )
        generated_at = _created_timestamp(value.get("created_at"))
        usage: dict[str, int] | None = None
        reasoning_tokens: int | None = None
        cached_input_tokens: int | None = None
        if "usage" in value:
            try:
                usage, reasoning_tokens, cached_input_tokens = _usage_mapping(value.get("usage"))
            except Exception:
                pass
        effective = None
        if isinstance(value.get("service_tier"), str) and value.get("service_tier") in _EFFECTIVE_SERVICE_TIERS:
            effective = value["service_tier"]
        return (
            finish_reason,
            provider_request_id,
            generated_at,
            usage,
            reasoning_tokens,
            cached_input_tokens,
            effective,
        )
    except Exception:
        return (None, None, None, None, None, None, None)


def map_openai_response(
    body: Any,
    *,
    profile_ref: Mapping[str, Any] | None = None,
    secret_values: tuple[str, ...] = (),
) -> OpenAIMappedResponse:
    """Validate the Responses envelope and the one G018 candidate text."""

    value = _body_mapping(body)
    if contains_secret_values(value, secret_values):
        raise ValueError("OpenAI response contains runtime credential data")
    if value.get("object") != "response":
        raise ValueError("OpenAI response object is unsupported")
    if value.get("model") != OPENAI_MODEL:
        raise ValueError("OpenAI response model is unsupported")
    if value.get("status") != "completed":
        raise ValueError("OpenAI response is incomplete")
    if value.get("incomplete_details") is not None:
        raise ValueError("OpenAI response contains incomplete details")
    if value.get("error") is not None:
        raise ValueError("OpenAI response contains an error")
    provider_request_id = _secret_safe_text(
        value.get("id") if isinstance(value.get("id"), str) else None,
        secret_values,
    )
    generated_at = _created_timestamp(value.get("created_at"))
    if generated_at is None:
        raise ValueError("OpenAI response creation timestamp is malformed")
    effective_service_tier = _service_tier(value.get("service_tier"))
    usage, reasoning_tokens, cached_input_tokens = _usage_mapping(value.get("usage"))
    text = _extract_output_text(value.get("output"))
    if contains_secret_values(text, secret_values):
        raise ValueError("OpenAI response contains runtime credential data")
    try:
        candidate = parse_candidate(text)
        if contains_secret_values(candidate, secret_values):
            raise ValueError("OpenAI response contains runtime credential data")
        normalized_candidate = map_candidate_to_normalized_model_candidate(candidate, profile_ref=profile_ref)
    except Exception:
        # Never retain model output text or its validation exception at this
        # boundary; only the typed malformed outcome is exposed by the adapter.
        raise ValueError("OpenAI candidate is invalid") from None
    return OpenAIMappedResponse(
        candidate=copy.deepcopy(candidate),
        normalized_candidate=copy.deepcopy(normalized_candidate),
        finish_reason="completed",
        provider_request_id=provider_request_id,
        generated_at=generated_at,
        usage=usage,
        reasoning_tokens=reasoning_tokens,
        cached_input_tokens=cached_input_tokens,
        effective_service_tier=effective_service_tier,
    )


map_response = map_openai_response
parse_openai_response = map_openai_response


class OpenAIResponseMapper:
    """Stateless object form of :func:`map_openai_response`."""

    @staticmethod
    def map(
        body: Any,
        *,
        profile_ref: Mapping[str, Any] | None = None,
        secret_values: tuple[str, ...] = (),
    ) -> OpenAIMappedResponse:
        return map_openai_response(body, profile_ref=profile_ref, secret_values=secret_values)

    __call__ = map


def _requested_service_tier(value: Any) -> str:
    if isinstance(value, str) and value in OPENAI_SPEEDS:
        return OPENAI_SERVICE_TIERS[value]
    if isinstance(value, str) and value in {"default", "fast"}:
        return value
    raise ValueError("OpenAI speed must be standard or fast")


def build_transport_payload(
    request: Mapping[str, Any],
    *,
    speed: str = "standard",
    service_tier: str | None = None,
) -> dict[str, Any]:
    """Map one normalized G017 request to the exact Responses JSON body."""

    result = validate_normalized_request(request)
    if not result.valid:
        raise MalformedRequestError()
    if request.get("provider") != OPENAI_PROVIDER or request.get("model") != OPENAI_MODEL:
        raise ValueError("normalized request is not for OpenAI GPT-5.6 Luna")
    if request.get("adapter") != OPENAI_ADAPTER_VERSION:
        raise ValueError("normalized request adapter is not the fixed OpenAI adapter")
    extensions = request.get("extensions")
    if not isinstance(extensions, Mapping) or extensions.get("reasoning_mode") != "enabled":
        raise ValueError("OpenAI requires enabled reasoning")
    parameters = request.get("parameters")
    if not isinstance(parameters, Mapping):
        raise ValueError("normalized request parameters are malformed")
    max_output_tokens = parameters.get("max_output_tokens")
    if type(max_output_tokens) is not int or max_output_tokens < 1:
        raise ValueError("normalized request max_output_tokens is malformed")
    if service_tier is not None:
        tier = _requested_service_tier(service_tier)
        if speed != "standard" and tier != _requested_service_tier(speed):
            raise ValueError("OpenAI speed and service_tier disagree")
    else:
        tier = _requested_service_tier(speed)

    # The payload deliberately contains no previous_response_id, conversation,
    # account state, provider reasoning transcript, or runtime credential.
    return {
        "model": OPENAI_MODEL,
        "instructions": request["instructions"],
        "input": request["context_text"],
        "reasoning": {"effort": OPENAI_REASONING_EFFORT},
        "text": {"format": _structured_output_format()},
        "max_output_tokens": max_output_tokens,
        "service_tier": tier,
        "store": False,
    }


build_request_payload = build_transport_payload
map_request_to_openai = build_transport_payload


class OpenAIRequestMapper:
    """Stateless object form of :func:`build_transport_payload`."""

    def __init__(self, *, speed: str = "standard") -> None:
        self.speed = speed

    def map(self, request: Mapping[str, Any]) -> dict[str, Any]:
        return build_transport_payload(request, speed=self.speed)

    __call__ = map


def _coerce_transport_result(value: Any) -> OpenAITransportResponse:
    if isinstance(value, OpenAITransportResponse):
        return OpenAITransportResponse(
            _status_code(value.status_code),
            value.body,
            value.headers if isinstance(value.headers, Mapping) else {},
        )
    if isinstance(value, (tuple, list)) and len(value) in {2, 3}:
        status, body = value[0], value[1]
        headers = value[2] if len(value) == 3 else {}
        return OpenAITransportResponse(_status_code(status), body, headers if isinstance(headers, Mapping) else {})
    if isinstance(value, Mapping):
        if ("status_code" in value or "status" in value) and "body" in value:
            return OpenAITransportResponse(
                _status_code(value.get("status_code", value.get("status"))),
                value.get("body"),
                value.get("headers") if isinstance(value.get("headers"), Mapping) else {},
            )
        return OpenAITransportResponse(200, value, {})
    status = getattr(value, "status_code", getattr(value, "status", None))
    if status is not None:
        body = getattr(value, "body", None)
        if body is None and hasattr(value, "read"):
            body = value.read()
        headers = getattr(value, "headers", {})
        return OpenAITransportResponse(_status_code(status), body, headers if isinstance(headers, Mapping) else {})
    raise ValueError("OpenAI transport returned an invalid response")


class RecordedTransport:
    """Queue/callback transport for deterministic offline conformance tests."""

    def __init__(self, responses: Any = ()) -> None:
        self._callback = responses if callable(responses) else None
        self._responses = list(responses) if not callable(responses) else []
        self.requests: list[OpenAITransportRequest] = []
        self._lock = threading.Lock()

    def send(self, request: OpenAITransportRequest) -> OpenAITransportResponse:
        with self._lock:
            self.requests.append(request)
            if self._callback is not None:
                raw = self._callback(request)
            elif self._responses:
                raw = self._responses.pop(0)
            else:
                raise OpenAIHTTPError(599)
        if isinstance(raw, Exception):
            raise raw
        return _coerce_transport_result(raw)


MockTransport = RecordedTransport
FixtureTransport = RecordedTransport
TransportRequest = OpenAITransportRequest
TransportResponse = OpenAITransportResponse
OpenAITransportError = OpenAIHTTPError


class OpenAIAdapter:
    """One-attempt OpenAI Responses adapter implementing G017."""

    _CAPABILITIES = CapabilityDescriptor(structured_json=True, reasoning_mode=True)

    def __init__(
        self,
        api_key: str | RuntimeCredential | None = None,
        *,
        credential: str | RuntimeCredential | None = None,
        credential_resolver: RuntimeCredentialResolver | Mapping[str, Any] | Callable[[str], Any] | None = None,
        resolver: RuntimeCredentialResolver | Mapping[str, Any] | Callable[[str], Any] | None = None,
        config: OpenAIConfig | None = None,
        transport: OpenAITransport | TransportCallable | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if api_key is not None and credential is not None:
            raise ValueError("pass api_key or credential, not both")
        if credential_resolver is not None and resolver is not None:
            raise ValueError("pass credential_resolver or resolver, not both")
        supplied = credential if credential is not None else api_key
        if supplied is not None and not isinstance(supplied, (str, RuntimeCredential)):
            raise ValueError("OpenAI credential must be a runtime key or handle")
        self._credential = self._credential_handle(supplied)
        supplied_resolver = credential_resolver if credential_resolver is not None else resolver
        if isinstance(supplied_resolver, Mapping) or callable(supplied_resolver):
            supplied_resolver = RuntimeCredentialResolver(supplied_resolver)
        self._resolver = supplied_resolver
        if self._resolver is not None and not isinstance(self._resolver, RuntimeCredentialResolver):
            raise ValueError("OpenAI resolver must be RuntimeCredentialResolver or a mapping")
        self._config = config or OpenAIConfig()
        self._transport: OpenAITransport | TransportCallable = transport or UrllibOpenAITransport()
        self._clock = clock or _utc_now
        self._attempt_lock = threading.Lock()
        self._attempt_count = 0

    @classmethod
    def from_env(
        cls,
        *,
        env_name: str = "OPENAI_API_KEY",
        **kwargs: Any,
    ) -> "OpenAIAdapter":
        return cls(api_key=os.environ.get(env_name), **kwargs)

    @staticmethod
    def _credential_handle(value: str | RuntimeCredential | None) -> RuntimeCredential | None:
        if isinstance(value, RuntimeCredential):
            return value
        if isinstance(value, str) and value:
            return RuntimeCredential(OPENAI_PROVIDER, {"api_key": value})
        return None

    @property
    def capabilities(self) -> CapabilityDescriptor:
        return self._CAPABILITIES

    @property
    def config(self) -> OpenAIConfig:
        return self._config

    @property
    def attempt_count(self) -> int:
        with self._attempt_lock:
            return self._attempt_count

    @property
    def calls(self) -> int:
        return self.attempt_count

    def __repr__(self) -> str:
        return "OpenAIAdapter(model='gpt-5.6-luna', credential=<runtime-only>)"

    __str__ = __repr__

    def _runtime_credential(self) -> RuntimeCredential | None:
        if self._resolver is not None:
            try:
                return self._resolver.resolve(OPENAI_PROVIDER)
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
        return value, (value,)

    def _next_attempt(self) -> None:
        with self._attempt_lock:
            self._attempt_count += 1

    def _invoke_transport(
        self,
        payload: Mapping[str, Any],
        api_key: str,
    ) -> OpenAITransportResponse:
        headers = {
            "Authorization": "Bearer " + api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        request = OpenAITransportRequest("POST", self._config.endpoint, headers, payload, self._config.timeout_seconds)
        sender = getattr(self._transport, "send", None)
        if callable(sender):
            result = sender(request)
        else:
            result = self._transport(self._config.endpoint, headers, payload, self._config.timeout_seconds)
        return _coerce_transport_result(result)

    def _metadata_extensions(
        self,
        *,
        effective_service_tier: str | None = None,
        reasoning_tokens: int | None = None,
        cached_input_tokens: int | None = None,
    ) -> dict[str, Any]:
        return {
            "openai": {
                "requested_service_tier": self._config.requested_service_tier,
                "effective_service_tier": effective_service_tier,
                "reasoning_tokens": reasoning_tokens,
                "cached_input_tokens": cached_input_tokens,
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
        effective_service_tier: str | None = None,
        reasoning_tokens: int | None = None,
        cached_input_tokens: int | None = None,
        profile_ref: Mapping[str, Any] | None = None,
    ) -> AdapterOutcome:
        normalized_classification = classification
        if normalized_classification == FailureClassification.SUCCESS.value:
            if not isinstance(candidate, Mapping) or not isinstance(normalized_candidate, Mapping):
                normalized_classification = FailureClassification.MALFORMED_RESPONSE.value
        error = None
        response_extensions: dict[str, Any] = {
            "openai": {
                "requested_service_tier": self._config.requested_service_tier,
                "effective_service_tier": effective_service_tier,
            }
        }
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
                effective_service_tier=effective_service_tier,
                reasoning_tokens=reasoning_tokens,
                cached_input_tokens=cached_input_tokens,
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
        if cancelled:
            return cancellation_outcome(request, secret_values=secret_values, generation=generation)
        capability = first_unsupported_capability(request, self.capabilities)
        if capability is not None:
            return unsupported_capability_outcome(request, capability, secret_values=secret_values, generation=generation)

        extensions = request.get("extensions")
        if (
            request.get("provider") != OPENAI_PROVIDER
            or request.get("model") != OPENAI_MODEL
            or request.get("adapter") != OPENAI_ADAPTER_VERSION
            or not isinstance(extensions, Mapping)
            or extensions.get("reasoning_mode") != "enabled"
        ):
            return self._outcome(
                request,
                classification=FailureClassification.INVALID_CONFIGURATION.value,
                secret_values=secret_values,
                generation=generation,
                started_at=None,
                completed_at=None,
                latency_ms=None,
                error_message="OpenAI configuration does not match the fixed Luna/Max runtime",
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
            payload = build_transport_payload(request, service_tier=self._config.requested_service_tier)
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
        if isinstance(extensions.get("strategy_profile_ref"), Mapping):
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
        except (asyncio.CancelledError, concurrent.futures.CancelledError):
            return cancellation_outcome(request, secret_values=secret_values, generation=generation)
        except OpenAIHTTPError as exc:
            start_text, end_text, latency = self._timing(started)
            status = exc.status_code
            del exc
            classification = _classify_http_status(status)
            return self._outcome(
                request,
                classification=classification,
                secret_values=secret_values,
                generation=generation,
                started_at=start_text,
                completed_at=end_text,
                latency_ms=latency,
                error_details=f"OpenAI HTTP status {status}",
                transient=classification == FailureClassification.SERVER.value,
            )
        except urllib.error.HTTPError as exc:
            status = exc.code if type(exc.code) is int else 599
            try:
                exc.close()
            except Exception:
                pass
            del exc
            start_text, end_text, latency = self._timing(started)
            classification = _classify_http_status(status) if 100 <= status <= 599 else FailureClassification.TRANSPORT.value
            return self._outcome(
                request,
                classification=classification,
                secret_values=secret_values,
                generation=generation,
                started_at=start_text,
                completed_at=end_text,
                latency_ms=latency,
                error_details=f"OpenAI HTTP status {status}",
                transient=classification == FailureClassification.SERVER.value,
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
            is_timeout = isinstance(getattr(exc, "reason", None), (socket.timeout, TimeoutError))
            del exc
            start_text, end_text, latency = self._timing(started)
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
        except Exception:
            # Test/runtime transports are untrusted boundaries.  Discard the
            # exception object instead of retaining provider text or a key.
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
        provider_request_id = _secret_safe_text(
            _header_value(transport_result.headers, "x-request-id"),
            secret_values,
        )
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
                error_details=f"OpenAI HTTP status {status_code}",
                transient=classification == FailureClassification.SERVER.value,
                provider_request_id=provider_request_id,
            )
        try:
            mapped = map_openai_response(
                transport_result.body,
                profile_ref=profile_ref,
                secret_values=secret_values,
            )
        except Exception:
            (
                failed_finish_reason,
                failed_provider_request_id,
                failed_generated_at,
                failed_usage,
                failed_reasoning_tokens,
                failed_cached_input_tokens,
                failed_effective_service_tier,
            ) = _response_telemetry(transport_result.body, secret_values)
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
                effective_service_tier=failed_effective_service_tier,
                reasoning_tokens=failed_reasoning_tokens,
                cached_input_tokens=failed_cached_input_tokens,
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
            effective_service_tier=mapped.effective_service_tier,
            reasoning_tokens=mapped.reasoning_tokens,
            cached_input_tokens=mapped.cached_input_tokens,
            profile_ref=profile_ref,
        )

    call = complete


OpenAIResponsesAdapter = OpenAIAdapter
OpenAIGPT56LunaAdapter = OpenAIAdapter


__all__ = [
    "ADAPTER_VERSION",
    "BASE_URL",
    "MODEL",
    "PROVIDER",
    "FixtureTransport",
    "MockTransport",
    "OpenAIAdapter",
    "OpenAIHTTPError",
    "OpenAIHTTPTransport",
    "OpenAINoRedirectHandler",
    "OpenAIGPT56LunaAdapter",
    "OpenAIRequestMapper",
    "OpenAIResponseMapper",
    "OpenAIResponsesAdapter",
    "OpenAIMappedResponse",
    "OpenAITransport",
    "OpenAITransportError",
    "OpenAITransportRequest",
    "OpenAITransportResponse",
    "RecordedTransport",
    "TransportRequest",
    "TransportResponse",
    "UrllibOpenAITransport",
    "UrllibTransport",
    "build_request_payload",
    "build_transport_payload",
    "map_openai_response",
    "map_request_to_openai",
    "map_response",
    "parse_openai_response",
]
