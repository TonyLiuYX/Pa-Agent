"""Small provider-neutral model adapter boundary for G017.

The module deliberately deals in the frozen G014 JSON-like dictionaries.  It
does not know how a provider transports a request and it never accepts a
credential as part of a normalized contract.  Deterministic adapters in this
package use the helpers here to make, validate and identify those dictionaries.
"""

from __future__ import annotations

import copy
import asyncio
import concurrent.futures
import hashlib
import json
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Protocol, TypeAlias, runtime_checkable

from pa_engine.contracts.canonical import (
    canonical_json_bytes,
    content_hash,
    finalize_artifact,
    semantic_projection,
)
from pa_engine.contracts.validation import ValidationIssue, ValidationResult, validate_artifact


SCHEMA_VERSION = "0.1.0"
ADAPTER_VERSION = "g017-provider-neutral-0.1.0"

FROZEN_NORMALIZED_ERROR_CODES = frozenset(
    {
        "model_timeout",
        "rate_limit",
        "authentication_error",
        "transport_error",
        "provider_error",
        "invalid_model_response",
        "unsupported_capability",
        "grounding_failure",
        "internal_error",
    }
)


class FailureClassification(str, Enum):
    """Typed adapter-level failure classes.

    G014 intentionally has no cancellation or late-response error code.  These
    values therefore live on :class:`AdapterOutcome`, while the serialized
    response uses a frozen normalized error code.
    """

    SUCCESS = "success"
    TIMEOUT = "timeout"
    AUTHENTICATION = "authentication"
    RATE_LIMIT = "rate_limit"
    SERVER = "server"
    PROVIDER = "provider"
    TRANSPORT = "transport"
    CANCELLATION = "cancellation"
    LATE_RESPONSE = "late_response"
    MALFORMED_RESPONSE = "malformed_response"
    GROUNDING_FAILURE = "grounding_failure"
    INVALID_CONFIGURATION = "invalid_configuration"
    INVALID_CONFIG = "invalid_configuration"
    INVALID_REQUEST = "invalid_request"
    INSUFFICIENT_BALANCE = "insufficient_balance"
    UNSUPPORTED_CAPABILITY = "unsupported_capability"
    REPLAY_MISSING = "replay_missing"
    REPLAY_MISMATCH = "replay_mismatch"
    SCRIPT_EXHAUSTED = "script_exhausted"
    INTERNAL = "internal"


# Both spellings are useful at an integration boundary and keep the public API
# friendly to callers that think in terms of errors rather than failures.
ErrorClassification = FailureClassification


class AdapterError(Exception):
    """Base exception whose text is always sanitized before it is exposed."""

    def __init__(
        self,
        message: str = "provider adapter error",
        *,
        classification: str | FailureClassification = FailureClassification.INTERNAL,
        secret_values: Sequence[str] = (),
    ) -> None:
        self.classification = _classification_value(classification)
        self.safe_message = redact_message(message, secret_values=secret_values)
        super().__init__(self.safe_message)


class ContractValidationError(AdapterError):
    """Raised at the adapter/contract boundary without echoing input data."""

    def __init__(self, kind: str, result: ValidationResult) -> None:
        # jsonschema messages can contain an invalid input value.  Keep only
        # stable codes/paths in the exception's retained result.
        safe_issues = [ValidationIssue(issue.code, issue.path, "contract validation failed") for issue in result.issues]
        self.kind = kind
        self.result = ValidationResult(False, result.artifact_kind, safe_issues)
        super().__init__(
            f"{kind} contract validation failed",
            classification=FailureClassification.INVALID_REQUEST,
        )


class MalformedRequestError(ContractValidationError):
    """A request was not a valid frozen normalized model request."""

    def __init__(self, message: str = "normalized model request is malformed", *, secret_values: Sequence[str] = ()) -> None:
        result = ValidationResult(
            False,
            "normalized_model_request",
            [ValidationIssue("invalid_request", "$.normalized_model_request", "normalized request rejected")],
        )
        # Do not retain caller text.  A malformed request often contains the
        # exact credential-bearing value that must never survive on an
        # exception object (including ``.message``, ``__dict__`` or pickle).
        # Keep the public attribute for backwards compatibility, but make it
        # a fixed, non-input-derived value.
        del message, secret_values
        self.message = "normalized model request is malformed"
        super().__init__("normalized_model_request", result)

    def __reduce__(self) -> Any:
        # Exception pickling normally replays ``args``.  Reconstruct this
        # class with its fixed message and never with a caller-supplied value.
        return (type(self), ())


class MalformedResponseError(AdapterError):
    """A response/candidate could not be normalized into G014."""


class CredentialResolutionError(AdapterError):
    """Credential lookup failed without disclosing the credential value."""

    def __init__(self, message: str = "runtime credential resolution failed") -> None:
        super().__init__(message, classification=FailureClassification.AUTHENTICATION)


class CapabilityDescriptor:
    """Provider-neutral capabilities consumed by later adapter integrations."""

    __slots__ = ("structured_json", "reasoning_mode")

    def __init__(self, *, structured_json: bool = True, reasoning_mode: bool = True) -> None:
        self.structured_json = bool(structured_json)
        self.reasoning_mode = bool(reasoning_mode)

    def to_dict(self) -> dict[str, bool]:
        return {
            "structured_json": self.structured_json,
            "reasoning_mode": self.reasoning_mode,
        }

    as_dict = to_dict

    def supports(self, capability: str) -> bool:
        if capability == "structured_json":
            return self.structured_json
        if capability == "reasoning_mode":
            return self.reasoning_mode
        return False

    def __repr__(self) -> str:
        return (
            "CapabilityDescriptor("
            f"structured_json={self.structured_json!r}, "
            f"reasoning_mode={self.reasoning_mode!r})"
        )


@runtime_checkable
class ModelAdapter(Protocol):
    """Minimal synchronous adapter protocol.

    Implementations receive and return normalized contract dictionaries only.
    An implementation must validate the normalized request and reject any
    known runtime secret values before it evaluates requested capabilities.
    The gateway deliberately does not perform that capability/secret
    preflight; this ordering is the adapter's boundary.
    ``cancelled`` and ``generation`` are optional scheduler hints; they are
    intentionally not provider-specific fields.
    """

    @property
    def capabilities(self) -> CapabilityDescriptor:
        ...

    def complete(
        self,
        request: Mapping[str, Any],
        *,
        cancelled: bool = False,
        generation: int | None = None,
    ) -> "AdapterOutcome":
        ...


ProviderAdapter = ModelAdapter
AdapterProtocol = ModelAdapter


class RuntimeCredential:
    """Ephemeral credential handle; it is intentionally not serializable."""

    __slots__ = ("provider", "_values", "_secret_values")

    def __init__(self, provider: str, values: Any) -> None:
        self.provider = provider
        self._values = copy.deepcopy(values)
        self._secret_values = tuple(_string_values(self._values))

    @property
    def secret_values(self) -> tuple[str, ...]:
        return self._secret_values

    def get(self, key: str, default: Any = None) -> Any:
        if isinstance(self._values, Mapping):
            return self._values.get(key, default)
        return default

    def as_runtime_value(self) -> Any:
        """Return a copy for the transport boundary, never for a contract."""

        return copy.deepcopy(self._values)

    def __repr__(self) -> str:
        return f"RuntimeCredential(provider={self.provider!r}, value=<redacted>)"

    __str__ = __repr__

    def __getstate__(self) -> dict[str, str]:
        raise TypeError("runtime credentials cannot be serialized")

    def __reduce__(self) -> Any:
        raise TypeError("runtime credentials cannot be serialized")


class RuntimeCredentialResolver:
    """Resolve credentials at runtime without putting them in project data.

    ``source`` may be a mapping keyed by provider or a callable accepting a
    provider name.  The returned handle deliberately has no JSON/dict export.
    """

    __slots__ = ("_source",)

    def __init__(self, source: Mapping[str, Any] | Callable[[str], Any]) -> None:
        if not isinstance(source, Mapping) and not callable(source):
            raise TypeError("credential source must be a mapping or callable")
        self._source = source

    def resolve(self, provider: str) -> RuntimeCredential | None:
        source_failed = False
        value: Any = None
        try:
            value = self._source(provider) if callable(self._source) else self._source.get(provider)
        except Exception:
            # Do not retain the source exception (or its secret-bearing
            # arguments) on the public error's context or traceback.
            source_failed = True
        if source_failed:
            raise CredentialResolutionError()
        if value is None:
            return None
        construction_failed = False
        credential: RuntimeCredential | None = None
        try:
            credential = RuntimeCredential(provider, value)
        except Exception:
            # Clear this local before raising outside the except block so
            # tracebacks that capture locals cannot retain a credential map.
            value = None
            construction_failed = True
        if construction_failed:
            raise CredentialResolutionError()
        return credential

    def require(self, provider: str) -> RuntimeCredential:
        credential = self.resolve(provider)
        if credential is None:
            raise CredentialResolutionError()
        return credential

    def __repr__(self) -> str:
        return "RuntimeCredentialResolver(<runtime-only>)"

    __str__ = __repr__

    def __getstate__(self) -> dict[str, str]:
        raise TypeError("credential resolvers cannot be serialized")

    def __reduce__(self) -> Any:
        raise TypeError("credential resolvers cannot be serialized")


EphemeralCredentialResolver = RuntimeCredentialResolver
StaticCredentialResolver = RuntimeCredentialResolver


_SECRET_EXACT = {
    "apikey",
    "apitoken",
    "apisecret",
    "accesskey",
    "accesstoken",
    "authtoken",
    "bearertoken",
    "refreshtoken",
    "clientsecret",
    "clientcredential",
    "authorization",
    "password",
    "passwd",
    "credential",
    "credentials",
    "secret",
    "token",
    "privatekey",
    "signingkey",
    "signingsecret",
    "webhooksecret",
    "oauthsecret",
    "oauthaccesstoken",
    "oauthrefreshtoken",
}
_SECRET_SUFFIXES = (
    "apikey",
    "apitoken",
    "apisecret",
    "accesstoken",
    "authtoken",
    "bearertoken",
    "refreshtoken",
    "clientsecret",
    "credential",
    "credentials",
    "authorization",
    "password",
    "passwd",
    "privatekey",
    "signingkey",
    "signingsecret",
    "webhooksecret",
)
_SECRET_SAFE_KEYS = {
    "prompttokens",
    "completiontokens",
    "totaltokens",
    "maxoutputtokens",
    "tokencount",
    "tokenusage",
    "tokenbudget",
}
_SECRET_TEXT_PATTERNS = (
    re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]+"),
    re.compile(r"(?i)\b(?:api[_ -]?key|api[_ -]?token|client[_ -]?secret|password|credential|authorization|secret|token)\s*[:=]\s*[^\s,;]+"),
    re.compile(r"(?i)\b(?:sk|ds)-[A-Za-z0-9._~+/=-]{8,}"),
)
_REDACTED = "[REDACTED]"


def _string_leaves(value: Any) -> list[str]:
    found: list[str] = []
    if isinstance(value, str) and value:
        found.append(value)
    elif isinstance(value, Mapping):
        for key, item in value.items():
            found.extend(_string_leaves(key))
            found.extend(_string_leaves(item))
    elif isinstance(value, (list, tuple)):
        for item in value:
            found.extend(_string_leaves(item))
    return found


def _string_values(value: Any) -> list[str]:
    """Collect recursive string mapping keys and values for credentials."""

    found: list[str] = []
    if isinstance(value, str) and value:
        found.append(value)
    elif isinstance(value, Mapping):
        for key, item in value.items():
            if isinstance(key, str) and key:
                found.append(key)
            found.extend(_string_values(item))
    elif isinstance(value, (list, tuple)):
        for item in value:
            found.extend(_string_values(item))
    return found


def contains_secret_values(value: Any, secret_values: Sequence[str] = ()) -> bool:
    """Return whether any explicitly supplied runtime secret occurs in *value*."""

    needles = tuple(secret for secret in secret_values if isinstance(secret, str) and secret)
    if not needles:
        return False
    return any(secret in text for text in _string_leaves(value) for secret in needles)


def _is_secret_key(key: Any) -> bool:
    if not isinstance(key, str):
        return False
    normalized = re.sub(r"[^a-z0-9]", "", key.lower())
    return normalized in _SECRET_EXACT or (
        normalized not in _SECRET_SAFE_KEYS
        and any(normalized.endswith(suffix) for suffix in _SECRET_SUFFIXES)
    )


def redact_message(message: Any, *, secret_values: Sequence[str] = ()) -> str:
    """Return a safe message with known and common credential forms removed."""

    text = str(message)
    values = sorted({value for value in secret_values if isinstance(value, str) and value}, key=len, reverse=True)
    for value in values:
        text = text.replace(value, _REDACTED)
    for pattern in _SECRET_TEXT_PATTERNS:
        text = pattern.sub(_REDACTED, text)
    return text


def redact_exception(exc: BaseException, *, secret_values: Sequence[str] = ()) -> str:
    """Sanitize an exception's type and message for logs or error details."""

    return f"{type(exc).__name__}: {redact_message(exc, secret_values=secret_values)}"


def redact_secrets(value: Any, *, secret_values: Sequence[str] = ()) -> Any:
    """Recursively redact dict/list/tuple/exception values."""

    if isinstance(value, BaseException):
        return {
            "type": type(value).__name__,
            "message": redact_exception(value, secret_values=secret_values),
        }
    if isinstance(value, Mapping):
        return {
            redact_message(key, secret_values=secret_values) if isinstance(key, str) else redact_secrets(key, secret_values=secret_values):
            _REDACTED if _is_secret_key(key) else redact_secrets(item, secret_values=secret_values)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact_secrets(item, secret_values=secret_values) for item in value]
    if isinstance(value, tuple):
        return tuple(redact_secrets(item, secret_values=secret_values) for item in value)
    if isinstance(value, str):
        return redact_message(value, secret_values=secret_values)
    return value


def redacted_json(value: Any, *, secret_values: Sequence[str] = ()) -> str:
    """Serialize a recursively redacted value as deterministic JSON."""

    safe = redact_secrets(value, secret_values=secret_values)
    return json.dumps(safe, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=lambda item: redact_message(repr(item), secret_values=secret_values))


safe_json = redacted_json


def _classification_value(value: str | FailureClassification) -> str:
    return value.value if isinstance(value, FailureClassification) else str(value)


_KIND_ALIASES = {
    "success": FailureClassification.SUCCESS.value,
    "completed": FailureClassification.SUCCESS.value,
    "timeout": FailureClassification.TIMEOUT.value,
    "timed_out": FailureClassification.TIMEOUT.value,
    "model_timeout": FailureClassification.TIMEOUT.value,
    "authentication": FailureClassification.AUTHENTICATION.value,
    "auth": FailureClassification.AUTHENTICATION.value,
    "authentication_error": FailureClassification.AUTHENTICATION.value,
    "credential": FailureClassification.AUTHENTICATION.value,
    "credentials": FailureClassification.AUTHENTICATION.value,
    "rate-limit": FailureClassification.RATE_LIMIT.value,
    "rate limit": FailureClassification.RATE_LIMIT.value,
    "rate_limit": FailureClassification.RATE_LIMIT.value,
    "server_error": FailureClassification.SERVER.value,
    "server": FailureClassification.SERVER.value,
    "provider_error": FailureClassification.PROVIDER.value,
    "provider": FailureClassification.PROVIDER.value,
    "transport_error": FailureClassification.TRANSPORT.value,
    "transport": FailureClassification.TRANSPORT.value,
    "cancellation": FailureClassification.CANCELLATION.value,
    "cancelled": FailureClassification.CANCELLATION.value,
    "canceled": FailureClassification.CANCELLATION.value,
    "cancel": FailureClassification.CANCELLATION.value,
    "late": FailureClassification.LATE_RESPONSE.value,
    "late_response": FailureClassification.LATE_RESPONSE.value,
    "malformed": FailureClassification.MALFORMED_RESPONSE.value,
    "malformed_response": FailureClassification.MALFORMED_RESPONSE.value,
    "invalid_model_response": FailureClassification.MALFORMED_RESPONSE.value,
    "grounding": FailureClassification.GROUNDING_FAILURE.value,
    "grounding_failure": FailureClassification.GROUNDING_FAILURE.value,
    "configuration": FailureClassification.INVALID_CONFIGURATION.value,
    "config": FailureClassification.INVALID_CONFIGURATION.value,
    "invalid_config": FailureClassification.INVALID_CONFIGURATION.value,
    "invalid_configuration": FailureClassification.INVALID_CONFIGURATION.value,
    "invalid_request": FailureClassification.INVALID_REQUEST.value,
    "insufficient_balance": FailureClassification.INSUFFICIENT_BALANCE.value,
    "insufficient_funds": FailureClassification.INSUFFICIENT_BALANCE.value,
    "unsupported": FailureClassification.UNSUPPORTED_CAPABILITY.value,
    "unsupported_capability": FailureClassification.UNSUPPORTED_CAPABILITY.value,
    "replay_missing": FailureClassification.REPLAY_MISSING.value,
    "replay_mismatch": FailureClassification.REPLAY_MISMATCH.value,
    "script_exhausted": FailureClassification.SCRIPT_EXHAUSTED.value,
    "internal": FailureClassification.INTERNAL.value,
    "internal_error": FailureClassification.INTERNAL.value,
}
_TERMINAL_CLASSIFICATIONS = frozenset(
    {
        FailureClassification.AUTHENTICATION.value,
        FailureClassification.CANCELLATION.value,
        FailureClassification.LATE_RESPONSE.value,
        FailureClassification.MALFORMED_RESPONSE.value,
        FailureClassification.GROUNDING_FAILURE.value,
        FailureClassification.INVALID_CONFIGURATION.value,
        FailureClassification.INVALID_REQUEST.value,
        FailureClassification.INSUFFICIENT_BALANCE.value,
        FailureClassification.UNSUPPORTED_CAPABILITY.value,
        FailureClassification.REPLAY_MISSING.value,
        FailureClassification.REPLAY_MISMATCH.value,
        FailureClassification.SCRIPT_EXHAUSTED.value,
        FailureClassification.INTERNAL.value,
    }
)


def _normalized_kind(kind: str | FailureClassification) -> str:
    key = _classification_value(kind).strip().lower().replace(" ", "_")
    return _KIND_ALIASES.get(key, key)


def normalize_classification(kind: str | FailureClassification) -> str:
    """Return the canonical provider-neutral classification name."""

    normalized = _normalized_kind(kind)
    return normalized if normalized in _KIND_ALIASES.values() else FailureClassification.INTERNAL.value


def _error_code_for(kind: str) -> str:
    return {
        FailureClassification.TIMEOUT.value: "model_timeout",
        FailureClassification.AUTHENTICATION.value: "authentication_error",
        FailureClassification.RATE_LIMIT.value: "rate_limit",
        FailureClassification.SERVER.value: "provider_error",
        FailureClassification.PROVIDER.value: "provider_error",
        FailureClassification.TRANSPORT.value: "transport_error",
        FailureClassification.MALFORMED_RESPONSE.value: "invalid_model_response",
        FailureClassification.GROUNDING_FAILURE.value: "grounding_failure",
        FailureClassification.UNSUPPORTED_CAPABILITY.value: "unsupported_capability",
    }.get(kind, "internal_error")


def _default_retryable(kind: str, *, transient: bool | None) -> bool:
    # Timeout and rate-limit are the only classes that are intrinsically
    # retryable.  A provider/server/transport error is retryable only when the
    # transport explicitly established that it was transient.  In particular,
    # an arbitrary ``retryable=true`` bit is not evidence of transience.
    if kind in {
        FailureClassification.TIMEOUT.value,
        FailureClassification.RATE_LIMIT.value,
    }:
        return True
    if kind in {
        FailureClassification.SERVER.value,
        FailureClassification.PROVIDER.value,
        FailureClassification.TRANSPORT.value,
    }:
        return transient is True
    if kind in _TERMINAL_CLASSIFICATIONS:
        return False
    return False


def normalize_error(
    kind: str | FailureClassification,
    *,
    message: str | None = None,
    details: str | None = None,
    retryable: bool | None = None,
    transient: bool | None = None,
    exception: BaseException | None = None,
    secret_values: Sequence[str] = (),
) -> dict[str, Any]:
    """Map an adapter failure to a frozen G014 ``normalizedError`` dict.

    Unknown classifications remain adapter-level ``internal_error`` values,
    rather than being silently interpreted as retryable provider failures.
    """

    normalized = _normalized_kind(kind)
    if normalized not in _KIND_ALIASES.values():
        normalized = FailureClassification.INTERNAL.value
    if message is None:
        if exception is not None:
            message = redact_exception(exception, secret_values=secret_values)
        else:
            message = {
                FailureClassification.TIMEOUT.value: "model call timed out",
                FailureClassification.AUTHENTICATION.value: "model authentication failed",
                FailureClassification.RATE_LIMIT.value: "model provider rate limit reached",
                FailureClassification.SERVER.value: "model provider server failure",
                FailureClassification.PROVIDER.value: "model provider failure",
                FailureClassification.TRANSPORT.value: "model transport failed",
                FailureClassification.GROUNDING_FAILURE.value: "model grounding failed",
                FailureClassification.CANCELLATION.value: "model call was cancelled",
                FailureClassification.LATE_RESPONSE.value: "model response arrived after cancellation or deadline",
                FailureClassification.MALFORMED_RESPONSE.value: "model response was malformed",
                FailureClassification.INVALID_CONFIGURATION.value: "model adapter configuration is invalid",
                FailureClassification.INVALID_REQUEST.value: "model request is invalid",
                FailureClassification.INSUFFICIENT_BALANCE.value: "model provider balance is insufficient",
                FailureClassification.UNSUPPORTED_CAPABILITY.value: "requested model capability is unsupported",
                FailureClassification.REPLAY_MISSING.value: "replay recording is missing",
                FailureClassification.REPLAY_MISMATCH.value: "replay recording does not match request",
                FailureClassification.SCRIPT_EXHAUSTED.value: "scripted adapter sequence is exhausted",
            }.get(normalized, "model adapter internal error")
    safe_message = redact_message(message, secret_values=secret_values)
    safe_details = None if details is None else redact_message(details, secret_values=secret_values)

    # Credential/configuration/capability classes are forced terminal even if
    # a caller accidentally supplies retryable=True.  For provider-like
    # classes the explicit transient bit is the only positive retry evidence;
    # ``retryable=True`` alone is intentionally ignored.
    is_retryable = _default_retryable(normalized, transient=transient)
    if retryable is not None and normalized in {
        FailureClassification.TIMEOUT.value,
        FailureClassification.RATE_LIMIT.value,
    }:
        is_retryable = True
    error: dict[str, Any] = {
        "code": _error_code_for(normalized),
        "message": safe_message or "model adapter error",
        "retryable": is_retryable,
    }
    if details is not None:
        error["details"] = safe_details
    # Preserve classes absent from frozen G014 as an adapter-neutral typed
    # extension.  Server/provider are both represented by ``provider_error``
    # in G014, so retaining the distinction is useful at the adapter seam too.
    if normalized not in {
        "timeout",
        "authentication",
        "rate_limit",
        "transport",
        "malformed_response",
        "grounding_failure",
        "unsupported_capability",
    }:
        error["extensions"] = {"provider_neutral_classification": normalized}
    if normalized in {
        FailureClassification.SERVER.value,
        FailureClassification.PROVIDER.value,
        FailureClassification.TRANSPORT.value,
    } and transient is not None:
        error.setdefault("extensions", {})["transient"] = bool(transient)
    return error


def normalize_exception(
    exception: BaseException,
    *,
    secret_values: Sequence[str] = (),
) -> dict[str, Any]:
    """Normalize common stdlib exceptions without exposing their arguments."""

    if isinstance(exception, (asyncio.CancelledError, concurrent.futures.CancelledError)):
        kind = FailureClassification.CANCELLATION
    elif isinstance(exception, (TimeoutError,)):  # asyncio.TimeoutError aliases this on supported Python.
        kind = FailureClassification.TIMEOUT
    elif isinstance(exception, (PermissionError,)):
        kind = FailureClassification.AUTHENTICATION
    elif isinstance(exception, (ConnectionError, OSError)):
        kind = FailureClassification.TRANSPORT
    elif isinstance(exception, (ValueError, TypeError, json.JSONDecodeError)):
        kind = FailureClassification.MALFORMED_RESPONSE
    else:
        kind = FailureClassification.INTERNAL
    return normalize_error(kind, exception=exception, secret_values=secret_values)


def _safe_copy(value: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise TypeError("contract value must be a mapping")
    return copy.deepcopy(dict(value))


_EMPTY_CONTEXT_REFS = {
    "analysis_request_id": "arq_" + "0" * 64,
    "analysis_request_hash": "0" * 64,
    "market_snapshot_id": "mkt_" + "0" * 64,
    "market_snapshot_hash": "0" * 64,
    "observation_frame_id": "obs_" + "0" * 64,
    "observation_frame_hash": "0" * 64,
    "evidence_package_id": "evp_" + "0" * 64,
    "evidence_package_hash": "0" * 64,
}


def _prompt_hash(prompt_template_version: str, instructions: str, context_text: str) -> str:
    payload = {
        "prompt_template_version": prompt_template_version,
        "instructions": instructions,
        "context_text": context_text,
    }
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _assert_valid(document: Mapping[str, Any], kind: str, *, related: Mapping[str, Any] | None = None) -> dict[str, Any]:
    if kind == "normalized_model_request" and related is None:
        result = validate_normalized_request(document)
    else:
        result = validate_artifact(document, expected_kind=kind, related_artifacts=related)
        if result.valid and kind in {"normalized_model_response", "model_call_metadata"}:
            provider_issues = _provider_neutral_extension_issues(document)
            if provider_issues:
                result = ValidationResult(False, result.artifact_kind, [*result.issues, *provider_issues])
            elif kind == "normalized_model_response":
                consistency_issues = _response_consistency_issues(document)
                if consistency_issues:
                    result = ValidationResult(False, result.artifact_kind, [*result.issues, *consistency_issues])
    if not result.valid:
        raise ContractValidationError(kind, result)
    return copy.deepcopy(dict(document))


_PROVIDER_SPECIFIC_REQUEST_KEYS = frozenset(
    {
        "thinking",
        "thinkingmode",
        "thinkingtype",
        "thinkingeffort",
        "reasoningeffort",
        "enablereasoning",
        "enablethinking",
        "thoughtmode",
        "reasoning",
        "reasoningtype",
    }
)


def _provider_neutral_extension_issues(request: Mapping[str, Any]) -> list[ValidationIssue]:
    """Reject vendor thinking spellings while keeping extensions extensible.

    ``extensions.reasoning_mode`` is the sole semantic carrier and is only
    valid at an artifact's own top-level ``extensions`` object.  We reject
    the known alternate carriers, including ``thinking.type`` and
    ``thinking.effort``, but leave unrelated extension keys untouched.
    """

    issues: list[ValidationIssue] = []

    def walk(value: Any, path: str, *, root: bool = False, under_thinking: bool = False) -> None:
        if isinstance(value, Mapping):
            for key, item in value.items():
                normalized = re.sub(r"[^a-z0-9]", "", key.lower()) if isinstance(key, str) else ""
                child_path = f"{path}.{key}"
                if key == "reasoning_mode":
                    if not root or item not in {"enabled", "disabled"}:
                        issues.append(
                            ValidationIssue(
                                "provider_specific_field",
                                child_path,
                                "reasoning_mode is only allowed as top-level enabled/disabled",
                            )
                        )
                elif normalized == "reasoningmode" or normalized in _PROVIDER_SPECIFIC_REQUEST_KEYS or (under_thinking and normalized in {"type", "effort"}):
                    issues.append(
                        ValidationIssue(
                            "provider_specific_field",
                            child_path,
                            "provider-specific thinking fields are not allowed in normalized artifacts",
                        )
                    )
                walk(item, child_path, root=False, under_thinking=under_thinking or normalized == "thinking")
        elif isinstance(value, list):
            for index, item in enumerate(value):
                walk(item, f"{path}[{index}]")

    def scan_artifact(value: Any, path: str) -> None:
        if isinstance(value, Mapping):
            for key, item in value.items():
                child_path = f"{path}.{key}"
                if key == "extensions" and isinstance(item, Mapping):
                    walk(item, child_path, root=path == "$")
                else:
                    scan_artifact(item, child_path)
        elif isinstance(value, list):
            for index, item in enumerate(value):
                scan_artifact(item, f"{path}[{index}]")

    scan_artifact(request, "$")
    return issues


def validate_normalized_request(request: Mapping[str, Any]) -> ValidationResult:
    result = validate_artifact(request, expected_kind="normalized_model_request")
    provider_issues = _provider_neutral_extension_issues(request) if result.valid else []
    if not provider_issues:
        return result
    return ValidationResult(False, result.artifact_kind, [*result.issues, *provider_issues])


def validate_normalized_response(
    response: Mapping[str, Any],
    *,
    request: Mapping[str, Any] | None = None,
) -> ValidationResult:
    related = {"normalized_model_request": request} if request is not None else None
    result = validate_artifact(response, expected_kind="normalized_model_response", related_artifacts=related)
    provider_issues = _provider_neutral_extension_issues(response) if result.valid else []
    consistency_issues = _response_consistency_issues(response) if result.valid and not provider_issues else []
    if not provider_issues and not consistency_issues:
        return result
    return ValidationResult(False, result.artifact_kind, [*result.issues, *provider_issues, *consistency_issues])


def validate_model_call_metadata(
    metadata: Mapping[str, Any],
    *,
    request: Mapping[str, Any] | None = None,
    response: Mapping[str, Any] | None = None,
) -> ValidationResult:
    related: dict[str, Any] = {}
    if request is not None:
        related["normalized_model_request"] = request
    if response is not None:
        related["normalized_model_response"] = response
    result = validate_artifact(metadata, expected_kind="model_call_metadata", related_artifacts=related or None)
    provider_issues = _provider_neutral_extension_issues(metadata) if result.valid else []
    if not provider_issues:
        return result
    return ValidationResult(False, result.artifact_kind, [*result.issues, *provider_issues])


def make_normalized_model_request(
    *,
    provider: str = "offline",
    model: str = "fixture-model",
    adapter: str = ADAPTER_VERSION,
    correlation_id: str = "g017-correlation",
    requested_capabilities: Sequence[str] = ("structured_json",),
    prompt_template_version: str = "g017-prompt-0.1.0",
    instructions: str = "Return only normalized PA candidate JSON.",
    context_text: str = "G017 deterministic model request.",
    context_refs: Mapping[str, Any] | None = None,
    response_schema_version: str = SCHEMA_VERSION,
    parameters: Mapping[str, Any] | None = None,
    extensions: Mapping[str, Any] | None = None,
    reasoning_mode: str | None = None,
    secret_values: Sequence[str] = (),
) -> dict[str, Any]:
    """Construct and validate one normalized model request.

    ``reasoning_mode`` is the sole provider-neutral thinking switch.  It is
    copied into ``extensions`` and never translated to a vendor field.
    """

    if isinstance(requested_capabilities, (str, bytes, bytearray)) or not isinstance(requested_capabilities, Sequence):
        raise MalformedRequestError()
    for field_value in (context_refs, parameters, extensions):
        if field_value is not None and not isinstance(field_value, Mapping):
            # Keep the exception fixed and input-independent; malformed
            # provider/request values must not echo list-of-pairs or secrets.
            raise MalformedRequestError()
    try:
        capabilities = list(requested_capabilities)
    except Exception:
        raise MalformedRequestError() from None
    if not all(isinstance(item, str) and item for item in capabilities):
        raise MalformedRequestError()
    try:
        ext = copy.deepcopy(dict(extensions or {}))
    except Exception:
        raise MalformedRequestError() from None
    supplied_mode = ext.get("reasoning_mode")
    if reasoning_mode is None:
        reasoning_mode = supplied_mode if supplied_mode is not None else "disabled"
    if reasoning_mode not in {"enabled", "disabled"}:
        raise ValueError("reasoning_mode must be enabled or disabled")
    if supplied_mode is not None and supplied_mode != reasoning_mode:
        raise ValueError("extensions.reasoning_mode conflicts with reasoning_mode")
    ext["reasoning_mode"] = reasoning_mode
    if reasoning_mode == "enabled" and "reasoning_mode" not in capabilities:
        capabilities.append("reasoning_mode")
    try:
        copied_context_refs = copy.deepcopy(dict(context_refs or _EMPTY_CONTEXT_REFS))
        copied_parameters = copy.deepcopy(dict(parameters or {"temperature": "0", "max_output_tokens": 256}))
    except Exception:
        raise MalformedRequestError() from None
    document: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "normalized_model_request",
        "correlation_id": correlation_id,
        "provider": provider,
        "model": model,
        "adapter": adapter,
        "requested_capabilities": capabilities,
        "prompt_template_version": prompt_template_version,
        "prompt_hash": _prompt_hash(prompt_template_version, instructions, context_text),
        "context_refs": copied_context_refs,
        "response_schema_version": response_schema_version,
        "instructions": instructions,
        "context_text": context_text,
        "parameters": copied_parameters,
        "extensions": ext,
    }
    if contains_secret_values(document, secret_values):
        raise MalformedRequestError("normalized model request contains runtime credential data")
    try:
        finalized = finalize_artifact(document, "normalized_model_request")
    except Exception:
        raise MalformedRequestError("normalized model request cannot be canonically serialized") from None
    if contains_secret_values(finalized, secret_values):
        raise MalformedRequestError("normalized model request contains runtime credential data")
    return _assert_valid(finalized, "normalized_model_request")


_ERROR_CODE_CLASSIFICATIONS = {
    "model_timeout": FailureClassification.TIMEOUT.value,
    "rate_limit": FailureClassification.RATE_LIMIT.value,
    "authentication_error": FailureClassification.AUTHENTICATION.value,
    "transport_error": FailureClassification.TRANSPORT.value,
    "provider_error": FailureClassification.PROVIDER.value,
    "invalid_model_response": FailureClassification.MALFORMED_RESPONSE.value,
    "unsupported_capability": FailureClassification.UNSUPPORTED_CAPABILITY.value,
    "grounding_failure": FailureClassification.GROUNDING_FAILURE.value,
    "internal_error": FailureClassification.INTERNAL.value,
}


def _classification_from_raw(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = _normalized_kind(value)
    return normalized if normalized in _KIND_ALIASES.values() else None


def _canonicalize_error_map(error: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize a supplied frozen error map at the response boundary.

    G014 intentionally keeps a small error-code vocabulary.  Adapter-only
    classes (cancellation, late response, configuration, and so on) remain in
    the typed ``provider_neutral_classification`` extension.  Contradictory
    class/code pairs are rejected instead of allowing an extension to turn a
    terminal error into a success.
    """

    candidate = copy.deepcopy(dict(error))
    unknown_fields = set(candidate) - {"code", "message", "retryable", "details", "extensions"}
    if unknown_fields:
        raise ValueError("normalized error contains unknown fields")
    code = candidate.get("code")
    if code not in _ERROR_CODE_CLASSIFICATIONS:
        raise ValueError("normalized error has an unknown code")
    extensions = candidate.get("extensions")
    if extensions is None:
        extensions = {}
    if not isinstance(extensions, Mapping):
        raise ValueError("normalized error extensions must be an object")
    extensions = copy.deepcopy(dict(extensions))
    code_class = _ERROR_CODE_CLASSIFICATIONS[code]
    extension_class_raw = extensions.get("provider_neutral_classification")
    extension_class = _classification_from_raw(extension_class_raw) if extension_class_raw is not None else None
    if extension_class_raw is not None and extension_class is None:
        raise ValueError("normalized error classification is unknown")
    if extension_class == FailureClassification.SUCCESS.value:
        raise ValueError("normalized error cannot carry success classification")
    if "message" in candidate and not isinstance(candidate.get("message"), str):
        raise ValueError("normalized error message must be a string")
    if "details" in candidate and candidate.get("details") is not None and not isinstance(candidate.get("details"), str):
        raise ValueError("normalized error details must be a string or null")
    if "retryable" in candidate and not isinstance(candidate.get("retryable"), bool):
        raise ValueError("normalized error retryable must be boolean")

    if extension_class is None:
        effective_class = code_class
    elif code_class == FailureClassification.INTERNAL.value:
        effective_class = extension_class
    elif code_class == FailureClassification.PROVIDER.value and extension_class in {
        FailureClassification.PROVIDER.value,
        FailureClassification.SERVER.value,
    }:
        effective_class = extension_class
    elif extension_class == code_class:
        effective_class = code_class
    else:
        raise ValueError("normalized error code and classification disagree")

    transient = extensions.get("transient") is True
    normalized = normalize_error(
        effective_class,
        message=candidate.get("message"),
        details=candidate.get("details"),
        retryable=candidate.get("retryable"),
        transient=transient,
    )
    # Preserve unrelated extension data while making the semantic class
    # explicit whenever it cannot be inferred from the frozen code.
    custom_extensions = dict(extensions)
    custom_extensions.update(normalized.get("extensions", {}))
    if custom_extensions:
        normalized["extensions"] = custom_extensions
    return normalized


def _coerce_error(error: Mapping[str, Any] | str | FailureClassification) -> dict[str, Any]:
    if isinstance(error, Mapping):
        candidate = _canonicalize_error_map(error)
        result = validate_artifact(
            {
                "schema_version": SCHEMA_VERSION,
                "artifact_kind": "normalized_model_response",
                "model_response_id": "mrs_" + "0" * 64,
                "content_hash": "0" * 64,
                "model_request_id": "mrq_" + "0" * 64,
                "provider": "offline",
                "model": "fixture-model",
                "adapter": ADAPTER_VERSION,
                "status": "error",
                "candidate": None,
                "finish_reason": None,
                "error": candidate,
                "extensions": {},
            },
            expected_kind="normalized_model_response",
            verify_hash=False,
        )
        if not result.valid:
            raise ValueError("normalized error is malformed")
        return candidate
    if not isinstance(error, (str, FailureClassification)):
        raise ValueError("normalized error must be a mapping or classification")
    return normalize_error(error)


def _response_consistency_issues(response: Mapping[str, Any]) -> list[ValidationIssue]:
    """Check status, error code/class and retry policy coherence."""

    issues: list[ValidationIssue] = []
    status = response.get("status")
    top_extensions = response.get("extensions")
    top_raw = top_extensions.get("adapter_outcome") if isinstance(top_extensions, Mapping) else None
    top_class = _classification_from_raw(top_raw) if top_raw is not None else None
    if top_raw is not None and top_class is None:
        issues.append(ValidationIssue("status_invariant", "$.extensions.adapter_outcome", "unknown adapter outcome classification"))

    if status == "completed":
        if top_class not in {None, FailureClassification.SUCCESS.value}:
            issues.append(ValidationIssue("status_invariant", "$.extensions.adapter_outcome", "completed response cannot carry a terminal adapter outcome"))
        return issues

    error = response.get("error")
    if not isinstance(error, Mapping):
        return issues
    code = error.get("code")
    code_class = _ERROR_CODE_CLASSIFICATIONS.get(code)
    if code_class is None:
        return issues
    error_extensions = error.get("extensions")
    error_raw = error_extensions.get("provider_neutral_classification") if isinstance(error_extensions, Mapping) else None
    error_class = _classification_from_raw(error_raw) if error_raw is not None else None
    if error_raw is not None and error_class is None:
        issues.append(ValidationIssue("status_invariant", "$.error.extensions.provider_neutral_classification", "unknown error classification"))
        return issues
    if error_class == FailureClassification.SUCCESS.value:
        issues.append(ValidationIssue("status_invariant", "$.error.extensions.provider_neutral_classification", "error cannot carry success classification"))
        return issues
    if error_class is None:
        effective_class = code_class
    elif code_class == FailureClassification.INTERNAL.value:
        effective_class = error_class
    elif code_class == FailureClassification.PROVIDER.value and error_class in {
        FailureClassification.PROVIDER.value,
        FailureClassification.SERVER.value,
    }:
        effective_class = error_class
    elif error_class == code_class:
        effective_class = code_class
    else:
        issues.append(ValidationIssue("status_invariant", "$.error.extensions.provider_neutral_classification", "error code and classification disagree"))
        return issues
    if code_class == FailureClassification.INTERNAL.value and error_class is not None and _error_code_for(error_class) != "internal_error":
        issues.append(ValidationIssue("status_invariant", "$.error.code", "typed provider error requires its normalized error code"))
        return issues

    # Some externally recorded responses carry the adapter-only class on the
    # response extension rather than inside ``error.extensions``.  It is safe
    # to use that class only where the frozen error code leaves the distinction
    # open (internal_error, or provider_error's server/provider split).
    if top_class is not None and top_class != FailureClassification.SUCCESS.value:
        if code_class == FailureClassification.INTERNAL.value and error_class is None:
            effective_class = top_class
        elif code_class == FailureClassification.PROVIDER.value and error_class is None and top_class in {
            FailureClassification.SERVER.value,
            FailureClassification.PROVIDER.value,
        }:
            effective_class = top_class
    if code_class == FailureClassification.INTERNAL.value and error_class is None and top_class is not None and _error_code_for(effective_class) != "internal_error":
        issues.append(ValidationIssue("status_invariant", "$.error.code", "typed provider error requires its normalized error code"))
        return issues

    expected_status_class = FailureClassification.UNSUPPORTED_CAPABILITY.value if status == "unsupported" else effective_class
    if status == "unsupported" and code != "unsupported_capability":
        issues.append(ValidationIssue("status_invariant", "$.error.code", "unsupported response requires unsupported_capability error"))
    if status == "error" and code == "unsupported_capability":
        issues.append(ValidationIssue("status_invariant", "$.status", "unsupported_capability error requires unsupported response status"))
    if status == "error" and effective_class == FailureClassification.UNSUPPORTED_CAPABILITY.value:
        issues.append(ValidationIssue("status_invariant", "$.status", "unsupported capability requires unsupported response status"))
    top_matches = top_class in {None, expected_status_class}
    if code_class == FailureClassification.PROVIDER.value and top_class in {
        FailureClassification.SERVER.value,
        FailureClassification.PROVIDER.value,
    }:
        top_matches = error_class is None or error_class == top_class
    if not top_matches:
        issues.append(ValidationIssue("status_invariant", "$.extensions.adapter_outcome", "adapter outcome and response error disagree"))
    if isinstance(error.get("retryable"), bool):
        transient = isinstance(error_extensions, Mapping) and error_extensions.get("transient") is True
        expected_retryable = _default_retryable(effective_class, transient=transient)
        if bool(error.get("retryable")) is not expected_retryable:
            issues.append(ValidationIssue("status_invariant", "$.error.retryable", "retryable flag conflicts with error classification"))
    return issues


def make_normalized_model_response(
    request: Mapping[str, Any],
    *,
    status: str = "completed",
    candidate: Mapping[str, Any] | None = None,
    error: Mapping[str, Any] | str | FailureClassification | None = None,
    finish_reason: str | None = None,
    generated_at: str | None = None,
    extensions: Mapping[str, Any] | None = None,
    secret_values: Sequence[str] = (),
) -> dict[str, Any]:
    """Construct and validate one normalized model response."""

    request_result = validate_normalized_request(request)
    if not request_result.valid:
        raise MalformedRequestError()
    if status not in {"completed", "error", "unsupported"}:
        raise ValueError("status must be completed, error or unsupported")
    if status == "completed":
        if candidate is None or error is not None:
            raise ValueError("completed response requires candidate and no error")
        response_error = None
        if not isinstance(candidate, Mapping):
            raise MalformedResponseError(
                "normalized model response candidate is malformed",
                classification=FailureClassification.MALFORMED_RESPONSE,
            )
        try:
            response_candidate = copy.deepcopy(dict(candidate))
        except Exception:
            raise MalformedResponseError(
                "normalized model response candidate is malformed",
                classification=FailureClassification.MALFORMED_RESPONSE,
            ) from None
    else:
        if candidate is not None or error is None:
            raise ValueError("terminal response requires error and no candidate")
        response_candidate = None
        try:
            response_error = _coerce_error(error)
        except Exception:
            raise MalformedResponseError(
                "normalized model response contains malformed error",
                classification=FailureClassification.MALFORMED_RESPONSE,
            ) from None
    if extensions is not None and not isinstance(extensions, Mapping):
        raise MalformedResponseError(
            "normalized model response extensions are malformed",
            classification=FailureClassification.MALFORMED_RESPONSE,
        )
    try:
        response_extensions = copy.deepcopy(dict(extensions or {}))
    except Exception:
        raise MalformedResponseError(
            "normalized model response extensions are malformed",
            classification=FailureClassification.MALFORMED_RESPONSE,
        ) from None
    document: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "normalized_model_response",
        "model_request_id": request["model_request_id"],
        "provider": request["provider"],
        "model": request["model"],
        "adapter": request["adapter"],
        "status": status,
        "candidate": response_candidate,
        "finish_reason": finish_reason,
        "error": response_error,
        "extensions": response_extensions,
    }
    if generated_at is not None:
        document["generated_at"] = generated_at
    if contains_secret_values(document, secret_values):
        raise MalformedResponseError(
            "normalized model response contains runtime credential data",
            classification=FailureClassification.MALFORMED_RESPONSE,
        )
    try:
        finalized = finalize_artifact(document, "normalized_model_response")
    except Exception:
        raise MalformedResponseError("normalized model response cannot be canonically serialized", classification=FailureClassification.MALFORMED_RESPONSE) from None
    if contains_secret_values(finalized, secret_values):
        raise MalformedResponseError(
            "normalized model response contains runtime credential data",
            classification=FailureClassification.MALFORMED_RESPONSE,
        )
    try:
        return _assert_valid(finalized, "normalized_model_response", related={"normalized_model_request": request})
    except ContractValidationError:
        raise MalformedResponseError(
            "normalized model response failed contract validation",
            classification=FailureClassification.MALFORMED_RESPONSE,
        ) from None


def make_model_call_metadata(
    request: Mapping[str, Any],
    response: Mapping[str, Any],
    *,
    started_at: str | None = None,
    completed_at: str | None = None,
    latency_ms: int | None = None,
    usage: Mapping[str, Any] | None = None,
    provider_request_id: str | None = None,
    correlation_id: str | None = None,
    extensions: Mapping[str, Any] | None = None,
    secret_values: Sequence[str] = (),
) -> dict[str, Any]:
    """Construct and validate optional call telemetry without zero defaults."""

    request_result = validate_normalized_request(request)
    if not request_result.valid:
        raise MalformedRequestError()
    response_result = validate_normalized_response(response, request=request)
    if not response_result.valid:
        raise ContractValidationError("normalized_model_response", response_result)
    if usage is not None and not isinstance(usage, Mapping):
        raise ContractValidationError(
            "model_call_metadata",
            ValidationResult(
                False,
                "model_call_metadata",
                [ValidationIssue("schema_violation", "$.usage", "contract validation failed")],
            ),
        )
    if extensions is not None and not isinstance(extensions, Mapping):
        raise ContractValidationError(
            "model_call_metadata",
            ValidationResult(
                False,
                "model_call_metadata",
                [ValidationIssue("schema_violation", "$.extensions", "contract validation failed")],
            ),
        )
    document: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "model_call_metadata",
        "model_request_id": request["model_request_id"],
        "model_request_hash": request["content_hash"],
        "model_response_id": response["model_response_id"],
        "model_response_hash": response["content_hash"],
        "provider": request["provider"],
        "model": request["model"],
        "adapter": request["adapter"],
        "started_at": started_at,
        "completed_at": completed_at,
        "latency_ms": latency_ms,
        "usage": copy.deepcopy(dict(usage)) if usage is not None else None,
        "provider_request_id": provider_request_id,
        "correlation_id": request.get("correlation_id") if correlation_id is None else correlation_id,
        "extensions": copy.deepcopy(dict(extensions or {})),
    }
    if contains_secret_values(document, secret_values):
        raise AdapterError("model call metadata contains runtime credential data")
    try:
        finalized = finalize_artifact(document, "model_call_metadata")
    except Exception:
        raise AdapterError("model call metadata cannot be canonically serialized") from None
    return _assert_valid(
        finalized,
        "model_call_metadata",
        related={"normalized_model_request": request, "normalized_model_response": response},
    )


def request_semantic_identity(request: Mapping[str, Any]) -> str:
    """Return the G014 semantic request hash, excluding correlation/identity."""

    result = validate_normalized_request(request)
    if not result.valid:
        raise MalformedRequestError()
    return content_hash(request, "normalized_model_request")


semantic_request_identity = request_semantic_identity
normalized_request_identity = request_semantic_identity


def first_unsupported_capability(
    request: Mapping[str, Any],
    capabilities: CapabilityDescriptor,
) -> str | None:
    """Return the first requested capability the adapter cannot provide."""

    if not isinstance(capabilities, CapabilityDescriptor):
        raise TypeError("capabilities must be a CapabilityDescriptor")
    request_result = validate_normalized_request(request)
    if not request_result.valid:
        raise MalformedRequestError()
    requested = request.get("requested_capabilities", ())
    for capability in requested:
        if not capabilities.supports(capability):
            return capability
    extensions = request.get("extensions")
    if isinstance(extensions, Mapping) and extensions.get("reasoning_mode") == "enabled" and not capabilities.reasoning_mode:
        return "reasoning_mode"
    return None


_RESPONSE_CODE_CLASSIFICATIONS = {
    "model_timeout": FailureClassification.TIMEOUT.value,
    "authentication_error": FailureClassification.AUTHENTICATION.value,
    "rate_limit": FailureClassification.RATE_LIMIT.value,
    "transport_error": FailureClassification.TRANSPORT.value,
    "provider_error": FailureClassification.PROVIDER.value,
    "invalid_model_response": FailureClassification.MALFORMED_RESPONSE.value,
    "unsupported_capability": FailureClassification.UNSUPPORTED_CAPABILITY.value,
    "grounding_failure": FailureClassification.GROUNDING_FAILURE.value,
    "internal_error": FailureClassification.INTERNAL.value,
}


def response_classification(response: Mapping[str, Any]) -> str:
    """Derive adapter status from a validated normalized response."""

    extensions = response.get("extensions")
    if response.get("status") == "completed":
        top_raw = extensions.get("adapter_outcome") if isinstance(extensions, Mapping) else None
        top_class = _classification_from_raw(top_raw) if top_raw is not None else None
        if top_raw is not None and top_class is None:
            return FailureClassification.INTERNAL.value
        if top_class not in {None, FailureClassification.SUCCESS.value}:
            return top_class
        return FailureClassification.SUCCESS.value
    if response.get("status") == "unsupported":
        return FailureClassification.UNSUPPORTED_CAPABILITY.value
    error = response.get("error")
    if isinstance(error, Mapping):
        error_extensions = error.get("extensions")
        if isinstance(error_extensions, Mapping) and error_extensions.get("provider_neutral_classification") is not None:
            classified = _classification_from_raw(error_extensions["provider_neutral_classification"])
            if classified is not None and classified != FailureClassification.SUCCESS.value:
                return classified
        code_class = _RESPONSE_CODE_CLASSIFICATIONS.get(str(error.get("code")), FailureClassification.INTERNAL.value)
        top_class = _classification_from_raw(extensions.get("adapter_outcome")) if isinstance(extensions, Mapping) else None
        if code_class == FailureClassification.PROVIDER.value and top_class in {
            FailureClassification.SERVER.value,
            FailureClassification.PROVIDER.value,
        }:
            return top_class
        if code_class == FailureClassification.INTERNAL.value and top_class not in {None, FailureClassification.SUCCESS.value}:
            return top_class
        if code_class == FailureClassification.PROVIDER.value and isinstance(error_extensions, Mapping):
            classified = _classification_from_raw(error_extensions.get("provider_neutral_classification"))
            if classified in {FailureClassification.SERVER.value, FailureClassification.PROVIDER.value}:
                return classified
        return code_class
    if isinstance(extensions, Mapping) and extensions.get("adapter_outcome") is not None:
        classified = _classification_from_raw(extensions.get("adapter_outcome"))
        if classified is not None:
            return classified
    return FailureClassification.INTERNAL.value


@dataclass(frozen=True)
class AdapterOutcome:
    """A normalized response plus adapter-only status for scheduler decisions."""

    response: dict[str, Any] | None
    call_metadata: dict[str, Any] | None
    classification: str = FailureClassification.SUCCESS.value
    request_semantic_hash: str | None = None
    retryable: bool | None = None
    late: bool = False
    cancelled: bool = False
    # Adapter-only scheduler generation.  This intentionally lives outside
    # G014 response/metadata artifacts and therefore cannot affect their
    # canonical hashes.
    generation: int | None = None

    def __post_init__(self) -> None:
        classification = normalize_classification(self.classification)
        if self.response is not None:
            response_copy = copy.deepcopy(dict(self.response))
            response_result = validate_normalized_response(response_copy)
            if not response_result.valid:
                raise MalformedResponseError(
                    "adapter outcome contains malformed normalized response",
                    classification=FailureClassification.MALFORMED_RESPONSE,
                )
            response_class = response_classification(response_copy)
            # A response's status/error is authoritative.  An adapter cannot
            # turn a terminal response into success by passing a conflicting
            # classification field.
            if response_class != FailureClassification.SUCCESS.value and classification == FailureClassification.SUCCESS.value:
                classification = response_class
            elif (
                classification not in {response_class, FailureClassification.LATE_RESPONSE.value}
                and response_class != FailureClassification.SUCCESS.value
            ):
                raise MalformedResponseError(
                    "adapter outcome classification disagrees with normalized response",
                    classification=FailureClassification.MALFORMED_RESPONSE,
                )
            elif response_class == FailureClassification.SUCCESS.value and classification not in {
                FailureClassification.SUCCESS.value,
                FailureClassification.LATE_RESPONSE.value,
            }:
                raise MalformedResponseError(
                    "adapter outcome classification disagrees with completed response",
                    classification=FailureClassification.MALFORMED_RESPONSE,
                )
            object.__setattr__(self, "response", response_copy)
            error = response_copy.get("error")
            if isinstance(error, Mapping) and isinstance(error.get("retryable"), bool):
                object.__setattr__(self, "retryable", bool(error["retryable"]))
        object.__setattr__(self, "classification", classification)
        if self.call_metadata is not None:
            object.__setattr__(self, "call_metadata", copy.deepcopy(dict(self.call_metadata)))
        if classification == FailureClassification.LATE_RESPONSE.value:
            object.__setattr__(self, "late", True)
        if classification == FailureClassification.CANCELLATION.value:
            object.__setattr__(self, "cancelled", True)

    @property
    def normalized_response(self) -> dict[str, Any] | None:
        return self.response

    @property
    def metadata(self) -> dict[str, Any] | None:
        return self.call_metadata

    @property
    def is_success(self) -> bool:
        return self.classification == FailureClassification.SUCCESS.value

    @property
    def is_late(self) -> bool:
        return self.late or self.classification == FailureClassification.LATE_RESPONSE.value

    @property
    def is_cancelled(self) -> bool:
        return self.cancelled or self.classification == FailureClassification.CANCELLATION.value

    @property
    def error(self) -> dict[str, Any] | None:
        if self.response is None:
            return None
        value = self.response.get("error")
        return copy.deepcopy(value) if isinstance(value, Mapping) else None

    def to_dict(self) -> dict[str, Any]:
        return redact_secrets(
            {
                "response": self.response,
                "call_metadata": self.call_metadata,
                "classification": self.classification,
                "request_semantic_hash": self.request_semantic_hash,
                "retryable": self.retryable,
                "late": self.is_late,
                "cancelled": self.is_cancelled,
                "generation": self.generation,
            }
        )

    as_dict = to_dict


def _error_outcome(
    request: Mapping[str, Any],
    *,
    classification: str | FailureClassification,
    adapter: str | None = None,
    finish_reason: str | None = None,
    message: str | None = None,
    details: str | None = None,
    retryable: bool | None = None,
    transient: bool | None = None,
    call_metadata: Mapping[str, Any] | None = None,
    secret_values: Sequence[str] = (),
    generation: int | None = None,
) -> AdapterOutcome:
    normalized = normalize_classification(classification)
    error = normalize_error(
        normalized,
        message=message,
        details=details,
        retryable=retryable,
        transient=transient,
        secret_values=secret_values,
    )
    response = make_normalized_model_response(
        request,
        status="error" if normalized != FailureClassification.UNSUPPORTED_CAPABILITY.value else "unsupported",
        error=error,
        finish_reason=finish_reason,
        secret_values=secret_values,
        extensions={"adapter_outcome": normalized},
    )
    metadata = (
        copy.deepcopy(dict(call_metadata))
        if call_metadata is not None
        else make_model_call_metadata(request, response, secret_values=secret_values)
    )
    if contains_secret_values(metadata, secret_values):
        raise AdapterError("model call metadata contains runtime credential data")
    if call_metadata is not None:
        metadata_result = validate_model_call_metadata(metadata, request=request, response=response)
        if not metadata_result.valid:
            raise ContractValidationError("model_call_metadata", metadata_result)
    # ``adapter`` is kept as an argument for a future transport but the frozen
    # request/response labels must remain equal; a caller cannot change them at
    # this point without invalidating G014 lineage.
    del adapter
    return AdapterOutcome(
        response=response,
        call_metadata=metadata,
        classification=normalized,
        request_semantic_hash=request_semantic_identity(request),
        retryable=bool(error["retryable"]),
        generation=generation,
    )


def unsupported_capability_outcome(
    request: Mapping[str, Any],
    capability: str,
    *,
    secret_values: Sequence[str] = (),
    generation: int | None = None,
) -> AdapterOutcome:
    return _error_outcome(
        request,
        classification=FailureClassification.UNSUPPORTED_CAPABILITY,
        details=f"unsupported capability: {capability}",
        secret_values=secret_values,
        generation=generation,
    )


def cancellation_outcome(
    request: Mapping[str, Any],
    *,
    reason: str | None = None,
    secret_values: Sequence[str] = (),
    generation: int | None = None,
) -> AdapterOutcome:
    return _error_outcome(
        request,
        classification=FailureClassification.CANCELLATION,
        details=reason,
        secret_values=secret_values,
        generation=generation,
    )


def late_response_outcome(
    request: Mapping[str, Any],
    response: Mapping[str, Any],
    *,
    call_metadata: Mapping[str, Any] | None = None,
    secret_values: Sequence[str] = (),
    generation: int | None = None,
) -> AdapterOutcome:
    """Mark an otherwise valid response as stale without changing its contract."""

    request_result = validate_normalized_request(request)
    if not request_result.valid:
        raise MalformedRequestError()
    response_result = validate_normalized_response(response, request=request)
    if not response_result.valid or contains_secret_values(response, secret_values):
        raise MalformedResponseError(
            "late response is not a normalized model response",
            classification=FailureClassification.MALFORMED_RESPONSE,
        )
    metadata = (
        copy.deepcopy(dict(call_metadata))
        if call_metadata is not None
        else make_model_call_metadata(request, response, secret_values=secret_values)
    )
    if contains_secret_values(metadata, secret_values):
        raise MalformedResponseError(
            "late response metadata contains runtime credential data",
            classification=FailureClassification.MALFORMED_RESPONSE,
        )
    if call_metadata is not None:
        metadata_result = validate_model_call_metadata(metadata, request=request, response=response)
        if not metadata_result.valid:
            raise ContractValidationError("model_call_metadata", metadata_result)
    return AdapterOutcome(
        response=copy.deepcopy(dict(response)),
        call_metadata=metadata,
        classification=FailureClassification.LATE_RESPONSE.value,
        request_semantic_hash=request_semantic_identity(request),
        retryable=False,
        late=True,
        generation=generation,
    )


def cancelled_outcome(
    request: Mapping[str, Any],
    *,
    reason: str | None = None,
    secret_values: Sequence[str] = (),
    generation: int | None = None,
) -> AdapterOutcome:
    return cancellation_outcome(request, reason=reason, secret_values=secret_values, generation=generation)


class ProviderNeutralModelGateway:
    """Tiny gateway that keeps callers independent from an adapter class."""

    def __init__(self, adapter: ModelAdapter) -> None:
        if not hasattr(adapter, "complete") and not hasattr(adapter, "call"):
            raise TypeError("adapter must implement complete(request) or call(request)")
        self.adapter = adapter

    @property
    def capabilities(self) -> CapabilityDescriptor:
        value = getattr(self.adapter, "capabilities", CapabilityDescriptor())
        return value() if callable(value) else value

    def complete(self, request: Mapping[str, Any], **kwargs: Any) -> AdapterOutcome:
        method = getattr(self.adapter, "complete", None) or getattr(self.adapter, "call")
        request_result = validate_normalized_request(request)
        if not request_result.valid:
            raise MalformedRequestError()
        # The concrete adapter owns request-secret validation, cancellation,
        # and capability ordering.  Keeping this call transparent prevents a
        # gateway capability preflight from bypassing an adapter's secret
        # boundary or generation semantics.
        return method(request, **kwargs)

    call = complete


ModelGateway = ProviderNeutralModelGateway


__all__ = [
    "ADAPTER_VERSION",
    "AdapterError",
    "AdapterOutcome",
    "AdapterProtocol",
    "CapabilityDescriptor",
    "ContractValidationError",
    "CredentialResolutionError",
    "EphemeralCredentialResolver",
    "ErrorClassification",
    "FailureClassification",
    "FROZEN_NORMALIZED_ERROR_CODES",
    "MalformedRequestError",
    "MalformedResponseError",
    "ModelAdapter",
    "ModelGateway",
    "ProviderAdapter",
    "ProviderNeutralModelGateway",
    "RuntimeCredential",
    "RuntimeCredentialResolver",
    "SCHEMA_VERSION",
    "StaticCredentialResolver",
    "cancelled_outcome",
    "cancellation_outcome",
    "contains_secret_values",
    "first_unsupported_capability",
    "late_response_outcome",
    "make_model_call_metadata",
    "make_normalized_model_request",
    "make_normalized_model_response",
    "normalize_error",
    "normalize_classification",
    "normalize_exception",
    "normalized_request_identity",
    "redact_exception",
    "redact_message",
    "redact_secrets",
    "redacted_json",
    "request_semantic_identity",
    "response_classification",
    "safe_json",
    "semantic_request_identity",
    "validate_model_call_metadata",
    "validate_normalized_request",
    "validate_normalized_response",
    "unsupported_capability_outcome",
]
