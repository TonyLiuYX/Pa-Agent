"""Deterministic scripted model adapter for G017 tests and local replay setup."""

from __future__ import annotations

import copy
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from .base import (
    ADAPTER_VERSION,
    AdapterError,
    AdapterOutcome,
    CapabilityDescriptor,
    contains_secret_values,
    ContractValidationError,
    FailureClassification,
    MalformedRequestError,
    MalformedResponseError,
    ModelAdapter,
    cancellation_outcome,
    late_response_outcome,
    make_model_call_metadata,
    make_normalized_model_response,
    first_unsupported_capability,
    normalize_classification,
    normalize_error,
    request_semantic_identity,
    response_classification,
    unsupported_capability_outcome,
    validate_normalized_request,
    validate_normalized_response,
)


class ScriptExhaustedError(AdapterError):
    """Raised when a scripted sequence is configured for raising exhaustion."""

    def __init__(self) -> None:
        super().__init__(
            "scripted adapter sequence is exhausted",
            classification=FailureClassification.SCRIPT_EXHAUSTED,
        )


@dataclass(frozen=True)
class ScriptedStep:
    """One deterministic scripted result.

    A step can be a candidate, a complete normalized response, or a typed
    error.  ``classification`` additionally permits cancellation and late
    response vectors without inventing a G014 error code.
    """

    candidate: Mapping[str, Any] | None = None
    response: Mapping[str, Any] | None = None
    error_kind: str | FailureClassification | None = None
    error_message: str | None = None
    error_details: str | None = None
    classification: str | FailureClassification | None = None
    finish_reason: str | None = None
    generated_at: str | None = None
    usage: Mapping[str, Any] | None = None
    latency_ms: int | None = None
    provider_request_id: str | None = None
    started_at: str | None = None
    completed_at: str | None = None
    extensions: Mapping[str, Any] | None = None
    retryable: bool | None = None
    transient: bool | None = None


ScriptedResult = ScriptedStep


def scripted_success(
    candidate: Mapping[str, Any],
    *,
    finish_reason: str | None = None,
    generated_at: str | None = None,
    usage: Mapping[str, Any] | None = None,
    latency_ms: int | None = None,
    provider_request_id: str | None = None,
    started_at: str | None = None,
    completed_at: str | None = None,
    extensions: Mapping[str, Any] | None = None,
) -> ScriptedStep:
    return ScriptedStep(
        candidate=candidate,
        finish_reason=finish_reason,
        generated_at=generated_at,
        usage=usage,
        latency_ms=latency_ms,
        provider_request_id=provider_request_id,
        started_at=started_at,
        completed_at=completed_at,
        extensions=extensions,
    )


def scripted_error(
    kind: str | FailureClassification,
    *,
    message: str | None = None,
    details: str | None = None,
    finish_reason: str | None = None,
    retryable: bool | None = None,
    transient: bool | None = None,
    usage: Mapping[str, Any] | None = None,
    latency_ms: int | None = None,
    provider_request_id: str | None = None,
    started_at: str | None = None,
    completed_at: str | None = None,
    extensions: Mapping[str, Any] | None = None,
) -> ScriptedStep:
    # The convenience helper represents a known transient vector for the
    # provider-like classes unless the caller explicitly opts out.  The
    # centralized ``normalize_error`` API remains conservative and requires
    # ``transient=True`` for those classes.
    if transient is None and retryable is None and normalize_classification(kind) in {
        FailureClassification.SERVER.value,
        FailureClassification.PROVIDER.value,
        FailureClassification.TRANSPORT.value,
    }:
        transient = True
    return ScriptedStep(
        error_kind=kind,
        error_message=message,
        error_details=details,
        finish_reason=finish_reason,
        retryable=retryable,
        transient=transient,
        usage=usage,
        latency_ms=latency_ms,
        provider_request_id=provider_request_id,
        started_at=started_at,
        completed_at=completed_at,
        extensions=extensions,
    )


class ScriptedAdapter:
    """Return a deterministic sequence of normalized responses.

    ``on_exhaustion`` is one of ``"error"`` (default), ``"raise"`` or
    ``"repeat_last"``.  A single step can therefore drive a large number of
    state transitions by selecting ``repeat_last`` without any network or SDK.
    """

    def __init__(
        self,
        steps: Sequence[Any] | None = None,
        *,
        responses: Sequence[Any] | None = None,
        script: Sequence[Any] | None = None,
        on_exhaustion: str = "error",
        repeat: bool = False,
        capabilities: CapabilityDescriptor | None = None,
        secret_values: Sequence[str] = (),
    ) -> None:
        supplied = [value for value in (steps, responses, script) if value is not None]
        if len(supplied) > 1:
            raise TypeError("provide only one of steps, responses or script")
        values = supplied[0] if supplied else ()
        if on_exhaustion not in {"error", "raise", "repeat_last"}:
            raise ValueError("on_exhaustion must be error, raise or repeat_last")
        if repeat:
            on_exhaustion = "repeat_last"
        self._secret_values = tuple(secret for secret in secret_values if isinstance(secret, str) and secret)
        # Snapshot every nested value at construction time.  A caller may
        # mutate its fixture dictionaries after creating the adapter without
        # changing a deterministic replay.
        self._steps = tuple(_coerce_step(value) for value in copy.deepcopy(values))
        self._on_exhaustion = on_exhaustion
        self._index = 0
        self._capabilities = capabilities or CapabilityDescriptor()

    @property
    def capabilities(self) -> CapabilityDescriptor:
        return self._capabilities

    def __getstate__(self) -> dict[str, Any]:
        if self._secret_values:
            raise TypeError("scripted adapters carrying runtime credentials cannot be serialized")
        return dict(self.__dict__)

    @property
    def adapter_name(self) -> str:
        return "scripted-g017-0.1.0"

    @property
    def index(self) -> int:
        return self._index

    @property
    def calls(self) -> int:
        return self._index

    @property
    def remaining(self) -> int:
        return max(0, len(self._steps) - self._index)

    def reset(self) -> None:
        self._index = 0

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
        if contains_secret_values(request, self._secret_values):
            raise MalformedRequestError(secret_values=self._secret_values)
        request_hash = request_semantic_identity(request)
        if cancelled:
            return cancellation_outcome(request, secret_values=self._secret_values, generation=generation)
        capability = first_unsupported_capability(request, self.capabilities)
        if capability is not None:
            return unsupported_capability_outcome(request, capability, secret_values=self._secret_values, generation=generation)
        step = self._next_step()
        if step is None:
            if self._on_exhaustion == "raise":
                raise ScriptExhaustedError()
            return self._error_outcome(request, FailureClassification.SCRIPT_EXHAUSTED, generation=generation)
        try:
            outcome = self._execute_step(request, step)
        except (AdapterError, ContractValidationError, TypeError, ValueError, KeyError):
            # Request validation already happened above.  Any contract or
            # telemetry failure from the scripted result is therefore a
            # typed malformed-response outcome, not a leaked boundary error.
            return self._error_outcome(request, FailureClassification.MALFORMED_RESPONSE, generation=generation)
        if outcome.request_semantic_hash is None:
            return AdapterOutcome(
                response=outcome.response,
                call_metadata=outcome.call_metadata,
                classification=outcome.classification,
                request_semantic_hash=request_hash,
                retryable=outcome.retryable,
                late=outcome.late,
                cancelled=outcome.cancelled,
                generation=generation,
            )
        if outcome.generation is None:
            return AdapterOutcome(
                response=outcome.response,
                call_metadata=outcome.call_metadata,
                classification=outcome.classification,
                request_semantic_hash=outcome.request_semantic_hash,
                retryable=outcome.retryable,
                late=outcome.late,
                cancelled=outcome.cancelled,
                generation=generation,
            )
        return outcome

    call = complete

    def cancel(
        self,
        request: Mapping[str, Any],
        *,
        reason: str | None = None,
        generation: int | None = None,
    ) -> AdapterOutcome:
        # Validation happens in cancellation_outcome, so malformed requests
        # are never silently converted to a cancellation.
        return cancellation_outcome(
            request,
            reason=reason,
            secret_values=self._secret_values,
            generation=generation,
        )

    def late_response(
        self,
        request: Mapping[str, Any],
        response: Mapping[str, Any],
        *,
        call_metadata: Mapping[str, Any] | None = None,
        generation: int | None = None,
    ) -> AdapterOutcome:
        return late_response_outcome(
            request,
            response,
            call_metadata=call_metadata,
            secret_values=self._secret_values,
            generation=generation,
        )

    def _next_step(self) -> ScriptedStep | None:
        if self._index < len(self._steps):
            step = self._steps[self._index]
            self._index += 1
            return step
        if self._on_exhaustion == "repeat_last" and self._steps:
            return self._steps[-1]
        return None

    def _execute_step(self, request: Mapping[str, Any], step: ScriptedStep) -> AdapterOutcome:
        classification = step.classification
        if classification is not None:
            normalized = str(classification.value if isinstance(classification, FailureClassification) else classification)
            if normalize_classification(normalized) == FailureClassification.INTERNAL.value and normalized.strip().lower() not in {
                FailureClassification.INTERNAL.value,
                "internal_error",
            }:
                return self._error_outcome(request, FailureClassification.MALFORMED_RESPONSE, step=step)
            if normalized in {"cancellation", "cancelled", "canceled", "cancel"}:
                return self._error_outcome(request, FailureClassification.CANCELLATION, step=step)
            if normalized in {"late", "late_response"}:
                response, metadata = self._response_and_metadata(request, step)
                return late_response_outcome(
                    request,
                    response,
                    call_metadata=metadata,
                    secret_values=self._secret_values,
                )
        if step.error_kind is not None:
            return self._error_outcome(request, step.error_kind, step=step)
        if step.response is not None:
            if not isinstance(step.response, Mapping):
                return self._error_outcome(request, FailureClassification.MALFORMED_RESPONSE, step=step)
            response = copy.deepcopy(dict(step.response))
            result = validate_normalized_response(response, request=request)
            if not result.valid or contains_secret_values(response, self._secret_values):
                return self._error_outcome(request, FailureClassification.MALFORMED_RESPONSE, step=step)
            metadata = self._metadata_for(request, response, step)
            error = response.get("error")
            classification = response_classification(response)
            return AdapterOutcome(
                response=response,
                call_metadata=metadata,
                classification=classification,
                request_semantic_hash=request_semantic_identity(request),
                retryable=error.get("retryable") if isinstance(error, Mapping) else None,
                late=classification == FailureClassification.LATE_RESPONSE.value,
                cancelled=classification == FailureClassification.CANCELLATION.value,
            )
        if step.candidate is None:
            return self._error_outcome(request, FailureClassification.MALFORMED_RESPONSE, step=step)
        response = make_normalized_model_response(
            request,
            status="completed",
            candidate=step.candidate,
            finish_reason=step.finish_reason,
            generated_at=step.generated_at,
            secret_values=self._secret_values,
            extensions=step.extensions,
        )
        metadata = self._metadata_for(request, response, step)
        return AdapterOutcome(
            response=response,
            call_metadata=metadata,
            request_semantic_hash=request_semantic_identity(request),
        )

    def _response_and_metadata(self, request: Mapping[str, Any], step: ScriptedStep) -> tuple[dict[str, Any], dict[str, Any]]:
        if step.response is not None:
            if not isinstance(step.response, Mapping):
                raise MalformedResponseError(
                    "scripted late response is malformed",
                    classification=FailureClassification.MALFORMED_RESPONSE,
                )
            response = copy.deepcopy(dict(step.response))
            result = validate_normalized_response(response, request=request)
            if not result.valid or contains_secret_values(response, self._secret_values):
                raise MalformedResponseError(
                    "scripted late response is malformed",
                    classification=FailureClassification.MALFORMED_RESPONSE,
                )
        elif step.candidate is not None:
            response = make_normalized_model_response(
                request,
                candidate=step.candidate,
                finish_reason=step.finish_reason,
                generated_at=step.generated_at,
                secret_values=self._secret_values,
                extensions=step.extensions,
            )
        else:
            raise MalformedResponseError(
                "scripted late response is missing a candidate",
                classification=FailureClassification.MALFORMED_RESPONSE,
            )
        return response, self._metadata_for(request, response, step)

    def _metadata_for(self, request: Mapping[str, Any], response: Mapping[str, Any], step: ScriptedStep) -> dict[str, Any]:
        return make_model_call_metadata(
            request,
            response,
            started_at=step.started_at,
            completed_at=step.completed_at,
            latency_ms=step.latency_ms,
            usage=step.usage,
            provider_request_id=step.provider_request_id,
            secret_values=self._secret_values,
        )

    def _error_outcome(
        self,
        request: Mapping[str, Any],
        classification: str | FailureClassification,
        *,
        step: ScriptedStep | None = None,
        generation: int | None = None,
    ) -> AdapterOutcome:
        step = step or ScriptedStep()
        normalized = normalize_classification(classification)
        error = normalize_error(
            normalized,
            message=step.error_message,
            details=step.error_details,
            retryable=step.retryable,
            transient=step.transient,
            secret_values=self._secret_values,
        )
        status = "unsupported" if normalized == FailureClassification.UNSUPPORTED_CAPABILITY.value else "error"
        # A malformed result must not re-use malformed extensions/telemetry
        # while constructing the diagnostic outcome; doing so could recurse
        # into the same validation error.
        use_step = step if normalized != FailureClassification.MALFORMED_RESPONSE.value else ScriptedStep()
        response = make_normalized_model_response(
            request,
            status=status,
            error=error,
            finish_reason=use_step.finish_reason,
            generated_at=use_step.generated_at,
            secret_values=self._secret_values,
            extensions={
                **dict(use_step.extensions or {}),
                "adapter_outcome": normalized,
            },
        )
        metadata = self._metadata_for(request, response, use_step)
        return AdapterOutcome(
            response=response,
            call_metadata=metadata,
            classification=normalized,
            request_semantic_hash=request_semantic_identity(request),
            retryable=bool(error["retryable"]),
            late=normalized in {"late", "late_response"},
            cancelled=normalized in {"cancellation", "cancelled", "canceled", "cancel"},
            generation=generation,
        )

def _coerce_step(value: Any) -> ScriptedStep:
    if isinstance(value, ScriptedStep):
        return ScriptedStep(
            candidate=copy.deepcopy(value.candidate),
            response=copy.deepcopy(value.response),
            error_kind=value.error_kind,
            error_message=value.error_message,
            error_details=value.error_details,
            classification=value.classification,
            finish_reason=value.finish_reason,
            generated_at=value.generated_at,
            usage=copy.deepcopy(value.usage),
            latency_ms=value.latency_ms,
            provider_request_id=value.provider_request_id,
            started_at=value.started_at,
            completed_at=value.completed_at,
            extensions=copy.deepcopy(value.extensions),
            retryable=value.retryable,
            transient=value.transient,
        )
    if isinstance(value, Mapping):
        artifact_kind = value.get("artifact_kind")
        if artifact_kind == "normalized_model_response":
            return ScriptedStep(response=value)
        # A compact spec is convenient in JSON fixtures.  ``error`` accepts a
        # kind string or a frozen normalizedError mapping.
        if "error_kind" in value or "classification" in value:
            error_kind = value.get("error_kind") or value.get("classification")
            return ScriptedStep(
                candidate=copy.deepcopy(value.get("candidate")),
                response=copy.deepcopy(value.get("response")),
                error_kind=error_kind,
                error_message=value.get("error_message"),
                error_details=value.get("error_details"),
                classification=value.get("classification"),
                finish_reason=value.get("finish_reason"),
                generated_at=value.get("generated_at"),
                usage=copy.deepcopy(value.get("usage")),
                latency_ms=value.get("latency_ms"),
                provider_request_id=value.get("provider_request_id"),
                started_at=value.get("started_at"),
                completed_at=value.get("completed_at"),
                extensions=copy.deepcopy(value.get("extensions")),
                retryable=value.get("retryable"),
                transient=value.get("transient"),
            )
        if "error" in value:
            raw_error = value.get("error")
            if isinstance(raw_error, Mapping):
                error_kind = raw_error.get("classification") or raw_error.get("code") or "provider"
                error_message = raw_error.get("message")
                error_details = raw_error.get("details")
            else:
                error_kind = str(raw_error)
                error_message = None
                error_details = None
            return ScriptedStep(
                error_kind=error_kind,
                error_message=error_message,
                error_details=error_details,
                finish_reason=value.get("finish_reason"),
                usage=copy.deepcopy(value.get("usage")),
                latency_ms=value.get("latency_ms"),
                provider_request_id=value.get("provider_request_id"),
                started_at=value.get("started_at"),
                completed_at=value.get("completed_at"),
                extensions=copy.deepcopy(value.get("extensions")),
                retryable=raw_error.get("retryable") if isinstance(raw_error, Mapping) else None,
                transient=raw_error.get("transient") if isinstance(raw_error, Mapping) else None,
            )
        # Otherwise a plain mapping is treated as a candidate.
        return ScriptedStep(candidate=value)
    if isinstance(value, str):
        return ScriptedStep(error_kind=value)
    if value is None:
        return ScriptedStep()
    raise TypeError("scripted step must be a mapping, ScriptedStep or error kind")


__all__ = [
    "ScriptExhaustedError",
    "ScriptedAdapter",
    "ScriptedResult",
    "ScriptedStep",
    "scripted_error",
    "scripted_success",
]
