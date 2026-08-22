"""In-memory and JSON replay adapter for G017."""

from __future__ import annotations

import copy
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .base import (
    AdapterError,
    AdapterOutcome,
    CapabilityDescriptor,
    contains_secret_values,
    ContractValidationError,
    FailureClassification,
    MalformedRequestError,
    MalformedResponseError,
    ModelAdapter,
    make_model_call_metadata,
    make_normalized_model_response,
    first_unsupported_capability,
    normalize_classification,
    normalize_error,
    request_semantic_identity,
    response_classification,
    cancellation_outcome,
    late_response_outcome,
    unsupported_capability_outcome,
    validate_model_call_metadata,
    validate_normalized_request,
    validate_normalized_response,
)


class ReplayError(AdapterError):
    """Base replay data/lookup failure."""


class ReplayDataError(ReplayError):
    """A recording is malformed or semantically contradictory."""


class ReplayMissingError(ReplayError):
    """No recording exists for a request semantic identity."""

    def __init__(self) -> None:
        super().__init__(
            "replay recording is missing",
            classification=FailureClassification.REPLAY_MISSING,
        )


class ReplayMismatchError(ReplayError):
    """A recording exists but its request/response lineage does not match."""

    def __init__(self) -> None:
        super().__init__(
            "replay recording does not match request",
            classification=FailureClassification.REPLAY_MISMATCH,
        )


@dataclass(frozen=True)
class ReplayRecord:
    request: dict[str, Any]
    response: dict[str, Any]
    call_metadata: dict[str, Any] | None = None

    @property
    def request_semantic_hash(self) -> str:
        return request_semantic_identity(self.request)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "request": copy.deepcopy(self.request),
            "response": copy.deepcopy(self.response),
        }
        if self.call_metadata is not None:
            result["call_metadata"] = copy.deepcopy(self.call_metadata)
        return result


class ReplayAdapter:
    """Replay a response by G014 request semantic identity.

    Correlation IDs and call telemetry do not participate in the lookup key.
    The adapter never calls a network or provider SDK.
    """

    def __init__(
        self,
        records: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        recordings: Sequence[Any] | Mapping[str, Any] | None = None,
        on_missing: str = "error",
        capabilities: CapabilityDescriptor | None = None,
        secret_values: Sequence[str] = (),
    ) -> None:
        supplied = [value for value in (records, recordings) if value is not None]
        if len(supplied) > 1:
            raise TypeError("provide only one of records or recordings")
        if on_missing not in {"error", "raise"}:
            raise ValueError("on_missing must be error or raise")
        self._on_missing = on_missing
        self._records: dict[str, ReplayRecord] = {}
        self._capabilities = capabilities or CapabilityDescriptor()
        self._secret_values = tuple(secret for secret in secret_values if isinstance(secret, str) and secret)
        if supplied:
            self.load(supplied[0])

    @classmethod
    def from_json(
        cls,
        source: str | bytes | Path | Mapping[str, Any] | Sequence[Any],
        *,
        on_missing: str = "error",
        secret_values: Sequence[str] = (),
    ) -> "ReplayAdapter":
        """Load a recording list, object with ``records``/``pairs``, or path."""

        value = _read_json_source(source)
        return cls(value, on_missing=on_missing, secret_values=secret_values)

    load_json = from_json

    @property
    def capabilities(self) -> CapabilityDescriptor:
        return self._capabilities

    def __getstate__(self) -> dict[str, Any]:
        if self._secret_values:
            raise TypeError("replay adapters carrying runtime credentials cannot be serialized")
        return dict(self.__dict__)

    @property
    def adapter_name(self) -> str:
        return "replay-g017-0.1.0"

    @property
    def size(self) -> int:
        return len(self._records)

    def load(self, records: Sequence[Any] | Mapping[str, Any]) -> int:
        """Validate and add records, returning the number of unique identities."""

        for item in _iter_records(records):
            record = _coerce_record(item)
            self._add_record(record)
        return len(self._records)

    add = load

    def lookup(self, request: Mapping[str, Any]) -> ReplayRecord:
        request_hash = request_semantic_identity(request)
        record = self._records.get(request_hash)
        if record is None:
            raise ReplayMissingError()
        if not _record_matches_request(record, request):
            raise ReplayMismatchError()
        return ReplayRecord(
            request=copy.deepcopy(record.request),
            response=copy.deepcopy(record.response),
            call_metadata=copy.deepcopy(record.call_metadata),
        )

    resolve = lookup

    def complete(
        self,
        request: Mapping[str, Any],
        *,
        cancelled: bool = False,
        generation: int | None = None,
    ) -> AdapterOutcome:
        # A malformed request is a caller error and must not be hidden as a
        # replay miss.
        request_result = validate_normalized_request(request)
        if not request_result.valid:
            raise MalformedRequestError()
        if contains_secret_values(request, self._secret_values):
            raise MalformedRequestError(secret_values=self._secret_values)
        if cancelled:
            return cancellation_outcome(request, secret_values=self._secret_values, generation=generation)
        capability = first_unsupported_capability(request, self.capabilities)
        if capability is not None:
            return unsupported_capability_outcome(request, capability, secret_values=self._secret_values, generation=generation)
        try:
            record = self.lookup(request)
        except ReplayMissingError:
            if self._on_missing == "raise":
                raise
            return self._lookup_error(request, FailureClassification.REPLAY_MISSING, generation=generation)
        except ReplayMismatchError:
            if self._on_missing == "raise":
                raise
            return self._lookup_error(request, FailureClassification.REPLAY_MISMATCH, generation=generation)
        response = copy.deepcopy(record.response)
        response_result = validate_normalized_response(response, request=request)
        if not response_result.valid or contains_secret_values(response, self._secret_values):
            if self._on_missing == "raise":
                raise ReplayMismatchError()
            return self._lookup_error(request, FailureClassification.MALFORMED_RESPONSE, generation=generation)
        try:
            metadata = self._metadata_for(request, response, record.call_metadata)
        except (AdapterError, ContractValidationError, TypeError, ValueError, KeyError):
            return self._lookup_error(request, FailureClassification.MALFORMED_RESPONSE, generation=generation)
        classification = response_classification(response)
        error = response.get("error")
        return AdapterOutcome(
            response=response,
            call_metadata=metadata,
            classification=classification,
            request_semantic_hash=request_semantic_identity(request),
            retryable=error.get("retryable") if isinstance(error, Mapping) else None,
            late=classification == FailureClassification.LATE_RESPONSE.value,
            cancelled=classification == FailureClassification.CANCELLATION.value,
            generation=generation,
        )

    call = complete

    def cancel(
        self,
        request: Mapping[str, Any],
        *,
        reason: str | None = None,
        generation: int | None = None,
    ) -> AdapterOutcome:
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

    def _add_record(self, record: ReplayRecord) -> None:
        request = record.request
        response = record.response
        if contains_secret_values(record.to_dict(), self._secret_values):
            raise ReplayDataError("replay recording contains runtime credential data", classification=FailureClassification.REPLAY_MISMATCH)
        request_result = validate_normalized_request(request)
        if not request_result.valid:
            raise ReplayDataError("replay request failed contract validation", classification=FailureClassification.REPLAY_MISMATCH)
        response_result = validate_normalized_response(response, request=request)
        if not response_result.valid:
            raise ReplayDataError("replay response failed contract validation", classification=FailureClassification.REPLAY_MISMATCH)
        if not _record_matches_request(record, request):
            raise ReplayDataError("replay response lineage does not match request", classification=FailureClassification.REPLAY_MISMATCH)
        if record.call_metadata is not None:
            metadata_result = validate_model_call_metadata(record.call_metadata, request=request, response=response)
            if not metadata_result.valid:
                raise ReplayDataError("replay call metadata failed contract validation", classification=FailureClassification.REPLAY_MISMATCH)
        key = request_semantic_identity(request)
        existing = self._records.get(key)
        if existing is not None and not _same_response(existing.response, response):
            raise ReplayDataError(
                "conflicting replay responses share one semantic request identity",
                classification=FailureClassification.REPLAY_MISMATCH,
            )
        self._records[key] = ReplayRecord(
            request=copy.deepcopy(dict(request)),
            response=copy.deepcopy(dict(response)),
            call_metadata=copy.deepcopy(dict(record.call_metadata)) if record.call_metadata is not None else None,
        )

    def _metadata_for(
        self,
        request: Mapping[str, Any],
        response: Mapping[str, Any],
        recorded: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        if recorded is None:
            return make_model_call_metadata(request, response, secret_values=self._secret_values)
        metadata = copy.deepcopy(dict(recorded))
        # Correlation is trace telemetry, not semantic identity.  Rebind it to
        # the current request while retaining any recorded optional telemetry.
        metadata["correlation_id"] = request.get("correlation_id")
        return make_model_call_metadata(
            request,
            response,
            started_at=metadata.get("started_at"),
            completed_at=metadata.get("completed_at"),
            latency_ms=metadata.get("latency_ms"),
            usage=metadata.get("usage"),
            provider_request_id=metadata.get("provider_request_id"),
            correlation_id=metadata.get("correlation_id"),
            secret_values=self._secret_values,
            extensions=metadata.get("extensions") if isinstance(metadata.get("extensions"), Mapping) else None,
        )

    def _lookup_error(
        self,
        request: Mapping[str, Any],
        classification: str | FailureClassification,
        *,
        generation: int | None = None,
    ) -> AdapterOutcome:
        normalized = normalize_classification(classification)
        error = normalize_error(normalized)
        response = make_normalized_model_response(
            request,
            status="error",
            error=error,
            secret_values=self._secret_values,
            extensions={"adapter_outcome": normalized},
        )
        metadata = make_model_call_metadata(request, response, secret_values=self._secret_values)
        return AdapterOutcome(
            response=response,
            call_metadata=metadata,
            classification=normalized,
            request_semantic_hash=request_semantic_identity(request),
            retryable=bool(error["retryable"]),
            generation=generation,
        )


def _read_json_source(source: str | bytes | Path | Mapping[str, Any] | Sequence[Any]) -> Any:
    if isinstance(source, Path):
        try:
            return json.loads(source.read_text(encoding="utf-8"))
        except Exception:
            raise ReplayDataError("replay JSON could not be read") from None
    if isinstance(source, bytes):
        try:
            return json.loads(source.decode("utf-8"))
        except Exception:
            raise ReplayDataError("replay JSON could not be parsed") from None
    if isinstance(source, str):
        candidate = Path(source)
        try:
            if candidate.exists():
                return json.loads(candidate.read_text(encoding="utf-8"))
        except OSError:
            pass
        try:
            return json.loads(source)
        except Exception:
            raise ReplayDataError("replay JSON could not be parsed") from None
    return copy.deepcopy(source)


def _iter_records(records: Sequence[Any] | Mapping[str, Any]) -> list[Any]:
    if isinstance(records, Mapping):
        for key in ("records", "recordings", "pairs"):
            if key in records:
                value = records[key]
                if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
                    raise ReplayDataError("replay records must be a JSON array")
                return list(value)
        if "request" in records and "response" in records:
            return [records]
        # Also accept an identity-keyed object for hand-authored fixtures.
        values: list[Any] = []
        for key, value in records.items():
            if not isinstance(value, Mapping):
                raise ReplayDataError("identity-keyed replay values must be objects")
            item = dict(value)
            item.setdefault("request_semantic_hash", key)
            values.append(item)
        return values
    if isinstance(records, Sequence) and not isinstance(records, (str, bytes, bytearray)):
        return list(records)
    raise ReplayDataError("replay records must be an array or object")


def _coerce_record(item: Any) -> ReplayRecord:
    expected_hash: str | None = None
    if isinstance(item, ReplayRecord):
        return ReplayRecord(
            request=copy.deepcopy(item.request),
            response=copy.deepcopy(item.response),
            call_metadata=copy.deepcopy(item.call_metadata),
        )
    if isinstance(item, (tuple, list)) and len(item) in {2, 3}:
        request, response = item[0], item[1]
        metadata = item[2] if len(item) == 3 else None
    elif isinstance(item, Mapping):
        request = item.get("request")
        response = item.get("response")
        metadata = item.get("call_metadata", item.get("metadata"))
        expected_hash = item.get("request_semantic_hash")
    else:
        raise ReplayDataError("replay record must contain request and response")
    if not isinstance(request, Mapping) or not isinstance(response, Mapping):
        raise ReplayDataError("replay record request and response must be objects")
    if metadata is not None and not isinstance(metadata, Mapping):
        raise ReplayDataError("replay call metadata must be an object")
    record = ReplayRecord(
        request=copy.deepcopy(dict(request)),
        response=copy.deepcopy(dict(response)),
        call_metadata=copy.deepcopy(dict(metadata)) if metadata is not None else None,
    )
    if expected_hash is not None:
        try:
            actual_hash = record.request_semantic_hash
        except Exception:
            raise ReplayDataError(
                "replay identity key does not match request",
                classification=FailureClassification.REPLAY_MISMATCH,
            ) from None
        if expected_hash != actual_hash:
            raise ReplayDataError(
                "replay identity key does not match request",
                classification=FailureClassification.REPLAY_MISMATCH,
            )
    return record


def _record_matches_request(record: ReplayRecord, request: Mapping[str, Any]) -> bool:
    response = record.response
    return (
        response.get("model_request_id") == request.get("model_request_id")
        and response.get("provider") == request.get("provider")
        and response.get("model") == request.get("model")
        and response.get("adapter") == request.get("adapter")
    )


def _same_response(first: Mapping[str, Any], second: Mapping[str, Any]) -> bool:
    try:
        return first.get("content_hash") == second.get("content_hash")
    except AttributeError:
        return False


__all__ = [
    "ReplayAdapter",
    "ReplayDataError",
    "ReplayError",
    "ReplayMismatchError",
    "ReplayMissingError",
    "ReplayRecord",
]
