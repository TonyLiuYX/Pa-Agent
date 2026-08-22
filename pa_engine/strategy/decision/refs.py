"""G018 shared-market and internal-second reference helpers."""

from __future__ import annotations

import copy
import hashlib
from typing import Any, Mapping

from pa_engine.contracts import canonical_json_bytes, validate_artifact

from .errors import DecisionValidationResult, issue
from .schema import MARKET_VIEW_FIELDS, SCHEMA_VERSION, _validate_market_view, make_second_event_ref


def market_view_ref_for(market_snapshot: Mapping[str, Any], observation_frame: Mapping[str, Any]) -> dict[str, str]:
    """Create the exact small view reference for a G014 snapshot/frame pair."""

    if not isinstance(market_snapshot, Mapping) or not isinstance(observation_frame, Mapping):
        raise TypeError("market snapshot and observation frame must be objects")
    pair = {
        "schema_version": SCHEMA_VERSION,
        "market_snapshot_id": str(market_snapshot.get("market_snapshot_id")),
        "market_snapshot_hash": str(market_snapshot.get("content_hash")),
        "observation_frame_id": str(observation_frame.get("observation_frame_id")),
        "observation_frame_hash": str(observation_frame.get("content_hash")),
    }
    # The shared-view identity is content-derived from the actual pair.  It is
    # intentionally separate from either artifact identity so a candidate
    # cannot self-sign a different market view with the same pair fields.
    shared_hash = hashlib.sha256(canonical_json_bytes(pair)).hexdigest()
    pair["shared_market_view_hash"] = shared_hash
    pair["shared_market_view_id"] = "smv_" + shared_hash
    return pair


shared_market_view_ref_for = market_view_ref_for
make_shared_market_view_ref = market_view_ref_for


def market_view_ref_digest(value: Mapping[str, Any]) -> str:
    """Return the shared-view content hash for a view reference.

    The projection intentionally excludes the derived ID/hash fields, so this
    function agrees with ``market_view_ref_for(...)["shared_market_view_hash"]``.
    """

    projection = {key: value[key] for key in MARKET_VIEW_FIELDS if key not in {"shared_market_view_id", "shared_market_view_hash"} and key in value}
    return hashlib.sha256(canonical_json_bytes(projection)).hexdigest()


def _artifact_map(artifacts: Any) -> dict[str, Mapping[str, Any]]:
    result: dict[str, Mapping[str, Any]] = {}
    if isinstance(artifacts, Mapping):
        # A mapping keyed by artifact kind is the convenient public form.
        if isinstance(artifacts.get("artifact_kind"), str):
            result[str(artifacts["artifact_kind"])] = artifacts
        else:
            for key, value in artifacts.items():
                if isinstance(value, Mapping) and isinstance(value.get("artifact_kind"), str):
                    result[str(value["artifact_kind"])] = value
                elif isinstance(key, str) and isinstance(value, Mapping):
                    result[key] = value
    elif isinstance(artifacts, (list, tuple)):
        for value in artifacts:
            if isinstance(value, Mapping) and isinstance(value.get("artifact_kind"), str):
                result[str(value["artifact_kind"])] = value
    return result


def validate_market_view_ref(
    value: Any,
    *,
    market_snapshot: Mapping[str, Any] | None = None,
    observation_frame: Mapping[str, Any] | None = None,
    related_artifacts: Any = None,
) -> DecisionValidationResult:
    """Validate a view reference and hash-bind it to actual G014 artifacts."""

    try:
        ref = _validate_market_view(value, "$.shared_market_view_ref")
    except Exception as exc:
        code = getattr(exc, "code", "invalid_market_reference")
        message = getattr(exc, "message", "shared market view reference is malformed")
        path = getattr(exc, "path", "$.shared_market_view_ref")
        return DecisionValidationResult(False, code=code, message=message, issues=[issue(code, path, message)])

    mapped = _artifact_map(related_artifacts)
    market_snapshot = market_snapshot or mapped.get("market_snapshot")
    observation_frame = observation_frame or mapped.get("market_observation_frame")
    if market_snapshot is None or observation_frame is None:
        return DecisionValidationResult(False, code="missing_market_lineage", message="shared market view requires snapshot and observation frame", issues=[issue("missing_market_lineage", "$.shared_market_view_ref", "market snapshot/observation frame is required")])

    snapshot_validation = validate_artifact(market_snapshot, expected_kind="market_snapshot")
    if not snapshot_validation.valid:
        return DecisionValidationResult(False, code="market_validation_failure", message="market snapshot validation failed", issues=[issue(i.code, i.path, "market snapshot validation failed") for i in snapshot_validation.issues])
    observation_validation = validate_artifact(observation_frame, expected_kind="market_observation_frame", related_artifacts=[market_snapshot])
    if not observation_validation.valid:
        return DecisionValidationResult(False, code="market_validation_failure", message="observation frame validation failed", issues=[issue(i.code, i.path, "observation frame validation failed") for i in observation_validation.issues])

    expected = market_view_ref_for(market_snapshot, observation_frame)
    if dict(ref) != expected:
        return DecisionValidationResult(False, code="market_view_mismatch", message="shared market view does not match supplied artifacts", issues=[issue("market_view_mismatch", "$.shared_market_view_ref", "market view reference differs from artifact pair")])
    return DecisionValidationResult(True, candidate=copy.deepcopy(ref))


def validate_second_event_ref(value: Any) -> DecisionValidationResult:
    """Validate a compact internal second-event reference."""

    from .schema import _validate_second_event

    try:
        result = _validate_second_event(value, "$.second_event_ref")
    except Exception as exc:
        code = getattr(exc, "code", "second_event_mismatch")
        message = getattr(exc, "message", "second-event reference is malformed")
        path = getattr(exc, "path", "$.second_event_ref")
        return DecisionValidationResult(False, code=code, message=message, issues=[issue(code, path, message)])
    return DecisionValidationResult(True, candidate=copy.deepcopy(result))


__all__ = [
    "make_second_event_ref",
    "make_shared_market_view_ref",
    "market_view_ref_digest",
    "market_view_ref_for",
    "shared_market_view_ref_for",
    "validate_market_view_ref",
    "validate_second_event_ref",
]
