"""Strict G018 decision-candidate schema and local invariants.

The wire shape is deliberately independent from the frozen G014 candidate
shape.  It is the compact, untrusted model output consumed by the finalizer.
Every field is JSON-native; prices and confidence are canonical decimal
strings so a Python ``float`` can never silently enter a decision.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
import re
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

from .errors import DecisionIssue, DecisionValidationError, DecisionValidationResult, issue


SCHEMA_VERSION = "0.1.0"
DECISION_SCHEMA_VERSION = SCHEMA_VERSION
SCHEMA_PATH = Path(__file__).with_name("decision-profile-0.1.0.schema.json")
REASON_MAX_CODEPOINTS = 240
MIN_DELAY_SECONDS = 1
MAX_DELAY_SECONDS = 86_400

BRANCHES = frozenset({"no_entry", "enter_immediately", "wait_for_price"})
DIRECTIONS = frozenset({"long", "short"})
DECLARATIONS = frozenset({"source_assertion", "derived_interpretation", "source_not_stated"})
UNCERTAINTY_LEVELS = frozenset({"low", "medium", "high"})
UNCERTAINTY_CODES = frozenset(
    {
        "insufficient_history",
        "partial_data",
        "stale_data",
        "incomplete_last_bar_excluded",
        "no_actionable_evidence",
        "conflicting_evidence",
        "model_timeout",
        "invalid_model_response",
        "grounding_failure",
        "high_uncertainty",
    }
)

POSITIVE_DECIMAL_RE = re.compile(r"^(?:[1-9][0-9]*(?:\.[0-9]*[1-9])?|0\.[0-9]*[1-9])$")
CONFIDENCE_RE = re.compile(r"^(?:0|1|0\.[0-9]*[1-9])$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
ARTIFACT_ID_RE = {
    "market_snapshot_id": re.compile(r"^mkt_[0-9a-f]{64}$"),
    "observation_frame_id": re.compile(r"^obs_[0-9a-f]{64}$"),
}
SHARED_VIEW_ID_RE = re.compile(r"^smv_[0-9a-f]{64}$")
EVENT_ID_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.:-]{0,127}$")
EVIDENCE_ID_RE = re.compile(r"^evi_[0-9a-f]{64}$")
CITATION_ID_RE = re.compile(r"^cit_[0-9a-f]{64}$")
MARKET_REF_RE = re.compile(r"^(?:bar|obv)_[0-9a-f]{64}$")

# A candidate always has the complete set of semantic fields.  Branch-specific
# nullability is checked below, which makes accidental field leakage visible.
CANDIDATE_FIELDS = (
    "schema_version",
    "branch",
    "reason",
    "direction",
    "entry_price",
    "stop_price",
    "target_price",
    "N",
    "X",
    "grounding",
    "confidence",
    "uncertainty",
    "shared_market_view_ref",
    "second_event_ref",
)
GROUNDING_FIELDS = ("reason", "entry_price", "stop_price", "target_price")
GROUNDING_REF_FIELDS = ("declaration_type", "claim_ids", "evidence_ids", "citation_ids", "market_refs")
MARKET_VIEW_FIELDS = (
    "schema_version",
    "market_snapshot_id",
    "market_snapshot_hash",
    "observation_frame_id",
    "observation_frame_hash",
    "shared_market_view_id",
    "shared_market_view_hash",
)
SECOND_EVENT_FIELDS = ("schema_version", "event_id", "event_hash", "reference_price")


def _reject(code: str, message: str, path: str = "$") -> None:
    raise DecisionValidationError(code, message, path=path)


def _assert_json_native(value: Any, path: str = "$") -> None:
    """Reject floats, Decimal, tuples, NaN/Infinity and non-string keys."""

    value_type = type(value)
    if value_type in {type(None), bool, int, str}:
        return
    if value_type is float:
        # Keep a separate code for NaN/Infinity; all floats are invalid even
        # when finite because the wire profile is exact JSON-native Decimal.
        code = "non_finite_number" if not math.isfinite(value) else "float_not_allowed"
        _reject(code, "floating-point numbers are not allowed", path)
    # JSON-native means the built-in containers, not Mapping/list subclasses
    # or dataclass/proxy objects that merely look serializable.
    if value_type is dict:
        for key, child in value.items():
            if type(key) is not str:
                _reject("non_string_key", "object keys must be strings", path)
            _assert_json_native(child, f"{path}.{key}")
        return
    if type(value) is list:
        for index, child in enumerate(value):
            _assert_json_native(child, f"{path}[{index}]")
        return
    _reject("non_json_native", "candidate contains a non JSON-native value", path)


def reject_duplicate_json_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """``json.loads`` hook that turns duplicate keys into a typed rejection."""

    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            _reject("duplicate_field", "candidate contains a duplicate field", f"$.{key}")
        result[key] = value
    return result


def parse_json_native(value: str | bytes | bytearray) -> Any:
    """Parse JSON while rejecting constants and duplicate object keys."""

    if not isinstance(value, (str, bytes, bytearray)):
        _reject("invalid_json", "model output must be a JSON string or bytes")
    try:
        parsed = json.loads(
            value,
            object_pairs_hook=reject_duplicate_json_keys,
            parse_constant=lambda token: _reject("non_finite_number", "NaN/Infinity are not allowed"),
        )
    except DecisionValidationError:
        raise
    except (TypeError, ValueError, UnicodeError) as exc:
        del exc
        _reject("invalid_json", "model output is not valid JSON")
    _assert_json_native(parsed)
    return parsed


def _require_exact_mapping(value: Any, fields: tuple[str, ...], path: str, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        _reject("wrong_type", f"{label} must be an object", path)
    keys = set(value)
    expected = set(fields)
    missing = expected - keys
    extra = keys - expected
    if missing:
        _reject("missing_field", f"{label} is missing required fields", path)
    if extra:
        _reject("unknown_field", f"{label} contains an unknown field", path)
    return {str(key): copy.deepcopy(value[key]) for key in fields}


def _canonical_positive(value: Any, path: str) -> Decimal:
    if type(value) is not str or not POSITIVE_DECIMAL_RE.fullmatch(value):
        _reject("invalid_decimal", "price must be a canonical positive Decimal string", path)
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError):
        _reject("invalid_decimal", "price must be a canonical positive Decimal string", path)
    if not parsed.is_finite() or parsed <= 0:
        _reject("invalid_decimal", "price must be positive and finite", path)
    # The regex already excludes exponent/leading zero/trailing zero forms;
    # this extra round trip protects unusual Decimal implementations.
    if format(parsed, "f") != value:
        _reject("noncanonical_decimal", "price is not canonical", path)
    return parsed


def canonical_positive_decimal(value: Any, path: str = "$") -> str:
    """Validate and return a canonical positive Decimal string."""

    _canonical_positive(value, path)
    return str(value)


def _canonical_confidence(value: Any, path: str) -> str | None:
    if value is None:
        return None
    if type(value) is not str or not CONFIDENCE_RE.fullmatch(value):
        _reject("invalid_confidence", "confidence must be a canonical Decimal string from 0 to 1", path)
    return value


def _positive_int(value: Any, path: str) -> int:
    if type(value) is not int:
        _reject("invalid_integer", "N/X must be an integer (bool is not accepted)", path)
    if not MIN_DELAY_SECONDS <= value <= MAX_DELAY_SECONDS:
        _reject("integer_out_of_range", "N/X must be between 1 and 86400 seconds", path)
    return value


def _validate_uncertainty(value: Any, path: str) -> dict[str, Any]:
    item = _require_exact_mapping(value, ("level", "codes", "details"), path, "uncertainty")
    if item["level"] not in UNCERTAINTY_LEVELS:
        _reject("invalid_uncertainty", "uncertainty level is not supported", f"{path}.level")
    codes = item["codes"]
    if type(codes) is not list or len(set(codes)) != len(codes) or any(type(code) is not str for code in codes):
        _reject("invalid_uncertainty", "uncertainty codes must be a unique string list", f"{path}.codes")
    if any(code not in UNCERTAINTY_CODES for code in codes):
        _reject("invalid_uncertainty", "uncertainty code is not supported", f"{path}.codes")
    if type(item["details"]) is not str or not item["details"].strip() or "\n" in item["details"] or "\r" in item["details"]:
        _reject("invalid_uncertainty", "uncertainty details must be one non-empty line", f"{path}.details")
    return item


def _validate_ref_list(value: Any, path: str, *, prefix: str | None = None, min_items: int = 1) -> list[str]:
    if type(value) is not list or len(value) < min_items or len(set(value)) != len(value):
        _reject("invalid_grounding", "grounding references must be a unique non-empty list", path)
    if any(type(item) is not str or not item for item in value):
        _reject("invalid_grounding", "grounding references must be strings", path)
    if prefix is not None and any(not item.startswith(prefix) for item in value):
        _reject("invalid_grounding", "grounding reference has the wrong identity prefix", path)
    return list(value)


def _validate_grounding_ref(value: Any, path: str, *, required: bool, field_name: str) -> dict[str, Any] | None:
    if value is None:
        if required:
            _reject("missing_grounding", f"{field_name} requires grounding references", path)
        return None
    item = _require_exact_mapping(value, GROUNDING_REF_FIELDS, path, f"{field_name} grounding")
    declaration = item["declaration_type"]
    if declaration not in DECLARATIONS:
        _reject("invalid_grounding", "unsupported grounding declaration type", f"{path}.declaration_type")
    if field_name in {"entry_price", "stop_price", "target_price"} and declaration == "source_not_stated":
        _reject("source_not_stated", "source_not_stated numeric levels cannot produce an actionable instruction", f"{path}.declaration_type")
    if field_name in {"entry_price", "stop_price", "target_price"} and declaration != "derived_interpretation":
        _reject("claim_declaration_mismatch", "model-proposed numeric levels must be derived_interpretation", f"{path}.declaration_type")
    _validate_ref_list(item["claim_ids"], f"{path}.claim_ids", prefix=None)
    _validate_ref_list(item["evidence_ids"], f"{path}.evidence_ids")
    if any(type(ref) is not str or not EVIDENCE_ID_RE.fullmatch(ref) for ref in item["evidence_ids"]):
        _reject("invalid_grounding", "evidence reference has the wrong identity", f"{path}.evidence_ids")
    _validate_ref_list(item["citation_ids"], f"{path}.citation_ids")
    if any(type(ref) is not str or not CITATION_ID_RE.fullmatch(ref) for ref in item["citation_ids"]):
        _reject("invalid_grounding", "citation reference has the wrong identity", f"{path}.citation_ids")
    refs = _validate_ref_list(item["market_refs"], f"{path}.market_refs", prefix=None)
    if any(not MARKET_REF_RE.fullmatch(ref) for ref in refs):
        _reject("invalid_grounding", "market refs must be G014 bar/observation IDs", f"{path}.market_refs")
    return item


def _validate_market_view(value: Any, path: str) -> dict[str, Any]:
    item = _require_exact_mapping(value, MARKET_VIEW_FIELDS, path, "shared market view reference")
    if item["schema_version"] != SCHEMA_VERSION:
        _reject("version_mismatch", "shared market view schema version is unsupported", f"{path}.schema_version")
    for field in ("market_snapshot_hash", "observation_frame_hash"):
        if type(item[field]) is not str or not SHA256_RE.fullmatch(item[field]):
            _reject("invalid_market_reference", "market artifact hash must be lowercase SHA-256", f"{path}.{field}")
    for field in ("market_snapshot_id", "observation_frame_id"):
        regex = ARTIFACT_ID_RE[field]
        if type(item[field]) is not str or not regex.fullmatch(item[field]):
            _reject("invalid_market_reference", "market artifact ID is malformed", f"{path}.{field}")
    if type(item["shared_market_view_hash"]) is not str or not SHA256_RE.fullmatch(item["shared_market_view_hash"]):
        _reject("invalid_market_reference", "shared market view hash is malformed", f"{path}.shared_market_view_hash")
    if type(item["shared_market_view_id"]) is not str or not SHARED_VIEW_ID_RE.fullmatch(item["shared_market_view_id"]):
        _reject("invalid_market_reference", "shared market view ID is malformed", f"{path}.shared_market_view_id")
    from pa_engine.contracts.canonical import canonical_json_bytes

    projection = {field: item[field] for field in MARKET_VIEW_FIELDS if field not in {"shared_market_view_id", "shared_market_view_hash"}}
    expected_hash = hashlib.sha256(canonical_json_bytes(projection)).hexdigest()
    if item["shared_market_view_hash"] != expected_hash or item["shared_market_view_id"] != "smv_" + expected_hash:
        _reject("market_view_mismatch", "shared market view identity does not match its snapshot/frame pair", path)
    return item


def second_event_digest(
    event_id: str,
    event_hash: str | None = None,
    reference_price: str | None = None,
    *,
    content_hash: str | None = None,
) -> str:
    """Return a diagnostic digest for an upstream second-event identity.

    The digest is never used as the event identity.  Equal prices from two
    events must remain distinguishable, so the upstream event ID and hash are
    part of the projection.
    """

    from pa_engine.contracts.canonical import canonical_json_bytes

    if event_hash is None:
        event_hash = content_hash
    if type(event_id) is not str or not EVENT_ID_RE.fullmatch(event_id) or type(event_hash) is not str or not SHA256_RE.fullmatch(event_hash) or reference_price is None:
        _reject("missing_field", "trusted second-event identity is incomplete", "$.second_event_ref")
    canonical_positive_decimal(reference_price, "$.second_event_ref.reference_price")

    payload = {
        "schema_version": SCHEMA_VERSION,
        "event_id": event_id,
        "event_hash": event_hash,
        "reference_price": reference_price,
    }
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def make_second_event_ref(
    event_id: str,
    event_hash: str | None = None,
    reference_price: str | None = None,
    *,
    content_hash: str | None = None,
) -> dict[str, str]:
    """Build a compact reference from a trusted upstream second event.

    A price-only helper is intentionally unsupported: a model must not be
    able to self-sign an entry event by inventing a price-derived digest.
    ``content_hash`` is accepted as an alias for upstream event producers.
    """

    if event_hash is None:
        event_hash = content_hash
    elif content_hash is not None and content_hash != event_hash:
        _reject("second_event_mismatch", "event_hash and content_hash differ", "$.second_event_ref")
    if type(event_id) is not str or not EVENT_ID_RE.fullmatch(event_id):
        _reject("invalid_identity", "trusted second-event ID is malformed", "$.second_event_ref.event_id")
    if type(event_hash) is not str or not SHA256_RE.fullmatch(event_hash):
        _reject("invalid_identity", "trusted second-event hash is malformed", "$.second_event_ref.event_hash")
    if reference_price is None:
        _reject("missing_field", "trusted second-event reference price is required", "$.second_event_ref.reference_price")
    canonical_positive_decimal(reference_price, "$.second_event_ref.reference_price")
    return {
        "schema_version": SCHEMA_VERSION,
        "event_id": event_id,
        "event_hash": event_hash,
        "reference_price": reference_price,
    }


def _validate_second_event(value: Any, path: str) -> dict[str, Any]:
    item = _require_exact_mapping(value, SECOND_EVENT_FIELDS, path, "second-event reference")
    if item["schema_version"] != SCHEMA_VERSION:
        _reject("version_mismatch", "second-event schema version is unsupported", f"{path}.schema_version")
    if type(item["event_id"]) is not str or not EVENT_ID_RE.fullmatch(item["event_id"]):
        _reject("invalid_identity", "second-event ID is malformed", f"{path}.event_id")
    if type(item["event_hash"]) is not str or not SHA256_RE.fullmatch(item["event_hash"]):
        _reject("invalid_identity", "second-event hash is malformed", f"{path}.event_hash")
    canonical_positive_decimal(item["reference_price"], f"{path}.reference_price")
    return item


def validate_candidate(value: Any, *, parse_json: bool = True) -> DecisionValidationResult:
    """Validate a raw candidate without raising implementation exceptions."""

    try:
        candidate = parse_json_native(value) if parse_json and isinstance(value, (str, bytes, bytearray)) else value
        _assert_json_native(candidate)
        if not isinstance(candidate, Mapping):
            _reject("invalid_candidate", "candidate must be a JSON object")
        item = _require_exact_mapping(candidate, CANDIDATE_FIELDS, "$", "candidate")
        if item["schema_version"] != SCHEMA_VERSION:
            _reject("version_mismatch", "candidate schema version is unsupported", "$.schema_version")
        branch = item["branch"]
        if branch not in BRANCHES:
            _reject("invalid_branch", "candidate branch is unsupported", "$.branch")
        reason = item["reason"]
        direction = item["direction"]
        if reason is not None and type(reason) is not str:
            _reject("invalid_reason", "reason must be a string or null", "$.reason")
        if isinstance(reason, str):
            if not reason.strip() or "\n" in reason or "\r" in reason:
                _reject("invalid_reason", "reason must be one non-empty line", "$.reason")
            if len(reason) > REASON_MAX_CODEPOINTS:
                _reject("reason_too_long", "reason exceeds the 240-codepoint limit", "$.reason")
        if direction is not None and direction not in DIRECTIONS:
            _reject("invalid_direction", "direction must be long, short or null", "$.direction")
        for field in ("entry_price", "stop_price", "target_price"):
            if item[field] is not None:
                canonical_positive_decimal(item[field], f"$.{field}")
        n_value, x_value = item["N"], item["X"]
        if n_value is not None:
            _positive_int(n_value, "$.N")
        if x_value is not None:
            _positive_int(x_value, "$.X")
        if n_value is not None and x_value is not None:
            _reject("mutually_exclusive_timers", "N and X are mutually exclusive", "$")
        _canonical_confidence(item["confidence"], "$.confidence")
        _validate_uncertainty(item["uncertainty"], "$.uncertainty")
        _validate_market_view(item["shared_market_view_ref"], "$.shared_market_view_ref")
        grounding = _require_exact_mapping(item["grounding"], GROUNDING_FIELDS, "$.grounding", "grounding")

        if branch == "no_entry":
            if n_value is None:
                _reject("missing_N", "no_entry requires N", "$.N")
            if any(item[field] is not None for field in ("reason", "direction", "entry_price", "stop_price", "target_price", "X")):
                _reject("branch_field_leakage", "no_entry cannot carry reason, direction, levels or X", "$")
            if item["confidence"] is not None:
                _reject("branch_field_leakage", "no_entry cannot carry confidence", "$.confidence")
            if any(grounding[field] is not None for field in GROUNDING_FIELDS):
                _reject("branch_field_leakage", "no_entry cannot carry grounding references", "$.grounding")
            if item["second_event_ref"] is not None:
                _reject("branch_field_leakage", "no_entry cannot carry a second-event reference", "$.second_event_ref")
        elif branch == "enter_immediately":
            if reason is None or direction is None or item["stop_price"] is None or item["target_price"] is None:
                _reject("missing_field", "enter_immediately requires reason, direction, stop and target", "$")
            if any(item[field] is not None for field in ("entry_price", "N", "X")):
                _reject("branch_field_leakage", "enter_immediately cannot carry requested entry, N or X", "$")
            if grounding["reason"] is None or grounding["stop_price"] is None or grounding["target_price"] is None:
                _reject("missing_grounding", "enter_immediately reason/levels require grounding", "$.grounding")
            if grounding["entry_price"] is not None:
                _reject("branch_field_leakage", "immediate entry comes only from second-event reference", "$.grounding.entry_price")
            _validate_second_event(item["second_event_ref"], "$.second_event_ref")
            if item["confidence"] is None:
                _reject("missing_confidence", "actionable candidate requires confidence", "$.confidence")
        else:
            if reason is None or direction is None or any(item[field] is None for field in ("entry_price", "stop_price", "target_price", "X")):
                _reject("missing_field", "wait_for_price requires reason, direction, entry, stop, target and X", "$")
            if item["N"] is not None or item["second_event_ref"] is not None:
                _reject("branch_field_leakage", "wait_for_price cannot carry N or a second-event reference", "$")
            if any(grounding[field] is None for field in GROUNDING_FIELDS):
                _reject("missing_grounding", "wait_for_price reason and levels require grounding", "$.grounding")
            if item["confidence"] is None:
                _reject("missing_confidence", "actionable candidate requires confidence", "$.confidence")

        required_grounding_fields = {
            "reason": branch in {"enter_immediately", "wait_for_price"},
            "entry_price": branch == "wait_for_price",
            "stop_price": branch in {"enter_immediately", "wait_for_price"},
            "target_price": branch in {"enter_immediately", "wait_for_price"},
        }
        for field_name in GROUNDING_FIELDS:
            grounding[field_name] = _validate_grounding_ref(
                grounding[field_name],
                f"$.grounding.{field_name}",
                required=required_grounding_fields[field_name],
                field_name=field_name,
            )

        # Directional level ordering uses the supplied internal event price for
        # immediate entries, never a fake G014 bar/observation reference.
        if branch == "enter_immediately":
            entry = Decimal(item["second_event_ref"]["reference_price"])
        elif branch == "wait_for_price":
            entry = Decimal(item["entry_price"])
        else:
            entry = None
        if entry is not None:
            stop, target = Decimal(item["stop_price"]), Decimal(item["target_price"])
            if direction == "long" and not (stop < entry < target):
                _reject("invalid_level_order", "long levels must satisfy stop < entry < target", "$")
            if direction == "short" and not (target < entry < stop):
                _reject("invalid_level_order", "short levels must satisfy target < entry < stop", "$")

        normalized = copy.deepcopy(dict(item))
        normalized["grounding"] = grounding
        return DecisionValidationResult(True, candidate=normalized)
    except DecisionValidationError as exc:
        return DecisionValidationResult(
            False,
            code=exc.code,
            message=exc.message,
            issues=[issue(exc.code, exc.path, exc.message)],
            details=exc.details,
        )
    except Exception:
        # This final catch is intentionally broad at the untrusted boundary;
        # callers get a stable code and never a traceback or model payload.
        return DecisionValidationResult(False, code="invalid_candidate", message="candidate validation failed", issues=[issue("invalid_candidate", "$", "candidate validation failed")])


def parse_candidate(value: Any) -> dict[str, Any]:
    """Strictly parse and validate a candidate, raising a typed error."""

    result = validate_candidate(value)
    if not result.valid or result.candidate is None:
        first = result.issues[0] if result.issues else issue(result.code, "$", result.message)
        raise DecisionValidationError(first.code, first.message, path=first.path, details=result.details)
    return copy.deepcopy(result.candidate)


def model_output_digest(candidate: Mapping[str, Any]) -> str:
    """Hash one parsed candidate for the compact G022 hand-off."""

    from pa_engine.contracts.canonical import canonical_json_bytes

    return hashlib.sha256(canonical_json_bytes(dict(candidate))).hexdigest()


def make_model_output_ref(candidate: Mapping[str, Any]) -> dict[str, Any]:
    """Build a compact content-derived model-output reference."""

    parsed = parse_candidate(candidate)
    digest = model_output_digest(parsed)
    return {"schema_version": SCHEMA_VERSION, "candidate_id": "moc_" + digest, "candidate_hash": digest, "candidate": parsed}


def validate_model_output_ref(value: Any) -> DecisionValidationResult:
    """Validate the compact G022 model-output reference."""

    try:
        item = _require_exact_mapping(value, ("schema_version", "candidate_id", "candidate_hash", "candidate"), "$", "model-output reference")
        if item["schema_version"] != SCHEMA_VERSION:
            _reject("version_mismatch", "model-output reference version is unsupported")
        if type(item["candidate_hash"]) is not str or not SHA256_RE.fullmatch(item["candidate_hash"]):
            _reject("invalid_identity", "model-output candidate hash is malformed")
        candidate = parse_candidate(item["candidate"])
        digest = model_output_digest(candidate)
        if item["candidate_hash"] != digest or item["candidate_id"] != "moc_" + digest:
            _reject("identity_mismatch", "model-output reference identity does not match candidate")
        return DecisionValidationResult(True, candidate=item)
    except DecisionValidationError as exc:
        return DecisionValidationResult(False, code=exc.code, message=exc.message, issues=[issue(exc.code, exc.path, exc.message)], details=exc.details)


# Naming aliases used by provider/model adapters and integration callers.
parse_model_output = parse_candidate
validate_model_output = validate_candidate
parse_untrusted_candidate = parse_candidate
validate_untrusted_candidate = validate_candidate


__all__ = [
    "BRANCHES",
    "CANDIDATE_FIELDS",
    "CONFIDENCE_RE",
    "DECISION_SCHEMA_VERSION",
    "DECLARATIONS",
    "DIRECTIONS",
    "GROUNDING_FIELDS",
    "GROUNDING_REF_FIELDS",
    "MAX_DELAY_SECONDS",
    "MIN_DELAY_SECONDS",
    "MARKET_VIEW_FIELDS",
    "REASON_MAX_CODEPOINTS",
    "SCHEMA_VERSION",
    "SCHEMA_PATH",
    "SECOND_EVENT_FIELDS",
    "UNCERTAINTY_CODES",
    "canonical_positive_decimal",
    "make_second_event_ref",
    "make_model_output_ref",
    "model_output_digest",
    "parse_candidate",
    "parse_json_native",
    "parse_model_output",
    "parse_untrusted_candidate",
    "second_event_digest",
    "validate_candidate",
    "validate_model_output",
    "validate_model_output_ref",
    "validate_untrusted_candidate",
]
