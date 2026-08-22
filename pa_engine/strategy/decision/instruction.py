"""Stable scheduler-instruction construction for G018."""

from __future__ import annotations

import copy
import hashlib
import re
from decimal import Decimal
from typing import Any, Mapping

from pa_engine.contracts import canonical_json_bytes

from .errors import DecisionValidationResult, issue
from .schema import (
    BRANCHES,
    DIRECTIONS,
    MAX_DELAY_SECONDS,
    MIN_DELAY_SECONDS,
    SCHEMA_VERSION,
    _validate_market_view,
    _validate_second_event,
    canonical_positive_decimal,
)


INSTRUCTION_VERSION = SCHEMA_VERSION
INSTRUCTION_TYPE = "strategy_scheduler_instruction"


def _instruction_projection(document: Mapping[str, Any]) -> dict[str, Any]:
    return {key: copy.deepcopy(value) for key, value in document.items() if key not in {"instruction_id", "instruction_hash"}}


def instruction_hash(document: Mapping[str, Any]) -> str:
    """Hash only content-derived instruction fields."""

    return hashlib.sha256(canonical_json_bytes(_instruction_projection(document))).hexdigest()


def make_scheduler_instruction(
    candidate: Mapping[str, Any],
    *,
    profile_ref: Mapping[str, Any],
    result: Mapping[str, Any],
    evidence_package: Mapping[str, Any],
) -> dict[str, Any]:
    """Build one deterministic, account-free scheduler instruction.

    The function assumes parser/grounding and G014 result validation already
    passed.  It still derives all identity fields from content, so repeating
    a decision yields byte-identical output.
    """

    branch = candidate["branch"]
    view = copy.deepcopy(candidate["shared_market_view_ref"])
    second = copy.deepcopy(candidate.get("second_event_ref"))
    if branch == "no_entry":
        entry = None
        direction = None
        next_call = candidate["N"]
        wait_seconds = None
    elif branch == "enter_immediately":
        entry = second["reference_price"]
        direction = candidate["direction"]
        next_call = None
        wait_seconds = None
    else:
        entry = candidate["entry_price"]
        direction = candidate["direction"]
        next_call = None
        wait_seconds = candidate["X"]

    # IDs/hashes are intentionally nested as immutable lineage references;
    # there are no account, quantity, R or P&L fields in this wire object.
    document: dict[str, Any] = {
        "schema_version": INSTRUCTION_VERSION,
        "instruction_type": INSTRUCTION_TYPE,
        "branch": branch,
        "strategy_id": profile_ref["strategy_id"],
        "strategy_profile_ref": copy.deepcopy(dict(profile_ref)),
        "profile_ref_id": profile_ref["profile_ref_id"],
        "profile_ref_hash": profile_ref["profile_ref_hash"],
        "result_id": result["pa_analysis_result_id"],
        "result_hash": result["content_hash"],
        "evidence_package_id": evidence_package["evidence_package_id"],
        "evidence_package_hash": evidence_package["content_hash"],
        "market_view_ref": view,
        "second_event_ref": second,
        "direction": direction,
        "entry_price": entry,
        "stop_price": candidate.get("stop_price"),
        "target_price": candidate.get("target_price"),
        "next_call_after_seconds": next_call,
        "entry_wait_seconds": wait_seconds,
    }
    digest = instruction_hash(document)
    document["instruction_hash"] = digest
    document["instruction_id"] = "sdi_" + digest
    return document


build_scheduler_instruction = make_scheduler_instruction
make_instruction = make_scheduler_instruction


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_STRATEGY_ID_RE = re.compile(r"^pa:[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_PROFILE_ID_RE = re.compile(r"^spr_[0-9a-f]{64}$")
_RESULT_ID_RE = re.compile(r"^par_[0-9a-f]{64}$")
_EVIDENCE_ID_RE = re.compile(r"^evp_[0-9a-f]{64}$")


def _assert_instruction_json(value: Any, path: str = "$") -> None:
    value_type = type(value)
    if value_type in {type(None), bool, int, str}:
        return
    if value_type is float:
        raise ValueError(f"float at {path}")
    if value_type is dict:
        for key, child in value.items():
            if type(key) is not str:
                raise ValueError(f"non-string key at {path}")
            _assert_instruction_json(child, f"{path}.{key}")
        return
    if value_type is list:
        for index, child in enumerate(value):
            _assert_instruction_json(child, f"{path}[{index}]")
        return
    raise ValueError(f"non JSON-native value at {path}")


def _identity_ok(document: Mapping[str, Any]) -> bool:
    try:
        digest = instruction_hash(document)
        return document.get("instruction_hash") == digest and document.get("instruction_id") == "sdi_" + digest
    except Exception:
        return False


def verify_instruction(
    document: Mapping[str, Any],
    *,
    expected_profile_ref: Mapping[str, Any] | None = None,
    registry: Any = None,
    packet: Mapping[str, Any] | None = None,
    related_artifacts: Any = None,
    expected_market_view_ref: Mapping[str, Any] | None = None,
    expected_second_event_ref: Mapping[str, Any] | None = None,
) -> bool:
    """Return False for every malformed or semantically invalid instruction."""

    try:
        return validate_instruction(
            document,
            expected_profile_ref=expected_profile_ref,
            registry=registry,
            packet=packet,
            related_artifacts=related_artifacts,
            expected_market_view_ref=expected_market_view_ref,
            expected_second_event_ref=expected_second_event_ref,
        ).valid
    except Exception:
        return False


def _validate_profile_self_shape(profile_ref: Any) -> tuple[bool, str, str]:
    """Run the authoritative G016 profile self-shape check when available."""

    try:
        from pa_engine.strategies.knowledge import validate_profile_lineage

        result = validate_profile_lineage(profile_ref)
        if getattr(result, "valid", bool(result)):
            return True, "ok", "profile reference is structurally valid"
        return False, str(getattr(result, "code", "profile_binding_mismatch")), str(getattr(result, "message", "profile reference is malformed"))
    except Exception:
        return False, "profile_binding_mismatch", "profile reference self-validation failed"


def validate_instruction(
    document: Any,
    *,
    expected_profile_ref: Mapping[str, Any] | None = None,
    registry: Any = None,
    packet: Mapping[str, Any] | None = None,
    related_artifacts: Any = None,
    expected_market_view_ref: Mapping[str, Any] | None = None,
    expected_second_event_ref: Mapping[str, Any] | None = None,
) -> DecisionValidationResult:
    """Validate identity, lineage references and every branch invariant.

    This function is intentionally exception-safe because scheduler input is a
    model-facing boundary.  A re-hashed but semantically malformed object is
    rejected before identity is considered valid.
    """

    try:
        if not isinstance(document, dict):
            raise ValueError("instruction must be a JSON object")
        _assert_instruction_json(document)
        required = {
            "schema_version", "instruction_type", "branch", "strategy_id", "strategy_profile_ref", "profile_ref_id", "profile_ref_hash",
            "result_id", "result_hash", "evidence_package_id", "evidence_package_hash", "market_view_ref",
            "second_event_ref", "direction", "entry_price", "stop_price", "target_price",
            "next_call_after_seconds", "entry_wait_seconds", "instruction_hash", "instruction_id",
        }
        if set(document) != required:
            raise ValueError("instruction fields are not frozen")
        if document["schema_version"] != SCHEMA_VERSION or document["instruction_type"] != INSTRUCTION_TYPE:
            return DecisionValidationResult(False, code="version_mismatch", message="instruction version/type is unsupported", issues=[issue("version_mismatch", "$", "instruction version/type is unsupported")])
        if type(document["strategy_id"]) is not str or not _STRATEGY_ID_RE.fullmatch(document["strategy_id"]):
            raise ValueError("strategy ID is malformed")
        nested_profile = document["strategy_profile_ref"]
        if not isinstance(nested_profile, dict):
            raise ValueError("strategy_profile_ref must be a JSON object")
        profile_valid, profile_code, profile_message = _validate_profile_self_shape(nested_profile)
        if not profile_valid:
            return DecisionValidationResult(False, code=profile_code, message=profile_message, issues=[issue(profile_code, "$.strategy_profile_ref", profile_message)])
        if document["strategy_id"] != nested_profile.get("strategy_id"):
            raise ValueError("strategy_id differs from nested strategy profile")
        for field, regex in (("profile_ref_id", _PROFILE_ID_RE), ("result_id", _RESULT_ID_RE), ("evidence_package_id", _EVIDENCE_ID_RE)):
            if type(document[field]) is not str or not regex.fullmatch(document[field]):
                raise ValueError(f"{field} is malformed")
        for field in ("profile_ref_hash", "result_hash", "evidence_package_hash"):
            if type(document[field]) is not str or not _SHA256_RE.fullmatch(document[field]):
                raise ValueError(f"{field} is malformed")
        if document["profile_ref_id"] != "spr_" + document["profile_ref_hash"]:
            raise ValueError("profile reference identity is not hash-bound")
        if document["profile_ref_id"] != nested_profile.get("profile_ref_id") or document["profile_ref_hash"] != nested_profile.get("profile_ref_hash"):
            raise ValueError("root profile identity differs from nested strategy profile")
        if expected_profile_ref is not None and dict(nested_profile) != dict(expected_profile_ref):
            return DecisionValidationResult(False, code="profile_binding_mismatch", message="instruction profile differs from expected authoritative profile", issues=[issue("profile_binding_mismatch", "$.strategy_profile_ref", "instruction profile differs from expected authoritative profile")])
        if registry is not None:
            if not hasattr(registry, "validate_profile_lineage"):
                raise ValueError("authoritative registry is malformed")
            try:
                lineage_result = registry.validate_profile_lineage(nested_profile, packet=packet, artifacts=related_artifacts or ())
            except Exception:
                return DecisionValidationResult(False, code="registry_validation_failure", message="authoritative profile lineage validation failed", issues=[issue("registry_validation_failure", "$.strategy_profile_ref", "authoritative profile lineage validation failed")])
            if not getattr(lineage_result, "valid", bool(lineage_result)):
                code = str(getattr(lineage_result, "code", "profile_binding_mismatch"))
                return DecisionValidationResult(False, code=code, message="authoritative profile lineage rejected instruction profile", issues=[issue(code, "$.strategy_profile_ref", "authoritative profile lineage rejected instruction profile")])
        if document["result_id"] != "par_" + document["result_hash"]:
            raise ValueError("result identity is not hash-bound")
        if document["evidence_package_id"] != "evp_" + document["evidence_package_hash"]:
            raise ValueError("evidence identity is not hash-bound")
        _validate_market_view(document["market_view_ref"], "$.market_view_ref")
        if expected_market_view_ref is not None and dict(document["market_view_ref"]) != dict(expected_market_view_ref):
            return DecisionValidationResult(False, code="market_view_mismatch", message="instruction market view differs from expected view", issues=[issue("market_view_mismatch", "$.market_view_ref", "instruction market view differs from expected view")])
        if document["second_event_ref"] is not None:
            _validate_second_event(document["second_event_ref"], "$.second_event_ref")
        if expected_second_event_ref is not None and dict(document["second_event_ref"] or {}) != dict(expected_second_event_ref):
            return DecisionValidationResult(False, code="second_event_mismatch", message="instruction second event differs from expected event", issues=[issue("second_event_mismatch", "$.second_event_ref", "instruction second event differs from expected event")])
        branch = document["branch"]
        if branch not in BRANCHES:
            return DecisionValidationResult(False, code="invalid_branch", message="instruction branch is unsupported", issues=[issue("invalid_branch", "$.branch", "instruction branch is unsupported")])
        direction = document["direction"]
        if direction is not None and direction not in DIRECTIONS:
            raise ValueError("direction is malformed")
        for field in ("entry_price", "stop_price", "target_price"):
            if document[field] is not None:
                canonical_positive_decimal(document[field], f"$.{field}")
        n_value, x_value = document["next_call_after_seconds"], document["entry_wait_seconds"]
        for name, value in (("next_call_after_seconds", n_value), ("entry_wait_seconds", x_value)):
            if value is not None and (type(value) is not int or not MIN_DELAY_SECONDS <= value <= MAX_DELAY_SECONDS):
                raise ValueError(f"{name} must be an integer in range")
        if branch == "no_entry":
            if type(n_value) is not int or not MIN_DELAY_SECONDS <= n_value <= MAX_DELAY_SECONDS or x_value is not None:
                raise ValueError("no_entry timer fields are inconsistent")
            if any(document[field] is not None for field in ("direction", "entry_price", "stop_price", "target_price", "second_event_ref")):
                raise ValueError("no_entry fields leak actionable values")
        elif branch == "enter_immediately":
            if n_value is not None or x_value is not None or document["second_event_ref"] is None:
                raise ValueError("immediate timer/event fields are inconsistent")
            if direction not in DIRECTIONS or any(document[field] is None for field in ("entry_price", "stop_price", "target_price")):
                raise ValueError("immediate direction/levels are incomplete")
            event_price = document["second_event_ref"]["reference_price"]
            if document["entry_price"] != event_price:
                raise ValueError("immediate entry does not match trusted event price")
            entry = Decimal(event_price)
            stop, target = Decimal(document["stop_price"]), Decimal(document["target_price"])
            if direction == "long" and not stop < entry < target:
                raise ValueError("long levels are not ordered")
            if direction == "short" and not target < entry < stop:
                raise ValueError("short levels are not ordered")
        else:
            if n_value is not None or type(x_value) is not int or not MIN_DELAY_SECONDS <= x_value <= MAX_DELAY_SECONDS or document["second_event_ref"] is not None:
                raise ValueError("wait timer/event fields are inconsistent")
            if direction not in DIRECTIONS or any(document[field] is None for field in ("entry_price", "stop_price", "target_price")):
                raise ValueError("wait direction/levels are incomplete")
            entry = Decimal(document["entry_price"])
            stop, target = Decimal(document["stop_price"]), Decimal(document["target_price"])
            if direction == "long" and not stop < entry < target:
                raise ValueError("long levels are not ordered")
            if direction == "short" and not target < entry < stop:
                raise ValueError("short levels are not ordered")
        if not _identity_ok(document):
            return DecisionValidationResult(False, code="instruction_identity_mismatch", message="instruction identity does not match content", issues=[issue("instruction_identity_mismatch", "$", "instruction identity does not match content")])
        return DecisionValidationResult(True, candidate=copy.deepcopy(document))
    except Exception:
        return DecisionValidationResult(False, code="invalid_instruction", message="instruction is malformed or semantically invalid", issues=[issue("invalid_instruction", "$", "instruction is malformed or semantically invalid")])


__all__ = [
    "INSTRUCTION_TYPE",
    "INSTRUCTION_VERSION",
    "build_scheduler_instruction",
    "instruction_hash",
    "make_instruction",
    "make_scheduler_instruction",
    "validate_instruction",
    "verify_instruction",
]
