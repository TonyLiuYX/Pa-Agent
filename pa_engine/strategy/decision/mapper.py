"""G018 candidate grounding and frozen G014 result mapping."""

from __future__ import annotations

import copy
from collections.abc import Mapping
from typing import Any

from pa_engine.contracts import finalize_artifact, validate_artifact

from .errors import DecisionFinalization, DecisionIssue, DecisionValidationResult, issue
from .grounding import validate_grounding
from .instruction import make_scheduler_instruction, validate_instruction
from .schema import SCHEMA_VERSION, make_model_output_ref, parse_candidate, validate_candidate


ENGINE_VERSION = "g018-decision-finalizer-0.1.0"


def _as_issue(value: Any, fallback_code: str = "grounding_failure") -> DecisionIssue:
    if isinstance(value, DecisionIssue):
        return value
    code = getattr(value, "code", fallback_code)
    path = getattr(value, "path", "$")
    message = getattr(value, "message", "decision validation failed")
    if isinstance(value, Mapping):
        code, path, message = value.get("code", code), value.get("path", path), value.get("message", message)
    return issue(str(code), str(path), str(message))


def _artifact_values(
    *,
    artifacts: Any = None,
    analysis_request: Mapping[str, Any] | None = None,
    evidence_package: Mapping[str, Any] | None = None,
    normalized_model_request: Mapping[str, Any] | None = None,
    normalized_model_response: Mapping[str, Any] | None = None,
    model_call_metadata: Mapping[str, Any] | None = None,
    market_snapshot: Mapping[str, Any] | None = None,
    observation_frame: Mapping[str, Any] | None = None,
) -> dict[str, Mapping[str, Any]]:
    values: dict[str, Mapping[str, Any]] = {}
    if isinstance(artifacts, Mapping):
        if isinstance(artifacts.get("artifact_kind"), str):
            values[str(artifacts["artifact_kind"])] = artifacts
        else:
            for key, value in artifacts.items():
                if isinstance(value, Mapping) and isinstance(value.get("artifact_kind"), str):
                    values[str(value["artifact_kind"])] = value
                elif isinstance(key, str) and isinstance(value, Mapping):
                    values[key] = value
    elif isinstance(artifacts, (list, tuple)):
        for value in artifacts:
            if isinstance(value, Mapping) and isinstance(value.get("artifact_kind"), str):
                values[str(value["artifact_kind"])] = value
    explicit = {
        "analysis_request": analysis_request,
        "knowledge_evidence_package": evidence_package,
        "normalized_model_request": normalized_model_request,
        "normalized_model_response": normalized_model_response,
        "model_call_metadata": model_call_metadata,
        "market_snapshot": market_snapshot,
        "market_observation_frame": observation_frame,
    }
    for kind, value in explicit.items():
        if isinstance(value, Mapping):
            values[kind] = value
    return values


def _lineage(document: Mapping[str, Any], id_field: str) -> tuple[Any, Any]:
    hash_field = "content_hash"
    return document.get(id_field), document.get(hash_field)


def _grounded_values(ref: Mapping[str, Any], *, text: str, support_key: str) -> dict[str, Any]:
    declaration = ref["declaration_type"]
    return {
        support_key: declaration,
        "text": text,
        "evidence_ids": list(ref["evidence_ids"]),
        "claim_ids": list(ref["claim_ids"]),
        "citation_ids": list(ref["citation_ids"]),
        "market_refs": list(ref["market_refs"]),
        "extensions": {},
    }


def _level(ref: Mapping[str, Any], *, level_kind: str, value: str, text: str) -> dict[str, Any]:
    return {
        "level_kind": level_kind,
        "value": value,
        "declaration_type": ref["declaration_type"],
        "text": text,
        "evidence_ids": list(ref["evidence_ids"]),
        "claim_ids": list(ref["claim_ids"]),
        "citation_ids": list(ref["citation_ids"]),
        "market_refs": list(ref["market_refs"]),
        "extensions": {},
    }


def _uncertainty(candidate: Mapping[str, Any], evidence: Mapping[str, Any]) -> dict[str, Any]:
    value = copy.deepcopy(candidate["uncertainty"])
    # Preserve evidence uncertainty/conflicts without changing the candidate's
    # declared level.  G014's closed warning vocabulary carries only the
    # conflict marker; the full conflict text remains in extensions.
    evidence_uncertainty = evidence.get("uncertainty") if isinstance(evidence.get("uncertainty"), Mapping) else None
    if isinstance(evidence_uncertainty, Mapping):
        level_rank = {"low": 0, "medium": 1, "high": 2}
        candidate_level = str(value.get("level", "low"))
        evidence_level = str(evidence_uncertainty.get("level", candidate_level))
        if level_rank.get(evidence_level, 0) > level_rank.get(candidate_level, 0):
            value["level"] = evidence_level
        codes = list(value.get("codes", []))
        for code in evidence_uncertainty.get("codes", []) if isinstance(evidence_uncertainty.get("codes"), list) else []:
            if code not in codes:
                codes.append(code)
        value["codes"] = codes
        details = str(value.get("details", ""))
        evidence_details = evidence_uncertainty.get("details")
        if evidence_details and str(evidence_details) not in details:
            value["details"] = details + "; evidence: " + str(evidence_details)
    return value


def map_candidate_to_normalized_model_candidate(
    candidate: Mapping[str, Any] | str,
    profile_ref: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Map one parsed G018 candidate to the frozen G014 candidate shape.

    This is the public content-binding seam shared by G018 and future G022
    adapters.  It is deterministic and contains no artifact IDs generated by
    the finalizer.  Callers should pass the same ``profile_ref`` that is used
    for the surrounding artifacts so the setup label is stable.
    """

    parsed = parse_candidate(candidate)
    branch = parsed["branch"]
    direction = parsed.get("direction")
    mapped: dict[str, Any] = {
        "status": {"no_entry": "no_trade", "enter_immediately": "signal", "wait_for_price": "conditional"}[branch],
        "stance": "neutral" if direction is None else ("bullish" if direction == "long" else "bearish"),
        "action": {"no_entry": "none", "enter_immediately": "buy" if direction == "long" else "sell", "wait_for_price": "wait"}[branch],
        "setups": [],
        "rationales": [],
        "conditions": [],
        "levels": [],
        "model_confidence": parsed.get("confidence"),
        "uncertainty": copy.deepcopy(parsed["uncertainty"]),
        "extensions": {},
    }
    grounding = parsed["grounding"]
    if branch != "no_entry":
        reason_ref = grounding["reason"]
        mapped["rationales"].append(_grounded_values(reason_ref, text=parsed["reason"], support_key="support_type"))
        mapped["setups"].append(
            {
                "label": str((profile_ref or {}).get("strategy_id", "g018")),
                "declaration_type": reason_ref["declaration_type"],
                "evidence_ids": list(reason_ref["evidence_ids"]),
                "claim_ids": list(reason_ref["claim_ids"]),
                "citation_ids": list(reason_ref["citation_ids"]),
                "market_refs": list(reason_ref["market_refs"]),
                "extensions": {},
            }
        )
        if branch == "wait_for_price":
            entry_ref = grounding["entry_price"]
            mapped["conditions"].append(
                _grounded_values(
                    entry_ref,
                    text="Wait for the requested entry price " + str(parsed["entry_price"]),
                    support_key="declaration_type",
                )
            )
            mapped["levels"].append(_level(entry_ref, level_kind="entry", value=parsed["entry_price"], text="Requested entry price"))
        mapped["levels"].append(_level(grounding["stop_price"], level_kind="stop", value=parsed["stop_price"], text="Model-derived protective stop"))
        mapped["levels"].append(_level(grounding["target_price"], level_kind="target", value=parsed["target_price"], text="Model-derived target"))
    return mapped


def _make_result(
    candidate: Mapping[str, Any],
    *,
    profile_ref: Mapping[str, Any],
    packet: Mapping[str, Any],
    request: Mapping[str, Any],
    market: Mapping[str, Any],
    observation: Mapping[str, Any],
    evidence: Mapping[str, Any],
    model_request: Mapping[str, Any],
    model_response: Mapping[str, Any],
    model_call: Mapping[str, Any] | None,
) -> dict[str, Any]:
    branch = candidate["branch"]
    mapped_candidate = map_candidate_to_normalized_model_candidate(candidate, profile_ref=profile_ref)
    status = mapped_candidate["status"]
    stance = mapped_candidate["stance"]
    action = mapped_candidate["action"]
    setups = copy.deepcopy(mapped_candidate["setups"])
    rationales = copy.deepcopy(mapped_candidate["rationales"])
    conditions = copy.deepcopy(mapped_candidate["conditions"])
    levels = copy.deepcopy(mapped_candidate["levels"])

    model_call_id, model_call_hash = (None, None)
    if isinstance(model_call, Mapping):
        model_call_id, model_call_hash = model_call.get("model_call_id"), model_call.get("content_hash")
    document: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "pa_analysis_result",
        "analysis_request_id": request.get("analysis_request_id"),
        "analysis_request_hash": request.get("content_hash"),
        "market_snapshot_id": market.get("market_snapshot_id"),
        "market_snapshot_hash": market.get("content_hash"),
        "observation_frame_id": observation.get("observation_frame_id"),
        "observation_frame_hash": observation.get("content_hash"),
        "evidence_package_id": evidence.get("evidence_package_id"),
        "evidence_package_hash": evidence.get("content_hash"),
        "model_request_id": model_request.get("model_request_id"),
        "model_request_hash": model_request.get("content_hash"),
        "model_response_id": model_response.get("model_response_id"),
        "model_response_hash": model_response.get("content_hash"),
        "model_call_id": model_call_id,
        "model_call_hash": model_call_hash,
        "engine_version": ENGINE_VERSION,
        "prompt_template_version": model_request.get("prompt_template_version", "g018-prompt-0.1.0"),
        "policy_version": "0.1.0",
        "adapter_version": model_response.get("adapter", "g018-normalized-response-0.1.0"),
        "status": status,
        "stance": stance,
        "action": action,
        "setups": setups,
        "rationales": rationales,
        "conditions": conditions,
        "levels": levels,
        "knowledge_confidence": candidate.get("confidence"),
        "model_confidence": candidate.get("confidence"),
        "analysis_uncertainty": _uncertainty(candidate, evidence),
        "warnings": [],
        "errors": [],
        "analysis_as_of": request.get("analysis_as_of"),
        "input_mode": request.get("input_mode", "ohlcv"),
        "no_live_orders": True,
        "extensions": {
            "strategy_profile_ref": copy.deepcopy(dict(profile_ref)),
            "g018_shared_market_view_ref": copy.deepcopy(candidate["shared_market_view_ref"]),
            "g018_second_event_ref": copy.deepcopy(candidate.get("second_event_ref")),
            "g018_model_output_ref": make_model_output_ref(candidate),
            "g018_evidence_conflicts": copy.deepcopy(evidence.get("conflicts", [])),
        },
    }
    # G014 warning codes are closed; preserve only recognized ones in the
    # warning list while retaining all details above.
    uncertainty_codes = document["analysis_uncertainty"].get("codes", [])
    if "conflicting_evidence" in uncertainty_codes or evidence.get("conflicts"):
        document["warnings"].append({"code": "conflicting_evidence", "message": "evidence package reports a conflict", "extensions": {}})
    for code in ("partial_data", "stale_data", "incomplete_last_bar_excluded"):
        if code in uncertainty_codes:
            document["warnings"].append({"code": code, "message": "decision input carries market/evidence uncertainty", "extensions": {}})
    return finalize_artifact(document, "pa_analysis_result")


def _required(values: Mapping[str, Mapping[str, Any]], kind: str) -> Mapping[str, Any] | None:
    return values.get(kind)


def finalize_decision(
    candidate: Mapping[str, Any] | str | bytes,
    *,
    registry: Any = None,
    packet: Mapping[str, Any] | None = None,
    profile_ref: Mapping[str, Any] | None = None,
    evidence_package: Mapping[str, Any] | None = None,
    market_snapshot: Mapping[str, Any] | None = None,
    observation_frame: Mapping[str, Any] | None = None,
    analysis_request: Mapping[str, Any] | None = None,
    normalized_model_request: Mapping[str, Any] | None = None,
    normalized_model_response: Mapping[str, Any] | None = None,
    model_call_metadata: Mapping[str, Any] | None = None,
    second_event: Mapping[str, Any] | None = None,
    artifacts: Any = None,
) -> DecisionFinalization:
    """Finalize an untrusted candidate into a validated G014 result/instruction."""

    parsed_result = validate_candidate(candidate, parse_json=not isinstance(candidate, Mapping))
    if not parsed_result.valid or parsed_result.candidate is None:
        return DecisionFinalization(False, parsed_result.code, parsed_result.message, issues=parsed_result.issues, details=parsed_result.details)
    values = _artifact_values(
        artifacts=artifacts,
        analysis_request=analysis_request,
        evidence_package=evidence_package,
        normalized_model_request=normalized_model_request,
        normalized_model_response=normalized_model_response,
        model_call_metadata=model_call_metadata,
        market_snapshot=market_snapshot,
        observation_frame=observation_frame,
    )
    required_kinds = ("analysis_request", "knowledge_evidence_package", "market_snapshot", "market_observation_frame", "normalized_model_request", "normalized_model_response")
    missing = [kind for kind in required_kinds if not isinstance(values.get(kind), Mapping)]
    if not isinstance(packet, Mapping) or not isinstance(profile_ref, Mapping):
        missing.append("strategy packet/profile_ref")
    if missing:
        return DecisionFinalization(False, "missing_artifact", "required G014 artifacts are missing", candidate=parsed_result.candidate, issues=[issue("missing_artifact", "$.artifacts", "required G014 artifacts are missing")], details={"missing": missing})
    try:
        grounding = validate_grounding(
            parsed_result.candidate,
            registry=registry,
            packet=packet,
            profile_ref=profile_ref,
            evidence_package=values["knowledge_evidence_package"],
            market_snapshot=values["market_snapshot"],
            observation_frame=values["market_observation_frame"],
            analysis_request=values["analysis_request"],
            normalized_model_request=values["normalized_model_request"],
            normalized_model_response=values["normalized_model_response"],
            second_event=second_event,
            artifacts=values,
        )
    except Exception:
        return DecisionFinalization(False, "grounding_failure", "decision grounding failed", candidate=parsed_result.candidate, issues=[issue("grounding_failure", "$", "decision grounding failed")])
    if not grounding.valid:
        return DecisionFinalization(False, grounding.code, grounding.message, candidate=parsed_result.candidate, issues=[_as_issue(item) for item in grounding.issues], details=grounding.details)
    try:
        result = _make_result(
            parsed_result.candidate,
            profile_ref=profile_ref,
            packet=packet,
            request=values["analysis_request"],
            market=values["market_snapshot"],
            observation=values["market_observation_frame"],
            evidence=values["knowledge_evidence_package"],
            model_request=values["normalized_model_request"],
            model_response=values["normalized_model_response"],
            model_call=values.get("model_call_metadata"),
        )
        related = [values[k] for k in required_kinds if isinstance(values.get(k), Mapping)]
        if isinstance(values.get("model_call_metadata"), Mapping):
            related.append(values["model_call_metadata"])
        validation = validate_artifact(result, expected_kind="pa_analysis_result", related_artifacts=related)
        if not validation.valid:
            return DecisionFinalization(False, "result_validation_failure", "G014 result validation failed", candidate=parsed_result.candidate, issues=[_as_issue(item, "result_validation_failure") for item in validation.issues])
        lineage_artifacts = [values[k] for k in ("analysis_request", "knowledge_evidence_package", "normalized_model_request", "normalized_model_response")]
        lineage_artifacts.append(result)
        lineage_result = registry.validate_profile_lineage(profile_ref, packet=packet, artifacts=lineage_artifacts)
        if not getattr(lineage_result, "valid", bool(lineage_result)):
            code = getattr(lineage_result, "code", "profile_binding_mismatch")
            return DecisionFinalization(False, str(code), "authoritative profile lineage rejected final result", candidate=parsed_result.candidate, issues=[issue(str(code), "$.result.extensions.strategy_profile_ref", "authoritative profile lineage rejected final result")])
        instruction = make_scheduler_instruction(parsed_result.candidate, profile_ref=profile_ref, result=result, evidence_package=values["knowledge_evidence_package"])
        instruction_validation = validate_instruction(
            instruction,
            expected_profile_ref=profile_ref,
            expected_market_view_ref=parsed_result.candidate["shared_market_view_ref"],
            expected_second_event_ref=parsed_result.candidate.get("second_event_ref"),
        )
        if not instruction_validation.valid:
            return DecisionFinalization(False, "instruction_validation_failure", "scheduler instruction validation failed", candidate=parsed_result.candidate, issues=[_as_issue(item, "instruction_validation_failure") for item in instruction_validation.issues])
        return DecisionFinalization(True, candidate=parsed_result.candidate, result=result, instruction=instruction, details={"engine_version": ENGINE_VERSION})
    except Exception:
        # No traceback crosses the finalizer boundary; callers can route the
        # typed failure to a retry/abstention path.
        return DecisionFinalization(False, "finalization_failure", "decision finalization failed", candidate=parsed_result.candidate, issues=[issue("finalization_failure", "$", "decision finalization failed")])


def finalize_candidate(*args: Any, **kwargs: Any) -> DecisionFinalization:
    """Compatibility alias for :func:`finalize_decision`."""

    return finalize_decision(*args, **kwargs)


map_to_normalized_model_candidate = map_candidate_to_normalized_model_candidate
map_g018_candidate_to_g014 = map_candidate_to_normalized_model_candidate
map_candidate_to_g014 = map_candidate_to_normalized_model_candidate
map_candidate = finalize_decision
ground_and_map = finalize_decision
map_to_g014_result = finalize_decision
finalize_artifacts = finalize_decision


__all__ = [
    "ENGINE_VERSION",
    "finalize_candidate",
    "finalize_decision",
    "ground_and_map",
    "finalize_artifacts",
    "map_candidate",
    "map_candidate_to_normalized_model_candidate",
    "map_to_normalized_model_candidate",
    "map_g018_candidate_to_g014",
    "map_candidate_to_g014",
    "map_to_g014_result",
]
