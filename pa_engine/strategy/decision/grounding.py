"""Authoritative G016/G014 grounding checks for G018 decisions."""

from __future__ import annotations

import copy
from collections.abc import Iterable, Mapping
from typing import Any

from pa_engine.contracts import validate_artifact

from .errors import DecisionValidationResult, issue
from .refs import validate_market_view_ref
from .schema import DECLARATIONS, make_second_event_ref, validate_candidate


def _artifacts_by_kind(artifacts: Any) -> dict[str, Mapping[str, Any]]:
    result: dict[str, Mapping[str, Any]] = {}
    if isinstance(artifacts, Mapping):
        if isinstance(artifacts.get("artifact_kind"), str):
            result[str(artifacts["artifact_kind"])] = artifacts
        else:
            for key, value in artifacts.items():
                if isinstance(value, Mapping) and isinstance(value.get("artifact_kind"), str):
                    result[str(value["artifact_kind"])] = value
                elif isinstance(key, str) and isinstance(value, Mapping):
                    result[key] = value
    elif isinstance(artifacts, Iterable) and not isinstance(artifacts, (str, bytes, bytearray)):
        for value in artifacts:
            if isinstance(value, Mapping) and isinstance(value.get("artifact_kind"), str):
                result[str(value["artifact_kind"])] = value
    return result


def _profile_ref_from_artifact(artifact: Mapping[str, Any]) -> Mapping[str, Any] | None:
    extensions = artifact.get("extensions")
    if isinstance(extensions, Mapping) and isinstance(extensions.get("strategy_profile_ref"), Mapping):
        return extensions["strategy_profile_ref"]
    return None


def _result(valid: bool, code: str = "ok", message: str = "decision grounding is valid", *, details: Mapping[str, Any] | None = None, issues: list[Any] | None = None, candidate: Mapping[str, Any] | None = None) -> DecisionValidationResult:
    return DecisionValidationResult(valid, code=code, message=message, details=dict(details or {}), issues=list(issues or []), candidate=dict(candidate) if isinstance(candidate, Mapping) else None)


def _fail(code: str, path: str, message: str, **details: Any) -> DecisionValidationResult:
    return _result(False, code, message, details=details, issues=[issue(code, path, message)])


def _trusted_second_event_ref(event: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """Normalize an upstream event artifact into the compact trusted ref.

    ``second_event`` is deliberately separate from the model candidate.  The
    event producer may call its hash ``event_hash`` or ``content_hash`` and its
    price ``reference_price`` or ``price``; all other identity fields are
    required and validated by :func:`make_second_event_ref`.
    """

    if not isinstance(event, Mapping):
        return None
    if event.get("schema_version", "0.1.0") != "0.1.0":
        return None
    event_id = event.get("event_id", event.get("second_event_id"))
    event_hash = event.get("event_hash", event.get("content_hash"))
    reference_price = event.get("reference_price", event.get("price"))
    try:
        return make_second_event_ref(event_id, event_hash, reference_price)
    except Exception:
        return None


def _claim_map(packet: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    claims = packet.get("claims")
    result: dict[str, Mapping[str, Any]] = {}
    if isinstance(claims, Mapping):
        for values in claims.values():
            if isinstance(values, list):
                for claim in values:
                    if isinstance(claim, Mapping) and isinstance(claim.get("claim_id"), str):
                        result[str(claim["claim_id"])] = claim
    return result


def _evidence_map(evidence: Mapping[str, Any]) -> tuple[dict[str, Mapping[str, Any]], dict[str, Mapping[str, Any]]]:
    items: dict[str, Mapping[str, Any]] = {}
    citations: dict[str, Mapping[str, Any]] = {}
    for item in evidence.get("evidence_items", []) if isinstance(evidence.get("evidence_items"), list) else []:
        if isinstance(item, Mapping) and isinstance(item.get("evidence_id"), str):
            items[item["evidence_id"]] = item
    for item in evidence.get("citations", []) if isinstance(evidence.get("citations"), list) else []:
        if isinstance(item, Mapping) and isinstance(item.get("citation_id"), str):
            citations[item["citation_id"]] = item
    return items, citations


def _market_refs(market_snapshot: Mapping[str, Any], observation_frame: Mapping[str, Any]) -> set[str]:
    refs: set[str] = set()
    for bar in market_snapshot.get("bars", []) if isinstance(market_snapshot.get("bars"), list) else []:
        if isinstance(bar, Mapping) and isinstance(bar.get("bar_id"), str):
            refs.add(bar["bar_id"])
    for observation in observation_frame.get("observations", []) if isinstance(observation_frame.get("observations"), list) else []:
        if isinstance(observation, Mapping) and isinstance(observation.get("observation_id"), str):
            refs.add(observation["observation_id"])
    return refs


_FIELD_TO_PACKET_FIELDS = {
    # Narrative reason claims may come from any strategy-context section but
    # never from a numeric entry/stop/target field.
    "reason": {"market_context", "prerequisites", "recognition_features", "trigger", "risk_notes", "prohibited", "exceptions", "examples", "interpretation_notes"},
    "entry_price": {"entry"},
    "stop_price": {"invalidation_stop"},
    "target_price": {"targets_exit"},
}


def _validate_field_grounding(
    field_name: str,
    ref: Mapping[str, Any],
    *,
    packet: Mapping[str, Any],
    evidence: Mapping[str, Any],
    market_refs: set[str],
    record_id: str,
    actionable: bool,
) -> DecisionValidationResult:
    declaration = ref.get("declaration_type")
    if declaration not in DECLARATIONS:
        return _fail("invalid_grounding", f"$.grounding.{field_name}.declaration_type", "grounding declaration is unsupported")
    # A model-proposed numeric risk level is always an interpretation.  It is
    # never allowed to masquerade as a source assertion or source omission.
    if actionable and declaration == "source_not_stated":
        return _fail("source_not_stated", f"$.grounding.{field_name}", "source_not_stated cannot produce an actionable instruction")
    if field_name in {"entry_price", "stop_price", "target_price"} and declaration != "derived_interpretation":
        return _fail("claim_declaration_mismatch", f"$.grounding.{field_name}.declaration_type", "model-proposed numeric levels must be derived_interpretation")

    claims = _claim_map(packet)
    evidence_items, citations = _evidence_map(evidence)
    claim_ids = ref.get("claim_ids") if isinstance(ref.get("claim_ids"), list) else []
    evidence_ids = ref.get("evidence_ids") if isinstance(ref.get("evidence_ids"), list) else []
    citation_ids = ref.get("citation_ids") if isinstance(ref.get("citation_ids"), list) else []
    refs = ref.get("market_refs") if isinstance(ref.get("market_refs"), list) else []
    if not claim_ids or not evidence_ids or not citation_ids or not refs:
        return _fail("missing_grounding", f"$.grounding.{field_name}", "grounding must include claims, evidence, citations and market refs")
    if any(market_ref not in market_refs for market_ref in refs):
        return _fail("market_reference_mismatch", f"$.grounding.{field_name}.market_refs", "market reference is absent from supplied snapshot/frame")

    selected_items: list[Mapping[str, Any]] = []
    for evidence_id in evidence_ids:
        item = evidence_items.get(evidence_id)
        if item is None:
            return _fail("forged_evidence", f"$.grounding.{field_name}.evidence_ids", "evidence ID is absent from the supplied package")
        if item.get("record_id") != record_id:
            return _fail("cross_strategy_evidence", f"$.grounding.{field_name}.evidence_ids", "evidence belongs to another strategy")
        if item.get("declaration_type") != declaration:
            return _fail("claim_declaration_mismatch", f"$.grounding.{field_name}", "candidate declaration differs from evidence declaration")
        selected_items.append(item)
    selected_claims: list[Mapping[str, Any]] = []
    for claim_id in claim_ids:
        claim = claims.get(claim_id)
        if claim is None:
            return _fail("forged_claim", f"$.grounding.{field_name}.claim_ids", "claim ID is absent from the authoritative packet")
        if str(claim_id).split(":", 1)[0] != record_id:
            return _fail("cross_strategy_claim", f"$.grounding.{field_name}.claim_ids", "claim belongs to another strategy")
        if actionable and field_name in {"entry_price", "stop_price", "target_price"} and claim.get("declaration_type") == "source_not_stated":
            return _fail("source_not_stated", f"$.grounding.{field_name}", "source_not_stated level cannot be made actionable")
        if claim.get("declaration_type") != declaration:
            return _fail("claim_declaration_mismatch", f"$.grounding.{field_name}", "candidate declaration differs from packet claim")
        expected_fields = _FIELD_TO_PACKET_FIELDS.get(field_name)
        if expected_fields is not None and claim.get("field") not in expected_fields:
            return _fail("wrong_field_claim", f"$.grounding.{field_name}.claim_ids", "claim field does not support this decision value")
        selected_claims.append(claim)

    selected_claim_ids = set(str(item.get("claim_id")) for item in selected_items)
    if any(item.get("claim_id") not in set(claim_ids) for item in selected_items):
        return _fail("grounding_mismatch", f"$.grounding.{field_name}.evidence_ids", "evidence claim is not listed by the candidate")
    if any(str(claim_id) not in selected_claim_ids for claim_id in claim_ids):
        # An evidence item is the exact claim-level binding in G016.  Requiring
        # each listed claim to be represented prevents a foreign/unused claim
        # from being smuggled into a level.
        return _fail("grounding_mismatch", f"$.grounding.{field_name}.claim_ids", "claim is not bound by listed evidence")

    for citation_id in citation_ids:
        citation = citations.get(citation_id)
        if citation is None:
            return _fail("forged_citation", f"$.grounding.{field_name}.citation_ids", "citation ID is absent from the supplied package")
        if citation.get("claim_id") not in set(claim_ids) or citation.get("declaration_type") != declaration:
            return _fail("wrong_citation", f"$.grounding.{field_name}.citation_ids", "citation does not bind the selected claim/declaration")
        if not any(citation_id in (item.get("citation_ids") or []) for item in selected_items):
            return _fail("grounding_mismatch", f"$.grounding.{field_name}.citation_ids", "citation is not bound by listed evidence")

    return _result(True, candidate=ref)


def validate_grounding(
    candidate: Mapping[str, Any] | str,
    *,
    registry: Any,
    packet: Mapping[str, Any],
    profile_ref: Mapping[str, Any],
    evidence_package: Mapping[str, Any],
    market_snapshot: Mapping[str, Any],
    observation_frame: Mapping[str, Any],
    analysis_request: Mapping[str, Any] | None = None,
    normalized_model_request: Mapping[str, Any] | None = None,
    normalized_model_response: Mapping[str, Any] | None = None,
    second_event: Mapping[str, Any] | None = None,
    artifacts: Any = None,
) -> DecisionValidationResult:
    """Validate candidate grounding against the authoritative G016 packet.

    The registry calls are mandatory.  A self-consistent forged packet or
    profile is not sufficient for an executable result.
    """

    parsed = validate_candidate(candidate).candidate if not isinstance(candidate, Mapping) else validate_candidate(candidate, parse_json=False).candidate
    candidate_result = validate_candidate(candidate, parse_json=not isinstance(candidate, Mapping))
    if not candidate_result.valid or parsed is None:
        return _result(False, candidate_result.code, candidate_result.message, issues=candidate_result.issues, details=candidate_result.details)
    if registry is None or not hasattr(registry, "validate_packet") or not hasattr(registry, "validate_profile_lineage"):
        return _fail("registry_required", "$", "authoritative strategy registry is required")
    try:
        packet_result = registry.validate_packet(packet)
    except Exception:
        return _fail("registry_validation_failure", "$.packet", "authoritative packet validation failed")
    if not getattr(packet_result, "valid", bool(packet_result)):
        code = getattr(packet_result, "code", "packet_binding_mismatch")
        return _fail(str(code), "$.packet", "authoritative packet validation rejected the packet")

    artifact_map = _artifacts_by_kind(artifacts)
    for value in (analysis_request, evidence_package, normalized_model_request, normalized_model_response):
        if isinstance(value, Mapping) and isinstance(value.get("artifact_kind"), str):
            artifact_map[value["artifact_kind"]] = value
    lineage_artifacts = [artifact_map.get(kind) for kind in ("analysis_request", "knowledge_evidence_package", "normalized_model_request", "normalized_model_response")]
    lineage_artifacts = [value for value in lineage_artifacts if isinstance(value, Mapping)]
    required_lineage_kinds = {"analysis_request", "knowledge_evidence_package", "normalized_model_request", "normalized_model_response"}
    if {str(value.get("artifact_kind")) for value in lineage_artifacts} != required_lineage_kinds:
        return _fail("missing_artifact", "$.artifacts", "analysis/evidence/model lineage artifacts are required")
    for artifact in lineage_artifacts:
        bound_profile = _profile_ref_from_artifact(artifact)
        if bound_profile is None or dict(bound_profile) != dict(profile_ref):
            return _fail("profile_binding_mismatch", f"$.artifacts.{artifact.get('artifact_kind')}.extensions.strategy_profile_ref", "artifact profile reference differs from supplied profile")
    # Upstream G014 objects are inputs to the finalizer, but they remain an
    # untrusted integration boundary here.  Validate their frozen identities
    # and pairwise lineage before asking G016 to bind the profile.
    related_for_validation = [
        artifact_map.get("analysis_request"),
        artifact_map.get("knowledge_evidence_package"),
        artifact_map.get("market_snapshot"),
        artifact_map.get("market_observation_frame"),
        artifact_map.get("normalized_model_request"),
        artifact_map.get("normalized_model_response"),
    ]
    related_for_validation = [value for value in related_for_validation if isinstance(value, Mapping)]
    for kind in ("analysis_request", "knowledge_evidence_package", "normalized_model_request", "normalized_model_response"):
        artifact = artifact_map.get(kind)
        if not isinstance(artifact, Mapping):
            continue
        try:
            artifact_validation = validate_artifact(artifact, expected_kind=kind, related_artifacts=related_for_validation)
        except Exception:
            return _fail("artifact_validation_failure", f"$.artifacts.{kind}", "G014 artifact validation failed")
        if not artifact_validation.valid:
            return _fail("artifact_validation_failure", f"$.artifacts.{kind}", "G014 artifact validation failed")
    try:
        lineage_result = registry.validate_profile_lineage(profile_ref, packet=packet, artifacts=lineage_artifacts)
    except Exception:
        return _fail("registry_validation_failure", "$.profile_ref", "authoritative profile lineage validation failed")
    if not getattr(lineage_result, "valid", bool(lineage_result)):
        code = getattr(lineage_result, "code", "profile_binding_mismatch")
        return _fail(str(code), "$.profile_ref", "authoritative profile lineage rejected supplied artifacts")
    profile_record = profile_ref.get("record_id") if isinstance(profile_ref, Mapping) else None
    if not isinstance(profile_record, str) or packet.get("record_id") != profile_record:
        return _fail("profile_binding_mismatch", "$.profile_ref.record_id", "profile and packet owner differ")

    market_result = validate_market_view_ref(parsed["shared_market_view_ref"], market_snapshot=market_snapshot, observation_frame=observation_frame)
    if not market_result.valid:
        return market_result
    if parsed["branch"] == "enter_immediately":
        # Immediate execution is only actionable when a separate upstream
        # event is supplied.  Never derive this identity from the candidate's
        # price: same-price events must remain distinct and model output cannot
        # self-sign an event.
        if second_event is None:
            return _fail("missing_second_event", "$.second_event_ref", "enter_immediately requires a trusted second event")
        expected_event = _trusted_second_event_ref(second_event)
        if expected_event is None:
            return _fail("invalid_second_event", "$.second_event_ref", "trusted second event is malformed")
        if dict(parsed.get("second_event_ref") or {}) != expected_event:
            return _fail("second_event_mismatch", "$.second_event_ref", "candidate second-event reference differs from supplied event")

    # The normalized model response is a frozen G014 artifact, but its
    # candidate payload must still be cryptographically/content bound to this
    # parsed G018 candidate.  Import lazily to avoid the mapper -> grounding
    # module cycle.
    if not isinstance(normalized_model_response, Mapping):
        return _fail("missing_artifact", "$.normalized_model_response", "normalized model response is required")
    try:
        from .mapper import map_candidate_to_normalized_model_candidate
        from .schema import make_model_output_ref

        expected_model_candidate = map_candidate_to_normalized_model_candidate(parsed, profile_ref=profile_ref)
        response_candidate = normalized_model_response.get("candidate")
        response_extensions = normalized_model_response.get("extensions")
        if not isinstance(response_candidate, Mapping) or dict(response_candidate) != expected_model_candidate:
            return _fail("model_response_binding_mismatch", "$.normalized_model_response.candidate", "normalized response candidate is not the exact G018 mapping")
        expected_output_ref = make_model_output_ref(parsed)
        if not isinstance(response_extensions, Mapping) or response_extensions.get("g018_model_output_ref") != expected_output_ref:
            return _fail("model_response_binding_mismatch", "$.normalized_model_response.extensions.g018_model_output_ref", "normalized response model-output reference is not bound to the candidate")
    except Exception:
        return _fail("model_response_binding_mismatch", "$.normalized_model_response", "normalized response candidate binding failed")
    market_refs = _market_refs(market_snapshot, observation_frame)
    branch = parsed["branch"]
    actionable = branch in {"enter_immediately", "wait_for_price"}
    for field_name in ("reason", "entry_price", "stop_price", "target_price"):
        ref = parsed["grounding"].get(field_name)
        if ref is None:
            continue
        field_result = _validate_field_grounding(field_name, ref, packet=packet, evidence=evidence_package, market_refs=market_refs, record_id=profile_record, actionable=actionable)
        if not field_result.valid:
            return field_result
    if actionable:
        for field_name in ("reason", "stop_price", "target_price"):
            if parsed["grounding"].get(field_name) is None:
                return _fail("missing_grounding", f"$.grounding.{field_name}", "actionable field grounding is required")
        if branch == "wait_for_price" and parsed["grounding"].get("entry_price") is None:
            return _fail("missing_grounding", "$.grounding.entry_price", "wait entry grounding is required")
    return _result(True, candidate=parsed, details={"market_refs": sorted(market_refs), "record_id": profile_record})


# Friendly alias used by callers that treat grounding as a validator.
grounding_validate = validate_grounding


__all__ = ["grounding_validate", "validate_grounding"]
