"""Focused G018 schema, grounding and mapper checks."""

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from pa_engine.contracts import finalize_artifact
from pa_engine.strategies.knowledge import bind_strategy_profile_ref, load_default_registry
from pa_engine.strategy.decision import (
    GROUNDING_FIELDS,
    finalize_decision,
    instruction_hash,
    make_model_output_ref,
    make_second_event_ref,
    map_candidate_to_normalized_model_candidate,
    market_view_ref_for,
    parse_candidate,
    SCHEMA_PATH,
    validate_instruction,
    validate_candidate,
    verify_instruction,
)
from pa_engine.contracts import canonical_json_bytes


ROOT = Path(__file__).resolve().parents[3]
FIXTURES = ROOT / "tests" / "fixtures" / "phase2" / "g018"


def load_fixture(name: str) -> dict:
    value = json.loads((FIXTURES / name).read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


@pytest.mark.parametrize("name", ["valid/no_entry.json", "valid/immediate_long.json", "valid/immediate_short.json", "valid/wait_long.json", "valid/wait_short.json"])
def test_positive_candidates_are_strictly_parseable(name: str):
    value = load_fixture(name)
    parsed = parse_candidate(value)
    assert parsed == value
    assert set(parsed) == {
        "schema_version", "branch", "reason", "direction", "entry_price", "stop_price", "target_price",
        "N", "X", "grounding", "confidence", "uncertainty", "shared_market_view_ref", "second_event_ref",
    }


@pytest.mark.parametrize(
    "name",
    [
        "invalid/n_zero.json", "invalid/x_zero.json", "invalid/bool_timer.json", "invalid/float_price.json",
        "invalid/reason_multiline.json", "invalid/reversed_levels.json", "invalid/branch_leakage.json",
        "invalid/second_event_mismatch.json",
    ],
)
def test_negative_candidates_are_typed_rejections(name: str):
    result = validate_candidate(load_fixture(name))
    assert not result.valid
    assert result.code
    assert result.instruction if hasattr(result, "instruction") else True


def test_json_parser_rejects_nan_infinity_duplicate_keys_and_unknown_fields():
    base = json.dumps(load_fixture("valid/no_entry.json"), ensure_ascii=False)
    assert not validate_candidate(base.replace("60", "NaN", 1)).valid
    duplicate = base[:-1] + ',"N":60}'
    assert validate_candidate(duplicate).code == "duplicate_field"
    unknown = load_fixture("valid/no_entry.json")
    unknown["extra"] = 1
    assert validate_candidate(unknown).code == "unknown_field"


def test_profile_schema_is_self_validating_for_all_fixtures():
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema)
    for path in (FIXTURES / "valid").glob("*.json"):
        assert not list(validator.iter_errors(load_fixture("valid/" + path.name))), path
    for path in (FIXTURES / "invalid").glob("*.json"):
        assert list(validator.iter_errors(load_fixture("invalid/" + path.name))), path


@pytest.mark.parametrize("value", [1, 86_400])
def test_timer_boundary_values_are_accepted(value: int):
    candidate = load_fixture("valid/no_entry.json")
    candidate["N"] = value
    assert validate_candidate(candidate).valid
    wait = load_fixture("valid/wait_long.json")
    wait["X"] = value
    assert validate_candidate(wait).valid


@pytest.mark.parametrize("value", [0, 86_401, True, 1.0])
def test_timer_invalid_types_and_ranges_are_rejected(value: object):
    candidate = load_fixture("valid/no_entry.json")
    candidate["N"] = value
    assert not validate_candidate(candidate).valid
    wait = load_fixture("valid/wait_long.json")
    wait["X"] = value
    assert not validate_candidate(wait).valid


@pytest.mark.parametrize("reason", ["", "   ", "two\nlines", "x" * 241])
def test_reason_invalid_forms_are_rejected(reason: str):
    candidate = load_fixture("valid/wait_long.json")
    candidate["reason"] = reason
    assert not validate_candidate(candidate).valid


def test_reason_exact_codepoint_limit_is_accepted():
    candidate = load_fixture("valid/wait_long.json")
    candidate["reason"] = "x" * 240
    assert validate_candidate(candidate).valid


@pytest.mark.parametrize("value", [100.0, "0", "-1", "1e2", "100.0", "100.00"])
def test_decimal_lexical_restrictions_are_rejected(value: object):
    candidate = load_fixture("valid/wait_long.json")
    candidate["entry_price"] = value
    assert not validate_candidate(candidate).valid


@pytest.mark.parametrize(
    "direction,stop,target",
    [("long", "100", "102"), ("long", "101", "99"), ("short", "100", "100"), ("short", "99", "101")],
)
def test_equal_or_reversed_directional_levels_are_rejected(direction: str, stop: str, target: str):
    candidate = load_fixture("valid/wait_long.json")
    candidate["direction"], candidate["stop_price"], candidate["target_price"] = direction, stop, target
    assert not validate_candidate(candidate).valid


def test_second_event_identity_is_content_derived():
    ref = make_second_event_ref("trusted_event", "1" * 64, "100")
    assert ref["event_id"] == "trusted_event"
    changed = copy.deepcopy(ref)
    changed["reference_price"] = "100.1"
    assert validate_candidate({**load_fixture("valid/immediate_long.json"), "second_event_ref": changed}).valid


def _inputs():
    registry = load_default_registry(ROOT / "knowledge")
    outcome = registry.load_packet("pa:nanqiao-range-extreme-limit")
    assert outcome.ok
    packet, profile, evidence = outcome.packet, outcome.profile_ref, outcome.evidence_package
    market = load_fixture_from(ROOT / "tests/fixtures/phase2/g015/valid/market_snapshot.json")
    observation = load_fixture_from(ROOT / "tests/fixtures/phase2/g015/valid/market_observation_frame.json")
    policy = load_fixture_from(ROOT / "pa_engine/contracts/fixtures/valid/analysis_policy.json")
    request = finalize_artifact(
        {
            "schema_version": "0.1.0", "artifact_kind": "analysis_request", "correlation_id": "g018-test",
            "input_mode": "ohlcv", "analysis_as_of": market["analysis_as_of"],
            "market_snapshot_id": market["market_snapshot_id"], "knowledge_snapshot_id": profile["snapshot_id"],
            "knowledge_snapshot_hash": profile["snapshot_hash"], "analysis_policy_id": policy["analysis_policy_id"],
            "purpose": "decision_support", "instrument": market["instrument"], "timeframe": market["timeframe"],
            "session": market["session"], "no_live_orders": True, "extensions": {},
        },
        "analysis_request",
    )
    request = bind_strategy_profile_ref(request, profile)
    context_refs = {
        "analysis_request_id": request["analysis_request_id"], "analysis_request_hash": request["content_hash"],
        "market_snapshot_id": market["market_snapshot_id"], "market_snapshot_hash": market["content_hash"],
        "observation_frame_id": observation["observation_frame_id"], "observation_frame_hash": observation["content_hash"],
        "evidence_package_id": evidence["evidence_package_id"], "evidence_package_hash": evidence["content_hash"],
    }
    model_request_payload = {
        "schema_version": "0.1.0", "artifact_kind": "normalized_model_request",
        "correlation_id": "g018-correlation", "provider": "offline", "model": "fixture-model",
        "adapter": "g018-normalized-response-0.1.0", "requested_capabilities": ["structured_json"],
        "prompt_template_version": "g018-prompt-0.1.0",
        "prompt_hash": hashlib.sha256(canonical_json_bytes({"prompt_template_version": "g018-prompt-0.1.0", "instructions": "Return compact decision", "context_text": "fixture"})).hexdigest(),
        "context_refs": context_refs, "response_schema_version": "0.1.0", "instructions": "Return compact decision",
        "context_text": "fixture", "parameters": {"temperature": "0", "max_output_tokens": 256},
        "extensions": {"reasoning_mode": "disabled"},
    }
    model_request = finalize_artifact(model_request_payload, "normalized_model_request")
    model_request = bind_strategy_profile_ref(model_request, profile)
    return registry, packet, profile, evidence, market, observation, request, model_request


def _model_response(model_request: dict, profile: dict, candidate: dict) -> dict:
    response = finalize_artifact(
        {
            "schema_version": "0.1.0", "artifact_kind": "normalized_model_response",
            "model_request_id": model_request["model_request_id"], "provider": model_request["provider"],
            "model": model_request["model"], "adapter": model_request["adapter"], "status": "completed",
            "candidate": map_candidate_to_normalized_model_candidate(candidate, profile_ref=profile),
            "finish_reason": "stop", "error": None,
            "extensions": {"g018_model_output_ref": make_model_output_ref(candidate)},
        },
        "normalized_model_response",
    )
    return bind_strategy_profile_ref(response, profile)


def load_fixture_from(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _candidate(branch: str, view: dict, evidence: dict, market: dict, *, direction: str = "long"):
    items = {item["claim_id"]: item for item in evidence["evidence_items"]}

    def ref(claim_id: str):
        item = items[claim_id]
        return {"declaration_type": item["declaration_type"], "claim_ids": [claim_id], "evidence_ids": [item["evidence_id"]], "citation_ids": item["citation_ids"], "market_refs": [market["bars"][0]["bar_id"]]}

    base = {
        "schema_version": "0.1.0", "branch": branch, "reason": "grounded range setup", "direction": direction,
        "entry_price": "100" if branch == "wait_for_price" else None,
        "stop_price": "99" if direction == "long" else "102",
        "target_price": "102" if direction == "long" else "98",
        "N": 60 if branch == "no_entry" else None, "X": 60 if branch == "wait_for_price" else None,
        "grounding": {"reason": ref("nanqiao-range-extreme-limit:market_context:1") if branch != "no_entry" else None,
                      "entry_price": ref("nanqiao-range-extreme-limit:entry:1") if branch == "wait_for_price" else None,
                      "stop_price": ref("nanqiao-range-extreme-limit:invalidation_stop:1") if branch != "no_entry" else None,
                      "target_price": ref("nanqiao-range-extreme-limit:targets_exit:1") if branch != "no_entry" else None},
        "confidence": "0.8" if branch != "no_entry" else None,
        "uncertainty": {"level": "low", "codes": [], "details": "grounded fixture"}, "shared_market_view_ref": view,
        "second_event_ref": make_second_event_ref("trusted_event", "1" * 64, "100") if branch == "enter_immediately" else None,
    }
    if branch == "no_entry":
        base.update(reason=None, direction=None, entry_price=None, stop_price=None, target_price=None)
    return base


def test_all_three_branches_map_to_frozen_g014_and_stable_instruction():
    registry, packet, profile, evidence, market, observation, request, model_request = _inputs()
    view = market_view_ref_for(market, observation)
    for branch in ("no_entry", "enter_immediately", "wait_for_price"):
        candidate = _candidate(branch, view, evidence, market)
        model_response = _model_response(model_request, profile, candidate)
        first = finalize_decision(candidate, registry=registry, packet=packet, profile_ref=profile, evidence_package=evidence, market_snapshot=market, observation_frame=observation, analysis_request=request, normalized_model_request=model_request, normalized_model_response=model_response, second_event=(candidate.get("second_event_ref") if branch == "enter_immediately" else None))
        second = finalize_decision(candidate, registry=registry, packet=packet, profile_ref=profile, evidence_package=evidence, market_snapshot=market, observation_frame=observation, analysis_request=request, normalized_model_request=model_request, normalized_model_response=model_response, second_event=(candidate.get("second_event_ref") if branch == "enter_immediately" else None))
        assert first.valid, first.to_dict()
        assert first.result and first.instruction
        assert first.result["status"] == {"no_entry": "no_trade", "enter_immediately": "signal", "wait_for_price": "conditional"}[branch]
        assert first.instruction == second.instruction
        assert verify_instruction(first.instruction)
        assert validate_instruction(first.instruction).valid
        assert instruction_hash(first.instruction) == first.instruction["instruction_hash"]
        forbidden = {"account", "quantity", "pnl", "r", "balance"}
        assert not forbidden.intersection(first.instruction)


@pytest.mark.parametrize("direction", ["long", "short"])
@pytest.mark.parametrize("branch", ["enter_immediately", "wait_for_price"])
def test_positive_short_and_long_actionable_finalizations(branch: str, direction: str):
    registry, packet, profile, evidence, market, observation, request, model_request = _inputs()
    candidate = _candidate(branch, market_view_ref_for(market, observation), evidence, market, direction=direction)
    response = _model_response(model_request, profile, candidate)
    outcome = finalize_decision(
        candidate,
        registry=registry,
        packet=packet,
        profile_ref=profile,
        evidence_package=evidence,
        market_snapshot=market,
        observation_frame=observation,
        analysis_request=request,
        normalized_model_request=model_request,
        normalized_model_response=response,
        second_event=candidate.get("second_event_ref"),
    )
    assert outcome.valid, outcome.to_dict()
    assert validate_instruction(outcome.instruction).valid


def test_immediate_requires_exact_trusted_event_and_bound_model_response():
    registry, packet, profile, evidence, market, observation, request, model_request = _inputs()
    candidate = _candidate("enter_immediately", market_view_ref_for(market, observation), evidence, market)
    response = _model_response(model_request, profile, candidate)
    kwargs = dict(registry=registry, packet=packet, profile_ref=profile, evidence_package=evidence, market_snapshot=market, observation_frame=observation, analysis_request=request, normalized_model_request=model_request, normalized_model_response=response)
    missing = finalize_decision(candidate, **kwargs)
    assert not missing.valid and missing.code == "missing_second_event"
    same_price_other_event = make_second_event_ref("other_event", "2" * 64, "100")
    mismatch = finalize_decision(candidate, second_event=same_price_other_event, **kwargs)
    assert not mismatch.valid and mismatch.code == "second_event_mismatch"
    tampered = copy.deepcopy(response)
    tampered["candidate"]["action"] = "sell"
    tampered = finalize_artifact(tampered, "normalized_model_response")
    tampered = bind_strategy_profile_ref(tampered, profile)
    rejected = finalize_decision(candidate, second_event=candidate["second_event_ref"], normalized_model_response=tampered, **{key: value for key, value in kwargs.items() if key != "normalized_model_response"})
    assert not rejected.valid and rejected.code == "model_response_binding_mismatch"


def test_instruction_semantics_cannot_be_washed_by_rehash():
    registry, packet, profile, evidence, market, observation, request, model_request = _inputs()
    candidate = _candidate("wait_for_price", market_view_ref_for(market, observation), evidence, market)
    response = _model_response(model_request, profile, candidate)
    outcome = finalize_decision(candidate, registry=registry, packet=packet, profile_ref=profile, evidence_package=evidence, market_snapshot=market, observation_frame=observation, analysis_request=request, normalized_model_request=model_request, normalized_model_response=response)
    assert outcome.valid
    mutated = copy.deepcopy(outcome.instruction)
    mutated["stop_price"] = "101"
    mutated["instruction_hash"] = instruction_hash(mutated)
    mutated["instruction_id"] = "sdi_" + mutated["instruction_hash"]
    assert not verify_instruction(mutated)
    assert not validate_instruction(mutated).valid


def test_instruction_cross_strategy_swap_requires_nested_authority():
    registry, packet, profile, evidence, market, observation, request, model_request = _inputs()
    candidate = _candidate("wait_for_price", market_view_ref_for(market, observation), evidence, market)
    response = _model_response(model_request, profile, candidate)
    outcome = finalize_decision(candidate, registry=registry, packet=packet, profile_ref=profile, evidence_package=evidence, market_snapshot=market, observation_frame=observation, analysis_request=request, normalized_model_request=model_request, normalized_model_response=response)
    assert outcome.valid
    swapped = copy.deepcopy(outcome.instruction)
    swapped["strategy_id"] = "pa:rose-gap-bar"
    swapped["instruction_hash"] = instruction_hash(swapped)
    swapped["instruction_id"] = "sdi_" + swapped["instruction_hash"]
    assert not validate_instruction(swapped).valid
    rose = registry.load_packet("pa:rose-gap-bar")
    assert rose.ok
    full_swap = copy.deepcopy(outcome.instruction)
    full_swap["strategy_id"] = rose.profile_ref["strategy_id"]
    full_swap["strategy_profile_ref"] = copy.deepcopy(rose.profile_ref)
    full_swap["profile_ref_id"] = rose.profile_ref["profile_ref_id"]
    full_swap["profile_ref_hash"] = rose.profile_ref["profile_ref_hash"]
    full_swap["instruction_hash"] = instruction_hash(full_swap)
    full_swap["instruction_id"] = "sdi_" + full_swap["instruction_hash"]
    assert validate_instruction(full_swap).valid
    assert not validate_instruction(full_swap, expected_profile_ref=profile).valid
    assert not validate_instruction(full_swap, registry=registry, packet=packet).valid


@pytest.mark.parametrize("mutation,expected_code", [
    ("wrong_field", "wrong_field_claim"),
    ("forged_claim", "forged_claim"),
    ("forged_evidence", "forged_evidence"),
    ("forged_citation", "forged_citation"),
    ("forged_bar", "market_reference_mismatch"),
    ("forged_obv", "market_reference_mismatch"),
])
def test_finalizer_rejects_grounding_forgery_matrix(mutation: str, expected_code: str):
    registry, packet, profile, evidence, market, observation, request, model_request = _inputs()
    candidate = _candidate("wait_for_price", market_view_ref_for(market, observation), evidence, market)
    stop_ref = candidate["grounding"]["stop_price"]
    if mutation == "wrong_field":
        reason_ref = candidate["grounding"]["reason"]
        stop_ref.update({key: copy.deepcopy(reason_ref[key]) for key in ("claim_ids", "evidence_ids", "citation_ids")})
    elif mutation == "forged_claim":
        stop_ref["claim_ids"] = ["nanqiao-range-extreme-limit:invalidation_stop:999"]
    elif mutation == "forged_evidence":
        stop_ref["evidence_ids"] = ["evi_" + "f" * 64]
    elif mutation == "forged_citation":
        stop_ref["citation_ids"] = ["cit_" + "f" * 64]
    elif mutation == "forged_bar":
        stop_ref["market_refs"] = ["bar_" + "f" * 64]
    else:
        stop_ref["market_refs"] = ["obv_" + "f" * 64]
    response = _model_response(model_request, profile, candidate)
    outcome = finalize_decision(candidate, registry=registry, packet=packet, profile_ref=profile, evidence_package=evidence, market_snapshot=market, observation_frame=observation, analysis_request=request, normalized_model_request=model_request, normalized_model_response=response)
    assert not outcome.valid
    assert outcome.instruction is None
    assert outcome.code == expected_code


@pytest.mark.parametrize("artifact_name", ["analysis_request", "knowledge_evidence_package", "normalized_model_request", "normalized_model_response"])
@pytest.mark.parametrize("tamper_mode", ["missing", "tampered"])
def test_each_lineage_artifact_requires_exact_profile_ref(artifact_name: str, tamper_mode: str):
    registry, packet, profile, evidence, market, observation, request, model_request = _inputs()
    candidate = _candidate("wait_for_price", market_view_ref_for(market, observation), evidence, market)
    response = _model_response(model_request, profile, candidate)
    values = {"analysis_request": request, "knowledge_evidence_package": evidence, "normalized_model_request": model_request, "normalized_model_response": response}
    forged = copy.deepcopy(values[artifact_name])
    extensions = forged.get("extensions")
    if not isinstance(extensions, dict):
        extensions = {}
    if tamper_mode == "missing":
        extensions.pop("strategy_profile_ref", None)
    else:
        profile_copy = copy.deepcopy(profile)
        profile_copy["strategy_id"] = "pa:rose-gap-bar"
        extensions["strategy_profile_ref"] = profile_copy
    forged["extensions"] = extensions
    values[artifact_name] = forged
    outcome = finalize_decision(
        candidate,
        registry=registry,
        packet=packet,
        profile_ref=profile,
        evidence_package=values["knowledge_evidence_package"],
        market_snapshot=market,
        observation_frame=observation,
        analysis_request=values["analysis_request"],
        normalized_model_request=values["normalized_model_request"],
        normalized_model_response=values["normalized_model_response"],
    )
    assert not outcome.valid
    assert outcome.instruction is None


def test_finalizer_profile_lineage_authority_is_called_for_result():
    registry, packet, profile, evidence, market, observation, request, model_request = _inputs()
    candidate = _candidate("wait_for_price", market_view_ref_for(market, observation), evidence, market)
    response = _model_response(model_request, profile, candidate)

    class SpyRegistry:
        def __init__(self, wrapped):
            self.wrapped = wrapped
            self.profile_calls = 0

        def validate_packet(self, *args, **kwargs):
            return self.wrapped.validate_packet(*args, **kwargs)

        def validate_profile_lineage(self, *args, **kwargs):
            self.profile_calls += 1
            return self.wrapped.validate_profile_lineage(*args, **kwargs)

    spy = SpyRegistry(registry)
    outcome = finalize_decision(candidate, registry=spy, packet=packet, profile_ref=profile, evidence_package=evidence, market_snapshot=market, observation_frame=observation, analysis_request=request, normalized_model_request=model_request, normalized_model_response=response)
    assert outcome.valid, outcome.to_dict()
    assert spy.profile_calls >= 2


def test_rose_gap_source_not_stated_levels_never_produce_instruction():
    registry, packet, profile, evidence, market, observation, request, model_request = _inputs()
    rose = registry.load_packet("pa:rose-gap-bar")
    assert rose.ok
    rose_items = {item["claim_id"]: item for item in rose.evidence_package["evidence_items"]}

    def ref(claim_id: str):
        item = rose_items[claim_id]
        return {"declaration_type": item["declaration_type"], "claim_ids": [claim_id], "evidence_ids": [item["evidence_id"]], "citation_ids": item["citation_ids"], "market_refs": [market["bars"][0]["bar_id"]]}

    candidate = {
        "schema_version": "0.1.0", "branch": "wait_for_price", "reason": "rose gap",
        "direction": "long", "entry_price": "100", "stop_price": "99", "target_price": "102",
        "N": None, "X": 60,
        "grounding": {
            "reason": ref("rose-gap-bar:market_context:1"),
            "entry_price": ref("rose-gap-bar:entry:1"),
            "stop_price": ref("rose-gap-bar:invalidation_stop:1"),
            "target_price": ref("rose-gap-bar:targets_exit:1"),
        },
        "confidence": "0.8", "uncertainty": {"level": "high", "codes": ["no_actionable_evidence"], "details": "source does not state levels"},
        "shared_market_view_ref": market_view_ref_for(market, observation), "second_event_ref": None,
    }
    assert not validate_candidate(candidate).valid
    outcome = finalize_decision(candidate)
    assert not outcome.valid and outcome.instruction is None


def test_cross_strategy_packet_and_evidence_are_rejected():
    registry, packet, profile, evidence, market, observation, request, model_request = _inputs()
    candidate = _candidate("no_entry", market_view_ref_for(market, observation), evidence, market)
    response = _model_response(model_request, profile, candidate)
    rose = registry.load_packet("pa:rose-gap-bar")
    assert rose.ok
    kwargs = dict(registry=registry, profile_ref=profile, market_snapshot=market, observation_frame=observation, analysis_request=request, normalized_model_request=model_request, normalized_model_response=response)
    packet_outcome = finalize_decision(candidate, packet=rose.packet, evidence_package=evidence, **kwargs)
    assert not packet_outcome.valid and packet_outcome.instruction is None
    evidence_outcome = finalize_decision(candidate, packet=packet, evidence_package=rose.evidence_package, **kwargs)
    assert not evidence_outcome.valid and evidence_outcome.instruction is None


@pytest.mark.parametrize("mutation", [
    "profile_id", "profile_hash", "nested_profile", "timer_bool", "timer_range",
    "noncanonical_price", "direction", "order", "view_identity", "result_identity", "evidence_identity",
])
def test_instruction_mutations_rehashed_are_rejected(mutation: str):
    registry, packet, profile, evidence, market, observation, request, model_request = _inputs()
    candidate = _candidate("wait_for_price", market_view_ref_for(market, observation), evidence, market)
    response = _model_response(model_request, profile, candidate)
    outcome = finalize_decision(candidate, registry=registry, packet=packet, profile_ref=profile, evidence_package=evidence, market_snapshot=market, observation_frame=observation, analysis_request=request, normalized_model_request=model_request, normalized_model_response=response)
    assert outcome.valid
    mutated = copy.deepcopy(outcome.instruction)
    expected_view = copy.deepcopy(mutated["market_view_ref"])
    if mutation == "profile_id":
        mutated["profile_ref_id"] = "spr_" + "f" * 64
    elif mutation == "profile_hash":
        mutated["profile_ref_hash"] = "f" * 64
    elif mutation == "nested_profile":
        mutated["strategy_profile_ref"]["packet_hash"] = "f" * 64
    elif mutation == "timer_bool":
        mutated["entry_wait_seconds"] = True
    elif mutation == "timer_range":
        mutated["entry_wait_seconds"] = 86_401
    elif mutation == "noncanonical_price":
        mutated["entry_price"] = "100.0"
    elif mutation == "direction":
        mutated["direction"] = "sideways"
    elif mutation == "order":
        mutated["stop_price"] = "101"
    elif mutation == "view_identity":
        mutated["market_view_ref"]["shared_market_view_hash"] = "f" * 64
    elif mutation == "result_identity":
        mutated["result_id"] = "par_" + "f" * 64
    else:
        mutated["evidence_package_id"] = "evp_" + "f" * 64
    mutated["instruction_hash"] = instruction_hash(mutated)
    mutated["instruction_id"] = "sdi_" + mutated["instruction_hash"]
    assert not validate_instruction(mutated, expected_profile_ref=profile, expected_market_view_ref=expected_view).valid


def test_immediate_instruction_second_event_identity_requires_expected_ref():
    registry, packet, profile, evidence, market, observation, request, model_request = _inputs()
    candidate = _candidate("enter_immediately", market_view_ref_for(market, observation), evidence, market)
    response = _model_response(model_request, profile, candidate)
    outcome = finalize_decision(candidate, registry=registry, packet=packet, profile_ref=profile, evidence_package=evidence, market_snapshot=market, observation_frame=observation, analysis_request=request, normalized_model_request=model_request, normalized_model_response=response, second_event=candidate["second_event_ref"])
    assert outcome.valid
    mutated = copy.deepcopy(outcome.instruction)
    mutated["second_event_ref"]["event_id"] = "other_event"
    mutated["instruction_hash"] = instruction_hash(mutated)
    mutated["instruction_id"] = "sdi_" + mutated["instruction_hash"]
    assert not validate_instruction(mutated, expected_profile_ref=profile, expected_second_event_ref=candidate["second_event_ref"]).valid


@pytest.mark.parametrize("tamper", ["missing_candidate", "missing_ref", "tampered_ref", "branch", "direction", "levels"])
def test_normalized_response_binding_rejects_missing_and_semantic_tamper(tamper: str):
    registry, packet, profile, evidence, market, observation, request, model_request = _inputs()
    candidate = _candidate("wait_for_price", market_view_ref_for(market, observation), evidence, market)
    response = _model_response(model_request, profile, candidate)
    tampered = copy.deepcopy(response)
    if tamper == "missing_candidate":
        tampered["candidate"] = None
        # Leave the frozen artifact untouched: finalizer must still reject the
        # completed response rather than attempting a best-effort mapping.
    else:
        candidate_payload = tampered["candidate"]
        if tamper == "missing_ref":
            tampered["extensions"].pop("g018_model_output_ref", None)
        elif tamper == "tampered_ref":
            other = copy.deepcopy(candidate)
            other["entry_price"] = "101"
            tampered["extensions"]["g018_model_output_ref"] = make_model_output_ref(other)
        elif tamper == "branch":
            candidate_payload.update(status="no_trade", stance="neutral", action="none", setups=[], rationales=[], conditions=[], levels=[], model_confidence=None)
        elif tamper == "direction":
            candidate_payload["stance"] = "bearish"
        else:
            candidate_payload["levels"][0]["value"] = "101"
        tampered = finalize_artifact(tampered, "normalized_model_response")
        tampered = bind_strategy_profile_ref(tampered, profile)
    outcome = finalize_decision(candidate, registry=registry, packet=packet, profile_ref=profile, evidence_package=evidence, market_snapshot=market, observation_frame=observation, analysis_request=request, normalized_model_request=model_request, normalized_model_response=tampered)
    assert not outcome.valid and outcome.instruction is None


def test_foreign_market_view_is_rejected_without_instruction():
    registry, packet, profile, evidence, market, observation, request, model_request = _inputs()
    candidate = _candidate("no_entry", market_view_ref_for(market, observation), evidence, market)
    model_response = _model_response(model_request, profile, candidate)
    candidate["shared_market_view_ref"]["market_snapshot_id"] = "mkt_" + "f" * 64
    outcome = finalize_decision(candidate, registry=registry, packet=packet, profile_ref=profile, evidence_package=evidence, market_snapshot=market, observation_frame=observation, analysis_request=request, normalized_model_request=model_request, normalized_model_response=model_response)
    assert not outcome.valid
    assert outcome.instruction is None


def test_profile_tamper_is_rejected_by_authoritative_registry():
    registry, packet, profile, evidence, market, observation, request, model_request = _inputs()
    candidate = _candidate("no_entry", market_view_ref_for(market, observation), evidence, market)
    forged = copy.deepcopy(profile)
    forged["strategy_id"] = "pa:rose-gap-bar"
    outcome = finalize_decision(candidate, registry=registry, packet=packet, profile_ref=forged, evidence_package=evidence, market_snapshot=market, observation_frame=observation, analysis_request=request, normalized_model_request=model_request, normalized_model_response=_model_response(model_request, profile, candidate))
    assert not outcome.valid
    assert outcome.code in {"profile_binding_mismatch", "registry_validation_failure"}
