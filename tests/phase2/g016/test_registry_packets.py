"""G016 AC1-AC3: exact setup registry, packet shape and G014 evidence."""
from __future__ import annotations

import copy
import json
import sys
from collections.abc import Mapping
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from conftest import (
    G014_FIXTURES,
    KNOWLEDGE,
    RECORDS_PATH,
    as_dict,
    assert_typed_nonexec,
    call_packet_hash,
    call_validate_packet,
    claims,
    evidence_dict,
    formal_records,
    load_json,
    load_packet,
    outcome_packet,
    packet_dict,
    registry_for,
    result_valid,
    strategy_ids,
)


CLAIM_FIELDS = (
    "market_context",
    "prerequisites",
    "recognition_features",
    "trigger",
    "entry",
    "invalidation_stop",
    "targets_exit",
    "risk_notes",
    "prohibited",
    "exceptions",
    "examples",
    "interpretation_notes",
)

TAMPER_FIXTURES = {
    item["mutation"]: item
    for item in load_json(Path(__file__).resolve().parents[2] / "fixtures" / "phase2" / "g016" / "tamper-cases.json")["cases"]
}


def _packet_result(registry, strategy_id: str):
    outcome = load_packet(registry, strategy_id)
    packet = outcome_packet(outcome)
    assert packet is not None, outcome
    return outcome, packet


def _packet_claims(packet: Mapping[str, object], field: str | None = None) -> list[dict]:
    value = packet.get("claims", {})
    if isinstance(value, Mapping):
        if field is not None:
            return [dict(as_dict(item)) for item in value.get(field, [])]
        return [
            dict(as_dict(item))
            for claim_field in CLAIM_FIELDS
            for item in value.get(claim_field, [])
        ]
    if isinstance(value, list):
        result = [dict(as_dict(item)) for item in value]
        return [item for item in result if field is None or item.get("field") == field]
    raise AssertionError(value)


def _evidence_claims(evidence: Mapping[str, object]) -> list[dict]:
    return [dict(as_dict(item)) for item in evidence.get("evidence_items", [])]


def _citation_by_id(evidence: Mapping[str, object]) -> dict[str, dict]:
    return {
        str(item["citation_id"]): dict(as_dict(item))
        for item in evidence.get("citations", [])
    }


def test_active_setup_registry_ids_are_exact_and_concepts_are_not_monitors(api):
    records = formal_records()
    setups = [item for item in records if item["type"] == "setup" and item.get("active")]
    concepts = [item for item in records if item["type"] in {"concept", "discipline"}]
    registry = registry_for(api)
    ids = strategy_ids(registry)
    identity = api.compute_snapshot_identity(KNOWLEDGE)
    assert isinstance(identity, api.SnapshotIdentity)
    assert as_dict(identity) == as_dict(registry.snapshot_identity)

    expected = {f"pa:{item['id']}" for item in setups}
    assert len(records) == 24
    assert len(setups) == 21
    assert set(ids) == expected
    assert len(ids) == len(set(ids)) == 21

    # The registry can still provide supplemental concepts to a packet, but
    # they never receive a scheduler/account identity of their own.
    for record in concepts:
        assert f"pa:{record['id']}" not in ids
        outcome = load_packet(registry, f"pa:{record['id']}")
        assert_typed_nonexec(api, outcome, "strategy_not_registered", "not_registered", "non_setup", "concept", "discipline")


def test_every_setup_packet_has_exact_primary_record_and_flattened_claims(api):
    registry = registry_for(api)
    records = {item["id"]: item for item in formal_records()}
    assert tuple(getattr(api, "CLAIM_FIELDS")) == CLAIM_FIELDS
    total = 0
    declaration_counts: dict[str, int] = {}
    for strategy_id in strategy_ids(registry):
        record_id = strategy_id.removeprefix("pa:")
        outcome, packet_obj = _packet_result(registry, strategy_id)
        packet = packet_dict(outcome)
        assert packet["strategy_id"] == strategy_id
        assert packet["record_id"] == record_id
        assert packet.get("record_type", packet["primary_record"]["type"]) == "setup"
        assert as_dict(outcome).get("executable") is True
        assert packet.get("executable", True) is True
        # The primary record is deliberately lossless, including JSON's
        # original confidence float.  Canonical hashing is packet-owned.
        assert packet["primary_record"] == records[record_id]

        fields = packet.get("claim_fields", tuple(CLAIM_FIELDS))
        assert tuple(fields) == CLAIM_FIELDS
        flattened = _packet_claims(packet)
        for field in CLAIM_FIELDS:
            assert field in packet.get("claims", {})
            assert all(item.get("field") == field for item in _packet_claims(packet, field))
        source_claims = claims(records[record_id])
        assert len(flattened) == len(source_claims)
        assert {item["claim_id"] for item in flattened} == {
            item["claim_id"] for _, item in source_claims
        }
        for field, source_claim in source_claims:
            packet_claim = next(item for item in flattened if item["claim_id"] == source_claim["claim_id"])
            assert packet_claim["field"] == field
            assert packet_claim["statement"] == source_claim["statement"]
            assert packet_claim["declaration_type"] == source_claim["declaration_type"]
            assert packet_claim.get("rationale") == source_claim.get("rationale")
            total += 1
            declaration = source_claim["declaration_type"]
            declaration_counts[declaration] = declaration_counts.get(declaration, 0) + 1

        assert packet.get("packet_hash") == call_packet_hash(api, packet_obj)
        assert packet.get("packet_id", "").startswith("spk_")

    assert total == 491
    assert declaration_counts == {
        "source_assertion": 265,
        "derived_interpretation": 197,
        "source_not_stated": 29,
    }


@pytest.mark.parametrize(
    ("field", "value"),
    [("record_type", "concept"), ("record_status", "invalidated"), ("executable", False)],
)
def test_rehashed_packet_top_level_execution_metadata_tamper_is_typed_invalid(api, field, value):
    registry = registry_for(api)
    strategy_id = sorted(strategy_ids(registry))[0]
    outcome, packet_obj = _packet_result(registry, strategy_id)
    packet = packet_dict(outcome)
    packet[field] = value
    packet["packet_hash"] = call_packet_hash(api, packet)
    packet["packet_id"] = "spk_" + packet["packet_hash"]
    validation = call_validate_packet(api, packet)
    assert not result_valid(validation), validation
    assert as_dict(validation).get("code") == "packet_binding_mismatch"


def test_packet_evidence_is_g014_valid_and_claim_lineage_is_complete(api):
    from pa_engine.contracts.validation import validate_artifact

    registry = registry_for(api)
    records = {item["id"]: item for item in formal_records()}
    chunks = {item["chunk_id"]: item for item in load_json(Path(KNOWLEDGE) / "data" / "chunks.json")["chunks"]}
    manifest = {
        item["source_id"]: item for item in load_json(Path(KNOWLEDGE) / "data" / "manifest.json")["documents"]
    }
    for strategy_id in strategy_ids(registry):
        outcome, packet_obj = _packet_result(registry, strategy_id)
        packet = packet_dict(outcome)
        evidence = evidence_dict(outcome)
        validation = validate_artifact(
            evidence,
            expected_kind="knowledge_evidence_package",
            knowledge_dir=KNOWLEDGE,
        )
        assert validation.valid, (strategy_id, validation.to_dict())
        record_id = strategy_id.removeprefix("pa:")
        assert all(item["record_id"] == record_id for item in _evidence_claims(evidence))
        citations = _citation_by_id(evidence)
        packet_claim_by_id = {item["claim_id"]: item for item in _packet_claims(packet)}
        assert packet_claim_by_id
        packet_excerpts = packet.get("chunk_excerpts", [])
        excerpt_by_id = {str(ex["chunk_id"]): ex for ex in packet_excerpts}
        for item in _evidence_claims(evidence):
            source_claim = next(claim for _, claim in claims(records[record_id]) if claim["claim_id"] == item["claim_id"])
            assert item["field"] in CLAIM_FIELDS
            assert item["statement"] == source_claim["statement"]
            assert item["statement_sha256"]
            assert item["declaration_type"] == source_claim["declaration_type"]
            assert item.get("rationale") == source_claim.get("rationale")
            packet_claim = packet_claim_by_id[item["claim_id"]]
            refs = source_claim.get("source_refs", [])
            for ref in refs:
                chunk = chunks[ref["chunk_id"]]
                source = manifest[ref["source_id"]]
                assert ref["source_id"] == chunk["source_id"] == source["source_id"]
                assert ref["source_file"] == chunk["source_file"] == source["source_file"]
                assert ref["source_sha256"] == chunk["source_sha256"] == source["sha256"]
                assert ref["page_start"] >= chunk["page_start"]
                assert ref["page_end"] <= chunk["page_end"]
                assert ref["page_start"] <= ref["page_end"]
                assert ref["chunk_id"] in excerpt_by_id
                excerpt = excerpt_by_id[ref["chunk_id"]]
                assert excerpt.get("source_text") == chunk["source_text"]
                assert excerpt.get("source_text_sha256") == chunk["source_text_sha256"]

            for citation_id in item["citation_ids"]:
                citation = citations[citation_id]
                assert citation["claim_id"] == item["claim_id"]
                assert citation["declaration_type"] == item["declaration_type"]
                assert citation["source_sha256"] == manifest[citation["source_id"]]["sha256"]
                assert citation["chunk_id"] in chunks
                assert citation["chunk_text_sha256"] == chunks[citation["chunk_id"]]["source_text_sha256"]
                assert citation["page_start"] <= citation["page_end"]
                assert citation["page_start"] >= chunks[citation["chunk_id"]]["page_start"]
                assert citation["page_end"] <= chunks[citation["chunk_id"]]["page_end"]

        # Concepts/discipline may only be marked supplemental and must not
        # appear as executable evidence items for this setup packet.
        assert not any(item.get("record_type") in {"concept", "discipline"} for item in _evidence_claims(evidence))


def test_declaration_types_and_rationales_are_never_upgraded(api):
    records = {item["id"]: item for item in formal_records()}
    registry = registry_for(api)
    for strategy_id in strategy_ids(registry):
        record_id = strategy_id.removeprefix("pa:")
        outcome, packet_obj = _packet_result(registry, strategy_id)
        packet = packet_dict(outcome)
        original = {item["claim_id"]: item for _, item in claims(records[record_id])}
        for claim in _packet_claims(packet):
            source = original[claim["claim_id"]]
            assert claim["declaration_type"] == source["declaration_type"]
            assert claim.get("rationale") == source.get("rationale")
            if source["declaration_type"] in {"derived_interpretation", "source_not_stated"}:
                assert claim["declaration_type"] != "source_assertion"
                assert claim.get("rationale")


@pytest.mark.parametrize(
    ("target", "fixture_mutation"),
    [("sibling", "sibling_claim"), ("foreign_owner", "foreign_claim_owner"), ("supplemental", "supplemental_setup")],
)
def test_cross_strategy_executable_claims_are_rejected(api, target, fixture_mutation):
    registry = registry_for(api)
    first_id, second_id = sorted(strategy_ids(registry))[:2]
    outcome, packet_obj = _packet_result(registry, first_id)
    packet = packet_dict(outcome)
    second_outcome, second_packet_obj = _packet_result(registry, second_id)
    second_packet = packet_dict(second_outcome)
    foreign_claim = copy.deepcopy(_packet_claims(second_packet)[0])
    if target == "sibling":
        packet["claims"][foreign_claim["field"]].append(foreign_claim)
    elif target == "foreign_owner":
        packet["claims"][foreign_claim["field"]][0]["record_id"] = second_id.removeprefix("pa:")
    else:
        packet["supplemental_context"] = {
            "executable": True,
            "record_ids": [second_id.removeprefix("pa:")],
            "records": [{"id": second_id.removeprefix("pa:"), "type": "setup", "claims": [foreign_claim]}],
        }
    validation = call_validate_packet(api, packet)
    assert not result_valid(validation), validation
    code = str(as_dict(validation).get("code", as_dict(validation).get("error_code", ""))).lower()
    expected = TAMPER_FIXTURES[fixture_mutation]["expected_codes"]
    assert any(token in code for token in ("cross", "owner", "record", "claim", "supplemental", "hash")), validation
    assert expected and fixture_mutation == TAMPER_FIXTURES[fixture_mutation]["mutation"]


def test_primary_packet_has_no_other_executable_setup_evidence(api):
    registry = registry_for(api)
    for strategy_id in strategy_ids(registry):
        outcome, packet_obj = _packet_result(registry, strategy_id)
        packet = packet_dict(outcome)
        evidence = evidence_dict(outcome)
        owner = strategy_id.removeprefix("pa:")
        executable_records = {packet.get("record_id")}
        executable_records.update(
            str(item.get("record_id"))
            for item in _evidence_claims(evidence)
            if item.get("executable", True)
        )
        assert executable_records == {owner}
