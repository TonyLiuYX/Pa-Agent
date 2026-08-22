"""G016 AC3-AC4: lifecycle, lineage and registration tamper matrix."""
from __future__ import annotations

import copy
import json
import shutil
import sys
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from conftest import (
    KNOWLEDGE,
    MANIFEST_PATH,
    RECORDS_PATH,
    CHUNKS_PATH,
    as_dict,
    assert_typed_nonexec,
    call_validate_packet,
    formal_records,
    load_json,
    outcome_code,
    registry_for,
    first_setup,
    packet_dict,
)


FIXTURE_DIR = Path(__file__).resolve().parents[2] / "fixtures" / "phase2" / "g016"
TAMPER_FIXTURES = {
    item["mutation"]: item
    for item in load_json(FIXTURE_DIR / "tamper-cases.json")["cases"]
}


def _copy_knowledge(tmp_path: Path) -> Path:
    destination = tmp_path / "knowledge"
    shutil.copytree(KNOWLEDGE, destination)
    return destination


def _data_root(knowledge_dir: Path) -> Path:
    return knowledge_dir / "data"


def _write(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _mutate_case(knowledge_dir: Path, mutation: str) -> str:
    data = _data_root(knowledge_dir)
    records_doc = load_json(data / "pa_records.json")
    records = records_doc["records"]
    selected = next(item for item in records if item.get("type") == "setup")
    selected_id = selected["id"]
    selected_ref = next(
        claim["source_refs"][0]
        for field in (
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
        for claim in selected.get(field, [])
        if claim.get("source_refs")
    )
    if mutation == "remove_record":
        records_doc["records"] = [item for item in records if item["id"] != selected_id]
    elif mutation == "inactive_record":
        selected["active"] = False
        selected["record_status"] = "invalidated"
        # Inactive is distinct from pending-source reverification.  Keep the
        # source fidelity marker successful and explicitly clear the pending
        # flag so lifecycle precedence is tested by the fixture itself.
        selected["source_fidelity"] = "source_faithful_with_normalization"
        selected["reverification_required"] = False
    elif mutation == "pending_source":
        selected["active"] = False
        selected["record_status"] = "invalidated"
        selected["source_fidelity"] = "pending_source_reverification"
        selected["reverification_required"] = True
        manifest = load_json(data / "manifest.json")
        for document in manifest["documents"]:
            if document["source_id"] == selected["source_id"]:
                document["source_reverification_status"] = "pending_source_reverification"
                document["source_reverification_required"] = True
                document["source_reverified_sha256"] = None
        _write(data / "manifest.json", manifest)
    elif mutation == "non_setup":
        selected["type"] = "concept"
    elif mutation == "version_mismatch":
        manifest = load_json(data / "manifest.json")
        manifest["knowledge_base_version"] = "9.9.9"
        _write(data / "manifest.json", manifest)
    elif mutation == "hash_mismatch":
        manifest = load_json(data / "manifest.json")
        manifest["documents"][0]["sha256"] = "0" * 64
        _write(data / "manifest.json", manifest)
    elif mutation in {"source_sha", "chunk_id", "page", "title_path", "source_file", "external_source"}:
        if mutation == "source_sha":
            selected_ref["source_sha256"] = "0" * 64
        elif mutation == "chunk_id":
            selected_ref["chunk_id"] = "missing-chunk"
        elif mutation == "page":
            selected_ref["page_start"] = 9999
            selected_ref["page_end"] = 9999
        elif mutation == "title_path":
            selected_ref["title_path"] = ["tampered title"]
        elif mutation == "source_file":
            selected_ref["source_file"] = "tampered.pdf"
        else:
            selected_ref["chunk_id"] = None
            selected_ref["page_start"] = None
            selected_ref["page_end"] = None
            selected_ref["external_url"] = "https://example.invalid/foreign"
            selected_ref["provenance_status"] = "supplemental_external_due_to_pdf_failure"
            claim = next(
                claim
                for field in (
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
                for claim in selected.get(field, [])
                if claim.get("source_refs") and claim["source_refs"][0] is selected_ref
            )
            claim["declaration_type"] = "source_assertion"
            claim.pop("rationale", None)
    elif mutation == "chunk_text_sha":
        chunks_doc = load_json(data / "chunks.json")
        chunk = next(item for item in chunks_doc["chunks"] if item["chunk_id"] == selected_ref["chunk_id"])
        chunk["source_text_sha256"] = "0" * 64
        _write(data / "chunks.json", chunks_doc)
    elif mutation == "declaration_relabel":
        claim = next(
            claim
            for field in (
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
            for claim in selected.get(field, [])
            if claim.get("declaration_type") in {"derived_interpretation", "source_not_stated"}
        )
        claim["declaration_type"] = "source_assertion"
        claim.pop("rationale", None)
    else:
        raise AssertionError(mutation)
    _write(data / "pa_records.json", records_doc)
    return selected_id


def _typed_build(api, knowledge_dir: Path, selected_id: str | None = None, *, expected_mismatch: str | None = None):
    try:
        registrations = None
        if selected_id is not None:
            registrations = [{"record_id": selected_id, "strategy_id": "pa:" + selected_id}]
        if expected_mismatch == "version":
            function = api.build_registry
            return function(knowledge_dir, registrations=registrations, expected_snapshot_version="9.9.9")
        if expected_mismatch == "hash":
            function = api.build_registry
            return function(knowledge_dir, registrations=registrations, expected_snapshot_hash="0" * 64)
        return registry_for(api, knowledge_dir, registrations=registrations)
    except Exception as exc:  # noqa: BLE001 - assert typed public failure below
        assert isinstance(exc, api.StrategyKnowledgeError), repr(exc)
        return exc


@pytest.mark.parametrize("case", load_json(FIXTURE_DIR / "lifecycle-cases.json")["cases"], ids=lambda item: item["name"])
def test_lifecycle_and_lineage_tamper_cases_are_typed_nonexecutables(api, tmp_path: Path, case: dict[str, Any]):
    knowledge_dir = _copy_knowledge(tmp_path)
    selected_id = _mutate_case(knowledge_dir, case["mutation"])
    mismatch = "version" if case["mutation"] == "version_mismatch" else "hash" if case["mutation"] == "hash_mismatch" else None
    built = _typed_build(api, knowledge_dir, selected_id, expected_mismatch=mismatch)
    if hasattr(built, "load_packet"):
        outcome = built.load_packet(f"pa:{selected_id}")
        if case["mutation"] == "declaration_relabel" and outcome.executable:
            packet = packet_dict(outcome)
            field = next(
                field
                for field in packet["claim_fields"]
                if any(item.get("declaration_type") != "source_assertion" for item in packet["claims"].get(field, []))
            )
            packet_claim = next(item for item in packet["claims"][field] if item.get("declaration_type") != "source_assertion")
            packet_claim["declaration_type"] = "source_assertion"
            packet_claim.pop("rationale", None)
            outcome = call_validate_packet(api, packet)
    else:
        outcome = built
    assert_typed_nonexec(api, outcome, *case["expected_codes"])
    # A failed load cannot accidentally expose a model-ready packet.
    plain = as_dict(outcome)
    if isinstance(plain, dict):
        assert not plain.get("packet")
        assert not plain.get("evidence_package")


def test_duplicate_record_and_claim_registration_are_typed_rejections(api, tmp_path: Path):
    for mutation in ("duplicate_record", "duplicate_claim"):
        knowledge_dir = _copy_knowledge(tmp_path / mutation)
        records_path = _data_root(knowledge_dir) / "pa_records.json"
        records_doc = load_json(records_path)
        selected = copy.deepcopy(next(item for item in records_doc["records"] if item["type"] == "setup"))
        if mutation == "duplicate_record":
            records_doc["records"].append(selected)
        else:
            first_claim = next(
                claim
                for field in (
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
                for claim in selected.get(field, [])
            )
            selected["entry"].append(copy.deepcopy(first_claim))
            records_doc["records"][0] = selected
        _write(records_path, records_doc)
        outcome = _typed_build(api, knowledge_dir)
        if mutation == "duplicate_claim" and hasattr(outcome, "load_packet"):
            first_id = next(item["id"] for item in records_doc["records"] if item["type"] == "setup")
            outcome = outcome.load_packet("pa:" + first_id)
        expected = TAMPER_FIXTURES["duplicate_record" if mutation == "duplicate_record" else "duplicate_claim"]["expected_codes"]
        assert_typed_nonexec(api, outcome, *expected, "snapshot", "invalid")


def test_noncanonical_strategy_id_and_cross_owner_packet_are_typed(api):
    registry = registry_for(api)
    strategy_id = sorted(registry.strategy_ids)[0] if not callable(registry.strategy_ids) else sorted(registry.strategy_ids())[0]
    try:
        outcome = registry.load_packet(strategy_id.replace("pa:", "strategy:"))
    except Exception as exc:  # noqa: BLE001 - typed error is the contract
        assert isinstance(exc, api.StrategyKnowledgeError), repr(exc)
        assert outcome_code(exc)
    else:
        # The implementation may return a PacketLoadOutcome or raise the
        # typed StrategyKnowledgeError; both preserve a stable code and no
        # traceback.
        expected = TAMPER_FIXTURES["noncanonical_strategy_id"]["expected_codes"]
        assert_typed_nonexec(api, outcome, *expected, "strategy_not_registered")
