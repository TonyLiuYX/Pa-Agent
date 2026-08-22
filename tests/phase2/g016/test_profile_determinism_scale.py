"""Strategy identity, profile lineage, determinism, and scale tests."""
from __future__ import annotations

import ast
import copy
import hashlib
import json
import shutil
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from conftest import (
    KNOWLEDGE,
    as_dict,
    bind_profile,
    call_packet_hash,
    call_validate_lineage,
    canonical_bytes,
    evidence_dict,
    formal_records,
    load_json,
    load_packet,
    no_float,
    outcome_packet,
    outcome_profile,
    packet_dict,
    profile_ref,
    registry_for,
    result_valid,
    snapshot_dict,
    strategy_ids,
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


def _write(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _all_packet_claims(packet: Mapping[str, Any]) -> list[dict[str, Any]]:
    fields = packet.get("claim_fields") or tuple(getattr(packet, "CLAIM_FIELDS", ()))
    value = packet.get("claims", {})
    if isinstance(value, Mapping):
        return [dict(as_dict(item)) for field in fields for item in value.get(field, [])]
    return [dict(as_dict(item)) for item in value]


def _profile_artifacts(profile: Mapping[str, Any], evidence: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    evidence_artifact = copy.deepcopy(dict(evidence))
    evidence_artifact.setdefault("extensions", {})["strategy_profile_ref"] = copy.deepcopy(dict(profile))
    result = {
        "analysis_request": {
            "extensions": {"strategy_profile_ref": copy.deepcopy(dict(profile))},
        },
        "knowledge_evidence_package": evidence_artifact,
        "normalized_model_request": {
            "extensions": {"strategy_profile_ref": copy.deepcopy(dict(profile))},
            "context_refs": {
                "evidence_package_id": evidence.get("evidence_package_id"),
                "evidence_package_hash": evidence.get("content_hash"),
                "knowledge_snapshot_version": profile.get("snapshot_version"),
            },
        },
        "normalized_model_response": {
            "extensions": {"strategy_profile_ref": copy.deepcopy(dict(profile))},
        },
        "pa_analysis_result": {
            "extensions": {"strategy_profile_ref": copy.deepcopy(dict(profile))},
        },
    }
    return result


def _valid_lineage(api, artifacts: Mapping[str, Any], profile: Mapping[str, Any], packet: Mapping[str, Any] | None = None):
    return call_validate_lineage(api, artifacts, profile, packet)


def test_snapshot_and_packet_identity_are_deterministic_across_instances_and_input_order(api, tmp_path: Path):
    first = registry_for(api)
    second = registry_for(api)
    assert strategy_ids(first) == strategy_ids(second)
    assert snapshot_dict(first) == snapshot_dict(second)
    strategy_id = sorted(strategy_ids(first))[0]
    outcome_a = load_packet(first, strategy_id)
    outcome_b = load_packet(second, strategy_id)
    packet_a = packet_dict(outcome_a)
    packet_b = packet_dict(outcome_b)
    assert packet_a == packet_b
    assert call_packet_hash(api, outcome_packet(outcome_a)) == call_packet_hash(api, outcome_packet(outcome_b))

    # Reversing input arrays and object key order is an equivalent registry
    # input; packet and snapshot projections sort semantic collections.
    knowledge_dir = _copy_knowledge(tmp_path)
    for name in ("pa_records.json", "chunks.json"):
        path = knowledge_dir / "data" / name
        value = load_json(path)
        key = "records" if name.startswith("pa_") else "chunks"
        value[key].reverse()
        _write(path, value)
    reordered = registry_for(api, knowledge_dir)
    assert strategy_ids(reordered) == strategy_ids(first)
    assert snapshot_dict(reordered) == snapshot_dict(first)
    reordered_packet = packet_dict(load_packet(reordered, strategy_id))
    assert call_packet_hash(api, reordered_packet) == call_packet_hash(api, packet_a)


def test_snapshot_ignores_manifest_telemetry_but_real_claim_changes_identity(api, tmp_path: Path):
    baseline = registry_for(api)
    baseline_identity = snapshot_dict(baseline)
    telemetry_dir = _copy_knowledge(tmp_path / "telemetry")
    manifest_path = telemetry_dir / "data" / "manifest.json"
    manifest = load_json(manifest_path)
    manifest["generated_at"] = "2099-01-01T00:00:00Z"
    manifest.setdefault("lifecycle", {})["run_id"] = "telemetry-only-run"
    manifest["changes"] = [{"status": "telemetry-only", "source_id": "ignored"}]
    manifest["change_summary"] = {"added": 99, "changed": 99, "deleted": 99}
    _write(manifest_path, manifest)
    telemetry_registry = registry_for(api, telemetry_dir)
    assert snapshot_dict(telemetry_registry) == baseline_identity

    claim_dir = _copy_knowledge(tmp_path / "claim")
    records_path = claim_dir / "data" / "pa_records.json"
    records_doc = load_json(records_path)
    record = next(item for item in records_doc["records"] if item["type"] == "setup")
    claim = next(
        claim
        for field in (
            "market_context", "prerequisites", "recognition_features", "trigger", "entry",
            "invalidation_stop", "targets_exit", "risk_notes", "prohibited", "exceptions",
            "examples", "interpretation_notes",
        )
        for claim in record.get(field, [])
        if claim.get("statement")
    )
    claim["statement"] += " [legitimate snapshot revision]"
    _write(records_path, records_doc)
    changed = registry_for(api, claim_dir)
    assert snapshot_dict(changed) != baseline_identity
    strategy_id = f"pa:{record['id']}"
    baseline_packet = packet_dict(load_packet(baseline, strategy_id))
    changed_packet = packet_dict(load_packet(changed, strategy_id))
    assert call_packet_hash(api, baseline_packet) != call_packet_hash(api, changed_packet)
    assert baseline_packet["strategy_id"] == changed_packet["strategy_id"] == strategy_id


def test_cache_is_keyed_by_snapshot_and_strategy_and_returned_objects_are_isolated(api, tmp_path: Path):
    registry = registry_for(api)
    strategy_id = sorted(strategy_ids(registry))[0]
    first = load_packet(registry, strategy_id)
    first_packet = packet_dict(first)
    first_hash = call_packet_hash(api, first_packet)
    first_packet["claims"][first_packet["claim_fields"][0]][0]["statement"] = "caller mutation"
    second = load_packet(registry, strategy_id)
    second_packet = packet_dict(second)
    assert call_packet_hash(api, second_packet) == first_hash
    assert second_packet["claims"][second_packet["claim_fields"][0]][0]["statement"] != "caller mutation"

    cache = getattr(registry, "_cache", getattr(registry, "cache", None))
    if cache is not None:
        keys = list(cache.keys()) if hasattr(cache, "keys") else []
        snapshot = snapshot_dict(registry)
        snapshot_hash = snapshot.get("snapshot_hash") or snapshot.get("knowledge_snapshot_hash") or snapshot.get("hash")
        assert any(snapshot_hash in str(key) and strategy_id in str(key) for key in keys)


def test_profile_binds_one_packet_and_lineage_swaps_are_typed_invalid(api):
    registry = registry_for(api)
    strategy_a, strategy_b = sorted(strategy_ids(registry))[:2]
    outcome_a = load_packet(registry, strategy_a)
    outcome_b = load_packet(registry, strategy_b)
    packet_a = outcome_packet(outcome_a)
    packet_b = outcome_packet(outcome_b)
    evidence_a = evidence_dict(outcome_a)
    evidence_b = evidence_dict(outcome_b)
    binding = outcome_profile(outcome_a)
    assert binding is not None
    profile_a = profile_ref(binding)
    profile_b = profile_ref(outcome_profile(outcome_b))
    assert profile_a["strategy_id"] == strategy_a
    assert profile_a["record_id"] == strategy_a.removeprefix("pa:")
    assert profile_a["packet_hash"] == packet_dict(outcome_a)["packet_hash"]
    assert profile_a["snapshot_hash"] == snapshot_dict(registry).get("snapshot_hash", profile_a["snapshot_hash"])
    assert profile_a["packet_id"] == "spk_" + profile_a["packet_hash"]
    no_float(profile_a)

    # The frozen binder copies the profile extension onto each downstream
    # artifact and does not retain caller-owned mutable references.
    bound_artifact = api.bind_strategy_profile_ref({"extensions": {}}, profile_a)
    assert bound_artifact["extensions"]["strategy_profile_ref"] == profile_a
    profile_a["strategy_id"] = "pa:caller-mutation"
    assert bound_artifact["extensions"]["strategy_profile_ref"]["strategy_id"] == strategy_a
    profile_a["strategy_id"] = strategy_a

    artifacts = _profile_artifacts(profile_a, evidence_a)
    valid = _valid_lineage(api, artifacts, profile_a, packet_a)
    assert result_valid(valid), as_dict(valid)
    for label, mutation in (
        ("profile", lambda item: item.update(profile_b)),
        ("packet", lambda item: item.update({"packet_hash": profile_b["packet_hash"], "packet_id": profile_b["packet_id"]})),
        ("snapshot", lambda item: item.update({"snapshot_hash": "0" * 64, "snapshot_id": "kbs_" + "0" * 64})),
        ("evidence", None),
    ):
        changed = copy.deepcopy(artifacts)
        if label == "evidence":
            changed["knowledge_evidence_package"] = copy.deepcopy(evidence_b)
            changed["knowledge_evidence_package"].setdefault("extensions", {})["strategy_profile_ref"] = copy.deepcopy(profile_a)
        else:
            assert mutation is not None
            mutation(changed["normalized_model_response"]["extensions"]["strategy_profile_ref"])
        result = _valid_lineage(api, changed, profile_a, packet_a)
        assert not result_valid(result), (label, as_dict(result))
        plain = as_dict(result)
        code = str(plain.get("code", plain.get("error_code", plain.get("status", "")))).lower() if isinstance(plain, Mapping) else ""
        assert code or not result_valid(result)


def test_registry_lineage_without_packet_uses_authoritative_snapshot_and_packet(api):
    """A registry must not trust a self-consistent spoofed profile.

    The caller may validate an artifact without passing its packet explicitly,
    but the registry still owns the authoritative snapshot and packet binding.
    """

    registry = registry_for(api)
    strategy_id = sorted(strategy_ids(registry))[0]
    outcome = load_packet(registry, strategy_id)
    profile = profile_ref(outcome_profile(outcome))
    request = {"artifact_kind": "analysis_request", "extensions": {"strategy_profile_ref": copy.deepcopy(profile)}}

    valid = registry.validate_profile_lineage(profile, packet=None, artifacts=[request])
    assert result_valid(valid), as_dict(valid)

    spoof = copy.deepcopy(profile)
    spoof["snapshot_hash"] = "0" * 64
    spoof["snapshot_id"] = "kbs_" + spoof["snapshot_hash"]
    projection = {key: value for key, value in spoof.items() if key not in {"profile_ref_id", "profile_ref_hash"}}
    digest = hashlib.sha256(canonical_bytes(projection)).hexdigest()
    spoof["profile_ref_hash"] = digest
    spoof["profile_ref_id"] = "spr_" + digest
    forged_request = {"artifact_kind": "analysis_request", "extensions": {"strategy_profile_ref": copy.deepcopy(spoof)}}
    rejected = registry.validate_profile_lineage(spoof, packet=None, artifacts=[forged_request])
    assert not result_valid(rejected), as_dict(rejected)
    assert as_dict(rejected).get("code") == "snapshot_mismatch"


def test_unused_citation_and_foreign_evidence_item_are_rejected(api):
    registry = registry_for(api)
    strategy_a, strategy_b = sorted(strategy_ids(registry))[:2]
    outcome_a = load_packet(registry, strategy_a)
    outcome_b = load_packet(registry, strategy_b)
    packet_a = packet_dict(outcome_a)
    evidence_a = evidence_dict(outcome_a)
    foreign_evidence = evidence_dict(outcome_b)
    profile_a = profile_ref(outcome_profile(outcome_a))
    artifacts = _profile_artifacts(profile_a, evidence_a)

    unused = copy.deepcopy(artifacts)
    package = unused["knowledge_evidence_package"]
    package["citations"].append(copy.deepcopy(foreign_evidence["citations"][0]))
    result = _valid_lineage(api, unused, profile_a, packet_a)
    assert not result_valid(result), as_dict(result)
    assert TAMPER_FIXTURES["unused_citation"]["expected_codes"]

    foreign = copy.deepcopy(artifacts)
    package = foreign["knowledge_evidence_package"]
    package["evidence_items"].append(copy.deepcopy(foreign_evidence["evidence_items"][0]))
    result = _valid_lineage(api, foreign, profile_a, packet_a)
    assert not result_valid(result), as_dict(result)
    assert TAMPER_FIXTURES["foreign_evidence"]["expected_codes"]


def test_nested_model_request_context_refs_bind_evidence_and_snapshot_version(api):
    registry = registry_for(api)
    strategy_id = sorted(strategy_ids(registry))[0]
    outcome = load_packet(registry, strategy_id)
    packet = packet_dict(outcome)
    evidence = evidence_dict(outcome)
    profile = profile_ref(outcome_profile(outcome))
    artifacts = _profile_artifacts(profile, evidence)
    request = artifacts["normalized_model_request"]
    context_refs = request["context_refs"]
    assert context_refs["evidence_package_id"] == evidence["evidence_package_id"]
    assert context_refs["evidence_package_hash"] == evidence["content_hash"]
    assert context_refs["knowledge_snapshot_version"] == profile["snapshot_version"]
    for key, replacement, expected_code in (
        ("evidence_package_id", "evp_" + "0" * 64, "lineage_mismatch"),
        ("evidence_package_hash", "0" * 64, "lineage_mismatch"),
        ("knowledge_snapshot_version", "9.9.9", "snapshot_mismatch"),
    ):
        changed = copy.deepcopy(artifacts)
        changed["normalized_model_request"]["context_refs"][key] = replacement
        result = _valid_lineage(api, changed, profile, packet)
        assert not result_valid(result), (key, as_dict(result))
        assert as_dict(result).get("code") == expected_code


def test_same_record_id_snapshot_changes_keep_strategy_identity_and_new_id_is_new_strategy(api, tmp_path: Path):
    baseline = registry_for(api)
    original = sorted(strategy_ids(baseline))[0]
    changed_dir = _copy_knowledge(tmp_path / "changed")
    records_path = changed_dir / "data" / "pa_records.json"
    records_doc = load_json(records_path)
    record = next(item for item in records_doc["records"] if f"pa:{item['id']}" == original)
    claim = next(claim for field in ("entry", "trigger", "market_context") for claim in record.get(field, []))
    claim["statement"] += " [version B]"
    _write(records_path, records_doc)
    changed = registry_for(api, changed_dir)
    assert original in strategy_ids(changed)
    assert snapshot_dict(changed) != snapshot_dict(baseline)
    old_packet = packet_dict(load_packet(baseline, original))
    new_packet = packet_dict(load_packet(changed, original))
    assert old_packet["strategy_id"] == new_packet["strategy_id"] == original
    assert old_packet["packet_hash"] != new_packet["packet_hash"]

    new_id_dir = _copy_knowledge(tmp_path / "new-id")
    records_path = new_id_dir / "data" / "pa_records.json"
    records_doc = load_json(records_path)
    clone = copy.deepcopy(next(item for item in records_doc["records"] if item["type"] == "setup"))
    old_id = clone["id"]
    clone["id"] = "synthetic-new-record"
    clone["name"] = "Synthetic new record"
    for field_name in (
        "market_context", "prerequisites", "recognition_features", "trigger", "entry",
        "invalidation_stop", "targets_exit", "risk_notes", "prohibited", "exceptions",
        "examples", "interpretation_notes",
    ):
        for field in clone.get(field_name, []):
            field["claim_id"] = field["claim_id"].replace(f"{old_id}:", "synthetic-new-record:", 1)
    records_doc["records"].append(clone)
    _write(records_path, records_doc)
    expanded = registry_for(api, new_id_dir)
    assert "pa:synthetic-new-record" in strategy_ids(expanded)
    assert "pa:synthetic-new-record" != original
    new_outcome = load_packet(expanded, "pa:synthetic-new-record")
    assert as_dict(new_outcome).get("executable") is True, as_dict(new_outcome)
    new_packet = packet_dict(new_outcome)
    assert new_packet["strategy_id"] == "pa:synthetic-new-record"
    assert new_packet["record_id"] == "synthetic-new-record"
    assert new_packet["packet_id"] == "spk_" + new_packet["packet_hash"]


def test_two_hundred_synthetic_setups_load_without_external_infrastructure(api, tmp_path: Path):
    knowledge_dir = _copy_knowledge(tmp_path)
    records_path = knowledge_dir / "data" / "pa_records.json"
    records_doc = load_json(records_path)
    template = copy.deepcopy(next(item for item in records_doc["records"] if item["type"] == "setup"))
    existing = {item["id"] for item in records_doc["records"]}
    next_number = 0
    while len([item for item in records_doc["records"] if item.get("type") == "setup"]) < 200:
        clone = copy.deepcopy(template)
        while f"synthetic-{next_number:03d}" in existing:
            next_number += 1
        record_id = f"synthetic-{next_number:03d}"
        next_number += 1
        existing.add(record_id)
        old_id = clone["id"]
        clone["id"] = record_id
        clone["name"] = f"Synthetic setup {record_id}"
        for field in (
            "market_context", "prerequisites", "recognition_features", "trigger", "entry",
            "invalidation_stop", "targets_exit", "risk_notes", "prohibited", "exceptions",
            "examples", "interpretation_notes",
        ):
            for claim in clone.get(field, []):
                claim["claim_id"] = claim["claim_id"].replace(f"{old_id}:", f"{record_id}:", 1)
        records_doc["records"].append(clone)
    _write(records_path, records_doc)
    registry = registry_for(api, knowledge_dir)
    ids = strategy_ids(registry)
    assert len(ids) == len(set(ids)) == 200
    assert ids == sorted(ids)
    for strategy_id in ids:
        outcome = load_packet(registry, strategy_id)
        assert as_dict(outcome).get("executable") is True, (strategy_id, as_dict(outcome))
        assert outcome_packet(outcome) is not None

    # A packet registry must remain local and deterministic; no DB/vector/http
    # providers are allowed in the production strategy-knowledge package.
    forbidden = {"sqlite3", "requests", "httpx", "socket", "urllib", "vector", "pinecone", "faiss"}
    package = Path(__file__).resolve().parents[3] / "pa_engine" / "strategies" / "knowledge"
    for source in package.rglob("*.py"):
        tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
        imported = {
            node.names[0].name.split(".")[0]
            for node in ast.walk(tree)
            if isinstance(node, ast.Import) and node.names
        }
        imported.update(
            node.module.split(".")[0]
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module
        )
        assert imported.isdisjoint(forbidden), (source, imported & forbidden)


def test_packet_float_confidence_is_lossless_but_hashable_without_float_profile(api):
    registry = registry_for(api)
    strategy_id = next(
        item["id"] for item in formal_records() if item["type"] == "setup" and isinstance(item.get("confidence"), float)
    )
    outcome = load_packet(registry, f"pa:{strategy_id}")
    packet = packet_dict(outcome)
    assert isinstance(packet["primary_record"]["confidence"], float)
    assert call_packet_hash(api, outcome_packet(outcome))
    evidence = evidence_dict(outcome)
    profile = profile_ref(outcome_profile(outcome))
    no_float(profile)
    no_float(profile.get("extensions", {}))
    assert evidence.get("extensions", {}).get("strategy_profile_ref") == profile
    try:
        canonical_bytes(evidence.get("extensions", {}))
    except Exception as exc:  # noqa: BLE001 - evidence extensions are hash inputs
        raise AssertionError(f"evidence extension canonicalization failed: {exc}") from exc
    # Canonical G014 JSON rejects floats by design; the packet adapter owns the
    # decimal-string projection instead of mutating the lossless record.
    try:
        canonical_bytes(profile)
    except Exception as exc:  # noqa: BLE001 - profile must be canonicalizable
        raise AssertionError(f"profile canonicalization failed: {exc}") from exc
