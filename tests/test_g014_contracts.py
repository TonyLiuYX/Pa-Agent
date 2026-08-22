"""Machine-checked contract fixtures and canonical/runtime acceptance for G014."""
from __future__ import annotations

import ast
import copy
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from pa_engine.contracts.canonical import (
    CanonicalizationError,
    canonical_json_bytes,
    content_hash,
    event_idempotency_key,
    bar_id_for,
    observation_id_for,
    finalize_artifact,
    text_sha256,
)
from pa_engine.contracts.registry import INDEX, artifact_kinds, schema_for
from pa_engine.contracts.validation import REQUIRED_POLICY_OUTCOMES, validate_artifact

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "pa_engine" / "contracts" / "fixtures"
INDEX_PATH = FIXTURES / "fixture-index.json"
KNOWLEDGE = ROOT / "knowledge"


def _manifest() -> dict[str, Any]:
    return json.loads(INDEX_PATH.read_text(encoding="utf-8"))


def _doc(rel: str) -> dict[str, Any]:
    value = json.loads((FIXTURES / rel).read_text(encoding="utf-8"))
    assert isinstance(value, dict), rel
    return value


def _entry_related(entry: dict[str, Any]) -> dict[str, dict[str, Any]]:
    related: dict[str, dict[str, Any]] = {}
    for rel in entry.get("related_fixture_paths", []):
        value = _doc(rel)
        related[value["artifact_kind"]] = value
    return related


def _validate_entry(entry: dict[str, Any]):
    kwargs: dict[str, Any] = {
        "expected_kind": entry["artifact_kind"],
        "related_artifacts": _entry_related(entry),
    }
    if entry.get("requires_knowledge_dir"):
        kwargs["knowledge_dir"] = KNOWLEDGE
    return validate_artifact(_doc(entry["path"]), **kwargs)


def _all_fixture_paths() -> set[str]:
    return {
        p.relative_to(FIXTURES).as_posix()
        for p in FIXTURES.rglob("*.json")
        if p.name != "fixture-index.json"
    }


def _sha_tree(path: Path) -> str:
    digest = hashlib.sha256()
    for item in sorted(path.rglob("*")):
        if item.is_file() and ".git" not in item.parts:
            digest.update(item.relative_to(path).as_posix().encode())
            digest.update(item.read_bytes())
    return digest.hexdigest()


def test_registry_has_thirteen_draft_202012_artifacts_and_manifest_coverage():
    entries = _manifest()["entries"]
    assert artifact_kinds() == [entry["artifact_kind"] for entry in INDEX["artifacts"]]
    assert len(artifact_kinds()) == 13
    assert all(schema_for(kind)["$schema"] == "https://json-schema.org/draft/2020-12/schema" for kind in artifact_kinds())
    by_kind = {entry["artifact_kind"]: entry for entry in entries if entry["valid"]}
    invalid_kinds = {entry["artifact_kind"] for entry in entries if not entry["valid"]}
    assert set(by_kind) == set(artifact_kinds())
    assert invalid_kinds == set(artifact_kinds())
    assert _all_fixture_paths() == {entry["path"] for entry in entries}
    for entry in entries:
        for related in entry["related_fixture_paths"]:
            assert related in _all_fixture_paths()


def test_every_manifest_entry_is_consumed_and_validity_matches_expected_codes():
    for entry in _manifest()["entries"]:
        result = _validate_entry(entry)
        if entry["valid"]:
            assert result.valid, (entry["path"], result.to_dict())
        else:
            assert not result.valid, entry["path"]
            codes = {issue.code for issue in result.issues}
            assert codes.intersection(entry["expected_issue_codes"]), (entry["path"], codes)


def test_canonical_object_reordering_is_byte_stable_and_arrays_remain_ordered():
    source = _doc("valid/market_snapshot.json")

    def reverse_objects(value: Any) -> Any:
        if isinstance(value, dict):
            return {key: reverse_objects(value[key]) for key in reversed(list(value))}
        if isinstance(value, list):
            return [reverse_objects(item) for item in value]
        return value

    reordered = reverse_objects(source)
    assert canonical_json_bytes(source) == canonical_json_bytes(reordered)
    assert content_hash(source) == content_hash(reordered)
    swapped = copy.deepcopy(source)
    swapped["bars"] = list(reversed(swapped["bars"]))
    assert canonical_json_bytes(swapped) != canonical_json_bytes(source)


def test_only_index_declared_volatile_pointers_are_excluded_from_identity():
    source = _doc("valid/market_snapshot.json")
    metadata = next(item for item in INDEX["artifacts"] if item["artifact_kind"] == "market_snapshot")
    for pointer in metadata["canonical_hash_excluded_json_pointers"]:
        changed = copy.deepcopy(source)
        field = pointer.rsplit("/", 1)[-1]
        if field == "generated_at":
            changed[field] = "2026-08-10T15:01:00Z"
        elif field == "market_snapshot_id":
            changed[field] = "mkt_" + "f" * 64
        elif field == "content_hash":
            changed[field] = "f" * 64
        assert content_hash(changed) == content_hash(source)
    changed = copy.deepcopy(source)
    changed["bars"][0]["close"] = "100.6"
    assert content_hash(changed) != content_hash(source)


def test_canonical_rejects_float_nan_infinity_and_noncanonical_decimals():
    for value in (1.25, float("nan"), float("inf"), float("-inf")):
        with pytest.raises(CanonicalizationError):
            canonical_json_bytes({"value": value})
    market = _doc("valid/market_snapshot.json")
    for decimal in ("01", "1.", "1e2", "-0", "NaN", "Infinity"):
        changed = copy.deepcopy(market)
        changed["bars"][0]["close"] = decimal
        result = validate_artifact(changed, expected_kind="market_snapshot")
        assert any(issue.code == "invalid_decimal" for issue in result.issues), decimal


def test_mtr_claim_and_citation_use_exact_phase1_grounding_chain():
    citation = _doc("valid/knowledge_citation.json")
    evidence = _doc("valid/knowledge_evidence_package.json")
    item = evidence["evidence_items"][0]
    assert citation["source_id"] == "rose-mtr"
    assert citation["claim_id"] == "rose-mtr:market_context:1"
    assert citation["chunk_id"] == "rose-mtr:intro-core"
    assert citation["page_start"] == citation["page_end"] == 1
    assert citation["source_sha256"] == "9ade5b6c706cdeff09fba8effadffc1d0bda3c4a2004774232bae24ed094e5be"
    assert citation["chunk_text_sha256"] == "5670fc037d987c4eab886d5833fa0f1718db764627e6fd071975060f2b496372"
    assert item["record_id"] == "rose-mtr" and item["field"] == "market_context"
    assert item["claim_id"] == citation["claim_id"]
    signal = _doc("valid/pa_analysis_result_signal.json")
    assert [level["claim_ids"][0] for level in signal["levels"]] == [
        "rose-mtr:entry:2", "rose-mtr:invalidation_stop:4", "rose-mtr:targets_exit:1"
    ]
    assert all(level["evidence_ids"] and level["citation_ids"] for level in signal["levels"])
    assert validate_artifact(citation, knowledge_dir=KNOWLEDGE).valid
    assert validate_artifact(evidence, knowledge_dir=KNOWLEDGE).valid


@pytest.mark.parametrize("field,value", [
    ("claim_id", "rose-mtr:market_context:999"),
    ("chunk_id", "rose-mtr:intro-core-tampered"),
    ("page_start", 2),
    ("source_sha256", "0" * 64),
    ("chunk_text_sha256", "0" * 64),
])
def test_each_mtr_grounding_link_tamper_is_grounding_failure(field: str, value: Any):
    citation = _doc("valid/knowledge_citation.json")
    citation[field] = value
    result = validate_artifact(citation, knowledge_dir=KNOWLEDGE)
    assert not result.valid
    assert any(issue.code == "grounding_failure" for issue in result.issues)


@pytest.mark.parametrize("mutation", ["source_file", "page_range"])
def test_mtr_chunk_lineage_metadata_tamper_fails_citation_and_package(tmp_path: Path, mutation: str):
    """Chunk metadata is immutable lineage, not an unchecked display field."""

    copied_data = tmp_path / "knowledge-data"
    shutil.copytree(KNOWLEDGE / "data", copied_data)
    chunks_path = copied_data / "chunks.json"
    chunks = json.loads(chunks_path.read_text(encoding="utf-8"))
    target = next(chunk for chunk in chunks["chunks"] if chunk.get("chunk_id") == "rose-mtr:intro-core")
    if mutation == "source_file":
        target["source_file"] = "tampered-mtr-source.pdf"
    else:
        target["page_range"] = [2, 2]
    chunks_path.write_text(json.dumps(chunks, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    citation = _doc("valid/knowledge_citation.json")
    package = _doc("valid/knowledge_evidence_package.json")
    for artifact in (citation, package):
        result = validate_artifact(artifact, knowledge_dir=copied_data)
        assert not result.valid, (mutation, result.to_dict())
        assert any(issue.code == "grounding_failure" for issue in result.issues), (mutation, result.to_dict())

    # The test only mutates the temporary copy; the formal Phase 1 snapshot remains valid.
    assert validate_artifact(citation, knowledge_dir=KNOWLEDGE).valid
    assert validate_artifact(package, knowledge_dir=KNOWLEDGE).valid


def test_record_level_citation_substitution_does_not_count_as_claim_grounding():
    citation = _doc("valid/knowledge_citation.json")
    citation["claim_id"] = "rose-mtr:entry:2"
    result = validate_artifact(citation, knowledge_dir=KNOWLEDGE)
    assert not result.valid
    assert any(issue.code == "grounding_failure" for issue in result.issues)


def test_result_status_variants_and_invariants_are_frozen():
    statuses = {"signal", "conditional", "no_trade", "abstain", "error"}
    seen = set()
    for entry in _manifest()["entries"]:
        if entry["valid"] and entry["artifact_kind"] == "pa_analysis_result":
            result = _validate_entry(entry)
            assert result.valid, result.to_dict()
            seen.add(_doc(entry["path"])["status"])
    assert seen == statuses
    signal = _doc("valid/pa_analysis_result_signal.json")
    bad = copy.deepcopy(signal)
    bad["action"] = "none"
    assert any(i.code == "status_invariant" for i in validate_artifact(bad, related_artifacts=_entry_related(next(e for e in _manifest()["entries"] if e["path"] == "valid/pa_analysis_result_signal.json"))).issues)
    abstain = _doc("valid/pa_analysis_result_abstain.json")
    bad = copy.deepcopy(abstain)
    bad["levels"] = [{"level_kind": "entry"}]
    assert not validate_artifact(bad).valid


def test_pre_provider_error_and_evidence_early_exit_are_explicit_no_call_branches():
    pre_request = _doc("valid/analysis_request_pre_provider_invalid_market.json")
    pre_error = _doc("valid/pa_analysis_result_error.json")
    pre_event = _doc("valid/analysis_run_decision_event_pre_provider_error.json")
    abstain = _doc("valid/pa_analysis_result_abstain.json")

    assert validate_artifact(pre_error, related_artifacts={"analysis_request": pre_request}).valid
    assert pre_error["status"] == "error"
    assert pre_error["errors"][0]["code"] == "invalid_market_data"
    for field in ("market_snapshot_id", "market_snapshot_hash", "observation_frame_id", "observation_frame_hash", "evidence_package_id", "evidence_package_hash", "model_request_id", "model_request_hash", "model_response_id", "model_response_hash", "model_call_id", "model_call_hash"):
        assert pre_error[field] is None

    assert validate_artifact(pre_event, related_artifacts={"analysis_request": pre_request, "pa_analysis_result": pre_error}).valid
    for field in ("market_snapshot_id", "market_snapshot_hash", "observation_frame_id", "observation_frame_hash", "evidence_package_id", "evidence_package_hash", "model_response_id", "model_response_hash"):
        assert pre_event["lineage"][field] is None
    assert pre_event["lineage"]["market_quality"]["status"] == "invalid"
    assert pre_event["lineage"]["market_quality"]["codes"] == ["invalid_market_data"]

    abstain_entry = next(e for e in _manifest()["entries"] if e["path"] == "valid/pa_analysis_result_abstain.json")
    abstain_related = _entry_related(abstain_entry)
    assert set(abstain_related) == {"analysis_request", "market_snapshot", "market_observation_frame", "knowledge_evidence_package"}
    assert validate_artifact(abstain, related_artifacts=abstain_related, knowledge_dir=KNOWLEDGE).valid
    for field in ("model_request_id", "model_request_hash", "model_response_id", "model_response_hash", "model_call_id", "model_call_hash"):
        assert abstain[field] is None
    assert abstain["errors"][0]["code"] == "no_actionable_evidence"


@pytest.mark.parametrize("status,path", [
    ("signal", "valid/pa_analysis_result_signal.json"),
    ("conditional", "valid/pa_analysis_result_conditional.json"),
    ("no_trade", "valid/pa_analysis_result_no_trade.json"),
])
def test_actionable_statuses_require_all_upstream_lineage_and_related_artifacts(status: str, path: str):
    entry = next(e for e in _manifest()["entries"] if e["path"] == path)
    source = _doc(path)
    related = _entry_related(entry)
    assert source["status"] == status
    assert not validate_artifact(source, related_artifacts={}).valid
    for pair in (("market_snapshot_id", "market_snapshot_hash"), ("observation_frame_id", "observation_frame_hash"), ("evidence_package_id", "evidence_package_hash"), ("model_request_id", "model_request_hash"), ("model_response_id", "model_response_hash")):
        changed = copy.deepcopy(source)
        changed[pair[0]] = None
        changed[pair[1]] = None
        changed = finalize_artifact(changed, "pa_analysis_result")
        assert not validate_artifact(changed, related_artifacts=related, knowledge_dir=KNOWLEDGE).valid, pair


def test_actionable_result_requires_market_observation_and_evidence_lineage():
    signal = _doc("valid/pa_analysis_result_signal.json")
    result = validate_artifact(signal)
    assert not result.valid
    assert any(i.code == "grounding_failure" for i in result.issues)


def test_source_not_stated_cannot_be_upgraded_to_knowledge_support():
    result_doc = _doc("valid/pa_analysis_result_signal.json")
    result_doc["rationales"][0]["support_type"] = "source_not_stated"
    result = validate_artifact(result_doc, related_artifacts={"knowledge_evidence_package": _doc("valid/knowledge_evidence_package.json")})
    assert not result.valid
    assert any(i.code == "grounding_failure" for i in result.issues)


def test_source_not_stated_is_not_an_actionable_level_declaration():
    result_doc = _doc("valid/pa_analysis_result_signal.json")
    result_doc["levels"][0]["declaration_type"] = "source_not_stated"
    result = validate_artifact(result_doc)
    assert not result.valid
    assert any(issue.code in {"schema_violation", "grounding_failure"} for issue in result.issues)


def test_incomplete_final_bar_is_excluded_from_observation_frame():
    market = _doc("valid/market_snapshot.json")
    market["bars"][-1]["is_closed"] = False
    market = finalize_artifact(market, "market_snapshot")
    frame = _doc("valid/market_observation_frame.json")
    frame["market_snapshot_id"] = market["market_snapshot_id"]
    frame["market_snapshot_hash"] = market["content_hash"]
    frame["observations"] = frame["observations"][:-1]
    for observation in frame["observations"]:
        observation["observation_id"] = observation_id_for(market["market_snapshot_id"], observation["bar_id"], frame["formula_version"])
    frame = finalize_artifact(frame, "market_observation_frame")
    assert validate_artifact(frame, related_artifacts={"market_snapshot": market}).valid
    retained = _doc("valid/market_observation_frame.json")
    retained["market_snapshot_id"] = market["market_snapshot_id"]
    retained["market_snapshot_hash"] = market["content_hash"]
    retained = finalize_artifact(retained, "market_observation_frame")
    result = validate_artifact(retained, related_artifacts={"market_snapshot": market})
    assert not result.valid and any(issue.code == "related_artifact_mismatch" for issue in result.issues)


def test_policy_matrix_contains_exactly_all_eighteen_frozen_rows():
    matrix = _doc("valid/policy_outcome_matrix.json")
    assert len(matrix["cases"]) == 18
    assert [case["condition"] for case in matrix["cases"]] == list(REQUIRED_POLICY_OUTCOMES)
    for case in matrix["cases"]:
        expected = REQUIRED_POLICY_OUTCOMES[case["condition"]]
        for key in ("stage", "disposition", "expected_status", "code", "model_call_allowed", "warning_codes", "exclude_incomplete_last_bar"):
            assert case[key] == (expected["status"] if key == "expected_status" else expected[key]), (case["condition"], key)
        got_uncertainty = case["uncertainty"]["level"] if case["uncertainty"] else None
        assert got_uncertainty == expected["uncertainty"]
    pre_provider = {c["condition"]: c for c in matrix["cases"] if c["stage"] in {"market", "evidence"}}
    assert all(case["model_call_allowed"] is False for case in pre_provider.values() if case["disposition"] == "terminal")
    assert pre_provider["partial_coverage_allowed"]["warning_codes"] == ["partial_data"]
    assert pre_provider["partial_coverage_allowed"]["uncertainty"]["level"] == "high"


@pytest.mark.parametrize("kind,path,field", [
    ("market_snapshot", "valid/market_snapshot.json", "quality"),
    ("pa_analysis_result", "valid/pa_analysis_result_signal.json", "analysis_uncertainty"),
])
def test_unknown_quality_or_uncertainty_codes_fail_after_identity_regeneration(kind: str, path: str, field: str):
    changed = _doc(path)
    changed[field]["codes"] = ["unknown_contract_code"]
    changed = finalize_artifact(changed, kind)
    result = validate_artifact(changed, expected_kind=kind)
    assert not result.valid
    assert any(issue.code == "schema_violation" for issue in result.issues)


def test_unknown_quality_warning_code_fails_after_identity_regeneration():
    changed = _doc("valid/market_snapshot.json")
    changed["quality"]["warnings"] = ["unknown_contract_warning"]
    changed = finalize_artifact(changed, "market_snapshot")
    result = validate_artifact(changed, expected_kind="market_snapshot")
    assert not result.valid
    assert any(issue.code == "schema_violation" for issue in result.issues)


def test_fixed_offset_and_non_iana_session_timezones_fail():
    for timezone_name in ("UTC+05:30", "Etc/GMT+5"):
        changed = _doc("valid/market_snapshot.json")
        changed["session"]["timezone"] = timezone_name
        changed = finalize_artifact(changed, "market_snapshot")
        result = validate_artifact(changed, expected_kind="market_snapshot")
        assert not result.valid
        assert any(issue.code in {"invalid_timezone", "schema_violation"} for issue in result.issues)


def test_mixed_local_external_source_assertion_is_precisely_grounding_failure():
    citation = _doc("invalid/knowledge_citation_mixed_source_assertion.json")
    result = validate_artifact(citation, knowledge_dir=KNOWLEDGE)
    assert not result.valid
    assert any(issue.code == "grounding_failure" for issue in result.issues)


def test_event_idempotency_excludes_recording_time_but_binds_mode_and_lineage():
    event = _doc("valid/analysis_run_decision_event.json")
    original = event_idempotency_key(event)
    stored_key_tampered = copy.deepcopy(event)
    stored_key_tampered["idempotency_key"] = "idem_" + "0" * 64
    tampered_result = validate_artifact(stored_key_tampered, expected_kind="analysis_run_decision_event")
    assert not tampered_result.valid
    assert any(issue.code == "status_invariant" for issue in tampered_result.issues)
    recorded_later = copy.deepcopy(event)
    recorded_later["recorded_at"] = "2026-08-10T16:00:00Z"
    assert event_idempotency_key(recorded_later) == original
    mode_changed = copy.deepcopy(event)
    mode_changed["mode"] = "forward_paper"
    mode_changed["stream"] = "forward_paper"
    assert event_idempotency_key(mode_changed) != original
    lineage_changed = copy.deepcopy(event)
    lineage_changed["lineage"]["engine_version"] = "other-engine"
    assert event_idempotency_key(lineage_changed) != original


@pytest.mark.parametrize("pair_prefix", ["market_snapshot", "observation_frame", "evidence_package", "model_response"])
def test_decision_event_lineage_pairs_cannot_be_half_null(pair_prefix: str):
    entry = next(e for e in _manifest()["entries"] if e["path"] == "valid/analysis_run_decision_event.json")
    event = _doc(entry["path"])
    event["lineage"][f"{pair_prefix}_id"] = None
    event["idempotency_key"] = event_idempotency_key(event)
    event = finalize_artifact(event, "analysis_run_decision_event")
    result = validate_artifact(
        event,
        expected_kind="analysis_run_decision_event",
        related_artifacts=_entry_related(entry),
    )
    assert not result.valid
    assert any(issue.code in {"schema_violation", "grounding_failure"} for issue in result.issues)


def test_decision_event_status_action_must_match_linked_signal_result():
    decision_entry = next(e for e in _manifest()["entries"] if e["path"] == "valid/analysis_run_decision_event.json")
    signal_entry = next(e for e in _manifest()["entries"] if e["path"] == "valid/pa_analysis_result_signal.json")
    event = _doc(decision_entry["path"])
    event["final_status"] = "error"
    event["action"] = "none"
    event["idempotency_key"] = event_idempotency_key(event)
    event = finalize_artifact(event, "analysis_run_decision_event")
    related = _entry_related(decision_entry)
    related.update(_entry_related(signal_entry))
    result = validate_artifact(event, expected_kind="analysis_run_decision_event", related_artifacts=related)
    assert not result.valid
    assert any(issue.code == "grounding_failure" for issue in result.issues)


def test_decision_event_instrument_must_match_related_market():
    entry = next(e for e in _manifest()["entries"] if e["path"] == "valid/analysis_run_decision_event.json")
    event = _doc(entry["path"])
    event["instrument"]["symbol"] = "MSFT"
    event["idempotency_key"] = event_idempotency_key(event)
    event = finalize_artifact(event, "analysis_run_decision_event")
    result = validate_artifact(
        event,
        expected_kind="analysis_run_decision_event",
        related_artifacts=_entry_related(entry),
    )
    assert not result.valid
    assert any(issue.code == "grounding_failure" for issue in result.issues)


def test_outcome_event_market_pair_must_match_related_market():
    entry = next(e for e in _manifest()["entries"] if e["path"] == "valid/analysis_run_outcome_event.json")
    outcome = _doc(entry["path"])
    related = _entry_related(entry)
    alternate_market = copy.deepcopy(related["market_snapshot"])
    alternate_market["bars"][0]["close"] = "100.25"
    alternate_market = finalize_artifact(alternate_market, "market_snapshot")
    outcome["market_snapshot_id"] = alternate_market["market_snapshot_id"]
    outcome["market_snapshot_hash"] = alternate_market["content_hash"]
    outcome["idempotency_key"] = event_idempotency_key(outcome)
    outcome = finalize_artifact(outcome, "analysis_run_outcome_event")
    result = validate_artifact(
        outcome,
        expected_kind="analysis_run_outcome_event",
        related_artifacts=related,
    )
    assert not result.valid
    assert any(issue.code == "grounding_failure" for issue in result.issues)


def test_cli_validate_hash_and_error_exit_codes_are_json(tmp_path: Path):
    env = dict(os.environ, PYTHONPATH=str(ROOT))
    valid = FIXTURES / "valid" / "market_snapshot.json"
    command = [sys.executable, "-m", "pa_engine.contracts"]
    proc = subprocess.run(command + ["validate", "--file", str(valid), "--kind", "market_snapshot"], cwd=ROOT, env=env, text=True, capture_output=True)
    assert proc.returncode == 0 and json.loads(proc.stdout)["valid"] is True
    proc = subprocess.run(command + ["hash", "--file", str(valid), "--kind", "market_snapshot"], cwd=ROOT, env=env, text=True, capture_output=True)
    assert proc.returncode == 0 and json.loads(proc.stdout)["content_hash"]
    invalid = FIXTURES / "invalid" / "market_snapshot_unknown_field.json"
    proc = subprocess.run(command + ["validate", "--file", str(invalid), "--kind", "market_snapshot"], cwd=ROOT, env=env, text=True, capture_output=True)
    assert proc.returncode == 1 and json.loads(proc.stdout)["valid"] is False
    invalid_quality = FIXTURES / "invalid" / "market_snapshot_unknown_quality_code.json"
    proc = subprocess.run(command + ["hash", "--file", str(invalid_quality), "--kind", "market_snapshot"], cwd=ROOT, env=env, text=True, capture_output=True)
    assert proc.returncode == 0 and json.loads(proc.stdout)["content_hash"]
    proc = subprocess.run(command + ["validate", "--file", str(invalid_quality), "--kind", "market_snapshot"], cwd=ROOT, env=env, text=True, capture_output=True)
    assert proc.returncode == 1 and json.loads(proc.stdout)["valid"] is False
    # malformed JSON/read errors are contract exit code 2 with JSON, never traceback
    malformed = tmp_path / "malformed.json"
    malformed.write_text("{not-json", encoding="utf-8")
    proc = subprocess.run(command + ["validate", "--file", str(malformed)], cwd=ROOT, env=env, text=True, capture_output=True)
    assert proc.returncode == 2 and json.loads(proc.stdout)["error"] == "read_or_json_error"


def test_contract_import_closure_and_fixtures_have_no_provider_network_or_credentials():
    forbidden = {"openai", "anthropic", "boto3", "requests", "httpx", "urllib", "socket", "aiohttp", "alpaca", "polygon", "yfinance"}
    contract_root = ROOT / "pa_engine" / "contracts"
    for path in contract_root.glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                assert all(alias.name.split(".")[0] not in forbidden for alias in node.names), path
            if isinstance(node, ast.ImportFrom) and node.module:
                assert node.module.split(".")[0] not in forbidden, path
    fixture_text = "\n".join(p.read_text(encoding="utf-8") for p in FIXTURES.rglob("*.json"))
    assert not re.search(r'"(?:api[_-]?key|api[_-]?token|client[_-]?secret|password|credential|authorization)"\s*:', fixture_text, re.I)


def test_phase1_paths_are_immutable_during_fixture_cli_checks(tmp_path: Path):
    protected_dirs = [ROOT / "knowledge", ROOT / "tests" / "fixtures"]
    protected_files = [ROOT / "scripts" / "kb.py"] + [
        path for path in (ROOT / "tests").glob("test_*.py") if path.name != "test_g014_contracts.py"
    ]
    before = {path: (path.read_bytes() if path.is_file() else _sha_tree(path)) for path in protected_dirs + protected_files}
    out = tmp_path / "hash.json"
    citation = FIXTURES / "valid" / "knowledge_citation.json"
    package = FIXTURES / "valid" / "knowledge_evidence_package.json"
    assert validate_artifact(json.loads(citation.read_text(encoding="utf-8")), knowledge_dir=KNOWLEDGE).valid
    assert validate_artifact(json.loads(package.read_text(encoding="utf-8")), knowledge_dir=KNOWLEDGE).valid
    for path, kind in ((citation, "knowledge_citation"), (package, "knowledge_evidence_package")):
        grounded = subprocess.run([sys.executable, "-m", "pa_engine.contracts", "validate", "--file", str(path), "--kind", kind, "--knowledge-dir", str(KNOWLEDGE)], cwd=ROOT, env=dict(os.environ, PYTHONPATH=str(ROOT)), text=True, capture_output=True)
        assert grounded.returncode == 0 and json.loads(grounded.stdout)["valid"] is True
    proc = subprocess.run([sys.executable, "-m", "pa_engine.contracts", "hash", "--file", str(FIXTURES / "valid/market_snapshot.json"), "--kind", "market_snapshot"], cwd=ROOT, env=dict(os.environ, PYTHONPATH=str(ROOT)), text=True, capture_output=True)
    out.write_text(proc.stdout, encoding="utf-8")
    after = {path: (path.read_bytes() if path.is_file() else _sha_tree(path)) for path in before}
    assert before == after


def test_index_driven_volatile_pointer_exclusions_and_business_identity():
    chosen: dict[str, str] = {}
    for entry in _manifest()["entries"]:
        if not entry["valid"] or entry["artifact_kind"] in chosen:
            continue
        chosen[entry["artifact_kind"]] = entry["path"]
    for kind, rel in chosen.items():
        source = _doc(rel)
        metadata = next(item for item in INDEX["artifacts"] if item["artifact_kind"] == kind)
        for pointer in metadata["canonical_hash_excluded_json_pointers"]:
            changed = copy.deepcopy(source)
            field = pointer.rsplit("/", 1)[-1]
            old = changed[field]
            if field.endswith("_id"):
                changed[field] = str(old).split("_", 1)[0] + "_" + "f" * 64
            elif field.endswith("hash"):
                changed[field] = "f" * 64
            elif field.endswith("_at") or field == "generated_at":
                changed[field] = "2026-08-10T15:59:59Z"
            elif field == "correlation_id":
                changed[field] = "volatile-correlation"
            elif field == "provider_request_id":
                changed[field] = "volatile-provider-request"
            elif field == "latency_ms":
                changed[field] = (old or 0) + 1
            elif field == "usage":
                changed[field] = {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3}
            else:
                pytest.fail(f"unhandled excluded pointer {kind}:{pointer}")
            assert content_hash(changed, kind) == content_hash(source, kind), (kind, pointer)
    business_fields = {
        "market_snapshot": ("bars", lambda d: d["bars"][0].update(close="101")),
        "analysis_request": ("purpose", lambda d: d.update(purpose="paper_replay")),
        "knowledge_citation": ("claim_id", lambda d: d.update(claim_id="rose-mtr:entry:2")),
        "knowledge_evidence_package": ("retrieval_version", lambda d: d.update(retrieval_version="0.1.1")),
        "normalized_model_request": ("model", lambda d: d.update(model="other-model")),
        "normalized_model_response": ("finish_reason", lambda d: d.update(finish_reason="length")),
        "model_call_metadata": ("provider", lambda d: d.update(provider="other-provider")),
        "pa_analysis_result": ("engine_version", lambda d: d.update(engine_version="other-engine")),
        "analysis_run_decision_event": ("mode", lambda d: d.update(mode="forward_paper")),
        "analysis_run_outcome_event": ("outcome_status", lambda d: d.update(outcome_status="expired")),
        "policy_outcome_matrix": ("case code", lambda d: d["cases"][0].update(code="partial_data")),
    }
    for kind, (_label, mutate) in business_fields.items():
        rel = chosen[kind]
        source = _doc(rel); changed = copy.deepcopy(source); mutate(changed)
        assert content_hash(changed, kind) != content_hash(source, kind), kind


def test_extensions_are_the_only_allowed_core_extension_namespace():
    source = _doc("valid/market_snapshot.json")
    extended = copy.deepcopy(source)
    extended["extensions"]["future_namespace"] = {"flag": True}
    extended = finalize_artifact(extended, "market_snapshot")
    assert validate_artifact(extended, expected_kind="market_snapshot").valid
    unknown = copy.deepcopy(source)
    unknown["bars"][0]["future_field"] = "not-allowed"
    result = validate_artifact(unknown, expected_kind="market_snapshot")
    assert not result.valid and any(issue.code == "unknown_field" for issue in result.issues)


def _market_related() -> dict[str, dict[str, Any]]:
    return {"market_snapshot": _doc("valid/market_snapshot.json")}


def test_market_quality_failures_cover_ohlc_duplicate_order_future_and_gap():
    baseline = _doc("valid/market_snapshot.json")
    cases = []
    duplicate = copy.deepcopy(baseline)
    duplicate["bars"][1] = copy.deepcopy(duplicate["bars"][0])
    cases.append(duplicate)
    out_of_order = copy.deepcopy(baseline)
    out_of_order["bars"][1]["start_at"] = "2026-08-10T14:30:00Z"
    cases.append(out_of_order)
    future = copy.deepcopy(baseline)
    future["bars"][2]["start_at"] = "2026-08-10T14:45:00Z"
    future["bars"][2]["end_at"] = "2026-08-10T14:50:00Z"
    cases.append(future)
    gap = copy.deepcopy(baseline)
    gap["bars"][2]["start_at"] = "2026-08-10T14:45:00Z"
    gap["bars"][2]["end_at"] = "2026-08-10T14:50:00Z"
    gap["bars"][2]["bar_id"] = bar_id_for(gap["instrument"], gap["timeframe"], gap["session"], gap["bars"][2]["start_at"], gap["bars"][2]["end_at"])
    gap["analysis_as_of"] = "2026-08-10T14:50:00Z"
    gap["coverage"]["expected"]["end_at"] = "2026-08-10T14:50:00Z"
    gap["coverage"]["actual"]["end_at"] = "2026-08-10T14:50:00Z"
    cases.append(finalize_artifact(gap, "market_snapshot"))
    ohlc = copy.deepcopy(baseline)
    ohlc["bars"][0]["low"] = "105"
    cases.append(ohlc)
    for value in cases:
        result = validate_artifact(value, expected_kind="market_snapshot")
        assert not result.valid
        assert any(issue.code == "invalid_market_data" for issue in result.issues)


def test_hash_and_id_tamper_is_rejected():
    source = _doc("valid/market_snapshot.json")
    for field in ("content_hash", "market_snapshot_id"):
        changed = copy.deepcopy(source)
        changed[field] = ("mkt_" if field.endswith("id") else "") + "0" * (61 if field.endswith("id") else 64)
        result = validate_artifact(changed, expected_kind="market_snapshot")
        assert not result.valid
        assert any(issue.code == "hash_mismatch" for issue in result.issues)


def test_incomplete_final_bar_is_allowed_only_as_final_exclusion():
    source = _doc("valid/market_snapshot.json")
    source["bars"][-1]["is_closed"] = False
    source = finalize_artifact(source, "market_snapshot")
    assert validate_artifact(source, expected_kind="market_snapshot").valid
    earlier = copy.deepcopy(source)
    earlier["bars"][0]["is_closed"] = False
    earlier = finalize_artifact(earlier, "market_snapshot")
    result = validate_artifact(earlier, expected_kind="market_snapshot")
    assert not result.valid
    assert any(issue.code == "invalid_market_data" for issue in result.issues)


def test_source_assertion_cannot_use_external_or_mixed_citation():
    citation = _doc("valid/knowledge_citation.json")
    citation["citation_kind"] = "external"
    citation.pop("source_id", None)
    citation.pop("source_file", None)
    citation.pop("source_sha256", None)
    citation.pop("chunk_id", None)
    citation.pop("chunk_text_sha256", None)
    citation.pop("page_start", None)
    citation.pop("page_end", None)
    citation.pop("pages", None)
    citation.pop("title_path", None)
    citation.pop("source_locator", None)
    citation["external_url"] = "https://example.invalid/mtr"
    result = validate_artifact(citation, knowledge_dir=KNOWLEDGE)
    assert not result.valid
    assert any(issue.code == "grounding_failure" for issue in result.issues)
    package = _doc("valid/knowledge_evidence_package.json")
    package["citations"][0] = citation
    result = validate_artifact(package, knowledge_dir=KNOWLEDGE)
    assert not result.valid
    assert any(issue.code == "grounding_failure" for issue in result.issues)


@pytest.mark.parametrize("field,prefix", [("evidence_ids", "evi_"), ("citation_ids", "cit_"), ("market_refs", "bar_")])
def test_unknown_result_references_are_grounding_failures(field: str, prefix: str):
    result_doc = _doc("valid/pa_analysis_result_signal.json")
    result_doc["rationales"][0][field] = [prefix + "0" * 64]
    entry = next(e for e in _manifest()["entries"] if e["path"] == "valid/pa_analysis_result_signal.json")
    result = validate_artifact(result_doc, related_artifacts=_entry_related(entry), knowledge_dir=KNOWLEDGE)
    assert not result.valid
    assert any(issue.code == "grounding_failure" for issue in result.issues)


def test_model_secret_and_status_malformed_cases_are_rejected():
    response = _doc("valid/normalized_model_response.json")
    response["extensions"]["api_key"] = "fixture-secret"
    assert any(issue.code == "secret_field" for issue in validate_artifact(response).issues)
    malformed = _doc("valid/normalized_model_response.json")
    malformed["status"] = "error"
    malformed["candidate"] = None
    malformed["error"] = None
    assert any(issue.code in {"status_invariant", "schema_violation"} for issue in validate_artifact(malformed).issues)


def test_event_mode_synthetic_and_outcome_lineage_invariants():
    event = _doc("valid/analysis_run_decision_event.json")
    for field, value in (("stream", "forward_paper"), ("synthetic", False)):
        changed = copy.deepcopy(event)
        changed[field] = value
        result = validate_artifact(changed)
        assert not result.valid
        assert any(issue.code == "status_invariant" for issue in result.issues)
    outcome = _doc("valid/analysis_run_outcome_event.json")
    changed = copy.deepcopy(outcome)
    changed["decision_event_hash"] = "0" * 64
    result = validate_artifact(changed, related_artifacts={"analysis_run_decision_event": _doc("valid/analysis_run_decision_event.json")})
    assert not result.valid
    assert any(issue.code == "grounding_failure" for issue in result.issues)
    changed = copy.deepcopy(outcome)
    changed["outcome_as_of"] = "2026-08-10T14:00:00Z"
    result = validate_artifact(changed, related_artifacts={"analysis_run_decision_event": _doc("valid/analysis_run_decision_event.json")})
    assert not result.valid
    assert any(issue.code == "status_invariant" for issue in result.issues)


def test_policy_missing_and_changed_rows_are_ambiguous():
    matrix = _doc("valid/policy_outcome_matrix.json")
    missing = copy.deepcopy(matrix)
    missing["cases"] = missing["cases"][:-1]
    result = validate_artifact(missing)
    assert not result.valid and any(issue.code == "policy_matrix_incomplete" for issue in result.issues)
    changed = copy.deepcopy(matrix)
    changed["cases"][0]["code"] = "partial_data"
    result = validate_artifact(changed)
    assert not result.valid and any(issue.code == "policy_matrix_ambiguous" for issue in result.issues)
