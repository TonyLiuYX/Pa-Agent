"""Second-round G016 regressions requiring complete snapshot grounding."""
from __future__ import annotations

import copy
import json
import shutil
import sys
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from conftest import (  # noqa: E402
    KNOWLEDGE,
    as_dict,
    call_packet_hash,
    call_registry_validate_packet,
    claims,
    evidence_dict,
    formal_records,
    load_json,
    load_packet,
    outcome_packet,
    outcome_profile,
    packet_dict,
    profile_ref,
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


def _rehash(packet: dict[str, Any], api: Any) -> None:
    packet["packet_hash"] = call_packet_hash(api, packet)
    packet["packet_id"] = "spk_" + packet["packet_hash"]


def _registry_profile_lineage(registry: Any, profile: dict[str, Any], packet: dict[str, Any], evidence: dict[str, Any]):
    function = getattr(registry, "validate_profile_lineage", None)
    assert callable(function), "StrategyRegistry.validate_profile_lineage public full-snapshot validator is required"
    return function(profile, packet=packet, artifacts=[evidence])


def _finalize_evidence_tamper(evidence: dict[str, Any], profile: dict[str, Any], mutation: str) -> dict[str, Any]:
    """Mutate one evidence provenance field, then regenerate all G014 IDs.

    Regenerating the inner citation/evidence IDs and the package identity is
    intentional: lineage validation must reject a hash-valid forged package,
    rather than merely noticing a stale content hash.
    """

    from pa_engine.contracts.canonical import citation_id_for, evidence_id_for, finalize_artifact, text_sha256

    package = copy.deepcopy(evidence)
    package.setdefault("extensions", {})["strategy_profile_ref"] = copy.deepcopy(profile)
    if mutation in {"citation_source_sha", "citation_page"}:
        citation = package["citations"][0]
        old_citation_id = citation["citation_id"]
        if mutation == "citation_source_sha":
            citation["source_sha256"] = "0" * 64
        else:
            invalid_page = int(citation["page_end"]) + 1000
            citation["page_start"] = invalid_page
            citation["page_end"] = invalid_page
        citation["citation_id"] = citation_id_for(citation)
        citation = finalize_artifact(citation, "knowledge_citation")
        for item in package.get("evidence_items", []):
            item["citation_ids"] = [
                citation["citation_id"] if value == old_citation_id else value
                for value in item.get("citation_ids", [])
            ]
        package["citations"][0] = citation
    else:
        item = package["evidence_items"][0]
        if mutation == "evidence_claim_id":
            item["claim_id"] = str(item["claim_id"]) + ":tampered"
        else:
            item["statement"] = str(item["statement"]) + " tampered"
            item["statement_sha256"] = text_sha256(item["statement"])
    for item in package.get("evidence_items", []):
        item["evidence_id"] = evidence_id_for(
            package["knowledge_snapshot_id"],
            item["record_id"],
            item["field"],
            item["claim_id"],
            item["declaration_type"],
            item["statement_sha256"],
            item["citation_ids"],
        )
    return finalize_artifact(package, "knowledge_evidence_package")


@pytest.mark.parametrize(
    ("mutation", "expected_code"),
    [
        ("source_text", "grounding_failure"),
        ("related_image_pages", "grounding_failure"),
        ("primary_record", "snapshot_mismatch"),
        ("visual_page", "grounding_failure"),
    ],
)
def test_hash_valid_packet_tamper_requires_complete_registry_snapshot(api, mutation: str, expected_code: str):
    registry = registry_for(api)
    strategy_id = sorted(strategy_ids(registry))[0]
    outcome = load_packet(registry, strategy_id)
    packet = packet_dict(outcome)
    if mutation == "source_text":
        excerpt = packet["chunk_excerpts"][0]
        excerpt["source_text"] = str(excerpt["source_text"]) + " tampered"
        # Keep the packet self hash valid while the excerpt diverges from the
        # immutable snapshot.  The public registry validator must catch it.
        from pa_engine.contracts.canonical import text_sha256

        excerpt["source_text_sha256"] = text_sha256(excerpt["source_text"])
    elif mutation == "related_image_pages":
        excerpt = packet["chunk_excerpts"][0]
        pages = list(excerpt.get("related_image_pages", []))
        excerpt["related_image_pages"] = pages + [9999]
    elif mutation == "primary_record":
        packet["primary_record"]["name"] = str(packet["primary_record"].get("name", "")) + " tampered"
    else:
        visual_claim = next(
            claim
            for field in CLAIM_FIELDS
            for claim in packet["claims"].get(field, [])
            if claim.get("visual_page") is not None
        )
        visual_claim["visual_page"] = 9999
    _rehash(packet, api)
    validation = call_registry_validate_packet(registry, packet)
    assert not result_valid(validation), validation
    assert as_dict(validation).get("code") == expected_code


@pytest.mark.parametrize(
    ("mutation", "expected_code"),
    [
        ("registry_version", "packet_binding_mismatch"),
        ("retrieval_algorithm", "packet_binding_mismatch"),
        ("retrieval_version", "packet_binding_mismatch"),
        ("claim_fields", "packet_binding_mismatch"),
        ("knowledge_snapshot_id", "snapshot_mismatch"),
        ("knowledge_snapshot_hash", "snapshot_mismatch"),
        ("knowledge_snapshot_version", "snapshot_mismatch"),
        ("snapshot_identity_extra", "packet_binding_mismatch"),
    ],
)
def test_hash_valid_packet_metadata_and_snapshot_alias_tamper_are_typed(
    api,
    mutation: str,
    expected_code: str,
):
    """Full registry validation rejects self-rehashed packet metadata forgeries."""

    registry = registry_for(api)
    strategy_id = sorted(strategy_ids(registry))[0]
    packet = packet_dict(load_packet(registry, strategy_id))
    valid = call_registry_validate_packet(registry, copy.deepcopy(packet))
    assert result_valid(valid), as_dict(valid)

    tampered = copy.deepcopy(packet)
    if mutation == "registry_version":
        tampered["registry_version"] = "9.9.9"
    elif mutation == "retrieval_algorithm":
        tampered["retrieval_algorithm"] = "forged-retriever"
    elif mutation == "retrieval_version":
        tampered["retrieval_version"] = "9.9.9"
    elif mutation == "claim_fields":
        tampered["claim_fields"] = list(reversed(tampered["claim_fields"]))
    elif mutation == "snapshot_identity_extra":
        tampered["snapshot_identity"]["forged_extra"] = "caller-extension"
    else:
        identity = tampered["snapshot_identity"]
        if mutation == "knowledge_snapshot_id":
            identity["knowledge_snapshot_id"] = "kbs_" + "0" * 64
        elif mutation == "knowledge_snapshot_hash":
            identity["knowledge_snapshot_hash"] = "0" * 64
        else:
            identity["knowledge_snapshot_version"] = "9.9.9"
    _rehash(tampered, api)
    rejected = call_registry_validate_packet(registry, tampered)
    assert not result_valid(rejected), rejected
    assert as_dict(rejected).get("code") == expected_code


def test_hash_valid_tamper_is_rejected_by_full_snapshot_profile_lineage(api):
    registry = registry_for(api)
    strategy_id = sorted(strategy_ids(registry))[0]
    outcome = load_packet(registry, strategy_id)
    packet = packet_dict(outcome)
    evidence = evidence_dict(outcome)
    profile = profile_ref(outcome_profile(outcome))
    packet["chunk_excerpts"][0]["source_text"] += " tampered"
    from pa_engine.contracts.canonical import text_sha256

    packet["chunk_excerpts"][0]["source_text_sha256"] = text_sha256(packet["chunk_excerpts"][0]["source_text"])
    _rehash(packet, api)
    validation = _registry_profile_lineage(registry, profile, packet, evidence)
    assert not result_valid(validation), validation
    # The profile's packet hash is checked before snapshot grounding in the
    # lineage path; a rehashed packet therefore fails its profile binding
    # first.  The registry.validate_packet direct path above still reports
    # the exact grounding_failure code for this same snapshot tamper.
    assert as_dict(validation).get("code") == "profile_binding_mismatch"


@pytest.mark.parametrize(
    "mutation",
    ["citation_source_sha", "citation_page", "evidence_claim_id", "evidence_statement"],
)
def test_hash_valid_evidence_tamper_is_rejected_by_full_snapshot_profile_lineage(api, mutation: str):
    """A re-finalized evidence forgery cannot pass profile lineage.

    This deliberately exercises evidence content, not only profile extensions:
    each mutation gets a fresh G014 package hash/ID while retaining the
    original profile reference.
    """

    registry = registry_for(api)
    strategy_id = sorted(strategy_ids(registry))[0]
    outcome = load_packet(registry, strategy_id)
    packet = packet_dict(outcome)
    evidence = evidence_dict(outcome)
    profile = profile_ref(outcome_profile(outcome))
    forged = _finalize_evidence_tamper(evidence, profile, mutation)
    assert forged["extensions"]["strategy_profile_ref"] == profile
    assert forged["content_hash"] != evidence.get("content_hash")
    assert forged["evidence_package_id"] != evidence.get("evidence_package_id")
    validation = _registry_profile_lineage(registry, profile, packet, forged)
    assert not result_valid(validation), validation
    assert as_dict(validation).get("code") == "grounding_failure"


def test_positive_supplemental_concept_and_discipline_preserve_claims_and_local_visual_lineage(api):
    records = {item["id"]: item for item in formal_records()}
    primary_id = next(item["id"] for item in records.values() if item["type"] == "setup")
    supplemental_ids = [item["id"] for item in records.values() if item["type"] in {"concept", "discipline"}]
    registration = [{"strategy_id": "pa:" + primary_id, "record_id": primary_id, "supplemental_record_ids": supplemental_ids}]
    registry = registry_for(api, KNOWLEDGE, registrations=registration)
    outcome = load_packet(registry, "pa:" + primary_id)
    assert as_dict(outcome).get("executable") is True, as_dict(outcome)
    packet = packet_dict(outcome)
    evidence = evidence_dict(outcome)
    supplemental = packet["supplemental_context"]
    assert supplemental["executable"] is False
    assert supplemental["record_ids"] == supplemental_ids
    supplemental_records = {item["id"]: item for item in supplemental["records"]}
    assert set(supplemental_records) == set(supplemental_ids)

    # Supplemental records are lossless: all twelve claim arrays, declaration
    # types, rationales, source refs and visual fields remain exact.
    for record_id in supplemental_ids:
        assert supplemental_records[record_id] == records[record_id]
        assert claims(supplemental_records[record_id])

    chunk_by_id = {item["chunk_id"]: item for item in load_json(KNOWLEDGE / "data" / "chunks.json")["chunks"]}
    supplemental_excerpts = supplemental.get("chunk_excerpts", packet.get("supplemental_chunk_excerpts", []))
    assert supplemental_excerpts, "supplemental packet must carry complete local chunk excerpts"
    excerpt_ids = {item["chunk_id"] for item in supplemental_excerpts}
    visual_refs = supplemental.get("visual_refs", packet.get("supplemental_visual_refs", []))
    visual_ref_ids = {item.get("chunk_id") for item in visual_refs if isinstance(item, dict)}
    for record_id in supplemental_ids:
        for field, claim in claims(records[record_id]):
            for ref in claim.get("source_refs", []):
                assert ref.get("citation_kind") != "external"
                assert ref["chunk_id"] in excerpt_ids
                assert next(item for item in supplemental_excerpts if item["chunk_id"] == ref["chunk_id"]) == chunk_by_id[ref["chunk_id"]]
            if claim.get("visual_page") is not None:
                assert claim["visual_page"] in {
                    page for ref in claim.get("source_refs", []) for page in range(ref["page_start"], ref["page_end"] + 1)
                }
                assert any(ref.get("chunk_id") in visual_ref_ids for ref in claim.get("source_refs", []))

    # Supplemental records provide context only; the G014 evidence package is
    # still executable-primary-only.
    assert {item["record_id"] for item in evidence["evidence_items"]} == {primary_id}
    assert all(item["record_id"] == primary_id for item in evidence["evidence_items"])
    assert all(item["claim_id"].startswith(primary_id + ":") for item in evidence["evidence_items"])


@pytest.mark.parametrize(
    ("mutation", "expected_code"),
    [
        ("unknown_chunk", "grounding_failure"),
        ("wrong_source_sha", "grounding_failure"),
        ("declaration_relabel", "claim_declaration_mismatch"),
    ],
)
def test_supplemental_claim_grounding_tamper_is_typed_nonexec(
    api,
    tmp_path: Path,
    mutation: str,
    expected_code: str,
):
    """Supplemental context remains snapshot-grounded and non-executable.

    These mutations deliberately keep the setup registration valid.  A
    supplemental record with an unresolved chunk, a wrong source digest, or
    a declaration relabel must fail while loading the primary packet; it must
    never silently become executable context.
    """

    destination = tmp_path / mutation
    shutil.copytree(KNOWLEDGE, destination)
    records_path = destination / "data" / "pa_records.json"
    records_doc = load_json(records_path)
    records = {item["id"]: item for item in records_doc["records"]}
    primary_id = next(item["id"] for item in records.values() if item["type"] == "setup")
    supplemental = next(item for item in records.values() if item["type"] == "concept")
    target = next(
        claim
        for field in CLAIM_FIELDS
        for claim in supplemental.get(field, [])
        if claim.get("source_refs")
    )
    if mutation == "unknown_chunk":
        target["source_refs"][0]["chunk_id"] = "does-not-exist:chunk"
    elif mutation == "wrong_source_sha":
        target["source_refs"][0]["source_sha256"] = "0" * 64
    else:
        assert target["declaration_type"] == "source_assertion"
        target["declaration_type"] = "derived_interpretation"
        target.pop("rationale", None)
    records_path.write_text(json.dumps(records_doc, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    registration = [{
        "strategy_id": "pa:" + primary_id,
        "record_id": primary_id,
        "supplemental_record_ids": [supplemental["id"]],
    }]
    try:
        registry = registry_for(api, destination, registrations=registration)
    except Exception as exc:  # noqa: BLE001 - typed public rejection is allowed
        assert isinstance(exc, api.StrategyKnowledgeError), repr(exc)
        assert exc.code == expected_code
        return
    outcome = registry.load_packet("pa:" + primary_id)
    plain = as_dict(outcome)
    assert plain.get("executable") is False, plain
    assert plain.get("code") == expected_code, plain


def test_all_formal_visual_claim_fields_are_preserved_exactly_across_setup_packets(api):
    records = {item["id"]: item for item in formal_records()}
    registry = registry_for(api)
    visual_total = 0
    visual_setup_total = 0
    for record in records.values():
        visual_total += sum(
            1
            for field, claim in claims(record)
            if any(key in claim for key in ("visual_page", "visual_block", "visual_locator"))
        )
    assert visual_total == 137
    for strategy_id in strategy_ids(registry):
        record_id = strategy_id.removeprefix("pa:")
        packet = packet_dict(load_packet(registry, strategy_id))
        packet_claims = {
            claim["claim_id"]: claim
            for field in CLAIM_FIELDS
            for claim in packet["claims"].get(field, [])
        }
        for field, source_claim in claims(records[record_id]):
            if not any(key in source_claim for key in ("visual_page", "visual_block", "visual_locator")):
                continue
            visual_setup_total += 1
            packet_claim = packet_claims[source_claim["claim_id"]]
            for key in ("visual_page", "visual_block", "visual_locator"):
                if key in source_claim:
                    assert packet_claim.get(key) == source_claim[key]
            refs = packet_claim.get("visual_refs")
            assert isinstance(refs, list) and refs, packet_claim
            # Every generated visual ref keeps the claim's page and remains
            # attached to one of its exact local source chunks.
            source_ref_chunks = {ref["chunk_id"] for ref in source_claim.get("source_refs", [])}
            visual_ref_chunks = {ref.get("chunk_id") for ref in refs}
            assert visual_ref_chunks <= source_ref_chunks
            assert all(ref.get("visual_page") == source_claim.get("visual_page") for ref in refs)
            source_ref_by_chunk = {ref["chunk_id"]: ref for ref in source_claim.get("source_refs", [])}
            for ref in refs:
                visual_page = ref.get("visual_page")
                if visual_page is None:
                    continue
                assert visual_page in (ref.get("pages") or [])
                source_ref = source_ref_by_chunk[ref["chunk_id"]]
                assert source_ref["page_start"] <= visual_page <= source_ref["page_end"]
            if source_claim.get("visual_block") is not None:
                assert any(
                    ref.get("visual_block") == source_claim["visual_block"]
                    or source_claim["visual_block"] in (ref.get("visual_blocks") or [])
                    for ref in refs
                )
            if source_claim.get("visual_locator") is not None:
                assert any(ref.get("visual_locator") == source_claim["visual_locator"] for ref in refs)
    assert visual_setup_total == 118

    # Regression lock: the page-4 annotation for this two-page example must
    # not be attached to the page-3 source chunk.
    special_packet = packet_dict(load_packet(registry, "pa:nanqiao-breakout-failure-breakout"))
    special_claim = next(
        claim
        for claim in special_packet["claims"]["examples"]
        if claim["claim_id"] == "nanqiao-breakout-failure-breakout:examples:2"
    )
    assert all(ref["chunk_id"] != "nanqiao-checklist:page-03-semantic" for ref in special_claim["visual_refs"])
    assert any(
        ref["chunk_id"] == "nanqiao-checklist:page-04-semantic"
        and ref.get("visual_page") == 4
        and 4 in (ref.get("pages") or [])
        for ref in special_claim["visual_refs"]
    )


@pytest.mark.parametrize("supplemental_mutation, expected_code", [("setup", "invalid_registration"), ("inactive", "record_inactive"), ("pending", "record_pending")])
def test_supplemental_setup_or_lifecycle_tamper_is_typed_rejected(api, tmp_path: Path, supplemental_mutation: str, expected_code: str):
    records = {item["id"]: item for item in formal_records()}
    primary_id = next(item["id"] for item in records.values() if item["type"] == "setup")
    if supplemental_mutation == "setup":
        supplemental_id = next(item["id"] for item in records.values() if item["type"] == "setup" and item["id"] != primary_id)
        try:
            registry = registry_for(
                api,
                KNOWLEDGE,
                registrations=[{"strategy_id": "pa:" + primary_id, "record_id": primary_id, "supplemental_record_ids": [supplemental_id]}],
            )
        except Exception as exc:  # noqa: BLE001 - typed public rejection is allowed
            assert isinstance(exc, api.StrategyKnowledgeError)
            assert exc.code == expected_code
            return
        outcome = registry.load_packet("pa:" + primary_id)
        assert as_dict(outcome).get("code") == expected_code
        return

    destination = tmp_path / supplemental_mutation
    shutil.copytree(KNOWLEDGE, destination)
    records_path = destination / "data" / "pa_records.json"
    records_doc = load_json(records_path)
    supplemental = next(item for item in records_doc["records"] if item["type"] == "concept")
    supplemental_id = supplemental["id"]
    supplemental["active"] = False
    if supplemental_mutation == "inactive":
        supplemental["record_status"] = "invalidated"
    else:
        supplemental["record_status"] = "invalidated"
        supplemental["source_fidelity"] = "pending_source_reverification"
        supplemental["reverification_required"] = True
    records_path.write_text(json.dumps(records_doc, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    registry = registry_for(
        api,
        destination,
        registrations=[{"strategy_id": "pa:" + primary_id, "record_id": primary_id, "supplemental_record_ids": [supplemental_id]}],
    )
    outcome = registry.load_packet("pa:" + primary_id)
    assert as_dict(outcome).get("code") == expected_code


@pytest.mark.parametrize("artifact_name", ["manifest.json", "chunks.json", "pa_records.json"])
@pytest.mark.parametrize("version_field", ["schema_version", "knowledge_base_version"])
def test_manifest_chunk_record_version_mismatch_is_snapshot_mismatch(api, tmp_path: Path, artifact_name: str, version_field: str):
    destination = tmp_path / f"{artifact_name}-{version_field}"
    shutil.copytree(KNOWLEDGE, destination)
    path = destination / "data" / artifact_name
    value = load_json(path)
    value[version_field] = "9.9.9"
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    try:
        registry_for(api, destination)
    except Exception as exc:  # noqa: BLE001 - contract requires typed outcome
        assert isinstance(exc, api.StrategyKnowledgeError), repr(exc)
        assert exc.code == "snapshot_mismatch"
    else:
        pytest.fail(f"{artifact_name} {version_field} mismatch was accepted")


def test_duplicate_manifest_source_id_is_snapshot_invalid(api, tmp_path: Path):
    destination = tmp_path / "duplicate-source-id"
    shutil.copytree(KNOWLEDGE, destination)
    path = destination / "data" / "manifest.json"
    manifest = load_json(path)
    manifest["documents"].append(copy.deepcopy(manifest["documents"][0]))
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    with pytest.raises(api.StrategyKnowledgeError) as caught:
        registry_for(api, destination)
    assert caught.value.code == "snapshot_invalid"


@pytest.mark.parametrize("mutation", ["visual_page", "visual_ref_page"])
def test_invalid_visual_page_or_source_ref_range_is_grounding_failure(api, tmp_path: Path, mutation: str):
    destination = tmp_path / mutation
    shutil.copytree(KNOWLEDGE, destination)
    records_path = destination / "data" / "pa_records.json"
    records_doc = load_json(records_path)
    record = next(item for item in records_doc["records"] if item["type"] == "setup")
    claim = next(
        claim
        for field in CLAIM_FIELDS
        for claim in record.get(field, [])
        if claim.get("visual_page") is not None and claim.get("source_refs")
    )
    if mutation == "visual_page":
        claim["visual_page"] = 9999
    else:
        claim["source_refs"][0]["page_start"] = 9999
        claim["source_refs"][0]["page_end"] = 9999
    records_path.write_text(json.dumps(records_doc, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    primary_id = record["id"]
    registry = registry_for(
        api,
        destination,
        registrations=[{"strategy_id": "pa:" + primary_id, "record_id": primary_id}],
    )
    outcome = registry.load_packet("pa:" + primary_id)
    assert as_dict(outcome).get("code") == "grounding_failure", as_dict(outcome)
