"""Deterministic strategy-scoped packet and evidence construction."""

from __future__ import annotations

import copy
import hashlib
from collections import OrderedDict
from typing import Any, Iterable, Mapping

from ...contracts.canonical import canonical_json_bytes, citation_id_for, evidence_id_for, finalize_artifact, text_sha256
from ...contracts.validation import validate_artifact
from .errors import ProfileBindingResult, StrategyKnowledgeError
from .snapshot import KnowledgeSnapshot, SnapshotIdentity


CLAIM_FIELDS: tuple[str, ...] = (
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
PACKET_VERSION = "0.1.0"
RETRIEVAL_ALGORITHM = "exact_strategy_packet"
RETRIEVAL_VERSION = "0.1.0"
REGISTRY_VERSION = "0.1.0"


def packet_content_hash(packet: Mapping[str, Any]) -> str:
    """Hash a packet after removing only its self identity fields."""

    if not isinstance(packet, Mapping):
        raise StrategyKnowledgeError("packet_invalid", "packet must be an object")
    projection = copy.deepcopy(dict(packet))
    projection.pop("packet_id", None)
    projection.pop("packet_hash", None)
    # The packet is allowed to retain source confidence floats, but the hash
    # projection must not depend on Python's float JSON representation.
    from .snapshot import _normalize_floats  # local import keeps helper private

    return hashlib.sha256(canonical_json_bytes(_normalize_floats(projection))).hexdigest()


def _record_claims(record: Mapping[str, Any]) -> OrderedDict[str, list[dict[str, Any]]]:
    record_id = record.get("id")
    if not isinstance(record_id, str) or not record_id:
        raise StrategyKnowledgeError("record_invalid", "record has no valid id")
    result: OrderedDict[str, list[dict[str, Any]]] = OrderedDict()
    seen_claim_ids: set[str] = set()
    for field in CLAIM_FIELDS:
        values = record.get(field)
        if not isinstance(values, list):
            raise StrategyKnowledgeError("record_invalid", f"claim field {field} must be a list", {"record_id": record_id})
        field_claims: list[dict[str, Any]] = []
        for ordinal, original in enumerate(values, 1):
            if not isinstance(original, Mapping):
                raise StrategyKnowledgeError("claim_invalid", "claim must be an object", {"record_id": record_id, "field": field, "ordinal": ordinal})
            claim = copy.deepcopy(dict(original))
            expected = f"{record_id}:{field}:{ordinal}"
            if claim.get("claim_id") in seen_claim_ids:
                raise StrategyKnowledgeError("duplicate_claim", "claim_id is duplicated across the primary record", {"record_id": record_id, "claim_id": claim.get("claim_id")})
            if claim.get("claim_id") != expected:
                raise StrategyKnowledgeError("claim_invalid", "claim_id is not canonical for its owner", {"record_id": record_id, "field": field, "ordinal": ordinal, "claim_id": claim.get("claim_id")})
            seen_claim_ids.add(claim.get("claim_id"))
            if claim.get("declaration_type") not in {"source_assertion", "derived_interpretation", "source_not_stated"}:
                raise StrategyKnowledgeError("claim_invalid", "claim declaration_type is unsupported", {"claim_id": expected})
            declaration = claim.get("declaration_type")
            rationale = claim.get("rationale")
            if declaration == "source_assertion" and rationale not in (None, ""):
                raise StrategyKnowledgeError("claim_declaration_mismatch", "source_assertion cannot carry derived/source-not-stated rationale", {"claim_id": expected})
            if declaration in {"derived_interpretation", "source_not_stated"} and (not isinstance(rationale, str) or not rationale.strip()):
                raise StrategyKnowledgeError("claim_declaration_mismatch", "derived and source_not_stated claims require rationale", {"claim_id": expected})
            refs = claim.get("source_refs")
            if not isinstance(refs, list) or not refs:
                raise StrategyKnowledgeError("grounding_failure", "every claim must declare at least one source ref", {"claim_id": expected})
            # Packet metadata is added to the copied claim, never to the source
            # record.  Ordinals are one-based to match the canonical IDs.
            claim["field"] = field
            claim["ordinal"] = ordinal
            claim["visual_refs"] = []
            field_claims.append(claim)
        result[field] = field_claims
    return result


def _source_maps(snapshot: KnowledgeSnapshot) -> tuple[dict[str, Mapping[str, Any]], dict[str, Mapping[str, Any]]]:
    docs = snapshot.manifest.get("documents", [])
    sources = {str(d.get("source_id")): d for d in docs if isinstance(d, Mapping) and isinstance(d.get("source_id"), str)}
    return sources, snapshot.chunks


def _validate_claim_declaration(claim: Mapping[str, Any], expected_id: str | None = None) -> None:
    """Validate immutable declaration semantics before any grounding work."""

    claim_id = expected_id or claim.get("claim_id")
    declaration = claim.get("declaration_type")
    if declaration not in {"source_assertion", "derived_interpretation", "source_not_stated"}:
        raise StrategyKnowledgeError("claim_declaration_mismatch", "claim declaration_type is unsupported", {"claim_id": claim_id})
    rationale = claim.get("rationale")
    if declaration == "source_assertion" and rationale not in (None, ""):
        raise StrategyKnowledgeError("claim_declaration_mismatch", "source_assertion cannot carry derived/source-not-stated rationale", {"claim_id": claim_id})
    if declaration in {"derived_interpretation", "source_not_stated"} and (not isinstance(rationale, str) or not rationale.strip()):
        raise StrategyKnowledgeError("claim_declaration_mismatch", "derived and source_not_stated claims require rationale", {"claim_id": claim_id})
    refs = claim.get("source_refs")
    if not isinstance(refs, list) or not refs:
        raise StrategyKnowledgeError("grounding_failure", "every claim must declare at least one source ref", {"claim_id": claim_id})
    if declaration == "source_assertion":
        for ref in refs:
            if not isinstance(ref, Mapping):
                raise StrategyKnowledgeError("grounding_failure", "claim source ref must be an object", {"claim_id": claim_id})
            if ref.get("citation_kind") == "external" or ref.get("external_url") or ref.get("url"):
                raise StrategyKnowledgeError("grounding_failure", "source_assertion cannot use external or mixed citation", {"claim_id": claim_id})


def _validate_claim_visual(snapshot: KnowledgeSnapshot, claim: Mapping[str, Any]) -> None:
    """Preserve and ground claim-level visual annotations exactly as declared."""

    if "visual_page" not in claim:
        return
    visual_page = claim.get("visual_page")
    if not isinstance(visual_page, int) or isinstance(visual_page, bool) or visual_page < 1:
        raise StrategyKnowledgeError("grounding_failure", "claim visual_page must be a positive page number", {"claim_id": claim.get("claim_id"), "visual_page": visual_page})
    sources, chunks = _source_maps(snapshot)
    matched = False
    for ref in claim.get("source_refs", []) if isinstance(claim.get("source_refs"), list) else []:
        if not isinstance(ref, Mapping) or ref.get("citation_kind") == "external" or ref.get("external_url") or ref.get("url"):
            continue
        chunk = chunks.get(ref.get("chunk_id"))
        source = sources.get(chunk.get("source_id")) if isinstance(chunk, Mapping) else None
        if not isinstance(chunk, Mapping) or not isinstance(source, Mapping):
            continue
        ref_start, ref_end = ref.get("page_start"), ref.get("page_end")
        page_count = source.get("page_count")
        if not isinstance(page_count, int) or isinstance(page_count, bool) or page_count < 1:
            raise StrategyKnowledgeError("grounding_failure", "manifest source page_count is invalid", {"source_id": source.get("source_id")})
        if visual_page > page_count:
            continue
        if isinstance(ref_start, int) and isinstance(ref_end, int) and ref_start <= visual_page <= ref_end:
            image_pages = source.get("image_pages") if isinstance(source.get("image_pages"), list) else []
            related_pages = chunk.get("related_image_pages") if isinstance(chunk.get("related_image_pages"), list) else []
            if visual_page in image_pages and visual_page in related_pages:
                matched = True
                break
    if not matched:
        raise StrategyKnowledgeError(
            "grounding_failure",
            "claim visual_page is not covered by a referenced page range and manifest image_pages",
            {"claim_id": claim.get("claim_id"), "visual_page": visual_page},
        )


def _ref_chunks(snapshot: KnowledgeSnapshot, record: Mapping[str, Any], claims: Mapping[str, Iterable[Mapping[str, Any]]]) -> list[dict[str, Any]]:
    sources, chunks = _source_maps(snapshot)
    refs: list[Mapping[str, Any]] = []
    if isinstance(record.get("source_refs"), list):
        refs.extend(ref for ref in record["source_refs"] if isinstance(ref, Mapping))
    for values in claims.values():
        for claim in values:
            _validate_claim_declaration(claim, claim.get("claim_id") if isinstance(claim, Mapping) else None)
            _validate_claim_visual(snapshot, claim)
            refs.extend(ref for ref in claim.get("source_refs", []) if isinstance(ref, Mapping))
    by_id: dict[str, dict[str, Any]] = {}
    for ref in refs:
        if ref.get("citation_kind") == "external" or ref.get("external_url") or ref.get("url"):
            # External refs are permitted only for non-source declarations;
            # they do not contribute a retrieval chunk excerpt.
            continue
        chunk_id = ref.get("chunk_id")
        if not isinstance(chunk_id, str) or chunk_id not in chunks:
            raise StrategyKnowledgeError("grounding_failure", "claim source ref chunk cannot be resolved", {"record_id": record.get("id"), "chunk_id": chunk_id})
        chunk = chunks[chunk_id]
        source = sources.get(chunk.get("source_id"))
        if source is None:
            raise StrategyKnowledgeError("grounding_failure", "chunk source cannot be resolved", {"chunk_id": chunk_id})
        if chunk.get("is_retrieval_chunk") is not True:
            raise StrategyKnowledgeError("grounding_failure", "claim ref points to a non-retrieval chunk", {"chunk_id": chunk_id})
        source_text = chunk.get("source_text")
        if not isinstance(source_text, str) or text_sha256(source_text) != chunk.get("source_text_sha256"):
            raise StrategyKnowledgeError("grounding_failure", "chunk text SHA does not match snapshot", {"chunk_id": chunk_id})
        if chunk.get("source_id") != source.get("source_id") or chunk.get("source_file") != source.get("source_file") or chunk.get("source_sha256") != source.get("sha256"):
            raise StrategyKnowledgeError("grounding_failure", "chunk source provenance does not match manifest", {"chunk_id": chunk_id})
        if ref.get("source_id") != chunk.get("source_id") or ref.get("source_file") != chunk.get("source_file") or ref.get("source_sha256") != chunk.get("source_sha256"):
            raise StrategyKnowledgeError("grounding_failure", "claim source ref does not match chunk provenance", {"chunk_id": chunk_id})
        for provenance_key in ("title_path", "source_locator", "provenance_status"):
            if provenance_key in ref and ref.get(provenance_key) != chunk.get(provenance_key):
                raise StrategyKnowledgeError("grounding_failure", "claim source ref provenance does not match chunk", {"chunk_id": chunk_id, "field": provenance_key})
        ref_start, ref_end = ref.get("page_start"), ref.get("page_end")
        chunk_start, chunk_end = chunk.get("page_start"), chunk.get("page_end")
        if (
            not isinstance(ref_start, int)
            or isinstance(ref_start, bool)
            or not isinstance(ref_end, int)
            or isinstance(ref_end, bool)
            or ref_start > ref_end
            or not isinstance(chunk_start, int)
            or not isinstance(chunk_end, int)
            or ref_start < chunk_start
            or ref_end > chunk_end
        ):
            raise StrategyKnowledgeError("grounding_failure", "claim source ref page range is outside chunk", {"chunk_id": chunk_id})
        source_pages = set(source.get("image_pages", []) if isinstance(source.get("image_pages"), list) else [])
        chunk_visual_pages = chunk.get("related_image_pages", []) if isinstance(chunk.get("related_image_pages"), list) else []
        page_count = source.get("page_count")
        if not isinstance(page_count, int) or isinstance(page_count, bool) or page_count < 1:
            raise StrategyKnowledgeError("grounding_failure", "manifest source page_count is invalid", {"source_id": source.get("source_id")})
        if any(
            not isinstance(page, int)
            or isinstance(page, bool)
            or page < 1
            or page > page_count
            or page < chunk.get("page_start", page)
            or page > chunk.get("page_end", page)
            or page not in source_pages
            for page in chunk_visual_pages
        ):
            raise StrategyKnowledgeError("grounding_failure", "chunk visual page is absent from manifest image_pages", {"chunk_id": chunk_id})
        if any(
            not isinstance(page, int)
            or isinstance(page, bool)
            or page < 1
            or page > page_count
            for page in source_pages
        ):
            raise StrategyKnowledgeError("grounding_failure", "manifest image_pages contains an invalid page", {"source_id": source.get("source_id")})
        by_id[chunk_id] = copy.deepcopy(chunk)
    source_scope = record.get("source_scope")
    if isinstance(source_scope, Mapping):
        visual_pages = source_scope.get("visual_pages", [])
        if isinstance(visual_pages, list):
            for source_id in {str(ref.get("source_id")) for ref in refs if ref.get("source_id") is not None}:
                source = sources.get(source_id)
                image_pages = set(source.get("image_pages", []) if isinstance(source, Mapping) and isinstance(source.get("image_pages"), list) else [])
                page_count = source.get("page_count") if isinstance(source, Mapping) else None
                if not isinstance(page_count, int) or isinstance(page_count, bool) or page_count < 1:
                    raise StrategyKnowledgeError("grounding_failure", "manifest source page_count is invalid", {"source_id": source_id})
                if any(
                    not isinstance(page, int)
                    or isinstance(page, bool)
                    or page < 1
                    or page > page_count
                    or page not in image_pages
                    for page in visual_pages
                ):
                    raise StrategyKnowledgeError("grounding_failure", "record visual page is absent from manifest image_pages", {"record_id": record.get("id"), "source_id": source_id})
    return [by_id[key] for key in sorted(by_id)]


def _visual_ref_for(snapshot: KnowledgeSnapshot, chunk: Mapping[str, Any]) -> dict[str, Any] | None:
    sources, _ = _source_maps(snapshot)
    source = sources.get(chunk.get("source_id"))
    if source is None:
        return None
    image_pages = set(source.get("image_pages", []) if isinstance(source.get("image_pages"), list) else [])
    pages = sorted(set(chunk.get("related_image_pages", []) if isinstance(chunk.get("related_image_pages"), list) else []) & image_pages)
    if not pages:
        return None
    return {"source_id": chunk.get("source_id"), "chunk_id": chunk.get("chunk_id"), "pages": pages}


def _attach_visual_refs(snapshot: KnowledgeSnapshot, claims: OrderedDict[str, list[dict[str, Any]]], chunks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    visual_by_chunk = {chunk.get("chunk_id"): _visual_ref_for(snapshot, chunk) for chunk in chunks}
    visual_refs_by_chunk: dict[str, dict[str, Any]] = {
        key: copy.deepcopy(value)
        for key, value in visual_by_chunk.items()
        if value is not None
    }
    for values in claims.values():
        for claim in values:
            claim_chunk_ids = sorted({ref.get("chunk_id") for ref in claim.get("source_refs", []) if isinstance(ref, Mapping)})
            claim_visual_refs: list[dict[str, Any]] = []
            for key in claim_chunk_ids:
                base = visual_by_chunk.get(key)
                # A claim-level visual page is itself an exact visual ref even
                # when a chunk's broader related_image_pages list is empty.
                if base is None and "visual_page" in claim:
                    chunk = next((item for item in chunks if item.get("chunk_id") == key), None)
                    sources, _ = _source_maps(snapshot)
                    source = sources.get(chunk.get("source_id")) if isinstance(chunk, Mapping) else None
                    if isinstance(chunk, Mapping) and isinstance(source, Mapping):
                        image_pages = source.get("image_pages") if isinstance(source.get("image_pages"), list) else []
                        if claim.get("visual_page") in image_pages:
                            base = {"source_id": chunk.get("source_id"), "chunk_id": key, "pages": [claim.get("visual_page")]}
                            visual_refs_by_chunk.setdefault(key, copy.deepcopy(base))
                if base is None:
                    continue
                if "visual_page" in claim:
                    pages = base.get("pages", []) if isinstance(base, Mapping) else []
                    if claim.get("visual_page") not in pages:
                        continue
                visual_ref = copy.deepcopy(base)
                # Keep source-declared visual annotations lossless while
                # making the derived claim reference deterministic.
                for visual_key in ("visual_page", "visual_block", "visual_blocks", "visual_locator"):
                    if visual_key in claim:
                        visual_ref[visual_key] = copy.deepcopy(claim[visual_key])
                claim_visual_refs.append(visual_ref)
            claim["visual_refs"] = claim_visual_refs
    return [visual_refs_by_chunk[key] for key in sorted(visual_refs_by_chunk)]


def _citation_for(snapshot: KnowledgeSnapshot, claim: Mapping[str, Any], ref: Mapping[str, Any]) -> dict[str, Any]:
    if ref.get("citation_kind") == "external" or ref.get("external_url") or ref.get("url"):
        if claim.get("declaration_type") == "source_assertion":
            raise StrategyKnowledgeError("grounding_failure", "source_assertion cannot use external or mixed citation", {"claim_id": claim.get("claim_id")})
        external_url = ref.get("external_url") or ref.get("url")
        if not isinstance(external_url, str) or not external_url:
            raise StrategyKnowledgeError("grounding_failure", "external citation has no URL", {"claim_id": claim.get("claim_id")})
        citation = {
            "schema_version": "0.1.0",
            "artifact_kind": "knowledge_citation",
            "extensions": {},
            "citation_kind": "external",
            "claim_id": claim.get("claim_id"),
            "declaration_type": claim.get("declaration_type"),
            "external_url": external_url,
        }
        return finalize_artifact(citation, "knowledge_citation")
    chunk = snapshot.chunks.get(ref.get("chunk_id"))
    if chunk is None:
        raise StrategyKnowledgeError("grounding_failure", "citation chunk cannot be resolved", {"chunk_id": ref.get("chunk_id")})
    page_start = ref.get("page_start", chunk.get("page_start"))
    page_end = ref.get("page_end", chunk.get("page_end"))
    citation = {
        "schema_version": "0.1.0",
        "artifact_kind": "knowledge_citation",
        "extensions": {},
        "citation_kind": "local_chunk",
        "claim_id": claim.get("claim_id"),
        "declaration_type": claim.get("declaration_type"),
        "source_id": chunk.get("source_id"),
        "source_file": chunk.get("source_file"),
        "source_sha256": chunk.get("source_sha256"),
        "chunk_id": chunk.get("chunk_id"),
        "chunk_text_sha256": chunk.get("source_text_sha256"),
        "page_start": page_start,
        "page_end": page_end,
        "pages": list(range(int(page_start), int(page_end) + 1)),
        "title_path": copy.deepcopy(chunk.get("title_path", [])),
        "source_locator": chunk.get("source_locator"),
        "provenance_status": chunk.get("provenance_status"),
    }
    return finalize_artifact(citation, "knowledge_citation")


def _evidence_package(snapshot: KnowledgeSnapshot, strategy_id: str, record: Mapping[str, Any], claims: OrderedDict[str, list[dict[str, Any]]], profile_ref: Mapping[str, Any]) -> dict[str, Any]:
    citations_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    evidence_items: list[dict[str, Any]] = []
    for field in CLAIM_FIELDS:
        for claim in claims[field]:
            cid_values: list[str] = []
            seen_ref_keys: set[tuple[str, str]] = set()
            for ref in claim.get("source_refs", []):
                if not isinstance(ref, Mapping):
                    raise StrategyKnowledgeError("grounding_failure", "source ref must be an object", {"claim_id": claim.get("claim_id")})
                ref_identity = (
                    "external:" + str(ref.get("external_url") or ref.get("url"))
                    if ref.get("citation_kind") == "external" or ref.get("external_url") or ref.get("url")
                    else "chunk:" + str(ref.get("chunk_id"))
                )
                key = (str(claim.get("claim_id")), ref_identity)
                if key in seen_ref_keys:
                    continue
                seen_ref_keys.add(key)
                citation = citations_by_key.get(key)
                if citation is None:
                    citation = _citation_for(snapshot, claim, ref)
                    citations_by_key[key] = citation
                cid_values.append(citation["citation_id"])
            cid_values.sort()
            statement = claim.get("statement")
            if not isinstance(statement, str) or not statement:
                raise StrategyKnowledgeError("claim_invalid", "claim statement must be non-empty", {"claim_id": claim.get("claim_id")})
            statement_sha = text_sha256(statement)
            item = {
                "record_id": record.get("id"),
                "field": field,
                "claim_id": claim.get("claim_id"),
                "declaration_type": claim.get("declaration_type"),
                "statement": statement,
                "statement_sha256": statement_sha,
                "rationale": claim.get("rationale"),
                "citation_ids": cid_values,
            }
            item["evidence_id"] = evidence_id_for(snapshot.identity.snapshot_id, item["record_id"], field, item["claim_id"], item["declaration_type"], statement_sha, cid_values)
            evidence_items.append(item)
    citations = sorted(citations_by_key.values(), key=lambda item: item["citation_id"])
    query_payload = {"strategy_id": strategy_id, "record_id": record.get("id"), "snapshot_hash": snapshot.identity.snapshot_hash}
    query_hash = hashlib.sha256(canonical_json_bytes(query_payload)).hexdigest()
    package = {
        "schema_version": "0.1.0",
        "artifact_kind": "knowledge_evidence_package",
        "knowledge_snapshot_id": snapshot.identity.snapshot_id,
        "knowledge_snapshot_version": snapshot.identity.snapshot_version,
        "knowledge_snapshot_hash": snapshot.identity.snapshot_hash,
        "retrieval_algorithm": RETRIEVAL_ALGORITHM,
        "retrieval_version": RETRIEVAL_VERSION,
        "query_hash": query_hash,
        "retrieval_status": "hit",
        "evidence_items": evidence_items,
        "citations": citations,
        "conflicts": [],
        "uncertainty": {"level": "low", "codes": [], "details": "exact strategy packet"},
        "extensions": {"strategy_profile_ref": copy.deepcopy(dict(profile_ref))},
    }
    package = finalize_artifact(package, "knowledge_evidence_package")
    validation = validate_artifact(package, expected_kind="knowledge_evidence_package", knowledge_dir=snapshot.knowledge_dir) if snapshot.knowledge_dir else None
    if validation is not None and not validation.valid:
        raise StrategyKnowledgeError("grounding_failure", "G014 evidence package validation failed", {"issues": [issue.to_dict() for issue in validation.issues]})
    return package


def _profile_ref(snapshot: KnowledgeSnapshot, packet: Mapping[str, Any], registry_version: str) -> dict[str, Any]:
    from .profile import make_profile_ref

    return make_profile_ref(snapshot.identity, packet, registry_version)


def _ground_record(snapshot: KnowledgeSnapshot, record: Mapping[str, Any]) -> tuple[OrderedDict[str, list[dict[str, Any]]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Build the complete deterministic claim/chunk/visual projection for a record."""

    claims = _record_claims(record)
    chunks = _ref_chunks(snapshot, record, claims)
    visual_refs = _attach_visual_refs(snapshot, claims, chunks)
    return claims, chunks, visual_refs


def build_packet(snapshot: KnowledgeSnapshot, registration: Mapping[str, Any], registry_version: str) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    record_id = registration.get("record_id")
    record = snapshot.records.get(record_id)
    if record is None:
        raise StrategyKnowledgeError("record_not_found", "registered record is absent from snapshot", {"record_id": record_id})
    if record.get("type") != "setup":
        raise StrategyKnowledgeError("non_setup_record", "only setup records can build executable packets", {"record_id": record_id, "type": record.get("type")})
    claims, chunks, visual_refs = _ground_record(snapshot, record)
    supplemental_ids = list(registration.get("supplemental_record_ids", []))
    supplemental_records: list[dict[str, Any]] = []
    supplemental_chunks_by_id: dict[str, dict[str, Any]] = {}
    supplemental_visual_by_id: dict[str, dict[str, Any]] = {}
    for supplemental_id in supplemental_ids:
        supplemental = snapshot.records.get(supplemental_id)
        if supplemental is None:
            raise StrategyKnowledgeError("record_not_found", "supplemental record is absent from snapshot", {"record_id": supplemental_id})
        if supplemental.get("type") == "setup":
            raise StrategyKnowledgeError("invalid_registration", "setup records cannot be supplemental context", {"record_id": supplemental_id})
        supplemental_claims, supplemental_chunks, supplemental_visual_refs = _ground_record(snapshot, supplemental)
        # Keep each source record lossless.  The deterministic grounded
        # projection for all supplemental records lives alongside the records
        # so consumers cannot mistake context for executable setup claims.
        supplemental_records.append(copy.deepcopy(supplemental))
        for chunk in supplemental_chunks:
            supplemental_chunks_by_id[chunk["chunk_id"]] = copy.deepcopy(chunk)
        for visual_ref in supplemental_visual_refs:
            if isinstance(visual_ref, Mapping) and isinstance(visual_ref.get("chunk_id"), str):
                supplemental_visual_by_id[visual_ref["chunk_id"]] = copy.deepcopy(visual_ref)
    packet: dict[str, Any] = {
        "packet_version": PACKET_VERSION,
        "strategy_id": registration.get("strategy_id"),
        "record_id": record_id,
        "record_type": "setup",
        "record_status": "active",
        "executable": True,
        "registry_version": registry_version,
        "snapshot_identity": snapshot.identity.to_dict(),
        "primary_record": copy.deepcopy(record),
        "claim_fields": list(CLAIM_FIELDS),
        "claims": claims,
        "chunk_excerpts": chunks,
        "visual_refs": visual_refs,
        "supplemental_context": {
            "executable": False,
            "record_ids": list(supplemental_ids),
            "records": supplemental_records,
            "chunk_excerpts": [supplemental_chunks_by_id[key] for key in sorted(supplemental_chunks_by_id)],
            "visual_refs": [supplemental_visual_by_id[key] for key in sorted(supplemental_visual_by_id)],
        },
        "retrieval_algorithm": RETRIEVAL_ALGORITHM,
        "retrieval_version": RETRIEVAL_VERSION,
    }
    digest = packet_content_hash(packet)
    packet["packet_hash"] = digest
    packet["packet_id"] = "spk_" + digest
    profile_ref = _profile_ref(snapshot, packet, registry_version)
    evidence = _evidence_package(snapshot, str(registration.get("strategy_id")), record, claims, profile_ref)
    return packet, profile_ref, evidence


def _claim_arrays_valid(
    claims: Any,
    primary: Mapping[str, Any],
    record_id: str,
    *,
    snapshot: KnowledgeSnapshot | None = None,
) -> ProfileBindingResult | None:
    if not isinstance(claims, Mapping):
        return ProfileBindingResult(False, "packet_invalid", "packet claims must be an object")
    if list(claims.keys()) != list(CLAIM_FIELDS):
        return ProfileBindingResult(False, "packet_invalid", "packet claim fields are not frozen")
    for field in CLAIM_FIELDS:
        values = claims.get(field)
        if not isinstance(values, list):
            return ProfileBindingResult(False, "packet_invalid", "packet claim array is missing", {"field": field})
        primary_values = primary.get(field)
        if not isinstance(primary_values, list) or len(values) != len(primary_values):
            return ProfileBindingResult(False, "claim_declaration_mismatch", "packet claim array differs from primary record", {"field": field})
        for ordinal, claim in enumerate(values, 1):
            if not isinstance(claim, Mapping) or claim.get("record_id", record_id) not in {record_id, None}:
                return ProfileBindingResult(False, "cross_strategy_claim", "claim owner does not match primary record", {"field": field, "ordinal": ordinal})
            expected_claim_id = f"{record_id}:{field}:{ordinal}"
            if claim.get("field") != field or claim.get("ordinal") != ordinal or claim.get("claim_id") != expected_claim_id:
                return ProfileBindingResult(False, "cross_strategy_claim", "claim owner/ordinal is not canonical", {"field": field, "ordinal": ordinal})
            source_claim = primary_values[ordinal - 1]
            if not isinstance(source_claim, Mapping):
                return ProfileBindingResult(False, "claim_declaration_mismatch", "primary record claim is malformed", {"field": field, "ordinal": ordinal})
            try:
                _validate_claim_declaration(claim, expected_claim_id)
                if snapshot is not None:
                    _validate_claim_visual(snapshot, claim)
            except StrategyKnowledgeError as exc:
                return ProfileBindingResult(False, exc.code, exc.message, exc.details)
            for source_key in ("claim_id", "statement", "declaration_type", "rationale", "source_refs"):
                if claim.get(source_key) != source_claim.get(source_key):
                    return ProfileBindingResult(False, "claim_declaration_mismatch", "packet claim declaration or source refs differ from primary record", {"field": field, "ordinal": ordinal, "field_name": source_key})
    return None


def _expected_ref_chunk_ids(record: Mapping[str, Any], claims: Mapping[str, Any]) -> set[str]:
    expected: set[str] = set()
    for ref in record.get("source_refs", []) if isinstance(record.get("source_refs"), list) else []:
        if isinstance(ref, Mapping) and isinstance(ref.get("chunk_id"), str):
            expected.add(ref["chunk_id"])
    for values in claims.values():
        for claim in values if isinstance(values, list) else []:
            for ref in claim.get("source_refs", []) if isinstance(claim, Mapping) and isinstance(claim.get("source_refs"), list) else []:
                if isinstance(ref, Mapping) and isinstance(ref.get("chunk_id"), str):
                    expected.add(ref["chunk_id"])
    return expected


def _validate_supplemental_context(
    packet: Mapping[str, Any],
    *,
    snapshot: KnowledgeSnapshot | None = None,
) -> ProfileBindingResult | None:
    supplemental = packet.get("supplemental_context")
    if not isinstance(supplemental, Mapping) or supplemental.get("executable") is not False:
        return ProfileBindingResult(False, "packet_invalid", "supplemental context must be non-executable")
    record_ids = supplemental.get("record_ids")
    records = supplemental.get("records")
    if not isinstance(record_ids, list) or any(not isinstance(item, str) for item in record_ids):
        return ProfileBindingResult(False, "packet_invalid", "supplemental record_ids must be a list of IDs")
    if len(set(record_ids)) != len(record_ids):
        return ProfileBindingResult(False, "packet_invalid", "supplemental record_ids must be unique")
    if not isinstance(records, list) or len(records) != len(record_ids):
        return ProfileBindingResult(False, "packet_invalid", "supplemental records must match record_ids")
    seen: set[str] = set()
    expected_chunks_by_id: dict[str, dict[str, Any]] = {}
    expected_visual_by_id: dict[str, dict[str, Any]] = {}
    for item in records:
        if not isinstance(item, Mapping):
            return ProfileBindingResult(False, "packet_invalid", "supplemental record must be an object")
        item_id = item.get("id", item.get("record_id"))
        if not isinstance(item_id, str) or item_id not in record_ids or item_id in seen:
            return ProfileBindingResult(False, "cross_strategy_claim", "supplemental record owner is not canonical", {"record_id": item_id})
        seen.add(item_id)
        item_type = item.get("type", item.get("record_type"))
        if item_type not in {"concept", "discipline"}:
            return ProfileBindingResult(False, "cross_strategy_claim", "supplemental context cannot contain setup records", {"record_id": item_id})
        if "executable" in item and item.get("executable") is not False:
            return ProfileBindingResult(False, "packet_invalid", "supplemental record must be non-executable", {"record_id": item_id})
        if snapshot is not None:
            expected_record = snapshot.records.get(item_id)
            if not isinstance(expected_record, Mapping):
                return ProfileBindingResult(False, "record_not_found", "supplemental record is absent from snapshot", {"record_id": item_id})
            if expected_record.get("type") not in {"concept", "discipline"}:
                return ProfileBindingResult(False, "cross_strategy_claim", "supplemental record type is not allowed", {"record_id": item_id})
            if dict(item) != dict(expected_record):
                return ProfileBindingResult(False, "grounding_failure", "supplemental record differs from snapshot", {"record_id": item_id})
            record_source = expected_record
        else:
            record_source = item
        try:
            # Validate the formal record's complete twelve arrays and preserve
            # declaration semantics without injecting packet-only metadata.
            grounded_claims, grounded_chunks, grounded_visual = _ground_record(snapshot, record_source) if snapshot is not None else (_record_claims(record_source), [], [])
        except StrategyKnowledgeError as exc:
            return ProfileBindingResult(False, exc.code, exc.message, {**exc.details, "record_id": item_id})
        for chunk in grounded_chunks:
            expected_chunks_by_id[chunk["chunk_id"]] = copy.deepcopy(chunk)
        for visual_ref in grounded_visual:
            if isinstance(visual_ref, Mapping) and isinstance(visual_ref.get("chunk_id"), str):
                expected_visual_by_id[visual_ref["chunk_id"]] = copy.deepcopy(visual_ref)
    actual_excerpts = supplemental.get("chunk_excerpts")
    actual_visual_refs = supplemental.get("visual_refs")
    if not isinstance(actual_excerpts, list) or not isinstance(actual_visual_refs, list):
        return ProfileBindingResult(False, "grounding_failure", "supplemental chunk excerpts and visual refs must be lists")
    expected_excerpts = [expected_chunks_by_id[key] for key in sorted(expected_chunks_by_id)]
    expected_visual_refs = [expected_visual_by_id[key] for key in sorted(expected_visual_by_id)]
    if snapshot is not None:
        if actual_excerpts != expected_excerpts:
            return ProfileBindingResult(False, "grounding_failure", "supplemental chunk excerpts do not match complete refs")
        if actual_visual_refs != expected_visual_refs:
            return ProfileBindingResult(False, "grounding_failure", "supplemental visual refs do not match complete refs")
    else:
        actual_ids = [entry.get("chunk_id") for entry in actual_excerpts if isinstance(entry, Mapping)]
        expected_ids = set()
        for item in records:
            expected_ids.update(_expected_ref_chunk_ids(item, {field: item.get(field, []) for field in CLAIM_FIELDS}))
        if len(actual_ids) != len(actual_excerpts) or actual_ids != sorted(set(actual_ids)) or set(actual_ids) != expected_ids:
            return ProfileBindingResult(False, "grounding_failure", "supplemental chunk excerpts do not match exact refs")
    if set(record_ids) != seen:
        return ProfileBindingResult(False, "packet_invalid", "supplemental records and IDs differ")
    return None


def validate_packet(packet: Mapping[str, Any], snapshot: KnowledgeSnapshot | SnapshotIdentity | Mapping[str, Any] | None = None) -> ProfileBindingResult:
    """Validate packet identity, immutable execution metadata and grounding."""

    if not isinstance(packet, Mapping):
        return ProfileBindingResult(False, "packet_invalid", "packet must be an object")
    strategy_id, record_id = packet.get("strategy_id"), packet.get("record_id")
    if not isinstance(strategy_id, str) or not strategy_id.startswith("pa:") or not isinstance(record_id, str) or strategy_id != "pa:" + record_id:
        return ProfileBindingResult(False, "packet_binding_mismatch", "strategy_id must bind exactly one record_id")
    if packet.get("packet_version") != PACKET_VERSION:
        return ProfileBindingResult(False, "packet_invalid", "unsupported packet version")
    if packet.get("registry_version") != REGISTRY_VERSION:
        return ProfileBindingResult(False, "packet_binding_mismatch", "packet registry version is not frozen")
    if packet.get("retrieval_algorithm") != RETRIEVAL_ALGORITHM or packet.get("retrieval_version") != RETRIEVAL_VERSION:
        return ProfileBindingResult(False, "packet_binding_mismatch", "packet retrieval metadata is not frozen")
    if packet.get("claim_fields") != list(CLAIM_FIELDS):
        return ProfileBindingResult(False, "packet_binding_mismatch", "packet claim field order is not frozen")
    # These fields are execution gates, not caller-controlled descriptive
    # metadata.  Check them before accepting a rehashed tampered packet.
    if packet.get("record_type") != "setup" or packet.get("record_status") != "active" or packet.get("executable") is not True:
        return ProfileBindingResult(False, "packet_binding_mismatch", "packet execution metadata is not the frozen active setup shape")
    try:
        expected_hash = packet_content_hash(packet)
    except StrategyKnowledgeError as exc:
        return ProfileBindingResult(False, exc.code, exc.message, exc.details)
    if packet.get("packet_hash") != expected_hash or packet.get("packet_id") != "spk_" + expected_hash:
        return ProfileBindingResult(False, "packet_hash_mismatch", "packet identity does not match its content")
    identity = packet.get("snapshot_identity")
    if not isinstance(identity, Mapping):
        return ProfileBindingResult(False, "snapshot_mismatch", "packet has no snapshot identity")
    identity_keys = {
        "algorithm",
        "version",
        "snapshot_hash",
        "snapshot_id",
        "snapshot_version",
        "knowledge_snapshot_id",
        "knowledge_snapshot_hash",
        "knowledge_snapshot_version",
    }
    identity_key_set = set(identity.keys())
    if identity_key_set - identity_keys:
        return ProfileBindingResult(False, "packet_binding_mismatch", "packet snapshot identity contains non-canonical keys")
    if identity_key_set != identity_keys:
        return ProfileBindingResult(False, "snapshot_mismatch", "packet snapshot identity shape is incomplete")
    if (
        identity.get("algorithm") != "g016.manifest-chunks-records.v1"
        or identity.get("version") != PACKET_VERSION
        or identity.get("snapshot_id") != "kbs_" + str(identity.get("snapshot_hash"))
        or identity.get("knowledge_snapshot_id") != identity.get("snapshot_id")
        or identity.get("knowledge_snapshot_hash") != identity.get("snapshot_hash")
        or identity.get("knowledge_snapshot_version") != identity.get("snapshot_version")
    ):
        return ProfileBindingResult(False, "snapshot_mismatch", "packet snapshot identity values are not canonical")
    full_snapshot = snapshot if isinstance(snapshot, KnowledgeSnapshot) else None
    if snapshot is not None:
        target = snapshot.identity if isinstance(snapshot, KnowledgeSnapshot) else snapshot
        target_dict = target.to_dict() if isinstance(target, SnapshotIdentity) else dict(target)
        for key in ("snapshot_hash", "snapshot_version", "snapshot_id"):
            if identity.get(key) != target_dict.get(key):
                return ProfileBindingResult(False, "snapshot_mismatch", "packet snapshot does not match expected snapshot", {"field": key})
    primary = packet.get("primary_record")
    if (
        not isinstance(primary, Mapping)
        or primary.get("id") != record_id
        or primary.get("type") != "setup"
        or primary.get("active") is not True
        or primary.get("record_status") != "active"
        or primary.get("reverification_required") is True
    ):
        return ProfileBindingResult(False, "packet_binding_mismatch", "primary record does not match strategy")
    claims = packet.get("claims")
    claim_result = _claim_arrays_valid(claims, primary, record_id, snapshot=full_snapshot)
    if claim_result is not None:
        return claim_result
    supplemental_result = _validate_supplemental_context(packet, snapshot=full_snapshot)
    if supplemental_result is not None:
        return supplemental_result
    expected_chunk_ids = _expected_ref_chunk_ids(primary, claims)
    excerpts = packet.get("chunk_excerpts")
    actual_chunk_ids = [item.get("chunk_id") for item in excerpts if isinstance(item, Mapping)] if isinstance(excerpts, list) else []
    if not isinstance(excerpts, list) or len(actual_chunk_ids) != len(excerpts) or len(set(actual_chunk_ids)) != len(actual_chunk_ids) or set(actual_chunk_ids) != expected_chunk_ids or actual_chunk_ids != sorted(actual_chunk_ids):
        return ProfileBindingResult(False, "grounding_failure", "packet chunk excerpts do not match exact claim/record refs")
    if not isinstance(packet.get("visual_refs"), list):
        return ProfileBindingResult(False, "grounding_failure", "packet visual_refs must be a list")
    if full_snapshot is not None:
        try:
            expected_claims, expected_chunks, expected_visual = _ground_record(full_snapshot, full_snapshot.records.get(record_id) or {})
        except StrategyKnowledgeError as exc:
            return ProfileBindingResult(False, exc.code, exc.message, exc.details)
        if primary != full_snapshot.records.get(record_id):
            return ProfileBindingResult(False, "snapshot_mismatch", "packet primary record differs from snapshot")
        if claims != expected_claims:
            return ProfileBindingResult(False, "grounding_failure", "packet claims differ from snapshot")
        if excerpts != expected_chunks:
            return ProfileBindingResult(False, "grounding_failure", "packet chunk excerpt differs from snapshot")
        if packet.get("visual_refs") != expected_visual:
            return ProfileBindingResult(False, "grounding_failure", "packet visual refs differ from snapshot")
    return ProfileBindingResult(True)


__all__ = ["CLAIM_FIELDS", "PACKET_VERSION", "build_packet", "packet_content_hash", "validate_packet"]
