"""Strategy profile reference and lineage binding helpers."""

from __future__ import annotations

import copy
import hashlib
from pathlib import Path
from typing import Any, Iterable, Mapping

from ...contracts.canonical import canonical_json_bytes, finalize_artifact
from .errors import ProfileBindingResult, StrategyKnowledgeError
from .snapshot import SNAPSHOT_VERSION, KnowledgeSnapshot, SnapshotIdentity, load_snapshot


PROFILE_REF_VERSION = "0.1.0"


def _profile_projection(profile_ref: Mapping[str, Any]) -> dict[str, Any]:
    projection = copy.deepcopy(dict(profile_ref))
    projection.pop("profile_ref_id", None)
    projection.pop("profile_ref_hash", None)
    return projection


def _profile_digest(profile_ref: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json_bytes(_profile_projection(profile_ref))).hexdigest()


def make_profile_ref(identity: SnapshotIdentity, packet: Mapping[str, Any], registry_version: str) -> dict[str, Any]:
    packet_hash = packet.get("packet_hash")
    packet_id = packet.get("packet_id")
    if not isinstance(packet_hash, str) or not isinstance(packet_id, str):
        raise StrategyKnowledgeError("profile_binding_mismatch", "packet must be finalized before profile binding")
    profile: dict[str, Any] = {
        "ref_version": PROFILE_REF_VERSION,
        "strategy_id": packet.get("strategy_id"),
        "record_id": packet.get("record_id"),
        "packet_version": packet.get("packet_version"),
        "packet_id": packet_id,
        "packet_hash": packet_hash,
        "registry_version": registry_version,
        "snapshot_id": identity.snapshot_id,
        "snapshot_version": identity.snapshot_version,
        "snapshot_hash": identity.snapshot_hash,
    }
    digest = _profile_digest(profile)
    profile["profile_ref_hash"] = digest
    profile["profile_ref_id"] = "spr_" + digest
    return profile


def _failure(code: str, message: str, **details: Any) -> ProfileBindingResult:
    return ProfileBindingResult(False, code, message, details)


def _artifact_profile(artifact: Mapping[str, Any]) -> Mapping[str, Any] | None:
    extensions = artifact.get("extensions")
    if isinstance(extensions, Mapping) and isinstance(extensions.get("strategy_profile_ref"), Mapping):
        return extensions["strategy_profile_ref"]
    return None


def _snapshot_lineage_check(value: Any, profile_ref: Mapping[str, Any], *, path: str = "$") -> ProfileBindingResult | None:
    """Check every snapshot id/hash/version field nested in an artifact."""

    if isinstance(value, Mapping):
        for key, child in value.items():
            key_text = str(key)
            mismatch_code = "snapshot_mismatch" if key_text in {"snapshot_version", "knowledge_snapshot_version"} else ("lineage_mismatch" if "context_refs" in path else "snapshot_mismatch")
            if key_text in {"snapshot_id", "knowledge_snapshot_id"} and child is not None and child != profile_ref.get("snapshot_id"):
                return _failure(mismatch_code, "artifact snapshot ID differs from profile", path=f"{path}.{key_text}")
            if key_text in {"snapshot_hash", "knowledge_snapshot_hash"} and child is not None and child != profile_ref.get("snapshot_hash"):
                return _failure(mismatch_code, "artifact snapshot hash differs from profile", path=f"{path}.{key_text}")
            if key_text in {"snapshot_version", "knowledge_snapshot_version"} and child is not None and child != profile_ref.get("snapshot_version"):
                return _failure(mismatch_code, "artifact snapshot version differs from profile", path=f"{path}.{key_text}")
            result = _snapshot_lineage_check(child, profile_ref, path=f"{path}.{key_text}")
            if result is not None:
                return result
    elif isinstance(value, list):
        for index, child in enumerate(value):
            result = _snapshot_lineage_check(child, profile_ref, path=f"{path}[{index}]")
            if result is not None:
                return result
    return None


def _expected_evidence_projection(snapshot: KnowledgeSnapshot, packet: Mapping[str, Any]) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    """Derive exact primary evidence items/citations from a validated packet."""

    from .packet import CLAIM_FIELDS, _citation_for

    record_id = packet.get("record_id")
    claims = packet.get("claims") if isinstance(packet.get("claims"), Mapping) else {}
    expected_citations: dict[str, dict[str, Any]] = {}
    expected_items: dict[str, dict[str, Any]] = {}
    for field in CLAIM_FIELDS:
        for claim in claims.get(field, []) if isinstance(claims.get(field), list) else []:
            if not isinstance(claim, Mapping):
                continue
            citation_ids: list[str] = []
            seen: set[tuple[str, str]] = set()
            for ref in claim.get("source_refs", []) if isinstance(claim.get("source_refs"), list) else []:
                if not isinstance(ref, Mapping):
                    continue
                ref_identity = (
                    "external:" + str(ref.get("external_url") or ref.get("url"))
                    if ref.get("citation_kind") == "external" or ref.get("external_url") or ref.get("url")
                    else "chunk:" + str(ref.get("chunk_id"))
                )
                key = (str(claim.get("claim_id")), ref_identity)
                if key in seen:
                    continue
                seen.add(key)
                citation = _citation_for(snapshot, claim, ref)
                expected_citations[citation["citation_id"]] = citation
                citation_ids.append(citation["citation_id"])
            citation_ids.sort()
            statement = claim.get("statement")
            if not isinstance(statement, str):
                continue
            from ...contracts.canonical import text_sha256, evidence_id_for

            statement_sha = text_sha256(statement)
            item = {
                "record_id": record_id,
                "field": field,
                "claim_id": claim.get("claim_id"),
                "declaration_type": claim.get("declaration_type"),
                "statement": statement,
                "statement_sha256": statement_sha,
                "rationale": claim.get("rationale"),
                "citation_ids": citation_ids,
            }
            item["evidence_id"] = evidence_id_for(snapshot.identity.snapshot_id, record_id, field, claim.get("claim_id"), claim.get("declaration_type"), statement_sha, citation_ids)
            expected_items[str(claim.get("claim_id"))] = item
    return expected_items, expected_citations


def _validate_profile_shape(profile_ref: Mapping[str, Any]) -> ProfileBindingResult:
    if not isinstance(profile_ref, Mapping):
        return _failure("profile_binding_mismatch", "profile reference must be an object")
    required = ("ref_version", "strategy_id", "record_id", "packet_version", "packet_id", "packet_hash", "registry_version", "snapshot_id", "snapshot_version", "snapshot_hash", "profile_ref_id", "profile_ref_hash")
    missing = [key for key in required if key not in profile_ref]
    if missing:
        return _failure("profile_binding_mismatch", "profile reference is missing fields", missing=missing)
    if profile_ref.get("ref_version") != PROFILE_REF_VERSION:
        return _failure("profile_binding_mismatch", "unsupported profile reference version")
    if profile_ref.get("strategy_id") != "pa:" + str(profile_ref.get("record_id")):
        return _failure("profile_binding_mismatch", "strategy and record IDs do not bind")
    digest = _profile_digest(profile_ref)
    if profile_ref.get("profile_ref_hash") != digest or profile_ref.get("profile_ref_id") != "spr_" + digest:
        return _failure("profile_binding_mismatch", "profile self identity does not match")
    if not isinstance(profile_ref.get("packet_hash"), str) or profile_ref.get("packet_id") != "spk_" + profile_ref.get("packet_hash"):
        return _failure("profile_binding_mismatch", "profile packet identity is malformed")
    if profile_ref.get("packet_version") != "0.1.0" or profile_ref.get("registry_version") != "0.1.0":
        return _failure("profile_binding_mismatch", "profile packet/registry version is unsupported")
    if not isinstance(profile_ref.get("snapshot_hash"), str) or profile_ref.get("snapshot_id") != "kbs_" + profile_ref.get("snapshot_hash"):
        return _failure("profile_binding_mismatch", "profile snapshot identity is malformed")
    if profile_ref.get("snapshot_version") != SNAPSHOT_VERSION:
        return _failure("snapshot_mismatch", "profile snapshot version is unsupported")
    return ProfileBindingResult(True)


def bind_strategy_profile_ref(artifact: Mapping[str, Any], profile_ref: Mapping[str, Any]) -> dict[str, Any]:
    """Deep-copy an artifact, attach a canonical profile extension and rehash it."""

    shape = _validate_profile_shape(profile_ref)
    if not shape.valid:
        raise StrategyKnowledgeError(shape.code, shape.message, shape.details)
    if not isinstance(artifact, Mapping):
        raise StrategyKnowledgeError("profile_binding_mismatch", "artifact must be an object")
    result = copy.deepcopy(dict(artifact))
    extensions = result.get("extensions")
    if extensions is None:
        extensions = {}
    if not isinstance(extensions, dict):
        raise StrategyKnowledgeError("profile_binding_mismatch", "artifact extensions must be an object")
    extensions["strategy_profile_ref"] = copy.deepcopy(dict(profile_ref))
    result["extensions"] = extensions
    kind = result.get("artifact_kind")
    try:
        return finalize_artifact(result, kind) if isinstance(kind, str) else result
    except Exception as exc:
        raise StrategyKnowledgeError("profile_binding_mismatch", "artifact cannot be finalized after profile binding") from exc


def validate_profile_lineage(
    profile_ref: Mapping[str, Any],
    *,
    packet: Mapping[str, Any] | None = None,
    artifacts: Iterable[Mapping[str, Any]] = (),
    snapshot: KnowledgeSnapshot | SnapshotIdentity | Mapping[str, Any] | str | Path | None = None,
) -> ProfileBindingResult:
    """Verify profile self identity and every bound packet/artifact lineage."""

    shape = _validate_profile_shape(profile_ref)
    if not shape.valid:
        return shape
    full_snapshot: KnowledgeSnapshot | None = None
    if isinstance(snapshot, KnowledgeSnapshot):
        full_snapshot = snapshot
    elif isinstance(snapshot, (str, Path)):
        try:
            full_snapshot = load_snapshot(snapshot)
        except StrategyKnowledgeError as exc:
            return _failure(exc.code, exc.message, **exc.details)
    if full_snapshot is not None:
        for key in ("snapshot_id", "snapshot_hash", "snapshot_version"):
            if profile_ref.get(key) != getattr(full_snapshot.identity, key):
                return _failure("snapshot_mismatch", "profile snapshot does not match supplied snapshot", field=key)
    if packet is not None:
        try:
            from .packet import packet_content_hash, validate_packet

            if packet.get("strategy_id") != profile_ref.get("strategy_id") or packet.get("record_id") != profile_ref.get("record_id"):
                return _failure("profile_binding_mismatch", "profile owner does not match packet owner")
            packet_hash = packet_content_hash(packet)
            if packet.get("packet_hash") != packet_hash or packet.get("packet_id") != profile_ref.get("packet_id") or packet_hash != profile_ref.get("packet_hash"):
                return _failure("profile_binding_mismatch", "profile packet identity does not match packet")
            packet_result = validate_packet(packet, snapshot=full_snapshot if full_snapshot is not None else None)
            if not packet_result.valid:
                return _failure(packet_result.code, packet_result.message, **packet_result.details)
            identity = packet.get("snapshot_identity", {})
            for key in ("snapshot_id", "snapshot_version", "snapshot_hash"):
                if identity.get(key) != profile_ref.get(key):
                    return _failure("lineage_mismatch", "profile snapshot does not match packet", field=key)
        except StrategyKnowledgeError as exc:
            return _failure(exc.code, exc.message, **exc.details)
    artifact_values = list(artifacts)
    evidence_packages: dict[str, Mapping[str, Any]] = {
        str(artifact.get("evidence_package_id")): artifact
        for artifact in artifact_values
        if isinstance(artifact, Mapping) and artifact.get("artifact_kind") == "knowledge_evidence_package" and isinstance(artifact.get("evidence_package_id"), str)
    }
    for index, artifact in enumerate(artifact_values):
        if not isinstance(artifact, Mapping):
            return _failure("lineage_mismatch", "artifact must be an object", index=index)
        bound = _artifact_profile(artifact)
        if bound is None or dict(bound) != dict(profile_ref):
            return _failure("profile_binding_mismatch", "artifact profile extension differs from requested profile", index=index)
        # G014 core and normalized model context lineage fields, wherever
        # nested, are immutable snapshot pins.
        snapshot_result = _snapshot_lineage_check(artifact, profile_ref)
        if snapshot_result is not None:
            return _failure(snapshot_result.code, snapshot_result.message, index=index, **snapshot_result.details)
        package_id = artifact.get("evidence_package_id")
        package_hash = artifact.get("evidence_package_hash")
        context_refs = artifact.get("context_refs")
        if isinstance(context_refs, Mapping) and (context_refs.get("evidence_package_id") is not None or context_refs.get("evidence_package_hash") is not None):
            context_id = context_refs.get("evidence_package_id")
            context_hash = context_refs.get("evidence_package_hash")
            package = evidence_packages.get(str(context_id))
            if package is None or package.get("evidence_package_id") != context_id or package.get("content_hash") != context_hash:
                return _failure("lineage_mismatch", "normalized model request evidence context is unresolved", index=index)
        if artifact.get("artifact_kind") != "knowledge_evidence_package" and (package_id is not None or package_hash is not None):
            package = evidence_packages.get(str(package_id))
            if package is None or package.get("evidence_package_id") != package_id or package.get("content_hash") != package_hash:
                return _failure("lineage_mismatch", "artifact evidence package lineage is unresolved", index=index)
        if artifact.get("artifact_kind") == "knowledge_evidence_package":
            if full_snapshot is not None:
                try:
                    from ...contracts.validation import validate_artifact

                    artifact_validation = validate_artifact(artifact, expected_kind="knowledge_evidence_package", knowledge_dir=full_snapshot.knowledge_dir)
                except Exception as exc:
                    return _failure("grounding_failure", "G014 evidence package validation failed", index=index, error=str(exc))
                if not artifact_validation.valid:
                    return _failure("grounding_failure", "G014 evidence package validation failed", index=index, issues=[issue.to_dict() for issue in artifact_validation.issues])
                if packet is not None:
                    try:
                        expected_items, expected_citations = _expected_evidence_projection(full_snapshot, packet)
                    except StrategyKnowledgeError as exc:
                        return _failure(exc.code, exc.message, index=index, **exc.details)
                    actual_items = {str(item.get("claim_id")): item for item in artifact.get("evidence_items", []) if isinstance(item, Mapping)}
                    actual_citations = {str(item.get("citation_id")): item for item in artifact.get("citations", []) if isinstance(item, Mapping)}
                    if actual_items != expected_items or actual_citations != expected_citations:
                        return _failure("grounding_failure", "evidence package claims or citations differ from packet grounding", index=index)
            if artifact.get("knowledge_snapshot_id") != profile_ref.get("snapshot_id") or artifact.get("knowledge_snapshot_hash") != profile_ref.get("snapshot_hash"):
                return _failure("lineage_mismatch", "evidence package snapshot differs from profile", index=index)
            items = artifact.get("evidence_items", [])
            for item_index, item in enumerate(items if isinstance(items, list) else []):
                if not isinstance(item, Mapping):
                    return _failure("lineage_mismatch", "evidence item is malformed", index=index, item_index=item_index)
                role = item.get("role", "primary")
                if role == "primary" and item.get("record_id") != profile_ref.get("record_id"):
                    return _failure("profile_binding_mismatch", "primary evidence owner differs from profile", index=index, item_index=item_index)
                if role == "supplemental" and isinstance(item.get("record_id"), str) and item.get("record_id") == profile_ref.get("record_id"):
                    return _failure("profile_binding_mismatch", "supplemental evidence cannot be the setup owner", index=index, item_index=item_index)
                if role not in {"primary", "supplemental"}:
                    return _failure("lineage_mismatch", "evidence role is unsupported", index=index, item_index=item_index)
            # All citations must be used by at least one evidence item.
            used = {cid for item in items if isinstance(item, Mapping) for cid in (item.get("citation_ids", []) if isinstance(item.get("citation_ids"), list) else [])}
            citation_ids = {item.get("citation_id") for item in artifact.get("citations", []) if isinstance(item, Mapping)}
            if used != citation_ids:
                return _failure("lineage_mismatch", "evidence package contains unused or missing citations", index=index)
    return ProfileBindingResult(True)


__all__ = ["PROFILE_REF_VERSION", "bind_strategy_profile_ref", "make_profile_ref", "validate_profile_lineage"]
