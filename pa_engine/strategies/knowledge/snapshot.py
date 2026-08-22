"""Read-only semantic snapshot construction for the accepted Phase 1 files."""

from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

from ...contracts.canonical import canonical_json_bytes
from .errors import StrategyKnowledgeError


SNAPSHOT_ALGORITHM = "g016.manifest-chunks-records.v1"
SNAPSHOT_VERSION = "0.1.0"
_MANIFEST_EXCLUDED = {"generated_at", "mode", "source_dir", "lifecycle", "changes", "change_summary"}
_DOCUMENT_EXCLUDED = {"attempted", "change_status", "lifecycle_status", "source_reverification_signal"}


def _read_json(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as fh:
            value = json.load(fh)
    except Exception as exc:
        raise StrategyKnowledgeError("snapshot_load_failure", f"cannot read {path.name}", {"path": str(path)}) from exc
    if not isinstance(value, dict):
        raise StrategyKnowledgeError("snapshot_invalid", f"{path.name} must contain an object", {"path": str(path)})
    return value


def _data_root(knowledge_dir: str | Path) -> Path:
    root = Path(knowledge_dir)
    if (root / "data").is_dir() and not (root / "manifest.json").exists():
        root = root / "data"
    return root


def _canonical_decimal(value: float) -> str:
    """Return a non-exponent canonical decimal string for a JSON float."""

    try:
        decimal = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:  # pragma: no cover - json parser only emits finite floats
        raise StrategyKnowledgeError("snapshot_invalid", "float cannot be normalized", {"value": repr(value)}) from exc
    if not decimal.is_finite():
        raise StrategyKnowledgeError("snapshot_invalid", "non-finite float in snapshot", {"value": repr(value)})
    if decimal == 0:
        return "0"
    text = format(decimal, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _normalize_floats(value: Any) -> Any:
    if isinstance(value, float):
        return _canonical_decimal(value)
    if isinstance(value, dict):
        return {key: _normalize_floats(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize_floats(item) for item in value]
    return copy.deepcopy(value)


def _semantic_projection(
    manifest: Mapping[str, Any],
    chunks: Mapping[str, Any],
    records: Mapping[str, Any],
    *,
    semantic_version: str = SNAPSHOT_VERSION,
) -> dict[str, Any]:
    manifest_projection = {
        key: copy.deepcopy(value)
        for key, value in manifest.items()
        if key not in _MANIFEST_EXCLUDED
    }
    documents = manifest_projection.get("documents")
    if not isinstance(documents, list):
        raise StrategyKnowledgeError("snapshot_invalid", "manifest.documents must be a list")
    cleaned_documents: list[dict[str, Any]] = []
    for item in documents:
        if not isinstance(item, Mapping):
            raise StrategyKnowledgeError("snapshot_invalid", "manifest document must be an object")
        cleaned_documents.append({key: copy.deepcopy(value) for key, value in item.items() if key not in _DOCUMENT_EXCLUDED})
    cleaned_documents.sort(key=lambda item: str(item.get("source_id", "")))
    manifest_projection["documents"] = cleaned_documents

    chunk_values = chunks.get("chunks")
    record_values = records.get("records")
    if not isinstance(chunk_values, list) or not isinstance(record_values, list):
        raise StrategyKnowledgeError("snapshot_invalid", "chunks.chunks and pa_records.records must be lists")
    chunks_projection = {key: copy.deepcopy(value) for key, value in chunks.items() if key != "chunks"}
    records_projection = {key: copy.deepcopy(value) for key, value in records.items() if key != "records"}
    chunks_projection["chunks"] = sorted((copy.deepcopy(item) for item in chunk_values), key=lambda item: str(item.get("chunk_id", "")) if isinstance(item, Mapping) else "")
    records_projection["records"] = sorted((copy.deepcopy(item) for item in record_values), key=lambda item: str(item.get("id", "")) if isinstance(item, Mapping) else "")
    return {
        "algorithm": SNAPSHOT_ALGORITHM,
        "version": semantic_version,
        "manifest": manifest_projection,
        "chunks": chunks_projection,
        "records": records_projection,
    }


@dataclass(frozen=True)
class SnapshotIdentity:
    """Stable identity of one semantic manifest/chunk/record projection."""

    algorithm: str
    version: str
    snapshot_hash: str
    snapshot_id: str
    snapshot_version: str

    @property
    def knowledge_snapshot_id(self) -> str:
        return self.snapshot_id

    @property
    def knowledge_snapshot_hash(self) -> str:
        return self.snapshot_hash

    @property
    def knowledge_snapshot_version(self) -> str:
        return self.snapshot_version

    def to_dict(self) -> dict[str, str]:
        return {
            "algorithm": self.algorithm,
            "version": self.version,
            "snapshot_hash": self.snapshot_hash,
            "snapshot_id": self.snapshot_id,
            "snapshot_version": self.snapshot_version,
            "knowledge_snapshot_id": self.snapshot_id,
            "knowledge_snapshot_hash": self.snapshot_hash,
            "knowledge_snapshot_version": self.snapshot_version,
        }

    def __getitem__(self, key: str) -> str:
        return self.to_dict()[key]


@dataclass
class KnowledgeSnapshot:
    identity: SnapshotIdentity
    projection: dict[str, Any]
    manifest: dict[str, Any]
    chunks: dict[str, dict[str, Any]]
    records: dict[str, dict[str, Any]]
    knowledge_dir: Path | None = None

    def clone_projection(self) -> dict[str, Any]:
        return copy.deepcopy(self.projection)


def load_snapshot(knowledge_dir: str | Path) -> KnowledgeSnapshot:
    root = _data_root(knowledge_dir)
    manifest = _read_json(root / "manifest.json")
    chunks_doc = _read_json(root / "chunks.json")
    records_doc = _read_json(root / "pa_records.json")

    # The three accepted Phase 1 files form one semantic versioned input.  Do
    # not hash a partially upgraded mixture: every top-level schema and
    # knowledge-base version must be present, supported and identical.
    documents = manifest.get("documents")
    chunk_values = chunks_doc.get("chunks")
    record_values = records_doc.get("records")
    if not isinstance(documents, list) or not isinstance(chunk_values, list) or not isinstance(record_values, list):
        raise StrategyKnowledgeError(
            "snapshot_invalid",
            "manifest.documents, chunks.chunks and pa_records.records must be lists",
        )
    version_pairs: list[tuple[str, str, Any]] = []
    for name, document in (("manifest", manifest), ("chunks", chunks_doc), ("pa_records", records_doc)):
        schema_version = document.get("schema_version")
        knowledge_version = document.get("knowledge_base_version")
        if not isinstance(schema_version, str) or not isinstance(knowledge_version, str):
            raise StrategyKnowledgeError(
                "snapshot_invalid",
                f"{name} must declare schema_version and knowledge_base_version",
                {"file": name},
            )
        version_pairs.append((schema_version, knowledge_version, name))
    if any(schema != SNAPSHOT_VERSION or knowledge != SNAPSHOT_VERSION for schema, knowledge, _ in version_pairs):
        raise StrategyKnowledgeError(
            "snapshot_mismatch",
            "knowledge files use an unsupported semantic version",
            {"versions": [{"file": name, "schema_version": schema, "knowledge_base_version": knowledge} for schema, knowledge, name in version_pairs]},
        )
    if len({(schema, knowledge) for schema, knowledge, _ in version_pairs}) != 1:
        raise StrategyKnowledgeError(
            "snapshot_mismatch",
            "manifest, chunks and records versions must agree",
            {"versions": [{"file": name, "schema_version": schema, "knowledge_base_version": knowledge} for schema, knowledge, name in version_pairs]},
        )
    semantic_version = version_pairs[0][1]

    source_ids: set[str] = set()
    for document in documents:
        if not isinstance(document, Mapping) or not isinstance(document.get("source_id"), str) or not document.get("source_id"):
            raise StrategyKnowledgeError("snapshot_invalid", "manifest document has no valid source_id")
        source_id = document["source_id"]
        if source_id in source_ids:
            raise StrategyKnowledgeError("snapshot_invalid", "manifest source IDs must be unique", {"source_id": source_id})
        source_ids.add(source_id)

    # Validate stable IDs before constructing dict projections, otherwise a
    # duplicate entry could be silently overwritten by dict comprehension.
    chunk_ids: set[str] = set()
    for chunk in chunk_values:
        if not isinstance(chunk, Mapping) or not isinstance(chunk.get("chunk_id"), str) or not chunk.get("chunk_id"):
            raise StrategyKnowledgeError("snapshot_invalid", "chunk has no valid chunk_id")
        chunk_id = chunk["chunk_id"]
        if chunk_id in chunk_ids:
            raise StrategyKnowledgeError("duplicate_chunk", "chunk IDs must be unique", {"chunk_id": chunk_id})
        chunk_ids.add(chunk_id)
    record_ids: set[str] = set()
    for record in record_values:
        if not isinstance(record, Mapping) or not isinstance(record.get("id"), str) or not record.get("id"):
            raise StrategyKnowledgeError("snapshot_invalid", "record has no valid id")
        record_id = record["id"]
        if record_id in record_ids:
            raise StrategyKnowledgeError("duplicate_record", "record IDs must be unique", {"record_id": record_id})
        record_ids.add(record_id)

    projection = _semantic_projection(manifest, chunks_doc, records_doc, semantic_version=semantic_version)
    hash_projection = _normalize_floats(projection)
    digest = hashlib.sha256(canonical_json_bytes(hash_projection)).hexdigest()
    identity = SnapshotIdentity(SNAPSHOT_ALGORITHM, semantic_version, digest, "kbs_" + digest, semantic_version)
    chunk_values = projection["chunks"].get("chunks", [])
    record_values = projection["records"].get("records", [])
    chunks = {item["chunk_id"]: item for item in chunk_values if isinstance(item, Mapping) and isinstance(item.get("chunk_id"), str)}
    records = {item["id"]: item for item in record_values if isinstance(item, Mapping) and isinstance(item.get("id"), str)}
    if len(chunks) != len(chunk_values) or len(records) != len(record_values):
        raise StrategyKnowledgeError("snapshot_invalid", "chunk IDs and record IDs must be unique")
    return KnowledgeSnapshot(identity, projection, manifest, chunks, records, root)


def compute_snapshot_identity(knowledge_dir: str | Path) -> SnapshotIdentity:
    """Compute the G016 identity without reading any other knowledge files."""

    return load_snapshot(knowledge_dir).identity


__all__ = [
    "KnowledgeSnapshot",
    "SNAPSHOT_ALGORITHM",
    "SNAPSHOT_VERSION",
    "SnapshotIdentity",
    "compute_snapshot_identity",
    "load_snapshot",
]
