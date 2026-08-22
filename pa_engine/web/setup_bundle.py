"""Deterministic validation and atomic installation of `.pa-setup.zip` v1."""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import shutil
import stat
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import jsonschema
from pypdf import PdfReader

from pa_engine.strategies.knowledge import build_registry

from .config import WebConfigError


MAX_ARCHIVE_BYTES = 50 * 1024 * 1024
MAX_EXTRACTED_BYTES = 100 * 1024 * 1024
REQUIRED_FILES = frozenset({"bundle.json", "source.pdf", "manifest.json", "chunks.json", "pa_records.json"})
BUNDLE_SCHEMA_VERSION = "pa-setup-bundle.v1"
BUNDLE_VERSION = "1.0.0"
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
_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_BUNDLE_ID = re.compile(r"^bundle-[0-9a-f]{16}$")
_SLUG = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")


BUNDLE_JSON_SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "additionalProperties": False,
    "required": ["schema_version", "bundle_version", "bundle_id", "source", "files", "setup_ids"],
    "properties": {
        "schema_version": {"const": BUNDLE_SCHEMA_VERSION},
        "bundle_version": {"const": BUNDLE_VERSION},
        "bundle_id": {"type": "string", "pattern": r"^bundle-[0-9a-f]{16}$"},
        "source": {
            "type": "object",
            "additionalProperties": False,
            "required": ["file", "sha256", "page_count"],
            "properties": {
                "file": {"const": "source.pdf"},
                "sha256": {"type": "string", "pattern": r"^[0-9a-f]{64}$"},
                "page_count": {"type": "integer", "minimum": 1, "maximum": 10000},
            },
        },
        "files": {
            "type": "object",
            "additionalProperties": False,
            "required": ["source.pdf", "manifest.json", "chunks.json", "pa_records.json"],
            "properties": {
                name: {"type": "string", "pattern": r"^[0-9a-f]{64}$"}
                for name in ("source.pdf", "manifest.json", "chunks.json", "pa_records.json")
            },
        },
        "setup_ids": {
            "type": "array",
            "minItems": 1,
            "maxItems": 100,
            "uniqueItems": True,
            "items": {"type": "string", "pattern": r"^pa:user-[0-9a-f]{8,16}-[a-z0-9]+(?:-[a-z0-9]+)*$"},
        },
    },
}


@dataclass(frozen=True, slots=True)
class InstalledBundle:
    bundle_id: str
    source_sha256: str
    page_count: int
    setup_ids: tuple[str, ...]
    registry: Any

    def public(self) -> dict[str, Any]:
        return {
            "bundle_id": self.bundle_id,
            "source_sha256_prefix": self.source_sha256[:12],
            "page_count": self.page_count,
            "setup_ids": list(self.setup_ids),
        }


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _strict_json(data: bytes, name: str) -> dict[str, Any]:
    def pairs(values):
        result: dict[str, Any] = {}
        for key, value in values:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    try:
        value = json.loads(data.decode("utf-8"), object_pairs_hook=pairs)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
        raise WebConfigError("bundle_json_invalid", f"{name} is not strict UTF-8 JSON") from None
    if not isinstance(value, dict):
        raise WebConfigError("bundle_json_invalid", f"{name} must contain a JSON object")
    return value


def _schema_validate(value: Mapping[str, Any], schema: Mapping[str, Any], name: str) -> None:
    try:
        jsonschema.Draft202012Validator(schema).validate(value)
    except jsonschema.ValidationError:
        raise WebConfigError("bundle_schema_invalid", f"{name} does not match the required schema") from None


class SetupBundleImporter:
    """Own validation, idempotency and atomic writes below one setup root."""

    def __init__(self, workspace_root: str | Path, *, schema_root: str | Path | None = None) -> None:
        self.workspace_root = Path(workspace_root)
        self.setup_root = self.workspace_root / "setups"
        project_root = Path(__file__).resolve().parents[2]
        self.schema_root = Path(schema_root) if schema_root is not None else project_root / "knowledge" / "schemas"

    def _knowledge_schemas(self) -> dict[str, dict[str, Any]]:
        result: dict[str, dict[str, Any]] = {}
        for name in ("manifest", "chunks", "pa_records"):
            path = self.schema_root / f"{name}-0.1.0.schema.json"
            result[name] = _strict_json(path.read_bytes(), path.name)
        return result

    def public_schemas(self) -> dict[str, Any]:
        return {"bundle": BUNDLE_JSON_SCHEMA, **self._knowledge_schemas()}

    @staticmethod
    def _archive_files(archive_bytes: bytes) -> dict[str, bytes]:
        if len(archive_bytes) > MAX_ARCHIVE_BYTES:
            raise WebConfigError("bundle_too_large", "setup bundle exceeds the 50 MB archive limit")
        try:
            archive = zipfile.ZipFile(io.BytesIO(archive_bytes), "r")
        except (zipfile.BadZipFile, OSError):
            raise WebConfigError("bundle_zip_invalid", "uploaded setup bundle is not a valid ZIP") from None
        with archive:
            infos = archive.infolist()
            names = [info.filename for info in infos]
            if len(names) != len(set(names)) or set(names) != REQUIRED_FILES:
                raise WebConfigError("bundle_file_set_invalid", "ZIP root must contain exactly the five setup bundle files")
            total = 0
            values: dict[str, bytes] = {}
            for info in infos:
                name = info.filename
                pure = Path(name)
                mode = (info.external_attr >> 16) & 0o170000
                if (
                    pure.is_absolute()
                    or len(pure.parts) != 1
                    or any(part in {"", ".", ".."} for part in pure.parts)
                    or info.is_dir()
                    or mode == stat.S_IFLNK
                    or bool(info.flag_bits & 0x1)
                ):
                    raise WebConfigError("bundle_path_invalid", "setup ZIP contains an unsafe path, link or encrypted entry")
                total += info.file_size
                if total > MAX_EXTRACTED_BYTES:
                    raise WebConfigError("bundle_too_large", "setup bundle exceeds the 100 MB extracted limit")
                data = archive.read(info)
                if len(data) != info.file_size:
                    raise WebConfigError("bundle_zip_invalid", "setup ZIP entry length is inconsistent")
                values[name] = data
            return values

    def _validate_semantics(self, files: Mapping[str, bytes]) -> tuple[dict[str, Any], tuple[str, ...], int]:
        bundle = _strict_json(files["bundle.json"], "bundle.json")
        manifest = _strict_json(files["manifest.json"], "manifest.json")
        chunks = _strict_json(files["chunks.json"], "chunks.json")
        records = _strict_json(files["pa_records.json"], "pa_records.json")
        _schema_validate(bundle, BUNDLE_JSON_SCHEMA, "bundle.json")
        schemas = self._knowledge_schemas()
        _schema_validate(manifest, schemas["manifest"], "manifest.json")
        _schema_validate(chunks, schemas["chunks"], "chunks.json")
        _schema_validate(records, schemas["pa_records"], "pa_records.json")

        source_sha = _sha(files["source.pdf"])
        if bundle["source"]["sha256"] != source_sha or bundle["files"]["source.pdf"] != source_sha:
            raise WebConfigError("bundle_hash_mismatch", "source.pdf hash does not match bundle.json")
        for name in ("manifest.json", "chunks.json", "pa_records.json"):
            if bundle["files"][name] != _sha(files[name]):
                raise WebConfigError("bundle_hash_mismatch", f"{name} hash does not match bundle.json")
        if bundle["bundle_id"] != "bundle-" + source_sha[:16]:
            raise WebConfigError("bundle_id_invalid", "bundle_id must be derived from the source PDF hash")
        try:
            page_count = len(PdfReader(io.BytesIO(files["source.pdf"])).pages)
        except Exception:
            raise WebConfigError("bundle_pdf_invalid", "source.pdf cannot be parsed") from None
        if page_count != bundle["source"]["page_count"]:
            raise WebConfigError("bundle_page_count_mismatch", "source PDF page count does not match bundle.json")

        documents = manifest.get("documents")
        if not isinstance(documents, list) or len(documents) != 1 or not isinstance(documents[0], Mapping):
            raise WebConfigError("bundle_source_invalid", "manifest must describe exactly source.pdf")
        document = documents[0]
        source_id = document.get("source_id")
        if (
            document.get("file_name") != "source.pdf"
            or document.get("source_file") != "source.pdf"
            or document.get("sha256") != source_sha
            or document.get("page_count") != page_count
            or document.get("status") != "success"
            or not isinstance(source_id, str)
            or not source_id
        ):
            raise WebConfigError("bundle_source_invalid", "manifest source identity does not match source.pdf")

        chunks_by_id: dict[str, Mapping[str, Any]] = {}
        for chunk in chunks.get("chunks", []):
            if not isinstance(chunk, Mapping):
                raise WebConfigError("bundle_chunk_invalid", "chunk must be an object")
            chunk_id = chunk.get("chunk_id")
            if not isinstance(chunk_id, str) or not chunk_id:
                raise WebConfigError("bundle_chunk_invalid", "chunk ID is invalid")
            if chunk_id in chunks_by_id:
                raise WebConfigError("bundle_chunk_invalid", "chunk IDs must be unique")
            if (
                chunk.get("source_id") != source_id
                or chunk.get("source_file") != "source.pdf"
                or chunk.get("source_sha256") != source_sha
                or type(chunk.get("page_start")) is not int
                or type(chunk.get("page_end")) is not int
                or not 1 <= chunk["page_start"] <= chunk["page_end"] <= page_count
                or chunk.get("page_range") != [chunk["page_start"], chunk["page_end"]]
            ):
                raise WebConfigError("bundle_chunk_invalid", "chunk source or page range is invalid")
            chunks_by_id[chunk_id] = chunk

        setup_ids = tuple(bundle["setup_ids"])
        records_by_id: dict[str, Mapping[str, Any]] = {}
        for record in records.get("records", []):
            record_id = record.get("id") if isinstance(record, Mapping) else None
            if not isinstance(record_id, str) or not record_id or record_id in records_by_id:
                raise WebConfigError("bundle_record_invalid", "record IDs must be non-empty and unique")
            records_by_id[record_id] = record
        expected_record_ids = {strategy_id.removeprefix("pa:") for strategy_id in setup_ids}
        actual_setup_ids = {
            record_id
            for record_id, record in records_by_id.items()
            if isinstance(record_id, str) and record.get("type") == "setup" and record.get("active", True) is not False
        }
        if expected_record_ids != actual_setup_ids:
            raise WebConfigError("bundle_setup_ids_invalid", "bundle setup IDs must exactly match active setup records")
        claim_ids: set[str] = set()
        for record_id in expected_record_ids:
            prefix, _, slug = record_id.partition("-")
            parts = record_id.split("-", 2)
            hash_prefix = parts[1] if len(parts) == 3 else ""
            slug = parts[2] if len(parts) == 3 else ""
            if prefix != "user" or not 8 <= len(hash_prefix) <= 16 or not source_sha.startswith(hash_prefix) or not _SLUG.fullmatch(slug):
                raise WebConfigError("bundle_setup_ids_invalid", "record IDs must use user-<source-hash-prefix>-<slug>")
            record = records_by_id[record_id]
            if record.get("source_id") != source_id:
                raise WebConfigError("bundle_record_invalid", "record source_id does not match the manifest")
            for field in CLAIM_FIELDS:
                claims = record.get(field)
                if not isinstance(claims, list):
                    raise WebConfigError("bundle_record_invalid", f"record field {field} must be a list")
                for claim in claims:
                    if not isinstance(claim, Mapping) or claim.get("declaration_type") not in {"source_assertion", "derived_interpretation", "source_not_stated"}:
                        raise WebConfigError("bundle_claim_invalid", "claim declaration_type is invalid")
                    claim_id = claim.get("claim_id")
                    if not isinstance(claim_id, str) or not claim_id or claim_id in claim_ids:
                        raise WebConfigError("bundle_claim_invalid", "claim IDs must be non-empty and unique")
                    claim_ids.add(claim_id)
                    refs = claim.get("source_refs")
                    if not isinstance(refs, list) or not refs:
                        raise WebConfigError("bundle_reference_invalid", "every claim requires at least one local page reference")
                    for ref in refs:
                        chunk = chunks_by_id.get(ref.get("chunk_id")) if isinstance(ref, Mapping) else None
                        if (
                            not isinstance(ref, Mapping)
                            or chunk is None
                            or ref.get("source_id") != source_id
                            or ref.get("source_file") != "source.pdf"
                            or ref.get("source_sha256") != source_sha
                            or type(ref.get("page_start")) is not int
                            or type(ref.get("page_end")) is not int
                            or not chunk["page_start"] <= ref["page_start"] <= ref["page_end"] <= chunk["page_end"]
                            or not 1 <= ref["page_start"] <= ref["page_end"] <= page_count
                        ):
                            raise WebConfigError("bundle_reference_invalid", "claim page reference cannot be resolved to source.pdf")
        return bundle, setup_ids, page_count

    @staticmethod
    def _write_files(directory: Path, files: Mapping[str, bytes]) -> None:
        for name in sorted(REQUIRED_FILES):
            path = directory / name
            with path.open("xb") as handle:
                handle.write(files[name])
                handle.flush()
                os.fsync(handle.fileno())

    def _build_installed(self, directory: Path, bundle: Mapping[str, Any] | None = None) -> InstalledBundle:
        if bundle is None:
            bundle = _strict_json((directory / "bundle.json").read_bytes(), "bundle.json")
            _schema_validate(bundle, BUNDLE_JSON_SCHEMA, "bundle.json")
        setup_ids = tuple(bundle["setup_ids"])
        registry = build_registry(directory, registrations=list(setup_ids))
        for strategy_id in setup_ids:
            loaded = registry.load_packet(strategy_id)
            if loaded.code != "ok" or not loaded.ok:
                raise WebConfigError("bundle_grounding_invalid", "setup packet could not be built from the imported source")
            if not registry.validate_packet(loaded.packet).valid:
                raise WebConfigError("bundle_grounding_invalid", "setup packet validation failed")
            if not registry.validate_profile_lineage(loaded.profile_ref, packet=loaded.packet).valid:
                raise WebConfigError("bundle_lineage_invalid", "setup profile lineage validation failed")
        return InstalledBundle(
            bundle_id=str(bundle["bundle_id"]),
            source_sha256=str(bundle["source"]["sha256"]),
            page_count=int(bundle["source"]["page_count"]),
            setup_ids=setup_ids,
            registry=registry,
        )

    def import_archive(self, archive_bytes: bytes, *, existing_strategy_ids=()) -> tuple[InstalledBundle, bool]:
        files = self._archive_files(archive_bytes)
        bundle, setup_ids, _ = self._validate_semantics(files)
        collisions = sorted(set(setup_ids) & set(existing_strategy_ids))
        target = self.setup_root / bundle["bundle_id"]
        if target.exists():
            installed = self._build_installed(target)
            identical = all((target / name).is_file() and _sha((target / name).read_bytes()) == _sha(files[name]) for name in REQUIRED_FILES)
            if identical and installed.setup_ids == setup_ids:
                return installed, True
            raise WebConfigError("bundle_id_conflict", "an installed bundle ID is occupied by different content")
        if collisions:
            raise WebConfigError("strategy_id_conflict", "one or more setup IDs already exist")

        self.setup_root.mkdir(parents=True, exist_ok=True)
        staging = Path(tempfile.mkdtemp(prefix=".import-", dir=self.setup_root))
        try:
            self._write_files(staging, files)
            installed = self._build_installed(staging, bundle)
            os.replace(staging, target)
            return InstalledBundle(
                installed.bundle_id,
                installed.source_sha256,
                installed.page_count,
                installed.setup_ids,
                build_registry(target, registrations=list(installed.setup_ids)),
            ), False
        except Exception:
            if staging.exists():
                shutil.rmtree(staging, ignore_errors=True)
            raise

    def load_installed(self) -> list[InstalledBundle]:
        if not self.setup_root.exists():
            return []
        result: list[InstalledBundle] = []
        for path in sorted(self.setup_root.iterdir(), key=lambda item: item.name):
            if not path.is_dir() or path.name.startswith(".import-"):
                continue
            if set(item.name for item in path.iterdir()) != REQUIRED_FILES:
                raise WebConfigError("workspace_bundle_invalid", "installed setup bundle has an unexpected file set")
            result.append(self._build_installed(path))
        return result


__all__ = [
    "BUNDLE_JSON_SCHEMA",
    "BUNDLE_SCHEMA_VERSION",
    "BUNDLE_VERSION",
    "InstalledBundle",
    "MAX_ARCHIVE_BYTES",
    "MAX_EXTRACTED_BYTES",
    "REQUIRED_FILES",
    "SetupBundleImporter",
]
