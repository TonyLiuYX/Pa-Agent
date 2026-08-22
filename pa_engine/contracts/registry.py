"""Schema registry for the versioned Phase 2 contract family.

The registry is deliberately local-only.  Every schema is loaded from the
frozen ``schema-index.json`` and registered with ``referencing`` so that
``$ref`` resolution never falls back to a network fetch.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource


CONTRACTS_DIR = Path(__file__).resolve().parent
SCHEMA_INDEX_PATH = CONTRACTS_DIR / "schema-index.json"


class RegistryIntegrityError(RuntimeError):
    """Raised when the frozen index/schema family is internally inconsistent."""


def _read_json(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as fh:
            value = json.load(fh)
    except Exception as exc:  # pragma: no cover - only malformed install data
        raise RegistryIntegrityError(f"cannot read schema file: {path}") from exc
    if not isinstance(value, dict):
        raise RegistryIntegrityError(f"schema must be an object: {path}")
    return value


def _load() -> tuple[dict[str, Any], dict[str, dict[str, Any]], Registry]:
    index = _read_json(SCHEMA_INDEX_PATH)
    if index.get("contract_version") != "0.1.0":
        raise RegistryIntegrityError("unsupported contract version")
    entries = index.get("artifacts")
    if not isinstance(entries, list) or len(entries) != 13:
        raise RegistryIntegrityError("schema index must contain exactly 13 artifacts")

    by_kind: dict[str, dict[str, Any]] = {}
    loaded: dict[str, dict[str, Any]] = {}
    resources: dict[str, Resource] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            raise RegistryIntegrityError("invalid schema index entry")
        kind = entry.get("artifact_kind")
        if not isinstance(kind, str) or not kind or kind in by_kind:
            raise RegistryIntegrityError("duplicate or invalid artifact kind")
        schema_file = entry.get("schema_file")
        schema_id = entry.get("schema_id")
        if not isinstance(schema_file, str) or not isinstance(schema_id, str):
            raise RegistryIntegrityError(f"invalid schema metadata for {kind}")
        path = CONTRACTS_DIR / schema_file
        if not path.is_file():
            raise RegistryIntegrityError(f"missing schema for {kind}")
        schema = _read_json(path)
        if schema.get("$id") != schema_id or entry.get("$id") != schema_id:
            raise RegistryIntegrityError(f"schema id mismatch for {kind}")
        if entry.get("version") != "0.1.0" or entry.get("schema_version") != "0.1.0":
            raise RegistryIntegrityError(f"schema version mismatch for {kind}")
        if schema.get("$schema") != "https://json-schema.org/draft/2020-12/schema":
            raise RegistryIntegrityError(f"schema dialect mismatch for {kind}")
        # Validate schema shape before exposing it to callers.
        try:
            Draft202012Validator.check_schema(schema)
        except Exception as exc:  # pragma: no cover - frozen files are valid
            raise RegistryIntegrityError(f"invalid schema for {kind}") from exc
        by_kind[kind] = dict(entry)
        loaded[kind] = schema
        resources[schema_id] = Resource.from_contents(schema)

    # The common definitions schema is referenced by every artifact wrapper,
    # but is not itself an artifact entry.  Register it explicitly and require
    # that all referenced local IDs are present.
    common_path = CONTRACTS_DIR / "schemas" / "common-0.1.0.schema.json"
    common = _read_json(common_path)
    common_id = common.get("$id")
    if not isinstance(common_id, str):
        raise RegistryIntegrityError("common schema has no $id")
    try:
        Draft202012Validator.check_schema(common)
    except Exception as exc:  # pragma: no cover
        raise RegistryIntegrityError("invalid common schema") from exc
    resources[common_id] = Resource.from_contents(common)
    loaded["common"] = common

    registry = Registry().with_resources(resources.items())
    return index, loaded, registry


INDEX, SCHEMA_DOCUMENTS, REGISTRY = _load()
METADATA: dict[str, dict[str, Any]] = {
    k: dict(v) for k, v in ((e["artifact_kind"], e) for e in INDEX["artifacts"])
}
VOCABULARIES: dict[str, Any] = dict(INDEX.get("vocabularies", {}))
CANONICALIZATION: dict[str, Any] = dict(INDEX.get("canonicalization", {}))


def artifact_kinds() -> list[str]:
    """Return the frozen artifact kinds in schema-index order."""

    return [entry["artifact_kind"] for entry in INDEX["artifacts"]]


def schema_for(kind: str) -> dict[str, Any]:
    """Return a schema document for *kind* (a defensive shallow copy)."""

    if kind not in METADATA:
        raise KeyError(kind)
    # Callers often inspect the schema; nested mutation should not affect the
    # registry, so use a JSON round-trip rather than exposing internal state.
    return json.loads(json.dumps(SCHEMA_DOCUMENTS[kind], ensure_ascii=False))


def metadata_for(kind: str) -> dict[str, Any]:
    """Return index metadata for an artifact kind."""

    if kind not in METADATA:
        raise KeyError(kind)
    return json.loads(json.dumps(METADATA[kind], ensure_ascii=False))


def artifact_metadata(kind: str) -> dict[str, Any]:
    """Alias for :func:`metadata_for`."""

    return metadata_for(kind)


def vocabulary(name: str, default: Any = None) -> Any:
    """Look up one index vocabulary by name."""

    value = VOCABULARIES.get(name, default)
    return json.loads(json.dumps(value, ensure_ascii=False)) if value is not None else None


def vocabularies() -> dict[str, Any]:
    """Return all frozen vocabularies."""

    return json.loads(json.dumps(VOCABULARIES, ensure_ascii=False))


def validator_for(kind: str) -> Draft202012Validator:
    """Create a Draft 2020-12 validator using the local reference registry."""

    if kind not in METADATA:
        raise KeyError(kind)
    return Draft202012Validator(SCHEMA_DOCUMENTS[kind], registry=REGISTRY, format_checker=FormatChecker())


def load_registry() -> Registry:
    """Return the already-integrity-checked local reference registry."""

    return REGISTRY


def schema_metadata(kind: str) -> dict[str, Any]:
    return metadata_for(kind)


def get_vocabulary(name: str, default: Any = None) -> Any:
    return vocabulary(name, default)


__all__ = [
    "CANONICALIZATION",
    "CONTRACTS_DIR",
    "INDEX",
    "METADATA",
    "REGISTRY",
    "RegistryIntegrityError",
    "SCHEMA_INDEX_PATH",
    "SCHEMA_DOCUMENTS",
    "VOCABULARIES",
    "artifact_kinds",
    "artifact_metadata",
    "metadata_for",
    "get_vocabulary",
    "load_registry",
    "schema_for",
    "schema_metadata",
    "validator_for",
    "vocabulary",
    "vocabularies",
]
