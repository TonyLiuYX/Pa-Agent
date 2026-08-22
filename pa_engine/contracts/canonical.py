"""Strict canonical JSON, content identity and stable helper IDs."""

from __future__ import annotations

import copy
import hashlib
import json
from decimal import Decimal
from typing import Any, Mapping

from .registry import METADATA


class CanonicalizationError(ValueError):
    """Raised when a value is not valid JSON-native contract data."""


_NATIVE = (type(None), bool, int, str, list, dict)


def _assert_native(value: Any, path: str = "$") -> None:
    # ``bool`` is an ``int`` subclass; checking exact types also prevents
    # Decimal, float, date and user-defined numeric objects leaking into JSON.
    if type(value) not in _NATIVE:
        raise CanonicalizationError(f"non JSON-native value at {path}")
    if isinstance(value, dict):
        for key, item in value.items():
            if type(key) is not str:
                raise CanonicalizationError(f"non-string object key at {path}")
            _assert_native(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _assert_native(item, f"{path}[{index}]")


def canonical_json_bytes(value: Any) -> bytes:
    """Serialize strict JSON-native data with the frozen canonical rules."""

    _assert_native(value)
    try:
        text = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError, UnicodeError) as exc:
        raise CanonicalizationError("value cannot be canonically serialized") from exc
    return text.encode("utf-8")


def canonical_json(value: Any) -> str:
    """String form of :func:`canonical_json_bytes`."""

    return canonical_json_bytes(value).decode("utf-8")


def _pointer_parts(pointer: str) -> list[str]:
    if pointer == "":
        return []
    if not isinstance(pointer, str) or not pointer.startswith("/"):
        raise CanonicalizationError(f"invalid JSON pointer: {pointer!r}")
    return [part.replace("~1", "/").replace("~0", "~") for part in pointer[1:].split("/")]


def _remove_pointer(root: Any, pointer: str) -> None:
    parts = _pointer_parts(pointer)
    if not parts:
        return
    current = root
    for part in parts[:-1]:
        if isinstance(current, dict):
            if part not in current:
                return
            current = current[part]
        elif isinstance(current, list):
            try:
                current = current[int(part)]
            except (ValueError, IndexError):
                return
        else:
            return
    leaf = parts[-1]
    if isinstance(current, dict):
        current.pop(leaf, None)
    elif isinstance(current, list):
        try:
            # Removing a pointer to an array element would alter array order
            # and is intentionally avoided; exclusion metadata currently uses
            # object fields.  A null placeholder preserves semantic positions.
            index = int(leaf)
            if 0 <= index < len(current):
                current[index] = None
        except (ValueError, TypeError):
            return


def semantic_projection(document: Mapping[str, Any], kind: str | None = None) -> dict[str, Any]:
    """Deep-copy *document* and remove only index-declared volatile pointers."""

    if not isinstance(document, dict):
        raise CanonicalizationError("artifact document must be an object")
    _assert_native(document)
    if kind is None:
        kind = document.get("artifact_kind")
    pointers = []
    if isinstance(kind, str) and kind in METADATA:
        pointers = METADATA[kind].get("canonical_hash_excluded_json_pointers", [])
    projection = copy.deepcopy(document)
    for pointer in pointers:
        _remove_pointer(projection, pointer)
    return projection


def _digest_payload(payload: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def content_hash(document: Mapping[str, Any], artifact_kind: str | None = None, *, kind: str | None = None) -> str:
    """Compute SHA-256 of the exact semantic projection for an artifact."""

    if kind is not None:
        artifact_kind = kind
    if artifact_kind is None and isinstance(document, dict):
        artifact_kind = document.get("artifact_kind")
    return _digest_payload(semantic_projection(document, artifact_kind))


def stable_id(prefix: str, payload: Any) -> str:
    """Return ``prefix`` followed by SHA-256 of canonical *payload*."""

    if not isinstance(prefix, str) or not prefix:
        raise ValueError("id prefix must be a non-empty string")
    return prefix + _digest_payload(payload)


def _metadata(kind: str) -> dict[str, Any]:
    try:
        return METADATA[kind]
    except KeyError as exc:
        raise KeyError(kind) from exc


def finalize_artifact(document: Mapping[str, Any], kind: str | None = None, *, artifact_kind: str | None = None) -> dict[str, Any]:
    """Deep-copy an artifact and fill its top-level content hash and ID.

    Existing fields are overwritten deterministically.  All volatile fields
    remain untouched; only the exact pointers in ``schema-index.json`` are
    omitted from the identity projection.
    """

    if artifact_kind is not None:
        kind = artifact_kind
    result = copy.deepcopy(dict(document))
    kind = kind or result.get("artifact_kind")
    if not isinstance(kind, str) or kind not in METADATA:
        raise KeyError(kind)
    result.setdefault("artifact_kind", kind)
    result.setdefault("schema_version", _metadata(kind).get("schema_version", "0.1.0"))
    digest = content_hash(result, kind)
    result[_metadata(kind)["content_hash_field"].lstrip("/")] = digest
    id_field = _metadata(kind)["id_field"].lstrip("/")
    result[id_field] = _metadata(kind)["id_prefix"] + digest
    return result


def verify_identity(document: Mapping[str, Any], kind: str | None = None) -> bool:
    """Return whether top-level ID and content hash match their projection."""

    if not isinstance(document, dict):
        return False
    kind = kind or document.get("artifact_kind")
    if kind not in METADATA:
        return False
    meta = METADATA[kind]
    try:
        digest = content_hash(document, kind)
    except (CanonicalizationError, TypeError, ValueError):
        return False
    id_key = meta["id_field"].lstrip("/")
    hash_key = meta["content_hash_field"].lstrip("/")
    return document.get(hash_key) == digest and document.get(id_key) == meta["id_prefix"] + digest


def bar_id_for(instrument: Any, timeframe: Any, session: Any, start_at: str, end_at: str) -> str:
    return stable_id("bar_", {"instrument": instrument, "timeframe": timeframe, "session": session, "start_at": start_at, "end_at": end_at})


def bar_id(instrument: Any, timeframe: Any, session: Any, start_at: str, end_at: str) -> str:
    return bar_id_for(instrument, timeframe, session, start_at, end_at)


def observation_id_for(market_snapshot_id: str, bar_id_value: str, formula_version: str) -> str:
    return stable_id("obv_", {"market_snapshot_id": market_snapshot_id, "bar_id": bar_id_value, "formula_version": formula_version})


def observation_id(market_snapshot_id: str, bar_id_value: str, formula_version: str) -> str:
    return observation_id_for(market_snapshot_id, bar_id_value, formula_version)


def evidence_id_for(
    knowledge_snapshot_id: str,
    record_id: str,
    field: str,
    claim_id: str,
    declaration_type: str,
    statement_sha256: str,
    citation_ids: Any,
) -> str:
    return stable_id(
        "evi_",
        {
            "knowledge_snapshot_id": knowledge_snapshot_id,
            "record_id": record_id,
            "field": field,
            "claim_id": claim_id,
            "declaration_type": declaration_type,
            "statement_sha256": statement_sha256,
            "citation_ids": citation_ids,
        },
    )


def evidence_id(*args: Any, **kwargs: Any) -> str:
    return evidence_id_for(*args, **kwargs)


def idempotency_key_for(payload: Any) -> str:
    return stable_id("idem_", payload)


def idempotency_key(payload: Any) -> str:
    return idempotency_key_for(payload)


def event_projection(document: Mapping[str, Any]) -> dict[str, Any]:
    """Stable decision/outcome projection used for event idempotency.

    IDs, content hashes, the idempotency key itself and recording telemetry are
    excluded; all other fields, including mode/stream and lineage, remain
    bound to the event key.
    """

    if not isinstance(document, Mapping):
        raise TypeError("event document must be an object")
    return {k: copy.deepcopy(v) for k, v in document.items() if k not in {"decision_event_id", "outcome_event_id", "content_hash", "idempotency_key", "recorded_at"}}


def event_idempotency_key(document: Mapping[str, Any]) -> str:
    return idempotency_key_for(event_projection(document))


def text_sha256(text: str) -> str:
    if not isinstance(text, str):
        raise TypeError("text must be a string")
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_text(text: str) -> str:
    return text_sha256(text)


def canonical_bytes(value: Any) -> bytes:
    return canonical_json_bytes(value)


def citation_id_for(citation: Mapping[str, Any]) -> str:
    # The index excludes citation_id/content_hash, so this is the complete
    # semantic citation content hash and remains stable across top identity
    # regeneration.
    return stable_id("cit_", semantic_projection(citation, "knowledge_citation"))


def citation_id(citation: Mapping[str, Any]) -> str:
    return citation_id_for(citation)


make_bar_id = bar_id_for
make_observation_id = observation_id_for
make_evidence_id = evidence_id_for
make_idempotency_key = idempotency_key_for


__all__ = [
    "CanonicalizationError",
    "bar_id",
    "bar_id_for",
    "canonical_bytes",
    "canonical_json",
    "canonical_json_bytes",
    "citation_id",
    "citation_id_for",
    "content_hash",
    "evidence_id",
    "evidence_id_for",
    "event_idempotency_key",
    "event_projection",
    "finalize_artifact",
    "idempotency_key",
    "idempotency_key_for",
    "make_bar_id",
    "make_evidence_id",
    "make_idempotency_key",
    "make_observation_id",
    "observation_id",
    "observation_id_for",
    "semantic_projection",
    "stable_id",
    "text_sha256",
    "sha256_text",
    "verify_identity",
]
