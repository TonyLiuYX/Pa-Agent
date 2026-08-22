"""Shared helpers for the G016 strategy-packet acceptance tests.

The packet implementation is deliberately imported through the frozen public
surface.  These helpers only smooth over the small representation choices
(mapping versus dataclass) that are allowed at that boundary; they do not
weaken any acceptance assertions.
"""
from __future__ import annotations

import dataclasses
import hashlib
import importlib
import inspect
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest


ROOT = Path(__file__).resolve().parents[3]
KNOWLEDGE = ROOT / "knowledge"
KNOWLEDGE_DATA = KNOWLEDGE / "data"
RECORDS_PATH = KNOWLEDGE_DATA / "pa_records.json"
CHUNKS_PATH = KNOWLEDGE_DATA / "chunks.json"
MANIFEST_PATH = KNOWLEDGE_DATA / "manifest.json"
VISUAL_INDEX_PATH = KNOWLEDGE_DATA / "visual_index.json"
G014_FIXTURES = ROOT / "pa_engine" / "contracts" / "fixtures"


def as_dict(value: Any) -> Any:
    """Convert a public dataclass/typed result to a plain JSON-like value."""

    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return as_dict(dataclasses.asdict(value))
    if isinstance(value, Mapping):
        return {str(key): as_dict(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [as_dict(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "to_dict") and callable(value.to_dict):
        return as_dict(value.to_dict())
    if hasattr(value, "model_dump") and callable(value.model_dump):
        return as_dict(value.model_dump())
    return value


def public_attr(value: Any, *names: str, default: Any = None) -> Any:
    """Read a field from a mapping or an object using a few public spellings."""

    plain = as_dict(value)
    if isinstance(plain, Mapping):
        for name in names:
            if name in plain:
                return plain[name]
    for name in names:
        if hasattr(value, name):
            return getattr(value, name)
    return default


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def text_sha256(value: str) -> str:
    try:
        from pa_engine.contracts.canonical import text_sha256 as canonical_text_sha256

        return canonical_text_sha256(value)
    except Exception:
        return sha256_text(value)


def canonical_bytes(value: Any) -> bytes:
    from pa_engine.contracts.canonical import canonical_json_bytes

    return canonical_json_bytes(value)


def canonical_json(value: Any) -> str:
    return canonical_bytes(value).decode("utf-8")


def _find_api_module() -> Any:
    names = (
        "pa_engine.strategies.knowledge",
        "pa_engine.strategies.knowledge.registry",
        "pa_engine.strategies.knowledge.packet",
    )
    required = {
        "CLAIM_FIELDS",
        "DEFAULT_REGISTRY_PATH",
        "SnapshotIdentity",
        "PacketLoadOutcome",
        "ProfileBindingResult",
        "StrategyKnowledgeError",
        "compute_snapshot_identity",
        "load_default_registry",
        "build_registry",
        "packet_content_hash",
        "validate_packet",
        "bind_strategy_profile_ref",
        "validate_profile_lineage",
    }
    modules: list[Any] = []
    for name in names:
        try:
            modules.append(importlib.import_module(name))
        except ImportError:
            continue
    for module in modules:
        if required.issubset(set(dir(module))):
            return module
    # Some implementations export the public names from more than one module.
    merged: dict[str, Any] = {}
    for module in modules:
        for name in required:
            if hasattr(module, name):
                merged[name] = getattr(module, name)
    if required.issubset(merged):
        class PublicAPI:
            pass

        api = PublicAPI()
        for name, value in merged.items():
            setattr(api, name, value)
        return api
    pytest.fail(
        "G016 public API is not importable; expected exports in "
        "pa_engine.strategies.knowledge (or its registry/packet modules)"
    )


@pytest.fixture(scope="session")
def api() -> Any:
    return _find_api_module()


def _path_argument(function: Any, path: Path) -> tuple[tuple[Any, ...], dict[str, Any]]:
    """Map a temporary knowledge path to a named public argument."""

    try:
        parameters = inspect.signature(function).parameters
    except (TypeError, ValueError):
        return (path,), {}
    names = (
        "knowledge_dir",
        "registry_path",
        "data_dir",
        "root",
        "path",
        "base_dir",
        "knowledge_path",
    )
    for name in names:
        if name in parameters:
            return (), {name: path}
    positional = [
        p
        for p in parameters.values()
        if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
    ]
    if positional:
        return (path,), {}
    return (), {}


def invoke_with_path(function: Any, path: Path) -> Any:
    args, kwargs = _path_argument(function, path)
    return function(*args, **kwargs)


def default_registry(api: Any) -> Any:
    return invoke_with_path(api.load_default_registry, KNOWLEDGE)


def registry_for(api: Any, path: Path | None = None, registrations: Any = None) -> Any:
    if path is None:
        return default_registry(api)
    function = api.build_registry
    try:
        parameters = inspect.signature(function).parameters
    except (TypeError, ValueError):
        parameters = {}
    args, kwargs = _path_argument(function, path)
    if registrations is not None and "registrations" in parameters:
        kwargs["registrations"] = registrations
    return function(*args, **kwargs)


def strategy_ids(registry: Any) -> list[str]:
    value = public_attr(registry, "strategy_ids", "ids", default=None)
    if value is None and hasattr(registry, "strategy_ids"):
        value = registry.strategy_ids
    if callable(value):
        value = value()
    return list(value or [])


def snapshot_identity(registry: Any) -> Any:
    value = public_attr(registry, "snapshot_identity", "snapshot", default=None)
    if callable(value):
        value = value()
    return value


def snapshot_dict(registry: Any) -> dict[str, Any]:
    value = as_dict(snapshot_identity(registry))
    assert isinstance(value, Mapping), value
    return dict(value)


def load_packet(registry: Any, strategy_id: str) -> Any:
    return registry.load_packet(strategy_id)


def outcome_dict(value: Any) -> dict[str, Any]:
    plain = as_dict(value)
    if isinstance(plain, Mapping):
        return dict(plain)
    return {"value": plain}


def outcome_packet(value: Any) -> Any:
    return public_attr(value, "packet", "knowledge_packet", default=None)


def outcome_evidence(value: Any) -> Any:
    packet = outcome_packet(value)
    evidence = public_attr(value, "evidence", "evidence_package", default=None)
    if evidence is not None:
        return evidence
    if packet is not None:
        return public_attr(packet, "evidence", "evidence_package", default=None)
    return None


def outcome_profile(value: Any) -> Any:
    return public_attr(value, "profile_ref", "strategy_profile_ref", default=None)


def packet_dict(value: Any) -> dict[str, Any]:
    packet = outcome_packet(value) or value
    plain = as_dict(packet)
    assert isinstance(plain, Mapping), plain
    return dict(plain)


def evidence_dict(value: Any) -> dict[str, Any]:
    evidence = outcome_evidence(value)
    assert evidence is not None, "packet does not expose its G014 evidence package"
    plain = as_dict(evidence)
    assert isinstance(plain, Mapping), plain
    return dict(plain)


def outcome_code(value: Any) -> str | None:
    result = outcome_dict(value)
    error = result.get("error")
    if isinstance(error, Mapping):
        for key in ("code", "error_code", "kind"):
            if error.get(key):
                return str(error[key])
    for key in ("error_code", "code", "status", "reason", "kind"):
        if result.get(key) and isinstance(result[key], (str, int)):
            return str(result[key])
    for key in ("error_code", "code", "status", "reason", "kind"):
        candidate = getattr(value, key, None)
        if candidate and isinstance(candidate, (str, int)):
            return str(candidate)
    return None


def is_executable(value: Any) -> bool:
    original = value
    if isinstance(original, BaseException):
        return False
    result = outcome_dict(original)
    flag = result.get("executable")
    if flag is not None:
        return bool(flag)
    flag = result.get("valid")
    if flag is not None:
        return bool(flag)
    packet = outcome_packet(original)
    if packet is not None:
        return bool(public_attr(packet, "executable", default=True))
    return True


def assert_typed_nonexec(api: Any, value: Any, *needles: str) -> None:
    """Accept either a typed non-executable result or the typed public error."""

    code = outcome_code(value)
    if code is None:
        error = public_attr(value, "error", "exception", default=None)
        if error is not None:
            code = outcome_code(error)
    if code is None:
        pytest.fail(f"expected typed non-executable outcome, got {value!r}")
    lowered = code.lower()
    assert not is_executable(value), value
    if needles:
        assert any(needle.lower() in lowered for needle in needles), (code, needles, value)


def call_validate_packet(api: Any, packet: Any, knowledge_dir: Path = KNOWLEDGE) -> Any:
    function = api.validate_packet
    try:
        parameters = inspect.signature(function).parameters
    except (TypeError, ValueError):
        parameters = {}
    if not any(name in parameters for name in ("knowledge_dir", "knowledge_path", "snapshot")):
        return function(packet)
    if "snapshot" in parameters and "knowledge_dir" not in parameters and "knowledge_path" not in parameters:
        snapshot = None
        try:
            snapshot = api.compute_snapshot_identity(knowledge_dir)
        except Exception:
            snapshot = None
        return function(packet, snapshot=snapshot)
    try:
        return function(packet, knowledge_dir=knowledge_dir)
    except TypeError as exc:
        # A frozen implementation may call this argument ``knowledge_path``.
        if "knowledge_dir" not in str(exc):
            raise
        return function(packet, knowledge_path=knowledge_dir)


def call_registry_validate_packet(registry: Any, packet: Any) -> Any:
    """Use the registry's full snapshot-aware public packet validator."""

    function = getattr(registry, "validate_packet", None)
    assert callable(function), "StrategyRegistry.validate_packet public full-snapshot validator is required"
    return function(packet)


def call_packet_hash(api: Any, packet: Any) -> str:
    value = api.packet_content_hash(packet)
    if isinstance(value, str):
        return value
    result = public_attr(value, "packet_hash", "content_hash", "hash", default=None)
    assert isinstance(result, str), value
    return result


def _call_profile_function(function: Any, packet: Any, evidence: Any | None = None) -> Any:
    """Call the profile binder while preserving its declared public signature."""

    try:
        parameters = inspect.signature(function).parameters
    except (TypeError, ValueError):
        parameters = {}
    values: dict[str, Any] = {}
    for name in parameters:
        lname = name.lower()
        if "packet" in lname:
            values[name] = packet
        elif "evidence" in lname:
            values[name] = evidence
    if values:
        return function(**values)
    if evidence is not None:
        try:
            return function(packet, evidence)
        except TypeError:
            pass
    return function(packet)


def bind_profile(api: Any, packet: Any, evidence: Any | None = None) -> Any:
    # Registry.load_packet returns the authoritative profile reference.  The
    # frozen binder itself binds that ref onto a downstream artifact, so use
    # the outcome/evidence extension when available rather than guessing a
    # packet-construction signature.
    candidate = public_attr(packet, "profile_ref", "strategy_profile_ref", default=None)
    if candidate is None and evidence is not None:
        evidence_plain = as_dict(evidence)
        if isinstance(evidence_plain, Mapping):
            ext = evidence_plain.get("extensions", {})
            if isinstance(ext, Mapping):
                candidate = ext.get("strategy_profile_ref")
    if candidate is None:
        candidate = public_attr(packet, "profile", default=None)
    function = api.bind_strategy_profile_ref
    try:
        parameters = inspect.signature(function).parameters
    except (TypeError, ValueError):
        parameters = {}
    if "profile_ref" in parameters and "artifact" in parameters:
        if candidate is None:
            raise AssertionError("profile reference is not available on packet/evidence")
        return function(packet, candidate)
    return _call_profile_function(function, packet, evidence)


def profile_ref(value: Any) -> dict[str, Any]:
    plain = as_dict(value)
    if isinstance(plain, Mapping):
        if "profile_ref" in plain and isinstance(plain["profile_ref"], Mapping):
            return dict(plain["profile_ref"])
        if "strategy_profile_ref" in plain and isinstance(plain["strategy_profile_ref"], Mapping):
            return dict(plain["strategy_profile_ref"])
        return dict(plain)
    raise AssertionError(value)


def call_validate_lineage(api: Any, artifacts: Any, profile: Any | None = None, packet: Any | None = None) -> Any:
    function = api.validate_profile_lineage
    try:
        parameters = inspect.signature(function).parameters
    except (TypeError, ValueError):
        parameters = {}
    if isinstance(artifacts, Mapping):
        kwargs: dict[str, Any] = {}
        values = list(artifacts.values())
        for name in parameters:
            lname = name.lower()
            if "profile" in lname:
                kwargs[name] = profile
            elif lname in {"artifacts", "bound_artifacts", "lineage", "items"}:
                kwargs[name] = values
            elif lname == "packet":
                kwargs[name] = packet if packet is not None else artifacts.get("packet")
            elif lname in artifacts:
                kwargs[name] = artifacts[lname]
        if kwargs:
            return function(**kwargs)
    if profile is not None:
        try:
            return function(profile, packet=packet, artifacts=artifacts)
        except TypeError:
            try:
                return function(artifacts, profile)
            except TypeError:
                pass
    return function(artifacts)


def result_valid(value: Any) -> bool:
    plain = as_dict(value)
    if isinstance(plain, Mapping):
        for key in ("valid", "ok", "is_valid"):
            if key in plain:
                return bool(plain[key])
    return bool(value)


def no_float(value: Any) -> None:
    if isinstance(value, float):
        raise AssertionError(f"float leaked into canonical profile/extension: {value!r}")
    if isinstance(value, Mapping):
        for item in value.values():
            no_float(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            no_float(item)


def formal_records() -> list[dict[str, Any]]:
    return load_json(RECORDS_PATH)["records"]


def formal_record(record_id: str) -> dict[str, Any]:
    return next(item for item in formal_records() if item["id"] == record_id)


def first_setup() -> dict[str, Any]:
    return next(item for item in formal_records() if item["type"] == "setup")


def claims(record: Mapping[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    fields = (
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
    return [(field, item) for field in fields for item in record.get(field, [])]
