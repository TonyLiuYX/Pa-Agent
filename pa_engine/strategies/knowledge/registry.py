"""Exact strategy-ID registry with a bounded, auditable packet cache."""

from __future__ import annotations

import copy
import json
from collections import OrderedDict
from pathlib import Path
from typing import Any, Iterable, Mapping

from .errors import PacketLoadOutcome, ProfileBindingResult, StrategyKnowledgeError
from .packet import build_packet
from .snapshot import SNAPSHOT_ALGORITHM, KnowledgeSnapshot, SnapshotIdentity, load_snapshot


DEFAULT_REGISTRY_PATH = Path(__file__).resolve().parent / "strategy-registry-0.1.0.json"
REGISTRY_VERSION = "0.1.0"


def _error_outcome(error: StrategyKnowledgeError, **details: Any) -> PacketLoadOutcome:
    merged = copy.deepcopy(error.details)
    merged.update(details)
    return PacketLoadOutcome(error.code, error.message, merged, False, None, None, None)


def _record_state(snapshot: KnowledgeSnapshot, record: Mapping[str, Any] | None) -> str:
    if record is None:
        return "record_not_found"
    # Reverification is a stronger lifecycle state than ordinary inactive:
    # callers need the exact pending code to avoid treating stale source data
    # as a permanent tombstone.  Some accepted lifecycle fixtures represent
    # this as invalidated + pending_source_reverification.
    pending_record = (
        record.get("source_reverification_status") in {"pending", "pending_source_reverification", "reverification_required"}
        or record.get("record_status") in {"pending", "reverification_required", "pending_source_reverification"}
        or record.get("reverification_required") is True
        or record.get("source_fidelity") == "pending_source_reverification"
    )
    if pending_record:
        return "record_pending"
    if record.get("active") is False or record.get("record_status") in {"inactive", "disabled", "tombstone", "invalidated"}:
        return "record_inactive"
    if record.get("type") != "setup":
        return "non_setup_record"
    source_id = record.get("source_id")
    source = next((item for item in snapshot.manifest.get("documents", []) if isinstance(item, Mapping) and item.get("source_id") == source_id), None)
    if source is None:
        return "record_pending"
    if source.get("source_reverification_required") is True or source.get("source_reverification_status") in {"pending", "pending_source_reverification", "reverification_required"}:
        return "record_pending"
    if source.get("status") != "success":
        return "record_pending"
    if source.get("source_reverified_sha256") and source.get("source_reverified_sha256") != source.get("sha256"):
        return "record_pending"
    return "active"


def _read_registry(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as fh:
            value = json.load(fh)
    except Exception as exc:
        raise StrategyKnowledgeError("registry_load_failure", "strategy registry file cannot be read", {"path": str(path)}) from exc
    if not isinstance(value, dict):
        raise StrategyKnowledgeError("registry_invalid", "strategy registry must be an object", {"path": str(path)})
    return value


def _normalize_registrations(registrations: Any, snapshot: KnowledgeSnapshot) -> list[dict[str, Any]]:
    if registrations is None:
        values: Any = [record for record in snapshot.records.values() if record.get("type") == "setup" and record.get("active") is True]
    elif isinstance(registrations, Mapping):
        if isinstance(registrations.get("strategies"), list):
            values = registrations.get("strategies")
        elif "strategy_id" in registrations or "record_id" in registrations:
            values = [registrations]
        else:
            values = []
            for key, value in registrations.items():
                if isinstance(value, Mapping):
                    item = dict(value)
                    item.setdefault("strategy_id", key)
                    values.append(item)
                else:
                    values.append({"strategy_id": key, "record_id": value})
    elif isinstance(registrations, (list, tuple)):
        values = registrations
    else:
        raise StrategyKnowledgeError("invalid_registration", "registrations must be a list or object")
    result: list[dict[str, Any]] = []
    seen_strategy: set[str] = set()
    seen_record: set[str] = set()
    for value in values:
        if isinstance(value, str):
            record_id = value[3:] if value.startswith("pa:") else value
            item: dict[str, Any] = {"strategy_id": "pa:" + record_id, "record_id": record_id}
        elif isinstance(value, Mapping):
            item = dict(value)
            record_id = item.get("record_id") or item.get("id")
            strategy_id = item.get("strategy_id")
            if record_id is None and isinstance(strategy_id, str) and strategy_id.startswith("pa:"):
                record_id = strategy_id[3:]
                item["record_id"] = record_id
            if strategy_id is None and isinstance(record_id, str):
                strategy_id = "pa:" + record_id
                item["strategy_id"] = strategy_id
            if record_id is not None:
                item["record_id"] = record_id
        else:
            raise StrategyKnowledgeError("invalid_registration", "registration must be an object or record ID")
        strategy_id, record_id = item.get("strategy_id"), item.get("record_id")
        if not isinstance(record_id, str) or not record_id or strategy_id != "pa:" + record_id:
            raise StrategyKnowledgeError("invalid_registration", "strategy_id must equal pa:record_id", {"strategy_id": strategy_id, "record_id": record_id})
        if strategy_id in seen_strategy:
            raise StrategyKnowledgeError("duplicate_registration", "strategy registration is duplicated", {"strategy_id": strategy_id})
        if record_id in seen_record:
            raise StrategyKnowledgeError("duplicate_record", "record registration is duplicated", {"record_id": record_id})
        seen_strategy.add(strategy_id)
        seen_record.add(record_id)
        supplemental = item.get("supplemental_record_ids", [])
        if supplemental is None:
            supplemental = []
        if not isinstance(supplemental, list) or any(not isinstance(s, str) for s in supplemental):
            raise StrategyKnowledgeError("invalid_registration", "supplemental_record_ids must be a list of IDs", {"record_id": record_id})
        if len(set(supplemental)) != len(supplemental):
            raise StrategyKnowledgeError("duplicate_record", "supplemental record IDs are duplicated", {"record_id": record_id})
        if record_id in supplemental:
            raise StrategyKnowledgeError("invalid_registration", "primary record cannot be supplemental", {"record_id": record_id})
        for supplemental_id in supplemental:
            supplemental_record = snapshot.records.get(supplemental_id)
            if supplemental_record is None:
                raise StrategyKnowledgeError("record_not_found", "supplemental record is absent from snapshot", {"record_id": supplemental_id})
            if supplemental_record.get("type") == "setup":
                raise StrategyKnowledgeError("invalid_registration", "setup records cannot be supplemental context", {"record_id": supplemental_id})
        item["strategy_id"] = strategy_id
        item["record_id"] = record_id
        item["supplemental_record_ids"] = list(supplemental)
        result.append(item)
    result.sort(key=lambda item: item["strategy_id"])
    return result


class StrategyRegistry:
    """Immutable registration view backed by one semantic snapshot."""

    def __init__(self, snapshot: KnowledgeSnapshot, registrations: Iterable[Mapping[str, Any]], *, registry_version: str = REGISTRY_VERSION, cache_size: int = 256):
        if not isinstance(cache_size, int) or isinstance(cache_size, bool) or cache_size < 1:
            raise StrategyKnowledgeError("invalid_cache_size", "cache_size must be a positive integer")
        self._snapshot = snapshot
        self.registry_version = registry_version
        self._registrations = {item["strategy_id"]: copy.deepcopy(dict(item)) for item in registrations}
        self._cache_capacity = cache_size
        self._cache: OrderedDict[tuple[str, str], tuple[dict[str, Any], dict[str, Any], dict[str, Any]]] = OrderedDict()

    @property
    def strategy_ids(self) -> list[str]:
        return sorted(self._registrations)

    @property
    def snapshot_identity(self) -> SnapshotIdentity:
        return self._snapshot.identity

    @property
    def cache_keys(self) -> list[tuple[str, str]]:
        return list(self._cache.keys())

    @property
    def cache_size(self) -> int:
        return len(self._cache)

    @property
    def cache_capacity(self) -> int:
        return self._cache_capacity

    @property
    def registrations(self) -> list[dict[str, Any]]:
        return [copy.deepcopy(self._registrations[key]) for key in sorted(self._registrations)]

    def validate_packet(self, packet: Mapping[str, Any]):
        """Validate with the immutable full snapshot held by this registry."""

        from .packet import validate_packet

        result = validate_packet(packet, snapshot=self._snapshot)
        if not result.valid:
            return result
        if not isinstance(packet, Mapping):
            return ProfileBindingResult(False, "packet_invalid", "packet must be an object")
        strategy_id = packet.get("strategy_id")
        if not isinstance(strategy_id, str) or strategy_id not in self._registrations:
            return ProfileBindingResult(False, "packet_binding_mismatch", "packet strategy is not registered", {"strategy_id": strategy_id})
        try:
            authoritative_packet, _, _ = self._load_payload(strategy_id)
        except StrategyKnowledgeError as exc:
            return ProfileBindingResult(False, exc.code, exc.message, exc.details)
        except Exception as exc:  # no raw implementation exceptions cross this API
            return ProfileBindingResult(False, "grounding_failure", "authoritative packet grounding failed", {"strategy_id": strategy_id, "error": str(exc)})
        if dict(packet) != authoritative_packet:
            return ProfileBindingResult(False, "packet_binding_mismatch", "packet differs from authoritative registry packet", {"strategy_id": strategy_id})
        return result

    def validate_profile_lineage(self, profile_ref: Mapping[str, Any], *, packet: Mapping[str, Any] | None = None, artifacts: Iterable[Mapping[str, Any]] = ()):
        """Validate profile/artifact lineage against this registry's snapshot.

        The registry owns the authoritative packet/profile pair.  Callers may
        omit ``packet`` for convenience, but they cannot turn this into a
        self-consistency check over a forged packet or profile.
        """

        from .profile import validate_profile_lineage

        if not isinstance(profile_ref, Mapping):
            return ProfileBindingResult(False, "profile_binding_mismatch", "profile reference must be an object")
        strategy_id = profile_ref.get("strategy_id")
        if not isinstance(strategy_id, str) or strategy_id not in self._registrations:
            return ProfileBindingResult(False, "profile_binding_mismatch", "profile strategy is not registered", {"strategy_id": strategy_id})
        try:
            authoritative_packet, authoritative_profile, _ = self._load_payload(strategy_id)
        except StrategyKnowledgeError as exc:
            return ProfileBindingResult(False, exc.code, exc.message, exc.details)
        except Exception as exc:  # no raw implementation exceptions cross this API
            return ProfileBindingResult(False, "grounding_failure", "authoritative packet grounding failed", {"strategy_id": strategy_id, "error": str(exc)})
        for key in ("snapshot_id", "snapshot_hash", "snapshot_version"):
            if profile_ref.get(key) != getattr(self._snapshot.identity, key):
                return ProfileBindingResult(False, "snapshot_mismatch", "profile snapshot does not match registry snapshot", {"field": key})
        if dict(profile_ref) != authoritative_profile:
            return ProfileBindingResult(False, "profile_binding_mismatch", "profile reference differs from authoritative registry profile", {"strategy_id": strategy_id})
        effective_packet = authoritative_packet if packet is None else packet
        if packet is not None and dict(packet) != authoritative_packet:
            return ProfileBindingResult(False, "profile_binding_mismatch", "packet differs from authoritative registry packet", {"strategy_id": strategy_id})
        return validate_profile_lineage(profile_ref, packet=effective_packet, artifacts=artifacts, snapshot=self._snapshot)

    def _load_payload(self, strategy_id: str) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        key = (self._snapshot.identity.snapshot_hash, strategy_id)
        if key in self._cache:
            payload = self._cache.pop(key)
            self._cache[key] = payload
            return copy.deepcopy(payload)
        registration = self._registrations[strategy_id]
        record = self._snapshot.records.get(registration.get("record_id"))
        state = _record_state(self._snapshot, record)
        if state != "active":
            messages = {
                "record_inactive": "strategy record is inactive",
                "record_pending": "strategy source or record is pending reverification",
                "record_not_found": "strategy record is absent from snapshot",
                "non_setup_record": "registered record is not a setup",
            }
            raise StrategyKnowledgeError(state, messages.get(state, "strategy record is not executable"), {"strategy_id": strategy_id, "record_id": registration.get("record_id")})
        for supplemental_id in registration.get("supplemental_record_ids", []):
            supplemental_record = self._snapshot.records.get(supplemental_id)
            supplemental_state = _record_state(self._snapshot, supplemental_record)
            if supplemental_state in {"record_inactive", "record_pending"}:
                message = "supplemental record is inactive" if supplemental_state == "record_inactive" else "supplemental record is pending reverification"
                raise StrategyKnowledgeError(supplemental_state, message, {"strategy_id": strategy_id, "record_id": supplemental_id})
        payload = build_packet(self._snapshot, registration, self.registry_version)
        self._cache[key] = copy.deepcopy(payload)
        while len(self._cache) > self._cache_capacity:
            self._cache.popitem(last=False)
        return copy.deepcopy(payload)

    def load_packet(self, strategy_id: str, expected_profile_ref: Mapping[str, Any] | None = None) -> PacketLoadOutcome:
        if strategy_id not in self._registrations:
            return PacketLoadOutcome("strategy_not_registered", "strategy ID is not registered", {"strategy_id": strategy_id}, False)
        if expected_profile_ref is not None and isinstance(expected_profile_ref, Mapping):
            for key in ("snapshot_hash", "snapshot_version", "snapshot_id"):
                alias = {"snapshot_hash": "knowledge_snapshot_hash", "snapshot_version": "knowledge_snapshot_version", "snapshot_id": "knowledge_snapshot_id"}[key]
                expected = expected_profile_ref.get(key, expected_profile_ref.get(alias))
                actual = {"snapshot_hash": self._snapshot.identity.snapshot_hash, "snapshot_version": self._snapshot.identity.snapshot_version, "snapshot_id": self._snapshot.identity.snapshot_id}[key]
                if expected is not None and expected != actual:
                    return PacketLoadOutcome("snapshot_mismatch", "expected profile snapshot does not match registry snapshot", {"field": key, "expected": expected, "actual": actual}, False)
            if expected_profile_ref.get("strategy_id") not in {None, strategy_id} or expected_profile_ref.get("record_id") not in {None, self._registrations[strategy_id].get("record_id")}:
                return PacketLoadOutcome("profile_binding_mismatch", "expected profile owner does not match strategy", {"strategy_id": strategy_id}, False)
        try:
            packet, profile_ref, evidence = self._load_payload(strategy_id)
            if expected_profile_ref is not None and dict(expected_profile_ref) != profile_ref:
                return PacketLoadOutcome("profile_binding_mismatch", "expected profile reference does not match loaded packet", {"strategy_id": strategy_id}, False)
            return PacketLoadOutcome("ok", "strategy packet loaded", {"cache_key": (self._snapshot.identity.snapshot_hash, strategy_id)}, True, packet, profile_ref, evidence)
        except StrategyKnowledgeError as exc:
            return _error_outcome(exc, strategy_id=strategy_id)
        except Exception as exc:  # no raw implementation traceback crosses API boundary
            return PacketLoadOutcome("grounding_failure", "strategy packet grounding failed", {"strategy_id": strategy_id, "error": str(exc)}, False)


def build_registry(knowledge_dir: str | Path, registrations: Any = None, expected_snapshot_hash: str | None = None, expected_snapshot_version: str | None = None, registry_version: str = REGISTRY_VERSION, cache_size: int = 256) -> StrategyRegistry:
    snapshot = load_snapshot(knowledge_dir)
    if expected_snapshot_hash is not None and expected_snapshot_hash != snapshot.identity.snapshot_hash:
        raise StrategyKnowledgeError("snapshot_mismatch", "knowledge snapshot hash does not match expected value", {"expected": expected_snapshot_hash, "actual": snapshot.identity.snapshot_hash})
    if expected_snapshot_version is not None and expected_snapshot_version != snapshot.identity.snapshot_version:
        raise StrategyKnowledgeError("snapshot_mismatch", "knowledge snapshot version does not match expected value", {"expected": expected_snapshot_version, "actual": snapshot.identity.snapshot_version})
    if registry_version != REGISTRY_VERSION:
        raise StrategyKnowledgeError("registry_version_mismatch", "unsupported strategy registry version", {"registry_version": registry_version})
    normalized = _normalize_registrations(registrations, snapshot)
    return StrategyRegistry(snapshot, normalized, registry_version=registry_version, cache_size=cache_size)


def load_default_registry(knowledge_dir: str | Path, cache_size: int = 256) -> StrategyRegistry:
    document = _read_registry(DEFAULT_REGISTRY_PATH)
    if document.get("registry_version") != REGISTRY_VERSION or document.get("snapshot_algorithm") != SNAPSHOT_ALGORITHM:
        raise StrategyKnowledgeError("registry_version_mismatch", "default strategy registry version is unsupported")
    expected_hash = document.get("snapshot_hash")
    expected_version = document.get("snapshot_version")
    if not isinstance(expected_hash, str) or document.get("snapshot_id") != "kbs_" + expected_hash:
        raise StrategyKnowledgeError("registry_invalid", "default registry snapshot ID/hash are not canonical")
    return build_registry(knowledge_dir, registrations=document.get("strategies", []), expected_snapshot_hash=expected_hash, expected_snapshot_version=expected_version, registry_version=REGISTRY_VERSION, cache_size=cache_size)


__all__ = ["DEFAULT_REGISTRY_PATH", "REGISTRY_VERSION", "StrategyRegistry", "build_registry", "load_default_registry"]
