"""Composite/filtered strategy registries and bounded browser catalog views."""

from __future__ import annotations

import copy
from collections.abc import Iterable, Mapping
from typing import Any

from pa_engine.strategies.knowledge.errors import PacketLoadOutcome, ProfileBindingResult


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


def _strategy_id_from_packet(packet: Any) -> str | None:
    return str(packet.get("strategy_id")) if isinstance(packet, Mapping) and isinstance(packet.get("strategy_id"), str) else None


class CompositeStrategyRegistry:
    """Dispatch each stable strategy ID to its owning immutable registry."""

    def __init__(self, registries: Iterable[Any]) -> None:
        self._owners: dict[str, Any] = {}
        for registry in registries:
            for strategy_id in registry.strategy_ids:
                if strategy_id in self._owners:
                    raise ValueError(f"strategy ID collision: {strategy_id}")
                self._owners[strategy_id] = registry

    @property
    def strategy_ids(self) -> list[str]:
        return sorted(self._owners)

    @property
    def registrations(self) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for strategy_id in self.strategy_ids:
            owner = self._owners[strategy_id]
            match = next((item for item in getattr(owner, "registrations", []) if item.get("strategy_id") == strategy_id), None)
            result.append(copy.deepcopy(match or {"strategy_id": strategy_id, "record_id": strategy_id.removeprefix("pa:")}))
        return result

    def owner_for(self, strategy_id: str) -> Any | None:
        return self._owners.get(strategy_id)

    def load_packet(self, strategy_id: str, expected_profile_ref: Mapping[str, Any] | None = None):
        owner = self._owners.get(strategy_id)
        if owner is None:
            return PacketLoadOutcome("strategy_not_registered", "strategy ID is not registered", {"strategy_id": strategy_id}, False)
        return owner.load_packet(strategy_id, expected_profile_ref)

    def validate_packet(self, packet: Mapping[str, Any]):
        strategy_id = _strategy_id_from_packet(packet)
        owner = self._owners.get(strategy_id or "")
        if owner is None:
            return ProfileBindingResult(False, "packet_binding_mismatch", "packet strategy is not registered", {"strategy_id": strategy_id})
        return owner.validate_packet(packet)

    def validate_profile_lineage(self, profile_ref: Mapping[str, Any], *, packet: Mapping[str, Any] | None = None, artifacts=()):
        strategy_id = str(profile_ref.get("strategy_id")) if isinstance(profile_ref, Mapping) and isinstance(profile_ref.get("strategy_id"), str) else ""
        owner = self._owners.get(strategy_id)
        if owner is None:
            return ProfileBindingResult(False, "profile_binding_mismatch", "profile strategy is not registered", {"strategy_id": strategy_id})
        return owner.validate_profile_lineage(profile_ref, packet=packet, artifacts=artifacts)


class FilteredStrategyRegistry:
    """Frozen selected-ID view over a composite registry."""

    def __init__(self, source: CompositeStrategyRegistry, selected_ids: Iterable[str]) -> None:
        selected = tuple(sorted(set(selected_ids)))
        if not selected:
            raise ValueError("at least one strategy must be selected")
        unknown = [strategy_id for strategy_id in selected if strategy_id not in source.strategy_ids]
        if unknown:
            raise ValueError("selected strategy is not registered")
        self._source = source
        self._strategy_ids = selected

    @property
    def strategy_ids(self) -> list[str]:
        return list(self._strategy_ids)

    @property
    def registrations(self) -> list[dict[str, Any]]:
        selected = set(self._strategy_ids)
        return [item for item in self._source.registrations if item.get("strategy_id") in selected]

    def load_packet(self, strategy_id: str, expected_profile_ref: Mapping[str, Any] | None = None):
        if strategy_id not in self._strategy_ids:
            return PacketLoadOutcome("strategy_not_registered", "strategy ID is not selected", {"strategy_id": strategy_id}, False)
        return self._source.load_packet(strategy_id, expected_profile_ref)

    def validate_packet(self, packet: Mapping[str, Any]):
        strategy_id = _strategy_id_from_packet(packet)
        if strategy_id not in self._strategy_ids:
            return ProfileBindingResult(False, "packet_binding_mismatch", "packet strategy is not selected", {"strategy_id": strategy_id})
        return self._source.validate_packet(packet)

    def validate_profile_lineage(self, profile_ref: Mapping[str, Any], *, packet: Mapping[str, Any] | None = None, artifacts=()):
        strategy_id = profile_ref.get("strategy_id") if isinstance(profile_ref, Mapping) else None
        if strategy_id not in self._strategy_ids:
            return ProfileBindingResult(False, "profile_binding_mismatch", "profile strategy is not selected", {"strategy_id": strategy_id})
        return self._source.validate_profile_lineage(profile_ref, packet=packet, artifacts=artifacts)


def _claim_projection(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    projected: list[dict[str, Any]] = []
    for claim in value[:24]:
        if not isinstance(claim, Mapping):
            continue
        refs: list[dict[str, Any]] = []
        for ref in claim.get("source_refs", []) if isinstance(claim.get("source_refs"), list) else []:
            if not isinstance(ref, Mapping):
                continue
            refs.append(
                {
                    "source_id": ref.get("source_id"),
                    "source_file": ref.get("source_file"),
                    "page_start": ref.get("page_start"),
                    "page_end": ref.get("page_end"),
                    "provenance_status": ref.get("provenance_status"),
                }
            )
        projected.append(
            {
                "claim_id": claim.get("claim_id"),
                "statement": str(claim.get("statement") or "")[:1200],
                "declaration_type": claim.get("declaration_type"),
                "rationale": str(claim.get("rationale") or "")[:800] or None,
                "source_refs": refs[:12],
            }
        )
    return projected


def catalog_item(strategy_id: str, registry: Any, *, custom: bool = False, bundle_id: str | None = None, detail: bool = False) -> dict[str, Any]:
    outcome = registry.load_packet(strategy_id)
    if outcome.code != "ok" or not isinstance(outcome.packet, Mapping):
        raise ValueError(f"strategy packet unavailable: {strategy_id}")
    record = outcome.packet.get("primary_record")
    if not isinstance(record, Mapping):
        raise ValueError(f"strategy record unavailable: {strategy_id}")
    summary_claims = _claim_projection(record.get("market_context")) or _claim_projection(record.get("recognition_features"))
    item = {
        "strategy_id": strategy_id,
        "record_id": record.get("id"),
        "name": str(record.get("name") or strategy_id)[:180],
        "aliases": [str(value)[:120] for value in record.get("aliases", []) if isinstance(value, str)][:12],
        "tags": [str(value)[:80] for value in record.get("tags", []) if isinstance(value, str)][:24],
        "source_id": record.get("source_id"),
        "source_files": [str(value)[:240] for value in record.get("source_files", []) if isinstance(value, str)][:6],
        "direction": [str(value)[:40] for value in record.get("direction", [])][:8] if isinstance(record.get("direction"), list) else [],
        "timeframes": [str(value)[:80] for value in record.get("timeframes", [])][:12] if isinstance(record.get("timeframes"), list) else [],
        "confidence": record.get("confidence"),
        "summary": summary_claims[0]["statement"] if summary_claims else "",
        "custom": bool(custom),
        "bundle_id": bundle_id,
    }
    if detail:
        item["claims"] = {field: _claim_projection(record.get(field)) for field in CLAIM_FIELDS}
        item["source_refs"] = _claim_projection([{"statement": "source", "declaration_type": "source_assertion", "source_refs": record.get("source_refs", [])}])[0]["source_refs"] if isinstance(record.get("source_refs"), list) else []
        item["packet_hash"] = outcome.packet.get("packet_hash")
        item["profile_ref"] = copy.deepcopy(dict(outcome.profile_ref)) if isinstance(outcome.profile_ref, Mapping) else None
    return item


__all__ = [
    "CLAIM_FIELDS",
    "CompositeStrategyRegistry",
    "FilteredStrategyRegistry",
    "catalog_item",
]
