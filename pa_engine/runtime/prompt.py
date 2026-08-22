"""Versioned, deterministic analysis prompt/request builder for G028/G031.

The builder composes a system ``instructions`` block and a user
``context_text`` block from exactly the supplied strategy packet, shared
market view, decision profile and evidence package.  No account, balance,
quantity, notional, R or P&L field ever enters the prompt; the formed request
is scanned with the field-level boundary denylist and must carry the intact
``strategy_profile_ref`` before it is returned for transport.

The resulting dictionary is JSON-native and deterministic: identical inputs
produce an identical normalized model request (canonical JSON round trip).
"""

from __future__ import annotations

import copy
import re
from typing import Any, Mapping

from pa_engine.providers.base import SCHEMA_VERSION, make_normalized_model_request
from pa_engine.providers.deepseek.config import DEEPSEEK_ADAPTER_VERSION, DEEPSEEK_MODEL, DEEPSEEK_PROVIDER
from pa_engine.providers.openai.config import OPENAI_ADAPTER_VERSION, OPENAI_MODEL, OPENAI_PROVIDER
from pa_engine.runtime.boundary import ModelBoundaryError, assert_model_boundary_safe, profile_ref_intact

PROMPT_VERSION = "0.1.0"
PROMPT_TEMPLATE_VERSION = "g028-prompt-0.1.0"
FEED_KEY = "alpaca:iex:single_exchange"
MARKET_DATA_USAGE = "internal_prototype"
PROVISIONAL_NOTE = (
    "PROVISIONAL FORMING CANDLE — not closed evidence. "
    "It must never be the sole support for an actionable entry or level and is not a citation target."
)
_OUTPUT_DIRECTIVE = "Return exactly one JSON object matching the contract above and nothing else."

_ZERO_HEX = "0" * 64

_INSTRUCTIONS = "\n".join(
    [
        "You are a price-action paper-trading decision agent.",
        "You are a price-action analysis model. You never receive, compute, or output any account balance, position quantity, notional, R multiple or P&L.",
        "",
        "OUTPUT CONTRACT",
        "Return exactly one JSON object with exactly these fields:",
        "- schema_version: string \"0.1.0\"",
        "- branch: one of \"no_entry\" | \"enter_immediately\" | \"wait_for_price\"",
        "- reason: null for no_entry; otherwise exactly one line of at most 240 codepoints",
        "- direction: \"long\" or \"short\" for actionable branches; null for no_entry",
        "- entry_price: canonical positive decimal string (e.g. \"100.5\") for wait_for_price; null otherwise",
        "- stop_price: canonical positive decimal string for enter_immediately and wait_for_price; null for no_entry",
        "- target_price: canonical positive decimal string for enter_immediately and wait_for_price; null for no_entry",
        "- N: integer from 1 to 86400; non-null only for no_entry",
        "- X: integer from 1 to 86400; non-null only for wait_for_price",
        "- grounding: object with keys reason, entry_price, stop_price, target_price; each is null for no_entry and otherwise an object with declaration_type, claim_ids, evidence_ids, citation_ids and market_refs referencing only supplied ids",
        "- confidence: canonical decimal string from 0 to 1; null for no_entry; required for actionable branches",
        "- uncertainty: object with level (\"low\" | \"medium\" | \"high\"), codes (list of supported codes) and details (exactly one line)",
        "- shared_market_view_ref: the exact supplied shared market view reference",
        "- second_event_ref: required and non-null only for enter_immediately",
        "Branch rules:",
        "- no_entry: N (1..86400) is the only non-null field; reason, direction, entry_price, stop_price, target_price, X, confidence, grounding fields and second_event_ref must all be null.",
        "- enter_immediately: reason, direction, stop_price, target_price, confidence and second_event_ref are required; entry_price, N and X must be null; the entry price comes from the referenced second event, never from a requested price.",
        "- wait_for_price: reason, direction, entry_price, stop_price, target_price, X and confidence are required; N and second_event_ref must be null.",
        "Format rules:",
        "- Prices are canonical positive decimal strings: no exponents, no leading zeros and no trailing fractional zeros (for example \"100.5\", never \"100.50\").",
        "- reason must be a single line of at most 240 codepoints.",
        "- uncertainty.level must be low, medium or high and codes must come from the supported vocabulary.",
        "- grounding must reference only claim_ids, evidence_ids, citation_ids and market_refs (bar_/obv_ identities) supplied in the context; never invent identities.",
        "- Level ordering: long requires stop_price < entry_price < target_price; short requires target_price < entry_price < stop_price.",
        "- No account, balance, position quantity, notional, R multiple, P&L or credential field is permitted anywhere in the output.",
        "",
        "no_live_orders: true — output is paper simulation only.",
    ]
)

_TIME_SUFFIXES = {"minute": "Min", "hour": "Hour", "day": "Day", "week": "Week"}
_CLAIM_FIELD_LABELS = {
    "market_context": "market_context",
    "prerequisites": "prerequisites",
    "recognition_features": "recognition_features",
    "trigger": "trigger",
    "entry": "entry",
    "invalidation_stop": "invalidation_stop",
    "targets_exit": "targets_exit",
    "risk_notes": "risk_notes",
    "prohibited": "prohibited",
    "exceptions": "exceptions",
    "examples": "examples",
    "interpretation_notes": "interpretation_notes",
}


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        converted = to_dict()
        if isinstance(converted, Mapping):
            return converted
    raise TypeError(f"{label} must be a mapping (or expose to_dict())")


def _one_line(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value)).strip()


def _join_list(value: Any) -> str:
    if isinstance(value, list):
        return ", ".join(str(item) for item in value)
    if isinstance(value, tuple):
        return ", ".join(str(item) for item in value)
    return str(value) if value is not None else ""


def _timeframe_text(timeframe: Any) -> str:
    if not isinstance(timeframe, Mapping):
        return str(timeframe)
    unit = timeframe.get("unit")
    multiplier = timeframe.get("multiplier")
    suffix = _TIME_SUFFIXES.get(unit, str(unit))
    return f"{multiplier}{suffix}"


def _zero_id(prefix: str) -> str:
    return f"{prefix}{_ZERO_HEX}"


def _provider_identity(
    *,
    provider: str | None,
    model: str | None,
    adapter: str | None,
) -> tuple[str, str, str]:
    identities = {
        DEEPSEEK_PROVIDER: (DEEPSEEK_MODEL, DEEPSEEK_ADAPTER_VERSION),
        OPENAI_PROVIDER: (OPENAI_MODEL, OPENAI_ADAPTER_VERSION),
    }
    if provider is None:
        # The G028 engine passes the provider-derived model for programmatic
        # compatibility but predates the explicit provider argument.  Infer
        # only one of the two fixed identities; arbitrary model selection is
        # still rejected below.
        matches = [name for name, (fixed_model, fixed_adapter) in identities.items() if model == fixed_model or adapter == fixed_adapter]
        provider = matches[0] if len(matches) == 1 else DEEPSEEK_PROVIDER
    if not isinstance(provider, str) or provider not in identities:
        raise ValueError("provider must be deepseek or openai")
    expected_model, expected_adapter = identities[provider]
    if model is not None and model != expected_model:
        raise ValueError(f"model is fixed to {expected_model!r} for provider {provider!r}")
    if adapter is not None and adapter != expected_adapter:
        raise ValueError(f"adapter is fixed to {expected_adapter!r} for provider {provider!r}")
    return provider, expected_model, expected_adapter


def _header(
    view: Mapping[str, Any],
) -> list[str]:
    return [
        "HEADER",
        f"symbol={view.get('symbol', '')}",
        f"timeframe={_timeframe_text(view.get('timeframe'))}",
        f"session={view.get('session', {}).get('kind', '') if isinstance(view.get('session'), Mapping) else ''}/"
        f"{view.get('session', {}).get('timezone', '') if isinstance(view.get('session'), Mapping) else ''}",
        f"feed={FEED_KEY} (IEX single exchange)",
        f"adjustment={view.get('raw_adjustment_basis', 'raw')}",
        f"market_data_usage={MARKET_DATA_USAGE}",
        f"prompt_version={PROMPT_VERSION}",
    ]


def _claim_line(field: str, claim: Mapping[str, Any], citation_ids_by_claim: Mapping[str, list[str]]) -> str:
    parts = [
        f"{_CLAIM_FIELD_LABELS.get(field, field)} #{claim.get('ordinal', '?')} "
        f"[{claim.get('declaration_type', '')}]: {_one_line(claim.get('statement', ''))}"
    ]
    citation_ids = citation_ids_by_claim.get(str(claim.get("claim_id")), [])
    if citation_ids:
        parts.append(f"citation_ids=[{', '.join(citation_ids)}]")
    refs = claim.get("source_refs", []) if isinstance(claim.get("source_refs"), list) else []
    ref_text = "; ".join(
        f"chunk={ref.get('chunk_id', '')} pages={ref.get('page_start', '')}-{ref.get('page_end', '')} "
        f"source_sha256={ref.get('source_sha256', '')}"
        for ref in refs
        if isinstance(ref, Mapping)
    )
    if ref_text:
        parts.append(ref_text)
    return " | ".join(parts)


def _strategy_section(
    packet: Mapping[str, Any],
    profile_ref: Mapping[str, Any],
    evidence_package: Mapping[str, Any] | None,
) -> list[str]:
    lines: list[str] = ["STRATEGY"]
    lines.append(f"strategy_id={packet.get('strategy_id', '')}")
    lines.append(f"profile_ref_id={profile_ref.get('profile_ref_id', '')}")
    lines.append(f"profile_ref_hash={profile_ref.get('profile_ref_hash', '')}")
    record = packet.get("primary_record")
    if isinstance(record, Mapping):
        details = [
            f"record id={record.get('id', '')}",
            f"name={record.get('name', '')}",
            f"type={record.get('type', '')}",
            f"record_status={record.get('record_status', '')}",
        ]
        if isinstance(record.get("direction"), list):
            details.append(f"direction={_join_list(record.get('direction'))}")
        if isinstance(record.get("timeframes"), list):
            details.append(f"timeframes={_join_list(record.get('timeframes'))}")
        lines.append(" ".join(details))
    citation_ids_by_claim: dict[str, list[str]] = {}
    if isinstance(evidence_package, Mapping):
        for item in evidence_package.get("evidence_items", []):
            if isinstance(item, Mapping) and isinstance(item.get("claim_id"), str):
                citation_ids = item.get("citation_ids", [])
                if isinstance(citation_ids, list):
                    citation_ids_by_claim[item["claim_id"]] = sorted(str(value) for value in citation_ids)
    claims = packet.get("claims")
    if isinstance(claims, Mapping):
        lines.append("CLAIMS")
        for field in packet.get("claim_fields", []):
            for claim in claims.get(field, []):
                if isinstance(claim, Mapping):
                    lines.append(_claim_line(field, claim, citation_ids_by_claim))
    excerpts = packet.get("chunk_excerpts", [])
    if isinstance(excerpts, list) and excerpts:
        lines.append("INTERNAL PROVENANCE (chunk excerpts; internal use only, not for external display)")
        for chunk in excerpts:
            if not isinstance(chunk, Mapping):
                continue
            lines.append(
                f"chunk {chunk.get('chunk_id', '')} | source={chunk.get('source_id', '')} "
                f"file={chunk.get('source_file', '')} pages={chunk.get('page_start', '')}-{chunk.get('page_end', '')} "
                f"sha256={chunk.get('source_text_sha256', '')} | {_one_line(chunk.get('source_text', ''))}"
            )
    return lines


def _market_section(view: Mapping[str, Any]) -> list[str]:
    lines: list[str] = ["MARKET"]
    snapshot = view.get("closed_snapshot")
    snapshot = snapshot if isinstance(snapshot, Mapping) else {}
    lines.append(f"symbol={view.get('symbol', '')} timeframe={_timeframe_text(view.get('timeframe'))}")
    lines.append("CLOSED SNAPSHOT")
    lines.append(f"snapshot_id={snapshot.get('market_snapshot_id', '')}")
    lines.append(f"snapshot_hash={snapshot.get('content_hash', '')}")
    lines.append("bars:")
    for bar in snapshot.get("bars", []):
        if isinstance(bar, Mapping) and bar.get("is_closed") is True:
            lines.append(
                f"start_at={bar.get('start_at', '')} end_at={bar.get('end_at', '')} "
                f"O={bar.get('open', '')} H={bar.get('high', '')} L={bar.get('low', '')} "
                f"C={bar.get('close', '')} V={bar.get('volume', '')}"
            )
    frame = view.get("observation_frame")
    if isinstance(frame, Mapping):
        lines.append("OBSERVATION FRAME")
        lines.append(f"observation_frame_id={frame.get('observation_frame_id', '')}")
        lines.append(f"observation_frame_hash={frame.get('content_hash', '')}")
        lines.append("observations:")
        for observation in frame.get("observations", []):
            if not isinstance(observation, Mapping):
                continue
            lines.append(
                f"bar={observation.get('bar_id', '')} direction={observation.get('direction', '')} "
                f"range={observation.get('range', '')} body={observation.get('body', '')} "
                f"upper_wick={observation.get('upper_wick', '')} lower_wick={observation.get('lower_wick', '')} "
                f"range_relation={observation.get('range_relation', '')} gap_relation={observation.get('gap_relation', '')}"
            )
    lines.extend(_latest_second_lines(view))
    lines.append(f"cutover_at={view.get('cutover_at', '')}")
    quality = view.get("quality")
    if isinstance(quality, Mapping):
        lines.append(
            f"quality: status={quality.get('status', '')} codes=[{_join_list(quality.get('codes'))}] "
            f"warnings=[{_join_list(quality.get('warnings'))}] details={quality.get('details', '')}"
        )
    readiness = view.get("readiness")
    if isinstance(readiness, Mapping):
        facts = {
            "regular_session": readiness.get("regular_session"),
            "fresh": readiness.get("fresh"),
            "complete": readiness.get("complete"),
            "non_gapped": readiness.get("non_gapped"),
            "sufficient": readiness.get("sufficient"),
            "frozen_second": readiness.get("frozen_second"),
            "recovery_required": readiness.get("recovery_required"),
        }
        lines.append("readiness: " + " ".join(f"{name}={value}" for name, value in facts.items()))
    forming = view.get("forming_candle")
    if isinstance(forming, Mapping):
        lines.append("provisional_context:")
        lines.append(PROVISIONAL_NOTE)
        lines.append(
            f"start_at={forming.get('start_at', '')} end_at={forming.get('end_at', '')} "
            f"O={forming.get('open', '')} H={forming.get('high', '')} L={forming.get('low', '')} "
            f"C={forming.get('close', '')} V={forming.get('volume', '')} "
            f"source_seconds=[{_join_list(forming.get('source_seconds'))}]"
        )
    return lines


def _latest_second_lines(view: Mapping[str, Any]) -> list[str]:
    latest = view.get("latest_second")
    if not isinstance(latest, Mapping):
        return []
    source_events: list[Mapping[str, Any]] = []
    quality = view.get("quality")
    if isinstance(quality, Mapping) and isinstance(quality.get("source_events"), list):
        source_events = [item for item in quality["source_events"] if isinstance(item, Mapping)]
    event = next((item for item in source_events if item.get("event_id") == latest.get("event_id")), None)
    event = event if isinstance(event, Mapping) else latest
    lines = ["LATEST FROZEN SECOND"]
    lines.append(f"event_id={event.get('event_id', '')}")
    lines.append(f"event_hash={event.get('event_hash', '')}")
    lines.append(f"interval_start={event.get('start_at', '')} interval_end={event.get('end_at', '')}")
    lines.append(f"open={event.get('open', '')} high={event.get('high', '')} low={event.get('low', '')} close={event.get('close', '')} volume={event.get('volume', '')}")
    lines.append(f"close={latest.get('close', event.get('close', ''))} (reference_price)")
    lines.append(f"execution_watermark={event.get('execution_watermark', '')}")
    lines.append(f"revision={event.get('revision_sequence', 0)}")
    return lines


def _context_refs(
    snapshot: Mapping[str, Any],
    frame: Mapping[str, Any] | None,
    evidence_package: Mapping[str, Any] | None,
    analysis_request: Mapping[str, Any] | None,
) -> dict[str, str]:
    evidence = evidence_package if isinstance(evidence_package, Mapping) else None
    request_artifact = analysis_request if isinstance(analysis_request, Mapping) else None
    return {
        "analysis_request_id": str(request_artifact.get("analysis_request_id") or _zero_id("arq_")) if request_artifact is not None else _zero_id("arq_"),
        "analysis_request_hash": str(request_artifact.get("content_hash") or _ZERO_HEX) if request_artifact is not None else _ZERO_HEX,
        "market_snapshot_id": str(snapshot.get("market_snapshot_id") or _zero_id("mkt_")),
        "market_snapshot_hash": str(snapshot.get("content_hash") or _ZERO_HEX),
        "observation_frame_id": str(frame.get("observation_frame_id")) if frame is not None else _zero_id("obs_"),
        "observation_frame_hash": str(frame.get("content_hash")) if frame is not None else _ZERO_HEX,
        "evidence_package_id": str(evidence.get("evidence_package_id")) if evidence is not None else _zero_id("evp_"),
        "evidence_package_hash": str(evidence.get("content_hash")) if evidence is not None else _ZERO_HEX,
    }


def _extension_context_refs(view: Mapping[str, Any], latest: Mapping[str, Any], packet: Mapping[str, Any]) -> dict[str, str]:
    values = {
        "shared_market_view_id": view.get("shared_market_view_id"),
        "shared_market_view_hash": view.get("shared_market_view_hash"),
        "latest_second_event_id": latest.get("event_id"),
        "latest_second_event_hash": latest.get("event_hash"),
        "packet_id": packet.get("packet_id"),
        "packet_hash": packet.get("packet_hash"),
    }
    return {key: str(value) for key, value in values.items() if value is not None}


def build_analysis_prompt(
    *,
    strategy_id: str,
    packet: Mapping[str, Any],
    profile_ref: Mapping[str, Any],
    market_view: Mapping[str, Any],
    reasoning_mode: str,
    provider: str | None = None,
    model: str | None = None,
    adapter: str | None = None,
    evidence_package: Mapping[str, Any] | None = None,
) -> tuple[str, str]:
    """Build and return ``(instructions, context_text)`` for tests and reuse."""
    if not isinstance(strategy_id, str) or not strategy_id:
        raise ValueError("strategy_id must be a non-empty string")
    view = _mapping(market_view, "market_view")
    if not isinstance(packet, Mapping) or not isinstance(profile_ref, Mapping):
        raise TypeError("packet and profile_ref must be mappings")
    if evidence_package is not None and not isinstance(evidence_package, Mapping):
        raise TypeError("evidence_package must be a mapping or None")
    if not isinstance(reasoning_mode, str) or reasoning_mode not in {"enabled", "disabled"}:
        raise ValueError("reasoning_mode must be enabled or disabled")
    provider, model, adapter = _provider_identity(provider=provider, model=model, adapter=adapter)
    if provider == OPENAI_PROVIDER and reasoning_mode != "enabled":
        raise ValueError("OpenAI GPT-5.6 Luna requires reasoning_mode=enabled")
    sections: list[str] = []
    sections.extend(_header(view))
    sections.append("")
    sections.extend(_strategy_section(packet, profile_ref, evidence_package))
    sections.append("")
    sections.extend(_market_section(view))
    sections.append("")
    sections.append("OUTPUT DIRECTIVE")
    sections.append(_OUTPUT_DIRECTIVE)
    return _INSTRUCTIONS, "\n".join(sections)


def build_analysis_request(
    *,
    strategy_id: str,
    packet: Mapping[str, Any],
    profile_ref: Mapping[str, Any],
    market_view: Mapping[str, Any],
    reasoning_mode: str,
    provider: str | None = None,
    model: str | None = None,
    adapter: str | None = None,
    max_output_tokens: int = 4096,
    evidence_package: Mapping[str, Any] | None = None,
    analysis_request: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build one validated, boundary-safe normalized model request.

    ``analysis_request`` carries the finalized G014 analysis-request lineage
    artifact (real id/hash) required by the G018 finalizer; when omitted,
    placeholder refs keep the request schema-valid for prompt-only use.
    """
    if not isinstance(reasoning_mode, str) or reasoning_mode not in {"enabled", "disabled"}:
        raise ValueError("reasoning_mode must be enabled or disabled")
    if isinstance(max_output_tokens, bool) or not isinstance(max_output_tokens, int) or max_output_tokens < 1:
        raise ValueError("max_output_tokens must be a positive integer")
    provider, model, adapter = _provider_identity(provider=provider, model=model, adapter=adapter)
    if provider == OPENAI_PROVIDER and reasoning_mode != "enabled":
        raise ValueError("OpenAI GPT-5.6 Luna requires reasoning_mode=enabled")
    instructions, context_text = build_analysis_prompt(
        strategy_id=strategy_id,
        packet=packet,
        profile_ref=profile_ref,
        market_view=market_view,
        reasoning_mode=reasoning_mode,
        provider=provider,
        model=model,
        adapter=adapter,
        evidence_package=evidence_package,
    )
    view = _mapping(market_view, "market_view")
    snapshot = view.get("closed_snapshot")
    snapshot = snapshot if isinstance(snapshot, Mapping) else {}
    frame = view.get("observation_frame")
    frame = frame if isinstance(frame, Mapping) else None
    latest = view.get("latest_second")
    latest = latest if isinstance(latest, Mapping) else {}
    request = make_normalized_model_request(
        provider=provider,
        model=model,
        adapter=adapter,
        requested_capabilities=("structured_json", "reasoning_mode"),
        prompt_template_version=PROMPT_TEMPLATE_VERSION,
        instructions=instructions,
        context_text=context_text,
        context_refs=_context_refs(snapshot, frame, evidence_package, analysis_request),
        response_schema_version=SCHEMA_VERSION,
        parameters={"temperature": "0", "max_output_tokens": max_output_tokens},
        extensions={
            "reasoning_mode": reasoning_mode,
            "strategy_profile_ref": copy.deepcopy(dict(profile_ref)),
            "market_data_usage": MARKET_DATA_USAGE,
            "no_live_orders": True,
            "context_refs": _extension_context_refs(view, latest, packet),
        },
        reasoning_mode=reasoning_mode,
    )
    assert_model_boundary_safe(request, context="model_request")
    if not profile_ref_intact(request, profile_ref):
        raise ModelBoundaryError(
            "profile_ref_mismatch" if _find_request_profile_ref(request) is not None else "profile_ref_missing",
            "model request must carry the intact strategy profile reference",
        )
    return request


def _find_request_profile_ref(request: Mapping[str, Any]) -> Mapping[str, Any] | None:
    extensions = request.get("extensions")
    if isinstance(extensions, Mapping) and isinstance(extensions.get("strategy_profile_ref"), Mapping):
        return extensions["strategy_profile_ref"]
    candidate = request.get("strategy_profile_ref")
    return candidate if isinstance(candidate, Mapping) else None


__all__ = [
    "MARKET_DATA_USAGE",
    "PROMPT_TEMPLATE_VERSION",
    "PROMPT_VERSION",
    "PROVISIONAL_NOTE",
    "build_analysis_prompt",
    "build_analysis_request",
]
