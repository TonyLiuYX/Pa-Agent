"""Shared deterministic fixtures for the G028 automatic runtime tests.

The builders compose the accepted upstream components exactly like the live
runtime: real G019 history snapshots, real execution-frozen G020 seconds, the
real G021 shared view through the feed session, real G016 packets/evidence
and real G017 ``AdapterOutcome`` values.  Everything is deterministic: fake
clock, scripted adapters and canned transport results, no sockets.
"""

from __future__ import annotations

import copy
import hashlib
import importlib.util
from datetime import datetime, timezone
from dataclasses import replace
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

from pa_engine.market import ALPACA_IEX_FEED, MarketFeedRef
from pa_engine.providers import (
    AdapterOutcome,
    make_model_call_metadata,
    make_normalized_model_response,
    normalize_error,
    request_semantic_identity,
)
from pa_engine.strategy.decision import (
    make_model_output_ref,
    make_second_event_ref,
    map_candidate_to_normalized_model_candidate,
)
from pa_engine.strategy.runtime.clock import FakeClock

ROOT = Path(__file__).resolve().parents[5]
FEED_BUILDERS = ROOT / "tests" / "fixtures" / "phase2" / "g028" / "feed" / "builders.py"

UTC = timezone.utc

SESSION_OPEN = "2026-03-09T13:30:00Z"
CUTOVER = "2026-03-09T13:33:00Z"
TIMEFRAME = {"unit": "minute", "multiplier": 1}

HISTORY_VENDOR_BARS = [
    {"t": "2026-03-09T13:30:00Z", "o": 100.0, "h": 101.25, "l": 99.5, "c": 100.75, "v": 1000},
    {"t": "2026-03-09T13:31:00Z", "o": 100.75, "h": 101.5, "l": 100.5, "c": 101.25, "v": 1200},
    {"t": "2026-03-09T13:32:00Z", "o": 101.25, "h": 101.75, "l": 100.75, "c": 101.0, "v": 1300},
]

# --------------------------------------------------------------------------
# G019/G020/G021 builders (loaded from the Round-1 feed fixture module)
# --------------------------------------------------------------------------


def _feed_builders() -> Any:
    spec = importlib.util.spec_from_file_location("g028_feed_builders", FEED_BUILDERS)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


B = _feed_builders()


def history_result(symbol: str = "AAPL", *, end_at: str = CUTOVER, start_at: str = SESSION_OPEN, bars: Iterable[Mapping[str, Any]] | None = None, snapshot: Mapping[str, Any] | None = None) -> Any:
    """A canonical G019 history result ending at *end_at*.

    ``snapshot`` bypasses the G019 conversion so tampering fixtures can
    supply a forged snapshot with the same request envelope.
    """
    if snapshot is None:
        snapshot = B.history_snapshot(symbol=symbol, start_at=start_at, end_at=end_at, bars=list(bars if bars is not None else HISTORY_VENDOR_BARS))
    return B.history_result(snapshot, start_at=start_at, end_at=end_at)


def empty_history_result(start_at: str = SESSION_OPEN, end_at: str = CUTOVER) -> Any:
    """A G019 result whose snapshot carries no closed bars."""
    payload = {"bars": {}}
    from pa_engine.market_sources.alpaca.history import AlpacaHistoryAdapter, RecordedTransport, TransportResponse

    adapter = AlpacaHistoryAdapter(key_id="fixture", secret_key="fixture", transport=RecordedTransport([TransportResponse(200, payload)]))
    result = adapter.fetch_history(symbol="AAPL", start_at=start_at, end_at=end_at, timeframe="1Min")
    return B.history_result(result.snapshot, start_at=start_at, end_at=end_at)


def recovery_result(*, start_at: str, end_at: str, bars: Iterable[Any]) -> Any:
    result = B.history_result(None, start_at=start_at, end_at=end_at, recovery_bars=tuple(bars))
    # The feed fixture's convenient default hash is a placeholder.  Bind it
    # to this concrete request so the runtime's strict result/bar lineage
    # check exercises the same contract as the live G019 adapter.  Explicit
    # hashes (including malformed/mixed adversarial values) remain untouched.
    placeholder = hashlib.sha256(b"g028-feed-recovery").hexdigest()
    normalized = tuple(
        replace(bar, source_request_hash=result.source_request_hash)
        if getattr(bar, "source_request_hash", None) == placeholder
        else bar
        for bar in result.recovery_bars
    )
    return replace(result, recovery_bars=normalized)


def _canonical_decimal_text(value: Any) -> str:
    from decimal import Decimal

    text = format(Decimal(str(value)), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def recovery_bar(*, start_at: str, end_at: str, low: str = "9.5", high: str = "10.5", close: str | None = None, open: str | None = None, complete: bool = True, source_request_hash: str | None = None) -> Any:
    """One real ``RecoveryBar 0.1.0`` minute value with consistent OHLC."""
    from decimal import Decimal

    if close is None:
        midpoint = (Decimal(low) + Decimal(high)) / 2
        close = _canonical_decimal_text(midpoint)
    open_value = open if open is not None else close
    return B.recovery_bar(
        start_at=start_at,
        end_at=end_at,
        open=open_value,
        high=high,
        low=low,
        close=close,
        volume="100",
        complete=complete,
        source_request_hash=source_request_hash,
    )


def frozen_second(*, symbol: str = "AAPL", start_at: str, end_at: str, close: str = "10.05", open: str | None = None, high: str | None = None, low: str | None = None, watermark: str | None = None, quality: Mapping[str, Any] | None = None) -> Any:
    """One real execution-frozen G020 second (``watermark`` defaults to end+3s)."""
    open_v = open if open is not None else close
    high_v = high if high is not None else max(open_v, close)
    low_v = low if low is not None else min(open_v, close)
    if watermark is None:
        from pa_engine.market_sources.alpaca.stream import execution_watermark

        watermark = execution_watermark(start_at, end_at, end_at)
    return B.frozen_second(
        symbol=symbol,
        start_at=start_at,
        end_at=end_at,
        open=open_v,
        high=high_v,
        low=low_v,
        close=close,
        volume="1",
        contributor_ids=(f"trade-{end_at}",),
        watermark=watermark,
        quality=quality,
    )


def sip_event(*, symbol: str = "AAPL", start_at: str, end_at: str, close: str = "10.05") -> Any:
    """A frozen second whose feed identity was forged to consolidated SIP."""
    event = frozen_second(symbol=symbol, start_at=start_at, end_at=end_at, close=close)
    object.__setattr__(event, "market_feed", MarketFeedRef("0.1.0", "alpaca", "sip", "consolidated"))
    return event


def gap_flagged_second(*, symbol: str = "AAPL", start_at: str, end_at: str, close: str = "10.05") -> Any:
    """A frozen second carrying an explicit connection-gap quality marker."""
    return frozen_second(
        symbol=symbol,
        start_at=start_at,
        end_at=end_at,
        close=close,
        quality={"status": "ok", "codes": ["connection_gap"], "connection_gap": True},
    )


class FakeHistoryAdapter:
    """Canned G019-style adapter with recorded history/recovery calls."""

    def __init__(self, *, history_result: Any = None, recovery_result: Any = None, recovery_error: Exception | None = None) -> None:
        self.history_result = history_result
        self.recovery_result = recovery_result
        self.recovery_error = recovery_error
        self.calls: list[dict[str, Any]] = []

    def fetch_history(self, *, symbol: str | None = None, start_at: Any = None, end_at: Any = None, timeframe: Any = None, limit: int | None = None, **kwargs: Any) -> Any:
        self.calls.append({"operation": "history", "symbol": symbol, "start_at": start_at, "end_at": end_at, "timeframe": timeframe, "limit": limit})
        return self.history_result

    def fetch_recovery(self, *, symbol: str | None = None, start_at: Any = None, end_at: Any = None, limit: int | None = None, **kwargs: Any) -> Any:
        self.calls.append({"operation": "recovery", "symbol": symbol, "start_at": start_at, "end_at": end_at, "limit": limit})
        if self.recovery_error is not None:
            raise self.recovery_error
        return self.recovery_result


class FakeAggregator:
    """Scripted ``freeze()`` batches of real frozen seconds."""

    def __init__(self, *, symbol: str = "AAPL", market_feed: MarketFeedRef = ALPACA_IEX_FEED, batches: Iterable[Iterable[Any]] = ()) -> None:
        self.symbol = symbol
        self.market_feed = market_feed
        self._batches = [list(batch) for batch in batches]
        self.freeze_calls: list[Any] = []
        self.gaps: list[Mapping[str, Any]] = []

    def freeze(self, *, now: Any = None) -> tuple[Any, ...]:
        self.freeze_calls.append(now)
        return tuple(self._batches.pop(0) if self._batches else ())

    def mark_connection_gap(self, start_at: str, end_at: str | None, *, reason: str = "disconnect") -> None:
        self.gaps.append({"start_at": start_at, "end_at": end_at, "reason": reason})

    def close_connection_gap(self, end_at: str) -> None:
        for index in range(len(self.gaps) - 1, -1, -1):
            if self.gaps[index]["end_at"] is None:
                self.gaps[index]["end_at"] = end_at
                return


class FakeStreamClient:
    """Scripted stream client used by start/stop-stream tests."""

    def __init__(self, fail_on: str | None = None) -> None:
        self.fail_on = fail_on
        self.connect_calls = 0
        self.close_calls = 0

    def connect(self) -> None:
        self.connect_calls += 1
        if self.fail_on == "connect":
            raise RuntimeError("connect failed")

    def close(self) -> None:
        self.close_calls += 1
        if self.fail_on == "close":
            raise RuntimeError("close failed")


class StorageClock:
    """Adapter turning the G023 ``FakeClock`` into the storage callable clock."""

    def __init__(self, clock: FakeClock) -> None:
        self._clock = clock

    def __call__(self) -> datetime:
        return self._clock.now()

    def advance(self, seconds: int = 1) -> None:
        self._clock.advance(seconds)


# --------------------------------------------------------------------------
# G017 adapter outcomes
# --------------------------------------------------------------------------


def success_outcome(request: Mapping[str, Any], candidate: Mapping[str, Any], *, profile_ref: Mapping[str, Any] | None = None, generation: int | None = None) -> AdapterOutcome:
    """Build the real ``AdapterOutcome`` for one parsed G018 candidate."""
    parsed = copy.deepcopy(dict(candidate))
    normalized = map_candidate_to_normalized_model_candidate(parsed, profile_ref=profile_ref)
    extensions: dict[str, Any] = {"g018_model_output_ref": make_model_output_ref(parsed)}
    if profile_ref is not None:
        extensions["strategy_profile_ref"] = copy.deepcopy(dict(profile_ref))
    response = make_normalized_model_response(
        request,
        status="completed",
        candidate=normalized,
        finish_reason="stop",
        generated_at="2026-03-09T13:34:05Z",
        extensions=extensions,
    )
    metadata = make_model_call_metadata(request, response, started_at="2026-03-09T13:34:04Z", completed_at="2026-03-09T13:34:05Z", latency_ms=1)
    return AdapterOutcome(
        response=response,
        call_metadata=metadata,
        classification="success",
        request_semantic_hash=request_semantic_identity(request),
        retryable=False,
        generation=generation,
    )


def error_outcome(request: Mapping[str, Any], classification: str, *, generation: int | None = None, message: str | None = None) -> AdapterOutcome:
    """Build a typed terminal/retryable ``AdapterOutcome`` for one request."""
    error = normalize_error(classification, message=message or classification, secret_values=())
    response = make_normalized_model_response(request, status="error", error=error)
    metadata = make_model_call_metadata(request, response)
    return AdapterOutcome(
        response=response,
        call_metadata=metadata,
        classification=classification,
        request_semantic_hash=request_semantic_identity(request),
        retryable=bool(error.get("retryable")),
        generation=generation,
    )


class ScriptedAdapter:
    """Queue of canned responses (outcomes or callables returning outcomes).

    Each ``complete`` call consumes exactly one script item; an exhausted
    script raises so tests detect unexpected provider calls.  Items may be a
    ``{strategy_id: [items...]}`` mapping when per-strategy scripts are
    needed (each strategy consumes its own queue).
    """

    def __init__(self, script: Iterable[Any] = ()) -> None:
        if isinstance(script, Mapping):
            self._by_strategy = {str(key): list(value) for key, value in script.items()}
            self.script = []
        else:
            self._by_strategy = {}
            self.script = list(script)
        self.requests: list[dict[str, Any]] = []
        self.generations: list[int | None] = []

    @property
    def call_count(self) -> int:
        return len(self.requests)

    def _pop_item(self, strategy_id: str) -> Any:
        queue = self._by_strategy.get(strategy_id)
        if queue is not None:
            if not queue:
                raise AssertionError(f"adapter script exhausted for {strategy_id}")
            return queue.pop(0)
        if not self.script:
            raise AssertionError("adapter script exhausted")
        return self.script.pop(0)

    def complete(self, request: Mapping[str, Any], *, cancelled: bool = False, generation: int | None = None) -> AdapterOutcome:
        self.requests.append(copy.deepcopy(dict(request)))
        self.generations.append(generation)
        extensions = request.get("extensions") if isinstance(request.get("extensions"), Mapping) else {}
        profile = extensions.get("strategy_profile_ref") if isinstance(extensions.get("strategy_profile_ref"), Mapping) else {}
        strategy_id = str(profile.get("strategy_id") or request.get("context_refs", {}).get("strategy_id") or "")
        item = self._pop_item(strategy_id)
        if callable(item):
            return item(request, generation=generation)
        return item


# --------------------------------------------------------------------------
# G016 grounding helpers: valid candidates from the authoritative packet
# --------------------------------------------------------------------------


def _claim_grounding(packet: Mapping[str, Any], evidence: Mapping[str, Any], *, claim_field: str, market_refs: list[str], declaration: str | None = None) -> dict[str, Any]:
    claims = packet.get("claims", {})
    candidates = [
        claim
        for claim in claims.get(claim_field, [])
        if claim.get("declaration_type") != "source_not_stated" and (declaration is None or claim.get("declaration_type") == declaration)
    ]
    if not candidates:
        raise AssertionError(f"packet has no usable claims for field {claim_field}")
    claim = candidates[0]
    selected = claim.get("declaration_type")
    items = [item for item in evidence.get("evidence_items", []) if item.get("claim_id") == claim["claim_id"] and item.get("declaration_type") == selected]
    if not items:
        raise AssertionError(f"evidence package has no {selected} item for claim {claim['claim_id']}")
    item = items[0]
    citations = [citation for citation in evidence.get("citations", []) if citation.get("claim_id") == claim["claim_id"] and citation.get("declaration_type") == selected]
    if not citations:
        raise AssertionError(f"evidence package has no citation for claim {claim['claim_id']}")
    citation_id = str(citations[0]["citation_id"])
    return {
        "declaration_type": selected,
        "claim_ids": [str(claim["claim_id"])],
        "evidence_ids": [str(item["evidence_id"])],
        "citation_ids": [citation_id],
        "market_refs": list(market_refs[:1]),
    }


def grounded_candidate(
    registry: Any,
    strategy_id: str,
    view: Any,
    *,
    branch: str,
    direction: str,
    entry: str | None = None,
    stop: str | None = None,
    target: str | None = None,
    n: int | None = None,
    x: int | None = None,
    second_event_ref: Mapping[str, Any] | None = None,
    reason_text: str = "grounded runtime decision",
    uncertainty: Mapping[str, Any] | None = None,
    confidence: str = "0.8",
) -> dict[str, Any]:
    """Build a candidate whose grounding references the real packet/evidence.

    The market references are the actual ``bar_id`` values of the shared
    view's closed snapshot, so the G018 finalizer accepts the decision.
    """
    loaded = registry.load_packet(strategy_id)
    if loaded.code != "ok":
        raise AssertionError(f"packet load failed: {loaded.code}")
    packet = loaded.packet
    evidence = loaded.evidence_package
    snapshot = view.closed_snapshot
    market_refs = [str(bar["bar_id"]) for bar in snapshot.get("bars", []) if isinstance(bar, Mapping) and bar.get("bar_id")]
    if not market_refs:
        raise AssertionError("view snapshot carries no bars")
    grounding = {field: None for field in ("reason", "entry_price", "stop_price", "target_price")}
    if branch != "no_entry":
        grounding["reason"] = _claim_grounding(packet, evidence, claim_field="market_context", market_refs=market_refs)
        grounding["stop_price"] = _claim_grounding(packet, evidence, claim_field="invalidation_stop", market_refs=market_refs, declaration="derived_interpretation")
        grounding["target_price"] = _claim_grounding(packet, evidence, claim_field="targets_exit", market_refs=market_refs, declaration="derived_interpretation")
        if branch == "wait_for_price":
            grounding["entry_price"] = _claim_grounding(packet, evidence, claim_field="entry", market_refs=market_refs, declaration="derived_interpretation")
    return {
        "schema_version": "0.1.0",
        "branch": branch,
        "reason": reason_text if branch != "no_entry" else None,
        "direction": direction if branch != "no_entry" else None,
        "entry_price": entry,
        "stop_price": stop,
        "target_price": target,
        "N": n,
        "X": x,
        "grounding": grounding,
        "confidence": confidence if branch != "no_entry" else None,
        "uncertainty": dict(uncertainty or {"level": "low", "codes": [], "details": "deterministic fixture"}),
        "shared_market_view_ref": copy.deepcopy(dict(view.to_decision_ref())),
        "second_event_ref": copy.deepcopy(dict(second_event_ref)) if second_event_ref is not None else None,
    }


def second_ref_of(view: Any) -> dict[str, str]:
    """The G018 trusted second-event reference for the view's frozen second."""
    latest = view.latest_second
    return make_second_event_ref(latest["event_id"], latest["event_hash"], latest["close"])


class TamperingRegistry:
    """G016 registry wrapper that returns tampered packets on demand.

    Used by the AC9/AC10 fixtures: ``tamper`` injects nested account-state
    fields into a packet so the engine boundary must reject the request
    before any transport work; ``load_error`` fails the authoritative load
    for the strategies in ``only`` (all by default).
    """

    def __init__(self, inner: Any, *, tamper: Callable[[dict[str, Any]], dict[str, Any]] | None = None, load_error: str | None = None, only: set[str] | None = None) -> None:
        self._inner = inner
        self._tamper = tamper
        self._load_error = load_error
        self._only = only

    @property
    def strategy_ids(self) -> list[str]:
        return self._inner.strategy_ids

    def load_packet(self, strategy_id: str, expected_profile_ref: Mapping[str, Any] | None = None) -> Any:
        from pa_engine.strategies.knowledge import PacketLoadOutcome

        if self._load_error is not None and (self._only is None or strategy_id in self._only):
            return PacketLoadOutcome(self._load_error, self._load_error, {"strategy_id": strategy_id}, False, None, None, None)
        outcome = self._inner.load_packet(strategy_id, expected_profile_ref=expected_profile_ref)
        if outcome.code != "ok" or self._tamper is None or (self._only is not None and strategy_id not in self._only):
            return outcome
        packet = copy.deepcopy(dict(outcome.packet))
        packet = self._tamper(packet)
        return PacketLoadOutcome("ok", "strategy packet loaded (tampered fixture)", {"cache_key": ("tampered", strategy_id)}, True, packet, outcome.profile_ref, outcome.evidence_package)

    def validate_packet(self, packet: Mapping[str, Any]) -> Any:
        return self._inner.validate_packet(packet)

    def validate_profile_lineage(self, profile_ref: Mapping[str, Any], *, packet: Mapping[str, Any] | None = None, artifacts: Iterable[Mapping[str, Any]] = ()) -> Any:
        return self._inner.validate_profile_lineage(profile_ref, packet=packet, artifacts=artifacts)


def nested_balance_tamper(packet: dict[str, Any]) -> dict[str, Any]:
    extensions = dict(packet.get("extensions", {}))
    nested = dict(extensions.get("nested_fixture", {}))
    nested["balance"] = "1000.00"
    nested["quantity"] = "5"
    extensions["nested_fixture"] = nested
    packet["extensions"] = extensions
    return packet


def fixture_index() -> dict[str, Any]:
    return {
        "name": "g028-engine-fixtures",
        "description": "Deterministic builders for the G028 automatic runtime tests",
        "entries": [{"path": "builders.py", "kind": "python-module"}],
    }
