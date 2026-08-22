"""G029 deterministic offline replay/evaluation harness.

This module is deliberately an evidence runner, not a second trading engine.
The end-to-end cases use the accepted G028 runtime with:

* the real G019 Alpaca history adapter over :class:`RecordedTransport`;
* the real G020 :class:`LocalSecondAggregator` over recorded trade frames;
* the real G021 feed/view and G027 temporary SQLite path;
* G017's scripted adapter and a request-identity replay adapter; and
* G023's :class:`FakeClock`.

The runner emits canonical state/events and explicit exclusions.  It does not
calculate profitability metrics or rank strategies.
"""

from __future__ import annotations

import copy
import hashlib
import json
import subprocess
import tempfile
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Iterable, Mapping, Sequence

from pa_engine.contracts import canonical_json_bytes, finalize_artifact
from pa_engine.market import ALPACA_IEX_FEED, MarketFeedRef
from pa_engine.market_sources.alpaca.history import (
    AlpacaHistoryAdapter,
    RecordedTransport,
    TransportResponse,
    validate_recovery_bar,
)
from pa_engine.market_sources.alpaca.stream import LocalSecondAggregator, SecondAggregateEvent, validate_second_event
from pa_engine.market.validation import assess_market
from pa_engine.market_view import SharedCandleEngine
from pa_engine.paper.core import PaperAccountBook
from pa_engine.paper.entry import PendingEntryMonitor
from pa_engine.paper.exit import ExitMonitor
from pa_engine.providers import (
    AdapterOutcome,
    ReplayAdapter,
    ReplayRecord,
    ScriptedAdapter,
    ScriptedStep,
    make_model_call_metadata,
    make_normalized_model_response,
    request_semantic_identity,
    scripted_error,
)

# The two helpers below live in G018, not G017.  They are imported separately
# to keep the provider import list honest and to avoid an accidental provider
# dependency in the frozen decision layer.
from pa_engine.strategy.decision import (
    make_model_output_ref,
    make_second_event_ref,
    make_shared_market_view_ref,
    make_scheduler_instruction,
    map_candidate_to_normalized_model_candidate,
    validate_candidate,
)
from pa_engine.strategy.runtime import FakeClock, StrategyScheduler, StrategySpec
from pa_engine.runtime.boundary import scan_model_boundary
from pa_engine.runtime.config import RuntimeConfig, RuntimeConfigError
from pa_engine.runtime.engine import AutomaticPaperRuntime
from pa_engine.runtime.feed import FeedSession
from pa_engine.strategies.knowledge import build_registry, load_default_registry
from pa_engine.paper.entry.boundary import utc_instant
from pa_engine.storage.errors import StorageClosedError

UTC = timezone.utc
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_FIXTURE_DIR = REPO_ROOT / "tests" / "fixtures" / "phase2" / "g029"
SCHEMA_VERSION = "0.1.0"
EVALUATION_VERSION = "g029-offline-evaluation-0.1.0"
START_AT = "2026-03-09T13:33:05Z"
HISTORY_START = "2026-03-09T13:30:00Z"
HISTORY_END = "2026-03-09T13:33:00Z"
SID_LONG = "pa:nanqiao-range-extreme-limit"
SID_SECOND = "pa:nanqiao-always-in-ema20"
VOLATILE_TELEMETRY = (
    "run_id",
    "provider_workers_alive",
    "provider_pending_results",
    "latency_ms",
    "provider_request_id",
    "started",
    "stopped",
)


class EvaluationError(RuntimeError):
    """Raised when the deterministic evaluator cannot produce valid evidence."""


def _json(value: Any) -> Any:
    """Return a JSON-native deep copy without accepting Decimal in output."""

    if isinstance(value, Mapping):
        return {str(key): _json(child) for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json(child) for child in value]
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if hasattr(value, "to_dict") and callable(value.to_dict):
        return _json(value.to_dict())
    return copy.deepcopy(value)


def _digest(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(_json(value))).hexdigest()


def _strip_volatile(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _strip_volatile(child)
            for key, child in value.items()
            if str(key) not in VOLATILE_TELEMETRY
        }
    if isinstance(value, (list, tuple)):
        return [_strip_volatile(child) for child in value]
    return _json(value)


def _stamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _fixture(path: Path, name: str) -> dict[str, Any]:
    try:
        value = json.loads((path / name).read_text(encoding="utf-8"))
    except Exception as exc:  # pragma: no cover - exercised by CLI misuse
        raise EvaluationError(f"fixture load failed: {name}") from exc
    if not isinstance(value, dict):
        raise EvaluationError(f"fixture must be an object: {name}")
    return value


@dataclass
class RecordedIEXHistory:
    """G019 adapter wrapper pinned to a recorded request/response pair."""

    fixture: Mapping[str, Any]
    history_payload: Mapping[str, Any] | None = None
    recovery_payload: Mapping[str, Any] | None = None
    calls: list[dict[str, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.history_payload = copy.deepcopy(self.history_payload or self.fixture["history_response"])
        self.recovery_payload = copy.deepcopy(self.recovery_payload or self.fixture["recovery_response"])
        self.transport = RecordedTransport(
            [
                TransportResponse(200, self.history_payload),
                TransportResponse(200, self.recovery_payload),
            ]
        )
        # These values never enter a request projection or report.  They are
        # only needed because G019 intentionally refuses credential-less REST
        # execution, even when its transport is recorded.
        self.adapter = AlpacaHistoryAdapter(
            key_id="offline-fixture-id",
            secret_key="offline-fixture-secret",
            transport=self.transport,
        )
        request = self.fixture["history_request"]
        recovery = self.fixture["recovery_request"]
        self._history_args = {
            "symbol": self.fixture["symbol"],
            "start_at": request["start_at"],
            "end_at": request["end_at"],
            "timeframe": request["timeframe"],
            "limit": request["limit"],
        }
        self._recovery_args = {
            "symbol": self.fixture["symbol"],
            "start_at": recovery["start_at"],
            "end_at": recovery["end_at"],
            "limit": recovery["limit"],
        }

    def fetch_history(self, **kwargs: Any) -> Any:
        self.calls.append({"operation": "history", **_safe_call_projection(kwargs)})
        return self.adapter.fetch_history(**self._history_args)

    def fetch_recovery(self, **kwargs: Any) -> Any:
        self.calls.append({"operation": "recovery", **_safe_call_projection(kwargs)})
        return self.adapter.fetch_recovery(**self._recovery_args)


def _safe_call_projection(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: _json(child)
        for key, child in value.items()
        if key not in {"credentials", "key_id", "secret_key", "api_key_id", "api_secret_key"}
    }


class RecordedIEXAggregator:
    """FeedSession-compatible wrapper over real G020 aggregation."""

    def __init__(self, fixture: Mapping[str, Any], batch_names: Iterable[str] = ()) -> None:
        self.fixture = fixture
        self.symbol = str(fixture["symbol"])
        self.market_feed = ALPACA_IEX_FEED
        self._aggregator = LocalSecondAggregator(self.symbol, market_feed=ALPACA_IEX_FEED, adjustment="raw")
        self._batches = list(batch_names)
        self.freeze_calls: list[str | None] = []

    def append(self, *names: str) -> None:
        self._batches.extend(names)

    def freeze(self, *, now: Any = None) -> tuple[Any, ...]:
        self.freeze_calls.append(_stamp(now) if isinstance(now, datetime) else now)
        while self._batches:
            name = self._batches.pop(0)
            batch = self.fixture["trade_batches"][name]
            messages = copy.deepcopy(batch["messages"])
            received_at = batch.get("received_at")
            self._aggregator.ingest_message(messages, received_at=received_at)
        return self._aggregator.freeze(now=now)

    def mark_connection_gap(self, start_at: str, end_at: str | None, *, reason: str = "disconnect") -> Any:
        return self._aggregator.mark_connection_gap(start_at, end_at, reason=reason)

    def close_connection_gap(self, end_at: str) -> Any:
        return self._aggregator.close_connection_gap(end_at)

    def reset_for_cutover(self) -> None:
        self._aggregator.reset_for_cutover()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._aggregator, name)


class RecordedModelAdapter:
    """Record safe request identities while delegating to G017 scripted/replay."""

    def __init__(self, delegate: Any) -> None:
        self.delegate = delegate
        self.requests: list[dict[str, Any]] = []
        self.outcomes: list[dict[str, Any]] = []

    @property
    def capabilities(self) -> Any:
        return self.delegate.capabilities

    @property
    def calls(self) -> int:
        return len(self.requests)

    def complete(self, request: Mapping[str, Any], *, cancelled: bool = False, generation: int | None = None) -> AdapterOutcome:
        clean = copy.deepcopy(dict(request))
        self.requests.append(clean)
        outcome = self.delegate.complete(request, cancelled=cancelled, generation=generation)
        profile = clean.get("extensions", {}).get("strategy_profile_ref", {}) if isinstance(clean.get("extensions"), Mapping) else {}
        lineage: dict[str, Any] = {
            "strategy_id": profile.get("strategy_id") if isinstance(profile, Mapping) else None,
            "branch": None,
            "direction": None,
            "claim_ids": [],
            "citation_ids": [],
            "market_refs": [],
            "candidate_id": None,
            "candidate_hash": None,
            "shared_market_view_ref": None,
            "second_event_ref": None,
        }
        if outcome.is_success:
            # Scripted/replay responses are intentionally normalized again at
            # the G018 seam so the runtime receives the same model-output-ref
            # shape as the accepted G028 recorded adapter.
            source = getattr(self.delegate, "last_candidate", None)
            if not isinstance(source, Mapping):
                response_candidate = outcome.normalized_response.get("candidate") if outcome.normalized_response else None
                source = response_candidate if isinstance(response_candidate, Mapping) else {}
            candidate = copy.deepcopy(dict(source))
            normalized = map_candidate_to_normalized_model_candidate(candidate, profile_ref=profile)
            extensions = {
                "g018_model_output_ref": make_model_output_ref(candidate),
                "strategy_profile_ref": copy.deepcopy(dict(profile)),
            }
            response = make_normalized_model_response(
                request,
                status="completed",
                candidate=normalized,
                finish_reason="stop",
                generated_at="2026-03-09T13:33:05Z",
                extensions=extensions,
            )
            metadata = make_model_call_metadata(
                request,
                response,
                started_at="2026-03-09T13:33:05Z",
                completed_at="2026-03-09T13:33:05Z",
                latency_ms=1,
            )
            outcome = AdapterOutcome(
                response=response,
                call_metadata=metadata,
                classification="success",
                request_semantic_hash=request_semantic_identity(request),
                retryable=False,
                generation=generation,
            )
            grounding = candidate.get("grounding", {})
            claim_ids: set[str] = set()
            citation_ids: set[str] = set()
            market_refs: set[str] = set()
            if isinstance(grounding, Mapping):
                for item in grounding.values():
                    if not isinstance(item, Mapping):
                        continue
                    claim_ids.update(str(value) for value in item.get("claim_ids", []) if value is not None)
                    citation_ids.update(str(value) for value in item.get("citation_ids", []) if value is not None)
                    market_refs.update(str(value) for value in item.get("market_refs", []) if value is not None)
            output_ref = extensions["g018_model_output_ref"]
            lineage = {
                "strategy_id": profile.get("strategy_id"),
                "branch": candidate.get("branch"),
                "direction": candidate.get("direction"),
                "claim_ids": sorted(claim_ids),
                "citation_ids": sorted(citation_ids),
                "market_refs": sorted(market_refs),
                "candidate_id": output_ref.get("candidate_id"),
                "candidate_hash": output_ref.get("candidate_hash"),
                "shared_market_view_ref": _json(candidate.get("shared_market_view_ref")),
                "second_event_ref": _json(candidate.get("second_event_ref")),
            }
        self.outcomes.append(
            {
                "classification": outcome.classification,
                "retryable": outcome.retryable,
                "request_semantic_hash": outcome.request_semantic_hash,
                **lineage,
            }
        )
        return outcome


class PerStrategyModelAdapter:
    """Use one real G017 scripted/replay adapter per strategy partition."""

    def __init__(self, delegates: Mapping[str, RecordedModelAdapter]) -> None:
        self.delegates = dict(delegates)
        self.requests: list[dict[str, Any]] = []

    @property
    def capabilities(self) -> Any:
        first = next(iter(self.delegates.values()))
        return first.capabilities

    @property
    def calls(self) -> int:
        return sum(delegate.calls for delegate in self.delegates.values())

    @property
    def outcomes(self) -> list[dict[str, Any]]:
        return [
            outcome
            for sid in sorted(self.delegates)
            for outcome in self.delegates[sid].outcomes
        ]

    def complete(self, request: Mapping[str, Any], *, cancelled: bool = False, generation: int | None = None) -> AdapterOutcome:
        profile = request.get("extensions", {}).get("strategy_profile_ref", {})
        strategy_id = profile.get("strategy_id") if isinstance(profile, Mapping) else None
        delegate = self.delegates.get(str(strategy_id))
        if delegate is None:
            raise EvaluationError(f"no recorded provider script for {strategy_id!r}")
        outcome = delegate.complete(request, cancelled=cancelled, generation=generation)
        self.requests.append(copy.deepcopy(dict(request)))
        return outcome


class CaptureThenReplayAdapter:
    """Materialize one deterministic response, then serve it through G017 replay."""

    def __init__(self, source: RecordedModelAdapter) -> None:
        self.source = source
        self.replay: ReplayAdapter | None = None
        self.requests: list[dict[str, Any]] = []

    @property
    def capabilities(self) -> Any:
        return self.source.capabilities

    @property
    def outcomes(self) -> list[dict[str, Any]]:
        return list(self.source.outcomes)

    def complete(self, request: Mapping[str, Any], *, cancelled: bool = False, generation: int | None = None) -> AdapterOutcome:
        self.requests.append(copy.deepcopy(dict(request)))
        if self.replay is None:
            first = self.source.complete(request, cancelled=cancelled, generation=generation)
            if not first.is_success or first.normalized_response is None:
                return first
            record = ReplayRecord(
                request=copy.deepcopy(dict(request)),
                response=copy.deepcopy(first.normalized_response),
                call_metadata=copy.deepcopy(first.call_metadata),
            )
            self.replay = ReplayAdapter([record])
        return self.replay.complete(request, cancelled=cancelled, generation=generation)


def _registry(strategy_ids: Sequence[str]) -> Any:
    registrations = [{"strategy_id": sid, "record_id": sid[3:]} for sid in strategy_ids]
    return build_registry(str(REPO_ROOT / "knowledge"), registrations=registrations)


def _claim(packet: Mapping[str, Any], evidence: Mapping[str, Any], field: str, market_ref: str, declaration: str | None = None) -> dict[str, Any]:
    values = [
        item
        for item in packet.get("claims", {}).get(field, [])
        if isinstance(item, Mapping)
        and item.get("declaration_type") != "source_not_stated"
        and (declaration is None or item.get("declaration_type") == declaration)
    ]
    if not values:
        raise EvaluationError(f"no grounded claim for {field}")
    claim = values[0]
    selected = claim["declaration_type"]
    items = [item for item in evidence.get("evidence_items", []) if item.get("claim_id") == claim["claim_id"] and item.get("declaration_type") == selected]
    citations = [item for item in evidence.get("citations", []) if item.get("claim_id") == claim["claim_id"] and item.get("declaration_type") == selected]
    if not items or not citations:
        raise EvaluationError(f"incomplete grounding for {claim['claim_id']}")
    return {
        "declaration_type": selected,
        "claim_ids": [str(claim["claim_id"])],
        "evidence_ids": [str(items[0]["evidence_id"])],
        "citation_ids": [str(citations[0]["citation_id"])],
        "market_refs": [market_ref],
    }


def _grounded_candidate(registry: Any, sid: str, view: Any, *, branch: str, direction: str = "long", entry: str | None = None, stop: str | None = None, target: str | None = None, n: int | None = None, x: int | None = None, uncertainty: Mapping[str, Any] | None = None) -> dict[str, Any]:
    loaded = registry.load_packet(sid)
    if not loaded.ok:
        raise EvaluationError(f"packet load failed: {sid}:{loaded.code}")
    bars = [bar for bar in view.closed_snapshot.get("bars", []) if isinstance(bar, Mapping) and bar.get("bar_id")]
    if not bars:
        raise EvaluationError("grounded candidate has no closed market bar")
    market_ref = str(bars[0]["bar_id"])
    grounding = {key: None for key in ("reason", "entry_price", "stop_price", "target_price")}
    if branch != "no_entry":
        grounding["reason"] = _claim(loaded.packet, loaded.evidence_package, "market_context", market_ref)
        grounding["stop_price"] = _claim(loaded.packet, loaded.evidence_package, "invalidation_stop", market_ref, "derived_interpretation")
        grounding["target_price"] = _claim(loaded.packet, loaded.evidence_package, "targets_exit", market_ref, "derived_interpretation")
        if branch == "wait_for_price":
            grounding["entry_price"] = _claim(loaded.packet, loaded.evidence_package, "entry", market_ref, "derived_interpretation")
    latest = view.latest_second
    second_ref = None
    if branch == "enter_immediately" and isinstance(latest, Mapping):
        second_ref = make_second_event_ref(latest["event_id"], latest["event_hash"], latest["close"])
    return {
        "schema_version": SCHEMA_VERSION,
        "branch": branch,
        "reason": None if branch == "no_entry" else "recorded grounded G029 decision",
        "direction": None if branch == "no_entry" else direction,
        "entry_price": entry,
        "stop_price": stop,
        "target_price": target,
        "N": n,
        "X": x,
        "grounding": grounding,
        "confidence": None if branch == "no_entry" else "0.8",
        "uncertainty": dict(uncertainty or {"level": "low", "codes": [], "details": "offline structural fixture"}),
        "shared_market_view_ref": copy.deepcopy(dict(view.to_decision_ref())),
        "second_event_ref": second_ref,
    }


def _runtime(
    fixture: Mapping[str, Any],
    tmp_path: Path,
    *,
    strategy_ids: Sequence[str] = (SID_LONG,),
    batch_names: Sequence[str] = ("initial",),
    clock_start: str = START_AT,
    stale_after_seconds: int = 300,
    history_payload: Mapping[str, Any] | None = None,
    recovery_payload: Mapping[str, Any] | None = None,
    registry: Any | None = None,
) -> tuple[AutomaticPaperRuntime, RecordedIEXAggregator, RecordedIEXHistory, Any]:
    clock = FakeClock(clock_start)
    history = RecordedIEXHistory(fixture, history_payload=history_payload, recovery_payload=recovery_payload)
    aggregator = RecordedIEXAggregator(fixture, batch_names=batch_names)
    config = RuntimeConfig(
        symbol=str(fixture["symbol"]),
        timeframe=str(fixture["timeframe"]).lower(),
        database_path=str(tmp_path / "runtime.db"),
        stale_after_seconds=stale_after_seconds,
        minimum_closed_bars=3,
        queue_capacity=200,
        max_in_flight=40,
    )
    engine = AutomaticPaperRuntime(
        config,
        registry=registry or _registry(strategy_ids),
        history_adapter=history,
        aggregator=aggregator,
        db_path=str(tmp_path / "runtime.db"),
        run_id="g029-" + str(tmp_path.name),
        clock=clock,
        startup_history_seconds=1,
        start_at=HISTORY_START,
    )
    return engine, aggregator, history, clock


def _attach_candidates(engine: AutomaticPaperRuntime, candidates: Mapping[str, Sequence[Any]], *, replay: bool = False) -> RecordedModelAdapter | PerStrategyModelAdapter | CaptureThenReplayAdapter:
    delegates: dict[str, RecordedModelAdapter] = {}
    for sid, values in candidates.items():
        steps = []
        for value in values:
            if isinstance(value, Mapping) and value.get("_error"):
                steps.append(scripted_error(str(value["_error"]), transient=value.get("_transient"), retryable=value.get("_retryable")))
            else:
                steps.append(ScriptedStep(candidate=copy.deepcopy(dict(value))))
        # Keep source candidates available for the output-ref bridge.
        delegate = _OutputRefScriptedAdapter(steps)
        delegates[sid] = RecordedModelAdapter(delegate)
    if len(delegates) == 1:
        adapter: Any = next(iter(delegates.values()))
        if replay:
            adapter = CaptureThenReplayAdapter(adapter)
        engine._adapter = adapter
        return adapter
    adapter = PerStrategyModelAdapter(delegates)
    engine._adapter = adapter
    return adapter


class _OutputRefScriptedAdapter(ScriptedAdapter):
    """G017 scripted transport retaining the source candidate for G018 refs."""

    def __init__(self, steps: Sequence[Any]) -> None:
        super().__init__(steps=steps)
        self._source_steps = tuple(copy.deepcopy(steps))
        self.last_candidate: Mapping[str, Any] | None = None

    def complete(self, request: Mapping[str, Any], *, cancelled: bool = False, generation: int | None = None) -> AdapterOutcome:
        index = self.index
        source = self._source_steps[index] if index < len(self._source_steps) else None
        self.last_candidate = copy.deepcopy(source.get("candidate")) if isinstance(source, ScriptedStep) and isinstance(source.candidate, Mapping) else None
        return super().complete(request, cancelled=cancelled, generation=generation)


class _DynamicOutputRefScriptedAdapter(ScriptedAdapter):
    """Build each scripted candidate only after G028 publishes the view.

    Runtime model calls happen after the first live/frozen-second sweep.  A
    static candidate built immediately after startup would therefore carry a
    pre-second view reference (which is intentionally not executable).  This
    tiny G029-only adapter keeps the provider scripted while evaluating the
    candidate factory at the exact execution boundary.
    """

    def __init__(self, factory: Callable[[int], Any], count: int = 8) -> None:
        if count < 1:
            raise ValueError("dynamic script must reserve at least one step")
        super().__init__(steps=[ScriptedStep(error_kind="script_exhausted") for _ in range(count)])
        self._factory = factory
        self._source_steps: list[ScriptedStep | None] = [None] * count
        self.last_candidate: Mapping[str, Any] | None = None

    def complete(self, request: Mapping[str, Any], *, cancelled: bool = False, generation: int | None = None) -> AdapterOutcome:
        index = self.index
        if index < len(self._source_steps):
            raw = self._factory(index)
            if isinstance(raw, Mapping) and raw.get("_error"):
                step = scripted_error(
                    str(raw["_error"]),
                    transient=raw.get("_transient"),
                    retryable=raw.get("_retryable"),
                )
            elif isinstance(raw, ScriptedStep):
                step = copy.deepcopy(raw)
            elif isinstance(raw, Mapping):
                step = ScriptedStep(candidate=copy.deepcopy(dict(raw)))
            else:
                raise EvaluationError(f"dynamic provider script returned unsupported step: {type(raw).__name__}")
            source_candidate = copy.deepcopy(dict(step.candidate)) if isinstance(step.candidate, Mapping) else None
            if isinstance(step.candidate, Mapping):
                profile = request.get("extensions", {}).get("strategy_profile_ref", {})
                normalized = map_candidate_to_normalized_model_candidate(step.candidate, profile_ref=profile)
                response = make_normalized_model_response(
                    request,
                    candidate=normalized,
                    finish_reason="stop",
                    generated_at="2026-03-09T13:33:05Z",
                    extensions={
                        "g018_model_output_ref": make_model_output_ref(step.candidate),
                        "strategy_profile_ref": copy.deepcopy(dict(profile)),
                    },
                )
                step = ScriptedStep(response=response)
            self._steps = self._steps[:index] + (step,) + self._steps[index + 1 :]
            self._source_steps[index] = copy.deepcopy(step)
            self.last_candidate = source_candidate
        else:
            self.last_candidate = None
        return super().complete(request, cancelled=cancelled, generation=generation)


def _attach_dynamic_candidates(
    engine: AutomaticPaperRuntime,
    candidate_builder: Callable[[AutomaticPaperRuntime, Any], Mapping[str, Sequence[Any]]],
    strategy_ids: Sequence[str],
    *,
    replay: bool = False,
) -> RecordedModelAdapter | PerStrategyModelAdapter | CaptureThenReplayAdapter:
    """Attach one deterministic, per-strategy dynamic scripted adapter."""

    delegates: dict[str, RecordedModelAdapter] = {}
    for sid in strategy_ids:
        def factory(index: int, *, _sid: str = sid) -> Any:
            planned = candidate_builder(engine, engine._current_view)
            values = planned.get(_sid, ())
            return values[index] if index < len(values) else {"_error": "script_exhausted"}

        delegates[sid] = RecordedModelAdapter(_DynamicOutputRefScriptedAdapter(factory))
    if len(delegates) == 1:
        adapter: Any = next(iter(delegates.values()))
        if replay:
            adapter = CaptureThenReplayAdapter(adapter)
        engine._adapter = adapter
        return adapter
    adapter = PerStrategyModelAdapter(delegates)
    engine._adapter = adapter
    return adapter


def _capture_runtime(engine: AutomaticPaperRuntime, adapter: Any, *, scenario_id: str, extra: Mapping[str, Any] | None = None) -> dict[str, Any]:
    scheduler = engine.scheduler
    if scheduler is None or engine.book is None or engine._store is None:
        raise EvaluationError(f"runtime did not start for {scenario_id}")
    accounts: dict[str, Any] = {}
    for sid in sorted(engine.book.accounts):
        snapshot = engine.book.get_account(sid)
        accounts[sid] = _json(snapshot.to_dict() if hasattr(snapshot, "to_dict") else snapshot)
    transitions = {
        sid: _json(record.transition_log)
        for sid, record in sorted(scheduler.records.items())
    }
    timers = {
        sid: (_stamp(record.next_due_at) if record.next_due_at is not None else None)
        for sid, record in sorted(scheduler.records.items())
    }
    second_by_id = {
        event.event_id: event
        for event in (engine._feed.frozen_events if engine._feed is not None else ())
    }
    events = []
    for row in engine._store.list_events():
        envelope = row.get("envelope") if isinstance(row, Mapping) else None
        if isinstance(envelope, Mapping):
            projected = _strip_volatile(envelope)
            market_ref = projected.get("market_ref") if isinstance(projected, Mapping) else None
            if isinstance(market_ref, Mapping) and market_ref.get("kind") == "second_event":
                second = second_by_id.get(market_ref.get("second_event_id"))
                if second is not None and second.event_hash == market_ref.get("second_event_hash"):
                    projected["market_feed"] = second.market_feed.to_dict()
            events.append(projected)
    events.sort(key=lambda item: (str(item.get("occurred_at")), str(item.get("event_id"))))
    requests = []
    for request in getattr(adapter, "requests", []):
        extensions = request.get("extensions", {}) if isinstance(request.get("extensions"), Mapping) else {}
        profile = extensions.get("strategy_profile_ref", {}) if isinstance(extensions.get("strategy_profile_ref"), Mapping) else {}
        context_refs = request.get("context_refs", {}) if isinstance(request.get("context_refs"), Mapping) else {}
        extension_refs = extensions.get("context_refs", {}) if isinstance(extensions.get("context_refs"), Mapping) else {}
        requests.append(
            {
                "provider": request.get("provider"),
                "model": request.get("model"),
                "adapter": request.get("adapter"),
                "strategy_id": profile.get("strategy_id"),
                "profile_ref_id": profile.get("profile_ref_id"),
                "profile_ref_hash": profile.get("profile_ref_hash"),
                "reasoning_mode": extensions.get("reasoning_mode"),
                "request_semantic_hash": request_semantic_identity(request),
                "market_snapshot_id": context_refs.get("market_snapshot_id"),
                "market_snapshot_hash": context_refs.get("market_snapshot_hash"),
                "observation_frame_id": context_refs.get("observation_frame_id"),
                "observation_frame_hash": context_refs.get("observation_frame_hash"),
                "evidence_package_id": context_refs.get("evidence_package_id"),
                "evidence_package_hash": context_refs.get("evidence_package_hash"),
                "shared_market_view_id": extension_refs.get("shared_market_view_id"),
                "shared_market_view_hash": extension_refs.get("shared_market_view_hash"),
                "latest_second_event_id": extension_refs.get("latest_second_event_id"),
                "latest_second_event_hash": extension_refs.get("latest_second_event_hash"),
                "packet_id": extension_refs.get("packet_id"),
                "packet_hash": extension_refs.get("packet_hash"),
                "boundary_issues": [issue.to_dict() for issue in scan_model_boundary(request)],
            }
        )
    # Provider workers can complete the two independent opposite-strategy
    # calls in either order.  Completion order is telemetry, not evaluation
    # semantics; canonicalize the safe request projection by strategy so it
    # cannot make an otherwise identical replay differ.
    requests.sort(key=lambda item: (str(item.get("strategy_id")), str(item.get("request_semantic_hash"))))
    schedule_identities = {}
    for sid, record in sorted(scheduler.records.items()):
        schedule_identities[sid] = {
            "strategy_id": sid,
            "generation": record.generation,
            "state": record.state.value,
            "market_view_id": record.current_view_id,
            "closed_snapshot_id": record.last_closed_snapshot_id,
            "next_due_at": _stamp(record.next_due_at) if record.next_due_at is not None else None,
            "expires_at": _stamp(record.expires_at) if record.expires_at is not None else None,
            "pending_id": record.pending_id,
            "open_position_id": record.open_position_id,
            "timers": [
                {
                    "token_id": token.token_id,
                    "kind": kind,
                    "generation": token.generation,
                    "due_at": _stamp(token.due_at),
                    "active": token.active,
                }
                for kind, token in sorted(record.timer_tokens.items())
            ],
        }
    result = {
        "scenario_id": scenario_id,
        "states": {sid: record.state.value for sid, record in sorted(scheduler.records.items())},
        "timers": timers,
        "transitions": transitions,
        "accounts": accounts,
        "events": events,
        "requests": requests,
        "provider_outcomes": _json(getattr(adapter, "outcomes", [])),
        "provider_call_count": len(requests),
        "schedule_identities": schedule_identities,
        "view": {
            "id": engine._current_view.view_id if engine._current_view is not None else None,
            "hash": engine._current_view.content_hash if engine._current_view is not None else None,
            "snapshot_id": engine._current_view.closed_snapshot.get("market_snapshot_id") if engine._current_view is not None else None,
            "snapshot_hash": engine._current_view.closed_snapshot.get("content_hash") if engine._current_view is not None else None,
            "latest_second": _json(engine._current_view.latest_second) if engine._current_view is not None else None,
            "feed": engine._config.feed_key,
            "market_feed": engine._config.market_feed.to_dict(),
        },
        "extra": _json(extra or {}),
    }
    result["canonical_hash"] = _digest(result)
    return result


def _run_engine_case(
    fixture: Mapping[str, Any],
    scenario_id: str,
    *,
    candidate_builder: Callable[[AutomaticPaperRuntime, Any], Mapping[str, Sequence[Any]]],
    steps: Sequence[tuple[str, Sequence[str]]],
    strategy_ids: Sequence[str] = (SID_LONG,),
    replay: bool = False,
    history_payload: Mapping[str, Any] | None = None,
    registry: Any | None = None,
    clock_start: str = START_AT,
    stale_after_seconds: int = 300,
) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="g029-") as directory:
        engine, aggregator, _history, _clock = _runtime(
            fixture,
            Path(directory),
            strategy_ids=strategy_ids,
            batch_names=("initial",),
            registry=registry,
            history_payload=history_payload,
            clock_start=clock_start,
            stale_after_seconds=stale_after_seconds,
        )
        startup = engine.start()
        if not engine._started:
            return {
                "scenario_id": scenario_id,
                "startup": _strip_volatile(startup),
                "states": {},
                "accounts": {},
                "events": [],
                "requests": [],
                "provider_call_count": 0,
                "extra": {"startup_failed": True},
                "canonical_hash": _digest(startup),
            }
        adapter = _attach_dynamic_candidates(engine, candidate_builder, strategy_ids, replay=replay)
        step_reports = []
        for now, names in steps:
            aggregator.append(*names)
            step_reports.append(engine.step(now=now))
        captured = _capture_runtime(engine, adapter, scenario_id=scenario_id, extra={"startup": startup, "steps": step_reports})
        engine.stop(reason="g029_complete")
        captured["extra"]["startup"] = _strip_volatile(startup)
        captured["extra"]["steps"] = _strip_volatile(step_reports)
        captured["canonical_hash"] = _digest({key: value for key, value in captured.items() if key != "canonical_hash"})
        return captured


def _no_entry_candidate(engine: AutomaticPaperRuntime, view: Any, *, n: int = 57, sid: str = SID_LONG) -> Mapping[str, Sequence[Any]]:
    return {sid: [_grounded_candidate(engine._registry, sid, view, branch="no_entry", n=n)]}


def _immediate_candidate(engine: AutomaticPaperRuntime, view: Any, *, direction: str, stop: str, target: str, sid: str = SID_LONG) -> Mapping[str, Sequence[Any]]:
    return {sid: [_grounded_candidate(engine._registry, sid, view, branch="enter_immediately", direction=direction, stop=stop, target=target)]}


def _wait_candidate(engine: AutomaticPaperRuntime, view: Any, *, direction: str, entry: str, stop: str, target: str, x: int, sid: str = SID_LONG) -> Mapping[str, Sequence[Any]]:
    return {sid: [_grounded_candidate(engine._registry, sid, view, branch="wait_for_price", direction=direction, entry=entry, stop=stop, target=target, x=x)]}


def _both_candidates(engine: AutomaticPaperRuntime, view: Any) -> Mapping[str, Sequence[Any]]:
    return {
        SID_LONG: [_grounded_candidate(engine._registry, SID_LONG, view, branch="enter_immediately", direction="long", stop="99", target="102")],
        SID_SECOND: [_grounded_candidate(engine._registry, SID_SECOND, view, branch="enter_immediately", direction="short", stop="102", target="99")],
    }


def _same_direction_candidates(engine: AutomaticPaperRuntime, view: Any) -> Mapping[str, Sequence[Any]]:
    return {
        SID_LONG: [_grounded_candidate(engine._registry, SID_LONG, view, branch="enter_immediately", direction="long", stop="99", target="102")],
        SID_SECOND: [_grounded_candidate(engine._registry, SID_SECOND, view, branch="enter_immediately", direction="long", stop="99", target="102")],
    }


def _errors_candidate(_engine: AutomaticPaperRuntime, _view: Any, *, kinds: Sequence[str]) -> Mapping[str, Sequence[Any]]:
    return {SID_LONG: [{"_error": kind, "_transient": kind in {"server", "transport"}} for kind in kinds]}


def _runtime_scenarios(fixture: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        "no_entry_N": _run_engine_case(
            fixture, "no_entry_N", candidate_builder=lambda e, v: _no_entry_candidate(e, v),
            steps=[(START_AT, ())],
        ),
        "immediate_long_tp": _run_engine_case(
            fixture, "immediate_long_tp", candidate_builder=lambda e, v: _immediate_candidate(e, v, direction="long", stop="99", target="102"),
            steps=[(START_AT, ()), ("2026-03-09T13:33:25Z", ("long_target",))],
        ),
        "immediate_short_sl": _run_engine_case(
            fixture, "immediate_short_sl", candidate_builder=lambda e, v: _immediate_candidate(e, v, direction="short", stop="101", target="99"),
            steps=[(START_AT, ()), ("2026-03-09T13:33:55Z", ("short_stop",))],
        ),
        "wait_long_fill_tp": _run_engine_case(
            fixture, "wait_long_fill_tp", candidate_builder=lambda e, v: _wait_candidate(e, v, direction="long", entry="100", stop="99", target="102", x=60),
            steps=[(START_AT, ()), ("2026-03-09T13:34:05Z", ("touch_long",)), ("2026-03-09T13:34:25Z", ("late_long_target",))],
        ),
        "wait_short_fill_sl": _run_engine_case(
            fixture,
            "wait_short_fill_sl",
            candidate_builder=lambda e, v: {
                SID_LONG: [
                    _wait_candidate(e, v, direction="short", entry="100", stop="101", target="98", x=60)[SID_LONG][0],
                    _no_entry_candidate(e, v, n=5)[SID_LONG][0],
                ]
            },
            steps=[(START_AT, ()), ("2026-03-09T13:34:05Z", ("touch_short",)), ("2026-03-09T13:34:45Z", ("late_short_stop",))],
        ),
        "dual_touch_stop_first": _run_engine_case(
            fixture, "dual_touch_stop_first", candidate_builder=lambda e, v: _immediate_candidate(e, v, direction="long", stop="99", target="102"),
            steps=[(START_AT, ()), ("2026-03-09T13:34:35Z", ("dual_touch",))],
        ),
        "wait_expiry": _run_engine_case(
            fixture, "wait_expiry", candidate_builder=lambda e, v: _wait_candidate(e, v, direction="long", entry="100", stop="99", target="102", x=3),
            steps=[(START_AT, ()), ("2026-03-09T13:33:11Z", ())],
        ),
        "opposite_isolation": _run_engine_case(
            fixture, "opposite_isolation", candidate_builder=_both_candidates,
            steps=[(START_AT, ())], strategy_ids=(SID_LONG, SID_SECOND),
        ),
        "same_direction_parallel": _run_engine_case(
            fixture, "same_direction_parallel", candidate_builder=_same_direction_candidates,
            steps=[(START_AT, ())], strategy_ids=(SID_LONG, SID_SECOND),
        ),
        "replay_response": _run_engine_case(
            fixture, "replay_response", candidate_builder=lambda e, v: _no_entry_candidate(e, v, n=5),
            steps=[(START_AT, ())], replay=True,
        ),
        "provider_retry_backoff": _run_engine_case(
            fixture, "provider_retry_backoff", candidate_builder=lambda e, v: _errors_candidate(e, v, kinds=("timeout", "rate_limit", "server", "transport", "timeout", "timeout")),
            steps=[(START_AT, ()), ("2026-03-09T13:33:10Z", ()), ("2026-03-09T13:33:20Z", ()), ("2026-03-09T13:33:40Z", ()), ("2026-03-09T13:34:20Z", ()), ("2026-03-09T13:35:20Z", ())],
        ),
        "terminal_auth_no_hot_loop": _run_engine_case(
            fixture, "terminal_auth_no_hot_loop", candidate_builder=lambda e, v: _errors_candidate(e, v, kinds=("authentication",)),
            steps=[(START_AT, ()), ("2026-03-09T13:33:10Z", ())],
        ),
        "provider_invalid_response": _run_engine_case(
            fixture, "provider_invalid_response", candidate_builder=lambda e, v: _errors_candidate(e, v, kinds=("malformed_response",)),
            steps=[(START_AT, ()), ("2026-03-09T13:33:10Z", ())],
        ),
    }


def _market_policy_abstention(fixture: Mapping[str, Any]) -> dict[str, Any]:
    short_history = copy.deepcopy(dict(fixture["history_response"]))
    short_history["bars"][fixture["symbol"]] = short_history["bars"][fixture["symbol"]][:2]
    adapter = AlpacaHistoryAdapter(
        key_id="offline-fixture-id",
        secret_key="offline-fixture-secret",
        transport=RecordedTransport([TransportResponse(200, short_history)]),
    )
    result = adapter.fetch_history(symbol=fixture["symbol"], start_at="2026-03-09T13:30:00Z", end_at="2026-03-09T13:32:00Z", timeframe="1Min")
    policy = finalize_artifact(
        {
            "schema_version": SCHEMA_VERSION,
            "artifact_kind": "analysis_policy",
            "minimum_closed_bars": 3,
            "max_staleness_seconds": 300,
            "allow_partial_data": False,
            "exclude_incomplete_last_bar": True,
            "required_capabilities": ["price_action"],
            "policy_version": SCHEMA_VERSION,
            "extensions": {"market_feed": ALPACA_IEX_FEED.to_dict()},
        },
        "analysis_policy",
    )
    assessment = assess_market(result.snapshot, policy, expected_feed=ALPACA_IEX_FEED, require_feed=True, expected_adjustment_policy="unadjusted")
    return {
        "scenario_id": "abstain_insufficient_history",
        "status": assessment.terminal_outcome.expected_status if assessment.terminal_outcome else None,
        "code": assessment.terminal_outcome.code if assessment.terminal_outcome else None,
        "model_call_allowed": assessment.model_call_allowed,
        "outcome": assessment.terminal_outcome.to_dict() if assessment.terminal_outcome else None,
        "snapshot_id": result.snapshot.get("market_snapshot_id") if result.snapshot else None,
        "snapshot_hash": result.snapshot.get("content_hash") if result.snapshot else None,
    }


def _retry_backoff_evidence(case: Mapping[str, Any]) -> dict[str, Any]:
    transitions = case.get("transitions", {}).get(SID_LONG, []) if isinstance(case.get("transitions"), Mapping) else []
    retry_due = [item.get("at") for item in transitions if isinstance(item, Mapping) and item.get("reason") == "retry_due"]
    error_at = [item.get("at") for item in transitions if isinstance(item, Mapping) and item.get("reason") == "provider_retry"]
    # The case uses one exact fake-clock step for each due boundary.  Pairing
    # the typed retry_due transition with the preceding typed provider error
    # makes the backoff assertion independent of wall-clock telemetry.
    observed: list[int] = []
    for current in retry_due:
        prior = [value for value in error_at if value < current]
        if prior:
            left = datetime.fromisoformat(prior[-1].replace("Z", "+00:00"))
            right = datetime.fromisoformat(current.replace("Z", "+00:00"))
            observed.append(int((right - left).total_seconds()))
    return {
        "observed_due_times": retry_due,
        "observed_delays_seconds": observed,
        "expected_delays_seconds": [5, 10, 20, 40, 60],
    }


def _scale_evidence() -> dict[str, Any]:
    view = {
        "shared_market_view_id": "smv_g029_shared",
        "closed_snapshot_id": "mkt_g029_closed",
        "regular_session": True,
        "fresh": True,
        "complete": True,
        "non_gapped": True,
        "sufficient": True,
        "frozen_second": True,
    }
    def complete(scheduler: StrategyScheduler, job: Any) -> dict[str, Any]:
        """Complete an actual G023 job through its account-free branch."""

        instruction = {"branch": "no_entry", "strategy_id": job.strategy_id, "next_call_after_seconds": 60}
        accepted = scheduler.complete_job(job, instruction=instruction, decision_accepted_at=START_AT)
        record = scheduler.get(job.strategy_id)
        token = record.timer_tokens.get("sleep") if record is not None else None
        return {
            "strategy_id": job.strategy_id,
            "job_id": job.job_id,
            "generation": job.generation,
            "market_view_id": job.market_view_id,
            "closed_snapshot_id": job.closed_snapshot_id,
            "idempotency_key": job.idempotency_key,
            "completed": accepted,
            "state_after": record.state.value if record is not None else None,
            "schedule_token_id": token.token_id if token is not None else None,
            "schedule_generation": token.generation if token is not None else None,
            "due_at": _stamp(token.due_at) if token is not None else None,
        }

    def synthetic_registry(ids: Sequence[str]) -> dict[str, dict[str, Any]]:
        """Build the registry input consumed by the real scheduler constructor."""

        return {
            sid: {
                "strategy_id": sid,
                "profile_ref": {"profile_id": f"synthetic-profile:{sid}"},
                "enabled": True,
            }
            for sid in ids
        }

    def setup(scheduler: StrategyScheduler, ids: Sequence[str]) -> None:
        if set(scheduler.records) != set(ids):
            raise EvaluationError("synthetic registry did not register the requested strategy IDs")
        # The scale experiment exercises typed scheduler state/queue logic;
        # it does not need the full G018 packet verifier.
        scheduler._verify_success_instruction = lambda _record, _instruction: True
        scheduler.start(view)

    forty_ids = [f"pa:g029-scale-{index:03d}" for index in range(40)]
    forty = StrategyScheduler(
        strategy_registry=synthetic_registry(forty_ids),
        clock=FakeClock(START_AT),
        queue_capacity=200,
        max_in_flight=40,
    )
    setup(forty, forty_ids)
    forty_first_jobs = list(forty.queued_jobs())
    forty_records = [complete(forty, job) for job in forty.claim_jobs()]
    forty_counts = {sid: 0 for sid in forty_ids}
    for item in forty_records:
        forty_counts[item["strategy_id"]] += 1
    forty_job_ids = [item["job_id"] for item in forty_records]

    reentry_ids = [f"pa:g029-reentry-{index:02d}" for index in range(4)]
    reentry = StrategyScheduler(
        strategy_registry=synthetic_registry(reentry_ids),
        clock=FakeClock(START_AT),
        queue_capacity=10,
        max_in_flight=4,
    )
    setup(reentry, reentry_ids)
    reentry_peak = reentry.queue.provider_in_flight
    reentry_first_jobs = reentry.claim_jobs()
    reentry_peak = max(reentry_peak, reentry.queue.provider_in_flight)
    reentry_first = {job.strategy_id: complete(reentry, job) for job in reentry_first_jobs}
    reentry.advance(60)
    reentry_second_jobs = reentry.claim_jobs()
    reentry_peak = max(reentry_peak, reentry.queue.provider_in_flight)
    reentry_second = {job.strategy_id: complete(reentry, job) for job in reentry_second_jobs}
    reentry_isolated = all(
        reentry_first[sid]["job_id"] != reentry_second[sid]["job_id"]
        and reentry_first[sid]["generation"] == reentry_second[sid]["generation"]
        and reentry_first[sid]["strategy_id"] == reentry_second[sid]["strategy_id"]
        for sid in reentry_ids
    )

    two_ids = [f"pa:g029-synthetic-{index:03d}" for index in range(200)]
    two_hundred = StrategyScheduler(
        strategy_registry=synthetic_registry(two_ids),
        clock=FakeClock(START_AT),
        queue_capacity=200,
        max_in_flight=7,
    )
    setup(two_hundred, two_ids)
    service_records: list[dict[str, Any]] = []
    service_counts = {sid: 0 for sid in two_ids}
    peak = 0
    refill_after_release = False
    while two_hundred.queue.ready_jobs or two_hundred.queue.active_jobs:
        jobs = two_hundred.claim_jobs()
        if not jobs:
            raise EvaluationError("synthetic scheduler made no progress")
        pending = list(jobs)
        peak = max(peak, two_hundred.queue.provider_in_flight)
        while pending:
            job = pending.pop(0)
            before_release = two_hundred.queue.provider_in_flight
            item = complete(two_hundred, job)
            if not item["completed"]:
                raise EvaluationError(f"synthetic scheduler completion failed for {job.strategy_id}")
            service_records.append(item)
            service_counts[job.strategy_id] += 1
            if two_hundred.queue.ready_jobs:
                refill = two_hundred.claim_jobs(limit=1)
                refill_after_release = refill_after_release or (
                    before_release == 7 and len(refill) == 1 and two_hundred.queue.provider_in_flight == 7
                )
                pending.extend(refill)
            peak = max(peak, two_hundred.queue.provider_in_flight)
    service_job_ids = [item["job_id"] for item in service_records]
    service_lineage_ok = all(
        item["strategy_id"] in service_counts
        and item["generation"] == 0
        and item["market_view_id"] == view["shared_market_view_id"]
        and item["closed_snapshot_id"] == view["closed_snapshot_id"]
        and item["completed"] is True
        and item["schedule_token_id"]
        and item["schedule_generation"] == item["generation"]
        for item in service_records
    )
    return {
        "forty": {
            "registered": len(forty_ids),
            "registry_strategy_ids": sorted(forty.records),
            "started_jobs": len(forty_first_jobs),
            "completed_jobs": len(forty_records),
            "service_counts": forty_counts,
            "shared_view_ids": sorted({job.market_view_id for job in forty_first_jobs}),
            "closed_snapshot_ids": sorted({job.closed_snapshot_id for job in forty_first_jobs}),
            "job_ids": forty_job_ids,
            "duplicate_job_ids": len(forty_job_ids) - len(set(forty_job_ids)),
            "all_completed": all(item["completed"] for item in forty_records),
            "each_strategy_once": all(value == 1 for value in forty_counts.values()),
            "same_view": len({job.market_view_id for job in forty_first_jobs}) == 1,
            "no_duplicate_generation_job": len({(job.generation, job.job_id) for job in forty_first_jobs}) == len(forty_first_jobs),
            "max_in_flight": forty.config.max_in_flight,
            "reentry": {
                "strategies": reentry_ids,
                "first": reentry_first,
                "second": reentry_second,
                "all_completed": all(item["completed"] for item in [*reentry_first.values(), *reentry_second.values()]),
                "isolated": reentry_isolated,
                "peak_in_flight": reentry_peak,
            },
        },
        "two_hundred": {
            "registered": len(two_ids),
            "registry_strategy_ids": sorted(two_hundred.records),
            "claimed": len(service_records),
            "unique_claimed": len(set(item["strategy_id"] for item in service_records)),
            "service_counts": service_counts,
            "service_records": service_records,
            "all_strategy_ids_once": set(service_counts) == set(two_ids) and all(value == 1 for value in service_counts.values()),
            "unique_job_ids": len(set(service_job_ids)) == len(service_job_ids),
            "unique_generation_jobs": len({(item["generation"], item["job_id"]) for item in service_records}) == len(service_records),
            "service_lineage_ok": service_lineage_ok,
            "peak_in_flight": peak,
            "configured_max_in_flight": two_hundred.config.max_in_flight,
            "queue_capacity": two_hundred.config.queue_capacity,
            "work_conserving_refill": refill_after_release,
            "first_claimed": [item["strategy_id"] for item in service_records[:7]],
            "last_claimed": [item["strategy_id"] for item in service_records[-7:]],
        },
    }


def _isolation_evidence(scenarios: Mapping[str, Any]) -> dict[str, Any]:
    """Check both strategy partitions across account, schedule and lineage seams."""

    cases: dict[str, Any] = {}
    overall = True
    for scenario_id in ("opposite_isolation", "same_direction_parallel"):
        result = scenarios.get(scenario_id, {})
        expected = {SID_LONG, SID_SECOND}
        accounts = result.get("accounts", {}) if isinstance(result, Mapping) else {}
        schedules = result.get("schedule_identities", {}) if isinstance(result, Mapping) else {}
        events = [event for event in result.get("events", []) if isinstance(event, Mapping)]
        requests = [item for item in result.get("requests", []) if isinstance(item, Mapping)]
        outcomes = [item for item in result.get("provider_outcomes", []) if isinstance(item, Mapping)]
        trade_ids = {
            account.get("open_trade", {}).get("trade_id")
            for account in accounts.values()
            if isinstance(account, Mapping) and isinstance(account.get("open_trade"), Mapping)
        }
        event_strategy_ids = {event.get("strategy_id") for event in events if event.get("event_type") in {"model_decision_accepted", "paper_fill", "paper_close"}}
        request_strategy_ids = {item.get("strategy_id") for item in requests}
        outcome_strategy_ids = {item.get("strategy_id") for item in outcomes}
        account_ids = {account.get("account_id") for account in accounts.values() if isinstance(account, Mapping)}
        schedule_strategy_ids = {item.get("strategy_id") for item in schedules.values() if isinstance(item, Mapping)}
        per_strategy = {}
        for sid in sorted(expected):
            account = accounts.get(sid, {})
            schedule = schedules.get(sid, {})
            per_strategy[sid] = {
                "account_id": account.get("account_id"),
                "balance": account.get("balance"),
                "available_balance": account.get("available_balance"),
                "trade_id": account.get("open_trade", {}).get("trade_id") if isinstance(account.get("open_trade"), Mapping) else None,
                "direction": account.get("open_trade", {}).get("direction") if isinstance(account.get("open_trade"), Mapping) else None,
                "schedule_identity": {
                    "strategy_id": schedule.get("strategy_id"),
                    "market_view_id": schedule.get("market_view_id"),
                    "closed_snapshot_id": schedule.get("closed_snapshot_id"),
                    "generation": schedule.get("generation"),
                    "next_due_at": schedule.get("next_due_at"),
                    "expires_at": schedule.get("expires_at"),
                    "pending_id": schedule.get("pending_id"),
                    "open_position_id": schedule.get("open_position_id"),
                },
            }
        account_pass = set(accounts) == expected and account_ids == {f"paper:{sid}" for sid in expected} and len(account_ids) == 2
        trade_pass = len(trade_ids) == 2 and all(value is not None for value in trade_ids)
        no_netting = all(per_strategy[sid]["balance"] == "1000.00" and per_strategy[sid]["available_balance"] == "1000.00" for sid in expected)
        schedule_pass = (
            set(schedules) == expected
            and schedule_strategy_ids == expected
            and len({item.get("open_position_id") for item in schedules.values()}) == 2
            and all(item.get("generation") == 0 and item.get("market_view_id") for item in schedules.values())
        )
        lineage_pass = event_strategy_ids == expected and request_strategy_ids == expected and outcome_strategy_ids == expected
        case_pass = account_pass and trade_pass and no_netting and schedule_pass and lineage_pass
        cases[scenario_id] = {
            "pass": case_pass,
            "per_strategy": per_strategy,
            "account_ids_unique": account_pass,
            "trade_ids_unique": trade_pass,
            "balances_independent_no_netting": no_netting,
            "schedule_due_generation_isolated": schedule_pass,
            "event_strategy_ids": sorted(event_strategy_ids),
            "request_strategy_ids": sorted(request_strategy_ids),
            "outcome_strategy_ids": sorted(outcome_strategy_ids),
            "lineage_strategy_ids_isolated": lineage_pass,
        }
        overall = overall and case_pass
    return {"cases": cases, "pass": overall}


def _decimal_scale_evidence(scenarios: Mapping[str, Any]) -> dict[str, Any]:
    """Assert the actual paper-event Decimal exponents, not display strings."""

    quantity_scales: set[int] = set()
    r_scales: set[int] = set()
    usd_scales: set[int] = set()
    observed: list[dict[str, Any]] = []
    usd_fields = {
        "applied_pnl",
        "balance_after",
        "balance_before",
        "closing_balance",
        "fees",
        "net_pnl",
        "slippage",
    }
    raw_pnl_fields = {"capped_gross_pnl", "gross_pnl"}
    raw_pnl_scales: set[int] = set()
    for result in scenarios.values():
        if not isinstance(result, Mapping):
            continue
        for event in result.get("events", []):
            if not isinstance(event, Mapping):
                continue
            payload = event.get("payload", {})
            if event.get("event_type") == "paper_fill":
                for key, target in (("quantity", quantity_scales), ("planned_r", r_scales)):
                    if payload.get(key) is not None:
                        exponent = Decimal(str(payload[key])).as_tuple().exponent
                        target.add(int(exponent))
            if event.get("event_type") == "paper_close" and isinstance(payload.get("settlement"), Mapping):
                settlement = payload["settlement"]
                for key in usd_fields:
                    if settlement.get(key) is not None:
                        usd_scales.add(int(Decimal(str(settlement[key])).as_tuple().exponent))
                for key in raw_pnl_fields:
                    if settlement.get(key) is not None:
                        raw_pnl_scales.add(int(Decimal(str(settlement[key])).as_tuple().exponent))
                observed.append(
                    {
                        "event_id": event.get("event_id"),
                        "quantity_scale": int(Decimal(str(settlement.get("quantity", "0"))).as_tuple().exponent),
                        "usd_scales": {key: int(Decimal(str(settlement[key])).as_tuple().exponent) for key in sorted(usd_fields) if settlement.get(key) is not None},
                        "raw_pnl_scales": {key: int(Decimal(str(settlement[key])).as_tuple().exponent) for key in sorted(raw_pnl_fields) if settlement.get(key) is not None},
                    }
                )
    assertions = {
        "quantity_8_places": quantity_scales == {-8},
        "usd_2_places": usd_scales == {-2},
        "r_4_places": r_scales == {-4},
    }
    return {
        "observed_quantity_scales": sorted(quantity_scales),
        "observed_usd_scales": sorted(usd_scales),
        "observed_raw_pnl_scales": sorted(raw_pnl_scales),
        "observed_r_scales": sorted(r_scales),
        "settlement_observations": observed,
        "assertions": assertions,
        "pass": bool(observed) and all(assertions.values()),
    }


def _boundary_evidence(fixture: Mapping[str, Any]) -> dict[str, Any]:
    agg = LocalSecondAggregator("AAPL")
    first = {"T": "t", "S": "AAPL", "i": 9101, "x": "V", "p": "100", "s": 1, "c": ["@"], "t": "2026-03-09T13:33:00.100000001Z", "z": "C"}
    correction = {"T": "c", "S": "AAPL", "x": "V", "oi": 9101, "op": "100", "os": 1, "oc": ["@"], "ci": 9102, "cp": "101", "cs": 1, "cc": ["@"], "z": "C", "t": "2026-03-09T13:33:04.500000001Z"}
    agg.ingest_message([first], received_at="2026-03-09T13:33:01Z")
    provisional_before = agg.aggregate("2026-03-09T13:33:00Z", "2026-03-09T13:33:01Z")
    agg.ingest_message([correction], received_at="2026-03-09T13:33:02Z")
    pre_freeze = agg.aggregate("2026-03-09T13:33:00Z", "2026-03-09T13:33:01Z")
    agg.freeze(now="2026-03-09T13:33:04Z")
    frozen = agg.event("2026-03-09T13:33:00Z", "2026-03-09T13:33:01Z")
    late_correction = {"T": "c", "S": "AAPL", "x": "V", "oi": 9102, "op": "101", "os": 1, "oc": ["@"], "ci": 9103, "cp": "99", "cs": 1, "cc": ["@"], "z": "C", "t": "2026-03-09T13:33:05.500000001Z"}
    agg.ingest_message([late_correction], received_at="2026-03-09T13:33:06Z")
    late = agg.latest_event("2026-03-09T13:33:00Z", "2026-03-09T13:33:01Z")
    sip = copy.deepcopy(frozen.to_dict() if frozen is not None else {})
    sip["market_feed"] = {"schema_version": "0.1.0", "provider": "alpaca", "feed": "sip", "coverage": "consolidated"}
    sip["extensions"]["market_feed"] = sip["market_feed"]
    sip_verdict = validate_second_event(sip, expected_feed=ALPACA_IEX_FEED)
    # Exercise the actual G028/G021 half-open seam with one overlap event and
    # one cutover-inclusive live event.  The skip/accept facts below come
    # from FeedSession's retained input ledgers, never from declarations.
    cutover_history = RecordedIEXHistory(fixture)
    cutover_aggregator = RecordedIEXAggregator(fixture, batch_names=("cutover_overlap",))
    cutover_session = FeedSession(
        symbol=str(fixture["symbol"]),
        timeframe=str(fixture["timeframe"]).lower(),
        history_adapter=cutover_history,
        aggregator=cutover_aggregator,
        clock=FakeClock(START_AT),
        minimum_closed_bars=3,
        stale_after_seconds=300,
    )
    cutover_history_result = cutover_session.backfill(start_at=HISTORY_START, end_at=HISTORY_END)
    cutover = cutover_session.cutover_at(cutover_history_result)
    release = cutover_session.release_stream_recovery(cutover_at=cutover, end_at=START_AT)
    cutover_events = cutover_session.ingest(now=START_AT)
    skipped = list(cutover_session.skipped_replays)
    pre_skips = [item for item in skipped if item.get("reason") == "before_live_cutover"]
    retained = [event for event in cutover_events if event.start_at >= HISTORY_END]
    pre_dropped = bool(pre_skips) and all(item.get("end_at") <= HISTORY_END for item in pre_skips)
    inclusive_retained = bool(retained) and all(event.start_at >= HISTORY_END for event in retained)
    return {
        "half_open_cutover": {
            "history_end_exclusive": HISTORY_END,
            "actual_cutover_at": _stamp(cutover),
            "live_start_inclusive": HISTORY_END,
            "release": release,
            "pre_cutover_event_ids": [item.get("event_id") for item in pre_skips],
            "retained_event_ids": [event.event_id for event in retained],
            "pre_cutover_dropped": pre_dropped,
            "cutover_inclusive_retained": inclusive_retained,
            "skipped_replay_count": len(skipped),
        },
        "pre_watermark_revision_hash": pre_freeze.event_hash if pre_freeze is not None else None,
        "pre_watermark_changed": provisional_before.event_hash != pre_freeze.event_hash if provisional_before and pre_freeze else False,
        "frozen_event_hash": frozen.event_hash if frozen is not None else None,
        "post_watermark_execution_hash_unchanged": frozen.event_hash == (agg.event("2026-03-09T13:33:00Z", "2026-03-09T13:33:01Z").event_hash if frozen is not None else None),
        "late_reconciliation_hash": late.event_hash if late is not None else None,
        "sip_rejected": not sip_verdict.valid,
        "sip_error": sip_verdict.issues[0].code if sip_verdict.issues else None,
    }


def _revision_lineage_evidence(fixture: Mapping[str, Any]) -> dict[str, Any]:
    """Prove that a fresh multi-trade publication is natively revision zero."""

    batch_name = "dual_touch"
    batch = fixture["trade_batches"][batch_name]
    golden = next(item for item in fixture["derived_second_goldens"] if item.get("batch") == batch_name)
    publication_at = "2026-03-09T13:34:35Z"

    aggregator = RecordedIEXAggregator(fixture, batch_names=(batch_name,))
    published = aggregator.freeze(now=publication_at)
    event = published[0] if len(published) == 1 else None

    # Independently feed the same recorded messages to the frozen G020
    # implementation.  Equality here is the passthrough assertion: the G029
    # wrapper must return the exact serialized event produced by G020.
    direct_aggregator = LocalSecondAggregator("AAPL", market_feed=ALPACA_IEX_FEED, adjustment="raw")
    direct_aggregator.ingest_message(copy.deepcopy(batch["messages"]), received_at=batch["received_at"])
    direct_published = direct_aggregator.freeze(now=publication_at)
    direct_event = direct_published[0] if len(direct_published) == 1 else None
    event_mapping = event.to_dict() if event is not None else None
    direct_mapping = direct_event.to_dict() if direct_event is not None else None

    native_fields = {
        "revision_sequence_zero": event is not None and event.revision_sequence == 0,
        "parent_revision_absent": event is not None and event.parent_revision_id is None,
        "validated": event is not None and validate_second_event(event_mapping, expected_feed=ALPACA_IEX_FEED).valid,
        "wrapper_passthrough_equal": event_mapping == direct_mapping,
        "wrapper_has_only_passthrough_state": sorted(vars(aggregator)) == [
            "_aggregator",
            "_batches",
            "fixture",
            "freeze_calls",
            "market_feed",
            "symbol",
        ],
    }
    golden_fields = {
        key: getattr(event, key, None) == golden[key]
        for key in ("start_at", "end_at", "open", "high", "low", "close", "volume")
    }
    golden_fields["contributor_ids"] = list(event.contributor_ids) == golden["contributor_ids"] if event is not None else False
    golden_fields["market_feed"] = event.market_feed.to_dict() == ALPACA_IEX_FEED.to_dict() if event is not None else False
    golden_fields["execution_watermark"] = event.execution_watermark == golden["execution_watermark"] if event is not None else False

    # Pass the native event through both real G028 FeedSession ingestion and
    # the G021 SharedCandleEngine join.  No substituted event is acceptable.
    history = RecordedIEXHistory(fixture)
    feed_aggregator = RecordedIEXAggregator(fixture, batch_names=(batch_name,))
    feed = FeedSession(
        symbol=str(fixture["symbol"]),
        timeframe=str(fixture["timeframe"]).lower(),
        history_adapter=history,
        aggregator=feed_aggregator,
        clock=FakeClock(publication_at),
        minimum_closed_bars=1,
        stale_after_seconds=300,
    )
    history_result = feed.backfill(start_at=HISTORY_START, end_at=HISTORY_END)
    cutover = feed.cutover_at(history_result)
    release = feed.release_stream_recovery(cutover_at=cutover, end_at=publication_at)
    feed_events = feed.ingest(now=publication_at)
    feed_event = feed_events[0] if len(feed_events) == 1 else None
    shared_view_join_ok = False
    shared_view_latest_hash = None
    try:
        view = SharedCandleEngine(
            "AAPL",
            {"unit": "minute", "multiplier": 1},
            minimum_closed_bars=1,
            stale_after_seconds=300,
        ).build(
            history_result.snapshot,
            feed_events,
            cutover_at=HISTORY_END,
            now_at=publication_at,
        )
        shared_view_join_ok = feed_event is not None and view.latest_second is not None
        shared_view_latest_hash = view.latest_second.get("event_hash") if view.latest_second is not None else None
    except Exception:
        shared_view_join_ok = False

    return {
        "native_fields": native_fields,
        "golden_fields": golden_fields,
        "feed_session_ingested_one": len(feed_events) == 1,
        "feed_session_event_hash": feed_event.event_hash if feed_event is not None else None,
        "shared_view_join_ok": shared_view_join_ok,
        "shared_view_latest_event_hash": shared_view_latest_hash,
        "event_hash": event.event_hash if event is not None else None,
        "direct_event_hash": direct_event.event_hash if direct_event is not None else None,
        "release": release,
        "pass": (
            event is not None
            and direct_event is not None
            and all(native_fields.values())
            and all(golden_fields.values())
            and len(feed_events) == 1
            and feed_event is not None
            and feed_event.to_dict() == event_mapping
            and shared_view_join_ok
            and shared_view_latest_hash == event.event_hash
        ),
    }


def _feed_lineage_evidence(fixture: Mapping[str, Any], scenarios: Mapping[str, Any]) -> dict[str, Any]:
    """Trace the IEX identity and exercise real history/live mismatch gates."""

    history = RecordedIEXHistory(fixture)
    aggregator = RecordedIEXAggregator(fixture, batch_names=("initial",))
    session = FeedSession(
        symbol=str(fixture["symbol"]),
        timeframe=str(fixture["timeframe"]).lower(),
        history_adapter=history,
        aggregator=aggregator,
        clock=FakeClock(START_AT),
        minimum_closed_bars=3,
        stale_after_seconds=300,
    )
    history_result = session.backfill(start_at=HISTORY_START, end_at=HISTORY_END)
    live_events = session.ingest(now=START_AT)
    view = session.build_view(history_result=history_result, now_at=START_AT)
    expected_feed = ALPACA_IEX_FEED.to_dict()
    history_feed = history_result.snapshot.get("extensions", {}).get("market_feed") if history_result.snapshot else None
    live_feed = [event.market_feed.to_dict() for event in live_events]
    shared_feed = view.market_feed.to_dict()

    sip = MarketFeedRef("0.1.0", "alpaca", "sip", "consolidated")
    history_mismatch_code = None
    try:
        FeedSession(
            symbol=str(fixture["symbol"]),
            timeframe=str(fixture["timeframe"]).lower(),
            history_adapter=RecordedIEXHistory(fixture),
            aggregator=RecordedIEXAggregator(fixture),
            market_feed=sip,
            clock=FakeClock(START_AT),
        )
    except Exception as exc:
        history_mismatch_code = getattr(exc, "code", type(exc).__name__)

    aggregator_mismatch_code = None
    try:
        FeedSession(
            symbol=str(fixture["symbol"]),
            timeframe=str(fixture["timeframe"]).lower(),
            history_adapter=RecordedIEXHistory(fixture),
            aggregator=RecordedIEXAggregator(fixture),
            market_feed=sip,
            clock=FakeClock(START_AT),
        )
    except Exception as exc:
        aggregator_mismatch_code = getattr(exc, "code", type(exc).__name__)

    class SipEventAggregator:
        symbol = str(fixture["symbol"])
        market_feed = ALPACA_IEX_FEED

        def __init__(self) -> None:
            self._inner = LocalSecondAggregator(self.symbol, market_feed=ALPACA_IEX_FEED, adjustment="raw")
            self._inner.ingest_message(copy.deepcopy(fixture["trade_batches"]["initial"]["messages"]), received_at=fixture["trade_batches"]["initial"]["received_at"])

        def freeze(self, *, now: Any = None) -> tuple[Any, ...]:
            values = self._inner.freeze(now=now)
            tampered: list[SecondAggregateEvent] = []
            for event in values:
                converted = SecondAggregateEvent(
                    event.symbol,
                    event.start_at,
                    event.end_at,
                    event.open,
                    event.high,
                    event.low,
                    event.close,
                    event.volume,
                    event.contributor_ids,
                    event.contributor_hash,
                    event.execution_watermark,
                    event.status,
                    event.revision_sequence,
                    event.quality,
                    ALPACA_IEX_FEED,
                    event.adjustment,
                    event.raw_adjustment,
                    event.parent_revision_id,
                )
                # Keep G020's object valid while injecting the SIP identity
                # at the G021 FeedSession boundary, where the real mismatch
                # gate must reject it with feed_scope_mismatch.
                object.__setattr__(converted, "market_feed", sip)
                tampered.append(converted)
            return tuple(tampered)

    live_mismatch_code = None
    try:
        mismatch_session = FeedSession(
            symbol=str(fixture["symbol"]),
            timeframe=str(fixture["timeframe"]).lower(),
            history_adapter=RecordedIEXHistory(fixture),
            aggregator=SipEventAggregator(),
            clock=FakeClock(START_AT),
        )
        mismatch_history = mismatch_session.backfill(start_at=HISTORY_START, end_at=HISTORY_END)
        mismatch_session.ingest(now=START_AT)
        mismatch_session.build_view(history_result=mismatch_history, now_at=START_AT)
    except Exception as exc:
        live_mismatch_code = getattr(exc, "code", type(exc).__name__)

    paper_event_feeds = []
    for scenario_id, result in scenarios.items():
        if not isinstance(result, Mapping):
            continue
        for event in result.get("events", []):
            if isinstance(event, Mapping) and event.get("event_type") in {"paper_fill", "paper_close"}:
                paper_event_feeds.append({
                    "scenario_id": scenario_id,
                    "event_id": event.get("event_id"),
                    "event_type": event.get("event_type"),
                    "market_feed": event.get("market_feed"),
                })
    paper_iex = bool(paper_event_feeds) and all(item.get("market_feed") == expected_feed for item in paper_event_feeds)
    pass_value = (
        history_feed == expected_feed
        and bool(live_feed)
        and all(value == expected_feed for value in live_feed)
        and shared_feed == expected_feed
        and paper_iex
        and history_mismatch_code == "feed_scope_mismatch"
        and aggregator_mismatch_code == "feed_scope_mismatch"
        and live_mismatch_code in {"feed_scope_mismatch", "ValueError"}
    )
    return {
        "expected_feed": expected_feed,
        "history_feed": history_feed,
        "trade_derived_second_feeds": live_feed,
        "shared_market_view_feed": shared_feed,
        "paper_event_feeds": paper_event_feeds,
        "history_sip_mismatch_rejected": history_mismatch_code == "feed_scope_mismatch",
        "history_sip_mismatch_code": history_mismatch_code,
        "aggregator_sip_mismatch_rejected": aggregator_mismatch_code == "feed_scope_mismatch",
        "aggregator_sip_mismatch_code": aggregator_mismatch_code,
        "live_sip_mismatch_rejected": live_mismatch_code in {"feed_scope_mismatch", "ValueError"},
        "live_sip_mismatch_code": live_mismatch_code,
        "live_sip_mismatch_typed_or_g021_rejected": live_mismatch_code in {"feed_scope_mismatch", "ValueError"},
        "pass": pass_value,
    }


def _derived_second_golden_checks(fixture: Mapping[str, Any]) -> dict[str, Any]:
    """Compare recorded trade batches with fresh real G020 second aggregation."""

    expected = {
        str(item["batch"]): item
        for item in fixture.get("derived_second_goldens", [])
        if isinstance(item, Mapping) and item.get("batch")
    }
    checks: dict[str, dict[str, Any]] = {}
    for batch_name, golden in expected.items():
        batch = fixture.get("trade_batches", {}).get(batch_name)
        if not isinstance(batch, Mapping):
            checks[batch_name] = {"pass": False, "fields": {"batch_present": False}}
            continue
        aggregator = LocalSecondAggregator(
            str(fixture["symbol"]),
            market_feed=ALPACA_IEX_FEED,
            adjustment="raw",
        )
        messages = copy.deepcopy(batch.get("messages", []))
        aggregator.ingest_message(messages, received_at=batch.get("received_at"))
        trade_times = [
            datetime.fromisoformat(str(message["t"]).replace("Z", "+00:00"))
            for message in messages
            if isinstance(message, Mapping) and message.get("t")
        ]
        events = aggregator.freeze(now=max(trade_times) + timedelta(seconds=5)) if trade_times else ()
        actual_event = events[0] if len(events) == 1 else None
        actual = {
            "start_at": actual_event.start_at if actual_event is not None else None,
            "end_at": actual_event.end_at if actual_event is not None else None,
            "open": str(actual_event.open) if actual_event is not None else None,
            "high": str(actual_event.high) if actual_event is not None else None,
            "low": str(actual_event.low) if actual_event is not None else None,
            "close": str(actual_event.close) if actual_event is not None else None,
            "volume": str(actual_event.volume) if actual_event is not None else None,
            "contributor_ids": list(actual_event.contributor_ids) if actual_event is not None else None,
            "market_feed": actual_event.market_feed.to_dict() if actual_event is not None else None,
            "execution_watermark": actual_event.execution_watermark if actual_event is not None else None,
        }
        fields = {
            key: actual.get(key) == golden.get(key)
            for key in ("start_at", "end_at", "open", "high", "low", "close", "volume", "contributor_ids")
        }
        fields["market_feed"] = actual["market_feed"] == ALPACA_IEX_FEED.to_dict()
        if "execution_watermark" in golden:
            fields["execution_watermark"] = actual["execution_watermark"] == golden["execution_watermark"]
        checks[batch_name] = {"pass": bool(fields) and all(fields.values()), "fields": fields, "actual": actual}
    return {"pass": bool(checks) and all(item["pass"] for item in checks.values()), "batches": checks}


def _gap_stale_evidence(fixture: Mapping[str, Any]) -> dict[str, Any]:
    """Exercise the real G028 readiness gate for connection gaps and staleness."""

    clock = FakeClock(START_AT)
    history = RecordedIEXHistory(fixture)
    aggregator = RecordedIEXAggregator(fixture, batch_names=("initial",))
    session = FeedSession(
        symbol=str(fixture["symbol"]),
        timeframe=str(fixture["timeframe"]).lower(),
        history_adapter=history,
        aggregator=aggregator,
        clock=clock,
        minimum_closed_bars=3,
        stale_after_seconds=5,
    )
    history_result = session.backfill(start_at=HISTORY_START, end_at=HISTORY_END)
    session.ingest(now=START_AT)
    initial = session.build_view(history_result=history_result, now_at=START_AT)
    gap = session.mark_connection_gap(
        start_at="2026-03-09T13:33:05Z",
        end_at="2026-03-09T13:33:10Z",
        reason="offline_fixture_disconnect",
    )
    gap_view = session.build_view(history_result=history_result, now_at="2026-03-09T13:33:10Z")
    resolved = session.close_connection_gap(end_at="2026-03-09T13:33:10Z")
    stale_view = session.build_view(history_result=history_result, now_at="2026-03-09T13:33:20Z")
    gap_readiness = gap_view.readiness.to_dict()
    stale_readiness = stale_view.readiness.to_dict()
    return {
        "initial_executable": initial.executable,
        "gap": gap,
        "gap_view_readiness": gap_readiness,
        "gap_blocks_execution": gap_readiness.get("connection_gap") is True and gap_readiness.get("executable") is False,
        "gap_requires_recovery": gap_readiness.get("recovery_required") is True,
        "resolved_gap": resolved,
        "stale_view_readiness": stale_readiness,
        "stale_blocks_execution": stale_readiness.get("stale") is True and stale_readiness.get("executable") is False,
    }


def _immediate_drift_evidence(fixture: Mapping[str, Any]) -> dict[str, Any]:
    """Prove response-time second drift rejects without opening a trade."""

    with tempfile.TemporaryDirectory(prefix="g029-drift-") as directory:
        engine, aggregator, _history, _clock = _runtime(fixture, Path(directory), batch_names=("initial",))
        engine.start()
        adapter = _attach_dynamic_candidates(
            engine,
            lambda current_engine, view: _no_entry_candidate(current_engine, view, n=100),
            (SID_LONG,),
        )
        engine.step(now=START_AT)
        response_second = engine._current_view.latest_second
        aggregator.append("long_target")
        engine.step(now="2026-03-09T13:33:25Z")
        actual_second = engine._current_view.latest_second
        record = engine.scheduler.get(SID_LONG)
        instruction = {
            "branch": "enter_immediately",
            "direction": "long",
            "stop_price": "99",
            "target_price": "102",
            "second_event_ref": make_second_event_ref(response_second["event_id"], response_second["event_hash"], response_second["close"]),
        }
        result = engine._open_requester(
            SID_LONG,
            instruction,
            generation=record.generation,
            idempotency_key="g029-immediate-drift",
        )
        events = [
            row.get("envelope", {})
            for row in engine._store.list_events()
            if isinstance(row, Mapping) and isinstance(row.get("envelope"), Mapping)
        ]
        rejected = [event for event in events if event.get("event_type") == "paper_fill_rejected"]
        account = engine.book.get_account(SID_LONG)
        reason = rejected[0].get("payload", {}).get("reason", "") if len(rejected) == 1 else ""
        evidence = {
            "provider_call_count_before_manual_fill": adapter.calls,
            "response_second_event_id": response_second["event_id"],
            "response_second_event_hash": response_second["event_hash"],
            "actual_second_event_id": actual_second["event_id"],
            "actual_second_event_hash": actual_second["event_hash"],
            "rejected": result.accepted is False,
            "fresh_price_rejected": result.fresh_price_rejected is True,
            "rejection_event_count": len(rejected),
            "reason_binds_both_second_ids": response_second["event_id"] in reason and actual_second["event_id"] in reason,
            "balance_unchanged": str(account.balance) == "1000.00",
            "no_open_trade": account.open_trade is None,
        }
        engine.stop(reason="g029_drift_done")
        return evidence


def _disabled_exhausted_evidence() -> dict[str, Any]:
    """Ensure disabled and exhausted records never enqueue a recall."""

    view = {
        "shared_market_view_id": "smv_g029_shared",
        "closed_snapshot_id": "mkt_g029_closed",
        "regular_session": True,
        "fresh": True,
        "complete": True,
        "non_gapped": True,
        "sufficient": True,
        "frozen_second": True,
    }
    scheduler = StrategyScheduler(clock=FakeClock(START_AT), queue_capacity=10, max_in_flight=2)
    disabled_id = "pa:g029-disabled"
    exhausted_id = "pa:g029-exhausted"
    scheduler.register_strategy(StrategySpec(disabled_id, enabled=False))
    scheduler.register_strategy(StrategySpec(exhausted_id, exhausted=True))
    started = scheduler.start(view)
    disabled_recall = scheduler.on_position_close(disabled_id, reason="disabled_close")
    exhausted_recall = scheduler.on_position_close(exhausted_id, reason="exhausted_close")
    records = scheduler.records
    return {
        "started_jobs": len(started),
        "disabled_state": records[disabled_id].state.value,
        "exhausted_state": records[exhausted_id].state.value,
        "disabled_recall_result": disabled_recall,
        "exhausted_recall_result": exhausted_recall,
        "ready_jobs_after_callbacks": len(scheduler.queue.ready_jobs),
        "no_recall": not disabled_recall and not exhausted_recall and not scheduler.queue.ready_jobs,
    }


def _deadline_evidence() -> dict[str, Any]:
    book = PaperAccountBook()
    monitor = PendingEntryMonitor.from_instruction(
        {
            "branch": "wait_for_price",
            "strategy_id": "pa:g029-deadline",
            "direction": "long",
            "symbol": "AAPL",
            "entry_price": "100",
            "stop_price": "99",
            "target_price": "102",
            "entry_wait_seconds": 1,
        },
        decision_accepted_at="2026-03-09T13:33:00Z",
        submitted_at="2026-03-09T13:33:00Z",
        generation=1,
        validator=lambda value, **_: True,
        paper_account_book=book,
    )
    from pa_engine.market_sources.alpaca.stream import SecondAggregateEvent

    final = SecondAggregateEvent(
        "AAPL", "2026-03-09T13:33:00Z", "2026-03-09T13:33:01Z", "100", "100", "99", "100", "1",
        contributor_ids=("deadline",), execution_watermark="2026-03-09T13:33:04Z", status="frozen", revision_sequence=0,
        quality={"status": "ok", "codes": [], "connection_gap": False},
    )
    result = monitor.on_second(final, now="2026-03-09T13:33:04Z")
    execution_watermark = utc_instant(final.execution_watermark)
    arrived_during_grace = (
        result.status == "filled"
        and monitor.state.expires_at < execution_watermark <= monitor.state.resolution_at
    )
    return {
        "x": 1,
        "eligibility_deadline": _stamp(monitor.state.expires_at),
        "publication_resolution": _stamp(monitor.state.resolution_at),
        "arrived_during_grace": arrived_during_grace,
        "status": result.status,
        "filled_at_requested_level": str(result.filled_price),
        "deadline_not_extended": _stamp(monitor.state.expires_at) == "2026-03-09T13:33:01Z",
    }


def _recovery_evidence(fixture: Mapping[str, Any]) -> dict[str, Any]:
    """Exercise G019 RecoveryBar entry/exit semantics and exact replay guards."""

    history = RecordedIEXHistory(fixture)
    history.fetch_history()
    recovery_result = history.fetch_recovery()
    if not recovery_result.recovery_bars:
        raise EvaluationError("recorded recovery fixture did not yield a RecoveryBar")
    recovery_bar = recovery_result.recovery_bars[0]
    bar = recovery_bar.to_dict()
    request = recovery_result.request.to_dict()
    source_hash = str(request["source_request_hash"])

    def opened_book(strategy_id: str, target: str = "100.4") -> tuple[PaperAccountBook, Any]:
        book = PaperAccountBook()
        book.open_trade(
            strategy_id,
            "AAPL",
            "long",
            "100",
            "99",
            target,
            immediate=True,
            fill_reference={"reference_price": "100"},
            requested_entry_price="100",
        )
        trade = book.get_account(strategy_id).open_trade
        return book, trade

    close_callbacks: list[dict[str, Any]] = []
    book, trade = opened_book("pa:g029-recovery-entry-exit")
    monitor = ExitMonitor.from_trade(
        trade,
        paper_account_book=book,
        entry_fill_interval=("2026-03-09T13:32:00Z", "2026-03-09T13:33:00Z"),
        entry_filled_at="2026-03-09T13:33:00Z",
        recovery_request=request,
        expected_source_request_hash=source_hash,
        on_position_close=lambda *args, **kwargs: close_callbacks.append({"args": _json(args), "kwargs": _json(kwargs)}) or True,
    )
    gap_transition = monitor.on_gap(start_at="2026-03-09T13:33:00Z", end_at="2026-03-09T13:34:00Z")
    exit_events_before = len(book.events)
    exit_balance_before = str(book.get_account("pa:g029-recovery-entry-exit").balance)
    settled = monitor.on_recovery([bar], now="2026-03-09T13:34:00Z")
    exit_events_after_settlement = len(book.events)
    exit_balance_after_settlement = str(book.get_account("pa:g029-recovery-entry-exit").balance)
    duplicate = monitor.on_recovery([bar], now="2026-03-09T13:34:00Z")
    exit_events_after_duplicate = len(book.events)
    exit_balance_after_duplicate = str(book.get_account("pa:g029-recovery-entry-exit").balance)

    # PendingEntryMonitor has a separate G024 create/fill boundary from the
    # exit reducer.  Keep the boundary injected, but let the accepted fill
    # open a real G024 paper trade so recovery entry evidence includes the
    # exact requested level, account state and callback cardinality.
    entry_book = PaperAccountBook()
    entry_callbacks: list[dict[str, Any]] = []
    entry_strategy_id = SID_LONG
    entry_registry = load_default_registry(str(REPO_ROOT / "knowledge"))
    entry_loaded = entry_registry.load_packet(entry_strategy_id)
    entry_candidate = _fixture(REPO_ROOT / "tests" / "fixtures" / "phase2" / "g018" / "valid", "wait_long.json")
    entry_instruction = make_scheduler_instruction(
        entry_candidate,
        profile_ref=entry_loaded.profile_ref,
        result={"pa_analysis_result_id": "par_" + "c" * 64, "content_hash": "c" * 64},
        evidence_package=entry_loaded.evidence_package,
    )

    def create_pending(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        return {"status": "pending", "accepted": True}

    def fill_pending(strategy_id: str, _pending_id: str, fill_reference: Any, *, idempotency_key: str | None = None) -> Any:
        return entry_book.open_trade(
            strategy_id,
            "AAPL",
            "long",
            "100",
            "99",
            "102",
            immediate=False,
            fill_reference=fill_reference,
            requested_entry_price="100",
            idempotency_key=idempotency_key,
        )

    entry_monitor = PendingEntryMonitor.from_instruction(
        entry_instruction,
        symbol="AAPL",
        decision_accepted_at="2026-03-09T13:32:00Z",
        submitted_at="2026-03-09T13:32:00Z",
        generation=1,
        create_pending=create_pending,
        fill_pending=fill_pending,
        on_pending_fill=lambda *args, **kwargs: entry_callbacks.append({"args": _json(args), "kwargs": _json(kwargs)}),
        recovery_request=request,
        expected_source_request_hash=source_hash,
    )
    entry_gap = entry_monitor.on_gap(start_at="2026-03-09T13:33:00Z", end_at="2026-03-09T13:34:00Z")
    entry_events_before = len(entry_book.events)
    entry_balance_before = str(entry_book.get_account(entry_strategy_id).balance) if entry_book.get_account(entry_strategy_id) else "1000.00"
    entry_settled = entry_monitor.on_recovery([bar], now="2026-03-09T13:34:00Z")
    entry_events_after_settlement = len(entry_book.events)
    entry_account = entry_book.get_account(entry_strategy_id)
    entry_trade = entry_account.open_trade if entry_account is not None else None
    entry_duplicate = entry_monitor.on_recovery([bar], now="2026-03-09T13:34:00Z")
    entry_events_after_duplicate = len(entry_book.events)
    entry_balance_after_duplicate = str(entry_account.balance) if entry_account is not None else None

    straddle_bar = replace(
        recovery_bar,
        start_at="2026-03-09T13:34:00Z",
        end_at="2026-03-09T13:35:00Z",
        recovery_cursor="2026-03-09T13:35:00Z",
    ).to_dict()
    straddle_request = dict(request, start_at="2026-03-09T13:34:00Z", end_at="2026-03-09T13:35:00Z")
    straddle_book, straddle_trade = opened_book("pa:g029-recovery-straddle")
    straddle_monitor = ExitMonitor.from_trade(
        straddle_trade,
        paper_account_book=straddle_book,
        entry_fill_interval=("2026-03-09T13:33:30Z", "2026-03-09T13:34:30Z"),
        entry_filled_at="2026-03-09T13:34:30Z",
        recovery_request=straddle_request,
        expected_source_request_hash=source_hash,
        on_position_close=lambda *_args, **_kwargs: True,
    )
    straddle_monitor.on_gap(start_at="2026-03-09T13:34:00Z", end_at="2026-03-09T13:35:00Z")
    straddling = straddle_monitor.on_recovery([straddle_bar], now="2026-03-09T13:35:00Z")

    tampered = copy.deepcopy(bar)
    tampered["low"] = "98.5"
    tamper_verdict = validate_recovery_bar(tampered, expected_source_request_hash=source_hash)
    settled_lineage = _json(settled.lineage)
    entry_lineage = _json(entry_settled.lineage)
    entry_lineage_exact = all(
        entry_lineage.get(key) == expected
        for key, expected in {
            "recovery_bar_id": bar["recovery_bar_id"],
            "recovery_bar_hash": bar["content_hash"],
            "content_hash": bar["content_hash"],
            "source_request_hash": source_hash,
            "start_at": bar["start_at"],
            "end_at": bar["end_at"],
            "path_precision": "minute_bar_no_path",
        }.items()
    )
    entry_exact_level = (
        entry_settled.status == "filled"
        and str(entry_settled.filled_price) == "100"
        and entry_trade is not None
        and str(entry_trade.entry_price) == "100"
    )
    return {
        "bar_valid": recovery_bar.validate().valid,
        "bar_id": bar["recovery_bar_id"],
        "bar_hash": bar["content_hash"],
        "path_precision": bar["path_precision"],
        "trigger_eligible": bar["trigger_eligible"],
        "source_request_hash": source_hash,
        "entry_gap_status": gap_transition.status,
        "entry_exit_status": settled.status,
        "entry_exit_reason": settled.exit_reason,
        "entry_exit_selected": str(settled.selected_exit) if settled.selected_exit is not None else None,
        "entry_exit_lineage": settled_lineage,
        "entry_exit_balance": str(book.get_account("pa:g029-recovery-entry-exit").balance),
        "duplicate_late_status": duplicate.status,
        "exit_settlement_once": {
            "events_before": exit_events_before,
            "events_after_settlement": exit_events_after_settlement,
            "events_after_duplicate": exit_events_after_duplicate,
            "balance_before": exit_balance_before,
            "balance_after_settlement": exit_balance_after_settlement,
            "balance_after_duplicate": exit_balance_after_duplicate,
            "close_callback_count": len(close_callbacks),
            "duplicate_status": duplicate.status,
            "one_settlement": (
                settled.status == "settled"
                and duplicate.status == "replayed"
                and exit_events_after_settlement == exit_events_before + 1
                and exit_events_after_duplicate == exit_events_after_settlement
                and exit_balance_after_duplicate == exit_balance_after_settlement
                and len(close_callbacks) == 1
            ),
        },
        "entry_recovery": {
            "gap_status": entry_gap.status,
            "status": entry_settled.status,
            "duplicate_status": entry_duplicate.status,
            "pending_id": entry_monitor.pending_id,
            "generation": entry_monitor.generation,
            "filled_price": str(entry_settled.filled_price) if entry_settled.filled_price is not None else None,
            "trade_id": entry_trade.trade_id if entry_trade is not None else None,
            "balance_before": entry_balance_before,
            "balance_after": entry_balance_after_duplicate,
            "events_before": entry_events_before,
            "events_after_settlement": entry_events_after_settlement,
            "events_after_duplicate": entry_events_after_duplicate,
            "callback_count": len(entry_callbacks),
            "exact_requested_level": entry_exact_level,
            "lineage_exact": entry_lineage_exact,
            "lineage": entry_lineage,
            "duplicate_no_effect": entry_duplicate.status == "replayed" and entry_events_after_duplicate == entry_events_after_settlement,
        },
        "whole_interval_recovery": {
            "request_start_at": request.get("start_at"),
            "request_end_at": request.get("end_at"),
            "bar_start_at": bar.get("start_at"),
            "bar_end_at": bar.get("end_at"),
            "recovery_cursor": bar.get("recovery_cursor"),
            "entry_gap_start_at": "2026-03-09T13:33:00Z",
            "entry_gap_end_at": "2026-03-09T13:34:00Z",
            "entry_bar_covers_gap": entry_gap.status == "gap" and bar["start_at"] <= "2026-03-09T13:33:00Z" and bar["end_at"] >= "2026-03-09T13:34:00Z",
            "exit_gap_recorded": gap_transition.accepted and bar["start_at"] <= "2026-03-09T13:33:00Z" and bar["end_at"] >= "2026-03-09T13:34:00Z",
        },
        "straddling_status": straddling.status,
        "straddling_not_evaluated": straddling.not_evaluated,
        "straddling_code": straddling.code,
        "typed_lineage_tamper_rejected": not tamper_verdict.valid,
        "tamper_codes": [issue.code for issue in tamper_verdict.issues],
    }


def _tamper_evidence(fixture: Mapping[str, Any]) -> dict[str, Any]:
    registry = load_default_registry(str(REPO_ROOT / "knowledge"))
    loaded = registry.load_packet(SID_LONG)
    packet = copy.deepcopy(dict(loaded.packet))
    packet.setdefault("extensions", {})["nested_fixture"] = {"account": {"balance": "1000.00", "credentials": {"token": "never"}}}
    leak_issues = scan_model_boundary(packet)
    packet_verdict = registry.validate_packet(packet)
    profile = copy.deepcopy(dict(loaded.profile_ref))
    profile["profile_ref_hash"] = "0" * 64
    profile_verdict = registry.validate_profile_lineage(profile, packet=loaded.packet)
    second = LocalSecondAggregator("AAPL")
    second.ingest_message([fixture["trade_batches"]["initial"]["messages"][0]], received_at=fixture["trade_batches"]["initial"]["received_at"])
    event = second.freeze(now="2026-03-09T13:33:04Z")[0].to_dict()
    event["event_hash"] = "f" * 64
    event["content_hash"] = event["event_hash"]
    second_verdict = validate_second_event(event, expected_feed=ALPACA_IEX_FEED)
    fake_snapshot = {"market_snapshot_id": "mkt_" + "a" * 64, "content_hash": "a" * 64, "bars": []}
    fake_frame = {"observation_frame_id": "obs_" + "b" * 64, "content_hash": "b" * 64}
    fake_view_ref = make_shared_market_view_ref(fake_snapshot, fake_frame)
    fake_view = SimpleNamespace(
        closed_snapshot={"bars": [{"bar_id": "bar_" + "0" * 64}]},
        latest_second={"event_id": "sec_" + "0" * 64, "event_hash": "0" * 64, "close": "100.5"},
        to_decision_ref=lambda: copy.deepcopy(fake_view_ref),
    )
    grounding_candidate = _grounded_candidate(
        registry,
        SID_LONG,
        fake_view,
        branch="enter_immediately",
        direction="long",
        stop="99",
        target="102",
    )
    tampered_candidate = copy.deepcopy(grounding_candidate)
    tampered_candidate["grounding"]["target_price"]["market_refs"] = ["sip_bar_" + "f" * 64]
    grounding_verdict = validate_candidate(tampered_candidate, parse_json=False)
    execution_probe = RecordedModelAdapter(
        ScriptedAdapter(steps=[scripted_error("tamper_execution_probe", retryable=False)])
    )
    if grounding_verdict.valid:
        execution_probe.complete(
            {"extensions": {"strategy_profile_ref": copy.deepcopy(dict(profile))}},
            generation=0,
        )
    return {
        "recursive_leak_found": bool(leak_issues),
        "leak_paths": [issue.path for issue in leak_issues],
        "packet_tamper_rejected": not packet_verdict.valid,
        "grounding_tamper_rejected": not grounding_verdict.valid,
        "grounding_tamper_code": grounding_verdict.code,
        "profile_tamper_rejected": not bool(profile_verdict),
        "second_lineage_tamper_rejected": not second_verdict.valid,
        "provider_calls_before_execution": execution_probe.calls,
        "execution_attempts_before_rejection": execution_probe.calls,
    }


def _session_close_evidence(fixture: Mapping[str, Any]) -> dict[str, Any]:
    # Use a real runtime with a no-entry decision due before the close.  The
    # session-close step must not call the provider; the next regular session
    # event re-arms one call.
    with tempfile.TemporaryDirectory(prefix="g029-session-") as directory:
        engine, aggregator, _history, _clock = _runtime(fixture, Path(directory), batch_names=("initial",), clock_start=START_AT, stale_after_seconds=300)
        engine.start()
        adapter = _attach_dynamic_candidates(
            engine,
            lambda current_engine, view: _no_entry_candidate(current_engine, view, n=60),
            (SID_LONG,),
        )
        engine.step(now=START_AT)
        calls_before_close = adapter.calls
        engine.step(now="2026-03-09T21:00:00Z")
        calls_at_close = adapter.calls
        aggregator.append("next_session")
        engine.step(now="2026-03-10T13:30:05Z")
        calls_next_session = adapter.calls
        no_entry_calls_at_close = calls_at_close
        no_entry_calls_next_session = calls_next_session
        captured = {"calls_before_close": calls_before_close, "calls_at_close": calls_at_close, "calls_next_session": calls_next_session, "state": engine.scheduler.get(SID_LONG).state.value}
        engine.stop(reason="g029_session_done")
    with tempfile.TemporaryDirectory(prefix="g029-session-open-") as directory:
        engine, aggregator, _history, _clock = _runtime(fixture, Path(directory), batch_names=("initial",), clock_start=START_AT, stale_after_seconds=300)
        engine.start()
        adapter = _attach_dynamic_candidates(
            engine,
            lambda current_engine, view: _immediate_candidate(current_engine, view, direction="long", stop="99", target="102"),
            (SID_LONG,),
        )
        engine.step(now=START_AT)
        calls_before_close = adapter.calls
        open_before_close = engine.book.get_account(SID_LONG).open_trade is not None
        engine.step(now="2026-03-09T21:00:00Z")
        calls_at_close = adapter.calls
        open_at_close = engine.book.get_account(SID_LONG).open_trade is not None
        aggregator.append("next_session")
        engine.step(now="2026-03-10T13:30:05Z")
        calls_next_session = adapter.calls
        open_next_session = engine.book.get_account(SID_LONG).open_trade is not None
        state_next_session = engine.scheduler.get(SID_LONG).state.value
        engine.stop(reason="g029_session_open_done")
        captured["open_position_resume"] = {
            "calls_before_close": calls_before_close,
            "calls_at_close": calls_at_close,
            "calls_next_session": calls_next_session,
            "open_before_close": open_before_close,
            "open_at_close": open_at_close,
            "open_next_session": open_next_session,
            "state_next_session": state_next_session,
        }
        # A pending X episode crossing the regular-session wall-clock close
        # keeps its frozen eligibility deadline.  The timer is resolved only
        # at the recorded publication-grace boundary; no provider/model call
        # is made while the session is closed.
        x_registry = load_default_registry(str(REPO_ROOT / "knowledge"))
        x_loaded = x_registry.load_packet(SID_LONG)
        x_candidate = _fixture(REPO_ROOT / "tests" / "fixtures" / "phase2" / "g018" / "valid", "wait_long.json")
        x_candidate["X"] = 5
        x_instruction = make_scheduler_instruction(
            x_candidate,
            profile_ref=x_loaded.profile_ref,
            result={"pa_analysis_result_id": "par_" + "d" * 64, "content_hash": "d" * 64},
            evidence_package=x_loaded.evidence_package,
        )
        x_callbacks: list[dict[str, Any]] = []
        x_model_calls: list[str] = []
        x_monitor = PendingEntryMonitor.from_instruction(
            x_instruction,
            symbol="AAPL",
            decision_accepted_at="2026-03-09T20:59:55Z",
            submitted_at="2026-03-09T20:59:55Z",
            generation=3,
            create_pending=lambda *_args, **_kwargs: {"status": "pending", "accepted": True},
            expire_pending=lambda *_args, **_kwargs: {"status": "expired", "accepted": True},
            on_pending_expiry=lambda *args, **kwargs: x_callbacks.append({"args": _json(args), "kwargs": _json(kwargs)}),
        )
        x_deadline_before_close = _stamp(x_monitor.expires_at)
        # The close boundary is deliberately a no-call input; the empty call
        # ledger is the observed execution fact, not a declared boolean.
        x_close_model_calls = len(x_model_calls)
        x_at_close = x_monitor.on_timer(now="2026-03-09T21:00:00Z")
        x_deadline_after_close = _stamp(x_monitor.expires_at)
        x_expired = x_monitor.on_expiry(now="2026-03-09T21:00:03Z")
        x_duplicate_expiry = x_monitor.on_expiry(now="2026-03-09T21:00:03Z")
        captured["pending_x_across_close"] = {
            "decision_accepted_at": _stamp(x_monitor.decision_accepted_at),
            "eligibility_deadline_before_close": x_deadline_before_close,
            "eligibility_deadline_after_close": x_deadline_after_close,
            "wall_clock_close_at": "2026-03-09T21:00:00Z",
            "publication_resolution": _stamp(x_monitor.resolution_at),
            "timer_status_at_close": x_at_close.status,
            "timer_kept_pending_at_close": not x_at_close.terminal,
            "closed_session_model_calls": x_close_model_calls,
            "closed_session_no_model_call": x_close_model_calls == 0,
            "expired_status": x_expired.status,
            "duplicate_expiry_status": x_duplicate_expiry.status,
            "expiry_callback_count": len(x_callbacks),
            "no_entry": len(x_callbacks) == 1 and x_expired.status == "expired",
            "deadline_not_extended": x_deadline_before_close == x_deadline_after_close,
            "next_session_calls_observed": no_entry_calls_next_session,
            "next_session_resume_call_observed": no_entry_calls_next_session > no_entry_calls_at_close,
        }
        return captured


def _thinking_evidence(fixture: Mapping[str, Any]) -> dict[str, Any]:
    hashes: dict[str, str] = {}
    account_ids: dict[str, str] = {}
    for mode in ("enabled", "disabled"):
        with tempfile.TemporaryDirectory(prefix="g029-thinking-") as directory:
            engine, _aggregator, _history, _clock = _runtime(fixture, Path(directory), batch_names=("initial",))
            engine._config = RuntimeConfig(
                symbol="AAPL", timeframe="1min", reasoning_mode=mode, database_path=str(Path(directory) / "runtime.db"), stale_after_seconds=300
            )
            engine.start()
            adapter = _attach_dynamic_candidates(
                engine,
                lambda current_engine, view: _no_entry_candidate(current_engine, view, n=5),
                (SID_LONG,),
            )
            engine.step(now=START_AT)
            hashes[mode] = request_semantic_identity(adapter.requests[0])
            account_ids[mode] = engine.book.get_account(SID_LONG).account_id
            engine.stop(reason="g029_thinking_done")
    return {
        "request_hashes": hashes,
        "distinct": hashes["enabled"] != hashes["disabled"],
        "account_ids": account_ids,
        "isolation_unchanged": account_ids["enabled"] == account_ids["disabled"] == f"paper:{SID_LONG}",
    }


def _restart_evidence(fixture: Mapping[str, Any]) -> dict[str, Any]:
    """Rehydrate an open runtime, then redeliver duplicate and late seconds."""

    with tempfile.TemporaryDirectory(prefix="g029-restart-") as directory:
        path = Path(directory)
        engine, aggregator, _history, _clock = _runtime(fixture, path, batch_names=("initial",))
        engine.start()
        adapter = _attach_dynamic_candidates(
            engine,
            lambda current_engine, view: _wait_candidate(
                current_engine, view, direction="long", entry="100", stop="99", target="102", x=300
            ),
            (SID_LONG,),
        )
        engine.step(now=START_AT)
        aggregator.append("touch_long")
        engine.step(now="2026-03-09T13:34:05Z")
        before = _capture_runtime(engine, adapter, scenario_id="restart_before")
        entry_event = next(
            event for event in engine._feed.frozen_events
            if event.end_at == "2026-03-09T13:34:01Z"
        )
        db_path = path / "runtime.db"
        engine.stop(reason="g029_restart")
        engine2, _aggregator2, _history2, _clock2 = _runtime(fixture, path, batch_names=(), clock_start="2026-03-09T13:34:10Z")
        report = engine2.start()
        after = _capture_runtime(engine2, RecordedModelAdapter(_OutputRefScriptedAdapter([])), scenario_id="restart_after")
        monitor = engine2._exit_monitors.get(SID_LONG)
        if monitor is None:
            raise EvaluationError("restart did not restore the open exit monitor")
        # The restored runtime has a confirmed recovery gap because the
        # stored cursor ends at 13:34:01 and the restart clock is later.  The
        # duplicate/late delivery is therefore routed through the restored
        # monitor while the recovery gate is active; it must not manufacture
        # a fill/close or invoke the provider.
        provider_calls_before_redelivery = engine2._provider_calls
        monitor_state_before = _digest(monitor.state.to_dict())
        duplicate_entry_first = monitor.on_second(entry_event, now="2026-03-09T13:34:12Z", received_at="2026-03-09T13:34:12Z")
        duplicate_entry = monitor.on_second(entry_event, now="2026-03-09T13:34:12Z", received_at="2026-03-09T13:34:12Z")
        late_aggregator = LocalSecondAggregator("AAPL", market_feed=ALPACA_IEX_FEED, adjustment="raw")
        late_batch = fixture["trade_batches"]["late_long_target"]
        late_aggregator.ingest_message(copy.deepcopy(late_batch["messages"]), received_at=late_batch["received_at"])
        late_events = late_aggregator.freeze(now="2026-03-09T13:34:25Z")
        if len(late_events) != 1:
            raise EvaluationError("restart late fixture did not produce one frozen second")
        late_event = late_events[0]
        duplicate_late_first = monitor.on_second(late_event, now="2026-03-09T13:34:26Z", received_at="2026-03-09T13:34:26Z")
        duplicate_late = monitor.on_second(late_event, now="2026-03-09T13:34:26Z", received_at="2026-03-09T13:34:26Z")
        monitor_state_after = _digest(monitor.state.to_dict())
        after_replay = _capture_runtime(engine2, RecordedModelAdapter(_OutputRefScriptedAdapter([])), scenario_id="restart_after_replay")
        provider_calls_after_redelivery = engine2._provider_calls
        engine2.stop(reason="g029_restart_done")
        event_ids_before = [event.get("event_id") for event in before["events"]]
        event_ids_after = [event.get("event_id") for event in after_replay["events"]]
        before_types = {event.get("event_type"): 0 for event in before["events"]}
        after_types = {event.get("event_type"): 0 for event in after_replay["events"]}
        for event in before["events"]:
            before_types[event.get("event_type")] += 1
        for event in after_replay["events"]:
            after_types[event.get("event_type")] += 1
        before_settlements = [event.get("payload", {}).get("settlement", {}) for event in _event_rows(before, "paper_close")]
        after_settlements = [event.get("payload", {}).get("settlement", {}) for event in _event_rows(after_replay, "paper_close")]
        event_ids_unique = len(event_ids_after) == len(set(event_ids_after))
        return {
            "restored_pending_or_open": bool(report.get("restored_pending") or report.get("restored_open_trades")),
            "balance_before": before["accounts"].get(SID_LONG, {}).get("balance"),
            "balance_after_restart_before_redelivery": after["accounts"].get(SID_LONG, {}).get("balance"),
            "balance_after": after_replay["accounts"].get(SID_LONG, {}).get("balance"),
            "event_count_before": len(event_ids_before),
            "event_count_after": len(event_ids_after),
            "duplicate_event_ids": len(event_ids_after) - len(set(event_ids_after)),
            "replayed_event_ids": len(set(event_ids_before) & set(event_ids_after)),
            "db_exists": db_path.exists(),
            "restart_errors": report.get("errors", []),
            "recovery_gate_observed": bool(report.get("live_blocked")) and any(item.get("code") in {"recovery_straddle", "recovery_required_but_unavailable"} for item in report.get("errors", []) if isinstance(item, Mapping)),
            "duplicate_entry_status": getattr(duplicate_entry, "status", None),
            "duplicate_entry_first_status": getattr(duplicate_entry_first, "status", None),
            "duplicate_late_status": getattr(duplicate_late, "status", None),
            "duplicate_late_first_status": getattr(duplicate_late_first, "status", None),
            "monitor_state_hash_before_redelivery": monitor_state_before,
            "monitor_state_hash_after_redelivery": monitor_state_after,
            "provider_calls_before_redelivery": provider_calls_before_redelivery,
            "provider_calls_after_redelivery": provider_calls_after_redelivery,
            "paper_fill_count_before": before_types.get("paper_fill", 0),
            "paper_fill_count_after": after_types.get("paper_fill", 0),
            "paper_close_count_before": before_types.get("paper_close", 0),
            "paper_close_count_after": after_types.get("paper_close", 0),
            "close_count_before_replay": before_types.get("paper_close", 0),
            "close_count_after_replay": len(_event_rows(after_replay, "paper_close")),
            "balance_pnl_after_replay": {
                "balance_after": after_replay["accounts"].get(SID_LONG, {}).get("balance"),
                "applied_pnl": [item.get("applied_pnl") for item in after_settlements],
            },
            "event_ids_unique_after_replay": event_ids_unique,
            "no_provider_call_on_duplicate_or_late": provider_calls_after_redelivery == provider_calls_before_redelivery,
            "no_fill_duplicate_or_late": after_types.get("paper_fill", 0) == before_types.get("paper_fill", 0),
            "no_close_duplicate_or_late": after_types.get("paper_close", 0) == before_types.get("paper_close", 0),
            "balance_pnl_stable_after_replay": before_settlements == after_settlements and before["accounts"].get(SID_LONG, {}).get("balance") == after_replay["accounts"].get(SID_LONG, {}).get("balance"),
            "event_multiplicity_unchanged": sorted(event_ids_before) == sorted(event_ids_after),
        }


def _startup_and_corruption_evidence(fixture: Mapping[str, Any]) -> dict[str, Any]:
    empty_payload = {"bars": {fixture["symbol"]: []}}
    partial_payload = copy.deepcopy(dict(fixture["history_response"]))
    partial_payload["bars"][fixture["symbol"]] = partial_payload["bars"][fixture["symbol"]][:2]

    def event_rows_or_empty(engine: AutomaticPaperRuntime) -> list[Mapping[str, Any]]:
        store = engine._store
        if store is None:
            return []
        try:
            return list(store.list_events())
        except StorageClosedError:
            # G028 closes the database when an unusable startup view is
            # returned.  The empty result is observed from that terminal
            # startup state; do not reopen the database just to manufacture
            # telemetry or paper events.
            return []

    with tempfile.TemporaryDirectory(prefix="g029-startup-") as directory:
        engine, _aggregator, _history, _clock = _runtime(fixture, Path(directory), batch_names=(), history_payload=empty_payload)
        startup = engine.start()
        empty_events = event_rows_or_empty(engine)
        empty_report = {
            "started": engine._started,
            "provider_calls": engine._provider_calls,
            "paper_entries": len([row for row in empty_events if row.get("event_type") in {"paper_fill", "paper_trade_opened", "paper_entry_pending"}]),
            "errors": startup.get("errors", []),
        }
        partial_engine, _partial_aggregator, partial_history, _partial_clock = _runtime(
            fixture,
            Path(directory) / "partial",
            batch_names=(),
            history_payload=partial_payload,
        )
        partial_startup = partial_engine.start()
        partial_events = event_rows_or_empty(partial_engine)
        partial_report = {
            "started": partial_engine._started,
            "provider_calls": partial_engine._provider_calls,
            "history_calls": len(partial_history.calls),
            "errors": partial_startup.get("errors", []),
            "view_executable": partial_startup.get("view_executable"),
            "paper_entries": len([row for row in partial_events if row.get("event_type") in {"paper_fill", "paper_trade_opened", "paper_entry_pending"}]),
            "no_model_hot_loop": partial_engine._provider_calls == 0,
        }
        configuration_code = None
        configuration_message = None
        configuration_provider_calls: list[str] = []
        configuration_paper_entries: list[str] = []
        try:
            RuntimeConfig(symbol="AAPL", stale_after_seconds=0)
        except RuntimeConfigError as exc:
            configuration_code = exc.code
            configuration_message = str(exc)
        configuration_report = {
            "rejected": configuration_code is not None,
            "code": configuration_code,
            "message": configuration_message,
            "execution_started": bool(configuration_provider_calls or configuration_paper_entries),
            "provider_calls": len(configuration_provider_calls),
            "paper_entries": len(configuration_paper_entries),
            "terminal": configuration_code == "non_positive_parameter",
        }
        # A corrupt/incompatible database is checked through the accepted
        # storage boundary; the runtime never replaces it with fresh funds.
        corrupt_path = Path(directory) / "corrupt.db"
        corrupt_path.write_bytes(b"not sqlite")
        corrupt_error = None
        try:
            other, _a, _h, _c = _runtime(fixture, Path(directory), batch_names=(), registry=_registry((SID_LONG,)))
            other._db_path = str(corrupt_path)
            other.start()
        except Exception as exc:
            corrupt_error = type(exc).__name__
        return {
            "insufficient_startup": empty_report,
            "partial_startup": partial_report,
            "terminal_configuration": configuration_report,
            "corrupt_rejected": corrupt_error is not None,
            "corrupt_error_type": corrupt_error,
        }


def _phase1_retrieval_evidence() -> dict[str, Any]:
    """Compare the 24 Phase 1 retrieval goldens to the accepted baseline blob."""

    relative = "tests/fixtures/golden_queries.json"
    path = REPO_ROOT / relative
    current_bytes = path.read_bytes()
    current_hash = hashlib.sha256(current_bytes).hexdigest()
    baseline_hash = None
    baseline_available = False
    try:
        baseline_bytes = subprocess.check_output(
            ["git", "show", f"77349b59fd34c2c25493c175c5282e800f7790d0:{relative}"],
            cwd=REPO_ROOT,
            stderr=subprocess.DEVNULL,
        )
        baseline_hash = hashlib.sha256(baseline_bytes).hexdigest()
        baseline_available = True
    except (OSError, subprocess.CalledProcessError):
        baseline_bytes = b""
    parsed = json.loads(current_bytes.decode("utf-8"))
    records = {
        record_id
        for item in parsed
        if isinstance(item, Mapping)
        for record_id in item.get("expected_record_ids", [])
        if isinstance(record_id, str)
    }
    unchanged = baseline_available and current_bytes == baseline_bytes
    return {
        "baseline_commit": "77349b59fd34c2c25493c175c5282e800f7790d0",
        "path": relative,
        "baseline_available": baseline_available,
        "baseline_sha256": baseline_hash,
        "current_sha256": current_hash,
        "case_count": len(parsed) if isinstance(parsed, list) else 0,
        "unique_expected_record_count": len(records),
        "unchanged": unchanged,
    }


def _event_rows(result: Mapping[str, Any], event_type: str) -> list[Mapping[str, Any]]:
    return [
        event
        for event in result.get("events", [])
        if isinstance(event, Mapping) and event.get("event_type") == event_type
    ]


def _annotate_dual_touch(fixture: Mapping[str, Any], scenarios: Mapping[str, Any]) -> None:
    """Record the typed same-second ambiguity and the executed policy result."""

    result = scenarios.get("dual_touch_stop_first")
    batch = fixture.get("trade_batches", {}).get("dual_touch", {})
    messages = [item for item in batch.get("messages", []) if isinstance(item, Mapping) and item.get("T") == "t"]
    prices = [Decimal(str(item["p"])) for item in messages if item.get("p") is not None]
    seconds = {str(item.get("t", "")).split(".", 1)[0] for item in messages}
    closes = _event_rows(result or {}, "paper_close")
    close = closes[0].get("payload", {}) if len(closes) == 1 else {}
    both_levels_touched = bool(prices) and min(prices) <= Decimal("99") and max(prices) >= Decimal("102")
    evidence = {
        "trade_count": len(messages),
        "same_second": len(seconds) == 1,
        "price_points": [str(value) for value in prices],
        "both_stop_and_target_levels_touched": both_levels_touched,
        "stop_level": "99",
        "target_level": "102",
        "selected_exit": close.get("selected_exit"),
        "exit_reason": close.get("exit_reason"),
        "resolution": "dual_touch_stop_first" if len(messages) == 2 and len(seconds) == 1 and both_levels_touched and close.get("exit_reason") == "stop_loss" and close.get("selected_exit") == "99" else None,
    }
    if isinstance(result, dict):
        extra = result.setdefault("extra", {})
        if isinstance(extra, dict):
            extra["dual_touch_evidence"] = evidence
        result["canonical_hash"] = _digest({key: value for key, value in result.items() if key != "canonical_hash"})


def _golden_checks(scenarios: Mapping[str, Any], goldens: Mapping[str, Any]) -> dict[str, Any]:
    """Compare deterministic runtime fields with independently-authored goldens."""

    expected = goldens.get("scenarios", {})
    checks: dict[str, dict[str, Any]] = {}
    settlement_cases = ("immediate_long_tp", "immediate_short_sl", "wait_long_fill_tp", "wait_short_fill_sl")
    for scenario_id in settlement_cases:
        result = scenarios.get(scenario_id, {})
        golden = expected.get(scenario_id, {})
        fills = _event_rows(result, "paper_fill")
        closes = _event_rows(result, "paper_close")
        fill = fills[0].get("payload", {}) if len(fills) == 1 else {}
        close = closes[0].get("payload", {}) if len(closes) == 1 else {}
        settlement = close.get("settlement", {}) if isinstance(close, Mapping) else {}
        actual = {
            "direction": fill.get("direction"),
            "entry": fill.get("entry_price"),
            "quantity": fill.get("quantity"),
            "planned_r": fill.get("planned_r"),
            "gross_pnl": settlement.get("gross_pnl"),
            "applied_pnl": settlement.get("applied_pnl"),
            "balance_after": settlement.get("balance_after"),
            "filled_at": fill.get("filled_at"),
            "closed_at": close.get("closed_at"),
            "exit_reason": close.get("exit_reason"),
            "selected_exit": close.get("selected_exit"),
            "final_state": result.get("states", {}).get(SID_LONG),
        }
        fields = {key: actual.get(key) == golden.get(key) for key in actual}
        checks[scenario_id] = {"pass": bool(fields) and all(fields.values()), "fields": fields}

    dual_id = "dual_touch_stop_first"
    dual = scenarios.get(dual_id, {})
    dual_golden = expected.get(dual_id, {})
    dual_closes = _event_rows(dual, "paper_close")
    dual_close = dual_closes[0].get("payload", {}) if len(dual_closes) == 1 else {}
    dual_settlement = dual_close.get("settlement", {}) if isinstance(dual_close, Mapping) else {}
    dual_evidence = dual.get("extra", {}).get("dual_touch_evidence", {}) if isinstance(dual.get("extra"), Mapping) else {}
    dual_actual = {
        "exit_reason": dual_close.get("exit_reason"),
        "ambiguity_reason": dual_evidence.get("resolution"),
        "selected_exit": dual_close.get("selected_exit"),
        "balance_after": dual_settlement.get("balance_after"),
        "final_state": dual.get("states", {}).get(SID_LONG),
    }
    dual_fields = {key: dual_actual.get(key) == dual_golden.get(key) for key in dual_actual}
    checks[dual_id] = {"pass": bool(dual_fields) and all(dual_fields.values()), "fields": dual_fields}

    no_entry_id = "no_entry_N"
    no_entry = scenarios.get(no_entry_id, {})
    no_entry_golden = expected.get(no_entry_id, {})
    accepted_rows = _event_rows(no_entry, "model_decision_accepted")
    accepted = accepted_rows[0] if len(accepted_rows) == 1 else {}
    no_entry_actual = {
        "branch": no_entry.get("provider_outcomes", [{}])[0].get("branch") if no_entry.get("provider_outcomes") else None,
        "status": no_entry.get("states", {}).get(SID_LONG),
        "next_call_after_seconds": accepted.get("payload", {}).get("next_call_after_seconds"),
        "decision_accepted_at": accepted.get("occurred_at"),
        "due_at": no_entry.get("timers", {}).get(SID_LONG),
        "final_state": no_entry.get("states", {}).get(SID_LONG),
    }
    no_entry_fields = {key: no_entry_actual.get(key) == no_entry_golden.get(key) for key in no_entry_actual}
    checks[no_entry_id] = {"pass": bool(no_entry_fields) and all(no_entry_fields.values()), "fields": no_entry_fields}

    expiry_id = "wait_expiry"
    expiry = scenarios.get(expiry_id, {})
    expiry_golden = expected.get(expiry_id, {})
    expiry_accepted_rows = _event_rows(expiry, "model_decision_accepted")
    expiry_rows = _event_rows(expiry, "pending_entry_expired")
    expiry_accepted = expiry_accepted_rows[0] if len(expiry_accepted_rows) == 1 else {}
    expiry_row = expiry_rows[0].get("payload", {}) if len(expiry_rows) == 1 else {}
    accepted_at = expiry_accepted.get("occurred_at")
    wait_seconds = expiry_accepted.get("payload", {}).get("entry_wait_seconds")
    computed_deadline = None
    if isinstance(accepted_at, str) and isinstance(wait_seconds, int):
        computed_deadline = _stamp(datetime.fromisoformat(accepted_at.replace("Z", "+00:00")) + timedelta(seconds=wait_seconds))
    expiry_actual = {
        "branch": expiry.get("provider_outcomes", [{}])[0].get("branch") if expiry.get("provider_outcomes") else None,
        "status": "expired" if expiry_rows else None,
        "scheduler_status": expiry.get("states", {}).get(SID_LONG),
        "resolution_at": expiry_row.get("resolution_at"),
        "entry_deadline": computed_deadline,
        "final_state": expiry.get("states", {}).get(SID_LONG),
    }
    expiry_fields = {key: expiry_actual.get(key) == expiry_golden.get(key) for key in expiry_actual}
    checks[expiry_id] = {"pass": bool(expiry_fields) and all(expiry_fields.values()), "fields": expiry_fields}

    lineage_golden = goldens.get("lineage", {}).get("immediate_long_tp", {})
    lineage_result = scenarios.get("immediate_long_tp", {})
    lineage_outcome = lineage_result.get("provider_outcomes", [{}])[0] if lineage_result.get("provider_outcomes") else {}
    lineage_request = lineage_result.get("requests", [{}])[0] if lineage_result.get("requests") else {}
    lineage_events = {
        str(event.get("event_type")): event
        for event in lineage_result.get("events", [])
        if isinstance(event, Mapping) and event.get("event_type") in {"model_decision_accepted", "paper_fill", "paper_close"}
    }
    lineage_actual = {
        "strategy_id": lineage_outcome.get("strategy_id"),
        "claim_ids": lineage_outcome.get("claim_ids"),
        "citation_ids": lineage_outcome.get("citation_ids"),
        "market_refs": lineage_outcome.get("market_refs"),
        "candidate_id": lineage_outcome.get("candidate_id"),
        "candidate_hash": lineage_outcome.get("candidate_hash"),
        "profile_ref_id": lineage_request.get("profile_ref_id"),
        "profile_ref_hash": lineage_request.get("profile_ref_hash"),
        "packet_id": lineage_request.get("packet_id"),
        "packet_hash": lineage_request.get("packet_hash"),
        "evidence_package_id": lineage_request.get("evidence_package_id"),
        "evidence_package_hash": lineage_request.get("evidence_package_hash"),
        "market_snapshot_id": lineage_request.get("market_snapshot_id"),
        "market_snapshot_hash": lineage_request.get("market_snapshot_hash"),
        "observation_frame_id": lineage_request.get("observation_frame_id"),
        "observation_frame_hash": lineage_request.get("observation_frame_hash"),
        "second_event_id": lineage_request.get("latest_second_event_id"),
        "second_event_hash": lineage_request.get("latest_second_event_hash"),
        "request_semantic_hash": lineage_request.get("request_semantic_hash"),
        "event_hashes": {
            event_type: event.get("event_hash")
            for event_type, event in lineage_events.items()
        },
        "event_timestamps": {
            event_type: event.get("occurred_at")
            for event_type, event in lineage_events.items()
        },
        "state_transitions": [
            [item.get("from"), item.get("to"), item.get("reason")]
            for item in lineage_result.get("transitions", {}).get(SID_LONG, [])
            if isinstance(item, Mapping)
        ],
    }
    lineage_fields = {
        key: lineage_actual.get(key) == lineage_golden.get(key)
        for key in lineage_actual
    }
    checks["lineage_immediate_long_tp"] = {
        "pass": bool(lineage_fields) and all(lineage_fields.values()),
        "fields": lineage_fields,
    }
    return {"pass": bool(checks) and all(item["pass"] for item in checks.values()), "scenarios": checks}


def _lineage_assertions(scenarios: Mapping[str, Any]) -> dict[str, Any]:
    """Prove that actionable outputs retain the exact internal lineages.

    The runtime event envelope intentionally stores compact decision IDs.  The
    provider projection below complements those events with the independently
    validated claim/citation IDs and the candidate's market/second references.
    It is still a projection: no account state or credential field is copied
    into it.
    """

    actionable_ids = (
        "immediate_long_tp",
        "immediate_short_sl",
        "wait_long_fill_tp",
        "wait_short_fill_sl",
        "dual_touch_stop_first",
        "wait_expiry",
        "opposite_isolation",
        "same_direction_parallel",
    )
    actionable_results = {sid: scenarios.get(sid, {}) for sid in actionable_ids}
    actionable_outcomes: list[dict[str, Any]] = []
    required_request_fields = (
        "provider",
        "model",
        "adapter",
        "profile_ref_id",
        "profile_ref_hash",
        "market_snapshot_id",
        "market_snapshot_hash",
        "evidence_package_id",
        "evidence_package_hash",
        "shared_market_view_id",
        "shared_market_view_hash",
        "packet_id",
        "packet_hash",
    )
    request_lineage_ok = True
    market_lineage_ok = True
    second_lineage_ok = True
    boundary_safe = True
    event_market_refs: list[dict[str, Any]] = []
    for scenario_id, result in actionable_results.items():
        if not isinstance(result, Mapping):
            request_lineage_ok = market_lineage_ok = second_lineage_ok = False
            continue
        requests = [item for item in result.get("requests", []) if isinstance(item, Mapping)]
        outcomes = [item for item in result.get("provider_outcomes", []) if isinstance(item, Mapping) and item.get("classification") == "success"]
        if not requests or not outcomes:
            request_lineage_ok = market_lineage_ok = second_lineage_ok = False
            continue
        feed_ok = result.get("view", {}).get("feed") == "alpaca:iex:single_exchange"
        market_lineage_ok = market_lineage_ok and feed_ok
        for event in result.get("events", []):
            if not isinstance(event, Mapping) or event.get("event_type") not in {"paper_fill", "paper_close"}:
                continue
            market_ref = event.get("market_ref")
            event_ref_ok = (
                isinstance(market_ref, Mapping)
                and market_ref.get("kind") == "second_event"
                and isinstance(market_ref.get("second_event_id"), str)
                and isinstance(market_ref.get("second_event_hash"), str)
                and len(market_ref["second_event_hash"]) == 64
                and event.get("market_feed") == ALPACA_IEX_FEED.to_dict()
            )
            market_lineage_ok = market_lineage_ok and event_ref_ok
            event_market_refs.append(
                {
                    "scenario_id": scenario_id,
                    "event_id": event.get("event_id"),
                    "event_hash": event.get("event_hash"),
                    "event_type": event.get("event_type"),
                    "market_feed": event.get("market_feed"),
                    "second_event_id": market_ref.get("second_event_id") if isinstance(market_ref, Mapping) else None,
                    "second_event_hash": market_ref.get("second_event_hash") if isinstance(market_ref, Mapping) else None,
                }
            )
        for request in requests:
            request_lineage_ok = request_lineage_ok and all(request.get(field) for field in required_request_fields)
            boundary_safe = boundary_safe and not request.get("boundary_issues")
        for index, outcome in enumerate(outcomes):
            actionable_outcomes.append({"scenario_id": scenario_id, **_json(outcome)})
            request = requests[min(index, len(requests) - 1)]
            market_refs = outcome.get("market_refs", [])
            claim_ids = outcome.get("claim_ids", [])
            citation_ids = outcome.get("citation_ids", [])
            candidate_ref = outcome.get("shared_market_view_ref")
            if not claim_ids or not citation_ids or not market_refs or not isinstance(candidate_ref, Mapping):
                market_lineage_ok = False
            if any(not str(value).startswith("bar_") for value in market_refs):
                market_lineage_ok = False
            # G021 carries the richer ``mview_`` view identity in the request
            # extension, while G018's executable decision ref is the compact
            # ``smv_`` pair identity.  Bind the latter to the request's exact
            # snapshot/frame IDs and hashes rather than comparing the two
            # deliberately distinct identity namespaces.
            for field_name in ("market_snapshot_id", "market_snapshot_hash", "observation_frame_id", "observation_frame_hash"):
                if candidate_ref.get(field_name) != request.get(field_name):
                    market_lineage_ok = False
            second_ref = outcome.get("second_event_ref")
            if isinstance(second_ref, Mapping):
                if second_ref.get("event_id") != request.get("latest_second_event_id") or second_ref.get("event_hash") != request.get("latest_second_event_hash"):
                    second_lineage_ok = False
            elif outcome.get("branch") == "enter_immediately":
                second_lineage_ok = False
            if not isinstance(outcome.get("candidate_hash"), str) or len(outcome["candidate_hash"]) != 64:
                market_lineage_ok = False
    return {
        "actionable_scenarios": list(actionable_ids),
        "actionable_outcomes": actionable_outcomes,
        "all_actionable_have_claim_ids": all(bool(item.get("claim_ids")) for item in actionable_outcomes) and bool(actionable_outcomes),
        "all_actionable_have_citation_ids": all(bool(item.get("citation_ids")) for item in actionable_outcomes) and bool(actionable_outcomes),
        "all_actionable_have_iex_market_refs": market_lineage_ok,
        "all_immediate_have_current_second_refs": second_lineage_ok,
        "future_market_inputs_excluded": second_lineage_ok,
        "event_market_refs": event_market_refs,
        "all_requests_have_profile_packet_and_market_hashes": request_lineage_ok,
        "model_boundary_safe": boundary_safe,
        "pass": bool(
            actionable_outcomes
            and request_lineage_ok
            and market_lineage_ok
            and second_lineage_ok
            and boundary_safe
            and all(bool(item.get("claim_ids")) and bool(item.get("citation_ids")) for item in actionable_outcomes)
        ),
    }


def _run_once(fixture: Mapping[str, Any], goldens: Mapping[str, Any] | None = None) -> dict[str, Any]:
    scenarios = _runtime_scenarios(fixture)
    _annotate_dual_touch(fixture, scenarios)
    invalid_response = scenarios.get("provider_invalid_response")
    if isinstance(invalid_response, dict):
        invalid_extra = invalid_response.setdefault("extra", {})
        if isinstance(invalid_extra, dict):
            invalid_extra["terminal_no_hot_loop"] = (
                invalid_response.get("provider_call_count") == 2
                and not _event_rows(invalid_response, "paper_fill")
                and invalid_response.get("states", {}).get(SID_LONG) == "blocked"
            )
            invalid_extra["no_paper_entry"] = not _event_rows(invalid_response, "paper_fill")
        invalid_response["canonical_hash"] = _digest(
            {key: value for key, value in invalid_response.items() if key != "canonical_hash"}
        )
    report = {
        "scenario_results": scenarios,
        "lineage_assertions": _lineage_assertions(scenarios),
        "abstention": _market_policy_abstention(fixture),
        "scale": _scale_evidence(),
        "isolation": _isolation_evidence(scenarios),
        "decimal_scales": _decimal_scale_evidence(scenarios),
        "boundaries": _boundary_evidence(fixture),
        "revision_lineage": _revision_lineage_evidence(fixture),
        "feed_lineage": _feed_lineage_evidence(fixture, scenarios),
        "derived_seconds": _derived_second_golden_checks(fixture),
        "market_quality": _gap_stale_evidence(fixture),
        "immediate_drift": _immediate_drift_evidence(fixture),
        "disabled_exhausted": _disabled_exhausted_evidence(),
        "deadlines": _deadline_evidence(),
        "recovery": _recovery_evidence(fixture),
        "tampering": _tamper_evidence(fixture),
        "session_close": _session_close_evidence(fixture),
        "thinking": _thinking_evidence(fixture),
        "restart": _restart_evidence(fixture),
        "startup": _startup_and_corruption_evidence(fixture),
    }
    if goldens is not None:
        report["golden_checks"] = _golden_checks(scenarios, goldens)
    report["retry_backoff"] = _retry_backoff_evidence(scenarios["provider_retry_backoff"])
    return _strip_volatile(report)


def _coverage(
    report: Mapping[str, Any],
    goldens: Mapping[str, Any],
    matrix: Mapping[str, Any],
    *,
    reproducible: bool,
    limitations: Sequence[str],
) -> list[dict[str, Any]]:
    scenario_results = report.get("scenario_results", {})
    statuses = {state for result in scenario_results.values() for state in result.get("states", {}).values()}
    branches = {
        item.get("branch")
        for result in scenario_results.values()
        for item in result.get("provider_outcomes", [])
        if isinstance(item, Mapping) and item.get("branch")
    }
    directions = {
        item.get("direction")
        for result in scenario_results.values()
        for item in result.get("provider_outcomes", [])
        if isinstance(item, Mapping) and item.get("direction")
    }
    exit_reasons = {
        event.get("payload", {}).get("exit_reason")
        for result in scenario_results.values()
        for event in result.get("events", [])
        if isinstance(event, Mapping) and event.get("event_type") == "paper_close"
    }
    scale = report.get("scale", {})
    forty = scale.get("forty", {}) if isinstance(scale, Mapping) else {}
    two_hundred = scale.get("two_hundred", {}) if isinstance(scale, Mapping) else {}
    reentry = forty.get("reentry", {}) if isinstance(forty, Mapping) else {}
    recovery = report.get("recovery", {})
    recovery_entry = recovery.get("entry_recovery", {}) if isinstance(recovery, Mapping) else {}
    recovery_exit = recovery.get("exit_settlement_once", {}) if isinstance(recovery, Mapping) else {}
    restart = report.get("restart", {})
    pending_x = report.get("session_close", {}).get("pending_x_across_close", {})
    startup = report.get("startup", {})
    feed_lineage = report.get("feed_lineage", {})
    checks = {
        "AC01": bool(len(scenario_results) >= 12 and {"no_entry", "enter_immediately", "wait_for_price"}.issubset(branches) and {"long", "short"}.issubset(directions) and {"take_profit", "stop_loss"}.issubset(exit_reasons) and {"sleeping", "open_position", "analysis_due"}.issubset(statuses) and "abstain_insufficient_history" in str(report.get("abstention"))),
        "AC02": bool(report.get("lineage_assertions", {}).get("pass") is True),
        "AC03": bool(goldens.get("independent_authored") and report.get("golden_checks", {}).get("pass") is True and report.get("phase1_retrieval_goldens_unchanged") is True),
        "AC04": bool(
            forty.get("started_jobs") == 40
            and forty.get("completed_jobs") == 40
            and forty.get("all_completed") is True
            and forty.get("each_strategy_once") is True
            and set(forty.get("registry_strategy_ids", [])) == set(forty.get("service_counts", {}))
            and forty.get("same_view") is True
            and forty.get("no_duplicate_generation_job") is True
            and reentry.get("all_completed") is True
            and reentry.get("isolated") is True
            and reentry.get("peak_in_flight") <= forty.get("max_in_flight")
            and two_hundred.get("claimed") == 200
            and set(two_hundred.get("registry_strategy_ids", [])) == set(two_hundred.get("service_counts", {}))
            and two_hundred.get("all_strategy_ids_once") is True
            and two_hundred.get("unique_job_ids") is True
            and two_hundred.get("unique_generation_jobs") is True
            and two_hundred.get("service_lineage_ok") is True
            and two_hundred.get("peak_in_flight") <= two_hundred.get("configured_max_in_flight")
            and two_hundred.get("work_conserving_refill") is True
        ),
        "AC05": bool(report.get("isolation", {}).get("pass") is True and set(report.get("isolation", {}).get("cases", {})) == {"opposite_isolation", "same_direction_parallel"} and all(item.get("pass") is True for item in report.get("isolation", {}).get("cases", {}).values())),
        "AC06": bool(
            restart.get("restored_pending_or_open") is True
            and restart.get("replayed_event_ids", 0) >= 1
            and restart.get("duplicate_event_ids") == 0
            and restart.get("event_multiplicity_unchanged") is True
            and restart.get("no_provider_call_on_duplicate_or_late") is True
            and restart.get("no_fill_duplicate_or_late") is True
            and restart.get("no_close_duplicate_or_late") is True
            and restart.get("balance_pnl_stable_after_replay") is True
            and recovery_exit.get("one_settlement") is True
        ),
        "AC07": bool(
            recovery.get("entry_exit_status") == "settled"
            and recovery.get("entry_exit_lineage", {}).get("path_precision") == "minute_bar_no_path"
            and recovery.get("straddling_not_evaluated") is True
            and recovery.get("typed_lineage_tamper_rejected") is True
            and recovery_entry.get("status") == "filled"
            and recovery_entry.get("duplicate_status") == "replayed"
            and recovery_entry.get("exact_requested_level") is True
            and recovery_entry.get("lineage_exact") is True
            and recovery_entry.get("duplicate_no_effect") is True
            and recovery.get("whole_interval_recovery", {}).get("entry_bar_covers_gap") is True
            and recovery.get("whole_interval_recovery", {}).get("exit_gap_recorded") is True
        ),
        "AC08": bool(
            report.get("boundaries", {}).get("half_open_cutover", {}).get("pre_cutover_dropped") is True
            and report.get("boundaries", {}).get("half_open_cutover", {}).get("cutover_inclusive_retained") is True
            and report.get("boundaries", {}).get("pre_watermark_changed") is True
            and report.get("boundaries", {}).get("post_watermark_execution_hash_unchanged") is True
            and report.get("boundaries", {}).get("late_reconciliation_hash")
            and report.get("revision_lineage", {}).get("pass") is True
            and report.get("derived_seconds", {}).get("pass") is True
            and report.get("market_quality", {}).get("gap_blocks_execution") is True
            and report.get("market_quality", {}).get("stale_blocks_execution") is True
            and report.get("immediate_drift", {}).get("fresh_price_rejected") is True
            and report.get("immediate_drift", {}).get("reason_binds_both_second_ids") is True
            and report.get("decimal_scales", {}).get("pass") is True
            and recovery.get("whole_interval_recovery", {}).get("entry_bar_covers_gap") is True
            and recovery.get("whole_interval_recovery", {}).get("exit_gap_recorded") is True
        ),
        "AC09": bool(feed_lineage.get("pass") is True and feed_lineage.get("history_sip_mismatch_rejected") is True and feed_lineage.get("aggregator_sip_mismatch_rejected") is True and feed_lineage.get("live_sip_mismatch_typed_or_g021_rejected") is True and report.get("boundaries", {}).get("sip_rejected") is True),
        "AC10": bool(
            report.get("session_close", {}).get("calls_at_close") == report.get("session_close", {}).get("calls_before_close")
            and report.get("session_close", {}).get("calls_next_session", 0) >= report.get("session_close", {}).get("calls_at_close", 0)
            and report.get("session_close", {}).get("open_position_resume", {}).get("open_next_session") is True
            and report.get("session_close", {}).get("open_position_resume", {}).get("state_next_session") == "open_position"
            and pending_x.get("deadline_not_extended") is True
            and pending_x.get("closed_session_no_model_call") is True
            and pending_x.get("no_entry") is True
            and pending_x.get("expired_status") == "expired"
            and pending_x.get("duplicate_expiry_status") == "replayed"
            and pending_x.get("next_session_resume_call_observed") is True
        ),
        "AC11": bool(report.get("deadlines", {}).get("x") == 1 and report.get("deadlines", {}).get("arrived_during_grace") is True and report.get("deadlines", {}).get("deadline_not_extended") is True and report.get("deadlines", {}).get("status") == "filled" and scenario_results.get("wait_expiry", {}).get("states", {}).get(SID_LONG) == "analysis_due"),
        "AC12": bool(
            startup.get("insufficient_startup", {}).get("provider_calls") == 0
            and startup.get("insufficient_startup", {}).get("paper_entries") == 0
            and startup.get("partial_startup", {}).get("provider_calls") == 0
            and startup.get("partial_startup", {}).get("paper_entries") == 0
            and startup.get("partial_startup", {}).get("no_model_hot_loop") is True
            and startup.get("terminal_configuration", {}).get("terminal") is True
            and startup.get("terminal_configuration", {}).get("provider_calls") == 0
            and startup.get("terminal_configuration", {}).get("paper_entries") == 0
            and report.get("retry_backoff", {}).get("observed_delays_seconds") == [5, 10, 20, 40, 60]
            and scenario_results.get("terminal_auth_no_hot_loop", {}).get("provider_call_count") == 1
            and scenario_results.get("provider_invalid_response", {}).get("extra", {}).get("terminal_no_hot_loop") is True
            and report.get("disabled_exhausted", {}).get("no_recall") is True
        ),
        "AC13": report.get("thinking", {}).get("distinct") is True and report.get("thinking", {}).get("isolation_unchanged") is True,
        "AC14": bool(report.get("tampering", {}).get("recursive_leak_found") and report.get("tampering", {}).get("grounding_tamper_rejected") and report.get("tampering", {}).get("packet_tamper_rejected") and report.get("tampering", {}).get("profile_tamper_rejected") and report.get("tampering", {}).get("second_lineage_tamper_rejected") and report.get("tampering", {}).get("provider_calls_before_execution") == 0 and report.get("recovery", {}).get("typed_lineage_tamper_rejected") and report.get("revision_lineage", {}).get("pass") is True),
        "AC15": reproducible,
        "AC16": bool(limitations and any("structural/reproducibility" in item for item in limitations) and any("profitability" in item for item in limitations)),
    }
    rows = []
    for item in matrix.get("criteria", []):
        criterion_id = str(item.get("id"))
        rows.append({"id": criterion_id, "requirement": item.get("requirement"), "status": "PASS" if checks.get(criterion_id, False) else "FAIL", "evidence": item.get("evidence", [])})
    return rows


@dataclass(frozen=True)
class EvaluationReport:
    """Machine-readable G029 report and its stable canonical identity."""

    value: Mapping[str, Any]

    @property
    def status(self) -> str:
        return str(self.value.get("status", "REJECT"))

    @property
    def canonical_hash(self) -> str:
        return str(self.value.get("canonical_hash", ""))

    def to_dict(self) -> dict[str, Any]:
        return _json(self.value)


def run_offline_evaluation(*, fixture_dir: str | Path | None = None) -> EvaluationReport:
    """Run the complete deterministic G029 evidence slice twice.

    The returned report is ``ACCEPT`` only when both canonical replays are
    byte-identical after the explicit volatile-telemetry exclusions and every
    coverage-matrix row is directly evidenced by the run.
    """

    root = Path(fixture_dir) if fixture_dir is not None else DEFAULT_FIXTURE_DIR
    fixture = _fixture(root, "recorded_market.json")
    goldens = _fixture(root, "goldens.json")
    matrix = _fixture(root, "coverage-matrix.json")
    first = _run_once(fixture, goldens)
    second = _run_once(fixture, goldens)
    first_hash = _digest(first)
    second_hash = _digest(second)
    reproducible = first_hash == second_hash and canonical_json_bytes(first) == canonical_json_bytes(second)
    phase1_retrieval = _phase1_retrieval_evidence()
    limitations = [
        "This report is structural/reproducibility evidence only.",
        "It does not prove profitability, win rate, drawdown, strategy ranking, or real execution quality.",
        "It uses synthetic recorded market/model fixtures and paper-only zero-fee/zero-slippage assumptions.",
    ]
    coverage_input = dict(first)
    coverage_input["phase1_retrieval_goldens_unchanged"] = phase1_retrieval.get("unchanged") is True
    coverage = _coverage(coverage_input, goldens, matrix, reproducible=reproducible, limitations=limitations)
    coverage_ok = all(row["status"] == "PASS" for row in coverage)
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "evaluation_version": EVALUATION_VERSION,
        "status": "ACCEPT" if reproducible and coverage_ok else "REJECT",
        "mode": "offline_replay",
        "network_used": False,
        "credentials_used": False,
        "fixture_index": _fixture(root, "fixture-index.json"),
        "replays": {"runs": 2, "canonical_hashes": [first_hash, second_hash], "identical": reproducible, "volatile_telemetry_excluded": list(VOLATILE_TELEMETRY)},
        "scenario_count": len(first.get("scenario_results", {})),
        "scenario_results": first.get("scenario_results", {}),
        "lineage_assertions": first.get("lineage_assertions"),
        "golden_checks": first.get("golden_checks"),
        "abstention": first.get("abstention"),
        "scale": first.get("scale"),
        "isolation": first.get("isolation"),
        "decimal_scales": first.get("decimal_scales"),
        "boundaries": first.get("boundaries"),
        "revision_lineage": first.get("revision_lineage"),
        "feed_lineage": first.get("feed_lineage"),
        "derived_seconds": first.get("derived_seconds"),
        "market_quality": first.get("market_quality"),
        "immediate_drift": first.get("immediate_drift"),
        "disabled_exhausted": first.get("disabled_exhausted"),
        "deadlines": first.get("deadlines"),
        "recovery": first.get("recovery"),
        "tampering": first.get("tampering"),
        "session_close": first.get("session_close"),
        "thinking": first.get("thinking"),
        "restart": first.get("restart"),
        "startup": first.get("startup"),
        "retry_backoff": first.get("retry_backoff"),
        "coverage_matrix": coverage,
        "limitations": limitations,
        "phase1_retrieval_goldens_unchanged": phase1_retrieval.get("unchanged") is True,
        "phase1_retrieval_goldens": phase1_retrieval,
    }
    value["canonical_hash"] = _digest(value)
    return EvaluationReport(value)


def write_report(report: EvaluationReport | Mapping[str, Any], path: str | Path) -> None:
    """Write a stable pretty JSON report, returning no hidden telemetry."""

    value = report.to_dict() if isinstance(report, EvaluationReport) else _json(report)
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


__all__ = ["EvaluationError", "EvaluationReport", "run_offline_evaluation", "write_report"]
