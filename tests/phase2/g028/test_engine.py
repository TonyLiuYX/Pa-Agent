"""Deterministic automatic-runtime tests.

The suite uses fake clocks, scripted adapters, canned history/recovery
results, and frozen seconds through a complete feed session. The full
end-to-end paper-trading path lives in ``test_vertical_slice``.
"""

from __future__ import annotations

import ast
import copy
from dataclasses import replace
import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from pa_engine.market import ALPACA_IEX_FEED
from pa_engine.runtime.boundary import scan_model_boundary
from pa_engine.runtime.config import RuntimeConfig
from pa_engine.runtime.engine import AutomaticPaperRuntime, RuntimeEngineError
from pa_engine.storage import open_database
from pa_engine.strategy.decision import make_second_event_ref
from pa_engine.strategy.runtime import FakeClock, StrategyState
from pa_engine.strategies.knowledge import build_registry

ROOT = Path(__file__).resolve().parents[3]
FIXTURE_ROOT = ROOT / "tests" / "fixtures" / "phase2" / "g028" / "engine"

SID_A = "pa:nanqiao-range-extreme-limit"
SID_B = "pa:nanqiao-always-in-ema20"
SID_C = "pa:nanqiao-always-in-breakout-scalp"

UTC = timezone.utc


def _builders() -> Any:
    spec = importlib.util.spec_from_file_location("g028_engine_builders", FIXTURE_ROOT / "builders.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


B = _builders()


def two_strategy_registry() -> Any:
    return build_registry(
        str(ROOT / "knowledge"),
        registrations=[
            {"strategy_id": SID_A, "record_id": "nanqiao-range-extreme-limit"},
            {"strategy_id": SID_B, "record_id": "nanqiao-always-in-ema20"},
        ],
    )


def three_strategy_registry() -> Any:
    return build_registry(
        str(ROOT / "knowledge"),
        registrations=[
            {"strategy_id": SID_A, "record_id": "nanqiao-range-extreme-limit"},
            {"strategy_id": SID_B, "record_id": "nanqiao-always-in-ema20"},
            {"strategy_id": SID_C, "record_id": "nanqiao-always-in-breakout-scalp"},
        ],
    )


def make_engine(
    tmp_path: Path,
    *,
    registry: Any = None,
    history: Any = None,
    aggregator: Any = None,
    clock: FakeClock | None = None,
    run_id: str = "run_g028_engine",
    db_name: str = "engine.db",
    config_overrides: dict[str, Any] | None = None,
) -> AutomaticPaperRuntime:
    kwargs = dict(config_overrides or {})
    kwargs.setdefault("symbol", "AAPL")
    kwargs.setdefault("timeframe", "1min")
    kwargs.setdefault("database_path", str(tmp_path / db_name))
    config = RuntimeConfig.from_kwargs(**kwargs)
    engine = AutomaticPaperRuntime(
        config,
        registry=registry if registry is not None else two_strategy_registry(),
        history_adapter=B.FakeHistoryAdapter(history_result=history or B.history_result()),
        aggregator=aggregator if aggregator is not None else B.FakeAggregator(),
        db_path=str(tmp_path / db_name),
        run_id=run_id,
        clock=clock if clock is not None else FakeClock("2026-03-09T13:34:03Z"),
    )
    return engine


def sec(start: str, end: str, close: str, high: str | None = None, low: str | None = None, quality: dict[str, Any] | None = None) -> Any:
    return B.frozen_second(start_at=start, end_at=end, close=close, high=high, low=low, quality=quality)


def outcome_for(registry: Any, request: dict[str, Any], sid: str, view: Any, branch: str, *, direction: str = "long", generation: int | None = None, **kwargs: Any) -> Any:
    loaded = registry.load_packet(sid)
    cand = B.grounded_candidate(registry, sid, view, branch=branch, direction=direction, **kwargs)
    return B.success_outcome(request, cand, profile_ref=loaded.profile_ref, generation=generation)


def wait_outcome(engine: AutomaticPaperRuntime, sid: str, *, direction: str = "long", entry: str = "10", stop: str = "9.9", target: str = "10.3", x: int = 3600) -> Any:
    registry = engine._registry

    def build(request: dict[str, Any], generation: int | None = None) -> Any:
        return outcome_for(registry, request, sid, engine._current_view, "wait_for_price", direction=direction, entry=entry, stop=stop, target=target, x=x, generation=generation)

    return build


def immediate_outcome(engine: AutomaticPaperRuntime, sid: str, *, direction: str = "short", stop: str = "10.3", target: str = "9.8") -> Any:
    registry = engine._registry

    def build(request: dict[str, Any], generation: int | None = None) -> Any:
        view = engine._current_view
        return outcome_for(registry, request, sid, view, "enter_immediately", direction=direction, stop=stop, target=target, second_event_ref=B.second_ref_of(view), generation=generation)

    return build


def no_entry_outcome(engine: AutomaticPaperRuntime, sid: str, *, n: int = 60) -> Any:
    registry = engine._registry

    def build(request: dict[str, Any], generation: int | None = None) -> Any:
        return outcome_for(registry, request, sid, engine._current_view, "no_entry", n=n, generation=generation)

    return build


def error_script(classification: str) -> Any:
    def build(request: dict[str, Any], generation: int | None = None) -> Any:
        return B.error_outcome(request, classification, generation=generation)

    return build


def attach_script(engine: AutomaticPaperRuntime, script: dict[str, list[Any]]) -> Any:
    adapter = B.ScriptedAdapter(script)
    engine._adapter = adapter
    return adapter


def events_of(engine: AutomaticPaperRuntime, event_type: str) -> list[dict[str, Any]]:
    return engine._store.list_events(event_type=event_type)


def calls_for(adapter: Any, sid: str) -> int:
    return sum(
        1
        for request in adapter.requests
        if request.get("extensions", {}).get("strategy_profile_ref", {}).get("strategy_id") == sid
    )


def balances(engine: AutomaticPaperRuntime) -> dict[str, str]:
    return {sid: str(engine.book.get_account(sid).balance) for sid in (SID_A, SID_B)}


def run_single_step(engine: AutomaticPaperRuntime, at: str) -> dict[str, Any]:
    return engine.step(now=at)


# ---------------------------------------------------------------------------
# AC1 — one shared initial view, independently grounded requests
# ---------------------------------------------------------------------------


class TestAC1SharedViewIndependentRequests:
    def test_ac01_same_initial_view_and_independent_requests(self, tmp_path):
        clock = FakeClock("2026-03-09T13:34:03Z")
        s1 = sec("2026-03-09T13:33:59Z", "2026-03-09T13:34:00Z", "10.05")
        engine = make_engine(tmp_path, clock=clock, aggregator=B.FakeAggregator(batches=[[s1]]))
        adapter = attach_script(engine, {SID_A: [no_entry_outcome(engine, SID_A)], SID_B: [no_entry_outcome(engine, SID_B)]})
        engine.start()
        run_single_step(engine, "2026-03-09T13:34:03Z")
        assert adapter.call_count == 2
        view = engine._current_view
        view_ref = view.to_decision_ref()
        requests = adapter.requests
        assert len(requests) == 2
        view_ids = {request["extensions"]["context_refs"]["shared_market_view_id"] for request in requests}
        assert len(view_ids) == 1
        assert view_ids.pop() == view.view_id
        for request in requests:
            assert request["extensions"]["context_refs"]["shared_market_view_hash"] == view.view_hash
            assert request["extensions"]["strategy_profile_ref"]["profile_ref_id"].startswith("spr_")
            assert request["extensions"]["no_live_orders"] is True
            assert request["context_refs"]["market_snapshot_id"] == view_ref["market_snapshot_id"]
            assert request["context_refs"]["observation_frame_id"] == view_ref["observation_frame_id"]
        profiles = {request["extensions"]["strategy_profile_ref"]["profile_ref_id"] for request in requests}
        assert len(profiles) == 2
        packets = {request["extensions"]["context_refs"]["packet_id"] for request in requests}
        assert len(packets) == 2
        assert requests[0]["extensions"]["strategy_profile_ref"]["strategy_id"] != requests[1]["extensions"]["strategy_profile_ref"]["strategy_id"]
        engine.stop()


# ---------------------------------------------------------------------------
# AC3 — opposite positions, independent balances, no netting
# ---------------------------------------------------------------------------


class TestAC3OppositePositions:
    def test_ac03_opposite_positions_independent_balances_no_netting(self, tmp_path):
        clock = FakeClock("2026-03-09T13:34:03Z")
        s1 = sec("2026-03-09T13:33:59Z", "2026-03-09T13:34:00Z", "10.05")
        engine = make_engine(tmp_path, clock=clock, aggregator=B.FakeAggregator(batches=[[s1]]))
        attach_script(
            engine,
            {
                SID_A: [immediate_outcome(engine, SID_A, direction="long", stop="9.9", target="10.3")],
                SID_B: [immediate_outcome(engine, SID_B, stop="10.3", target="9.8")],
            },
        )
        engine.start()
        run_single_step(engine, "2026-03-09T13:34:03Z")
        trade_a = engine.book.get_account(SID_A).open_trade
        trade_b = engine.book.get_account(SID_B).open_trade
        assert trade_a is not None and trade_a.direction == "long"
        assert trade_b is not None and trade_b.direction == "short"
        assert trade_a.symbol == trade_b.symbol == "AAPL"
        assert balances(engine) == {SID_A: "1000.00", SID_B: "1000.00"}

        s2 = sec("2026-03-09T13:34:05Z", "2026-03-09T13:34:06Z", "9.9", high="10.05", low="9.85")
        engine._aggregator._batches.append([s2])
        run_single_step(engine, "2026-03-09T13:34:09Z")
        assert engine.scheduler.get(SID_A).state == StrategyState.ANALYSIS_DUE
        assert engine.scheduler.get(SID_B).state == StrategyState.OPEN_POSITION
        assert engine.book.get_account(SID_A).balance < 1000
        assert engine.book.get_account(SID_B).balance == 1000
        assert engine.book.get_account(SID_B).open_trade is not None
        engine.stop()


# ---------------------------------------------------------------------------
# AC4 — zero model calls while waiting or open
# ---------------------------------------------------------------------------


class TestAC4ZeroModelCalls:
    def test_ac04_zero_model_calls_while_waiting_or_open(self, tmp_path):
        clock = FakeClock("2026-03-09T13:34:03Z")
        s1 = sec("2026-03-09T13:33:59Z", "2026-03-09T13:34:00Z", "10.05")
        engine = make_engine(tmp_path, clock=clock, aggregator=B.FakeAggregator(batches=[[s1]]))
        adapter = attach_script(
            engine,
            {
                SID_A: [wait_outcome(engine, SID_A, x=3600)],
                SID_B: [immediate_outcome(engine, SID_B)],
            },
        )
        engine.start()
        run_single_step(engine, "2026-03-09T13:34:03Z")
        assert adapter.call_count == 2
        assert engine.scheduler.get(SID_A).state == StrategyState.WAITING_ENTRY
        assert engine.scheduler.get(SID_B).state == StrategyState.OPEN_POSITION

        for index in range(3):
            start = f"2026-03-09T13:34:{10 + index * 2:02d}Z"
            end = f"2026-03-09T13:34:{11 + index * 2:02d}Z"
            engine._aggregator._batches.append([sec(start, end, "10.2", high="10.25", low="10.15")])
            run_single_step(engine, f"2026-03-09T13:34:{14 + index * 2:02d}Z")
        assert adapter.call_count == 2

        engine._aggregator._batches.append([sec("2026-03-09T13:34:16Z", "2026-03-09T13:34:17Z", "9.8", high="10.05", low="9.75")])
        run_single_step(engine, "2026-03-09T13:34:20Z")
        assert adapter.call_count == 2  # close happened; recall runs next step
        engine._aggregator._batches.append([sec("2026-03-09T13:34:21Z", "2026-03-09T13:34:22Z", "10.05")])
        run_single_step(engine, "2026-03-09T13:34:25Z")
        assert adapter.call_count == 3  # exactly one recall call
        engine.stop()

    def test_ac04_terminal_fill_persistence_retries_after_transient_failure(self, tmp_path):
        clock = FakeClock("2026-03-09T13:34:03Z")
        s1 = sec("2026-03-09T13:33:59Z", "2026-03-09T13:34:00Z", "10.05")
        engine = make_engine(tmp_path, clock=clock, aggregator=B.FakeAggregator(batches=[[s1]]))
        attach_script(
            engine,
            {
                SID_A: [immediate_outcome(engine, SID_A, direction="long", stop="9.9", target="10.3")],
                SID_B: [no_entry_outcome(engine, SID_B)],
            },
        )
        engine.start()
        run_single_step(engine, "2026-03-09T13:34:03Z")
        original = engine._sink.commit_close
        calls = {"count": 0}

        def flaky_commit_close(*args: Any, **kwargs: Any) -> Any:
            calls["count"] += 1
            if calls["count"] == 1:
                raise RuntimeError("transient close write failure")
            return original(*args, **kwargs)

        engine._sink.commit_close = flaky_commit_close
        engine._aggregator._batches.append(
            [sec("2026-03-09T13:34:05Z", "2026-03-09T13:34:06Z", "10.3", high="10.35", low="10.2")]
        )
        run_single_step(engine, "2026-03-09T13:34:09Z")
        assert calls["count"] == 2
        assert len(events_of(engine, "paper_close")) == 1
        assert engine._exit_monitors.get(SID_A) is None
        assert engine._closes == 1
        engine.stop()


# ---------------------------------------------------------------------------
# AC5 — restart preserves balances, pending/open trades and schedules
# ---------------------------------------------------------------------------


class TestAC5RestartPreservesState:
    def test_ac05_restart_preserves_balances_pending_and_no_duplicates(self, tmp_path):
        clock1 = FakeClock("2026-03-09T13:34:03Z")
        s1 = sec("2026-03-09T13:33:59Z", "2026-03-09T13:34:00Z", "10.05")
        engine1 = make_engine(tmp_path, clock=clock1, aggregator=B.FakeAggregator(batches=[[s1]]), run_id="run_ac05_a")
        attach_script(engine1, {SID_A: [wait_outcome(engine1, SID_A, x=3600)], SID_B: [immediate_outcome(engine1, SID_B)]})
        engine1.start()
        run_single_step(engine1, "2026-03-09T13:34:03Z")
        pending_id = engine1._pending_monitors[SID_A].pending_id
        trade_id_b = engine1.book.get_account(SID_B).open_trade.trade_id
        engine1.stop()

        clock2 = FakeClock("2026-03-09T13:35:00Z")
        engine2 = make_engine(tmp_path, clock=clock2, run_id="run_ac05_b")
        engine2._history_adapter.recovery_result = B.recovery_result(
            start_at="2026-03-09T13:34:00Z",
            end_at="2026-03-09T13:35:00Z",
            bars=[B.recovery_bar(start_at="2026-03-09T13:34:00Z", end_at="2026-03-09T13:35:00Z", low="10.1", high="10.2")],
        )
        report = engine2.start()
        assert report["errors"] == []
        assert report["restored_pending"] == 1
        assert report["restored_open_trades"] == 1
        assert report["restored_accounts"] == 2
        assert engine2._pending_monitors[SID_A].pending_id == pending_id
        assert engine2.book.get_account(SID_B).open_trade.trade_id == trade_id_b
        assert engine2.book.get_account(SID_B).balance == 1000
        assert len(events_of(engine2, "model_decision_accepted")) == 2  # no duplicate decisions
        assert engine2._sink.is_live_blocked("AAPL") is False
        assert len(events_of(engine2, "paper_close")) == 0
        fills_after_start = len(events_of(engine2, "paper_fill"))
        assert fills_after_start == 1  # B's phase-1 immediate fill only

        s2 = sec("2026-03-09T13:36:05Z", "2026-03-09T13:36:06Z", "10", high="10.05", low="9.95")
        engine2._aggregator._batches.append([s2])
        run_single_step(engine2, "2026-03-09T13:36:09Z")
        fills = events_of(engine2, "paper_fill")
        assert len(fills) == fills_after_start + 1  # exactly one restored fill
        assert engine2._store.get_pending_entry(pending_id)["status"] == "filled"
        assert len(events_of(engine2, "model_decision_accepted")) == 2
        assert engine2.book.get_account(SID_B).open_trade is not None
        engine2.stop()


# ---------------------------------------------------------------------------
# AC6 — restart recovery starts from the persisted cursor
# ---------------------------------------------------------------------------


class TestAC6RestartRecovery:
    def test_ac06_recovery_from_cursor_no_live_processing_until_reconciled(self, tmp_path):
        clock1 = FakeClock("2026-03-09T13:34:03Z")
        s1 = sec("2026-03-09T13:33:59Z", "2026-03-09T13:34:00Z", "10.05")
        engine1 = make_engine(tmp_path, clock=clock1, aggregator=B.FakeAggregator(batches=[[s1]]), run_id="run_ac06_a")
        attach_script(engine1, {SID_A: [wait_outcome(engine1, SID_A, x=3600)]})
        engine1.start()
        run_single_step(engine1, "2026-03-09T13:34:03Z")
        assert engine1.scheduler.get(SID_A).state == StrategyState.WAITING_ENTRY
        engine1.stop()

        # Phase 2: recovery is unavailable at first; the runtime must stay
        # blocked and process no live events until bars arrive.
        clock2 = FakeClock("2026-03-09T13:35:03Z")
        engine2 = make_engine(tmp_path, clock=clock2, run_id="run_ac06_b")
        engine2._history_adapter.recovery_error = RuntimeError("recovery unavailable")
        s_pre = sec("2026-03-09T13:34:02Z", "2026-03-09T13:34:03Z", "10.05")
        engine2._aggregator._batches.append([s_pre])
        report = engine2.start()
        assert report["live_blocked"] is True
        assert any(error.get("code") == "recovery_required_but_unavailable" for error in report["errors"])
        assert engine2._recovery_pending is True

        blocked = run_single_step(engine2, "2026-03-09T13:35:03Z")
        assert blocked["ready_jobs"] == 0
        assert blocked["provider_calls"] == 0
        assert blocked["fills"] == 0 and blocked["closes"] == 0
        assert engine2._sink.get_cursor("AAPL")["last_second_end_at"] == "2026-03-09T13:34:00Z"
        assert engine2._sink.is_live_blocked("AAPL") is True

        engine2._history_adapter.recovery_error = None
        engine2._history_adapter.recovery_result = B.recovery_result(
            start_at="2026-03-09T13:35:00Z",
            end_at="2026-03-09T13:36:00Z",
            bars=[B.recovery_bar(start_at="2026-03-09T13:35:00Z", end_at="2026-03-09T13:36:00Z", low="9.95", high="10.05")],
        )
        # The bounded recovery retry backoff (5s) must elapse first.
        reconciled = run_single_step(engine2, "2026-03-09T13:36:09Z")
        assert reconciled["recovery"]["status"] == "recovered"
        assert engine2._recovery_pending is False
        assert engine2._sink.is_live_blocked("AAPL") is False
        fills = events_of(engine2, "paper_fill")
        assert len(fills) == 1
        ref = fills[0]["envelope"]["market_ref"]
        assert ref["kind"] == "recovery_bar" and ref["path_precision"] == "minute"
        assert ref["start_at"] == "2026-03-09T13:35:00Z"

        # A pre-cursor live second is never processed (no second fill/close).
        s_pre2 = sec("2026-03-09T13:34:05Z", "2026-03-09T13:34:06Z", "9.9", high="10.05", low="9.85")
        engine2._aggregator._batches.append([s_pre2])
        run_single_step(engine2, "2026-03-09T13:36:09Z")
        assert len(events_of(engine2, "paper_fill")) == 1
        assert len(events_of(engine2, "paper_close")) == 0
        engine2.stop()


# ---------------------------------------------------------------------------
# AC7 — recovery bar fills carry typed refs; straddling/incomplete bars
# cannot be substituted for execution-frozen seconds
# ---------------------------------------------------------------------------


class TestAC7RecoveryBarTypedRefs:
    def _pending_engine(self, tmp_path: Path, *, run_id: str, recovery_result: Any) -> AutomaticPaperRuntime:
        # The wait decision is accepted exactly at the minute boundary so a
        # grid-aligned recovery bar is fully inside the X window (a bar
        # starting before decision acceptance is a typed straddle).
        clock = FakeClock("2026-03-09T13:34:00Z")
        s1 = sec("2026-03-09T13:33:56Z", "2026-03-09T13:33:57Z", "10.05")
        engine = make_engine(tmp_path, clock=clock, aggregator=B.FakeAggregator(batches=[[s1]]), run_id=run_id, db_name=f"{run_id}.db")
        engine._history_adapter.recovery_result = recovery_result
        attach_script(engine, {SID_A: [wait_outcome(engine, SID_A, x=3600)]})
        engine.start()
        run_single_step(engine, "2026-03-09T13:34:00Z")
        assert engine.scheduler.get(SID_A).state == StrategyState.WAITING_ENTRY
        return engine

    def _trigger_live_gap(self, engine: AutomaticPaperRuntime) -> None:
        engine.feed.mark_connection_gap(start_at="2026-03-09T13:34:05Z")
        s2 = sec("2026-03-09T13:34:05Z", "2026-03-09T13:34:06Z", "10.05", quality={"status": "ok", "codes": ["connection_gap"], "connection_gap": True})
        engine._aggregator._batches.append([s2])

    def test_ac07_straddling_bar_cannot_fill(self, tmp_path):
        straddle = B.recovery_bar(start_at="2026-03-09T13:33:00Z", end_at="2026-03-09T13:34:00Z", low="9.95", high="10.05")
        engine = self._pending_engine(
            tmp_path,
            run_id="run_ac07_straddle",
            recovery_result=B.recovery_result(start_at="2026-03-09T13:33:00Z", end_at="2026-03-09T13:34:00Z", bars=[straddle]),
        )
        self._trigger_live_gap(engine)
        report = run_single_step(engine, "2026-03-09T13:34:09Z")
        # A bar straddling the persisted execution cursor cannot reconcile
        # the market: the runtime stays blocked, fills nothing and keeps the
        # pending episode intact.
        assert report["recovery"]["status"] == "rejected"
        assert report["recovery"]["code"] == "recovery_straddle"
        assert engine._recovery_pending is True
        assert len(events_of(engine, "paper_fill")) == 0
        assert engine.book.get_account(SID_A).open_trade is None
        assert engine._pending_monitors[SID_A].state.status == "pending"
        engine.stop()

    def test_ac07_incomplete_bar_yields_no_assumed_trade(self, tmp_path):
        incomplete = B.recovery_bar(start_at="2026-03-09T13:34:00Z", end_at="2026-03-09T13:35:00Z", low="9.95", high="10.05", complete=False)
        engine = self._pending_engine(
            tmp_path,
            run_id="run_ac07_incomplete",
            recovery_result=B.recovery_result(start_at="2026-03-09T13:34:00Z", end_at="2026-03-09T13:35:00Z", bars=[incomplete]),
        )
        self._trigger_live_gap(engine)
        report = run_single_step(engine, "2026-03-09T13:34:09Z")
        assert report["recovery"]["status"] == "rejected"
        assert report["recovery"]["code"] == "recovery_bar_incomplete"
        assert len(events_of(engine, "paper_fill")) == 0
        assert engine.book.get_account(SID_A).open_trade is None
        engine.stop()

    def test_ac07_future_complete_bar_cannot_open_live_cutover(self, tmp_path):
        future = B.recovery_bar(start_at="2026-03-09T13:34:00Z", end_at="2026-03-09T13:35:00Z", low="9.95", high="10.05")
        engine = self._pending_engine(
            tmp_path,
            run_id="run_ac07_future",
            recovery_result=B.recovery_result(start_at="2026-03-09T13:34:00Z", end_at="2026-03-09T13:35:00Z", bars=[future]),
        )
        self._trigger_live_gap(engine)
        report = run_single_step(engine, "2026-03-09T13:34:09Z")
        assert report["recovery"]["status"] == "rejected"
        assert report["recovery"]["code"] == "recovery_bar_future"
        assert engine._recovery_pending is True
        assert engine.feed.connection_gaps
        engine.stop()

    def test_ac07_complete_bar_fills_with_typed_recovery_ref(self, tmp_path):
        bar = B.recovery_bar(start_at="2026-03-09T13:34:00Z", end_at="2026-03-09T13:35:00Z", low="9.95", high="10.05")
        engine = self._pending_engine(
            tmp_path,
            run_id="run_ac07_complete",
            recovery_result=B.recovery_result(start_at="2026-03-09T13:34:00Z", end_at="2026-03-09T13:35:00Z", bars=[bar]),
        )
        self._trigger_live_gap(engine)
        report = run_single_step(engine, "2026-03-09T13:35:09Z")
        assert report["recovery"]["status"] == "recovered"
        fills = events_of(engine, "paper_fill")
        assert len(fills) == 1
        ref = fills[0]["envelope"]["market_ref"]
        assert ref["kind"] == "recovery_bar"
        assert ref["path_precision"] == "minute"
        persisted_bar = engine._history_adapter.recovery_result.recovery_bars[0]
        assert ref["recovery_bar_id"] == persisted_bar.recovery_bar_id
        assert ref["start_at"] == "2026-03-09T13:34:00Z" and ref["end_at"] == "2026-03-09T13:35:00Z"
        assert engine.book.get_account(SID_A).open_trade is not None
        state = engine.scheduler.get(SID_A).state
        assert state in (StrategyState.OPEN_POSITION, StrategyState.MARKET_WAIT)
        if state == StrategyState.MARKET_WAIT:
            assert engine.scheduler.get(SID_A).intended_resume_state == StrategyState.OPEN_POSITION
        engine.stop()

    def test_pending_fill_persistence_retries_without_duplicate_fill(self, tmp_path):
        bar = B.recovery_bar(start_at="2026-03-09T13:34:00Z", end_at="2026-03-09T13:35:00Z", low="9.95", high="10.05")
        engine = self._pending_engine(
            tmp_path,
            run_id="run_ac07_fill_retry",
            recovery_result=B.recovery_result(start_at="2026-03-09T13:34:00Z", end_at="2026-03-09T13:35:00Z", bars=[bar]),
        )
        original = engine._sink.commit_fill
        calls = {"count": 0}

        def flaky_commit_fill(*args: Any, **kwargs: Any) -> Any:
            calls["count"] += 1
            if calls["count"] == 1:
                raise RuntimeError("transient fill write failure")
            return original(*args, **kwargs)

        engine._sink.commit_fill = flaky_commit_fill
        self._trigger_live_gap(engine)
        run_single_step(engine, "2026-03-09T13:35:09Z")
        assert calls["count"] == 2
        assert len(events_of(engine, "paper_fill")) == 1
        assert engine._fills == 1
        assert engine._pending_monitors.get(SID_A) is None
        engine.stop()


# ---------------------------------------------------------------------------
# AC8 — prompts and transport payloads contain no account data
# ---------------------------------------------------------------------------


class TestAC8BoundaryCleanPayloads:
    def test_ac08_transport_payloads_are_boundary_clean(self, tmp_path):
        clock = FakeClock("2026-03-09T13:34:03Z")
        s1 = sec("2026-03-09T13:33:59Z", "2026-03-09T13:34:00Z", "10.05")
        engine = make_engine(tmp_path, clock=clock, aggregator=B.FakeAggregator(batches=[[s1]]))
        adapter = attach_script(engine, {SID_A: [wait_outcome(engine, SID_A, x=3600)], SID_B: [immediate_outcome(engine, SID_B)]})
        engine.start()
        run_single_step(engine, "2026-03-09T13:34:03Z")
        assert adapter.call_count == 2
        for request in adapter.requests:
            assert scan_model_boundary(request) == []
            text = json.dumps(request)
            assert "api_key" not in text.lower()
            assert "authorization" not in text.lower()
            assert request["extensions"]["no_live_orders"] is True
            assert request["extensions"]["market_data_usage"] == "internal_prototype"
            assert "balance" not in request["extensions"]
        engine.stop()


# ---------------------------------------------------------------------------
# AC9 — structured nested leak/tampering fixtures
# ---------------------------------------------------------------------------


class TestAC9Tampering:
    def test_ac09_nested_balance_packet_rejected_before_transport(self, tmp_path):
        registry = B.TamperingRegistry(two_strategy_registry(), tamper=B.nested_balance_tamper)
        clock = FakeClock("2026-03-09T13:34:03Z")
        s1 = sec("2026-03-09T13:33:59Z", "2026-03-09T13:34:00Z", "10.05")
        engine = make_engine(tmp_path, clock=clock, aggregator=B.FakeAggregator(batches=[[s1]]), registry=registry)
        adapter = attach_script(engine, {SID_A: [no_entry_outcome(engine, SID_A)]})
        engine.start()
        run_single_step(engine, "2026-03-09T13:34:03Z")
        assert adapter.call_count == 0  # rejected before transport
        record = engine.scheduler.get(SID_A)
        assert record.state == StrategyState.BLOCKED
        assert record.blocked_reason == "invalid_configuration"
        assert engine.book.get_account(SID_A).open_trade is None
        assert len(events_of(engine, "paper_fill")) == 0
        engine.stop()

    def test_ac09_tampered_profile_ref_fails_finalization_and_blocks(self, tmp_path):
        clock = FakeClock("2026-03-09T13:34:03Z")
        s1 = sec("2026-03-09T13:33:59Z", "2026-03-09T13:34:00Z", "10.05")
        engine = make_engine(tmp_path, clock=clock, aggregator=B.FakeAggregator(batches=[[s1]]))
        registry = engine._registry
        loaded = registry.load_packet(SID_A)
        forged = copy.deepcopy(dict(loaded.profile_ref))
        forged["profile_ref_hash"] = "1" * 64
        forged["profile_ref_id"] = "spr_" + "1" * 64

        def tampered_outcome(request: dict[str, Any], generation: int | None = None) -> Any:
            view = engine._current_view
            cand = B.grounded_candidate(registry, SID_A, view, branch="no_entry", direction="long", n=60)
            return B.success_outcome(request, cand, profile_ref=forged, generation=generation)

        adapter = attach_script(engine, {SID_A: [tampered_outcome, tampered_outcome, tampered_outcome, tampered_outcome]})
        engine.start()
        run_single_step(engine, "2026-03-09T13:34:03Z")
        assert engine.scheduler.get(SID_A).state == StrategyState.RETRY_WAIT
        for seconds, at in ((5, "2026-03-09T13:34:08Z"), (10, "2026-03-09T13:34:18Z"), (20, "2026-03-09T13:34:38Z"), (20, "2026-03-09T13:34:58Z")):
            engine._clock.advance(seconds)
            run_single_step(engine, at)
        record = engine.scheduler.get(SID_A)
        assert record.state == StrategyState.BLOCKED
        assert record.blocked_reason == "semantic_retry_exhausted"
        assert calls_for(adapter, SID_A) == 4
        assert engine.book.get_account(SID_A).open_trade is None
        assert len(events_of(engine, "paper_fill")) == 0
        engine.stop()

    def test_ac09_persisted_profile_lineage_intact_after_restart(self, tmp_path):
        clock1 = FakeClock("2026-03-09T13:34:03Z")
        s1 = sec("2026-03-09T13:33:59Z", "2026-03-09T13:34:00Z", "10.05")
        engine1 = make_engine(tmp_path, clock=clock1, aggregator=B.FakeAggregator(batches=[[s1]]), run_id="run_ac09_a")
        attach_script(engine1, {SID_A: [wait_outcome(engine1, SID_A, x=3600)]})
        engine1.start()
        run_single_step(engine1, "2026-03-09T13:34:03Z")
        decision = events_of(engine1, "model_decision_accepted")[0]
        compact = decision["envelope"]["profile_ref"]
        loaded = engine1._registry.load_packet(SID_A)
        assert compact == {
            "profile_ref_id": loaded.profile_ref["profile_ref_id"],
            "profile_ref_hash": loaded.profile_ref["profile_ref_hash"],
            "strategy_id": SID_A,
        }
        instruction = engine1.scheduler.get(SID_A).latest_instruction
        engine1.stop()

        clock2 = FakeClock("2026-03-09T13:35:00Z")
        engine2 = make_engine(tmp_path, clock=clock2, run_id="run_ac09_b")
        engine2._history_adapter.recovery_result = B.recovery_result(
            start_at="2026-03-09T13:34:00Z",
            end_at="2026-03-09T13:35:00Z",
            bars=[B.recovery_bar(start_at="2026-03-09T13:34:00Z", end_at="2026-03-09T13:35:00Z", low="10.1", high="10.2")],
        )
        engine2.start()
        restored_instruction = engine2.scheduler.get(SID_A).latest_instruction
        assert restored_instruction is not None
        assert restored_instruction["instruction_hash"] == instruction["instruction_hash"]
        assert restored_instruction["strategy_profile_ref"]["profile_ref_hash"] == loaded.profile_ref["profile_ref_hash"]
        engine2.stop()


# ---------------------------------------------------------------------------
# AC10 — invalid market, model errors and grounding failures never open
# ---------------------------------------------------------------------------


class TestAC10TypedRetryPaths:
    def test_ac10_provider_timeout_retries_and_never_opens(self, tmp_path):
        clock = FakeClock("2026-03-09T13:34:03Z")
        s1 = sec("2026-03-09T13:33:59Z", "2026-03-09T13:34:00Z", "10.05")
        engine = make_engine(tmp_path, clock=clock, aggregator=B.FakeAggregator(batches=[[s1]]))
        attach_script(engine, {SID_A: [error_script("timeout")], SID_B: [error_script("timeout")]})
        engine.start()
        run_single_step(engine, "2026-03-09T13:34:03Z")
        record = engine.scheduler.get(SID_A)
        assert record.state == StrategyState.RETRY_WAIT
        assert record.next_due_at == datetime(2026, 3, 9, 13, 34, 8, tzinfo=UTC)
        assert record.provider_retry_count == 1
        assert engine.book.get_account(SID_A).open_trade is None
        assert engine.book.get_account(SID_B).open_trade is None
        assert len(events_of(engine, "paper_fill")) == 0
        engine._clock.advance(5)
        run_single_step(engine, "2026-03-09T13:34:08Z")
        assert engine.scheduler.get(SID_A).provider_retry_count == 2
        assert engine.scheduler.get(SID_A).next_due_at == datetime(2026, 3, 9, 13, 34, 18, tzinfo=UTC)
        engine.stop()

    def test_ac10_authentication_blocks_without_position(self, tmp_path):
        clock = FakeClock("2026-03-09T13:34:03Z")
        s1 = sec("2026-03-09T13:33:59Z", "2026-03-09T13:34:00Z", "10.05")
        engine = make_engine(tmp_path, clock=clock, aggregator=B.FakeAggregator(batches=[[s1]]))
        attach_script(engine, {SID_A: [error_script("authentication")], SID_B: [error_script("authentication")]})
        engine.start()
        run_single_step(engine, "2026-03-09T13:34:03Z")
        assert engine.scheduler.get(SID_A).state == StrategyState.BLOCKED
        assert engine.scheduler.get(SID_A).blocked_reason == "authentication"
        assert engine.book.get_account(SID_A).open_trade is None
        assert len(events_of(engine, "paper_fill")) == 0
        engine.stop()

    def test_ac10_grounding_failure_semantic_retries_then_blocks(self, tmp_path):
        clock = FakeClock("2026-03-09T13:34:03Z")
        s1 = sec("2026-03-09T13:33:59Z", "2026-03-09T13:34:00Z", "10.05")
        engine = make_engine(tmp_path, clock=clock, aggregator=B.FakeAggregator(batches=[[s1]]))
        registry = engine._registry

        def forged_claim_outcome(request: dict[str, Any], generation: int | None = None) -> Any:
            view = engine._current_view
            cand = B.grounded_candidate(registry, SID_A, view, branch="wait_for_price", direction="long", entry="10", stop="9.9", target="10.3", x=3600)
            cand["grounding"]["stop_price"]["claim_ids"] = ["nanqiao-range-extreme-limit:invalidation_stop:999"]
            return B.success_outcome(request, cand, profile_ref=registry.load_packet(SID_A).profile_ref, generation=generation)

        adapter = attach_script(engine, {SID_A: [forged_claim_outcome] * 4})
        engine.start()
        for seconds, at in ((0, "2026-03-09T13:34:03Z"), (5, "2026-03-09T13:34:08Z"), (10, "2026-03-09T13:34:18Z"), (20, "2026-03-09T13:34:38Z")):
            if seconds:
                engine._clock.advance(seconds)
            run_single_step(engine, at)
        record = engine.scheduler.get(SID_A)
        assert record.state == StrategyState.BLOCKED
        assert record.blocked_reason == "semantic_retry_exhausted"
        assert calls_for(adapter, SID_A) == 4
        assert engine.book.get_account(SID_A).open_trade is None
        assert len(events_of(engine, "paper_fill")) == 0
        engine.stop()


# ---------------------------------------------------------------------------
# AC11 — insufficient startup history / missing frozen second
# ---------------------------------------------------------------------------


class TestAC11InsufficientStartup:
    def test_ac11_insufficient_history_zero_calls_zero_entries(self, tmp_path):
        two_bar = [
            {"t": "2026-03-09T13:30:00Z", "o": 100.0, "h": 101.25, "l": 99.5, "c": 100.75, "v": 1000},
            {"t": "2026-03-09T13:31:00Z", "o": 100.75, "h": 101.5, "l": 100.5, "c": 101.25, "v": 1200},
        ]
        history = B.history_result(end_at="2026-03-09T13:32:00Z", start_at="2026-03-09T13:30:00Z", bars=two_bar)
        clock = FakeClock("2026-03-09T13:33:03Z")
        # The second stays inside the forming bucket so no third closed bar
        # joins the closed snapshot: the view remains insufficient.
        s1 = sec("2026-03-09T13:33:00Z", "2026-03-09T13:33:01Z", "10.05")
        engine = make_engine(
            tmp_path,
            clock=clock,
            history=history,
            aggregator=B.FakeAggregator(batches=[[s1]]),
            config_overrides={"minimum_closed_bars": 3},
        )
        adapter = attach_script(engine, {SID_A: [no_entry_outcome(engine, SID_A)]})
        report = engine.start()
        assert report["view_executable"] is False
        report2 = run_single_step(engine, "2026-03-09T13:33:03Z")
        assert report2["view_executable"] is False
        assert report2["provider_calls"] == 0
        assert adapter.call_count == 0
        assert engine.book.get_account(SID_A).open_trade is None
        assert len(events_of(engine, "paper_fill")) == 0
        engine.stop()

    def test_ac11_missing_frozen_second_zero_calls(self, tmp_path):
        clock = FakeClock("2026-03-09T13:34:03Z")
        engine = make_engine(tmp_path, clock=clock, aggregator=B.FakeAggregator(batches=[]))
        adapter = attach_script(engine, {SID_A: [no_entry_outcome(engine, SID_A)]})
        report = engine.start()
        assert report["view_executable"] is False
        report2 = run_single_step(engine, "2026-03-09T13:34:03Z")
        assert report2["provider_calls"] == 0
        assert adapter.call_count == 0
        assert engine.scheduler.get(SID_A).state == StrategyState.MARKET_WAIT
        engine.stop()

    def test_ac11_empty_history_view_failure_keeps_pre_start(self, tmp_path):
        engine = make_engine(tmp_path, history=B.empty_history_result())
        report = engine.start()
        assert report["view_id"] is None
        assert report["view_executable"] is False
        assert report["errors"]
        assert engine.status()["started"] is False
        with pytest.raises(RuntimeEngineError) as excinfo:
            engine.step(now="2026-03-09T13:34:03Z")
        assert excinfo.value.code == "not_started"
        engine.stop()


# ---------------------------------------------------------------------------
# AC12 — immediate drift records the second, rejects, recalls once
# ---------------------------------------------------------------------------


class TestAC12ImmediateDrift:
    def test_ac12_drift_rejects_records_second_and_recalls_once(self, tmp_path):
        clock = FakeClock("2026-03-09T13:34:03Z")
        s1 = sec("2026-03-09T13:33:59Z", "2026-03-09T13:34:00Z", "10.05")
        s2 = sec("2026-03-09T13:34:00Z", "2026-03-09T13:34:01Z", "10.6")
        engine = make_engine(tmp_path, clock=clock, aggregator=B.FakeAggregator(batches=[[s1], [s2]]))
        adapter = attach_script(
            engine,
            {
                SID_A: [no_entry_outcome(engine, SID_A), no_entry_outcome(engine, SID_A)],
                SID_B: [no_entry_outcome(engine, SID_B), no_entry_outcome(engine, SID_B)],
            },
        )
        engine.start()
        run_single_step(engine, "2026-03-09T13:34:03Z")
        run_single_step(engine, "2026-03-09T13:34:04Z")
        calls_before = adapter.call_count
        record = engine.scheduler.get(SID_A)
        second = engine._current_view.latest_second
        drift_instruction = {
            "branch": "enter_immediately",
            "direction": "long",
            "stop_price": "9.9",
            "target_price": "10.3",
            "second_event_ref": make_second_event_ref(s1.event_id, s1.event_hash, "10.05"),
        }
        result = engine._open_requester(SID_A, drift_instruction, generation=record.generation, idempotency_key="open:ac12")
        assert result.accepted is False
        assert result.fresh_price_rejected is True
        rejected = events_of(engine, "paper_fill_rejected")
        assert len(rejected) == 1
        reason = rejected[0]["envelope"]["payload"]["reason"]
        assert second["event_id"] in reason
        assert second["event_hash"] in reason
        assert engine.book.get_account(SID_A).balance == 1000
        assert engine.book.get_account(SID_A).open_trade is None
        assert len(events_of(engine, "paper_fill")) == 0
        assert adapter.call_count == calls_before

        record.state = StrategyState.ANALYSIS_DUE
        record.rejected_immediate_view_id = engine._current_view.view_id
        engine._scheduler.on_market_view(engine._view_mapping())
        assert engine._scheduler.queue_size == 0
        s3 = sec("2026-03-09T13:34:05Z", "2026-03-09T13:34:06Z", "10.05")
        engine._aggregator._batches.append([s3])
        run_single_step(engine, "2026-03-09T13:34:09Z")
        assert adapter.call_count == calls_before + 1  # exactly one recall
        engine.stop()


# ---------------------------------------------------------------------------
# AC13 — raw adjustment + regular session identical across the pipeline
# ---------------------------------------------------------------------------


class TestAC13RawSessionIdentity:
    def test_ac13_raw_adjustment_regular_session_identical(self, tmp_path):
        clock = FakeClock("2026-03-09T13:34:03Z")
        s1 = sec("2026-03-09T13:33:59Z", "2026-03-09T13:34:00Z", "10.05")
        engine = make_engine(tmp_path, clock=clock, aggregator=B.FakeAggregator(batches=[[s1]]))
        attach_script(engine, {SID_A: [immediate_outcome(engine, SID_A, direction="long", stop="9.9", target="10.3")]})
        engine.start()
        history = engine.feed.history_result.market_snapshot
        assert history["adjustment_policy"] == "unadjusted"
        assert history["extensions"]["market_feed"] == ALPACA_IEX_FEED.to_dict()
        assert history["session"] == {"kind": "regular", "timezone": "America/New_York"}
        assert history["instrument"]["venue"] == "IEX"

        run_single_step(engine, "2026-03-09T13:34:03Z")
        event = engine.feed.frozen_events[0]
        assert event.market_feed == ALPACA_IEX_FEED
        assert event.source == "alpaca_iex"
        assert event.adjustment == "raw" and event.raw_adjustment == "raw"

        view = engine._current_view
        assert view.market_feed == ALPACA_IEX_FEED
        assert view.adjustment_policy == "unadjusted"
        assert view.to_dict()["raw_adjustment_basis"] == "raw"
        assert view.session == {"kind": "regular", "timezone": "America/New_York"}

        fills = events_of(engine, "paper_fill")
        assert len(fills) == 1
        ref = fills[0]["envelope"]["market_ref"]
        assert ref["kind"] == "second_event"
        assert ref["symbol"] == "AAPL"
        assert fills[0]["envelope"]["mode"] == "automatic"
        engine.stop()


# ---------------------------------------------------------------------------
# AC14 — feed-scope mismatch stops the runtime
# ---------------------------------------------------------------------------


class TestAC14FeedScopeMismatch:
    def test_ac14_sip_event_stops_runtime(self, tmp_path):
        clock = FakeClock("2026-03-09T13:34:03Z")
        sip = B.sip_event(start_at="2026-03-09T13:33:59Z", end_at="2026-03-09T13:34:00Z", close="10.05")
        engine = make_engine(tmp_path, clock=clock, aggregator=B.FakeAggregator(batches=[[sip]]))
        engine.start()
        with pytest.raises(RuntimeEngineError) as excinfo:
            run_single_step(engine, "2026-03-09T13:34:03Z")
        assert excinfo.value.code == "feed_scope_mismatch"
        assert engine.status()["stopped"] is True
        with pytest.raises(RuntimeEngineError) as excinfo2:
            run_single_step(engine, "2026-03-09T13:34:04Z")
        assert excinfo2.value.code == "stopped"
        assert engine._fills == 0
        assert engine._closes == 0

    def test_ac14_sip_history_rejected_at_startup(self, tmp_path):
        forged = copy.deepcopy(dict(B.history_result().market_snapshot))
        forged["extensions"]["market_feed"]["feed"] = "sip"
        history = B.history_result(snapshot=forged)
        engine = make_engine(tmp_path, history=history)
        report = engine.start()
        assert any(error.get("code") == "history_feed_mismatch" for error in report["errors"])
        assert report["view_executable"] is False


# ---------------------------------------------------------------------------
# AC15 — internal_prototype labels, no export surface
# ---------------------------------------------------------------------------


class TestAC15InternalPrototypeLabels:
    def test_ac15_status_labels_and_no_export_surface(self, tmp_path):
        engine = make_engine(tmp_path)
        engine.start()
        status = engine.status()
        assert status["market_data_usage"] == "internal_prototype"
        assert "PAPER TRADING ONLY" in status["paper_only"]
        assert "internal_prototype" in status["paper_only"]
        assert status["no_live_orders"] is True
        assert status["feed"] == "alpaca:iex:single_exchange"
        assert engine.feed.status()["market_data_usage"] == "internal_prototype"
        for name in ("export", "display", "publish", "redistribute"):
            assert not hasattr(engine, name)
        engine.stop()


# ---------------------------------------------------------------------------
# AC16 — no OpenAI call, broker import or real-order code path
# ---------------------------------------------------------------------------


class TestFixtureIndex:
    def test_engine_fixture_index_is_closed(self):
        index = json.loads((FIXTURE_ROOT / "fixture-index.json").read_text(encoding="utf-8"))
        declared = {entry["path"] for entry in index["entries"]}
        actual = {path.name for path in FIXTURE_ROOT.glob("*") if path.name != "fixture-index.json" and not path.name.startswith("__")}
        assert declared == actual


class TestAC16NoOpenAIOrBroker:
    def test_ac16_no_openai_no_broker_no_live_order_imports(self):

        runtime_dir = ROOT / "pa_engine" / "runtime"
        forbidden_roots = ("openai", "broker", "brokers", "alpaca_trade_api", "alpaca_py")
        forbidden_segments = ("order", "live", "broker")
        for path in sorted(runtime_dir.glob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    modules = [alias.name for alias in node.names]
                elif isinstance(node, ast.ImportFrom):
                    modules = [node.module or ""]
                else:
                    continue
                for module in modules:
                    root = module.split(".", 1)[0].lower()
                    assert root not in forbidden_roots, (path, module)
                    last = module.split(".")[-1].lower()
                    assert not any(segment in last for segment in forbidden_segments), (path, module)
        engine_source = (runtime_dir / "engine.py").read_text(encoding="utf-8")
        lowered = engine_source.lower()
        assert "openai" not in lowered
        assert "broker" not in lowered
        assert "live order" not in lowered
        assert "pa_engine.storage" in engine_source
        assert "pa_engine.strategy.runtime" in engine_source


# ---------------------------------------------------------------------------
# Construction/runtime guard tests
# ---------------------------------------------------------------------------


class TestRuntimeGuards:
    def test_step_before_start_raises_not_started(self, tmp_path):
        engine = make_engine(tmp_path)
        with pytest.raises(RuntimeEngineError) as excinfo:
            engine.step(now="2026-03-09T13:34:03Z")
        assert excinfo.value.code == "not_started"

    def test_stop_releases_lease_for_new_engine(self, tmp_path):
        engine1 = make_engine(tmp_path, run_id="run_lease_a")
        engine1.start()
        engine1.stop()
        engine2 = make_engine(tmp_path, run_id="run_lease_b")
        report = engine2.start()
        assert report["errors"] == []
        engine2.stop()

    def test_run_with_max_steps(self, tmp_path):
        clock = FakeClock("2026-03-09T13:34:03Z")
        s1 = sec("2026-03-09T13:33:59Z", "2026-03-09T13:34:00Z", "10.05")
        engine = make_engine(tmp_path, clock=clock, aggregator=B.FakeAggregator(batches=[[s1]]))
        attach_script(engine, {SID_A: [no_entry_outcome(engine, SID_A)]})
        summary = engine.run(max_steps=2)
        assert summary["steps"] == 2
        assert summary["final_status"]["started"] is True
        engine.stop()

    def test_failed_start_releases_lease_and_run_does_not_retry_external_startup(self, tmp_path):
        engine = make_engine(tmp_path, history=B.empty_history_result(), run_id="run_empty_history")
        report = engine.start()
        assert report["view_id"] is None
        assert engine.status()["started"] is False

        # The failed startup has released its singleton lease; another run
        # can inspect/open the same file immediately.
        replacement = open_database(str(tmp_path / "run_empty_history.db"), run_id="replacement", mode="automatic")
        replacement.close(release_lease=True)

        history_calls = len(engine._history_adapter.calls)
        summary = engine.run(max_steps=1)
        assert summary["steps"] == 0
        assert len(engine._history_adapter.calls) == history_calls
        engine.stop(reason="startup_failed")

    def test_start_exception_releases_lease_for_registry_failure(self, tmp_path):
        registry = B.TamperingRegistry(two_strategy_registry(), load_error="packet_invalid")
        engine = make_engine(tmp_path, registry=registry, run_id="run_registry_failure")
        with pytest.raises(RuntimeEngineError) as error:
            engine.start()
        assert error.value.code == "invalid_config"
        replacement = open_database(str(tmp_path / "engine.db"), run_id="replacement", mode="automatic")
        replacement.close(release_lease=True)
        engine.stop(reason="startup_exception")

    def test_scheduler_disable_cancels_pending_without_stranding(self, tmp_path):
        clock = FakeClock("2026-03-09T13:34:03Z")
        s1 = sec("2026-03-09T13:33:59Z", "2026-03-09T13:34:00Z", "10.05")
        engine = make_engine(tmp_path, clock=clock, aggregator=B.FakeAggregator(batches=[[s1]]))
        attach_script(engine, {SID_A: [wait_outcome(engine, SID_A, x=3600)], SID_B: [no_entry_outcome(engine, SID_B)]})
        engine.start()
        run_single_step(engine, "2026-03-09T13:34:03Z")
        assert engine.scheduler.get(SID_A).state == StrategyState.WAITING_ENTRY
        assert engine._scheduler.disable(SID_A, reason="operator") is True
        assert engine.scheduler.get(SID_A).state == StrategyState.DISABLED
        assert engine.book.get_account(SID_A).pending is False
        assert engine.book.get_account(SID_A).pending_trade is None
        assert len(events_of(engine, "pending_entry_cancelled")) == 1
        rows = engine._store.list_pending_entries(strategy_id=SID_A)
        assert rows and rows[0]["status"] == "cancelled"
        assert len(engine._pending_monitors) == 0
        engine.stop()


# ---------------------------------------------------------------------------
# Sol adversarial-review regression tests (audit findings)
# ---------------------------------------------------------------------------


class TestAuditSessionStalenessGate:
    def test_session_close_stops_calls_and_next_session_recalls_once(self, tmp_path):
        # 2026-03-09 is a Monday; EDT (UTC-4) after the 2026-03-08 DST
        # switch.  The decision is accepted at 15:59:50 ET and N=30 lands
        # after the 16:00 session close (20:00 UTC).
        clock = FakeClock("2026-03-09T19:59:50Z")
        s1 = sec("2026-03-09T19:59:44Z", "2026-03-09T19:59:45Z", "10.05")
        engine = make_engine(tmp_path, clock=clock, aggregator=B.FakeAggregator(batches=[[s1]]))
        adapter = attach_script(engine, {SID_A: [no_entry_outcome(engine, SID_A, n=30), no_entry_outcome(engine, SID_A)], SID_B: [no_entry_outcome(engine, SID_B, n=30), no_entry_outcome(engine, SID_B)]})
        engine.start()
        for at in ("2026-03-09T19:59:50Z", "2026-03-09T20:00:21Z"):
            engine._clock.set(at)
            run_single_step(engine, at)
        assert adapter.call_count == 2

        # 16:00 ET = 20:00 UTC: the N timer fires outside the session; no
        # provider call may happen on the closed session.
        assert engine.scheduler.get(SID_A).state == StrategyState.MARKET_WAIT

        # The next session day with a fresh second recalls each strategy
        # exactly once (cross-day watermarks must not drop the new day's
        # seconds).
        s2 = sec("2026-03-10T13:34:47Z", "2026-03-10T13:34:48Z", "10.05")
        engine._aggregator._batches.append([s2])
        for at in ("2026-03-10T13:34:51Z", "2026-03-10T13:34:52Z", "2026-03-10T13:34:53Z"):
            engine._clock.set(at)
            run_single_step(engine, at)
        assert adapter.call_count == 4
        engine.stop()

    def test_stale_view_blocks_fill_and_new_second_rearms(self, tmp_path):
        clock = FakeClock("2026-03-09T13:34:03Z")
        s1 = sec("2026-03-09T13:33:59Z", "2026-03-09T13:34:00Z", "10.05")
        engine = make_engine(tmp_path, clock=clock, aggregator=B.FakeAggregator(batches=[[s1]]))
        attach_script(engine, {SID_A: [wait_outcome(engine, SID_A, x=3600)], SID_B: [no_entry_outcome(engine, SID_B)]})
        engine.start()
        for at in ("2026-03-09T13:34:03Z", "2026-03-09T13:35:12Z", "2026-03-09T13:35:15Z"):
            engine._clock.set(at)
        engine._clock.set("2026-03-09T13:34:03Z")
        run_single_step(engine, "2026-03-09T13:34:03Z")
        assert engine.scheduler.get(SID_A).state == StrategyState.WAITING_ENTRY

        # 61 seconds later the view is stale (fresh=False): a touching
        # second must not fill the pending entry.
        s_touch = sec("2026-03-09T13:34:10Z", "2026-03-09T13:34:11Z", "10", high="10.1", low="9.9")
        engine._aggregator._batches.append([s_touch])
        engine._clock.set("2026-03-09T13:35:12Z")
        run_single_step(engine, "2026-03-09T13:35:12Z")
        assert engine._pending_monitors[SID_A].state.status == "pending"
        assert len(events_of(engine, "paper_fill")) == 0

        # A fresh second re-arms the view and the touch fills exactly once.
        s_fresh = sec("2026-03-09T13:35:11Z", "2026-03-09T13:35:12Z", "10", high="10.1", low="9.9")
        engine._aggregator._batches.append([s_fresh])
        engine._clock.set("2026-03-09T13:35:15Z")
        run_single_step(engine, "2026-03-09T13:35:15Z")
        assert len(events_of(engine, "paper_fill")) == 1
        engine.stop()


class TestAuditPersistenceHardening:
    def test_semantic_failure_persists_no_decision(self, tmp_path):
        clock = FakeClock("2026-03-09T13:34:03Z")
        s1 = sec("2026-03-09T13:33:59Z", "2026-03-09T13:34:00Z", "10.05")
        engine = make_engine(tmp_path, clock=clock, aggregator=B.FakeAggregator(batches=[[s1]]))

        def forged(request: dict[str, Any], generation: int | None = None) -> Any:
            view = engine._current_view
            cand = B.grounded_candidate(engine._registry, SID_A, view, branch="wait_for_price", direction="long", entry="10", stop="9.9", target="10.3", x=3600)
            cand["grounding"]["stop_price"]["claim_ids"] = ["nanqiao-range-extreme-limit:invalidation_stop:999"]
            return B.success_outcome(request, cand, profile_ref=engine._registry.load_packet(SID_A).profile_ref, generation=generation)

        attach_script(engine, {SID_A: [forged, no_entry_outcome(engine, SID_A)], SID_B: [no_entry_outcome(engine, SID_B)]})
        engine.start()
        run_single_step(engine, "2026-03-09T13:34:03Z")
        assert len(events_of(engine, "model_decision_accepted")) == 1  # only B's no-entry
        assert engine.scheduler.get(SID_A).state == StrategyState.RETRY_WAIT
        # After the semantic retry succeeds, exactly one decision event exists
        # for A and no duplicated lineage.
        run_single_step(engine, "2026-03-09T13:34:09Z")
        decisions = events_of(engine, "model_decision_accepted")
        assert len(decisions) == 2
        assert sum(1 for event in decisions if event["strategy_id"] == SID_A) == 1
        engine.stop()

    def test_ac12_rejection_gate_survives_restart(self, tmp_path):
        clock = FakeClock("2026-03-09T13:34:03Z")
        s1 = sec("2026-03-09T13:33:59Z", "2026-03-09T13:34:00Z", "10.05")
        s2 = sec("2026-03-09T13:34:00Z", "2026-03-09T13:34:01Z", "10.6")
        engine1 = make_engine(tmp_path, clock=clock, aggregator=B.FakeAggregator(batches=[[s1], [s2]]), run_id="run_gate_a")
        attach_script(engine1, {SID_A: [no_entry_outcome(engine1, SID_A), no_entry_outcome(engine1, SID_A)], SID_B: [no_entry_outcome(engine1, SID_B), no_entry_outcome(engine1, SID_B)]})
        engine1.start()
        run_single_step(engine1, "2026-03-09T13:34:03Z")
        run_single_step(engine1, "2026-03-09T13:34:04Z")
        record = engine1.scheduler.get(SID_A)
        drift_instruction = {
            "branch": "enter_immediately",
            "direction": "long",
            "stop_price": "9.9",
            "target_price": "10.3",
            "second_event_ref": make_second_event_ref(s1.event_id, s1.event_hash, "10.05"),
        }
        result = engine1._open_requester(SID_A, drift_instruction, generation=record.generation, idempotency_key="open:gate")
        assert result.fresh_price_rejected is True
        view_id = engine1._current_view.view_id
        record.state = StrategyState.ANALYSIS_DUE
        record.rejected_immediate_view_id = view_id
        record.transition_count += 1
        record.transition_log.append(
            {"from": "analyzing", "to": "analysis_due", "reason": "open_rejected", "generation": record.generation}
        )
        engine1._persist_sweep(engine1._now())
        assert engine1._store.get_strategy_state(SID_A)["rejected_immediate_view_id"] == view_id
        engine1.stop()

        # Restart against the same database: the gate value must survive.
        clock2 = FakeClock("2026-03-09T13:35:03Z")
        engine2 = make_engine(tmp_path, clock=clock2, run_id="run_gate_b")
        engine2._history_adapter.recovery_result = B.recovery_result(
            start_at="2026-03-09T13:34:01Z",
            end_at="2026-03-09T13:35:01Z",
            bars=[B.recovery_bar(start_at="2026-03-09T13:34:01Z", end_at="2026-03-09T13:35:01Z", low="9.9", high="10.7")],
        )
        engine2.start()
        restored = engine2.scheduler.get(SID_A)
        assert restored.rejected_immediate_view_id == view_id
        engine2.stop()

    def test_stop_keeps_first_reason_and_run_after_stop_raises(self, tmp_path):
        clock = FakeClock("2026-03-09T13:34:03Z")
        engine = make_engine(tmp_path, clock=clock)
        engine.start()
        first = engine.stop(reason="operator")
        second = engine.stop(reason="other")
        assert second["reason"] == "operator"
        assert first["stopped"] is True and second["stopped"] is True
        with pytest.raises(RuntimeEngineError):
            engine.run(max_steps=1)


class TestAuditRecoveryBinding:
    def _gapped_pending_engine(self, tmp_path: Path, *, run_id: str, bars: list[Any], start: str, end: str) -> AutomaticPaperRuntime:
        return TestAC7RecoveryBarTypedRefs()._pending_engine(
            tmp_path,
            run_id=run_id,
            recovery_result=B.recovery_result(start_at=start, end_at=end, bars=bars),
        )

    def test_foreign_symbol_recovery_bar_rejected(self, tmp_path):
        foreign = B.recovery_bar(start_at="2026-03-09T13:34:00Z", end_at="2026-03-09T13:35:00Z", low="9.95", high="10.05")
        foreign_dict = foreign.to_dict()
        foreign_dict["instrument"] = dict(foreign_dict["instrument"], symbol="MSFT")
        engine = self._gapped_pending_engine(tmp_path, run_id="run_bind_sym", bars=[foreign_dict], start="2026-03-09T13:34:00Z", end="2026-03-09T13:35:00Z")
        TestAC7RecoveryBarTypedRefs()._trigger_live_gap(engine)
        report = run_single_step(engine, "2026-03-09T13:34:09Z")
        assert report["recovery"]["code"] == "recovery_symbol_mismatch"
        assert engine._recovery_pending is True
        assert len(events_of(engine, "paper_fill")) == 0
        engine.stop()

    def test_mixed_request_hash_recovery_rejected(self, tmp_path):
        bar_a = B.recovery_bar(start_at="2026-03-09T13:34:00Z", end_at="2026-03-09T13:35:00Z", low="9.95", high="10.05", source_request_hash="h1")
        bar_b = B.recovery_bar(start_at="2026-03-09T13:35:00Z", end_at="2026-03-09T13:36:00Z", low="9.95", high="10.05", source_request_hash="h2")
        engine = self._gapped_pending_engine(tmp_path, run_id="run_bind_hash", bars=[bar_a, bar_b], start="2026-03-09T13:34:00Z", end="2026-03-09T13:36:00Z")
        TestAC7RecoveryBarTypedRefs()._trigger_live_gap(engine)
        report = run_single_step(engine, "2026-03-09T13:37:09Z")
        assert report["recovery"]["code"] == "recovery_request_hash_mismatch"
        assert len(events_of(engine, "paper_fill")) == 0
        engine.stop()

    def test_invalid_no_path_recovery_bar_is_rejected_before_commit(self, tmp_path):
        bar = B.recovery_bar(start_at="2026-03-09T13:34:00Z", end_at="2026-03-09T13:35:00Z")
        result = B.recovery_result(
            start_at="2026-03-09T13:34:00Z",
            end_at="2026-03-09T13:35:00Z",
            bars=[bar],
        )
        forged = bar.to_dict()
        forged["source_request_hash"] = result.source_request_hash
        forged["path_precision"] = "no_path"
        result = replace(result, recovery_bars=(forged,))
        engine = self._gapped_pending_engine(
            tmp_path,
            run_id="run_bind_no_path",
            bars=[forged],
            start="2026-03-09T13:34:00Z",
            end="2026-03-09T13:35:00Z",
        )
        engine._history_adapter.recovery_result = result
        TestAC7RecoveryBarTypedRefs()._trigger_live_gap(engine)
        report = run_single_step(engine, "2026-03-09T13:34:09Z")
        assert report["recovery"]["status"] == "rejected"
        assert report["recovery"]["code"] == "recovery_bar_path_precision_mismatch"
        assert engine._recovery_pending is True
        assert len(events_of(engine, "market_recovery")) == 0
        engine.stop()

    def test_non_contiguous_recovery_rejected(self, tmp_path):
        bar_a = B.recovery_bar(start_at="2026-03-09T13:34:00Z", end_at="2026-03-09T13:35:00Z", low="9.95", high="10.05")
        bar_b = B.recovery_bar(start_at="2026-03-09T13:36:00Z", end_at="2026-03-09T13:37:00Z", low="9.95", high="10.05")
        engine = self._gapped_pending_engine(tmp_path, run_id="run_bind_gap", bars=[bar_a, bar_b], start="2026-03-09T13:34:00Z", end="2026-03-09T13:37:00Z")
        TestAC7RecoveryBarTypedRefs()._trigger_live_gap(engine)
        report = run_single_step(engine, "2026-03-09T13:37:09Z")
        assert report["recovery"]["code"] == "recovery_bars_not_contiguous"
        assert len(events_of(engine, "paper_fill")) == 0
        engine.stop()
