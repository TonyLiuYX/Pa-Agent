"""G028 full vertical slice: fake clock + canned seconds + scripted model.

The slice runs two strategies through the complete user process in one
deterministic run: no-entry/N, immediate entry, waiting entry/X expiry,
take-profit, stop-loss and immediate recalls, with a mid-pending restart
that preserves the episode and resumes to a single fill.
"""

from __future__ import annotations

import importlib.util
from datetime import timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from pa_engine.runtime.config import RuntimeConfig
from pa_engine.runtime.engine import AutomaticPaperRuntime
from pa_engine.strategy.runtime import FakeClock, StrategyState

ROOT = Path(__file__).resolve().parents[3]
FIXTURE_ROOT = ROOT / "tests" / "fixtures" / "phase2" / "g028" / "engine"

SID_A = "pa:nanqiao-range-extreme-limit"
SID_B = "pa:nanqiao-always-in-ema20"

UTC = timezone.utc


def _builders() -> Any:
    spec = importlib.util.spec_from_file_location("g028_engine_builders", FIXTURE_ROOT / "builders.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


B = _builders()


def _registry() -> Any:
    from pa_engine.strategies.knowledge import build_registry

    return build_registry(
        str(ROOT / "knowledge"),
        registrations=[
            {"strategy_id": SID_A, "record_id": "nanqiao-range-extreme-limit"},
            {"strategy_id": SID_B, "record_id": "nanqiao-always-in-ema20"},
        ],
    )


def _engine(tmp_path: Path, *, clock: FakeClock, run_id: str, history: Any, aggregator: Any, db_name: str = "slice.db") -> AutomaticPaperRuntime:
    config = RuntimeConfig(symbol="AAPL", timeframe="1min", database_path=str(tmp_path / db_name))
    return AutomaticPaperRuntime(
        config,
        registry=_registry(),
        history_adapter=B.FakeHistoryAdapter(history_result=history),
        aggregator=aggregator,
        db_path=str(tmp_path / db_name),
        run_id=run_id,
        clock=clock,
    )


def _sec(start: str, end: str, close: str, high: str | None = None, low: str | None = None) -> Any:
    return B.frozen_second(start_at=start, end_at=end, close=close, high=high, low=low)


def _attach(engine: AutomaticPaperRuntime, script: dict[str, list[Any]]) -> Any:
    adapter = B.ScriptedAdapter(script)
    engine._adapter = adapter
    return adapter


def _outcome(engine: AutomaticPaperRuntime, sid: str, branch: str, **kwargs: Any) -> Any:
    registry = engine._registry

    def build(request: dict[str, Any], generation: int | None = None) -> Any:
        loaded = registry.load_packet(sid)
        cand = B.grounded_candidate(registry, sid, engine._current_view, branch=branch, **kwargs)
        return B.success_outcome(request, cand, profile_ref=loaded.profile_ref, generation=generation)

    return build


def _no_entry(engine: AutomaticPaperRuntime, sid: str, n: int) -> Any:
    return _outcome(engine, sid, "no_entry", direction="long", n=n)


def _wait(engine: AutomaticPaperRuntime, sid: str, *, entry: str, stop: str, target: str, x: int) -> Any:
    return _outcome(engine, sid, "wait_for_price", direction="long", entry=entry, stop=stop, target=target, x=x)


def _immediate(engine: AutomaticPaperRuntime, sid: str, *, stop: str, target: str) -> Any:
    def build(request: dict[str, Any], generation: int | None = None) -> Any:
        view = engine._current_view
        loaded = engine._registry.load_packet(sid)
        cand = B.grounded_candidate(
            engine._registry, sid, view, branch="enter_immediately", direction="long",
            stop=stop, target=target, second_event_ref=B.second_ref_of(view),
        )
        return B.success_outcome(request, cand, profile_ref=loaded.profile_ref, generation=generation)

    return build


def _events(engine: AutomaticPaperRuntime, event_type: str) -> list[dict[str, Any]]:
    return engine._store.list_events(event_type=event_type)


def _calls_for(adapter: Any, sid: str) -> int:
    return sum(
        1
        for request in adapter.requests
        if request.get("extensions", {}).get("strategy_profile_ref", {}).get("strategy_id") == sid
    )


class TestVerticalSlice:
    def test_full_cycle_no_entry_wait_expiry_tp_sl_and_recalls(self, tmp_path):
        clock = FakeClock("2026-03-09T13:34:03Z")
        history = B.history_result()
        s1 = _sec("2026-03-09T13:33:59Z", "2026-03-09T13:34:00Z", "100.5")
        engine = _engine(tmp_path, clock=clock, run_id="run_slice_a", history=history, aggregator=B.FakeAggregator(batches=[[s1]]))
        adapter = _attach(
            engine,
            {
                SID_A: [
                    _no_entry(engine, SID_A, 57),  # call1: no entry, N=57
                    _wait(engine, SID_A, entry="100", stop="99", target="102", x=60),  # call2: wait, X=60
                    _wait(engine, SID_A, entry="100", stop="99", target="102", x=120),  # call3: wait after expiry
                    _no_entry(engine, SID_A, 60),  # call4: no entry after stop
                ],
                SID_B: [
                    _immediate(engine, SID_B, stop="99.5", target="102"),  # call1: immediate long
                    _no_entry(engine, SID_B, 60),  # call2: recall after TP
                ],
            },
        )
        report = engine.start()
        assert report["errors"] == []

        # Step 1: shared view with s1; A no_entry (N=57), B immediate entry.
        engine.step(now="2026-03-09T13:34:03Z")
        assert engine.scheduler.get(SID_A).state == StrategyState.SLEEPING
        assert engine.scheduler.get(SID_B).state == StrategyState.OPEN_POSITION
        assert engine.book.get_account(SID_B).open_trade.direction == "long"

        # Step 2 @13:35:00: A's N expires -> call2 wait X=60 at the grid
        # boundary; no new seconds.
        engine.step(now="2026-03-09T13:35:00Z")
        assert engine.scheduler.get(SID_A).state == StrategyState.WAITING_ENTRY
        assert len(engine._pending_monitors) == 1

        # Step 3: non-touching seconds in the 13:34 and 13:35 buckets (a
        # no-trade minute would gap the closed snapshot suffix); zero model
        # calls while pending/open.
        engine._aggregator._batches.append(
            [
                _sec("2026-03-09T13:34:05Z", "2026-03-09T13:34:06Z", "100.2", high="100.5", low="100.05"),
                _sec("2026-03-09T13:35:05Z", "2026-03-09T13:35:06Z", "100.2", high="100.5", low="100.05"),
            ]
        )
        engine.step(now="2026-03-09T13:35:09Z")
        assert _calls_for(adapter, SID_A) == 2
        assert _calls_for(adapter, SID_B) == 1
        assert adapter.call_count == 3

        # Step 4 @13:36:03: B's target touched -> TP settle + recall job; A's
        # X=60 expiry (resolution 13:36:03) -> analysis_due + recall job.
        engine._aggregator._batches.append([_sec("2026-03-09T13:35:59Z", "2026-03-09T13:36:00Z", "102", high="102.2", low="101")])
        engine.step(now="2026-03-09T13:36:03Z")
        assert engine.scheduler.get(SID_B).state == StrategyState.ANALYSIS_DUE
        assert engine.scheduler.get(SID_A).state == StrategyState.ANALYSIS_DUE
        assert len(_events(engine, "paper_close")) == 1
        assert len(_events(engine, "pending_entry_expired")) == 1

        # Step 5 @13:36:00 (grid boundary): both recall jobs run -> A call3
        # wait X=120 accepted at the boundary, B call2 no_entry.
        engine.step(now="2026-03-09T13:36:00Z")
        assert engine.scheduler.get(SID_A).state == StrategyState.WAITING_ENTRY
        assert engine.scheduler.get(SID_B).state == StrategyState.SLEEPING
        assert _calls_for(adapter, SID_A) == 3
        assert _calls_for(adapter, SID_B) == 2

        # Step 6: a touching second fills A's pending at 100.
        engine._aggregator._batches.append([_sec("2026-03-09T13:36:05Z", "2026-03-09T13:36:06Z", "100", high="100.05", low="99.95")])
        engine.step(now="2026-03-09T13:36:09Z")
        assert engine.scheduler.get(SID_A).state == StrategyState.OPEN_POSITION
        assert len(_events(engine, "paper_fill")) == 2  # B immediate + A pending
        trade_a = engine.book.get_account(SID_A).open_trade
        assert trade_a.entry_price == Decimal("100")

        # Step 7: a stop-touching second settles A at 99 (stop-loss).
        engine._aggregator._batches.append([_sec("2026-03-09T13:36:10Z", "2026-03-09T13:36:11Z", "99", high="100.5", low="98.8")])
        engine.step(now="2026-03-09T13:36:14Z")
        assert engine.scheduler.get(SID_A).state == StrategyState.ANALYSIS_DUE
        assert len(_events(engine, "paper_close")) == 2
        assert engine.book.get_account(SID_A).open_trade is None

        # Step 8: A's recall -> call4 no_entry.
        engine.step(now="2026-03-09T13:36:19Z")
        assert engine.scheduler.get(SID_A).state == StrategyState.SLEEPING
        assert _calls_for(adapter, SID_A) == 4

        # G024 exact balances: A lost (100-99) * 10 shares; B won (102-100.5)
        # * floor(1000/100.5) shares rounded to cents half-up.
        assert engine.book.get_account(SID_A).balance == Decimal("990.00")
        assert engine.book.get_account(SID_B).balance == Decimal("1014.93")

        # Adapter call counts and monitor churn.
        assert adapter.call_count == 6
        assert len(engine._pending_monitors) == 0
        assert len(engine._exit_monitors) == 0

        # Persisted event vocabulary.
        types = {event["event_type"] for event in engine._store.list_events()}
        assert {
            "model_decision_accepted",
            "schedule_set",
            "strategy_state_changed",
            "pending_entry_created",
            "pending_entry_expired",
            "paper_fill",
            "paper_close",
            "account_changed",
        } <= types
        for event in engine._store.list_events():
            if event["event_type"] in ("paper_fill", "paper_close"):
                assert event["envelope"]["market_ref"]["kind"] == "second_event"
        engine.stop()

    def test_restart_mid_pending_preserves_episode_and_fills_once(self, tmp_path):
        clock1 = FakeClock("2026-03-09T13:34:03Z")
        history = B.history_result()
        s1 = _sec("2026-03-09T13:33:59Z", "2026-03-09T13:34:00Z", "100.5")
        engine1 = _engine(tmp_path, clock=clock1, run_id="run_slice_restart_a", history=history, aggregator=B.FakeAggregator(batches=[[s1]]), db_name="restart.db")
        _attach(
            engine1,
            {
                SID_A: [
                    _no_entry(engine1, SID_A, 57),
                    _wait(engine1, SID_A, entry="100", stop="99", target="102", x=60),
                    _wait(engine1, SID_A, entry="100", stop="99", target="102", x=3600),
                ],
                SID_B: [_immediate(engine1, SID_B, stop="99.5", target="102")],
            },
        )
        engine1.start()
        engine1.step(now="2026-03-09T13:34:03Z")  # A no_entry; B immediate
        engine1.step(now="2026-03-09T13:35:00Z")  # A call2 wait X=60
        engine1._aggregator._batches.append(
            [
                _sec("2026-03-09T13:34:05Z", "2026-03-09T13:34:06Z", "100.2", high="100.5", low="100.05"),
                _sec("2026-03-09T13:35:05Z", "2026-03-09T13:35:06Z", "100.2", high="100.5", low="100.05"),
            ]
        )
        engine1.step(now="2026-03-09T13:35:09Z")
        engine1._aggregator._batches.append([_sec("2026-03-09T13:35:59Z", "2026-03-09T13:36:00Z", "102", high="102.2", low="101")])
        engine1.step(now="2026-03-09T13:36:03Z")  # A X-expiry recall; B TP
        engine1.step(now="2026-03-09T13:36:00Z")  # A call3 wait X=3600 at grid boundary
        pending_id = engine1._pending_monitors[SID_A].pending_id
        decisions_before = len(_events(engine1, "model_decision_accepted"))
        fills_before = len(_events(engine1, "paper_fill"))
        balance_b = engine1.book.get_account(SID_B).balance
        assert balance_b == Decimal("1014.93")
        engine1.stop()

        # Restart mid-pending: the recovery bar (starting exactly at the
        # call3 decision boundary) touches the entry and fills the restored
        # episode exactly once; no decision is duplicated.
        clock2 = FakeClock("2026-03-09T13:37:00Z")
        engine2 = _engine(tmp_path, clock=clock2, run_id="run_slice_restart_b", history=history, aggregator=B.FakeAggregator(), db_name="restart.db")
        engine2._history_adapter.recovery_result = B.recovery_result(
            start_at="2026-03-09T13:36:00Z",
            end_at="2026-03-09T13:37:00Z",
            bars=[B.recovery_bar(start_at="2026-03-09T13:36:00Z", end_at="2026-03-09T13:37:00Z", low="99.95", high="100.05")],
        )
        report = engine2.start()
        assert report["errors"] == []
        assert report["restored_pending"] == 1
        assert engine2._store.get_pending_entry(pending_id)["status"] == "filled"
        assert engine2.book.get_account(SID_B).balance == balance_b
        assert engine2.book.get_account(SID_B).open_trade is None
        assert engine2.scheduler.get(SID_A).state in (StrategyState.OPEN_POSITION, StrategyState.MARKET_WAIT)
        assert engine2.book.get_account(SID_A).open_trade is not None
        assert len(_events(engine2, "model_decision_accepted")) == decisions_before
        fills_after = len(_events(engine2, "paper_fill"))
        assert fills_after == fills_before + 1  # exactly one restored fill
        restored_fill = [event for event in _events(engine2, "paper_fill") if event["envelope"]["payload"]["trade_id"] == engine2.book.get_account(SID_A).open_trade.trade_id][-1]
        assert restored_fill["envelope"]["market_ref"]["kind"] == "recovery_bar"
        assert restored_fill["envelope"]["market_ref"]["path_precision"] == "minute"

        # A post-recovery stop-touching second settles the restored position.
        engine2._aggregator._batches.append([_sec("2026-03-09T13:37:05Z", "2026-03-09T13:37:06Z", "99", high="100.5", low="98.7")])
        engine2.step(now="2026-03-09T13:37:09Z")
        assert engine2.scheduler.get(SID_A).state == StrategyState.ANALYSIS_DUE
        assert engine2.book.get_account(SID_A).balance == Decimal("990.00")
        assert len(_events(engine2, "paper_close")) == 2  # B TP + A stop
        engine2.stop()
