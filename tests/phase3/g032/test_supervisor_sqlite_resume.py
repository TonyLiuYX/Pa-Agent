from __future__ import annotations

import importlib.util
import time
from pathlib import Path
from typing import Any

import pytest

from pa_engine.runtime.config import RuntimeConfig
from pa_engine.runtime.engine import AutomaticPaperRuntime
from pa_engine.storage import RuntimeStore, open_database
from pa_engine.strategy.runtime import FakeClock
from pa_engine.strategies.knowledge import build_registry
from pa_engine.web.config import StartRequest, WebConfig
from pa_engine.web.supervisor import RuntimeSupervisor, SupervisorError

from .conftest import configured_workspace


ROOT = Path(__file__).resolve().parents[3]
FIXTURE_BUILDERS = ROOT / "tests" / "fixtures" / "phase2" / "g028" / "engine" / "builders.py"
_spec = importlib.util.spec_from_file_location("g032_g028_builders", FIXTURE_BUILDERS)
assert _spec is not None and _spec.loader is not None
B = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(B)

SID_A = "pa:nanqiao-range-extreme-limit"
SID_B = "pa:nanqiao-always-in-ema20"
SID_C = "pa:nanqiao-always-in-breakout-scalp"


def _registry() -> Any:
    return build_registry(
        str(ROOT / "knowledge"),
        registrations=[
            {"strategy_id": SID_A, "record_id": "nanqiao-range-extreme-limit"},
            {"strategy_id": SID_B, "record_id": "nanqiao-always-in-ema20"},
            {"strategy_id": SID_C, "record_id": "nanqiao-always-in-breakout-scalp"},
        ],
    )


def _request(*, symbol: str = "AAPL", timeframe: str = "1min") -> StartRequest:
    return StartRequest.from_payload(
        {
            "symbol": symbol,
            "timeframe": timeframe,
            "execution_policy": "paper",
        }
    )


def _wait_for_active(supervisor: RuntimeSupervisor) -> None:
    deadline = time.monotonic() + 4
    while time.monotonic() < deadline:
        if supervisor.lifecycle in {"running", "waiting_market", "recovery", "model_error"}:
            return
        time.sleep(0.01)
    raise AssertionError(f"supervisor did not publish an active lifecycle: {supervisor.lifecycle}")


def _db_projection(path: Path) -> dict[str, Any]:
    db = open_database(str(path), run_id="g032-sqlite-inspect", mode="automatic")
    try:
        store = RuntimeStore(db)
        accounts = [
            {key: row.get(key) for key in ("strategy_id", "balance", "initial_balance", "exhausted", "pending", "open_trade", "pending_trade")}
            for row in store.list_accounts()
        ]
        pending = [
            {key: row.get(key) for key in ("pending_id", "strategy_id", "generation", "status", "symbol", "direction", "entry_price", "stop_price", "target_price", "expires_at", "resolution_at", "episode_id")}
            for row in store.list_pending_entries(status="pending")
        ]
        x_timers = [
            {key: row.get(key) for key in ("pending_id", "strategy_id", "expires_at", "resolution_at")}
            for row in store.list_pending_entries(status="pending")
        ]
        open_trades = [
            {key: row.get(key) for key in ("trade_id", "strategy_id", "status", "symbol", "direction", "quantity", "entry_price", "stop_price", "target_price", "planned_r", "settlement_json", "exit_reason")}
            for row in store.list_trades(status="open")
        ]
        cursors = [
            {key: row.get(key) for key in ("symbol", "feed_key", "last_second_event_id", "last_second_event_hash", "last_second_end_at", "recovery_cursor")}
            for row in store.list_cursors()
        ]
        cursor_recovery_gate = [
            {key: row.get(key) for key in ("symbol", "live_blocked", "pending_recovery_json")}
            for row in store.list_cursors()
        ]
        states = [
            {key: row.get(key) for key in ("strategy_id", "state", "generation", "pending_id", "open_position_id", "next_due_at", "expires_at")}
            for row in store.list_strategy_states()
        ]
        timers = [
            {key: timer.get(key) for key in ("strategy_id", "kind", "generation", "due_at", "active", "fired")}
            for row in store.list_strategy_states()
            for timer in store.get_timers(row["strategy_id"])
            if timer.get("active") and not timer.get("fired")
        ]
        events = [
            {key: row.get(key) for key in ("event_id", "event_hash", "event_type", "strategy_id")}
            for row in store.list_events()
            if row.get("event_type") in {"paper_fill", "paper_close"}
        ]
        return {"accounts": accounts, "pending": pending, "x_timers": x_timers, "open_trades": open_trades, "cursors": cursors, "cursor_recovery_gate": cursor_recovery_gate, "states": states, "timers": timers, "fill_close_events": events}
    finally:
        db.close()


class _RealFixtureRuntime:
    """Small supervisor seam around the real runtime and real SQLite store."""

    constructions = 0
    adapters: list[Any] = []

    def __init__(self, config: RuntimeConfig, request: StartRequest, web_config: WebConfig) -> None:
        self.index = self.__class__.constructions
        self.__class__.constructions += 1
        self.registry = _registry()
        self.clock = FakeClock("2026-03-09T13:34:03Z")
        second = B.frozen_second(
            start_at="2026-03-09T13:33:59Z",
            end_at="2026-03-09T13:34:00Z",
            close="100.5",
        )
        self.aggregator = B.FakeAggregator(batches=[[second], []] if self.index == 0 else [])
        self.history = B.FakeHistoryAdapter(history_result=B.history_result())
        self.adapter = B.ScriptedAdapter({})
        self.engine = AutomaticPaperRuntime(
            config,
            registry=self.registry,
            adapter=self.adapter,
            history_adapter=self.history,
            aggregator=self.aggregator,
            db_path=web_config.database_path,
            run_id=f"g032-sqlite-runtime-{self.index}",
            clock=self.clock,
        )
        self.__class__.adapters.append(self.adapter)
        if self.index == 0:
            self.adapter._by_strategy = {
                SID_A: [self._no_entry, self._wait_for_price],
                SID_B: [self._immediate],
                SID_C: [self._sleep],
            }

    def _no_entry(self, request: dict[str, Any], generation: int | None = None) -> Any:
        loaded = self.registry.load_packet(SID_A)
        candidate = B.grounded_candidate(self.registry, SID_A, self.engine._current_view, branch="no_entry", direction="long", n=57)
        return B.success_outcome(request, candidate, profile_ref=loaded.profile_ref, generation=generation)

    def _wait_for_price(self, request: dict[str, Any], generation: int | None = None) -> Any:
        loaded = self.registry.load_packet(SID_A)
        candidate = B.grounded_candidate(
            self.registry,
            SID_A,
            self.engine._current_view,
            branch="wait_for_price",
            direction="long",
            entry="100",
            stop="99",
            target="102",
            x=60,
        )
        return B.success_outcome(request, candidate, profile_ref=loaded.profile_ref, generation=generation)

    def _immediate(self, request: dict[str, Any], generation: int | None = None) -> Any:
        loaded = self.registry.load_packet(SID_B)
        candidate = B.grounded_candidate(
            self.registry,
            SID_B,
            self.engine._current_view,
            branch="enter_immediately",
            direction="long",
            stop="99.5",
            target="102",
            second_event_ref=B.second_ref_of(self.engine._current_view),
        )
        return B.success_outcome(request, candidate, profile_ref=loaded.profile_ref, generation=generation)

    def _sleep(self, request: dict[str, Any], generation: int | None = None) -> Any:
        loaded = self.registry.load_packet(SID_C)
        candidate = B.grounded_candidate(
            self.registry,
            SID_C,
            self.engine._current_view,
            branch="no_entry",
            direction="long",
            n=300,
        )
        return B.success_outcome(request, candidate, profile_ref=loaded.profile_ref, generation=generation)

    def start(self) -> dict[str, Any]:
        report = self.engine.start()
        if self.index == 0:
            # This is deliberately performed inside the supervisor owner
            # command.  It creates a durable N sleep -> X pending + open
            # position/cursor seam without using a fake in-memory runtime.
            self.engine.step(now="2026-03-09T13:34:03Z")
            self.engine.step(now="2026-03-09T13:35:00Z")
        return report

    def step(self) -> Any:
        return self.engine.step()

    def stop(self, *, reason: str = "stopped") -> Any:
        return self.engine.stop(reason=reason)

    def status(self) -> dict[str, Any]:
        return self.engine.status()

    def __getattr__(self, name: str) -> Any:
        return getattr(self.engine, name)


def test_web_supervisor_same_sqlite_stop_resume_preserves_runtime_state_without_replay(tmp_path: Path):
    _RealFixtureRuntime.constructions = 0
    _RealFixtureRuntime.adapters = []
    database = tmp_path / "same-runtime.db"
    config = WebConfig(database_path=str(database), workspace_path=str(tmp_path / "workspace"), step_interval_seconds=1_000_000_000.0)
    workspace = configured_workspace(config, registry=_registry())
    supervisor = RuntimeSupervisor(config, workspace=workspace, runtime_factory=lambda runtime_config, request, web_config: _RealFixtureRuntime(runtime_config, request, web_config))
    try:
        supervisor.request_start(_request())
        _wait_for_active(supervisor)
        first_snapshot = supervisor.snapshot().to_dict()
        assert first_snapshot["run"]["provider_calls"] == 4
        assert first_snapshot["strategies"]
        assert any(item.get("pending") for item in first_snapshot["strategies"])
        assert any(item.get("open_trade") for item in first_snapshot["strategies"])

        assert supervisor.request_stop()["lifecycle"] == "stopped"
        before = _db_projection(database)
        assert len(before["accounts"]) == 3
        assert len(before["pending"]) == 1
        assert len(before["open_trades"]) == 1
        assert len(before["cursors"]) == 1
        assert any(row["expires_at"] for row in before["states"])
        assert any(row["kind"] == "sleep" for row in before["timers"])
        assert before["x_timers"] and all(row["expires_at"] and row["resolution_at"] for row in before["x_timers"])
        assert before["fill_close_events"] and all(row["event_type"] == "paper_fill" for row in before["fill_close_events"])

        # Pending/open/X state is non-terminal: a different timeframe or
        # symbol is rejected at the web preflight seam before a new real
        # AutomaticPaperRuntime/adapters/network object can be constructed.
        conflicting_timeframe = _request(timeframe="5min")
        with pytest.raises(SupervisorError, match="symbol/timeframe"):
            supervisor._preflight_database_state(workspace.prepare_launch(conflicting_timeframe))
        conflicting_symbol = _request(symbol="TSLA")
        with pytest.raises(SupervisorError, match="another symbol"):
            supervisor._preflight_database_state(workspace.prepare_launch(conflicting_symbol))
        assert _RealFixtureRuntime.constructions == 1

        supervisor.request_start(_request())
        _wait_for_active(supervisor)
        assert supervisor.lifecycle == "recovery"
        assert supervisor.request_stop()["lifecycle"] == "stopped"
        after = _db_projection(database)

        assert after["accounts"] == before["accounts"]
        assert after["pending"] == before["pending"]
        assert after["x_timers"] == before["x_timers"]
        assert after["open_trades"] == before["open_trades"]
        assert after["cursors"] == before["cursors"]
        assert after["cursor_recovery_gate"] != before["cursor_recovery_gate"]
        assert after["states"] == before["states"]
        assert after["timers"] == before["timers"]
        assert after["fill_close_events"] == before["fill_close_events"]
        assert _RealFixtureRuntime.adapters[0].call_count == 4
        assert _RealFixtureRuntime.adapters[1].call_count == 0
    finally:
        supervisor.close()
