from __future__ import annotations

import json
import threading
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping

from pa_engine.runtime.config import RuntimeConfig
from pa_engine.web.config import CURATED_SYMBOLS, WebConfig
from pa_engine.web.workspace import ProductWorkspace


FIXTURE_ROOT = Path(__file__).resolve().parents[2] / "fixtures" / "phase3" / "g032"


class FixtureAssetValidator:
    """Offline Alpaca Assets seam used by the web regression suite."""

    def validate_universe(self, key_id: str, secret_key: str) -> list[dict[str, Any]]:
        assert key_id and secret_key
        return [
            {
                "symbol": symbol,
                "asset_class": "us_equity",
                "status": "active",
                "tradable": True,
                "exchange": "NASDAQ",
            }
            for symbol in CURATED_SYMBOLS
        ]


def configured_workspace(
    config: WebConfig,
    *,
    registry: Any | None = None,
    model_key: str = "G032_MODEL_CANARY_7f3c",
    alpaca_key_id: str = "G032_ALPACA_ID_CANARY_9a1d",
    alpaca_secret_key: str = "G032_ALPACA_SECRET_CANARY_4b8e",
) -> ProductWorkspace:
    workspace = ProductWorkspace(
        config,
        built_in_registry=registry,
        asset_validator=FixtureAssetValidator(),
    )
    workspace.vault.set_alpaca(alpaca_key_id, alpaca_secret_key)
    workspace.vault.set_model("deepseek", model_key)
    return workspace


def fixture_document() -> dict[str, Any]:
    return json.loads((FIXTURE_ROOT / "presentation_fixture.json").read_text(encoding="utf-8"))


def _envelope(event_type: str, strategy_id: str | None, payload: Mapping[str, Any]) -> dict[str, Any]:
    return {"event_type": event_type, "strategy_id": strategy_id, "payload": dict(payload)}


class FixtureStore:
    def __init__(self) -> None:
        self.accounts = [
            {"strategy_id": "pa:due", "balance": "1000.00", "exhausted": False},
            {"strategy_id": "pa:inflight", "balance": "1000.00", "exhausted": False},
            {"strategy_id": "pa:sleep", "balance": "1000.00", "exhausted": False},
            {"strategy_id": "pa:pending", "balance": "1000.00", "exhausted": False},
            {"strategy_id": "pa:open", "balance": "1000.00", "exhausted": False},
            {"strategy_id": "pa:tp", "balance": "1014.93", "exhausted": False},
            {"strategy_id": "pa:sl", "balance": "990.00", "exhausted": False},
            {"strategy_id": "pa:market", "balance": "1000.00", "exhausted": False},
            {"strategy_id": "pa:blocked", "balance": "1000.00", "exhausted": False},
        ]
        self.states = [
            {"strategy_id": "pa:due", "state": "analysis_due", "enabled": True, "exhausted": False, "next_due_at": None, "expires_at": None, "latest_instruction": None, "blocked_reason": None},
            {"strategy_id": "pa:inflight", "state": "analyzing", "enabled": True, "exhausted": False, "next_due_at": None, "expires_at": None, "latest_instruction": {"branch": "analysis", "reason": "Checking the latest closed evidence."}, "blocked_reason": None},
            {"strategy_id": "pa:sleep", "state": "sleeping", "enabled": True, "exhausted": False, "next_due_at": "2026-03-09T14:35:00Z", "expires_at": None, "latest_instruction": {"branch": "no_entry", "reason": "No clean setup; wait for the next scheduled analysis.", "next_call_after_seconds": 60}, "blocked_reason": None},
            {"strategy_id": "pa:pending", "state": "waiting_entry", "enabled": True, "exhausted": False, "next_due_at": None, "expires_at": "2026-03-09T14:36:00Z", "latest_instruction": {"branch": "wait_for_price", "reason": "Breakout trigger is valid; wait at the exact level.", "entry_price": "109.00", "stop_price": "108.00", "target_price": "111.00", "entry_wait_seconds": 60}, "blocked_reason": None},
            {"strategy_id": "pa:open", "state": "open_position", "enabled": True, "exhausted": False, "next_due_at": None, "expires_at": None, "latest_instruction": {"branch": "enter_immediately", "reason": "Immediate long trigger accepted."}, "blocked_reason": None},
            {"strategy_id": "pa:tp", "state": "sleeping", "enabled": True, "exhausted": False, "next_due_at": "2026-03-09T14:40:00Z", "expires_at": None, "latest_instruction": {"branch": "enter_immediately", "reason": "Target reached; scheduling a fresh analysis."}, "blocked_reason": None},
            {"strategy_id": "pa:sl", "state": "sleeping", "enabled": True, "exhausted": False, "next_due_at": "2026-03-09T14:40:00Z", "expires_at": None, "latest_instruction": {"branch": "enter_immediately", "reason": "Stop settled; scheduling a fresh analysis."}, "blocked_reason": None},
            {"strategy_id": "pa:market", "state": "market_wait", "enabled": True, "exhausted": False, "next_due_at": None, "expires_at": None, "latest_instruction": None, "blocked_reason": "market view is partial"},
            {"strategy_id": "pa:blocked", "state": "blocked", "enabled": True, "exhausted": False, "next_due_at": None, "expires_at": None, "latest_instruction": None, "blocked_reason": "provider authentication failed"},
        ]
        self.timers = {"pa:sleep": [{"kind": "analysis", "due_at": "2026-03-09T14:35:00Z", "generation": 1, "active": True}]}
        self.pending = [{"pending_id": "pending_fixture", "strategy_id": "pa:pending", "status": "pending", "symbol": "AAPL", "direction": "long", "entry_price": "109.00", "stop_price": "108.00", "target_price": "111.00", "expires_at": "2026-03-09T14:36:00Z"}]
        self.trades = [
            {"trade_id": "open_fixture", "strategy_id": "pa:open", "status": "open", "symbol": "AAPL", "direction": "long", "quantity": "9.09090909", "entry_price": "109.00", "stop_price": "108.00", "target_price": "111.00", "opened_at": "2026-03-09T14:30:00Z", "closed_at": None, "exit_reason": None, "settlement_json": None},
            {"trade_id": "tp_fixture", "strategy_id": "pa:tp", "status": "settled", "symbol": "AAPL", "direction": "long", "quantity": "9.90099009", "entry_price": "101.00", "stop_price": "100.00", "target_price": "102.50", "opened_at": "2026-03-09T14:00:00Z", "closed_at": "2026-03-09T14:10:00Z", "exit_reason": "take_profit", "settlement_json": {"gross_pnl": "14.851485135", "applied_pnl": "14.93", "net_pnl": "14.93"}},
            {"trade_id": "sl_fixture", "strategy_id": "pa:sl", "status": "settled", "symbol": "AAPL", "direction": "short", "quantity": "10.00", "entry_price": "100.00", "stop_price": "101.00", "target_price": "98.00", "opened_at": "2026-03-09T14:00:00Z", "closed_at": "2026-03-09T14:12:00Z", "exit_reason": "stop_loss", "settlement_json": {"gross_pnl": "-10.00", "applied_pnl": "-10.00", "net_pnl": "-10.00"}},
        ]
        self.events = [
            {"event_id": "decision_fixture", "event_type": "model_decision_accepted", "occurred_at": "2026-03-09T14:29:00Z", "strategy_id": "pa:pending", "envelope": _envelope("model_decision_accepted", "pa:pending", {"branch": "wait_for_price", "instruction_hash": "fixture-hash", "provider_raw_payload": "MUST_NOT_APPEAR"})},
            {"event_id": "tp_close_fixture", "event_type": "paper_close", "occurred_at": "2026-03-09T14:10:00Z", "strategy_id": "pa:tp", "envelope": _envelope("paper_close", "pa:tp", {"exit_reason": "take_profit", "settlement": {"applied_pnl": "14.93"}, "trade_json": "MUST_NOT_APPEAR"})},
            {"event_id": "blocked_fixture", "event_type": "strategy_state_changed", "occurred_at": "2026-03-09T14:20:00Z", "strategy_id": "pa:blocked", "envelope": _envelope("strategy_state_changed", "pa:blocked", {"to": "blocked", "reason": "provider authentication failed"})},
        ]

    def list_accounts(self): return list(self.accounts)
    def list_strategy_states(self): return list(self.states)
    def get_timers(self, strategy_id): return list(self.timers.get(strategy_id, []))
    def list_pending_entries(self, **kwargs): return list(self.pending)
    def list_trades(self, **kwargs): return list(self.trades)
    def list_events(self, **kwargs): return list(self.events)


class FixtureRuntime:
    def __init__(self, config: RuntimeConfig, request: Any | None = None, web_config: Any | None = None) -> None:
        document = fixture_document()
        self.config = config
        self.started = False
        self.stopped = False
        self.calls = 0
        self._store = FixtureStore()
        self._current_view = SimpleNamespace(
            closed_snapshot={"bars": document["closed_bars"]},
            forming_candle=document["forming"],
            latest_second=document["latest_second"],
            current_price="109.90",
            readiness=document["readiness"],
        )
        self.feed = SimpleNamespace(status=lambda: {"stream": {"started": True, "consumer_alive": True, "transport_ready": True, "live_dispatch_allowed": True, "gap_open": False, "status": {"state": "ready", "outcome": {"kind": "ready", "ok": True}}}})
        records = {}
        for row in self._store.states:
            record = SimpleNamespace(
                strategy_id=row["strategy_id"],
                state=row["state"],
                enabled=row["enabled"],
                exhausted=row["exhausted"],
                packet={"primary_record": {"name": row["strategy_id"].removeprefix("pa:").replace("_", " ")}},
                latest_instruction=row["latest_instruction"],
                blocked_reason=row["blocked_reason"],
                timer_tokens={},
                next_due_at=None,
                expires_at=None,
                transition_log=[],
            )
            records[row["strategy_id"]] = record
        self.scheduler = SimpleNamespace(records=records)

    def start(self):
        self.start_thread = threading.get_ident()
        self.started = True
        return {"started": True, "errors": []}

    def step(self):
        self.step_thread = threading.get_ident()
        self.calls += 1
        return {"step_index": self.calls}

    def stop(self, *, reason="stopped"):
        self.stop_thread = threading.get_ident()
        self.stopped = True
        self.started = False
        return {"stopped": True, "reason": reason}

    def status(self):
        return {"run_id": "fixture-runtime", "started": self.started, "stopped": self.stopped, "provider_calls": self.calls, "provider_in_flight": 0, "recovery_pending": False, "errors": []}


def fixture_runtime_factory(config, request, web_config):
    return FixtureRuntime(config, request, web_config)
