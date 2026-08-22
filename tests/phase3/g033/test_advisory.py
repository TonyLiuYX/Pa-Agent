from __future__ import annotations

import importlib.util
from datetime import datetime, timezone
from pathlib import Path

from pa_engine.strategy.runtime import FakeClock, StrategyScheduler, StrategyState


ROOT = Path(__file__).resolve().parents[3]
ENGINE_TEST = ROOT / "tests" / "phase2" / "g028" / "test_engine.py"
_spec = importlib.util.spec_from_file_location("g033_g028_engine", ENGINE_TEST)
assert _spec is not None and _spec.loader is not None
E = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(E)

START = datetime(2026, 1, 2, tzinfo=timezone.utc)


def _view(view_id: str, snapshot_id: str) -> dict:
    return {
        "shared_market_view_id": view_id,
        "closed_snapshot_id": snapshot_id,
        "regular_session": True,
        "fresh": True,
        "complete": True,
        "non_gapped": True,
        "sufficient": True,
        "frozen_second": True,
    }


def _instruction(snapshot_id: str) -> dict:
    return {
        "instruction_hash": "a" * 64,
        "strategy_id": "pa:test",
        "branch": "enter_immediately",
        "reason": "A valid trigger was accepted for advisory display only.",
        "direction": "long",
        "entry_price": "100",
        "stop_price": "99",
        "target_price": "102",
        "market_view_ref": {"market_snapshot_id": snapshot_id},
    }


def test_advisory_schedules_exactly_once_per_closed_snapshot_and_never_opens(tmp_path: Path):
    opened: list[object] = []
    scheduler = StrategyScheduler(
        ["pa:test"],
        clock=FakeClock(START),
        execution_policy="advisory",
        open_requester=lambda *args, **kwargs: opened.append((args, kwargs)),
    )
    scheduler._verify_success_instruction = lambda _record, _instruction: True
    scheduler.start(_view("view-1", "snapshot-1"))
    first = scheduler.claim_jobs()
    assert len(first) == 1
    assert scheduler.complete_job(first[0], instruction=_instruction("snapshot-1"), decision_accepted_at=START)

    record = scheduler.get("pa:test")
    assert record is not None
    assert record.state == StrategyState.ANALYSIS_DUE
    assert record.pending_id is None and record.open_position_id is None
    assert record.timer_tokens == {}
    assert scheduler.queue_size == 0
    assert opened == []

    scheduler.on_market_view(_view("view-1-repeat", "snapshot-1"))
    assert scheduler.queue_size == 0
    scheduler.on_market_view(_view("view-2", "snapshot-2"))
    second = scheduler.claim_jobs()
    assert len(second) == 1
    assert scheduler.complete_job(second[0], instruction=_instruction("snapshot-2"), decision_accepted_at=START)
    assert scheduler.queue_size == 0
    assert opened == []


def test_advisory_runtime_persists_decisions_without_pending_fills_positions_or_money_changes(tmp_path: Path):
    first_second = E.sec("2026-03-09T13:33:59Z", "2026-03-09T13:34:00Z", "10.05")
    engine = E.make_engine(
        tmp_path,
        clock=FakeClock("2026-03-09T13:34:03Z"),
        aggregator=E.B.FakeAggregator(batches=[[first_second], []]),
        config_overrides={"execution_policy": "advisory"},
    )
    adapter = E.attach_script(
        engine,
        {
            E.SID_A: [E.immediate_outcome(engine, E.SID_A), E.immediate_outcome(engine, E.SID_A)],
            E.SID_B: [E.wait_outcome(engine, E.SID_B), E.wait_outcome(engine, E.SID_B)],
        },
    )
    engine.start()
    engine.step(now="2026-03-09T13:34:03Z")
    assert adapter.call_count == 2
    assert len(E.events_of(engine, "model_decision_accepted")) == 2

    engine.step(now="2026-03-09T13:34:04Z")
    assert adapter.call_count == 2

    next_close = E.sec("2026-03-09T13:34:59Z", "2026-03-09T13:35:00Z", "10.15")
    engine._aggregator._batches.append([next_close])
    engine.step(now="2026-03-09T13:35:03Z")
    assert adapter.call_count == 4
    assert len(E.events_of(engine, "model_decision_accepted")) == 4

    assert engine._store.list_pending_entries(status="pending") == []
    assert engine._store.list_trades(status="open") == []
    assert E.events_of(engine, "paper_fill") == []
    assert E.events_of(engine, "paper_close") == []
    assert E.events_of(engine, "account_changed") == []
    assert {row["balance"] for row in engine._store.list_accounts()} == {"1000.00"}
    assert all(engine.scheduler.get(sid).state == StrategyState.ANALYSIS_DUE for sid in (E.SID_A, E.SID_B))
    engine.stop()
