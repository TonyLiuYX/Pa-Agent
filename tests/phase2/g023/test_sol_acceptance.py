"""Sol adversarial acceptance cases for G023.

These tests exercise contract and race paths that are easy to miss in the
worker's focused happy-path suite.  They intentionally use the public runtime
API and keep G024-G026 behind injected fakes.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pa_engine.strategies.knowledge import load_default_registry
from pa_engine.strategy.decision import make_scheduler_instruction
from pa_engine.strategy.runtime import (
    FakeClock,
    OpenRequestResult,
    StrategyScheduler,
    StrategySpec,
    StrategyState,
)


ROOT = Path(__file__).resolve().parents[3]
START = datetime(2026, 1, 2, tzinfo=timezone.utc)


def _view(view_id: str, snapshot_id: str, *, ready: bool = True) -> dict:
    return {
        "shared_market_view_id": view_id,
        "closed_snapshot_id": snapshot_id,
        "regular_session": ready,
        "fresh": ready,
        "complete": ready,
        "non_gapped": ready,
        "sufficient": ready,
        "frozen_second": ready,
    }


def _scheduler(*, view: dict | None = None, **kwargs) -> StrategyScheduler:
    scheduler = StrategyScheduler(["pa:test"], clock=FakeClock(START), **kwargs)
    scheduler._verify_success_instruction = lambda _record, _instruction: True
    scheduler.start(view or _view("view-1", "snapshot-1"))
    return scheduler


def _instruction(branch: str = "no_entry", *, n: int = 5, x: int = 7) -> dict:
    if branch == "no_entry":
        return {"branch": branch, "next_call_after_seconds": n}
    if branch == "wait_for_price":
        return {
            "branch": branch,
            "entry_wait_seconds": x,
            "entry_price": "100",
            "stop_price": "99",
            "target_price": "102",
            "direction": "long",
        }
    return {
        "branch": branch,
        "entry_price": "100",
        "stop_price": "99",
        "target_price": "102",
        "direction": "long",
    }


@pytest.mark.parametrize("fixture_name", ["no_entry.json", "immediate_long.json", "wait_long.json"])
def test_real_g018_instruction_is_authoritatively_verified_and_bound(fixture_name):
    registry = load_default_registry(ROOT / "knowledge")
    loaded = registry.load_packet("pa:nanqiao-range-extreme-limit")
    assert loaded.ok
    candidate = json.loads((ROOT / "tests/fixtures/phase2/g018/valid" / fixture_name).read_text())
    instruction = make_scheduler_instruction(
        candidate,
        profile_ref=loaded.profile_ref,
        result={"pa_analysis_result_id": "par_" + "c" * 64, "content_hash": "c" * 64},
        evidence_package=loaded.evidence_package,
    )
    market_ref = candidate["shared_market_view_ref"]
    view = {
        **_view(market_ref["shared_market_view_id"], market_ref["market_snapshot_id"]),
        "market_view_ref": market_ref,
    }
    scheduler = StrategyScheduler(
        [StrategySpec("pa:nanqiao-range-extreme-limit", loaded.profile_ref, loaded.packet)],
        clock=FakeClock(START),
        registry=registry,
        open_requester=lambda *_args, **_kwargs: OpenRequestResult.accepted_result("pos-real"),
    )
    scheduler.start(view)
    job = scheduler.claim_jobs()[0]
    assert scheduler.complete_job(job, instruction, decision_accepted_at=START)
    expected = {
        "no_entry.json": StrategyState.SLEEPING,
        "immediate_long.json": StrategyState.OPEN_POSITION,
        "wait_long.json": StrategyState.WAITING_ENTRY,
    }[fixture_name]
    assert scheduler.state_for(job.strategy_id) == expected


def test_immediate_entry_without_open_boundary_never_fabricates_a_position():
    scheduler = _scheduler()
    assert scheduler.submit_instruction("pa:test", _instruction("enter_immediately"), decision_accepted_at=START)
    record = scheduler.get("pa:test")
    assert record is not None
    assert record.open_position_id is None
    assert record.state != StrategyState.OPEN_POSITION


@pytest.mark.parametrize("starting_state", [StrategyState.WAITING_ENTRY, StrategyState.OPEN_POSITION])
def test_shared_market_failure_retains_waiting_and_open_resume_state(starting_state):
    scheduler = _scheduler(open_requester=lambda *_args, **_kwargs: OpenRequestResult.accepted_result("pos-1"))
    branch = "wait_for_price" if starting_state == StrategyState.WAITING_ENTRY else "enter_immediately"
    assert scheduler.submit_instruction("pa:test", _instruction(branch), decision_accepted_at=START)
    scheduler.on_market_view(_view("bad-view", "bad-snapshot", ready=False))
    record = scheduler.get("pa:test")
    assert record is not None
    assert record.state == StrategyState.MARKET_WAIT
    assert record.intended_resume_state == starting_state
    assert scheduler.queue_size == 0
    scheduler.on_market_view(_view("recovered-view", "recovered-snapshot"))
    assert record.state == starting_state
    assert scheduler.queue_size == 0


def test_market_quality_failure_waits_for_a_new_recovered_view_without_spin():
    scheduler = _scheduler()
    job = scheduler.claim_jobs()[0]
    assert not scheduler.complete_job(job, {"classification": "stale_market"})
    record = scheduler.get("pa:test")
    assert record is not None
    assert record.state == StrategyState.MARKET_WAIT
    assert record.intended_resume_state == StrategyState.ANALYSIS_DUE
    assert scheduler.queue_size == 0
    scheduler.on_market_view(_view("view-1", "snapshot-1"))
    assert record.state == StrategyState.MARKET_WAIT
    assert record.intended_resume_state == StrategyState.ANALYSIS_DUE
    assert scheduler.queue_size == 0
    scheduler.on_market_view(_view("view-2", "snapshot-2"))
    assert scheduler.queue_size == 1


def test_semantic_block_recovers_when_changed_snapshot_later_becomes_ready():
    scheduler = _scheduler()
    for delay in (5, 10, 20):
        job = scheduler.claim_jobs()[0]
        assert not scheduler.complete_job(job, {"classification": "malformed_response"})
        scheduler.advance(delay)
    job = scheduler.claim_jobs()[0]
    assert not scheduler.complete_job(job, {"classification": "grounding_failure"})
    assert scheduler.state_for("pa:test") == StrategyState.BLOCKED
    scheduler.on_market_view(_view("view-2-bad", "snapshot-2", ready=False))
    assert scheduler.state_for("pa:test") == StrategyState.BLOCKED
    scheduler.on_market_view(_view("view-2-good", "snapshot-2"))
    assert scheduler.queue_size == 1
    assert scheduler.get("pa:test").semantic_retry_count == 0


def test_disable_keeps_existing_trade_closeable_by_its_original_generation():
    scheduler = _scheduler(open_requester=lambda *_args, **_kwargs: OpenRequestResult.accepted_result("pos-1"))
    assert scheduler.submit_instruction("pa:test", _instruction("enter_immediately"), decision_accepted_at=START)
    trade_generation = scheduler.get("pa:test").generation
    assert scheduler.disable("pa:test")
    assert scheduler.on_position_close("pa:test", "pos-1", generation=trade_generation)
    record = scheduler.get("pa:test")
    assert record.state == StrategyState.DISABLED
    assert record.open_position_id is None
    assert scheduler.queue_size == 0


@pytest.mark.parametrize("state_name", ["analyzing", "sleeping", "retry_wait", "market_wait"])
def test_disable_cancels_all_non_trade_work_and_invalidates_late_completion(state_name):
    scheduler = _scheduler()
    claimed = None
    if state_name == "analyzing":
        claimed = scheduler.claim_jobs()[0]
    elif state_name == "sleeping":
        assert scheduler.submit_instruction("pa:test", _instruction(), decision_accepted_at=START)
    elif state_name == "retry_wait":
        claimed = scheduler.claim_jobs()[0]
        assert not scheduler.complete_job(claimed, {"classification": "timeout"})
        claimed = None
    else:
        scheduler.on_market_view(_view("bad-view", "bad-snapshot", ready=False))
    before = scheduler.get("pa:test").generation
    assert scheduler.disable("pa:test")
    record = scheduler.get("pa:test")
    assert record.generation == before + 1
    assert record.state == StrategyState.DISABLED
    assert record.timer_tokens == {}
    assert scheduler.queue_size == 0
    if claimed is not None:
        assert not scheduler.complete_job(claimed, _instruction(), decision_accepted_at=START)


def test_expiry_and_close_are_exactly_once_and_recall_only_when_allowed():
    scheduler = _scheduler()
    assert scheduler.submit_instruction("pa:test", _instruction("wait_for_price", x=5), decision_accepted_at=START)
    pending = scheduler.get("pa:test").pending_id
    scheduler.advance(5)
    assert scheduler.queue_size == 1
    assert not scheduler.on_pending_expiry("pa:test", pending)

    scheduler = _scheduler(open_requester=lambda *_args, **_kwargs: OpenRequestResult.accepted_result("pos-1"))
    assert scheduler.submit_instruction("pa:test", _instruction("enter_immediately"), decision_accepted_at=START)
    assert scheduler.on_position_close("pa:test", "pos-1")
    assert scheduler.queue_size == 1
    assert not scheduler.on_position_close("pa:test", "pos-1")

    scheduler = _scheduler(open_requester=lambda *_args, **_kwargs: OpenRequestResult.accepted_result("pos-1"))
    assert scheduler.submit_instruction("pa:test", _instruction("enter_immediately"), decision_accepted_at=START)
    assert scheduler.disable("pa:test")
    assert scheduler.on_position_close("pa:test", "pos-1")
    assert scheduler.state_for("pa:test") == StrategyState.DISABLED
    assert scheduler.queue_size == 0


def test_exhausted_callbacks_never_recall_or_open_again():
    scheduler = _scheduler()
    assert scheduler.on_account_exhausted("pa:test")
    assert scheduler.state_for("pa:test") == StrategyState.EXHAUSTED
    scheduler.on_market_view(_view("view-2", "snapshot-2"))
    scheduler.process_timers()
    assert scheduler.queue_size == 0
    assert not scheduler.submit_instruction("pa:test", _instruction("enter_immediately"), decision_accepted_at=START)


def test_sleep_deadline_elapsed_during_market_wait_recalls_on_recovery():
    scheduler = _scheduler()
    assert scheduler.submit_instruction("pa:test", _instruction("no_entry", n=5), decision_accepted_at=START)
    scheduler.on_market_view(_view("bad-view", "bad-snapshot", ready=False))
    record = scheduler.get("pa:test")
    assert record.state == StrategyState.MARKET_WAIT
    assert record.intended_resume_state == StrategyState.SLEEPING
    scheduler.advance(5)
    assert record.state == StrategyState.MARKET_WAIT
    assert record.intended_resume_state == StrategyState.ANALYSIS_DUE
    scheduler.on_market_view(_view("recovered-view", "recovered-snapshot"))
    assert scheduler.queue_size == 1

def test_market_view_missing_frozen_second_is_not_execution_ready():
    incomplete = {
        "shared_market_view_id": "view-without-second",
        "closed_snapshot_id": "snapshot-without-second",
        "regular_session": True,
        "fresh": True,
        "complete": True,
        "non_gapped": True,
        "sufficient": True,
    }
    scheduler = _scheduler(view=incomplete)
    assert scheduler.state_for("pa:test") == StrategyState.MARKET_WAIT
    assert scheduler.queue_size == 0


def test_same_profile_ref_does_not_clear_a_semantic_retry_block():
    profile = {"profile_ref_hash": "a" * 64}
    scheduler = StrategyScheduler(
        [StrategySpec("pa:test", profile_ref=profile)],
        clock=FakeClock(START),
    )
    scheduler._verify_success_instruction = lambda _record, _instruction: True
    scheduler.start(_view("view-1", "snapshot-1"))
    for delay in (5, 10, 20):
        job = scheduler.claim_jobs()[0]
        assert not scheduler.complete_job(job, {"classification": "grounding_failure"})
        scheduler.advance(delay)
    job = scheduler.claim_jobs()[0]
    assert not scheduler.complete_job(job, {"classification": "grounding_failure"})
    assert scheduler.state_for("pa:test") == StrategyState.BLOCKED
    assert not scheduler.configuration_changed("pa:test", profile_ref=dict(profile))
    assert scheduler.state_for("pa:test") == StrategyState.BLOCKED
    assert scheduler.queue_size == 0


def test_compound_account_fields_are_removed_recursively_from_model_jobs():
    scheduler = StrategyScheduler(
        [
            {
                "strategy_id": "pa:test",
                "request": {
                    "context": {
                        "account_balance": "1000.00",
                        "paper_account": {"available_funds": "1000.00"},
                        "position_quantity": "10",
                        "applied_pnl": "2.00",
                        "safe_market_field": "kept",
                    }
                },
            }
        ],
        clock=FakeClock(START),
    )
    scheduler.start(_view("view-1", "snapshot-1"))
    serialized = json.dumps(scheduler.queued_jobs()[0].to_dict(), sort_keys=True).lower()
    for forbidden in ("account", "balance", "funds", "quantity", "pnl"):
        assert forbidden not in serialized
    assert "safe_market_field" in serialized


def test_provider_backoff_caps_at_60_and_terminal_configuration_never_spins():
    scheduler = _scheduler()
    for expected in (5, 10, 20, 40, 60, 60, 60):
        job = scheduler.claim_jobs()[0]
        assert not scheduler.complete_job(job, {"classification": "timeout"})
        record = scheduler.get("pa:test")
        assert (record.timer_tokens["retry"].due_at - scheduler.clock.now()).total_seconds() == expected
        scheduler.advance(expected)
    job = scheduler.claim_jobs()[0]
    assert not scheduler.complete_job(job, {"classification": "authentication"})
    assert scheduler.state_for("pa:test") == StrategyState.BLOCKED
    assert scheduler.get("pa:test").timer_tokens == {}
    scheduler.advance(300)
    assert scheduler.queue_size == 0


def test_expiry_and_close_during_market_wait_recall_only_after_recovery():
    scheduler = _scheduler()
    assert scheduler.submit_instruction("pa:test", _instruction("wait_for_price", x=5), decision_accepted_at=START)
    scheduler.on_market_view(_view("bad-view", "bad-snapshot", ready=False))
    scheduler.advance(5)
    record = scheduler.get("pa:test")
    assert record.state == StrategyState.MARKET_WAIT
    assert record.intended_resume_state == StrategyState.ANALYSIS_DUE
    assert scheduler.queue_size == 0
    scheduler.on_market_view(_view("recovered-view", "recovered-snapshot"))
    assert scheduler.queue_size == 1


def test_pending_fill_during_market_wait_retains_open_resume_until_recovery():
    scheduler = _scheduler()
    assert scheduler.submit_instruction("pa:test", _instruction("wait_for_price", x=30), decision_accepted_at=START)
    pending = scheduler.get("pa:test").pending_id
    scheduler.on_market_view(_view("bad-view", "bad-snapshot", ready=False))
    assert scheduler.on_pending_fill("pa:test", pending, "pos-1")
    record = scheduler.get("pa:test")
    assert record.pending_id is None
    assert record.open_position_id == "pos-1"
    assert record.state == StrategyState.MARKET_WAIT
    assert record.intended_resume_state == StrategyState.OPEN_POSITION
    assert scheduler.queue_size == 0
    scheduler.on_market_view(_view("recovered-view", "recovered-snapshot"))
    assert record.state == StrategyState.OPEN_POSITION
    assert scheduler.queue_size == 0

    scheduler = _scheduler(open_requester=lambda *_args, **_kwargs: OpenRequestResult.accepted_result("pos-1"))
    assert scheduler.submit_instruction("pa:test", _instruction("enter_immediately"), decision_accepted_at=START)
    scheduler.on_market_view(_view("bad-view", "bad-snapshot", ready=False))
    assert scheduler.on_position_close("pa:test", "pos-1")
    record = scheduler.get("pa:test")
    assert record.state == StrategyState.MARKET_WAIT
    assert record.intended_resume_state == StrategyState.ANALYSIS_DUE
    assert scheduler.queue_size == 0
    scheduler.on_market_view(_view("recovered-view", "recovered-snapshot"))
    assert scheduler.queue_size == 1


def test_200_strategy_backlog_is_fair_bounded_and_exactly_once():
    strategy_ids = [f"pa:scale-{index:03d}" for index in range(200)]
    scheduler = StrategyScheduler(
        strategy_ids,
        clock=FakeClock(START),
        queue_capacity=17,
        max_in_flight=7,
    )
    scheduler._verify_success_instruction = lambda _record, _instruction: True
    scheduler.start(_view("view-scale", "snapshot-scale"))
    seen: list[str] = []
    while scheduler.queue_size or scheduler.provider_in_flight:
        jobs = scheduler.claim_jobs()
        assert scheduler.queue_size <= 17
        assert scheduler.provider_in_flight <= 7
        assert jobs
        for job in jobs:
            seen.append(job.strategy_id)
            assert scheduler.complete_job(job, instruction=_instruction("no_entry", n=60), decision_accepted_at=START)
    assert len(seen) == 200
    assert set(seen) == set(strategy_ids)
    assert len(set(seen)) == len(seen)
    assert all(record.state == StrategyState.SLEEPING for record in scheduler.records.values())


def test_transition_table_fixture_covers_frozen_states_and_material_paths():
    table = json.loads((ROOT / "tests/fixtures/phase2/g023/transition-table.json").read_text())
    frozen_states = {state.value for state in StrategyState}
    assert table["schema_version"] == "0.1.0"
    assert set(table["states"]) == frozen_states
    rows = {(row["from"], row["event"], row["to"]) for row in table["transitions"]}
    assert len(rows) == len(table["transitions"])
    assert {row["from"] for row in table["transitions"]} == frozen_states - {"exhausted"}
    allowed_targets = frozen_states | {"retained_resume_state"}
    assert all(row["to"] in allowed_targets for row in table["transitions"])
    for required in {
        ("analyzing", "no_entry", "sleeping"),
        ("analyzing", "conditional_entry", "waiting_entry"),
        ("analyzing", "immediate_open_accepted", "open_position"),
        ("retry_wait", "deadline", "analysis_due"),
        ("market_wait", "market_recovered", "retained_resume_state"),
        ("open_position", "disable_monitor_only", "open_position"),
        ("waiting_entry", "account_exhausted", "exhausted"),
    }:
        assert required in rows


def test_simultaneous_timers_wake_each_partition_once_and_duplicates_are_noops():
    scheduler = StrategyScheduler(["pa:first", "pa:second"], clock=FakeClock(START))
    scheduler._verify_success_instruction = lambda _record, _instruction: True
    scheduler.start(_view("view-timers", "snapshot-timers"))
    for strategy_id in ("pa:first", "pa:second"):
        assert scheduler.submit_instruction(
            strategy_id,
            _instruction("no_entry", n=5),
            decision_accepted_at=START,
        )
    handled = scheduler.advance(5)
    assert len(handled) == 2
    assert scheduler.queue_size == 2
    assert {job.strategy_id for job in scheduler.queued_jobs()} == {"pa:first", "pa:second"}
    assert scheduler.process_timers() == []
    assert scheduler.queue_size == 2
