from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pa_engine.strategy.runtime import (
    FakeClock,
    MarketReadiness,
    MarketView,
    OpenRequestResult,
    StrategyScheduler,
    StrategyState,
    market_ready,
)


ROOT = Path(__file__).resolve().parents[3]
VIEWS = json.loads((ROOT / "tests/fixtures/phase2/g023/views.json").read_text())
START = datetime(2026, 1, 2, tzinfo=timezone.utc)


def _scheduler(ids=("pa:test",), **kwargs):
    scheduler = StrategyScheduler(ids, clock=FakeClock(START), **kwargs)
    # Unit fixtures exercise transitions with compact fake instructions.  The
    # production path still calls the frozen G018 verifier for every success.
    scheduler._verify_success_instruction = lambda record, instruction: True
    scheduler.start(VIEWS["ready"])
    return scheduler


def _instruction(branch="no_entry", *, n=5, x=7):
    if branch == "no_entry":
        return {"branch": branch, "next_call_after_seconds": n}
    if branch == "wait_for_price":
        return {"branch": branch, "entry_wait_seconds": x, "entry_price": "100", "stop_price": "99", "target_price": "102", "direction": "long"}
    return {"branch": branch, "entry_price": "100", "stop_price": "99", "target_price": "102", "direction": "long"}


def test_startup_40_is_independent_and_queue_provider_limits_are_separate():
    scheduler = _scheduler([f"pa:s{i}" for i in range(40)], max_in_flight=7)
    assert scheduler.queue_size == 40
    first = scheduler.claim_jobs()
    assert len(first) == 7
    assert len({job.strategy_id for job in first}) == 7
    assert scheduler.provider_in_flight == 7
    assert scheduler.queue_size == 33
    scheduler.complete_job(first[0], {"classification": "timeout"})
    assert scheduler.provider_in_flight == 6
    assert scheduler.queue_size == 33


@pytest.mark.parametrize(
    ("branch", "expected", "seconds"),
    [("no_entry", StrategyState.SLEEPING, 5), ("wait_for_price", StrategyState.WAITING_ENTRY, 7), ("enter_immediately", StrategyState.OPEN_POSITION, 0)],
)
def test_three_g018_branches_have_exact_state_timestamps(branch, expected, seconds):
    scheduler = _scheduler()
    if branch == "enter_immediately":
        scheduler._open_requester = lambda *_args, **_kwargs: OpenRequestResult.accepted_result("pos-1")
    assert scheduler.submit_instruction("pa:test", _instruction(branch), decision_accepted_at=START)
    record = scheduler.get("pa:test")
    assert record.state == expected
    if branch == "no_entry":
        scheduler.advance(seconds - 1)
        assert record.state == StrategyState.SLEEPING
        scheduler.advance(1)
        assert record.state == StrategyState.ANALYZING or scheduler.queue_size == 1
    elif branch == "wait_for_price":
        assert record.timer_tokens["pending_expiry"].due_at == START.replace(second=START.second + 7)
    else:
        assert record.open_position_id == "pos-1"


def test_waiting_and_open_receive_zero_model_calls_across_market_updates():
    scheduler = _scheduler()
    assert scheduler.submit_instruction("pa:test", _instruction("wait_for_price"), decision_accepted_at=START)
    scheduler.on_market_view(VIEWS["ready_next"])
    assert scheduler.queue_size == 0
    scheduler.on_pending_fill("pa:test", scheduler.get("pa:test").pending_id or "", "pos-1")
    scheduler.on_market_view(VIEWS["not_ready"])
    scheduler.on_market_view(VIEWS["ready"])
    assert scheduler.queue_size == 0


def test_disable_invalidates_late_generation_and_pending_callback():
    cancelled = []
    scheduler = _scheduler(cancel_pending=lambda *args, **kwargs: cancelled.append((args, kwargs)))
    job = scheduler.claim_jobs()[0]
    assert scheduler.disable("pa:test")
    assert scheduler.get("pa:test").generation == 1
    assert not scheduler.complete_job(job, _instruction())
    assert scheduler.state_for("pa:test") == StrategyState.DISABLED
    assert not scheduler.submit_instruction("pa:test", _instruction())

    scheduler = _scheduler(cancel_pending=lambda *args, **kwargs: cancelled.append((args, kwargs)))
    assert scheduler.submit_instruction("pa:test", _instruction("wait_for_price"), decision_accepted_at=START)
    pending = scheduler.get("pa:test").pending_id
    scheduler.disable("pa:test")
    assert pending is not None and cancelled
    assert scheduler.state_for("pa:test") == StrategyState.DISABLED


def test_retry_delays_are_exact_and_semantic_retries_cap_then_block():
    scheduler = _scheduler()
    for expected in (5, 10, 20, 40, 60):
        job = scheduler.claim_jobs()[0]
        assert not scheduler.complete_job(job, {"classification": "timeout"})
        record = scheduler.get("pa:test")
        assert record.state == StrategyState.RETRY_WAIT
        assert (record.timer_tokens["retry"].due_at - scheduler.clock.now()).total_seconds() == expected
        scheduler.advance(expected)
    # Completion at the fifth due wakeup enqueues the sixth attempt.
    for expected in (5, 10, 20):
        job = scheduler.claim_jobs()[0]
        assert not scheduler.complete_job(job, {"classification": "malformed_response"})
        record = scheduler.get("pa:test")
        assert record.semantic_retry_count in {1, 2, 3}
        if record.state == StrategyState.RETRY_WAIT:
            scheduler.advance(expected)
    job = scheduler.claim_jobs()[0]
    assert not scheduler.complete_job(job, {"classification": "grounding_failure"})
    assert scheduler.state_for("pa:test") == StrategyState.BLOCKED


def test_market_wait_resumes_due_only_after_complete_view():
    scheduler = StrategyScheduler(["pa:test"], clock=FakeClock(START))
    scheduler._verify_success_instruction = lambda record, instruction: True
    scheduler.start(VIEWS["not_ready"])
    assert scheduler.state_for("pa:test") == StrategyState.MARKET_WAIT
    assert scheduler.queue_size == 0
    scheduler.on_market_view(VIEWS["ready"])
    assert scheduler.state_for("pa:test") == StrategyState.ANALYSIS_DUE
    assert scheduler.queue_size == 1


def test_200_strategy_scale_is_bounded_and_isolated():
    scheduler = _scheduler([f"pa:s{i}" for i in range(200)], queue_capacity=25, max_in_flight=11)
    assert scheduler.queue_size == 25
    claimed = scheduler.claim_jobs()
    assert len(claimed) == 11
    assert scheduler.queue_size <= 25
    first = claimed[0]
    scheduler.complete_job(first, {"classification": "timeout"})
    assert scheduler.get(first.strategy_id).state == StrategyState.RETRY_WAIT
    assert sum(record.state == StrategyState.ANALYZING for record in scheduler.records.values()) == 10
    assert all(record.strategy_id != first.strategy_id or record.provider_retry_count == 1 for record in scheduler.records.values())


def test_immediate_rejection_gates_on_next_view_exactly_once():
    scheduler = _scheduler(open_requester=lambda *args, **kwargs: OpenRequestResult.rejected_result(fresh_price=True))
    assert scheduler.submit_instruction("pa:test", _instruction("enter_immediately"), decision_accepted_at=START)
    assert scheduler.state_for("pa:test") == StrategyState.ANALYSIS_DUE
    assert scheduler.queue_size == 0
    scheduler.on_market_view(VIEWS["ready"])
    assert scheduler.queue_size == 0
    scheduler.on_market_view(VIEWS["ready_next"])
    assert scheduler.queue_size == 1
    scheduler.on_market_view(VIEWS["ready_next"])
    assert scheduler.queue_size == 1


def _contains_forbidden(value):
    if isinstance(value, dict):
        for key, child in value.items():
            if str(key).lower() in {"account", "balance", "quantity", "pnl", "r"}:
                return True
            if _contains_forbidden(child):
                return True
    elif isinstance(value, list):
        return any(_contains_forbidden(child) for child in value)
    return False


def test_jobs_and_logs_are_account_free():
    scheduler = StrategyScheduler([{"strategy_id": "pa:secret", "request": {"balance": 1000, "quantity": 2, "prompt": "ok"}}], clock=FakeClock(START))
    scheduler.start(VIEWS["ready"])
    job = scheduler.queued_jobs()[0]
    assert not _contains_forbidden(job.to_dict())
    assert not _contains_forbidden(list(scheduler.logs))


def test_missing_immediate_open_boundary_blocks_without_fabricating_position():
    scheduler = _scheduler()
    assert scheduler.submit_instruction("pa:test", _instruction("enter_immediately"), decision_accepted_at=START)
    record = scheduler.get("pa:test")
    assert record.open_position_id is None
    assert record.state == StrategyState.BLOCKED
    scheduler.on_market_view(VIEWS["ready_next"])
    assert scheduler.queue_size == 0


@pytest.mark.parametrize("branch", ["wait_for_price", "enter_immediately"])
def test_not_ready_shared_view_retains_trade_state_and_ids(branch):
    scheduler = _scheduler(open_requester=lambda *_args, **_kwargs: OpenRequestResult.accepted_result("pos-1"))
    assert scheduler.submit_instruction("pa:test", _instruction(branch), decision_accepted_at=START)
    record = scheduler.get("pa:test")
    pending = record.pending_id
    position = record.open_position_id
    scheduler.on_market_view(VIEWS["not_ready"])
    assert record.state == StrategyState.MARKET_WAIT
    assert record.intended_resume_state == (StrategyState.WAITING_ENTRY if branch == "wait_for_price" else StrategyState.OPEN_POSITION)
    assert record.pending_id == pending
    assert record.open_position_id == position
    scheduler.on_market_view(VIEWS["ready_next"])
    assert record.state == (StrategyState.WAITING_ENTRY if branch == "wait_for_price" else StrategyState.OPEN_POSITION)


def test_market_quality_failure_is_local_and_requires_new_view():
    scheduler = _scheduler()
    job = scheduler.claim_jobs()[0]
    assert not scheduler.complete_job(job, {"classification": "stale_market"})
    assert scheduler.state_for("pa:test") == StrategyState.MARKET_WAIT
    assert scheduler.queue_size == 0
    scheduler.on_market_view(VIEWS["ready"])
    assert scheduler.queue_size == 0
    scheduler.on_market_view(VIEWS["ready_next"])
    assert scheduler.queue_size == 1


def test_market_quality_duplicate_same_view_keeps_market_wait_resume_marker():
    scheduler = _scheduler()
    job = scheduler.claim_jobs()[0]
    assert not scheduler.complete_job(job, {"classification": "stale_market"})
    record = scheduler.get("pa:test")
    assert record.state == StrategyState.MARKET_WAIT
    assert record.intended_resume_state == StrategyState.ANALYSIS_DUE
    scheduler.on_market_view(VIEWS["ready"])
    assert record.state == StrategyState.MARKET_WAIT
    assert record.intended_resume_state == StrategyState.ANALYSIS_DUE
    assert scheduler.queue_size == 0


def test_pending_fill_during_market_wait_keeps_open_resume_and_is_idempotent():
    scheduler = _scheduler()
    assert scheduler.submit_instruction("pa:test", _instruction("wait_for_price", x=30), decision_accepted_at=START)
    record = scheduler.get("pa:test")
    pending = record.pending_id
    generation = record.generation
    scheduler.on_market_view(VIEWS["not_ready"])
    assert scheduler.on_pending_fill("pa:test", pending, "pos-1", generation=generation)
    assert record.state == StrategyState.MARKET_WAIT
    assert record.intended_resume_state == StrategyState.OPEN_POSITION
    assert record.pending_id is None
    assert record.open_position_id == "pos-1"
    assert not scheduler.on_pending_fill("pa:test", pending, "pos-2", generation=generation)
    scheduler.on_market_view(VIEWS["ready_next"])
    assert record.state == StrategyState.OPEN_POSITION
    assert scheduler.queue_size == 0


def test_semantic_block_uses_block_key_not_previous_event():
    scheduler = _scheduler()
    for delay in (5, 10, 20):
        job = scheduler.claim_jobs()[0]
        assert not scheduler.complete_job(job, {"classification": "malformed_response"})
        scheduler.advance(delay)
    job = scheduler.claim_jobs()[0]
    assert not scheduler.complete_job(job, {"classification": "grounding_failure"})
    assert scheduler.state_for("pa:test") == StrategyState.BLOCKED
    bad = dict(VIEWS["ready_next"])
    bad.update({"shared_market_view_id": "view-2-bad", "regular_session": False, "fresh": False, "complete": False, "non_gapped": False, "sufficient": False, "frozen_second": False})
    scheduler.on_market_view(bad)
    good = dict(VIEWS["ready_next"])
    good.update({"shared_market_view_id": "view-2-good"})
    scheduler.on_market_view(good)
    assert scheduler.queue_size == 1
    assert scheduler.get("pa:test").semantic_retry_count == 0


def test_disabled_trade_closes_with_original_trade_generation():
    scheduler = _scheduler(open_requester=lambda *_args, **_kwargs: OpenRequestResult.accepted_result("pos-1"))
    assert scheduler.submit_instruction("pa:test", _instruction("enter_immediately"), decision_accepted_at=START)
    record = scheduler.get("pa:test")
    trade_generation = record.open_position_generation
    assert scheduler.disable("pa:test")
    assert scheduler.on_position_close("pa:test", "pos-1", generation=trade_generation)
    assert record.state == StrategyState.DISABLED
    assert record.open_position_id is None
    assert record.open_position_generation is None


def test_market_readiness_mapping_is_fail_closed_but_typed_view_is_ergonomic():
    incomplete = dict(VIEWS["ready"])
    incomplete.pop("frozen_second")
    assert not market_ready(incomplete)
    assert not market_ready({"shared_market_view_id": "v", "closed_snapshot_id": "s"})
    rejected = dict(VIEWS["ready"])
    rejected["ready"] = False
    assert not market_ready(rejected)
    assert market_ready(MarketReadiness())
    assert market_ready(MarketView("typed-view", "typed-snapshot"))


def test_semantic_block_requires_actual_profile_or_packet_content_change():
    profile = {"profile_ref_hash": "a" * 64, "version": 1}
    scheduler = _scheduler([{"strategy_id": "pa:test", "profile_ref": profile}])
    for delay in (5, 10, 20):
        job = scheduler.claim_jobs()[0]
        assert not scheduler.complete_job(job, {"classification": "grounding_failure"})
        scheduler.advance(delay)
    job = scheduler.claim_jobs()[0]
    assert not scheduler.complete_job(job, {"classification": "grounding_failure"})
    assert scheduler.state_for("pa:test") == StrategyState.BLOCKED
    assert not scheduler.configuration_changed("pa:test", profile_ref=dict(profile))
    changed = dict(profile, version=2)
    assert scheduler.configuration_changed("pa:test", profile_ref=changed)
    assert scheduler.state_for("pa:test") == StrategyState.ANALYZING or scheduler.queue_size == 1


def test_model_projection_scrubs_compound_camel_kebab_fields_recursively():
    request = {
        "context": {
            "accountBalance": "1000",
            "paper-account": {"availableFunds": "1000"},
            "positionQuantity": "10",
            "applied-pnl": "2",
            "risk_r": "1R",
            "marketPrice": "100",
        }
    }
    scheduler = StrategyScheduler([{"strategy_id": "pa:test", "request": request}], clock=FakeClock(START))
    scheduler.start(VIEWS["ready"])
    serialized = json.dumps(scheduler.queued_jobs()[0].to_dict(), sort_keys=True).lower()
    for forbidden in ("account", "balance", "funds", "quantity", "pnl", "risk_r"):
        assert forbidden not in serialized
    assert "marketprice" in serialized
