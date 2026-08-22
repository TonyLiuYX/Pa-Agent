"""Second-round G025 integration checks against the real G018/G023/G024 seams."""

from __future__ import annotations

import copy
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pa_engine.market_sources.alpaca.history import RecoveryBar
from pa_engine.market_sources.alpaca.stream import SecondAggregateEvent
from pa_engine.paper.core import PaperAccountBook
from pa_engine.paper.entry import PendingEntryMonitor
from pa_engine.strategies.knowledge import load_default_registry
from pa_engine.strategy.decision import make_scheduler_instruction
from pa_engine.strategy.runtime import FakeClock, StrategyScheduler, StrategySpec, StrategyState


ROOT = Path(__file__).resolve().parents[3]
UTC = timezone.utc
START = datetime(2026, 1, 2, 14, 30, tzinfo=UTC)
STRATEGY_ID = "pa:nanqiao-range-extreme-limit"


def _ready_view(ref: dict, *, view_id: str | None = None, snapshot_id: str | None = None, ready: bool = True, session_closed: bool = False) -> dict:
    return {
        "shared_market_view_id": view_id or ref["shared_market_view_id"],
        "closed_snapshot_id": snapshot_id or ref["market_snapshot_id"],
        "regular_session": ready and not session_closed,
        "fresh": ready,
        "complete": ready,
        "non_gapped": ready,
        "sufficient": ready,
        "frozen_second": ready,
        "market_view_ref": copy.deepcopy(ref),
    }


def _real_instruction() -> tuple[dict, dict, dict, dict]:
    registry = load_default_registry(ROOT / "knowledge")
    loaded = registry.load_packet(STRATEGY_ID)
    assert loaded.ok
    candidate = json.loads((ROOT / "tests/fixtures/phase2/g018/valid/wait_long.json").read_text(encoding="utf-8"))
    instruction = make_scheduler_instruction(
        candidate,
        profile_ref=loaded.profile_ref,
        result={"pa_analysis_result_id": "par_" + "c" * 64, "content_hash": "c" * 64},
        evidence_package=loaded.evidence_package,
    )
    return registry, loaded.profile_ref, loaded.packet, instruction


def _stack(*, model_calls: list | None = None, market_ready: bool = True, session_closed: bool = False):
    registry, profile, packet, instruction = _real_instruction()
    ref = instruction["market_view_ref"]
    calls = model_calls if model_calls is not None else []
    scheduler = StrategyScheduler(
        [StrategySpec(STRATEGY_ID, profile, packet)],
        clock=FakeClock(START),
        registry=registry,
        model_executor=lambda *args, **kwargs: calls.append((args, kwargs)),
    )
    scheduler.start(_ready_view(ref, ready=market_ready, session_closed=session_closed))
    # This is the real G023 startup job and authoritative instruction path;
    # no verifier is replaced or monkeypatched.
    jobs = scheduler.claim_jobs()
    if market_ready and not session_closed:
        assert len(jobs) == 1
        assert scheduler.complete_job(jobs[0], instruction, decision_accepted_at=START)
    return scheduler, instruction, calls, ref


def _monitor(scheduler: StrategyScheduler, instruction: dict, *, book: PaperAccountBook | None = None, recovery_callback=None, accepted_at: datetime = START, **kwargs):
    record = scheduler.get(STRATEGY_ID)
    assert record is not None
    return PendingEntryMonitor.from_instruction(
        instruction,
        symbol="AAPL",
        decision_accepted_at=accepted_at,
        submitted_at=accepted_at,
        generation=record.generation,
        paper_account_book=book or PaperAccountBook(),
        scheduler=scheduler,
        recovery_callback=recovery_callback,
        **kwargs,
    )


def _second(start: datetime, *, high: str = "100", low: str = "100", watermark: datetime | None = None, symbol: str = "AAPL") -> SecondAggregateEvent:
    end = start + timedelta(seconds=1)
    watermark = watermark or end + timedelta(seconds=3)
    stamp = lambda value: value.isoformat().replace("+00:00", "Z")
    return SecondAggregateEvent(
        symbol,
        stamp(start),
        stamp(end),
        low,
        high,
        low,
        high,
        "1",
        contributor_ids=(f"trade-{start.isoformat()}",),
        execution_watermark=stamp(watermark),
        status="frozen",
        revision_sequence=0,
        quality={"status": "ok", "codes": [], "connection_gap": False},
    )


def _bar(*, symbol: str = "XYZ", start: datetime = START, end: datetime | None = None, low: str = "99", high: str = "101", complete: bool = True, source_hash: str = "a" * 64) -> RecoveryBar:
    end = end or (start + timedelta(minutes=1))
    stamp = lambda value: value.isoformat().replace("+00:00", "Z")
    open_close = low if float(low) > 100 else "100"
    return RecoveryBar(
        instrument={"asset_class": "us_equity", "symbol": symbol, "venue": "IEX", "currency": "USD"},
        timeframe={"unit": "minute", "multiplier": 1},
        session={"kind": "regular", "timezone": "America/New_York"},
        start_at=stamp(start),
        end_at=stamp(end),
        open=open_close,
        high=high,
        low=low,
        close=open_close,
        volume="10",
        complete=complete,
        coverage_state="complete" if complete else "partial",
        source_request_hash=source_hash,
        recovery_cursor=stamp(end),
        market_feed={"schema_version": "0.1.0", "provider": "alpaca", "feed": "iex", "coverage": "single_exchange"},
    )


def test_real_g023_timer_is_claimed_and_final_eligible_second_fills_once():
    calls: list = []
    scheduler, instruction, _, _ = _stack(model_calls=calls)
    book = PaperAccountBook()
    monitor = _monitor(scheduler, instruction, book=book)
    x = instruction["entry_wait_seconds"]
    assert scheduler.advance(x) == []
    record = scheduler.get(STRATEGY_ID)
    assert record is not None
    assert scheduler.state_for(STRATEGY_ID) == StrategyState.WAITING_ENTRY
    assert record.pending_id == monitor.pending_id and record.expires_at == monitor.expires_at
    assert scheduler.queue_size == 0 and calls == []

    final_start = START + timedelta(seconds=x - 1)
    final = _second(final_start, high="100", low="99", watermark=START + timedelta(seconds=x + 3))
    outcome = monitor.on_second(final, now=START + timedelta(seconds=x + 3))
    assert outcome.status == "filled"
    assert scheduler.state_for(STRATEGY_ID) == StrategyState.OPEN_POSITION
    assert scheduler.queue_size == 0 and calls == []
    account = book.get_account(STRATEGY_ID)
    assert account.pending_trade is None and account.open_trade is not None
    assert len([event for event in book.events if event.event_type == "paper_trade_opened"]) == 1
    assert monitor.on_second(final, now=START + timedelta(seconds=x + 3)).replayed


def test_real_scheduler_no_touch_waits_through_grace_then_recalls_exactly_once():
    calls: list = []
    scheduler, instruction, _, _ = _stack(model_calls=calls)
    book = PaperAccountBook()
    monitor = _monitor(scheduler, instruction, book=book)
    x = instruction["entry_wait_seconds"]
    scheduler.advance(x)
    assert scheduler.state_for(STRATEGY_ID) == StrategyState.WAITING_ENTRY and scheduler.queue_size == 0
    scheduler.advance(3)
    assert scheduler.state_for(STRATEGY_ID) == StrategyState.WAITING_ENTRY and scheduler.queue_size == 0 and calls == []
    first = monitor.on_expiry(scheduler.clock.now())
    assert first.status == "expired" and not first.not_evaluated
    assert scheduler.state_for(STRATEGY_ID) == StrategyState.ANALYSIS_DUE
    assert scheduler.queue_size == 1 and calls == []
    assert not book.get_account(STRATEGY_ID).pending
    second = monitor.on_expiry(scheduler.clock.now())
    assert second.replayed and scheduler.queue_size == 1 and calls == []


@pytest.mark.parametrize("session_closed", [False, True])
def test_real_gap_expiry_keeps_market_wait_and_recalls_after_distinct_complete_view(session_closed: bool):
    scheduler, instruction, _, ref = _stack()
    # A started, real G023 pending entry enters MARKET_WAIT before G025 binds.
    bad = _ready_view(ref, view_id="bad-view", snapshot_id="bad-snapshot", ready=False, session_closed=session_closed)
    scheduler.on_market_view(bad)
    assert scheduler.state_for(STRATEGY_ID) == StrategyState.MARKET_WAIT
    assert scheduler.get(STRATEGY_ID).intended_resume_state == StrategyState.WAITING_ENTRY
    calls: list = []
    book = PaperAccountBook()
    recovery_calls: list = []
    monitor = _monitor(scheduler, instruction, book=book, recovery_callback=lambda *args, **kwargs: recovery_calls.append(1) or [])
    monitor.on_gap(start_at=START.isoformat().replace("+00:00", "Z"), end_at=None)
    scheduler.advance(instruction["entry_wait_seconds"] + 3)
    outcome = monitor.on_expiry(scheduler.clock.now())
    record = scheduler.get(STRATEGY_ID)
    assert outcome.status == "expired" and outcome.not_evaluated and outcome.partial_data
    assert recovery_calls == [1]
    assert record.pending_id is None and scheduler.queue_size == 0
    assert record.state == StrategyState.MARKET_WAIT
    assert record.intended_resume_state == StrategyState.ANALYSIS_DUE
    assert not book.get_account(STRATEGY_ID).pending and calls == []

    fresh = _ready_view(ref, view_id="distinct-view", snapshot_id="distinct-snapshot", ready=True)
    scheduler.on_market_view(fresh)
    assert scheduler.state_for(STRATEGY_ID) == StrategyState.ANALYSIS_DUE
    assert scheduler.queue_size == 1
    assert calls == []


def test_real_session_closure_is_not_gap_and_normal_expiry_is_not_partial():
    calls: list = []
    scheduler, instruction, _, ref = _stack(model_calls=calls)
    closed = _ready_view(ref, view_id="closed-view", snapshot_id="closed-snapshot", ready=False, session_closed=True)
    scheduler.on_market_view(closed)
    assert scheduler.state_for(STRATEGY_ID) == StrategyState.MARKET_WAIT
    assert scheduler.get(STRATEGY_ID).intended_resume_state == StrategyState.WAITING_ENTRY
    book = PaperAccountBook()
    monitor = _monitor(scheduler, instruction, book=book)
    scheduler.advance(instruction["entry_wait_seconds"] + 3)
    outcome = monitor.on_expiry(scheduler.clock.now())
    record = scheduler.get(STRATEGY_ID)
    assert outcome.status == "expired" and not outcome.not_evaluated and not outcome.partial_data and not outcome.gap_overlap
    assert record.pending_id is None and record.state == StrategyState.MARKET_WAIT
    assert record.intended_resume_state == StrategyState.ANALYSIS_DUE
    assert scheduler.queue_size == 0 and calls == []
    scheduler.on_market_view(_ready_view(ref, view_id="closed-recovered", snapshot_id="closed-recovered-snapshot", ready=True))
    assert scheduler.state_for(STRATEGY_ID) == StrategyState.ANALYSIS_DUE
    assert scheduler.queue_size == 1 and calls == []


def test_same_instruction_second_pending_episode_gets_new_g024_identity():
    scheduler, instruction, _, _ = _stack()
    book = PaperAccountBook()
    first = _monitor(scheduler, instruction, book=book)
    first_key = first.creation_result.idempotency_key
    x = instruction["entry_wait_seconds"]
    scheduler.advance(x + 3)
    first_outcome = first.on_expiry(scheduler.clock.now())
    assert first_outcome.status == "expired" and not book.get_account(STRATEGY_ID).pending
    first_expiry_key = first_outcome.expiry_result.idempotency_key
    first_job = scheduler.claim_jobs()[0]
    later = scheduler.clock.now()
    assert scheduler.complete_job(first_job, instruction, decision_accepted_at=later)
    second = _monitor(scheduler, instruction, book=book, accepted_at=later)
    assert second.pending_id == first.pending_id  # G023 compatibility identity
    assert second.creation_result.idempotency_key != first_key
    assert second.creation_result.status == "pending"
    assert book.get_account(STRATEGY_ID).pending_trade is not None
    second_final = _second(later + timedelta(seconds=x - 1), high="100", low="99", watermark=later + timedelta(seconds=x + 3))
    second_fill = second.on_second(second_final, now=later + timedelta(seconds=x + 3))
    assert second_fill.status == "filled"
    assert book.get_account(STRATEGY_ID).open_trade is not None
    assert second.on_second(second_final, now=later + timedelta(seconds=x + 3)).replayed
    assert first_expiry_key != second.creation_result.idempotency_key


def test_same_instruction_expiry_key_changes_with_later_episode():
    scheduler, instruction, _, _ = _stack()
    book = PaperAccountBook()
    first = _monitor(scheduler, instruction, book=book)
    x = instruction["entry_wait_seconds"]
    scheduler.advance(x + 3)
    first_outcome = first.on_expiry(scheduler.clock.now())
    first_key = first_outcome.expiry_result.idempotency_key
    job = scheduler.claim_jobs()[0]
    later = scheduler.clock.now()
    assert scheduler.complete_job(job, instruction, decision_accepted_at=later)
    second = _monitor(scheduler, instruction, book=book, accepted_at=later)
    scheduler.advance(x + 3)
    second_outcome = second.on_expiry(scheduler.clock.now())
    assert second_outcome.status == "expired"
    assert second_outcome.expiry_result.idempotency_key != first_key
    assert not book.get_account(STRATEGY_ID).pending


def test_real_direct_recovery_delivery_is_idempotent_after_first_attempt():
    scheduler, instruction, _, _ = _stack()
    book = PaperAccountBook()
    monitor = _monitor(scheduler, instruction, book=book)
    monitor.on_gap(start_at=START.isoformat().replace("+00:00", "Z"), end_at=(START + timedelta(minutes=1)).isoformat().replace("+00:00", "Z"))
    bar = _bar(symbol="AAPL", low="103", high="104")
    first = monitor.on_recovery(bar)
    second = monitor.on_recovery(bar)
    assert first.status == "pending" and first.code == "recovery_no_touch"
    assert second.status == "replayed" and second.code == "idempotent_replay"
    assert len(monitor.state.recovery_lineage) == 1


@pytest.mark.parametrize("mutation", ["generation", "disabled", "exhausted"])
def test_real_scheduler_identity_change_cancels_account_pending_without_fill_or_model(mutation: str):
    calls: list = []
    scheduler, instruction, _, _ = _stack(model_calls=calls)
    book = PaperAccountBook()
    monitor = _monitor(scheduler, instruction, book=book)
    record = scheduler.get(STRATEGY_ID)
    assert record is not None and record.pending_id == monitor.pending_id
    if mutation == "generation":
        record.generation += 1
    elif mutation == "disabled":
        assert scheduler.disable(STRATEGY_ID)
    else:
        assert scheduler.on_account_exhausted(STRATEGY_ID)
    final = _second(START + timedelta(seconds=instruction["entry_wait_seconds"] - 1), high="100", low="99", watermark=START + timedelta(seconds=instruction["entry_wait_seconds"] + 3))
    outcome = monitor.on_second(final, now=START + timedelta(seconds=instruction["entry_wait_seconds"] + 3))
    assert outcome.status == "cancelled"
    assert book.get_account(STRATEGY_ID).pending_trade is None
    assert book.get_account(STRATEGY_ID).open_trade is None
    assert scheduler.queue_size == 0 and calls == []
    if mutation == "generation":
        assert scheduler.get(STRATEGY_ID).pending_id is None


def test_real_scheduler_false_fill_callback_is_fail_closed_before_g024():
    scheduler, instruction, _, _ = _stack()
    book = PaperAccountBook()
    monitor = _monitor(scheduler, instruction, book=book)
    scheduler.on_pending_fill = lambda *args, **kwargs: False
    x = instruction["entry_wait_seconds"]
    final = _second(START + timedelta(seconds=x - 1), high="100", low="99", watermark=START + timedelta(seconds=x + 3))
    outcome = monitor.on_second(final, now=START + timedelta(seconds=x + 3))
    assert outcome.status == "rejected" and outcome.code == "scheduler_callback_rejected"
    assert monitor.state.pending
    assert book.get_account(STRATEGY_ID).pending_trade is not None
    assert book.get_account(STRATEGY_ID).open_trade is None


def test_real_scheduler_fill_guard_is_read_only_when_g024_rejects():
    scheduler, instruction, _, _ = _stack()
    notices: list = []
    scheduler._on_pending_fill = lambda *args, **kwargs: notices.append((args, kwargs))
    book = PaperAccountBook()
    monitor = _monitor(
        scheduler,
        instruction,
        book=book,
        fill_pending=lambda *args, **kwargs: {"status": "rejected", "accepted": False, "code": "forced_reject"},
    )
    record = scheduler.get(STRATEGY_ID)
    assert record is not None
    before_transitions = copy.deepcopy(scheduler.transitions)
    before_logs = copy.deepcopy(scheduler.logs)
    before_count = record.transition_count
    x = instruction["entry_wait_seconds"]
    final = _second(START + timedelta(seconds=x - 1), high="100", low="99", watermark=START + timedelta(seconds=x + 3))
    outcome = monitor.on_second(final, now=START + timedelta(seconds=x + 3))
    assert outcome.status == "rejected" and outcome.code == "fill_rejected"
    assert notices == []
    assert scheduler.transitions == before_transitions
    assert scheduler.logs == before_logs
    assert record.transition_count == before_count
    assert record.pending_id == monitor.pending_id and record.state == StrategyState.WAITING_ENTRY
    assert book.get_account(STRATEGY_ID).pending_trade is not None


def test_missing_create_boundary_does_not_claim_timer_or_mutate_scheduler():
    scheduler, instruction, calls, _ = _stack(model_calls=[])
    record = scheduler.get(STRATEGY_ID)
    assert record is not None
    token = record.timer_tokens.get("pending_expiry")
    assert token is not None and token.active
    before = {
        "state": record.state,
        "pending_id": record.pending_id,
        "generation": record.generation,
        "expires_at": record.expires_at,
        "transitions": copy.deepcopy(scheduler.transitions),
        "logs": copy.deepcopy(scheduler.logs),
        "queue": scheduler.queue_size,
    }
    with pytest.raises(ValueError, match="create_pending boundary"):
        PendingEntryMonitor.from_instruction(
            instruction,
            symbol="AAPL",
            decision_accepted_at=START,
            submitted_at=START,
            generation=record.generation,
            scheduler=scheduler,
        )
    after = scheduler.get(STRATEGY_ID)
    assert after is record
    assert after.state == before["state"] and after.pending_id == before["pending_id"]
    assert after.generation == before["generation"] and after.expires_at == before["expires_at"]
    assert after.timer_tokens.get("pending_expiry") is token and token.active
    assert scheduler.transitions == before["transitions"]
    assert scheduler.logs == before["logs"]
    assert scheduler.queue_size == before["queue"] and calls == []


def test_unknown_monitor_keyword_is_rejected_before_g024_create():
    scheduler, instruction, _, _ = _stack()
    book = PaperAccountBook()
    record = scheduler.get(STRATEGY_ID)
    assert record is not None
    token = record.timer_tokens.get("pending_expiry")
    before_transitions = copy.deepcopy(scheduler.transitions)
    with pytest.raises(TypeError, match="unknown pending-entry monitor keyword"):
        PendingEntryMonitor.from_instruction(
            instruction,
            symbol="AAPL",
            decision_accepted_at=START,
            submitted_at=START,
            generation=record.generation,
            scheduler=scheduler,
            paper_account_book=book,
            typo_not_a_boundary=True,
        )
    assert book.get_account(STRATEGY_ID) is None
    assert scheduler.get(STRATEGY_ID).timer_tokens.get("pending_expiry") is token and token.active
    assert scheduler.transitions == before_transitions


def test_monitor_constructor_failure_cleans_accepted_g024_pending():
    scheduler, instruction, _, _ = _stack()
    book = PaperAccountBook()

    class BrokenMonitor(PendingEntryMonitor):
        def __init__(self, *args, **kwargs):
            raise RuntimeError("constructor failure")

    with pytest.raises(RuntimeError, match="constructor failure"):
        BrokenMonitor.from_instruction(
            instruction,
            symbol="AAPL",
            decision_accepted_at=START,
            submitted_at=START,
            generation=scheduler.get(STRATEGY_ID).generation,
            scheduler=scheduler,
            paper_account_book=book,
        )
    account = book.get_account(STRATEGY_ID)
    assert account is not None and not account.pending
    record = scheduler.get(STRATEGY_ID)
    assert record is not None and record.pending_id is None and record.state == StrategyState.DISABLED


def test_cancel_reject_throw_then_success_is_atomic_and_episode_idempotent():
    scheduler, instruction, _, _ = _stack()
    book = PaperAccountBook()
    mode = ["reject"]
    keys: list[str] = []

    def cancel(*args, **kwargs):
        keys.append(kwargs["idempotency_key"])
        if mode[0] == "reject":
            return {"status": "rejected", "accepted": False}
        if mode[0] == "throw":
            raise RuntimeError("cancel boundary unavailable")
        kwargs.pop("generation", None)
        return book.expire_pending_entry(*args, **kwargs)

    monitor = _monitor(scheduler, instruction, book=book, cancel_pending=cancel)
    record = scheduler.get(STRATEGY_ID)
    assert record is not None
    before_transitions = copy.deepcopy(scheduler.transitions)
    before_logs = copy.deepcopy(scheduler.logs)
    first = monitor.disable(reason="operator_disabled")
    assert first.status == "rejected" and first.code == "cancel_rejected"
    assert monitor.state.pending and book.get_account(STRATEGY_ID).pending
    assert record.state == StrategyState.WAITING_ENTRY and record.pending_id == monitor.pending_id
    assert scheduler.transitions == before_transitions and scheduler.logs == before_logs

    mode[0] = "throw"
    second = monitor.disable(reason="operator_disabled")
    assert second.status == "rejected" and second.code == "cancel_boundary_error"
    assert monitor.state.pending and book.get_account(STRATEGY_ID).pending
    assert record.state == StrategyState.WAITING_ENTRY and record.pending_id == monitor.pending_id
    assert scheduler.transitions == before_transitions and scheduler.logs == before_logs

    mode[0] = "success"
    third = monitor.disable(reason="operator_disabled_retry")
    assert third.status == "cancelled" and third.accepted
    assert not book.get_account(STRATEGY_ID).pending
    assert record.state == StrategyState.DISABLED and record.pending_id is None
    assert len(keys) == 3 and len(set(keys)) == 1
    replay = monitor.disable(reason="operator_disabled_retry")
    assert replay.replayed and len(keys) == 3


def test_scheduler_bridge_expiry_commits_once_and_returns_true():
    scheduler, instruction, _, _ = _stack()
    notices: list[tuple] = []
    scheduler._on_pending_expiry = lambda *args, **kwargs: notices.append((args, kwargs))
    monitor = _monitor(scheduler, instruction, book=PaperAccountBook())
    bridge = monitor._scheduler_bridge
    assert bridge is not None and bridge.prepare_expiry()
    assert bridge.on_expiry(STRATEGY_ID, monitor.pending_id, generation=monitor.generation) is True
    assert scheduler.state_for(STRATEGY_ID) == StrategyState.ANALYSIS_DUE
    assert scheduler.get(STRATEGY_ID).pending_id is None
    assert scheduler.queue_size == 1 and len(notices) == 1
    after_commit_transitions = len(scheduler.transitions)
    after_commit_logs = len(scheduler.logs)
    assert bridge.on_expiry(STRATEGY_ID, monitor.pending_id, generation=monitor.generation) is False
    assert bridge.complete_expiry() is False
    assert len(notices) == 1
    assert len(scheduler.transitions) == after_commit_transitions
    assert len(scheduler.logs) == after_commit_logs


def test_monitor_disable_cancels_live_scheduler_without_recall():
    calls: list = []
    scheduler, instruction, _, _ = _stack(model_calls=calls)
    book = PaperAccountBook()
    monitor = _monitor(scheduler, instruction, book=book)
    assert monitor.disable().status == "cancelled"
    record = scheduler.get(STRATEGY_ID)
    assert record is not None
    assert record.pending_id is None and record.state == StrategyState.DISABLED
    assert scheduler.queue_size == 0 and calls == []
    assert not book.get_account(STRATEGY_ID).pending


def test_pending_create_rejection_disables_claimed_scheduler_record_fail_closed():
    scheduler, instruction, calls, _ = _stack(model_calls=[])
    with pytest.raises(ValueError):
        PendingEntryMonitor.from_instruction(
            instruction,
            symbol="AAPL",
            decision_accepted_at=START,
            submitted_at=START,
            generation=0,
            scheduler=scheduler,
            create_pending=lambda *args, **kwargs: {"status": "rejected", "accepted": False},
        )
    record = scheduler.get(STRATEGY_ID)
    assert record is not None and record.state == StrategyState.DISABLED and record.pending_id is None
    assert scheduler.queue_size == 0 and calls == []


@pytest.mark.parametrize(
    "bar,expected_code,expected_status",
    [
        (_bar(low="103", high="104"), "recovery_no_touch", "pending"),
        (_bar(low="99", high="101"), None, "filled"),
        (_bar(start=START - timedelta(minutes=1), end=START, low="99", high="101"), "recovery_not_evaluated", "not_evaluated"),
        (_bar(start=START + timedelta(minutes=1), end=START + timedelta(minutes=2), low="99", high="101"), "recovery_not_evaluated", "not_evaluated"),
        (_bar(complete=False, low="99", high="101"), "recovery_not_evaluated", "not_evaluated"),
        (_bar(symbol="OTHER", low="99", high="101"), "recovery_identity_mismatch", "not_evaluated"),
    ],
)
def test_recovery_identity_temporal_and_coverage_matrix(bar: RecoveryBar, expected_code: str | None, expected_status: str):
    # Use the small local monitor for typed RecoveryBar outcomes; G019 still
    # performs the authoritative strict validation inside recovery_mapping.
    from tests.phase2.g025.test_entry import monitor as local_monitor

    monitor = local_monitor(x=60)
    monitor.on_gap(start_at=START.isoformat().replace("+00:00", "Z"), end_at=(START + timedelta(minutes=1)).isoformat().replace("+00:00", "Z"))
    outcome = monitor.on_recovery(bar)
    assert outcome.status == expected_status
    if expected_code is not None:
        assert outcome.code == expected_code
    if expected_status == "filled":
        assert outcome.lineage["path_precision"] == "minute_bar_no_path"
        assert outcome.exit_eligible_after == datetime.fromisoformat(bar.end_at.replace("Z", "+00:00"))
        assert monitor.on_recovery(bar).replayed
    elif expected_code == "recovery_identity_mismatch":
        assert monitor.state.recovery_lineage == ()


def test_recovery_source_request_binding_is_fail_closed():
    from tests.phase2.g025.test_entry import monitor as local_monitor

    monitor = local_monitor(x=60, expected_source_request_hash="b" * 64)
    monitor.on_gap(start_at=START.isoformat().replace("+00:00", "Z"), end_at=(START + timedelta(minutes=1)).isoformat().replace("+00:00", "Z"))
    outcome = monitor.on_recovery(_bar(source_hash="a" * 64, low="99", high="101"))
    assert outcome.status == "not_evaluated" and outcome.code == "recovery_identity_mismatch"
    assert monitor.state.recovery_lineage == ()

    mismatched_request = {
        "instrument": {"symbol": "OTHER"},
        "market_feed": {"schema_version": "0.1.0", "provider": "alpaca", "feed": "iex", "coverage": "single_exchange"},
        "source_request_hash": "a" * 64,
    }
    monitor = local_monitor(x=60, recovery_request=mismatched_request)
    monitor.on_gap(start_at=START.isoformat().replace("+00:00", "Z"), end_at=(START + timedelta(minutes=1)).isoformat().replace("+00:00", "Z"))
    mismatch = monitor.on_recovery(_bar(low="99", high="101"))
    assert mismatch.status == "not_evaluated" and mismatch.code == "recovery_identity_mismatch"
    assert monitor.state.recovery_lineage == ()

    monitor = local_monitor(x=60, expected_recovery_cursor="2026-01-02T14:32:00Z")
    monitor.on_gap(start_at=START.isoformat().replace("+00:00", "Z"), end_at=(START + timedelta(minutes=1)).isoformat().replace("+00:00", "Z"))
    cursor_mismatch = monitor.on_recovery(_bar(low="99", high="101"))
    assert cursor_mismatch.status == "not_evaluated" and cursor_mismatch.code == "recovery_identity_mismatch"
    assert monitor.state.recovery_lineage == ()
