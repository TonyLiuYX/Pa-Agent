"""P0 concurrency, completion-time and lifecycle checks for G028."""

from __future__ import annotations

from pathlib import Path
from threading import Event, Lock, Thread
from time import monotonic
from typing import Any

from pa_engine.strategy.runtime import StrategyState

from g028.test_engine import (
    B,
    FakeClock,
    SID_A,
    SID_B,
    SID_C,
    calls_for,
    events_of,
    make_engine,
    no_entry_outcome,
    outcome_for,
    sec,
    three_strategy_registry,
    wait_outcome,
)


# build_registry returns deterministic strategy order; these aliases make the
# three-strategy counterexamples explicit: A/B are the first two claims and C
# is the queued replacement.
WORK_A = SID_C
WORK_B = SID_B
WORK_C = SID_A


class BlockingAdapter:
    """Deterministic provider fake that makes overlap and release explicit."""

    def __init__(self, engine: Any, gates: dict[str, Event], outcomes: dict[str, Any] | None = None) -> None:
        self.engine = engine
        self.gates = gates
        self.outcomes = outcomes or {}
        self.started = {sid: Event() for sid in gates}
        self.finished = {sid: Event() for sid in gates}
        self.requests: list[dict[str, Any]] = []
        self._lock = Lock()
        self.active = 0
        self.max_active = 0

    @property
    def call_count(self) -> int:
        return len(self.requests)

    def complete(self, request: dict[str, Any], *, generation: int | None = None) -> Any:
        self.requests.append(dict(request))
        profile = request["extensions"]["strategy_profile_ref"]
        sid = profile["strategy_id"]
        with self._lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        self.started[sid].set()
        try:
            if not self.gates[sid].wait(timeout=3):
                raise AssertionError(f"provider gate was not released for {sid}")
            outcome = self.outcomes.get(sid) or no_entry_outcome(self.engine, sid, n=30)
            return outcome(request, generation=generation)
        finally:
            with self._lock:
                self.active -= 1
            self.finished[sid].set()


def _initial_engine(tmp_path: Path, *, max_in_flight: int = 2, clock: Any = None) -> Any:
    s1 = sec("2026-03-09T13:33:59Z", "2026-03-09T13:34:00Z", "10.05")
    return make_engine(
        tmp_path,
        clock=clock,
        aggregator=B.FakeAggregator(batches=[[s1]]),
        config_overrides={"max_in_flight": max_in_flight},
    )


def test_provider_calls_overlap_and_are_strictly_bounded(tmp_path):
    engine = _initial_engine(tmp_path, max_in_flight=2, clock=FakeClock("2026-03-09T13:34:03Z"))
    gates = {SID_A: Event(), SID_B: Event()}
    adapter = BlockingAdapter(engine, gates)
    engine._adapter = adapter
    engine.start()

    engine.step()

    assert adapter.started[SID_A].wait(0.5)
    assert adapter.started[SID_B].wait(0.5)
    assert adapter.max_active == 2
    assert engine.scheduler.provider_in_flight == 2
    assert engine.status()["provider_in_flight"] == 2

    gates[SID_A].set()
    gates[SID_B].set()
    assert adapter.finished[SID_A].wait(1)
    assert adapter.finished[SID_B].wait(1)
    engine.step()
    assert engine.scheduler.provider_in_flight == 0
    assert len(events_of(engine, "model_decision_accepted")) == 2
    engine.stop()


def test_completion_refills_available_slot_while_other_provider_is_blocked(tmp_path):
    clock = FakeClock("2026-03-09T13:34:03Z")
    first = sec("2026-03-09T13:33:59Z", "2026-03-09T13:34:00Z", "10.05")
    engine = make_engine(
        tmp_path,
        registry=three_strategy_registry(),
        clock=clock,
        aggregator=B.FakeAggregator(batches=[[first]]),
        config_overrides={"max_in_flight": 2},
    )
    gates = {WORK_A: Event(), WORK_B: Event(), WORK_C: Event()}
    gates[WORK_C].set()
    adapter = BlockingAdapter(
        engine,
        gates,
        outcomes={
            WORK_A: no_entry_outcome(engine, WORK_A, n=30),
            WORK_B: no_entry_outcome(engine, WORK_B, n=30),
            WORK_C: no_entry_outcome(engine, WORK_C, n=30),
        },
    )
    engine._adapter = adapter
    engine.start()

    engine.step()
    assert adapter.started[WORK_A].wait(0.5)
    assert adapter.started[WORK_B].wait(0.5)
    assert not adapter.started[WORK_C].wait(0.05)

    # Releasing only B must free one logical and one physical slot.  C is
    # claimed immediately while A remains blocked; the pool never exceeds 2.
    gates[WORK_B].set()
    assert adapter.finished[WORK_B].wait(1)
    engine._drain_provider_results({"errors": []})
    assert engine.scheduler.queue_size == 1
    assert engine.scheduler.available_provider_slots == 1
    assert engine._next_runtime_wait(clock.now(), None) == 0.0
    engine.step()
    assert adapter.started[WORK_C].wait(0.5)
    assert calls_for(adapter, WORK_C) == 1
    assert not adapter.finished[WORK_A].is_set()
    assert adapter.max_active == 2
    assert engine._strategy_provider_busy(WORK_A)

    gates[WORK_A].set()
    engine.step()
    assert adapter.finished[WORK_A].wait(1)
    engine.step()
    assert engine.scheduler.provider_in_flight == 0
    assert not engine._provider_tasks
    assert not engine._deferred_provider_jobs
    engine.stop()

    # The completed batch released the SQLite lease as well as all provider
    # slots; a fresh runtime can reopen the same database without leakage.
    restarted = make_engine(
        tmp_path,
        registry=three_strategy_registry(),
        clock=FakeClock("2026-03-09T13:34:03Z"),
        run_id="run_g028_work_conserving_restart",
    )
    restarted.start()
    restarted.stop()


def test_slow_strategy_does_not_freeze_scheduler_view_or_followup_calls(tmp_path):
    clock = FakeClock("2026-03-09T13:34:03Z")
    first = sec("2026-03-09T13:33:59Z", "2026-03-09T13:34:00Z", "10.05")
    second = sec("2026-03-09T13:34:05Z", "2026-03-09T13:34:06Z", "10.10")
    third = sec("2026-03-09T13:34:10Z", "2026-03-09T13:34:11Z", "10.15")
    engine = make_engine(
        tmp_path,
        registry=three_strategy_registry(),
        clock=clock,
        aggregator=B.FakeAggregator(batches=[[first]]),
        config_overrides={"max_in_flight": 2},
    )
    gates = {WORK_A: Event(), WORK_B: Event(), WORK_C: Event()}
    gates[WORK_B].set()
    gates[WORK_C].set()
    adapter = BlockingAdapter(
        engine,
        gates,
        outcomes={
            WORK_A: no_entry_outcome(engine, WORK_A, n=30),
            WORK_B: no_entry_outcome(engine, WORK_B, n=1),
            WORK_C: no_entry_outcome(engine, WORK_C, n=30),
        },
    )
    engine._adapter = adapter
    engine.start()
    engine.step()
    assert adapter.started[WORK_A].wait(0.5)
    assert calls_for(adapter, WORK_B) == 1

    # A remains physically blocked, but a complete new view must still reach
    # G023 and advance B's N timer into a fresh provider call.
    engine._aggregator._batches.append([second])
    report = engine.step(now="2026-03-09T13:34:09Z")
    assert report["ingested"] == 1
    assert engine.scheduler.shared_market_view_id == engine._current_view.view_id
    assert engine.scheduler.get(WORK_A).state == StrategyState.ANALYZING
    assert not adapter.finished[WORK_A].is_set()
    assert calls_for(adapter, WORK_B) == 2
    assert adapter.max_active == 2

    # A's old claim is now superseded.  Its late response is discarded once,
    # its physical busy guard is cleared, and it is not re-called on the same
    # view.  Only the next complete view may requeue A.
    gates[WORK_A].set()
    assert adapter.finished[WORK_A].wait(1)
    engine.step(now="2026-03-09T13:34:10Z")
    assert calls_for(adapter, WORK_A) == 1
    assert not engine._strategy_provider_busy(WORK_A)
    assert engine.scheduler.get(WORK_A).state == StrategyState.ANALYSIS_DUE
    stale = [
        transition
        for transition in engine.scheduler.transitions
        if transition.get("strategy_id") == WORK_A and transition.get("reason") == "stale_market_view_response"
    ]
    assert len(stale) == 1

    engine._aggregator._batches.append([third])
    engine.step(now="2026-03-09T13:34:14Z")
    assert calls_for(adapter, WORK_A) == 2
    assert engine.scheduler.get(WORK_A).state != StrategyState.ANALYZING or not engine._strategy_provider_busy(WORK_A)
    engine.stop()


def test_completion_clock_is_read_per_response_and_anchors_n_and_x(tmp_path):
    clock = FakeClock("2026-03-09T13:34:03Z")
    engine = _initial_engine(tmp_path, max_in_flight=2, clock=clock)
    gates = {SID_A: Event(), SID_B: Event()}
    adapter = BlockingAdapter(
        engine,
        gates,
        outcomes={SID_A: no_entry_outcome(engine, SID_A, n=30), SID_B: wait_outcome(engine, SID_B, x=30)},
    )
    engine._adapter = adapter
    engine.start()
    engine.step()

    clock.set("2026-03-09T13:34:08Z")
    gates[SID_A].set()
    assert adapter.finished[SID_A].wait(1)
    clock.set("2026-03-09T13:34:13Z")
    gates[SID_B].set()
    assert adapter.finished[SID_B].wait(1)
    engine.step()

    accepted = {
        event["strategy_id"]: event["occurred_at"]
        for event in events_of(engine, "model_decision_accepted")
    }
    assert accepted == {
        SID_A: "2026-03-09T13:34:08Z",
        SID_B: "2026-03-09T13:34:13Z",
    }
    schedules = {
        event["strategy_id"]: event["envelope"]["payload"]
        for event in events_of(engine, "schedule_set")
    }
    assert {sid: payload["decision_accepted_at"] for sid, payload in schedules.items()} == accepted
    assert schedules[SID_A]["due_at"] == "2026-03-09T13:34:38Z"
    assert schedules[SID_B]["due_at"] == "2026-03-09T13:34:43Z"
    assert schedules[SID_A]["timer_kind"] == "sleep"
    assert schedules[SID_B]["timer_kind"] == "pending_expiry"
    assert engine.scheduler.get(SID_A).next_due_at.isoformat().replace("+00:00", "Z") == "2026-03-09T13:34:38Z"
    assert engine.scheduler.get(SID_B).expires_at.isoformat().replace("+00:00", "Z") == "2026-03-09T13:34:43Z"
    assert all(expected != "2026-03-09T13:34:03Z" for expected in accepted.values())
    engine.stop()


def test_immediate_completion_uses_response_time_second_and_original_request_view(tmp_path):
    clock = FakeClock("2026-03-09T13:34:03Z")
    first = sec("2026-03-09T13:33:59Z", "2026-03-09T13:34:00Z", "10.05")
    second = sec("2026-03-09T13:34:05Z", "2026-03-09T13:34:06Z", "10.10")
    engine = make_engine(
        tmp_path,
        clock=clock,
        aggregator=B.FakeAggregator(batches=[[first]]),
        config_overrides={"max_in_flight": 2},
    )
    gates = {SID_A: Event(), SID_B: Event()}
    adapter = BlockingAdapter(engine, gates)
    engine._adapter = adapter
    engine.start()
    engine.step()
    assert adapter.started[SID_A].wait(0.5)
    assert adapter.started[SID_B].wait(0.5)
    claim_view = engine._current_view

    def immediate_against_claim(request: dict[str, Any], *, generation: int | None = None, sid: str = SID_A) -> Any:
        return outcome_for(
            engine._registry,
            request,
            sid,
            claim_view,
            "enter_immediately",
            direction="long" if sid == SID_A else "short",
            stop="9.9" if sid == SID_A else "10.3",
            target="10.3" if sid == SID_A else "9.8",
            second_event_ref=B.second_ref_of(claim_view),
            generation=generation,
        )

    adapter.outcomes[SID_A] = immediate_against_claim
    adapter.outcomes[SID_B] = lambda request, generation=None: immediate_against_claim(request, generation=generation, sid=SID_B)

    # Exercise the same runtime feed-ingest/build-view seam while leaving the
    # scheduler's claim view unchanged until the next serialized step.  This
    # lets both completions use the newly accepted second while still proving
    # that request grounding remains bound to the original claim view.
    engine._aggregator._batches.append([second])
    clock.set("2026-03-09T13:34:09Z")
    ingest_at = clock.now()
    events = engine._filter_events(list(engine._feed.ingest(now=ingest_at)))
    engine._record_accepted_seconds(events, ingest_at)
    engine._rebuild_view(ingest_at, {"errors": []})
    assert len(events) == 1
    assert engine._current_view.latest_second["event_id"] == second.event_id
    assert all(
        request["extensions"]["context_refs"]["shared_market_view_id"] == claim_view.view_id
        for request in adapter.requests
    )

    clock.set("2026-03-09T13:34:10Z")
    gates[SID_A].set()
    assert adapter.finished[SID_A].wait(1)
    clock.set("2026-03-09T13:34:13Z")
    gates[SID_B].set()
    assert adapter.finished[SID_B].wait(1)
    engine.step(now="2026-03-09T13:34:13Z")

    expected_ref = {"schema_version": "0.1.0", "event_id": second.event_id, "event_hash": second.event_hash, "reference_price": second.close}
    accepted = {event["strategy_id"]: event for event in events_of(engine, "model_decision_accepted")}
    assert set(accepted) == {SID_A, SID_B}
    assert {sid: event["occurred_at"] for sid, event in accepted.items()} == {
        SID_A: "2026-03-09T13:34:10Z",
        SID_B: "2026-03-09T13:34:13Z",
    }
    for sid in (SID_A, SID_B):
        assert engine.scheduler.get(sid).latest_instruction["second_event_ref"] == expected_ref
        assert engine.scheduler.get(sid).latest_instruction["market_view_ref"] == claim_view.to_decision_ref()
    fills = events_of(engine, "paper_fill")
    assert len(fills) == 2
    assert all(
        fill["envelope"]["market_ref"]["second_event_id"] == second.event_id
        and fill["envelope"]["market_ref"]["second_event_hash"] == second.event_hash
        for fill in fills
    )
    engine.stop()


def test_second_accepted_after_provider_completion_is_not_used_and_fails_closed(tmp_path):
    clock = FakeClock("2026-03-09T13:34:03Z")
    first = sec("2026-03-09T13:33:59Z", "2026-03-09T13:34:00Z", "10.05")
    late = sec("2026-03-09T13:34:35Z", "2026-03-09T13:34:36Z", "10.10")
    engine = make_engine(
        tmp_path,
        clock=clock,
        aggregator=B.FakeAggregator(batches=[[first]]),
        config_overrides={"max_in_flight": 2, "stale_after_seconds": 5},
    )
    gates = {SID_A: Event(), SID_B: Event()}
    adapter = BlockingAdapter(engine, gates)
    engine._adapter = adapter
    engine.start()
    gates[SID_B].set()
    engine.step()
    assert adapter.started[SID_A].wait(0.5)
    claim_view = engine._current_view

    adapter.outcomes[SID_A] = lambda request, generation=None: outcome_for(
        engine._registry,
        request,
        SID_A,
        claim_view,
        "enter_immediately",
        direction="long",
        stop="9.9",
        target="10.3",
        second_event_ref=B.second_ref_of(claim_view),
        generation=generation,
    )
    clock.set("2026-03-09T13:34:40Z")
    gates[SID_A].set()
    assert adapter.finished[SID_A].wait(1)
    completion_time = clock.now()

    # This event is accepted by the normal ingest/build-view path only after
    # the worker has already captured its completion time.  The response must
    # not retroactively use it as the completion reference.
    engine._aggregator._batches.append([late])
    # Keep the fake wall clock equal to the provider completion time.  The
    # runtime acceptance order, not an ambiguous equal timestamp, must prove
    # that this event arrived after completion.
    report = engine.step(now="2026-03-09T13:34:40Z")
    assert report["ingested"] == 1
    assert engine._second_accepted_at[late.event_id] == completion_time
    record = engine.scheduler.get(SID_A)
    assert record.state.value == "market_wait"
    assert record.latest_instruction is None
    assert not [event for event in events_of(engine, "model_decision_accepted") if event["strategy_id"] == SID_A]
    assert not [event for event in events_of(engine, "paper_fill") if event["strategy_id"] == SID_A]
    assert any(error.get("code") == "post_response_second_unavailable" for error in engine.status()["errors"])
    engine.stop()


def test_run_keyboard_interrupt_during_idle_wait_stops_and_releases_lease(tmp_path, monkeypatch):
    engine = make_engine(
        tmp_path,
        clock=FakeClock("2026-03-09T13:34:03Z"),
        aggregator=B.FakeAggregator(),
    )
    engine._adapter = object()
    engine.start()

    def interrupt_wait(**_kwargs: Any) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(engine, "_wait_for_runtime_activity", interrupt_wait)
    summary = engine.run(max_steps=1)
    assert summary["steps"] == 1
    assert summary["final_status"]["stopped"] is True
    assert engine._stop_reason == "keyboard_interrupt"
    assert engine._db is None

    restarted = make_engine(
        tmp_path,
        clock=FakeClock("2026-03-09T13:34:03Z"),
        aggregator=B.FakeAggregator(),
        run_id="run_g028_keyboard_restart",
    )
    restarted.start()
    restarted.stop()


def test_slow_provider_does_not_block_market_ingest(tmp_path):
    clock = FakeClock("2026-03-09T13:34:03Z")
    first = sec("2026-03-09T13:33:59Z", "2026-03-09T13:34:00Z", "10.05")
    second = sec("2026-03-09T13:34:05Z", "2026-03-09T13:34:06Z", "10.10")
    engine = make_engine(
        tmp_path,
        clock=clock,
        aggregator=B.FakeAggregator(batches=[[first]]),
        config_overrides={"max_in_flight": 2},
    )
    gates = {SID_A: Event(), SID_B: Event()}
    adapter = BlockingAdapter(engine, gates)
    engine._adapter = adapter
    engine.start()
    engine.step()
    assert adapter.started[SID_A].wait(0.5)

    engine._aggregator._batches.append([second])
    clock.set("2026-03-09T13:34:09Z")
    report = engine.step()
    assert report["ingested"] == 1
    assert engine.feed.latest_second.end_at == "2026-03-09T13:34:06Z"
    assert adapter.max_active == 2

    gates[SID_A].set()
    gates[SID_B].set()
    engine.stop()


def test_idle_run_waits_and_worker_exception_is_typed(tmp_path):
    engine = make_engine(
        tmp_path,
        clock=FakeClock("2026-03-09T13:34:03Z"),
        aggregator=B.FakeAggregator(),
    )
    engine._adapter = object()
    engine.start()
    started = monotonic()
    summary = engine.run(max_steps=3)
    elapsed = monotonic() - started
    assert summary["steps"] == 3
    assert elapsed >= 0.15
    assert summary["provider_calls"] == 0
    engine.stop()

    engine2 = _initial_engine(tmp_path / "worker-error", max_in_flight=1)
    class ExplodingAdapter:
        def complete(self, request: dict[str, Any], *, generation: int | None = None) -> Any:
            raise RuntimeError("provider failure")

    engine2._adapter = ExplodingAdapter()
    engine2.start()
    engine2.step()
    retry_records = [record for record in engine2.scheduler.records.values() if record.state.value == "retry_wait"]
    assert len(retry_records) == 2
    engine2.stop()


def test_max_in_flight_one_keeps_one_actual_call(tmp_path):
    engine = _initial_engine(tmp_path, max_in_flight=1, clock=FakeClock("2026-03-09T13:34:03Z"))
    gates = {SID_A: Event(), SID_B: Event()}
    adapter = BlockingAdapter(engine, gates)
    engine._adapter = adapter
    engine.start()
    engine.step()
    first_sid = next((sid for sid, started in adapter.started.items() if started.is_set()), None)
    assert first_sid in {SID_A, SID_B}
    second_sid = SID_B if first_sid == SID_A else SID_A
    assert not adapter.started[second_sid].wait(0.05)
    assert adapter.max_active == 1
    assert engine.scheduler.provider_in_flight == 1

    gates[first_sid].set()
    assert adapter.finished[first_sid].wait(1)
    engine.step()
    assert adapter.started[second_sid].wait(0.5)
    assert adapter.max_active == 1
    gates[second_sid].set()
    assert adapter.finished[second_sid].wait(1)
    engine.step()
    assert engine.scheduler.provider_in_flight == 0
    engine.stop()


def test_stop_does_not_join_an_unbounded_provider_call(tmp_path):
    engine = _initial_engine(tmp_path, max_in_flight=1, clock=FakeClock("2026-03-09T13:34:03Z"))
    gate = Event()
    gates = {SID_A: gate, SID_B: Event()}
    adapter = BlockingAdapter(engine, gates)
    engine._adapter = adapter
    engine.start()
    engine.step()
    first_sid = next((sid for sid, started in adapter.started.items() if started.is_set()), None)
    assert first_sid in {SID_A, SID_B}
    started = monotonic()
    report = engine.stop(reason="test_stop")
    assert monotonic() - started < 0.8
    assert report["stopped"] is True
    gates[first_sid].set()
    assert adapter.finished[first_sid].wait(1)


def test_concurrent_stop_serializes_step_cleanup(tmp_path):
    engine = _initial_engine(tmp_path, max_in_flight=1, clock=FakeClock("2026-03-09T13:34:03Z"))
    gates = {SID_A: Event(), SID_B: Event()}
    adapter = BlockingAdapter(engine, gates)
    engine._adapter = adapter
    engine.start()
    step_errors: list[BaseException] = []

    def run_step() -> None:
        try:
            engine.step()
        except BaseException as exc:
            step_errors.append(exc)

    step_thread = Thread(target=run_step, daemon=True)
    step_thread.start()
    first_sid = next((sid for sid, started in adapter.started.items() if started.wait(0.5)), None)
    assert first_sid in {SID_A, SID_B}
    started = monotonic()
    report = engine.stop(reason="concurrent_stop")
    step_thread.join(1)
    assert monotonic() - started < 1
    assert not step_thread.is_alive()
    assert step_errors == []
    assert report["stopped"] is True
    gates[first_sid].set()
    assert adapter.finished[first_sid].wait(1)
