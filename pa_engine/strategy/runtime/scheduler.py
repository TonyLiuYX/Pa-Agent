"""Deterministic per-strategy scheduler/state machine for G023.

The scheduler is intentionally event-driven.  It creates typed
``AnalysisJob`` values in a bounded queue; external workers claim jobs and
return a result through ``complete_job``.  There are no worker threads,
network calls, sleeps or account calculations in this module.
"""

from __future__ import annotations

import copy
import hashlib
import inspect
import re
from collections.abc import Iterable, Mapping
from datetime import datetime, timedelta
from typing import Any, Callable

from pa_engine.strategy import decision as _decision_api
from pa_engine.contracts import canonical_json_bytes

verify_instruction = _decision_api.verify_instruction

try:  # G017 is an accepted dependency, but keep imports friendly to fakes.
    from pa_engine.providers import FailureClassification, normalize_classification
except Exception:  # pragma: no cover - only useful in partial installations
    FailureClassification = None  # type: ignore[assignment]

from .callbacks import OpenRequestResult, normalize_open_result
from .clock import Clock, FakeClock, SystemClock, utc_instant
from .state import (
    AnalysisJob,
    StrategyRecord,
    StrategySpec,
    StrategyState,
    TimerToken,
    closed_snapshot_id,
    market_ready,
    stable_view_id,
    view_ref,
    view_to_mapping,
)
from .work_queue import WorkQueue


RETRY_DELAYS = (5, 10, 20, 40, 60)
SEMANTIC_RETRY_DELAYS = (5, 10, 20)
FORBIDDEN_MODEL_KEYS = frozenset({
    "account", "balance", "funds", "capital", "quantity", "qty", "pnl", "profit", "loss", "pl", "r", "p&l", "p_and_l",
})


def _normal_key(value: Any) -> str:
    return str(value or "").strip().lower().replace("-", "_").replace(" ", "_")


def _scrub_model_value(value: Any) -> Any:
    """Recursively omit account/position fields from model-facing values."""

    if isinstance(value, Mapping):
        output: dict[str, Any] = {}
        for key, child in value.items():
            raw_key = str(key)
            # Split camelCase as well as snake/kebab/space-delimited names so
            # compound account fields cannot evade the projection.  Matching
            # whole tokens avoids treating ordinary words containing ``r`` as
            # account math.
            split_key = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", raw_key)
            tokens = [token for token in re.split(r"[^A-Za-z0-9]+", split_key.lower()) if token]
            compact = "".join(tokens)
            forbidden = (
                any(token in FORBIDDEN_MODEL_KEYS for token in tokens)
                or compact in {"accountbalance", "paperaccount", "availablefunds", "positionsize", "positionquantity", "positionqty", "appliedpnl", "profitloss", "p_and_l".replace("_", "")}
                or ("position" in tokens and any(token in {"size", "quantity", "qty", "notional", "value"} for token in tokens))
                or "p&l" in raw_key.lower()
                or ("p" in tokens and "l" in tokens and any(separator in raw_key for separator in ("_", "-", " ", "&")))
            )
            if forbidden:
                continue
            output[str(key)] = _scrub_model_value(child)
        return output
    if isinstance(value, list):
        return [_scrub_model_value(item) for item in value]
    if isinstance(value, tuple):
        return [_scrub_model_value(item) for item in value]
    return copy.deepcopy(value)


def _safe_callback(callback: Callable[..., Any] | None, *args: Any, **kwargs: Any) -> Any:
    """Invoke a fake callback with a small arity-compatible argument set."""

    if callback is None:
        return None
    try:
        signature = inspect.signature(callback)
    except (TypeError, ValueError):
        return callback(*args, **kwargs)
    # Prefer the complete typed call, then trim unsupported keyword/positional
    # arguments without invoking user code more than once.
    try:
        bound = signature.bind_partial(*args, **kwargs)
    except TypeError:
        bound = None
    if bound is not None:
        return callback(*args, **kwargs)
    try:
        bound = signature.bind_partial(*args)
    except TypeError:
        bound = None
    if bound is not None:
        return callback(*args)
    if args:
        try:
            bound = signature.bind_partial(args[0])
        except TypeError:
            bound = None
        if bound is not None:
            return callback(args[0])
    return callback()


def _callback_bool(value: Any, names: tuple[str, ...], default: bool = False) -> bool:
    if isinstance(value, Mapping):
        for name in names:
            if name in value:
                return bool(value[name])
        return default
    for name in names:
        if hasattr(value, name):
            return bool(getattr(value, name))
    return default


class SchedulerConfig:
    """Immutable-ish runtime limits exposed for integrations/tests."""

    def __init__(self, queue_capacity: int = 200, max_in_flight: int = 40, execution_policy: str = "paper") -> None:
        if execution_policy not in {"advisory", "paper"}:
            raise ValueError("execution_policy must be advisory or paper")
        self.queue_capacity = queue_capacity
        self.max_in_flight = max_in_flight
        self.execution_policy = execution_policy

    def to_dict(self) -> dict[str, Any]:
        return {
            "queue_capacity": self.queue_capacity,
            "max_in_flight": self.max_in_flight,
            "execution_policy": self.execution_policy,
        }


class StrategyScheduler:
    """One independent logical monitor per registered strategy."""

    def __init__(
        self,
        strategies: Iterable[Any] | None = None,
        *,
        clock: Clock | None = None,
        queue_capacity: int = 200,
        max_in_flight: int = 40,
        initial_market_view: Mapping[str, Any] | None = None,
        model_executor: Callable[..., Any] | None = None,
        model_request_factory: Callable[..., Any] | None = None,
        decision_finalizer: Callable[..., Any] | None = None,
        open_requester: Callable[..., Any] | None = None,
        cancel_pending: Callable[..., Any] | None = None,
        on_pending_expiry: Callable[..., Any] | None = None,
        on_pending_fill: Callable[..., Any] | None = None,
        on_position_close: Callable[..., Any] | None = None,
        registry: Any = None,
        callbacks: Any = None,
        initial_view: Mapping[str, Any] | None = None,
        provider_max_in_flight: int | None = None,
        model_callback: Callable[..., Any] | None = None,
        paper_open_requester: Callable[..., Any] | None = None,
        open_request_callback: Callable[..., Any] | None = None,
        pending_cancel_callback: Callable[..., Any] | None = None,
        expiry_callback: Callable[..., Any] | None = None,
        fill_callback: Callable[..., Any] | None = None,
        close_callback: Callable[..., Any] | None = None,
        execution_policy: str = "paper",
        **aliases: Any,
    ) -> None:
        if strategies is None:
            strategies = aliases.pop("strategy_specs", None) or aliases.pop("strategy_profiles", None) or aliases.pop("strategy_registry", None)
        registry = registry or aliases.pop("profile_registry", None) or aliases.pop("knowledge_registry", None)
        if aliases.get("provider_concurrency") is not None:
            max_in_flight = aliases.pop("provider_concurrency")
        if provider_max_in_flight is not None:
            max_in_flight = provider_max_in_flight
        if initial_market_view is None:
            initial_market_view = initial_view
        model_executor = model_executor or model_callback
        open_requester = open_requester or paper_open_requester or open_request_callback
        cancel_pending = cancel_pending or pending_cancel_callback
        on_pending_expiry = on_pending_expiry or expiry_callback
        on_pending_fill = on_pending_fill or fill_callback
        on_position_close = on_position_close or close_callback
        self.clock: Clock = clock or SystemClock()
        self.config = SchedulerConfig(queue_capacity, max_in_flight, execution_policy)
        self.queue = WorkQueue(queue_capacity, max_in_flight)
        self.model_executor = model_executor
        self.model_request_factory = model_request_factory
        self.decision_finalizer = decision_finalizer
        self.registry = registry
        self._open_requester = open_requester
        self._cancel_pending = cancel_pending
        self._on_pending_expiry = on_pending_expiry
        self._on_pending_fill = on_pending_fill
        self._on_position_close = on_position_close
        if callbacks is not None:
            self._open_requester = self._open_requester or getattr(callbacks, "request_open", None) or getattr(callbacks, "open", None)
            self._cancel_pending = self._cancel_pending or getattr(callbacks, "cancel_pending", None)
            self._on_pending_expiry = self._on_pending_expiry or getattr(callbacks, "pending_expiry", None) or getattr(callbacks, "on_pending_expiry", None)
            self._on_pending_fill = self._on_pending_fill or getattr(callbacks, "pending_fill", None) or getattr(callbacks, "on_pending_fill", None)
            self._on_position_close = self._on_position_close or getattr(callbacks, "position_close", None) or getattr(callbacks, "on_position_close", None)
        self._records: dict[str, StrategyRecord] = {}
        self._current_market_view: dict[str, Any] | None = None
        self._started = False
        self._job_counter = 0
        self._timer_counter = 0
        self._transitions: list[dict[str, Any]] = []
        self._logs: list[dict[str, Any]] = []
        if strategies is not None:
            if isinstance(strategies, Mapping):
                for strategy_id, profile in strategies.items():
                    if isinstance(profile, Mapping) and "strategy_id" not in profile:
                        self.register_strategy({"strategy_id": str(strategy_id), "profile_ref": profile})
                    else:
                        self.register_strategy(profile if isinstance(profile, Mapping) else str(strategy_id))
            else:
                for spec in strategies:
                    self.register_strategy(spec)
        if initial_market_view is not None:
            self.start(initial_market_view)

    # ------------------------------------------------------------------
    # Registration, views and state inspection
    # ------------------------------------------------------------------
    @staticmethod
    def _coerce_spec(value: Any) -> StrategySpec:
        if isinstance(value, StrategySpec):
            return value
        if isinstance(value, str):
            return StrategySpec(value)
        if isinstance(value, Mapping):
            strategy_id = value.get("strategy_id") or value.get("id")
            if not isinstance(strategy_id, str) or not strategy_id:
                raise ValueError("strategy registration requires strategy_id")
            return StrategySpec(
                strategy_id=strategy_id,
                profile_ref=value.get("profile_ref") or value.get("strategy_profile_ref") or value.get("profile"),
                packet=value.get("packet"),
                enabled=bool(value.get("enabled", value.get("active", True))),
                request=value.get("request") or value.get("model_request"),
                exhausted=bool(value.get("exhausted", False)),
            )
        strategy_id = getattr(value, "strategy_id", None) or getattr(value, "id", None)
        if not isinstance(strategy_id, str) or not strategy_id:
            raise ValueError("strategy registration requires strategy_id")
        return StrategySpec(
            strategy_id=strategy_id,
            profile_ref=getattr(value, "profile_ref", None) or getattr(value, "strategy_profile_ref", None),
            packet=getattr(value, "packet", None),
            enabled=bool(getattr(value, "enabled", getattr(value, "active", True))),
            request=getattr(value, "request", None) or getattr(value, "model_request", None),
            exhausted=bool(getattr(value, "exhausted", False)),
        )

    def register_strategy(self, strategy: Any, **kwargs: Any) -> StrategyRecord:
        spec = self._coerce_spec(strategy)
        if kwargs:
            spec = StrategySpec(
                strategy_id=spec.strategy_id,
                profile_ref=kwargs.get("profile_ref", spec.profile_ref),
                packet=kwargs.get("packet", spec.packet),
                enabled=kwargs.get("enabled", spec.enabled),
                request=kwargs.get("request", spec.request),
                exhausted=kwargs.get("exhausted", spec.exhausted),
            )
        if spec.strategy_id in self._records:
            raise ValueError(f"strategy already registered: {spec.strategy_id}")
        state = StrategyState.EXHAUSTED if spec.exhausted else StrategyState.ANALYSIS_DUE if spec.enabled else StrategyState.DISABLED
        record = StrategyRecord(spec.strategy_id, spec.profile_ref, spec.packet, spec.enabled and not spec.exhausted, state=state, exhausted=spec.exhausted)
        # Keep the caller's model request only as an account-free projection.
        if spec.request is not None:
            record.latest_instruction = None
            setattr(record, "model_request", _scrub_model_value(spec.request))
        self._records[spec.strategy_id] = record
        if self._started:
            self._reconcile_record(record)
        return record

    add_strategy = register_strategy
    register = register_strategy

    @property
    def records(self) -> dict[str, StrategyRecord]:
        return self._records

    @property
    def strategies(self) -> dict[str, StrategyRecord]:
        return self._records

    @property
    def states(self) -> dict[str, StrategyState]:
        return {key: record.state for key, record in self._records.items()}

    @property
    def state(self) -> dict[str, StrategyState]:
        return self.states

    @property
    def current_market_view(self) -> Mapping[str, Any] | None:
        return copy.deepcopy(self._current_market_view) if self._current_market_view is not None else None

    @property
    def current_view(self) -> Mapping[str, Any] | None:
        return self.current_market_view

    @property
    def shared_market_view_id(self) -> str | None:
        return self._view_id() if self._current_market_view is not None else None

    @property
    def current_closed_snapshot_id(self) -> str | None:
        return closed_snapshot_id(self._current_market_view) if self._current_market_view is not None else None

    def get(self, strategy_id: str) -> StrategyRecord | None:
        return self._records.get(strategy_id)

    def state_for(self, strategy_id: str) -> StrategyState | None:
        record = self._records.get(strategy_id)
        return record.state if record else None

    def snapshot(self, strategy_id: str | None = None) -> dict[str, Any]:
        if strategy_id is not None:
            record = self._records.get(strategy_id)
            return record.snapshot() if record is not None else {}
        return {key: record.snapshot() for key, record in self._records.items()}

    to_dict = snapshot

    def start(self, market_view: Mapping[str, Any] | None = None) -> list[AnalysisJob]:
        if market_view is not None:
            self._current_market_view = _scrub_model_value(view_to_mapping(market_view))
        self._started = True
        for record in self._records.values():
            self._reconcile_record(record)
        return list(self.queue.ready_jobs)

    start_run = start
    startup = start

    def on_market_view(self, market_view: Mapping[str, Any] | Any, market_status: Mapping[str, Any] | Any | None = None) -> list[AnalysisJob]:
        if market_status is not None:
            base = view_to_mapping(market_view)
            base["status"] = view_to_mapping(market_status) if not isinstance(market_status, Mapping) else copy.deepcopy(dict(market_status))
            market_view = base
        if not isinstance(market_view, Mapping):
            market_view = view_to_mapping(market_view)
        self._current_market_view = _scrub_model_value(view_to_mapping(market_view))
        current_id = self._view_id()
        current_snapshot = closed_snapshot_id(self._current_market_view)
        ready = self._market_is_ready()
        for record in self._records.values():
            semantic_snapshot = record.semantic_retry_key[1] if record.semantic_retry_key is not None else None
            if (
                record.state == StrategyState.BLOCKED
                and record.blocked_reason == "semantic_retry_exhausted"
                and semantic_snapshot is not None
                and current_snapshot is not None
                and current_snapshot != semantic_snapshot
                and ready
                and record.enabled
            ):
                record.generation += 1
                record.semantic_retry_key = None
                record.semantic_retry_count = 0
                record.blocked_reason = None
                self._set_state(record, StrategyState.ANALYSIS_DUE, "closed_snapshot_changed")
            record.current_view_id = current_id
            record.last_closed_snapshot_id = current_snapshot
            queued = self.queue.queued_job(record.strategy_id)
            if queued is not None and queued.market_view_id != current_id:
                self.queue.cancel_queued_strategy(record.strategy_id)
            if record.state == StrategyState.MARKET_WAIT and ready:
                # A quality failure or fresh-price rejection gates the exact
                # shared view that produced it.  Do this check before
                # changing state so a duplicate ready event cannot clear the
                # retained resume marker or enqueue same-view work.
                if record.rejected_immediate_view_id == current_id:
                    self._reconcile_record(record)
                    continue
                resume = record.intended_resume_state or StrategyState.ANALYSIS_DUE
                record.intended_resume_state = None
                if resume == StrategyState.ANALYZING:
                    # Analyzing work is never resumed after a market wait; a
                    # fresh generation must be claimed instead.
                    resume = StrategyState.ANALYSIS_DUE
                self._set_state(record, resume, "market_ready")
            elif not ready and record.state in {
                StrategyState.ANALYSIS_DUE,
                StrategyState.RETRY_WAIT,
                StrategyState.SLEEPING,
                StrategyState.WAITING_ENTRY,
                StrategyState.OPEN_POSITION,
                StrategyState.ANALYZING,
            }:
                if record.state == StrategyState.ANALYZING and record.in_flight_job_id is not None:
                    # The shared view that justified an in-flight request is
                    # no longer executable.  Cancel that typed work item and
                    # retain an analyzing resume marker; recovery maps it to a
                    # fresh analysis_due generation rather than reusing stale
                    # model output.
                    self.queue.cancel_strategy(record.strategy_id)
                    record.in_flight_job_id = None
                    record.in_flight_generation = None
                    record.generation += 1
                self._enter_market_wait(record, record.state)
            self._reconcile_record(record)
        return list(self.queue.ready_jobs)

    update_market_view = on_market_view
    market_update = on_market_view
    handle_market_view = on_market_view

    def _view_id(self) -> str:
        return stable_view_id(self._current_market_view or {})

    def _market_is_ready(self) -> bool:
        return market_ready(self._current_market_view)

    def _view_reference(self) -> Mapping[str, Any] | None:
        return view_ref(self._current_market_view or {})

    # ------------------------------------------------------------------
    # Queue claiming/completion
    # ------------------------------------------------------------------
    @property
    def queue_size(self) -> int:
        return self.queue.queued

    @property
    def provider_in_flight(self) -> int:
        return self.queue.active

    @property
    def max_in_flight(self) -> int:
        return self.config.max_in_flight

    @property
    def queue_capacity(self) -> int:
        return self.config.queue_capacity

    @property
    def available_provider_slots(self) -> int:
        return self.queue.available_provider_slots

    @property
    def ready_capacity(self) -> int:
        return self.queue.remaining_capacity

    @property
    def logs(self) -> list[dict[str, Any]]:
        return copy.deepcopy(self._logs)

    @property
    def transitions(self) -> list[dict[str, Any]]:
        return copy.deepcopy(self._transitions)

    @property
    def events(self) -> list[dict[str, Any]]:
        return self.transitions

    def queued_jobs(self) -> tuple[AnalysisJob, ...]:
        return self.queue.ready_jobs

    ready_jobs = queued_jobs

    def enqueue_strategy(self, strategy_id: str, market_view: Mapping[str, Any] | None = None) -> AnalysisJob | None:
        if market_view is not None:
            self.on_market_view(market_view)
        record = self._records.get(strategy_id)
        if record is None or record.state != StrategyState.ANALYSIS_DUE:
            return self.queue.queued_job(strategy_id)
        self._reconcile_record(record)
        return self.queue.queued_job(strategy_id)

    enqueue_due = enqueue_strategy
    recall = enqueue_strategy

    def claim_jobs(self, limit: int | None = None) -> list[AnalysisJob]:
        claimed = self.queue.claim(limit)
        accepted: list[AnalysisJob] = []
        for job in claimed:
            record = self._records.get(job.strategy_id)
            if record is None or not record.enabled or record.exhausted or record.generation != job.generation or record.state != StrategyState.ANALYSIS_DUE:
                self.queue.finish(job.job_id)
                continue
            record.in_flight_job_id = job.job_id
            record.in_flight_generation = job.generation
            self._set_state(record, StrategyState.ANALYZING, "job_claimed")
            accepted.append(job)
        # Claiming frees ready-queue slots even when provider slots are full;
        # keep due backlog bounded and visible instead of dropping it.
        self._drain_due_records()
        return accepted

    claim = claim_jobs
    claim_ready = claim_jobs

    def run_ready(self, limit: int | None = None) -> list[AnalysisJob]:
        """Claim and optionally execute work through an injected executor."""

        jobs = self.claim_jobs(limit)
        if self.model_executor is None:
            return jobs
        for job in jobs:
            try:
                result = _safe_callback(self.model_executor, job)
            except Exception as exc:  # callback failures become typed terminal/retry paths
                result = exc
            completion_time = None
            if isinstance(result, Mapping):
                completion_time = result.get("decision_accepted_at")
            else:
                completion_time = getattr(result, "decision_accepted_at", None)
            self.complete_job(job.job_id, result, decision_accepted_at=completion_time or self.clock.now())
        return jobs

    run = run_ready
    step = run_ready

    def complete_job(
        self,
        job: AnalysisJob | str,
        result: Any = None,
        *,
        instruction: Mapping[str, Any] | None = None,
        outcome: Any = None,
        error: Any = None,
        decision_accepted_at: datetime | str | None = None,
        strategy_id: str | None = None,
        generation: int | None = None,
        idempotency_key: str | None = None,
    ) -> bool:
        """Complete exactly one claimed generation/idempotency key.

        Unknown, duplicate and stale completions return ``False`` without
        changing state.  Successful instructions are verified by G018 before
        they can touch a strategy state or callback.
        """

        job_id = job.job_id if isinstance(job, AnalysisJob) else str(job)
        # Convenience completion by strategy partition is still generation
        # checked; the durable identity remains the claimed job ID.
        if not isinstance(job, AnalysisJob) and (job_id not in self._records) and strategy_id is not None:
            job_id = strategy_id
        if job_id in self._records:
            record_hint = self._records[job_id]
            if generation is not None and generation != record_hint.generation:
                return False
            job_id = record_hint.in_flight_job_id or ""
        queued = self.queue.get(job_id)
        if queued is None or self.queue.active_job(queued.strategy_id) is None:
            return False
        record = self._records.get(queued.strategy_id)
        active = self.queue.active_job(queued.strategy_id)
        if record is None or active is None or active.job_id != job_id:
            return False
        if generation is not None and generation != queued.generation:
            return False
        if idempotency_key is not None and idempotency_key != queued.idempotency_key:
            return False
        # Release provider concurrency before invoking callbacks; a callback
        # may synchronously enqueue a new due job for another strategy.
        self.queue.finish(job_id)
        if record.in_flight_job_id != job_id or record.in_flight_generation != queued.generation or record.generation != queued.generation:
            return False
        record.in_flight_job_id = None
        record.in_flight_generation = None
        if self._view_id() != queued.market_view_id:
            # A newer shared view superseded this generation while the worker
            # was running.  Do not interpret the old response as malformed or
            # schedule a retry against it.
            self._set_state(record, StrategyState.ANALYSIS_DUE, "stale_market_view_response")
            record.rejected_immediate_view_id = self._view_id()
            if not self._market_is_ready():
                self._enter_market_wait(record, StrategyState.ANALYSIS_DUE)
            return False
        effective = outcome if outcome is not None else result
        if error is not None:
            effective = error
        if isinstance(effective, tuple) and len(effective) == 2:
            possible_value, possible_timestamp = effective
            if decision_accepted_at is None:
                decision_accepted_at = possible_timestamp
            effective = possible_value
        if decision_accepted_at is None:
            if isinstance(effective, Mapping):
                decision_accepted_at = effective.get("decision_accepted_at")
            else:
                decision_accepted_at = getattr(effective, "decision_accepted_at", None)
        if self._is_late(effective):
            self._set_state(record, StrategyState.ANALYSIS_DUE, "late_response_discarded")
            # A late worker result must not immediately launch another call on
            # the same shared view.  The next complete view clears this gate.
            record.rejected_immediate_view_id = self._view_id()
            if not self._market_is_ready():
                self._enter_market_wait(record, StrategyState.ANALYSIS_DUE)
            return False
        instruction_value = instruction or self._extract_instruction(effective)
        if instruction_value is None:
            self._handle_failure(record, effective, job=queued)
            self._drain_due_records()
            return False
        if not self._verify_success_instruction(record, instruction_value):
            self._handle_semantic_failure(record, job=queued, reason="instruction_validation_failed")
            self._drain_due_records()
            return False
        try:
            if decision_accepted_at is None:
                raise ValueError("decision_accepted_at is required for a model completion")
            accepted_at = utc_instant(decision_accepted_at)
        except ValueError:
            self._handle_semantic_failure(record, job=queued, reason="invalid_decision_timestamp")
            self._drain_due_records()
            return False
        self._apply_instruction(record, instruction_value, accepted_at)
        self._drain_due_records()
        return True

    complete = complete_job
    finish_job = complete_job

    def _extract_instruction(self, value: Any) -> Mapping[str, Any] | None:
        if value is None:
            return None
        if isinstance(value, Mapping):
            if "instruction_hash" in value and "branch" in value:
                return value
            for key in ("instruction", "scheduler_instruction", "strategy_scheduler_instruction"):
                candidate = value.get(key)
                if isinstance(candidate, Mapping):
                    return candidate
            response = value.get("response")
            if isinstance(response, Mapping):
                return self._extract_instruction(response)
            extensions = value.get("extensions")
            if isinstance(extensions, Mapping):
                for key in ("instruction", "scheduler_instruction", "strategy_scheduler_instruction"):
                    candidate = extensions.get(key)
                    if isinstance(candidate, Mapping):
                        return candidate
            # G018 finalizer can be injected for a normalized model response.
            if "candidate" in value and self.decision_finalizer is not None:
                try:
                    finalized = _safe_callback(self.decision_finalizer, value)
                except Exception:
                    return None
                if isinstance(finalized, Mapping):
                    return self._extract_instruction(finalized)
                candidate = getattr(finalized, "instruction", None)
                if isinstance(candidate, Mapping):
                    return candidate
            return None
        response = getattr(value, "response", None) or getattr(value, "normalized_response", None)
        if response is not None:
            return self._extract_instruction(response)
        candidate = getattr(value, "instruction", None)
        return candidate if isinstance(candidate, Mapping) else None

    def _is_late(self, value: Any) -> bool:
        if value is None:
            return False
        if bool(getattr(value, "late", False)) or bool(getattr(value, "is_late", False)):
            return True
        if isinstance(value, Mapping) and bool(value.get("late")):
            return True
        classification = self._classification(value)
        return classification in {"late", "late_response"}

    # ------------------------------------------------------------------
    # Instruction application and callback events
    # ------------------------------------------------------------------
    def _verify_success_instruction(self, record: StrategyRecord, instruction: Mapping[str, Any]) -> bool:
        try:
            if instruction.get("strategy_id") != record.strategy_id:
                return False
            expected_profile = record.profile_ref or instruction.get("strategy_profile_ref")
            expected_view = self._view_reference()
            kwargs: dict[str, Any] = {"expected_profile_ref": expected_profile}
            if self.registry is not None:
                kwargs["registry"] = self.registry
                kwargs["packet"] = record.packet
            if expected_view is not None:
                kwargs["expected_market_view_ref"] = expected_view
            verifier = verify_instruction if verify_instruction is not _decision_api.verify_instruction else _decision_api.verify_instruction
            if not bool(verifier(instruction, **kwargs)):
                return False
            supplied_view = instruction.get("market_view_ref") if isinstance(instruction, Mapping) else None
            if expected_view is None and isinstance(supplied_view, Mapping):
                supplied_id = supplied_view.get("shared_market_view_id")
                explicit_id = None
                if isinstance(self._current_market_view, Mapping):
                    explicit_id = self._current_market_view.get("shared_market_view_id") or self._current_market_view.get("market_view_id") or self._current_market_view.get("view_id")
                if isinstance(explicit_id, str) and explicit_id and supplied_id != explicit_id:
                    return False
            return True
        except Exception:
            return False

    def submit_instruction(
        self,
        strategy_id: str,
        instruction: Mapping[str, Any],
        *,
        decision_accepted_at: datetime | str | None = None,
        generation: int | None = None,
    ) -> bool:
        """Convenience path for tests/fakes that already finalized G018."""

        record = self._records.get(strategy_id)
        if record is None or not record.enabled or record.exhausted:
            return False
        if generation is not None and generation != record.generation:
            return False
        if record.in_flight_job_id is not None:
            job_id = record.in_flight_job_id
            # Let completion enforce generation and idempotency; this path is
            # intentionally equivalent to an external worker response.
            return self.complete_job(job_id, instruction, decision_accepted_at=decision_accepted_at)
        if record.state not in {StrategyState.ANALYSIS_DUE, StrategyState.ANALYZING}:
            return False
        queued = self.queue.queued_job(strategy_id)
        if queued is not None:
            self.queue.cancel(queued.job_id)
        if not self._verify_success_instruction(record, instruction):
            self._handle_semantic_failure(record, job=self._synthetic_job(record), reason="instruction_validation_failed")
            return False
        accepted_at = utc_instant(decision_accepted_at, default=self.clock.now())
        self._apply_instruction(record, instruction, accepted_at)
        return True

    submit = submit_instruction
    apply_instruction = submit_instruction

    def _apply_instruction(self, record: StrategyRecord, instruction: Mapping[str, Any], accepted_at: datetime) -> None:
        branch = instruction.get("branch")
        record.decision_accepted_at = accepted_at
        record.next_due_at = None
        record.expires_at = None
        record.latest_instruction = copy.deepcopy(dict(instruction))
        record.provider_retry_count = 0
        record.semantic_retry_count = 0
        record.semantic_retry_key = None
        record.rejected_immediate_view_id = None
        self._cancel_timers(record)
        if self.config.execution_policy == "advisory":
            # Advisory keeps the validated decision and its grounding but
            # deliberately ignores every execution branch.  The accepted
            # instruction's market_snapshot_id is the durable once-per-
            # selected-candle gate, including after a service restart.
            record.pending_id = None
            record.open_position_id = None
            record.open_position_generation = None
            self._set_state(record, StrategyState.ANALYSIS_DUE, "advisory_decision_accepted")
            return
        if branch == "no_entry":
            n = instruction.get("next_call_after_seconds")
            if type(n) is not int or not 1 <= n <= 86400:
                self._handle_semantic_failure(record, job=self._synthetic_job(record), reason="missing_no_entry_delay")
                return
            self._set_state(record, StrategyState.SLEEPING, "no_entry")
            record.next_due_at = accepted_at + timedelta(seconds=n)
            self._schedule(record, "sleep", record.next_due_at)
            return
        if branch == "wait_for_price":
            x = instruction.get("entry_wait_seconds")
            if type(x) is not int or not 1 <= x <= 86400:
                self._handle_semantic_failure(record, job=self._synthetic_job(record), reason="missing_entry_wait")
                return
            pending_id = "pend_" + hashlib.sha256(f"{record.strategy_id}:{record.generation}:{instruction.get('instruction_hash')}".encode()).hexdigest()[:32]
            record.pending_id = pending_id
            self._set_state(record, StrategyState.WAITING_ENTRY, "conditional_entry")
            record.expires_at = accepted_at + timedelta(seconds=x)
            self._schedule(record, "pending_expiry", record.expires_at)
            return
        if branch == "enter_immediately":
            if self._open_requester is None:
                # Immediate entries are executable only through the injected
                # G024 boundary.  Never synthesize a position in its absence;
                # block this strategy until configuration supplies a boundary.
                self._set_state(record, StrategyState.BLOCKED, "invalid_configuration")
                return
            try:
                result = normalize_open_result(
                    _safe_callback(
                        self._open_requester,
                        record.strategy_id,
                        copy.deepcopy(dict(instruction)),
                        generation=record.generation,
                        idempotency_key=f"open:{record.strategy_id}:{record.generation}:{self._view_id()}:{instruction.get('instruction_hash')}",
                    )
                )
            except Exception:
                result = OpenRequestResult.rejected_result(reason="open_request_failed")
            if result.exhausted:
                self._enter_exhausted(record, "open_exhausted")
            elif result.accepted:
                record.open_position_id = result.position_id or "pos_" + hashlib.sha256(f"{record.strategy_id}:{record.generation}:{instruction.get('instruction_hash')}".encode()).hexdigest()[:32]
                record.open_position_generation = record.generation
                self._set_state(record, StrategyState.OPEN_POSITION, "immediate_open_accepted")
            else:
                # An immediate fresh-price rejection is explicitly gated on a
                # different complete view; no same-view hot loop is allowed.
                record.rejected_immediate_view_id = self._view_id()
                self._set_state(record, StrategyState.ANALYSIS_DUE, "immediate_open_rejected")
                self._reconcile_record(record)
            return
        self._handle_semantic_failure(record, job=self._synthetic_job(record), reason="unsupported_instruction_branch")

    def on_pending_expiry(self, strategy_id: str, pending_id: str | None = None, *, token_id: str | None = None, generation: int | None = None) -> bool:
        record = self._records.get(strategy_id)
        if record is None or record.state not in {StrategyState.WAITING_ENTRY, StrategyState.MARKET_WAIT} or record.pending_id is None:
            return False
        if generation is not None and generation != record.generation:
            return False
        if pending_id is not None and pending_id != record.pending_id:
            return False
        if record.state == StrategyState.MARKET_WAIT and record.intended_resume_state != StrategyState.WAITING_ENTRY:
            return False
        current = record.pending_id
        record.pending_id = None
        record.expires_at = None
        self._cancel_timer_kind(record, "pending_expiry")
        if self._on_pending_expiry is not None:
            try:
                _safe_callback(self._on_pending_expiry, strategy_id, current, generation=record.generation)
            except Exception:
                pass
        if record.enabled and not record.exhausted:
            self._set_state(record, StrategyState.ANALYSIS_DUE, "pending_expired")
            self._reconcile_record(record)
        else:
            self._set_state(record, StrategyState.DISABLED if not record.enabled else StrategyState.EXHAUSTED, "pending_expired_no_recall")
        return True

    pending_expired = on_pending_expiry
    expiry = on_pending_expiry

    def on_pending_fill(self, strategy_id: str, pending_id: str, position_id: str | None = None, *, generation: int | None = None, **_: Any) -> bool:
        record = self._records.get(strategy_id)
        if record is None or record.pending_id != pending_id or record.state not in {StrategyState.WAITING_ENTRY, StrategyState.MARKET_WAIT}:
            return False
        if generation is not None and generation != record.generation:
            return False
        if record.state == StrategyState.MARKET_WAIT and record.intended_resume_state != StrategyState.WAITING_ENTRY:
            return False
        waiting_for_market = record.state == StrategyState.MARKET_WAIT
        record.pending_id = None
        record.expires_at = None
        record.open_position_id = position_id or "pos_" + hashlib.sha256(f"{strategy_id}:{record.generation}:{pending_id}".encode()).hexdigest()[:32]
        record.open_position_generation = record.generation
        self._cancel_timer_kind(record, "pending_expiry")
        if waiting_for_market:
            # The fill is accepted against the original pending generation,
            # but shared market quality is still unavailable.  Keep monitor
            # state in MARKET_WAIT and retain OPEN_POSITION for recovery.
            record.intended_resume_state = StrategyState.OPEN_POSITION
        else:
            self._set_state(record, StrategyState.OPEN_POSITION, "pending_filled")
        if self._on_pending_fill is not None:
            try:
                _safe_callback(self._on_pending_fill, strategy_id, pending_id, generation=record.generation, position_id=record.open_position_id)
            except Exception:
                pass
        return True

    pending_filled = on_pending_fill
    fill = on_pending_fill

    def on_open_rejection(self, strategy_id: str, *, fresh_price: bool = True, view_id: str | None = None, generation: int | None = None) -> bool:
        record = self._records.get(strategy_id)
        if record is None or record.state != StrategyState.ANALYZING:
            return False
        if generation is not None and generation != record.generation:
            return False
        record.rejected_immediate_view_id = view_id or self._view_id() if fresh_price else None
        self._set_state(record, StrategyState.ANALYSIS_DUE, "open_rejected_callback")
        if fresh_price:
            return True
        self._reconcile_record(record)
        return True

    def on_position_close(self, strategy_id: str, position_id: str | None = None, *, exhausted: bool = False, reason: str | None = None, generation: int | None = None, **_: Any) -> bool:
        record = self._records.get(strategy_id)
        if record is None or record.open_position_id is None:
            return False
        trade_generation = record.open_position_generation
        if generation is not None and generation != trade_generation:
            return False
        if position_id is not None and position_id != record.open_position_id:
            return False
        current = record.open_position_id
        record.open_position_id = None
        record.open_position_generation = None
        if self._on_position_close is not None:
            try:
                _safe_callback(
                    self._on_position_close,
                    strategy_id,
                    current,
                    generation=trade_generation if trade_generation is not None else record.generation,
                    reason=reason,
                )
            except Exception:
                pass
        if exhausted:
            self._enter_exhausted(record, "position_closed_exhausted")
        elif record.enabled and not record.exhausted:
            self._set_state(record, StrategyState.ANALYSIS_DUE, "position_closed")
            self._reconcile_record(record)
        else:
            self._set_state(record, StrategyState.DISABLED if not record.enabled else StrategyState.EXHAUSTED, "position_closed_no_recall")
        return True

    position_closed = on_position_close
    trade_closed = on_position_close

    def on_account_exhausted(self, strategy_id: str, *, generation: int | None = None, **_: Any) -> bool:
        record = self._records.get(strategy_id)
        if record is None:
            return False
        if generation is not None and generation != record.generation:
            return False
        if record.exhausted:
            return False
        self._enter_exhausted(record, "account_exhausted_callback")
        return True

    account_exhausted = on_account_exhausted

    def handle_callback(self, event: Mapping[str, Any] | Any) -> bool:
        kind = event.get("type") or event.get("event") or event.get("kind") if isinstance(event, Mapping) else getattr(event, "type", None) or getattr(event, "event", None) or getattr(event, "kind", None)
        strategy_id = event.get("strategy_id") if isinstance(event, Mapping) else getattr(event, "strategy_id", None)
        if not isinstance(strategy_id, str):
            return False
        kind = _normal_key(kind)
        if kind in {"pending_expiry", "expiry", "entry_expired"}:
            pending = event.get("pending_id") if isinstance(event, Mapping) else getattr(event, "pending_id", None)
            return self.on_pending_expiry(strategy_id, pending)
        if kind in {"pending_fill", "fill", "entry_filled"}:
            pending = event.get("pending_id") if isinstance(event, Mapping) else getattr(event, "pending_id", None)
            position = event.get("position_id") if isinstance(event, Mapping) else getattr(event, "position_id", None)
            return bool(pending) and self.on_pending_fill(strategy_id, pending, position)
        if kind in {"position_close", "close", "trade_closed"}:
            position = event.get("position_id") if isinstance(event, Mapping) else getattr(event, "position_id", None)
            exhausted = bool(event.get("exhausted")) if isinstance(event, Mapping) else bool(getattr(event, "exhausted", False))
            return self.on_position_close(strategy_id, position, exhausted=exhausted)
        if kind in {"account_exhausted", "exhausted"}:
            return self.on_account_exhausted(strategy_id)
        if kind in {"open_rejection", "rejected"}:
            fresh = bool(event.get("fresh_price_rejected", True)) if isinstance(event, Mapping) else bool(getattr(event, "fresh_price_rejected", True))
            return self.on_open_rejection(strategy_id, fresh_price=fresh)
        return False

    # ------------------------------------------------------------------
    # Timers and failure policy
    # ------------------------------------------------------------------
    def _schedule(self, record: StrategyRecord, kind: str, due_at: datetime) -> TimerToken:
        self._timer_counter += 1
        token = TimerToken(f"tmr_{self._timer_counter}", record.strategy_id, record.generation, kind, due_at)
        record.timer_tokens[kind] = token
        return token

    def _cancel_timer_kind(self, record: StrategyRecord, kind: str) -> None:
        token = record.timer_tokens.pop(kind, None)
        if token is not None:
            token.active = False

    def _cancel_timers(self, record: StrategyRecord) -> None:
        for token in record.timer_tokens.values():
            token.active = False
        record.timer_tokens.clear()
        record.next_due_at = None
        record.expires_at = None

    def process_timers(self, now: datetime | str | None = None) -> list[str]:
        instant = utc_instant(now, default=self.clock.now())
        due: list[TimerToken] = []
        for record in self._records.values():
            due.extend(token for token in record.timer_tokens.values() if token.active and token.due_at <= instant)
        due.sort(key=lambda token: (token.due_at, token.token_id))
        handled: list[str] = []
        for token in due:
            record = self._records.get(token.strategy_id)
            if record is None or not token.active:
                continue
            token.active = False
            if record.timer_tokens.get(token.kind) is token:
                record.timer_tokens.pop(token.kind, None)
            if token.generation != record.generation:
                continue
            handled.append(token.token_id)
            if token.kind == "pending_expiry":
                self.on_pending_expiry(record.strategy_id, record.pending_id, token_id=token.token_id)
            elif token.kind in {"sleep", "retry"}:
                record.next_due_at = None
                if record.state == StrategyState.MARKET_WAIT:
                    if record.intended_resume_state in {StrategyState.RETRY_WAIT, StrategyState.SLEEPING}:
                        record.intended_resume_state = StrategyState.ANALYSIS_DUE
                    continue
                if record.state not in {StrategyState.SLEEPING, StrategyState.RETRY_WAIT}:
                    continue
                self._set_state(record, StrategyState.ANALYSIS_DUE, f"{token.kind}_due")
                self._reconcile_record(record)
        return handled

    process_due_timers = process_timers
    tick = process_timers

    def process_timer(self, token_id: str) -> bool:
        for record in self._records.values():
            token = next((item for item in record.timer_tokens.values() if item.token_id == token_id), None)
            if token is not None:
                if not token.active or token.due_at > self.clock.now():
                    return False
                return token_id in self.process_timers()
        return False

    wake = process_timer

    def advance(self, seconds: int | float, *, process: bool = True) -> list[str]:
        if not hasattr(self.clock, "advance"):
            raise TypeError("scheduler clock is not advanceable")
        self.clock.advance(seconds)  # type: ignore[attr-defined]
        return self.process_timers() if process else []

    advance_clock = advance

    def _handle_failure(self, record: StrategyRecord, value: Any, *, job: AnalysisJob) -> None:
        classification = self._classification(value)
        if not classification and (value is None or isinstance(value, Mapping)):
            self._handle_semantic_failure(record, job=job, reason="malformed_response")
            return
        if classification in {"timeout", "rate_limit"} or (classification in {"server", "provider", "transport"} and self._retryable(value)):
            self._schedule_provider_retry(record)
            return
        if classification in {"malformed_response", "invalid_model_response", "grounding_failure", "malformed", "grounding"}:
            self._handle_semantic_failure(record, job=job, reason=classification)
            return
        if classification in {"market_quality", "market_not_ready", "stale_market", "partial_market", "gapped_market", "insufficient_market"}:
            # A quality failure must wait for a newer complete view, even if
            # the scheduler's current view still reports ready.
            record.rejected_immediate_view_id = self._view_id()
            self._enter_market_wait(record, StrategyState.ANALYSIS_DUE)
            return
        if classification in {"authentication", "invalid_configuration", "invalid_config", "invalid_request", "insufficient_balance", "unsupported_capability"}:
            self._set_state(record, StrategyState.BLOCKED, classification)
            return
        self._set_state(record, StrategyState.BLOCKED, classification or "terminal_failure")

    def _handle_semantic_failure(self, record: StrategyRecord, *, job: AnalysisJob, reason: str) -> None:
        key = ((record.profile_ref or {}).get("profile_ref_hash") if isinstance(record.profile_ref, Mapping) else None, job.closed_snapshot_id)
        if record.semantic_retry_key != key:
            record.semantic_retry_key = key
            record.semantic_retry_count = 0
        if record.semantic_retry_count >= len(SEMANTIC_RETRY_DELAYS):
            self._set_state(record, StrategyState.BLOCKED, "semantic_retry_exhausted")
            return
        delay = SEMANTIC_RETRY_DELAYS[record.semantic_retry_count]
        record.semantic_retry_count += 1
        self._set_state(record, StrategyState.RETRY_WAIT, reason)
        record.next_due_at = self.clock.now() + timedelta(seconds=delay)
        self._schedule(record, "retry", record.next_due_at)

    def _schedule_provider_retry(self, record: StrategyRecord) -> None:
        record.provider_retry_count += 1
        delay = RETRY_DELAYS[min(record.provider_retry_count - 1, len(RETRY_DELAYS) - 1)]
        self._set_state(record, StrategyState.RETRY_WAIT, "provider_retry")
        record.next_due_at = self.clock.now() + timedelta(seconds=delay)
        self._schedule(record, "retry", record.next_due_at)

    def _classification(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, BaseException):
            value = getattr(value, "classification", None) or getattr(value, "code", None) or type(value).__name__
        if isinstance(value, Mapping):
            original = value
            value = original.get("classification") or original.get("error_kind") or original.get("status")
            if value is None and isinstance(original.get("error"), Mapping):
                err = original["error"]
                value = err.get("classification") or err.get("code")
            elif value is None and isinstance(original.get("error"), str):
                value = original.get("error")
            if value in {None, "error", "unsupported"} and isinstance(original.get("response"), Mapping):
                nested = original["response"]
                nested_error = nested.get("error") if isinstance(nested, Mapping) else None
                value = (nested_error.get("classification") or nested_error.get("code")) if isinstance(nested_error, Mapping) else nested.get("status")
        else:
            original = value
            if getattr(original, "valid", None) is False:
                value = getattr(original, "code", None) or "grounding_failure"
            else:
                value = getattr(original, "classification", None) or getattr(original, "error_kind", None) or getattr(original, "status", None)
            if value is None and hasattr(original, "error"):
                error = getattr(original, "error")
                if isinstance(error, Mapping):
                    value = error.get("classification") or error.get("code")
        normalized = _normal_key(value)
        local_market_aliases = {
            "market_quality": "market_quality",
            "market_quality_failure": "market_quality",
            "market_not_ready": "market_not_ready",
            "market_unready": "market_not_ready",
            "not_ready": "market_not_ready",
            "stale_market": "stale_market",
            "stale_data": "stale_market",
            "partial_market": "partial_market",
            "partial_data": "partial_market",
            "gapped_market": "gapped_market",
            "market_gap": "gapped_market",
            "insufficient_market": "insufficient_market",
            "insufficient_data": "insufficient_market",
        }
        if normalized in local_market_aliases:
            return local_market_aliases[normalized]
        aliases = {
            "model_timeout": "timeout", "timed_out": "timeout", "auth": "authentication", "authentication_error": "authentication",
            "rate_limit": "rate_limit", "provider_error": "provider", "transport_error": "transport", "server_error": "server",
            "invalid_model_response": "malformed_response", "malformed": "malformed_response", "grounding": "grounding_failure",
            "configuration": "invalid_configuration", "config": "invalid_configuration", "insufficient_funds": "insufficient_balance",
            "unsupported": "unsupported_capability", "late_response": "late_response", "cancelled": "cancellation",
            "valueerror": "malformed_response", "typeerror": "malformed_response", "jsondecodeerror": "malformed_response",
            "malformedresponseerror": "malformed_response", "groundingerror": "grounding_failure",
            "timeouterror": "timeout", "ratelimiterror": "rate_limit", "connectionerror": "transport", "oserror": "transport", "permissionerror": "authentication", "servererror": "server",
        }
        if FailureClassification is not None:
            try:
                normalized = normalize_classification(normalized)
            except Exception:
                pass
        if "ground" in normalized or "binding_mismatch" in normalized:
            return "grounding_failure"
        if "malform" in normalized or normalized in {"invalid_instruction", "invalid_candidate", "model_response_binding_mismatch"}:
            return "malformed_response"
        return aliases.get(normalized, normalized)

    def _retryable(self, value: Any) -> bool:
        classification = self._classification(value)
        if isinstance(value, Mapping):
            if classification in {"server", "provider", "transport"}:
                extension = value.get("extensions")
                if value.get("transient") is True or isinstance(extension, Mapping) and extension.get("transient") is True:
                    return True
            elif value.get("retryable") is True:
                return True
            error = value.get("error")
            if not isinstance(error, Mapping) and isinstance(value.get("response"), Mapping):
                error = value["response"].get("error")
            if isinstance(error, Mapping):
                if classification in {"server", "provider", "transport"}:
                    return bool(error.get("transient") is True or isinstance(error.get("extensions"), Mapping) and error["extensions"].get("transient") is True)
                return bool(error.get("retryable") is True or isinstance(error.get("extensions"), Mapping) and error["extensions"].get("transient") is True)
        if classification not in {"server", "provider", "transport"} and bool(getattr(value, "retryable", False)):
            return True
        error = getattr(value, "error", None)
        if isinstance(error, Mapping):
            if classification in {"server", "provider", "transport"}:
                return bool(error.get("transient") is True or isinstance(error.get("extensions"), Mapping) and error["extensions"].get("transient") is True)
            return bool(error.get("retryable") is True or isinstance(error.get("extensions"), Mapping) and error["extensions"].get("transient") is True)
        return False

    # ------------------------------------------------------------------
    # Disable/invalidation/configuration and internal helpers
    # ------------------------------------------------------------------
    def disable(self, strategy_id: str, *, reason: str = "disabled") -> bool:
        record = self._records.get(strategy_id)
        if record is None:
            return False
        if not record.enabled:
            return False
        record.enabled = False
        record.generation += 1
        self.queue.cancel_strategy(strategy_id)
        record.in_flight_job_id = None
        record.in_flight_generation = None
        self._cancel_timers(record)
        if record.pending_id is not None:
            pending = record.pending_id
            record.pending_id = None
            if self._cancel_pending is not None:
                try:
                    _safe_callback(self._cancel_pending, strategy_id, pending, generation=record.generation)
                except Exception:
                    pass
        if record.open_position_id is not None:
            # Keep the trade monitor-only until its close callback.
            self._set_state(record, StrategyState.OPEN_POSITION, "disabled_monitor_only")
        else:
            self._set_state(record, StrategyState.DISABLED, reason)
        self._drain_due_records()
        return True

    invalidate = disable
    disable_strategy = disable

    def enable(self, strategy_id: str) -> bool:
        record = self._records.get(strategy_id)
        if record is None or record.exhausted:
            return False
        record.enabled = True
        record.generation += 1
        record.in_flight_job_id = None
        record.in_flight_generation = None
        if record.open_position_id is not None:
            self._set_state(record, StrategyState.OPEN_POSITION, "enabled_monitor")
        else:
            self._set_state(record, StrategyState.ANALYSIS_DUE, "enabled")
            self._reconcile_record(record)
        return True

    enable_strategy = enable

    def configuration_changed(self, strategy_id: str | None = None, *, profile_ref: Mapping[str, Any] | None = None, packet: Mapping[str, Any] | None = None) -> bool:
        records = [self._records[strategy_id]] if strategy_id is not None and strategy_id in self._records else list(self._records.values()) if strategy_id is None else []
        changed = False
        for record in records:
            # Compare references before assignment.  A semantic retry block is
            # keyed to the profile/packet content, so replaying the identical
            # object must not act as a configuration change.
            profile_changed = (
                profile_ref is not None
                and canonical_json_bytes(dict(record.profile_ref or {})) != canonical_json_bytes(dict(profile_ref))
            )
            packet_changed = (
                packet is not None
                and canonical_json_bytes(dict(record.packet or {})) != canonical_json_bytes(dict(packet))
            )
            if profile_ref is not None:
                record.profile_ref = copy.deepcopy(dict(profile_ref))
            if packet is not None:
                record.packet = copy.deepcopy(dict(packet))
            config_block = record.blocked_reason in {
                "authentication", "invalid_configuration", "invalid_config", "invalid_request", "insufficient_balance", "unsupported_capability"
            }
            semantic_block = record.blocked_reason == "semantic_retry_exhausted"
            can_clear = config_block or (semantic_block and (profile_changed or packet_changed))
            if record.state == StrategyState.BLOCKED and record.enabled and not record.exhausted and can_clear:
                record.generation += 1
                self._cancel_timers(record)
                record.semantic_retry_key = None
                record.semantic_retry_count = 0
                record.provider_retry_count = 0
                record.blocked_reason = None
                self._set_state(record, StrategyState.ANALYSIS_DUE, "configuration_changed")
                self._reconcile_record(record)
                changed = True
        return changed

    reconfigure = configuration_changed

    def _enter_exhausted(self, record: StrategyRecord, reason: str) -> None:
        if record.exhausted and record.state == StrategyState.EXHAUSTED:
            return
        record.exhausted = True
        record.enabled = False
        record.generation += 1
        self.queue.cancel_strategy(record.strategy_id)
        record.in_flight_job_id = None
        record.in_flight_generation = None
        self._cancel_timers(record)
        if record.pending_id is not None:
            pending = record.pending_id
            record.pending_id = None
            if self._cancel_pending is not None:
                try:
                    _safe_callback(self._cancel_pending, record.strategy_id, pending, generation=record.generation)
                except Exception:
                    pass
        self._set_state(record, StrategyState.EXHAUSTED, reason)

    def _enter_market_wait(self, record: StrategyRecord, intended: StrategyState | None = None) -> None:
        if record.state == StrategyState.MARKET_WAIT:
            if intended is not None:
                record.intended_resume_state = intended
            return
        if record.state in {StrategyState.WAITING_ENTRY, StrategyState.OPEN_POSITION}:
            intended = record.state
        elif intended is None:
            intended = record.state
        record.intended_resume_state = intended
        self._set_state(record, StrategyState.MARKET_WAIT, "market_not_ready")

    def _reconcile_record(self, record: StrategyRecord) -> None:
        if not self._started or not record.enabled or record.exhausted:
            return
        if record.state == StrategyState.MARKET_WAIT:
            if not self._market_is_ready():
                return
            # Quality failures and immediate fresh-price rejections gate the
            # same ready view.  Keep the retained state until a distinct
            # complete view arrives instead of spinning a same-view call.
            if record.rejected_immediate_view_id == self._view_id():
                return
            resume = record.intended_resume_state or StrategyState.ANALYSIS_DUE
            record.intended_resume_state = None
            self._set_state(record, resume, "market_ready")
        if record.state in {StrategyState.WAITING_ENTRY, StrategyState.OPEN_POSITION, StrategyState.SLEEPING, StrategyState.RETRY_WAIT, StrategyState.BLOCKED, StrategyState.DISABLED, StrategyState.EXHAUSTED}:
            return
        if record.state != StrategyState.ANALYSIS_DUE or record.in_flight_job_id is not None or record.has_trade:
            return
        if not self._market_is_ready():
            self._enter_market_wait(record, StrategyState.ANALYSIS_DUE)
            return
        current_id = self._view_id()
        if self.config.execution_policy == "advisory":
            instruction = record.latest_instruction if isinstance(record.latest_instruction, Mapping) else {}
            accepted_ref = instruction.get("market_view_ref") if isinstance(instruction, Mapping) else None
            accepted_snapshot = accepted_ref.get("market_snapshot_id") if isinstance(accepted_ref, Mapping) else None
            current_snapshot = closed_snapshot_id(self._current_market_view or {})
            if isinstance(accepted_snapshot, str) and accepted_snapshot and accepted_snapshot == current_snapshot:
                return
        if record.rejected_immediate_view_id is not None and record.rejected_immediate_view_id == current_id:
            return
        if self.queue.has_strategy(record.strategy_id):
            return
        job = self._make_job(record)
        if self.queue.put(job):
            record.current_view_id = current_id
            record.last_closed_snapshot_id = job.closed_snapshot_id

    def _drain_due_records(self) -> None:
        if not self._started or not self._market_is_ready() or self.queue.remaining_capacity <= 0:
            return
        for record in self._records.values():
            if self.queue.remaining_capacity <= 0:
                break
            self._reconcile_record(record)

    def _make_job(self, record: StrategyRecord) -> AnalysisJob:
        self._job_counter += 1
        current_view = self._current_market_view or {}
        view_id = self._view_id()
        digest = hashlib.sha256(f"{record.strategy_id}:{record.generation}:{view_id}:{self._job_counter}".encode()).hexdigest()
        request = getattr(record, "model_request", None)
        if self.model_request_factory is not None:
            try:
                request = _safe_callback(self.model_request_factory, record.strategy_id, copy.deepcopy(current_view), record.profile_ref)
            except Exception:
                request = request
        request = _scrub_model_value(request) if isinstance(request, Mapping) else request
        return AnalysisJob(
            job_id="ajb_" + digest,
            idempotency_key=f"analysis:{record.strategy_id}:{record.generation}:{view_id}:{self._job_counter}",
            strategy_id=record.strategy_id,
            generation=record.generation,
            market_view_id=view_id,
            closed_snapshot_id=closed_snapshot_id(current_view),
            created_at=self.clock.now(),
            market_view=_scrub_model_value(current_view),
            profile_ref=_scrub_model_value(record.profile_ref) if isinstance(record.profile_ref, Mapping) else record.profile_ref,
            request=request if isinstance(request, Mapping) else None,
        )

    def _synthetic_job(self, record: StrategyRecord) -> AnalysisJob:
        return self._make_job(record)

    def _set_state(self, record: StrategyRecord, state: StrategyState, reason: str) -> None:
        previous = record.state
        if previous == state and reason not in {"market_ready", "job_claimed"}:
            return
        record.state = state
        if state == StrategyState.BLOCKED:
            record.blocked_reason = reason
        elif state != StrategyState.BLOCKED:
            record.blocked_reason = None
        record.transition_count += 1
        record.last_transition_at = self.clock.now()
        event = {
            "strategy_id": record.strategy_id,
            "from": previous.value if isinstance(previous, StrategyState) else str(previous),
            "to": state.value,
            "state": state.value,
            "event_type": "state_transition",
            "generation": record.generation,
            "at": record.last_transition_at.isoformat().replace("+00:00", "Z"),
            "timestamp": record.last_transition_at.isoformat().replace("+00:00", "Z"),
            "reason": str(reason),
        }
        record.transition_log.append(copy.deepcopy(event))
        self._transitions.append(copy.deepcopy(event))
        self._logs.append(copy.deepcopy(event))


Scheduler = StrategyScheduler
PerStrategyScheduler = StrategyScheduler
DeterministicScheduler = StrategyScheduler


__all__ = [
    "DeterministicScheduler",
    "FORBIDDEN_MODEL_KEYS",
    "PerStrategyScheduler",
    "RETRY_DELAYS",
    "SEMANTIC_RETRY_DELAYS",
    "Scheduler",
    "SchedulerConfig",
    "StrategyScheduler",
]
