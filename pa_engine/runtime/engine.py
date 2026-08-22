"""G028 automatic paper-trading runtime: the thin composition service.

The runtime wires the accepted G015-G027 components into the user's complete
process: one symbol/timeframe IEX feed session, one G021 shared market view,
one DeepSeek adapter per run (injected, never credential-constructing), one
G023 scheduler with independent per-strategy records, G024 paper accounts,
G025 pending-entry monitors, G026 exit monitors and the G027 SQLite store.

Ordering contract
-----------------
``step`` is one deterministic loop iteration:

1. ingest newly frozen seconds from the aggregator;
2. rebuild the shared view when new seconds arrived (or when recovery did);
3. merge completed provider calls on the runtime thread;
4. notify the scheduler of the current view id;
5. process due scheduler timers;
6. claim and execute model jobs (persisting decisions/schedules);
7. fan out every new second to pending then exit monitors and advance the
   stored cursor;
8. sweep pending expiry deadlines;
9. persist advanced scheduler transitions and timers;
10. detect gaps and reconcile with G019 recovery bars.

Every model-facing value is boundary-scanned before transport and no account
state ever reaches the adapter; paper events are persisted with exact
second/recovery-bar references and content-bound idempotency keys.

Startup/restart
---------------
``start`` opens the database under a singleton lease, restores durable rows,
wraps restored monitors, backfills history to the persisted cursor and
reconciles the market before live seconds are processed.  ``stop`` releases
the lease so a later engine instance can reopen the same database.
"""

from __future__ import annotations

import copy
import time
import uuid
from dataclasses import dataclass, field as dataclass_field
from datetime import datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from queue import Empty, Full, Queue
from threading import Condition, Event as ThreadingEvent, Lock, RLock, Thread
from typing import Any, Callable, Mapping
from zoneinfo import ZoneInfo

from pa_engine.paper.core import PaperAccountBook
from pa_engine.contracts import finalize_artifact
from pa_engine.market_sources.alpaca.history import validate_recovery_bar
from pa_engine.paper.entry import PendingEntryMonitor
from pa_engine.paper.exit import ExitMonitor, make_exit_state
from pa_engine.runtime.boundary import ModelBoundaryError, assert_model_boundary_safe
from pa_engine.runtime.config import RuntimeConfig
from pa_engine.runtime.feed import FeedSession, FeedSessionError
from pa_engine.runtime.persistence import RuntimeHydrator, RuntimePersistenceError, RuntimeSink
from pa_engine.storage import (
    LeaseConflictError,
    RuntimeRecovery,
    RuntimeStore,
    StorageClosedError,
    StorageError,
    instant_from_text,
    instant_to_text,
    make_recovery_bar_ref,
    make_second_ref,
)
from pa_engine.strategy.decision import (
    finalize_decision,
    make_model_output_ref,
    map_candidate_to_normalized_model_candidate,
    parse_candidate,
)
from pa_engine.strategy.runtime import (
    StrategyScheduler,
    StrategyState,
    SystemClock,
    TimerToken,
    utc_instant,
)
from pa_engine.strategy.runtime.callbacks import OpenRequestResult

# The engine never constructs credentials; ``ZERO`` is the G024 fixed
# zero-fee/zero-slippage boundary for every paper transition.
from pa_engine.paper.core.math import ZERO

REPORT_RECENT_EVENTS = 50
_TIMER_KINDS = ("sleep", "retry", "pending_expiry")
_NY = ZoneInfo("America/New_York")
_PROVIDER_COMPLETION_GRACE_SECONDS = 0.25
_RUNTIME_WAIT_SECONDS = 0.10


@dataclass(frozen=True)
class _ProviderExecution:
    """One provider result returned from a worker to the runtime thread.

    Workers deliberately return a value instead of invoking scheduler or
    persistence callbacks.  ``completed_at`` is captured immediately after
    the adapter returns and is the only timestamp used for an accepted
    decision.  ``provider_called`` keeps diagnostic counts accurate when a
    request fails before transport or a worker cannot be submitted.
    """

    value: Any
    completed_at: datetime
    provider_called: bool = False
    error_code: str | None = None
    context: Any = None
    completed_order: float = dataclass_field(default_factory=time.monotonic)


@dataclass(frozen=True)
class _ProviderResponseContext:
    """Immutable worker-to-runtime context for main-thread G018 finalization."""

    view: Any
    loaded: Any
    analysis_request: Mapping[str, Any]
    request: Mapping[str, Any]


@dataclass(frozen=True)
class _ProviderTask:
    job: Any
    view: Any


class _ProviderWorkerPool:
    """Small bounded provider worker pool owned by one runtime instance.

    The scheduler owns the logical in-flight lease; this pool owns the actual
    provider call threads.  Its task queue is bounded to the same limit as
    the worker count, and worker exceptions are contained by the engine's
    worker wrapper.  Threads are daemonized because an arbitrary injected
    provider may block beyond a caller's stop deadline; normal stop still
    requests cancellation and joins for a short bounded interval.
    """

    def __init__(
        self,
        max_workers: int,
        worker: Callable[[_ProviderTask], _ProviderExecution],
        on_result: Callable[[], None],
    ) -> None:
        if type(max_workers) is not int or max_workers <= 0:
            raise ValueError("provider worker count must be a positive integer")
        self.max_workers = max_workers
        self._worker = worker
        self._on_result = on_result
        self._tasks: Queue[_ProviderTask] = Queue(maxsize=max_workers)
        self._results: Queue[tuple[_ProviderTask, _ProviderExecution]] = Queue()
        self._stop_event = ThreadingEvent()
        self._lock = Lock()
        self._threads: list[Thread] = []
        self._closed = False

    def _start_locked(self, target_workers: int) -> None:
        target_workers = min(self.max_workers, max(1, target_workers))
        while len(self._threads) < target_workers:
            index = len(self._threads)
            thread = Thread(
                target=self._worker_loop,
                name=f"pa-provider-{index + 1}",
                daemon=True,
            )
            self._threads.append(thread)
            thread.start()

    def submit(self, task: _ProviderTask) -> None:
        with self._lock:
            if self._closed:
                raise RuntimeError("provider worker pool is closed")
            try:
                self._tasks.put_nowait(task)
            except Full as exc:
                raise RuntimeError("provider worker queue is full") from exc
            # Grow lazily with submitted work.  A default limit of 40 must
            # not create 40 threads for a two-strategy run, while a full
            # claim still creates one worker per concurrently callable job.
            self._start_locked(len(self._threads) + 1)

    def _worker_loop(self) -> None:
        while True:
            try:
                task = self._tasks.get(timeout=0.05)
            except Empty:
                if self._stop_event.is_set():
                    return
                continue
            try:
                if self._stop_event.is_set():
                    continue
                execution = self._worker(task)
                if not isinstance(execution, _ProviderExecution):
                    raise TypeError("provider worker returned an invalid execution")
                self._results_put(task, execution)
            except BaseException as exc:
                # The engine wrapper normally contains failures.  Keep a
                # second containment boundary here so an unexpected worker
                # bug cannot silently consume the scheduler's provider lease.
                execution = _ProviderExecution(
                    value={
                        "classification": "transport",
                        "transient": True,
                        "retryable": True,
                    },
                    completed_at=datetime.now(timezone.utc),
                    provider_called=False,
                    error_code=f"worker_{type(exc).__name__}",
                )
                self._results_put(task, execution)
            finally:
                self._tasks.task_done()

    def _results_put(self, task: _ProviderTask, execution: _ProviderExecution) -> None:
        # The result queue is unbounded and only carries one item per claimed
        # job, so a completed provider call can never block its worker here.
        self._results.put((task, execution))
        self._on_result()

    def drain(self) -> list[tuple[_ProviderTask, _ProviderExecution]]:
        results: list[tuple[_ProviderTask, _ProviderExecution]] = []
        while True:
            try:
                results.append(self._results.get_nowait())
            except Empty:
                return results

    @property
    def pending_results(self) -> int:
        return self._results.qsize()

    @property
    def alive_workers(self) -> int:
        return sum(thread.is_alive() for thread in self._threads)

    def close(self, *, join_timeout: float = 0.20) -> dict[str, Any]:
        with self._lock:
            if self._closed:
                return {"closed": True, "alive_workers": self.alive_workers}
            self._closed = True
            self._stop_event.set()
            threads = tuple(self._threads)
        deadline = time.monotonic() + max(0.0, join_timeout)
        for thread in threads:
            remaining = max(0.0, deadline - time.monotonic())
            if remaining <= 0:
                break
            thread.join(remaining)
        return {
            "closed": True,
            "alive_workers": self.alive_workers,
            "stop_timeout": self.alive_workers > 0,
        }


def _in_regular_session(instant: datetime) -> bool:
    """Wall-clock U.S. regular-session membership (G021's exact rule)."""
    local = instant.astimezone(_NY)
    if local.weekday() >= 5:
        return False
    current = local.time()
    return dt_time(9, 30) <= current < dt_time(16, 0)


def _session_day(instant: datetime) -> Any:
    """Local regular-session day for cross-day watermark comparison."""
    return instant.astimezone(_NY).date()


class RuntimeEngineError(ValueError):
    """Typed failure of the automatic runtime carrying a stable ``code``.

    Codes: ``not_started``, ``already_started``, ``stopped``,
    ``feed_scope_mismatch``, ``recovery_required_but_unavailable``,
    ``invalid_config``, ``registry_required``, ``lease_conflict``.
    """

    def __init__(self, code: str, message: str | None = None) -> None:
        self.code = str(code)
        super().__init__(message or code)


def _stamp(value: datetime) -> str:
    return instant_to_text(value)


def _state_from_text(value: Any) -> StrategyState | None:
    if not isinstance(value, str):
        return None
    try:
        return StrategyState(value)
    except ValueError:
        return None


class AutomaticPaperRuntime:
    """One automatic paper-trading runtime over a fixed configuration.

    The runtime owns the scheduler, the paper account book, the feed session
    and the G027 persistence facade.  Adapters are injected by the caller:
    the runtime never reads credentials and never constructs a provider.
    """

    def __init__(
        self,
        config: RuntimeConfig,
        *,
        registry: Any = None,
        adapter: Any = None,
        history_adapter: Any = None,
        aggregator: Any = None,
        stream_client: Any = None,
        db_path: str | Path | None = None,
        run_id: str | None = None,
        clock: Any | None = None,
        max_output_tokens: int = 4096,
        startup_history_seconds: int = 7 * 86400,
        start_at: datetime | str | None = None,
    ) -> None:
        if not isinstance(config, RuntimeConfig):
            raise RuntimeEngineError("invalid_config", "config must be a validated RuntimeConfig")
        if isinstance(startup_history_seconds, bool) or not isinstance(startup_history_seconds, int) or startup_history_seconds < 1:
            raise RuntimeEngineError("invalid_config", "startup_history_seconds must be positive")
        if isinstance(max_output_tokens, bool) or not isinstance(max_output_tokens, int) or max_output_tokens < 1:
            raise RuntimeEngineError("invalid_config", "max_output_tokens must be positive")
        self._config = config
        self._registry = registry if registry is not None else self._default_registry()
        self._adapter = adapter
        self._history_adapter = history_adapter
        self._aggregator = aggregator
        self._stream_client = stream_client
        self._db_path = str(db_path if db_path is not None else config.database_path)
        self._run_id = run_id if run_id is not None else uuid.uuid4().hex
        self._clock = clock if clock is not None else SystemClock()
        self._max_output_tokens = max_output_tokens
        self._startup_history_seconds = startup_history_seconds
        self._start_at = start_at

        self._db: Any = None
        self._store: RuntimeStore | None = None
        self._sink: RuntimeSink | None = None
        self._hydrator: RuntimeHydrator | None = None
        self._feed: FeedSession | None = None
        self._scheduler: StrategyScheduler | None = None
        self._book: PaperAccountBook | None = None

        self._started = False
        self._stopped = False
        self._stop_reason: str | None = None
        self._startup_report: dict[str, Any] | None = None
        self._step_index = 0
        self._provider_calls = 0
        self._fills = 0
        self._closes = 0
        self._expiries = 0
        self._rejections = 0
        self._errors: list[dict[str, Any]] = []
        self._stream_stop_report: dict[str, Any] | None = None

        # Provider calls run outside the runtime/SQLite thread.  Only
        # ``_drain_provider_results`` mutates scheduler, account, monitor and
        # persistence state; workers return immutable completion envelopes.
        self._provider_pool: _ProviderWorkerPool | None = None
        self._provider_tasks: dict[str, _ProviderTask] = {}
        self._provider_strategy_tasks: dict[str, set[str]] = {}
        self._deferred_provider_jobs: dict[str, _ProviderTask] = {}
        self._provider_condition = Condition()
        self._lifecycle_lock = RLock()

        self._current_view: Any = None
        self._view_second_events: dict[str, Any] = {}
        # FeedSession is only mutated by the runtime thread.  Keep the
        # runtime acceptance instant beside each retained frozen event so a
        # result drained after a later ingest cannot use that later event as
        # an earlier provider-completion reference.
        self._second_accepted_at: dict[str, datetime] = {}
        self._second_accept_order: dict[str, float] = {}
        self._history_result: Any = None
        self._last_view_id: str | None = None
        self._view_ok = False
        self._status_facts: dict[str, bool] = {}

        self._recovery_generation = 0
        self._recovered_flag = False
        self._recovery_evidence: Mapping[str, Any] | None = None
        self._recovery_pending = False
        self._recovery_result: dict[str, Any] | None = None
        self._recovery_retry_at: datetime | None = None
        self._recovery_retry_count = 0
        self._last_recovery_signature: tuple[Any, ...] | None = None

        self._pending_monitors: dict[str, Any] = {}
        self._exit_monitors: dict[str, Any] = {}
        self._created_envelopes: dict[str, dict[str, Any]] = {}
        self._pending_created_states: dict[str, dict[str, Any]] = {}
        self._persisted_pending_creates: set[str] = set()
        self._persisted_pending_fills: set[tuple[str, str]] = set()
        self._persisted_pending_expiries: set[tuple[str, str]] = set()
        self._persisted_closes: set[tuple[str, str]] = set()
        self._closing_trades: dict[tuple[str, str], bool] = {}
        self._current_pending_source: dict[str, Any] = {}
        self._current_exit_source: dict[str, Any] = {}

        self._processed_end_at: str | None = None
        self._live_cutover_at: str | None = None
        self._committed_transitions: dict[str, int] = {}
        self._committed_timer_kinds: dict[str, frozenset[str]] = {}
        self._committed_transition_keys: dict[str, set[tuple[Any, Any, Any]]] = {}
        self._recent_events: list[dict[str, Any]] = []

    # ------------------------------------------------------------------
    # Construction helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _default_registry() -> Any:
        try:
            from pa_engine.strategies.knowledge import load_default_registry

            knowledge_dir = str(Path(__file__).resolve().parents[2] / "knowledge")
            return load_default_registry(knowledge_dir)
        except Exception as exc:
            raise RuntimeEngineError("registry_required", f"registry is required and the default could not be loaded: {exc}") from exc

    @property
    def clock(self) -> Any:
        return self._clock

    @property
    def config(self) -> RuntimeConfig:
        return self._config

    @property
    def feed(self) -> FeedSession | None:
        return self._feed

    @property
    def scheduler(self) -> StrategyScheduler | None:
        return self._scheduler

    @property
    def book(self) -> PaperAccountBook | None:
        return self._book

    def _now(self, value: datetime | str | None = None) -> datetime:
        if value is None:
            value = self._clock.now()
        return utc_instant(value)

    def _db_clock(self) -> datetime:
        if callable(self._clock):
            return self._clock()
        return self._clock.now()

    def _record_error(self, message: str, *, code: str | None = None) -> None:
        entry = {"message": message, "step_index": self._step_index}
        if code is not None:
            entry["code"] = code
        self._errors.append(entry)

    # ------------------------------------------------------------------
    # Startup / restart
    # ------------------------------------------------------------------

    def start(self) -> dict[str, Any]:
        """Start the runtime and release partial startup resources on error."""
        try:
            return self._start_impl()
        except BaseException:
            self._cleanup_failed_start()
            raise

    def _cleanup_failed_start(self) -> None:
        """Close a partially initialized feed/database without changing retryability."""
        self._started = False
        if self._feed is not None:
            try:
                self._stream_stop_report = self._feed.stop_stream()
            except Exception:
                pass
        if self._db is not None:
            try:
                self._db.close(release_lease=True)
            except Exception:
                pass
            finally:
                self._db = None

    def _start_impl(self) -> dict[str, Any]:
        """Open the database, restore durable state and prepare the first view.

        Idempotent: a second call while the runtime is started returns the
        stored startup report.  After ``stop``, a new engine instance may
        reopen the same database because the lease was released.
        """
        if self._started:
            return dict(self._startup_report or self.status())
        if self._stopped:
            raise RuntimeEngineError("stopped", "runtime was stopped; construct a new instance to restart")
        clock_now = self._now()
        errors: list[dict[str, Any]] = []
        try:
            db = _open_runtime_database(self._db_path, run_id=self._run_id, mode=self._config.mode, clock=self._db_clock)
        except LeaseConflictError as exc:
            raise RuntimeEngineError("lease_conflict", f"runtime database is held by another run: {exc}") from exc
        self._db = db
        self._store = RuntimeStore(db)
        self._sink = RuntimeSink(self._store)
        self._hydrator = RuntimeHydrator(self._store, registry=self._registry)
        recovery = RuntimeRecovery(self._store, db)

        enabled_ids = list(self._registry.strategy_ids)
        restored = recovery.startup_sequence(enabled_ids, now=clock_now)
        self._book = self._hydrator.hydrate_account_book(enabled_ids)

        # -- feed session and history ------------------------------------
        session_feed = FeedSession(
            symbol=self._config.symbol,
            timeframe=self._config.timeframe_mapping(),
            history_adapter=self._history_adapter,
            aggregator=self._aggregator,
            stream_client=self._stream_client,
            session=self._config.session,
            market_feed=self._config.market_feed,
            adjustment_policy=self._config.adjustment_policy,
            minimum_closed_bars=self._config.minimum_closed_bars,
            stale_after_seconds=self._config.stale_after_seconds,
            clock=self._clock,
        )
        self._feed = session_feed
        # A persisted execution cursor means this is a restart seam.  Arm
        # the feed gate before connecting the socket so the consumer cannot
        # reduce a buffered/replayed frame in the small window before the
        # runtime finishes wiring the restart recovery.
        cursor = self._sink.get_cursor(self._config.symbol)
        cutover_target: datetime | None = None
        if cursor is not None and cursor.get("last_second_end_at"):
            cutover_target = instant_from_text(cursor["last_second_end_at"])
            self._processed_end_at = cursor["last_second_end_at"]
            try:
                session_feed.mark_connection_gap(
                    start_at=_stamp(cutover_target),
                    reason="restart_recovery_required",
                )
            except FeedSessionError as exc:
                errors.append({"message": str(exc), "code": exc.code})
        stream_status = session_feed.start_stream()
        if not stream_status.get("ok"):
            client_status = stream_status.get("client_status")
            outcome = client_status.get("outcome") if isinstance(client_status, Mapping) else None
            code = outcome.get("kind") if isinstance(outcome, Mapping) else stream_status.get("error")
            errors.append(
                {
                    "message": f"stream start failed: {stream_status.get('error') or code}",
                    "code": code,
                    "stream": stream_status.get("stream"),
                }
            )

        backfill_start = self._start_at if self._start_at is not None else clock_now - timedelta(seconds=self._startup_history_seconds)
        try:
            if cutover_target is None:
                history_result = session_feed.backfill(start_at=backfill_start, end_at=clock_now)
            else:
                # The history window ends just past the persisted cursor so the
                # G021 half-open cutover equals the cursor exactly; live seconds
                # must therefore start after it (pre-cursor seconds are dropped
                # by the cutover rule and skipped by the engine cursor filter).
                history_result = session_feed.backfill(start_at=backfill_start, end_at=cutover_target + timedelta(seconds=1))
            self._history_result = history_result
        except FeedSessionError as exc:
            errors.append({"message": str(exc), "code": exc.code})
            self._history_result = None

        # -- scheduler seeding -------------------------------------------
        scheduler = self._seed_scheduler(enabled_ids, restored.strategy_states, restored.timers)
        self._scheduler = scheduler
        self._committed_transitions = {
            row["strategy_id"]: row.get("transition_count", 0) for row in restored.strategy_states
        }

        # -- restored monitor wrapping ------------------------------------
        for row in restored.pending_entries:
            if row.get("status") != "pending":
                continue
            self._wrap_restored_pending(row, errors)
        for row in restored.trades:
            if row.get("status") != "open":
                continue
            self._wrap_restored_exit(row, errors)

        # -- restart reconciliation ---------------------------------------
        needs_recovery = bool(
            restored.live_blocked
            or (cursor is not None and cursor.get("last_second_end_at") is not None)
            or self._hydrator.live_blocked()
        )
        if needs_recovery:
            session_feed.reset_seconds()
            self._mark_restart_gap(cutover_target, clock_now)
            self._run_recovery(clock_now, errors, startup=True)

        # -- first view ---------------------------------------------------
        if self._current_view is None:
            try:
                view = session_feed.build_view(
                    history_result=self._history_result,
                    now_at=clock_now,
                    connection_gaps=session_feed.connection_gaps,
                    recovery_generation=self._recovery_generation,
                    recovered=self._recovered_flag,
                    recovery_evidence=self._recovery_evidence,
                )
                self._adopt_view(view)
            except FeedSessionError as exc:
                errors.append({"message": str(exc), "code": exc.code})
                self._view_ok = False

        if self._current_view is None:
            # No market view at all (for example unusable startup history):
            # the runtime stays in its pre-start state and records the error.
            try:
                self._stream_stop_report = session_feed.stop_stream()
            except Exception:
                pass
            self._started = False
            try:
                live_blocked = self._sink.is_live_blocked(self._config.symbol)
            except Exception:
                live_blocked = True
            try:
                if self._db is not None:
                    self._db.close(release_lease=True)
            except Exception as exc:
                errors.append({"message": "startup database cleanup failed", "code": "startup_cleanup_failed"})
                self._record_error(f"startup database cleanup failed: {type(exc).__name__}", code="startup_cleanup_failed")
            finally:
                # A failed startup is retryable on the same object, but it
                # must not retain a closed connection or singleton lease.
                self._db = None
            report = self._startup_report_for(
                restored_accounts=len(restored.accounts),
                restored_pending=len([row for row in restored.pending_entries if row.get("status") == "pending"]),
                restored_open_trades=len([row for row in restored.trades if row.get("status") == "open"]),
                due_work=[{"strategy_id": item.strategy_id, "kind": item.kind} for item in restored.due_once],
                live_blocked=live_blocked,
                errors=errors,
            )
            self._startup_report = report
            self._errors.extend(errors)
            return report

        # -- scheduler start ----------------------------------------------
        scheduler.start(self._view_mapping())
        self._started = True
        self._last_view_id = self._current_view.view_id

        # -- initial durable states ---------------------------------------
        # On a fresh database the very first transitions (for example the
        # market_wait entry produced by ``scheduler.start`` against a
        # non-executable startup view) are committed by the regular sweep.
        self._persist_sweep(clock_now)
        if not restored.strategy_states:
            for sid in enabled_ids:
                if sid not in self._committed_transitions:
                    record = scheduler.get(sid)
                    if record is not None:
                        self._committed_transitions[sid] = record.transition_count

        report = self._startup_report_for(
            restored_accounts=len(restored.accounts),
            restored_pending=len([row for row in restored.pending_entries if row.get("status") == "pending"]),
            restored_open_trades=len([row for row in restored.trades if row.get("status") == "open"]),
            due_work=[{"strategy_id": item.strategy_id, "kind": item.kind} for item in restored.due_once],
            live_blocked=self._sink.is_live_blocked(self._config.symbol),
            errors=errors,
        )
        self._startup_report = report
        self._errors.extend(errors)
        return report

    resume = start

    def _seed_scheduler(self, enabled_ids: list[str], state_rows: tuple[Any, ...], timer_rows: tuple[Any, ...]) -> StrategyScheduler:
        scheduler = StrategyScheduler(
            clock=self._clock,
            queue_capacity=self._config.queue_capacity,
            max_in_flight=self._config.max_in_flight,
            model_executor=None,
            model_request_factory=None,
            decision_finalizer=None,
            open_requester=self._open_requester,
            cancel_pending=self._cancel_pending,
            on_pending_expiry=self._on_pending_expiry_cb,
            on_pending_fill=self._on_pending_fill_cb,
            on_position_close=self._on_position_close_cb,
            registry=self._registry,
            execution_policy=self._config.execution_policy,
        )
        rows = {row["strategy_id"]: row for row in state_rows}
        timers_by_sid: dict[str, list[dict[str, Any]]] = {}
        for timer in timer_rows:
            timers_by_sid.setdefault(timer["strategy_id"], []).append(timer)
        for sid in enabled_ids:
            loaded = self._registry.load_packet(sid)
            if loaded.code != "ok":
                raise RuntimeEngineError("invalid_config", f"strategy packet for {sid} could not be loaded: {loaded.code}")
            exhausted = False
            account_row = self._store.get_account(sid)
            if account_row is not None:
                exhausted = bool(account_row.get("exhausted", 0))
            record = scheduler.register_strategy(
                {
                    "strategy_id": sid,
                    "profile_ref": loaded.profile_ref,
                    "packet": loaded.packet,
                    "enabled": not exhausted,
                    "exhausted": exhausted,
                }
            )
            row = rows.get(sid)
            if row is not None:
                self._apply_restored_row(record, row)
            for index, timer in enumerate(timers_by_sid.get(sid, ())):
                self._seed_timer_token(record, timer, index)
            # A live pending episode owns the G023 expiry token even though
            # the bridge removes it from the scheduler once claimed (the
            # durable timer row is cleared with it).  Re-seed the token for
            # binding when the restored row is still waiting.
            if record.pending_id is not None and "pending_expiry" not in record.timer_tokens:
                state = _state_from_text((row or {}).get("state"))
                waiting = state == StrategyState.WAITING_ENTRY or (
                    state == StrategyState.MARKET_WAIT and record.intended_resume_state == StrategyState.WAITING_ENTRY
                )
                if waiting and record.expires_at is not None:
                    token = TimerToken(f"tmr_rst_x_{sid}", sid, record.generation, "pending_expiry", record.expires_at)
                    record.timer_tokens["pending_expiry"] = token
        return scheduler

    def _apply_restored_row(self, record: Any, row: Mapping[str, Any]) -> None:
        state = _state_from_text(row.get("state"))
        if state is not None:
            record.state = state
        record.generation = row.get("generation", record.generation)
        record.enabled = bool(row.get("enabled", record.enabled))
        record.exhausted = bool(row.get("exhausted", record.exhausted))
        record.pending_id = row.get("pending_id")
        record.open_position_id = row.get("open_position_id")
        record.open_position_generation = row.get("open_position_generation")
        intended = _state_from_text(row.get("intended_resume_state"))
        record.intended_resume_state = intended
        record.blocked_reason = row.get("blocked_reason")
        record.rejected_immediate_view_id = row.get("rejected_immediate_view_id")
        record.current_view_id = row.get("current_view_id")
        record.last_closed_snapshot_id = row.get("last_closed_snapshot_id")
        record.transition_count = row.get("transition_count", 0)
        record.provider_retry_count = row.get("provider_retry_count", 0)
        record.semantic_retry_count = row.get("semantic_retry_count", 0)
        retry_key = row.get("semantic_retry_key")
        record.semantic_retry_key = tuple(retry_key) if isinstance(retry_key, (list, tuple)) and len(retry_key) == 2 else None
        for field in ("decision_accepted_at", "next_due_at", "expires_at"):
            value = row.get(field)
            if isinstance(value, str) and value:
                setattr(record, field, instant_from_text(value))
        instruction = row.get("latest_instruction")
        if isinstance(instruction, Mapping):
            record.latest_instruction = copy.deepcopy(dict(instruction))

    def _seed_timer_token(self, record: Any, timer: Mapping[str, Any], index: int) -> None:
        kind = timer.get("kind")
        if kind not in _TIMER_KINDS:
            return
        generation = timer.get("generation", record.generation)
        try:
            due_at = instant_from_text(timer["due_at"])
        except Exception:
            return
        token = TimerToken(f"tmr_rst_{index}", record.strategy_id, generation, kind, due_at)
        token.active = bool(timer.get("active", 1)) and not bool(timer.get("fired", 0))
        record.timer_tokens[kind] = token

    def _wrap_restored_pending(self, row: Mapping[str, Any], errors: list[dict[str, Any]]) -> None:
        sid = row["strategy_id"]
        record = self._scheduler.get(sid)
        if record is None:
            errors.append({"message": f"pending row for unknown strategy {sid}", "code": "monitor_bind_failed"})
            return
        try:
            state = self._hydrator.restore_pending_state(row)
            monitor = self._hydrator.wrap_pending_monitor(
                state,
                scheduler=self._scheduler,
                book=self._book,
                on_pending_fill=self._on_pending_fill_cb,
                on_pending_expiry=self._on_pending_expiry_cb,
                recovery_request=None,
                expected_source_request_hash=None,
                expected_recovery_cursor=None,
                enabled=record.enabled,
                exhausted=record.exhausted,
            )
            self._pending_monitors[sid] = monitor
            created = self._stored_pending_envelope(row["pending_id"])
            if created is not None:
                self._created_envelopes[row["pending_id"]] = created
                self._persisted_pending_creates.add(row["pending_id"])
            self._pending_created_states[row["pending_id"]] = copy.deepcopy(state.to_dict())
        except RuntimePersistenceError as exc:
            errors.append({"message": f"restored pending monitor failed: {exc}", "code": exc.code})

    def _stored_pending_envelope(self, pending_id: str) -> dict[str, Any] | None:
        """Return the original ``pending_entry_created`` envelope from storage.

        The created event is content-bound to the episode's original state
        hash; replaying it with the terminal filled state moves the row to
        ``filled`` without fabricating a second event (G027 contract).
        """
        for row in self._store.list_events(event_type="pending_entry_created"):
            payload = row.get("envelope", {}).get("payload", {}) if isinstance(row.get("envelope"), Mapping) else {}
            if payload.get("pending_id") == pending_id:
                return row["envelope"]
        return None

    def _wrap_restored_exit(self, row: Mapping[str, Any], errors: list[dict[str, Any]]) -> None:
        sid = row["strategy_id"]
        record = self._scheduler.get(sid)
        if record is None:
            errors.append({"message": f"open trade for unknown strategy {sid}", "code": "monitor_bind_failed"})
            return
        try:
            state = self._hydrator.restore_exit_state(row)
            monitor = self._hydrator.wrap_exit_monitor(
                state,
                scheduler=self._scheduler,
                book=self._book,
                on_position_close=self._on_position_close_cb,
                recovery_request=None,
                expected_source_request_hash=None,
                enabled=record.enabled,
                exhausted=record.exhausted,
            )
            self._exit_monitors[sid] = monitor
        except RuntimePersistenceError as exc:
            errors.append({"message": f"restored exit monitor failed: {exc}", "code": exc.code})

    def _mark_restart_gap(self, cutover_target: datetime | None, now: datetime) -> None:
        """Record one confirmed finite connection gap at the cursor.

        The gap marks every active monitor so G019 recovery bars are consumed
        (G025/G026 only act on an overlapping gap) and ends at the recovery
        instant so post-recovery live seconds are eligible again.
        """
        if cutover_target is None:
            return
        gap_start = _stamp(cutover_target)
        gap_end = _stamp(now)
        existing = any(gap.get("start_at") == gap_start for gap in self._feed.connection_gaps)
        if not existing:
            try:
                self._feed.mark_connection_gap(start_at=gap_start, end_at=gap_end, reason="restart_recovery_required")
            except Exception:
                return
        for monitor in self._pending_monitors.values():
            try:
                monitor.on_gap(start_at=gap_start, end_at=gap_end, reason="restart_recovery_required")
            except Exception:
                pass
        for monitor in self._exit_monitors.values():
            try:
                monitor.on_gap(start_at=gap_start, end_at=gap_end, reason="restart_recovery_required")
            except Exception:
                pass

    def _startup_report_for(self, *, restored_accounts: int, restored_pending: int, restored_open_trades: int, due_work: list[dict[str, Any]], live_blocked: bool, errors: list[dict[str, Any]]) -> dict[str, Any]:
        stream = self._feed.status().get("stream", {}) if self._feed is not None else {}
        return {
            "run_id": self._run_id,
            "mode": self._config.mode,
            "execution_policy": self._config.execution_policy,
            "symbol": self._config.symbol,
            "timeframe": self._config.timeframe_mapping(),
            "feed_key": self._config.feed_key,
            "paper_only": self._config.paper_only_label(),
            "market_data_usage": self._config.market_data_usage,
            "restored_accounts": restored_accounts,
            "restored_pending": restored_pending,
            "restored_open_trades": restored_open_trades,
            "due_work": due_work,
            "live_blocked": live_blocked,
            "view_id": self._current_view.view_id if self._current_view is not None else None,
            "view_executable": bool(self._current_view is not None and self._current_view.executable),
            "errors": errors,
            "stream": stream,
        }

    # ------------------------------------------------------------------
    # Views
    # ------------------------------------------------------------------

    def _view_mapping(self, view: Any = None) -> dict[str, Any]:
        current = view if view is not None else self._current_view
        mapping = current.to_dict()
        ref = current.to_decision_ref()
        if isinstance(ref, Mapping):
            mapping["market_view_ref"] = copy.deepcopy(dict(ref))
        else:
            mapping["market_view_ref"] = None
        return mapping

    def _adopt_view(self, view: Any) -> None:
        self._current_view = view
        self._view_ok = True
        retained = self._feed.frozen_events
        self._view_second_events = {event.event_id: event for event in retained}
        retained_ids = {event.event_id for event in retained}
        self._second_accepted_at = {
            event_id: accepted_at
            for event_id, accepted_at in self._second_accepted_at.items()
            if event_id in retained_ids
        }
        self._second_accept_order = {
            event_id: accepted_order
            for event_id, accepted_order in self._second_accept_order.items()
            if event_id in retained_ids
        }

    def _record_accepted_seconds(self, events: list[Any], accepted_at: datetime) -> None:
        """Record main-thread feed acceptance, never worker observation."""
        for event in events:
            if getattr(event, "status", None) != "frozen":
                continue
            event_id = getattr(event, "event_id", None)
            if isinstance(event_id, str) and event_id:
                self._second_accepted_at.setdefault(event_id, accepted_at)
                self._second_accept_order.setdefault(event_id, time.monotonic())

    def _second_event_for_ref(self, reference: Mapping[str, Any] | None) -> Any | None:
        """Resolve and exact-match one trusted G020 second-event reference."""
        if self._feed is None or not isinstance(reference, Mapping):
            return None
        event_id = reference.get("event_id")
        event_hash = reference.get("event_hash") or reference.get("content_hash")
        if not isinstance(event_id, str) or not isinstance(event_hash, str):
            return None
        event = self._view_second_events.get(event_id)
        if event is None:
            event = next((item for item in self._feed.frozen_events if item.event_id == event_id), None)
        if event is None or getattr(event, "status", None) != "frozen":
            return None
        mapping = event.to_dict()
        if mapping.get("event_hash") != event_hash:
            return None
        for key in ("start_at", "end_at"):
            if reference.get(key) is not None and reference.get(key) != mapping.get(key):
                return None
        requested_price = reference.get("reference_price")
        if requested_price is not None and requested_price != mapping.get("close"):
            return None
        return event

    def _response_second_event(self, completed_at: datetime, completed_order: float | None = None) -> dict[str, Any] | None:
        """Select the latest G020 second accepted no later than completion.

        The current view remains the quality gate, while the event registry
        and acceptance ledger provide the response-time cutoff.  In
        particular, neither the claim-time view nor a feed event ingested
        after ``completed_at`` is an acceptable fallback.
        """
        view = self._current_view
        if self._feed is None or view is None or not self._view_ok or self._recovery_pending:
            return None
        readiness = view.readiness
        if any(
            bool(getattr(readiness, name, False))
            for name in ("connection_gap", "partial", "insufficient_history", "recovery_required")
        ):
            return None
        if not bool(getattr(readiness, "frozen_second", False)) or not _in_regular_session(completed_at):
            return None
        candidates: list[tuple[datetime, str, Any]] = []
        for event in self._feed.frozen_events:
            if getattr(event, "status", None) != "frozen":
                continue
            accepted_at = self._second_accepted_at.get(getattr(event, "event_id", ""))
            if accepted_at is None or accepted_at > completed_at:
                continue
            accepted_order = self._second_accept_order.get(getattr(event, "event_id", ""))
            if completed_order is None:
                if accepted_at >= completed_at:
                    continue
            elif accepted_order is None or accepted_order >= completed_order:
                continue
            try:
                end_at = utc_instant(event.end_at)
                watermark = utc_instant(event.execution_watermark or event.end_at)
            except Exception:
                continue
            if watermark > completed_at or end_at > completed_at:
                continue
            age = (completed_at - end_at).total_seconds()
            if age < 0 or age > self._config.stale_after_seconds:
                continue
            quality = getattr(event, "quality", {})
            if isinstance(quality, Mapping) and (
                quality.get("status") != "ok" or bool(quality.get("connection_gap"))
            ):
                continue
            candidates.append((end_at, event.event_id, event))
        if not candidates:
            return None
        event = max(candidates, key=lambda item: (item[0], item[1]))[2]
        second = event.to_dict()
        second["reference_price"] = second.get("close")
        return second

    def _compute_status_facts(self, instant: datetime, view: Any = None) -> dict[str, bool]:
        """Derive the six execution-quality facts for the current instant.

        Session membership and staleness are wall-clock facts re-derived
        every step (G021 computes them from the build instant); the remaining
        facts come from the current stable view and are fail-closed.
        """
        view = self._current_view if view is None else view
        if view is None:
            return {
                "regular_session": False,
                "fresh": False,
                "complete": False,
                "non_gapped": False,
                "sufficient": False,
                "frozen_second": False,
            }
        readiness = view.readiness
        latest = view.latest_second
        fresh = True
        if latest is not None and isinstance(latest, Mapping) and latest.get("end_at"):
            fresh = instant - utc_instant(latest["end_at"]) <= timedelta(seconds=self._config.stale_after_seconds)
        elif latest is None:
            fresh = False
        return {
            "regular_session": _in_regular_session(instant),
            "fresh": fresh,
            "complete": not bool(getattr(readiness, "partial", False)),
            "non_gapped": not bool(getattr(readiness, "connection_gap", False)),
            "sufficient": not bool(getattr(readiness, "insufficient_history", False)),
            "frozen_second": bool(getattr(readiness, "frozen_second", False)),
        }

    def _view_executable_now(self, view: Any = None, *, instant: datetime | None = None) -> bool:
        """Fail-closed execution gate on the current shared view."""
        current = self._current_view if view is None else view
        if self._recovery_pending or not self._view_ok or current is None:
            return False
        moment = self._now() if instant is None else instant
        facts = self._status_facts if view is None and self._status_facts else self._compute_status_facts(moment, current)
        return all(bool(facts.get(name)) for name in ("regular_session", "fresh", "complete", "non_gapped", "sufficient", "frozen_second"))

    def _filter_events(self, events: list[Any]) -> list[Any]:
        cutovers = [value for value in (self._processed_end_at, self._live_cutover_at) if value is not None]
        if not cutovers:
            return events
        cutoff = max((instant_from_text(value) for value in cutovers), default=None)
        if cutoff is None:
            return events
        cutoff_day = _session_day(cutoff)
        accepted = [
            event
            for event in events
            if utc_instant(event.end_at) > cutoff or _session_day(utc_instant(event.end_at)) > cutoff_day
        ]
        return accepted

    # ------------------------------------------------------------------
    # Scheduler callbacks (injected seams)
    # ------------------------------------------------------------------

    def _open_requester(self, strategy_id: str, instruction: Mapping[str, Any], *, generation: int, idempotency_key: str) -> OpenRequestResult:
        """Open an immediate paper position against the trusted second ref.

        A fresh-price drift rejects without changing funds and records the
        exact response-time execution-second id/hash; exhausted capital
        blocks the strategy.
        """
        view = self._current_view
        if view is None or not self._view_executable_now(view, instant=self._now()):
            return OpenRequestResult.rejected_result(reason="no_frozen_second")
        requested = instruction.get("second_event_ref")
        event = self._second_event_for_ref(requested if isinstance(requested, Mapping) else None)
        if event is None:
            return OpenRequestResult.rejected_result(reason="no_frozen_second")
        now = self._now()
        second = event.to_dict()
        fill_ref = {
            "reference_price": str(second["close"]),
            "event_id": second["event_id"],
            "revision_id": second.get("revision_id"),
            "start_at": second["start_at"],
            "end_at": second["end_at"],
        }
        # A newer current second may have arrived after the provider
        # completed.  It is a fresh-price drift, not permission to replace
        # the trusted response-time lineage with the drain-time event.
        current_ref = view.latest_second if isinstance(view.latest_second, Mapping) else None
        current_event = self._second_event_for_ref(current_ref)
        if current_event is None:
            return OpenRequestResult.rejected_result(reason="no_frozen_second")
        if current_event.event_id != event.event_id or current_event.event_hash != event.event_hash:
            current = current_event.to_dict()
            reason = (
                "immediate_price_drift; "
                f"second_event_id={second['event_id']} second_event_hash={second.get('event_hash') or second.get('content_hash')} "
                f"actual_second_event_id={current['event_id']} actual_second_event_hash={current.get('event_hash') or current.get('content_hash')}"
            )
            try:
                self._sink.commit_fill_rejected(
                    strategy_id,
                    generation,
                    reason=reason,
                    occurred_at=now,
                )
            except Exception as exc:
                self._record_error(f"fill rejection persistence failed for {strategy_id}: {exc}", code="fill_rejected_persistence_failed")
            self._rejections += 1
            self._recent_events.append({"kind": "fill_rejected", "strategy_id": strategy_id, "at": _stamp(now)})
            return OpenRequestResult.rejected_result(fresh_price=True, reason="invalid_level_order")
        account_before = self._book.get_account(strategy_id)
        balance_before = account_before.balance if account_before is not None else None
        requested_price = requested.get("reference_price") if isinstance(requested, Mapping) else None
        try:
            result = self._book.open_trade(
                strategy_id,
                self._config.symbol,
                instruction["direction"],
                None,
                instruction["stop_price"],
                instruction["target_price"],
                immediate=True,
                fill_reference=fill_ref,
                requested_entry_price=requested_price,
                idempotency_key=idempotency_key,
                model_payload=None,
                fees=ZERO,
                slippage=ZERO,
            )
        except Exception as exc:
            self._record_error(f"open request failed for {strategy_id}: {exc}", code="open_request_failed")
            return OpenRequestResult.rejected_result(reason="open_request_failed")
        if result.accepted:
            trade = result.trade
            try:
                exit_state = make_exit_state(
                    trade,
                    entry_fill_interval=(second["start_at"], second["end_at"]),
                    entry_filled_at=second["end_at"],
                    generation=generation,
                    market_feed=self._config.market_feed,
                )
                self._sink.commit_fill(exit_state.to_dict(), market_ref=make_second_ref(second), occurred_at=now)
                if balance_before is not None and balance_before != result.account.balance:
                    self._sink.commit_account_change(
                        strategy_id,
                        balance_before=balance_before,
                        balance_after=result.account.balance,
                        generation=generation,
                        occurred_at=now,
                    )
            except Exception as exc:
                self._record_error(f"immediate fill persistence failed for {strategy_id}: {exc}", code="fill_persistence_failed")
            self._fills += 1
            self._recent_events.append({"kind": "fill", "strategy_id": strategy_id, "trade_id": trade.trade_id, "at": _stamp(now)})
            return OpenRequestResult.accepted_result(position_id=trade.trade_id)
        if result.fresh_price_rejected:
            reason = f"{result.code}; second_event_id={second['event_id']} second_event_hash={second.get('event_hash') or second.get('content_hash')}"
            try:
                self._sink.commit_fill_rejected(
                    strategy_id,
                    generation,
                    reason=reason,
                    occurred_at=now,
                )
            except Exception as exc:
                self._record_error(f"fill rejection persistence failed for {strategy_id}: {exc}", code="fill_rejected_persistence_failed")
            self._rejections += 1
            self._recent_events.append({"kind": "fill_rejected", "strategy_id": strategy_id, "at": _stamp(now)})
            return OpenRequestResult.rejected_result(fresh_price=True, reason=result.code)
        if result.exhausted:
            try:
                self._sink.commit_account_exhausted(
                    strategy_id,
                    balance=result.account.balance if result.account is not None else ZERO,
                    reason=result.code,
                    generation=generation,
                    occurred_at=now,
                )
            except Exception as exc:
                self._record_error(f"exhausted persistence failed for {strategy_id}: {exc}", code="exhausted_persistence_failed")
            return OpenRequestResult.exhausted_result(reason=result.code)
        self._rejections += 1
        return OpenRequestResult.rejected_result(reason=result.code)

    def _cancel_pending(self, strategy_id: str, pending_id: str, *, generation: int) -> Any:
        monitor = self._pending_monitors.pop(strategy_id, None)
        if monitor is None:
            return False
        try:
            # The scheduler clears the record's pending id before invoking
            # this callback, so the G025 disable must use the stale-record
            # reason to let the G024 expiry still commit.
            outcome = monitor.disable(reason="scheduler generation is no longer active")
        except Exception as exc:
            self._record_error(f"pending cancellation failed for {strategy_id}: {exc}", code="cancel_failed")
            return False
        try:
            self._sink.commit_pending(monitor.state.to_dict(), event="cancelled", occurred_at=self._now())
        except Exception as exc:
            self._record_error(f"pending cancellation persistence failed for {strategy_id}: {exc}", code="cancel_persist_failed")
        self._recent_events.append({"kind": "pending_cancelled", "strategy_id": strategy_id, "pending_id": pending_id})
        return bool(outcome.accepted)

    def _on_pending_fill_cb(self, strategy_id: str, pending_id: str, *, generation: int | None = None, position_id: str | None = None, **_: Any) -> bool:
        """Persist one filled pending episode exactly once (fill or recovery).

        The callback is invoked by the G025 reducer bridge and the G023
        scheduler; the ``(strategy_id, pending_id)`` guard keeps the
        persistence single-shot.
        """
        key = (strategy_id, pending_id)
        if key in self._persisted_pending_fills:
            return True
        monitor = self._pending_monitors.get(strategy_id)
        if monitor is None or not monitor.state.terminal or monitor.state.status != "filled":
            return False
        now = self._now()
        if not self._persist_pending_created(monitor, now):
            return False
        try:
            self._sink.commit_pending(
                monitor.state.to_dict(),
                event="created",
                envelope=self._created_envelopes.get(pending_id),
                occurred_at=now,
            )
        except Exception as exc:
            self._record_error(f"pending fill row persistence failed for {strategy_id}: {exc}", code="pending_fill_persist_failed")
            return False
        trade = self._book.get_account(strategy_id).open_trade if self._book.get_account(strategy_id) is not None else None
        source = self._current_pending_source.get(strategy_id)
        interval, filled_at, market_ref = self._fill_interval_for(source, monitor)
        if trade is None:
            self._record_error(f"pending fill has no open trade for {strategy_id}", code="fill_persist_failed")
            return False
        try:
            exit_state = make_exit_state(
                trade,
                entry_fill_interval=interval,
                entry_filled_at=filled_at,
                generation=monitor.state.generation,
                market_feed=self._config.market_feed,
                decision_lineage=monitor.state.decision_lineage.to_dict(),
            )
            self._sink.commit_fill(exit_state.to_dict(), market_ref=market_ref, occurred_at=now)
            self._create_exit_monitor_for(exit_state, strategy_id)
        except Exception as exc:
            self._record_error(f"pending fill persistence failed for {strategy_id}: {exc}", code="fill_persist_failed")
            return False
        self._persisted_pending_fills.add(key)
        self._fills += 1
        self._pending_monitors.pop(strategy_id, None)
        self._recent_events.append({"kind": "fill", "strategy_id": strategy_id, "pending_id": pending_id, "at": _stamp(now)})
        return True

    def _on_pending_expiry_cb(self, strategy_id: str, pending_id: str, *, generation: int | None = None, **_: Any) -> bool:
        key = (strategy_id, pending_id)
        if key in self._persisted_pending_expiries:
            return True
        monitor = self._pending_monitors.get(strategy_id)
        if monitor is None or monitor.state.status != "expired":
            return False
        now = self._now()
        try:
            self._sink.commit_pending(monitor.state.to_dict(), event="expired", occurred_at=now)
        except Exception as exc:
            self._record_error(f"pending expiry persistence failed for {strategy_id}: {exc}", code="expiry_persist_failed")
            return False
        self._persisted_pending_expiries.add(key)
        self._expiries += 1
        self._pending_monitors.pop(strategy_id, None)
        self._recent_events.append({"kind": "expiry", "strategy_id": strategy_id, "pending_id": pending_id, "at": _stamp(now)})
        return True

    def _on_position_close_cb(self, strategy_id: str, position_id: str, *, generation: int | None = None, reason: str | None = None, **_: Any) -> bool:
        """Record one close notification; persistence happens post-transition.

        The G026 bridge notifies the scheduler before the monitor state is
        replaced with its terminal settlement, so the engine defers the
        durable ``paper_close``/``account_changed`` commit until
        :meth:`_persist_exit_terminal` observes the terminal state (live
        fan-out or recovery path).
        """
        key = (strategy_id, position_id)
        if key in self._persisted_closes:
            return True
        self._closing_trades[key] = True
        return True

    def _persist_exit_terminal(self, strategy_id: str, monitor: Any) -> None:
        """Persist one terminal exit exactly once and drop the monitor."""
        key = (strategy_id, monitor.trade_id)
        if key in self._persisted_closes:
            self._exit_monitors.pop(strategy_id, None)
            return
        if not monitor.state.terminal:
            return
        now = self._now()
        try:
            market_ref = self._close_market_ref(monitor)
            self._sink.commit_close(monitor.state.to_dict(), market_ref=market_ref, occurred_at=now)
        except Exception as exc:
            self._record_error(f"close persistence failed for {strategy_id}: {exc}", code="close_persist_failed")
            return
        settlement = monitor.state.settlement
        exhausted = monitor.state.status == "exhausted" or bool(
            isinstance(settlement, Mapping) and settlement.get("exhausted")
        )
        if isinstance(settlement, Mapping) and settlement.get("balance_before") is not None and settlement.get("balance_after") is not None:
            try:
                self._sink.commit_account_change(
                    strategy_id,
                    balance_before=settlement["balance_before"],
                    balance_after=settlement["balance_after"],
                    generation=monitor.state.generation,
                    occurred_at=now,
                )
                if exhausted:
                    self._sink.commit_account_exhausted(
                        strategy_id,
                        balance=settlement["balance_after"],
                        reason=str(monitor.state.exit_reason or "over_loss_capped"),
                        generation=monitor.state.generation,
                        occurred_at=now,
                    )
            except Exception as exc:
                self._record_error(f"account persistence failed for {strategy_id}: {exc}", code="account_persist_failed")
                return
        self._persisted_closes.add(key)
        self._closes += 1
        self._exit_monitors.pop(strategy_id, None)
        self._recent_events.append({"kind": "close", "strategy_id": strategy_id, "position_id": monitor.trade_id, "at": _stamp(now)})

    def _close_market_ref(self, monitor: Any) -> dict[str, Any]:
        """Resolve the market reference for one terminal close.

        A live settlement uses the triggering frozen second; a recovery
        settlement uses the settling ``recovery_bar`` lineage; a stale or
        replayed settlement falls back to the current frozen second.
        """
        source = self._current_exit_source.get(monitor.strategy_id)
        if isinstance(source, Mapping) and source.get("start_at") and source.get("end_at"):
            if source.get("recovery_bar_id") or source.get("kind") == "recovery_bar":
                return make_recovery_bar_ref(source)
            return make_second_ref(source)
        lineage = monitor.state.recovery_lineage
        if isinstance(lineage, (list, tuple)) and lineage:
            item = lineage[-1]
            if isinstance(item, Mapping) and item.get("start_at") and item.get("end_at"):
                if item.get("recovery_bar_id"):
                    return make_recovery_bar_ref(item)
                return make_second_ref(item)
        view = self._current_view
        if view is not None and view.latest_second is not None:
            event = self._view_second_events.get(view.latest_second.get("event_id"))
            if event is not None:
                return make_second_ref(event.to_dict())
        raise RuntimeEngineError("close_reference_unavailable", "no close market reference is available")

    def _fill_interval_for(self, source: Any, monitor: Any) -> tuple[tuple[str, str], str, dict[str, Any]]:
        """Derive the exit-state fill interval and market ref for a fill.

        For a recovery fill the source is the consumed G019 bar mapping and
        the ref carries the typed ``recovery_bar`` no-path/minute lineage;
        otherwise the source is the triggering G020 frozen second.
        """
        if isinstance(source, Mapping) and source.get("start_at") and source.get("end_at"):
            interval = (str(source["start_at"]), str(source["end_at"]))
            if source.get("recovery_bar_id") or source.get("kind") == "recovery_bar":
                return interval, str(source["end_at"]), make_recovery_bar_ref(source)
            return interval, str(source["end_at"]), make_second_ref(source)
        lineage = monitor.state.recovery_lineage
        if isinstance(lineage, (list, tuple)) and lineage:
            item = lineage[-1]
            if isinstance(item, Mapping) and item.get("start_at") and item.get("end_at"):
                interval = (str(item["start_at"]), str(item["end_at"]))
                if item.get("recovery_bar_id"):
                    return interval, str(item["end_at"]), make_recovery_bar_ref(item)
                return interval, str(item["end_at"]), make_second_ref(item)
        view = self._current_view
        if view is not None and view.latest_second is not None:
            event = self._view_second_events.get(view.latest_second.get("event_id"))
            if event is not None:
                second = event.to_dict()
                return (second["start_at"], second["end_at"]), second["end_at"], make_second_ref(second)
        raise RuntimeEngineError("fill_reference_unavailable", "no fill market reference is available")

    # ------------------------------------------------------------------
    # Monitor creation
    # ------------------------------------------------------------------

    def _create_pending_monitor(self, record: Any, now: datetime) -> None:
        instruction = record.latest_instruction
        if not isinstance(instruction, Mapping):
            return
        try:
            monitor = PendingEntryMonitor.from_instruction(
                instruction,
                symbol=self._config.symbol,
                decision_accepted_at=record.decision_accepted_at,
                submitted_at=record.decision_accepted_at,
                generation=record.generation,
                pending_id=record.pending_id,
                market_feed=self._config.market_feed,
                expected_profile_ref=record.profile_ref,
                scheduler=self._scheduler,
                paper_account_book=self._book,
                fill_pending=self._book.fill_pending_entry,
                expire_pending=self._book.expire_pending_entry,
                cancel_pending=self._book.expire_pending_entry,
                on_pending_fill=self._on_pending_fill_cb,
                on_pending_expiry=self._on_pending_expiry_cb,
                recovery_request=None,
                expected_source_request_hash=None,
                expected_recovery_cursor=None,
                enabled=record.enabled,
                exhausted=record.exhausted,
            )
        except Exception as exc:
            # G025 already disables the strategy on a rejected creation; the
            # runtime records the typed error and does not crash the loop.
            self._record_error(f"pending monitor creation failed for {record.strategy_id}: {exc}", code="pending_create_failed")
            return
        self._pending_monitors[record.strategy_id] = monitor
        self._pending_created_states[monitor.pending_id] = copy.deepcopy(monitor.state.to_dict())
        self._persist_pending_created(monitor, now)
        self._recent_events.append({"kind": "pending_created", "strategy_id": record.strategy_id, "pending_id": monitor.pending_id})

    def _persist_pending_created(self, monitor: Any, occurred_at: datetime) -> bool:
        """Persist the original pending-created envelope before terminal fill.

        A provider/fill can complete in the same runtime step as monitor
        creation.  Retaining the original state and prepared envelope makes
        a retry after an uncertain SQLite write replay the exact G027 event
        instead of fabricating a new ``created`` event from terminal state.
        """
        pending_id = monitor.pending_id
        if pending_id in self._persisted_pending_creates:
            return True
        state = self._pending_created_states.get(pending_id)
        if state is None:
            state = copy.deepcopy(monitor.state.to_dict())
            self._pending_created_states[pending_id] = state
        envelope = self._created_envelopes.get(pending_id)
        try:
            if envelope is None:
                envelope = self._sink.prepare_pending(state, event="created", occurred_at=occurred_at)
                self._created_envelopes[pending_id] = envelope
            self._sink.commit_pending(state, event="created", envelope=envelope, occurred_at=occurred_at)
        except Exception as exc:
            self._record_error(
                f"pending creation persistence failed for {monitor.strategy_id}: {exc}",
                code="pending_create_persist_failed",
            )
            return False
        self._persisted_pending_creates.add(pending_id)
        return True

    def _create_exit_monitor(self, record: Any, now: datetime) -> None:
        """Create the exit monitor for an immediate fill (post-OPEN_POSITION)."""
        account = self._book.get_account(record.strategy_id)
        trade = account.open_trade if account is not None else None
        if trade is None:
            self._record_error(f"open trade missing for {record.strategy_id} while creating exit monitor", code="exit_monitor_no_trade")
            return
        instruction = record.latest_instruction
        requested = instruction.get("second_event_ref") if isinstance(instruction, Mapping) else None
        event = self._second_event_for_ref(requested if isinstance(requested, Mapping) else None)
        if event is None and not isinstance(requested, Mapping):
            view = self._current_view
            second_id = view.latest_second.get("event_id") if view is not None and view.latest_second is not None else None
            event = self._view_second_events.get(second_id) if second_id else None
        if event is None:
            self._record_error(f"fill second missing for {record.strategy_id}", code="exit_monitor_no_second")
            return
        second = event.to_dict()
        try:
            exit_state = make_exit_state(
                trade,
                entry_fill_interval=(second["start_at"], second["end_at"]),
                entry_filled_at=second["end_at"],
                generation=record.open_position_generation if record.open_position_generation is not None else record.generation,
                market_feed=self._config.market_feed,
            )
            self._create_exit_monitor_for(exit_state, record.strategy_id)
        except Exception as exc:
            self._record_error(f"exit monitor creation failed for {record.strategy_id}: {exc}", code="exit_monitor_failed")

    def _create_exit_monitor_for(self, exit_state: Any, strategy_id: str) -> None:
        if strategy_id in self._exit_monitors:
            return
        record = self._scheduler.get(strategy_id)
        if record is None:
            return
        monitor = ExitMonitor(
            exit_state,
            scheduler=self._scheduler,
            paper_account_book=self._book,
            on_position_close=self._on_position_close_cb,
            recovery_request=None,
            expected_source_request_hash=None,
            enabled=record.enabled,
            exhausted=record.exhausted,
        )
        self._exit_monitors[strategy_id] = monitor

    # ------------------------------------------------------------------
    # Model job execution
    # ------------------------------------------------------------------

    def _notify_provider_result(self) -> None:
        """Wake a run loop without touching runtime state from a worker."""
        with self._provider_condition:
            self._provider_condition.notify_all()

    def _ensure_provider_pool(self) -> _ProviderWorkerPool:
        pool = self._provider_pool
        if pool is None:
            pool = _ProviderWorkerPool(
                self._config.max_in_flight,
                self._run_provider_task,
                self._notify_provider_result,
            )
            self._provider_pool = pool
        return pool

    def _run_provider_task(self, task: _ProviderTask) -> _ProviderExecution:
        """Execute only provider-side work in a worker.

        The wrapper catches every worker exception and converts it into the
        scheduler's typed transient transport path.  No scheduler, SQLite,
        account or monitor method is reachable from this thread.
        """
        try:
            return self._execute_job(task.job, task.view)
        except BaseException as exc:
            return _ProviderExecution(
                value={
                    "classification": "transport",
                    "transient": True,
                    "retryable": True,
                },
                completed_at=self._now(),
                provider_called=False,
                error_code=f"worker_{type(exc).__name__}",
            )

    def _track_provider_task(self, task: _ProviderTask) -> None:
        job_id = task.job.job_id
        self._provider_tasks[job_id] = task
        self._provider_strategy_tasks.setdefault(task.job.strategy_id, set()).add(job_id)

    def _untrack_provider_task(self, job_id: str) -> _ProviderTask | None:
        task = self._provider_tasks.pop(job_id, None)
        if task is None:
            return None
        strategy_jobs = self._provider_strategy_tasks.get(task.job.strategy_id)
        if strategy_jobs is not None:
            strategy_jobs.discard(job_id)
            if not strategy_jobs:
                self._provider_strategy_tasks.pop(task.job.strategy_id, None)
        return task

    def _strategy_provider_busy(self, strategy_id: str) -> bool:
        return bool(self._provider_strategy_tasks.get(strategy_id))

    def _submit_provider_task(self, task: _ProviderTask, report: dict[str, Any]) -> bool:
        """Submit one claimed job or complete it through a typed worker error."""
        try:
            self._ensure_provider_pool().submit(task)
        except Exception as exc:
            # The scheduler has already claimed the logical lease.  Release it
            # through the normal completion path so queue/provider accounting
            # and retry policy remain identical to an ordinary transport error.
            self._complete_provider_execution(
                task,
                _ProviderExecution(
                    value={"classification": "transport", "transient": True, "retryable": True},
                    completed_at=self._now(),
                    provider_called=False,
                    error_code=f"worker_submit_{type(exc).__name__}",
                ),
                report,
            )
            return False
        self._track_provider_task(task)
        return True

    def _submit_deferred_provider_jobs(self, report: dict[str, Any]) -> None:
        """Submit claimed jobs held behind a still-running old generation."""
        if self._scheduler is None or self._stopped:
            return
        for job_id, task in list(self._deferred_provider_jobs.items()):
            strategy_id = task.job.strategy_id
            if self._strategy_provider_busy(strategy_id):
                continue
            active = self._scheduler.queue.active_job(strategy_id)
            if active is None or active.job_id != job_id:
                self._deferred_provider_jobs.pop(job_id, None)
                continue
            current = self._current_view
            if current is None or current.view_id != task.job.market_view_id:
                self._deferred_provider_jobs.pop(job_id, None)
                self._complete_provider_execution(
                    task,
                    _ProviderExecution(
                        value={"classification": "stale_market"},
                        completed_at=self._now(),
                        provider_called=False,
                    ),
                    report,
                )
                continue
            self._deferred_provider_jobs.pop(job_id, None)
            self._submit_provider_task(task, report)

    def _complete_provider_execution(
        self,
        task: _ProviderTask,
        execution: _ProviderExecution,
        report: dict[str, Any] | None = None,
    ) -> bool:
        """Merge one worker result on the runtime/SQLite thread."""
        if execution.provider_called:
            self._provider_calls += 1
        value = execution.value
        error_code = execution.error_code
        if isinstance(execution.context, _ProviderResponseContext):
            try:
                value, postprocess_error = self._finalize_provider_response(
                    task.job,
                    value,
                    execution.context,
                    completed_at=execution.completed_at,
                    completed_order=execution.completed_order,
                )
                error_code = error_code or postprocess_error
            except BaseException as exc:
                value = {"classification": "grounding_failure", "code": "response_finalize_failed"}
                error_code = error_code or f"response_finalize_{type(exc).__name__}"
        if execution.error_code is not None:
            self._record_error(
                f"provider worker failed for {task.job.strategy_id}",
                code=error_code,
            )
            if report is not None:
                report["errors"].append({"message": "provider worker failed", "code": error_code})
        elif error_code is not None:
            self._record_error(
                f"provider response rejected for {task.job.strategy_id}",
                code=error_code,
            )
            if report is not None:
                report["errors"].append({"message": "provider response rejected", "code": error_code})
        completed = self._scheduler.complete_job(
            task.job.job_id,
            value,
            decision_accepted_at=execution.completed_at,
        )
        if not completed:
            # G023 validates a completion against the scheduler view that
            # claimed it.  If this result was a typed market-quality failure,
            # retain the newer runtime view as the retry gate so the
            # subsequent on_market_view call cannot immediately spin a
            # same-view retry after the response-time second was rejected.
            if isinstance(value, Mapping) and value.get("classification") == "market_quality":
                record = self._scheduler.get(task.job.strategy_id)
                if record is not None and self._current_view is not None:
                    record.rejected_immediate_view_id = self._current_view.view_id
            return False
        record = self._scheduler.get(task.job.strategy_id)
        if record is None:
            return False
        if isinstance(value, Mapping) and value.get("instruction") is not None:
            self._persist_accepted_decision(record, execution.completed_at)
        if record.state == StrategyState.WAITING_ENTRY and task.job.strategy_id not in self._pending_monitors:
            self._create_pending_monitor(record, execution.completed_at)
        if record.state == StrategyState.OPEN_POSITION and task.job.strategy_id not in self._exit_monitors:
            self._create_exit_monitor(record, execution.completed_at)
        return True

    def _finalize_provider_response(
        self,
        job: Any,
        outcome: Any,
        context: _ProviderResponseContext,
        *,
        completed_at: datetime,
        completed_order: float | None = None,
    ) -> tuple[dict[str, Any], str | None]:
        """Parse/ground one response on the runtime thread.

        The provider worker returns as soon as the adapter response is
        available.  Keeping G018 finalization here makes the completion path
        deterministic and prevents provider latency from occupying the main
        loop, while all durable callbacks remain serialized.
        """
        response = outcome.normalized_response
        extensions = response.get("extensions") if isinstance(response, Mapping) else None
        output_ref = extensions.get("g018_model_output_ref") if isinstance(extensions, Mapping) else None
        candidate = output_ref.get("candidate") if isinstance(output_ref, Mapping) else None
        if not isinstance(candidate, Mapping):
            return {"classification": "grounding_failure", "code": "model_output_ref_missing"}, "model_output_ref_missing"
        try:
            parsed = parse_candidate(candidate)
        except Exception:
            return {"classification": "grounding_failure", "code": "invalid_candidate"}, "invalid_candidate"
        response_for_finalization = response
        call_metadata_for_finalization = outcome.call_metadata
        second_event = None
        if parsed.get("branch") == "enter_immediately":
            second_event = self._response_second_event(completed_at, completed_order=completed_order)
            if second_event is None:
                return {"classification": "market_quality", "code": "post_response_second_unavailable"}, "post_response_second_unavailable"
            # The model grounded its intent against the immutable request
            # view, but the trusted execution second is a runtime-owned
            # post-response fact.  Rebind only this execution reference before
            # invoking G018; all packet/evidence/view grounding below remains
            # bound to the original request context.
            parsed = dict(parsed)
            parsed["second_event_ref"] = {
                "schema_version": "0.1.0",
                "event_id": second_event["event_id"],
                "event_hash": second_event["event_hash"],
                "reference_price": second_event["reference_price"],
            }
            # G018 binds the normalized response's candidate and output-ref
            # digest to the parsed candidate.  Recompute that runtime-owned
            # response envelope after the trusted second rebinding; the
            # request and its market-view lineage are intentionally untouched.
            response_for_finalization = copy.deepcopy(dict(response))
            response_for_finalization["candidate"] = map_candidate_to_normalized_model_candidate(
                parsed,
                profile_ref=context.loaded.profile_ref,
            )
            response_extensions = response_for_finalization.get("extensions")
            if not isinstance(response_extensions, Mapping):
                return {"classification": "grounding_failure", "code": "model_output_ref_missing"}, "model_output_ref_missing"
            response_extensions = copy.deepcopy(dict(response_extensions))
            response_extensions["g018_model_output_ref"] = make_model_output_ref(parsed)
            response_for_finalization["extensions"] = response_extensions
            response_for_finalization = finalize_artifact(response_for_finalization, "normalized_model_response")
            if isinstance(outcome.call_metadata, Mapping):
                call_metadata_for_finalization = copy.deepcopy(dict(outcome.call_metadata))
                call_metadata_for_finalization["model_response_id"] = response_for_finalization["model_response_id"]
                call_metadata_for_finalization["model_response_hash"] = response_for_finalization["content_hash"]
                call_metadata_for_finalization = finalize_artifact(call_metadata_for_finalization, "model_call_metadata")
        finalization = finalize_decision(
            parsed,
            registry=self._registry,
            packet=context.loaded.packet,
            profile_ref=context.loaded.profile_ref,
            evidence_package=context.loaded.evidence_package,
            market_snapshot=context.view.closed_snapshot,
            observation_frame=context.view.observation_frame,
            analysis_request=context.analysis_request,
            normalized_model_request=context.request,
            normalized_model_response=response_for_finalization,
            model_call_metadata=call_metadata_for_finalization,
            second_event=second_event,
        )
        if not finalization.valid:
            return {"classification": "grounding_failure", "code": finalization.code}, finalization.code
        return {"instruction": finalization.instruction}, None

    def _drain_provider_results(self, report: dict[str, Any] | None = None) -> int:
        """Drain completed calls in completion order on the runtime thread."""
        pool = self._provider_pool
        if pool is None:
            return 0
        drained = 0
        for task, execution in pool.drain():
            self._untrack_provider_task(task.job.job_id)
            self._complete_provider_execution(task, execution, report)
            drained += 1
        self._submit_deferred_provider_jobs(report or {"errors": []})
        return drained

    def _claim_and_submit_provider_jobs(self, report: dict[str, Any]) -> list[str]:
        """Claim scheduler jobs and hand them to bounded provider workers."""
        available = self._scheduler.available_provider_slots
        if available <= 0:
            return []
        jobs = self._scheduler.claim_jobs(limit=available)
        report["ready_jobs"] = report.get("ready_jobs", 0) + len(jobs)
        current = self._current_view
        for job in jobs:
            task = _ProviderTask(job=job, view=current)
            if self._strategy_provider_busy(job.strategy_id):
                # G023 may invalidate a logical active job while its provider
                # call is still physically blocked.  Hold the replacement
                # claim until the old call returns so one strategy never has
                # two actual provider calls at once.
                self._deferred_provider_jobs[job.job_id] = task
                continue
            self._submit_provider_task(task, report)
        self._submit_deferred_provider_jobs(report)
        return [job.job_id for job in jobs]

    def _wait_for_fast_provider_results(self, job_ids: list[str], report: dict[str, Any]) -> None:
        """Give already-fast providers a deterministic bounded completion window."""
        if not job_ids:
            return
        pending_ids = set(job_ids)
        deadline = time.monotonic() + _PROVIDER_COMPLETION_GRACE_SECONDS
        while True:
            self._drain_provider_results(report)
            # A fast completion can free a logical scheduler lease while this
            # bounded grace is still running.  Refill immediately so the
            # worker pool remains work-conserving even when the completion
            # arrives between the main step's normal drain and this wait.
            pending_ids.update(self._claim_and_submit_provider_jobs(report))
            remaining_jobs = [
                job_id
                for job_id in pending_ids
                if job_id in self._provider_tasks or job_id in self._deferred_provider_jobs
            ]
            if not remaining_jobs:
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            with self._provider_condition:
                self._provider_condition.wait(timeout=remaining)

    def _analysis_request_artifact(self, view: Any, profile_ref: Mapping[str, Any]) -> dict[str, Any]:
        """Build the G014 ``analysis_request`` lineage artifact for one job.

        The finalizer requires a distinct analysis-request artifact whose
        strategy profile is bound and whose snapshot lineage matches the
        shared view.
        """
        from pa_engine.contracts import finalize_artifact
        from pa_engine.strategies.knowledge import bind_strategy_profile_ref

        snapshot = view.closed_snapshot
        policy = finalize_artifact(
            {
                "schema_version": "0.1.0",
                "artifact_kind": "analysis_policy",
                "minimum_closed_bars": 1,
                "max_staleness_seconds": 86_400,
                "allow_partial_data": False,
                "exclude_incomplete_last_bar": True,
                "required_capabilities": ["price_action"],
                "policy_version": "0.1.0",
                "extensions": {"market_feed": self._config.market_feed.to_dict()},
            },
            "analysis_policy",
        )
        document = {
            "schema_version": "0.1.0",
            "artifact_kind": "analysis_request",
            "correlation_id": "g028-runtime",
            "input_mode": "ohlcv",
            "analysis_as_of": snapshot.get("analysis_as_of"),
            "market_snapshot_id": snapshot.get("market_snapshot_id"),
            "knowledge_snapshot_id": profile_ref.get("snapshot_id"),
            "knowledge_snapshot_hash": profile_ref.get("snapshot_hash"),
            "analysis_policy_id": policy["analysis_policy_id"],
            "purpose": "decision_support",
            "instrument": snapshot.get("instrument"),
            "timeframe": snapshot.get("timeframe"),
            "session": snapshot.get("session"),
            "no_live_orders": True,
            "extensions": {},
        }
        return bind_strategy_profile_ref(finalize_artifact(document, "analysis_request"), profile_ref)

    def _normalized_request(
        self,
        *,
        strategy_id: str,
        packet: Mapping[str, Any],
        profile_ref: Mapping[str, Any],
        view: Any,
        evidence_package: Mapping[str, Any],
        analysis_request: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Build the boundary-safe normalized model request for one job.

        Uses the canonical G028 prompt builder with the real finalized
        ``analysis_request`` lineage artifact so the G018 finalizer's lineage
        validation accepts the request.
        """
        from pa_engine.runtime.prompt import build_analysis_request

        return build_analysis_request(
            strategy_id=strategy_id,
            packet=packet,
            profile_ref=profile_ref,
            market_view=view.to_dict(),
            reasoning_mode=self._config.reasoning_mode,
            provider=self._config.provider,
            model=self._config.model,
            adapter=self._config.adapter,
            max_output_tokens=self._max_output_tokens,
            evidence_package=evidence_package,
            analysis_request=analysis_request,
        )

    def _execute_job(self, job: Any, view: Any = None) -> _ProviderExecution:
        """Build, call and normalize one job without mutating runtime state."""
        view = self._current_view if view is None else view
        if view is None or view.view_id != job.market_view_id:
            return _ProviderExecution({"classification": "stale_market"}, self._now())
        worker_now = self._now()
        if not self._view_executable_now(view, instant=worker_now):
            return _ProviderExecution({"classification": "market_quality", "code": "view_not_executable"}, worker_now)
        loaded = self._registry.load_packet(job.strategy_id, expected_profile_ref=job.profile_ref)
        if loaded.code != "ok":
            return _ProviderExecution({"classification": "grounding_failure", "code": loaded.code}, self._now())
        try:
            # The packet/evidence/profile are model-facing inputs; a tampered
            # nested account-state field is rejected before any prompt work.
            assert_model_boundary_safe(
                {
                    "packet": loaded.packet,
                    "evidence_package": loaded.evidence_package,
                    "profile_ref": loaded.profile_ref,
                },
                context="model_input",
            )
        except ModelBoundaryError as exc:
            return _ProviderExecution({"classification": "invalid_configuration", "code": exc.code}, self._now())
        try:
            analysis_request = self._analysis_request_artifact(view, loaded.profile_ref)
            request = self._normalized_request(
                strategy_id=job.strategy_id,
                packet=loaded.packet,
                profile_ref=loaded.profile_ref,
                view=view,
                evidence_package=loaded.evidence_package,
                analysis_request=analysis_request,
            )
        except ModelBoundaryError as exc:
            return _ProviderExecution({"classification": "invalid_configuration", "code": exc.code}, self._now())
        except Exception:
            return _ProviderExecution({"classification": "invalid_configuration"}, self._now(), error_code="invalid_request")
        if self._adapter is None:
            return _ProviderExecution({"classification": "invalid_configuration"}, self._now())
        try:
            outcome = self._adapter.complete(request, generation=job.generation)
        except BaseException:
            # Capture the attempted-call completion instant even on an
            # adapter exception; the main thread owns the diagnostic count.
            return _ProviderExecution(
                {"classification": "transport", "transient": True, "retryable": True},
                self._now(),
                provider_called=True,
                error_code="adapter_failure",
            )
        # This clock read intentionally occurs immediately after the provider
        # returns.  It must not use the step-loop instant: N/X deadlines are
        # anchored to the actual response completion of this job.
        provider_completed_at = self._now()
        if not getattr(outcome, "is_success", False):
            return _ProviderExecution(outcome, provider_completed_at, provider_called=True)
        return _ProviderExecution(
            outcome,
            provider_completed_at,
            provider_called=True,
            context=_ProviderResponseContext(
                view=view,
                loaded=loaded,
                analysis_request=analysis_request,
                request=request,
            ),
        )

    def _persist_accepted_decision(self, record: Any, accepted_at: datetime) -> None:
        """Persist one scheduler-accepted decision and its schedule atomically.

        Runs only after ``complete_job`` accepted the instruction, so a
        rejected or retried result leaves no durable decision lineage.  The
        schedule commit registers the timer kind with the sweep so a later
        bridge claim or fired timer clears the row.
        """
        instruction = record.latest_instruction
        if not isinstance(instruction, Mapping) or not instruction.get("instruction_hash"):
            return
        try:
            self._sink.commit_decision(record.snapshot(), instruction, occurred_at=accepted_at)
            branch = instruction.get("branch")
            timer_kind = None
            due_at = None
            if branch == "no_entry" and isinstance(instruction.get("next_call_after_seconds"), int):
                timer_kind = "sleep"
                due_at = accepted_at + timedelta(seconds=instruction["next_call_after_seconds"])
            elif branch == "wait_for_price" and isinstance(instruction.get("entry_wait_seconds"), int):
                timer_kind = "pending_expiry"
                due_at = accepted_at + timedelta(seconds=instruction["entry_wait_seconds"])
            if timer_kind is not None and due_at is not None:
                self._sink.commit_schedule(
                    record.snapshot(),
                    timer_kind=timer_kind,
                    due_at=due_at,
                    decision_accepted_at=accepted_at,
                    occurred_at=accepted_at,
                )
                previous = self._committed_timer_kinds.get(record.strategy_id, frozenset())
                self._committed_timer_kinds[record.strategy_id] = previous | {timer_kind}
        except Exception as exc:
            self._record_error(f"decision persistence failed for {record.strategy_id}: {exc}", code="decision_persist_failed")
        self._recent_events.append({"kind": "decision", "strategy_id": record.strategy_id, "branch": instruction.get("branch")})

    # ------------------------------------------------------------------
    # Recovery
    # ------------------------------------------------------------------

    def _run_recovery(self, now: datetime, errors: list[dict[str, Any]], *, startup: bool = False) -> None:
        """Fetch G019 recovery bars, feed them to gapped monitors and rebuild.

        The stored market recovery commit clears the live-blocked gate; the
        rebuilt view carries the typed recovery evidence so later reseeded
        views stay executable.  When recovery is unavailable the runtime
        stays blocked (``_recovery_pending``) and processes no live events.
        Failed attempts retry with a bounded 5/10/20/40/60-second delay.
        """
        if self._recovery_retry_at is not None and now < self._recovery_retry_at:
            return
        gap_start = None
        gaps = list(self._feed.connection_gaps)
        if gaps:
            gap_start = gaps[0].get("start_at")
            gap_end = gaps[0].get("end_at") or _stamp(now)
            for monitor in self._pending_monitors.values():
                if not monitor.state.terminal:
                    try:
                        monitor.on_gap(start_at=gap_start, end_at=gap_end, reason="feed_gap")
                    except Exception:
                        pass
            for monitor in self._exit_monitors.values():
                if not monitor.state.terminal:
                    try:
                        monitor.on_gap(start_at=gap_start, end_at=gap_end, reason="feed_gap")
                    except Exception:
                        pass
        if gap_start is None and self._processed_end_at is not None:
            gap_start = self._processed_end_at
        if gap_start is None:
            gap_start = _stamp(now - timedelta(seconds=60))
        bars: list[Mapping[str, Any]] = []
        try:
            result = self._feed.fetch_recovery(start_at=gap_start, end_at=now)
            bars = list(self._feed.recovery_bars(result))
            recovery_cursor = self._feed.recovery_cursor(result)
            source_request_hash = self._feed.source_request_hash(result)
        except Exception as exc:
            self._recovery_pending = True
            self._schedule_recovery_retry(now)
            self._recovery_result = {"status": "unavailable", "error": str(exc)}
            errors.append({"message": f"market recovery unavailable: {exc}", "code": "recovery_required_but_unavailable"})
            return
        if not bars:
            self._recovery_pending = True
            self._schedule_recovery_retry(now)
            self._recovery_result = {"status": "unavailable", "error": "no recovery bars"}
            errors.append({"message": "market recovery returned no bars", "code": "recovery_required_but_unavailable"})
            return
        try:
            baseline = None
            cursor_row = self._sink.get_cursor(self._config.symbol)
            if isinstance(cursor_row, Mapping) and cursor_row.get("last_second_end_at"):
                baseline = cursor_row["last_second_end_at"]
            recovery_cutover = self._validate_recovery_bars(
                bars,
                recovery_cursor,
                baseline,
                now=now,
                expected_source_request_hash=source_request_hash,
            )
        except RuntimeEngineError as exc:
            self._recovery_pending = True
            self._schedule_recovery_retry(now)
            self._recovery_result = {"status": "rejected", "error": str(exc), "code": exc.code}
            errors.append({"message": str(exc), "code": exc.code})
            return
        stream_status = self._feed.status().get("stream", {})
        if self._feed.stream_client is not None and not stream_status.get("transport_ready", False):
            # Recovery evidence cannot be committed as executable market
            # state while the new authenticated/subscribed transport is down.
            self._recovery_pending = True
            self._schedule_recovery_retry(now)
            self._recovery_result = {
                "status": "waiting_stream",
                "error": "stream_not_ready",
                "code": "stream_not_ready",
            }
            errors.append({"message": "stream recovery is not ready: stream_not_ready", "code": "recovery_stream_not_ready"})
            return
        for monitor in list(self._pending_monitors.values()):
            if monitor.state.gap_overlap and not monitor.state.terminal:
                try:
                    self._current_pending_source[monitor.strategy_id] = None
                    monitor.on_recovery(bars, now=now)
                except Exception as exc:
                    errors.append({"message": f"pending recovery failed for {monitor.strategy_id}: {exc}", "code": "pending_recovery_failed"})
        for monitor in list(self._exit_monitors.values()):
            if monitor.state.gap_active and not monitor.state.terminal:
                try:
                    self._current_exit_source[monitor.strategy_id] = None
                    monitor.on_recovery(bars, now=now)
                    if monitor.state.terminal:
                        self._persist_exit_terminal(monitor.strategy_id, monitor)
                except Exception as exc:
                    errors.append({"message": f"exit recovery failed for {monitor.strategy_id}: {exc}", "code": "exit_recovery_failed"})
        try:
            self._sink.commit_market_recovery(
                self._config.symbol,
                bars=[make_recovery_bar_ref(bar) for bar in bars],
                recovery_cursor=recovery_cursor,
                occurred_at=now,
            )
        except Exception as exc:
            # The stored live-blocked gate must stay armed; keep the runtime
            # blocked and retry the commit later instead of declaring the
            # market recovered.
            self._recovery_pending = True
            self._schedule_recovery_retry(now)
            self._record_error(f"market recovery persistence failed: {exc}", code="recovery_persist_failed")
            errors.append({"message": f"market recovery persistence failed: {exc}", "code": "recovery_persist_failed"})
            self._recovery_result = {"status": "rejected", "error": str(exc), "code": "recovery_persist_failed"}
            return
        # Gap-frozen seconds keep the view partial even after the evidence
        # clears ``recovery_required`` (G021 semantics); drop them so fresh
        # post-recovery seconds can reseed an executable view.
        self._feed.reset_seconds()
        self._recovery_generation += 1
        self._recovered_flag = True
        self._recovery_evidence = self._feed.build_recovery_evidence(bars, generation=self._recovery_generation)
        try:
            view = self._feed.build_view(
                history_result=self._history_result,
                now_at=now,
                connection_gaps=(),
                recovery_generation=self._recovery_generation,
                recovered=True,
                recovery_evidence=self._recovery_evidence,
                cutover_at=recovery_cutover,
            )
            self._adopt_view(view)
        except FeedSessionError as exc:
            # The live consumer remains gated until a replacement view has
            # been accepted.  A failed build must therefore stay retryable
            # and must not release the unresolved stream gap.
            self._recovery_pending = True
            self._schedule_recovery_retry(now)
            self._recovery_result = {"status": "rejected", "error": str(exc), "code": exc.code}
            errors.append({"message": str(exc), "code": exc.code})
            return
        release = self._feed.release_stream_recovery(cutover_at=recovery_cutover, end_at=now)
        if not release.get("ok"):
            self._recovery_pending = True
            self._schedule_recovery_retry(now)
            error = release.get("error") or "stream_recovery_blocked"
            self._recovery_result = {"status": "waiting_stream", "error": error, "code": error}
            errors.append({"message": f"stream recovery is not ready: {error}", "code": "recovery_stream_not_ready"})
            return
        recovery_cutover_text = _stamp(recovery_cutover)
        self._live_cutover_at = recovery_cutover_text
        self._processed_end_at = recovery_cutover_text
        self._recovery_pending = False
        self._recovery_retry_at = None
        self._recovery_retry_count = 0
        if self._current_view is not None:
            self._last_recovery_signature = self._recovery_signature(self._current_view)
        self._recovery_result = {
            "status": "recovered",
            "generation": self._recovery_generation,
            "bar_count": len(bars),
            "cursor": recovery_cursor,
            "cutover_at": recovery_cutover_text,
        }

    def _schedule_recovery_retry(self, now: datetime) -> None:
        delays = (5, 10, 20, 40, 60)
        delay = delays[min(self._recovery_retry_count, len(delays) - 1)]
        self._recovery_retry_count += 1
        self._recovery_retry_at = now + timedelta(seconds=delay)

    def _validate_recovery_bars(
        self,
        bars: list[Mapping[str, Any]],
        recovery_cursor: Any,
        baseline: str | None = None,
        *,
        now: datetime | None = None,
        expected_source_request_hash: str | None = None,
    ) -> datetime | None:
        """Fail-closed cross-symbol/feed/request binding at the runtime seam.

        The G025/G026 reducers re-validate per-bar content; this seam rejects
        a foreign symbol, feed, venue, request hash, non-contiguous
        minute-bar set, or a leading bar that straddles the persisted
        execution cursor, before any monitor consumes it.
        """
        feed_ref = self._config.market_feed.to_dict()
        if not isinstance(recovery_cursor, str):
            raise RuntimeEngineError("recovery_cursor_missing", "recovery result has no canonical resume cursor")
        try:
            instant_from_text(recovery_cursor)
        except Exception as exc:
            raise RuntimeEngineError("recovery_cursor_invalid", "recovery result cursor is not canonical UTC") from exc
        request_hash: str | None = None
        previous_end: datetime | None = None
        last_end: datetime | None = None
        for index, bar in enumerate(bars):
            mapping = dict(bar)
            if mapping.get("complete") is not True or mapping.get("coverage_state") != "complete":
                raise RuntimeEngineError("recovery_bar_incomplete", f"recovery bar {index} is not complete")
            instrument = mapping.get("instrument")
            if isinstance(instrument, Mapping):
                symbol = instrument.get("symbol")
            else:
                symbol = mapping.get("symbol")
            if symbol != self._config.symbol:
                raise RuntimeEngineError("recovery_symbol_mismatch", f"recovery bar {index} carries symbol {symbol!r}")
            bar_feed = mapping.get("market_feed")
            bar_feed_dict = bar_feed.to_dict() if hasattr(bar_feed, "to_dict") else bar_feed if isinstance(bar_feed, Mapping) else None
            if bar_feed_dict is not None and bar_feed_dict.get("feed") != feed_ref.get("feed"):
                raise RuntimeEngineError("recovery_feed_mismatch", f"recovery bar {index} carries a non-IEX feed")
            venue = mapping.get("venue")
            if venue is not None and str(venue).upper() != "IEX":
                raise RuntimeEngineError("recovery_venue_mismatch", f"recovery bar {index} carries venue {venue!r}")
            bar_hash = mapping.get("source_request_hash")
            if isinstance(bar_hash, str) and bar_hash:
                if request_hash is None:
                    request_hash = bar_hash
                elif bar_hash != request_hash:
                    raise RuntimeEngineError("recovery_request_hash_mismatch", "recovery bars mix request hashes")
            if expected_source_request_hash is not None and bar_hash != expected_source_request_hash:
                raise RuntimeEngineError("recovery_request_hash_mismatch", "recovery bars do not match the recovery request")
            if mapping.get("complete") is True and mapping.get("coverage_state") == "complete":
                validation = validate_recovery_bar(
                    mapping,
                    expected_market_feed=feed_ref,
                    expected_source_request_hash=expected_source_request_hash,
                )
                if not validation.valid:
                    issue = validation.issues[0] if validation.issues else None
                    code = f"recovery_bar_{issue.code}" if issue is not None else "recovery_bar_invalid"
                    raise RuntimeEngineError(code, f"recovery bar {index} failed strict validation")
            start_at = mapping.get("start_at")
            end_at = mapping.get("end_at")
            if isinstance(start_at, str) and isinstance(end_at, str):
                try:
                    start = instant_from_text(start_at)
                    end = instant_from_text(end_at)
                except ValueError as exc:
                    raise RuntimeEngineError("recovery_interval_invalid", f"recovery bar {index} has an invalid interval") from exc
                if end != start + timedelta(minutes=1):
                    raise RuntimeEngineError("recovery_interval_invalid", f"recovery bar {index} is not a whole minute")
                if now is not None and end > utc_instant(now):
                    raise RuntimeEngineError("recovery_bar_future", f"recovery bar {index} ends after recovery now")
                if previous_end is not None and start != previous_end:
                    raise RuntimeEngineError("recovery_bars_not_contiguous", "recovery bars are not chronologically contiguous")
                previous_end = end
                last_end = end
            else:
                raise RuntimeEngineError("recovery_interval_invalid", f"recovery bar {index} has an invalid interval")
        if baseline is not None and bars:
            first = dict(bars[0])
            first_start = first.get("start_at")
            if isinstance(first_start, str) and isinstance(baseline, str):
                try:
                    start_instant = instant_from_text(first_start)
                    baseline_instant = instant_from_text(baseline)
                except ValueError:
                    pass
                else:
                    if start_instant < baseline_instant:
                        raise RuntimeEngineError(
                            "recovery_straddle",
                            f"first recovery bar starts before the stored second cursor {baseline}",
                        )
        return last_end

    def _recovery_needed(self) -> bool:
        if self._recovery_pending:
            return True
        if self._feed.connection_gaps:
            return True
        view = self._current_view
        if view is not None:
            readiness = view.readiness
            if bool(getattr(readiness, "recovery_required", False)):
                # Do not re-attempt a recovery that already reconciled this
                # gap signature; coverage-partial history is not bar-
                # recoverable and must stay a fail-closed market wait.
                if self._last_recovery_signature == self._recovery_signature(view):
                    return False
                return True
        for monitor in self._pending_monitors.values():
            if monitor.state.gap_overlap and not monitor.state.terminal and not monitor.state.recovery_attempted:
                return True
        for monitor in self._exit_monitors.values():
            if monitor.state.gap_active and not monitor.state.terminal and not monitor.state.recovery_attempted:
                return True
        return False

    def _recovery_signature(self, view: Any) -> tuple[Any, ...]:
        gaps = tuple(
            (gap.get("start_at"), gap.get("end_at"))
            for gap in sorted(self._feed.connection_gaps, key=lambda item: str(item))
        )
        monitor_gaps = tuple(
            sorted(
                {
                    (getattr(monitor.state, "gap_overlap", False), getattr(monitor.state, "gap_active", False))
                    for monitor in [*self._pending_monitors.values(), *self._exit_monitors.values()]
                    if not getattr(monitor.state, "terminal", False)
                }
            )
        )
        return (
            gaps,
            monitor_gaps,
            getattr(view.readiness, "recovery_required", False),
        )

    # ------------------------------------------------------------------
    # Step loop
    # ------------------------------------------------------------------

    def step(self, *, now: datetime | str | None = None) -> dict[str, Any]:
        """Run one loop iteration while serializing lifecycle shutdown."""
        with self._lifecycle_lock:
            return self._step_impl(now=now)

    def _step_impl(self, *, now: datetime | str | None = None) -> dict[str, Any]:
        """Run one deterministic loop iteration and return a StepReport."""
        if self._stopped:
            raise RuntimeEngineError("stopped", "the runtime is stopped")
        if not self._started:
            raise RuntimeEngineError("not_started", "start() must be called before step()")
        instant = self._now(now)
        # ``now=`` is the deterministic test/replay clock input.  Keep an
        # injected advanceable clock aligned with it so worker completion
        # timestamps read from ``clock.now()`` remain consistent with the
        # explicit step instant rather than an older fake-clock value.
        if now is not None:
            set_clock = getattr(self._clock, "set", None)
            if callable(set_clock):
                set_clock(instant)
        self._step_index += 1
        report = self._step_report_base(instant)
        events: list[Any] = []
        try:
            events = list(self._feed.ingest(now=instant))
        except FeedSessionError as exc:
            if exc.code == "feed_scope_mismatch":
                self.stop(reason=f"feed_scope_mismatch: {exc}")
                raise RuntimeEngineError("feed_scope_mismatch", str(exc)) from exc
            report["errors"].append({"message": str(exc), "code": exc.code})
        events = self._filter_events(events)
        report["ingested"] = len(events)
        self._record_accepted_seconds(events, instant)

        # The view content stays stable between new market data (G021 stamps
        # the snapshot with the build instant, so rebuilding every step would
        # churn content-derived view ids and defeat the scheduler's
        # same-view rejection/semantic-retry gates).  Time-dependent
        # readiness (session closure, staleness) is re-derived each step and
        # fed to the scheduler as an explicit market-status override.
        if events or self._feed.connection_gaps:
            self._rebuild_view(instant, report)
        self._status_facts = self._compute_status_facts(instant)
        recovery_ran = False
        if self._recovery_needed():
            self._run_recovery(instant, report["errors"])
            recovery_ran = True
            report["recovery"] = self._recovery_result
            if self._recovery_pending:
                # X wall-clock deadlines are never extended: pending expiry
                # still materializes while the market recovery is pending.
                self._expiry_sweep(instant)
                self._persist_sweep(instant)
                return report
        # Completed provider responses are merged only on this runtime
        # thread before G023 is shown a newer view.  A result that is already
        # available therefore keeps its claim-time scheduler identity; a
        # result that completes after the scheduler advances is handled by
        # G023's typed stale-view response path.  G018 finalization still
        # receives the independently current runtime view for its response-
        # time second selection.
        self._drain_provider_results(report)
        # Advance G023 independently of other strategies' provider latency.
        # Its completion path rejects any response whose claim view has been
        # superseded, then requeues only that strategy on a later fresh view;
        # a blocked provider must not freeze the global scheduler view.
        if self._current_view is not None:
            self._scheduler.on_market_view(self._view_mapping(), market_status=self._status_facts)
            self._last_view_id = self._current_view.view_id

        handled = self._scheduler.process_timers(instant)
        report["timer_events"] = len(handled)
        job_ids = self._claim_and_submit_provider_jobs(report)
        # Fast deterministic adapters retain the historical one-step testing
        # ergonomics, while a slow/blocking provider returns after this short
        # bounded grace and lets later steps continue consuming the feed.
        self._wait_for_fast_provider_results(job_ids, report)

        # Recovery reconciles the gap with minute bars and drops the
        # pre-recovery seconds; those seconds must not fan out in the same
        # step, and no second is executable unless the rebuilt view is.
        if not recovery_ran and self._view_executable_now():
            self._fan_out_events(events, instant, report)
        self._expiry_sweep(instant)
        self._persist_sweep(instant)
        report["recovery"] = report["recovery"] or self._recovery_result
        self._recovery_result = None
        report["fills"] = self._fills
        report["closes"] = self._closes
        report["expiries"] = self._expiries
        report["rejections"] = self._rejections
        report["provider_calls"] = self._provider_calls
        report["provider_in_flight"] = len(self._provider_tasks)
        report["provider_pending_results"] = self._provider_pool.pending_results if self._provider_pool is not None else 0
        return report

    def _step_report_base(self, instant: datetime) -> dict[str, Any]:
        view = self._current_view
        readiness = view.readiness.to_dict() if view is not None else {}
        return {
            "step_index": self._step_index,
            "now": _stamp(instant),
            "ingested": 0,
            "view_id": view.view_id if view is not None else None,
            "view_executable": bool(view is not None and view.executable),
            "readiness": readiness,
            "ready_jobs": 0,
            "provider_calls": 0,
            "provider_in_flight": len(self._provider_tasks),
            "provider_pending_results": self._provider_pool.pending_results if self._provider_pool is not None else 0,
            "timer_events": 0,
            "fills": self._fills,
            "closes": self._closes,
            "expiries": self._expiries,
            "rejections": self._rejections,
            "recovery": None,
            "errors": [],
        }

    def _rebuild_view(self, instant: datetime, report: dict[str, Any]) -> None:
        try:
            view = self._feed.build_view(
                history_result=self._history_result,
                now_at=instant,
                connection_gaps=self._feed.connection_gaps,
                recovery_generation=self._recovery_generation,
                recovered=self._recovered_flag,
                recovery_evidence=self._recovery_evidence,
                cutover_at=self._live_cutover_at,
            )
            self._adopt_view(view)
            report["view_id"] = view.view_id
            report["view_executable"] = bool(view.executable)
            report["readiness"] = view.readiness.to_dict()
        except FeedSessionError as exc:
            if exc.code == "feed_scope_mismatch":
                self.stop(reason=f"feed_scope_mismatch: {exc}")
                raise RuntimeEngineError("feed_scope_mismatch", str(exc)) from exc
            report["errors"].append({"message": str(exc), "code": exc.code})
            self._view_ok = False

    def _fan_out_events(self, events: list[Any], instant: datetime, report: dict[str, Any]) -> None:
        for event in events:
            event_mapping = event.to_dict() if hasattr(event, "to_dict") else dict(event)
            for sid, monitor in list(self._pending_monitors.items()):
                if monitor.state.terminal:
                    continue
                self._current_pending_source[sid] = event_mapping
                transition = monitor.on_second(event, now=instant, received_at=instant)
                self._pending_terminal_check(sid, monitor, transition, report)
            for sid, monitor in list(self._exit_monitors.items()):
                if monitor.state.terminal:
                    continue
                self._current_exit_source[sid] = event_mapping
                transition = monitor.on_second(event, now=instant, received_at=instant)
                self._exit_terminal_check(sid, monitor, transition, report)
            try:
                self._sink.advance_cursor(self._config.symbol, make_second_ref(event_mapping))
            except Exception as exc:
                report["errors"].append({"message": f"cursor advance failed: {exc}", "code": "cursor_advance_failed"})
            if self._processed_end_at is None or utc_instant(event.end_at) > utc_instant(self._processed_end_at):
                self._processed_end_at = event.end_at

    def _pending_terminal_check(self, sid: str, monitor: Any, transition: Any, report: dict[str, Any]) -> None:
        status = getattr(transition, "status", None)
        if status == "filled":
            self._on_pending_fill_cb(sid, monitor.pending_id, generation=monitor.generation)
        elif status == "expired":
            self._on_pending_expiry_cb(sid, monitor.pending_id, generation=monitor.generation)
        elif monitor.state.terminal and sid in self._pending_monitors:
            try:
                if monitor.state.status == "filled":
                    self._on_pending_fill_cb(sid, monitor.pending_id, generation=monitor.generation)
                elif monitor.state.status == "expired":
                    self._on_pending_expiry_cb(sid, monitor.pending_id, generation=monitor.generation)
                elif monitor.state.status == "cancelled":
                    self._sink.commit_pending(monitor.state.to_dict(), event="cancelled", occurred_at=self._now())
                    self._pending_monitors.pop(sid, None)
            except Exception as exc:
                report["errors"].append({"message": f"pending terminal persistence failed for {sid}: {exc}", "code": "pending_terminal_failed"})

    def _exit_terminal_check(self, sid: str, monitor: Any, transition: Any, report: dict[str, Any]) -> None:
        if not monitor.state.terminal:
            return
        if sid in self._exit_monitors:
            self._persist_exit_terminal(sid, monitor)

    def _expiry_sweep(self, instant: datetime) -> None:
        for sid, monitor in list(self._pending_monitors.items()):
            if monitor.state.terminal or monitor.state.resolution_at is None:
                continue
            if instant >= monitor.state.resolution_at:
                try:
                    monitor.on_expiry(now=instant)
                except Exception as exc:
                    self._record_error(f"pending expiry failed for {sid}: {exc}", code="expiry_failed")

    # ------------------------------------------------------------------
    # Persistence sweep
    # ------------------------------------------------------------------

    def _row_snapshot(self, record: Any) -> dict[str, Any]:
        """Project one scheduler record into a durable row snapshot.

        The G027 envelope validator accepts only the compact
        ``{profile_ref_id, profile_ref_hash, strategy_id}`` profile
        projection, and the G027 restart planner builds its own transition
        envelopes directly from the stored row, so the durable strategy row
        must never carry the full G016 reference (the registry remains the
        authoritative re-seed source on restart).
        """
        snapshot = record.snapshot()
        profile = snapshot.get("profile_ref")
        if isinstance(profile, Mapping) and isinstance(profile.get("profile_ref_id"), str) and isinstance(profile.get("profile_ref_hash"), str):
            snapshot["profile_ref"] = {
                "profile_ref_id": profile["profile_ref_id"],
                "profile_ref_hash": profile["profile_ref_hash"],
                "strategy_id": profile.get("strategy_id"),
            }
        else:
            snapshot["profile_ref"] = None
        return snapshot

    def _persist_sweep(self, instant: datetime) -> None:
        if self._scheduler is None or self._sink is None:
            return
        # Reducers invoke the notification callback at most once.  If a
        # SQLite commit failed after the in-memory terminal transition, keep
        # the monitor in place and retry it from the single runtime thread on
        # the next sweep instead of losing the durable event permanently.
        for sid, monitor in list(self._pending_monitors.items()):
            self._persist_pending_created(monitor, instant)
            if monitor.state.terminal and monitor.state.status == "filled":
                self._on_pending_fill_cb(sid, monitor.pending_id, generation=monitor.generation)
            elif monitor.state.terminal and monitor.state.status == "expired":
                self._on_pending_expiry_cb(sid, monitor.pending_id, generation=monitor.generation)
        for sid, monitor in list(self._exit_monitors.items()):
            if monitor.state.terminal:
                self._persist_exit_terminal(sid, monitor)
        for sid, record in self._scheduler.records.items():
            committed = self._committed_transitions.get(sid, 0)
            if record.transition_count <= committed:
                continue
            committed_keys = self._committed_transition_keys.get(sid, set())
            timers: list[dict[str, Any]] = []
            kinds: set[str] = set()
            for kind, token in record.timer_tokens.items():
                if token.active:
                    kinds.add(kind)
                    timers.append(
                        {
                            "strategy_id": sid,
                            "kind": kind,
                            "generation": token.generation,
                            "due_at": token.due_at,
                        }
                    )
            previous_kinds = self._committed_timer_kinds.get(sid, frozenset())
            clear_timers = sorted(previous_kinds - kinds)
            failed = False
            for event in record.transition_log[committed:]:
                key = (event.get("generation"), event.get("to"), event.get("reason"))
                transition = {
                    "from": event.get("from"),
                    "to": event.get("to"),
                    "reason": event.get("reason"),
                }
                try:
                    if key in committed_keys:
                        # The G027 state-transition idempotency key binds
                        # (generation, to, reason): a repeated same-key
                        # transition (for example a market_wait flap) is
                        # persisted as a row-only update, never as a second
                        # conflicting event.
                        self._sink.commit_state(
                            self._row_snapshot(record),
                            transition=None,
                            timers=timers,
                            clear_timers=clear_timers,
                            occurred_at=instant,
                        )
                    else:
                        self._sink.commit_state(
                            self._row_snapshot(record),
                            transition=transition,
                            timers=timers,
                            clear_timers=clear_timers,
                            occurred_at=instant,
                        )
                        committed_keys.add(key)
                except Exception as exc:
                    self._record_error(f"state persistence failed for {sid}: {exc}", code="state_persist_failed")
                    failed = True
                    break
            if failed:
                continue
            self._committed_transitions[sid] = record.transition_count
            self._committed_timer_kinds[sid] = frozenset(kinds)
            self._committed_transition_keys[sid] = committed_keys

    # ------------------------------------------------------------------
    # Run / stop / status
    # ------------------------------------------------------------------

    def _next_runtime_wait(self, now: datetime, deadline: datetime | None) -> float:
        """Return one bounded wait interval for an idle run loop."""
        if self._provider_pool is not None and self._provider_pool.pending_results:
            return 0.0
        if self._scheduler is None:
            return 0.0
        if self._scheduler.queue_size > 0 and self._scheduler.available_provider_slots > 0:
            return 0.0
        due_times: list[datetime] = []
        for record in self._scheduler.records.values():
            for token in record.timer_tokens.values():
                if token.active:
                    due_times.append(token.due_at)
        if self._recovery_retry_at is not None:
            due_times.append(self._recovery_retry_at)
        timeout = _RUNTIME_WAIT_SECONDS
        if due_times:
            timeout = min(timeout, max(0.0, (min(due_times) - now).total_seconds()))
        if deadline is not None:
            timeout = min(timeout, max(0.0, (deadline - now).total_seconds()))
        return max(0.0, timeout)

    def _wait_for_runtime_activity(
        self,
        *,
        now: datetime,
        deadline: datetime | None,
        stop_event: ThreadingEvent | None,
    ) -> None:
        timeout = self._next_runtime_wait(now, deadline)
        if timeout <= 0 or stop_event is not None and stop_event.is_set():
            return
        # Provider completions and stop() notify this condition.  The bounded
        # timeout also gives a live feed and fake-clock caller a chance to be
        # observed without a background thread mutating scheduler/SQLite.
        with self._provider_condition:
            self._provider_condition.wait(timeout=timeout)

    def run(
        self,
        *,
        max_steps: int | None = None,
        until: datetime | str | None = None,
        stop_event: ThreadingEvent | None = None,
    ) -> dict[str, Any]:
        """Loop :meth:`step` until stopped, until the deadline or max_steps."""
        if self._stopped:
            raise RuntimeEngineError("stopped", "the runtime is stopped")
        if not self._started:
            # A prior start may have completed its typed pre-start failure
            # report (for example missing history).  Do not reopen the same
            # database and repeat external startup work just because the
            # caller asked to run after that report.
            if self._startup_report is None:
                self.start()
            if not self._started:
                # start() has already stopped the feed and released the
                # database lease when no executable startup view exists.
                return {
                    "steps": 0,
                    "provider_calls": self._provider_calls,
                    "fills": self._fills,
                    "closes": self._closes,
                    "expiries": self._expiries,
                    "rejections": self._rejections,
                    "final_status": self.status(),
                }
        steps = 0
        deadline = utc_instant(until) if until is not None else None
        while not self._stopped:
            if stop_event is not None and stop_event.is_set():
                break
            if deadline is not None and self._now() >= deadline:
                break
            if max_steps is not None and steps >= max_steps:
                break
            try:
                self.step()
                steps += 1
                if not self._stopped:
                    self._wait_for_runtime_activity(
                        now=self._now(),
                        deadline=deadline,
                        stop_event=stop_event,
                    )
            except RuntimeEngineError as exc:
                self.stop(reason=str(exc))
                break
            except (LeaseConflictError, StorageClosedError, StorageError) as exc:
                self.stop(reason=f"storage failure: {exc}")
                break
            except KeyboardInterrupt:
                self.stop(reason="keyboard_interrupt")
                break
            except Exception as exc:
                self.stop(reason=f"runtime error: {type(exc).__name__}")
                break
        return {
            "steps": steps,
            "provider_calls": self._provider_calls,
            "fills": self._fills,
            "closes": self._closes,
            "expiries": self._expiries,
            "rejections": self._rejections,
            "final_status": self.status(),
        }

    def stop(self, *, reason: str = "stopped") -> dict[str, Any]:
        """Stop the feed, persist pending transitions and release the lease."""
        with self._lifecycle_lock:
            if self._stopped:
                return self._stop_report(self._stop_reason or reason)
            self._stopped = True
            self._started = False
            self._stop_reason = reason
            with self._provider_condition:
                self._provider_condition.notify_all()
            if self._provider_pool is not None:
                try:
                    self._provider_pool.close()
                except Exception:
                    pass
            if self._feed is not None:
                try:
                    self._stream_stop_report = self._feed.stop_stream()
                except Exception:
                    pass
            # Preserve already-completed responses before the database lease is
            # released.  A provider that remains blocked is left as an
            # analyzing generation for G027 restart recovery; no worker writes
            # SQLite.
            try:
                self._drain_provider_results()
            except Exception:
                pass
            try:
                self._persist_sweep(self._now())
            except Exception:
                pass
            if self._db is not None:
                try:
                    self._db.close(release_lease=True)
                except Exception:
                    pass
                self._db = None
            return self._stop_report(reason)

    def _stop_report(self, reason: str) -> dict[str, Any]:
        report = {
            "run_id": self._run_id,
            "stopped": True,
            "reason": reason,
            "started": self._started,
            "provider_calls": self._provider_calls,
            "fills": self._fills,
            "closes": self._closes,
            "expiries": self._expiries,
            "rejections": self._rejections,
        }
        if self._stream_stop_report is not None:
            report["stream"] = self._stream_stop_report
        return report

    def status(self) -> dict[str, Any]:
        """Local diagnostic/status mapping (never model-facing)."""
        strategies: list[dict[str, Any]] = []
        accounts: list[dict[str, Any]] = []
        if self._scheduler is not None:
            for sid in sorted(self._scheduler.records):
                record = self._scheduler.records[sid]
                strategies.append(
                    {
                        "strategy_id": sid,
                        "state": record.state.value if isinstance(record.state, StrategyState) else str(record.state),
                        "generation": record.generation,
                        "enabled": record.enabled,
                        "exhausted": record.exhausted,
                        "pending_id": record.pending_id,
                        "open_position_id": record.open_position_id,
                        "next_due_at": _stamp(record.next_due_at) if record.next_due_at else None,
                        "expires_at": _stamp(record.expires_at) if record.expires_at else None,
                        "timers": sorted(
                            {
                                "kind": kind,
                                "generation": token.generation,
                                "due_at": _stamp(token.due_at),
                                "active": token.active,
                            }
                            for kind, token in record.timer_tokens.items()
                        ),
                        "blocked_reason": record.blocked_reason,
                    }
                )
        if self._book is not None:
            for sid in sorted(self._book.accounts):
                account = self._book.accounts[sid]
                accounts.append(
                    {
                        "strategy_id": sid,
                        "exhausted": bool(account.exhausted),
                        "pending": bool(account.pending),
                        "open": account.open_trade is not None,
                    }
                )
        cursors: list[dict[str, Any]] = []
        live_blocked = False
        if self._sink is not None:
            try:
                cursors = self._sink.list_cursors()
                live_blocked = self._sink.is_live_blocked(self._config.symbol)
            except Exception:
                pass
        events = list(self._recent_events[-REPORT_RECENT_EVENTS:])
        view = self._current_view
        stream = self._feed.status().get("stream", {}) if self._feed is not None else {}
        return {
            "run_id": self._run_id,
            "started": self._started,
            "stopped": self._stopped,
            "mode": self._config.mode,
            "paper_only": self._config.paper_only_label(),
            "no_live_orders": True,
            "symbol": self._config.symbol,
            "timeframe": self._config.timeframe_mapping(),
            "feed": self._config.feed_key,
            "market_data_usage": self._config.market_data_usage,
            "view_id": view.view_id if view is not None else None,
            "view_executable": bool(view is not None and view.executable),
            "readiness": view.readiness.to_dict() if view is not None else {},
            "strategies": strategies,
            "accounts": accounts,
            "cursors": cursors,
            "live_blocked": live_blocked,
            "provider_calls": self._provider_calls,
            "provider_in_flight": len(self._provider_tasks),
            "provider_workers_alive": self._provider_pool.alive_workers if self._provider_pool is not None else 0,
            "provider_pending_results": self._provider_pool.pending_results if self._provider_pool is not None else 0,
            "events": events,
            "errors": list(self._errors),
            "recovery_pending": self._recovery_pending,
            "stream": stream,
        }


def _open_runtime_database(path: str, *, run_id: str, mode: str, clock: Callable[[], datetime]) -> Any:
    from pa_engine.storage import open_database

    return open_database(path, run_id=run_id, mode=mode, lease_ttl_seconds=30, clock=clock)


__all__ = ["AutomaticPaperRuntime", "RuntimeEngineError"]
