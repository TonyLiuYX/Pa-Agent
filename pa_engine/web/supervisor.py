"""Single-owner runtime supervisor for the G032 local web service."""

from __future__ import annotations

import queue
import json
import threading
import time
import uuid
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any, Callable, Mapping

from pa_engine.market_sources.alpaca.history.adapter import AlpacaHistoryAdapter
from pa_engine.market_sources.alpaca.stream.aggregate import LocalSecondAggregator
from pa_engine.market_sources.alpaca.stream.client import AlpacaIEXClient
from pa_engine.providers.deepseek.adapter import DeepSeekAdapter
from pa_engine.providers.openai.adapter import OpenAIAdapter
from pa_engine.providers.openai.config import OpenAIConfig
from pa_engine.runtime.config import RuntimeConfig
from pa_engine.runtime.engine import AutomaticPaperRuntime, RuntimeEngineError
from pa_engine.storage import LeaseConflictError, RuntimeStore, StorageError, open_database
from pa_engine.strategy.runtime.clock import SystemClock

from .config import StartRequest, WebConfig, WebConfigError
from .presentation import PresentationSnapshot, build_presentation_snapshot
from .workspace import ProductWorkspace, RuntimeLaunch


_ACTIVE_LIFECYCLES = frozenset({"starting", "running", "waiting_market", "recovery", "stopping"})
_DERIVED_LIFECYCLES = frozenset({"starting", "running", "waiting_market", "recovery"})
_MODEL_ERROR_TOKENS = frozenset(
    {
        "auth",
        "authentication",
        "credential",
        "forbidden",
        "provider",
        "rate",
        "too_many_requests",
        "transport",
        "network",
        "connection",
        "timeout",
        "unauthorized",
    }
)
_WEB_RUNTIME_CONFIG_KEY = "web:g032:runtime_binding:v1"
_TERMINAL_STRATEGY_STATES = frozenset({"disabled", "exhausted"})
_STOP_TIMEOUT_SECONDS = 8.0


class SupervisorError(RuntimeError):
    """Secret-safe owner/supervisor failure."""

    def __init__(self, code: str, message: str) -> None:
        self.code = str(code)
        super().__init__(str(message))


@dataclass
class _Command:
    kind: str
    launch: RuntimeLaunch | None = None
    done: Future | None = None


def _safe_error(code: str, message: Any, secrets: tuple[str, ...] = ()) -> dict[str, str]:
    text = str(message)
    for secret in secrets:
        if secret and len(secret) >= 4:
            text = text.replace(secret, "<redacted>")
    # Provider adapters already return typed/sanitized text.  The generic
    # fallback deliberately keeps only a stable implementation-free message.
    if len(text) > 400:
        text = text[:400]
    return {"code": str(code)[:120], "message": text}


def _is_model_error(value: Any) -> bool:
    text = str(value or "").lower()
    return any(token in text for token in _MODEL_ERROR_TOKENS)


def _error_lifecycle(error: Mapping[str, Any]) -> str:
    return "model_error" if _is_model_error(error.get("code")) or _is_model_error(error.get("message")) else "blocked"


class RuntimeSupervisor:
    """Own one runtime and all runtime mutations on one background thread.

    HTTP handlers never hold a runtime reference.  They enqueue start/stop
    commands and read the last immutable :class:`PresentationSnapshot`.
    """

    def __init__(
        self,
        web_config: WebConfig | None = None,
        *,
        runtime_factory: Callable[[RuntimeConfig, RuntimeLaunch, WebConfig], Any] | None = None,
        registry: Any = None,
        workspace: ProductWorkspace | None = None,
        preflight_database: bool = True,
    ) -> None:
        self.web_config = web_config or WebConfig()
        self._runtime_factory = runtime_factory
        self.workspace = workspace or ProductWorkspace(self.web_config, built_in_registry=registry)
        self._preflight_database = bool(preflight_database)
        self._commands: queue.Queue[_Command] = queue.Queue()
        self._wake = threading.Event()
        self._closed = threading.Event()
        self._state_lock = threading.RLock()
        self._owner_thread = threading.Thread(target=self._owner_loop, name="pa-web-runtime-owner", daemon=False)
        self._runtime: Any = None
        self._public_config: dict[str, Any] | None = None
        self._active_secret_values: tuple[str, ...] | None = None
        self._last_error: dict[str, str] | None = None
        self._active_model_error: dict[str, str] | None = None
        self._lifecycle = "idle"
        self._pending_mutations = 0
        self._mutation_in_flight = False
        self._last_snapshot = build_presentation_snapshot(lifecycle="idle")
        self._owner_thread.start()

    def __repr__(self) -> str:
        return f"RuntimeSupervisor(lifecycle={self.lifecycle!r}, runtime=<owner-thread>)"

    @property
    def lifecycle(self) -> str:
        with self._state_lock:
            return self._lifecycle

    @property
    def owner_thread_ident(self) -> int | None:
        return self._owner_thread.ident

    @property
    def runtime_present(self) -> bool:
        with self._state_lock:
            return self._runtime is not None

    def snapshot(self) -> PresentationSnapshot:
        with self._state_lock:
            return self._last_snapshot

    def health(self) -> dict[str, Any]:
        with self._state_lock:
            return {
                "ok": not self._closed.is_set(),
                "service": "pa_engine.web",
                "lifecycle": self._lifecycle,
                "owner_thread_alive": self._owner_thread.is_alive(),
                "runtime_present": self._runtime is not None,
                "paper_only": True,
                "no_live_orders": True,
                "market_feed": "alpaca:iex:single_exchange",
                "market_data_usage": "internal_prototype",
                "configuration_locked": self._configuration_locked_locked(),
            }

    def _configuration_locked_locked(self) -> bool:
        return bool(
            self._runtime is not None
            or self._mutation_in_flight
            or self._pending_mutations
            or self._lifecycle in _ACTIVE_LIFECYCLES
        )

    @property
    def configuration_locked(self) -> bool:
        with self._state_lock:
            return self._configuration_locked_locked()

    def mutate_configuration(self, callback: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Serialize a non-runtime workspace mutation against start/stop."""

        with self.workspace.lock:
            with self._state_lock:
                if self._closed.is_set():
                    raise SupervisorError("service_closed", "web service is closing")
                if self._configuration_locked_locked():
                    raise SupervisorError("configuration_locked", "settings and strategy selection are locked while a runtime is active")
            return callback(*args, **kwargs)

    def request_start(self, request: StartRequest | Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(request, StartRequest):
            request = StartRequest.from_payload(request)
        with self.workspace.lock:
            with self._state_lock:
                if self._closed.is_set():
                    raise SupervisorError("service_closed", "web service is closing")
                if self._configuration_locked_locked():
                    raise SupervisorError("already_running", "one runtime is already starting or active")
            launch = self.workspace.prepare_launch(request)
            with self._state_lock:
                if self._configuration_locked_locked():
                    raise SupervisorError("already_running", "one runtime is already starting or active")
                self._lifecycle = "starting"
                self._public_config = launch.public_config()
                self._last_error = None
                self._active_model_error = None
                self._active_secret_values = launch.credentials.secret_values()
                self._publish_locked()
                self._pending_mutations += 1
                self._commands.put(_Command("start", launch=launch))
                self._wake.set()
                snapshot = self._last_snapshot
        return {"accepted": True, "lifecycle": "starting", "snapshot": snapshot.to_dict()}

    # Friendly alias for callers that treat the supervisor as a command API.
    start = request_start

    def request_stop(self, *, reason: str = "web_stop") -> dict[str, Any]:
        with self._state_lock:
            if self._closed.is_set():
                return {"accepted": False, "lifecycle": "stopped", "snapshot": self._last_snapshot.to_dict()}
            if self._lifecycle in {"idle", "stopped"} and self._runtime is None:
                self._lifecycle = "stopped"
                self._publish_locked()
                return {"accepted": False, "lifecycle": "stopped", "snapshot": self._last_snapshot.to_dict()}
            if self._lifecycle != "stopping":
                self._lifecycle = "stopping"
                self._publish_locked()
            done: Future = Future()
            self._pending_mutations += 1
            self._commands.put(_Command("stop", done=done))
            self._wake.set()
        try:
            result = done.result(timeout=_STOP_TIMEOUT_SECONDS)
        except Exception:
            with self._state_lock:
                return {"accepted": True, "lifecycle": self._lifecycle, "snapshot": self._last_snapshot.to_dict()}
        result = result if isinstance(result, Mapping) else {}
        return dict(result)

    stop = request_stop

    def close(self, *, timeout: float = _STOP_TIMEOUT_SECONDS) -> None:
        """Ask the owner to stop and terminate; safe to call repeatedly."""

        if self._closed.is_set():
            if self._owner_thread.is_alive():
                self._owner_thread.join(timeout=max(0.0, float(timeout)))
            return
        done: Future = Future()
        self._commands.put(_Command("shutdown", done=done))
        self._wake.set()
        try:
            done.result(timeout=max(0.0, float(timeout)))
        except Exception:
            pass
        self._closed.set()
        self._owner_thread.join(timeout=max(0.0, float(timeout)))
        self.workspace.close()

    def _publish_locked(self, *, runtime: Any | None = None, lifecycle: str | None = None, last_error: Mapping[str, Any] | None = None) -> None:
        selected_runtime = self._runtime if runtime is None else runtime
        selected_error = self._last_error if last_error is None else dict(last_error)
        requested_lifecycle = self._lifecycle if lifecycle is None else lifecycle
        # ``starting``/``running`` are not runtime facts once an object has
        # been constructed.  Let the immutable projection derive the active
        # state from status + readiness; only control/failure states remain
        # explicit at this seam.
        if selected_runtime is not None and requested_lifecycle in _DERIVED_LIFECYCLES:
            requested_lifecycle = ""
        snapshot = build_presentation_snapshot(
            selected_runtime,
            lifecycle=requested_lifecycle,
            public_config=self._public_config,
            last_error=selected_error,
            secret_values=self._active_secret_values or (),
        )
        self._last_snapshot = snapshot
        if selected_runtime is self._runtime and self._lifecycle not in {"stopping", "stopped"} and requested_lifecycle == "":
            self._lifecycle = snapshot.lifecycle

    def _build_config(self, launch: RuntimeLaunch) -> RuntimeConfig:
        request = launch.request
        return RuntimeConfig(
            symbol=request.symbol,
            timeframe=request.timeframe,
            provider=launch.provider,
            reasoning_mode=launch.reasoning_mode,
            openai_speed=launch.openai_speed,
            execution_policy=request.execution_policy,
            database_path=self.web_config.database_path,
            minimum_closed_bars=self.web_config.minimum_closed_bars,
            stale_after_seconds=self.web_config.stale_after_seconds,
            queue_capacity=self.web_config.queue_capacity,
            max_in_flight=self.web_config.max_in_flight,
        )

    def _preflight_database_state(self, launch: RuntimeLaunch) -> None:
        """Check and bind non-terminal web state before constructing adapters/network clients.

        The binding lives in one namespaced ``meta`` row.  It contains only
        the selected symbol/timeframe; credentials never enter this table.
        The row is read and updated under the normal database lease and one
        ``BEGIN IMMEDIATE`` transaction, so a web start cannot race another
        owner into silently changing the recovery configuration.
        """

        if not self._preflight_database:
            return
        request = launch.request
        selected_ids = set(launch.selected_strategy_ids)
        run_id = "web-preflight-" + uuid.uuid4().hex
        db = None
        try:
            db = open_database(self.web_config.database_path, run_id=run_id, mode="automatic")
            store = RuntimeStore(db)
            with db.transaction() as conn:
                pending_rows = store.list_pending_entries(status="pending")
                open_rows = store.list_trades(status="open")
                cursor_rows = store.list_cursors()
                state_rows = store.list_strategy_states()
                if any(row.get("symbol") != request.symbol for row in pending_rows):
                    raise SupervisorError("database_configuration_conflict", "database has a pending position for another symbol")
                if any(row.get("symbol") != request.symbol for row in open_rows):
                    raise SupervisorError("database_configuration_conflict", "database has an open position for another symbol")
                active_trade_ids = {
                    str(row.get("strategy_id"))
                    for row in (*pending_rows, *open_rows)
                    if isinstance(row.get("strategy_id"), str)
                }
                if not active_trade_ids.issubset(selected_ids):
                    raise SupervisorError(
                        "strategy_selection_conflict",
                        "selected setups exclude a strategy with pending or open paper state",
                    )
                if active_trade_ids and request.execution_policy != "paper":
                    raise SupervisorError(
                        "execution_policy_conflict",
                        "advisory mode cannot resume pending or open paper state",
                    )
                if any(
                    row.get("symbol") != request.symbol
                    and (
                        row.get("last_second_end_at") is not None
                        or bool(row.get("live_blocked"))
                        or row.get("recovery_cursor") is not None
                        or row.get("pending_recovery_json") is not None
                    )
                    for row in cursor_rows
                ):
                    raise SupervisorError("database_configuration_conflict", "database has a market cursor for another symbol")
                active_timer = conn.execute(
                    "SELECT 1 FROM timers WHERE active = 1 AND fired = 0 LIMIT 1"
                ).fetchone()
                non_terminal = bool(
                    pending_rows
                    or open_rows
                    or active_timer is not None
                    or any(
                        row.get("last_second_end_at") is not None
                        or bool(row.get("live_blocked"))
                        or row.get("recovery_cursor") is not None
                        or row.get("pending_recovery_json") is not None
                        for row in cursor_rows
                    )
                    or any(row.get("state") not in _TERMINAL_STRATEGY_STATES for row in state_rows)
                )
                bound_row = conn.execute(
                    "SELECT value FROM meta WHERE key = ?", (_WEB_RUNTIME_CONFIG_KEY,)
                ).fetchone()
                bound: dict[str, str] | None = None
                if bound_row is not None:
                    try:
                        candidate = json.loads(bound_row[0])
                    except (TypeError, ValueError, json.JSONDecodeError):
                        candidate = None
                    if isinstance(candidate, Mapping) and isinstance(candidate.get("symbol"), str) and isinstance(candidate.get("timeframe"), str):
                        bound = {
                            "symbol": candidate["symbol"],
                            "timeframe": candidate["timeframe"],
                            "execution_policy": candidate.get("execution_policy", "paper"),
                        }
                requested = {
                    "symbol": request.symbol,
                    "timeframe": request.timeframe,
                    "execution_policy": request.execution_policy,
                }
                if non_terminal:
                    if bound is None:
                        raise SupervisorError(
                            "database_configuration_conflict",
                            "non-terminal database has no web symbol/timeframe binding",
                        )
                    bound_identity = {key: bound[key] for key in ("symbol", "timeframe")}
                    requested_identity = {key: requested[key] for key in ("symbol", "timeframe")}
                    if bound_identity != requested_identity:
                        raise SupervisorError(
                            "database_configuration_conflict",
                            "non-terminal database is bound to another symbol/timeframe",
                        )
                # With no pending/open trade, advisory and paper are compatible
                # views over the same symbol/timeframe recovery state.  The
                # explicit active-trade guard above blocks the only unsafe
                # paper-to-advisory switch.  Persist the newly frozen policy for
                # the next resume without touching account/trade history.
                conn.execute(
                    "INSERT INTO meta (key, value) VALUES (?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                    (_WEB_RUNTIME_CONFIG_KEY, json.dumps(requested, sort_keys=True, separators=(",", ":"))),
                )
        except SupervisorError:
            raise
        except LeaseConflictError as exc:
            raise SupervisorError("lease_conflict", "runtime database is already in use") from exc
        except StorageError as exc:
            raise SupervisorError(getattr(exc, "code", "database_error"), "runtime database cannot be used") from exc
        except OSError as exc:
            raise SupervisorError("database_error", "runtime database cannot be opened") from exc
        finally:
            if db is not None:
                try:
                    db.close(release_lease=True)
                except Exception:
                    pass

    def _make_runtime(self, config: RuntimeConfig, launch: RuntimeLaunch) -> Any:
        request = launch.request
        credentials = launch.credentials
        if self._runtime_factory is not None:
            return self._runtime_factory(config, launch, self.web_config)
        aggregator = LocalSecondAggregator(request.symbol)
        registry = launch.registry
        if launch.provider == "deepseek":
            adapter = DeepSeekAdapter(api_key=credentials.model_api_key)
        else:
            adapter = OpenAIAdapter(api_key=credentials.model_api_key, config=OpenAIConfig(speed=launch.openai_speed))
        history_adapter = AlpacaHistoryAdapter(key_id=credentials.alpaca_key_id, secret_key=credentials.alpaca_secret_key)
        stream_client = AlpacaIEXClient(
            request.symbol,
            key_id=credentials.alpaca_key_id,
            secret_key=credentials.alpaca_secret_key,
            aggregator=aggregator,
        )
        return AutomaticPaperRuntime(
            config,
            registry=registry,
            adapter=adapter,
            history_adapter=history_adapter,
            aggregator=aggregator,
            stream_client=stream_client,
            db_path=self.web_config.database_path,
            run_id="web-" + uuid.uuid4().hex,
            clock=SystemClock(),
            startup_history_seconds=self.web_config.startup_history_seconds,
        )

    def _start_owner(self, launch: RuntimeLaunch) -> None:
        runtime: Any = None
        try:
            config = self._build_config(launch)
            self._preflight_database_state(launch)
            runtime = self._make_runtime(config, launch)
            report = runtime.start()
            status_after_start = _mapping_status(runtime)
            started = (
                bool(report.get("started"))
                if isinstance(report, Mapping) and "started" in report
                else bool(status_after_start.get("started"))
            )
            if not started:
                errors = report.get("errors") if isinstance(report, Mapping) else None
                if not isinstance(errors, list) or not errors:
                    errors = status_after_start.get("errors") if isinstance(status_after_start.get("errors"), list) else None
                first = errors[-1] if isinstance(errors, list) and errors else {"code": "startup_failed", "message": "runtime did not become ready"}
                error = _safe_error(first.get("code", "startup_failed") if isinstance(first, Mapping) else "startup_failed", first.get("message", "runtime did not become ready") if isinstance(first, Mapping) else first, self._active_secret_values or ())
                with self._state_lock:
                    self._runtime = runtime
                    self._last_error = error
                    self._active_model_error = error if _is_model_error(error.get("code")) or _is_model_error(error.get("message")) else None
                    self._lifecycle = _error_lifecycle(error)
                    self._publish_locked(runtime=runtime, lifecycle=self._lifecycle)
                self._drop_runtime(runtime, reason="startup_failed", keep_snapshot=True)
                return
            with self._state_lock:
                self._runtime = runtime
                startup_error = _latest_model_error(report) or _latest_model_error(status_after_start)
                self._active_model_error = startup_error
                self._last_error = startup_error
                self._publish_locked(runtime=runtime, lifecycle="")
        except SupervisorError as exc:
            error = _safe_error(exc.code, str(exc), self._active_secret_values or ())
            with self._state_lock:
                self._last_error = error
                self._active_model_error = error if _is_model_error(error.get("code")) or _is_model_error(error.get("message")) else None
                self._lifecycle = "blocked"
                self._publish_locked(runtime=runtime, last_error=error)
            if runtime is not None:
                self._drop_runtime(runtime, reason=exc.code, keep_snapshot=True)
        except (RuntimeEngineError, StorageError, LeaseConflictError) as exc:
            code = getattr(exc, "code", "runtime_start_failed")
            error = _safe_error(code, str(exc), self._active_secret_values or ())
            with self._state_lock:
                self._last_error = error
                self._active_model_error = error if _is_model_error(error.get("code")) or _is_model_error(error.get("message")) else None
                self._lifecycle = _error_lifecycle(error)
                self._publish_locked(runtime=runtime, last_error=error)
            if runtime is not None:
                self._drop_runtime(runtime, reason=str(code), keep_snapshot=True)
        except Exception as exc:
            error = _safe_error("runtime_start_failed", "runtime start failed", self._active_secret_values or ())
            with self._state_lock:
                self._last_error = error
                self._active_model_error = error if _is_model_error(error.get("code")) or _is_model_error(error.get("message")) else None
                self._lifecycle = "blocked"
                self._publish_locked(runtime=runtime, last_error=error)
            if runtime is not None:
                self._drop_runtime(runtime, reason="runtime_start_failed", keep_snapshot=True)
        finally:
            # The command object is cleared by the owner loop as well; this
            # local deletion makes the lifetime explicit at the adapter seam.
            launch = None  # type: ignore[assignment]

    def _step_owner(self) -> None:
        runtime = self._runtime
        if runtime is None:
            return
        before_status = _mapping_status(runtime)
        before_errors = _status_error_signatures(before_status)
        try:
            runtime.step()
            after_status = _mapping_status(runtime)
            current_error = _current_status_model_error(after_status, self._active_model_error)
            new_error = _new_status_model_error(after_status, before_errors)
            with self._state_lock:
                if self._lifecycle not in {"stopping", "stopped"}:
                    self._active_model_error = current_error or new_error
                    self._last_error = self._active_model_error
                    self._publish_locked(runtime=runtime, lifecycle="")
        except (RuntimeEngineError, StorageError, LeaseConflictError) as exc:
            error = _safe_error(getattr(exc, "code", "runtime_step_failed"), str(exc), self._active_secret_values or ())
            with self._state_lock:
                self._last_error = error
                self._active_model_error = error if _is_model_error(error.get("code")) or _is_model_error(error.get("message")) else None
                self._lifecycle = _error_lifecycle(error)
            self._drop_runtime(runtime, reason=error["code"], keep_snapshot=True, error=error)
        except Exception:
            error = _safe_error("runtime_step_failed", "runtime step failed", self._active_secret_values or ())
            with self._state_lock:
                self._last_error = error
                self._active_model_error = error if _is_model_error(error.get("code")) or _is_model_error(error.get("message")) else None
                self._lifecycle = "blocked"
            self._drop_runtime(runtime, reason="runtime_step_failed", keep_snapshot=True, error=error)

    def _drop_runtime(
        self,
        runtime: Any,
        *,
        reason: str,
        keep_snapshot: bool,
        error: Mapping[str, Any] | None = None,
    ) -> None:
        with self._state_lock:
            if keep_snapshot:
                self._last_snapshot = build_presentation_snapshot(
                    runtime,
                    lifecycle=self._lifecycle,
                    public_config=self._public_config,
                    last_error=error or self._last_error,
                    secret_values=self._active_secret_values or (),
                )
        try:
            runtime.stop(reason=reason)
        except Exception:
            pass
        with self._state_lock:
            if self._runtime is runtime:
                self._runtime = None
            self._active_secret_values = None
            self._active_model_error = None

    def _stop_owner(self) -> dict[str, Any]:
        runtime = self._runtime
        if runtime is None:
            with self._state_lock:
                self._lifecycle = "stopped"
                self._active_secret_values = None
                self._publish_locked()
                return {"accepted": False, "lifecycle": "stopped", "snapshot": self._last_snapshot.to_dict()}
        with self._state_lock:
            final_snapshot = build_presentation_snapshot(
                runtime,
                lifecycle="stopped",
                public_config=self._public_config,
                last_error=self._last_error,
                secret_values=self._active_secret_values or (),
            )
        try:
            runtime.stop(reason="web_stop")
        except Exception:
            pass
        with self._state_lock:
            self._runtime = None
            self._active_secret_values = None
            self._active_model_error = None
            self._lifecycle = "stopped"
            self._last_snapshot = final_snapshot
            return {"accepted": True, "lifecycle": "stopped", "snapshot": final_snapshot.to_dict()}

    def _owner_loop(self) -> None:
        last_step = 0.0
        while not self._closed.is_set():
            command: _Command | None = None
            try:
                command = self._commands.get(timeout=0.20)
            except queue.Empty:
                command = None
            if command is not None:
                if command.kind in {"start", "stop"}:
                    with self._state_lock:
                        self._pending_mutations = max(0, self._pending_mutations - 1)
                        self._mutation_in_flight = True
                    try:
                        if command.kind == "start" and command.launch is not None:
                            launch = command.launch
                            command.launch = None
                            self._start_owner(launch)
                        elif command.kind == "stop":
                            result = self._stop_owner()
                            if command.done is not None and not command.done.done():
                                command.done.set_result(result)
                    finally:
                        with self._state_lock:
                            self._mutation_in_flight = False
                elif command.kind == "shutdown":
                    self._stop_owner()
                    self._closed.set()
                    if command.done is not None and not command.done.done():
                        command.done.set_result({"closed": True})
                    break
            now = time.monotonic()
            if (
                self._runtime is not None
                and self._lifecycle in {"running", "waiting_market", "recovery", "model_error"}
                and now - last_step >= self.web_config.step_interval_seconds
            ):
                self._step_owner()
                last_step = now
            self._wake.clear()


def _mapping_status(runtime: Any) -> Mapping[str, Any]:
    try:
        value = runtime.status()
    except Exception:
        return {}
    return value if isinstance(value, Mapping) else {}


def _status_error_signatures(status: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    errors = status.get("errors")
    if not isinstance(errors, (list, tuple)):
        return ()
    return tuple(
        (str(error.get("code") or ""), str(error.get("message") or ""))
        for error in errors
        if isinstance(error, Mapping)
    )


def _latest_model_error(value: Any) -> dict[str, str] | None:
    errors = value.get("errors") if isinstance(value, Mapping) else None
    if not isinstance(errors, (list, tuple)):
        return None
    for error in reversed(errors):
        if isinstance(error, Mapping) and (_is_model_error(error.get("code")) or _is_model_error(error.get("message"))):
            return _safe_error(error.get("code") or "provider_error", error.get("message") or "provider error")
    return None


def _current_status_model_error(status: Mapping[str, Any], prior_active: Mapping[str, Any] | None = None) -> dict[str, str] | None:
    latest = _latest_model_error(status)
    strategies = status.get("strategies")
    if not isinstance(strategies, (list, tuple)):
        return None
    for strategy in strategies:
        if not isinstance(strategy, Mapping):
            continue
        state = str(strategy.get("state") or "").lower()
        reason = strategy.get("blocked_reason") or strategy.get("error")
        if state == "blocked" and _is_model_error(reason):
            return latest or _safe_error("provider_error", reason)
        if state == "retry_wait" and prior_active is not None:
            return dict(prior_active)
    return None


def _new_status_model_error(status: Mapping[str, Any], before: tuple[tuple[str, str], ...]) -> dict[str, str] | None:
    before_set = set(before)
    errors = status.get("errors")
    if not isinstance(errors, (list, tuple)):
        return None
    for error in reversed(errors):
        if not isinstance(error, Mapping):
            continue
        signature = (str(error.get("code") or ""), str(error.get("message") or ""))
        if signature not in before_set and (_is_model_error(error.get("code")) or _is_model_error(error.get("message"))):
            return _safe_error(error.get("code") or "provider_error", error.get("message") or "provider error")
    return None


__all__ = ["RuntimeSupervisor", "StartRequest", "SupervisorError"]
