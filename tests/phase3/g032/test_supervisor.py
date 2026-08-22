from __future__ import annotations

import time
import threading
from pathlib import Path

import pytest

from pa_engine.web.config import StartRequest, WebConfig
from pa_engine.web.supervisor import RuntimeSupervisor, SupervisorError

from .conftest import FixtureRuntime, configured_workspace, fixture_runtime_factory


MODEL_CANARY = "G032_MODEL_CANARY_7f3c"
ALPACA_ID_CANARY = "G032_ALPACA_ID_CANARY_9a1d"
ALPACA_SECRET_CANARY = "G032_ALPACA_SECRET_CANARY_4b8e"


def request() -> StartRequest:
    return StartRequest.from_payload(
        {
            "symbol": "AAPL",
            "timeframe": "1min",
            "execution_policy": "paper",
        }
    )


def wait_for(supervisor: RuntimeSupervisor, lifecycle: str, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if supervisor.lifecycle == lifecycle:
            return
        time.sleep(0.01)
    raise AssertionError(f"expected lifecycle {lifecycle!r}, got {supervisor.lifecycle!r}")


def test_one_owner_thread_serializes_start_step_stop_and_releases_keys(tmp_path: Path):
    config = WebConfig(database_path=str(tmp_path / "runtime.db"), workspace_path=str(tmp_path / "workspace"), step_interval_seconds=1.0)
    workspace = configured_workspace(config)
    supervisor = RuntimeSupervisor(config, workspace=workspace, runtime_factory=fixture_runtime_factory, preflight_database=False)
    try:
        accepted = supervisor.request_start(request())
        assert accepted["accepted"] is True
        assert accepted["lifecycle"] == "starting"
        assert MODEL_CANARY not in str(accepted)
        assert ALPACA_SECRET_CANARY not in str(accepted)

        wait_for(supervisor, "running")
        runtime = supervisor._runtime
        assert isinstance(runtime, FixtureRuntime)
        assert runtime.start_thread == supervisor.owner_thread_ident
        assert runtime.step_thread == supervisor.owner_thread_ident
        assert supervisor.runtime_present is True

        # The owner loop is deliberately throttled to one step per second.
        calls = runtime.calls
        time.sleep(0.2)
        assert runtime.calls == calls

        with pytest.raises(SupervisorError) as duplicate:
            supervisor.request_start(request())
        assert duplicate.value.code == "already_running"

        stopped = supervisor.request_stop()
        assert stopped["lifecycle"] == "stopped"
        assert runtime.stop_thread == supervisor.owner_thread_ident
        assert supervisor.runtime_present is False
        assert supervisor._active_secret_values is None
        assert workspace.settings()["credentials"] == {"alpaca": True, "deepseek": True, "openai": False}
        serialized = str(supervisor.snapshot().to_dict())
        assert MODEL_CANARY not in serialized
        assert ALPACA_ID_CANARY not in serialized
        assert ALPACA_SECRET_CANARY not in serialized

        # A second run can resume through the same supervisor only after the
        # first runtime has fully released its owner-held references.
        supervisor.request_start(request())
        wait_for(supervisor, "running")
        second_runtime = supervisor._runtime
        assert isinstance(second_runtime, FixtureRuntime)
        assert second_runtime is not runtime
        assert supervisor.request_stop()["lifecycle"] == "stopped"
    finally:
        supervisor.close()


def test_stop_before_start_is_idempotent(tmp_path: Path):
    config = WebConfig(database_path=str(tmp_path / "runtime.db"), workspace_path=str(tmp_path / "workspace"))
    supervisor = RuntimeSupervisor(
        config,
        workspace=configured_workspace(config),
        runtime_factory=fixture_runtime_factory,
        preflight_database=False,
    )
    try:
        first = supervisor.request_stop()
        second = supervisor.request_stop()
        assert first["accepted"] is False
        assert second["accepted"] is False
        assert first["lifecycle"] == second["lifecycle"] == "stopped"
    finally:
        supervisor.close()


def test_typed_preflight_failure_is_safe_and_does_not_construct_runtime(tmp_path: Path):
    config = WebConfig(database_path=str(tmp_path / "runtime.db"), workspace_path=str(tmp_path / "workspace"))
    supervisor = RuntimeSupervisor(
        config,
        workspace=configured_workspace(config),
        runtime_factory=fixture_runtime_factory,
        preflight_database=False,
    )
    supervisor._preflight_database_state = lambda _launch: (_ for _ in ()).throw(  # type: ignore[method-assign]
        SupervisorError("lease_conflict", "runtime database is already in use")
    )
    try:
        supervisor.request_start(request())
        wait_for(supervisor, "blocked")
        value = supervisor.snapshot().to_dict()
        assert value["errors"][-1] == {"code": "lease_conflict", "message": "runtime database is already in use"}
        assert value["lifecycle"] == "blocked"
        assert supervisor.runtime_present is False
        assert MODEL_CANARY not in str(value)
        assert ALPACA_SECRET_CANARY not in str(value)
    finally:
        supervisor.close()


def test_preflight_database_bytes_do_not_contain_runtime_keys(tmp_path: Path):
    database_path = tmp_path / "runtime.db"
    config = WebConfig(database_path=str(database_path), workspace_path=str(tmp_path / "workspace"))
    supervisor = RuntimeSupervisor(
        config,
        workspace=configured_workspace(config),
        runtime_factory=fixture_runtime_factory,
        preflight_database=True,
    )
    try:
        supervisor.request_start(request())
        wait_for(supervisor, "running")
        supervisor.request_stop()
    finally:
        supervisor.close()
    assert database_path.exists()
    database_bytes = database_path.read_bytes()
    assert MODEL_CANARY.encode() not in database_bytes
    assert ALPACA_ID_CANARY.encode() not in database_bytes
    assert ALPACA_SECRET_CANARY.encode() not in database_bytes


class _LifecycleRuntime(FixtureRuntime):
    def __init__(self, config, mode: str):
        super().__init__(config)
        self.mode = mode
        if mode == "waiting_market":
            self._current_view.readiness = {**self._current_view.readiness, "executable": False, "session_ready": False}
        elif mode == "recovery":
            self._current_view.readiness = {**self._current_view.readiness, "recovery_required": True, "executable": False}

    def status(self):
        value = super().status()
        if self.mode == "recovery":
            value["recovery_pending"] = True
        elif self.mode == "provider_rate_failure":
            value["errors"] = [{"code": "provider_rate_limited", "message": "selected provider rate limit; retry later"}]
            value["strategies"] = [{"state": "retry_wait", "blocked_reason": None}]
        return value


@pytest.mark.parametrize(
    ("mode", "expected"),
    (("waiting_market", "waiting_market"), ("recovery", "recovery"), ("provider_rate_failure", "model_error")),
)
def test_active_lifecycle_is_derived_from_runtime_facts(tmp_path: Path, mode: str, expected: str):
    config = WebConfig(database_path=str(tmp_path / f"{mode}.db"), workspace_path=str(tmp_path / f"workspace-{mode}"), step_interval_seconds=1000)
    supervisor = RuntimeSupervisor(
        config,
        workspace=configured_workspace(config),
        runtime_factory=lambda config, request, web_config: _LifecycleRuntime(config, mode),
        preflight_database=False,
    )
    try:
        supervisor.request_start(request())
        wait_for(supervisor, expected)
        value = supervisor.snapshot().to_dict()
        assert value["lifecycle"] == expected
        if expected == "model_error":
            assert value["errors"][-1]["code"] == "provider_rate_limited"
            assert supervisor.runtime_present is True
            with pytest.raises(SupervisorError) as duplicate:
                supervisor.request_start(request())
            assert duplicate.value.code == "already_running"
    finally:
        supervisor.close()


class _RecoveringProviderRuntime(FixtureRuntime):
    def __init__(self, config):
        super().__init__(config)
        self.step_entered = threading.Event()
        self.release_step = threading.Event()

    def step(self):
        self.step_entered.set()
        self.release_step.wait(timeout=2)
        return super().step()

    def status(self):
        value = super().status()
        value["errors"] = [{"code": "provider_rate_limited", "message": "selected provider rate limit; retry later"}]
        value["strategies"] = [{"state": "retry_wait"}] if self.calls == 0 else [{"state": "analysis_due"}]
        return value


def test_historical_provider_error_does_not_stick_model_error_after_recovery(tmp_path: Path):
    config = WebConfig(database_path=str(tmp_path / "recovering-provider.db"), workspace_path=str(tmp_path / "workspace-recovery"), step_interval_seconds=1)
    supervisor = RuntimeSupervisor(
        config,
        workspace=configured_workspace(config),
        runtime_factory=lambda config, request, web_config: _RecoveringProviderRuntime(config),
        preflight_database=False,
    )
    try:
        supervisor.request_start(request())
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and not supervisor.runtime_present:
            time.sleep(0.01)
        runtime = supervisor._runtime
        assert isinstance(runtime, _RecoveringProviderRuntime)
        assert runtime.step_entered.wait(timeout=2)
        wait_for(supervisor, "model_error")
        runtime.release_step.set()
        wait_for(supervisor, "running", timeout=3)
        value = supervisor.snapshot().to_dict()
        assert value["lifecycle"] == "running"
        assert value["errors"][-1]["code"] == "provider_rate_limited"
    finally:
        if isinstance(getattr(supervisor, "_runtime", None), _RecoveringProviderRuntime):
            supervisor._runtime.release_step.set()
        supervisor.close()


def test_start_stop_mutual_exclusion_covers_owner_operation_not_just_lifecycle(tmp_path: Path):
    entered = threading.Event()
    release = threading.Event()

    class BlockingRuntime(FixtureRuntime):
        def start(self):
            entered.set()
            release.wait(timeout=2)
            return super().start()

    config = WebConfig(database_path=str(tmp_path / "blocking.db"), workspace_path=str(tmp_path / "workspace-blocking"), step_interval_seconds=1000)
    supervisor = RuntimeSupervisor(
        config,
        workspace=configured_workspace(config),
        runtime_factory=lambda config, request, web_config: BlockingRuntime(config),
        preflight_database=False,
    )
    try:
        supervisor.request_start(request())
        assert entered.wait(timeout=1)
        with pytest.raises(SupervisorError) as duplicate:
            supervisor.request_start(request())
        assert duplicate.value.code == "already_running"
        release.set()
        wait_for(supervisor, "running")
    finally:
        release.set()
        supervisor.close()
