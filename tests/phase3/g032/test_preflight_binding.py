from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from pa_engine.storage import RuntimeStore, open_database
from pa_engine.web.config import StartRequest, WebConfig
from pa_engine.web.supervisor import RuntimeSupervisor, SupervisorError

from .conftest import configured_workspace


def _request(*, symbol: str = "AAPL", timeframe: str = "1min") -> StartRequest:
    return StartRequest.from_payload(
        {
            "symbol": symbol,
            "timeframe": timeframe,
            "execution_policy": "paper",
        }
    )


def _wait(supervisor: RuntimeSupervisor, lifecycle: str) -> None:
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        if supervisor.lifecycle == lifecycle:
            return
        time.sleep(0.01)
    raise AssertionError(f"expected {lifecycle}, got {supervisor.lifecycle}")


def _active_timer(path: Path) -> None:
    db = open_database(str(path), run_id="g032-binding-seed", mode="automatic")
    try:
        RuntimeStore(db).save_timers(
            "pa:binding",
            [{"kind": "sleep", "generation": 1, "due_at": "2026-08-22T10:01:00Z", "active": True, "fired": False}],
        )
    finally:
        db.close()


def test_same_binding_resumes_and_nonterminal_timer_fails_closed_before_factory(tmp_path: Path):
    database = tmp_path / "binding.db"
    calls: list[tuple[str, str]] = []

    def factory(config, request, web_config):
        calls.append((request.request.symbol, request.request.timeframe))
        raise AssertionError("runtime/adapters must not be constructed for a binding conflict")

    config = WebConfig(database_path=str(database), workspace_path=str(tmp_path / "workspace"))
    workspace = configured_workspace(config)
    supervisor = RuntimeSupervisor(config, workspace=workspace, runtime_factory=factory)
    try:
        supervisor._preflight_database_state(workspace.prepare_launch(_request()))
        _active_timer(database)
        supervisor._preflight_database_state(workspace.prepare_launch(_request()))
        with pytest.raises(SupervisorError, match="symbol/timeframe"):
            supervisor._preflight_database_state(workspace.prepare_launch(_request(timeframe="5min")))
        with pytest.raises(SupervisorError, match="symbol/timeframe"):
            supervisor._preflight_database_state(workspace.prepare_launch(_request(symbol="TSLA")))

        supervisor.request_start(_request(timeframe="5min"))
        _wait(supervisor, "blocked")
        assert calls == []
    finally:
        supervisor.close()

    raw = database.read_bytes()
    assert b"G032_BINDING_MODEL_CANARY" not in raw
    assert b"G032_BINDING_ID_CANARY" not in raw
    assert b"G032_BINDING_SECRET_CANARY" not in raw


def test_empty_or_terminal_database_allows_a_new_selection_and_only_writes_public_binding(tmp_path: Path):
    database = tmp_path / "terminal.db"
    config = WebConfig(database_path=str(database), workspace_path=str(tmp_path / "workspace-terminal"))
    workspace = configured_workspace(config)
    supervisor = RuntimeSupervisor(config, workspace=workspace, preflight_database=True)
    try:
        supervisor._preflight_database_state(workspace.prepare_launch(_request()))
        supervisor._preflight_database_state(workspace.prepare_launch(_request(symbol="TSLA", timeframe="5min")))
    finally:
        supervisor.close()

    db = open_database(str(database), run_id="g032-binding-inspect", mode="automatic")
    try:
        value = db.conn.execute("SELECT value FROM meta WHERE key = ?", ("web:g032:runtime_binding:v1",)).fetchone()[0]
    finally:
        db.close()
    assert json.loads(value) == {"execution_policy": "paper", "symbol": "TSLA", "timeframe": "5min"}
