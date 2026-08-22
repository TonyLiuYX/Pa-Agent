from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from pa_engine.storage import open_database
from pa_engine.contracts import canonical_json_bytes
from pa_engine.web.config import StartRequest
from pa_engine.web.supervisor import RuntimeSupervisor, SupervisorError

from .conftest import make_workspace


def _request(policy: str) -> StartRequest:
    return StartRequest.from_payload({"symbol": "AAPL", "timeframe": "1min", "execution_policy": policy})


def _seed_pending(database: Path, strategy_id: str) -> None:
    db = open_database(str(database), run_id="g033-seed", mode="automatic")
    try:
        now = "2026-08-22T10:00:00Z"
        state = {
            "pending_id": "pending-g033",
            "strategy_id": strategy_id,
            "generation": 1,
            "status": "pending",
            "symbol": "AAPL",
            "direction": "long",
        }
        state_json = canonical_json_bytes(state).decode("utf-8")
        state_hash = hashlib.sha256(canonical_json_bytes(state)).hexdigest()
        with db.transaction() as conn:
            conn.execute(
                "INSERT INTO pending_entries (pending_id, strategy_id, generation, status, symbol, direction, "
                "entry_price, stop_price, target_price, decision_accepted_at, submitted_at, expires_at, "
                "resolution_at, episode_id, state_hash, state_json, flags_json, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "pending-g033",
                    strategy_id,
                    1,
                    "pending",
                    "AAPL",
                    "long",
                    "100",
                    "99",
                    "102",
                    now,
                    now,
                    "2026-08-22T10:01:00Z",
                    "2026-08-22T10:01:00Z",
                    "episode-g033",
                    state_hash,
                    state_json,
                    "{}",
                    now,
                    now,
                ),
            )
    finally:
        db.close()


def _seed_analysis_state(database: Path, strategy_id: str) -> None:
    db = open_database(str(database), run_id="g033-analysis-seed", mode="automatic")
    try:
        now = "2026-08-22T10:00:00Z"
        with db.transaction() as conn:
            conn.execute(
                "INSERT INTO strategy_states "
                "(strategy_id, state, enabled, exhausted, generation, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (strategy_id, "analysis_due", 1, 0, 0, now, now),
            )
    finally:
        db.close()


def test_pending_or_open_state_cannot_be_excluded_or_resumed_in_advisory(tmp_path: Path):
    config, workspace = make_workspace(tmp_path, configured=True)
    supervisor = RuntimeSupervisor(config, workspace=workspace, preflight_database=True)
    selected = workspace.strategies()["selected_ids"]
    protected = selected[0]
    try:
        supervisor._preflight_database_state(workspace.prepare_launch(_request("paper")))
        _seed_pending(Path(config.database_path), protected)

        workspace.update_selection({"strategy_ids": selected[1:]})
        with pytest.raises(SupervisorError) as omitted:
            supervisor._preflight_database_state(workspace.prepare_launch(_request("paper")))
        assert omitted.value.code == "strategy_selection_conflict"

        workspace.update_selection({"strategy_ids": selected})
        with pytest.raises(SupervisorError) as mode:
            supervisor._preflight_database_state(workspace.prepare_launch(_request("advisory")))
        assert mode.value.code == "execution_policy_conflict"

        supervisor._preflight_database_state(workspace.prepare_launch(_request("paper")))
    finally:
        supervisor.close()


def test_execution_policy_can_switch_after_stop_when_no_pending_or_open_state(tmp_path: Path):
    config, workspace = make_workspace(tmp_path, configured=True)
    supervisor = RuntimeSupervisor(config, workspace=workspace, preflight_database=True)
    try:
        supervisor._preflight_database_state(workspace.prepare_launch(_request("paper")))
        _seed_analysis_state(Path(config.database_path), workspace.strategies()["selected_ids"][0])

        supervisor._preflight_database_state(workspace.prepare_launch(_request("advisory")))
        db = open_database(str(config.database_path), run_id="g033-binding-probe", mode="automatic")
        try:
            row = db.conn.execute(
                "SELECT value FROM meta WHERE key = ?", ("web:g032:runtime_binding:v1",)
            ).fetchone()
            assert row is not None
            assert json.loads(row[0]) == {
                "symbol": "AAPL",
                "timeframe": "1min",
                "execution_policy": "advisory",
            }
        finally:
            db.close()
    finally:
        supervisor.close()
