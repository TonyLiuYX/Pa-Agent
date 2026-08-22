"""G028 runtime CLI tests: run/status/smoke commands.

The CLI is exercised through :func:`pa_engine.runtime.cli.main` with injected
fake engines and adapters so no network or real credential is ever needed.
The tests also prove the secret boundary: fake credential values placed in the
environment never appear in captured output.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from pa_engine.runtime.cli import main
from pa_engine.runtime.engine import RuntimeEngineError
from pa_engine.storage import (
    RuntimeStore,
    envelope_for_state_transition,
    open_database,
)

ROOT = Path(__file__).resolve().parents[3]

PAPER_LABEL = "PAPER TRADING ONLY"


class FakeEngine:
    """Minimal engine double recording the CLI lifecycle calls."""

    def __init__(self) -> None:
        self.start_calls = 0
        self.stop_calls = 0
        self.run_calls = 0
        self.started = False
        self.stopped = False
        self.stop_reason: str | None = None

    def start(self) -> dict[str, Any]:
        self.start_calls += 1
        self.started = True
        return {
            "run_id": "fake-run",
            "mode": "automatic",
            "symbol": "AAPL",
            "timeframe": {"unit": "minute", "multiplier": 1},
            "feed_key": "alpaca:iex:single_exchange",
            "paper_only": "PAPER TRADING ONLY — no live orders. Market data: Alpaca Basic IEX, internal_prototype (no public display/redistribution).",
            "market_data_usage": "internal_prototype",
            "restored_accounts": 0,
            "restored_pending": 0,
            "restored_open_trades": 0,
            "due_work": [],
            "live_blocked": False,
            "view_id": None,
            "view_executable": False,
            "errors": [],
        }

    def stop(self, *, reason: str = "stopped") -> dict[str, Any]:
        self.stop_calls += 1
        self.stopped = True
        self.stop_reason = reason
        return {
            "run_id": "fake-run",
            "stopped": True,
            "reason": reason,
            "started": False,
            "provider_calls": 0,
            "fills": 0,
            "closes": 0,
            "expiries": 0,
            "rejections": 0,
        }

    def run(self, *, max_steps: int | None = None, **_: Any) -> dict[str, Any]:
        self.run_calls += 1
        return {
            "steps": 0,
            "provider_calls": 0,
            "fills": 0,
            "closes": 0,
            "expiries": 0,
            "rejections": 0,
            "final_status": {"started": True, "stopped": False},
        }

    def status(self) -> dict[str, Any]:
        return {"started": self.started, "stopped": self.stopped}


def fake_engine_factory(engine: FakeEngine) -> Any:
    captured: dict[str, Any] = {}

    def factory(config: Any, **kwargs: Any) -> FakeEngine:
        captured["config"] = config
        captured["kwargs"] = kwargs
        return engine

    factory.captured = captured  # type: ignore[attr-defined]
    return factory


def clear_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
    monkeypatch.delenv("ALPACA_API_KEY_ID", raising=False)
    monkeypatch.delenv("ALPACA_API_SECRET_KEY", raising=False)


def json_error_from_stderr(text: str) -> dict[str, Any]:
    """Extract the JSON error object from stderr that may also carry warnings."""
    for line in text.splitlines():
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict) and "error" in payload:
            return payload
    raise AssertionError(f"no JSON error object found in stderr: {text!r}")


# ---------------------------------------------------------------------------
# help and paper-only labeling
# ---------------------------------------------------------------------------


def test_help_contains_paper_only_label(capsys: pytest.CaptureFixture) -> None:
    with pytest.raises(SystemExit) as exc_info:
        main(["--help"])
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert PAPER_LABEL in captured.out
    assert "internal_prototype" in captured.out


def test_subcommand_help_contains_paper_only_label(capsys: pytest.CaptureFixture) -> None:
    for subcommand in ("run", "status", "smoke"):
        with pytest.raises(SystemExit) as exc_info:
            main([subcommand, "--help"])
        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        assert PAPER_LABEL in captured.out
        assert "internal_prototype" in captured.out


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


def test_run_invalid_config_exits_2(capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch) -> None:
    clear_credentials(monkeypatch)
    code = main(["run", "--symbol", "not-upper"])
    assert code == 2
    captured = capsys.readouterr()
    payload = json.loads(captured.err)
    assert payload["error"]["code"] == "invalid_symbol"
    assert payload["paper_only"] is True
    assert "Traceback" not in captured.err


def test_run_invalid_timeframe_exits_2(capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch) -> None:
    clear_credentials(monkeypatch)
    code = main(["run", "--symbol", "AAPL", "--timeframe", "7min"])
    assert code == 2
    captured = capsys.readouterr()
    payload = json.loads(captured.err)
    assert payload["error"]["code"] == "invalid_timeframe"
    assert "Traceback" not in captured.err


def test_run_max_steps_zero_lifecycle_and_redaction(
    capsys: pytest.CaptureFixture,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_credentials(monkeypatch)
    monkeypatch.setenv("DEEPSEEK_API_KEY", "fake-secret-123")
    engine = FakeEngine()
    factory = fake_engine_factory(engine)

    code = main(
        ["run", "--symbol", "AAPL", "--max-steps", "0", "--db", str(tmp_path / "run.db")],
        engine_factory=factory,
    )
    assert code == 0
    captured = capsys.readouterr()
    combined = captured.out + captured.err
    assert engine.start_calls == 1
    assert engine.stop_calls == 1
    assert engine.run_calls == 0
    assert engine.stop_reason == "started_by_request"
    assert PAPER_LABEL in captured.out
    assert "internal_prototype" in captured.out
    assert "fake-secret-123" not in combined
    assert "api_key" not in combined
    assert "secret" not in combined
    assert factory.captured["config"].symbol == "AAPL"
    assert factory.captured["config"].reasoning_mode == "enabled"


def test_run_max_steps_one_runs_then_stops(
    capsys: pytest.CaptureFixture,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_credentials(monkeypatch)
    engine = FakeEngine()
    code = main(
        ["run", "--symbol", "AAPL", "--max-steps", "1", "--db", str(tmp_path / "run.db")],
        engine_factory=fake_engine_factory(engine),
    )
    assert code == 0
    captured = capsys.readouterr()
    assert engine.run_calls == 1
    assert engine.stop_calls == 1
    assert engine.stop_reason == "cli_stop"
    assert PAPER_LABEL in captured.out
    assert "fake-secret" not in captured.out


def test_run_engine_error_exits_1(
    capsys: pytest.CaptureFixture,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_credentials(monkeypatch)

    class FailingEngine(FakeEngine):
        def start(self) -> dict[str, Any]:
            raise RuntimeEngineError("lease_conflict", "database is held by another run")

    code = main(
        ["run", "--symbol", "AAPL", "--db", str(tmp_path / "run.db")],
        engine_factory=fake_engine_factory(FailingEngine()),
    )
    assert code == 1
    captured = capsys.readouterr()
    payload = json_error_from_stderr(captured.err)
    assert payload["error"]["code"] == "lease_conflict"
    assert "Traceback" not in captured.err


def test_run_without_credentials_warns_on_stderr(
    capsys: pytest.CaptureFixture,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_credentials(monkeypatch)
    code = main(
        ["run", "--symbol", "AAPL", "--max-steps", "0", "--db", str(tmp_path / "run.db")],
        engine_factory=fake_engine_factory(FakeEngine()),
    )
    assert code == 0
    captured = capsys.readouterr()
    assert "DeepSeek credentials are not set" in captured.err
    assert "Alpaca credentials are not set" in captured.err
    assert "api_key" not in (captured.out + captured.err)
    assert "secret" not in (captured.out + captured.err)


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------


def _seed_status_database(path: Path) -> None:
    db = open_database(str(path), run_id="seed-run", mode="automatic")
    try:
        store = RuntimeStore(db)
        store.ensure_accounts(["pa:seed-strategy"])
        store.update_account("pa:seed-strategy", balance="1000.00")
        snapshot = {
            "strategy_id": "pa:seed-strategy",
            "state": "market_wait",
            "generation": 0,
            "enabled": True,
            "exhausted": False,
        }
        store.commit_strategy_state(snapshot)
        updated = {**snapshot, "state": "analysis_due"}
        store.commit_strategy_state(
            updated,
            transition={"from": "market_wait", "to": "analysis_due", "reason": "seeded", "resume_state": None},
            envelope=envelope_for_state_transition(
                snapshot,
                from_state="market_wait",
                to_state="analysis_due",
                reason="seeded",
                occurred_at="2026-03-09T13:30:00Z",
                idempotency_key="seed:1",
            ),
        )
    finally:
        db.close()


def test_status_reports_seeded_database(
    capsys: pytest.CaptureFixture,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_credentials(monkeypatch)
    monkeypatch.setenv("ALPACA_API_SECRET_KEY", "fake-secret-789")
    path = tmp_path / "status.db"
    _seed_status_database(path)

    code = main(["status", "--db", str(path)])
    assert code == 0
    captured = capsys.readouterr()
    report = json.loads(captured.out)
    assert "balance" not in captured.out
    assert "quantity" not in captured.out
    assert "pnl" not in captured.out.lower()
    assert "fake-secret-789" not in captured.out
    assert report["database"] == str(path)
    assert report["mode"] == "automatic"
    assert report["lease"]["held"] is True
    assert report["lease"]["run_id"].startswith("status-")
    assert report["lease"]["mode"] == "automatic"
    assert report["accounts"] == [
        {
            "strategy_id": "pa:seed-strategy",
            "exhausted": False,
            "pending": False,
            "open": False,
        }
    ]
    assert report["strategies"][0]["strategy_id"] == "pa:seed-strategy"
    assert report["strategies"][0]["state"] == "analysis_due"
    assert report["strategies"][0]["timers"] == []
    assert report["pending_entries"] == []
    assert report["trades"] == []
    assert report["cursors"] == []
    assert report["live_blocked"] is False
    assert report["events_count"] == 1
    assert PAPER_LABEL in report["paper_only"]
    assert report["market_data_usage"] == "internal_prototype"
    assert "fake-secret-789" not in captured.out
    assert "fake-secret" not in captured.out


def test_status_missing_database_exits_1(
    capsys: pytest.CaptureFixture,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_credentials(monkeypatch)
    code = main(["status", "--db", str(tmp_path / "missing.db")])
    assert code == 1
    captured = capsys.readouterr()
    payload = json_error_from_stderr(captured.err)
    assert payload["error"]["code"] == "database_missing"
    assert "Traceback" not in captured.err


def test_status_live_lease_held_exits_0(
    capsys: pytest.CaptureFixture,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_credentials(monkeypatch)
    path = tmp_path / "live.db"
    db = open_database(str(path), run_id="live-run-001", mode="automatic")
    try:
        code = main(["status", "--db", str(path)])
    finally:
        db.close()
    assert code == 0
    captured = capsys.readouterr()
    report = json.loads(captured.out)
    assert report["status"] == "live_lease_held"
    assert report["lease"]["held"] is True
    assert report["lease"]["run_id"] == "live-run-001"
    assert PAPER_LABEL in report["paper_only"]


# ---------------------------------------------------------------------------
# smoke
# ---------------------------------------------------------------------------


def test_smoke_no_credentials_exits_2(
    capsys: pytest.CaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_credentials(monkeypatch)
    code = main(["smoke", "--symbol", "AAPL"])
    assert code == 2
    captured = capsys.readouterr()
    evidence = json.loads(captured.out)
    assert evidence["ok"] is False
    assert evidence["attempted"] is False
    assert "DEEPSEEK_API_KEY" in evidence["missing_credentials"]
    assert "ALPACA_API_KEY_ID" in evidence["missing_credentials"]
    assert "ALPACA_API_SECRET_KEY" in evidence["missing_credentials"]
    assert "hint" in evidence
    assert evidence["paper_only"] is True


def test_smoke_fake_credentials_injected_adapters(
    capsys: pytest.CaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_credentials(monkeypatch)
    monkeypatch.setenv("DEEPSEEK_API_KEY", "fake-secret-123")
    monkeypatch.setenv("ALPACA_API_KEY_ID", "fake-secret-456")
    monkeypatch.setenv("ALPACA_API_SECRET_KEY", "fake-secret-789")

    history = FakeHistory()
    deepseek = FakeDeepSeek()
    stream = FakeStream()
    code = main(
        ["smoke", "--symbol", "AAPL", "--include-stream"],
        smoke_adapters={"history": history, "deepseek": deepseek, "stream": stream},
    )
    assert code == 0
    captured = capsys.readouterr()
    evidence = json.loads(captured.out)
    assert evidence["alpaca"]["ok"] is True
    assert evidence["alpaca"]["bars"] == 2
    assert evidence["alpaca"]["snapshot_id"] == "snap_abc"
    assert evidence["deepseek"]["ok"] is True
    assert evidence["deepseek"]["model"] == "deepseek-v4-flash"
    assert evidence["deepseek"]["reasoning"] == "disabled"
    assert evidence["deepseek"]["finish_reason"] == "stop"
    assert evidence["deepseek"]["usage"]["total_tokens"] == 15
    assert evidence["stream"]["ok"] is True
    assert evidence["stream"]["messages"] == 0
    assert evidence["feed"] == "alpaca:iex:single_exchange"
    assert evidence["paper_only"] is True
    assert evidence["ok"] is True
    assert "fake-secret" not in captured.out
    assert "api_key" not in captured.out

    assert deepseek.request["extensions"]["reasoning_mode"] == "disabled"
    assert deepseek.request["parameters"]["max_output_tokens"] == 1024
    assert history.kwargs["limit"] == 1000


def test_smoke_fake_credentials_all_failed_exits_1(
    capsys: pytest.CaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_credentials(monkeypatch)
    monkeypatch.setenv("DEEPSEEK_API_KEY", "fake-secret-123")
    monkeypatch.setenv("ALPACA_API_KEY_ID", "fake-secret-456")
    monkeypatch.setenv("ALPACA_API_SECRET_KEY", "fake-secret-789")

    code = main(
        ["smoke", "--symbol", "AAPL"],
        smoke_adapters={
            "history": FakeHistory(success=False),
            "deepseek": FakeDeepSeek(success=False),
        },
    )
    assert code == 1
    captured = capsys.readouterr()
    evidence = json.loads(captured.out)
    assert evidence["alpaca"]["ok"] is False
    assert evidence["alpaca"]["error_code"] == "authentication"
    assert evidence["deepseek"]["ok"] is False
    assert evidence["deepseek"]["error_code"] == "authentication"
    assert evidence["ok"] is False
    assert "fake-secret" not in captured.out


def test_smoke_partial_credentials_attempt_present_providers_only(
    capsys: pytest.CaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_credentials(monkeypatch)
    monkeypatch.setenv("DEEPSEEK_API_KEY", "fake-secret-123")
    code = main(
        ["smoke", "--symbol", "AAPL"],
        smoke_adapters={"history": FakeHistory(success=False), "deepseek": FakeDeepSeek()},
    )
    assert code == 0
    captured = capsys.readouterr()
    evidence = json.loads(captured.out)
    assert evidence["deepseek"]["ok"] is True
    assert evidence["alpaca"]["ok"] is False
    assert evidence["alpaca"]["error_code"] == "authentication"
    assert "ALPACA_API_KEY_ID" in evidence["missing_credentials"]
    assert "fake-secret" not in captured.out


# ---------------------------------------------------------------------------
# module entry point
# ---------------------------------------------------------------------------


def test_module_entry_help_via_subprocess() -> None:
    proc = subprocess.run(
        [sys.executable, "-m", "pa_engine.runtime", "--help"],
        capture_output=True,
        text=True,
        cwd=str(ROOT),
        env={**os.environ, "DEEPSEEK_API_KEY": "", "ALPACA_API_KEY_ID": "", "ALPACA_API_SECRET_KEY": ""},
        timeout=60,
    )
    assert proc.returncode == 0
    assert PAPER_LABEL in proc.stdout
    assert "internal_prototype" in proc.stdout


def test_smoke_module_entry_no_credentials_via_subprocess() -> None:
    proc = subprocess.run(
        [sys.executable, "-m", "pa_engine.runtime.smoke", "--symbol", "AAPL"],
        capture_output=True,
        text=True,
        cwd=str(ROOT),
        env={**os.environ, "DEEPSEEK_API_KEY": "", "ALPACA_API_KEY_ID": "", "ALPACA_API_SECRET_KEY": ""},
        timeout=60,
    )
    assert proc.returncode == 2
    evidence = json.loads(proc.stdout)
    assert evidence["ok"] is False
    assert "DEEPSEEK_API_KEY" in evidence["missing_credentials"]
    assert evidence["paper_only"] is True


# ---------------------------------------------------------------------------
# fakes
# ---------------------------------------------------------------------------


class FakeHistory:
    """History double with the G019 ``fetch_history`` surface."""

    def __init__(self, success: bool = True) -> None:
        self.success = success
        self.kwargs: dict[str, Any] = {}

    def fetch_history(self, **kwargs: Any) -> Any:
        self.kwargs = kwargs
        if self.success:
            return SimpleNamespace(
                success=True,
                bars=[{"x": 1}, {"x": 2}],
                outcome=SimpleNamespace(code="ok", classification="success", retryable=False),
                market_snapshot={"market_snapshot_id": "snap_abc"},
            )
        return SimpleNamespace(
            success=False,
            bars=[],
            outcome=SimpleNamespace(code="authentication", classification="authentication", retryable=False),
            market_snapshot=None,
        )


class FakeDeepSeek:
    """Adapter double with the G022 ``complete`` surface."""

    def __init__(self, success: bool = True) -> None:
        self.success = success
        self.request: dict[str, Any] = {}

    def complete(self, request: dict[str, Any], **_: Any) -> Any:
        self.request = request
        if self.success:
            return SimpleNamespace(
                is_success=True,
                classification="success",
                retryable=False,
                response={"status": "completed", "finish_reason": "stop"},
                call_metadata={"usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}},
            )
        return SimpleNamespace(
            is_success=False,
            classification="authentication",
            retryable=False,
            response={"status": "error", "error": {"code": "authentication"}},
            call_metadata={},
        )


class FakeStream:
    """Stream double with the G020 ``run`` surface."""

    def __init__(self, success: bool = True) -> None:
        self.success = success
        self.messages: list[Any] = []

    def run(self, *, max_messages: int | None = None) -> Any:
        return SimpleNamespace(outcome=SimpleNamespace(ok=self.success, kind=None if self.success else "authentication", retryable=False))

    def close(self) -> None:
        pass
