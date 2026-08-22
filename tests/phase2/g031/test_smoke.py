"""G031 selected-provider smoke routing tests."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from pa_engine.providers.base import validate_normalized_request
from pa_engine.runtime.config import RuntimeConfigError
from pa_engine.runtime.smoke import run_smoke, smoke_json


class FakeOutcome:
    is_success = True
    retryable = False
    classification = "success"
    response = {"status": "completed", "finish_reason": "completed", "error": None}
    call_metadata = {
        "usage": {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3},
        "extensions": {"openai": {"effective_service_tier": "default"}},
    }


class FakeOpenAI:
    def __init__(self):
        self.requests = []

    def complete(self, request):
        self.requests.append(request)
        return FakeOutcome()


class UnexpectedDeepSeek:
    def complete(self, request):  # pragma: no cover - proves the fake is not touched
        raise AssertionError("unselected DeepSeek adapter was called")


class FakeHistory:
    def fetch_history(self, **kwargs):
        return SimpleNamespace(success=True, bars=[], market_snapshot=None, outcome=SimpleNamespace(code=None, retryable=False))


def test_openai_smoke_attempts_only_selected_openai_fake_and_alpaca(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "sk-g031-smoke-selected")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "ds-g031-smoke-unselected")
    selected = FakeOpenAI()
    evidence = run_smoke(
        symbol="AAPL",
        provider="openai",
        openai_speed="fast",
        adapters={"openai": selected, "deepseek": UnexpectedDeepSeek(), "history": FakeHistory()},
        environ={
            "OPENAI_API_KEY": "sk-g031-smoke-selected",
            "DEEPSEEK_API_KEY": "ds-g031-smoke-unselected",
        },
    )
    assert evidence["provider"] == "openai"
    assert evidence["openai"]["ok"] is True
    assert evidence["openai"]["model"] == "gpt-5.6-luna"
    assert evidence["openai"]["speed"] == "fast"
    assert evidence["deepseek"] is None
    assert evidence["missing_credentials"] == ["ALPACA_API_KEY_ID", "ALPACA_API_SECRET_KEY"]
    assert selected.requests[0]["provider"] == "openai"
    assert selected.requests[0]["extensions"]["reasoning_mode"] == "enabled"
    assert selected.requests[0]["parameters"]["max_output_tokens"] == 1024
    assert validate_normalized_request(selected.requests[0]).valid
    serialized = smoke_json(evidence, ("sk-g031-smoke-selected", "ds-g031-smoke-unselected"))
    assert "sk-g031-smoke-selected" not in serialized
    assert "ds-g031-smoke-unselected" not in serialized


def test_unselected_credential_does_not_make_openai_smoke_attempt(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("ALPACA_API_KEY_ID", raising=False)
    monkeypatch.delenv("ALPACA_API_SECRET_KEY", raising=False)
    evidence = run_smoke(
        symbol="AAPL",
        provider="openai",
        environ={"DEEPSEEK_API_KEY": "ds-g031-only-unselected"},
    )
    assert evidence["attempted"] is False
    assert evidence["openai"] is None
    assert evidence["missing_credentials"] == [
        "OPENAI_API_KEY",
        "ALPACA_API_KEY_ID",
        "ALPACA_API_SECRET_KEY",
    ]
    assert "DEEPSEEK_API_KEY" not in evidence["missing_credentials"]


def test_smoke_rejects_openai_disabled_reasoning_before_adapter_call():
    with pytest.raises(RuntimeConfigError) as error:
        run_smoke(symbol="AAPL", provider="openai", thinking="disabled", adapters={"openai": FakeOpenAI()})
    assert error.value.code == "openai_reasoning_required"


def test_deepseek_smoke_keeps_legacy_selected_shape():
    class DeepSeekFake:
        def __init__(self):
            self.requests = []

        def complete(self, request):
            self.requests.append(request)
            result = FakeOutcome()
            result.response = {"status": "completed", "finish_reason": "stop", "error": None}
            result.call_metadata = {"usage": {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3}}
            return result

    selected = DeepSeekFake()
    evidence = run_smoke(
        symbol="AAPL",
        provider="deepseek",
        adapters={"deepseek": selected},
        environ={"DEEPSEEK_API_KEY": "ds-g031-selected"},
    )
    assert evidence["deepseek"]["model"] == "deepseek-v4-flash"
    assert evidence["deepseek"]["reasoning"] == "disabled"
    assert evidence["openai"] is None
    assert evidence["missing_credentials"] == ["ALPACA_API_KEY_ID", "ALPACA_API_SECRET_KEY"]
    assert selected.requests[0]["extensions"]["reasoning_mode"] == "disabled"
    assert selected.requests[0]["parameters"]["max_output_tokens"] == 1024
    assert validate_normalized_request(selected.requests[0]).valid
