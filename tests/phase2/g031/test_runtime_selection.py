"""G031 runtime/config/CLI exact-one-provider acceptance tests."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import pytest

from pa_engine.providers import make_normalized_model_request, request_semantic_identity
from pa_engine.runtime.config import RuntimeConfig, RuntimeConfigError
from pa_engine.runtime.engine import AutomaticPaperRuntime
from pa_engine.runtime.prompt import build_analysis_prompt, build_analysis_request


def _prompt_inputs() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    packet = {
        "strategy_id": "pa:test-g031",
        "packet_id": "spk_" + "a" * 64,
        "packet_hash": "a" * 64,
        "primary_record": {},
        "claim_fields": [],
        "claims": {},
        "chunk_excerpts": [],
    }
    profile_ref = {
        "profile_ref_id": "spr_" + "b" * 64,
        "profile_ref_hash": "b" * 64,
        "strategy_id": "pa:test-g031",
    }
    view = {
        "symbol": "AAPL",
        "timeframe": {"unit": "minute", "multiplier": 1},
        "session": {"kind": "regular", "timezone": "America/New_York"},
        "closed_snapshot": {},
        "observation_frame": None,
        "latest_second": None,
        "quality": {},
        "readiness": {},
        "cutover_at": "2026-04-11T10:40:00Z",
    }
    return packet, profile_ref, view


def test_default_config_remains_deepseek_but_openai_derives_fixed_identity():
    deepseek = RuntimeConfig(symbol="AAPL")
    openai = RuntimeConfig(symbol="AAPL", provider="openai", openai_speed="fast")
    assert deepseek.provider == "deepseek"
    assert deepseek.model == "deepseek-v4-flash"
    assert deepseek.adapter == "g022-deepseek-v4-flash-0.1.0"
    assert openai.provider == "openai"
    assert openai.model == "gpt-5.6-luna"
    assert openai.adapter == "g031-openai-gpt-5.6-luna-0.1.0"
    assert openai.openai_speed == "fast"


def test_runtime_rejects_openai_disabled_reasoning_and_deepseek_fast_before_work():
    with pytest.raises(RuntimeConfigError) as openai_error:
        RuntimeConfig(symbol="AAPL", provider="openai", reasoning_mode="disabled")
    assert openai_error.value.code == "openai_reasoning_required"
    with pytest.raises(RuntimeConfigError) as deepseek_error:
        RuntimeConfig(symbol="AAPL", provider="deepseek", openai_speed="fast")
    assert deepseek_error.value.code == "invalid_openai_speed"


def test_runtime_rejects_arbitrary_model_but_accepts_programmatic_fixed_model():
    assert RuntimeConfig(symbol="AAPL", provider="openai", model="gpt-5.6-luna").model == "gpt-5.6-luna"
    with pytest.raises(RuntimeConfigError) as error:
        RuntimeConfig(symbol="AAPL", provider="openai", model="gpt-5.6-sol")
    assert error.value.code == "invalid_model"


def test_openai_prompt_and_normalized_request_emit_provider_model_adapter_identity():
    packet, profile_ref, view = _prompt_inputs()
    instructions, context = build_analysis_prompt(
        strategy_id=packet["strategy_id"],
        packet=packet,
        profile_ref=profile_ref,
        market_view=view,
        reasoning_mode="enabled",
        provider="openai",
    )
    request = build_analysis_request(
        strategy_id=packet["strategy_id"],
        packet=packet,
        profile_ref=profile_ref,
        market_view=view,
        reasoning_mode="enabled",
        provider="openai",
    )
    assert instructions == request["instructions"]
    assert context == request["context_text"]
    assert request["provider"] == "openai"
    assert request["model"] == "gpt-5.6-luna"
    assert request["adapter"] == "g031-openai-gpt-5.6-luna-0.1.0"
    assert "analysis_provider=" not in context
    assert "analysis_model=" not in context
    assert "analysis_adapter=" not in context
    assert "reasoning_mode=" not in context


def test_default_deepseek_prompt_and_request_identity_are_pre_g031_stable():
    packet, profile_ref, view = _prompt_inputs()
    request = build_analysis_request(
        strategy_id=packet["strategy_id"],
        packet=packet,
        profile_ref=profile_ref,
        market_view=view,
        reasoning_mode="enabled",
    )

    assert request["provider"] == "deepseek"
    assert request["model"] == "deepseek-v4-flash"
    assert request["adapter"] == "g022-deepseek-v4-flash-0.1.0"
    assert request["prompt_hash"] == "a5cc4ad401ba12184c341df3d31ba4693bb6e420d9af0ec1b3b1d934b0d38afe"
    assert request_semantic_identity(request) == "ba42277adc97a9a46a01e6dc000b204a307093adf5a98f95f7d6ded312c0293a"
    assert "analysis_provider=" not in request["context_text"]
    assert "analysis_model=" not in request["context_text"]
    assert "analysis_adapter=" not in request["context_text"]
    assert "reasoning_mode=" not in request["context_text"]


def test_openai_prompt_rejects_disabled_reasoning_before_normalization():
    packet, profile_ref, view = _prompt_inputs()
    with pytest.raises(ValueError, match="requires reasoning_mode=enabled"):
        build_analysis_request(
            strategy_id=packet["strategy_id"],
            packet=packet,
            profile_ref=profile_ref,
            market_view=view,
            reasoning_mode="disabled",
            provider="openai",
        )


def test_prompt_infers_fixed_openai_identity_for_legacy_engine_model_argument():
    packet, profile_ref, view = _prompt_inputs()
    request = build_analysis_request(
        strategy_id=packet["strategy_id"],
        packet=packet,
        profile_ref=profile_ref,
        market_view=view,
        reasoning_mode="enabled",
        model="gpt-5.6-luna",
    )
    assert request["provider"] == "openai"
    assert request["model"] == "gpt-5.6-luna"
    assert request["adapter"] == "g031-openai-gpt-5.6-luna-0.1.0"


@pytest.mark.parametrize(
    "provider,model,adapter",
    [
        ("deepseek", "deepseek-v4-flash", "g022-deepseek-v4-flash-0.1.0"),
        ("openai", "gpt-5.6-luna", "g031-openai-gpt-5.6-luna-0.1.0"),
    ],
)
def test_engine_passes_selected_provider_identity_explicitly(
    monkeypatch,
    provider: str,
    model: str,
    adapter: str,
):
    import pa_engine.runtime.prompt as prompt

    captured: dict[str, Any] = {}

    def fake_build_analysis_request(**kwargs):
        captured.update(kwargs)
        return {"provider": kwargs["provider"]}

    monkeypatch.setattr(prompt, "build_analysis_request", fake_build_analysis_request)
    runtime = object.__new__(AutomaticPaperRuntime)
    runtime._config = RuntimeConfig(symbol="AAPL", provider=provider)
    runtime._max_output_tokens = 4096

    result = runtime._normalized_request(
        strategy_id="pa:test-g031",
        packet={},
        profile_ref={},
        view=SimpleNamespace(to_dict=lambda: {}),
        evidence_package={},
        analysis_request={},
    )

    assert result == {"provider": provider}
    assert captured["provider"] == provider
    assert captured["model"] == model
    assert captured["adapter"] == adapter


class FakeEngine:
    def start(self):
        return {"errors": [], "paper_only": "PAPER TRADING ONLY — no live orders."}

    def stop(self, *, reason: str):
        return {"stopped": True, "reason": reason}

    def run(self, **kwargs):
        return {"steps": 0}

    def status(self):
        return {"started": True}


def test_cli_openai_run_constructs_only_selected_adapter(monkeypatch, tmp_path, capsys):
    import pa_engine.runtime.cli as cli

    openai_calls: list[Any] = []
    deepseek_calls: list[Any] = []

    class OpenAIFake:
        @classmethod
        def from_env(cls, **kwargs):
            openai_calls.append(kwargs)
            return cls()

    class DeepSeekFake:
        @classmethod
        def from_env(cls, **kwargs):
            deepseek_calls.append(kwargs)
            return cls()

    monkeypatch.setattr(cli, "OpenAIAdapter", OpenAIFake)
    monkeypatch.setattr(cli, "DeepSeekAdapter", DeepSeekFake)
    monkeypatch.setenv("OPENAI_API_KEY", "sk-g031-cli-secret")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "ds-g031-unselected-secret")
    monkeypatch.delenv("ALPACA_API_KEY_ID", raising=False)
    monkeypatch.delenv("ALPACA_API_SECRET_KEY", raising=False)
    engine = FakeEngine()
    captured: dict[str, Any] = {}

    def factory(config, **kwargs):
        captured["config"] = config
        captured["adapter"] = kwargs["adapter"]
        return engine

    assert cli.main(
        ["run", "--symbol", "AAPL", "--provider", "openai", "--openai-speed", "fast", "--max-steps", "0", "--db", str(tmp_path / "run.db")],
        engine_factory=factory,
    ) == 0
    assert captured["config"].provider == "openai"
    assert captured["config"].model == "gpt-5.6-luna"
    assert captured["config"].openai_speed == "fast"
    assert isinstance(captured["adapter"], OpenAIFake)
    assert openai_calls == [{"config": cli.OpenAIConfig(speed="fast")}]
    assert deepseek_calls == []
    output = capsys.readouterr().out
    assert "sk-g031-cli-secret" not in output
    assert "ds-g031-unselected-secret" not in output


def test_cli_has_no_public_model_flag_and_rejects_incompatible_openai_options(capsys):
    import pa_engine.runtime.cli as cli

    with pytest.raises(SystemExit) as help_exit:
        cli.main(["run", "--help"])
    assert help_exit.value.code == 0
    help_text = capsys.readouterr().out
    assert "--model" not in help_text
    assert "--provider" in help_text
    assert "--openai-speed" in help_text

    assert cli.main(["run", "--symbol", "AAPL", "--provider", "openai", "--thinking", "disabled"]) == 2
    error = json.loads(capsys.readouterr().err)
    assert error["error"]["code"] == "openai_reasoning_required"
