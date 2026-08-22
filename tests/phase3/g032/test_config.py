from __future__ import annotations

import pytest

from pa_engine.web.config import StartRequest, WebConfig, WebConfigError, validate_loopback_host


def _payload(**overrides):
    value = {
        "symbol": "AAPL",
        "timeframe": "1min",
        "execution_policy": "advisory",
    }
    value.update(overrides)
    return value


def test_only_loopback_hosts_are_accepted():
    assert validate_loopback_host("127.0.0.1") == "127.0.0.1"
    assert validate_loopback_host("localhost") == "127.0.0.1"
    assert validate_loopback_host("::1") == "::1"
    for host in ("0.0.0.0", "192.168.1.4", "example.test", ""):
        with pytest.raises(WebConfigError) as error:
            validate_loopback_host(host)
        assert error.value.code in {"non_loopback_bind", "invalid_host"}


def test_start_contract_is_closed_and_secret_free_before_runtime_construction():
    request = StartRequest.from_payload(_payload())
    assert request.execution_policy == "advisory"
    with pytest.raises(WebConfigError) as secret:
        StartRequest.from_payload(_payload(model_key="UNSELECTED_KEY"))
    assert secret.value.code == "start_secrets_forbidden"
    with pytest.raises(WebConfigError) as unknown:
        StartRequest.from_payload(_payload(provider="deepseek"))
    assert unknown.value.code == "unknown_start_field"
    with pytest.raises(WebConfigError, match="database path"):
        StartRequest.from_payload(_payload(database_path="/tmp/other.db"))


def test_runtime_public_projection_contains_only_start_fields():
    request = StartRequest.from_payload(_payload(execution_policy="paper"))
    assert request.public_config() == {
        "symbol": "AAPL",
        "timeframe": "1min",
        "execution_policy": "paper",
    }
    rendered = repr(request) + str(request.public_config())
    assert "G032_MODEL_CANARY" not in rendered
    assert "G032_ALPACA" not in rendered


def test_polling_is_never_faster_than_one_second():
    assert WebConfig(step_interval_seconds=1).step_interval_seconds == 1
    with pytest.raises(WebConfigError, match="at least one second"):
        WebConfig(step_interval_seconds=0.5)
