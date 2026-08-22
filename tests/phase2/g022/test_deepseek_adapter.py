"""Focused G022 conformance, failure and secret-boundary tests."""

from __future__ import annotations

import copy
import io
import json
import os
import subprocess
import sys
import threading
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from pa_engine.providers import RuntimeCredentialResolver, make_normalized_model_request, validate_normalized_request
from pa_engine.providers.deepseek import (
    DeepSeekAdapter,
    DeepSeekConfig,
    DeepSeekHTTPError,
    DeepSeekNoRedirectHandler,
    DeepSeekTransportResponse,
    UrllibDeepSeekTransport,
    build_transport_payload,
)
from pa_engine.providers.deepseek.smoke import SMOKE_CANDIDATE, make_smoke_request
from pa_engine.strategy.decision import parse_candidate


ROOT = Path(__file__).resolve().parents[3]
FIXTURES = ROOT / "tests" / "fixtures" / "phase2" / "g022"
G018 = ROOT / "tests" / "fixtures" / "phase2" / "g018" / "valid" / "no_entry.json"
SECRET = "ds-secret-g022-only-in-runtime"


def fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def request(*, thinking: str = "disabled", correlation_id: str = "g022-test") -> dict:
    return make_normalized_model_request(
        provider="deepseek",
        model="deepseek-v4-flash",
        adapter="g022-deepseek-v4-flash-0.1.0",
        correlation_id=correlation_id,
        reasoning_mode=thinking,
        requested_capabilities=("structured_json", "reasoning_mode"),
        instructions="Return exactly one JSON object matching the G018 candidate schema.",
        context_text="G022 fixture context; never include a key here.",
    )


def body(name: str = "success.json") -> dict:
    return fixture(name)


class CaptureTransport:
    def __init__(self, response: DeepSeekTransportResponse | None = None, *, error: Exception | None = None):
        self.response = response or DeepSeekTransportResponse(200, body())
        self.error = error
        self.calls: list[tuple[str, dict[str, str], dict, float]] = []
        self.lock = threading.Lock()

    def __call__(self, url, headers, payload, timeout):
        with self.lock:
            self.calls.append((url, dict(headers), copy.deepcopy(dict(payload)), timeout))
        if self.error is not None:
            raise self.error
        return self.response


def test_documented_payload_and_thinking_switch_stay_transport_local():
    disabled = request(thinking="disabled")
    enabled = request(thinking="enabled")
    disabled_payload = build_transport_payload(disabled)
    enabled_payload = build_transport_payload(enabled)

    assert disabled["extensions"] == {"reasoning_mode": "disabled"}
    assert enabled["extensions"] == {"reasoning_mode": "enabled"}
    assert disabled["content_hash"] != enabled["content_hash"]
    assert disabled_payload["model"] == "deepseek-v4-flash"
    assert disabled_payload["response_format"] == {"type": "json_object"}
    assert disabled_payload["thinking"] == {"type": "disabled"}
    assert enabled_payload["thinking"] == {"type": "enabled"}
    assert "thinking" not in json.dumps(disabled)
    assert "reasoning_effort" not in json.dumps(enabled_payload)
    assert set(disabled_payload) == {"model", "messages", "thinking", "response_format", "temperature", "max_tokens"}
    assert set(enabled_payload) == {"model", "messages", "thinking", "response_format", "max_tokens"}
    assert "temperature" not in enabled_payload
    assert "top_p" not in enabled_payload
    assert "presence_penalty" not in enabled_payload
    assert "frequency_penalty" not in enabled_payload


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
def test_no_redirect_handler_refuses_every_http_redirect(status: int):
    handler = DeepSeekNoRedirectHandler()
    request = urllib.request.Request(
        "https://api.deepseek.com/chat/completions",
        data=b"{}",
        method="POST",
        headers={"Authorization": f"Bearer {SECRET}"},
    )
    assert handler.redirect_request(request, object(), status, "redirect", {"Location": "https://attacker.invalid"}, "https://attacker.invalid") is None


class RedirectingOpener:
    def __init__(self, status: int = 302):
        self.status = status
        self.calls: list[tuple[urllib.request.Request, float]] = []

    def open(self, request, timeout):
        self.calls.append((request, timeout))
        raise urllib.error.HTTPError(
            request.full_url,
            self.status,
            "redirect",
            {"Location": "https://attacker.invalid/collect", "X-Leaked": SECRET},
            io.BytesIO(SECRET.encode("utf-8")),
        )


def test_default_urllib_transport_builds_no_redirect_opener_and_discards_redirect_data(monkeypatch):
    opener = RedirectingOpener(302)
    captured: dict[str, tuple[object, ...]] = {}

    def fake_build_opener(*handlers):
        captured["handlers"] = handlers
        return opener

    monkeypatch.setattr(urllib.request, "build_opener", fake_build_opener)
    transport = UrllibDeepSeekTransport()
    assert any(isinstance(handler, DeepSeekNoRedirectHandler) for handler in captured["handlers"])
    with pytest.raises(DeepSeekHTTPError) as exc_info:
        transport(
            "https://api.deepseek.com/chat/completions",
            {"Authorization": f"Bearer {SECRET}"},
            {"model": "deepseek-v4-flash"},
            1.0,
        )
    assert len(opener.calls) == 1
    first_request = opener.calls[0][0]
    assert first_request.full_url == "https://api.deepseek.com/chat/completions"
    assert first_request.get_method() == "POST"
    assert first_request.get_header("Authorization") == f"Bearer {SECRET}"
    assert exc_info.value.status_code == 302
    assert exc_info.value.headers == {}
    assert exc_info.value.__context__ is None
    assert SECRET not in repr(exc_info.value)
    assert SECRET not in str(exc_info.value)


@pytest.mark.parametrize("thinking", ["enabled", "disabled"])
def test_official_success_normalizes_candidate_finish_usage_and_reasoning(thinking: str):
    transport = CaptureTransport()
    adapter = DeepSeekAdapter(api_key=SECRET, transport=transport)
    outcome = adapter.complete(request(thinking=thinking))

    assert outcome.is_success
    assert outcome.response["status"] == "completed"
    assert outcome.response["finish_reason"] == "stop"
    assert outcome.response["candidate"]["status"] == "no_trade"
    assert outcome.response["extensions"]["g018_model_output_ref"]["candidate"]["branch"] == "no_entry"
    assert outcome.call_metadata["usage"] == {"prompt_tokens": 128, "completion_tokens": 64, "total_tokens": 192}
    assert outcome.call_metadata["provider_request_id"] == "cmpl-g022-success"
    assert outcome.call_metadata["extensions"]["deepseek"]["reasoning_tokens"] == 21
    assert transport.calls[0][0] == "https://api.deepseek.com/chat/completions"
    assert transport.calls[0][1]["Authorization"] == f"Bearer {SECRET}"


@pytest.mark.parametrize("missing_field", ["object", "model"])
def test_success_requires_official_object_and_model_identity(missing_field: str):
    response = body()
    del response[missing_field]
    outcome = DeepSeekAdapter(
        api_key=SECRET,
        transport=CaptureTransport(DeepSeekTransportResponse(200, response)),
    ).complete(request())
    assert outcome.classification == "malformed_response"
    assert outcome.retryable is False
    assert outcome.response["candidate"] is None


@pytest.mark.parametrize("variant", ["missing_index", "wrong_index", "multiple_choices"])
def test_success_requires_one_zero_indexed_choice(variant: str):
    response = body()
    if variant == "missing_index":
        del response["choices"][0]["index"]
    elif variant == "wrong_index":
        response["choices"][0]["index"] = 1
    else:
        response["choices"].append(copy.deepcopy(response["choices"][0]))
    outcome = DeepSeekAdapter(
        api_key=SECRET,
        transport=CaptureTransport(DeepSeekTransportResponse(200, response)),
    ).complete(request())
    assert outcome.classification == "malformed_response"
    assert outcome.retryable is False
    assert outcome.response["candidate"] is None


def test_optional_reasoning_metadata_is_null_when_provider_omits_it():
    transport = CaptureTransport(DeepSeekTransportResponse(200, body("success_reasoning.json")))
    outcome = DeepSeekAdapter(api_key=SECRET, transport=transport).complete(request(thinking="disabled"))
    assert outcome.is_success
    assert outcome.call_metadata["usage"] == {"prompt_tokens": 128, "completion_tokens": 64, "total_tokens": 192}
    deepseek = outcome.call_metadata["extensions"]["deepseek"]
    assert deepseek["reasoning_tokens"] is None
    assert deepseek["prompt_cache_hit_tokens"] is None
    assert deepseek["prompt_cache_miss_tokens"] is None


@pytest.mark.parametrize(
    "status,classification,retryable",
    [
        (400, "invalid_request", False),
        (401, "authentication", False),
        (402, "insufficient_balance", False),
        (422, "invalid_request", False),
        (429, "rate_limit", True),
        (500, "server", True),
        (503, "server", True),
    ],
)
def test_documented_http_failures_map_to_g017(status: int, classification: str, retryable: bool):
    transport = CaptureTransport(DeepSeekTransportResponse(status, {"error": {"message": "provider detail omitted"}}))
    adapter = DeepSeekAdapter(api_key=SECRET, transport=transport)
    outcome = adapter.complete(request())
    assert outcome.classification == classification
    assert outcome.retryable is retryable
    assert outcome.response["status"] == "error"
    assert outcome.response["candidate"] is None
    assert outcome.error["retryable"] is retryable
    assert adapter.attempt_count == 1


def test_injected_and_default_redirect_statuses_share_terminal_provider_mapping():
    injected_transport = CaptureTransport(DeepSeekTransportResponse(302, {"ignored": SECRET}))
    injected = DeepSeekAdapter(api_key=SECRET, transport=injected_transport).complete(request())

    opener = RedirectingOpener(302)
    default_transport = UrllibDeepSeekTransport(opener=opener)
    default = DeepSeekAdapter(api_key=SECRET, transport=default_transport).complete(request())

    for outcome in (injected, default):
        assert outcome.classification == "provider"
        assert outcome.retryable is False
        assert outcome.response["candidate"] is None
        assert outcome.response["error"]["code"] == "provider_error"
        assert outcome.response["error"]["retryable"] is False
        assert SECRET not in json.dumps(outcome.to_dict())
    assert len(opener.calls) == 1


def test_timeout_is_retryable_but_has_no_automatic_retry():
    transport = CaptureTransport(error=TimeoutError("key=" + SECRET))
    adapter = DeepSeekAdapter(api_key=SECRET, transport=transport)
    outcome = adapter.complete(request())
    assert outcome.classification == "timeout"
    assert outcome.retryable is True
    assert adapter.attempt_count == 1
    assert len(transport.calls) == 1
    assert SECRET not in json.dumps(outcome.to_dict())


def test_unclassified_transport_failure_is_terminal_without_transient_evidence():
    transport = CaptureTransport(error=RuntimeError("provider transport failed"))
    outcome = DeepSeekAdapter(api_key=SECRET, transport=transport).complete(request())
    assert outcome.classification == "transport"
    assert outcome.retryable is False


@pytest.mark.parametrize("status", [0, 99, 600, -1, SECRET])
def test_malformed_transport_status_is_terminal_and_secret_safe(status):
    # A response object can be structurally typed while still carrying an
    # untrusted status value.  It must not escape as ValueError (which could
    # echo the injected runtime key).
    transport = CaptureTransport(DeepSeekTransportResponse(status, {}))
    adapter = DeepSeekAdapter(api_key=SECRET, transport=transport)
    outcome = adapter.complete(request())
    assert outcome.classification == "transport"
    assert outcome.retryable is False
    assert adapter.attempt_count == 1
    assert len(transport.calls) == 1
    assert SECRET not in repr(outcome)
    assert SECRET not in json.dumps(outcome.to_dict())


@pytest.mark.parametrize("fixture_name", ["finish_length.json", "malformed_content.json"])
def test_truncated_or_malformed_output_never_becomes_a_decision(fixture_name: str):
    outcome = DeepSeekAdapter(api_key=SECRET, transport=CaptureTransport(DeepSeekTransportResponse(200, body(fixture_name)))).complete(request())
    assert outcome.classification == "malformed_response"
    assert outcome.response["status"] == "error"
    assert outcome.response["error"]["code"] == "invalid_model_response"
    assert outcome.response["candidate"] is None
    if fixture_name == "finish_length.json":
        assert outcome.response["finish_reason"] == "length"
        assert outcome.call_metadata["usage"]["total_tokens"] == 384


@pytest.mark.parametrize("finish_reason", [SECRET, "future_finish_reason"])
def test_unknown_or_secret_finish_reason_is_null_and_returns_terminal_outcome(finish_reason: str):
    response = body()
    response["choices"][0]["finish_reason"] = finish_reason
    transport = CaptureTransport(DeepSeekTransportResponse(200, response))
    outcome = DeepSeekAdapter(api_key=SECRET, transport=transport).complete(request())

    assert outcome.classification == "malformed_response"
    assert outcome.retryable is False
    assert outcome.response["candidate"] is None
    assert outcome.response["finish_reason"] is None
    assert outcome.call_metadata["usage"] == {"prompt_tokens": 128, "completion_tokens": 64, "total_tokens": 192}
    assert SECRET not in json.dumps(outcome.to_dict())
    assert SECRET not in json.dumps(outcome.call_metadata)
    assert SECRET not in str(outcome)
    assert SECRET not in repr(outcome)
    assert transport.calls and len(transport.calls) == 1


@pytest.mark.parametrize("empty_content", ["", None])
def test_insufficient_system_resource_is_retryable_server_without_candidate(empty_content):
    resource = body("finish_resource.json")
    resource["choices"][0]["message"]["content"] = empty_content
    transport = CaptureTransport(
        DeepSeekTransportResponse(200, resource)
    )
    adapter = DeepSeekAdapter(api_key=SECRET, transport=transport)
    outcome = adapter.complete(request())

    assert outcome.classification == "server"
    assert outcome.retryable is True
    assert outcome.response["status"] == "error"
    assert outcome.response["candidate"] is None
    assert outcome.response["finish_reason"] == "insufficient_system_resource"
    assert outcome.response["generated_at"] == "2026-04-11T10:40:00.000Z"
    assert outcome.response["error"]["code"] == "provider_error"
    assert outcome.response["error"]["retryable"] is True
    assert outcome.response["error"]["extensions"]["provider_neutral_classification"] == "server"
    assert outcome.response["error"]["extensions"]["transient"] is True
    assert outcome.call_metadata["usage"] == {"prompt_tokens": 128, "completion_tokens": 0, "total_tokens": 128}
    assert outcome.call_metadata["provider_request_id"] == "cmpl-g022-resource"
    assert outcome.call_metadata["extensions"]["deepseek"] == {
        "reasoning_tokens": 0,
        "prompt_cache_hit_tokens": 32,
        "prompt_cache_miss_tokens": 96,
        "system_fingerprint": "fp-g022-resource",
    }
    serialized = json.dumps(outcome.to_dict())
    assert '"content":' not in serialized
    assert '"reasoning_content":' not in serialized
    assert adapter.attempt_count == 1
    assert len(transport.calls) == 1


@pytest.mark.parametrize("finish_reason", ["length", "tool_calls", "content_filter"])
def test_other_non_stop_finish_reasons_remain_terminal_malformed(finish_reason: str):
    interrupted = body("finish_resource.json")
    interrupted["choices"][0]["finish_reason"] = finish_reason
    outcome = DeepSeekAdapter(
        api_key=SECRET,
        transport=CaptureTransport(DeepSeekTransportResponse(200, interrupted)),
    ).complete(request())
    assert outcome.classification == "malformed_response"
    assert outcome.retryable is False
    assert outcome.response["candidate"] is None
    assert outcome.response["finish_reason"] == finish_reason


def test_resource_finish_with_secret_bearing_content_stays_terminal_and_redacted():
    leaked = body("finish_resource.json")
    leaked["choices"][0]["message"]["content"] = SECRET
    outcome = DeepSeekAdapter(
        api_key=SECRET,
        transport=CaptureTransport(DeepSeekTransportResponse(200, leaked)),
    ).complete(request())
    assert outcome.classification == "malformed_response"
    assert outcome.retryable is False
    assert outcome.response["candidate"] is None
    assert SECRET not in json.dumps(outcome.to_dict())


def test_known_secret_never_enters_request_response_metadata_or_exception():
    capture = CaptureTransport()
    adapter = DeepSeekAdapter(api_key=SECRET, transport=capture)
    leaked_request = request()
    leaked_request["context_text"] = "do not persist " + SECRET
    # The normalized request is already finalized; the adapter rejects it at
    # the runtime secret boundary before a transport attempt.
    with pytest.raises(Exception) as exc_info:
        adapter.complete(leaked_request)
    assert SECRET not in repr(exc_info.value)
    assert SECRET not in str(exc_info.value)
    assert adapter.attempt_count == 0

    leaked_body = body()
    leaked_body["choices"][0]["message"]["content"] = SECRET
    outcome = DeepSeekAdapter(api_key=SECRET, transport=CaptureTransport(DeepSeekTransportResponse(200, leaked_body))).complete(request())
    assert outcome.classification == "malformed_response"
    assert SECRET not in json.dumps(outcome.to_dict())
    assert SECRET not in repr(DeepSeekAdapter(api_key=SECRET, transport=capture))


def test_runtime_resolver_key_is_reusable_concurrently_without_cross_call_leakage():
    transport = CaptureTransport()
    resolver = RuntimeCredentialResolver({"deepseek": {"api_key": SECRET}})
    adapter = DeepSeekAdapter(credential_resolver=resolver, transport=transport)
    with ThreadPoolExecutor(max_workers=8) as pool:
        outcomes = list(pool.map(lambda i: adapter.complete(request(correlation_id=f"corr-{i}")), range(16)))
    assert all(outcome.is_success for outcome in outcomes)
    assert adapter.attempt_count == 16
    assert len(transport.calls) == 16
    assert all(call[1]["Authorization"] == f"Bearer {SECRET}" for call in transport.calls)
    assert all(SECRET not in json.dumps(outcome.to_dict()) for outcome in outcomes)


def test_cancel_and_unsupported_capability_do_not_attempt_transport():
    transport = CaptureTransport()
    adapter = DeepSeekAdapter(api_key=SECRET, transport=transport)
    cancelled = adapter.complete(request(), cancelled=True, generation=7)
    assert cancelled.is_cancelled and cancelled.generation == 7
    assert adapter.attempt_count == 0
    unsupported_request = make_normalized_model_request(
        provider="deepseek",
        model="deepseek-v4-flash",
        adapter="g022-deepseek-v4-flash-0.1.0",
        reasoning_mode="disabled",
        requested_capabilities=("structured_json", "tools"),
    )
    unsupported = adapter.complete(unsupported_request)
    assert unsupported.classification == "unsupported_capability"
    assert adapter.attempt_count == 0


def test_config_and_smoke_payload_are_non_secret():
    config = DeepSeekConfig()
    assert config.endpoint == "https://api.deepseek.com/chat/completions"
    assert "reasoning_effort" not in repr(config)
    assert SECRET not in repr(DeepSeekAdapter(api_key=SECRET))


def test_config_rejects_non_official_endpoint_but_normalizes_trailing_slash():
    assert DeepSeekConfig(endpoint="https://api.deepseek.com/chat/completions/").endpoint == "https://api.deepseek.com/chat/completions"
    with pytest.raises(ValueError):
        DeepSeekConfig(endpoint="https://attacker.invalid/chat/completions")
    with pytest.raises(ValueError):
        DeepSeekConfig(endpoint="http://api.deepseek.com/chat/completions")
    with pytest.raises(ValueError):
        DeepSeekConfig(endpoint="https://api.deepseek.com/other")


def test_smoke_request_embeds_a_strict_g018_candidate():
    smoke_request = make_smoke_request(thinking="disabled")
    parsed = parse_candidate(smoke_request["context_text"])
    assert parsed == SMOKE_CANDIDATE
    assert smoke_request["parameters"]["max_output_tokens"] == 1024
    enabled_smoke_request = make_smoke_request(thinking="enabled")
    assert enabled_smoke_request["parameters"]["max_output_tokens"] == 4096
    assert validate_normalized_request(enabled_smoke_request).valid


@pytest.mark.parametrize("thinking", ["enabled", "disabled"])
def test_smoke_cli_dry_run_is_executable_for_both_modes(thinking: str):
    result = subprocess.run(
        [sys.executable, "-m", "pa_engine.providers.deepseek.smoke", "--dry-run", "--thinking", thinking],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    document = json.loads(result.stdout)
    assert document["ok"] is True
    assert document["thinking"] == thinking
    payload = document["payload"]
    assert payload["thinking"] == {"type": thinking}
    assert payload["max_tokens"] == (4096 if thinking == "enabled" else 1024)
    if thinking == "enabled":
        assert "temperature" not in payload
    else:
        assert payload["temperature"] == 0
    assert SECRET not in result.stdout
    assert SECRET not in result.stderr


def test_smoke_cli_without_key_returns_stable_nonzero_without_secret_leak():
    environment = os.environ.copy()
    environment.pop("DEEPSEEK_API_KEY", None)
    result = subprocess.run(
        [sys.executable, "-m", "pa_engine.providers.deepseek.smoke", "--thinking", "disabled"],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 2
    assert json.loads(result.stdout) == {"error": "DEEPSEEK_API_KEY is not set", "ok": False}
    assert SECRET not in result.stdout
    assert SECRET not in result.stderr
