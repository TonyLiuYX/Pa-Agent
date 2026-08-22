"""Offline G031 OpenAI Responses adapter conformance tests."""

from __future__ import annotations

import asyncio
import copy
import io
import json
import threading
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest

from pa_engine.providers import RuntimeCredentialResolver, make_normalized_model_request
from pa_engine.providers.openai import (
    OpenAIAdapter,
    OpenAIConfig,
    OpenAIHTTPError,
    OpenAINoRedirectHandler,
    OpenAITransportResponse,
    UrllibOpenAITransport,
    build_transport_payload,
)
from pa_engine.providers.openai.config import OPENAI_ADAPTER_VERSION, OPENAI_API_URL, OPENAI_MODEL, OPENAI_PROVIDER
from pa_engine.providers.openai.smoke import SMOKE_CANDIDATE, make_smoke_request
from pa_engine.providers.base import late_response_outcome


ROOT = Path(__file__).resolve().parents[3]
FIXTURES = ROOT / "tests" / "fixtures" / "phase2" / "g031"
SECRET = "sk-g031-offline-secret-7f3a"


def fixture(name: str = "response_success.json") -> dict[str, Any]:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def request(*, reasoning_mode: str = "enabled", adapter: str = OPENAI_ADAPTER_VERSION) -> dict[str, Any]:
    return make_normalized_model_request(
        provider=OPENAI_PROVIDER,
        model=OPENAI_MODEL,
        adapter=adapter,
        correlation_id="g031-test",
        reasoning_mode=reasoning_mode,
        requested_capabilities=("structured_json", "reasoning_mode"),
        instructions="Return exactly one JSON object matching the G018 candidate schema.",
        context_text="G031 fixture context; never include a key here.",
    )


class CaptureTransport:
    def __init__(self, response: OpenAITransportResponse | None = None, *, error: BaseException | None = None):
        self.response = response or OpenAITransportResponse(200, fixture())
        self.error = error
        self.calls: list[tuple[str, dict[str, str], dict[str, Any], float]] = []
        self.lock = threading.Lock()

    def __call__(self, url, headers, payload, timeout):
        with self.lock:
            self.calls.append((url, dict(headers), copy.deepcopy(dict(payload)), timeout))
        if self.error is not None:
            raise self.error
        return self.response


def test_payload_fixes_luna_max_structured_json_stateless_storage_and_tier():
    standard = build_transport_payload(request())
    fast = build_transport_payload(request(), speed="fast")

    assert standard["model"] == OPENAI_MODEL
    assert standard["reasoning"] == {"effort": "max"}
    assert standard["service_tier"] == "default"
    assert fast["service_tier"] == "fast"
    assert standard["store"] is False
    assert standard["text"]["format"]["type"] == "json_schema"
    assert standard["text"]["format"]["strict"] is True
    assert standard["text"]["format"]["name"] == "pa_decision_candidate"
    schema = standard["text"]["format"]["schema"]
    assert schema["type"] == "object"
    assert schema["additionalProperties"] is False
    assert set(schema["required"]) == {
        "schema_version",
        "branch",
        "reason",
        "direction",
        "entry_price",
        "stop_price",
        "target_price",
        "N",
        "X",
        "grounding",
        "confidence",
        "uncertainty",
        "shared_market_view_ref",
        "second_event_ref",
    }
    assert schema["$defs"]["derivedGroundingRef"]["properties"]["declaration_type"] == {
        "const": "derived_interpretation",
        "type": "string",
    }
    assert "allOf" not in schema["$defs"]["derivedGroundingRef"]
    assert "previous_response_id" not in standard
    assert "conversation" not in standard
    assert "temperature" not in standard
    assert "top_p" not in standard


def test_structured_schema_has_no_unsupported_keywords_anywhere():
    schema = build_transport_payload(request())["text"]["format"]["schema"]
    unsupported = {
        "allOf",
        "not",
        "dependentRequired",
        "dependentSchemas",
        "if",
        "then",
        "else",
        "uniqueItems",
    }

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            assert unsupported.isdisjoint(value)
            if "const" in value or "enum" in value:
                assert "type" in value
            if value.get("type") == "object":
                assert value.get("additionalProperties") is False
                assert set(value.get("properties", {})) == set(value.get("required", []))
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(schema)
    assert "$schema" not in schema
    assert "$id" not in schema
    assert "title" not in schema


def test_success_accepts_reasoning_item_and_one_assistant_text_and_normalizes_usage_and_tier():
    outcome = OpenAIAdapter(api_key=SECRET, transport=CaptureTransport()).complete(request())

    assert outcome.is_success
    assert outcome.response["status"] == "completed"
    assert outcome.response["finish_reason"] == "completed"
    assert outcome.response["candidate"]["status"] == "no_trade"
    assert outcome.call_metadata["usage"] == {"prompt_tokens": 128, "completion_tokens": 64, "total_tokens": 192}
    assert outcome.call_metadata["provider_request_id"] == "resp-g031-success"
    assert outcome.call_metadata["extensions"]["openai"] == {
        "requested_service_tier": "default",
        "effective_service_tier": "default",
        "reasoning_tokens": 21,
        "cached_input_tokens": 32,
    }


def test_fast_request_preserves_effective_priority_tier_without_promising_a_multiplier():
    transport = CaptureTransport(OpenAITransportResponse(200, fixture("response_fast.json")))
    outcome = OpenAIAdapter(api_key=SECRET, config=OpenAIConfig(speed="fast"), transport=transport).complete(request())

    assert outcome.is_success
    assert transport.calls[0][0] == OPENAI_API_URL
    assert transport.calls[0][2]["service_tier"] == "fast"
    assert outcome.call_metadata["extensions"]["openai"]["requested_service_tier"] == "fast"
    assert outcome.call_metadata["extensions"]["openai"]["effective_service_tier"] == "priority"


@pytest.mark.parametrize(
    "variant",
    [
        "object",
        "model",
        "status",
        "output",
        "multiple_messages",
        "filtered",
        "invalid_candidate",
        "branch_inconsistent",
        "missing_usage",
        "bad_usage",
        "missing_tier",
    ],
)
def test_malformed_incomplete_filtered_ambiguous_or_invalid_output_fails_closed(variant: str):
    response = fixture()
    if variant == "object":
        response["object"] = "chat.completion"
    elif variant == "model":
        response["model"] = "gpt-5.6-sol"
    elif variant == "status":
        response["status"] = "incomplete"
    elif variant == "output":
        response["output"] = []
    elif variant == "multiple_messages":
        response["output"].append(copy.deepcopy(response["output"][1]))
    elif variant == "filtered":
        response["output"][1]["content"] = [{"type": "refusal", "refusal": "not available"}]
    elif variant == "invalid_candidate":
        response["output"][1]["content"][0]["text"] = "{}"
    elif variant == "branch_inconsistent":
        candidate = copy.deepcopy(SMOKE_CANDIDATE)
        candidate["reason"] = "This reason is invalid for the no_entry branch."
        response["output"][1]["content"][0]["text"] = json.dumps(candidate)
    elif variant == "missing_usage":
        del response["usage"]
    elif variant == "bad_usage":
        response["usage"]["total_tokens"] = 999
    elif variant == "missing_tier":
        del response["service_tier"]

    transport = CaptureTransport(OpenAITransportResponse(200, response))
    outcome = OpenAIAdapter(api_key=SECRET, transport=transport).complete(request())
    assert outcome.classification == "malformed_response"
    assert outcome.response["status"] == "error"
    assert outcome.response["error"]["code"] == "invalid_model_response"
    assert outcome.response["candidate"] is None
    assert outcome.retryable is False
    assert len(transport.calls) == 1


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
        (302, "provider", False),
        (504, "timeout", True),
    ],
)
def test_http_failures_follow_g017_and_make_one_attempt(status: int, classification: str, retryable: bool):
    transport = CaptureTransport(OpenAITransportResponse(status, {"error": {"message": SECRET}}))
    adapter = OpenAIAdapter(api_key=SECRET, transport=transport)
    outcome = adapter.complete(request())
    assert outcome.classification == classification
    assert outcome.retryable is retryable
    assert outcome.response["candidate"] is None
    assert adapter.attempt_count == 1
    assert SECRET not in json.dumps(outcome.to_dict())


@pytest.mark.parametrize("error", [TimeoutError("authorization=" + SECRET), RuntimeError("transport=" + SECRET)])
def test_transport_failures_are_typed_without_automatic_retry(error: BaseException):
    transport = CaptureTransport(error=error)
    adapter = OpenAIAdapter(api_key=SECRET, transport=transport)
    outcome = adapter.complete(request())
    assert outcome.classification in {"timeout", "transport"}
    assert outcome.retryable is (outcome.classification == "timeout")
    assert adapter.attempt_count == 1
    assert len(transport.calls) == 1
    assert SECRET not in repr(outcome)


def test_disabled_reasoning_is_invalid_configuration_before_transport_and_no_key_is_not_called():
    transport = CaptureTransport()
    outcome = OpenAIAdapter(api_key=SECRET, transport=transport).complete(request(reasoning_mode="disabled"))
    assert outcome.classification == "invalid_configuration"
    assert outcome.response["error"]["code"] == "internal_error"
    assert transport.calls == []
    assert OpenAIAdapter(transport=transport).complete(request()).classification == "authentication"
    assert transport.calls == []


def test_adapter_resolves_only_openai_credential_and_redacts_both_secret_kinds():
    deepseek_secret = "ds-g031-unselected-secret"
    openai_secret = "sk-g031-selected-secret"
    seen: list[str] = []

    def resolve(provider: str):
        seen.append(provider)
        return {"deepseek": {"api_key": deepseek_secret}, "openai": {"api_key": openai_secret}}.get(provider)

    transport = CaptureTransport()
    outcome = OpenAIAdapter(credential_resolver=RuntimeCredentialResolver(resolve), transport=transport).complete(request())
    serialized = json.dumps(outcome.to_dict(), ensure_ascii=False)
    assert outcome.is_success
    assert seen == ["openai"]
    assert transport.calls[0][1]["Authorization"] == "Bearer " + openai_secret
    assert deepseek_secret not in serialized
    assert openai_secret not in serialized
    assert openai_secret not in repr(outcome)


def test_shared_key_is_safe_under_concurrent_calls():
    transport = CaptureTransport()
    adapter = OpenAIAdapter(api_key=SECRET, transport=transport)
    with ThreadPoolExecutor(max_workers=8) as pool:
        outcomes = list(pool.map(lambda _: adapter.complete(request()), range(24)))
    assert all(outcome.is_success for outcome in outcomes)
    assert adapter.attempt_count == 24
    assert len(transport.calls) == 24
    assert all(call[0] == OPENAI_API_URL for call in transport.calls)


def test_cancellation_and_late_response_follow_g017_types():
    transport = CaptureTransport()
    adapter = OpenAIAdapter(api_key=SECRET, transport=transport)
    cancelled = adapter.complete(request(), cancelled=True, generation=7)
    assert cancelled.is_cancelled
    assert cancelled.classification == "cancellation"
    assert cancelled.generation == 7
    assert transport.calls == []

    completed = adapter.complete(request(), generation=8)
    late = late_response_outcome(request(), completed.response, call_metadata=completed.call_metadata, generation=8)
    assert late.is_late
    assert late.classification == "late_response"
    assert late.retryable is False


@pytest.mark.parametrize("status", [0, 99, 600, -1, SECRET])
def test_malformed_transport_status_is_terminal_and_secret_safe(status: Any):
    transport = CaptureTransport(OpenAITransportResponse(status, {}))
    adapter = OpenAIAdapter(api_key=SECRET, transport=transport)
    outcome = adapter.complete(request())
    assert outcome.classification == "transport"
    assert outcome.retryable is False
    assert adapter.attempt_count == 1
    assert SECRET not in repr(outcome)


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
            io.BytesIO(SECRET.encode()),
        )


def test_default_transport_refuses_redirects_and_discards_provider_body():
    opener = RedirectingOpener()
    transport = UrllibOpenAITransport(opener=opener)
    with pytest.raises(OpenAIHTTPError) as error:
        transport(OPENAI_API_URL, {"Authorization": "Bearer " + SECRET}, {"model": OPENAI_MODEL}, 1.0)
    assert len(opener.calls) == 1
    assert opener.calls[0][0].full_url == OPENAI_API_URL
    assert opener.calls[0][0].get_method() == "POST"
    assert error.value.status_code == 302
    assert SECRET not in repr(error.value)
    assert SECRET not in str(error.value)
    handler = OpenAINoRedirectHandler()
    req = urllib.request.Request(OPENAI_API_URL, data=b"{}", method="POST", headers={"Authorization": "Bearer " + SECRET})
    assert handler.redirect_request(req, object(), 302, "redirect", {}, "https://attacker.invalid") is None


def test_openai_smoke_request_is_fixed_and_candidate_is_json_native():
    req = make_smoke_request()
    assert req["provider"] == "openai"
    assert req["model"] == "gpt-5.6-luna"
    assert req["extensions"]["reasoning_mode"] == "enabled"
    assert SMOKE_CANDIDATE["branch"] == "no_entry"
