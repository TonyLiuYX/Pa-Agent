"""Focused G017 acceptance tests for the provider-neutral model seam."""

from __future__ import annotations

import json
import pickle
import asyncio
import concurrent.futures
import traceback
from pathlib import Path

import pytest

from pa_engine.contracts.canonical import content_hash
from pa_engine.providers import (
    AdapterOutcome,
    CapabilityDescriptor,
    ContractValidationError,
    CredentialResolutionError,
    FailureClassification,
    MalformedRequestError,
    MalformedResponseError,
    ProviderNeutralModelGateway,
    ReplayAdapter,
    ReplayDataError,
    ReplayMissingError,
    RuntimeCredentialResolver,
    ScriptExhaustedError,
    ScriptedAdapter,
    ScriptedStep,
    late_response_outcome,
    make_model_call_metadata,
    make_normalized_model_request,
    make_normalized_model_response,
    normalize_error,
    normalize_exception,
    redact_exception,
    redact_secrets,
    redacted_json,
    request_semantic_identity,
    scripted_error,
    scripted_success,
    validate_model_call_metadata,
    validate_normalized_request,
    validate_normalized_response,
)


def candidate(status: str = "no_trade") -> dict[str, object]:
    action = "none" if status == "no_trade" else "buy"
    stance = "neutral" if status == "no_trade" else "bullish"
    return {
        "status": status,
        "stance": stance,
        "action": action,
        "setups": [],
        "rationales": [],
        "conditions": [],
        "levels": [],
        "model_confidence": None,
        "uncertainty": {"level": "low", "codes": [], "details": "G017 deterministic fixture"},
        "extensions": {},
    }


def request(*, correlation_id: str = "corr-g017", reasoning_mode: str = "disabled") -> dict[str, object]:
    return make_normalized_model_request(
        correlation_id=correlation_id,
        provider="offline",
        model="fixture-model",
        adapter="g017-test-adapter-0.1.0",
        reasoning_mode=reasoning_mode,
    )


def test_request_extensions_reasoning_mode_is_semantic_and_vendor_neutral():
    disabled = request(reasoning_mode="disabled")
    enabled = request(reasoning_mode="enabled")

    assert validate_normalized_request(disabled).valid
    assert validate_normalized_request(enabled).valid
    assert disabled["extensions"] == {"reasoning_mode": "disabled"}
    assert enabled["extensions"] == {"reasoning_mode": "enabled"}
    assert "thinking.type" not in json.dumps(enabled)
    assert request_semantic_identity(disabled) != request_semantic_identity(enabled)
    assert disabled["content_hash"] != enabled["content_hash"]


def test_contract_constructors_validate_and_preserve_missing_telemetry_as_none():
    req = request()
    response = make_normalized_model_response(req, candidate=candidate(), finish_reason=None)
    metadata = make_model_call_metadata(req, response)

    assert validate_normalized_response(response, request=req).valid
    assert validate_model_call_metadata(metadata, request=req, response=response).valid
    assert response["finish_reason"] is None
    assert metadata["started_at"] is None
    assert metadata["completed_at"] is None
    assert metadata["latency_ms"] is None
    assert metadata["usage"] is None
    assert metadata["provider_request_id"] is None
    assert metadata["correlation_id"] == req["correlation_id"]
    assert content_hash(metadata, "model_call_metadata") == metadata["content_hash"]


def test_scripted_and_replay_adapters_share_one_request_without_provider_sdk_or_network():
    req = request()
    scripted = ScriptedAdapter([scripted_success(candidate(), finish_reason="stop")])
    scripted_outcome = ProviderNeutralModelGateway(scripted).call(req)
    assert scripted_outcome.is_success
    assert scripted_outcome.response["model_request_id"] == req["model_request_id"]

    replay = ReplayAdapter([{"request": req, "response": scripted_outcome.response, "call_metadata": scripted_outcome.call_metadata}])
    replay_outcome = ProviderNeutralModelGateway(replay).complete(req)
    assert replay_outcome.is_success
    assert replay_outcome.response["content_hash"] == scripted_outcome.response["content_hash"]
    assert replay_outcome.call_metadata["model_response_hash"] == scripted_outcome.response["content_hash"]
    assert scripted.capabilities.supports("structured_json")
    assert scripted.capabilities.supports("reasoning_mode")
    assert replay.capabilities.to_dict() == {"structured_json": True, "reasoning_mode": True}


def test_scripted_sequence_is_deterministic_and_has_explicit_exhaustion_modes():
    req = request()
    adapter = ScriptedAdapter([candidate(), scripted_error("timeout")])
    first = adapter.complete(req)
    second = adapter.complete(req)
    exhausted = adapter.complete(req)

    assert first.is_success
    assert second.classification == "timeout"
    assert second.error["code"] == "model_timeout"
    assert second.retryable is True
    assert exhausted.classification == "script_exhausted"
    assert exhausted.retryable is False
    assert adapter.remaining == 0

    repeat = ScriptedAdapter([candidate()], on_exhaustion="repeat_last")
    assert repeat.complete(req).is_success
    assert repeat.complete(req).is_success
    raising = ScriptedAdapter([], on_exhaustion="raise")
    with pytest.raises(ScriptExhaustedError):
        raising.complete(req)


def test_json_script_fixture_is_loadable_and_deterministic():
    fixture_path = Path(__file__).parents[2] / "fixtures" / "phase2" / "g017" / "scripted_sequence.json"
    steps = json.loads(fixture_path.read_text(encoding="utf-8"))
    adapter = ScriptedAdapter(steps)

    first = adapter.complete(request())
    timeout = adapter.complete(request())
    authentication = adapter.complete(request())

    assert first.is_success
    assert timeout.classification == FailureClassification.TIMEOUT.value
    assert timeout.retryable is True
    assert authentication.classification == FailureClassification.AUTHENTICATION.value
    assert authentication.retryable is False


def test_replay_uses_semantic_identity_not_correlation_or_call_telemetry():
    recorded_request = request(correlation_id="recorded-correlation")
    recorded_response = ScriptedAdapter([candidate()]).complete(recorded_request).response
    adapter = ReplayAdapter([{"request": recorded_request, "response": recorded_response}])

    current_request = request(correlation_id="current-correlation")
    assert request_semantic_identity(recorded_request) == request_semantic_identity(current_request)
    outcome = adapter.complete(current_request)

    assert outcome.is_success
    assert outcome.response["content_hash"] == recorded_response["content_hash"]
    assert outcome.call_metadata["correlation_id"] == "current-correlation"
    assert outcome.call_metadata["latency_ms"] is None
    assert outcome.call_metadata["usage"] is None

    json_replay = ReplayAdapter.from_json(
        json.dumps({"records": [{"request": recorded_request, "response": recorded_response}]})
    )
    assert json_replay.complete(current_request).is_success

    missing_request = request(correlation_id="missing", reasoning_mode="enabled")
    missing = adapter.complete(missing_request)
    assert missing.classification == "replay_missing"
    assert missing.error["code"] == "internal_error"
    assert missing.retryable is False
    strict = ReplayAdapter([{"request": recorded_request, "response": recorded_response}], on_missing="raise")
    with pytest.raises(ReplayMissingError):
        strict.complete(missing_request)


def test_replay_rejects_malformed_or_conflicting_recordings():
    req = request()
    response = ScriptedAdapter([candidate()]).complete(req).response
    broken = dict(response)
    broken["model_request_id"] = "mrq_" + "f" * 64
    with pytest.raises(ReplayDataError):
        ReplayAdapter([{"request": req, "response": broken}])

    other = request(reasoning_mode="enabled")
    other_response = ScriptedAdapter([candidate("no_trade")]).complete(other).response
    with pytest.raises(ReplayDataError):
        ReplayAdapter(
            [
                {"request": req, "response": response},
                {"request": req, "response": other_response},
            ]
        )


@pytest.mark.parametrize(
    "kind,code,retryable",
    [
        ("timeout", "model_timeout", True),
        ("authentication", "authentication_error", False),
        ("rate_limit", "rate_limit", True),
        ("server", "provider_error", False),
        ("provider", "provider_error", False),
        ("transport", "transport_error", False),
        ("cancellation", "internal_error", False),
        ("late_response", "internal_error", False),
        ("malformed_response", "invalid_model_response", False),
        ("invalid_configuration", "internal_error", False),
        ("invalid_request", "internal_error", False),
        ("insufficient_balance", "internal_error", False),
        ("unsupported_capability", "unsupported_capability", False),
    ],
)
def test_normalized_error_matrix_is_frozen_and_terminal_classes_cannot_retry(kind, code, retryable):
    error = normalize_error(kind, retryable=True)
    assert error["code"] == code
    assert error["retryable"] is retryable
    if kind in {"cancellation", "late_response"}:
        assert error["extensions"]["provider_neutral_classification"] == kind


def test_cancellation_and_late_response_are_typed_without_changing_g014_schema():
    req = request()
    cancelled = ScriptedAdapter([candidate()]).complete(req, cancelled=True)
    assert cancelled.is_cancelled
    assert cancelled.classification == FailureClassification.CANCELLATION.value
    assert cancelled.error["code"] == "internal_error"
    assert cancelled.error["extensions"]["provider_neutral_classification"] == "cancellation"

    valid = ScriptedAdapter([candidate()]).complete(req)
    late = late_response_outcome(req, valid.response, call_metadata=valid.call_metadata)
    assert late.is_late
    assert late.classification == "late_response"
    assert late.response["status"] == "completed"
    assert late.retryable is False


def test_recursive_secret_redaction_covers_collections_exceptions_and_json():
    synthetic_secret = "fixture-only-secret-7f3a"
    exc = RuntimeError(f"authorization=Bearer {synthetic_secret}")
    value = {
        "api_key": synthetic_secret,
        "nested": [synthetic_secret, ("safe", exc)],
        "message": f"token={synthetic_secret}",
    }
    redacted = redact_secrets(value, secret_values=(synthetic_secret,))
    serialized = redacted_json(value, secret_values=(synthetic_secret,))

    assert synthetic_secret not in repr(redacted)
    assert synthetic_secret not in serialized
    assert redact_exception(exc, secret_values=(synthetic_secret,)).find(synthetic_secret) == -1
    assert redacted["api_key"] == "[REDACTED]"
    assert isinstance(redacted["nested"][1], tuple)

    resolver = RuntimeCredentialResolver({"offline": {"api_key": synthetic_secret}})
    credential = resolver.require("offline")
    assert synthetic_secret not in repr(credential)
    assert synthetic_secret not in str(resolver)
    with pytest.raises(TypeError):
        pickle.dumps(credential)
    with pytest.raises(TypeError):
        json.dumps(credential)


def test_malformed_scripted_response_is_typed_not_silently_swallowed():
    req = request()
    malformed = ScriptedAdapter([ScriptedStep(response={"artifact_kind": "normalized_model_response"})]).complete(req)
    assert malformed.classification == "malformed_response"
    assert malformed.error["code"] == "invalid_model_response"
    assert malformed.retryable is False


def test_provider_specific_thinking_extensions_are_rejected_at_gateway_boundary():
    with pytest.raises(ContractValidationError):
        make_normalized_model_request(
            extensions={"thinking": {"type": "enabled"}},
        )


def test_malformed_candidate_is_normalized_as_invalid_model_response():
    req = request()
    outcome = ScriptedAdapter([scripted_success({"status": "no_trade"})]).complete(req)

    assert outcome.classification == "malformed_response"
    assert outcome.error["code"] == "invalid_model_response"
    assert outcome.retryable is False


def test_standard_cancellation_exceptions_are_terminal_and_typed():
    for exc in (asyncio.CancelledError("cancelled"), concurrent.futures.CancelledError()):
        error = normalize_exception(exc)
        assert error["code"] == "internal_error"
        assert error["retryable"] is False
        assert error["extensions"]["provider_neutral_classification"] == "cancellation"


def test_late_response_rejects_unvalidated_call_metadata():
    req = request()
    response = ScriptedAdapter([candidate()]).complete(req).response

    with pytest.raises(ContractValidationError):
        late_response_outcome(req, response, call_metadata={"unexpected": True})


def test_scripted_error_aliases_expose_canonical_classifications():
    req = request()
    outcome = ScriptedAdapter([scripted_error("server_error")]).complete(req)

    assert outcome.classification == "server"
    assert outcome.error["code"] == "provider_error"


def test_terminal_recorded_responses_are_not_reported_as_success():
    req = request()
    response = make_normalized_model_response(
        req,
        status="error",
        error=normalize_error("timeout"),
    )

    scripted = ScriptedAdapter([ScriptedStep(response=response)]).complete(req)
    replay = ReplayAdapter([{"request": req, "response": response}]).complete(req)

    for outcome in (scripted, replay):
        assert not outcome.is_success
        assert outcome.classification == "timeout"
        assert outcome.error["code"] == "model_timeout"
        assert outcome.retryable is True


def test_capability_descriptor_is_enforced_before_adapter_script_consumption():
    req = request(reasoning_mode="enabled")
    adapter = ScriptedAdapter(
        [candidate()],
        capabilities=CapabilityDescriptor(structured_json=True, reasoning_mode=False),
    )

    outcome = ProviderNeutralModelGateway(adapter).complete(req)

    assert outcome.classification == "unsupported_capability"
    assert outcome.error["code"] == "unsupported_capability"
    assert outcome.retryable is False
    assert adapter.remaining == 1


def test_response_and_call_metadata_reject_vendor_thinking_extensions():
    req = request()
    with pytest.raises(MalformedResponseError):
        make_normalized_model_response(
            req,
            candidate=candidate(),
            extensions={"thinking": {"type": "enabled"}},
        )

    response = ScriptedAdapter([candidate()]).complete(req).response
    with pytest.raises(ContractValidationError):
        make_model_call_metadata(
            req,
            response,
            extensions={"thinking": {"type": "enabled"}},
        )


def test_known_runtime_secrets_are_blocked_or_redacted_at_contract_boundaries():
    secret = "opaque-runtime-secret-7f3a"
    with pytest.raises(MalformedRequestError) as request_error:
        make_normalized_model_request(context_text=secret, secret_values=(secret,))
    assert secret not in str(request_error.value)

    outcome = ScriptedAdapter(
        [scripted_error("provider", message=secret)],
        secret_values=(secret,),
    ).complete(request())
    serialized = redacted_json(outcome.to_dict(), secret_values=(secret,))
    assert secret not in serialized
    assert secret not in str(outcome.error)

    assert secret not in redacted_json({secret: secret}, secret_values=(secret,))
    with pytest.raises(TypeError):
        pickle.dumps(ScriptedAdapter([scripted_error("provider", message=secret)], secret_values=(secret,)))


def test_identity_keyed_replay_rejects_an_external_hash_mismatch():
    req = request()
    response = ScriptedAdapter([candidate()]).complete(req).response
    wrong_key = "f" * 64

    with pytest.raises(ReplayDataError):
        ReplayAdapter({wrong_key: {"request": req, "response": response}})


@pytest.mark.parametrize(
    "kind",
    [
        "authentication",
        "invalid_configuration",
        "invalid_request",
        "insufficient_balance",
        "unsupported_capability",
        "malformed_response",
        "cancellation",
        "late_response",
    ],
)
def test_terminal_error_maps_are_normalized_before_scripted_and_replay_output(kind):
    req = request()
    supplied = {"code": "internal_error", "message": "fixture", "retryable": True}
    if kind == "authentication":
        supplied["code"] = "authentication_error"
    elif kind == "unsupported_capability":
        supplied["code"] = "unsupported_capability"
    elif kind == "malformed_response":
        supplied["code"] = "invalid_model_response"
    elif kind == "cancellation":
        supplied["extensions"] = {"provider_neutral_classification": "cancellation"}
    elif kind == "late_response":
        supplied["extensions"] = {"provider_neutral_classification": "late_response"}
    else:
        supplied["extensions"] = {"provider_neutral_classification": kind}

    normalized = make_normalized_model_response(
        req,
        status="unsupported" if kind == "unsupported_capability" else "error",
        error=supplied,
    )
    assert normalized["error"]["retryable"] is False
    scripted = ScriptedAdapter([ScriptedStep(response=normalized)]).complete(req)
    replay = ReplayAdapter([{"request": req, "response": normalized}]).complete(req)
    for outcome in (scripted, replay):
        assert not outcome.is_success
        assert outcome.retryable is False


@pytest.mark.parametrize("kind", ["timeout", "rate_limit"])
def test_timeout_and_rate_limit_are_retryable_even_when_map_says_false(kind):
    req = request()
    response = make_normalized_model_response(
        req,
        status="error",
        error={"code": "model_timeout" if kind == "timeout" else "rate_limit", "message": "fixture", "retryable": False},
    )
    assert response["error"]["retryable"] is True
    assert ScriptedAdapter([ScriptedStep(response=response)]).complete(req).retryable is True
    assert ReplayAdapter([{"request": req, "response": response}]).complete(req).retryable is True


def test_provider_like_errors_require_explicit_transient_evidence():
    req = request()
    for kind, code in (("server", "provider_error"), ("provider", "provider_error"), ("transport", "transport_error")):
        no_evidence = normalize_error(kind, retryable=True)
        assert no_evidence["retryable"] is False
        transient = normalize_error(kind, retryable=False, transient=True)
        assert transient["retryable"] is True
        response = make_normalized_model_response(req, status="error", error=transient)
        assert response["error"]["code"] == code


def test_malformed_scripted_telemetry_is_typed_and_not_leaked():
    req = request()
    malformed_usage = {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 999}
    outcome = ScriptedAdapter([scripted_success(candidate(), usage=malformed_usage)]).complete(req)
    assert outcome.classification == "malformed_response"
    assert outcome.error["code"] == "invalid_model_response"


def test_replay_rejects_a_hash_valid_but_semantically_contradictory_response():
    req = request()
    response = make_normalized_model_response(req, status="error", error=normalize_error("timeout"))
    response["extensions"] = {"adapter_outcome": "success"}
    from pa_engine.contracts.canonical import finalize_artifact

    contradictory = finalize_artifact(response, "normalized_model_response")
    with pytest.raises(ReplayDataError):
        ReplayAdapter([{"request": req, "response": contradictory}])


def test_replay_rejects_metadata_without_response_identity():
    req = request()
    scripted = ScriptedAdapter([candidate()]).complete(req)
    metadata = dict(scripted.call_metadata)
    metadata.pop("model_response_id")
    with pytest.raises(ReplayDataError):
        ReplayAdapter([{"request": req, "response": scripted.response, "call_metadata": metadata}])


def test_generation_survives_cancellation_late_and_capability_preflight():
    req = request(reasoning_mode="enabled")
    adapter = ScriptedAdapter(
        [candidate()],
        capabilities=CapabilityDescriptor(reasoning_mode=False),
    )
    cancelled = ProviderNeutralModelGateway(adapter).complete(req, cancelled=True, generation=41)
    assert cancelled.classification == "cancellation"
    assert cancelled.generation == 41
    assert adapter.remaining == 1

    valid = ScriptedAdapter([candidate()]).complete(request(), generation=7)
    late = ScriptedAdapter([]).late_response(request(), valid.response, generation=6)
    assert valid.generation == 7
    assert late.generation == 6


def test_scripted_constructor_snapshots_nested_fixture_values():
    req = request()
    raw = candidate()
    raw["extensions"] = {"nested": {"values": [1]}}
    adapter = ScriptedAdapter([scripted_success(raw)])
    raw["status"] = "signal"
    raw["extensions"]["nested"]["values"].append(2)
    first = adapter.complete(req)
    assert first.response["candidate"]["status"] == "no_trade"
    assert first.response["candidate"]["extensions"]["nested"]["values"] == [1]
    first.response["candidate"]["status"] = "signal"


@pytest.mark.parametrize(
    "extension",
    [
        {"thinking_mode": "enabled"},
        {"enable_reasoning": True},
        {"reasoning": True},
        {"thinking": {"type": "enabled"}},
        {"thinking": {"effort": "high"}},
    ],
)
def test_all_known_vendor_reasoning_carriers_are_rejected_but_unrelated_extensions_survive(extension):
    with pytest.raises(ContractValidationError):
        make_normalized_model_request(extensions=extension)
    req = request()
    with pytest.raises(MalformedResponseError):
        make_normalized_model_response(req, candidate=candidate(), extensions=extension)
    assert make_normalized_model_request(extensions={"ui_type": "fixture"})["extensions"]["ui_type"] == "fixture"


def test_malformed_request_exception_never_retains_input_text_across_pickle():
    secret = "pickle-only-secret-42"
    with pytest.raises(MalformedRequestError) as caught:
        raise MalformedRequestError(f"request contains {secret}")
    error = caught.value
    assert secret not in error.message
    assert secret not in repr(error)
    assert secret not in repr(error.__dict__)
    assert secret not in repr(pickle.loads(pickle.dumps(error)))


def test_late_response_rejects_known_secrets_in_forwarded_metadata():
    secret = "late-metadata-secret-99"
    req = request()
    valid = ScriptedAdapter([candidate()]).complete(req)
    metadata = dict(valid.call_metadata)
    metadata["provider_request_id"] = secret
    with pytest.raises(MalformedResponseError):
        late_response_outcome(req, valid.response, call_metadata=metadata, secret_values=(secret,))


def test_response_shape_boundaries_reject_coercible_iterables_and_secret_generated_at():
    req = request()
    with pytest.raises(MalformedResponseError):
        make_normalized_model_response(req, candidate=[("status", "no_trade")])
    with pytest.raises(MalformedResponseError):
        make_normalized_model_response(req, candidate="status=no_trade")
    with pytest.raises(MalformedResponseError):
        make_normalized_model_response(req, candidate=candidate(), extensions=[("ui", "fixture")])
    with pytest.raises(MalformedResponseError):
        make_normalized_model_response(req, status="error", error=[("code", "model_timeout")])

    generated_secret = "generated-at-secret-7f3a9c2e4d8b6a1f"
    with pytest.raises(MalformedResponseError):
        make_normalized_model_response(
            req,
            candidate=candidate(),
            generated_at=generated_secret,
            secret_values=(generated_secret,),
        )
    outcome = ScriptedAdapter(
        [scripted_success(candidate(), generated_at=generated_secret)],
        secret_values=(generated_secret,),
    ).complete(req)
    assert outcome.classification == FailureClassification.MALFORMED_RESPONSE.value
    assert outcome.error["code"] == "invalid_model_response"


@pytest.mark.parametrize("capabilities", ["structured_json", b"structured_json", [("structured_json",)]])
def test_request_capabilities_and_mapping_fields_reject_non_contract_shapes(capabilities):
    with pytest.raises((MalformedRequestError, ValueError)):
        make_normalized_model_request(requested_capabilities=capabilities)
    for field in ("context_refs", "parameters", "extensions"):
        with pytest.raises((MalformedRequestError, ValueError)):
            make_normalized_model_request(**{field: [("fixture", "value")]})


@pytest.mark.parametrize("extension_key", ["reasoning_type", "reasoning-type", "reasoningType"])
def test_normalized_reasoning_type_aliases_are_rejected(extension_key):
    with pytest.raises(ContractValidationError):
        make_normalized_model_request(extensions={extension_key: "enabled"})


def test_gateway_delegates_secret_and_capability_order_to_concrete_adapter():
    secret = "gateway-request-secret-7f3a9c2e4d8b6a1f"
    req = make_normalized_model_request(context_text=secret)
    adapter = ScriptedAdapter(
        [candidate()],
        capabilities=CapabilityDescriptor(reasoning_mode=False),
        secret_values=(secret,),
    )
    with pytest.raises(MalformedRequestError):
        ProviderNeutralModelGateway(adapter).complete(req)
    assert adapter.remaining == 1


def test_runtime_resolver_clears_context_and_secret_bearing_construction_locals():
    secret = "resolver-source-secret-7f3a9c2e4d8b6a1f"

    def source(_provider):
        raise RuntimeError(secret)

    with pytest.raises(CredentialResolutionError) as caught:
        RuntimeCredentialResolver(source).require("offline")
    error = caught.value
    assert error.__context__ is None
    assert error.__cause__ is None
    assert secret not in repr(error)
    assert secret not in repr(error.__dict__)
    assert secret not in repr(pickle.loads(pickle.dumps(error)))
    assert secret not in "".join(traceback.format_exception(type(error), error, error.__traceback__))

    class BrokenCredential:
        def __deepcopy__(self, _memo):
            raise RuntimeError(secret)

    with pytest.raises(CredentialResolutionError) as caught_construction:
        RuntimeCredentialResolver({"offline": BrokenCredential()}).require("offline")
    error = caught_construction.value
    assert error.__context__ is None
    assert error.__cause__ is None
    assert secret not in repr(error)
    assert secret not in "".join(traceback.format_exception(type(error), error, error.__traceback__))


def test_runtime_credential_secret_values_include_recursive_mapping_keys():
    key_secret = "credential-key-secret-7f3a9c2e4d8b6a1f"
    value_secret = "credential-value-secret-1f6a8b4d2e9c3a7f"
    credential = RuntimeCredentialResolver({"offline": {key_secret: {"nested": value_secret}}}).require("offline")
    assert key_secret in credential.secret_values
    assert value_secret in credential.secret_values
    with pytest.raises(MalformedResponseError):
        make_normalized_model_response(
            request(),
            candidate={key_secret: "fixture"},
            secret_values=credential.secret_values,
        )
