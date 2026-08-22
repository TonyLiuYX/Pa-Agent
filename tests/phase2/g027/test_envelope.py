"""Focused tests for the G027 RuntimeEventEnvelope validation and identity."""

from __future__ import annotations

import hashlib
import json

import pytest

from pa_engine.contracts import ValidationResult, canonical_json_bytes
from pa_engine.storage.envelope import (
    ENVELOPE_ARTIFACT_KIND,
    ENVELOPE_SCHEMA_VERSION,
    RuntimeEventEnvelope,
    envelope_hash,
    validate_dict,
)
from pa_engine.storage.errors import EnvelopeValidationError

H64 = "a" * 64
H64B = "b" * 64
INSTANT = "2026-08-12T14:00:00Z"

SECOND_REF = {
    "kind": "second_event",
    "symbol": "SPY",
    "second_event_id": "sec_" + H64,
    "second_event_hash": H64,
    "start_at": "2026-08-12T14:00:04Z",
    "end_at": "2026-08-12T14:00:05Z",
}
BAR_REF = {
    "kind": "recovery_bar",
    "symbol": "SPY",
    "recovery_bar_id": "rbar_" + H64,
    "recovery_bar_hash": H64,
    "path_precision": "minute",
    "start_at": "2026-08-12T14:00:00Z",
    "end_at": "2026-08-12T14:01:00Z",
}

LINEAGE = {
    "instruction_id": "sdi_" + H64,
    "instruction_hash": H64,
    "result_id": "par_" + H64,
    "result_hash": H64,
    "evidence_package_id": "evp_" + H64,
    "evidence_package_hash": H64,
    "strategy_id": "pa:rose",
}


def valid_payloads():
    return {
        "strategy_state_changed": {
            "from": "sleeping",
            "to": "analyzing",
            "reason": "timer fired",
            "resume_state": None,
        },
        "model_decision_accepted": {
            "branch": "no_entry",
            "instruction_id": "sdi_" + H64,
            "instruction_hash": H64,
            "result_id": "par_" + H64,
            "result_hash": H64,
            "evidence_package_id": "evp_" + H64,
            "evidence_package_hash": H64,
            "next_call_after_seconds": 60,
        },
        "schedule_set": {
            "timer_kind": "sleep",
            "due_at": "2026-08-12T15:00:00Z",
            "decision_accepted_at": INSTANT,
        },
        "pending_entry_created": {
            "pending_id": "pe_1",
            "episode_id": "ep_1",
            "symbol": "SPY",
            "direction": "long",
            "entry_price": "510.25",
            "stop_price": "509.00",
            "target_price": "515.00",
            "decision_accepted_at": INSTANT,
            "submitted_at": "2026-08-12T14:00:01Z",
            "expires_at": "2026-08-12T14:05:00Z",
            "resolution_at": "2026-08-12T14:05:30Z",
            "state_hash": H64B,
        },
        "pending_entry_expired": {
            "pending_id": "pe_1",
            "episode_id": "ep_1",
            "resolution_at": "2026-08-12T14:05:30Z",
        },
        "pending_entry_cancelled": {
            "pending_id": "pe_1",
            "episode_id": "ep_1",
            "resolution_at": "2026-08-12T14:05:30Z",
            "reason": "manual",
        },
        "paper_fill": {
            "trade_id": "pt_1",
            "episode_id": "ep_1",
            "symbol": "SPY",
            "direction": "long",
            "entry_price": "510.25",
            "quantity": "19.6",
            "planned_r": "1.2",
            "filled_at": "2026-08-12T14:00:05Z",
        },
        "paper_fill_rejected": {"reason": "insufficient balance"},
        "paper_close": {
            "trade_id": "pt_1",
            "episode_id": "ep_1",
            "exit_reason": "target",
            "closed_at": "2026-08-12T15:00:00Z",
            "selected_exit": "target",
            "settlement": {
                "gross_pnl": "19.60",
                "applied_pnl": "19.60",
                "net_pnl": "19.60",
                "balance_before": "1000.00",
                "balance_after": "1019.60",
            },
        },
        "account_changed": {"balance_before": "1000.00", "balance_after": "1019.60"},
        "account_exhausted": {"balance": "0.00", "reason": "balance exhausted"},
        "market_recovery": {
            "symbol": "SPY",
            "feed_key": "alpaca:iex:single_exchange",
            "recovery_cursor": INSTANT,
            "bars": [BAR_REF],
            "bar_count": 1,
        },
    }


ALL_TYPES = sorted(valid_payloads())


def envelope_dict(event_type, **overrides):
    document = {
        "schema_version": ENVELOPE_SCHEMA_VERSION,
        "artifact_kind": ENVELOPE_ARTIFACT_KIND,
        "event_type": event_type,
        "occurred_at": INSTANT,
        "mode": "automatic",
        "strategy_id": None if event_type == "market_recovery" else "pa:rose",
        "generation": None if event_type == "market_recovery" else 1,
        "profile_ref": None,
        "decision_lineage": LINEAGE if event_type == "model_decision_accepted" else None,
        "market_ref": SECOND_REF if event_type in ("paper_fill", "paper_close") else None,
        "idempotency_key": "idem_1",
        "payload": valid_payloads().get(event_type, {}),
    }
    content = {key: value for key, value in document.items()}
    content.update({key: value for key, value in overrides.items() if key not in ("event_hash", "event_id")})
    try:
        digest = envelope_hash(content)
    except Exception:
        digest = "0" * 64
    content["event_hash"] = overrides.get("event_hash", digest)
    content["event_id"] = overrides.get("event_id", "rev_" + digest)
    return content


def make_valid(event_type, **overrides):
    return RuntimeEventEnvelope.from_dict(envelope_dict(event_type, **overrides))


def codes(result):
    return {issue.code for issue in result.issues}


def make_kwargs(event_type):
    kwargs = {
        "occurred_at": INSTANT,
        "idempotency_key": "idem_1",
        "payload": valid_payloads()[event_type],
    }
    if event_type != "market_recovery":
        kwargs["strategy_id"] = "pa:rose"
        kwargs["generation"] = 1
    if event_type == "model_decision_accepted":
        kwargs["decision_lineage"] = LINEAGE
    if event_type in ("paper_fill", "paper_close"):
        kwargs["market_ref"] = SECOND_REF
    return kwargs


class TestIdentity:
    def test_make_computes_hash_and_id(self):
        envelope = RuntimeEventEnvelope.make("schedule_set", **make_kwargs("schedule_set"))
        assert len(envelope.event_hash) == 64
        assert all(character in "0123456789abcdef" for character in envelope.event_hash)
        assert envelope.event_id == "rev_" + envelope.event_hash
        assert envelope.event_hash == envelope_hash(envelope.to_dict())

    def test_event_hash_is_sha256_of_canonical_projection(self):
        envelope = make_valid("schedule_set")
        document = envelope.to_dict()
        projection = {
            "schema_version": ENVELOPE_SCHEMA_VERSION,
            "artifact_kind": ENVELOPE_ARTIFACT_KIND,
            "event_type": "schedule_set",
            "occurred_at": INSTANT,
            "mode": "automatic",
            "strategy_id": "pa:rose",
            "generation": 1,
            "profile_ref": None,
            "decision_lineage": None,
            "market_ref": None,
            "idempotency_key": "idem_1",
            "payload": document["payload"],
        }
        expected = hashlib.sha256(canonical_json_bytes(projection)).hexdigest()
        assert envelope.event_hash == expected

    def test_remake_from_identical_input_is_byte_identical(self):
        first = make_valid("schedule_set")
        second = make_valid("schedule_set")
        assert first == second
        assert first.event_hash == second.event_hash
        assert json.dumps(first.to_dict(), sort_keys=True) == json.dumps(second.to_dict(), sort_keys=True)

    def test_make_and_from_dict_agree(self):
        envelope = RuntimeEventEnvelope.make("schedule_set", **make_kwargs("schedule_set"))
        reloaded = RuntimeEventEnvelope.from_dict(envelope.to_dict())
        assert reloaded == envelope
        assert reloaded.event_id == envelope.event_id

    def test_content_change_changes_hash(self):
        first = make_valid("schedule_set")
        second = make_valid("schedule_set", occurred_at="2026-08-12T14:00:01Z")
        assert first.event_hash != second.event_hash

    def test_from_dict_to_dict_round_trip_equal(self):
        envelope = make_valid("paper_close")
        reloaded = RuntimeEventEnvelope.from_dict(envelope.to_dict())
        assert reloaded == envelope
        assert reloaded.to_dict() == envelope.to_dict()

    def test_to_dict_is_json_native(self):
        envelope = make_valid("paper_close")
        assert json.loads(json.dumps(envelope.to_dict())) == envelope.to_dict()


class TestPerTypeValid:
    @pytest.mark.parametrize("event_type", ALL_TYPES)
    def test_valid_for_all_types(self, event_type):
        result = validate_dict(envelope_dict(event_type))
        assert result.valid, [issue.code for issue in result.issues]

    @pytest.mark.parametrize("event_type", ALL_TYPES)
    def test_round_trip_for_all_types(self, event_type):
        envelope = make_valid(event_type)
        reloaded = RuntimeEventEnvelope.from_dict(envelope.to_dict())
        assert reloaded == envelope
        assert reloaded.event_hash == envelope.event_hash

    @pytest.mark.parametrize("event_type", ALL_TYPES)
    def test_make_for_all_types(self, event_type):
        envelope = RuntimeEventEnvelope.make(event_type, **make_kwargs(event_type))
        assert envelope.event_id == "rev_" + envelope.event_hash
        assert envelope.event_type == event_type

    def test_reason_field_is_not_blocked(self):
        result = validate_dict(envelope_dict("strategy_state_changed"))
        assert result.valid
        assert "reasoning_field" not in codes(result)
        assert "secret_field" not in codes(result)

    def test_wait_for_price_branch_valid(self):
        payload = valid_payloads()["model_decision_accepted"]
        del payload["next_call_after_seconds"]
        payload["branch"] = "wait_for_price"
        payload["entry_wait_seconds"] = 120
        payload["entry_price"] = "510.25"
        result = validate_dict(envelope_dict("model_decision_accepted", payload=payload))
        assert result.valid

    def test_enter_immediately_branch_valid(self):
        payload = valid_payloads()["model_decision_accepted"]
        del payload["next_call_after_seconds"]
        payload["branch"] = "enter_immediately"
        result = validate_dict(envelope_dict("model_decision_accepted", payload=payload))
        assert result.valid


class TestPayloadKeySets:
    MISSING_KEY_BY_TYPE = {
        "strategy_state_changed": "from",
        "model_decision_accepted": "branch",
        "schedule_set": "due_at",
        "pending_entry_created": "symbol",
        "pending_entry_expired": "pending_id",
        "pending_entry_cancelled": "reason",
        "paper_fill": "quantity",
        "paper_fill_rejected": "reason",
        "paper_close": "trade_id",
        "account_changed": "balance_after",
        "account_exhausted": "balance",
        "market_recovery": "bars",
    }

    @pytest.mark.parametrize("event_type", ALL_TYPES)
    def test_missing_required_field_rejected(self, event_type):
        payload = valid_payloads()[event_type]
        del payload[TestPayloadKeySets.MISSING_KEY_BY_TYPE[event_type]]
        result = validate_dict(envelope_dict(event_type, payload=payload))
        assert not result.valid
        assert "missing_field" in codes(result)

    @pytest.mark.parametrize("event_type", ALL_TYPES)
    def test_extra_field_rejected(self, event_type):
        payload = valid_payloads()[event_type]
        payload["bogus"] = 1
        result = validate_dict(envelope_dict(event_type, payload=payload))
        assert not result.valid
        assert "unknown_field" in codes(result)


class TestIdentityTamper:
    def test_hash_tamper_rejected(self):
        document = envelope_dict("schedule_set", event_hash="0" * 64)
        result = validate_dict(document)
        assert not result.valid
        assert "identity_mismatch" in codes(result)
        with pytest.raises(EnvelopeValidationError):
            RuntimeEventEnvelope.from_dict(document)

    def test_event_id_tamper_rejected(self):
        document = envelope_dict("schedule_set", event_id="rev_" + "0" * 64)
        result = validate_dict(document)
        assert not result.valid
        assert "identity_mismatch" in codes(result)

    def test_payload_change_without_rehash_rejected(self):
        document = envelope_dict("schedule_set")
        document["payload"]["timer_kind"] = "retry"
        result = validate_dict(document)
        assert not result.valid
        assert "identity_mismatch" in codes(result)

    def test_identity_is_stable_after_remake(self):
        document = envelope_dict("schedule_set")
        first = RuntimeEventEnvelope.from_dict(document)
        second = RuntimeEventEnvelope.from_dict(document)
        assert first.event_hash == second.event_hash == document["event_hash"]


class TestSecretAndReasoningScan:
    def test_secret_key_in_payload_rejected(self):
        payload = valid_payloads()["schedule_set"]
        payload["api_key"] = "sk-live-123"
        result = validate_dict(envelope_dict("schedule_set", payload=payload))
        assert not result.valid
        assert "secret_field" in codes(result)

    def test_secret_key_with_token_suffix_rejected(self):
        payload = valid_payloads()["schedule_set"]
        payload["access_token"] = "token-1"
        result = validate_dict(envelope_dict("schedule_set", payload=payload))
        assert not result.valid
        assert "secret_field" in codes(result)

    def test_secret_key_nested_in_settlement_rejected(self):
        payload = valid_payloads()["paper_close"]
        payload["settlement"]["api_key"] = "sk-1"
        result = validate_dict(envelope_dict("paper_close", payload=payload))
        assert not result.valid
        assert "secret_field" in codes(result)

    def test_secret_key_in_lineage_rejected(self):
        lineage = dict(LINEAGE)
        lineage["authorization"] = "Bearer x"
        result = validate_dict(envelope_dict("model_decision_accepted", decision_lineage=lineage))
        assert not result.valid
        assert "secret_field" in codes(result)

    def test_prompt_tokens_is_safe(self):
        payload = valid_payloads()["schedule_set"]
        payload["prompt_tokens"] = 12
        result = validate_dict(envelope_dict("schedule_set", payload=payload))
        assert not result.valid
        assert "secret_field" not in codes(result)
        assert "unknown_field" in codes(result)

    @pytest.mark.parametrize("name", ["reasoning_content", "thinking", "chain_of_thought", "inner_monologue"])
    def test_reasoning_key_rejected(self, name):
        payload = valid_payloads()["schedule_set"]
        payload[name] = "hidden chain of thought"
        result = validate_dict(envelope_dict("schedule_set", payload=payload))
        assert not result.valid
        assert "reasoning_field" in codes(result)

    def test_cot_key_rejected(self):
        payload = valid_payloads()["schedule_set"]
        payload["cot"] = "hidden"
        result = validate_dict(envelope_dict("schedule_set", payload=payload))
        assert not result.valid
        assert "reasoning_field" in codes(result)

    @pytest.mark.parametrize("name", ["broker_order_id", "real_order", "broker_secret", "broker_credential"])
    def test_broker_key_rejected(self, name):
        payload = valid_payloads()["schedule_set"]
        payload[name] = "order-1"
        result = validate_dict(envelope_dict("schedule_set", payload=payload))
        assert not result.valid
        assert "broker_field" in codes(result)

    def test_reason_vocabulary_does_not_block_reason_field(self):
        payload = valid_payloads()["strategy_state_changed"]
        result = validate_dict(envelope_dict("strategy_state_changed", payload=payload))
        assert result.valid
        assert "reasoning_field" not in codes(result)

    def test_secret_scan_covers_profile_ref(self):
        profile_ref = {"profile_ref_id": "spr_" + H64, "profile_ref_hash": H64, "secret": "x"}
        result = validate_dict(envelope_dict("schedule_set", profile_ref=profile_ref))
        assert not result.valid
        assert "secret_field" in codes(result)

    @pytest.mark.parametrize("name", ["provider_raw", "provider_raw_payload", "raw_provider_response"])
    def test_provider_raw_payload_is_rejected_recursively(self, name):
        payload = valid_payloads()["schedule_set"]
        payload["nested"] = {name: {"response": "opaque"}}
        result = validate_dict(envelope_dict("schedule_set", payload=payload))
        assert not result.valid
        assert "provider_raw_field" in codes(result)


class TestJsonNativeScan:
    def test_float_value_rejected(self):
        payload = valid_payloads()["schedule_set"]
        payload["due_at"] = 14.0
        result = validate_dict(envelope_dict("schedule_set", payload=payload))
        assert not result.valid
        assert "invalid_json" in codes(result)

    def test_float_occurred_at_rejected(self):
        result = validate_dict(envelope_dict("schedule_set", occurred_at=14.0))
        assert not result.valid
        assert "invalid_json" in codes(result)

    def test_non_string_dict_key_rejected(self):
        payload = valid_payloads()["schedule_set"]
        payload[1] = "x"
        result = validate_dict(envelope_dict("schedule_set", payload=payload))
        assert not result.valid
        assert "invalid_json" in codes(result)

    def test_nested_non_json_value_rejected(self):
        payload = valid_payloads()["paper_close"]
        payload["settlement"]["gross_pnl"] = 12.5
        result = validate_dict(envelope_dict("paper_close", payload=payload))
        assert not result.valid
        assert "invalid_json" in codes(result)


class TestEnvelopeFields:
    def test_occurred_at_non_canonical_rejected(self):
        result = validate_dict(envelope_dict("schedule_set", occurred_at="2026-08-12T14:00:00+02:00"))
        assert not result.valid
        assert "non_utc_timestamp" in codes(result)

    def test_mode_vocabulary_rejected(self):
        result = validate_dict(envelope_dict("schedule_set", mode="forward_paper"))
        assert not result.valid
        assert "invalid_mode" in codes(result)

    def test_replay_mode_accepted(self):
        result = validate_dict(envelope_dict("schedule_set", mode="replay"))
        assert result.valid

    def test_unknown_event_type_rejected(self):
        result = validate_dict(
            envelope_dict(
                "bogus",
                payload={"timer_kind": "sleep", "due_at": "2026-08-12T15:00:00Z", "decision_accepted_at": INSTANT},
            )
        )
        assert not result.valid
        assert "unknown_event_type" in codes(result)

    def test_version_mismatch_rejected(self):
        result = validate_dict(envelope_dict("schedule_set", schema_version="0.2.0"))
        assert not result.valid
        assert "version_mismatch" in codes(result)

    def test_artifact_kind_mismatch_rejected(self):
        result = validate_dict(envelope_dict("schedule_set", artifact_kind="other"))
        assert not result.valid
        assert "artifact_kind_mismatch" in codes(result)

    def test_empty_idempotency_key_rejected(self):
        result = validate_dict(envelope_dict("schedule_set", idempotency_key=""))
        assert not result.valid
        assert "invalid_idempotency_key" in codes(result)

    def test_non_string_idempotency_key_rejected(self):
        result = validate_dict(envelope_dict("schedule_set", idempotency_key=7))
        assert not result.valid
        assert "invalid_idempotency_key" in codes(result)

    def test_missing_envelope_field_rejected(self):
        document = envelope_dict("schedule_set")
        del document["idempotency_key"]
        result = validate_dict(document)
        assert not result.valid
        assert "missing_field" in codes(result)

    def test_unknown_envelope_field_rejected(self):
        document = envelope_dict("schedule_set")
        document["extra"] = 1
        result = validate_dict(document)
        assert not result.valid
        assert "unknown_field" in codes(result)


class TestStrategyAndGeneration:
    def test_strategy_id_required_for_schedule_set(self):
        result = validate_dict(envelope_dict("schedule_set", strategy_id=None))
        assert not result.valid
        assert "invalid_strategy_id" in codes(result)

    def test_empty_strategy_id_rejected(self):
        result = validate_dict(envelope_dict("schedule_set", strategy_id=""))
        assert not result.valid
        assert "invalid_strategy_id" in codes(result)

    def test_non_string_strategy_id_rejected(self):
        result = validate_dict(envelope_dict("schedule_set", strategy_id=123))
        assert not result.valid
        assert "invalid_strategy_id" in codes(result)

    def test_market_recovery_strategy_id_must_be_none(self):
        result = validate_dict(envelope_dict("market_recovery", strategy_id="pa:rose"))
        assert not result.valid
        assert "invalid_strategy_id" in codes(result)

    def test_generation_required_for_schedule_set(self):
        result = validate_dict(envelope_dict("schedule_set", generation=None))
        assert not result.valid
        assert "invalid_generation" in codes(result)

    def test_negative_generation_rejected(self):
        result = validate_dict(envelope_dict("schedule_set", generation=-1))
        assert not result.valid
        assert "invalid_generation" in codes(result)

    def test_bool_generation_rejected(self):
        result = validate_dict(envelope_dict("schedule_set", generation=True))
        assert not result.valid
        assert "invalid_generation" in codes(result)

    def test_market_recovery_generation_must_be_none(self):
        result = validate_dict(envelope_dict("market_recovery", generation=1))
        assert not result.valid
        assert "invalid_generation" in codes(result)


class TestProfileRef:
    def test_valid_profile_ref_accepted(self):
        profile_ref = {"profile_ref_id": "spr_" + H64, "profile_ref_hash": H64}
        result = validate_dict(envelope_dict("schedule_set", profile_ref=profile_ref))
        assert result.valid

    def test_profile_ref_with_matching_strategy_id_accepted(self):
        profile_ref = {"profile_ref_id": "spr_" + H64, "profile_ref_hash": H64, "strategy_id": "pa:rose"}
        result = validate_dict(envelope_dict("schedule_set", profile_ref=profile_ref))
        assert result.valid

    def test_profile_ref_bad_hash_rejected(self):
        profile_ref = {"profile_ref_id": "spr_" + H64, "profile_ref_hash": "xyz"}
        result = validate_dict(envelope_dict("schedule_set", profile_ref=profile_ref))
        assert not result.valid
        assert "invalid_hash" in codes(result)

    def test_profile_ref_id_not_bound_to_hash_rejected(self):
        profile_ref = {"profile_ref_id": "spr_" + H64B, "profile_ref_hash": H64}
        result = validate_dict(envelope_dict("schedule_set", profile_ref=profile_ref))
        assert not result.valid
        assert "invalid_profile_ref" in codes(result)

    def test_profile_ref_extra_key_rejected(self):
        profile_ref = {"profile_ref_id": "spr_" + H64, "profile_ref_hash": H64, "bogus": 1}
        result = validate_dict(envelope_dict("schedule_set", profile_ref=profile_ref))
        assert not result.valid
        assert "invalid_profile_ref" in codes(result)

    def test_profile_ref_missing_key_rejected(self):
        profile_ref = {"profile_ref_hash": H64}
        result = validate_dict(envelope_dict("schedule_set", profile_ref=profile_ref))
        assert not result.valid
        assert "invalid_profile_ref" in codes(result)

    def test_profile_ref_non_mapping_rejected(self):
        result = validate_dict(envelope_dict("schedule_set", profile_ref="spr_x"))
        assert not result.valid
        assert "invalid_profile_ref" in codes(result)

    def test_profile_ref_cross_strategy_rejected(self):
        profile_ref = {"profile_ref_id": "spr_" + H64, "profile_ref_hash": H64, "strategy_id": "pa:other"}
        result = validate_dict(envelope_dict("schedule_set", profile_ref=profile_ref))
        assert not result.valid
        assert "cross_strategy_profile" in codes(result)


class TestDecisionLineage:
    def test_valid_decision_lineage_accepted(self):
        lineage = {"instruction_id": "sdi_" + H64, "instruction_hash": H64}
        result = validate_dict(envelope_dict("schedule_set", decision_lineage=lineage))
        assert result.valid

    def test_decision_lineage_missing_instruction_id_rejected(self):
        lineage = {"instruction_hash": H64}
        result = validate_dict(envelope_dict("schedule_set", decision_lineage=lineage))
        assert not result.valid
        assert "invalid_decision_lineage" in codes(result)

    def test_decision_lineage_bad_hash_rejected(self):
        lineage = {"instruction_id": "sdi_" + H64, "instruction_hash": "xyz"}
        result = validate_dict(envelope_dict("schedule_set", decision_lineage=lineage))
        assert not result.valid
        assert "invalid_hash" in codes(result)

    def test_decision_lineage_id_not_bound_rejected(self):
        lineage = {"instruction_id": "sdi_" + H64B, "instruction_hash": H64}
        result = validate_dict(envelope_dict("schedule_set", decision_lineage=lineage))
        assert not result.valid
        assert "invalid_decision_lineage" in codes(result)

    def test_decision_lineage_non_mapping_rejected(self):
        result = validate_dict(envelope_dict("schedule_set", decision_lineage="sdi_x"))
        assert not result.valid
        assert "invalid_decision_lineage" in codes(result)

    def test_decision_lineage_cross_strategy_rejected(self):
        lineage = {
            "instruction_id": "sdi_" + H64,
            "instruction_hash": H64,
            "strategy_id": "pa:other",
        }
        result = validate_dict(envelope_dict("schedule_set", decision_lineage=lineage))
        assert not result.valid
        assert "cross_strategy_decision" in codes(result)

    def test_model_decision_requires_lineage(self):
        result = validate_dict(envelope_dict("model_decision_accepted", decision_lineage=None))
        assert not result.valid
        assert "missing_field" in codes(result)

    def test_model_decision_lineage_payload_mismatch_rejected(self):
        payload = valid_payloads()["model_decision_accepted"]
        payload["result_hash"] = H64B
        result = validate_dict(envelope_dict("model_decision_accepted", payload=payload))
        assert not result.valid
        assert "lineage_mismatch" in codes(result)

    def test_model_decision_lineage_missing_field_rejected(self):
        lineage = dict(LINEAGE)
        del lineage["result_hash"]
        result = validate_dict(envelope_dict("model_decision_accepted", decision_lineage=lineage))
        assert not result.valid
        assert "lineage_mismatch" in codes(result)

    def test_model_decision_payload_instruction_id_binding(self):
        payload = valid_payloads()["model_decision_accepted"]
        payload["instruction_id"] = "sdi_" + H64B
        result = validate_dict(envelope_dict("model_decision_accepted", payload=payload))
        assert not result.valid
        assert "invalid_instruction_id" in codes(result)


class TestModelDecisionRanges:
    @pytest.mark.parametrize("value", [0, 86401, -5, "60", True])
    def test_no_entry_next_call_out_of_range(self, value):
        payload = valid_payloads()["model_decision_accepted"]
        payload["next_call_after_seconds"] = value
        result = validate_dict(envelope_dict("model_decision_accepted", payload=payload))
        assert not result.valid
        assert "invalid_seconds" in codes(result)

    def test_no_entry_carries_wait_price_rejected(self):
        payload = valid_payloads()["model_decision_accepted"]
        payload["entry_price"] = "510.25"
        result = validate_dict(envelope_dict("model_decision_accepted", payload=payload))
        assert not result.valid
        assert "branch_field_mismatch" in codes(result)

    @pytest.mark.parametrize("value", [0, 86401, -5, "120"])
    def test_wait_for_price_x_out_of_range(self, value):
        payload = valid_payloads()["model_decision_accepted"]
        payload["branch"] = "wait_for_price"
        payload["entry_wait_seconds"] = value
        payload["entry_price"] = "510.25"
        result = validate_dict(envelope_dict("model_decision_accepted", payload=payload))
        assert not result.valid
        assert "invalid_seconds" in codes(result)

    @pytest.mark.parametrize("price", ["abc", "-5.0", "0.00"])
    def test_wait_for_price_bad_entry_price(self, price):
        payload = valid_payloads()["model_decision_accepted"]
        payload["branch"] = "wait_for_price"
        payload["entry_wait_seconds"] = 120
        payload["entry_price"] = price
        result = validate_dict(envelope_dict("model_decision_accepted", payload=payload))
        assert not result.valid
        assert "invalid_decimal" in codes(result)

    def test_enter_immediately_with_timer_fields_rejected(self):
        payload = valid_payloads()["model_decision_accepted"]
        payload["branch"] = "enter_immediately"
        payload["next_call_after_seconds"] = 60
        result = validate_dict(envelope_dict("model_decision_accepted", payload=payload))
        assert not result.valid
        assert "branch_field_mismatch" in codes(result)

    def test_unknown_branch_rejected(self):
        payload = valid_payloads()["model_decision_accepted"]
        payload["branch"] = "bogus"
        result = validate_dict(envelope_dict("model_decision_accepted", payload=payload))
        assert not result.valid
        assert "invalid_branch" in codes(result)


class TestMarketRecovery:
    def test_market_recovery_with_market_ref_rejected(self):
        result = validate_dict(envelope_dict("market_recovery", market_ref=SECOND_REF))
        assert not result.valid
        assert "invalid_market_ref" in codes(result)

    def test_market_recovery_bad_feed_key_rejected(self):
        result = validate_dict(envelope_dict("market_recovery", payload=dict(valid_payloads()["market_recovery"], feed_key="other")))
        assert not result.valid
        assert "invalid_feed_key" in codes(result)

    def test_market_recovery_bad_cursor_rejected(self):
        result = validate_dict(
            envelope_dict("market_recovery", payload=dict(valid_payloads()["market_recovery"], recovery_cursor="2026-08-12"))
        )
        assert not result.valid
        assert "non_utc_timestamp" in codes(result)

    def test_market_recovery_bars_failing_ref_validation_rejected(self):
        payload = dict(valid_payloads()["market_recovery"], bars=[BAR_REF, {"kind": "bogus"}], bar_count=2)
        result = validate_dict(envelope_dict("market_recovery", payload=payload))
        assert not result.valid
        assert "unknown_market_ref_kind" in codes(result)

    def test_market_recovery_bar_count_mismatch_rejected(self):
        result = validate_dict(
            envelope_dict("market_recovery", payload=dict(valid_payloads()["market_recovery"], bar_count=2))
        )
        assert not result.valid
        assert "invalid_bar_count" in codes(result)

    def test_market_recovery_bars_not_a_list_rejected(self):
        payload = dict(valid_payloads()["market_recovery"], bars=BAR_REF, bar_count=1)
        result = validate_dict(envelope_dict("market_recovery", payload=payload))
        assert not result.valid
        assert "invalid_payload" in codes(result)


class TestMarketRefCrossChecks:
    def test_paper_fill_requires_market_ref(self):
        result = validate_dict(envelope_dict("paper_fill", market_ref=None))
        assert not result.valid
        assert "missing_field" in codes(result)

    def test_paper_close_requires_market_ref(self):
        result = validate_dict(envelope_dict("paper_close", market_ref=None))
        assert not result.valid
        assert "missing_field" in codes(result)

    def test_paper_fill_with_recovery_bar_ref_accepted(self):
        result = validate_dict(envelope_dict("paper_fill", market_ref=BAR_REF))
        assert result.valid

    def test_paper_fill_with_invalid_market_ref_rejected(self):
        result = validate_dict(envelope_dict("paper_fill", market_ref={"kind": "bogus"}))
        assert not result.valid
        assert "unknown_market_ref_kind" in codes(result)

    def test_cross_symbol_market_ref_rejected(self):
        ref = dict(SECOND_REF, symbol="QQQ")
        result = validate_dict(envelope_dict("paper_fill", market_ref=ref))
        assert not result.valid
        assert "cross_symbol_market_ref" in codes(result)

    def test_matching_symbol_market_ref_accepted(self):
        result = validate_dict(envelope_dict("paper_fill", market_ref=SECOND_REF))
        assert result.valid


class TestPayloadValueChecks:
    def test_account_changed_equal_balances_rejected(self):
        payload = {"balance_before": "1000.00", "balance_after": "1000.00"}
        result = validate_dict(envelope_dict("account_changed", payload=payload))
        assert not result.valid
        assert "balance_unchanged" in codes(result)

    def test_account_changed_negative_balance_rejected(self):
        payload = {"balance_before": "1000.00", "balance_after": "-5.00"}
        result = validate_dict(envelope_dict("account_changed", payload=payload))
        assert not result.valid
        assert "invalid_decimal" in codes(result)

    def test_account_changed_bad_decimal_rejected(self):
        payload = {"balance_before": "abc", "balance_after": "1019.60"}
        result = validate_dict(envelope_dict("account_changed", payload=payload))
        assert not result.valid
        assert "invalid_decimal" in codes(result)

    def test_account_exhausted_bad_balance_rejected(self):
        payload = {"balance": "-1.00", "reason": "exhausted"}
        result = validate_dict(envelope_dict("account_exhausted", payload=payload))
        assert not result.valid
        assert "invalid_decimal" in codes(result)

    def test_schedule_set_instant_order_rejected(self):
        payload = {
            "timer_kind": "sleep",
            "due_at": "2026-08-12T13:00:00Z",
            "decision_accepted_at": INSTANT,
        }
        result = validate_dict(envelope_dict("schedule_set", payload=payload))
        assert not result.valid
        assert "instant_order" in codes(result)

    def test_schedule_set_bad_timer_kind_rejected(self):
        payload = {"timer_kind": "bogus", "due_at": "2026-08-12T15:00:00Z", "decision_accepted_at": INSTANT}
        result = validate_dict(envelope_dict("schedule_set", payload=payload))
        assert not result.valid
        assert "invalid_timer_kind" in codes(result)

    def test_pending_entry_created_instant_order_rejected(self):
        payload = dict(valid_payloads()["pending_entry_created"])
        payload["resolution_at"] = "2026-08-12T14:00:00Z"
        result = validate_dict(envelope_dict("pending_entry_created", payload=payload))
        assert not result.valid
        assert "instant_order" in codes(result)

    def test_pending_entry_created_bad_direction_rejected(self):
        payload = dict(valid_payloads()["pending_entry_created"], direction="up")
        result = validate_dict(envelope_dict("pending_entry_created", payload=payload))
        assert not result.valid
        assert "invalid_direction" in codes(result)

    def test_pending_entry_created_bad_state_hash_rejected(self):
        payload = dict(valid_payloads()["pending_entry_created"], state_hash="xyz")
        result = validate_dict(envelope_dict("pending_entry_created", payload=payload))
        assert not result.valid
        assert "invalid_hash" in codes(result)

    def test_state_unchanged_rejected(self):
        payload = {"from": "sleeping", "to": "sleeping", "reason": "noop", "resume_state": None}
        result = validate_dict(envelope_dict("strategy_state_changed", payload=payload))
        assert not result.valid
        assert "state_unchanged" in codes(result)

    def test_state_out_of_vocabulary_rejected(self):
        payload = {"from": "sleeping", "to": "flying", "reason": "x", "resume_state": None}
        result = validate_dict(envelope_dict("strategy_state_changed", payload=payload))
        assert not result.valid
        assert "invalid_state" in codes(result)

    def test_resume_state_out_of_vocabulary_rejected(self):
        payload = {"from": "sleeping", "to": "analyzing", "reason": "x", "resume_state": "flying"}
        result = validate_dict(envelope_dict("strategy_state_changed", payload=payload))
        assert not result.valid
        assert "invalid_state" in codes(result)

    def test_empty_reason_rejected(self):
        payload = {"from": "sleeping", "to": "analyzing", "reason": "", "resume_state": None}
        result = validate_dict(envelope_dict("strategy_state_changed", payload=payload))
        assert not result.valid
        assert "invalid_reason" in codes(result)

    def test_paper_fill_bad_quantity_rejected(self):
        payload = dict(valid_payloads()["paper_fill"], quantity="abc")
        result = validate_dict(envelope_dict("paper_fill", payload=payload))
        assert not result.valid
        assert "invalid_decimal" in codes(result)

    def test_paper_fill_bad_symbol_rejected(self):
        payload = dict(valid_payloads()["paper_fill"], symbol="spy")
        result = validate_dict(envelope_dict("paper_fill", payload=payload))
        assert not result.valid
        assert "invalid_symbol" in codes(result)

    def test_paper_close_bad_settlement_decimal_rejected(self):
        payload = dict(valid_payloads()["paper_close"])
        payload["settlement"] = {"gross_pnl": "abc"}
        result = validate_dict(envelope_dict("paper_close", payload=payload))
        assert not result.valid
        assert "invalid_decimal" in codes(result)

    def test_paper_close_non_mapping_settlement_rejected(self):
        payload = dict(valid_payloads()["paper_close"], settlement="19.60")
        result = validate_dict(envelope_dict("paper_close", payload=payload))
        assert not result.valid
        assert "invalid_payload" in codes(result)

    def test_paper_fill_with_pending_id_accepted(self):
        payload = dict(valid_payloads()["paper_fill"], pending_id="pe_1")
        result = validate_dict(envelope_dict("paper_fill", payload=payload))
        assert result.valid


class TestExceptionSafety:
    @pytest.mark.parametrize("value", [None, 42, "x", [1], 1.5, {"a": 1}, True])
    def test_validate_dict_never_raises(self, value):
        result = validate_dict(value)
        assert isinstance(result, ValidationResult)
        assert result.valid is False

    def test_make_raises_envelope_validation_error(self):
        with pytest.raises(EnvelopeValidationError):
            RuntimeEventEnvelope.make(
                "schedule_set",
                strategy_id="pa:rose",
                generation=1,
                occurred_at="not-an-instant",
                idempotency_key="idem_1",
                payload={"timer_kind": "sleep", "due_at": "2026-08-12T15:00:00Z", "decision_accepted_at": INSTANT},
            )

    def test_make_rejects_secret_payload(self):
        payload = dict(valid_payloads()["schedule_set"], api_key="sk-1")
        kwargs = make_kwargs("schedule_set")
        kwargs["payload"] = payload
        with pytest.raises(EnvelopeValidationError):
            RuntimeEventEnvelope.make("schedule_set", **kwargs)

    def test_from_dict_raises_on_invalid(self):
        with pytest.raises(EnvelopeValidationError):
            RuntimeEventEnvelope.from_dict(envelope_dict("schedule_set", occurred_at="bad"))

    def test_from_dict_raises_on_tampered_hash(self):
        with pytest.raises(EnvelopeValidationError):
            RuntimeEventEnvelope.from_dict(envelope_dict("schedule_set", event_hash="0" * 64))

    def test_make_never_accepts_float_payload(self):
        payload = {"timer_kind": "sleep", "due_at": 14.0, "decision_accepted_at": INSTANT}
        with pytest.raises(EnvelopeValidationError):
            RuntimeEventEnvelope.make(
                "schedule_set",
                strategy_id="pa:rose",
                generation=1,
                occurred_at=INSTANT,
                idempotency_key="idem_1",
                payload=payload,
            )


class TestImmutability:
    def test_payload_is_frozen(self):
        envelope = make_valid("schedule_set")
        with pytest.raises(TypeError):
            envelope.payload["timer_kind"] = "retry"

    def test_payload_nested_lists_are_frozen(self):
        envelope = make_valid("market_recovery")
        with pytest.raises(TypeError):
            envelope.payload["bars"][0]["symbol"] = "QQQ"

    def test_market_ref_is_frozen(self):
        envelope = make_valid("paper_fill")
        with pytest.raises(TypeError):
            envelope.market_ref["symbol"] = "QQQ"

    def test_envelope_is_frozen(self):
        envelope = make_valid("schedule_set")
        with pytest.raises(Exception):
            envelope.event_type = "paper_fill"
