"""Focused tests for pa_engine.storage.refs market-ref validation."""

from __future__ import annotations

import pytest

from pa_engine.contracts import ValidationResult
from pa_engine.storage.refs import (
    MARKET_REF_KINDS,
    PATH_PRECISIONS,
    is_recovery_bar_ref,
    is_second_ref,
    make_recovery_bar_ref,
    make_second_ref,
    validate_market_ref,
)

H64 = "a" * 64
H64B = "b" * 64


def second_ref(**overrides):
    ref = {
        "kind": "second_event",
        "symbol": "SPY",
        "second_event_id": "sec_" + H64,
        "second_event_hash": H64,
        "start_at": "2026-08-12T14:00:04Z",
        "end_at": "2026-08-12T14:00:05Z",
    }
    ref.update(overrides)
    return ref


def bar_ref(**overrides):
    ref = {
        "kind": "recovery_bar",
        "symbol": "SPY",
        "recovery_bar_id": "rbar_" + H64B,
        "recovery_bar_hash": H64B,
        "path_precision": "minute",
        "start_at": "2026-08-12T14:00:00Z",
        "end_at": "2026-08-12T14:01:00Z",
    }
    ref.update(overrides)
    return ref


def codes(result):
    return {issue.code for issue in result.issues}


class TestValidRefs:
    def test_second_ref_valid(self):
        result = validate_market_ref(second_ref())
        assert result.valid
        assert result.artifact_kind == "market_ref"
        assert result.issues == []

    def test_minute_recovery_bar_valid(self):
        result = validate_market_ref(bar_ref())
        assert result.valid

    def test_no_path_bar_valid(self):
        result = validate_market_ref(
            bar_ref(path_precision="no_path", start_at="2026-08-12T14:00:00Z", end_at="2026-08-12T14:03:00Z")
        )
        assert result.valid

    def test_no_path_bar_long_span_up_to_24h_valid(self):
        result = validate_market_ref(
            bar_ref(path_precision="no_path", start_at="2026-08-12T14:00:00Z", end_at="2026-08-12T16:00:00Z")
        )
        assert result.valid

    def test_fractional_second_span_valid(self):
        result = validate_market_ref(
            second_ref(start_at="2026-08-12T14:00:04.5Z", end_at="2026-08-12T14:00:05.5Z")
        )
        assert result.valid

    def test_constants(self):
        assert MARKET_REF_KINDS == ("second_event", "recovery_bar")
        assert PATH_PRECISIONS == ("minute", "no_path")


class TestFactories:
    def test_make_second_ref_from_aggregate_mapping(self):
        source = {
            "event_id": "sec_" + H64,
            "content_hash": H64,
            "symbol": "SPY",
            "start_at": "2026-08-12T14:00:04Z",
            "end_at": "2026-08-12T14:00:05Z",
        }
        ref = make_second_ref(source)
        assert ref["kind"] == "second_event"
        assert ref["second_event_id"] == "sec_" + H64
        assert ref["second_event_hash"] == H64
        assert validate_market_ref(ref).valid

    def test_make_second_ref_prefers_explicit_keys(self):
        source = {
            "second_event_id": "sec_" + H64,
            "event_id": "sec_other",
            "second_event_hash": H64,
            "event_hash": "other",
            "content_hash": "other",
            "symbol": "SPY",
            "start_at": "2026-08-12T14:00:04Z",
            "end_at": "2026-08-12T14:00:05Z",
        }
        ref = make_second_ref(source)
        assert ref["second_event_id"] == "sec_" + H64
        assert ref["second_event_hash"] == H64

    def test_make_recovery_bar_ref_from_g019_mapping(self):
        source = {
            "recovery_bar_id": "rbar_" + H64B,
            "content_hash": H64B,
            "instrument": {"symbol": "SPY"},
            "start_at": "2026-08-12T14:00:00Z",
            "end_at": "2026-08-12T14:01:00Z",
            "path_precision": "minute_bar_no_path",
        }
        ref = make_recovery_bar_ref(source)
        assert ref["kind"] == "recovery_bar"
        assert ref["symbol"] == "SPY"
        assert ref["recovery_bar_id"] == "rbar_" + H64B
        assert ref["recovery_bar_hash"] == H64B
        assert ref["path_precision"] == "minute"
        assert validate_market_ref(ref).valid

    def test_make_recovery_bar_ref_aliases(self):
        source = {
            "bar_id": "rbar_" + H64,
            "recovery_bar_hash": H64,
            "symbol": "QQQ",
            "start_at": "2026-08-12T14:00:00Z",
            "end_at": "2026-08-12T14:01:00Z",
        }
        ref = make_recovery_bar_ref(source)
        assert ref["recovery_bar_id"] == "rbar_" + H64
        assert ref["recovery_bar_hash"] == H64
        assert ref["path_precision"] == "minute"
        assert validate_market_ref(ref).valid

    def test_make_recovery_bar_ref_derives_no_path_precision(self):
        source = {
            "recovery_bar_id": "rbar_" + H64,
            "recovery_bar_hash": H64,
            "symbol": "QQQ",
            "start_at": "2026-08-12T14:00:00Z",
            "end_at": "2026-08-12T14:03:00Z",
        }
        ref = make_recovery_bar_ref(source)
        assert ref["path_precision"] == "no_path"
        assert validate_market_ref(ref).valid

    def test_factory_rejects_non_frozen_feed_and_request_lineage(self):
        with pytest.raises(ValueError):
            make_second_ref(
                {
                    "event_id": "sec_" + H64,
                    "content_hash": H64,
                    "symbol": "SPY",
                    "start_at": "2026-08-12T14:00:04Z",
                    "end_at": "2026-08-12T14:00:05Z",
                    "feed_key": "alpaca:sip:consolidated",
                }
            )
        with pytest.raises(ValueError):
            make_recovery_bar_ref(
                {
                    "recovery_bar_id": "rbar_" + H64,
                    "content_hash": H64,
                    "instrument": {"symbol": "SPY"},
                    "start_at": "2026-08-12T14:00:00Z",
                    "end_at": "2026-08-12T14:01:00Z",
                    "source_request_hash": "not-a-hash",
                }
            )

    def test_recovery_bar_id_hash_substitution_is_rejected(self):
        ref = bar_ref(recovery_bar_id="rbar_" + H64)
        result = validate_market_ref(ref)
        assert not result.valid
        assert "identity_mismatch" in codes(result)

    def test_make_second_ref_non_mapping_raises(self):
        with pytest.raises(TypeError):
            make_second_ref(["not", "a", "mapping"])

    def test_make_recovery_bar_ref_non_mapping_raises(self):
        with pytest.raises(TypeError):
            make_recovery_bar_ref(None)


class TestTamperClasses:
    def test_bad_second_hash_rejected(self):
        result = validate_market_ref(second_ref(second_event_hash="xyz"))
        assert not result.valid
        assert "invalid_second_hash" in codes(result)

    def test_bad_bar_hash_rejected(self):
        result = validate_market_ref(bar_ref(recovery_bar_hash="ABCD"))
        assert not result.valid
        assert "invalid_bar_hash" in codes(result)

    def test_second_id_with_rbar_prefix_rejected(self):
        result = validate_market_ref(second_ref(second_event_id="rbar_" + H64B))
        assert not result.valid
        assert "invalid_second_id" in codes(result)

    def test_bar_id_with_sec_prefix_rejected(self):
        result = validate_market_ref(bar_ref(recovery_bar_id="sec_" + H64))
        assert not result.valid
        assert "invalid_bar_id" in codes(result)

    def test_empty_second_id_rejected(self):
        result = validate_market_ref(second_ref(second_event_id=""))
        assert not result.valid
        assert "invalid_second_id" in codes(result)

    def test_empty_bar_id_rejected(self):
        result = validate_market_ref(bar_ref(recovery_bar_id=""))
        assert not result.valid
        assert "invalid_bar_id" in codes(result)

    def test_cross_prefix_substitution_second_ref_with_bar_id(self):
        ref = second_ref()
        ref["recovery_bar_id"] = "rbar_" + H64B
        result = validate_market_ref(ref)
        assert not result.valid

    def test_cross_prefix_substitution_bar_ref_with_second_id(self):
        ref = bar_ref()
        ref["second_event_id"] = "sec_" + H64
        result = validate_market_ref(ref)
        assert not result.valid

    def test_both_kind_ids_present_rejected(self):
        ref = second_ref()
        ref["recovery_bar_id"] = "rbar_" + H64B
        assert not validate_market_ref(ref).valid

    def test_cross_kind_reference_code(self):
        ref = second_ref()
        ref["recovery_bar_id"] = "rbar_" + H64B
        result = validate_market_ref(ref)
        assert "cross_kind_reference" in codes(result)

    def test_non_canonical_timestamp_rejected(self):
        result = validate_market_ref(second_ref(start_at="2026-08-12T14:00:04+02:00"))
        assert not result.valid
        assert "non_utc_timestamp" in codes(result)

    def test_wrong_second_span_rejected(self):
        result = validate_market_ref(
            second_ref(start_at="2026-08-12T14:00:04Z", end_at="2026-08-12T14:00:06Z")
        )
        assert not result.valid
        assert "second_interval_mismatch" in codes(result)

    def test_wrong_minute_span_rejected(self):
        result = validate_market_ref(
            bar_ref(start_at="2026-08-12T14:00:00Z", end_at="2026-08-12T14:00:30Z")
        )
        assert not result.valid
        assert "bar_interval_mismatch" in codes(result)

    def test_no_path_bar_over_24h_rejected(self):
        result = validate_market_ref(
            bar_ref(path_precision="no_path", start_at="2026-08-12T14:00:00Z", end_at="2026-08-13T15:00:00Z")
        )
        assert not result.valid
        assert "bar_interval_mismatch" in codes(result)

    def test_interval_order_rejected(self):
        result = validate_market_ref(
            second_ref(start_at="2026-08-12T14:00:05Z", end_at="2026-08-12T14:00:04Z")
        )
        assert not result.valid
        assert "interval_order" in codes(result)

    def test_zero_span_rejected(self):
        result = validate_market_ref(
            bar_ref(path_precision="no_path", start_at="2026-08-12T14:00:00Z", end_at="2026-08-12T14:00:00Z")
        )
        assert not result.valid
        assert "interval_order" in codes(result)

    def test_unknown_kind_rejected(self):
        result = validate_market_ref(second_ref(kind="candle"))
        assert not result.valid
        assert "unknown_market_ref_kind" in codes(result)

    def test_missing_kind_rejected(self):
        ref = second_ref()
        del ref["kind"]
        result = validate_market_ref(ref)
        assert not result.valid
        assert "unknown_market_ref_kind" in codes(result)

    def test_unknown_extra_key_rejected(self):
        ref = second_ref()
        ref["bogus"] = 1
        result = validate_market_ref(ref)
        assert not result.valid
        assert "invalid_market_ref" in codes(result)

    def test_missing_required_key_rejected(self):
        ref = bar_ref()
        del ref["path_precision"]
        result = validate_market_ref(ref)
        assert not result.valid
        assert "invalid_market_ref" in codes(result)

    def test_invalid_path_precision_rejected(self):
        result = validate_market_ref(bar_ref(path_precision="bogus"))
        assert not result.valid
        assert "invalid_path_precision" in codes(result)

    def test_bad_symbol_rejected(self):
        for symbol in ("spy", "SP Y", "", "spy:"):
            result = validate_market_ref(second_ref(symbol=symbol))
            assert not result.valid, symbol
            assert "invalid_symbol" in codes(result)

    def test_non_string_symbol_rejected(self):
        result = validate_market_ref(second_ref(symbol=123))
        assert not result.valid
        assert "invalid_symbol" in codes(result)


class TestExceptionSafety:
    def test_float_value_never_raises(self):
        result = validate_market_ref(second_ref(start_at=14.0))
        assert isinstance(result, ValidationResult)
        assert not result.valid
        assert "invalid_market_ref" in codes(result)

    def test_non_mapping_inputs_never_raise(self):
        for value in (None, 42, "SPY", ["a"], True, 1.5):
            result = validate_market_ref(value)
            assert isinstance(result, ValidationResult)
            assert not result.valid
            assert "invalid_market_ref" in codes(result)

    def test_non_string_dict_key_never_raises(self):
        result = validate_market_ref({1: "x"})
        assert not result.valid
        assert "invalid_market_ref" in codes(result)


class TestKindPredicates:
    def test_is_second_ref_true_for_valid_second(self):
        assert is_second_ref(second_ref()) is True

    def test_is_second_ref_false_for_valid_bar(self):
        assert is_second_ref(bar_ref()) is False

    def test_is_recovery_bar_ref_true_for_valid_bar(self):
        assert is_recovery_bar_ref(bar_ref()) is True

    def test_is_recovery_bar_ref_false_for_valid_second(self):
        assert is_recovery_bar_ref(second_ref()) is False

    def test_predicates_false_for_invalid_refs(self):
        assert is_second_ref({"kind": "second_event"}) is False
        assert is_recovery_bar_ref(None) is False
        assert is_second_ref(42) is False
