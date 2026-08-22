"""Focused G028 field-level model-boundary denylist tests."""

from __future__ import annotations

import pytest

from pa_engine.runtime.boundary import (
    RULE_ACCOUNT_FIELD,
    RULE_FORBIDDEN_MODEL_FIELD,
    RULE_SECRET_FIELD,
    ModelBoundaryError,
    assert_model_boundary_safe,
    profile_ref_intact,
    scan_model_boundary,
)


def test_issue_at_depth_three_is_found_with_full_path():
    value = {"a": {"b": {"c": {"balance": "1"}}}}
    issues = scan_model_boundary(value)
    assert len(issues) == 1
    assert issues[0].path == "$.a.b.c.balance"
    assert issues[0].rule == RULE_FORBIDDEN_MODEL_FIELD
    assert issues[0].key == "balance"


def test_list_and_tuple_index_paths_are_walked():
    issues = scan_model_boundary({"items": [{"quantity": "1"}, {"pnl": "2"}], "more": ({"equity": "3"},)})
    paths = {issue.path: issue.rule for issue in issues}
    assert paths == {
        "$.items[0].quantity": RULE_FORBIDDEN_MODEL_FIELD,
        "$.items[1].pnl": RULE_FORBIDDEN_MODEL_FIELD,
        "$.more[0].equity": RULE_ACCOUNT_FIELD,
    }


@pytest.mark.parametrize(
    "key,rule",
    [
        ("balance", RULE_FORBIDDEN_MODEL_FIELD),
        ("accountBalance", RULE_FORBIDDEN_MODEL_FIELD),
        ("account_balance", RULE_FORBIDDEN_MODEL_FIELD),
        ("positionSize", RULE_FORBIDDEN_MODEL_FIELD),
        ("position_size", RULE_FORBIDDEN_MODEL_FIELD),
        ("positionValue", RULE_FORBIDDEN_MODEL_FIELD),
        ("P&L", RULE_FORBIDDEN_MODEL_FIELD),
        ("p-l", RULE_FORBIDDEN_MODEL_FIELD),
        ("available_funds", RULE_FORBIDDEN_MODEL_FIELD),
        ("paperAccount", RULE_FORBIDDEN_MODEL_FIELD),
        ("appliedPnl", RULE_FORBIDDEN_MODEL_FIELD),
        ("profit_loss", RULE_FORBIDDEN_MODEL_FIELD),
        ("quantity", RULE_FORBIDDEN_MODEL_FIELD),
        ("notional", RULE_FORBIDDEN_MODEL_FIELD),
        ("pnl", RULE_FORBIDDEN_MODEL_FIELD),
        ("profit", RULE_FORBIDDEN_MODEL_FIELD),
        ("loss", RULE_FORBIDDEN_MODEL_FIELD),
        ("pl", RULE_FORBIDDEN_MODEL_FIELD),
        ("r", RULE_FORBIDDEN_MODEL_FIELD),
        ("capital", RULE_FORBIDDEN_MODEL_FIELD),
        ("funds", RULE_FORBIDDEN_MODEL_FIELD),
        ("qty", RULE_FORBIDDEN_MODEL_FIELD),
        ("equity", RULE_ACCOUNT_FIELD),
        ("cash", RULE_ACCOUNT_FIELD),
        ("margin", RULE_ACCOUNT_FIELD),
        ("exposure", RULE_ACCOUNT_FIELD),
        ("buying_power", RULE_ACCOUNT_FIELD),
        ("buyingPower", RULE_ACCOUNT_FIELD),
        ("deposit", RULE_ACCOUNT_FIELD),
        ("withdrawal", RULE_ACCOUNT_FIELD),
        ("fees", RULE_ACCOUNT_FIELD),
        ("slippage", RULE_ACCOUNT_FIELD),
        ("api_key", RULE_SECRET_FIELD),
        ("APIKey", RULE_SECRET_FIELD),
        ("password", RULE_SECRET_FIELD),
        ("credential", RULE_SECRET_FIELD),
        ("credentials", RULE_SECRET_FIELD),
        ("bearer", RULE_SECRET_FIELD),
        ("passphrase", RULE_SECRET_FIELD),
        ("private_key", RULE_SECRET_FIELD),
        ("x-secret-token", RULE_SECRET_FIELD),
        ("clientSecret", RULE_SECRET_FIELD),
        ("access_token", RULE_SECRET_FIELD),
    ],
)
def test_forbidden_keys_trip_with_correct_rules(key, rule):
    issues = scan_model_boundary({key: "value"})
    assert len(issues) == 1
    assert issues[0].path == f"$.{key}"
    assert issues[0].rule == rule
    assert issues[0].key == key


@pytest.mark.parametrize(
    "key",
    [
        "reason",
        "usage",
        "prompt_tokens",
        "prompt_tokens_total",
        "token_usage",
        "max_output_tokens",
        "completion_tokens",
        "total_tokens",
        "schema_version",
        "branch",
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
        "claim_id",
        "chunk_id",
        "packet_id",
        "packet_hash",
        "snapshot_id",
        "strategy_id",
        "close",
        "volume",
        "start_at",
        "end_at",
        "bar_id",
        "event_hash",
        "readiness",
        "quality",
    ],
)
def test_safe_keys_never_trip(key):
    assert scan_model_boundary({key: "value"}) == []


def test_prose_values_under_safe_keys_do_not_trip():
    value = {
        "reason": "the balance sheet context is irrelevant here",
        "notes": "P&L stands for profit and loss in ordinary prose",
        "analysis": "account balance and position quantity are never provided",
        "usage": {"prompt_tokens": 12, "completion_tokens": 4, "total_tokens": 16},
        "market": [{"close": "100.5", "volume": "5"}],
    }
    assert scan_model_boundary(value) == []


def test_assert_model_boundary_safe_raises_typed_error_with_limited_issues():
    value = {
        "a": {"balance": "1"},
        "b": {"margin": "2"},
        "c": {"api_key": "3"},
        "d": {"quantity": "4"},
        "e": {"fees": "5"},
        "f": {"equity": "6"},
        "g": {"cash": "7"},
        "h": {"pnl": "8"},
        "i": {"password": "9"},
        "j": {"exposure": "10"},
        "k": {"deposit": "11"},
        "l": {"withdrawal": "12"},
    }
    with pytest.raises(ModelBoundaryError) as error:
        assert_model_boundary_safe(value, context="model_request")
    assert error.value.code == "account_state_leak"
    assert len(error.value.issues) == 10
    assert str(error.value).startswith("model_request violates")


def test_assert_model_boundary_safe_passes_for_clean_values():
    assert_model_boundary_safe({"reason": "safe", "usage": {"prompt_tokens": 1}}, context="model_request")


def test_profile_ref_intact_true_for_top_level_and_extensions():
    expected = {"profile_ref_id": "spr_abc", "profile_ref_hash": "h" * 64, "strategy_id": "pa:test"}
    assert profile_ref_intact({"strategy_profile_ref": dict(expected)}, expected)
    assert profile_ref_intact({"extensions": {"strategy_profile_ref": dict(expected)}}, expected)
    assert profile_ref_intact({"extensions": {"nested": {"strategy_profile_ref": dict(expected)}}}, expected)
    forged = {"profile_ref_id": "spr_forged", "profile_ref_hash": "x" * 64, "strategy_id": "pa:test"}
    assert not profile_ref_intact(
        {"strategy_profile_ref": dict(expected), "payload": [{"nested": {"strategy_profile_ref": forged}}]},
        expected,
    )


def test_profile_ref_intact_false_for_missing_wrong_hash_and_wrong_strategy():
    expected = {"profile_ref_id": "spr_abc", "profile_ref_hash": "h" * 64, "strategy_id": "pa:test"}
    assert not profile_ref_intact({"extensions": {}}, expected)
    assert not profile_ref_intact({"strategy_profile_ref": {"profile_ref_id": "spr_abc"}}, expected)
    wrong_hash = dict(expected)
    wrong_hash["profile_ref_hash"] = "x" * 64
    assert not profile_ref_intact({"strategy_profile_ref": wrong_hash}, expected)
    wrong_id = dict(expected)
    wrong_id["profile_ref_id"] = "spr_zzz"
    assert not profile_ref_intact({"extensions": {"strategy_profile_ref": wrong_id}}, expected)
    wrong_strategy = dict(expected)
    wrong_strategy["strategy_id"] = "pa:other"
    assert not profile_ref_intact({"strategy_profile_ref": wrong_strategy}, expected)
    assert not profile_ref_intact({"strategy_profile_ref": dict(expected)}, {"profile_ref_id": "spr_abc"})
    assert not profile_ref_intact({"strategy_profile_ref": dict(expected)}, None)


# ---------------------------------------------------------------------------
# Sol adversarial-review evasion payloads (audit findings)
# ---------------------------------------------------------------------------


EVASION_PAYLOADS = [
    {"realizedPnL": "2.5"},
    {"unrealizedPnL": "1.1"},
    {"netPnL": "3"},
    {"netLiquidation": "5000.00"},
    {"net_liquidation_value": "5000"},
    {"p/l": "2.5"},
    {"P.L": "1.2"},
    {"pl2": "5.0"},
    {"r2": "2.5"},
    {"positionSizes": "3"},
    {"deepseek_api_key": "sk-123"},
    {"alpaca_api_key_id": "AK-123"},
    {"access_key": "v"},
    {"oauthaccesstoken": "t"},
    {"x-webhook-secret": "s"},
    {"authorization": "Bearer x"},
]


@pytest.mark.parametrize("payload", EVASION_PAYLOADS)
def test_evasion_payloads_are_blocked(payload):
    issues = scan_model_boundary({"nested": {"deeper": payload}})
    assert issues, f"payload escaped the boundary: {payload}"
    rule = issues[0].rule
    assert rule in {"forbidden_model_field", "account_field", "secret_field"}


def test_safe_usage_keys_still_pass():
    assert not scan_model_boundary({"max_output_tokens": 100, "prompt_tokens": 1, "token_usage": 2, "reason": "ok"})
    assert not scan_model_boundary({"note": "the balance sheet and P&L are discussed", "title": "loss explained"})


def test_forged_second_profile_ref_is_rejected():
    expected = {"profile_ref_id": "spr_abc", "profile_ref_hash": "h" * 64, "strategy_id": "pa:test"}
    forged = {"profile_ref_id": "spr_zzz", "profile_ref_hash": "x" * 64}
    assert not profile_ref_intact(
        {"strategy_profile_ref": dict(expected), "extensions": {"strategy_profile_ref": forged}},
        expected,
    )
    assert profile_ref_intact(
        {"strategy_profile_ref": dict(expected), "extensions": {"strategy_profile_ref": dict(expected)}},
        expected,
    )
