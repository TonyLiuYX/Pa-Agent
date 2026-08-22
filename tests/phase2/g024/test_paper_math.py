from decimal import Decimal
import json
from pathlib import Path

import pytest

from pa_engine.paper.core import (
    MONEY_QUANTUM,
    R_QUANTUM,
    SHARE_QUANTUM,
    InvalidPaperInput,
    PaperAccount,
    PaperAccountBook,
    PaperEvent,
    calculate_settlement,
    calculate_trade,
    planned_r,
    quantity_for_balance,
    stable_hash,
    validate_model_payload,
)


FIXTURES = Path(__file__).parents[2] / "fixtures" / "phase2" / "g024"


def load_fixture(name):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def test_account_initializes_once_without_reset():
    book = PaperAccountBook()
    first = book.ensure_account("pa:one")
    opened = book.open_trade("pa:one", "XYZ", "long", Decimal("3"), Decimal("2"), Decimal("5"), idempotency_key="open")
    assert opened.accepted
    closed = book.close_trade("pa:one", Decimal("5"), idempotency_key="close")
    second = book.ensure_account("pa:one")
    assert first.balance == Decimal("1000.00")
    assert closed.balance_after == Decimal("1666.67")
    assert second.balance == Decimal("1666.67")
    assert second.initial_balance == Decimal("1000.00")
    assert [event.event_type for event in book.events].count("account_initialized") == 1


def test_long_and_short_golden_math():
    long_fixture = load_fixture("golden_long.json")
    long = calculate_trade("long", Decimal(long_fixture["balance_before"]), Decimal(long_fixture["entry_price"]), Decimal(long_fixture["stop_price"]), Decimal(long_fixture["target_price"]))
    assert long["allocation_fraction"] == Decimal("1")
    assert long["quantity"] == Decimal(long_fixture["quantity"])
    assert long["planned_r"] == Decimal(long_fixture["planned_r"])
    settled_long = calculate_settlement("long", Decimal(long_fixture["balance_before"]), Decimal(long_fixture["entry_price"]), Decimal(long_fixture["exit_price"]), long["quantity"])
    assert settled_long["gross_pnl"] == Decimal(long_fixture["gross_pnl"])
    assert settled_long["applied_pnl"] == Decimal(long_fixture["applied_pnl"])
    assert settled_long["closing_balance"] == Decimal(long_fixture["closing_balance"])

    short_fixture = load_fixture("golden_short.json")
    short = calculate_trade("short", Decimal(short_fixture["balance_before"]), Decimal(short_fixture["entry_price"]), Decimal(short_fixture["stop_price"]), Decimal(short_fixture["target_price"]))
    assert short["quantity"] == Decimal(short_fixture["quantity"])
    assert short["planned_r"] == Decimal(short_fixture["planned_r"])
    settled_short = calculate_settlement("short", Decimal(short_fixture["balance_before"]), Decimal(short_fixture["entry_price"]), Decimal(short_fixture["exit_price"]), short["quantity"])
    assert settled_short["gross_pnl"] == Decimal(short_fixture["gross_pnl"])
    assert settled_short["applied_pnl"] == Decimal(short_fixture["applied_pnl"])
    assert settled_short["closing_balance"] == Decimal(short_fixture["closing_balance"])


def test_fixed_scales_and_stable_hash():
    assert quantity_for_balance(Decimal("1000"), Decimal("7")) == Decimal("142.85714285")
    assert planned_r(Decimal("3"), Decimal("2"), Decimal("7")) == Decimal("4.0000")
    assert quantity_for_balance(Decimal("1"), Decimal("3")) == Decimal("0.33333333")
    assert Decimal("0.00000001").as_tuple().exponent == SHARE_QUANTUM.as_tuple().exponent
    assert R_QUANTUM == Decimal("0.0001") and MONEY_QUANTUM == Decimal("0.01")
    one = {"b": Decimal("2.00"), "a": [Decimal("1.0"), "x"]}
    two = {"a": [Decimal("1.0"), "x"], "b": Decimal("2.00")}
    assert stable_hash(one) == stable_hash(two)


def test_nonterminating_r_and_half_up_pnl_boundaries():
    assert planned_r(Decimal("4"), Decimal("1"), Decimal("5")) == Decimal("0.3333")
    assert planned_r(Decimal("100001"), Decimal("1"), Decimal("223446")) == Decimal("1.2345")
    positive = calculate_settlement("long", Decimal("100.00"), Decimal("10"), Decimal("10.005"), Decimal("1"))
    negative = calculate_settlement("long", Decimal("100.00"), Decimal("10"), Decimal("9.995"), Decimal("1"))
    assert positive["gross_pnl"] == Decimal("0.005") and positive["applied_pnl"] == Decimal("0.01")
    assert negative["gross_pnl"] == Decimal("-0.005") and negative["applied_pnl"] == Decimal("-0.01")


@pytest.mark.parametrize(
    "gross, expected_applied, expected_balance",
    [
        (Decimal("-999.994"), Decimal("-999.99"), Decimal("0.01")),
        (Decimal("-1000.00"), Decimal("-1000.00"), Decimal("0.00")),
        (Decimal("-1000.004"), Decimal("-1000.00"), Decimal("0.00")),
    ],
)
def test_loss_cap_order(gross, expected_applied, expected_balance):
    # This uses a simple synthetic gross directly to isolate the frozen order.
    from pa_engine.paper.core import apply_pnl

    applied, closing, capped, exhausted = apply_pnl(Decimal("1000.00"), gross)
    assert applied == expected_applied
    assert closing == expected_balance
    assert capped == max(gross, Decimal("-1000.00"))
    assert exhausted == (expected_balance == Decimal("0.00"))


def test_loss_cap_fixture_vectors_are_consumed():
    from pa_engine.paper.core import apply_pnl

    for vector in load_fixture("loss_cap.json"):
        applied, closing, _capped, _exhausted = apply_pnl(Decimal("1000.00"), Decimal(vector["gross"]))
        assert applied == Decimal(vector["applied_pnl"])
        assert closing == Decimal(vector["closing_balance"])


def test_opposite_strategies_are_isolated_and_not_netted():
    isolation = load_fixture("isolation.json")
    assert isolation["netting"] is False and isolation["shared_balance"] is False
    book = PaperAccountBook(["pa:long", "pa:short"])
    left = book.open_trade("pa:long", "XYZ", "long", Decimal("10"), Decimal("9"), Decimal("12"), idempotency_key="l-open")
    right = book.open_trade("pa:short", "XYZ", "short", Decimal("10"), Decimal("11"), Decimal("8"), idempotency_key="s-open")
    assert left.accepted and right.accepted
    assert left.quantity == right.quantity
    left_close = book.close_trade("pa:long", Decimal("12"), idempotency_key="l-close")
    right_close = book.close_trade("pa:short", Decimal("12"), idempotency_key="s-close")
    assert left_close.balance_after == Decimal("1200.00")
    assert right_close.balance_after == Decimal("800.00")
    assert book.get_account("pa:long").balance != book.get_account("pa:short").balance
    assert left_close.event_hash != right_close.event_hash


def test_invalid_levels_immediate_drift_and_idempotency_do_not_mutate():
    idem = load_fixture("idempotency.json")
    assert idem["same_key_same_content"] == "replay" and idem["same_key_changed_content"] == "conflict"
    book = PaperAccountBook()
    book.ensure_account("pa:test")
    invalid = book.open_trade("pa:test", "XYZ", "long", Decimal("10"), Decimal("11"), Decimal("12"), idempotency_key="bad")
    assert not invalid.accepted
    assert book.get_account("pa:test").open_trade is None
    immediate = book.open_trade("pa:test", "XYZ", "long", None, Decimal("9"), Decimal("11"), immediate=True, fill_reference=Decimal("12"), idempotency_key="drift")
    assert not immediate.accepted and immediate.fresh_price_rejected
    assert book.get_account("pa:test").balance == Decimal("1000.00")
    ok = book.open_trade("pa:test", "XYZ", "long", Decimal("10"), Decimal("9"), Decimal("11"), idempotency_key="same")
    replay = book.open_trade("pa:test", "XYZ", "long", Decimal("10"), Decimal("9"), Decimal("11"), idempotency_key="same")
    conflict = book.open_trade("pa:test", "XYZ", "long", Decimal("10"), Decimal("9"), Decimal("12"), idempotency_key="same")
    assert ok.accepted and replay.replayed and not replay.changed
    assert conflict.conflict and not conflict.changed
    assert len(book.history("pa:test")) == 1


def test_fixed_public_constructor_rejects_arbitrary_restore_balance():
    from pa_engine.paper.core import PaperAccount

    account = PaperAccount("pa:custom")
    assert account.snapshot().balance == Decimal("1000.00")
    assert account.snapshot().initial_balance == Decimal("1000.00")
    with pytest.raises(TypeError):
        PaperAccount("pa:custom", "12.34")


def test_equal_entry_stop_is_zero_risk_and_does_not_create_account():
    book = PaperAccountBook()
    result = book.open_trade("pa:zero-risk", "XYZ", "long", "10", "10", "11", idempotency_key="zero")
    assert result.status == "rejected" and result.code == "zero_risk"
    assert book.get_account("pa:zero-risk") is None


def test_conditional_fill_reference_must_equal_requested_entry():
    book = PaperAccountBook()
    result = book.open_trade("pa:conditional", "XYZ", "long", "10", "9", "11", fill_reference="11", idempotency_key="mismatch")
    assert result.status == "rejected" and result.code == "fill_price_mismatch"
    assert book.get_account("pa:conditional") is None
    pending = book.create_pending_entry("pa:conditional", "XYZ", "long", "10", "9", "11", idempotency_key="pending")
    filled = book.fill_pending_entry("pa:conditional", pending.pending_id, "11", idempotency_key="fill-mismatch")
    assert filled.status == "rejected" and filled.code == "fill_price_mismatch"
    assert book.get_account("pa:conditional").open_trade is None


def test_immediate_fresh_flag_is_only_for_valid_original_plan_with_moved_fill():
    book = PaperAccountBook()
    drift = book.open_trade("pa:drift", "XYZ", "long", None, "9", "11", immediate=True, fill_reference="12", requested_entry_price="10", idempotency_key="drift")
    invalid_plan = book.open_trade("pa:invalid-plan", "XYZ", "long", None, "11", "9", immediate=True, fill_reference="10", requested_entry_price="10", idempotency_key="invalid")
    assert drift.code == "invalid_level_order" and drift.fresh_price_rejected
    assert invalid_plan.code == "invalid_level_order" and not invalid_plan.fresh_price_rejected


def test_instruction_requires_exact_branch_and_strategy_binding():
    book = PaperAccountBook()
    base = {"strategy_id": "pa:A", "branch": "wait_for_price", "direction": "long", "entry_price": "10", "stop_price": "9", "target_price": "11"}
    mismatch = book.open_from_instruction("pa:B", base, symbol="XYZ", idempotency_key="binding")
    alias = book.open_from_instruction("pa:A", {**base, "branch": "immediate"}, symbol="XYZ", idempotency_key="alias")
    assert mismatch.code == "strategy_binding_mismatch"
    assert alias.code == "invalid_branch"
    assert book.get_account("pa:A") is None and book.get_account("pa:B") is None


@pytest.mark.parametrize("fill", [float("nan"), float("inf"), True, Decimal("NaN"), Decimal("Infinity")])
def test_invalid_immediate_fill_is_typed_and_not_fresh_drift(fill):
    book = PaperAccountBook()
    result = book.open_trade("pa:bad-immediate", "XYZ", "long", None, "9", "11", immediate=True, fill_reference=fill, idempotency_key=f"bad-{repr(fill)}")
    assert result.status == "rejected" and not result.fresh_price_rejected
    assert book.get_account("pa:bad-immediate") is None


@pytest.mark.parametrize("fill", [float("nan"), float("inf"), True, Decimal("NaN"), Decimal("Infinity")])
def test_invalid_pending_fill_is_typed_and_does_not_change_state(fill):
    book = PaperAccountBook()
    pending = book.create_pending_entry("pa:bad-pending", "XYZ", "long", "10", "9", "11", idempotency_key="pending")
    result = book.fill_pending_entry("pa:bad-pending", pending.pending_id, fill, idempotency_key=f"bad-{repr(fill)}")
    assert result.status == "rejected"
    assert book.get_account("pa:bad-pending").pending_trade is not None


def test_over_loss_short_clips_and_exhausts():
    book = PaperAccountBook()
    book.open_trade("pa:short-loss", "XYZ", "short", Decimal("100"), Decimal("110"), Decimal("90"), idempotency_key="open")
    result = book.close_trade("pa:short-loss", Decimal("1000"), idempotency_key="close")
    assert result.status == "exhausted"
    assert result.gross_pnl == Decimal("-9000.00")
    assert result.applied_pnl == Decimal("-1000.00")
    assert result.balance_after == Decimal("0.00")
    assert result.exhausted and book.get_account("pa:short-loss").exhausted
    assert result.event.payload["fees"] == Decimal("0.00")
    assert result.event.payload["slippage"] == Decimal("0.00")


def test_nested_engine_only_payload_is_rejected():
    payload = {"extensions": [{"telemetry": {"accountBalance": "1000.00"}}]}
    result = validate_model_payload(payload)
    assert not result.valid
    assert result.path == "$.extensions[0].telemetry.accountBalance"


@pytest.mark.parametrize("payload, path", [
    ({"account": {"balance": "1000.00"}}, "$.account"),
    ({"nested": [{"available_quantity": "1"}]}, "$.nested[0].available_quantity"),
    ({"meta": {"realized_pnl": "1.00"}}, "$.meta.realized_pnl"),
    ({"meta": {"appliedPnl": "1.00"}}, "$.meta.appliedPnl"),
])
def test_nested_engine_fields_are_rejected_directly(payload, path):
    result = validate_model_payload(payload)
    assert not result.valid and result.path == path


def test_opaque_provider_model_payloads_fail_closed():
    class Opaque:
        pass

    assert not validate_model_payload(Opaque()).valid
    assert not validate_model_payload(("tuple",)).valid
    assert not validate_model_payload({1: "non-string-key"}).valid


def test_conditional_pending_fill_and_expiry_keep_one_position_boundary():
    book = PaperAccountBook()
    pending = book.create_pending_entry("pa:pending", "XYZ", "long", Decimal("10"), Decimal("9"), Decimal("12"), idempotency_key="pending")
    assert pending.status == "pending" and pending.pending_id
    assert book.get_account("pa:pending").pending_trade is not None
    filled = book.fill_pending_entry("pa:pending", pending.pending_id, Decimal("10"), idempotency_key="fill")
    assert filled.accepted and filled.trade.entry_price == Decimal("10")
    assert book.get_account("pa:pending").pending_trade is None
    assert book.get_account("pa:pending").open_trade is not None

    other = PaperAccountBook()
    p = other.create_pending_entry("pa:expire", "XYZ", "short", Decimal("10"), Decimal("11"), Decimal("8"), idempotency_key="p")
    expired = other.expire_pending_entry("pa:expire", p.pending_id, idempotency_key="e")
    assert expired.status == "expired" and expired.changed is True
    assert other.get_account("pa:expire").pending_trade is None


def test_g018_like_conditional_instruction_stops_at_pending_until_fill():
    book = PaperAccountBook()
    instruction = {
        "strategy_id": "pa:instruction",
        "branch": "wait_for_price",
        "direction": "short",
        "entry_price": "10",
        "stop_price": "11",
        "target_price": "8",
    }
    pending = book.open_from_instruction("pa:instruction", instruction, symbol="XYZ", idempotency_key="instruction")
    assert pending.status == "pending" and pending.pending_id
    assert book.get_account("pa:instruction").open_trade is None
    filled = book.fill_pending_entry("pa:instruction", pending.pending_id, "10", idempotency_key="instruction-fill")
    assert filled.accepted and filled.trade.direction == "short"


def test_untradeable_share_quantum_marks_strategy_exhausted():
    book = PaperAccountBook()
    result = book.open_trade("pa:tiny", "XYZ", "long", Decimal("100000000001"), Decimal("100000000000"), Decimal("100000000002"), idempotency_key="tiny")
    assert result.status == "exhausted" and result.untradeable
    assert book.get_account("pa:tiny").exhausted


def test_negative_level_fixture_vectors_are_executed():
    vectors = load_fixture("negative_levels.json")
    for index, vector in enumerate(vectors):
        book = PaperAccountBook()
        result = book.open_trade(
            f"pa:negative-{index}",
            "XYZ",
            vector["direction"],
            vector["entry_price"],
            vector["stop_price"],
            vector["target_price"],
            idempotency_key=f"negative-{index}",
        )
        assert result.code == vector["code"]
        assert result.changed is False and book.get_account(f"pa:negative-{index}") is None


def test_pending_fill_id_is_caller_bound_for_idempotency():
    book = PaperAccountBook()
    pending = book.create_pending_entry("pa:fill-id", "XYZ", "long", "10", "9", "11", idempotency_key="pending")
    wrong = book.fill_pending_entry("pa:fill-id", "wrong", "10", idempotency_key="fill")
    retry = book.fill_pending_entry("pa:fill-id", pending.pending_id, "10", idempotency_key="fill")
    assert wrong.code == "pending_mismatch" and not wrong.changed
    assert retry.status == "conflict" and retry.code == "idempotency_conflict"
    assert book.get_account("pa:fill-id").pending_trade is not None


def test_close_requires_dual_ids_to_match_each_other_and_position():
    book = PaperAccountBook()
    opened = book.open_trade("pa:dual-id", "XYZ", "long", "10", "9", "11", trade_id="correct", idempotency_key="open")
    rejected = book.close_trade("pa:dual-id", "11", position_id="correct", trade_id="wrong", idempotency_key="close")
    assert opened.accepted and rejected.code == "position_mismatch" and not rejected.changed
    assert book.get_account("pa:dual-id").open_trade is not None


def test_wrapper_state_is_read_only_and_fixed_balance_cannot_be_overwritten():
    from pa_engine.paper.core import PaperAccount

    account = PaperAccount("pa:wrapped")
    for field, value in (("balance", "12.34"), ("pending", True), ("pending_trade", object())):
        with pytest.raises(AttributeError):
            setattr(account, field, value)
    assert account.snapshot().balance == Decimal("1000.00")


def test_event_payload_is_recursively_immutable_and_to_dict_is_detached():
    book = PaperAccountBook()
    opened = book.open_trade("pa:event-immutable", "XYZ", "long", "10", "9", "11", idempotency_key="open")
    event = opened.event
    with pytest.raises(TypeError):
        event.payload["balance_before"] = "tampered"
    with pytest.raises(TypeError):
        event.payload["trade"]["entry_price"] = Decimal("99")
    projection = {
        "paper_math_version": event.paper_math_version,
        "event_type": event.event_type,
        "strategy_id": event.strategy_id,
        "payload": event.payload,
    }
    assert stable_hash(projection) == event.content_hash
    detached = event.to_dict()
    detached["payload"]["balance_before"] = "tampered"
    assert event.payload["balance_before"] == Decimal("1000.00")
    with pytest.raises(TypeError):
        opened.calculation["quantity"] = Decimal("0")


def test_immediate_instruction_preserves_full_g018_second_event_reference():
    book = PaperAccountBook()
    second = {"schema_version": "0.1.0", "event_id": "trusted-event", "event_hash": "a" * 64, "reference_price": "10"}
    instruction = {
        "strategy_id": "pa:g018-lineage",
        "branch": "enter_immediately",
        "direction": "long",
        "stop_price": "9",
        "target_price": "11",
        "second_event_ref": second,
    }
    result = book.open_from_instruction("pa:g018-lineage", instruction, symbol="XYZ", idempotency_key="lineage")
    assert result.accepted and result.trade.entry_price == Decimal("10")
    assert dict(result.event.payload["fill_reference"]) == second
    fresh = {"reference_price": "10", "event_id": "fresh-event", "event_hash": "b" * 64}
    book_fresh = PaperAccountBook()
    fresh_result = book_fresh.open_from_instruction("pa:g018-lineage", instruction, symbol="XYZ", fill_reference=fresh, idempotency_key="fresh-lineage")
    assert fresh_result.accepted and dict(fresh_result.event.payload["fill_reference"]) == fresh


def test_open_costs_participate_in_request_identity_before_rejection():
    book = PaperAccountBook()
    nonzero = book.open_trade("pa:cost-id", "XYZ", "long", "10", "9", "11", fees="1.00", idempotency_key="same")
    zero = book.open_trade("pa:cost-id", "XYZ", "long", "10", "9", "11", fees="0.00", idempotency_key="same")
    assert nonzero.code == "nonzero_costs" and zero.status == "conflict"
    assert book.get_account("pa:cost-id") is None


@pytest.mark.parametrize("kwargs, code", [
    ({"trade_id": ""}, "invalid_trade_id"),
    ({"trade_id": 7}, "invalid_trade_id"),
    ({"immediate": 1}, "invalid_immediate"),
    ({"immediate": "true"}, "invalid_immediate"),
])
def test_open_rejects_invalid_ids_and_immediate_type_without_account(kwargs, code):
    book = PaperAccountBook()
    result = book.open_trade("pa:bad-boundary", "XYZ", "long", "10", "9", "11", idempotency_key="bad", **kwargs)
    assert result.status == "rejected" and result.code == code and not result.changed
    assert book.get_account("pa:bad-boundary") is None


@pytest.mark.parametrize("pending_id", ["", 7])
def test_pending_id_is_validated_before_account_creation(pending_id):
    book = PaperAccountBook()
    result = book.create_pending_entry("pa:bad-pending-id", "XYZ", "long", "10", "9", "11", pending_id=pending_id, idempotency_key="bad")
    assert result.status == "rejected" and result.code == "invalid_pending_id"
    assert book.get_account("pa:bad-pending-id") is None


def test_mark_pending_escape_hatch_is_not_public():
    assert not hasattr(PaperAccountBook, "mark_pending")


def test_wrapper_strategy_id_is_read_only_and_operations_keep_identity():
    account = PaperAccount("pa:wrapped-id")
    with pytest.raises(AttributeError):
        account.strategy_id = "pa:other"
    opened = account.open_trade("XYZ", "long", "10", "9", "11", idempotency_key="open")
    closed = account.close_trade("11", idempotency_key="close")
    assert opened.accepted and closed.accepted
    assert account.strategy_id == "pa:wrapped-id"
    assert closed.strategy_id == "pa:wrapped-id"


def test_paper_event_rejects_forged_hash_or_id_and_roundtrips_make_output():
    event = PaperEvent.make("paper_test", "pa:event", {"value": Decimal("1.00")})
    serialized = event.to_dict()
    roundtrip = PaperEvent(**serialized)
    assert roundtrip.content_hash == event.content_hash
    assert roundtrip.event_id == "pev_" + roundtrip.content_hash
    with pytest.raises(InvalidPaperInput):
        PaperEvent("paper_test", "pa:event", "forged", "forged", {"value": Decimal("1.00")})
    with pytest.raises(InvalidPaperInput):
        PaperEvent("paper_test", "pa:event", event.content_hash, "forged", {"value": Decimal("1.00")})
