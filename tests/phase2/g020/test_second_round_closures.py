from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from pa_engine.market_sources.alpaca.stream import (
    AlpacaIEXClient,
    InMemoryWebSocket,
    LocalSecondAggregator,
    MalformedPayloadError,
    RecordedWebSocketTransport,
    SecondReconciliationTombstone,
    combine_rules,
    condition_rule,
    condition_table,
    parse_cancel_error,
    validate_second_event,
)


def _trade(trade_id: int, *, price: str = "1", receipt: str = "2026-03-09T13:30:01Z", conditions: list[str] | None = None):
    return {
        "T": "t",
        "S": "AAPL",
        "i": trade_id,
        "x": "V",
        "p": price,
        "s": 1,
        "c": conditions or ["@"],
        "t": "2026-03-09T13:30:00.2Z",
        "z": "C",
    }, receipt


def _cancel(trade_id: int, *, price: str = "1", receipt: str = "2026-03-09T13:30:05Z"):
    return {
        "T": "x",
        "S": "AAPL",
        "i": trade_id,
        "x": "V",
        "p": price,
        "s": 1,
        "a": "C",
        "t": "2026-03-09T13:30:02Z",
        "z": "C",
    }, receipt


def _ready_socket(*, trade: dict | None = None, connected: bool = False):
    incoming = []
    if connected:
        incoming.append('[{"T":"success","msg":"connected"}]')
    incoming.extend(
        [
            '[{"T":"success","msg":"authenticated"}]',
            '[{"T":"subscription","trades":["AAPL"],"corrections":["AAPL"],"cancelErrors":["AAPL"]}]',
        ]
    )
    if trade is not None:
        incoming.append(json.dumps([trade]))
    return InMemoryWebSocket(incoming=incoming)


def test_connected_is_not_authenticated_and_subscription_must_match_all_controls():
    socket = InMemoryWebSocket(
        incoming=[
            '[{"T":"success","msg":"connected"}]',
            '[{"T":"success","msg":"authenticated"}]',
            '[{"T":"subscription","trades":["AAPL"],"corrections":[],"cancelErrors":[]}]',
        ]
    )
    client = AlpacaIEXClient("AAPL", key_id="k", secret_key="s", transport=RecordedWebSocketTransport([socket]))
    status = client.connect()
    assert status.state == "error"
    assert status.outcome.kind == "subscription_mismatch"
    assert not status.subscribed

    extra = InMemoryWebSocket(
        incoming=[
            '[{"T":"success","msg":"authenticated"}]',
            '[{"T":"subscription","trades":["AAPL","MSFT"],"corrections":["AAPL"],"cancelErrors":["AAPL"]}]',
        ]
    )
    status = AlpacaIEXClient("AAPL", key_id="k", secret_key="s", transport=RecordedWebSocketTransport([extra])).connect()
    assert status.outcome.kind == "subscription_mismatch"


def test_data_before_subscription_ack_is_not_a_ready_signal():
    trade, _ = _trade(1)
    socket = InMemoryWebSocket(
        incoming=[
            '[{"T":"success","msg":"authenticated"}]',
            json.dumps([trade]),
        ]
    )
    client = AlpacaIEXClient("AAPL", key_id="k", secret_key="s", transport=RecordedWebSocketTransport([socket]))
    status = client.connect()
    assert status.outcome.kind == "protocol"
    assert not client.aggregator.registry


@pytest.mark.parametrize(
    ("code", "kind", "retryable"),
    [(401, "authentication", False), (402, "authentication", False), (404, "timeout", True), (405, "entitlement", False), (406, "entitlement", False), (407, "disconnect", True), (409, "entitlement", False), (410, "subscription", False), (500, "server", True)],
)
def test_official_control_error_codes_are_safely_classified(code: int, kind: str, retryable: bool):
    socket = InMemoryWebSocket(incoming=[json.dumps([{"T": "error", "code": code, "msg": "provider text redacted"}])])
    client = AlpacaIEXClient("AAPL", key_id="k", secret_key="s", transport=RecordedWebSocketTransport([socket]))
    status = client.connect()
    assert status.outcome.kind == kind
    assert status.outcome.retryable is retryable
    assert "provider text" not in json.dumps(status.to_dict())


def test_cancel_actions_are_official_enum_only():
    value, receipt = _cancel(1)
    assert parse_cancel_error(value, received_at=receipt).action == "C"
    value["a"] = "E"
    assert parse_cancel_error(value, received_at=receipt).action == "E"
    value["a"] = "WRONG"
    with pytest.raises(MalformedPayloadError):
        parse_cancel_error(value, received_at=receipt)


def test_post_freeze_cancel_emits_tombstone_without_replacing_execution_event():
    trade, receipt = _trade(1)
    aggregator = LocalSecondAggregator("AAPL")
    aggregator.add_trade(trade, received_at=receipt)
    frozen = aggregator.freeze(now="2026-03-09T13:30:04Z")[0]
    cancel, cancel_receipt = _cancel(1)
    update = aggregator.apply_cancel(cancel, received_at=cancel_receipt)
    assert update.reconciliation and update.frozen
    assert isinstance(update.event, SecondReconciliationTombstone)
    assert aggregator.event(frozen.start_at, frozen.end_at) == frozen
    latest = aggregator.latest_event(frozen.start_at, frozen.end_at)
    assert isinstance(latest, SecondReconciliationTombstone)
    assert latest.tombstone and latest.open is None
    assert latest.revision_id != frozen.revision_id


def test_pre_freeze_correction_and_cancel_only_rebuild_revision_zero_candidate():
    first, first_receipt = _trade(1, price="1", receipt="2026-03-09T13:30:01Z")
    second, second_receipt = _trade(2, price="2", receipt="2026-03-09T13:30:01.1Z")
    aggregator = LocalSecondAggregator("AAPL")
    aggregator.add_trade(first, received_at=first_receipt)
    aggregator.add_trade(second, received_at=second_receipt)

    correction = {
        "T": "c", "S": "AAPL", "x": "V", "oi": 1, "op": "1", "os": 1, "oc": ["@"],
        "ci": 3, "cp": "3", "cs": 1, "cc": ["@"], "z": "C", "t": "2026-03-09T13:30:02Z",
    }
    corrected = aggregator.apply_correction(correction, received_at="2026-03-09T13:30:02Z")
    assert corrected.accepted and not corrected.frozen and not corrected.reconciliation
    assert corrected.event is not None and corrected.event.revision_sequence == 0 and corrected.event.parent_revision_id is None

    cancel, cancel_receipt = _cancel(2, price="2", receipt="2026-03-09T13:30:02.5Z")
    canceled = aggregator.apply_cancel(cancel, received_at=cancel_receipt)
    assert canceled.accepted and not canceled.frozen and not canceled.reconciliation
    assert canceled.event is not None and canceled.event.revision_sequence == 0 and canceled.event.parent_revision_id is None

    frozen = aggregator.freeze(now="2026-03-09T13:30:04Z")[0]
    assert frozen.revision_sequence == 0 and frozen.parent_revision_id is None
    assert frozen.contributor_ids == ("3",)
    assert aggregator.reconciliation_history == {}


def test_post_freeze_reconciliation_history_preserves_three_step_lineage():
    trade, receipt = _trade(1, price="1")
    aggregator = LocalSecondAggregator("AAPL")
    aggregator.add_trade(trade, received_at=receipt)
    frozen = aggregator.freeze(now="2026-03-09T13:30:04Z")[0]
    first_correction = {
        "T": "c", "S": "AAPL", "x": "V", "oi": 1, "op": "1", "os": 1, "oc": ["@"],
        "ci": 2, "cp": "2", "cs": 1, "cc": ["@"], "z": "C", "t": "2026-03-09T13:30:01Z",
    }
    second_correction = {
        "T": "c", "S": "AAPL", "x": "V", "oi": 2, "op": "2", "os": 1, "oc": ["@"],
        "ci": 3, "cp": "3", "cs": 1, "cc": ["@"], "z": "C", "t": "2026-03-09T13:30:02Z",
    }
    aggregator.apply_correction(first_correction, received_at="2026-03-09T13:30:05Z")
    aggregator.apply_correction(second_correction, received_at="2026-03-09T13:30:06Z")
    cancel, cancel_receipt = _cancel(3, price="3", receipt="2026-03-09T13:30:07Z")
    aggregator.apply_cancel(cancel, received_at=cancel_receipt)
    bucket = (frozen.start_at, frozen.end_at)
    history = aggregator.reconciliation_history[bucket]
    assert len(history) == 3
    assert [revision.revision_sequence for revision in history] == [1, 2, 3]
    assert [revision.parent_revision_id for revision in history] == [frozen.revision_id, history[0].revision_id, history[1].revision_id]
    assert len({revision.revision_id for revision in history}) == 3
    assert [revision.revision_id for revision in aggregator.events(include_reconciliation=True)[1:]] == [revision.revision_id for revision in history]
    assert aggregator.frozen_events[bucket].to_dict() == frozen.to_dict()
    assert aggregator.latest_event(*bucket).tombstone


def test_post_freeze_late_trade_has_reconciliation_flags_and_boundary_freeze():
    trade, receipt = _trade(1, price="1")
    aggregator = LocalSecondAggregator("AAPL")
    aggregator.add_trade(trade, received_at=receipt)
    late, late_receipt = _trade(2, price="2", receipt="2026-03-09T13:30:04Z")
    update = aggregator.add_trade(late, received_at=late_receipt)
    assert update.accepted and update.frozen and update.reconciliation
    assert update.event is not None and update.event.status == "reconciliation"
    bucket = ("2026-03-09T13:30:00Z", "2026-03-09T13:30:01Z")
    assert aggregator.frozen_events[bucket].status == "frozen"
    assert len(aggregator.reconciliation_history[bucket]) == 1
    assert aggregator.latest_event(*bucket).status == "reconciliation"


def test_post_freeze_trade_correction_cancel_lineage_is_contiguous_and_idempotent():
    first, first_receipt = _trade(1, price="1")
    aggregator = LocalSecondAggregator("AAPL")
    aggregator.add_trade(first, received_at=first_receipt)
    frozen = aggregator.freeze(now="2026-03-09T13:30:04Z")[0]

    late, late_receipt = _trade(2, price="2", receipt="2026-03-09T13:30:05Z")
    late_update = aggregator.add_trade(late, received_at=late_receipt)
    assert late_update.accepted and late_update.reconciliation
    assert late_update.event is not None
    assert late_update.event.revision_sequence == 1
    assert late_update.event.parent_revision_id == frozen.revision_id

    late_duplicate = aggregator.add_trade(late, received_at="2026-03-09T13:30:05.5Z")
    assert late_duplicate.duplicate and not late_duplicate.accepted

    correction = {
        "T": "c", "S": "AAPL", "x": "V", "oi": 2, "op": "2", "os": 1, "oc": ["@"],
        "ci": 3, "cp": "3", "cs": 1, "cc": ["@"], "z": "C", "t": "2026-03-09T13:30:02Z",
    }
    correction_update = aggregator.apply_correction(correction, received_at="2026-03-09T13:30:06Z")
    assert correction_update.accepted and correction_update.reconciliation
    assert correction_update.event is not None
    assert correction_update.event.revision_sequence == 2
    assert correction_update.event.parent_revision_id == late_update.event.revision_id

    correction_duplicate = aggregator.apply_correction(correction, received_at="2026-03-09T13:30:06.5Z")
    assert correction_duplicate.duplicate and not correction_duplicate.accepted
    assert correction_duplicate.event is not None
    assert correction_duplicate.event.revision_id == correction_update.event.revision_id

    cancel, cancel_receipt = _cancel(3, price="3", receipt="2026-03-09T13:30:07Z")
    cancel_update = aggregator.apply_cancel(cancel, received_at=cancel_receipt)
    assert cancel_update.accepted and cancel_update.reconciliation
    assert cancel_update.event is not None
    assert cancel_update.event.revision_sequence == 3
    assert cancel_update.event.parent_revision_id == correction_update.event.revision_id

    cancel_duplicate = aggregator.apply_cancel(cancel, received_at="2026-03-09T13:30:07.5Z")
    assert cancel_duplicate.duplicate and not cancel_duplicate.accepted
    assert cancel_duplicate.event is not None
    assert cancel_duplicate.event.revision_id == cancel_update.event.revision_id

    bucket = (frozen.start_at, frozen.end_at)
    history = aggregator.reconciliation_history[bucket]
    assert [revision.revision_sequence for revision in history] == [1, 2, 3]
    assert [revision.parent_revision_id for revision in history] == [
        frozen.revision_id,
        history[0].revision_id,
        history[1].revision_id,
    ]
    assert aggregator.frozen_events[bucket].to_dict() == frozen.to_dict()


def test_exact_watermark_boundary_freezes_before_cancel_but_just_before_does_not():
    trade, receipt = _trade(1)
    value, _ = _cancel(1, receipt="2026-03-09T13:30:03.999999999Z")
    before = LocalSecondAggregator("AAPL")
    before.add_trade(trade, received_at=receipt)
    before.apply_cancel(value, received_at="2026-03-09T13:30:03.999999999Z")
    assert not before.frozen_events

    exact = LocalSecondAggregator("AAPL")
    exact.add_trade(trade, received_at=receipt)
    exact.apply_cancel(value, received_at="2026-03-09T13:30:04Z")
    assert exact.frozen_events
    assert exact.reconciliation_events


def test_correction_replay_and_forged_original_are_idempotent_without_mutation():
    trade, receipt = _trade(1)
    aggregator = LocalSecondAggregator("AAPL")
    aggregator.add_trade(trade, received_at=receipt)
    forged = {
        "T": "c",
        "S": "AAPL",
        "x": "V",
        "oi": 1,
        "op": "9",
        "os": 1,
        "oc": ["@"],
        "ci": 2,
        "cp": "2",
        "cs": 1,
        "cc": ["@"],
        "z": "C",
        "t": "2026-03-09T13:31:00Z",
    }
    before = dict(aggregator.registry)
    assert aggregator.apply_correction(forged, received_at="2026-03-09T13:31:01Z").error == "correction_mismatch"
    assert dict(aggregator.registry) == before
    forged["op"] = "1"
    first = aggregator.apply_correction(forged, received_at="2026-03-09T13:31:01Z")
    replay = aggregator.apply_correction(forged, received_at="2026-03-09T13:31:02Z")
    assert first.accepted and replay.duplicate
    assert replay.error is None


def test_reconnect_reauth_subscribe_and_duplicate_trade_closes_gap():
    trade, _ = _trade(1)
    first = _ready_socket(trade=trade)
    second = _ready_socket(trade=trade)
    client = AlpacaIEXClient(
        "AAPL",
        key_id="k",
        secret_key="s",
        transport=RecordedWebSocketTransport([first, second]),
        reconnect_attempts=1,
    )
    status = client.run(max_messages=2)
    assert status.state == "ready"
    assert client.reconnect_count == 1
    assert len(client.aggregator.registry) == 1
    assert client.aggregator.connection_gaps and all(gap.end_at is not None for gap in client.aggregator.connection_gaps)


def test_event_validator_rejects_watermark_hash_feed_adjustment_and_secret_tampering():
    trade, receipt = _trade(1)
    aggregator = LocalSecondAggregator("AAPL")
    event = aggregator.add_trade(trade, received_at=receipt).event
    assert event is not None
    payload = event.to_dict()
    for mutate in (
        lambda value: value.update(execution_watermark="2026-03-09T13:30:03Z"),
        lambda value: value["contributors"].update(hash="0" * 64),
        lambda value: value["market_feed"].update(feed="sip"),
        lambda value: value.update(raw_adjustment="split-adjusted"),
        lambda value: value["quality"].update(api_secret="do-not-accept"),
    ):
        tampered = copy.deepcopy(payload)
        mutate(tampered)
        assert not validate_second_event(tampered).valid


def test_condition_table_strictness_remains_independent_per_field():
    assert combine_rules(["@", "4", "I"], "C").to_dict() == {"open_close": "red", "high_low": "red", "volume": "green"}
    assert not condition_rule("M", "A").updates_volume
    assert condition_rule("9", "C").to_dict() == {"open_close": "red", "high_low": "red", "volume": "red"}
    assert condition_rule("T", "O").updates_high_low


def test_corrected_consolidated_close_9_is_not_volume_eligible_or_ohlc():
    aggregator = LocalSecondAggregator("AAPL")
    ninth, receipt = _trade(9, price="9", conditions=["9"])
    update = aggregator.add_trade(ninth, received_at=receipt)
    assert update.event is None and update.audit is None
    normal, normal_receipt = _trade(10, price="10")
    aggregator.add_trade(normal, received_at=normal_receipt)
    event = aggregator.aggregate("2026-03-09T13:30:00Z", "2026-03-09T13:30:01Z")
    assert event is not None
    assert event.volume == "1"
    assert event.contributor_ids == ("10",)


def test_fully_ineligible_arrival_order_does_not_change_event_identity_or_watermark():
    ignored, ignored_receipt = _trade(9, price="9", receipt="2026-03-09T13:30:00.5Z", conditions=["9"])
    normal, normal_receipt = _trade(10, price="10", receipt="2026-03-09T13:30:05Z")
    first = LocalSecondAggregator("AAPL")
    first.add_trade(ignored, received_at=ignored_receipt)
    first.add_trade(normal, received_at=normal_receipt)
    second = LocalSecondAggregator("AAPL")
    second.add_trade(normal, received_at=normal_receipt)
    second.add_trade(ignored, received_at=ignored_receipt)
    bucket = ("2026-03-09T13:30:00Z", "2026-03-09T13:30:01Z")
    only_normal = LocalSecondAggregator("AAPL")
    only_normal.add_trade(normal, received_at=normal_receipt)
    event_a = first.aggregate(*bucket)
    event_b = second.aggregate(*bucket)
    event_normal = only_normal.aggregate(*bucket)
    assert event_a is not None and event_b is not None and event_normal is not None
    assert event_a.to_dict() == event_b.to_dict() == event_normal.to_dict()
    assert event_a.execution_watermark == normal_receipt
    assert event_a.contributor_ids == ("10",)
    assert first.registry[9].conditions == ("9",)


def test_correction_ineligible_to_eligible_uses_correction_receipt_as_first_contributor():
    ignored, ignored_receipt = _trade(9, price="9", receipt="2026-03-09T13:30:00Z", conditions=["9"])
    normal, normal_receipt = _trade(10, price="10", receipt="2026-03-09T13:30:10Z")
    aggregator = LocalSecondAggregator("AAPL")
    aggregator.add_trade(ignored, received_at=ignored_receipt)
    aggregator.add_trade(normal, received_at=normal_receipt)
    correction = {
        "T": "c", "S": "AAPL", "x": "V", "oi": 9, "op": "9", "os": 1, "oc": ["9"],
        "ci": 9, "cp": "9", "cs": 1, "cc": ["@"], "z": "C", "t": "2026-03-09T13:30:00Z",
    }
    update = aggregator.apply_correction(correction, received_at="2026-03-09T13:30:05Z")
    assert update.accepted and update.event is not None
    assert update.event.execution_watermark == "2026-03-09T13:30:05Z"
    assert update.event.contributor_ids == ("9", "10")


def test_correction_eligible_to_eligible_keeps_original_first_receipt_before_watermark():
    first, first_receipt = _trade(1, price="1", receipt="2026-03-09T13:30:05Z")
    second, second_receipt = _trade(2, price="2", receipt="2026-03-09T13:30:10Z")
    aggregator = LocalSecondAggregator("AAPL")
    aggregator.add_trade(first, received_at=first_receipt)
    aggregator.add_trade(second, received_at=second_receipt)
    correction = {
        "T": "c", "S": "AAPL", "x": "V", "oi": 1, "op": "1", "os": 1, "oc": ["@"],
        "ci": 1, "cp": "3", "cs": 1, "cc": ["@"], "z": "C", "t": "2026-03-09T13:30:00Z",
    }
    update = aggregator.apply_correction(correction, received_at="2026-03-09T13:30:06Z")
    assert update.event is not None
    assert update.event.execution_watermark == first_receipt


def test_correction_eligible_to_ineligible_recomputes_first_receipt_and_cancel_ignored_is_noop():
    first, first_receipt = _trade(1, price="1", receipt="2026-03-09T13:30:05Z")
    second, second_receipt = _trade(2, price="2", receipt="2026-03-09T13:30:10Z")
    aggregator = LocalSecondAggregator("AAPL")
    aggregator.add_trade(first, received_at=first_receipt)
    aggregator.add_trade(second, received_at=second_receipt)
    before = aggregator.aggregate("2026-03-09T13:30:00Z", "2026-03-09T13:30:01Z")
    correction = {
        "T": "c", "S": "AAPL", "x": "V", "oi": 1, "op": "1", "os": 1, "oc": ["@"],
        "ci": 1, "cp": "1", "cs": 1, "cc": ["9"], "z": "C", "t": "2026-03-09T13:30:00Z",
    }
    update = aggregator.apply_correction(correction, received_at="2026-03-09T13:30:04Z")
    assert update.event is not None
    assert update.event.execution_watermark == second_receipt
    ignored, ignored_receipt = _trade(9, price="9", receipt="2026-03-09T13:30:00Z", conditions=["9"])
    aggregator.add_trade(ignored, received_at=ignored_receipt)
    cancel, cancel_receipt = _cancel(9, price="9", receipt="2026-03-09T13:30:07Z")
    canceled = aggregator.apply_cancel(cancel, received_at=cancel_receipt)
    after = aggregator.aggregate("2026-03-09T13:30:00Z", "2026-03-09T13:30:01Z")
    assert canceled.accepted and canceled.event is None
    assert before is not None and after is not None
    assert after.contributor_ids == ("2",) and after.execution_watermark == second_receipt
    assert after.revision_sequence == update.event.revision_sequence


def test_official_condition_fixture_covers_every_supported_code_tape_field():
    fixture = json.loads((Path(__file__).parents[2] / "fixtures" / "phase2" / "g020" / "minute_condition_table.json").read_text())
    expected = {(row["code"], row["tape"]): row for row in fixture}
    actual = condition_table()
    assert {(code, tape) for code, tapes in actual.items() for tape in tapes} == set(expected)
    for (code, tape), row in expected.items():
        rule = condition_rule(code, tape)
        assert {"open_close": rule.open_close, "high_low": rule.high_low, "volume": rule.volume} == {
            "open_close": row["open_close"],
            "high_low": row["high_low"],
            "volume": row["volume"],
        }
