"""Execution tests use authored detections, not claimed recognition accuracy."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import copy
import json
import unittest

from recognition.models import MarketEvent
from . import ExecutionPolicy, PaperSimulator, execution_profiles

OPEN = datetime(2026, 9, 30, 13, 30, tzinfo=timezone.utc)


def event(kind="quote", seconds=1, symbol="SPY", **payload):
    t = OPEN + timedelta(seconds=seconds)
    default = {"bid": 99.99, "ask": 100.01} if kind == "quote" else {}
    return MarketEvent(event_id=f"E-{kind}-{symbol}-{seconds}", symbol=symbol, venue="IEX", kind=kind,
                       event_time=t, arrival_time=t, payload={**default, **payload})


def session(symbol="SPY", minutes=390):
    return event("session", 0, symbol, open_time=OPEN.isoformat(), close_time=(OPEN + timedelta(minutes=minutes)).isoformat(), tick_size=.01)


def signal(identifier="D1", setup="SET-0005", variant="base", direction=1, symbol="SPY", state="triggered", seconds=1):
    profile = next(p for p in execution_profiles() if p["setup_id"] == setup and p["variant_id"] == variant)
    return {"detection_id": identifier, "setup_id": setup, "variant_id": variant, "direction": direction,
            "symbol": symbol, "venue": "IEX", "state": state, "scale_seconds": 60, "data_status": "complete", "missing": [],
            "evidence_cutoff": (OPEN + timedelta(seconds=seconds)).isoformat(),
            **{k: profile[k] for k in ("source_version", "source_sha256", "rule_version")}}


class PaperTests(unittest.TestCase):
    def setUp(self):
        self.sim = PaperSimulator()
        self.sim.on_event(session(), [], 1)

    def submit(self, rows=None):
        return self.sim.on_event(event("bar", 1), rows or [signal()], 2)

    def fill(self):
        self.submit()
        return self.sim.on_event(event(seconds=2), [], 3)

    def test_all_graphs_registered(self):
        profiles = execution_profiles()
        self.assertEqual(len(profiles), 84)
        self.assertEqual(len({(p["setup_id"], p["variant_id"]) for p in profiles}), 84)
        self.assertTrue(all(p["market_validation"] == "not_run" for p in profiles))

    def test_each_execution_profile_can_submit_when_complete(self):
        for profile in execution_profiles():
            sim = PaperSimulator()
            sim.on_event(session(), [], 1)
            detection = signal(setup=profile["setup_id"], variant=profile["variant_id"], direction=profile["directions"][0])
            detection["scale_seconds"] = profile["scales"][0]
            out = sim.on_event(event("bar", 1), [detection], 2)
            with self.subTest(setup=profile["setup_id"], variant=profile["variant_id"]):
                self.assertEqual(out["decisions"][0]["reason"], "entry_submitted")
                self.assertEqual(out["orders"][0]["profile"]["rule_sha256"], profile["rule_sha256"])

    def test_next_quote_only_and_cent_accounting(self):
        out = self.sim.on_event(event(seconds=1), [signal()], 2)
        self.assertEqual(out["fills"], [])
        self.assertEqual(out["account"]["reserved_cents"], 1_000_000)
        out = self.sim.on_event(event(seconds=2), [], 3)
        self.assertEqual(out["fills"][0]["price"], "100.020000")
        self.assertEqual(out["fills"][0]["quantity"], 99)
        self.assertEqual(out["account"]["locked_cents"], 990198)
        self.assertEqual(out["account"]["reserved_cents"], 0)
        self.assertEqual(out["account"]["fees_cents"], 0)

    def test_foreign_quote_cannot_fill_registered_venue_entry(self):
        self.submit()
        foreign = MarketEvent(**{**event(seconds=2).__dict__, "venue": "OTHER"})
        out = self.sim.on_event(foreign, [], 3)
        self.assertEqual(out["fills"], [])
        self.assertEqual(out["account"]["reserved_cents"], 1_000_000)
        self.assertNotIn("SPY", self.sim.marks)
        out = self.sim.on_event(event(seconds=3), [], 4)
        self.assertEqual(len(out["fills"]), 1)
        self.assertEqual(self.sim.positions["SPY"]["venue"], "IEX")
        self.assertEqual(self.sim.marks["SPY"]["venue"], "IEX")

    def test_foreign_quote_cannot_value_trigger_or_fill_exit(self):
        self.fill()
        before = copy.deepcopy(self.sim.marks)
        foreign = MarketEvent(**{**event(seconds=3, bid=1, ask=1.02).__dict__, "venue": "OTHER"})
        out = self.sim.on_event(foreign, [], 4)
        self.assertEqual(self.sim.marks, before)
        self.assertEqual(out["orders"], [])
        self.assertFalse(out["account"]["daily_entry_blocked"])
        out = self.sim.on_event(event(seconds=4, bid=98.5, ask=98.52), [], 5)
        self.assertEqual(out["orders"][0]["purpose"], "exit")
        foreign = MarketEvent(**{**event(seconds=5, bid=90, ask=90.02).__dict__, "venue": "OTHER"})
        self.assertEqual(self.sim.on_event(foreign, [], 6)["fills"], [])
        out = self.sim.on_event(event(seconds=6, bid=97, ask=97.02), [], 7)
        self.assertEqual(out["fills"][0]["price"], "96.990000")
        self.assertEqual(out["account"]["realized_pnl_cents"], -29997)

    def test_foreign_session_cannot_replace_execution_calendar_or_day(self):
        before = copy.deepcopy(self.sim.sessions)
        day = self.sim.day
        foreign = MarketEvent(**{**session(minutes=5).__dict__, "venue": "OTHER"})
        out = self.sim.on_event(foreign, [], 2)
        self.assertEqual(self.sim.sessions, before)
        self.assertEqual(self.sim.day, day)
        self.assertEqual(out["decisions"][0]["reason"], "session_venue_mismatch")
        self.sim.on_event(event("bar", 1), [signal()], 3)
        self.assertEqual(len(self.sim.on_event(event(seconds=2), [], 4)["fills"]), 1)

    def test_foreign_status_cannot_change_registered_connectivity(self):
        foreign = MarketEvent(**{**event("status", 1, gap=True, halted=True).__dict__, "venue": "OTHER"})
        self.sim.on_event(foreign, [], 2)
        self.assertNotIn("SPY", self.sim.halted)
        self.assertNotIn("SPY", self.sim.disconnected)
        self.sim.on_event(event("status", 2, halted=True, gap=True, stream="all"), [], 3)
        foreign = MarketEvent(**{**event("status", 3, halted=False, connected=True).__dict__, "venue": "OTHER"})
        self.sim.on_event(foreign, [], 4)
        self.assertIn("SPY", self.sim.halted)
        self.assertIn("SPY", self.sim.disconnected)

    def test_foreign_signal_does_not_associate_or_invalidate_owner(self):
        self.submit()
        out = self.sim.on_event(event("bar", 2), [dict(signal("F", seconds=2), venue="OTHER")], 3)
        self.assertEqual(out["decisions"][0]["reason"], "execution_venue_mismatch")
        owner = self.sim._pending_entry("SPY")
        self.assertEqual(owner["related_detection_ids"], [])
        self.sim.on_event(event("bar", 3), [dict(signal(state="invalidated"), venue="OTHER")], 4)
        self.assertEqual(owner["status"], "pending")
        self.sim.on_event(event("bar", 4), [signal(state="invalidated")], 5)
        self.assertEqual(owner["status"], "cancelled")

    def test_snapshot_retains_venue_binding_and_foreign_quote_block(self):
        self.submit()
        restored = PaperSimulator.from_snapshot(json.loads(json.dumps(self.sim.snapshot())))
        foreign = MarketEvent(**{**event(seconds=2).__dict__, "venue": "OTHER"})
        self.assertEqual(restored.on_event(foreign, [], 3)["fills"], [])
        self.assertEqual(restored.sessions["SPY"]["venue"], "IEX")
        self.assertEqual(len(restored.on_event(event(seconds=3), [], 4)["fills"]), 1)

    def test_short_proceeds_do_not_expand_capital(self):
        self.submit([signal(direction=-1)])
        out = self.sim.on_event(event(seconds=2), [], 3)
        self.assertEqual(out["fills"][0]["price"], "99.980000")
        self.assertEqual(out["account"]["balance_cents"], 10_000_000)
        self.assertLess(out["account"]["available_cents"], 10_000_000)

    def test_conflict_blocks_whole_batch(self):
        out = self.submit([signal(), signal("D2", direction=-1)])
        self.assertEqual(out["orders"], [])
        self.assertEqual({d["reason"] for d in out["decisions"]}, {"same_batch_direction_conflict"})

    def test_final_invalidation_and_candidate_never_open(self):
        out = self.submit([signal(state="candidate"), signal(), signal(state="invalidated")])
        self.assertEqual(out["orders"], [])

    def test_base_priority_association_and_unique_owner_exit(self):
        variant = next(p for p in execution_profiles() if p["variant_id"] != "base")
        other = signal("V", variant["setup_id"], variant["variant_id"])
        out = self.submit([other, signal()])
        self.assertEqual(out["orders"][0]["detection_id"], "D1")
        self.assertEqual(out["orders"][0]["related_detection_ids"], ["V"])
        self.sim.on_event(event(seconds=2), [], 3)
        out = self.sim.on_event(event("bar", 3), [dict(other, state="invalidated")], 4)
        self.assertEqual(out["orders"], [])
        out = self.sim.on_event(event("bar", 4), [signal(state="invalidated")], 5)
        self.assertEqual(out["orders"][0]["purpose"], "exit")
        self.assertEqual(out["fills"], [])

    def test_timeout_and_clock_do_not_unhalt(self):
        self.submit()
        self.sim.on_event(event("status", 2, gap=True, halted=False, stream="all"), [], 3)
        out = self.sim.on_event(event("status", 62, clock=True, gap=False, halted=False), [], 4)
        self.assertEqual(out["orders"][0]["cancel_reason"], "entry_timeout")
        self.assertIn("SPY", self.sim.disconnected)
        self.assertEqual(out["fills"], [])

    def test_quote_conditions(self):
        self.submit()
        rows = [event(seconds=2, bid=0), event(seconds=3, bid=102, ask=101), event(seconds=4)]
        rows[-1] = MarketEvent(**{**rows[-1].__dict__, "arrival_time": OPEN + timedelta(seconds=10)})
        for i, row in enumerate(rows, 3):
            self.assertEqual(self.sim.on_event(row, [], i)["fills"], [])
        self.assertEqual(len(self.sim.on_event(event(seconds=11), [], 6)["fills"]), 1)

    def test_stop_gap_price_next_quote(self):
        self.fill()
        out = self.sim.on_event(event(seconds=3, bid=98.5, ask=98.52), [], 4)
        self.assertEqual(out["fills"], [])
        self.assertIn("stop_threshold", out["orders"][0]["reasons"])
        out = self.sim.on_event(event(seconds=4, bid=97, ask=97.02), [], 5)
        self.assertEqual(out["fills"][0]["price"], "96.990000")
        self.assertEqual(out["account"]["realized_pnl_cents"], -29997)
        self.assertEqual(out["account"]["locked_cents"], 0)

    def test_max_hold_and_half_day_close_wait_for_quote(self):
        self.fill()
        out = self.sim.on_event(event("status", 3602, clock=True), [], 4)
        self.assertEqual(out["fills"], [])
        self.assertIn("max_hold", out["orders"][0]["reasons"])
        half = PaperSimulator()
        half.on_event(session(minutes=210), [], 1)
        half.on_event(event("bar", 1), [signal()], 2)
        half.on_event(event(seconds=2), [], 3)
        out = half.on_event(event("status", 205 * 60, clock=True), [], 4)
        self.assertIn("session_close_buffer", out["orders"][0]["reasons"])
        self.assertEqual(out["fills"], [])

    def test_fee_inside_budget_and_pnl(self):
        sim = PaperSimulator(ExecutionPolicy(fee_per_fill="1.00"))
        sim.on_event(session(), [], 1)
        sim.on_event(event("bar", 1), [signal()], 2)
        out = sim.on_event(event(seconds=2), [], 3)
        self.assertEqual(out["fills"][0]["fee_cents"], 100)
        self.assertEqual(out["account"]["balance_cents"], 9999900)
        self.assertGreaterEqual(out["account"]["available_cents"], 0)

    def test_pause_cancel_and_resume(self):
        self.submit()
        out = self.sim.control("pause", 3, OPEN + timedelta(seconds=2))
        self.assertEqual(out["orders"][0]["cancel_reason"], "run_pause")
        self.assertEqual(out["fills"], [])
        out = self.sim.on_event(event("bar", 3), [signal("D2", seconds=3)], 4)
        self.assertEqual(out["decisions"][0]["reason"], "run_paused")
        self.sim.control("resume", 5, OPEN + timedelta(seconds=4))
        out = self.sim.on_event(event("bar", 5), [signal("D3", seconds=5)], 6)
        self.assertEqual(out["orders"][0]["status"], "pending")

    def test_snapshot_duplicate_and_version_guard(self):
        self.fill()
        restored = PaperSimulator.from_snapshot(json.loads(json.dumps(self.sim.snapshot())))
        ev = event(seconds=3)
        self.assertEqual(self.sim.on_event(ev, [], 4), restored.on_event(ev, [], 4))
        self.assertEqual(restored.on_event(ev, [], 4)["fills"], [])
        snapshot = restored.snapshot()
        snapshot["policy_sha256"] = "bad"
        with self.assertRaises(ValueError):
            PaperSimulator.from_snapshot(snapshot)

    def test_clock_prevents_queued_old_quote_fills(self):
        self.submit()
        self.sim.on_event(event("status", 10, clock=True), [], 3)
        self.assertEqual(self.sim.on_event(event(seconds=5), [], 4)["fills"], [])
        self.assertEqual(len(self.sim.on_event(event(seconds=11), [], 5)["fills"]), 1)

    def test_numeric_policy_strictness(self):
        for value in (True, float("nan"), float("inf"), "-1"):
            with self.assertRaises(ValueError):
                ExecutionPolicy(initial_cash=value)
        with self.assertRaises(ValueError):
            ExecutionPolicy(max_positions=True)

    def test_pending_reservations_count_toward_position_limit(self):
        sim = PaperSimulator()
        for n, symbol in enumerate(("SPY", "QQQ", "AAPL", "MSFT", "NVDA", "AMD"), 1):
            sim.on_event(session(symbol), [], n)
        for n, symbol in enumerate(("SPY", "QQQ", "AAPL", "MSFT", "NVDA", "AMD"), 7):
            out = sim.on_event(event("bar", 1, symbol), [signal(symbol, symbol=symbol)], n)
        self.assertEqual(out["decisions"][0]["reason"], "position_limit")
        self.assertEqual(out["account"]["reserved_cents"], 5_000_000)
        self.assertEqual(out["account"]["pending_entries"], 5)

    def test_daily_loss_cancels_and_exits_once(self):
        sim = PaperSimulator(ExecutionPolicy(allocation_fraction="0.5"))
        sim.on_event(session(), [], 1)
        sim.on_event(session("QQQ"), [], 2)
        sim.on_event(event("bar", 1), [signal()], 3)
        sim.on_event(event(seconds=2), [], 4)
        sim.on_event(event("bar", 3, "QQQ"), [signal("Q1", symbol="QQQ", seconds=3)], 5)
        out = sim.on_event(event(seconds=4, bid=94, ask=94.02), [], 6)
        self.assertTrue(out["account"]["daily_entry_blocked"])
        self.assertEqual(out["account"]["reserved_cents"], 0)
        self.assertEqual(len([o for o in out["orders"] if o["purpose"] == "exit"]), 2)  # one row updated with an extra reason
        pending_exit = [o for o in sim.orders.values() if o["purpose"] == "exit" and o["status"] == "pending"]
        self.assertEqual(len(pending_exit), 1)
        self.assertEqual(set(pending_exit[0]["reasons"]), {"daily_loss_limit", "stop_threshold"})
        self.assertEqual(out["fills"], [])

    def test_ledger_reconciles_fees_and_realized_balance(self):
        sim = PaperSimulator(ExecutionPolicy(fee_per_fill="1.00"))
        outputs = [sim.on_event(session(), [], 1), sim.on_event(event("bar", 1), [signal()], 2),
                   sim.on_event(event(seconds=2), [], 3), sim.on_event(event("bar", 3), [signal(state="invalidated")], 4),
                   sim.on_event(event(seconds=4, bid=101, ask=101.02), [], 5)]
        ledger_delta = sum(r["balance_delta_cents"] for output in outputs for r in output["account_events"])
        self.assertEqual(10_000_000 + ledger_delta, sim.balance_cents)
        self.assertEqual(sim.account()["fees_cents"], 200)

    def test_bar_proxy_separate_model_never_fills_from_quote(self):
        sim = PaperSimulator(ExecutionPolicy(model="next_bar_proxy_experimental"))
        sim.on_event(session(), [], 1)
        sim.on_event(event("bar", 60, open=100), [signal(seconds=60)], 2)
        self.assertEqual(sim.on_event(event(seconds=61), [], 3)["fills"], [])
        out = sim.on_event(event("bar", 120, open=101), [], 4)
        # The 60-second boundary is included; a later arrival is cancelled.
        self.assertEqual(len(out["fills"]), 1)
        sim = PaperSimulator(ExecutionPolicy(model="next_bar_proxy_experimental", entry_timeout_seconds=121))
        sim.on_event(session(), [], 1)
        sim.on_event(event("bar", 60, open=100), [signal(seconds=60)], 2)
        out = sim.on_event(event("bar", 120, open=101), [], 3)
        self.assertEqual(out["fills"][0]["execution_model"], "next_bar_proxy_experimental")
        self.assertEqual(out["fills"][0]["price"], "101.010000")

    def test_disconnect_then_clock_or_correction_cannot_release(self):
        self.submit()
        self.sim.on_event(event("status", 2, gap=True, halted=False, stream="all"), [], 3)
        self.sim.on_event(event("status", 3, gap=True, halted=False, stream="trade", correction=True), [], 4)
        self.assertEqual(self.sim.on_event(event(seconds=4), [], 5)["fills"], [])
        self.sim.on_event(event("status", 5, connected=True, gap=False, halted=False, stream="all"), [], 6)
        self.assertEqual(len(self.sim.on_event(event(seconds=6), [], 7)["fills"]), 1)

    def test_runtime_source_status_blocks_pending_and_replays(self):
        self.submit()
        self.sim.source_status("DISCONNECTED", 3, OPEN + timedelta(seconds=2))
        self.assertEqual(self.sim.on_event(event(seconds=3), [], 4)["fills"], [])
        restored = PaperSimulator.from_snapshot(self.sim.snapshot())
        for sim in (self.sim, restored):
            sim.source_status("CONNECTED", 5, OPEN + timedelta(seconds=4))
            self.assertEqual(sim.on_event(event(seconds=3), [], 6)["fills"], [])
            self.assertEqual(len(sim.on_event(event(seconds=5), [], 7)["fills"]), 1)
        self.assertEqual(self.sim.snapshot(), restored.snapshot())

    def test_connected_transport_does_not_clear_real_market_halt(self):
        self.submit()
        self.sim.on_event(event("status", 2, halted=True, gap=False), [], 3)
        self.sim.on_event(event("status", 3, connected=True, halted=False, gap=False), [], 4)
        self.assertIn("SPY", self.sim.halted)
        self.assertEqual(self.sim.on_event(event(seconds=4), [], 5)["fills"], [])
        self.sim.source_status("CONNECTED", 6, OPEN + timedelta(seconds=5))
        self.assertIn("SPY", self.sim.halted)
        self.sim.on_event(event("status", 6, halted=False, gap=False, vendor_status="T"), [], 7)
        self.assertEqual(len(self.sim.on_event(event(seconds=7), [], 8)["fills"]), 1)

    def test_event_scale_zero_submits_and_next_quote_fills(self):
        row = signal(setup="SET-0003")
        row["scale_seconds"] = 0
        out = self.sim.on_event(event(seconds=1), [row], 2)
        self.assertEqual(out["decisions"][0]["reason"], "entry_submitted")
        self.assertEqual(out["fills"], [])
        self.assertEqual(len(self.sim.on_event(event(seconds=2), [], 3)["fills"]), 1)

    def test_negative_boolean_and_unregistered_scales_block(self):
        for scale, reason in ((-1, "invalid_scale"), (True, "invalid_scale"), (23, "unsupported_profile_scale")):
            with self.subTest(scale=scale):
                sim = PaperSimulator()
                sim.on_event(session(), [], 1)
                row = signal()
                row["scale_seconds"] = scale
                out = sim.on_event(event("bar", 1), [row], 2)
                self.assertEqual(out["decisions"][0]["reason"], reason)
                self.assertEqual(out["orders"], [])

    def test_canonical_snapshot_multi_position_exit_order_is_stable(self):
        sim = PaperSimulator()
        for seq, symbol in enumerate(("SPY", "QQQ", "AAPL"), 1):
            sim.on_event(session(symbol), [], seq)
        seq = 3
        for symbol in ("SPY", "QQQ", "AAPL"):
            seq += 1
            sim.on_event(event("bar", 1, symbol), [signal(symbol, symbol=symbol)], seq)
            seq += 1
            sim.on_event(event(seconds=2, symbol=symbol), [], seq)
        restored = PaperSimulator.from_snapshot(json.loads(json.dumps(sim.snapshot(), sort_keys=True)))
        when = event("status", 3602, clock=True)
        self.assertEqual(sim.on_event(when, [], seq + 1), restored.on_event(when, [], seq + 1))
        self.assertEqual(sim.snapshot(), restored.snapshot())

    def test_pending_cancel_ids_ignore_serialized_mapping_order(self):
        sim = PaperSimulator()
        for seq, symbol in enumerate(("SPY", "QQQ", "AAPL"), 1):
            sim.on_event(session(symbol), [], seq)
        for seq, symbol in enumerate(("SPY", "QQQ", "AAPL"), 4):
            sim.on_event(event("bar", 1, symbol), [signal(symbol, symbol=symbol)], seq)
        snapshot = sim.snapshot()
        snapshot["orders"] = dict(reversed(list(snapshot["orders"].items())))
        restored = PaperSimulator.from_snapshot(snapshot)
        self.assertEqual(sim.control("pause", 7, OPEN + timedelta(seconds=2)), restored.control("pause", 7, OPEN + timedelta(seconds=2)))
        self.assertEqual(sim.snapshot(), restored.snapshot())

    def test_administrative_false_flags_never_clear_venue_halt(self):
        payloads = ({"connected": False}, {"connection_state": "disconnected"}, {"correction": True}, {"clock": True})
        for payload in payloads:
            with self.subTest(payload=payload):
                sim = PaperSimulator()
                sim.on_event(session(), [], 1)
                sim.on_event(event("status", 1, halted=True), [], 2)
                sim.on_event(event("status", 2, halted=False, gap=False, **payload), [], 3)
                self.assertIn("SPY", sim.halted)
                sim.on_event(event("status", 3, halted=False, gap=False), [], 4)
                self.assertNotIn("SPY", sim.halted)


if __name__ == "__main__":
    unittest.main()
