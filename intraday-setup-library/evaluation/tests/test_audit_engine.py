"""Regressions for audited timing, event continuation and sampled re-entry.

Graph cases isolate the interpreter; raw events also check the ingestion path.
These synthetic observations do not measure market precision or recall.
"""
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
import unittest

from recognition.engine import GraphRecognizer, RecognitionEngine
from recognition.specs import load_specs
from .graph_cases import CASES
from .support import OPEN, bar, event, reference, session
from .test_graphs import frame


class AuditEngineTests(unittest.TestCase):
    def spec(self, number, variant="base"):
        return deepcopy(next(s for s in load_specs([f"SET-{number:04}"], True)
                             if s["variant_id"] == variant))

    def at(self, spec, values, minute, *, seconds=60, caps=None):
        f = frame(spec, values, caps=caps, seconds=seconds)
        end = OPEN + timedelta(minutes=minute)
        return replace(f, bar=replace(f.bar, start=end - timedelta(seconds=max(1, seconds)), end=end),
                       arrived_at=end, observation_kind="event_window" if seconds == 0 else "bar")

    def test_explicit_same_frame_keeps_every_stage_and_capture(self):
        spec = self.spec(35)
        spec["stages"][1]["same_frame"] = True
        graph = GraphRecognizer([spec])
        initial = CASES[35][0] | CASES[35][1]
        rows = graph.process(self.at(spec, initial, 7))
        self.assertEqual([r.state for r in rows], ["candidate", "candidate"])
        self.assertEqual(graph.active[0].step, 2)
        self.assertEqual(graph.active[0].captured["level"], initial["ib_edge"])
        graph.process(self.at(spec, CASES[35][2], 8))
        last = graph.process(self.at(spec, CASES[35][3], 9))[-1]
        self.assertEqual(last.state, "triggered")
        checks = [c for c in last.conditions if c["operation"] != "score"]
        self.assertEqual(len(checks), sum(len(s["tests"]) for s in spec["stages"]))
        self.assertEqual([c["observed_at"] for c in checks], sorted(c["observed_at"] for c in checks))

    def test_same_frame_is_opt_in_and_does_not_skip_false_stage(self):
        for allow in (False, True):
            spec = self.spec(4)
            spec["stages"][2]["same_frame"] = allow
            graph = GraphRecognizer([spec])
            graph.process(self.at(spec, CASES[4][0], 1))
            rows = graph.process(self.at(spec, CASES[4][1] | {"resume": True}, 2))
            self.assertEqual(any(r.state == "triggered" for r in rows), allow)
        spec = self.spec(4)
        spec["stages"][2]["same_frame"] = True
        graph = GraphRecognizer([spec])
        graph.process(self.at(spec, CASES[4][0], 1))
        rows = graph.process(self.at(spec, CASES[4][1] | {"resume": None}, 2))
        self.assertEqual(rows[-1].state, "formed")
        self.assertEqual(graph.active[0].step, 2)

    def test_final_signal_endpoint_cannot_be_ordered_by_its_own_ohlc(self):
        spec = self.spec(12)
        spec["stages"][1]["same_frame"] = True
        graph = GraphRecognizer([spec])
        rows = graph.process(self.at(spec, CASES[12][0] | CASES[12][1], 1))
        last = rows[-1]
        self.assertEqual(last.state, "triggered")
        self.assertEqual(last.data_status, "partial")
        self.assertIn("intrabar_order_unknown", last.missing)
        self.assertTrue(any(c["feature"] == "intrabar_order" and c["result"] == "unknown"
                            for c in last.conditions))

    def test_price_path_must_cover_both_trigger_and_stop_for_order(self):
        for path, expected, status in (([10., 12.], ["triggered"], "partial"),
                                       ([10., 12., 8.], ["triggered", "invalidated"], "complete"),
                                       ([8., 12.], ["triggered"], "complete")):
            with self.subTest(path=path):
                spec = self.spec(5)
                graph = GraphRecognizer([spec])
                graph.process(self.at(spec, CASES[5][0], 1))
                rows = graph.process(self.at(spec, CASES[5][1] | {
                    "directional_low": 8., "price_path": path}, 2))
                self.assertEqual([r.state for r in rows], expected)
                self.assertEqual(rows[0].data_status, status)
                if expected[-1] == "invalidated":
                    self.assertEqual(graph.active, [])

    def test_atr_stop_touch_after_trigger_is_inclusive(self):
        spec = self.spec(2)
        graph = GraphRecognizer([spec])
        graph.process(self.at(spec, CASES[2][0], 5))
        rows = graph.process(self.at(spec, CASES[2][1] | {
            "directional_low": 9., "price_path": [11., 9.]}, 6))
        self.assertEqual([r.state for r in rows], ["triggered", "invalidated"])
        stop = next(c for c in rows[-1].conditions if c["stage"] == "结构失效")
        self.assertEqual(stop["operation"], "le")

    def test_same_frame_does_not_certify_missing_core_data(self):
        spec = self.spec(35)
        spec["stages"][1]["same_frame"] = True
        graph = GraphRecognizer([spec])
        values = CASES[35][0] | CASES[35][1]
        rows = graph.process(self.at(spec, values, 7, caps=set(spec["requirements"]) - {"classified_trades"}))
        self.assertEqual([r.state for r in rows], ["candidate"])
        self.assertEqual(graph.active[0].step, 0)

    def test_noise_reentry_waits_for_later_sampling_and_valid_protection(self):
        spec = self.spec(48, "same_side_vwap")
        spec["reentry_after_invalidation"] = True
        graph = GraphRecognizer([spec])
        outside = {"noise_break": True, "half_hour": True, "directional_price": 12.,
                   "directional_low": 11.9, "vwap_band": 11.}
        first = graph.process(self.at(spec, outside, 30))[-1]
        self.assertEqual(first.state, "triggered")
        stopped = graph.process(self.at(spec, outside | {"vwap_band": 13.}, 60))
        self.assertEqual([r.state for r in stopped], ["invalidated"])
        self.assertFalse(graph.process(self.at(spec, outside | {"half_hour": False}, 61)))
        self.assertFalse(graph.process(self.at(spec, outside | {"vwap_band": 13.}, 90)))
        recovered = graph.process(self.at(spec, outside, 120))[-1]
        self.assertEqual(recovered.state, "triggered")
        self.assertNotEqual(first.detection_id, recovered.detection_id)
        self.assertFalse(graph.process(self.at(spec, outside, 150)))
        graph.process(self.at(spec, outside | {"noise_break": False}, 180))
        self.assertFalse(graph.process(self.at(spec, outside, 210)))
        self.assertEqual(len({r.detection_id for r in graph.transitions if r.state == "triggered"}), 2)

    def test_noise_initial_invalid_risk_does_not_consume_future_entry(self):
        spec = self.spec(48, "same_side_vwap")
        graph = GraphRecognizer([spec])
        values = {"noise_break": True, "half_hour": True, "directional_price": 12., "vwap_band": 13.}
        self.assertFalse(graph.process(self.at(spec, values, 30)))
        rows = graph.process(self.at(spec, values | {"vwap_band": 11.}, 60))
        self.assertEqual(rows[-1].state, "triggered")

    def test_missing_sampled_risk_keeps_uncertainty_and_does_not_use_old_risk(self):
        spec = self.spec(48, "same_side_vwap")
        graph = GraphRecognizer([spec])
        values = {"noise_break": True, "half_hour": True, "directional_price": 12., "vwap_band": 11.}
        graph.process(self.at(spec, values, 30))
        rows = graph.process(self.at(spec, values | {"directional_price": 10., "vwap_band": None}, 60))
        self.assertEqual(rows[-1].state, "triggered")
        self.assertIn("sampled_risk_unknown", rows[-1].missing)
        self.assertEqual(rows[-1].data_status, "partial")

    def test_only_configured_scale_may_continue_an_existing_candidate(self):
        spec = self.spec(2)
        spec["continuation_scales"] = [0]
        graph = GraphRecognizer([spec])
        self.assertFalse(graph.process(self.at(spec, CASES[2][0], 5, seconds=0)))
        graph.process(self.at(spec, CASES[2][0], 5))
        self.assertFalse(graph.process(self.at(spec, CASES[2][1], 6, seconds=120)))
        rows = graph.process(self.at(spec, CASES[2][1], 6, seconds=0))
        self.assertEqual(rows[-1].state, "triggered")
        self.assertEqual(rows[-1].scale_seconds, 60)
        self.assertEqual(rows[-1].anchor["scales"], [0, 60])
        stopped = graph.process(self.at(spec, {"directional_low": 8.}, 7, seconds=0))
        self.assertEqual(stopped[-1].state, "invalidated")

    def test_continuation_window_excludes_prices_before_frozen_anchor(self):
        spec = self.spec(2)
        spec["continuation_scales"] = [0]
        graph = GraphRecognizer([spec])
        graph.process(self.at(spec, CASES[2][0], 5))
        values = {"directional_extreme": 12., "directional_price": 10., "directional_low": 10.,
                  "price_observations": [
                      {"time": (OPEN + timedelta(minutes=4)).isoformat(), "price": 12.},
                      {"time": (OPEN + timedelta(minutes=6)).isoformat(), "price": 10.}]}
        self.assertFalse(graph.process(self.at(spec, values, 6, seconds=0)))
        values["price_observations"].append({"time": (OPEN + timedelta(minutes=7)).isoformat(), "price": 12.})
        rows = graph.process(self.at(spec, values, 7, seconds=0))
        self.assertEqual(rows[-1].state, "triggered")

    def test_continuation_same_frame_stages_share_the_original_price_cutoff(self):
        for direction in (1, -1):
            with self.subTest(direction=direction):
                spec = self.spec(2)
                spec["directions"] = [direction]
                spec["continuation_scales"] = [0]
                spec["stages"].append({"name": "顺向成交确认", "tests": [["directional_price", "ge", "@trigger"]],
                                       "capture": {"confirmed": "directional_price"}, "same_frame": True})
                trigger, risk, price = (11., 9., 12.) if direction == 1 else (-11., -13., -10.)
                graph = GraphRecognizer([spec])
                graph.process(self.at(spec, CASES[2][0] | {"orb_edge": trigger, "orb_stop": risk}, 5))
                observed = self.at(spec, {"price_observations": [
                    {"time": (OPEN + timedelta(minutes=6)).isoformat(), "price": price}]}, 6, seconds=0)
                rows = graph.process(observed)
                self.assertEqual([r.state for r in rows], ["formed", "triggered"])
                self.assertEqual(rows[-1].anchor["captures"]["confirmed"], price)
                core = [c for c in rows[-1].conditions if c["operation"] != "score"]
                self.assertEqual(len(core), sum(len(s["tests"]) for s in spec["stages"]))

    def test_later_equal_atr_and_sampled_stops_report_inclusive_predicate(self):
        for number, variant in ((2, "base"), (48, "same_side_vwap")):
            for direction in (1, -1):
                with self.subTest(number=number, direction=direction):
                    spec = self.spec(number, variant)
                    spec["directions"] = [direction]
                    trigger, risk, price = (11., 9., 12.) if direction == 1 else (-11., -13., -10.)
                    def observation(values, minute):
                        f = self.at(spec, {}, minute)
                        f.values[direction].update(values)
                        return f
                    graph = GraphRecognizer([spec])
                    if number == 2:
                        graph.process(observation(CASES[2][0] | {"orb_edge": trigger, "orb_stop": risk}, 5))
                        graph.process(observation({"directional_extreme": price, "directional_low": price}, 6))
                        values = {"directional_low": risk}
                    else:
                        graph.process(observation({"noise_break": True, "half_hour": True,
                                                  "directional_price": price, "vwap_band": risk}, 30))
                        values = {"noise_break": True, "half_hour": True,
                                  "directional_price": risk, "vwap_band": risk}
                    rows = graph.process(observation(values, 60))
                    stop = next(c for c in rows[0].conditions if c["stage"] == "结构失效")
                    self.assertEqual((stop["actual"], stop["expected"], stop["operation"]), (risk, risk, "le"))

    def orb_engine(self):
        engine = RecognitionEngine(["SET-0002"])
        engine.graph.specs[0]["continuation_scales"] = [0]
        engine.feed(session({"references": {"atr14": reference(2.), "average_daily_volume14": reference(2_000_000)},
                             "coverage": {"trades": {"complete": True, "start_time": OPEN.isoformat()}}}))
        return engine

    def test_real_trade_continues_orb_before_next_minute_closes(self):
        engine = self.orb_engine()
        for i in range(1, 6):
            engine.feed(bar(i, 10., 10. + .1 * i, 9.9, 10. + .05 * i))
        trade_time = OPEN + timedelta(minutes=5, seconds=5)
        tick = event("trade", trade_time, {"price": 10.51, "size": 100, "side": "buy"})
        rows = engine.feed(tick)
        triggers = [r for r in rows if r.state == "triggered"]
        self.assertEqual(len(triggers), 1)
        self.assertEqual(triggers[0].evidence_cutoff, tick.event_time.isoformat())
        self.assertEqual(triggers[0].observation_kind, "event_window")
        self.assertEqual(triggers[0].scale_seconds, 60)
        self.assertEqual(triggers[0].data_status, "complete")
        self.assertFalse(engine.feed(tick))

    def test_sparse_raw_window_does_not_reuse_pre_orb_endpoint_trade(self):
        engine = self.orb_engine()
        for i in range(1, 5):
            engine.feed(bar(i, 10., 10. + .1 * i, 9.9, 10. + .05 * i))
        engine.feed(event("trade", OPEN + timedelta(minutes=4, seconds=30),
                          {"price": 10.6, "size": 100, "side": "buy"}))
        engine.feed(bar(5, 10.2, 10.6, 10.1, 10.25))
        inside = engine.feed(event("trade", OPEN + timedelta(minutes=5, seconds=5),
                                   {"price": 10.51, "size": 100, "side": "buy"}))
        self.assertFalse(any(r.state == "triggered" for r in inside))
        outside = engine.feed(event("trade", OPEN + timedelta(minutes=5, seconds=6),
                                    {"price": 10.61, "size": 100, "side": "buy"}))
        self.assertTrue(any(r.state == "triggered" and r.data_status == "complete" for r in outside))


if __name__ == "__main__":
    unittest.main()
