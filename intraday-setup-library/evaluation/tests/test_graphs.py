from datetime import datetime, timedelta, timezone
import unittest

from recognition.engine import GraphRecognizer
from recognition.features import Frame
from recognition.models import Bar
from recognition.specs import load_specs
from .graph_cases import CASES

START = datetime(2026, 9, 30, 13, 30, tzinfo=timezone.utc)
COMMON = dict(directional_price=12., directional_extreme=12.2, directional_low=11.9,
              extreme=9., signal_edge=11.5, signal_risk=9., trend_origin=8., pause_edge=11.,
              news_risk=9., range_break_edge=11., tolerance=.01, tick_size=.01, noise_scale=.2,
              recent_directional_closes=[12., 12., 12.])


def frame(spec, changes, index=0, caps=None, seconds=None):
    seconds = seconds if seconds is not None else spec["scales"][0]
    time = START + timedelta(seconds=(index + 1) * max(1, seconds))
    values = COMMON | changes
    direction = spec["directions"][0]
    if direction == -1:
        # Shift normalised coordinates into valid negative physical price space.
        prices = {"directional_price", "directional_extreme", "directional_low", "extreme", "signal_edge", "signal_risk", "trend_origin", "pause_edge", "news_risk", "range_break_edge", "cash_open", "overnight_risk"}
        for k in list(values):
            if k in prices and isinstance(values[k], (int, float)):
                values[k] -= 100
        if "recent_directional_closes" in values:
            values["recent_directional_closes"] = [v - 100 for v in values["recent_directional_closes"]]
    physical = direction * values["directional_price"] if values.get("directional_price") is not None else 12
    bar = Bar(time - timedelta(seconds=max(1, seconds)), time, physical, physical + 1, physical - 1, physical, 100, physical * 100, (f"fixture-{index}",))
    return Frame("SYN", "SYN", seconds, bar, time, {direction: values, -direction: {}},
                 set(spec["requirements"]) if caps is None else caps, [], [f"fixture-{index}"], "equity")


class AllGraphTests(unittest.TestCase):
    def setUp(self):
        self.specs = {s["setup_id"]: s for s in load_specs()}

    def test_all_fifty_hand_authored_paths(self):
        self.assertEqual(len(CASES), 50)
        for number, steps in CASES.items():
            with self.subTest(setup=number):
                spec = self.specs[f"SET-{number:04}"]
                graph = GraphRecognizer([spec])
                rows = [r for i, step in enumerate(steps) for r in graph.process(frame(spec, step, i))]
                self.assertTrue(any(r.state == "triggered" for r in rows))
                times = [c["observed_at"] for r in rows[-1:] for c in r.conditions if c["operation"] != "score"]
                self.assertEqual(times, sorted(times))

    def test_missing_required_data_never_confirms(self):
        for number, steps in CASES.items():
            with self.subTest(setup=number):
                spec = self.specs[f"SET-{number:04}"]
                graph = GraphRecognizer([spec])
                rows = [r for i, step in enumerate(steps) for r in graph.process(frame(spec, step, i, caps=set()))]
                self.assertFalse(any(r.state in {"formed", "triggered"} for r in rows))

    def test_later_data_cannot_certify_a_missing_earlier_stage(self):
        spec = self.specs["SET-0003"]
        graph = GraphRecognizer([spec])
        graph.process(frame(spec, CASES[3][0], caps={"bars", "key_levels", "classified_trades"}))
        rows = graph.process(frame(spec, CASES[3][1], 1))
        self.assertFalse(any(r.state == "triggered" for r in rows))

    def test_reversed_paths_do_not_trigger(self):
        for number, steps in CASES.items():
            if len(steps) < 2:
                continue
            with self.subTest(setup=number):
                spec = self.specs[f"SET-{number:04}"]
                graph = GraphRecognizer([spec])
                rows = [r for i, step in enumerate(reversed(steps)) for r in graph.process(frame(spec, step, i))]
                self.assertFalse(any(r.state == "triggered" for r in rows))

    def test_unknown_last_core_condition_never_triggers(self):
        for number, steps in CASES.items():
            if len(steps) < 2:
                continue
            with self.subTest(setup=number):
                spec = self.specs[f"SET-{number:04}"]
                graph = GraphRecognizer([spec])
                for i, step in enumerate(steps[:-1]):
                    graph.process(frame(spec, step, i))
                last = dict(steps[-1])
                feature = spec["stages"][-1]["tests"][0][0]
                last[feature] = None
                if feature == "captured_retest":
                    last["directional_low"] = None
                if feature == "accepted":
                    last["recent_directional_closes"] = None
                rows = graph.process(frame(spec, last, len(steps) - 1))
                self.assertFalse(any(r.state == "triggered" for r in rows))

    def test_hikkake_deadline_is_inclusive_and_variant_is_separate(self):
        base = self.specs["SET-0005"]
        for wait, expected in ((3, True), (4, False)):
            graph = GraphRecognizer([base])
            graph.process(frame(base, CASES[5][0]))
            for i in range(1, wait):
                graph.process(frame(base, {"directional_extreme": 10.5}, i))
            rows = graph.process(frame(base, CASES[5][1], wait))
            self.assertEqual(any(r.state == "triggered" for r in rows), expected)
        variant = next(s for s in load_specs(["SET-0005"], True) if s["variant_id"] == "extended_window")
        graph = GraphRecognizer([variant])
        graph.process(frame(variant, CASES[5][0]))
        for i in range(1, 4):
            graph.process(frame(variant, {"directional_extreme": 10.5}, i))
        self.assertTrue(any(r.state == "triggered" for r in graph.process(frame(variant, CASES[5][1], 4))))

    def test_one_bar_limit_for_trend_bar_failure(self):
        spec = self.specs["SET-0017"]
        graph = GraphRecognizer([spec])
        graph.process(frame(spec, CASES[17][0]))
        graph.process(frame(spec, {"directional_extreme": 11}, 1))
        rows = graph.process(frame(spec, CASES[17][1], 2))
        self.assertFalse(any(r.state == "triggered" for r in rows))
        self.assertTrue(any(r.state == "expired" for r in rows))

    def test_noise_risk_is_dynamic_sampled_close_and_no_duplicate_episode(self):
        spec = self.specs["SET-0048"]
        graph = GraphRecognizer([spec])
        first = CASES[48][0] | {"directional_price": 12, "opposite_band": 9}
        graph.process(frame(spec, first))
        self.assertEqual(graph.process(frame(spec, {"half_hour": False, "directional_low": 1}, 1)), [])
        rows = graph.process(frame(spec, first | {"directional_low": 5}, 30))
        self.assertEqual(rows, [])  # a wick is not the sampling close
        rows = graph.process(frame(spec, {"half_hour": True, "opposite_band": 13, "directional_price": 12}, 60))
        self.assertTrue(any(r.state == "invalidated" for r in rows))

    def test_post_trigger_invalidation_keeps_trigger_history(self):
        spec = self.specs["SET-0005"]
        graph = GraphRecognizer([spec])
        graph.process(frame(spec, CASES[5][0]))
        graph.process(frame(spec, CASES[5][1], 1))
        rows = graph.process(frame(spec, {"directional_low": 8}, 2))
        self.assertTrue(any(r.state == "invalidated" for r in rows))
        self.assertTrue(any(r.state == "triggered" for r in graph.transitions))

    def test_intrabar_order_stays_unknown_without_tape(self):
        spec = self.specs["SET-0005"]
        graph = GraphRecognizer([spec])
        graph.process(frame(spec, CASES[5][0]))
        rows = graph.process(frame(spec, CASES[5][1] | {"directional_low": 8}, 1))
        self.assertIn("intrabar_order_unknown", rows[0].missing)

    def test_terminal_edges_do_not_erase_other_setup_tags(self):
        spec = self.specs["SET-0005"]
        other = self.specs["SET-0012"]
        graph = GraphRecognizer([spec, other])
        start = CASES[5][0] | CASES[12][0]
        f = frame(spec, start)
        graph.process(f)
        rows = graph.process(frame(spec, CASES[5][1], 1))
        self.assertEqual({r.setup_id for r in rows if r.state == "triggered"}, {"SET-0005", "SET-0012"})


if __name__ == "__main__":
    unittest.main()
