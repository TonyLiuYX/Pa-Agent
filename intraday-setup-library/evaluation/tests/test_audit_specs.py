"""Source boundary regressions prompted by the external 250-situation audit."""
import unittest

from recognition.engine import GraphRecognizer, RecognitionEngine
from recognition.specs import load_specs
from .graph_cases import CASES
from .support import session, reference, bar
from .test_graphs import frame


class AuditSpecificationTests(unittest.TestCase):
    def run_graph(self, spec, path):
        graph = GraphRecognizer([spec])
        return [row for i, values in enumerate(path)
                for row in graph.process(frame(spec, values, i))]

    def test_prior_failure_entry_uses_intrabar_endpoint(self):
        engine = RecognitionEngine(["SET-0001"])
        inputs = [session({"references": {"prev_high": reference(12), "prev_low": reference(10),
                                          "prev_close": reference(11)}}),
                  bar(1, 9.9, 10, 9.7, 9.8),
                  bar(2, 9.8, 10.2, 9.8, 10.15),
                  bar(3, 10.15, 10.3, 10.1, 10.18)]
        rows = [r for e in inputs for r in engine.feed(e)]
        complete = [r for r in rows if r.state == "triggered" and r.data_status == "complete"]
        self.assertEqual(len(complete), 1)
        self.assertEqual(complete[0].direction, 1)
        endpoint = next(c for c in complete[0].conditions if c["feature"] == "directional_extreme")
        self.assertGreater(endpoint["actual"], 10.2)

    def test_wide_hl_ll_retests_use_explicit_versions(self):
        specs = load_specs(["SET-0008"], include_variants=True)
        for low, expected in ((11.5, "higher_low"), (10.5, "lower_low")):
            path = [CASES[8][0], {"directional_low": low}, *CASES[8][2:]]
            for spec in specs:
                with self.subTest(low=low, variant=spec["variant_id"]):
                    triggered = any(r.state == "triggered" for r in self.run_graph(spec, path))
                    self.assertEqual(triggered, spec["variant_id"] == expected)

    def test_last_flag_waits_for_frozen_signal_endpoint(self):
        spec = load_specs(["SET-0011"])[0]
        combined = CASES[11][1] | CASES[11][2] | {"signal_edge": 13, "directional_extreme": 12.5}
        path = [CASES[11][0], combined, {"resume": True, "directional_extreme": 12.8}]
        self.assertFalse(any(r.state == "triggered" for r in self.run_graph(spec, path)))
        rows = self.run_graph(spec, [*path, {"directional_extreme": 13.1}])
        self.assertTrue(any(r.state == "triggered" and r.anchor["captures"]["trigger"] == 13 for r in rows))

    def test_prior_range_edge_can_break_in_failure_observation(self):
        spec = load_specs(["SET-0034"])[0]
        rows = self.run_graph(spec, [CASES[34][0] | CASES[34][1]])
        self.assertTrue(any(r.state == "triggered" for r in rows))

    def test_fast_vwap_observation_keeps_required_data(self):
        spec = load_specs(["SET-0024"])[0]
        self.assertTrue({0, 1, 5, 10}.issubset(spec["scales"]))
        for seconds in (1, 5, 10):
            graph = GraphRecognizer([spec])
            caps = set(spec["requirements"]) - {"exact_vwap"}
            rows = [r for i, values in enumerate(CASES[24])
                    for r in graph.process(frame(spec, values, i, caps=caps, seconds=seconds))]
            self.assertFalse(any(r.state == "triggered" for r in rows))

    def test_only_compatible_stage_pairs_share_frames(self):
        specs = {s["setup_id"]: s for s in load_specs()}
        for number in (22, 24, 30, 32, 33, 50):
            with self.subTest(setup=number):
                self.assertFalse(any(st.get("same_frame") for st in specs[f"SET-{number:04}"]["stages"]))
        for number in (1, 6, 7, 11, 18, 19, 22, 23):
            with self.subTest(endpoint=number):
                self.assertFalse(specs[f"SET-{number:04}"]["stages"][-1].get("same_frame", False))


if __name__ == "__main__":
    unittest.main()
