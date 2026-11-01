"""Hand-authored variant narratives, plus boundary cases that differ from base.
Feature-level checks exercise the interpreter, not real-market detector accuracy.
"""
from copy import deepcopy
import unittest
from recognition.engine import GraphRecognizer
from recognition.specs import load_specs
from .graph_cases import CASES
from .test_graphs import frame


def variant_cases():
    cases = {
        (1, "intraday_position"): [CASES[1][0] | {"opening": False}, *CASES[1][1:]],
        (4, "active_cross"): [CASES[4][0] | {"ib_break": True}],
        (5, "extended_window"): CASES[5],
        (6, "ema_reference"): [*CASES[6][:3], CASES[6][3] | {"ema_near": True}, CASES[6][4]],
        (7, "parabolic"): [CASES[7][0] | {"parabolic": True}, *CASES[7][1:]],
        (8, "lower_low"): [CASES[8][0], {"directional_low": 10.9}, *CASES[8][2:]],
        (8, "higher_low"): [CASES[8][0], {"directional_low": 11.1}, *CASES[8][2:]],
        (8, "double_test"): CASES[8],
        (9, "ross_10s"): CASES[9],
        (10, "strict_tick_hold"): CASES[10],
        (10, "double_test"): [*CASES[10][:2], {"resume": True}, CASES[10][1], CASES[10][2]],
        (13, "inner_edge"): [CASES[13][0] | {"inner_edge": 10.8}, CASES[13][1]],
        (13, "close_inside"): [{"trend": .8, "close_double_inside": True, "mother_edge": 11, "mother_risk": 9}, CASES[13][1]],
        (13, "triple_inside"): [{"trend": .8, "triple_inside": True, "mother_edge": 11, "mother_risk": 9}, CASES[13][1]],
        (20, "source_open"): [{"prior_tail_open": True, "prior_tail_risk": 9, "instant_open": True, "price_basis": "synthetic_observation"}],
        (20, "source_0800_BST"): [CASES[20][0], CASES[20][1] | {"london_0800": True, "volume_increase": 1.4}],
        (21, "tight_flag"): [CASES[21][0] | {"tight_flag_edge": 11, "compression": .8}, CASES[21][1]],
        (21, "sai_gap"): [{"opening": True, "sai_catalyst_gap": True, "near_pm_edge": True, "pm_edge": 11}, CASES[21][1]],
        (22, "early_v"): [CASES[22][0], {"signal": True, "resume": True}],
        (23, "direct_reclaim"): CASES[23][:2],
        (25, "touch_early"): CASES[25][:1],
        (27, "public_price_proxy"): CASES[27],
        (28, "direct_first_break"): CASES[28][:1],
        (29, "window90"): [{"news_fresh90": True, "impulse": .9}, {"news_fresh90": True, "resume": True}],
        (36, "edge_limit_observation"): CASES[36][:2],
        (37, "two_bracket_touch"): [CASES[37][0], {"value_two_brackets": True}],
        (37, "volume_va_proxy"): [{"volume_value_return": True, "volume_value_entry": 11, "volume_value_risk": 9}, {"volume_value_inside60": True}],
        (38, "double_hold"): [{"profile_stretch": True, "extreme": 11}, {"failed_extension": True}, {"resume": True}, {"directional_low": 11}, {"resume": True}],
        (38, "early_failure"): CASES[38][:2],
        (42, "ratio150"): [{"stacked150": True, "imbalance150_edge": 11, "imbalance150_risk": 9}, *CASES[42][1:]],
        (48, "same_side_band"): [{"noise_break": True, "same_band": 11, "half_hour": True}],
        (48, "same_side_vwap"): [{"noise_break": True, "vwap_band": 11, "half_hour": True}],
        (49, "joint_sign"): [{"last_half_hour": True, "joint_sign": True}],
        (50, "early_extreme"): CASES[50][:1],
    }
    return {(f"SET-{n:04}", variant): deepcopy(steps) for (n, variant), steps in cases.items()}


class VariantTests(unittest.TestCase):
    def setUp(self):
        self.specs = {(s["setup_id"], s["variant_id"]): s for s in load_specs(include_variants=True)}

    def run_path(self, key, steps, caps=None):
        spec = self.specs[key]
        graph = GraphRecognizer([spec])
        rows = []
        for i, step in enumerate(steps):
            f = frame(spec, step, i, caps=caps)
            if key[1] == "source_open":
                f.observation_kind = "open_price"
            rows.extend(graph.process(f))
        return rows

    def test_all_thirty_four_variant_paths(self):
        cases = variant_cases()
        self.assertEqual(set(cases), {k for k in self.specs if k[1] != "base"})
        self.assertEqual(len(cases), 34)
        for key, steps in cases.items():
            with self.subTest(variant=key):
                self.assertTrue(any(r.state == "triggered" for r in self.run_path(key, steps)))

    def test_variants_do_not_confirm_missing_data(self):
        for key, steps in variant_cases().items():
            with self.subTest(variant=key):
                self.assertFalse(any(r.state in {"formed", "triggered"} for r in self.run_path(key, steps, caps=set())))

    def test_retest_variants_have_distinct_frozen_price_geometry(self):
        for low, allowed in ((10.9, "lower_low"), (11., "double_test"), (11.1, "higher_low")):
            for variant in ("lower_low", "double_test", "higher_low"):
                path = [CASES[8][0], {"directional_low": low}, *CASES[8][2:]]
                rows = self.run_path(("SET-0008", variant), path)
                self.assertEqual(any(r.state == "triggered" for r in rows), variant == allowed)

    def test_early_variants_do_not_satisfy_later_base_confirmation(self):
        for n, variant in ((22, "early_v"), (23, "direct_reclaim"), (36, "edge_limit_observation"), (38, "early_failure"), (50, "early_extreme")):
            path = variant_cases()[(f"SET-{n:04}", variant)]
            with self.subTest(variant=variant):
                self.assertFalse(any(r.state == "triggered" for r in self.run_path((f"SET-{n:04}", "base"), path)))

    def test_open_observation_cannot_be_reconstructed_from_minute_bar(self):
        spec = self.specs[("SET-0020", "source_open")]
        graph = GraphRecognizer([spec])
        f = frame(spec, variant_cases()[("SET-0020", "source_open")][0])
        self.assertEqual(graph.process(f), [])
