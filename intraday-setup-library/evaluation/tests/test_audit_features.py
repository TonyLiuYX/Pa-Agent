"""Raw observations for source-supported gaps found by the October setup audit.

These synthetic regressions test causal feature semantics, not market accuracy.
"""
from datetime import timedelta
import unittest

from recognition.data import SymbolData
from recognition.engine import RecognitionEngine
from recognition.features import make_frame, _price_features, _profile_features, _flow_features, _news_features, _cross_features
from recognition.models import Bar
from .support import OPEN, bar, event, reference, session


def candle(minute, close, opening=None, volume=1000, width=.05):
    opening = close if opening is None else opening
    return Bar(OPEN + timedelta(minutes=minute - 1), OPEN + timedelta(minutes=minute),
               opening, max(opening, close) + width, min(opening, close) - width,
               close, volume, close * volume, (f"synthetic-bar-{minute}-{close}",))


def state(symbol="SYN", refs=None, extra=None):
    data = SymbolData(symbol, "SYN")
    data.update(session({"references": {k: reference(v) for k, v in (refs or {}).items()}, **(extra or {})}, symbol=symbol))
    return data


def price_features(data, bars, direction=1, rvol=None, vol=.2):
    f = {"rvol": rvol, "impulse": 0., "observation_scale_seconds": 60}
    _price_features(f, direction, bars, vol, .01, data)
    return f


def cross_features(data, leader, own, direction=1):
    data.bars[60] = own
    f = {"overnight_failure": False, "tolerance": .01}
    _cross_features(f, direction, [leader], [leader.symbol], data, own, 60)
    return f


class AuditFeatureRegressionTests(unittest.TestCase):
    def test_two_shallow_opening_bars_can_precede_vwap_reclaim(self):
        engine = RecognitionEngine(["SET-0023"])
        engine.feed(session())
        rows = []
        for values in ((1, 100, 100.03, 99.90, 99.95), (2, 99.95, 99.98, 99.87, 99.90),
                       (3, 99.90, 100.12, 99.89, 100.10), (4, 100.10, 100.25, 100.08, 100.22)):
            rows += engine.feed(bar(*values))
            if values[0] == 3:
                f = engine.scale_frames[("SYN", "SYN", 60, "bar")].values[1]
                self.assertTrue(f["weak_open_drop"])
                self.assertTrue(f["vwap_reclaim"])
        self.assertTrue(any(r.state == "triggered" and r.scale_seconds == 60 for r in rows))

    def test_weak_open_decline_rejects_bear_drive_and_missing_open(self):
        data = state()
        strong = [candle(1, 99.5, 100), candle(2, 99., 99.5), candle(3, 99.2, 99.)]
        self.assertFalse(price_features(data, strong)["weak_open_drop"])
        shallow = [candle(1, 99.95, 100), candle(2, 99.9, 99.95), candle(3, 100.1, 99.9)]
        self.assertIsNone(price_features(data, shallow[1:])["weak_open_drop"])
        self.assertIsNone(price_features(data, [shallow[0], candle(3, 99.9), candle(4, 100.1)])["weak_open_drop"])

    def test_volume_cliff_keeps_rvol_and_adds_earlier_participation_ratio(self):
        data = state()
        push = [candle(i, 100 + i * .02, 100 + (i - 1) * .02) for i in range(1, 7)]
        current = candle(7, 100., 100.12, volume=200)
        f = price_features(data, push + [current], -1, .8)
        self.assertTrue(f["volume_cliff"])
        self.assertFalse(f["volume_cliff_rvol"])
        self.assertEqual(f["volume_cliff_reference_volume"], 1000)
        self.assertEqual(f["volume_cliff_drive_ratio"], .2)
        self.assertFalse(price_features(data, push + [candle(7, 100., volume=201)], -1, .8)["volume_cliff"])
        self.assertTrue(price_features(data, push + [candle(7, 100., volume=1000)], -1, .49)["volume_cliff"])
        self.assertIsNone(price_features(data, [push[0], push[2], current], -1)["volume_cliff"])
        self.assertIsNone(price_features(data, [current], -1)["volume_cliff"])

    def test_news_acceptance_reference_does_not_replace_prior_session_extreme(self):
        data = state(refs={"prev_high": 103., "prev_low": 99., "prev_close": 100., "prior_acceptance_level": 100.5})
        data.bars[60] = [candle(1, 100.7, 100.4)]
        f = make_frame(data, 60, data.bars[60][-1], data.bars[60][-1].end, {}).values[1]
        self.assertTrue(f["second_day_break"])
        self.assertEqual(f["second_day_edge"], 100.5)
        self.assertFalse(f["prior_break"])
        self.assertEqual(f["prior_edge"], 103.)
        data.bars[60] = [candle(1, 100.4)]
        self.assertFalse(make_frame(data, 60, data.bars[60][-1], OPEN + timedelta(minutes=1), {}).values[1]["second_day_break"])
        absent = state()
        absent.bars[60] = [candle(1, 100.7)]
        self.assertIsNone(make_frame(absent, 60, absent.bars[60][-1], absent.bars[60][-1].end, {}).values[1]["second_day_break"])

    def test_prior_news_survives_unrelated_updates_but_explicit_correction_denies_it(self):
        data = state(refs={"prev_close": 100.})
        old = event("news", OPEN - timedelta(days=1), {"event_key": "contract", "source": "synthetic", "is_new": True})
        update = event("news", OPEN, {"event_key": "product", "source": "synthetic", "is_new": True})
        f = {"signal_risk": 99.}
        _news_features(f, 1, [old, update], data, candle(1, 100.), OPEN + timedelta(minutes=1), 100., 100.)
        self.assertTrue(f["second_day_news"])
        self.assertIn(old.event_id, f["second_day_news_event_ids"])
        correction = event("news", OPEN, {"event_key": "contract", "source": "synthetic", "invalidates_prior": True})
        _news_features(f, 1, [old, correction], data, candle(1, 100.), OPEN + timedelta(minutes=1), 100., 100.)
        self.assertFalse(f["second_day_news"])
        future = event("news", OPEN + timedelta(minutes=2), {"event_key": "contract", "source": "synthetic", "invalidates_prior": True})
        _news_features(f, 1, [old, future], data, candle(1, 100.), OPEN + timedelta(minutes=1), 100., 100.)
        self.assertTrue(f["second_day_news"])
        _news_features(f, 1, [update], state(), candle(1, 100.), OPEN + timedelta(minutes=1), None, 100.)
        self.assertIsNone(f["second_day_news"])

    def test_leader_break_remains_known_during_hold_and_is_mirrored(self):
        for direction in (1, -1):
            own = state(refs={"leadership": {"symbol": "LEAD", "correlation": .8, "lag_bars": 1}})
            leader = state("LEAD", refs={"key_level": 100.})
            transform = lambda p: 100 + direction * (p - 100)
            leader.bars[60] = [candle(i, transform(p)) for i, p in enumerate((99.9, 100.2, 100.21, 100.2, 100.21, 100.2), 1)]
            own_bars = [candle(i, transform(99.8 + .02 * i)) for i in range(1, 7)]
            self.assertTrue(cross_features(own, leader, own_bars, direction)["leader_break"])
            leader.bars[60][-1] = candle(6, transform(99.9))
            self.assertFalse(cross_features(own, leader, own_bars, direction)["leader_break"])

    def test_cross_context_is_unknown_when_unsynchronized_or_gapped_and_ignores_future(self):
        own = state(refs={"leadership": {"symbol": "LEAD", "correlation": .8, "lag_bars": 1}})
        leader = state("LEAD", refs={"key_level": 100.})
        leader.bars[60] = [candle(i, p) for i, p in enumerate((99.9, 100.2, 100.21, 100.2, 100.21, 100.2), 1)]
        own_bars = [candle(i, 99.8 + .02 * i) for i in range(1, 7)]
        expected = cross_features(own, leader, own_bars)
        leader.bars[60].append(candle(7, 99.5))
        self.assertEqual(cross_features(own, leader, own_bars), expected)
        leader.bars[60] = leader.bars[60][1:]
        self.assertIsNone(cross_features(own, leader, own_bars)["leader_break"])
        self.assertIsNone(cross_features(own, leader, own_bars)["relative_strength"])

    def test_resistance_to_market_pressure_is_remembered_until_support_breaks(self):
        for direction in (1, -1):
            own, leader = state(), state("LEAD")
            transform = lambda p: 100 + direction * (p - 100)
            stocks = [100.] * 6 + [100., 100.1, 100.2, 100.3, 100.4, 100.5]
            market = [101., 100.8, 100.6, 100.4, 100.2, 100.] + [100., 100.2, 100.4, 100.6, 100.8, 101.]
            own_bars = [candle(i, transform(p)) for i, p in enumerate(stocks, 1)]
            leader.bars[60] = [candle(i, transform(p)) for i, p in enumerate(market, 1)]
            f = cross_features(own, leader, own_bars, direction)
            self.assertLess(f["relative_strength_current"], .6)
            self.assertGreaterEqual(f["relative_strength_memory"], .6)
            self.assertGreaterEqual(f["relative_strength"], .6)
            self.assertTrue(f["market_confirm"])
            broken = list(own_bars)
            broken[6] = candle(7, transform(99.8))
            self.assertIsNone(cross_features(own, leader, broken, direction)["relative_strength_memory"])
            self.assertIsNone(cross_features(own, leader, own_bars[-6:], direction)["relative_strength_memory"])
            declining = [candle(i, transform(100 - .04 * i)) for i in range(1, 7)] + own_bars[6:]
            self.assertIsNone(cross_features(own, leader, declining, direction)["relative_strength_memory"])

    def test_tpo_ledge_clusters_one_tick_noise_but_requires_the_full_edge_band(self):
        data = state()
        data.tpo.update({0: {9900, 10000}, 1: {9900, 10001}, 2: {9900, 9999}})
        current, prior = candle(91, 100.05, 99.99), [candle(90, 99.99)]
        f = {}
        _profile_features(f, 1, None, None, None, data, current, prior, .01, .1)
        self.assertTrue(f["ledge_break"])
        self.assertAlmostEqual(f["ledge"], 100.01)
        _profile_features(f, 1, None, None, None, data, candle(91, 100.005), prior, .01, .1)
        self.assertFalse(f["ledge_break"])
        data.tpo[2] = {9900, 10004}
        _profile_features(f, 1, None, None, None, data, current, prior, .01, .1)
        self.assertFalse(f["ledge_break"])
        data.tpo.pop(2)
        _profile_features(f, 1, None, None, None, data, current, prior, .01, .1)
        self.assertIsNone(f["ledge_break"])

    def test_failed_tpo_ledge_uses_clustered_opposite_edges(self):
        data = state()
        data.tpo.update({0: {9980, 10080}, 1: {9981, 10080}, 2: {9979, 10080}})
        tp = {"kind": "tpo", "venue": "SYN", "val": 99., "vah": 101.}
        f = {}
        _profile_features(f, 1, None, tp, None, data, candle(91, 100.05), [candle(90, 99.99)], .01, .1)
        self.assertTrue(f["ledge_failed"])
        _profile_features(f, 1, None, tp, None, data, candle(91, 101.01), [candle(90, 99.99)], .01, .1)
        self.assertFalse(f["ledge_failed"])

    def test_partial_replenishment_is_observable_despite_net_queue_consumption(self):
        data = state()
        current = [{"price": 100., "size": 40., "side": "sell", "time": OPEN + timedelta(seconds=s)} for s in (10, 20)]
        previous = [{"price": 100., "size": 100., "side": "sell", "time": OPEN - timedelta(seconds=1)}]
        def depth(second, amount):
            return event("depth", OPEN + timedelta(seconds=second), {"mode": "snapshot", "bids": [[100., amount]], "asks": [[100.01, 100.]]})
        depths = [depth(0, 100), depth(15, 80), depth(30, 70)]
        f = {}
        _flow_features(f, 1, True, current, previous, depths, data, candle(1, 100.), .1, .01)
        self.assertTrue(f["passive_support"])
        self.assertEqual(f["passive_reload_count"], 2)
        self.assertEqual(f["passive_reload_volume"], 50.)
        consumed = [depth(0, 100), depth(15, 60), depth(30, 20)]
        _flow_features(f, 1, True, current, previous, consumed, data, candle(1, 100.), .1, .01)
        self.assertFalse(f["passive_support"])
        _flow_features(f, 1, True, current, previous, [], data, candle(1, 100.), .1, .01)
        self.assertIsNone(f["passive_support"])
        _flow_features(f, 1, False, current, previous, depths, data, candle(1, 100.), .1, .01)
        self.assertIsNone(f["passive_support"])
        partial = event("depth", OPEN + timedelta(seconds=30), {"mode": "snapshot", "bids": [[100., 70]], "asks": [[100.01, 100.]]}, quality="partial")
        _flow_features(f, 1, True, current, previous, depths[:2] + [partial], data, candle(1, 100.), .1, .01)
        self.assertIsNone(f["passive_support"])

    def test_overnight_failure_can_turn_before_retouch_and_remains_unknown_without_coverage(self):
        for direction in (1, -1):
            data = state(extra={"premarket_complete": True})
            transform = lambda p: 100 + direction * (p - 100)
            pm = [candle(-2, transform(99.8), transform(99.7)), candle(-1, transform(100.4), transform(99.8))]
            cash = candle(1, transform(100.05), transform(100.2))
            data.bars[60] = pm + [cash]
            f = make_frame(data, 60, cash, cash.end, {}).values[-direction]
            self.assertTrue(f["overnight_failure"])
            improving = candle(1, transform(100.6), transform(100.2))
            data.bars[60] = pm + [improving]
            self.assertFalse(make_frame(data, 60, improving, improving.end, {}).values[-direction]["overnight_failure"])
            data.session["premarket_complete"] = False
            data.bars[60] = pm + [cash]
            self.assertIsNone(make_frame(data, 60, cash, cash.end, {}).values[-direction]["overnight_failure"])


if __name__ == "__main__":
    unittest.main()
