"""Exercise execution venue isolation through committed Runner and HTTP state."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import http.client
import json
from tempfile import TemporaryDirectory
import unittest

from paper import PaperSimulator
from recognition.models import MarketEvent
from runtime.config import RuntimeConfig
from runtime.runner import Runner
from runtime.server import WorkbenchServer

OPEN = datetime(2026, 9, 30, 13, 30, tzinfo=timezone.utc)


def event(kind, offset_seconds, venue="IEX", **payload):
    at = OPEN + timedelta(seconds=offset_seconds)
    defaults = {"bid": 99.99, "ask": 100.01} if kind == "quote" else {}
    return MarketEvent.from_dict({"event_id": f"venue-fixture-{venue}-{kind}-{offset_seconds}", "symbol": "SPY",
        "venue": venue, "kind": kind, "event_time": at.isoformat(), "arrival_time": at.isoformat(),
        "payload": {**defaults, **payload}})


def session(venue="IEX", with_prior=False):
    prior = [{"date": "2026-09-29", "as_of": "2026-09-29T20:00:00+00:00", "source": "authored_previous_session",
              "complete": True, "cash_open": 100, "close": 100, "high": 101, "low": 99, "volume": 100,
              "closes": {"09:31": 100}, "volumes": {"09:31": 10}}] if with_prior else []
    return event("session", 0, venue, open_time=OPEN.isoformat(), close_time=(OPEN + timedelta(minutes=390)).isoformat(),
                 tick_size=.01, history=prior)


def bar(venue="IEX", minute=1, close=100):
    return event("bar", minute * 60, venue, open=100, high=max(101, close), low=min(99, close), close=close,
                 volume=10, seconds=60)


class ExecutionVenueIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.config = RuntimeConfig(mode="replay", source_kind="synthetic", symbols=("SPY",), pace_seconds=0)
        self.runner = Runner(self.directory.name, self.config)
        self.server = None

    def tearDown(self):
        if self.server:
            self.server.close()
        self.runner.close()
        self.directory.cleanup()

    def request(self, path):
        if self.server is None:
            self.server = WorkbenchServer(self.runner, port=0).start()
        client = http.client.HTTPConnection("127.0.0.1", self.server.server_port, timeout=3)
        try:
            client.request("GET", path)
            response = client.getresponse()
            self.assertEqual(response.status, 200)
            return json.loads(response.read())
        finally:
            client.close()

    def bind_both_streams(self):
        self.runner.ingest(session())
        self.runner.ingest(session("OTHER", with_prior=True))
        self.runner.ingest(event("status", 0, connected=True, connection_state="connected", gap=False, stream="all"))
        self.assertEqual(set(self.runner.engine.universe), {("SPY", "IEX"), ("SPY", "OTHER")})
        self.assertEqual(self.runner.paper.sessions["SPY"]["venue"], "IEX")

    def test_foreign_market_connection_does_not_disconnect_or_reconnect_execution(self):
        self.bind_both_streams()
        self.runner.ingest(event("status", 1, "OTHER", connected=False, connection_state="disconnected", gap=True, stream="all"))
        self.assertEqual(self.request("/api/status")["state"], "RUNNING")
        self.assertEqual(self.runner.paper.disconnected, set())
        self.runner.ingest(event("status", 2, connected=False, connection_state="disconnected", gap=True, stream="all"))
        self.assertEqual(self.request("/api/status")["state"], "DISCONNECTED")
        self.runner.ingest(event("status", 3, "OTHER", connected=True, connection_state="connected", gap=False, stream="all"))
        self.assertEqual(self.request("/api/status")["state"], "DISCONNECTED")
        self.assertEqual(self.runner.paper.disconnected, {"SPY"})

    def test_foreign_quote_and_bar_remain_auditable_without_overwriting_latest_execution_views(self):
        self.bind_both_streams()
        own_quote, own_bar = event("quote", 1), bar()
        self.runner.ingest(own_quote)
        self.runner.ingest(own_bar)
        quote_before = deepcopy(self.runner.store.meta("quote:SPY"))
        bar_before = deepcopy(self.runner.store.meta("bar:SPY"))
        foreign_quote, foreign_bar = event("quote", 61, "OTHER", bid=1, ask=1.02), bar("OTHER", 2, close=1)
        quote_seq, _ = self.runner.ingest(foreign_quote)
        bar_seq, _ = self.runner.ingest(foreign_bar)
        self.assertEqual(self.runner.store.meta("quote:SPY"), quote_before)
        self.assertEqual(self.runner.store.meta("bar:SPY"), bar_before)
        self.assertEqual(self.runner.paper.marks["SPY"]["venue"], "IEX")
        raw = {r["seq"]: r for r in self.request("/api/events?limit=1000")["items"]}
        self.assertEqual(raw[quote_seq]["payload"], foreign_quote.to_dict())
        self.assertEqual(raw[bar_seq]["payload"], foreign_bar.to_dict())
        self.assertTrue(raw[quote_seq]["processed"])
        bars = self.request("/api/bars?after=0&limit=1000")["items"]
        self.assertTrue(any(row["event_seq"] == bar_seq and row["venue"] == "OTHER" for row in bars))
        self.assertTrue(self.runner.store.conn.execute("SELECT 1 FROM records WHERE topic='account' AND event_seq=?", (quote_seq,)).fetchone())
        self.assertEqual(self.request("/api/account")["balance_cents"], 10_000_000)

    def test_http_coverage_does_not_combine_foreign_capabilities_with_bound_quote(self):
        self.bind_both_streams()
        self.runner.ingest(bar())
        self.runner.ingest(bar("OTHER"))
        self.runner.ingest(event("quote", 61))
        self.runner.checkpoint()  # Publish actual engine coverage, no authored meta substitution.
        coverage = {r["rule_id"]: r for r in self.request("/api/coverage")["items"]}
        row = coverage["SET-0001:base"]
        symbol = row["per_symbol"]["SPY"]
        self.assertTrue(row["recognition_eligible"])
        self.assertFalse(row["auto_simulate"])
        self.assertTrue(any(key.startswith("SPY|OTHER|") and not missing for key, missing in symbol["observations"].items()))
        self.assertEqual(symbol["execution_venue"], "IEX")
        self.assertTrue(symbol["execution_observations"])
        self.assertTrue(all(key.startswith("SPY|IEX|") for key in symbol["execution_observations"]))
        self.assertTrue(all("prior_session" in missing for missing in symbol["execution_observations"].values()))
        self.assertFalse(symbol["execution_eligible"])
        # A rule with genuinely available IEX inputs remains executable.
        self.assertTrue(coverage["SET-0005:base"]["auto_simulate"])

    def test_global_source_status_keeps_global_scope_and_preserves_venue_halt(self):
        self.bind_both_streams()
        self.runner.source_status("DISCONNECTED")
        self.runner.drain()
        self.assertEqual(self.request("/api/status")["state"], "DISCONNECTED")
        self.assertEqual(self.runner.paper.disconnected, {"SPY"})
        self.runner.ingest(event("status", 1, halted=True, gap=False))
        self.runner.source_status("CONNECTED")
        self.runner.drain()
        self.assertEqual(self.request("/api/status")["state"], "RUNNING")
        self.assertEqual(self.runner.paper.disconnected, set())
        self.assertEqual(self.runner.paper.halted, {"SPY"})
        self.assertEqual(self.runner.store.meta("execution_connectivity")["halted"], ["SPY"])
        states = [r for r in self.runner.store.journal() if r["kind"] == "source_status"]
        self.assertEqual([r["payload"]["state"] for r in states], ["DISCONNECTED", "CONNECTED"])
        self.assertTrue(all(r["processed"] for r in states))

    def test_recovery_rebuilds_execution_binding_with_checkpoint_and_committed_tail(self):
        for checkpoint in (False, True):
            with self.subTest(checkpoint=checkpoint), TemporaryDirectory() as directory:
                runner = Runner(directory, self.config)
                try:
                    runner.ingest(session())
                    runner.ingest(session("OTHER", with_prior=True))
                    runner.ingest(event("status", 0, connected=True, connection_state="connected", gap=False, stream="all"))
                    runner.ingest(event("quote", 1))
                    runner.ingest(bar())
                    if checkpoint:
                        runner.checkpoint()
                    runner.ingest(event("quote", 61, "OTHER"))
                    runner.ingest(bar("OTHER", 2))
                    runner.ingest(event("status", 121, "OTHER", connected=False, connection_state="disconnected", gap=True, stream="all"))
                    expected = runner.paper.snapshot()
                    cursor, derived = runner.cursor, runner.store.derived_hash()
                    runner.store.set_meta("execution_sessions", {"SPY": {"venue": "OTHER"}})
                    runner.close()
                    runner = Runner(directory, self.config)
                    self.assertEqual(runner.cursor, cursor)
                    self.assertEqual(runner.store.derived_hash(), derived)
                    self.assertEqual(runner.paper.snapshot(), expected)
                    self.assertEqual(runner.store.meta("execution_sessions"), runner.paper.sessions)
                    self.assertEqual(runner.store.meta("execution_sessions")["SPY"]["venue"], "IEX")
                    self.assertEqual(runner.store.meta("quote:SPY")["venue"], "IEX")
                    self.assertEqual(runner.store.meta("bar:SPY")["venue"], "IEX")
                    self.assertEqual(runner.state, "RUNNING")
                finally:
                    runner.close()


class PreSessionPaperStatusTests(unittest.TestCase):
    def test_unregistered_foreign_status_cannot_poison_first_execution_session(self):
        simulator = PaperSimulator()
        simulator.on_event(event("status", 0, "OTHER", halted=True, gap=True, stream="all"), [], 1)
        simulator.on_event(session(), [], 2)
        self.assertEqual(simulator.halted, set())
        self.assertEqual(simulator.disconnected, set())
        self.assertEqual(simulator.sessions["SPY"]["venue"], "IEX")
        simulator.on_event(event("status", 1, halted=True, gap=True, stream="all"), [], 3)
        self.assertEqual(simulator.halted, {"SPY"})
        self.assertEqual(simulator.disconnected, {"SPY"})


if __name__ == "__main__":
    unittest.main()
