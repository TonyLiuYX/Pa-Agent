import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from dataclasses import replace
from datetime import timedelta
from unittest.mock import patch

from evaluation.tests.raw_scenarios import price_scenarios
from evaluation.tests.support import OPEN, bar, session
from recognition.engine import RecognitionEngine
from recognition.models import MarketEvent
from recognition.provenance import file_sha256
from runtime.backfill import analyze_backfill
from runtime.config import RuntimeConfig, canonical
from runtime.store import Store


class BackfillTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.source = self.root / "source"
        self.store = Store(self.source)
        self.store.set_meta("configuration", RuntimeConfig(mode="replay", source_kind="synthetic", symbols=("SYN",)).to_dict())
        self.store.set_meta("run_id", "original-live-or-replay-account")
        self.store.set_meta("identity", {"algorithm_sha256": "original_algorithm"})
        self.store.set_meta("account", {"cash": 123456.75, "positions": [{"symbol": "SYN", "quantity": 7}]})

    def tearDown(self):
        self.store.close()
        self.temporary.cleanup()

    def append(self, event, *, kind="backfill", committed=True):
        seq, created = self.store.append(kind, "original-fixture", event.to_dict(), event.event_id)
        self.assertTrue(created)
        if committed:
            with self.store.transaction() as connection:
                self.store.record(connection, "runtime", seq, 0, {"kind": kind, "event_id": event.event_id})
                self.store.set_meta("processed_cursor", seq, connection)
                connection.execute("UPDATE journal SET processed=1 WHERE seq=?", (seq,))
        return seq

    def source_hashes(self):
        with self.store.lock:
            digest = hashlib.sha256()
            for table in ("meta", "journal", "records", "checkpoints"):
                for row in self.store.conn.execute("SELECT * FROM " + table + " ORDER BY rowid"):
                    digest.update((canonical(list(row)) + "\n").encode())
        return digest.hexdigest(), self.store.derived_hash(), self.store.meta("account")

    def read(self, target, filename):
        return [json.loads(line) for line in (target / filename).read_text().splitlines() if line]

    def hikkake(self):
        return next(s for s in price_scenarios() if s.scenario_id == "hikkake-long")

    def test_missing_session_is_blocked_and_preserves_raw_inputs(self):
        self.append(bar(1, 10, 10.2, 9.9, 10.1))
        before = self.source_hashes()
        output = self.root / "missing"
        report = analyze_backfill(self.source, output)
        self.assertEqual(report["status"], "blocked")
        self.assertEqual(report["execution"], "recognition_only")
        self.assertEqual(report["missing_context"][0]["reason"], "missing_received_session_for_market_interval")
        self.assertEqual(report["analyzed_backfill_count"], 0)
        self.assertEqual(len(self.read(output, "events.jsonl")), 1)
        self.assertEqual(self.read(output, "detections.jsonl"), [])
        self.assertEqual(before, self.source_hashes())

    def test_current_day_session_does_not_authorize_yesterdays_bar(self):
        self.append(session(), kind="market")
        self.append(bar(1, 10, 10.2, 9.9, 10.1, op=OPEN-timedelta(days=1), arrival=OPEN))
        report = analyze_backfill(self.source, self.root / "wrong-day")
        self.assertEqual(report["status"], "blocked")
        self.assertEqual(report["context_event_count"], 0)
        self.assertEqual(report["missing_context"][0]["reason"], "missing_received_session_for_market_interval")

    def test_context_received_after_backfill_cannot_be_borrowed(self):
        self.append(replace(session(), arrival_time=OPEN+timedelta(minutes=10)), kind="market")
        self.append(bar(1, 10, 10.2, 9.9, 10.1))
        report = analyze_backfill(self.source, self.root / "future-receipt")
        self.assertEqual(report["status"], "blocked")
        self.assertEqual(report["context_event_count"], 0)

    def test_complete_backfill_triggers_without_account_or_source_mutation(self):
        scenario = self.hikkake()
        for event in scenario.events:
            self.append(event)
        before = self.source_hashes()
        output = self.root / "complete"
        # A recognition-only path must not instantiate either execution owner.
        with patch("runtime.runner.Runner", side_effect=AssertionError("must not create runner")), patch("paper.PaperSimulator", side_effect=AssertionError("must not create paper account")):
            report = analyze_backfill(self.source, output)
        self.assertEqual(report["status"], "completed")
        self.assertEqual(report["selected_backfill_count"], 5)
        self.assertEqual(report["analyzed_backfill_count"], 5)
        self.assertEqual(report["source_kind"], "synthetic")
        actual = self.read(output, "detections.jsonl")
        self.assertTrue(any(row["setup_id"] == "SET-0005" and row["variant_id"] == "base" and row["state"] == "triggered" and row["direction"] == 1 and row["data_status"] == "complete" for row in actual))
        self.assertEqual(before, self.source_hashes())
        self.assertFalse((output / "orders.jsonl").exists())
        self.assertFalse((output / "runtime.sqlite3").exists())
        self.assertEqual(len(json.loads((output / "rules.json").read_text())["rules"]), 84)
        for kind, filename in (("data", "events.jsonl"), ("rule", "rules.json"), ("detections", "detections.jsonl")):
            self.assertEqual(report["input_provenance"][kind+"_sha256"], file_sha256(output / filename))

    def test_market_context_order_and_receipt_clocks_are_preserved(self):
        scenario = self.hikkake()
        self.append(scenario.events[0], kind="market")
        events = [replace(e, arrival_time=OPEN+timedelta(minutes=10+i)) for i, e in enumerate(scenario.events[1:])]
        for event in events:
            self.append(event)
        output = self.root / "context"
        report = analyze_backfill(self.source, output)
        self.assertEqual(report["status"], "completed")
        self.assertEqual(report["context_event_count"], 1)
        inputs = self.read(output, "events.jsonl")
        self.assertEqual(inputs, [scenario.events[0].to_dict(), *[e.to_dict() for e in events]])
        mappings = self.read(output, "source-inputs.jsonl")
        self.assertEqual([r["source_seq"] for r in mappings], [1, 2, 3, 4, 5])
        self.assertEqual([r["role"] for r in mappings], ["session_context", "backfill", "backfill", "backfill", "backfill"])

    def test_pending_backfill_is_excluded_and_prior_detection_prefix_is_identical(self):
        scenario = self.hikkake()
        for event in scenario.events[:4]:
            self.append(event)
        prefix_target = self.root / "prefix"
        prefix = analyze_backfill(self.source, prefix_target)
        self.assertEqual(prefix["status"], "completed")
        pending_seq = self.append(scenario.events[-1], committed=False)
        pending_target = self.root / "pending"
        pending = analyze_backfill(self.source, pending_target)
        self.assertEqual(pending["source_snapshot"]["pending_backfill_excluded"], 1)
        self.assertEqual(pending["source_snapshot"]["processed_cursor"], 4)
        self.assertEqual((pending_target / "detections.jsonl").read_bytes(), (prefix_target / "detections.jsonl").read_bytes())
        self.assertEqual((pending_target / "events.jsonl").read_bytes(), (prefix_target / "events.jsonl").read_bytes())
        with self.store.transaction() as connection:
            self.store.set_meta("processed_cursor", pending_seq, connection)
            connection.execute("UPDATE journal SET processed=1 WHERE seq=?", (pending_seq,))
        complete_target = self.root / "extended"
        analyze_backfill(self.source, complete_target)
        earlier = self.read(prefix_target, "detections.jsonl")
        extended = self.read(complete_target, "detections.jsonl")
        self.assertEqual(extended[:len(earlier)], earlier)
        self.assertTrue(any(r["setup_id"] == "SET-0005" and r["state"] == "triggered" for r in extended[len(earlier):]))

    def test_same_day_symbols_share_one_engine_and_dates_have_separate_context(self):
        first = self.hikkake()
        next_day = OPEN+timedelta(days=1)
        second = [replace(e, event_id="next-"+e.event_id, event_time=e.event_time+timedelta(days=1), arrival_time=e.arrival_time+timedelta(days=1), payload={**e.payload, **({"open_time": next_day.isoformat(), "close_time": (next_day+timedelta(minutes=390)).isoformat()} if e.kind=="session" else {})}) for e in first.events]
        self.append(first.events[0], kind="market")
        other = replace(session(symbol="QQQ"), event_id="QQQ-real-session")
        self.append(other, kind="market")
        self.append(replace(bar(1, 20, 20.2, 19.9, 20.1, symbol="QQQ"), event_id="QQQ-first-minute"))
        for event in first.events[1:]:
            self.append(event)
        for event in second:
            self.append(event)
        engines = []
        def create(*args, **kwargs):
            instance = RecognitionEngine(*args, **kwargs)
            engines.append(instance)
            return instance
        with patch("runtime.backfill.RecognitionEngine", side_effect=create):
            report = analyze_backfill(self.source, self.root / "sessions")
        self.assertEqual(report["status"], "completed")
        self.assertEqual(len(engines), 2)
        self.assertEqual(set(engines[0].universe), {("SYN", "SYN"), ("QQQ", "SYN")})
        self.assertEqual(len(report["session_input_counts"]), 2)

    def test_unknown_provenance_is_never_upgraded_to_real_market(self):
        configuration = self.store.meta("configuration")
        configuration["source_kind"] = "unknown"
        self.store.set_meta("configuration", configuration)
        for event in self.hikkake().events:
            self.append(event)
        output = self.root / "unknown"
        report = analyze_backfill(self.source, output)
        self.assertEqual(report["source_kind"], "unknown")
        self.assertFalse(any(r.get("source_kind") == "real_market" for r in self.read(output, "detections.jsonl")))

    def test_ambiguous_real_session_intervals_block_without_calendar_guessing(self):
        self.append(session(), kind="market")
        different = session(op=OPEN+timedelta(minutes=30), close=OPEN+timedelta(minutes=380))
        self.append(different, kind="market")
        self.append(bar(1, 10, 10.2, 9.9, 10.1))
        report = analyze_backfill(self.source, self.root / "ambiguous")
        self.assertEqual(report["status"], "blocked")
        self.assertEqual(report["missing_context"][0]["reason"], "ambiguous_declared_session_intervals")
        self.assertEqual(report["context_event_count"], 0)

    def test_reversed_original_receipts_are_retained_and_explicitly_blocked(self):
        self.append(session())
        first = bar(1, 10, 10.2, 9.9, 10.1, arrival=OPEN+timedelta(minutes=10))
        second = bar(2, 10.1, 10.3, 10, 10.2, arrival=OPEN+timedelta(minutes=5))
        self.append(first)
        self.append(second)
        output = self.root / "reversed-receipts"
        report = analyze_backfill(self.source, output)
        self.assertEqual(report["status"], "blocked")
        self.assertEqual(report["missing_context"][0]["reason"], "original_stream_rejected_without_reordering")
        self.assertIn("arrival time", report["missing_context"][0]["detail"])
        self.assertEqual(report["analyzed_backfill_count"], 2)
        self.assertEqual(self.read(output, "events.jsonl")[1:], [first.to_dict(), second.to_dict()])

    def test_no_committed_backfill_keeps_pending_input_out_of_analysis(self):
        self.append(session(), kind="market")
        self.append(bar(1, 10, 10.2, 9.9, 10.1), committed=False)
        output = self.root / "only-pending"
        report = analyze_backfill(self.source, output)
        self.assertEqual(report["status"], "blocked")
        self.assertEqual(report["missing_context"], [{"reason": "no_committed_backfill"}])
        self.assertEqual(report["source_snapshot"]["pending_backfill_excluded"], 1)
        self.assertEqual(self.read(output, "events.jsonl"), [])

    def test_input_capacity_blocks_without_claiming_partial_analysis_is_complete(self):
        for event in self.hikkake().events:
            self.append(event)
        before = self.source_hashes()
        for name, limits in (("byte-capacity", {"max_input_bytes": 1}), ("event-capacity", {"max_backfill_events": 2})):
            with self.subTest(name=name):
                output = self.root / name
                report = analyze_backfill(self.source, output, **limits)
                self.assertEqual(report["status"], "blocked")
                self.assertEqual(report["missing_context"][0]["reason"], "backfill_input_capacity_exceeded")
                self.assertFalse(report["input_selection_complete"])
                self.assertFalse(report["input_provenance"]["analysis_completed"])
                self.assertEqual(report["input_provenance"]["scope"], "blocked_backfill_selection")
                self.assertEqual(report["source_snapshot"]["processed_cursor"], 5)
                self.assertEqual(report["source_snapshot"]["committed_backfill_count"], 5)
                self.assertEqual(report["selected_backfill_count"], 0)
                self.assertEqual(self.read(output, "events.jsonl"), [])
                self.assertEqual(self.read(output, "detections.jsonl"), [])
                self.assertEqual(report["input_provenance"]["data_sha256"], file_sha256(output / "events.jsonl"))
        full = analyze_backfill(self.source, self.root / "after-capacity")
        limited = json.loads((self.root / "byte-capacity" / "run.json").read_text())
        self.assertEqual(full["source_snapshot"]["journal_prefix_sha256"], limited["source_snapshot"]["journal_prefix_sha256"])
        self.assertEqual(full["status"], "completed")
        self.assertEqual(before, self.source_hashes())

    def test_invalid_capacity_options_are_rejected(self):
        for value in (True, 0, -1, float("inf"), 1.5):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "positive integers"):
                analyze_backfill(self.source, self.root / "invalid-limit", max_input_bytes=value)

    def test_wal_snapshot_stays_on_one_prefix_while_another_writer_commits(self):
        import runtime.backfill as module
        for event in self.hikkake().events[:4]:
            self.append(event)
        original = module._hash_text
        committed = []
        def concurrent_commit(digest, text):
            if not committed:
                committed.append(self.append(self.hikkake().events[-1]))
            return original(digest, text)
        output = self.root / "wal-snapshot"
        with patch("runtime.backfill._hash_text", side_effect=concurrent_commit):
            report = analyze_backfill(self.source, output)
        self.assertEqual(self.store.meta("processed_cursor"), 5)
        self.assertEqual(report["source_snapshot"]["processed_cursor"], 4)
        self.assertEqual(report["source_snapshot"]["committed_backfill_count"], 4)
        self.assertEqual(report["selected_backfill_count"], 4)
        self.assertEqual(len(self.read(output, "events.jsonl")), 4)
        self.assertFalse(any(r["setup_id"] == "SET-0005" and r["state"] == "triggered" for r in self.read(output, "detections.jsonl")))

    def test_existing_or_nested_output_cannot_overwrite_source(self):
        for event in self.hikkake().events:
            self.append(event)
        before = self.source_hashes()
        with self.assertRaisesRegex(ValueError, "independent"):
            analyze_backfill(self.source, self.source / "analysis")
        output = self.root / "existing"
        output.mkdir()
        (output / "marker").write_text("preserve")
        with self.assertRaises(FileExistsError):
            analyze_backfill(self.source, output)
        self.assertEqual((output / "marker").read_text(), "preserve")
        self.assertEqual(before, self.source_hashes())


if __name__ == "__main__":
    unittest.main()
