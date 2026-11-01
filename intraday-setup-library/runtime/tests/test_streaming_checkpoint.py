import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from connectors import DemoProvider
from runtime.config import RuntimeConfig, canonical
from runtime.runner import Runner, engine_snapshot_sha256, combined_snapshot_sha256


class StreamingCheckpointTests(unittest.TestCase):
    def test_actual_checkpoint_and_hashes_match_legacy_canonical_state(self):
        with TemporaryDirectory() as directory:
            runner = Runner(directory,RuntimeConfig(pace_seconds=0))
            try:
                for event in DemoProvider(minutes=10,symbols=("SPY",)).events():
                    runner.ingest(event,"demo")
                engine = runner.engine.snapshot()
                paper = runner.paper.snapshot()
                self.assertEqual(engine_snapshot_sha256(runner.engine),hashlib.sha256(canonical(engine).encode()).hexdigest())
                self.assertEqual(combined_snapshot_sha256(runner),hashlib.sha256(canonical({"engine":engine,"paper":paper}).encode()).hexdigest())
                runner.checkpoint()
                payload = runner.store.checkpoint()["payload"]
                self.assertEqual(payload["engine"],engine)
                self.assertEqual(payload["paper"],paper)
                raw = runner.store.conn.execute("SELECT payload FROM checkpoints ORDER BY id DESC LIMIT 1").fetchone()[0]
                self.assertEqual(raw,canonical(payload))
                self.assertEqual(payload["derived_sha256"],runner.store.derived_hash())
                expected = runner.paper.account()
                runner.recover()
                self.assertEqual(runner.paper.account(),expected)
                self.assertEqual(runner.engine.snapshot(),engine)
                self.assertIn("process_provenance",runner.status()["health"])
                self.assertNotIn("process_provenance",payload)
            finally:
                runner.close()

    def test_interrupted_stream_cannot_replace_prior_checkpoint_or_progress(self):
        with TemporaryDirectory() as directory:
            runner = Runner(directory,RuntimeConfig(pace_seconds=0))
            try:
                for event in DemoProvider(minutes=1,symbols=("SPY",)).events():
                    runner.ingest(event,"demo")
                runner.checkpoint()
                before = [tuple(row) for row in runner.store.conn.execute("SELECT * FROM checkpoints ORDER BY id")]
                cursor = runner.store.meta("checkpoint_cursor")
                def broken(engine,handle):
                    handle.write('{"partial":')
                    raise RuntimeError("stream interrupted")
                with patch.object(type(runner.engine),"write_snapshot_json",new=broken):
                    with self.assertRaisesRegex(RuntimeError,"stream interrupted"):
                        runner.checkpoint()
                self.assertEqual([tuple(row) for row in runner.store.conn.execute("SELECT * FROM checkpoints ORDER BY id")],before)
                self.assertEqual(runner.store.meta("checkpoint_cursor"),cursor)
                self.assertFalse(runner.store.conn.in_transaction)
                runner.checkpoint()
                self.assertEqual(runner.store.checkpoint()["cursor"],runner.cursor)
            finally:
                runner.close()


if __name__ == "__main__":
    unittest.main()
