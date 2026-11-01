import json
from pathlib import Path
from tempfile import TemporaryDirectory
import threading
import unittest
from unittest.mock import patch

from connectors import DemoProvider
from runtime.config import RuntimeConfig
from runtime.runner import Runner, RecoveryBlocked
from runtime.store import Store, CollisionError


class DurableRuntimeTests(unittest.TestCase):
    def events(self,minutes=4):
        return list(DemoProvider(symbols=("SPY",),minutes=minutes,pace_seconds=0).events())

    def test_journal_duplicate_arrival_and_collision_retains_original(self):
        with TemporaryDirectory() as d:
            s=Store(d)
            event=self.events()[0].to_dict()
            self.assertEqual(s.append("market","source",event,event["event_id"]),(1,True))
            newer=dict(event,arrival_time="2026-10-03T18:00:00+00:00")
            self.assertEqual(s.append("market","source",newer,event["event_id"]),(1,False))
            changed=dict(event,symbol="QQQ")
            with self.assertRaises(CollisionError): s.append("market","source",changed,event["event_id"])
            self.assertEqual(s.journal()[0]["payload"],event)
            self.assertEqual(s.conn.execute("SELECT COUNT(*) FROM quarantine").fetchone()[0],1)
            self.assertEqual(s.integrity(),"ok")
            s.close()

    def test_full_replay_checkpoint_resume_and_export(self):
        with TemporaryDirectory() as d:
            config=RuntimeConfig(pace_seconds=0)
            r=Runner(d,config)
            events=self.events(12)
            for event in events: r.ingest(event,"demo")
            r.checkpoint()
            account=r.paper.account(); cursor=r.cursor
            self.assertEqual(r.status()["health"]["events_processed"],len(events))
            r.close()
            r=Runner(d,config)
            self.assertEqual(r.cursor,cursor)
            self.assertEqual(r.paper.account(),account)
            from runtime.export import export_run
            report=export_run(r)
            self.assertTrue(Path(report["archive"]).is_file())
            manifest=json.loads((Path(report["directory"])/"manifest.json").read_text())
            self.assertGreater(len(manifest["files"]),5)
            r.close()

    def test_commit_fault_discards_mutated_memory_and_retries_pending(self):
        for point in ("before_commit","after_commit"):
            with self.subTest(point=point),TemporaryDirectory() as d:
                r=Runner(d,RuntimeConfig(pace_seconds=0))
                events=self.events()
                r.ingest(events[0],"demo"); r.checkpoint()
                fired=[]
                def fault(where,row):
                    if where==point and not fired:
                        fired.append(True); raise RuntimeError("injected fault")
                r._fault_hook=fault
                with self.assertRaises(RuntimeError): r.ingest(events[1],"demo")
                r._fault_hook=None
                r.drain()
                self.assertEqual(r.cursor,2)
                self.assertEqual(r.engine.arrival_watermark,events[1].arrival_time)
                self.assertEqual(r.store.conn.execute("SELECT SUM(processed) FROM journal").fetchone()[0],2)
                r.close()

    def test_committed_tail_reconstructs_without_checkpoint(self):
        with TemporaryDirectory() as d:
            config=RuntimeConfig(pace_seconds=0)
            r=Runner(d,config)
            for event in self.events(7): r.ingest(event,"demo")
            account=r.paper.account(); count=r.cursor
            r.close()
            r=Runner(d,config)
            self.assertEqual(r.cursor,count)
            self.assertEqual(r.paper.account(),account)
            self.assertEqual(r.status()["health"]["events_processed"],count)
            r.close()

    def test_backfill_cannot_trade_and_changed_configuration_blocks_resume(self):
        with TemporaryDirectory() as d:
            config=RuntimeConfig(pace_seconds=0)
            r=Runner(d,config)
            event=self.events()[0]
            r.ingest(event,"history",lane="backfill")
            self.assertEqual(r.engine.universe,{})
            self.assertEqual(r.store.collection("fills")["items"],[])
            r.close()
            with self.assertRaises(RecoveryBlocked): Runner(d,RuntimeConfig(pace_seconds=0,symbols=("SPY",)))

    def test_same_run_directory_has_one_owner(self):
        with TemporaryDirectory() as d:
            r=Runner(d)
            with self.assertRaises(RuntimeError): Runner(d)
            r.close()

    def test_initialization_transport_callback_precedes_market_session(self):
        class Provider:
            def describe(self): return {"name":"initialization_fixture"}
            def set_status_callback(self, callback): self.callback=callback
            def events(self, stop):
                self.callback("DISCONNECTED",phase="calendar_initialization",code="network_or_invalid_response",retry_seconds=1)
                return iter(())
        with TemporaryDirectory() as d:
            r=Runner(d,RuntimeConfig(pace_seconds=0))
            try:
                r.start(Provider())
                r._producer.join(timeout=2)
                self.assertFalse(r._producer.is_alive())
                r.drain()
                rows=r.store.journal()
                self.assertEqual(rows[0]["kind"],"source_status")
                self.assertEqual(rows[0]["payload"]["phase"],"calendar_initialization")
                self.assertTrue(all(row["processed"] for row in rows))
                self.assertIsNone(r.engine.arrival_watermark)
            finally: r.close()

    def test_queue_overflow_preserves_raw_input_and_capacity_state(self):
        events=self.events(4)
        class Provider:
            def describe(self): return {"name":"queue_fixture"}
            def events(self, stop): return iter(events)
        with TemporaryDirectory() as d:
            r=Runner(d,RuntimeConfig(queue_size=1,pace_seconds=0))
            try:
                # Hold processing while the independent collector exceeds its
                # bounded notification queue. Raw input must remain durable.
                with r._mutex:
                    r.start(Provider())
                    r._producer.join(timeout=7)
                    self.assertFalse(r._producer.is_alive())
                    self.assertLessEqual(r._queue.qsize(),1)
                    rows=r.store.journal()
                    market=[row for row in rows if row["kind"]=="market"]
                    self.assertGreaterEqual(len(market),2)
                    self.assertEqual([row["payload"] for row in market],
                                     [event.to_dict() for event in events[:len(market)]])
                    states=[row["payload"]["state"] for row in rows if row["kind"]=="source_status"]
                    self.assertIn("CAPACITY_BLOCKED",states)
                    self.assertNotIn("SOURCE_ENDED",states)
                r.drain()
                self.assertEqual(r.cursor,r.store.latest_seq())
                self.assertEqual(r.status()["state"],"CAPACITY_BLOCKED")
            finally: r.close()

    def test_export_pins_committed_prefix_without_blocking_collection(self):
        from runtime.export import export_run, file_sha256
        with TemporaryDirectory() as d:
            directory=Path(d)
            r=Runner(directory/"run",RuntimeConfig(pace_seconds=0))
            events=self.events(4)
            for event in events[:4]: r.ingest(event,"fixture")
            before=r.cursor
            pinned=threading.Event(); release=threading.Event(); written=threading.Event()
            result={}
            def paused_hash(path):
                if Path(path).name=="events.jsonl" and not pinned.is_set():
                    pinned.set()
                    if not release.wait(5): raise RuntimeError("export test coordination timeout")
                return file_sha256(path)
            def export():
                try: result.update(export_run(r,directory/"export"))
                except Exception as error: result["error"]=repr(error)
            def collect():
                try: r.ingest(events[4],"fixture")
                finally: written.set()
            exporter=threading.Thread(target=export)
            collector=threading.Thread(target=collect)
            try:
                with patch("runtime.export.file_sha256",side_effect=paused_hash):
                    exporter.start()
                    self.assertTrue(pinned.wait(2))
                    collector.start()
                    self.assertTrue(written.wait(2),"export must not retain the event writer lock")
                    self.assertEqual(r.cursor,before+1)
                    release.set()
                    exporter.join(timeout=3)
                    self.assertFalse(exporter.is_alive())
                    self.assertNotIn("error",result)
                report=json.loads((directory/"export/run.json").read_text())
                self.assertEqual(report["status"]["processed_cursor"],before)
                self.assertEqual(report["input_provenance"]["processed_cursor"],before)
                journal=[json.loads(line) for line in (directory/"export/journal.jsonl").read_text().splitlines()]
                self.assertEqual(journal[-1]["seq"],before)
            finally:
                release.set()
                if exporter.is_alive(): exporter.join(timeout=6)
                if collector.is_alive(): collector.join(timeout=6)
                r.close()


if __name__=="__main__": unittest.main()
