import json
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest

from connectors import DemoProvider
from runtime.config import ROOT, RuntimeConfig
from runtime.contracts import validate_contract
from runtime.export import export_run
from runtime.replay import replay_journal, derived_hash
from runtime.runner import Runner


class JournalReplayTests(unittest.TestCase):
    def test_controls_clocks_pending_raw_and_derived_outputs_replay_exactly(self):
        with TemporaryDirectory() as d:
            cfg=RuntimeConfig(pace_seconds=0)
            r=Runner(Path(d)/'original',cfg)
            events=list(DemoProvider(symbols=('SPY',),minutes=20).events())
            for event in events[:20]: r.ingest(event,'demo')
            r.submit_control('pause','request-pause')
            r.submit_control('resume','request-resume')
            r.source_status('DISCONNECTED'); r.drain()
            r.source_status('CONNECTED'); r.drain()
            for event in events[20:-1]: r.ingest(event,'demo')
            r.append(events[-1],'demo')  # durable but uncommitted observation
            r.checkpoint()
            account=r.paper.account(); expected=derived_hash(r.store)
            output=Path(export_run(r)['directory'])
            report=json.loads((output/'run.json').read_text())
            validate_contract('report',report,require_jsonschema=True)
            r.close()
            result=replay_journal(output/'journal.jsonl',Path(d)/'replay',cfg.to_dict(),
                expected_identity=report['status']['identity'],committed_cursor=report['status']['processed_cursor'],
                expected_journal_sha256=report['journal_sha256'])
            self.assertEqual(result['derived_sha256'],expected)
            self.assertEqual(result['account'],account)
            self.assertEqual(result['pending_retained'],1)

    def test_real_process_death_at_fill_commit_boundary_is_idempotent(self):
        code='''from connectors import DemoProvider
from runtime.runner import Runner
from runtime.config import RuntimeConfig
import os,sys
point=sys.argv[2]
r=Runner(sys.argv[1],RuntimeConfig(pace_seconds=0))
def fault(where,row):
    if where==point and row["seq"]==36: os._exit(17)
r._fault_hook=fault
for event in DemoProvider(symbols=("SPY",),minutes=20).events(): r.ingest(event,"demo")
'''
        with TemporaryDirectory() as d:
            child=Path(d)/'child.py'; child.write_text('import sys\nsys.path.insert(0,'+repr(str(ROOT))+')\n'+code)
            cfg=RuntimeConfig(pace_seconds=0)
            baseline=Runner(Path(d)/'baseline',cfg)
            events=list(DemoProvider(symbols=('SPY',),minutes=20).events())
            for event in events: baseline.ingest(event,'demo')
            expected=derived_hash(baseline.store); account=baseline.paper.snapshot()
            baseline.close()
            for point in ('before_commit','after_commit'):
                directory=Path(d)/point
                process=subprocess.run([sys.executable,str(child),str(directory),point],capture_output=True,timeout=20)
                self.assertEqual(process.returncode,17,process.stderr.decode())
                r=Runner(directory,cfg)
                for event in events: r.ingest(event,'demo')
                r.drain()
                self.assertEqual(derived_hash(r.store),expected)
                self.assertEqual(r.paper.snapshot(),account)
                self.assertEqual(r.store.integrity(),'ok')
                r.close()


if __name__=='__main__': unittest.main()
