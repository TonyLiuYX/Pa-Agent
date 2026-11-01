import http.client
import json
from tempfile import TemporaryDirectory
import unittest

from connectors import DemoProvider
from runtime.config import RuntimeConfig
from runtime.runner import Runner, RecoveryBlocked
from runtime.server import WorkbenchServer
from runtime.contracts import validate_contract, envelope
from runtime.advisor import AdviceRequest, AdviceResult, DisabledAdvisor


class WorkbenchTests(unittest.TestCase):
    def setUp(self):
        self.directory=TemporaryDirectory()
        self.runner=Runner(self.directory.name,RuntimeConfig(pace_seconds=0))
        self.server=WorkbenchServer(self.runner,port=0).start()
        self.port=self.server.server_port

    def tearDown(self):
        self.server.close(); self.runner.close(); self.directory.cleanup()

    def request(self,path,body=None,headers=None):
        c=http.client.HTTPConnection('127.0.0.1',self.port,timeout=3)
        defaults={'Host':f'127.0.0.1:{self.port}'}
        defaults.update(headers or {})
        c.request('GET' if body is None else 'POST',path,body=None if body is None else json.dumps(body),headers=defaults)
        r=c.getresponse(); status=r.status; raw=r.read(); c.close()
        return status,json.loads(raw) if r.getheader('Content-Type','').startswith('application/json') else raw

    def ingest(self,minutes=8):
        for event in DemoProvider(symbols=('SPY',),minutes=minutes).events(): self.runner.ingest(event,'demo')

    def test_host_origin_token_and_route_containment(self):
        self.assertEqual(self.request('/api/status',headers={'Host':'attacker.invalid'})[0],403)
        self.assertEqual(self.request('/api/runtime/pause',{})[0],403)
        token=self.request('/api/status')[1]['session_token']
        self.assertEqual(self.request('/api/runtime/pause',{}, {'X-Session-Token':token,'Origin':'https://attacker.invalid'})[0],403)
        for path in ('/workbench/%2e%2e/README.md','/library/%2e%2e/.env','/assets/%2e%2e/README.md','/downloads/%2e%2e/runtime.sqlite3'):
            self.assertEqual(self.request(path)[0],404,path)
        self.assertEqual(self.request('/')[0],200)

    def test_latest_history_and_forward_journal(self):
        self.ingest()
        status,first=self.request('/api/bars?limit=3')
        self.assertEqual(status,200); self.assertEqual(len(first['items']),3)
        self.assertIsNotNone(first['next_cursor'])
        _,older=self.request('/api/bars?limit=3&before='+str(first['next_cursor']))
        self.assertLess(older['items'][-1]['event_seq'],first['items'][0]['event_seq'])
        _,events=self.request('/api/events?limit=1000')
        self.assertEqual([e['seq'] for e in events['items']],list(range(1,self.runner.cursor+1)))
        self.assertIsNone(events['next_cursor'])
        self.assertEqual(self.request('/api/events?after=-1')[0],400)

    def test_replay_time_range_uses_received_time_and_respects_offset(self):
        self.ingest()
        from urllib.parse import urlencode
        params=urlencode({'start':'2026-09-30T09:33:00-04:00','end':'2026-09-30T13:35:00Z','limit':2})
        _,page=self.request('/api/events?'+params)
        ids=[]
        while True:
            ids.extend(r['seq'] for r in page['items'])
            if page['next_cursor'] is None: break
            _,page=self.request('/api/events?'+params+'&after='+str(page['next_cursor']))
        expected=self.runner.store.journal(start='2026-09-30T13:33:00Z',end='2026-09-30T13:35:00Z')
        self.assertEqual(ids,[r['seq'] for r in expected])
        self.assertEqual(len(ids),7)  # two bars+quotes and the inclusive final bar
        self.assertEqual(self.request('/api/events?start=2026-09-30T13:33:00')[0],400)

    def test_control_idempotency_and_collision(self):
        token=self.server.token
        headers={'X-Session-Token':token,'Idempotency-Key':'pause-request'}
        status,first=self.request('/api/runtime/pause',{},headers)
        self.assertEqual(status,202)
        _,second=self.request('/api/runtime/pause',{},headers)
        self.assertEqual(first['event_seq'],second['event_seq']); self.assertTrue(second['duplicate'])
        self.assertEqual(self.request('/api/runtime/resume',{},headers)[0],409)
        self.runner.source_status('DISCONNECTED'); self.runner.drain()
        self.runner.source_status('CONNECTED'); self.runner.drain()
        self.assertEqual(self.runner.state,'PAUSED')

    def test_reports_use_committed_evaluation_and_reject_unknown_complete_coverage(self):
        status,report=self.request('/api/reports')
        self.assertEqual(status,200)
        kinds={item['kind']:item for item in report['items']}
        self.assertEqual(set(kinds),{'runtime','recognition_evaluation','experimental_account'})
        self.assertEqual(kinds['recognition_evaluation']['status'],'not_run')
        self.assertIsNone(kinds['recognition_evaluation']['metrics'])
        label={'case_id':'coverage-fixture','setup_id':'SET-0005','variant_id':'base','symbol':'SPY','venue':'SYN',
               'direction':1,'positive':False,'start':'2026-09-30T13:31:00Z','end':'2026-09-30T13:32:00Z',
               'group_id':'fixture','source_kind':'synthetic','source':'fixture','data_source':'fixture',
               'annotator':'test','rule_version':'0.2.1','complete_coverage':True}
        for flags in ({'disputed':True},{'data_eligible':False},{'complete_coverage':1}):
            with self.subTest(flags=flags):
                self.assertEqual(self.request('/api/labels',{**label,**flags},{'X-Session-Token':self.server.token})[0],400)

    def test_sse_committed_cursor_and_reconnect(self):
        self.ingest(1)
        def next_row(after):
            c=http.client.HTTPConnection('127.0.0.1',self.port,timeout=3)
            c.request('GET','/api/events/stream',headers={'Last-Event-ID':str(after)})
            r=c.getresponse(); row_id=None; data=None
            while data is None:
                line=r.readline().decode().strip()
                if line.startswith('id:'): row_id=int(line[3:])
                if line.startswith('data:'): data=json.loads(line[5:])
            c.close(); return row_id,data
        first,data=next_row(0)
        second,data2=next_row(first)
        self.assertGreater(second,first)
        self.assertLessEqual(data2['event_seq'],self.runner.cursor)
        self.assertEqual(data['id'],first)

    def test_public_contracts_cover_real_outputs(self):
        from paper import execution_profiles
        self.ingest(20)
        for row in self.runner.store.journal(): validate_contract('envelope',envelope(self.runner.run_id,row),require_jsonschema=True)
        for row in self.runner.store.journal(): validate_contract('market',row['payload'],require_jsonschema=True)
        for row in execution_profiles(): validate_contract('profile',row,require_jsonschema=True)
        for topic,kind in (('detections','detection'),('orders','order'),('fills','fill'),('account_events','account_event')):
            for row in self.runner.store.collection(topic,limit=1000)['items']: validate_contract(kind,row,require_jsonschema=True)
        validate_contract('account',self.runner.paper.account(),require_jsonschema=True)
        validate_contract('config',RuntimeConfig().to_dict(),require_jsonschema=True)

    def test_advisor_rejects_future_and_unknown_evidence(self):
        event={'event_id':'past','event_time':'2026-10-01T13:30:00Z','arrival_time':'2026-10-01T13:30:01Z'}
        request=AdviceRequest('d','2026-10-01T13:30:00Z','2026-10-01T13:30:02Z',(event,),'0.2.1')
        self.assertEqual(DisabledAdvisor().evaluate(request).unknown,('model_calls_disabled',))
        with self.assertRaises(ValueError): AdviceResult('d',(),('future',),(),'test').validate(request)
        with self.assertRaises(ValueError): AdviceRequest('d',request.evidence_cutoff,request.decision_time,({**event,'arrival_time':'2026-10-01T13:30:03Z'},),'0.2.1').validate()


class RecoveryAuditTests(unittest.TestCase):
    def test_ledger_tampering_without_checkpoint_blocks_recovery(self):
        with TemporaryDirectory() as d:
            cfg=RuntimeConfig(pace_seconds=0)
            r=Runner(d,cfg)
            r.submit_control('pause','control')
            r.store.conn.execute("UPDATE records SET payload='{}' WHERE topic='account_events'")
            r.close()
            with self.assertRaises(RecoveryBlocked): Runner(d,cfg)

    def test_checkpoint_protects_old_ledger_and_rebuilds_account_view(self):
        for corruption in ('ledger_changed','ledger_deleted','cursor_changed','account_view_changed'):
            with self.subTest(corruption=corruption),TemporaryDirectory() as d:
                cfg=RuntimeConfig(pace_seconds=0); r=Runner(d,cfg)
                r.submit_control('pause','control'); r.checkpoint()
                expected=r.paper.account()
                if corruption=='ledger_changed': r.store.conn.execute("UPDATE records SET payload='{}' WHERE topic='account_events'")
                elif corruption=='ledger_deleted': r.store.conn.execute("DELETE FROM records WHERE topic='account_events'")
                elif corruption=='cursor_changed': r.store.set_meta('processed_cursor',0)
                else: r.store.set_meta('account',{})
                r.close()
                if corruption=='account_view_changed':
                    r=Runner(d,cfg); self.assertEqual(r.store.meta('account'),expected); r.close()
                else:
                    with self.assertRaises(RecoveryBlocked): Runner(d,cfg)


class CoveragePolicyTests(unittest.TestCase):
    def test_live_coverage_uses_configured_quote_age(self):
        from datetime import datetime,timedelta,timezone
        now=datetime.now(timezone.utc)
        for max_age,expected in ((5,False),(10,True)):
            with self.subTest(max_age=max_age),TemporaryDirectory() as d:
                runner=Runner(d,RuntimeConfig(mode='live',symbols=('SPY',),paper={'quote_max_age_seconds':max_age}))
                server=WorkbenchServer(runner,port=0).start()
                try:
                    runner.store.set_meta('execution_sessions',{'SPY':{'venue':'IEX'}})
                    runner.store.set_meta('coverage',{'SET-0005:base':{'requirements':['bars'],'last_frame_missing_by_stream_scale':{'SPY|IEX|60|bar':[]}}})
                    runner.store.set_meta('quote:SPY',{'venue':'IEX','quality':'complete','arrival_time':(now-timedelta(seconds=1)).isoformat(),'event_time':(now-timedelta(seconds=8)).isoformat()})
                    client=http.client.HTTPConnection('127.0.0.1',server.server_port,timeout=3)
                    client.request('GET','/api/coverage')
                    response=client.getresponse()
                    self.assertEqual(response.status,200)
                    row=json.loads(response.read())['items'][0]
                    client.close()
                    self.assertEqual(row['auto_simulate'],expected)
                finally: server.close(); runner.close()


if __name__=='__main__': unittest.main()
