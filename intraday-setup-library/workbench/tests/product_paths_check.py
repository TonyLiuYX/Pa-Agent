"""Focused product-path audit; evidence stays outside the project.

Uses a current synthetic preview read-only except its normal export action.
Controlled browser routes check new pagination and causal replay boundaries.
It never uses the independent long-run Runner or submits labels/orders.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
import argparse
import hashlib
import json
import subprocess
import zipfile
from playwright.sync_api import sync_playwright, expect
from run_switch_check import exercise_run_switch


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url',default='http://127.0.0.1:8781')
    parser.add_argument('--output',required=True)
    parser.add_argument('--before-ref',help='Optional verified Git ref for reproducing the earlier interface')
    parser.add_argument('--compatibility-preview',action='store_true',help='Frontend compatibility evidence only; never current-core API acceptance')
    args=parser.parse_args();base=args.url.rstrip('/');out=Path(args.output).resolve()
    root=Path(__file__).resolve().parents[2]
    assert not out.is_relative_to(root),'Keep run evidence outside the release tree'
    out.mkdir(parents=True,exist_ok=True);checks=[];screenshots=[];errors=[]
    import sys
    sys.path.insert(0,str(root))
    from runtime.soak import core_manifest
    from runtime.runner import runtime_hash
    from recognition.provenance import algorithm_sha256
    core_before=core_manifest();source_runtime_before=runtime_hash();source_algorithm_before=algorithm_sha256()
    def passed(name,scope='actual_synthetic_preview'):checks.append({'name':name,'scope':scope,'status':'passed'})
    def capture(page,name):
        page.evaluate("() => window.scrollTo(0,0)")
        page.screenshot(path=str(out/name),full_page=True);screenshots.append(name)
    with sync_playwright() as p:
        browser=p.chromium.launch(headless=True)
        page=browser.new_page(viewport={'width':1440,'height':1050})
        page.on('pageerror',lambda error:errors.append(str(error)))
        page.goto(base,wait_until='domcontentloaded');page.wait_for_selector('#watchlist .symbol-card')
        status=page.request.get(base+'/api/status').json()
        assert status['mode']=='demo' and status['source_kind']=='synthetic','Only use an isolated synthetic preview'
        if args.before_ref:
            # Capture the released pre-change interface against the same real API.
            before=browser.new_page(viewport={'width':1440,'height':1050})
            old_app=subprocess.check_output(['git','show',args.before_ref+':workbench/app.js'],cwd=root,text=True)
            old_html=subprocess.check_output(['git','show',args.before_ref+':workbench/index.html'],cwd=root,text=True)
            assert 'id="replay-chart"' not in old_html,'Before snapshot already contains this change'
            before.route('**/workbench/app.js',lambda route:route.fulfill(content_type='text/javascript',body=old_app))
            before.route(base+'/',lambda route:route.fulfill(content_type='text/html',body=old_html))
            before.goto(base,wait_until='domcontentloaded');before.wait_for_selector('#watchlist .symbol-card')
            before.locator('[data-view=account]').click();before.wait_for_selector('#orders table')
            old_more=before.locator('#more-orders').is_visible();assert not old_more
            capture(before,'before-orders.png')
            before.locator('[data-view=replay]').click();before.locator('#replay-load').click()
            expect(before.locator('#replay-progress')).to_contain_text('1 /')
            assert before.locator('#replay-chart').count()==0
            capture(before,'before-replay.png');before.close()
            passed('before interface reproduced: no order history continuation or causal replay chart','before_actual_preview_released_ui')
        page.locator('[data-view=account]').click();expect(page.locator('#more-orders')).to_be_visible()
        first_orders=page.request.get(base+'/api/orders?before=9223372036854775807&limit=50').json()
        latest={r['order_id']:r for r in reversed(first_orders['items'])}
        initial_count=page.locator('#orders tbody tr').count();page.locator('#more-orders').click()
        expect(page.locator('#orders tbody tr')).not_to_have_count(initial_count)
        labels=page.locator('#orders tbody tr td:first-child').all_text_contents();assert len(labels)==len(set(labels))
        passed('order history has a working product entry and deduplicates order identities')
        status_names={'filled':'已成交','pending':'等待报价','cancelled':'已取消','rejected':'已拒绝'}
        for identity,order in latest.items():
            row=page.locator('#orders tbody tr').filter(has=page.locator('td:first-child',has_text=identity))
            assert row.count()==1 and status_names.get(order['status'],order['status']) in row.locator('td').nth(4).inner_text()
        loaded_count=page.locator('#orders tbody tr').count();page.locator('#refresh').click()
        expect(page.locator('#orders tbody tr')).to_have_count(loaded_count)
        passed('older order revisions do not replace the newest status and refresh retains loaded history')
        capture(page,'after-orders-history.png')
        # Real range, exact inclusive boundary and chart prefix progression.
        records=page.request.get(base+'/api/events?limit=1000').json()['items']
        bars=[r for r in records if r['payload'].get('kind')=='bar' and r['payload'].get('symbol')=='SPY']
        assert len(bars)>=4
        lo=bars[0]['payload']['arrival_time'];hi=bars[3]['payload']['arrival_time']
        page.locator('[data-view=replay]').click()
        page.locator('#replay-start').fill(lo);page.locator('#replay-end').fill(hi)
        page.locator('#replay-load').click();expect(page.locator('#replay-progress')).to_contain_text('1 /')
        expected=page.request.get(base+'/api/events?start='+lo.replace('+','%2B')+'&end='+hi.replace('+','%2B')+'&limit=100').json()['items']
        assert expected[0]['seq']==bars[0]['seq']
        expect(page.locator('#replay-chart-description')).to_contain_text('1根已观察K线')
        assert not page.locator('#show-conclusions').is_checked() and not page.locator('#replay-conclusions').is_visible()
        assert page.locator('#replay-detections').inner_text()==''
        assert not page.locator('#show-existing-labels').is_checked() and not page.locator('#labels').is_visible()
        passed('inclusive real range starts with one observed candle and no loaded engine conclusion')
        passed('blind annotation also hides already-saved future-window labels until explicit opt-in')
        for _ in range(3):page.locator('#replay-step').click()
        observed=expected[3];expected_bars=sum(r['payload'].get('kind')=='bar' and r['payload'].get('symbol')=='SPY' for r in expected[:4])
        expect(page.locator('#replay-chart-description')).to_contain_text(f'{expected_bars}根已观察K线')
        page.locator('#replay-prev').click();expect(page.locator('#replay-progress')).to_contain_text('3 /')
        page.locator('#replay-reset').click();expect(page.locator('#replay-progress')).to_contain_text('1 /')
        passed('next, previous and reset project only the already-viewed raw bar prefix')
        form=page.locator('#label-form');form.locator('[name=setup_id]').fill('SET-0007');form.locator('[name=positive]').select_option('false')
        page.locator('#label-use-current').click()
        assert form.locator('[name=start]').input_value()==bars[0]['payload']['event_time']
        assert form.locator('[name=setup_id]').input_value()=='SET-0007' and form.locator('[name=positive]').input_value()=='false'
        passed('adopt current event fills time/symbol without supplying setup or structure judgement')
        capture(page,'after-blind-replay.png')
        historical=page.request.get(base+'/api/detections?after=0&limit=100&symbol=SPY').json()['items']
        first_decision=historical[0]
        raw=page.request.get(base+f"/api/events?after={first_decision['event_seq']-1}&limit=1").json()['items'][0]
        decision_time=raw['payload'].get('arrival_time') or raw['known_at']
        page.locator('#replay-end').fill(decision_time);page.locator('#replay-load').click()
        expect(page.locator('#replay-progress')).to_contain_text('1 /')
        prefix=page.request.get(base+'/api/events?start='+lo.replace('+','%2B')+'&end='+decision_time.replace('+','%2B')+'&limit=1000').json()['items']
        index=next(i for i,row in enumerate(prefix) if row['seq']==first_decision['event_seq'])
        for _ in range(index):page.locator('#replay-step').click()
        page.locator('#show-conclusions').check()
        eligible=[row for row in historical if prefix[0]['seq']<=row['event_seq']<=first_decision['event_seq']]
        expect(page.locator('#replay-conclusion-status')).to_contain_text(f"共{len(eligible)}条")
        assert page.locator('#replay-detections tbody tr').count()==len(eligible)
        page.locator('#replay-detections tbody tr').first.click();expect(page.locator('#replay-evidence')).to_contain_text(first_decision['setup_id'])
        passed('actual early historical decisions are loaded by event sequence beyond the initial latest snapshot')
        capture(page,'after-actual-historical-conclusion.png')
        page.locator('[data-view=market]').click();page.wait_for_selector('#detections tbody tr')
        page.locator('#detections tbody tr').first.click()
        expect(page.locator('#evidence')).to_contain_text('观察时刻')
        detail=page.locator('#evidence .evidence-conditions details').first
        detail.locator('summary').click();assert detail.locator('pre').inner_text()
        passed('signal condition exposes observation time, provenance and source evidence identifiers')
        capture(page,'after-condition-evidence.png')
        page.locator('[data-view=research]').click()
        with page.expect_download() as download:page.locator('#export-reports').click()
        report_path=out/'committed-reports.json';download.value.save_as(report_path)
        report=json.loads(report_path.read_text());assert {r['kind'] for r in report['items']}=={'runtime','recognition_evaluation','experimental_account'}
        score=next(r for r in report['items'] if r['kind']=='recognition_evaluation')
        assert score['market_validation']=='not_run'
        passed('downloaded research JSON retains current committed three-stage report and validation scope')
        with page.expect_download(timeout=30000) as download:page.locator('#export').click()
        run_path=out/'current-preview-export.zip';download.value.save_as(run_path)
        with zipfile.ZipFile(run_path) as archive:
            assert archive.testzip() is None
            run_name=next(name for name in archive.namelist() if name.endswith('/run.json') or name=='run.json')
            run=json.loads(archive.read(run_name));assert run['input_provenance']['detections_sha256']
        passed('run export button downloads an actual CRC-valid research archive with input provenance')
        capture(page,'after-research-download.png')
        # The actual stream can reconnect without a page reload or a write request.
        reloads=[];posts=[]
        page.on('request',lambda request:posts.append(request.url) if request.method=='POST' else None)
        page.on('framenavigated',lambda frame:reloads.append(frame.url) if frame==page.main_frame else None)
        page.context.set_offline(True);expect(page.locator('#reconnect-stream')).to_be_visible(timeout=10000)
        page.context.set_offline(False);page.locator('#reconnect-stream').click()
        expect(page.locator('#reconnect-stream')).to_be_hidden(timeout=10000)
        assert not posts and not reloads
        passed('actual SSE network interruption recovers through visible reconnect without reload or resubmission')
        sample=page.request.get(base+'/api/detections?limit=1').json()['items'][0]
        page.close()
        # Deliberately larger histories and same-time future decisions.
        mock=browser.new_page(viewport={'width':1440,'height':1050});mock.on('pageerror',lambda error:errors.append(str(error)))
        start=datetime(2026,9,30,13,30,tzinfo=timezone.utc)
        event_rows=[{'seq':i,'kind':'market','processed':True,'payload':{'kind':'bar','symbol':'SPY','venue':'SYNTHETIC','event_time':(start+timedelta(minutes=i)).isoformat(),'arrival_time':(start+timedelta(minutes=i)).isoformat(),'payload':{'open':100+i,'high':102+i,'low':99+i,'close':101+i}}} for i in range(1,106)]
        detections=[{**sample,'record_id':i,'event_seq':1 if i<=80 else 105,'detection_id':f'causal-fixture-{i}','symbol':'SPY','identified_at':event_rows[0]['payload']['arrival_time'],'evidence_cutoff':event_rows[0]['payload']['event_time'],'anchor':{'captures':{'level':100},'source_ids':['synthetic-prefix-bar']},'conditions':[{'feature':'synthetic_prefix_condition','result':'satisfied','actual':True,'expected':True,'observed_at':event_rows[0]['payload']['event_time'],'source_ids':['synthetic-prefix-bar'],'provenance':'synthetic_boundary_fixture'}]} for i in range(1,121)]
        stream_requests=[]
        def route_api(route):
            path=urlsplit(route.request.url);q=parse_qs(path.query)
            if path.path=='/api/events':
                after=int(q.get('after',[0])[0]);rows=[r for r in event_rows if r['seq']>after];limit=int(q.get('limit',[100])[0]);value={'items':rows[:limit],'next_cursor':rows[limit-1]['seq'] if len(rows)>limit else None}
            elif path.path=='/api/detections' and 'after' in q:
                after=int(q['after'][0]);limit=int(q.get('limit',[100])[0]);rows=[r for r in detections if r['record_id']>after];value={'items':rows[:limit],'next_cursor':rows[limit-1]['record_id'] if len(rows)>=limit else None}
            elif path.path=='/api/events/stream':
                stream_requests.append(dict(q))
                if len(stream_requests)==1:route.fulfill(status=200,content_type='text/event-stream',body='event: committed\nid: 900001\ndata: {}\n\n')
                else:route.abort()
                return
            else:route.continue_();return
            route.fulfill(content_type='application/json',body=json.dumps(value))
        mock.route('**/api/**',route_api)
        mock.goto(base,wait_until='domcontentloaded');mock.wait_for_selector('#watchlist .symbol-card')
        mock.locator('[data-view=replay]').click();mock.locator('#replay-load').click();expect(mock.locator('#replay-progress')).to_contain_text('1 / 100+')
        mock.locator('#show-conclusions').check();expect(mock.locator('#replay-conclusion-status')).to_contain_text('共80条')
        assert mock.locator('#replay-detections tbody tr').count()==80
        passed('replay loads forward detection pages and excludes same-timestamp future event_seq decisions','controlled_synthetic_browser_routes')
        for _ in range(100):mock.locator('#replay-step').click()
        expect(mock.locator('#replay-progress')).to_contain_text('101 / 105')
        expect(mock.locator('#replay-chart-description')).to_contain_text('101根已观察K线')
        assert mock.locator('#replay-detections tbody tr').count()==80
        passed('raw replay crosses its 100-event page without exposing four future candles or decisions','controlled_synthetic_browser_routes')
        for _ in range(4):mock.locator('#replay-step').click()
        expect(mock.locator('#replay-conclusion-status')).to_contain_text('共120条')
        assert mock.locator('#replay-detections tbody tr').count()==120
        mock.locator('#replay-prev').click();assert mock.locator('#replay-detections tbody tr').count()==80
        passed('later detection page appears only at its decision event and disappears when stepping back','controlled_synthetic_browser_routes')
        mock.locator('#replay-detections tbody tr').first.focus();mock.keyboard.press('Enter');expect(mock.locator('#replay-evidence')).to_contain_text('逐条件结果')
        passed('historical replay decisions expose keyboard-selectable condition evidence','controlled_synthetic_browser_routes')
        capture(mock,'after-replay-history-boundary.png')
        many=[{**row,'record_id':i,'event_seq':1,'detection_id':f'large-history-{i}'} for i,row in ((i,detections[0]) for i in range(1,651))]
        def large_detection_history(route):
            q=parse_qs(urlsplit(route.request.url).query)
            if 'after' not in q:route.fallback();return
            after=int(q['after'][0]);limit=int(q.get('limit',[100])[0]);rows=[row for row in many if row['record_id']>after]
            route.fulfill(content_type='application/json',body=json.dumps({'items':rows[:limit],'next_cursor':rows[limit-1]['record_id'] if len(rows)>=limit else None}))
        mock.route('**/api/detections?*',large_detection_history)
        mock.locator('#show-conclusions').uncheck();mock.locator('#replay-symbol').select_option('QQQ');mock.locator('#replay-symbol').select_option('SPY');mock.locator('#show-conclusions').check()
        expect(mock.locator('#replay-conclusion-status')).to_contain_text('历史尚未读取完整')
        assert mock.locator('#replay-detections tbody tr').count()==300 and mock.locator('#replay-more-conclusions').is_visible()
        mock.locator('#replay-more-conclusions').click();expect(mock.locator('#replay-detections tbody tr')).to_have_count(600)
        mock.locator('#replay-more-conclusions').click();expect(mock.locator('#replay-conclusion-status')).to_contain_text('共650条')
        assert mock.locator('#replay-more-conclusions').is_hidden()
        passed('three-page action budget explicitly preserves unknown coverage until continuation reads all 650 decisions','controlled_synthetic_browser_routes')
        mock.locator('#show-conclusions').uncheck()
        # Force a stream retry after a delivered continuation ID; no write is sent.
        expect(mock.locator('#reconnect-stream')).to_be_visible()
        count=len(stream_requests);mock.locator('#reconnect-stream').click()
        expect(mock.locator('#reconnect-stream')).to_be_visible()
        for _ in range(20):
            if len(stream_requests)>count:break
            mock.wait_for_timeout(100)
        assert len(stream_requests)>count and stream_requests[-1].get('after')==['900001']
        passed('manual SSE continuation uses the last received ID rather than zero','controlled_synthetic_browser_routes')
        mock.set_viewport_size({'width':390,'height':844});capture(mock,'after-replay-mobile.png')
        assert mock.evaluate('() => document.documentElement.scrollWidth <= window.innerWidth')
        passed('new replay controls and causal chart remain usable without mobile horizontal overflow','controlled_synthetic_browser_routes')
        mock.locator('[data-view=research]').click()
        assert mock.locator('#export-reports').is_visible() and mock.locator('#export').is_visible()
        assert mock.evaluate('() => document.documentElement.scrollWidth <= window.innerWidth')
        capture(mock,'after-research-mobile.png')
        passed('mobile research page keeps both report and full-run download entries usable','controlled_synthetic_browser_routes')
        global_gate_fixture={'setup_id':'SET-0005','variant_id':'base','recognition_eligible':True,'auto_simulate':False,'entry_paused':True,'scope':'synthetic_browser_fixture','per_symbol':{'SPY':{'recognition_eligible':True,'execution_eligible':True,'missing':[]}}}
        def cover_route(route):route.fulfill(content_type='application/json',body=json.dumps({'items':[global_gate_fixture],'next_cursor':None}))
        mock.route('**/api/coverage?*',cover_route);mock.locator('#refresh').click()
        expect(mock.locator('#coverage tbody tr')).to_have_count(1)
        mock.locator('#coverage-symbol').select_option('SPY')
        expect(mock.locator('#coverage tbody tr td').nth(1)).to_have_text('是')
        expect(mock.locator('#coverage tbody tr td').nth(2)).to_have_text('否')
        passed('symbol can remain recognizable while a global paused-entry gate blocks automatic simulation','controlled_synthetic_browser_routes')
        capture(mock,'after-symbol-global-gate.png')
        global_gate_fixture.pop('auto_simulate');global_gate_fixture.pop('entry_paused')
        mock.locator('#refresh').click();expect(mock.locator('#coverage tbody tr td').nth(2)).to_have_text('是')
        passed('legacy coverage fixture without a global gate retains the per-symbol execution capability','controlled_synthetic_browser_routes')
        mock.close()
        exercise_run_switch(browser,base,out,capture,passed)
        browser_version=browser.version;browser.close()
    assert not errors,errors
    core_after=core_manifest()
    import urllib.request
    with urllib.request.urlopen(base+'/api/status') as response:api_after=json.load(response)
    assert api_after['run_id']==status['run_id'] and api_after['identity']==status['identity']
    api_matches_source=status['identity'].get('runtime_sha256')==source_runtime_before and status['identity'].get('algorithm_sha256')==source_algorithm_before
    accepted=core_before==core_after and api_matches_source
    validation_status='frontend_compatibility_only' if args.compatibility_preview else 'passed' if accepted else 'failed'
    files={name:{'bytes':(out/name).stat().st_size,'sha256':hashlib.sha256((out/name).read_bytes()).hexdigest()} for name in screenshots+['committed-reports.json','current-preview-export.zip']}
    evidence={'status':validation_status,'executed_at':datetime.now(timezone.utc).isoformat(),'run_id':status['run_id'],'runtime_sha256':status.get('identity',{}).get('runtime_sha256'),'browser':browser_version,'checks':checks,'checks_passed':len(checks),'page_errors':errors,'source_core_before':core_before['sha256'],'source_core_after':core_after['sha256'],'source_core_unchanged_during_checks':core_before==core_after,'preview_identity':status['identity'],'preview_identity_unchanged_during_checks':status['identity']==api_after['identity'],'api_matches_current_source_identity':api_matches_source,'source_runtime_sha256':source_runtime_before,'source_algorithm_sha256':source_algorithm_before,'core_validation_scope':'Current-core acceptance requires identical source-core start/end and preview runtime/algorithm identity equal to source. Explicit compatibility mode never claims current-core API acceptance.','frontend_files':{name:hashlib.sha256((root/name).read_bytes()).hexdigest() for name in ('workbench/app.js','workbench/index.html','workbench/style.css','workbench/tests/product_paths_check.py','workbench/tests/run_switch_check.py')},'files':files,'before_source':{'git_ref':args.before_ref,'scope':'Optional earlier frontend served by controlled browser routes against same preview API; see preserved before screenshots when supplied.'},'source_kind':'synthetic','market_validation':'not_run','long_run_scope':'independent soak Runner was not opened or modified','limitations':['Replay reads the committed journal and does not recalculate historical engine/account state.','No blind human labels or market-performance validation are claimed.','Synthetic route assertions exercise pagination/network boundaries; they are separated from actual preview checks.']}
    (out/'product-path-validation.json').write_text(json.dumps(evidence,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({'status':validation_status,'checks_passed':len(checks),'output':str(out/'product-path-validation.json'),'errors':errors,'api_matches_current_source_identity':api_matches_source,'source_core_unchanged':core_before==core_after},ensure_ascii=False))
    if validation_status=='failed':raise SystemExit('Current-core API validation gate rejected source change or stale preview identity')


if __name__=='__main__':main()
