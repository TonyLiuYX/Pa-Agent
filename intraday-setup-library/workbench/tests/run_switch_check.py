"""Controlled browser proof for a run change while the same page stays open."""
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
import argparse
import hashlib
import json
from playwright.sync_api import sync_playwright, expect


def exercise_run_switch(browser, base, output, capture, passed, expect_fixed=True):
    page=browser.new_page(viewport={'width':1440,'height':1050})
    errors=[];page.on('pageerror',lambda error:errors.append(str(error)))
    current={'run':'A','source':'controlled-stream-A','hold_events':False};streams=[];posts=[];held=[]
    when='2026-10-02T13:31:00+00:00'
    def raw(run):return {'seq':1,'payload':{'event_id':run+'-raw','kind':'bar','symbol':'SPY','venue':'SYNTHETIC','event_time':when,'arrival_time':when,'payload':{'open':100,'high':101,'low':99,'close':100.5}}}
    def detection(run):return {'record_id':30,'event_seq':1,'detection_id':run+'-decision','setup_id':'SET-0005','variant_id':'base','symbol':'SPY','venue':'SYNTHETIC','direction':1,'state':'triggered','data_status':'complete','identified_at':when,'evidence_cutoff':when,'conditions':[{'feature':run+'-condition','result':'satisfied'}],'anchor':{'captures':{}}}
    def route_api(route):
        request=route.request;path=urlsplit(request.url);query=parse_qs(path.query);run=current['run']
        if request.method=='POST':posts.append({'url':request.url,'body':request.post_data});route.fulfill(status=409,content_type='application/json',body='{"error":"controlled_write_not_expected"}');return
        if path.path=='/api/status':value={'run_id':'controlled-run-'+run,'mode':'demo','source_kind':'synthetic','source':current['source'],'state':'running','watchlist':['SPY'],'processed_cursor':500,'outbox_cursor':900 if run=='A' else 10,'session_token':'controlled-'+run,'health':{'queue_depth':0,'journal_backlog':0}}
        elif path.path=='/api/events/stream':
            streams.append({'run':run,'after':query.get('after',[None])[0]})
            route.fulfill(status=200,content_type='text/event-stream',body=('event: committed\nid: 901\ndata: {}\n\n' if run=='A' else ''));return
        elif path.path=='/api/orders':
            older='before' in query and int(query['before'][0])<100
            value={'items':[{'order_id':run+('-old-order' if older else '-order'),'record_id':10 if older else 30,'symbol':'SPY','direction':1,'purpose':'entry','status':'filled','quantity':1}], 'next_cursor':None if older else 20}
        elif path.path=='/api/fills':
            older='before' in query
            value={'items':[{'fill_id':run+('-old-fill' if older else '-fill'),'record_id':10 if older else 30,'symbol':'SPY','quantity':1,'price':100}], 'next_cursor':None if older else 20}
        elif path.path=='/api/detections':value={'items':[detection(run)],'next_cursor':None}
        elif path.path=='/api/events':
            if current['hold_events']:held.append((route,{'items':[raw(run)],'next_cursor':None}));return
            value={'items':[raw(run)],'next_cursor':None}
        elif path.path=='/api/bars':value={'items':[raw(run)['payload']],'next_cursor':None}
        elif path.path=='/api/account':value={'equity':100000 if run=='A' else 200000,'cash':100000,'reserved':0}
        else:value={'items':[],'next_cursor':None}
        route.fulfill(content_type='application/json',body=json.dumps(value))
    page.route('**/api/**',route_api)
    page.goto(base,wait_until='domcontentloaded');page.wait_for_selector('#watchlist .symbol-card')
    page.locator('[data-view=account]').click();page.locator('#more-orders').click();page.locator('#more-fills').click()
    expect(page.locator('#orders')).to_contain_text('A-old-order');expect(page.locator('#fills')).to_contain_text('A-old-fill')
    page.locator('[data-view=market]').click();page.locator('#detections tbody tr').first.click()
    expect(page.locator('#evidence')).to_contain_text('A-condition')
    page.locator('[data-view=replay]').click();page.locator('#replay-load').click();expect(page.locator('#replay-progress')).to_contain_text('1 / 1')
    page.locator('#show-conclusions').check();page.locator('#replay-detections tbody tr').first.click()
    expect(page.locator('#replay-evidence')).to_contain_text('A-condition')
    draft={'case_id':'unsaved-human-draft','setup_id':'SET-0007','source':'human-notes-run-A','rule_version':'human-rule-version','start':when,'end':when,'annotator':'human-reviewer'}
    for name,value in draft.items():page.locator(f'#label-form [name={name}]').fill(value)
    expect(page.locator('#reconnect-stream')).to_be_visible();page.locator('#reconnect-stream').click()
    page.wait_for_timeout(100)
    assert streams[-1]=={'run':'A','after':'901'}
    passed('same-run manual SSE recovery retains the last received continuation cursor','controlled_run_change_browser_routes')
    capture(page,'run-change-A-loaded.png')
    # An old response is deliberately delivered after the run has changed.
    current['hold_events']=True;page.locator('#replay-load').click()
    for _ in range(20):
        if held:break
        page.wait_for_timeout(50)
    assert held
    current['run']='B';current['source']='controlled-stream-B';current['hold_events']=False;page.locator('#refresh').click()
    expect(page.locator('#run-id')).to_have_text('controlled-run-B')
    for route,value in held:route.fulfill(content_type='application/json',body=json.dumps(value))
    page.wait_for_timeout(200)
    observed={'orders':page.locator('#orders').text_content(),'fills':page.locator('#fills').text_content(),'replay_progress':page.locator('#replay-progress').text_content(),'replay_raw':page.locator('#replay-event').text_content(),'market_evidence':page.locator('#evidence').text_content(),'replay_evidence':page.locator('#replay-evidence').text_content(),'show_conclusions':page.locator('#show-conclusions').is_checked(),'streams':streams,'posts':posts,'draft':{name:page.locator(f'#label-form [name={name}]').input_value() for name in draft}}
    capture(page,'run-change-B-after-refresh.png')
    if not expect_fixed:
        assert 'A-old-order' in observed['orders'] and 'A-old-fill' in observed['fills']
        assert 'A-raw' in observed['replay_raw'] and 'A-condition' in observed['market_evidence']
        observed['status']='defect_reproduced';observed['scope']='Controlled synthetic routes demonstrate stale run-A state after a run-B refresh; not production market data.'
    else:
        assert 'A-' not in observed['orders'] and 'A-' not in observed['fills']
        assert 'B-order' in observed['orders'] and 'B-fill' in observed['fills']
        passed('new run clears old paginated orders and fills while loading only its own snapshot','controlled_run_change_browser_routes')
        assert 'A-raw' not in observed['replay_raw'] and 'A-condition' not in observed['market_evidence'] and 'A-condition' not in observed['replay_evidence']
        assert observed['replay_progress']=='日志中没有事件' and not observed['show_conclusions']
        passed('new run clears selected evidence, replay and conclusions and rejects a late old raw response','controlled_run_change_browser_routes')
        assert any(row=={'run':'B','after':'10'} for row in streams)
        passed('new run reconnects SSE from its own committed cursor rather than the prior run cursor','controlled_run_change_browser_routes')
        assert observed['draft']==draft
        expect(page.locator('#label-run-change')).to_be_visible();expect(page.locator('#label-run-change')).to_contain_text('草稿已保留')
        page.locator('#label-form').dispatch_event('submit');expect(page.locator('#toast')).to_contain_text('重新确认')
        assert not posts
        passed('human draft survives a run change and cannot be submitted until its new-run provenance is reconfirmed','controlled_run_change_browser_routes')
        page.locator('#label-confirm-run').click();expect(page.locator('#label-run-change')).to_be_hidden();assert not posts
        passed('explicit provenance reconfirmation does not submit or repeat any write operation','controlled_run_change_browser_routes')
        current['run']='C';page.locator('#refresh').click();expect(page.locator('#run-id')).to_have_text('controlled-run-C')
        expect(page.locator('#label-run-change')).to_be_hidden()
        assert all(page.locator(f'#label-form [name={name}]').input_value()==value for name,value in draft.items())
        assert not posts
        passed('same-source new run preserves the human draft without adding a provenance confirmation step','controlled_run_change_browser_routes')
        empty=browser.new_page();empty.route('**/api/**',route_api);empty.goto(base,wait_until='domcontentloaded');empty.wait_for_selector('#watchlist .symbol-card')
        current['run']='D';current['source']='controlled-stream-D';empty.locator('#refresh').click();expect(empty.locator('#run-id')).to_have_text('controlled-run-D')
        expect(empty.locator('#label-run-change')).to_be_hidden();assert not posts;empty.close()
        passed('empty annotation form changes source without unnecessary confirmation or automatic writes','controlled_run_change_browser_routes')
        observed['status']='passed';observed['scope']='Current frontend with controlled synthetic routes; independent long-run Runner was not opened or modified.'
    assert not errors,errors
    observed['page_errors']=errors;page.close()
    (output/('run-change-after.json' if expect_fixed else 'run-change-before.json')).write_text(json.dumps(observed,ensure_ascii=False,indent=2)+'\n')
    return observed


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--url',required=True);parser.add_argument('--output',type=Path,required=True);parser.add_argument('--before',action='store_true');args=parser.parse_args()
    root=Path(__file__).resolve().parents[2];out=args.output.resolve();assert not out.is_relative_to(root);out.mkdir(parents=True,exist_ok=True);checks=[];screens=[]
    def capture(page,name):page.evaluate('window.scrollTo(0,0)');page.screenshot(path=str(out/name),full_page=True);screens.append(name)
    def passed(name,scope):checks.append({'name':name,'scope':scope,'status':'passed'})
    with sync_playwright() as playwright:
        browser=playwright.chromium.launch(headless=True);result=exercise_run_switch(browser,args.url.rstrip('/'),out,capture,passed,not args.before);browser.close()
    evidence={'status':result['status'],'executed_at_utc':datetime.now(timezone.utc).isoformat(),'checks':checks,'source_kind':'synthetic','market_validation':'not_run','frontend_files':{name:hashlib.sha256((root/name).read_bytes()).hexdigest() for name in ('workbench/app.js','workbench/index.html','workbench/tests/run_switch_check.py')},'screenshots_sha256':{name:hashlib.sha256((out/name).read_bytes()).hexdigest() for name in screens},'scope':result['scope']}
    (out/'run-change-validation.json').write_text(json.dumps(evidence,ensure_ascii=False,indent=2)+'\n');print(json.dumps({'status':evidence['status'],'checks':len(checks),'output':str(out)},ensure_ascii=False))


if __name__=='__main__':main()
