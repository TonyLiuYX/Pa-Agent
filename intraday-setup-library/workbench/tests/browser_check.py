"""Exercise user-visible controls against a running local workbench."""
from pathlib import Path
import argparse
import hashlib
import json
import platform
import time
from datetime import datetime, timezone
from urllib.parse import urlencode
from playwright.sync_api import sync_playwright, expect


def main():
    parser=argparse.ArgumentParser();parser.add_argument("--url",default="http://127.0.0.1:8769");parser.add_argument("--output",default="/tmp/intraday-workbench-ui");parser.add_argument("--fixture",action="store_true");args=parser.parse_args()
    out=Path(args.output);out.mkdir(parents=True,exist_ok=True)
    started=time.perf_counter()
    with sync_playwright() as p:
        browser=p.chromium.launch(headless=True);page=browser.new_page(viewport={"width":1440,"height":1080},device_scale_factor=1)
        errors=[];page.on("pageerror",lambda error:errors.append(str(error)))
        def capture(name):
            page.evaluate("window.scrollTo(0,0)")
            page.screenshot(path=str(out/name),full_page=True)
        # A live SSE stream does not reach network idle. Wait for the committed
        # snapshot's visible readiness instead of timing out on healthy traffic.
        page.goto(args.url,wait_until="domcontentloaded");page.wait_for_selector("#watchlist .symbol-card")
        expect(page.locator("#overview-metrics")).to_contain_text("账户净值")
        status=page.request.get(args.url.rstrip("/")+"/api/status").json()
        maintenance=status.get("health",{}).get("memory_maintenance")
        expected_maintenance={"ready":"待首次维护","completed":"完成","unsupported":"环境不支持","error":"错误"}.get((maintenance or {}).get("status"),"未知") if maintenance else "未报告"
        expect(page.locator("#memory-maintenance-state")).to_contain_text(expected_maintenance)
        if args.fixture:
            memory_cases=[
                ("ready",{"status":"ready","attempts":0,"backend":"fixture_backend"},"待首次维护 · 0次"),
                ("completed_bytes_zero",{"status":"completed","attempts":3,"backend":"darwin_malloc_zone_pressure_relief","api_reported_bytes":0,"api_return_flag":None,"last_duration_seconds":.002},"完成 · 3次"),
                ("completed_flag_zero",{"status":"completed","attempts":4,"backend":"glibc_malloc_trim","api_reported_bytes":None,"api_return_flag":0,"last_duration_seconds":.001},"完成 · 4次"),
                ("unsupported",{"status":"unsupported","attempts":2,"backend":None,"reason":"unsupported_platform"},"环境不支持 · 2次"),
                ("error",{"status":"error","attempts":1,"backend":"fixture_backend","reason":"native_call_failed","error_code":"OSError"},"错误 · 1次"),
                ("missing",None,"未报告")]
            for name,value,expected in memory_cases:
                payload={**status,"health":{**status.get("health",{})}}
                if value is None:payload["health"].pop("memory_maintenance",None)
                else:payload["health"]["memory_maintenance"]=value
                page.route("**/api/status",lambda route:route.fulfill(status=200,content_type="application/json",body=json.dumps(payload)))
                page.locator("#refresh").click()
                expect(page.locator("#memory-maintenance-state")).to_have_text(expected)
                assert "malloc" not in page.locator("#health").inner_text(),"backend implementation leaked into the primary health summary"
                if value is None:assert page.locator("#memory-maintenance-details").is_hidden()
                else:
                    assert json.loads(page.locator("#memory-maintenance-json").text_content())==value
                    if name.startswith("completed"):
                        assert "无效" not in page.locator("#memory-maintenance-state").inner_text()
                        assert "未释放" not in page.locator("#memory-maintenance-state").inner_text()
                page.unroute("**/api/status")
            page.locator("#refresh").click()
            expect(page.locator("#memory-maintenance-state")).to_have_text("未报告")
        assert not page.locator('#label-form [name="complete_coverage"]').is_checked()
        if args.fixture:expect(page.locator("#reports")).to_contain_text("没有已提交标注")
        capture("overview.png")
        assert page.locator(".nav-button[aria-current=page]").inner_text().endswith("总览")
        page.locator('.nav-button[data-view="market"]').click();page.wait_for_selector("#market-chart canvas")
        chart_width=page.locator("#market-chart").evaluate("element=>element.clientWidth")
        expect(page.locator("#market-chart .tv-lightweight-charts")).to_have_css("width",f"{chart_width}px")
        assert page.locator("#market-chart canvas").first.evaluate("element=>element.closest('td').clientWidth")>chart_width-100,"chart plot clipped by surrounding table styles"
        chosen=page.locator("#detections tbody tr").first
        setup=chosen.locator("td").first.inner_text().splitlines()[0]
        chosen.focus();page.keyboard.press("Enter")
        assert setup in page.locator("#evidence").inner_text()
        capture("market.png")
        page.locator('.nav-button[data-view="account"]').click()
        assert page.locator("#account-metrics").inner_text()
        if args.fixture:assert "待退出" in page.locator("#positions").inner_text()
        page.locator('.nav-button[data-view="replay"]').click();page.locator("#replay-load").click();expect(page.locator("#replay-progress")).to_contain_text("1 /")
        assert not page.locator("#show-conclusions").is_checked();assert not page.locator("#replay-conclusions").is_visible()
        first=page.locator("#replay-event").inner_text();page.locator("#replay-step").click();assert page.locator("#replay-event").inner_text()!=first
        page.locator("#replay-reset").click();assert page.locator("#replay-event").inner_text()==first
        if not args.fixture:
            seed=page.request.get(args.url.rstrip("/")+"/api/events?limit=1").json()["items"][0]
            payload=seed.get("event") or seed.get("payload") or seed
            bound=payload.get("arrival_time") or payload.get("at") or seed.get("recorded_at")
            expected=page.request.get(args.url.rstrip("/")+"/api/events?"+urlencode({"limit":100,"start":bound,"end":bound})).json()
            assert expected["items"],"inclusive range omitted known boundary"
            page.locator("#replay-start").fill(bound);page.locator("#replay-start").press("Tab")
            page.locator("#replay-end").fill(bound);page.locator("#replay-end").press("Tab")
            expect(page.locator("#replay-progress")).to_contain_text(f'1 / {len(expected["items"])}')
            assert "筛选" in page.locator("#replay-range").inner_text()
        if args.fixture:
            page.locator("#replay-start").fill("2026-10-02T13:31:00+00:00");page.locator("#replay-start").press("Tab")
            page.locator("#replay-end").fill("2026-10-02T13:31:00+00:00");page.locator("#replay-end").press("Tab")
            expect(page.locator("#replay-progress")).to_contain_text("1 / 1")
            assert "fixture-1" in page.locator("#replay-event").text_content()
            assert "筛选" in page.locator("#replay-range").inner_text()
            assert page.locator('#label-form [name="source_kind"]').input_value()=="synthetic"
            assert page.locator('#label-form [name="source_kind"] option[value="real_market"]').evaluate("option=>option.disabled")
            fields=dict(case_id="browser-label-1",setup_id="SET-0005",variant_id="base",symbol="SPY",venue="IEX",start="2026-10-02T09:40:00-04:00",end="2026-10-02T09:44:00-04:00",trigger_time="2026-10-02T09:42:00-04:00",group_id="2026-10-02-ui",source="synthetic-browser-fixture",annotator="browser-check",rule_version="1.0.0")
            for name,value in fields.items():page.locator(f'#label-form [name="{name}"]').fill(value)
            page.locator(".stage-annotations summary").click()
            page.locator('#label-form [name="candidate_start"]').fill(fields["start"])
            page.locator('#label-form [name="candidate_end"]').fill(fields["end"])
            pending={"accepted":False,"polls":0}
            def acknowledge_label(route):
                response=route.fetch()
                if route.request.method=="POST":
                    pending["accepted"]=True
                    route.fulfill(status=202,content_type="application/json",body=json.dumps({"accepted":True,"event_seq":999000}))
                else:route.fulfill(response=response)
            def delayed_commit_status(route):
                response=route.fetch();body=response.json()
                if pending["accepted"]:
                    pending["polls"]+=1
                    body["processed_cursor"]=999000 if pending["polls"]>=3 else 998999
                route.fulfill(response=response,json=body)
            page.route("**/api/labels",acknowledge_label)
            page.route("**/api/status",delayed_commit_status)
            page.get_by_role("button",name="保存标注").click()
            expect(page.locator("#toast")).to_contain_text("保存中")
            assert page.get_by_role("button",name="保存标注").is_disabled()
            expect(page.locator("#toast")).to_contain_text("标注已保存")
            assert pending["polls"]>=3,"accepted label reported saved before its committed cursor"
            page.unroute("**/api/labels",acknowledge_label)
            page.unroute("**/api/status",delayed_commit_status)
            page.locator("#show-existing-labels").check()
            page.wait_for_selector("#labels tbody tr")
            assert "browser-label-1" in page.locator("#labels").inner_text()
            saved=page.request.get(args.url.rstrip("/")+"/api/labels").json()["items"][0]
            assert saved["complete_coverage"] is False
            attempts=[]
            def acknowledge_without_confirmation(route):
                response=route.fetch();attempts.append(route.request.post_data)
                route.fulfill(status=202,content_type="application/json",body=json.dumps({"accepted":True,"event_seq":999001}))
            page.route("**/api/labels",acknowledge_without_confirmation)
            page.evaluate("window.realDateNow=Date.now; let calls=0; Date.now=()=>window.realDateNow()+20000*(calls++);")
            page.get_by_role("button",name="保存标注").click()
            expect(page.locator("#toast")).to_contain_text("等待保存完成")
            expect(page.get_by_role("button",name="保存标注")).to_be_enabled()
            assert len(attempts)==1,"pending save automatically submitted a second POST"
            assert "标注已保存" not in page.locator("#toast").inner_text()
            page.evaluate("Date.now=window.realDateNow; delete window.realDateNow;")
            page.unroute("**/api/labels",acknowledge_without_confirmation)
            page.locator('#label-form [name="positive"]').select_option("false")
            page.locator('#label-form [name="complete_coverage"]').check()
            page.get_by_role("button",name="保存标注").click()
            expect(page.locator("#labels")).to_contain_text("负例")
            assert page.locator("#labels tbody tr").count()==1,"old label revision appears twice"
            saved=page.request.get(args.url.rstrip("/")+"/api/labels").json()["items"][0]
            assert saved["complete_coverage"] is True
            page.locator('#label-form [name="disputed"]').check()
            assert page.locator('#label-form [name="complete_coverage"]').is_disabled()
            assert not page.locator('#label-form [name="complete_coverage"]').is_checked()
            page.locator('#label-form [name="disputed"]').uncheck()
            page.locator("#pause").click();page.wait_for_selector("#resume:visible");page.locator("#resume").click();page.wait_for_selector("#pause:visible")
        capture("replay.png")
        page.locator('.nav-button[data-view="research"]').click();assert "市场验证未运行" in page.locator("#validation-badge").inner_text()
        page.wait_for_selector("#reports .evaluation-state")
        if args.fixture:
            expect(page.locator("#reports")).to_contain_text("三阶段事件匹配")
            assert page.locator("#reports tbody tr").count()==3
            assert all(key in page.locator("#reports").inner_text() for key in ("候选","形成","触发","TP","FP","FN"))
        else:
            scores=page.request.get(args.url.rstrip("/")+"/api/reports").json()["items"]
            score=next(item for item in scores if item.get("kind")=="recognition_evaluation")
            if score["status"]=="not_run":
                expect(page.locator("#reports")).to_contain_text("没有已提交标注")
                assert not page.locator("#reports tbody tr").count()
            else:
                assert score["status"]=="completed","use a clean demo run for base browser validation"
                expect(page.locator("#reports")).to_contain_text("三阶段事件匹配")
        capture("research.png")
        if args.fixture:
            for rejected,reason,expected_text in (("version_mismatch","annotation_and_detection_rule_versions_must_agree","版本不一致"),
                    ("capacity_blocked","committed_label_limit_exceeded","标注数量超过评价容量上限")):
                body={"items":[{"kind":"recognition_evaluation","status":rejected,"reason":reason,"metrics":None,"label_count":1}],"next_cursor":None}
                page.route("**/api/reports*",lambda route:route.fulfill(status=200,content_type="application/json",body=json.dumps(body)))
                page.locator("#refresh").click();expect(page.locator("#reports")).to_contain_text(expected_text)
                assert not page.locator("#reports tbody tr").count(),"blocked evaluation fabricated metrics"
                page.unroute("**/api/reports*")
            page.locator("#refresh").click();expect(page.locator("#reports")).to_contain_text("三阶段事件匹配")
        page.set_viewport_size({"width":390,"height":844});page.locator('.nav-button[data-view="overview"]').click();capture("mobile.png")
        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth"),"mobile overflow"
        page.locator('.nav-button[data-view="market"]').click()
        chart_width=page.locator("#market-chart").evaluate("element=>element.clientWidth")
        expect(page.locator("#market-chart .tv-lightweight-charts")).to_have_css("width",f"{chart_width}px")
        assert page.locator("#market-chart canvas").first.evaluate("element=>element.closest('td').clientWidth")>chart_width-100,"mobile chart plot clipped"
        page.locator('.nav-button[data-view="overview"]').click()
        if args.fixture:
            unknown=browser.new_page()
            unknown_status={**status,"run_id":"unknown-replay-fixture","mode":"replay","source_kind":"unknown"}
            unknown.route("**/api/status",lambda route:route.fulfill(status=200,content_type="application/json",body=json.dumps(unknown_status)))
            unknown.goto(args.url,wait_until="domcontentloaded");unknown.wait_for_selector("#watchlist .symbol-card")
            assert unknown.locator('#label-form [name="source_kind"]').input_value()==""
            assert not unknown.locator('#label-form [name="source_kind"] option[value="real_market"]').evaluate("option=>option.disabled")
            unknown.locator('#label-form [name="source_kind"]').select_option("real_market",force=True)
            unknown.locator("#refresh").click()
            expect(unknown.locator('#label-form [name="source_kind"]')).to_have_value("real_market")
            unknown.close()
            delayed=browser.new_page();held=[]
            def delay_first_labels(route):
                if not held:held.append(route)
                else:route.continue_()
            delayed.route("**/api/labels?*",delay_first_labels)
            delayed.goto(args.url,wait_until="domcontentloaded")
            expect(delayed.locator("#mode-badge")).to_contain_text("演示")
            delayed.locator('.nav-button[data-view="replay"]').click()
            for name,value in {**fields,"case_id":"browser-late-snapshot"}.items():delayed.locator(f'#label-form [name="{name}"]').fill(value)
            delayed.get_by_role("button",name="保存标注").click()
            expect(delayed.locator("#labels")).to_contain_text("browser-late-snapshot")
            held[0].fulfill(status=200,content_type="application/json",body=json.dumps({"items":[],"next_cursor":None}))
            expect(delayed.locator("#overview-metrics")).to_contain_text("账户净值")
            expect(delayed.locator("#labels")).to_contain_text("browser-late-snapshot")
            delayed.close()
        assert not errors,errors
        browser_version=browser.version
        browser.close()
    screenshots={name:hashlib.sha256((out/name).read_bytes()).hexdigest() for name in
                 ("overview.png","market.png","replay.png","research.png","mobile.png")}
    release=json.loads((Path(__file__).resolve().parents[2]/"release.json").read_text())
    report={"status":"passed","version":release["version"],"content_date":release["content_date"],"executed_at_utc":datetime.now(timezone.utc).isoformat(),
            "duration_seconds":round(time.perf_counter()-started,3),"environment":{"python":platform.python_version(),"browser":"Chromium "+browser_version},
            "fixture":args.fixture,"run_id":status.get("run_id"),"mode":status.get("mode"),
            "identity":status.get("identity",{}),"market_validation":"not_run",
            "checks":["five views render","memory maintenance health summary reflects the current API without inferring released bytes","keyboard evidence selection","local candlestick chart fits desktop and mobile containers","account state renders","raw-event step and reset","inclusive known-time range resets replay and filters events","conclusions hidden by default","complete annotation coverage unchecked by default","market validation boundary visible","research evaluation states and metrics render","390px viewport has no document overflow","no JavaScript page errors"],
            "screenshots_sha256":screenshots,"console_errors":errors,
            "scope":"Browser behavior and rendering; synthetic/recorded engineering evidence, no real-market performance validation."}
    if args.fixture:report["checks"].extend(["memory maintenance ready waits for its first call","memory maintenance completed preserves a zero native byte result","memory maintenance completed preserves a zero native flag","memory maintenance unsupported platform is explicit","memory maintenance error is explicit with raw details","older API missing memory maintenance remains unreported","synthetic run locks annotation source type","unknown replay requires and preserves manually declared source","stage-window label submitted with session token","accepted label remains saving until its committed cursor is visible","expired commit wait remains explicitly pending and performs no automatic duplicate POST","complete coverage false and true submit as booleans; disputed disables claim","three stage TP FP FN values render separately","version mismatch and capacity limits block metrics with visible reasons","latest label revision replaces earlier case without duplicates","older in-flight snapshot cannot replace newly submitted label","pause and resume entry controls"])
    (out/"workbench-validation.json").write_text(json.dumps(report,ensure_ascii=False,indent=2)+"\n")
    print(f"Browser checks passed; screenshots: {out}")


if __name__=="__main__":main()
