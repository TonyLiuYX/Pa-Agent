"""Check committed annotation/report integration only in an isolated demo run.

Labels are constructed from synthetic output to verify UI/API consistency; they
are deliberately not independent annotations or real-market validation data.
"""
import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import time
from playwright.sync_api import sync_playwright, expect


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url",required=True)
    parser.add_argument("--output",required=True,type=Path)
    parser.add_argument("--require-active",action="store_true",help="Require collection to remain active when submitting the synthetic test label")
    args=parser.parse_args();args.output.mkdir(parents=True,exist_ok=True)
    with sync_playwright() as playwright:
        browser=playwright.chromium.launch(headless=True)
        page=browser.new_page(viewport={"width":1440,"height":1080})
        errors=[];page.on("pageerror",lambda error:errors.append(str(error)))
        page.goto(args.url,wait_until="domcontentloaded")
        page.wait_for_selector("#watchlist .symbol-card")
        status=page.request.get(args.url+"/api/status").json()
        if status.get("mode")!="demo" or status.get("source_kind")!="synthetic":
            raise ValueError("research write checks require an isolated synthetic demo; live/replay rejected")
        deadline=time.monotonic()+30
        detection=None
        while detection is None and time.monotonic()<deadline:
            rows=page.request.get(args.url+"/api/detections?limit=1000").json()["items"]
            detection=next((row for row in rows if row["state"]=="triggered" and row["data_status"]=="complete"),None)
            if detection is None:time.sleep(.1)
        if detection is None:raise ValueError("demo did not produce a complete triggered detection within 30 seconds")
        cutoff=datetime.fromisoformat(detection["evidence_cutoff"])
        page.locator('.nav-button[data-view="replay"]').click()
        fields={"case_id":"browser-actual-research-fixture","setup_id":detection["setup_id"],
            "variant_id":detection["variant_id"],"symbol":detection["symbol"],"venue":detection["venue"],
            "start":(cutoff-timedelta(minutes=6)).isoformat(),"end":cutoff.isoformat(),
            "trigger_time":cutoff.isoformat(),"group_id":"synthetic-ui-"+cutoff.date().isoformat(),
            "source":"synthetic-local-demo-ui-endpoint-fixture","annotator":"browser-engineering-test",
            "rule_version":detection["rule_version"],"regime":"synthetic_ui_contract"}
        for name,value in fields.items():page.locator(f'#label-form [name="{name}"]').fill(value)
        page.locator('#label-form [name="direction"]').select_option(str(detection["direction"]))
        assert not page.locator('#label-form [name="complete_coverage"]').is_checked()
        page.locator(".stage-annotations summary").click()
        for stage,delta in (("candidate",5),("formed",2),("triggered",0)):
            page.locator(f'#label-form [name="{stage}_start"]').fill((cutoff-timedelta(minutes=delta)).isoformat())
            page.locator(f'#label-form [name="{stage}_end"]').fill(cutoff.isoformat())
        page.locator('#label-form [name="complete_coverage"]').check()
        before_submission=page.request.get(args.url+"/api/status").json()
        source_active=not before_submission.get("health",{}).get("source_ended",False)
        if args.require_active and not source_active:raise ValueError("require-active verification needs a fresh collecting demo")
        with page.expect_response(lambda response:response.url.endswith("/api/labels") and response.request.method=="POST") as submission:
            page.get_by_role("button",name="保存标注").click()
        if not submission.value.ok:
            raise ValueError("actual annotation API rejected UI submission: "+submission.value.text())
        expect(page.locator("#toast")).to_contain_text("标注已保存",timeout=20000)
        page.locator("#show-existing-labels").check()
        expect(page.locator("#labels")).to_contain_text(fields["case_id"])
        label=page.request.get(args.url+"/api/labels").json()["items"][0]
        assert label["complete_coverage"] is True and label["source_kind"]=="synthetic"
        assert set(label["stage_windows"])=={"candidate","formed","triggered"}
        page.locator('.nav-button[data-view="research"]').click()
        expect(page.locator("#reports")).to_contain_text("三阶段事件匹配")
        report=page.request.get(args.url+"/api/reports").json()
        score=next(item for item in report["items"] if item["kind"]=="recognition_evaluation")
        assert score["status"]=="completed" and len(score["metrics"]["groups"])==1
        group=score["metrics"]["groups"][0]
        rendered=page.locator("#reports tbody tr")
        assert rendered.count()==3
        for index,stage in enumerate(("candidate","confirmed","triggered")):
            cells=rendered.nth(index).locator("td")
            assert [int(cells.nth(column).inner_text()) for column in (2,3,4)]==[group[stage][k] for k in ("tp","fp","fn")]
        assert group["triggered"]["tp"]==1
        assert score["coverage"][0]["direction"]==detection["direction"]
        assert score["snapshot"]["identity"]["algorithm_sha256"]==status["identity"]["algorithm_sha256"]
        page.evaluate("window.scrollTo(0,0)");page.screenshot(path=str(args.output/"research-committed.png"),full_page=True)
        # A schema-valid but wrong rule version must retain the annotation and
        # visibly block scoring, rather than quietly reporting a false negative.
        page.locator('.nav-button[data-view="replay"]').click()
        page.locator('#label-form [name="rule_version"]').fill("engineering-wrong-version")
        page.get_by_role("button",name="保存标注").click()
        expect(page.locator("#toast")).to_contain_text("标注已保存")
        page.locator('.nav-button[data-view="research"]').click()
        expect(page.locator("#reports")).to_contain_text("版本不一致")
        assert not page.locator("#reports tbody tr").count()
        page.evaluate("window.scrollTo(0,0)");page.screenshot(path=str(args.output/"research-version-mismatch.png"),full_page=True)
        page.locator('.nav-button[data-view="replay"]').click()
        page.locator('#label-form [name="rule_version"]').fill(detection["rule_version"])
        page.get_by_role("button",name="保存标注").click()
        expect(page.locator("#toast")).to_contain_text("标注已保存")
        page.locator('.nav-button[data-view="research"]').click()
        expect(page.locator("#reports")).to_contain_text("三阶段事件匹配")
        assert not errors,errors
        browser_version=browser.version;browser.close()
    release=json.loads((Path(__file__).resolve().parents[2]/"release.json").read_text())
    evidence={"status":"passed","version":release["version"],"content_date":release["content_date"],
        "executed_at_utc":datetime.now(timezone.utc).isoformat(),"fixture":True,"source":"actual local synthetic demo API",
        "run_id":status["run_id"],"identity":status["identity"],"browser":"Chromium "+browser_version,
        "source_active_at_submission":source_active,
        "market_validation":"not_run","checks":["writes restricted to demo synthetic mode",
            "complete coverage explicit true with three independent windows persisted",
            "actual committed report snapshot contains matching algorithm identity",
            "three visible stage TP FP FN equal actual API metrics",
            "complete coverage preserves selected direction scope",
            "wrong rule version visibly blocks scoring without fabricated stage values",
            "latest revision with corrected version restores scoring","no JavaScript page errors"],
        "evaluated_group":group,"evaluation_snapshot":score["snapshot"],"console_errors":errors,
        "screenshots_sha256":{name:hashlib.sha256((args.output/name).read_bytes()).hexdigest() for name in
            ("research-committed.png","research-version-mismatch.png")},
        "scope":"Synthetic annotations derived from fixture output test UI/API matching only; not blind human labels or market performance."}
    (args.output/"workbench-research-validation.json").write_text(json.dumps(evidence,ensure_ascii=False,indent=2)+"\n")
    print("Actual synthetic annotation/research browser integration passed")


if __name__=="__main__":main()
