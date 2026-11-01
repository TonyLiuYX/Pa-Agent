"""Browser-contract fixture. All prices and outcomes are synthetic."""
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit, parse_qs
from datetime import datetime
import json
import sys

ROOT = Path(__file__).resolve().parents[2]
T = "2026-10-02T13:42:00+00:00"
LABELS = []
DETECTION = dict(detection_id="synthetic-d1", setup_id="SET-0005", variant_id="base", symbol="SPY", venue="IEX", direction=1,
                 state="triggered", data_status="complete", identified_at=T, evidence_cutoff=T, scale_seconds=60,
                 source_version="1.0.0", rule_version="1.0.0", market_status="migration_unvalidated", missing=[],
                 conditions=[dict(stage="trigger", feature="return_inside", passed=True, value=True)], anchor={"captures":{"level":100.15}})
STATE = dict(run_id="synthetic-ui-fixture", mode="demo", source_kind="synthetic",state="running", source="synthetic", source_scope="合成测试资料",watchlist=["SPY","QQQ","AAPL","MSFT","NVDA"],last_event_time=T,event_count=6,last_seq=6,outbox_cursor=6,session_token="fixture-token",health={"connection":"connected","queue_depth":0})


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs): super().__init__(*args, directory=str(ROOT), **kwargs)
    def log_message(self, *args): pass
    def send_json(self, value, code=200):
        body=json.dumps(value, ensure_ascii=False).encode();self.send_response(code);self.send_header("Content-Type","application/json");self.send_header("Content-Length",str(len(body)));self.end_headers();self.wfile.write(body)
    def do_GET(self):
        parsed=urlsplit(self.path); path=parsed.path; query=parse_qs(parsed.query)
        if path=="/":self.path="/workbench/index.html";return super().do_GET()
        if path=="/api/events/stream":self.send_response(204);self.end_headers();return
        if path=="/api/status":return self.send_json(STATE)
        if path=="/api/account":return self.send_json(dict(equity=100020,cash=90000,reserved=10000,unrealized_pnl=20,valuation_time=T))
        if path=="/api/detections":return self.send_json({"items":[DETECTION],"next_cursor":None})
        if path=="/api/bars":return self.send_json({"items":[dict(event_time=f"2026-10-02T13:{30+i:02d}:00+00:00",payload=dict(open=100+i*.05,high=100.1+i*.05,low=99.9+i*.05,close=100.04+i*.05)) for i in range(12)],"next_cursor":None})
        if path=="/api/positions":return self.send_json({"items":[dict(symbol="SPY",direction=1,quantity=99,entry_price=100.03,mark_price=100.23,state="pending_exit",setup_id="SET-0005",exit_reasons=["max_hold"])],"next_cursor":None})
        if path=="/api/orders":return self.send_json({"items":[dict(order_id="order-1",symbol="SPY",direction=1,purpose="entry",status="filled",quantity=99,submitted_at=T,reasons=[])],"next_cursor":None})
        if path=="/api/fills":return self.send_json({"items":[dict(fill_id="fill-1",symbol="SPY",quantity=99,price=100.03,slippage=.01,fee=0,filled_at=T)],"next_cursor":None})
        if path=="/api/coverage":return self.send_json({"items":[dict(setup_id="SET-0005",variant_id="base",recognizable=True,auto_simulate=True,missing=[],source_scope="合成测试",market_status="migration_unvalidated"),dict(setup_id="SET-0003",variant_id="base",recognizable=False,auto_simulate=False,missing=["depth"],source_scope="IEX",market_status="migration_unvalidated")],"next_cursor":None})
        if path=="/api/reports":
            stats=dict(tp=1,fp=2,fn=3,precision=1/3,recall=.25)
            group=dict(setup_id="SET-0005",variant_id="base",symbol="SPY",venue="IEX",source_kind="synthetic",
                split="test",regime="browser_fixture",candidate=stats,confirmed={**stats,"tp":4,"precision":4/6,"recall":4/7},triggered={**stats,"tp":5,"precision":5/7,"recall":5/8},acceptance="synthetic_logic_only")
            score=dict(kind="recognition_evaluation",title="识别评价",status="completed" if LABELS else "not_run",
                scope="合成浏览器展示资料",label_count=len(LABELS),eligible_label_count=len(LABELS),market_validation="not_run",
                metrics={"groups":[group]} if LABELS else None,reason=None if LABELS else "no_committed_labels")
            return self.send_json({"items":[score],"next_cursor":None})
        if path=="/api/decisions":return self.send_json({"items":[dict(symbol="SPY",detection_id="fixture-detection",reason="same_batch_opposite_signals",event_seq=4)],"next_cursor":None})
        if path=="/api/labels":
            before=int(query.get("before",[2**63])[0]);limit=int(query.get("limit",[50])[0])
            items=sorted((row for row in LABELS if row["record_id"]<before),key=lambda row:row["record_id"],reverse=True)
            return self.send_json({"items":items[:limit],"next_cursor":items[limit-1]["record_id"] if len(items)>limit else None})
        if path=="/api/events":
            items=[dict(seq=i,event=dict(event_id=f"fixture-{i}",kind="bar",symbol="SPY",venue="IEX",event_time=f"2026-10-02T13:{30+i:02d}:00+00:00",arrival_time=f"2026-10-02T13:{30+i:02d}:00+00:00",payload={"close":100+i})) for i in range(3)]
            for key,compare in (("start",lambda t,b:t>=b),("end",lambda t,b:t<=b)):
                if key in query:
                    bound=datetime.fromisoformat(query[key][0]);items=[row for row in items if compare(datetime.fromisoformat(row["event"]["arrival_time"]),bound)]
            return self.send_json({"items":items,"next_cursor":None})
        return super().do_GET()
    def do_POST(self):
        if self.headers.get("X-Session-Token")!="fixture-token":return self.send_json({"error":"missing token"},403)
        body=json.loads(self.rfile.read(int(self.headers.get("Content-Length",0))) or b"{}")
        if self.path.startswith("/api/runtime/"):
            action=self.path.rsplit("/",1)[1];STATE["state"]={"pause":"paused","resume":"running","stop":"stopped"}[action];return self.send_json(STATE)
        if self.path=="/api/labels":
            next_id=max((row["record_id"] for row in LABELS),default=0)+1
            LABELS[:]=[row for row in LABELS if row["case_id"]!=body["case_id"]]
            body["record_id"]=next_id;LABELS.append(body);return self.send_json(body)
        if self.path=="/api/export":return self.send_json({"synthetic":True,"run_id":STATE["run_id"]})
        self.send_json({"error":"unknown action"},404)


if __name__=="__main__":ThreadingHTTPServer(("127.0.0.1",int(sys.argv[1]) if len(sys.argv)>1 else 8769),Handler).serve_forever()
