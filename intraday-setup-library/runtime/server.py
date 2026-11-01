"""Loopback-only workbench HTTP API and resumable committed SSE events."""
from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import mimetypes
from pathlib import Path
import secrets
import threading
import time
from urllib.parse import parse_qs, unquote, urlsplit

from .config import ROOT
from .export import export_run


class WorkbenchServer(ThreadingHTTPServer):
    daemon_threads=True
    allow_reuse_address=True

    def __init__(self,runner,port=None):
        self.runner=runner
        self.token=secrets.token_urlsafe(32)
        self.closed=threading.Event()
        super().__init__(("127.0.0.1",port if port is not None else runner.config.port),Handler)
        self.thread=None

    def start(self):
        self.thread=threading.Thread(target=self.serve_forever,name="local-workbench",daemon=True)
        self.thread.start()
        return self

    def close(self):
        self.closed.set()
        if self.thread: self.shutdown(); self.thread.join(timeout=3)
        self.server_close()


class Handler(BaseHTTPRequestHandler):
    protocol_version="HTTP/1.1"

    def log_message(self,*args):
        pass  # request paths can carry user data; logs are structured elsewhere

    @property
    def runner(self): return self.server.runner

    def _valid_host(self):
        port=self.server.server_port
        return self.headers.get("Host") in {f"127.0.0.1:{port}",f"localhost:{port}"}

    def _send(self,value,status=200):
        raw=json.dumps(value,ensure_ascii=False,allow_nan=False).encode()
        self.send_response(status)
        self.send_header("Content-Type","application/json; charset=utf-8")
        self.send_header("Content-Length",str(len(raw)))
        self.send_header("Cache-Control","no-store")
        self.send_header("X-Content-Type-Options","nosniff")
        self.end_headers(); self.wfile.write(raw)

    def _static(self,path):
        parsed=unquote(path)
        if parsed=="/": base=ROOT/"workbench"; target=base/"index.html"
        elif parsed.startswith("/workbench/"): base=ROOT/"workbench"; target=base/parsed.removeprefix("/workbench/")
        elif parsed.startswith("/library/"): base=ROOT/"site"; target=base/parsed.removeprefix("/library/")
        elif parsed.startswith("/assets/"): base=ROOT/"assets"; target=base/parsed.removeprefix("/assets/")
        elif parsed.startswith("/downloads/"):
            name=parsed.removeprefix("/downloads/")
            if '/' in name or not name.endswith('.zip'): return self._send({"error":"not_found"},404)
            base=self.runner.store.directory/"exports"; target=base/name
        else: return self._send({"error":"not_found"},404)
        target=target.resolve()
        if not target.is_relative_to(base.resolve()) or any(p.startswith('.') for p in target.relative_to(base.resolve()).parts):
            return self._send({"error":"not_found"},404)
        if target.is_dir(): target=target/"index.html"
        if not target.is_file(): return self._send({"error":"not_found"},404)
        content=target.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type",mimetypes.guess_type(target.name)[0] or "application/octet-stream")
        self.send_header("Content-Length",str(len(content)))
        self.send_header("X-Content-Type-Options","nosniff")
        self.send_header("Content-Security-Policy","default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'")
        self.end_headers(); self.wfile.write(content)

    def _sse(self,query):
        after=int(self.headers.get("Last-Event-ID",query.get("after",["0"])[0]))
        if after<0: raise ValueError("invalid cursor")
        self.send_response(200)
        self.send_header("Content-Type","text/event-stream")
        self.send_header("Cache-Control","no-cache")
        self.send_header("Connection","close")
        self.end_headers()
        self.close_connection=True
        try:
            while not self.server.closed.is_set():
                rows=self.runner.store.outbox(after,100)
                if rows:
                    for row in rows:
                        text=f"id: {row['id']}\ndata: {json.dumps(row,ensure_ascii=False,allow_nan=False)}\n\n"
                        self.wfile.write(text.encode()); after=row['id']
                else: self.wfile.write(b": heartbeat\n\n")
                self.wfile.flush()
                self.server.closed.wait(.5)
        except (BrokenPipeError,ConnectionResetError,OSError): pass

    def do_GET(self):
        if not self._valid_host(): return self._send({"error":"invalid_host"},403)
        parts=urlsplit(self.path); path=parts.path; q=parse_qs(parts.query)
        try:
            if not path.startswith("/api/"): return self._static(path)
            if path=="/api/status": return self._send({**self.runner.status(),"session_token":self.server.token})
            if path=="/api/account":
                a=self.runner.store.meta("account",{})
                times=list(a.get("valuation_times",{}).values())
                from recognition.models import timestamp
                from datetime import datetime,timezone
                stale=self.runner.config.mode=="live" and bool(times) and any((datetime.now(timezone.utc)-timestamp(t)).total_seconds()>30 for t in times)
                return self._send({**a,"cash":a.get("balance"),"available_cash":a.get("available"),"positions_count":a.get("position_count",0),
                    "valuation_time":max(times) if times else None,"valuation_stale":stale})
            if path=="/api/positions": return self._send({"items":self.runner.store.meta("positions",[]),"next_cursor":None})
            if path=="/api/coverage":
                data=self.runner.store.meta("coverage",{})
                source=self.runner.store.meta("source",{})
                connectivity=self.runner.store.meta("execution_connectivity",{})
                sessions=self.runner.store.meta("execution_sessions",{})
                proxy=self.runner.policy.model=="next_bar_proxy_experimental"
                scope=source.get("market_scope",source.get("source_scope",source.get("scope","unknown")))
                items=[]
                for key,value in data.items():
                    observations=value.get("last_frame_missing_by_stream_scale",{})
                    per_symbol={}
                    for symbol in self.runner.config.symbols:
                        relevant={k:v for k,v in observations.items() if k.startswith(symbol+"|")}
                        eligible=any(not v for v in relevant.values())
                        missing=[] if eligible else sorted(set(m for v in relevant.values() for m in v)) if relevant else value.get("requirements",[])
                        venue=sessions.get(symbol,{}).get("venue")
                        execution_observations={k:v for k,v in relevant.items() if venue and k.startswith(symbol+"|"+venue+"|")}
                        execution_eligible=any(not v for v in execution_observations.values())
                        quote=self.runner.store.meta(("bar:" if proxy else "quote:")+symbol)
                        fresh=bool(venue and quote) and quote.get("venue")==venue and quote.get("quality")=="complete"
                        if fresh and self.runner.config.mode=="live":
                            from datetime import datetime,timezone
                            from recognition.models import timestamp
                            max_age=self.runner.policy.quote_max_age_seconds
                            fresh=(datetime.now(timezone.utc)-timestamp(quote["arrival_time"])).total_seconds()<=max_age and (timestamp(quote["arrival_time"])-timestamp(quote["event_time"])).total_seconds()<=max_age
                        fresh=fresh and symbol not in connectivity.get("halted",[]) and symbol not in connectivity.get("disconnected",[])
                        per_symbol[symbol]={"recognition_eligible":eligible,"execution_eligible":execution_eligible and fresh,
                                            "missing":missing,"observations":relevant,"execution_venue":venue,
                                            "execution_observations":execution_observations}
                    eligible=any(v["recognition_eligible"] for v in per_symbol.values())
                    executable=any(v["execution_eligible"] for v in per_symbol.values())
                    missing=sorted(set(m for v in per_symbol.values() for m in v["missing"]))
                    setup,variant=key.split(":",1)
                    items.append({**value,"rule_id":key,"setup_id":setup,"variant_id":variant,"missing_capabilities":missing,
                        "recognition_eligible":eligible,"auto_simulate":executable and not connectivity.get("entry_paused",False),"per_symbol":per_symbol,"execution_model":self.runner.policy.model,
                        "source_scope":scope,"migration_status":"migration_unvalidated"})
                return self._send({"items":items,"next_cursor":None})
            if path=="/api/profiles":
                from paper import execution_profiles
                return self._send({"items":execution_profiles(self.runner.policy),"next_cursor":None})
            if path=="/api/labels": return self._send(self.runner.store.label_page(q.get("before",[None])[0],int(q.get("limit",[100])[0])))
            if path=="/api/reports":
                from .research import committed_research_report
                report=committed_research_report(self.runner)
                return self._send({"items":[report[key] for key in ("runtime","recognition_evaluation","experimental_account")],
                                   "snapshot":report["snapshot"],"next_cursor":None})
            if path=="/api/events/stream": return self._sse(q)
            if path=="/api/events":
                limit=max(1,min(int(q.get("limit",[100])[0]),1000))
                after=int(q.get("after",[0])[0])
                if after<0: raise ValueError("invalid cursor")
                items=self.runner.store.journal(after,limit+1,start=q.get("start",[None])[0],end=q.get("end",[None])[0])
                items=[r for r in items if r["processed"]]
                more=len(items)>limit; items=items[:limit]
                return self._send({"items":items,"next_cursor":items[-1]["seq"] if items and more else None,"time_filter":{"start":q.get("start",[None])[0],"end":q.get("end",[None])[0],"clock":"known_at"}})
            topic=path.removeprefix("/api/")
            if topic in {"bars","detections","orders","fills","account_events","decisions"}:
                after=int(q.get("after",[0])[0]); limit=int(q.get("limit",[100])[0]); symbol=q.get("symbol",[None])[0]
                if after<0 or limit<1: raise ValueError("invalid cursor/limit")
                if topic=="orders" and "after" not in q and "before" not in q:
                    return self._send({"items":self.runner.store.latest_records(topic,limit),"next_cursor":None})
                if "after" in q: return self._send(self.runner.store.collection(topic,after,limit,symbol))
                return self._send(self.runner.store.history(topic,q.get("before",[None])[0],limit,symbol))
            return self._send({"error":"not_found"},404)
        except (ValueError,TypeError,KeyError): return self._send({"error":"invalid_request"},400)
        except Exception: return self._send({"error":"read_failed"},503)

    def do_POST(self):
        if not self._valid_host(): return self._send({"error":"invalid_host"},403)
        origin=self.headers.get("Origin")
        expected={f"http://127.0.0.1:{self.server.server_port}",f"http://localhost:{self.server.server_port}"}
        if origin is not None and origin not in expected: return self._send({"error":"invalid_origin"},403)
        if not secrets.compare_digest(self.headers.get("X-Session-Token",""),self.server.token):
            return self._send({"error":"session_token_required"},403)
        try:
            size=int(self.headers.get("Content-Length","0"))
            if not 0<=size<=65536: return self._send({"error":"body_too_large"},413)
            row=json.loads(self.rfile.read(size)) if size else {}
            if not isinstance(row,dict): raise ValueError("body must be object")
            path=urlsplit(self.path).path
            request_id=self.headers.get("Idempotency-Key")
            if path.startswith("/api/runtime/"):
                return self._send(self.runner.submit_control(path.split('/')[-1],request_id),202)
            if path=="/api/labels": return self._send(self.runner.submit_label(row,request_id),202)
            if path=="/api/export":
                result=export_run(self.runner)
                return self._send({**result,"download_url":"/downloads/"+Path(result["archive"]).name},201)
            return self._send({"error":"not_found"},404)
        except ValueError as exc:
            from .store import CollisionError
            return self._send({"error":"idempotency_conflict" if isinstance(exc,CollisionError) else "invalid_request"},409 if isinstance(exc,CollisionError) else 400)
        except (TypeError,KeyError): return self._send({"error":"invalid_request"},400)
        except Exception: return self._send({"error":"operation_failed"},500)
