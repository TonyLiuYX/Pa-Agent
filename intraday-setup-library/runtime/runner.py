"""Single writer: durable source journal -> recognition -> paper -> atomic outbox."""
from __future__ import annotations

from datetime import datetime, timezone
import fcntl
import hashlib
import io
import json
from pathlib import Path
import queue
import threading
import time
import uuid

from recognition.config import RecognitionConfig
from recognition.engine import RecognitionEngine
from recognition.models import MarketEvent
from recognition.policy import RuntimePolicy, CapacityExceeded
from recognition.provenance import algorithm_sha256
from paper import ExecutionPolicy, PaperSimulator
from .config import ROOT, RuntimeConfig, canonical, fingerprint
from .store import Store
from .memory import MemoryMaintenance, process_provenance


class RecoveryBlocked(ValueError):
    pass


class _HashTextWriter:
    """A UTF-8 digest sink for the streaming, canonical checkpoint writer."""
    def __init__(self):
        self.digest = hashlib.sha256()

    def write(self, text):
        self.digest.update(text.encode("utf-8"))
        return len(text)


class _Utf8TextWriter:
    """Keep the temporary checkpoint buffer in UTF-8, rather than UCS-4."""
    def __init__(self, buffer):
        self.buffer = buffer

    def write(self, text):
        self.buffer.write(text.encode("utf-8"))
        return len(text)


def engine_snapshot_sha256(engine):
    sink = _HashTextWriter()
    engine.write_snapshot_json(sink)
    return sink.digest.hexdigest()


def combined_snapshot_sha256(runner):
    sink = _HashTextWriter()
    sink.write('{"engine":')
    runner.engine.write_snapshot_json(sink)
    sink.write(',"paper":')
    sink.write(canonical(runner.paper.snapshot()))
    sink.write('}')
    return sink.digest.hexdigest()


def runtime_hash():
    h = hashlib.sha256()
    for package in ("runtime", "paper", "connectors"):
        for path in sorted((ROOT/package).glob("*.py")):
            h.update(path.relative_to(ROOT).as_posix().encode())
            h.update(path.read_bytes())
    for path in sorted((ROOT/"schema").glob("*.schema.json")) + [ROOT/"requirements-runtime.txt"]:
        h.update(path.relative_to(ROOT).as_posix().encode())
        h.update(path.read_bytes())
    return h.hexdigest()


class Runner:
    def __init__(self, directory, config=None, *, fault_hook=None):
        self.config=(config or RuntimeConfig()).validate()
        self.store=Store(directory)
        self._lease=(self.store.directory/"run.lock").open("a+")
        try:
            fcntl.flock(self._lease,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:
            self._lease.close(); self.store.close()
            raise RuntimeError("run directory already owned by another process") from None
        self._mutex=threading.RLock()
        self._fault_hook=fault_hook
        self.policy=ExecutionPolicy(**self.config.paper)
        self.retention=RuntimePolicy(**self.config.retention).validate()
        self._identity={"schema_version":"1.0.0","configuration_sha256":self.config.fingerprint(),
            "algorithm_sha256":algorithm_sha256(),"runtime_sha256":runtime_hash(),
            "execution_sha256":self.policy.fingerprint()}
        previous=self.store.meta("identity")
        if previous and previous != self._identity:
            self.close()
            raise RecoveryBlocked("run code/configuration/execution version changed; use a new directory and replay")
        self.run_id=self.store.meta("run_id") or str(uuid.uuid4())
        self.store.set_meta("identity",self._identity)
        self.store.set_meta("run_id",self.run_id)
        self.store.set_meta("configuration",self.config.to_dict())
        self.cursor=0
        self._record_cursor=0
        self.state="READY"
        self.connection_state="UNKNOWN"
        self.requested_state="RUNNING"
        self.error=None
        self.last_event=None
        self.last_checkpoint=time.monotonic()
        self.last_stats=0
        self._queue=queue.Queue(maxsize=self.config.queue_size)
        self._stop=threading.Event()
        self._source_stop=threading.Event()
        self._thread=self._producer=None
        self._source_description={"id":self.config.mode,"latency_class":self.config.mode,"market_scope":"synthetic" if self.config.mode=="demo" else "unknown"}
        self.metrics={"events_processed":0,"duplicates":0,"errors":0,"recoveries":0,"feed_seconds":[],"source_ended":False}
        self._memory_maintenance=MemoryMaintenance()
        self._process_provenance=process_provenance()
        self._new_processors()
        try:
            self.recover()
        except BaseException:
            self.close()
            raise
        self.store.set_meta("state",self.state)
        if self.store.meta("account") is None:
            self.store.set_meta("account",self.paper.account())
            self.store.set_meta("positions",[])

    def _new_processors(self):
        self.engine=RecognitionEngine(include_variants=self.config.include_variants,
            config=RecognitionConfig(**self.config.recognition),runtime_policy=self.retention)
        self.paper=PaperSimulator(self.policy)

    def _fault(self, point, row):
        if self._fault_hook: self._fault_hook(point,row)

    def append(self, event, source="input", lane="observation"):
        event.validate()
        payload=event.to_dict()
        if lane != "observation":
            # Backfill is retained for an independent replay. Never touches live state.
            return self.store.append("backfill",source,payload,event.event_id)
        seq,created=self.store.append("market",source,payload,event.event_id)
        if not created: self.metrics["duplicates"]+=1
        return seq,created

    def ingest(self,event,source="input",lane="observation"):
        seq,created=self.append(event,source,lane)
        if created: self.drain()
        return seq,created

    def submit_control(self,action,request_id=None):
        if action not in {"pause","resume","stop","start"}:
            raise ValueError("unknown runtime control")
        at=(self.store.meta("last_event") or {}).get("arrival_time") or datetime.now(timezone.utc).isoformat()
        seq,created=self.store.append("control","local",{"action":action,"at":at},request_id)
        if self._thread is None and created: self.drain()
        return {"event_seq":seq,"accepted":True,"duplicate":not created}

    def _refresh_state(self):
        if self.requested_state in {"PAUSED","STOPPED"}:
            self.state=self.requested_state
        elif self.connection_state in {"DISCONNECTED","SOURCE_ERROR","AUTH_REQUIRED","CAPACITY_BLOCKED","SOURCE_ENDED"}:
            self.state=self.connection_state
        else:
            self.state="RUNNING"

    def _advance(self,row):
        """One deterministic transition, used for both processing and recovery."""
        seq,kind,payload=row["seq"],row["kind"],row["payload"]
        if kind not in {"market","control","clock","source_status","backfill","label"}:
            raise ValueError("unsupported journal event kind")
        changes={}; outputs=[]
        if kind=="market":
            event=MarketEvent.from_dict(payload)
            if event.kind=="status" and self.paper._execution_session(event.symbol,event.venue):
                connection=event.payload.get("connection_state")
                if connection in {"connected","disconnected"}:
                    self.connection_state=connection.upper()
                    self._refresh_state()
            started=time.perf_counter()
            outputs=[d.to_dict() for d in self.engine.feed(event)]
            for d in outputs: d["source_kind"]=self.config.effective_source_kind
            self.metrics["feed_seconds"].append(time.perf_counter()-started)
            self.metrics["feed_seconds"]=self.metrics["feed_seconds"][-10000:]
            blocked=self.state in {"PAUSED","STOPPED","DISCONNECTED","SOURCE_ERROR","AUTH_REQUIRED","CAPACITY_BLOCKED","SOURCE_ENDED","PROCESSING_BLOCKED"}
            entry_paused=self.paper.entry_paused
            if blocked: self.paper.entry_paused=True
            try:
                # Complete batches preserve final invalidation order and consume blocked signals.
                changes=self.paper.on_event(event,outputs,seq)
            finally:
                self.paper.entry_paused=entry_paused
            changes["account"]=self.paper.account()
            if blocked:
                changes["decisions"].extend({"detection_id":d["detection_id"],"symbol":d["symbol"],"reason":"runtime_"+self.state.lower(),"event_seq":seq} for d in outputs if d["state"]=="triggered")
            self.last_event=payload
            self.metrics["events_processed"]+=1
        elif kind=="control":
            action=payload["action"]
            self.requested_state={"pause":"PAUSED","resume":"RUNNING","start":"RUNNING","stop":"STOPPED"}[action]
            self._refresh_state()
            changes=self.paper.control("resume" if action=="start" else action,seq,payload["at"])
        elif kind=="clock":
            # Administrative wall clocks cannot advance recognition's input watermark.
            changes=self.paper.on_event(MarketEvent.from_dict(payload),[],seq)
        elif kind=="source_status":
            connection=payload.get("state")
            if connection in {"DISCONNECTED","SOURCE_ERROR","AUTH_REQUIRED","CAPACITY_BLOCKED","SOURCE_ENDED","CONNECTED"}:
                self.connection_state=connection
                self._refresh_state()
            if connection in {"DISCONNECTED","SOURCE_ERROR","AUTH_REQUIRED","CAPACITY_BLOCKED","CONNECTED"}:
                changes=self.paper.source_status(connection,seq,payload["at"],payload.get("symbols"))
            if connection=="SOURCE_ENDED": self.metrics["source_ended"]=True
        records=[]
        for i,d in enumerate(outputs): records.append(("detections",i,d,d["detection_id"]))
        for topic in ("orders","fills","account_events","decisions"):
            for i,value in enumerate(changes.get(topic,[])):
                identity=value.get("order_id") if topic=="orders" else value.get("fill_id") if topic=="fills" else value.get("account_event_id")
                records.append((topic,i,value,identity))
        if changes: records.append(("account",0,self.paper.account(),"account"))
        if kind=="market" and payload["kind"]=="bar": records.append(("bars",0,payload,payload["event_id"]))
        if kind in {"control","source_status","backfill"}: records.append(("runtime",0,{"kind":kind,**payload},None))
        if kind=="label": records.append(("labels",0,payload,payload["case_id"]))
        return records,changes

    def _process(self,row):
        seq,kind,payload=row["seq"],row["kind"],row["payload"]
        records,changes=self._advance(row)
        record_cursor=self._record_cursor
        with self.store.transaction() as c:
            for topic,index,value,identity in records:
                record_cursor=self.store.record(c,topic,seq,index,value,identity)
            if changes:
                self.store.set_meta("account",self.paper.account(),c)
                self.store.set_meta("positions",changes["positions"],c)
                self.store.set_meta("execution_sessions",self.paper.sessions,c)
                self.store.set_meta("execution_connectivity",{"halted":sorted(self.paper.halted),"disconnected":sorted(self.paper.disconnected),"entry_paused":self.paper.entry_paused},c)
            if kind=="market" and self.paper._execution_session(payload["symbol"],payload["venue"]):
                if payload["kind"]=="quote": self.store.set_meta("quote:"+payload["symbol"],payload,c)
                if payload["kind"]=="bar": self.store.set_meta("bar:"+payload["symbol"],payload,c)
            if kind=="source_status" and payload.get("state") in {"SOURCE_ERROR","AUTH_REQUIRED","CAPACITY_BLOCKED","CONNECTED"}:
                self.store.set_meta("source_error",None if payload["state"]=="CONNECTED" else payload,c)
            if kind=="label":
                c.execute("INSERT INTO labels VALUES(?,?,?) ON CONFLICT(case_id) DO UPDATE SET payload=excluded.payload,updated_at=excluded.updated_at",(payload["case_id"],canonical(payload),self.store.now()))
            for key,value in (("processed_cursor",seq),("state",self.state),("requested_state",self.requested_state),("connection_state",self.connection_state),("last_event",self.last_event)):
                self.store.set_meta(key,value,c)
            self.store.set_meta("record_cursor",record_cursor,c)
            self.store.set_meta("metrics",{k:v for k,v in self.metrics.items() if k!="feed_seconds"},c)
            c.execute("UPDATE journal SET processed=1 WHERE seq=?",(seq,))
            self._fault("before_commit",row)
        self.cursor=seq
        self._record_cursor=record_cursor
        self._fault("after_commit",row)

    def drain(self):
        with self._mutex:
            while True:
                rows=self.store.journal(after=self.cursor,limit=100)
                if not rows: break
                for row in rows:
                    try:
                        self._process(row)
                    except BaseException:
                        # feed mutated memory before commit. Reconstruct before any retry.
                        self.recover()
                        raise
            now=time.monotonic()
            if self.cursor and (now-self.last_checkpoint>=self.config.checkpoint_seconds or self.cursor-self.store.meta("checkpoint_cursor",0)>=self.config.checkpoint_events):
                self.checkpoint()
            if now-self.last_stats>=1:
                self.store.set_meta("coverage",self.engine.coverage())
                self.store.set_meta("metrics",{k:v for k,v in self.metrics.items() if k!="feed_seconds"})
                self.last_stats=now

    def checkpoint(self):
        with self._mutex:
            self._commit_checkpoint()
            # Both the encoded tree and Store's JSON text have left their scopes.
            # Allocator maintenance cannot change the already committed state.
            self._memory_maintenance.maintain()

    def _commit_checkpoint(self):
        payload={"identity":self._identity,"paper":self.paper.snapshot(),
                 "cursor":self.cursor,"state":self.state,"requested_state":self.requested_state,
                 "connection_state":self.connection_state,"last_event":self.last_event,
                 "metrics":{k:v for k,v in self.metrics.items() if k!="feed_seconds"},"journal_sha256":self.journal_hash(self.cursor),
                 "derived_sha256":self.store.derived_hash(self.cursor),"record_cursor":self._record_cursor}
        # Write directly from live state. A complete encoded object tree would
        # create many temporary small allocations at every minute checkpoint.
        with io.BytesIO() as encoded:
            writer = _Utf8TextWriter(encoded)
            writer.write('{')
            for index, key in enumerate(sorted((*payload, "engine"))):
                if index:
                    writer.write(',')
                writer.write(canonical(key))
                writer.write(':')
                if key == "engine":
                    self.engine.write_snapshot_json(writer)
                else:
                    writer.write(canonical(payload[key]))
            writer.write('}')
            text = encoded.getvalue().decode("utf-8")
        self.store._save_checkpoint_text(self.cursor,text)
        self.store.set_meta("checkpoint_cursor",self.cursor)
        self.store.set_meta("coverage",self.engine.coverage())
        self.store.set_meta("metrics",{k:v for k,v in self.metrics.items() if k!="feed_seconds"})
        self.last_checkpoint=time.monotonic()

    def journal_hash(self,cursor=None):
        h=hashlib.sha256()
        after=0
        while True:
            rows=self.store.journal(after,1000)
            if not rows: break
            for row in rows:
                if cursor is not None and row["seq"]>cursor: return h.hexdigest()
                h.update(canonical({"seq":row["seq"],"kind":row["kind"],"source":row["source"],"payload":row["payload"]}).encode())
                after=row["seq"]
        return h.hexdigest()

    def recover(self):
        with self._mutex:
            self._recover_committed_state()
            # Validation and materialized views completed before this opportunity.
            self._memory_maintenance.maintain()

    def _recover_committed_state(self):
        committed=self.store.meta("processed_cursor",0)
        with self.store.lock:
            last=self.store.conn.execute("SELECT COALESCE(MAX(seq),0) FROM journal WHERE processed=1").fetchone()[0]
            gap=self.store.conn.execute("SELECT 1 FROM journal WHERE seq<=? AND processed=0 LIMIT 1",(committed,)).fetchone()
            future=self.store.conn.execute("SELECT 1 FROM records WHERE event_seq>? LIMIT 1",(committed,)).fetchone()
        if type(committed) is not int or committed<0 or last!=committed or gap or future:
            raise RecoveryBlocked("committed cursor/journal/derived prefix are inconsistent")
        checkpoint=self.store.checkpoint()
        previous_metrics=self.store.meta("metrics",{})
        recovery_count=max(self.metrics["recoveries"],previous_metrics.get("recoveries",0))
        self.metrics.update(events_processed=0,source_ended=False)
        self._new_processors()
        self.cursor=0
        self._record_cursor=0
        self.state="READY"
        self.connection_state="UNKNOWN"
        self.requested_state="RUNNING"
        self.last_event=None
        if checkpoint:
            snap=checkpoint["payload"]
            if snap["identity"]!=self._identity or snap["journal_sha256"]!=self.journal_hash(checkpoint["cursor"]) or snap["derived_sha256"]!=self.store.derived_hash(checkpoint["cursor"]):
                raise RecoveryBlocked("checkpoint/log identity or content mismatch")
            if checkpoint["cursor"]>committed:
                raise RecoveryBlocked("checkpoint exceeds committed cursor")
            if snap["cursor"]!=checkpoint["cursor"]:
                raise RecoveryBlocked("checkpoint sequence mismatch")
            self.engine=RecognitionEngine.restore(snap["engine"],runtime_policy=self.retention)
            self.paper=PaperSimulator.from_snapshot(snap["paper"],self.policy)
            self.cursor=snap["cursor"]
            self._record_cursor=snap["record_cursor"]
            self.state=snap["state"]
            self.requested_state=snap["requested_state"]
            self.connection_state=snap["connection_state"]
            self.last_event=snap["last_event"]
            self.metrics.update(snap.get("metrics",{}))
            recovery_count=max(recovery_count,self.metrics["recoveries"])
        # Replay only committed source records and compare their derived event content.
        while self.cursor<committed:
            for row in self.store.journal(self.cursor,100):
                if row["seq"]>committed: break
                self._reconstruct(row)
        self.metrics["recoveries"]=recovery_count+1
        self.error=None
        # Materialized UI views are reconstructed from verified committed state.
        with self.store.transaction() as c:
            for key,value in (("account",self.paper.account()),("positions",[self.paper.positions[k] for k in sorted(self.paper.positions)]),
                              ("last_event",self.last_event),("state",self.state),("requested_state",self.requested_state),
                              ("connection_state",self.connection_state),("record_cursor",self._record_cursor),
                              ("coverage",self.engine.coverage()),("execution_sessions",self.paper.sessions),("error",None)):
                self.store.set_meta(key,value,c)
            self.store.set_meta("execution_connectivity",{"halted":sorted(self.paper.halted),"disconnected":sorted(self.paper.disconnected),"entry_paused":self.paper.entry_paused},c)

    def _reconstruct(self,row):
        records,_=self._advance(row)
        with self.store.lock:
            saved=self.store.conn.execute("SELECT id,topic,output_index,payload,identity FROM records WHERE event_seq=? ORDER BY id",(row["seq"],)).fetchall()
        expected=[(self._record_cursor+i+1,topic,index,canonical(value),identity) for i,(topic,index,value,identity) in enumerate(records)]
        if [tuple(r) for r in saved]!=expected:
            raise RecoveryBlocked("committed detection/order/fill/ledger replay differs; journal retained")
        self.cursor=row["seq"]
        self._record_cursor+=len(records)

    def start(self,provider):
        if self._thread and self._thread.is_alive(): raise RuntimeError("runtime already running")
        if self.store.meta("journal_replay"):
            raise RecoveryBlocked("journal audit runs remain frozen; use a separate directory for a new source")
        previous_source=self.store.meta("source",{})
        self._source_description=provider.describe()
        if previous_source.get("input_sha256") and previous_source["input_sha256"]!=self._source_description.get("input_sha256"):
            raise RecoveryBlocked("replay input changed; retain this run and use a new directory")
        self.store.set_meta("source",self._source_description)
        if hasattr(provider,"set_status_callback"):
            # Initialization can fail before any valid market session exists.
            # Persist transport health without advancing the recognition clock.
            provider.set_status_callback(self.source_status)
        if self.cursor==0: self.state="RUNNING"
        self._stop.clear(); self._source_stop.clear()
        def collect():
            try:
                for event in provider.events(self._source_stop):
                    if self._source_stop.is_set(): break
                    if event.kind=="session" and self.config.mode=="live":
                        old=self.paper.sessions.get(event.symbol)
                        if old and old["open_time"]==event.payload["open_time"]:
                            self.source_status("SESSION_REUSED",symbol=event.symbol,open_time=old["open_time"],reason="restore_keeps_original_session_context")
                            continue
                    # The source is durably appended before wakeup/backpressure.
                    seq,created=self.append(event,self._source_description.get("name",self._source_description.get("id",self.config.mode)))
                    if not created: continue
                    try: self._queue.put(seq,timeout=5)
                    except queue.Full:
                        self.source_status("CAPACITY_BLOCKED",code="input_queue_full")
                        self._source_stop.set(); break
            except Exception as exc:
                # Exceptions from external SDKs can contain headers. Persist only type.
                from connectors.alpaca import AlpacaDataError
                code=type(exc).__name__
                if isinstance(exc,AlpacaDataError):
                    prefix=str(exc).split(";",1)[0].split(":",1)[0]
                    if prefix in {"credentials_not_configured","authentication_failed","feed_not_entitled","rate_limited","http_error","network_or_invalid_response","invalid_calendar_response","invalid_history_response","history_pagination_loop","history_pagination_limit","optional_dependency_missing","authentication_or_subscription_timeout","invalid_stream_frame","stream_auth_or_subscription_error","stream_error","incomplete_subscription"}:
                        code=prefix
                state="AUTH_REQUIRED" if code in {"credentials_not_configured","authentication_failed"} else "SOURCE_ERROR"
                try: self.source_status(state,code=code)
                except Exception:
                    self.error="source_status_persistence_failed"; self.connection_state=state; self._refresh_state()
            else:
                if not self._source_stop.is_set():
                    self.source_status("SOURCE_ENDED",code="input_exhausted")
            finally:
                self.metrics["source_ended"]=True
        def process():
            last_clock=time.monotonic()
            while not self._stop.is_set():
                try:
                    self._queue.get(timeout=.2)
                    self._queue.task_done()
                except queue.Empty: pass
                try:
                    if self.config.mode=="live" and time.monotonic()-last_clock>=1:
                        self._append_clock()
                        last_clock=time.monotonic()
                    self.drain()
                    if self.state=="STOPPED": self._source_stop.set()
                except Exception as exc:
                    self.error=type(exc).__name__
                    self.metrics["errors"]+=1
                    self.state="CAPACITY_BLOCKED" if isinstance(exc,CapacityExceeded) else "PROCESSING_BLOCKED"
                    try:
                        self.store.set_meta("state",self.state)
                        self.store.set_meta("error",self.error)
                    except Exception: pass  # disk failure remains visible in in-memory health
                    self._source_stop.set(); break
            try: self.checkpoint()
            except Exception: pass
        self._thread=threading.Thread(target=process,name="research-single-writer",daemon=True)
        self._producer=threading.Thread(target=collect,name="market-source",daemon=True)
        self._thread.start(); self._producer.start()

    def source_status(self,state,**details):
        at=datetime.now(timezone.utc).isoformat() if self.config.mode=="live" else (self.store.meta("last_event") or {}).get("arrival_time") or datetime.now(timezone.utc).isoformat()
        return self.store.append("source_status","provider",{"state":state,"at":at,**details})

    def _append_clock(self):
        now=datetime.now(timezone.utc)
        for symbol in sorted(self.paper.sessions):
            venue=next((key[1] for key in self.engine.universe if key[0]==symbol),"IEX")
            event=MarketEvent.from_dict({"event_id":"clock:"+symbol+":"+now.isoformat(),"symbol":symbol,"venue":venue,
              "kind":"status","event_time":now.isoformat(),"arrival_time":now.isoformat(),
              "payload":{"gap":False,"stream":"quote","clock":True}})
            self.store.append("clock","clock",event.to_dict(),event.event_id)

    def status(self):
        with self.store.lock: return self._status_locked()

    def _status_locked(self):
        import shutil
        metrics=self.store.meta("metrics",{})
        samples=sorted(self.metrics["feed_seconds"])
        latency={f"p{p}_feed_ms":round(samples[min(len(samples)-1,int(len(samples)*p/100))]*1000,3) if samples else None for p in (50,95,99)}
        return {"schema_version":"1.0.0","run_id":self.run_id,"mode":self.config.mode,
          "state":self.state if self.error else self.store.meta("state",self.state),"connection_state":self.store.meta("connection_state",self.connection_state),"source":self.store.meta("source",self._source_description),
          "source_kind":self.config.effective_source_kind,"watchlist":list(self.config.symbols),"processed_cursor":self.store.meta("processed_cursor",0),"outbox_cursor":self.store.latest_record_id(),
          "health":{**metrics,**latency,"queue_depth":self._queue.qsize(),"journal_backlog":self.store.latest_seq()-self.store.meta("processed_cursor",0),
                    "disk_free_bytes":shutil.disk_usage(self.store.directory).free,
                    "memory_maintenance":self._memory_maintenance.health(),
                    "process_provenance":self._process_provenance,
                    "error":self.error or self.store.meta("error"),"source_error":self.store.meta("source_error"),"last_received_at":(self.store.meta("last_event") or {}).get("arrival_time")},
          "identity":self._identity,"journal_replay":bool(self.store.meta("journal_replay")),"market_validation":"not_run","execution":"paper_only"}

    def submit_label(self,label,request_id=None):
        from evaluation.metrics import validate_labels
        for field in ("annotator","rule_version","data_source"):
            if not isinstance(label.get(field),str) or not label[field]:
                raise ValueError("runtime annotations require "+field)
        from .contracts import validate_contract
        validate_contract("annotation",label)
        if label.get("complete_coverage") and (label.get("disputed",False) or not label.get("data_eligible",True)):
            raise ValueError("complete coverage cannot include disputed or data-ineligible annotations")
        labels={r["case_id"]:r for r in self.store.labels()}
        labels[label["case_id"]]=label
        validate_labels(list(labels.values()))
        if self.config.effective_source_kind!="unknown" and label["source_kind"]!=self.config.effective_source_kind:
            raise ValueError("annotation source_kind differs from the observed run")
        if (label["setup_id"],label.get("variant_id","base")) not in {(spec["setup_id"],spec["variant_id"]) for spec in self.engine.graph.specs}:
            raise ValueError("unknown setup or variant")
        seq,created=self.store.append("label","local",label,request_id)
        if self._thread is None and created: self.drain()
        return {"accepted":True,"event_seq":seq,"case_id":label["case_id"]}

    def close(self):
        if hasattr(self,"_stop"): self._stop.set(); self._source_stop.set()
        if getattr(self,"_producer",None): self._producer.join(timeout=20)
        if getattr(self,"_thread",None): self._thread.join(timeout=20)
        if any(t is not None and t.is_alive() for t in (getattr(self,"_producer",None),getattr(self,"_thread",None))):
            raise RuntimeError("workers did not stop; database and exclusive lease retained")
        if hasattr(self,"store"):
            self.store.close()
        if hasattr(self,"_lease") and not self._lease.closed:
            fcntl.flock(self._lease,fcntl.LOCK_UN); self._lease.close()
