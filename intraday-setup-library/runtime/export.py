"""Portable run exports contain observations and simulated results, never credentials."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import zipfile

from .config import canonical, validate_data_dir
from recognition.provenance import file_sha256


def export_run(runner, output=None):
    target=Path(output) if output else runner.store.directory/"exports"/datetime.now(timezone.utc).strftime("run-%Y%m%dT%H%M%S%fZ")
    target=validate_data_dir(target)
    target.mkdir(parents=True,exist_ok=False)
    files=[]
    c=sqlite3.connect(runner.store.path.as_uri()+"?mode=ro",uri=True)
    c.row_factory=sqlite3.Row
    # Pin a WAL read snapshot at a committed boundary, then release the writer.
    # Large exports must not stop collection or allocate whole files in memory.
    try:
        with runner._mutex, runner.store.lock:
            c.execute("BEGIN")
            metadata={row["key"]:json.loads(row["value"]) for row in c.execute("SELECT key,value FROM meta")}
            status=runner.status()
            specs=json.loads(canonical(runner.engine.graph.specs))
            recognition_config_hash=runner.engine.config.fingerprint()
    except BaseException:
        c.close()
        raise
    try:
        for topic in ("detections","orders","fills","account_events","decisions","bars"):
            path=target/(topic+".jsonl")
            with path.open("w",encoding="utf-8") as f:
                for row in c.execute("SELECT payload FROM records WHERE topic=? ORDER BY id",(topic,)):
                    f.write(row[0]+"\n")
            files.append(path)
        path=target/"events.jsonl"
        with path.open("w",encoding="utf-8") as f:
            for row in c.execute("SELECT payload FROM journal WHERE kind='market' AND processed=1 ORDER BY seq"):
                f.write(row[0]+"\n")
        files.append(path)
        from .contracts import envelope
        journal=target/"journal.jsonl"
        with journal.open("w",encoding="utf-8") as f:
            after=0
            while True:
                rows=c.execute("SELECT * FROM journal WHERE seq>? ORDER BY seq LIMIT 1000",(after,)).fetchall()
                if not rows: break
                for row in rows:
                    row=dict(row,payload=json.loads(row["payload"]))
                    f.write(canonical(envelope(runner.run_id,row))+"\n")
                    after=row["seq"]
        files.append(journal)
        labels=target/"labels.jsonl"
        with labels.open("w",encoding="utf-8") as f:
            for row in c.execute("SELECT payload FROM labels ORDER BY case_id"):
                f.write(row[0]+"\n")
        files.append(labels)
        rules=target/"rules.json"
        rules.write_text(json.dumps({"schema_version":"1.0.0","rules":specs},ensure_ascii=False,indent=2,allow_nan=False)+"\n",encoding="utf-8")
        files.append(rules)
        input_provenance={"data_sha256":file_sha256(target/"events.jsonl"),
            "rule_sha256":file_sha256(rules),
            "detections_sha256":file_sha256(target/"detections.jsonl"),
            "algorithm_sha256":status["identity"]["algorithm_sha256"],
            "configuration_sha256":recognition_config_hash,
            "data_file":"events.jsonl","rule_file":"rules.json","detections_file":"detections.jsonl",
            "scope":"committed_market_events","processed_cursor":metadata["processed_cursor"]}
        raw_digest=hashlib.sha256()
        for row in c.execute("SELECT seq,kind,source,payload FROM journal ORDER BY seq"):
            raw_digest.update(canonical(dict(row,payload=json.loads(row["payload"]))).encode())
        derived_digest=hashlib.sha256()
        for row in c.execute("SELECT id,topic,event_seq,output_index,identity,payload FROM records ORDER BY id"):
            derived_digest.update(canonical(list(row)).encode()+b'\n')
        from .research import committed_research_report
        report={"schema_version":"1.0.0","status":status,"account":metadata.get("account",{}),
            "coverage":metadata.get("coverage",{}),"journal_sha256":raw_digest.hexdigest(),
            "derived_sha256":derived_digest.hexdigest(),
            "configuration":metadata["configuration"],
            "input_provenance":input_provenance,
            "research_reports":committed_research_report(runner,connection=c,status=status),
            "scope":"simulated execution and engineering evidence; market accuracy requires independent labels"}
        summary=target/"run.json"
        summary.write_text(json.dumps(report,ensure_ascii=False,indent=2,allow_nan=False)+"\n",encoding="utf-8")
        files.append(summary)
    finally:
        c.close()
    manifest={"schema_version":"1.0.0","files":[{"path":p.name,"bytes":p.stat().st_size,"sha256":file_sha256(p)} for p in files]}
    (target/"manifest.json").write_text(json.dumps(manifest,indent=2)+"\n")
    archive=target.with_suffix(".zip")
    with zipfile.ZipFile(archive,"x",zipfile.ZIP_DEFLATED) as z:
        for path in sorted(target.iterdir()): z.write(path,path.name)
    with zipfile.ZipFile(archive) as z:
        if z.testzip() is not None: raise ValueError("export ZIP CRC failure")
    return {"directory":str(target),"archive":str(archive),"files":len(files)+1,
        "sha256":file_sha256(archive)}
