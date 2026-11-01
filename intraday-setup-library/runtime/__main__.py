"""python3 -m runtime: start a local, simulated-only research workbench."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import signal
import threading
import time

from .config import ROOT, RuntimeConfig


def provider_for(config,input_path=None):
    from connectors import DemoProvider, JsonlProvider, AlpacaIEXProvider
    if config.mode=="demo": return DemoProvider(symbols=config.symbols,minutes=config.demo_minutes,pace_seconds=config.pace_seconds,sessions=config.demo_sessions)
    if config.mode=="replay":
        if not input_path: raise ValueError("replay requires --input")
        return JsonlProvider(Path(input_path),pace_seconds=config.pace_seconds)
    return AlpacaIEXProvider(symbols=config.symbols,history_sessions=config.alpaca_history_sessions)


def main():
    ap=argparse.ArgumentParser(description=__doc__)
    commands=ap.add_subparsers(dest="command",required=True)
    for name in ("serve","replay","resume"):
        p=commands.add_parser(name)
        p.add_argument("--config",type=Path)
        p.add_argument("--data-dir",type=Path)
        p.add_argument("--mode",choices=["demo","live","replay"])
        p.add_argument("--source-kind",choices=["synthetic","real_market","unknown"],help="declare JSONL replay provenance")
        p.add_argument("--input",type=Path)
        p.add_argument("--port",type=int)
        p.add_argument("--pace",type=float)
        p.add_argument("--minutes",type=int)
        p.add_argument("--sessions",type=int,help="number of explicitly synthetic demo trading sessions")
        p.add_argument("--history-sessions",type=int,help="IEX completed historical sessions to request, 0..30")
    doctor=commands.add_parser("doctor")
    doctor.add_argument("--config",type=Path)
    export=commands.add_parser("export")
    export.add_argument("--data-dir",type=Path,required=True)
    export.add_argument("--output",type=Path)
    backfill=commands.add_parser("analyze-backfill",help="analyze committed backfill in a separate read-only recognition run; never trade it")
    backfill.add_argument("--data-dir",type=Path,required=True)
    backfill.add_argument("--output",type=Path,required=True)
    audit=commands.add_parser("replay-journal",help="replay exported controls/clocks and compare the committed account")
    audit.add_argument("--input",type=Path,required=True)
    audit.add_argument("--run-report",type=Path,required=True)
    audit.add_argument("--data-dir",type=Path,required=True)
    args=ap.parse_args()
    if args.command=="analyze-backfill":
        from .backfill import analyze_backfill
        result=analyze_backfill(args.data_dir,args.output)
        print(json.dumps(result,ensure_ascii=False))
        if result["status"]=="blocked":
            raise SystemExit(2)
        return
    if args.command=="replay-journal":
        from .replay import replay_journal
        original=json.loads(args.run_report.read_text(encoding="utf-8"))
        result=replay_journal(args.input,args.data_dir,original["configuration"],
            expected_identity=original["status"]["identity"],committed_cursor=original["status"]["processed_cursor"],expected_journal_sha256=original["journal_sha256"])
        if result["account"]!=original["account"] or result["derived_sha256"]!=original["derived_sha256"]:
            raise ValueError("replayed account/detections/orders/fills/ledger differ from the export")
        print(json.dumps({**result,"account_matches_export":True},ensure_ascii=False))
        return
    if args.command=="doctor":
        import os
        from importlib.metadata import version,PackageNotFoundError
        from .memory import process_provenance
        import sys
        try: websocket_version=version("websockets")
        except PackageNotFoundError: websocket_version=None
        config=RuntimeConfig.load(args.config)
        key=bool(os.environ.get("ALPACA_API_KEY_ID") or os.environ.get("APCA_API_KEY_ID"))
        secret=bool(os.environ.get("ALPACA_API_SECRET_KEY") or os.environ.get("APCA_API_SECRET_KEY"))
        print(json.dumps({"python_core":"available","websockets":bool(importlib.util.find_spec("websockets")),
            "python_version":sys.version.split()[0],"live_python_compatible":sys.version_info>=(3,11),"websockets_version":websocket_version,"expected_websockets_version":"17.1",
            "alpaca_configured":key and secret,"watchlist":list(config.symbols),"feed":"iex",
            "mode":config.mode,"demo_sessions":config.demo_sessions,"alpaca_history_sessions":config.alpaca_history_sessions,
            "process_provenance":process_provenance(),
            "execution":"paper_only","market_validation":"not_run"},ensure_ascii=False))
        return
    from .runner import Runner
    if args.command=="export":
        from .store import Store
        if not (args.data_dir/"runtime.sqlite3").is_file(): raise ValueError("run database not found")
        store=Store(args.data_dir)
        config=RuntimeConfig.load(**store.meta("configuration",{})); store.close()
        runner=Runner(args.data_dir,config)
        try:
            from .export import export_run
            print(json.dumps(export_run(runner,args.output),ensure_ascii=False))
        finally: runner.close()
        return
    directory=args.data_dir
    if args.command=="resume":
        if not directory: raise ValueError("resume requires --data-dir")
        from .store import Store
        if not (directory/"runtime.sqlite3").is_file(): raise ValueError("run database not found")
        store=Store(directory)
        saved=store.meta("configuration"); saved_source=store.meta("source",{}); store.close()
        if saved is None: raise ValueError("no run configuration to resume")
        if (args.config or args.mode and args.mode!=saved["mode"]
            or args.minutes is not None and args.minutes!=saved["demo_minutes"]
            or args.sessions is not None and args.sessions!=saved.get("demo_sessions",1)
            or args.history_sessions is not None and args.history_sessions!=saved.get("alpaca_history_sessions",15)
            or args.source_kind and args.source_kind!=saved.get("source_kind","unknown")):
            raise ValueError("resume keeps the recorded observation/execution configuration; use a new run to change it")
        config=RuntimeConfig.load(**{**saved,**({"port":args.port} if args.port is not None else {}),**({"pace_seconds":args.pace} if args.pace is not None else {})})
        if not args.input and config.mode=="replay": args.input=saved_source.get("input")
    else:
        config=RuntimeConfig.load(args.config,mode=args.mode or ("replay" if args.command=="replay" else None),source_kind=args.source_kind,port=args.port,pace_seconds=args.pace,demo_minutes=args.minutes,demo_sessions=args.sessions,alpaca_history_sessions=args.history_sessions)
    directory=directory or ROOT.parent/"runtime-runs"/datetime.now(timezone.utc).strftime("run-%Y%m%dT%H%M%S%fZ")
    runner=Runner(directory,config)
    if args.command=="replay":
        try:
            provider=provider_for(config,args.input)
            runner.store.set_meta("source",provider.describe())
            for event in provider.events(): runner.ingest(event,source=provider.describe().get("name","replay"))
            runner.source_status("SOURCE_ENDED",code="input_exhausted"); runner.drain()
            runner.checkpoint()
            from .export import export_run
            print(json.dumps({"status":runner.status(),"export":export_run(runner)},ensure_ascii=False))
        finally: runner.close()
        return
    from .server import WorkbenchServer
    server=WorkbenchServer(runner).start()
    shutdown=threading.Event()
    def stop(*_): shutdown.set()
    for s in (signal.SIGINT,signal.SIGTERM): signal.signal(s,stop)
    try:
        runner.start(provider_for(config,args.input))
        print(json.dumps({"url":f"http://127.0.0.1:{server.server_port}","data_dir":str(runner.store.directory),"mode":config.mode,"execution":"paper_only"},ensure_ascii=False),flush=True)
        while not shutdown.wait(.5): pass
    finally:
        server.close(); runner.close()


if __name__=="__main__": main()
