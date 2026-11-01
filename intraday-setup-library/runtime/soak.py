"""Actual wall-time stability acceptance; shorter runs are explicitly smoke tests.

python3 -m runtime.soak --data-dir /outside/library/soak --duration-seconds 36000 --pace .25
"""
from __future__ import annotations

from collections import deque
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import resource
import shutil
import signal
import statistics
import subprocess
import sys
from threading import Event
import time

from connectors import DEFAULT_SYMBOLS, DemoProvider
from recognition.models import MarketEvent, timestamp
from paper.simulator import cents
from .config import ROOT, RuntimeConfig, canonical, validate_data_dir
from .runner import Runner, engine_snapshot_sha256, combined_snapshot_sha256

REQUIRED_SECONDS = 36000
FAULT_KINDS = ("duplicate", "backlog_burst", "disconnect_reconnect", "before_commit", "after_commit", "checkpoint_restart")


class SoakFailure(RuntimeError):
    pass


class InjectedFault(RuntimeError):
    pass


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def core_manifest():
    """Hash executable code and source rules; reports/UI/docs may keep evolving."""
    paths = []
    for package in ("recognition", "evaluation", "runtime", "paper", "connectors"):
        paths.extend((ROOT / package).glob("*.py"))
    paths.extend((ROOT / "setups").glob("SET-*.md"))
    paths.extend((ROOT / "schema").glob("*.schema.json"))
    paths.append(ROOT / "connectors" / "requirements.txt")
    paths.append(ROOT / "requirements-runtime.txt")
    files = {}
    digest = hashlib.sha256()
    for path in sorted(paths):
        relative = path.relative_to(ROOT).as_posix()
        value = hashlib.sha256(path.read_bytes()).hexdigest()
        files[relative] = value
        digest.update(canonical([relative, value]).encode() + b"\n")
    return {"sha256": digest.hexdigest(), "files": files}


def check_core(expected):
    actual = core_manifest()
    if actual != expected:
        names = sorted(set(expected["files"]) | set(actual["files"]))
        changed = [p for p in names if expected["files"].get(p) != actual["files"].get(p)]
        raise SoakFailure("core_version_changed:" + ",".join(changed))
    return actual


def rss_bytes():
    """Current RSS, separate from the process high-water RSS."""
    proc = Path("/proc/self/statm")
    if proc.is_file():
        return int(proc.read_text().split()[1]) * os.sysconf("SC_PAGE_SIZE")
    result = subprocess.run(["ps", "-o", "rss=", "-p", str(os.getpid())], capture_output=True, text=True, timeout=5, check=True)
    return int(result.stdout.strip()) * 1024


def peak_rss_bytes():
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(value if sys.platform == "darwin" else value * 1024)


def percentiles(samples):
    rows = sorted(samples)
    return {f"p{p}_ms": round(rows[min(len(rows) - 1, math.ceil(len(rows) * p / 100) - 1)] * 1000, 3) if rows else None
            for p in (50, 95, 99)}


def write_json_atomic(path, row):
    temporary = path.with_name(path.name + ".partial")
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False, indent=2, allow_nan=False) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(path)


def invariants(runner):
    """Durable uniqueness and cent reconciliation, plus active-order constraints."""
    with runner.store.lock:
        conn = runner.store.conn
        for topic in ("fills", "account_events"):
            count, distinct, nulls = conn.execute("SELECT COUNT(*),COUNT(DISTINCT identity),SUM(identity IS NULL) FROM records WHERE topic=?", (topic,)).fetchone()
            if count != distinct or nulls:
                raise SoakFailure("duplicate_or_missing_" + topic + "_identity")
        duplicated_order = conn.execute("SELECT json_extract(payload,'$.order_id') FROM records WHERE topic='fills' GROUP BY json_extract(payload,'$.order_id') HAVING COUNT(*)>1 LIMIT 1").fetchone()
        if duplicated_order:
            raise SoakFailure("one_order_filled_twice")
        ledger = conn.execute("SELECT COALESCE(SUM(json_extract(payload,'$.balance_delta_cents')),0) FROM records WHERE topic='account_events'").fetchone()[0]
        totals = {topic: conn.execute("SELECT COUNT(*) FROM records WHERE topic=?", (topic,)).fetchone()[0]
                  for topic in ("detections", "orders", "fills", "account_events", "decisions")}
        pending_rows = conn.execute("SELECT payload FROM records WHERE id IN (SELECT MAX(id) FROM records WHERE topic='orders' GROUP BY identity) AND json_extract(payload,'$.status')='pending'").fetchall()
    account = runner.paper.account()
    if cents(runner.policy.initial_cash) + ledger != account["balance_cents"]:
        raise SoakFailure("ledger_balance_cents_mismatch")
    pending = [json.loads(r[0]) for r in pending_rows]
    entries = [o for o in pending if o["purpose"] == "entry"]
    exits = [o for o in pending if o["purpose"] == "exit"]
    positions = runner.paper.positions
    if len(positions) + len(entries) > runner.policy.max_positions:
        raise SoakFailure("position_limit_exceeded")
    if len({o["symbol"] for o in entries}) != len(entries) or any(o["symbol"] in positions for o in entries):
        raise SoakFailure("multiple_entries_or_entry_added_to_position")
    if len({o["symbol"] for o in exits}) != len(exits) or any(o["symbol"] not in positions for o in exits):
        raise SoakFailure("multiple_or_orphan_exits")
    reserved = sum(o["budget_cents"] for o in entries)
    locked = sum(p["entry_notional_cents"] for p in positions.values())
    if reserved != account["reserved_cents"] or locked != account["locked_cents"]:
        raise SoakFailure("reserved_or_locked_capital_mismatch")
    if account["available_cents"] != account["balance_cents"] - locked - reserved:
        raise SoakFailure("buying_power_mismatch")
    if runner.store.integrity() != "ok":
        raise SoakFailure("sqlite_integrity_failure")
    policy = runner.engine.runtime_policy
    if len(runner.engine.seen) > policy.max_seen or len(runner.engine.graph.transitions) > policy.max_transitions or len(runner.engine.audit) > policy.max_audit:
        raise SoakFailure("recognition_cache_limit_exceeded")
    terminal = sum(o["status"] != "pending" for o in runner.paper.orders.values())
    if terminal > runner.policy.recent_rows_limit or len(runner.paper.recent_fills) > runner.policy.recent_rows_limit or len(runner.paper.recent_account_events) > runner.policy.recent_rows_limit:
        raise SoakFailure("paper_history_limit_exceeded")
    return {"passed": True, "totals": totals, "ledger_balance_delta_cents": ledger,
            "balance_cents": account["balance_cents"], "positions": len(positions), "pending_entries": len(entries),
            "pending_exits": len(exits), "sqlite_integrity": "ok", "seen_cache": len(runner.engine.seen),
            "transition_cache": len(runner.engine.graph.transitions), "terminal_orders_memory": terminal}


def verify_replay(runner, directory, expected_core):
    """Replay the entire immutable journal into a second database and compare."""
    from .replay import derived_hash, replay_journal
    check_core(expected_core)
    runner.drain()
    journal_path = directory / "soak-journal.jsonl"
    with journal_path.open("w", encoding="utf-8") as handle:
        after = 0
        while True:
            rows = runner.store.journal(after, 1000)
            if not rows:
                break
            for row in rows:
                handle.write(canonical({k: row[k] for k in ("seq", "kind", "source", "event_id", "payload")}) + "\n")
                after = row["seq"]
    original = {"derived_sha256": derived_hash(runner.store), "account": runner.paper.account(),
                "engine_sha256": engine_snapshot_sha256(runner.engine),
                "paper_sha256": hashlib.sha256(canonical(runner.paper.snapshot()).encode()).hexdigest(),
                "processed_cursor": runner.cursor}
    started = time.monotonic()
    replay = replay_journal(journal_path, directory / "replay-verification", runner.config.to_dict(),
                            expected_identity=runner._identity, committed_cursor=runner.cursor,
                            expected_journal_sha256=runner.journal_hash())
    check_core(expected_core)
    mismatches = [key for key, value in original.items() if replay.get(key) != value]
    if mismatches:
        raise SoakFailure("full_journal_replay_mismatch:" + ",".join(mismatches))
    return {"passed": True, "seconds": round(time.monotonic() - started, 6), **original,
            "directory": str((directory / "replay-verification").resolve())}


def _finite_positive(value, name, zero=False):
    if type(value) not in {int, float} or not math.isfinite(value) or value < 0 or (value == 0 and not zero):
        raise ValueError(name + " must be a finite " + ("nonnegative" if zero else "positive") + " number")
    return value


def run_soak(data_dir, duration_seconds=REQUIRED_SECONDS, pace=.25, *, fault_interval_seconds=900,
             status_interval_seconds=60, burst_size=300, stop_event=None, max_rss_mb=1024,
             rss_growth_allowance_mb=128, demo_minutes=390):
    """Run synchronously; every short test remains smoke_passed, never passed."""
    _finite_positive(duration_seconds, "duration_seconds")
    _finite_positive(pace, "pace", zero=True)
    _finite_positive(fault_interval_seconds, "fault_interval_seconds")
    _finite_positive(status_interval_seconds, "status_interval_seconds")
    _finite_positive(max_rss_mb, "max_rss_mb")
    _finite_positive(rss_growth_allowance_mb, "rss_growth_allowance_mb")
    if type(burst_size) is not int or not 1 <= burst_size <= 10000:
        raise ValueError("burst_size must be an integer 1..10000")
    if type(demo_minutes) is not int or not 1 <= demo_minutes <= 390:
        raise ValueError("demo_minutes must be an integer 1..390")
    directory = validate_data_dir(data_dir)
    directory.mkdir(parents=True, exist_ok=True)
    if any((directory / name).exists() for name in ("runtime.sqlite3", "soak-status.json", "soak-report.json", "replay-verification")):
        raise FileExistsError("soak requires a new run directory; previous evidence is retained")
    stop = stop_event or Event()
    manifest = core_manifest()
    started_utc, started = utc_now(), time.monotonic()
    config = RuntimeConfig(mode="demo", symbols=tuple(DEFAULT_SYMBOLS), include_variants=True, pace_seconds=0,
                           demo_minutes=demo_minutes, checkpoint_seconds=60, checkpoint_events=10000)
    runner = None
    processing = deque(maxlen=10000)
    resource_samples = deque(maxlen=720)
    last_status = {}
    counters = {"raw_received": 0, "duplicate_attempts": 0, "faults_completed": 0, "restarts": 0, "sessions_started": 0}
    faults = {kind: 0 for kind in FAULT_KINDS}
    max_backlog, last_event, recent_quote = 0, None, None
    observation_seconds = 0
    recovery_seconds = deque(maxlen=1000)
    fault_path = directory / "soak-faults.jsonl"
    report = {"schema_version": "1.0.0", "status": "running", "required_seconds": REQUIRED_SECONDS,
              "requested_seconds": duration_seconds, "started_at": started_utc, "source_kind": "synthetic",
              "execution": "paper_only", "market_validation": "not_run", "symbols": list(DEFAULT_SYMBOLS),
              "graphs": 84, "core": manifest, "settings": {"pace": pace, "fault_interval_seconds": fault_interval_seconds,
              "status_interval_seconds": status_interval_seconds, "burst_size": burst_size, "max_rss_mb": max_rss_mb,
              "rss_growth_allowance_mb": rss_growth_allowance_mb, "demo_minutes": demo_minutes}}

    def elapsed():
        return time.monotonic() - started

    def fault_record(kind, **detail):
        row = {"kind": kind, "recorded_at": utc_now(), "elapsed_seconds": round(elapsed(), 6), **detail}
        with fault_path.open("a", encoding="utf-8") as handle:
            handle.write(canonical(row) + "\n")
            handle.flush()
            os.fsync(handle.fileno())

    def received(stream):
        nonlocal last_event, recent_quote
        event = next(stream)
        counters["raw_received"] += 1
        if event.kind == "session" and event.symbol == DEFAULT_SYMBOLS[0]:
            counters["sessions_started"] += 1
        last_event = event
        if event.kind == "quote":
            recent_quote = event
        return event

    def ingest(event):
        begin = time.monotonic()
        runner.ingest(event, "soak_demo")
        processing.append(time.monotonic() - begin)

    def snapshot_status(final=False):
        nonlocal max_backlog, last_status
        check_core(manifest)
        checks = invariants(runner)
        rss = rss_bytes()
        backlog = runner.store.latest_seq() - runner.cursor
        max_backlog = max(max_backlog, backlog)
        sample = {"at": utc_now(), "elapsed_seconds": round(elapsed(), 6), "rss_bytes": rss,
                  "peak_rss_bytes": peak_rss_bytes(), "disk_free_bytes": shutil.disk_usage(directory).free,
                  "database_bytes": sum(p.stat().st_size for p in directory.glob("runtime.sqlite3*")), "backlog": backlog,
                  "memory_maintenance": runner.status()["health"]["memory_maintenance"],
                  "processed_cursor": runner.cursor,
                  "sessions_started": counters["sessions_started"],
                  "cache_state": {"seen":len(runner.engine.seen),
                                  "archive":len(runner.engine.graph.archive),
                                  "transitions":len(runner.engine.graph.transitions),
                                  "active":len(runner.engine.graph.active),
                                  "candidate_sources":sum(len(c.sources) for c in runner.engine.graph.archive+runner.engine.graph.active),
                                  "candidate_condition_source_examples":sum(len(row.get("source_ids",[])) for c in runner.engine.graph.archive+runner.engine.graph.active for row in c.conditions)}}
        resource_samples.append(sample)
        row = {**report, "updated_at": utc_now(), "elapsed_seconds": round(elapsed(), 6), "observation_seconds": round(observation_seconds or elapsed(), 6),
               "counters": dict(counters), "fault_counts": dict(faults), "checks": checks,
               "journal_events": runner.store.latest_seq(), "processed_cursor": runner.cursor,
               "max_backlog": max_backlog, "throughput_events_per_second": round(runner.cursor / max(elapsed(), .000001), 6),
               "processing_latency": percentiles(processing), "feed_latency": percentiles(runner.metrics["feed_seconds"]),
               "latency_sample_limit": processing.maxlen, "resource": sample, "resource_samples": list(resource_samples),
               "memory_maintenance": sample["memory_maintenance"],
               "core_unchanged": True, "identity": runner._identity, "recovery_seconds": list(recovery_seconds),
               "process_provenance":runner.status()["health"]["process_provenance"],
               "pending_state": deepcopy({"positions": list(runner.paper.positions.values()), "orders": [o for o in runner.paper.orders.values() if o["status"] == "pending"]})}
        # Retain the failing observation as well as the preceding bounded
        # history. A resource failure must not erase its own measurement.
        last_status = row
        if rss > max_rss_mb * 1024 * 1024:
            report["rss_budget_failure"] = {"observed_rss_bytes": rss,
                                            "limit_rss_bytes": int(max_rss_mb * 1024 * 1024),
                                            "sample_at": sample["at"]}
            raise SoakFailure("rss_budget_exceeded")
        write_json_atomic(directory / "soak-status.json", row)
        if final:
            write_json_atomic(directory / "soak-report.json", row)
        return row

    def recover_instance():
        nonlocal runner
        begin = time.monotonic()
        runner.close()
        # Release the old evidence graph before allocating a replacement. An
        # assignment with Runner(...) on its RHS retains the old runner until
        # the new constructor and its memory-maintenance opportunity finish.
        maintenance = runner._memory_maintenance
        runner = None
        maintenance.maintain()
        check_core(manifest)
        runner = Runner(directory, config)
        runner.drain()
        counters["restarts"] += 1
        recovery_seconds.append(time.monotonic() - begin)

    def inject(kind, stream):
        nonlocal max_backlog
        check_core(manifest)
        begin = time.monotonic()
        detail = {}
        if kind == "duplicate":
            if last_event is None:
                ingest(received(stream))
            prior = runner.store.latest_seq()
            copy = MarketEvent(**{**last_event.__dict__, "arrival_time": last_event.arrival_time + timedelta(milliseconds=1)})
            seq, created = runner.ingest(copy, "soak_demo")
            counters["duplicate_attempts"] += 1
            if created or runner.store.latest_seq() != prior:
                raise SoakFailure("retransmission_created_new_event")
            detail = {"original_sequence": seq, "created": created}
        elif kind == "backlog_burst":
            for _ in range(burst_size):
                runner.append(received(stream), "soak_demo")
            backlog = runner.store.latest_seq() - runner.cursor
            max_backlog = max(max_backlog, backlog)
            runner.drain()
            detail = {"durable_backlog": backlog, "burst_size": burst_size}
        elif kind == "disconnect_reconnect":
            before = invariants(runner)["totals"]["fills"]
            runner.source_status("DISCONNECTED", symbols=list(DEFAULT_SYMBOLS))
            runner.drain()
            for _ in range(20):
                ev = received(stream)
                ingest(ev)
                if ev.kind == "quote":
                    break
            if invariants(runner)["totals"]["fills"] != before:
                raise SoakFailure("fill_while_disconnected")
            if recent_quote is None:
                raise SoakFailure("disconnect_test_lacks_quote")
            reconnect_at = recent_quote.arrival_time + timedelta(seconds=2)
            runner.source_status("CONNECTED", symbols=list(DEFAULT_SYMBOLS), at=reconnect_at.isoformat())
            runner.drain()
            stale = MarketEvent(**{**recent_quote.__dict__, "event_id": f"SOAK-STALE-{runner.cursor}",
                                   "arrival_time": max(recent_quote.arrival_time, runner.engine.arrival_watermark)})
            ingest(stale)
            if invariants(runner)["totals"]["fills"] != before:
                raise SoakFailure("queued_old_quote_filled_after_reconnect")
            detail = {"reconnected_at": reconnect_at.isoformat(), "stale_quote_event_id": stale.event_id, "fills_unchanged": True}
        elif kind in {"before_commit", "after_commit"}:
            runner.checkpoint()
            ev = received(stream)
            seq, created = runner.append(ev, "soak_demo")
            if not created:
                raise SoakFailure("fault_target_not_new")
            fired = []
            def hook(point, row):
                if point == kind and row["seq"] == seq and not fired:
                    fired.append(point)
                    raise InjectedFault(kind)
            runner._fault_hook = hook
            try:
                runner.drain()
            except InjectedFault:
                pass
            finally:
                runner._fault_hook = None
            if not fired:
                raise SoakFailure("fault_hook_not_reached")
            recover_instance()
            if runner.cursor < seq:
                raise SoakFailure("fault_target_not_recovered")
            detail = {"target_sequence": seq, "hook": fired[0], "recovered_cursor": runner.cursor}
        else:
            runner.checkpoint()
            before = combined_snapshot_sha256(runner)
            recover_instance()
            after = combined_snapshot_sha256(runner)
            if before != after:
                raise SoakFailure("checkpoint_restart_state_mismatch")
            detail = {"snapshot_sha256": after}
        invariants(runner)
        check_core(manifest)
        faults[kind] += 1
        counters["faults_completed"] += 1
        fault_record(kind, passed=True, seconds=round(time.monotonic() - begin, 6), **detail)

    try:
        runner = Runner(directory, config)
        if len(runner.engine.graph.specs) != 84:
            raise SoakFailure("all_84_graphs_not_enabled")
        provider = DemoProvider(symbols=DEFAULT_SYMBOLS, minutes=demo_minutes, sessions=2**31 - 1, pace_seconds=0)
        runner.store.set_meta("source", provider.describe())
        stream = iter(provider.events())
        next_fault = time.monotonic() + fault_interval_seconds
        next_status = time.monotonic()
        fault_index = 0
        while elapsed() < duration_seconds and not stop.is_set():
            check_core(manifest)
            ingest(received(stream))
            now = time.monotonic()
            if now >= next_fault:
                inject(FAULT_KINDS[fault_index % len(FAULT_KINDS)], stream)
                fault_index += 1
                next_fault = time.monotonic() + fault_interval_seconds
            if now >= next_status:
                snapshot_status()
                next_status = time.monotonic() + status_interval_seconds
            remaining = duration_seconds - elapsed()
            if remaining > 0 and pace:
                stop.wait(min(pace, remaining, 60))
        observation_seconds = elapsed()
        runner.drain()
        runner.checkpoint()
        checks = invariants(runner)
        check_core(manifest)
        report["replay"] = verify_replay(runner, directory, manifest)
        report["ended_at"] = utc_now()
        report["observation_seconds"] = round(observation_seconds, 6)
        report["checks"] = checks
        complete_faults = all(faults.values())
        report["required_faults_completed"] = complete_faults
        enough = observation_seconds >= REQUIRED_SECONDS
        if stop.is_set():
            report["status"] = "interrupted"
        elif not enough:
            report["status"] = "smoke_passed"
        elif not complete_faults or counters["sessions_started"] < 2:
            raise SoakFailure("long_run_lacks_required_faults_or_session_boundary")
        else:
            samples = list(resource_samples)
            if len(samples) < 10:
                raise SoakFailure("long_run_lacks_resource_observations")
            quarter = len(samples) // 4
            baseline = statistics.median(r["rss_bytes"] for r in samples[quarter:2*quarter])
            ending = statistics.median(r["rss_bytes"] for r in samples[-quarter:])
            growth = ending - baseline
            report["late_rss_growth_bytes"] = growth
            if growth > rss_growth_allowance_mb * 1024 * 1024:
                raise SoakFailure("unexplained_late_rss_growth")
            report["status"] = "passed"
        return snapshot_status(final=True)
    except BaseException as exc:
        # Keep completed invariant checks, identities, cursors and resource
        # observations when the run stops; they remain evidence, not acceptance.
        report = {**last_status, **report}
        try:
            unchanged = core_manifest() == manifest
        except Exception:
            unchanged = None
        if "core_version_changed" in str(exc):
            unchanged = False
        report.update(status="failed", ended_at=utc_now(), elapsed_seconds=round(elapsed(), 6),
                      observation_seconds=round(observation_seconds or elapsed(), 6),
                      error={"code": type(exc).__name__, "detail": str(exc) if isinstance(exc, (SoakFailure, ValueError, FileExistsError)) else "execution_failed_see_fault_evidence"},
                      counters=dict(counters), fault_counts=dict(faults), core_unchanged=unchanged)
        fault_record("failed", passed=False, error=report["error"])
        write_json_atomic(directory / "soak-status.json", report)
        write_json_atomic(directory / "soak-report.json", report)
        raise
    finally:
        if runner is not None:
            runner.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data-dir", required=True, type=Path)
    ap.add_argument("--duration-seconds", type=float, default=REQUIRED_SECONDS)
    ap.add_argument("--pace", type=float, default=.25)
    ap.add_argument("--fault-interval-seconds", type=float, default=900)
    ap.add_argument("--status-interval-seconds", type=float, default=60)
    ap.add_argument("--burst-size", type=int, default=300)
    ap.add_argument("--max-rss-mb", type=float, default=1024)
    ap.add_argument("--rss-growth-allowance-mb", type=float, default=128)
    ap.add_argument("--demo-minutes", type=int, default=390)
    args = ap.parse_args()
    stop = Event()
    for name in (signal.SIGINT, signal.SIGTERM):
        signal.signal(name, lambda *_: stop.set())
    try:
        row = run_soak(args.data_dir, args.duration_seconds, args.pace, fault_interval_seconds=args.fault_interval_seconds,
                       status_interval_seconds=args.status_interval_seconds, burst_size=args.burst_size, stop_event=stop,
                       max_rss_mb=args.max_rss_mb, rss_growth_allowance_mb=args.rss_growth_allowance_mb, demo_minutes=args.demo_minutes)
        print(json.dumps({"status": row["status"], "observation_seconds": row["observation_seconds"],
                          "required_seconds": REQUIRED_SECONDS, "events": row["processed_cursor"],
                          "report": str((args.data_dir / "soak-report.json").resolve())}, ensure_ascii=False))
        if row["status"] == "interrupted":
            raise SystemExit(130)
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": type(exc).__name__, "report": str((args.data_dir / "soak-report.json").resolve())}))
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
