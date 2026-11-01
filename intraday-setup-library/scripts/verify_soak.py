"""Read-only release gate for a completed, current-version ten-hour soak.

Usage: python3 scripts/verify_soak.py --report /outside/library/soak/soak-report.json
An optional --output writes the verification evidence, never the runtime database.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import statistics
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runtime.config import RuntimeConfig, canonical, fingerprint, validate_data_dir
from runtime.soak import FAULT_KINDS, REQUIRED_SECONDS, core_manifest


def require(condition, message):
    if not condition:
        raise ValueError(message)


def number(value, name, minimum=0):
    require(type(value) in {int, float} and math.isfinite(value) and value >= minimum,
            f"invalid {name}")
    return value


def integer(value, name, minimum=0):
    require(type(value) is int and value >= minimum, f"invalid {name}")
    return value


def validate_report(report, expected_core):
    """Validate assertions; this alone does not verify the persisted evidence."""
    from connectors import DEFAULT_SYMBOLS
    require(report.get("status") == "passed", "soak is not completed and passed")
    require(report.get("required_seconds") == REQUIRED_SECONDS, "wrong minimum observation duration")
    observed = number(report.get("observation_seconds"), "observation duration", REQUIRED_SECONDS)
    number(report.get("requested_seconds"), "requested duration", REQUIRED_SECONDS)
    number(report.get("elapsed_seconds"), "total elapsed duration", observed)
    times = [datetime.fromisoformat(report[name]) for name in ("started_at", "ended_at")]
    require(all(t.tzinfo is not None and t.utcoffset() is not None for t in times), "UTC evidence needs timezone")
    require((times[1] - times[0]).total_seconds() >= observed - 5, "UTC evidence contradicts observation duration")
    require(report.get("source_kind") == "synthetic" and report.get("execution") == "paper_only",
            "unexpected validation source or execution")
    require(report.get("market_validation") == "not_run", "synthetic soak cannot validate the market")
    require(report.get("symbols") == list(DEFAULT_SYMBOLS) and report.get("graphs") == 84,
            "soak must enable five symbols and all 84 graphs")
    require(report.get("core_unchanged") is True and report.get("core") == expected_core,
            "soak does not belong to the current frozen core")
    require(report.get("required_faults_completed") is True, "required faults incomplete")
    counts = report.get("fault_counts", {})
    for kind in FAULT_KINDS:
        integer(counts.get(kind), kind + " count", 1)
    counters = report.get("counters", {})
    integer(counters.get("sessions_started"), "session count", 2)
    require(counters.get("faults_completed") == sum(counts.values()), "fault totals disagree")
    checks = report.get("checks", {})
    require(checks.get("passed") is True and checks.get("sqlite_integrity") == "ok", "invariants failed")
    cursor = integer(report.get("processed_cursor"), "processed cursor", 1)
    require(report.get("journal_events") == cursor, "unprocessed journal remains")
    replay = report.get("replay", {})
    require(replay.get("passed") is True and replay.get("processed_cursor") == cursor, "full replay failed")
    require(replay.get("account", {}).get("balance_cents") == checks.get("balance_cents"), "account evidence disagrees")
    number(replay.get("seconds"), "separate replay duration")
    settings = report.get("settings", {})
    samples = report.get("resource_samples", [])
    require(len(samples) >= 10, "insufficient resource observations")
    budget = number(settings.get("max_rss_mb"), "RSS budget", 1) * 1024 * 1024
    allowance = number(settings.get("rss_growth_allowance_mb"), "RSS growth budget") * 1024 * 1024
    for sample in samples:
        number(sample.get("rss_bytes"), "RSS sample")
        require(sample["rss_bytes"] <= budget, "RSS budget exceeded")
        require(sample.get("disk_free_bytes", 0) > 0, "no free disk space")
    quarter = len(samples) // 4
    growth = statistics.median(r["rss_bytes"] for r in samples[-quarter:]) - statistics.median(r["rss_bytes"] for r in samples[quarter:2 * quarter])
    require(growth <= allowance and report.get("late_rss_growth_bytes", allowance + 1) <= allowance,
            "unexplained late RSS growth")
    require(report.get("resource", {}).get("backlog") == 0, "final backlog is nonzero")


def database_evidence(path, expected_identity):
    from paper import ExecutionPolicy
    from runtime.store import Store
    require(path.is_file(), "missing evidence database")
    with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as conn:
        conn.create_function("known_at", 3, Store._known_at, deterministic=True)
        conn.execute("BEGIN")
        require(conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok", "evidence database is corrupt")
        metadata = {key: json.loads(value) for key, value in conn.execute("SELECT key,value FROM meta")}
        require(metadata.get("identity") == expected_identity, "database identity disagrees")
        config = RuntimeConfig.load(**metadata["configuration"])
        require(config.fingerprint() == expected_identity["configuration_sha256"], "configuration identity disagrees")
        require(ExecutionPolicy(**config.paper).fingerprint() == expected_identity["execution_sha256"], "execution identity disagrees")
        cursor = metadata["processed_cursor"]
        count, last, pending = conn.execute("SELECT COUNT(*),COALESCE(MAX(seq),0),SUM(processed=0) FROM journal").fetchone()
        require(count == last == cursor and not pending, "evidence journal has gaps or uncommitted input")
        raw_hash, envelope_hash = hashlib.sha256(), hashlib.sha256()
        for seq, kind, source, event_id, payload in conn.execute("SELECT seq,kind,source,event_id,payload FROM journal ORDER BY seq"):
            value = json.loads(payload)
            raw_hash.update(canonical({"seq": seq, "kind": kind, "source": source, "payload": value}).encode())
            envelope_hash.update((canonical({"seq": seq, "kind": kind, "source": source, "event_id": event_id, "payload": value}) + "\n").encode())
        derived = hashlib.sha256()
        for row in conn.execute("SELECT id,topic,event_seq,output_index,identity,payload FROM records ORDER BY id"):
            derived.update((canonical(list(row)) + "\n").encode())
        checkpoint_row = conn.execute("SELECT cursor,signature,payload FROM checkpoints ORDER BY id DESC LIMIT 1").fetchone()
        require(checkpoint_row is not None and checkpoint_row[0] == cursor, "missing final checkpoint")
        checkpoint = json.loads(checkpoint_row[2])
        require(fingerprint(checkpoint) == checkpoint_row[1], "checkpoint checksum disagrees")
        require(checkpoint["cursor"] == cursor and checkpoint["identity"] == expected_identity and checkpoint["journal_sha256"] == raw_hash.hexdigest()
                and checkpoint["derived_sha256"] == derived.hexdigest(), "checkpoint evidence disagrees")
        return {"processed_cursor": cursor, "derived_sha256": derived.hexdigest(),
                "journal_file_sha256": envelope_hash.hexdigest(), "account": metadata["account"],
                "engine_sha256": fingerprint(checkpoint["engine"]), "paper_sha256": fingerprint(checkpoint["paper"])}


def verify_soak(report_path):
    from recognition.provenance import algorithm_sha256
    from runtime.runner import runtime_hash
    report_path = Path(report_path).resolve()
    directory = validate_data_dir(report_path.parent)
    report_bytes = report_path.read_bytes()
    report = json.loads(report_bytes)
    validate_report(report, core_manifest())
    identity = report["identity"]
    require(identity["algorithm_sha256"] == algorithm_sha256() and identity["runtime_sha256"] == runtime_hash(),
            "current executable identity disagrees")
    faults = [json.loads(line) for line in (directory / "soak-faults.jsonl").read_text().splitlines() if line]
    require(all(row.get("passed") is True and row.get("kind") in FAULT_KINDS for row in faults), "fault evidence includes failures")
    for kind in FAULT_KINDS:
        require(sum(row["kind"] == kind for row in faults) == report["fault_counts"][kind], "fault evidence count disagrees")
    original = database_evidence(directory / "runtime.sqlite3", identity)
    replay_dir = directory / "replay-verification"
    require(Path(report["replay"]["directory"]).resolve() == replay_dir, "unexpected replay evidence location")
    replay = database_evidence(replay_dir / "runtime.sqlite3", identity)
    require(original == replay, "original and replay database evidence differ")
    for key in ("processed_cursor", "derived_sha256", "account", "engine_sha256", "paper_sha256"):
        require(original[key] == report["replay"][key], key + " disagrees with published replay")
    journal_digest = hashlib.sha256()
    with (directory / "soak-journal.jsonl").open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            journal_digest.update(chunk)
    require(journal_digest.hexdigest() == original["journal_file_sha256"], "exported full journal disagrees with databases")
    require(report_path.read_bytes() == report_bytes, "report changed during verification")
    require(report["core"] == core_manifest(), "core changed during verification")
    return {"status": "passed", "validation": "actual_ten_hour_soak_and_persisted_full_replay",
            "verified_at_utc": datetime.now(timezone.utc).isoformat(), "report": str(report_path),
            "report_sha256": hashlib.sha256(report_bytes).hexdigest(), "core_sha256": report["core"]["sha256"],
            "identity": identity, "observation_seconds": report["observation_seconds"],
            "required_seconds": REQUIRED_SECONDS, "fault_counts": report["fault_counts"],
            "journal_events": original["processed_cursor"], "journal_sha256": journal_digest.hexdigest(),
            "derived_sha256": original["derived_sha256"], "source_kind": "synthetic", "market_validation": "not_run"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        evidence = verify_soak(args.report)
    except (ValueError, KeyError, TypeError, OSError, sqlite3.Error) as error:
        print(json.dumps({"status": "failed", "reason": str(error)}, ensure_ascii=False))
        return 1
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(evidence, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
