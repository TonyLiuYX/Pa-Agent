"""Independent recognition of committed backfill, with original causal clocks.

This reader never constructs a runtime runner or a paper account. Historical
events require an actual, already received session for their symbol and venue;
a current-day session is not a substitute for missing historical context.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import uuid

from recognition.config import RecognitionConfig
from recognition.engine import RecognitionEngine
from recognition.models import MarketEvent, timestamp
from recognition.provenance import algorithm_sha256, file_sha256
from recognition.specs import RULE_VERSION, load_specs
from .config import canonical, fingerprint, validate_data_dir

DEFAULT_MAX_INPUT_BYTES = 64 * 1024 * 1024
DEFAULT_MAX_BACKFILL_EVENTS = 10000


def _load_json(text):
    def reject(token):
        raise ValueError("nonfinite JSON number: " + token)
    return json.loads(text, parse_constant=reject)


def _session_day(event):
    explicit = event.payload.get("session_date")
    if explicit is not None:
        if not isinstance(explicit, str) or date.fromisoformat(explicit).isoformat() != explicit:
            raise ValueError("session_date must be an explicit ISO date")
        return explicit
    # Preserve the offset supplied by the source rather than guessing a venue's
    # calendar or converting an overnight session to a different local date.
    return datetime.fromisoformat(event.payload["open_time"].replace("Z", "+00:00")).date().isoformat()


def _hash_text(digest, text):
    size = 0
    for offset in range(0, len(text), 65536):
        chunk = text[offset:offset + 65536].encode("utf-8")
        size += len(chunk)
        digest.update(chunk)
    return size


def _read_prefix(database, max_input_bytes, max_backfill_events):
    connection = sqlite3.connect(database.as_uri() + "?mode=ro", uri=True, timeout=15)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        metadata = {r["key"]: _load_json(r["value"]) for r in connection.execute(
            "SELECT key,value FROM meta WHERE key IN ('configuration','processed_cursor','run_id','identity')")}
        cursor = metadata.get("processed_cursor", 0)
        if type(cursor) is not int or cursor < 0:
            raise ValueError("invalid source processed cursor")
        record_cursor = connection.execute("SELECT COALESCE(MAX(id),0) FROM records WHERE event_seq<=?", (cursor,)).fetchone()[0]
        backfill, sessions = [], []
        journal_digest = hashlib.sha256()
        count = input_bytes = backfill_count = context_count = 0
        capacity = []
        for raw in connection.execute("SELECT *,CASE WHEN kind='market' THEN json_extract(payload,'$.kind') END AS market_event_kind "
                                      "FROM journal WHERE seq<=? ORDER BY seq", (cursor,)):
            if raw["seq"] != count + 1 or raw["processed"] != 1:
                raise ValueError("source committed prefix is not contiguous and fully processed")
            envelope = {k: raw[k] for k in ("seq", "kind", "source", "event_id", "signature", "recorded_at", "processed")}
            journal_digest.update((canonical(envelope) + "\n").encode("utf-8"))
            payload_bytes = _hash_text(journal_digest, raw["payload"])
            journal_digest.update(b"\n")
            is_context = raw["kind"] == "market" and raw["market_event_kind"] == "session"
            if raw["kind"] == "backfill":
                backfill_count += 1
            elif is_context:
                context_count += 1
            if raw["kind"] == "backfill" or is_context:
                input_bytes += payload_bytes
                if not capacity and (input_bytes > max_input_bytes or backfill_count > max_backfill_events):
                    capacity.append({"reason": "backfill_input_capacity_exceeded", "source_seq": raw["seq"],
                        "max_input_bytes": max_input_bytes, "max_backfill_events": max_backfill_events,
                        "input_bytes_at_limit": input_bytes, "backfill_events_at_limit": backfill_count})
                    backfill.clear()
                    sessions.clear()
                if not capacity:
                    row = {"source_seq": raw["seq"], "source": raw["source"], "journal_kind": raw["kind"],
                           "event_id": raw["event_id"], "payload": _load_json(raw["payload"]), "source_signature": raw["signature"]}
                    (backfill if raw["kind"] == "backfill" else sessions).append(row)
            count = raw["seq"]
        if count != cursor:
            raise ValueError("source committed cursor is absent from journal")
        pending = connection.execute("SELECT COUNT(*) FROM journal WHERE kind='backfill' AND (seq>? OR processed=0)", (cursor,)).fetchone()[0]
        connection.execute("ROLLBACK")
        return metadata, backfill, sessions, {"processed_cursor": cursor, "outbox_cursor": record_cursor,
            "journal_prefix_sha256": journal_digest.hexdigest(), "journal_hash_encoding": "ordered_envelope_and_stored_payload_utf8_v1",
            "pending_backfill_excluded": pending, "committed_backfill_count": backfill_count,
            "available_market_session_count": context_count, "input_candidate_bytes": input_bytes,
            "input_selection_complete": not capacity}, capacity
    finally:
        connection.close()


def _bind_sessions(backfill, source_sessions):
    """Resolve contexts before recognition, preserving source sequence order."""
    available = defaultdict(list)
    selected_contexts, bindings, missing = {}, {}, []
    backfill_sequences = {r["source_seq"] for r in backfill}
    for row in sorted([*source_sessions, *backfill], key=lambda r: r["source_seq"]):
        try:
            event = MarketEvent.from_dict(row["payload"])
            if event.kind == "session":
                day = _session_day(event)
                available[(event.symbol, event.venue)].append((row, event, day))
                if row["source_seq"] in backfill_sequences:
                    bindings[row["source_seq"]] = day
                continue
            if row["source_seq"] not in backfill_sequences:
                continue
            cutoff = event.event_time
            start = cutoff - timedelta(seconds=event.payload["seconds"]) if event.kind == "bar" else cutoff
            eligible = [(context, session, day) for context, session, day in available[(event.symbol, event.venue)]
                        if session.arrival_time <= event.arrival_time and session.event_time <= start
                        and cutoff <= timestamp(session.payload["close_time"])]
            windows = {(day, timestamp(session.payload["open_time"]), timestamp(session.payload["close_time"]))
                       for _, session, day in eligible}
            if not eligible or len(windows) != 1:
                reason = "missing_received_session_for_market_interval" if not eligible else "ambiguous_declared_session_intervals"
                missing.append({"source_seq": row["source_seq"], "event_id": event.event_id,
                    "symbol": event.symbol, "venue": event.venue, "event_time": event.to_dict()["event_time"], "reason": reason})
                continue
            context, _, day = eligible[-1]
            bindings[row["source_seq"]] = day
            if context["source_seq"] not in backfill_sequences:
                selected_contexts[context["source_seq"]] = dict(context, session_day=day)
        except (KeyError, TypeError, ValueError) as error:
            missing.append({"source_seq": row["source_seq"], "event_id": row["event_id"],
                "symbol": row["payload"].get("symbol"), "venue": row["payload"].get("venue"),
                "reason": "invalid_original_event_or_session", "detail": str(error)})
    selected = [dict(row, role="session_context") for row in selected_contexts.values()]
    selected.extend(dict(row, role="backfill", session_day=bindings.get(row["source_seq"])) for row in backfill)
    selected.sort(key=lambda r: r["source_seq"])
    return selected, missing


def _source_kind(configuration):
    mode = configuration.get("mode")
    if mode == "demo":
        return "synthetic"
    if mode == "live":
        return "real_market"
    kind = configuration.get("source_kind", "unknown")
    return kind if kind in {"synthetic", "real_market"} else "unknown"


def analyze_backfill(source_directory, output_directory, *, max_input_bytes=DEFAULT_MAX_INPUT_BYTES,
                     max_backfill_events=DEFAULT_MAX_BACKFILL_EVENTS):
    """Analyze only committed backfill in a new, exclusive research directory.

    A blocked result is a normal return and retains its inputs and explanations.
    Filesystem errors, invalid source journal structure and malformed source
    configuration raise instead. No event is reordered by market or receipt
    time, no context is invented, and no order or fill is created.
    """
    source = Path(source_directory).expanduser().resolve(strict=True)
    database = (source / "runtime.sqlite3").resolve(strict=True)
    target = validate_data_dir(output_directory)
    if target == source or target.is_relative_to(source):
        raise ValueError("backfill output must be independent of the source directory")
    if any(type(value) is not int or value <= 0 for value in (max_input_bytes, max_backfill_events)):
        raise ValueError("backfill input limits must be positive integers")
    metadata, backfill, contexts, prefix, capacity = _read_prefix(database, max_input_bytes, max_backfill_events)
    source_config = metadata.get("configuration", {})
    if not isinstance(source_config, dict):
        raise ValueError("source configuration must be an object")
    config = RecognitionConfig(**source_config.get("recognition", {})).validate()
    kind = _source_kind(source_config)
    selected, missing = ([], capacity) if capacity else _bind_sessions(backfill, contexts)
    target.mkdir(parents=True, exist_ok=False)
    rules = load_specs(include_variants=True)
    analysis_config = {"recognition": asdict(config), "include_variants": True,
        "execution": "recognition_only", "engine_scope": "independent_declared_session_day",
        "context_policy": "received_matching_symbol_venue_and_market_interval", "runtime_policy": None,
        "max_input_bytes": max_input_bytes, "max_backfill_events": max_backfill_events}
    (target / "configuration.json").write_text(canonical(analysis_config) + "\n", encoding="utf-8")
    (target / "rules.json").write_text(canonical({"schema_version": "1.0.0", "rules": rules}) + "\n", encoding="utf-8")
    with (target / "events.jsonl").open("w", encoding="utf-8") as events, (target / "source-inputs.jsonl").open("w", encoding="utf-8") as provenance:
        for row in selected:
            events.write(canonical(row["payload"]) + "\n")
            provenance.write(canonical({k: v for k, v in row.items() if k != "payload"}) + "\n")
    if not backfill and not capacity:
        missing.append({"reason": "no_committed_backfill"})
    engines, counts = {}, defaultdict(int)
    last_for_day = {r["session_day"]: r["source_seq"] for r in selected if r.get("session_day")}
    analyzed = detections_count = 0
    status = "blocked" if missing else "completed"
    # Output remains an exact prefix on a stream error; the full, untouched raw
    # input is retained separately for a researcher to repair its context.
    with (target / "detections.jsonl").open("w", encoding="utf-8") as detections:
        if not missing:
            for row in selected:
                day = row["session_day"]
                engine = engines.setdefault(day, None)
                if engine is None:
                    engine = engines[day] = RecognitionEngine(include_variants=True, config=config)
                try:
                    output = engine.feed(MarketEvent.from_dict(row["payload"]))
                except (KeyError, TypeError, ValueError) as error:
                    status = "blocked"
                    missing.append({"source_seq": row["source_seq"], "event_id": row["event_id"],
                        "reason": "original_stream_rejected_without_reordering", "detail": str(error)})
                    break
                for detection in output:
                    document = detection.to_dict()
                    if kind != "unknown":
                        document["source_kind"] = kind
                    detections.write(canonical(document) + "\n")
                    detections_count += 1
                counts[day] += 1
                analyzed += row["role"] == "backfill"
                if row["source_seq"] == last_for_day[day]:
                    del engines[day]
    report = {"schema_version": "1.0.0", "run_id": str(uuid.uuid4()), "status": status,
        "created_at": datetime.now(timezone.utc).isoformat(), "analysis_kind": "independent_backfill_research",
        "execution": "recognition_only", "market_validation": "not_run", "source_kind": kind,
        "source_kind_basis": "original_runtime_mode_or_replay_declaration_not_independent_authenticity_verification",
        "source_directory": str(source), "source_run_id": metadata.get("run_id"),
        "source_identity": metadata.get("identity"), "source_configuration_sha256": fingerprint(source_config),
        "source_snapshot": prefix, "source_sequence_policy": "preserved_original_source_seq_and_arrival_time",
        "input_selection_complete": not capacity,
        "selected_backfill_count": len(backfill), "analyzed_backfill_count": analyzed,
        "context_event_count": sum(r["role"] == "session_context" for r in selected),
        "detection_count": detections_count, "session_input_counts": dict(counts), "missing_context": missing,
        "rule_version": RULE_VERSION, "algorithm_sha256": algorithm_sha256(),
        "configuration_sha256": config.fingerprint(), "analysis_configuration_sha256": file_sha256(target / "configuration.json"),
        "input_provenance": {"scope": "blocked_backfill_selection" if capacity else "independent_backfill_research", "processed_cursor": prefix["processed_cursor"],
            "input_selection_complete": not capacity, "analysis_completed": status == "completed",
            "source_kind": kind, "data_file": "events.jsonl", "rule_file": "rules.json", "detections_file": "detections.jsonl",
            "data_sha256": file_sha256(target / "events.jsonl"), "rule_sha256": file_sha256(target / "rules.json"),
            "detections_sha256": file_sha256(target / "detections.jsonl"), "algorithm_sha256": algorithm_sha256(),
            "configuration_sha256": config.fingerprint(), "source_mapping_file": "source-inputs.jsonl",
            "source_mapping_sha256": file_sha256(target / "source-inputs.jsonl")}}
    (target / "run.json").write_text(canonical(report) + "\n", encoding="utf-8")
    return report
