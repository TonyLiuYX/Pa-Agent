"""Schema and semantic validation for Phase 2 contract artifacts.

Validation is intentionally deterministic and returns typed issues rather than
raising implementation tracebacks.  The module has no provider, market-data
or network dependencies.
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Mapping, MutableSequence
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .canonical import (
    CanonicalizationError,
    bar_id_for,
    citation_id_for,
    content_hash,
    evidence_id_for,
    idempotency_key_for,
    observation_id_for,
    text_sha256,
    verify_identity,
)
from .registry import METADATA, artifact_kinds, validator_for


# Secret detection is intentionally controlled rather than a broad substring
# search: JSON-schema fields such as ``prompt_tokens`` and ``total_tokens`` are
# ordinary telemetry, while credential-bearing names must still be rejected in
# the core object and inside ``extensions``.
SECRET_EXACT = {
    "apikey", "apitoken", "apisecret", "accesskey", "accesstoken", "authtoken",
    "bearertoken", "refreshtoken", "clientsecret", "clientcredential",
    "authorization", "password", "passwd", "credential", "credentials",
    "secret", "token", "privatekey", "signingkey", "signingsecret", "webhooksecret",
    "oauthsecret", "oauthaccesstoken", "oauthrefreshtoken",
}
SECRET_SUFFIXES = ("apikey", "apitoken", "apisecret", "accesstoken", "authtoken", "bearertoken", "refreshtoken", "clientsecret", "credential", "credentials", "authorization", "password", "passwd", "privatekey", "signingkey", "signingsecret", "webhooksecret")
SECRET_SAFE_KEYS = {
    "prompttokens", "completiontokens", "totaltokens", "maxoutputtokens",
    "tokencount", "tokenusage", "tokenbudget",
}
UTC_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,9})?Z$")
DECIMAL_RE = re.compile(r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]*[1-9])?$")
POSITIVE_RE = re.compile(r"^(?:[1-9][0-9]*(?:\.[0-9]*[1-9])?|0\.[0-9]*[1-9])$")
NONNEGATIVE_RE = re.compile(r"^(?:0|[1-9][0-9]*)(?:\.[0-9]*[1-9])?$")


@dataclass(frozen=True)
class ValidationIssue:
    code: str
    path: str
    message: str

    def to_dict(self) -> dict[str, str]:
        return {"code": self.code, "path": self.path, "message": self.message}

    as_dict = to_dict


@dataclass
class ValidationResult:
    valid: bool
    artifact_kind: str | None
    issues: list[ValidationIssue] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "valid": bool(self.valid),
            "artifact_kind": self.artifact_kind,
            "issues": [issue.to_dict() for issue in self.issues],
        }

    as_dict = to_dict

    def __bool__(self) -> bool:
        return self.valid


def _path(parts: Iterable[Any]) -> str:
    value = "$"
    for part in parts:
        value += f"[{part}]" if isinstance(part, int) else f".{part}"
    return value


def _add(issues: MutableSequence[ValidationIssue], code: str, path: str, message: str) -> None:
    issues.append(ValidationIssue(code, path or "$", message))


def _walk(value: Any, path: str = "$", parent: Any = None, key: Any = None):
    yield value, path, parent, key
    if isinstance(value, dict):
        for k, v in value.items():
            yield from _walk(v, f"{path}.{k}", value, k)
    elif isinstance(value, list):
        for i, v in enumerate(value):
            yield from _walk(v, f"{path}[{i}]", value, i)


def _secret_scan(document: Any, issues: MutableSequence[ValidationIssue]) -> None:
    for value, path, _parent, key in _walk(document):
        if isinstance(key, str):
            normalized = re.sub(r"[^a-z0-9]", "", key.lower())
            is_secret = normalized in SECRET_EXACT or (
                normalized not in SECRET_SAFE_KEYS and any(normalized.endswith(suffix) for suffix in SECRET_SUFFIXES)
            )
            if is_secret:
                _add(issues, "secret_field", path.rsplit(".", 1)[0], "secret-like field is not allowed")


def _native_scan(document: Any, issues: MutableSequence[ValidationIssue]) -> None:
    for value, path, _parent, key in _walk(document):
        if type(value) not in (type(None), bool, int, str, list, dict):
            _add(issues, "invalid_json", path, "value is not strict JSON-native data")
        if isinstance(value, dict) and any(type(k) is not str for k in value):
            _add(issues, "invalid_json", path, "object keys must be strings")


def _schema_error_code(error: Any) -> str:
    path = list(error.path)
    joined = ".".join(str(p) for p in path)
    msg = str(error.message)
    if error.validator == "additionalProperties":
        return "unknown_field"
    if "schema_version" in joined or error.validator == "const" and "0.1.0" in msg:
        return "version_mismatch"
    if error.validator == "format" and ("date" in msg.lower() or "time" in msg.lower()):
        return "non_utc_timestamp"
    if any(token in joined.lower() for token in ("open", "high", "low", "close", "volume", "price", "decimal", "range", "wick", "value", "confidence", "temperature")) and error.validator in {"pattern", "type", "not"}:
        return "invalid_decimal"
    if "timezone" in joined.lower():
        return "invalid_timezone"
    return "schema_violation"


def _schema_validate(document: Any, kind: str, issues: MutableSequence[ValidationIssue]) -> None:
    try:
        validator = validator_for(kind)
        errors = sorted(validator.iter_errors(document), key=lambda e: (list(e.path), e.message))
    except Exception as exc:  # never expose traceback to callers
        _add(issues, "schema_violation", "$", "schema validator unavailable")
        return
    for error in errors:
        path = _path(error.path)
        code = _schema_error_code(error)
        if code == "unknown_field":
            extra = ""
            match = re.search(r"'([^']+)'", str(error.message))
            if match:
                extra = f" ({match.group(1)})"
            _add(issues, code, path, f"unknown field{extra}")
        else:
            _add(issues, code, path, str(error.message))


def _parse_utc(value: Any) -> datetime | None:
    if not isinstance(value, str) or not UTC_RE.fullmatch(value):
        return None
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError:
        return None
    return parsed if parsed.tzinfo == timezone.utc else None


def _timestamp_scan(document: Any, issues: MutableSequence[ValidationIssue]) -> None:
    for value, path, _parent, key in _walk(document):
        if not isinstance(key, str):
            continue
        lower = key.lower()
        if lower == "timezone":
            if not isinstance(value, str):
                continue
            try:
                if value not in {"UTC", "GMT"} and "/" not in value:
                    raise ZoneInfoNotFoundError(value)
                ZoneInfo(value)
            except (ZoneInfoNotFoundError, ValueError):
                _add(issues, "invalid_timezone", path, "timezone is not a valid IANA zone")
        if lower.endswith("_at") or lower.endswith("_as_of") or lower == "generated_at":
            if value is None:
                continue
            if _parse_utc(value) is None:
                _add(issues, "non_utc_timestamp", path, "timestamp must be a real UTC RFC3339 value ending in Z")


def _decimal_scan(document: Any, issues: MutableSequence[ValidationIssue]) -> None:
    names = {"open", "high", "low", "close", "volume", "range", "body", "upper_wick", "lower_wick", "value", "temperature", "model_confidence", "knowledge_confidence"}
    for value, path, _parent, key in _walk(document):
        if isinstance(key, str) and key.lower() in names and value is not None:
            if not isinstance(value, str) or not DECIMAL_RE.fullmatch(value):
                _add(issues, "invalid_decimal", path, "decimal must be a finite canonical decimal string")
            else:
                try:
                    decimal = Decimal(value)
                    if not decimal.is_finite():
                        raise InvalidOperation
                except (InvalidOperation, ValueError):
                    _add(issues, "invalid_decimal", path, "decimal is not finite")


def _as_decimal(value: Any) -> Decimal | None:
    if not isinstance(value, str):
        return None
    try:
        result = Decimal(value)
    except (InvalidOperation, ValueError):
        return None
    return result if result.is_finite() else None


def _related_map(related_artifacts: Any) -> dict[str, dict[str, Any]]:
    """Normalize supported related-artifact inputs into kind -> document."""

    out: dict[str, dict[str, Any]] = {}
    if related_artifacts is None:
        return out
    items: Iterable[Any]
    if isinstance(related_artifacts, Mapping):
        items = related_artifacts.items()
    elif isinstance(related_artifacts, (list, tuple)):
        items = related_artifacts
    else:
        return out
    for item in items:
        key: str | None = None
        value: Any = item
        if isinstance(item, tuple) and len(item) == 2 and isinstance(item[0], str):
            key, value = item
        elif isinstance(related_artifacts, Mapping):
            key, value = item
        if isinstance(value, (str, Path)):
            try:
                with Path(value).open("r", encoding="utf-8") as fh:
                    value = json.load(fh)
            except Exception:
                continue
        if isinstance(value, Mapping):
            kind = value.get("artifact_kind") or key
            if isinstance(kind, str):
                out[kind] = dict(value)
    return out


def _first(related: Mapping[str, Mapping[str, Any]], *kinds: str) -> Mapping[str, Any] | None:
    for kind in kinds:
        if kind in related:
            return related[kind]
    return None


def _check_hash_ref(doc: Mapping[str, Any], related: Mapping[str, Mapping[str, Any]], field: str, kinds: tuple[str, ...], issues: MutableSequence[ValidationIssue], path: str) -> None:
    other = _first(related, *kinds)
    if other is None or field not in doc:
        return
    target_hash = other.get("content_hash")
    if target_hash is not None and doc.get(field) != target_hash:
        _add(issues, "related_artifact_mismatch", path + "." + field, "related artifact hash does not match")


def _check_id_ref(doc: Mapping[str, Any], related: Mapping[str, Mapping[str, Any]], field: str, kinds: tuple[str, ...], issues: MutableSequence[ValidationIssue], path: str) -> None:
    for kind in kinds:
        other = related.get(kind)
        if other is None or field not in doc:
            continue
        meta = METADATA.get(kind)
        id_key = meta.get("id_field", "").lstrip("/") if meta else ""
        target = other.get(id_key) if id_key else None
        if target is not None and doc.get(field) != target:
            _add(issues, "related_artifact_mismatch", path + "." + field, "related artifact id does not match")


def _related_id(kind: str, artifact: Mapping[str, Any]) -> Any:
    meta = METADATA.get(kind)
    if not meta:
        return None
    return artifact.get(meta.get("id_field", "").lstrip("/"))


def _validate_market(document: Mapping[str, Any], issues: MutableSequence[ValidationIssue]) -> None:
    bars = document.get("bars")
    if not isinstance(bars, list):
        return
    instrument = document.get("instrument")
    timeframe = document.get("timeframe")
    session = document.get("session")
    as_of = _parse_utc(document.get("analysis_as_of"))
    seen_ids: set[str] = set()
    seen_starts: set[str] = set()
    previous_start: datetime | None = None
    previous_end: datetime | None = None
    gaps: list[tuple[datetime, datetime]] = []
    incomplete = []
    expected_duration: timedelta | None = None
    if isinstance(timeframe, Mapping):
        unit, mult = timeframe.get("unit"), timeframe.get("multiplier")
        if isinstance(mult, int) and not isinstance(mult, bool):
            if unit == "minute":
                expected_duration = timedelta(minutes=mult)
            elif unit == "hour":
                expected_duration = timedelta(hours=mult)
    for i, bar in enumerate(bars):
        path = f"$.bars[{i}]"
        if not isinstance(bar, Mapping):
            continue
        start = _parse_utc(bar.get("start_at"))
        end = _parse_utc(bar.get("end_at"))
        if start is None or end is None:
            continue
        if end <= start:
            _add(issues, "invalid_market_data", path, "bar interval must have start before end")
        if expected_duration is not None and end - start != expected_duration:
            _add(issues, "invalid_market_data", path + ".end_at", "bar duration does not match timeframe")
        if previous_start is not None and start <= previous_start:
            _add(issues, "invalid_market_data", path + ".start_at", "bars must be strictly ordered")
        if previous_end is not None and start < previous_end:
            _add(issues, "invalid_market_data", path + ".start_at", "bar intervals overlap")
        if expected_duration is not None and previous_end is not None and start > previous_end:
            gaps.append((previous_end, start))
        previous_start, previous_end = start, end
        if bar.get("start_at") in seen_starts:
            _add(issues, "invalid_market_data", path + ".start_at", "duplicate bar timestamp")
        seen_starts.add(bar.get("start_at"))
        expected_id = bar_id_for(instrument, timeframe, session, bar.get("start_at"), bar.get("end_at"))
        if bar.get("bar_id") != expected_id:
            _add(issues, "invalid_market_data", path + ".bar_id", "bar_id does not match helper identity")
        if bar.get("bar_id") in seen_ids:
            _add(issues, "invalid_market_data", path + ".bar_id", "duplicate bar_id")
        seen_ids.add(bar.get("bar_id"))
        values = {name: _as_decimal(bar.get(name)) for name in ("open", "high", "low", "close", "volume")}
        if any(v is None for v in values.values()):
            continue
        if values["volume"] < 0 or values["high"] < max(values["open"], values["close"]) or values["low"] > min(values["open"], values["close"]) or values["high"] < values["low"]:
            _add(issues, "invalid_market_data", path, "OHLC invariant violated")
        if as_of is not None and end > as_of:
            _add(issues, "invalid_market_data", path + ".end_at", "bar ends after analysis_as_of")
        if bar.get("is_closed") is False:
            incomplete.append(i)
    if len(incomplete) > 1 or (incomplete and incomplete[0] != len(bars) - 1):
        _add(issues, "invalid_market_data", "$.bars", "at most the final bar may be incomplete")
    coverage = document.get("coverage")
    if isinstance(coverage, Mapping):
        expected, actual = coverage.get("expected"), coverage.get("actual")
        e_start, e_end = (_parse_utc(expected.get("start_at")), _parse_utc(expected.get("end_at"))) if isinstance(expected, Mapping) else (None, None)
        a_start, a_end = (_parse_utc(actual.get("start_at")), _parse_utc(actual.get("end_at"))) if isinstance(actual, Mapping) else (None, None)
        if bars and (a_start is None or a_end is None or a_start != _parse_utc(bars[0].get("start_at")) or a_end != _parse_utc(bars[-1].get("end_at"))):
            _add(issues, "invalid_market_data", "$.coverage.actual", "actual coverage must match bar boundaries")
        if e_start is not None and e_end is not None and e_end <= e_start:
            _add(issues, "invalid_market_data", "$.coverage.expected", "expected coverage interval must be ordered")
        if a_start is not None and a_end is not None and a_end <= a_start:
            _add(issues, "invalid_market_data", "$.coverage.actual", "actual coverage interval must be ordered")
        missing = coverage.get("missing_intervals")
        if isinstance(missing, list):
            previous_missing_end: datetime | None = None
            for j, interval in enumerate(missing):
                if not isinstance(interval, Mapping):
                    continue
                ms, me = _parse_utc(interval.get("start_at")), _parse_utc(interval.get("end_at"))
                if ms is None or me is None or me <= ms or (previous_missing_end is not None and ms < previous_missing_end):
                    _add(issues, "invalid_market_data", f"$.coverage.missing_intervals[{j}]", "missing intervals must be ordered")
                previous_missing_end = me
        status = coverage.get("status")
        if status == "complete" and missing != []:
            _add(issues, "invalid_market_data", "$.coverage.missing_intervals", "complete coverage must have no missing intervals")
        if status == "partial" and not missing:
            _add(issues, "invalid_market_data", "$.coverage.missing_intervals", "partial coverage must list missing intervals")
        declared_missing: list[tuple[datetime, datetime]] = []
        if isinstance(missing, list):
            for interval in missing:
                if isinstance(interval, Mapping):
                    ms, me = _parse_utc(interval.get("start_at")), _parse_utc(interval.get("end_at"))
                    if ms is not None and me is not None:
                        declared_missing.append((ms, me))
        if gaps:
            if status == "complete":
                _add(issues, "invalid_market_data", "$.coverage", "complete coverage cannot contain undeclared bar gaps")
            else:
                for gap_start, gap_end in gaps:
                    if not any(ms <= gap_start and me >= gap_end for ms, me in declared_missing):
                        _add(issues, "invalid_market_data", "$.coverage.missing_intervals", "partial coverage must declare every bar gap")
        for ms, me in declared_missing:
            if any(bs is not None and be is not None and ms < be and me > bs for bs, be in ((_parse_utc(b.get("start_at")), _parse_utc(b.get("end_at"))) for b in bars if isinstance(b, Mapping))):
                _add(issues, "invalid_market_data", "$.coverage.missing_intervals", "missing interval overlaps a bar")
        quality = document.get("quality")
        if isinstance(quality, Mapping):
            if status == "complete" and (quality.get("status") != "ok"):
                _add(issues, "invalid_market_data", "$.quality", "complete coverage requires ok quality")
            if status == "partial" and quality.get("status") != "partial":
                _add(issues, "invalid_market_data", "$.quality", "partial coverage requires partial quality")
        if e_start and e_end and a_start and a_end and (e_start > a_start or a_end > e_end):
            _add(issues, "invalid_market_data", "$.coverage", "actual coverage falls outside expected coverage")


def _validate_observation(document: Mapping[str, Any], related: Mapping[str, Mapping[str, Any]], issues: MutableSequence[ValidationIssue]) -> None:
    observations = document.get("observations")
    if not isinstance(observations, list):
        return
    snapshot = _first(related, "market_snapshot")
    formula = document.get("formula_version")
    seen_ids: set[str] = set()
    seen_bars: set[str] = set()
    market_bars = {bar.get("bar_id"): bar for bar in (snapshot or {}).get("bars", []) if isinstance(bar, Mapping)}
    # An incomplete final bar is deliberately excluded from the deterministic
    # observation frame.  Every closed bar must remain present and ordered.
    market_order = [bar.get("bar_id") for bar in (snapshot or {}).get("bars", []) if isinstance(bar, Mapping) and bar.get("is_closed") is True]
    if snapshot is not None:
        if document.get("market_snapshot_id") != snapshot.get("market_snapshot_id") or document.get("market_snapshot_hash") != snapshot.get("content_hash"):
            _add(issues, "related_artifact_mismatch", "$.market_snapshot_id", "observation frame market lineage mismatch")
    for i, obs in enumerate(observations):
        if not isinstance(obs, Mapping):
            continue
        path = f"$.observations[{i}]"
        oid = obs.get("observation_id")
        bid = obs.get("bar_id")
        expected = observation_id_for(document.get("market_snapshot_id"), bid, formula)
        if oid != expected:
            _add(issues, "related_artifact_mismatch", path + ".observation_id", "observation_id does not match helper identity")
        if oid in seen_ids or bid in seen_bars:
            _add(issues, "related_artifact_mismatch", path, "observation or bar reference is duplicated")
        seen_ids.add(oid)
        seen_bars.add(bid)
        if snapshot is not None and (bid not in market_bars or market_bars[bid].get("is_closed") is not True):
            _add(issues, "related_artifact_mismatch", path + ".bar_id", "observation bar is absent from related market snapshot")
        for j, window in enumerate(obs.get("rolling_windows", []) if isinstance(obs.get("rolling_windows"), list) else []):
            if not isinstance(window, Mapping):
                continue
            if window.get("status") == "available":
                high, low = _as_decimal(window.get("high")), _as_decimal(window.get("low"))
                if high is None or low is None or high < low:
                    _add(issues, "invalid_market_data", f"{path}.rolling_windows[{j}]", "available rolling high/low is invalid")
            elif window.get("status") == "insufficient_history" and (window.get("high") is not None or window.get("low") is not None):
                _add(issues, "invalid_market_data", f"{path}.rolling_windows[{j}]", "insufficient rolling history must be null")
    if market_order and [o.get("bar_id") for o in observations if isinstance(o, Mapping)] != market_order:
        _add(issues, "related_artifact_mismatch", "$.observations", "observation order does not match market bars")


def _load_kb(knowledge_dir: str | Path) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]] | None:
    root = Path(knowledge_dir)
    if (root / "data").is_dir() and not (root / "manifest.json").exists():
        root = root / "data"
    try:
        values = []
        for name in ("manifest.json", "chunks.json", "pa_records.json"):
            with (root / name).open("r", encoding="utf-8") as fh:
                values.append(json.load(fh))
        if not all(isinstance(v, dict) for v in values):
            return None
        return values[0], values[1], values[2]
    except Exception:
        return None


def _claims_from_records(records_doc: Mapping[str, Any]) -> dict[tuple[str, str, str], dict[str, Any]]:
    claims: dict[tuple[str, str, str], dict[str, Any]] = {}

    def visit(value: Any, record_id: str, field_name: str) -> None:
        if isinstance(value, dict):
            if isinstance(value.get("claim_id"), str):
                claims[(record_id, field_name, value["claim_id"])] = value
            for key, child in value.items():
                if key not in {"source_refs", "claim_id", "statement", "declaration_type", "rationale"}:
                    visit(child, record_id, key if isinstance(child, list) else field_name)
        elif isinstance(value, list):
            for child in value:
                visit(child, record_id, field_name)

    for record in records_doc.get("records", []) if isinstance(records_doc.get("records"), list) else []:
        if isinstance(record, Mapping) and isinstance(record.get("id"), str):
            rid = record["id"]
            for field_name, value in record.items():
                if field_name != "id":
                    visit(value, rid, field_name)
    return claims


def _validate_kb_lineage(document: Mapping[str, Any], knowledge_dir: str | Path, issues: MutableSequence[ValidationIssue]) -> None:
    loaded = _load_kb(knowledge_dir)
    if loaded is None:
        _add(issues, "grounding_failure", "$.knowledge_dir", "knowledge snapshot files cannot be read")
        return
    manifest, chunks_doc, records_doc = loaded
    summary = manifest.get("processing_summary")
    if isinstance(summary, Mapping) and summary.get("failed", 0):
        _add(issues, "grounding_failure", "$.knowledge_dir/manifest.json", "knowledge manifest is not successful")
    for i, source in enumerate(manifest.get("documents", []) if isinstance(manifest.get("documents"), list) else []):
        if not isinstance(source, Mapping):
            continue
        path = f"$.knowledge_dir/manifest.json.documents[{i}]"
        if source.get("status") != "success":
            _add(issues, "grounding_failure", path, "source is not successful")
        if source.get("source_reverification_required") or source.get("source_reverification_status") in {"pending", "inactive", "reverification_required"}:
            _add(issues, "grounding_failure", path, "source requires reverification")
        if source.get("source_reverified_sha256") and source.get("source_reverified_sha256") != source.get("sha256"):
            _add(issues, "grounding_failure", path, "reverified source SHA does not match manifest SHA")
    sources = {s.get("source_id"): s for s in manifest.get("documents", []) if isinstance(s, Mapping) and s.get("source_id")}
    chunks = {c.get("chunk_id"): c for c in chunks_doc.get("chunks", []) if isinstance(c, Mapping) and c.get("chunk_id")}
    for chunk_id, chunk in chunks.items():
        source = sources.get(chunk.get("source_id"))
        if source is None:
            _add(issues, "grounding_failure", "$.knowledge_dir/chunks.json." + str(chunk_id), "chunk source SHA does not match manifest")
            continue
        for field_name, source_field in (("source_id", "source_id"), ("source_file", "source_file"), ("source_sha256", "sha256")):
            if chunk.get(field_name) != source.get(source_field):
                _add(issues, "grounding_failure", "$.knowledge_dir/chunks.json." + str(chunk_id) + "." + field_name, "chunk source field does not match manifest")
        chunk_page_start, chunk_page_end = chunk.get("page_start"), chunk.get("page_end")
        if not isinstance(chunk_page_start, int) or isinstance(chunk_page_start, bool) or not isinstance(chunk_page_end, int) or isinstance(chunk_page_end, bool) or chunk_page_start > chunk_page_end:
            _add(issues, "grounding_failure", "$.knowledge_dir/chunks.json." + str(chunk_id), "chunk page range is invalid")
        if chunk.get("page_range") != [chunk_page_start, chunk_page_end]:
            _add(issues, "grounding_failure", "$.knowledge_dir/chunks.json." + str(chunk_id) + ".page_range", "chunk page_range must match page_start/page_end")
        page_count = source.get("page_count")
        if isinstance(page_count, int) and ((chunk_page_start or 0) < 1 or (chunk_page_end or 0) > page_count):
            _add(issues, "grounding_failure", "$.knowledge_dir/chunks.json." + str(chunk_id), "chunk page range is outside source")
        if text_sha256(chunk.get("source_text", "")) != chunk.get("source_text_sha256"):
            _add(issues, "grounding_failure", "$.knowledge_dir/chunks.json." + str(chunk_id), "chunk text SHA does not match")
    claims = _claims_from_records(records_doc)
    if document.get("artifact_kind") == "knowledge_citation":
        evidence_items = []
        citation_documents = [document]
    else:
        evidence_items = document.get("evidence_items", []) if isinstance(document.get("evidence_items"), list) else []
        citation_documents = document.get("citations", []) if isinstance(document.get("citations"), list) else []
    citations = {c.get("citation_id"): c for c in citation_documents if isinstance(c, Mapping) and c.get("citation_id")}
    if len(citations) != len(citation_documents):
        _add(issues, "grounding_failure", "$.citations", "citation IDs must be unique")
    for i, citation in enumerate(citations.values()):
        path = f"$.citations[{i}]"
        # A local chunk citation is a physical snapshot reference.  Supplying
        # an external URL alongside it creates mixed provenance even when the
        # schema accepts the nullable field, so reject it semantically in both
        # standalone citations and evidence packages.
        if citation.get("citation_kind") == "local_chunk" and "external_url" in citation:
            _add(issues, "grounding_failure", path, "local citation cannot carry external_url")
        if citation.get("citation_kind") != "local_chunk":
            if citation.get("declaration_type") == "source_assertion":
                _add(issues, "grounding_failure", path, "source assertion cannot use external citation")
            continue
        source = sources.get(citation.get("source_id"))
        chunk = chunks.get(citation.get("chunk_id"))
        if source is None or chunk is None:
            _add(issues, "grounding_failure", path, "citation source or chunk cannot be resolved")
            continue
        if not chunk.get("is_retrieval_chunk"):
            _add(issues, "grounding_failure", path, "citation chunk is not a retrieval chunk")
        if text_sha256(chunk.get("source_text", "")) != chunk.get("source_text_sha256") or citation.get("chunk_text_sha256") != chunk.get("source_text_sha256"):
            _add(issues, "grounding_failure", path, "citation chunk text SHA does not match")
        for field_name in ("source_id", "source_file", "source_sha256", "chunk_id", "page_start", "page_end", "title_path", "source_locator", "provenance_status"):
            if citation.get(field_name) != chunk.get(field_name):
                _add(issues, "grounding_failure", path + "." + field_name, "citation provenance does not match snapshot")
        if citation.get("source_file") != source.get("source_file") or citation.get("source_sha256") != source.get("sha256") or citation.get("source_id") != source.get("source_id"):
            _add(issues, "grounding_failure", path, "citation source fields do not match chunk and manifest")
        page_count = source.get("page_count")
        page_start, page_end = citation.get("page_start"), citation.get("page_end")
        if not isinstance(page_start, int) or not isinstance(page_end, int) or page_start > page_end:
            _add(issues, "grounding_failure", path, "citation page range is invalid")
        if isinstance(page_count, int) and ((page_start or 0) < 1 or (page_end or 0) > page_count):
            _add(issues, "grounding_failure", path, "citation page range is outside source")
        pages = citation.get("pages")
        if pages is not None:
            if not isinstance(pages, list) or any(not isinstance(page, int) or page < (page_start or 1) or page > (page_end or 0) for page in pages):
                _add(issues, "grounding_failure", path + ".pages", "citation pages must lie within page range")

    def ref_matches(citation: Mapping[str, Any], ref: Mapping[str, Any]) -> bool:
        ref_is_external = ref.get("citation_kind") == "external" or bool(ref.get("external_url") or ref.get("url"))
        if citation.get("citation_kind") == "external":
            if not ref_is_external:
                return False
            expected_url = ref.get("external_url") or ref.get("url")
            return citation.get("external_url") == expected_url and citation.get("claim_id") == ref.get("claim_id", citation.get("claim_id"))
        if ref_is_external or citation.get("citation_kind") != "local_chunk":
            return False
        chunk = chunks.get(citation.get("chunk_id"))
        if chunk is None or not chunk.get("is_retrieval_chunk"):
            return False
        # A claim-level ref may omit the derived chunk text SHA; it is bound by
        # the immutable chunk record in that case.
        expected = {
            "source_id": ref.get("source_id"),
            "source_file": ref.get("source_file"),
            "source_sha256": ref.get("source_sha256"),
            "chunk_id": ref.get("chunk_id"),
            "page_start": ref.get("page_start"),
            "page_end": ref.get("page_end"),
            "title_path": ref.get("title_path"),
            "source_locator": ref.get("source_locator"),
            "provenance_status": ref.get("provenance_status"),
        }
        for key, expected_value in expected.items():
            if expected_value is not None and citation.get(key) != expected_value:
                return False
        return citation.get("chunk_text_sha256") == chunk.get("source_text_sha256")

    # A standalone citation is still grounded against the exact claim-level
    # source refs; it is not exempt merely because it is not wrapped in a
    # knowledge evidence package.
    if document.get("artifact_kind") == "knowledge_citation" and document.get("citation_kind"):
        standalone_claim = next((claim for claim in claims.values() if claim.get("claim_id") == document.get("claim_id")), None)
        if standalone_claim is None:
            _add(issues, "grounding_failure", "$.claim_id", "citation claim cannot be resolved")
        else:
            if standalone_claim.get("declaration_type") != document.get("declaration_type"):
                _add(issues, "grounding_failure", "$.declaration_type", "citation declaration does not match claim")
            refs = standalone_claim.get("source_refs") if isinstance(standalone_claim.get("source_refs"), list) else []
            if not any(ref_matches(document, ref) for ref in refs if isinstance(ref, Mapping)):
                _add(issues, "grounding_failure", "$.citation_id", "citation is not one of the claim-level source refs")
    for i, item in enumerate(evidence_items):
        if not isinstance(item, Mapping):
            continue
        path = f"$.evidence_items[{i}]"
        expected_sha = text_sha256(item.get("statement", ""))
        if item.get("statement_sha256") != expected_sha:
            _add(issues, "grounding_failure", path + ".statement_sha256", "statement SHA does not match")
        expected_evi = evidence_id_for(document.get("knowledge_snapshot_id"), item.get("record_id"), item.get("field"), item.get("claim_id"), item.get("declaration_type"), item.get("statement_sha256"), item.get("citation_ids"))
        if item.get("evidence_id") != expected_evi:
            _add(issues, "grounding_failure", path + ".evidence_id", "evidence ID does not match helper identity")
        claim = claims.get((item.get("record_id"), item.get("field"), item.get("claim_id")))
        if claim is None or any(item.get(k) != claim.get(k) for k in ("statement", "declaration_type", "rationale")):
            _add(issues, "grounding_failure", path, "evidence does not match exact record claim")
            continue
        refs = claim.get("source_refs") if isinstance(claim.get("source_refs"), list) else []
        if not refs:
            _add(issues, "grounding_failure", path, "claim has no claim-level source refs")
        for citation_id_value in item.get("citation_ids", []) if isinstance(item.get("citation_ids"), list) else []:
            citation = citations.get(citation_id_value)
            if citation is None:
                _add(issues, "grounding_failure", path, "citation is unresolved")
                continue
            if citation.get("claim_id") != item.get("claim_id") or citation.get("declaration_type") != item.get("declaration_type"):
                _add(issues, "grounding_failure", path, "citation claim/declaration does not match item")
            if item.get("declaration_type") == "source_assertion" and citation.get("citation_kind") != "local_chunk":
                _add(issues, "grounding_failure", path, "source assertion citation must be local")
            if not any(ref_matches(citation, ref) for ref in refs if isinstance(ref, Mapping)):
                _add(issues, "grounding_failure", path, "citation is not one of the exact claim-level source refs")


def _validate_model(document: Mapping[str, Any], related: Mapping[str, Mapping[str, Any]], issues: MutableSequence[ValidationIssue]) -> None:
    kind = document.get("artifact_kind")
    if kind == "normalized_model_request":
        payload = {k: document.get(k) for k in ("prompt_template_version", "instructions", "context_text")}
        expected = hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")).hexdigest()
        if document.get("prompt_hash") != expected:
            _add(issues, "related_artifact_mismatch", "$.prompt_hash", "prompt hash does not match normalized request text")
        _check_id_ref(document.get("context_refs", {}), related, "analysis_request_id", ("analysis_request",), issues, "$.context_refs")
        _check_hash_ref(document.get("context_refs", {}), related, "analysis_request_hash", ("analysis_request",), issues, "$.context_refs")
        _check_id_ref(document.get("context_refs", {}), related, "market_snapshot_id", ("market_snapshot",), issues, "$.context_refs")
        _check_hash_ref(document.get("context_refs", {}), related, "market_snapshot_hash", ("market_snapshot",), issues, "$.context_refs")
        _check_id_ref(document.get("context_refs", {}), related, "observation_frame_id", ("market_observation_frame",), issues, "$.context_refs")
        _check_hash_ref(document.get("context_refs", {}), related, "observation_frame_hash", ("market_observation_frame",), issues, "$.context_refs")
        _check_id_ref(document.get("context_refs", {}), related, "evidence_package_id", ("knowledge_evidence_package",), issues, "$.context_refs")
        _check_hash_ref(document.get("context_refs", {}), related, "evidence_package_hash", ("knowledge_evidence_package",), issues, "$.context_refs")
    elif kind == "normalized_model_response":
        request = _first(related, "normalized_model_request")
        if request is not None and document.get("model_request_id") != request.get("model_request_id"):
            _add(issues, "related_artifact_mismatch", "$.model_request_id", "model response request ID mismatch")
        status, candidate, error = document.get("status"), document.get("candidate"), document.get("error")
        if status == "completed" and (candidate is None or error is not None):
            _add(issues, "status_invariant", "$", "completed response must have a candidate and no error")
        if status in {"error", "unsupported"} and (candidate is not None or error is None):
            _add(issues, "status_invariant", "$", "terminal response must have an error and no candidate")
    elif kind == "model_call_metadata":
        request = _first(related, "normalized_model_request")
        response = _first(related, "normalized_model_response")
        if request is not None and (document.get("model_request_id") != request.get("model_request_id") or document.get("model_request_hash") != request.get("content_hash")):
            _add(issues, "related_artifact_mismatch", "$.model_request_id", "model call request lineage mismatch")
        if response is not None and (document.get("model_response_id") != response.get("model_response_id") or document.get("model_response_hash") != response.get("content_hash")):
            _add(issues, "related_artifact_mismatch", "$.model_response_id", "model call response lineage mismatch")
        for related_doc in (request, response):
            if related_doc is not None:
                for field_name in ("provider", "model", "adapter"):
                    if document.get(field_name) != related_doc.get(field_name):
                        _add(issues, "related_artifact_mismatch", "$." + field_name, "model call metadata label mismatch")
        usage = document.get("usage")
        if isinstance(usage, Mapping) and usage.get("total_tokens") != usage.get("prompt_tokens", 0) + usage.get("completion_tokens", 0):
            _add(issues, "status_invariant", "$.usage.total_tokens", "usage total must equal prompt plus completion")


def _validate_result(document: Mapping[str, Any], related: Mapping[str, Mapping[str, Any]], issues: MutableSequence[ValidationIssue]) -> None:
    # Top-level lineage must bind to every supplied immutable package.
    lineage_pairs = (
        ("analysis_request_id", "analysis_request_hash", ("analysis_request",)),
        ("market_snapshot_id", "market_snapshot_hash", ("market_snapshot",)),
        ("observation_frame_id", "observation_frame_hash", ("market_observation_frame",)),
        ("evidence_package_id", "evidence_package_hash", ("knowledge_evidence_package",)),
        ("model_request_id", "model_request_hash", ("normalized_model_request",)),
        ("model_response_id", "model_response_hash", ("normalized_model_response",)),
        ("model_call_id", "model_call_hash", ("model_call_metadata",)),
    )
    for id_field, hash_field, kinds in lineage_pairs:
        kind = next((candidate for candidate in kinds if candidate in related), None)
        other = related.get(kind) if kind else None
        if other is None:
            continue
        target_id = _related_id(kind, other)
        # Nullable pre-provider result branches intentionally leave stage
        # lineage absent.  Compare only fields the result actually supplies;
        # a supplied non-null ID/hash must still match its related artifact.
        if document.get(id_field) is not None and target_id is not None and document.get(id_field) != target_id:
            _add(issues, "grounding_failure", "$." + id_field, "result lineage ID does not match related artifact")
        if document.get(hash_field) is not None and document.get(hash_field) != other.get("content_hash"):
            _add(issues, "grounding_failure", "$." + hash_field, "result lineage hash does not match related artifact")

    evidence_package = _first(related, "knowledge_evidence_package")
    observations = _first(related, "market_observation_frame")
    market = _first(related, "market_snapshot")
    # Actionable result statuses are only meaningful when all upstream stages
    # are present.  Pre-provider abstain/error results are intentionally
    # nullable and therefore do not enter this requirement unless they carry
    # actionable content themselves.
    if (document.get("status") in {"signal", "conditional", "no_trade"} or any(document.get(name) for name in ("setups", "conditions", "levels"))):
        required = (
            ("market_snapshot", "market"),
            ("market_observation_frame", "observation"),
            ("knowledge_evidence_package", "evidence"),
            ("analysis_request", "analysis request"),
            ("normalized_model_request", "normalized model request"),
            ("normalized_model_response", "normalized model response"),
        )
        for required_kind, label in required:
            if required_kind not in related:
                _add(issues, "grounding_failure", "$", f"actionable result requires related {label} artifact")
    if (document.get("model_call_id") is not None or document.get("model_call_hash") is not None) and "model_call_metadata" not in related:
        _add(issues, "grounding_failure", "$", "result model call lineage requires related model call metadata")
    if evidence_package is not None:
        _validate_snapshot_binding(evidence_package, issues)
    evidence_by_id = {e.get("evidence_id"): e for e in (evidence_package or {}).get("evidence_items", []) if isinstance(e, Mapping)}
    citation_by_id = {c.get("citation_id"): c for c in (evidence_package or {}).get("citations", []) if isinstance(c, Mapping)}
    market_refs: set[str] = set()
    if market:
        market_refs.update(b.get("bar_id") for b in market.get("bars", []) if isinstance(b, Mapping))
    if observations:
        market_refs.update(o.get("observation_id") for o in observations.get("observations", []) if isinstance(o, Mapping))

    def check_grounded(value: Any, path: str, allow_source_not_stated: bool = False) -> None:
        if not isinstance(value, Mapping):
            return
        support = value.get("support_type") or value.get("declaration_type")
        if support == "source_not_stated" and not allow_source_not_stated:
            _add(issues, "grounding_failure", path, "source_not_stated is only allowed on a rationale")
        eids = value.get("evidence_ids", []) if isinstance(value.get("evidence_ids"), list) else []
        cids = value.get("citation_ids", []) if isinstance(value.get("citation_ids"), list) else []
        claim_ids = value.get("claim_ids", []) if isinstance(value.get("claim_ids"), list) else []
        refs = value.get("market_refs", []) if isinstance(value.get("market_refs"), list) else []
        referenced = []
        for eid in eids:
            evidence = evidence_by_id.get(eid)
            if evidence is None:
                _add(issues, "grounding_failure", path + ".evidence_ids", "unknown evidence ID")
            else:
                referenced.append(evidence)
        for cid in cids:
            if cid not in citation_by_id:
                _add(issues, "grounding_failure", path + ".citation_ids", "unknown citation ID")
        if market or observations:
            for ref in refs:
                if (str(ref).startswith("bar_") or str(ref).startswith("obv_")) and ref not in market_refs:
                    _add(issues, "grounding_failure", path + ".market_refs", "unknown market reference")
        if support == "model_interpretation" and (eids or cids or claim_ids):
            _add(issues, "grounding_failure", path, "model interpretation cannot claim knowledge references")
        if support == "market_observation" and (eids or cids or claim_ids):
            _add(issues, "grounding_failure", path, "market observation cannot claim knowledge references")
        if support in {"source_assertion", "derived_interpretation", "source_not_stated"}:
            if evidence_package is not None and not eids:
                _add(issues, "grounding_failure", path, "knowledge-backed statement has no evidence")
            declarations = {e.get("declaration_type") for e in referenced}
            if declarations and declarations != {support}:
                _add(issues, "grounding_failure", path, "declaration type does not match referenced evidence")
            bound_claims = {e.get("claim_id") for e in referenced}
            bound_citations = {cid for e in referenced for cid in (e.get("citation_ids", []) if isinstance(e.get("citation_ids"), list) else [])}
            if any(claim_id not in bound_claims for claim_id in claim_ids):
                _add(issues, "grounding_failure", path + ".claim_ids", "claim is not bound by a referenced evidence item")
            if any(cid not in bound_citations for cid in cids):
                _add(issues, "grounding_failure", path + ".citation_ids", "citation is not bound by a referenced evidence item")
            for evidence in referenced:
                for cid in evidence.get("citation_ids", []) if isinstance(evidence.get("citation_ids"), list) else []:
                    citation = citation_by_id.get(cid)
                    if citation is None:
                        _add(issues, "grounding_failure", path, "referenced evidence contains unknown citation")
                    elif citation.get("claim_id") != evidence.get("claim_id") or citation.get("declaration_type") != evidence.get("declaration_type"):
                        _add(issues, "grounding_failure", path, "citation claim/declaration does not match referenced evidence")

    for name in ("setups", "conditions", "levels"):
        values = document.get(name)
        if isinstance(values, list):
            for i, value in enumerate(values):
                check_grounded(value, f"$.{name}[{i}]")
    rationales = document.get("rationales")
    if isinstance(rationales, list):
        for i, value in enumerate(rationales):
            check_grounded(value, f"$.rationales[{i}]", allow_source_not_stated=True)
            if isinstance(value, Mapping) and value.get("support_type") == "market_observation" and value.get("claim_ids"):
                _add(issues, "grounding_failure", f"$.rationales[{i}]", "market observation rationale cannot carry knowledge claims")
    status, action = document.get("status"), document.get("action")
    levels = document.get("levels")
    if status == "signal" and action not in {"buy", "sell"}:
        _add(issues, "status_invariant", "$.action", "signal requires buy or sell")
    if status == "conditional" and (action != "wait" or not document.get("conditions")):
        _add(issues, "status_invariant", "$.action", "conditional requires wait and conditions")
    if status in {"no_trade", "abstain", "error"} and (action != "none" or levels != []):
        _add(issues, "status_invariant", "$", "non-actionable status cannot contain trade levels")
    if status in {"abstain", "error"} and not document.get("errors"):
        _add(issues, "status_invariant", "$.errors", "abstain/error requires an error")


def _event_projection(document: Mapping[str, Any]) -> dict[str, Any]:
    volatile = {"decision_event_id", "outcome_event_id", "content_hash", "idempotency_key", "recorded_at"}
    return {k: copy.deepcopy(v) for k, v in document.items() if k not in volatile}


def _validate_event(document: Mapping[str, Any], related: Mapping[str, Mapping[str, Any]], issues: MutableSequence[ValidationIssue]) -> None:
    kind = document.get("artifact_kind")
    if document.get("mode") != document.get("stream"):
        _add(issues, "status_invariant", "$.stream", "mode and stream must match")
    origin, synthetic = document.get("data_origin"), document.get("synthetic")
    if origin == "synthetic_fixture" and synthetic is not True:
        _add(issues, "status_invariant", "$.synthetic", "synthetic fixture must be marked synthetic")
    if origin == "live_market" and synthetic is not False:
        _add(issues, "status_invariant", "$.synthetic", "live market data cannot be marked synthetic")
    if document.get("mode") == "forward_paper" and origin == "historical_fixture":
        _add(issues, "status_invariant", "$.data_origin", "forward_paper cannot use historical_fixture")
    if document.get("mode") == "paper_replay" and origin == "live_market":
        _add(issues, "status_invariant", "$.data_origin", "paper_replay cannot use live_market")
    if origin == "historical_fixture" and synthetic is not False:
        _add(issues, "status_invariant", "$.synthetic", "historical fixture must not be marked synthetic")
    expected_idem = idempotency_key_for(_event_projection(document))
    if document.get("idempotency_key") != expected_idem:
        _add(issues, "status_invariant", "$.idempotency_key", "idempotency key does not match stable event projection")
    if kind == "analysis_run_decision_event":
        status, action = document.get("final_status"), document.get("action")
        if status == "signal" and action not in {"buy", "sell"} or status == "conditional" and action != "wait" or status in {"no_trade", "abstain", "error"} and action != "none":
            _add(issues, "status_invariant", "$", "decision status/action invariant violated")
        lineage = document.get("lineage", {})
        if not isinstance(lineage, Mapping):
            lineage = {}
        _validate_snapshot_binding(lineage, issues)

        def compare_lineage(field: str, target: Any) -> None:
            if target is not None and lineage.get(field) != target:
                _add(issues, "grounding_failure", "$.lineage." + field, "decision lineage does not match related artifact")

        market = related.get("market_snapshot")
        if market is not None:
            compare_lineage("market_snapshot_id", market.get("market_snapshot_id"))
            compare_lineage("market_snapshot_hash", market.get("content_hash"))
            compare_lineage("market_quality", market.get("quality"))
            # The event's root context is part of the caller's market
            # projection, not merely display metadata.  Bind it to the
            # supplied market snapshot so a re-rooted event cannot reuse the
            # same lineage hash with a different instrument or as-of time.
            for field_name in ("instrument", "timeframe", "session", "input_mode"):
                target = market.get(field_name)
                if target is not None and document.get(field_name) != target:
                    _add(issues, "grounding_failure", "$." + field_name, "decision root context does not match related market snapshot")
            if market.get("analysis_as_of") is not None and document.get("decision_as_of") != market.get("analysis_as_of"):
                _add(issues, "grounding_failure", "$.decision_as_of", "decision as-of does not match related market snapshot")
        observation = related.get("market_observation_frame")
        if observation is not None:
            compare_lineage("observation_frame_id", observation.get("observation_frame_id"))
            compare_lineage("observation_frame_hash", observation.get("content_hash"))
        evidence = related.get("knowledge_evidence_package")
        if evidence is not None:
            compare_lineage("evidence_package_id", evidence.get("evidence_package_id"))
            compare_lineage("evidence_package_hash", evidence.get("content_hash"))
            compare_lineage("knowledge_snapshot_id", evidence.get("knowledge_snapshot_id"))
            compare_lineage("knowledge_snapshot_version", evidence.get("knowledge_snapshot_version"))
            compare_lineage("knowledge_snapshot_hash", evidence.get("knowledge_snapshot_hash"))
        request = related.get("analysis_request")
        if request is not None:
            compare_lineage("analysis_request_id", request.get("analysis_request_id"))
            compare_lineage("analysis_request_hash", request.get("content_hash"))
            for field_name in ("input_mode",):
                target = request.get(field_name)
                if target is not None and document.get(field_name) != target:
                    _add(issues, "grounding_failure", "$." + field_name, "decision root context does not match related analysis request")
            if request.get("analysis_as_of") is not None and document.get("decision_as_of") != request.get("analysis_as_of"):
                _add(issues, "grounding_failure", "$.decision_as_of", "decision as-of does not match related analysis request")
            for field_name in ("instrument", "timeframe", "session"):
                target = request.get(field_name)
                if target is not None and document.get(field_name) != target:
                    _add(issues, "grounding_failure", "$." + field_name, "decision root context does not match related analysis request")
            for field_name in ("knowledge_snapshot_id", "knowledge_snapshot_hash"):
                target = request.get(field_name)
                if target is not None and lineage.get(field_name) != target:
                    _add(issues, "grounding_failure", "$.lineage." + field_name, "decision knowledge snapshot pin does not match related analysis request")
        response = related.get("normalized_model_response")
        if response is not None:
            compare_lineage("model_response_id", response.get("model_response_id"))
            compare_lineage("model_response_hash", response.get("content_hash"))
            compare_lineage("provider", response.get("provider"))
            compare_lineage("model", response.get("model"))
        result = related.get("pa_analysis_result")
        if result is not None:
            for field_name, result_field in {
                "analysis_result_id": "pa_analysis_result_id", "analysis_result_hash": "content_hash",
                "engine_version": "engine_version", "prompt_template_version": "prompt_template_version",
                "policy_version": "policy_version", "adapter_version": "adapter_version",
            }.items():
                compare_lineage(field_name, result.get(result_field))
            for event_field, result_field in (
                ("final_status", "status"),
                ("action", "action"),
                ("decision_as_of", "analysis_as_of"),
                ("input_mode", "input_mode"),
                ("no_live_orders", "no_live_orders"),
            ):
                target = result.get(result_field)
                if target is not None and document.get(event_field) != target:
                    _add(issues, "grounding_failure", "$." + event_field, "decision field does not match related analysis result")
    else:
        decision = _first(related, "analysis_run_decision_event")
        if decision is not None:
            for key in ("decision_event_id", "decision_event_hash", "mode", "stream", "data_origin", "synthetic"):
                target_key = "decision_event_id" if key == "decision_event_id" else "content_hash" if key == "decision_event_hash" else key
                if document.get(key) != decision.get(target_key):
                    _add(issues, "grounding_failure", "$." + key, "outcome decision lineage mismatch")
            outcome_as_of = _parse_utc(document.get("outcome_as_of"))
            decision_as_of = _parse_utc(decision.get("decision_as_of"))
            if outcome_as_of is not None and decision_as_of is not None and outcome_as_of < decision_as_of:
                _add(issues, "status_invariant", "$.outcome_as_of", "outcome cannot precede decision")
        market = related.get("market_snapshot")
        if market is not None:
            for field_name, market_field in (("market_snapshot_id", "market_snapshot_id"), ("market_snapshot_hash", "content_hash")):
                target = market.get(market_field)
                if target is not None and document.get(field_name) != target:
                    _add(issues, "grounding_failure", "$." + field_name, "outcome market snapshot does not match related market snapshot")


REQUIRED_POLICY_OUTCOMES: dict[str, dict[str, Any]] = {
    "malformed_ohlcv": {"stage": "market", "disposition": "terminal", "status": "error", "code": "invalid_market_data", "model_call_allowed": False, "warning_codes": [], "uncertainty": None, "exclude_incomplete_last_bar": False},
    "ohlc_invariant": {"stage": "market", "disposition": "terminal", "status": "error", "code": "invalid_market_data", "model_call_allowed": False, "warning_codes": [], "uncertainty": None, "exclude_incomplete_last_bar": False},
    "duplicate_bars": {"stage": "market", "disposition": "terminal", "status": "error", "code": "invalid_market_data", "model_call_allowed": False, "warning_codes": [], "uncertainty": None, "exclude_incomplete_last_bar": False},
    "out_of_order_bars": {"stage": "market", "disposition": "terminal", "status": "error", "code": "invalid_market_data", "model_call_allowed": False, "warning_codes": [], "uncertainty": None, "exclude_incomplete_last_bar": False},
    "future_bars": {"stage": "market", "disposition": "terminal", "status": "error", "code": "invalid_market_data", "model_call_allowed": False, "warning_codes": [], "uncertainty": None, "exclude_incomplete_last_bar": False},
    "insufficient_closed_history": {"stage": "market", "disposition": "terminal", "status": "abstain", "code": "insufficient_history", "model_call_allowed": False, "warning_codes": [], "uncertainty": "high", "exclude_incomplete_last_bar": True},
    "partial_coverage_disallowed": {"stage": "market", "disposition": "terminal", "status": "abstain", "code": "partial_data", "model_call_allowed": False, "warning_codes": [], "uncertainty": "high", "exclude_incomplete_last_bar": False},
    "partial_coverage_allowed": {"stage": "market", "disposition": "continue", "status": None, "code": "partial_data", "model_call_allowed": True, "warning_codes": ["partial_data"], "uncertainty": "high", "exclude_incomplete_last_bar": False},
    "stale_beyond_policy": {"stage": "market", "disposition": "terminal", "status": "abstain", "code": "stale_data", "model_call_allowed": False, "warning_codes": [], "uncertainty": "high", "exclude_incomplete_last_bar": False},
    "incomplete_final_bar_sufficient": {"stage": "market", "disposition": "continue", "status": None, "code": "incomplete_last_bar_excluded", "model_call_allowed": True, "warning_codes": ["incomplete_last_bar_excluded"], "uncertainty": None, "exclude_incomplete_last_bar": True},
    "incomplete_final_bar_insufficient": {"stage": "market", "disposition": "terminal", "status": "abstain", "code": "insufficient_history", "model_call_allowed": False, "warning_codes": ["incomplete_last_bar_excluded"], "uncertainty": "high", "exclude_incomplete_last_bar": True},
    "no_actionable_evidence": {"stage": "evidence", "disposition": "terminal", "status": "abstain", "code": "no_actionable_evidence", "model_call_allowed": False, "warning_codes": [], "uncertainty": "high", "exclude_incomplete_last_bar": False},
    "only_source_not_stated": {"stage": "evidence", "disposition": "terminal", "status": "abstain", "code": "no_actionable_evidence", "model_call_allowed": False, "warning_codes": [], "uncertainty": "high", "exclude_incomplete_last_bar": False},
    "unresolved_knowledge_conflict": {"stage": "evidence", "disposition": "terminal", "status": "abstain", "code": "conflicting_evidence", "model_call_allowed": False, "warning_codes": [], "uncertainty": "high", "exclude_incomplete_last_bar": False},
    "provider_timeout": {"stage": "model", "disposition": "terminal", "status": "error", "code": "model_timeout", "model_call_allowed": True, "warning_codes": [], "uncertainty": "high", "exclude_incomplete_last_bar": False},
    "provider_malformed_response": {"stage": "model", "disposition": "terminal", "status": "error", "code": "invalid_model_response", "model_call_allowed": True, "warning_codes": [], "uncertainty": "high", "exclude_incomplete_last_bar": False},
    "forged_lineage": {"stage": "grounding", "disposition": "terminal", "status": "error", "code": "grounding_failure", "model_call_allowed": True, "warning_codes": [], "uncertainty": "high", "exclude_incomplete_last_bar": False},
    "unresolvable_lineage": {"stage": "grounding", "disposition": "terminal", "status": "error", "code": "grounding_failure", "model_call_allowed": True, "warning_codes": [], "uncertainty": "high", "exclude_incomplete_last_bar": False},
}


def _validate_policy(document: Mapping[str, Any], issues: MutableSequence[ValidationIssue]) -> None:
    cases = document.get("cases")
    if not isinstance(cases, list):
        return
    seen: set[str] = set()
    seen_case_ids: set[str] = set()
    seen_precedences: set[int] = set()
    previous_precedence = 0
    for i, case in enumerate(cases):
        if not isinstance(case, Mapping):
            continue
        case_id = case.get("case_id")
        precedence = case.get("precedence")
        if case_id in seen_case_ids:
            _add(issues, "policy_matrix_ambiguous", f"$.cases[{i}].case_id", "duplicate policy case_id")
        seen_case_ids.add(case_id)
        if not isinstance(precedence, int) or isinstance(precedence, bool) or precedence in seen_precedences or precedence <= previous_precedence or precedence != i + 1:
            _add(issues, "policy_matrix_ambiguous", f"$.cases[{i}].precedence", "precedence must be unique and strictly increasing from 1")
        if isinstance(precedence, int) and not isinstance(precedence, bool):
            seen_precedences.add(precedence)
            previous_precedence = precedence
        condition = case.get("condition")
        if condition in seen:
            _add(issues, "policy_matrix_ambiguous", f"$.cases[{i}].condition", "duplicate policy condition")
        seen.add(condition)
        expected = REQUIRED_POLICY_OUTCOMES.get(condition)
        if expected is None:
            _add(issues, "policy_matrix_ambiguous", f"$.cases[{i}].condition", "unexpected policy condition")
            continue
        checks = {
            "stage": expected["stage"],
            "disposition": expected["disposition"],
            "expected_status": expected["status"],
            "code": expected["code"],
            "model_call_allowed": expected["model_call_allowed"],
            "warning_codes": expected["warning_codes"],
            "exclude_incomplete_last_bar": expected["exclude_incomplete_last_bar"],
        }
        for field_name, want in checks.items():
            if case.get(field_name) != want:
                _add(issues, "policy_matrix_ambiguous", f"$.cases[{i}].{field_name}", "policy outcome does not match frozen matrix")
        uncertainty = case.get("uncertainty")
        got_level = uncertainty.get("level") if isinstance(uncertainty, Mapping) else None
        if got_level != expected["uncertainty"]:
            _add(issues, "policy_matrix_ambiguous", f"$.cases[{i}].uncertainty", "policy uncertainty level does not match frozen matrix")
    missing = set(REQUIRED_POLICY_OUTCOMES) - seen
    if missing:
        _add(issues, "policy_matrix_incomplete", "$.cases", "missing required policy conditions: " + ", ".join(sorted(missing)))


def _validate_evidence_basics(document: Mapping[str, Any], issues: MutableSequence[ValidationIssue]) -> None:
    citations = document.get("citations")
    if not isinstance(citations, list):
        return
    seen_citations: set[str] = set()
    citation_by_id: dict[str, Mapping[str, Any]] = {}
    for i, citation in enumerate(citations):
        if not isinstance(citation, Mapping):
            continue
        path = f"$.citations[{i}]"
        if citation.get("citation_kind") == "local_chunk" and "external_url" in citation:
            _add(issues, "grounding_failure", path, "local citation cannot carry external_url")
        if citation.get("citation_kind") == "external" and citation.get("declaration_type") == "source_assertion":
            _add(issues, "grounding_failure", path, "source assertion cannot use external citation")
        cid = citation.get("citation_id")
        if cid in seen_citations:
            _add(issues, "grounding_failure", path, "duplicate citation ID")
        seen_citations.add(cid)
        try:
            expected = citation_id_for(citation)
            if cid != expected or citation.get("content_hash") != content_hash(citation, "knowledge_citation"):
                _add(issues, "hash_mismatch", path, "citation identity/hash mismatch")
        except Exception:
            _add(issues, "hash_mismatch", path, "citation identity cannot be computed")
        if isinstance(cid, str):
            citation_by_id[cid] = citation
    items = document.get("evidence_items")
    if isinstance(items, list):
        seen_items: set[str] = set()
        for i, item in enumerate(items):
            if not isinstance(item, Mapping):
                continue
            path = f"$.evidence_items[{i}]"
            eid = item.get("evidence_id")
            if eid in seen_items:
                _add(issues, "grounding_failure", path, "duplicate evidence ID")
            seen_items.add(eid)
            if item.get("statement_sha256") != text_sha256(item.get("statement", "")):
                _add(issues, "grounding_failure", path + ".statement_sha256", "statement SHA mismatch")
            expected = evidence_id_for(document.get("knowledge_snapshot_id"), item.get("record_id"), item.get("field"), item.get("claim_id"), item.get("declaration_type"), item.get("statement_sha256"), item.get("citation_ids"))
            if eid != expected:
                _add(issues, "grounding_failure", path + ".evidence_id", "evidence ID mismatch")
            citation_ids = item.get("citation_ids", []) if isinstance(item.get("citation_ids"), list) else []
            for cid in citation_ids:
                citation = citation_by_id.get(cid)
                if citation is None:
                    _add(issues, "grounding_failure", path + ".citation_ids", "unknown citation ID")
                    continue
                if citation.get("claim_id") != item.get("claim_id") or citation.get("declaration_type") != item.get("declaration_type"):
                    _add(issues, "grounding_failure", path + ".citation_ids", "citation claim/declaration does not match evidence item")
                if item.get("declaration_type") == "source_assertion" and citation.get("citation_kind") != "local_chunk":
                    _add(issues, "grounding_failure", path + ".citation_ids", "source assertion cannot use external or mixed citations")


def _validate_citation_basics(document: Mapping[str, Any], issues: MutableSequence[ValidationIssue]) -> None:
    """Apply provenance semantics to a standalone citation as well as packages."""

    if document.get("citation_kind") == "local_chunk" and "external_url" in document:
        _add(issues, "grounding_failure", "$.external_url", "local citation cannot carry external_url")
    if document.get("citation_kind") == "external" and document.get("declaration_type") == "source_assertion":
        _add(issues, "grounding_failure", "$.citation_kind", "source assertion cannot use external citation")


def _validate_request(document: Mapping[str, Any], related: Mapping[str, Mapping[str, Any]], issues: MutableSequence[ValidationIssue]) -> None:
    market = _first(related, "market_snapshot")
    if market is not None:
        if document.get("market_snapshot_id") != market.get("market_snapshot_id"):
            _add(issues, "related_artifact_mismatch", "$.market_snapshot_id", "request market snapshot ID mismatch")
        if document.get("market_snapshot_hash") and document.get("market_snapshot_hash") != market.get("content_hash"):
            _add(issues, "related_artifact_mismatch", "$.market_snapshot_hash", "request market snapshot hash mismatch")
        if document.get("analysis_as_of") != market.get("analysis_as_of"):
            _add(issues, "related_artifact_mismatch", "$.analysis_as_of", "request analysis_as_of mismatch")
    policy = _first(related, "analysis_policy")
    if policy is not None and document.get("analysis_policy_id") != policy.get("analysis_policy_id"):
        _add(issues, "related_artifact_mismatch", "$.analysis_policy_id", "request policy ID mismatch")
    evidence = related.get("knowledge_evidence_package")
    if evidence is not None:
        for field in ("knowledge_snapshot_id", "knowledge_snapshot_hash"):
            if document.get(field) != evidence.get(field):
                _add(issues, "related_artifact_mismatch", "$." + field, "request knowledge snapshot lineage mismatch")


def _validate_snapshot_binding(document: Mapping[str, Any], issues: MutableSequence[ValidationIssue]) -> None:
    snapshot_id = document.get("knowledge_snapshot_id")
    snapshot_hash = document.get("knowledge_snapshot_hash")
    if snapshot_id is not None and snapshot_hash is not None and snapshot_id != "kbs_" + str(snapshot_hash):
        _add(issues, "grounding_failure", "$.knowledge_snapshot_id", "knowledge snapshot ID must bind to its hash")


def _dedupe_issues(issues: list[ValidationIssue]) -> list[ValidationIssue]:
    seen: set[tuple[str, str, str]] = set()
    result: list[ValidationIssue] = []
    for issue in issues:
        key = (issue.code, issue.path, issue.message)
        if key not in seen:
            seen.add(key)
            result.append(issue)
    return result


def validate_artifact(
    document: Any,
    expected_kind: str | None = None,
    knowledge_dir: str | Path | None = None,
    related_artifacts: Any = None,
    verify_hash: bool = True,
) -> ValidationResult:
    """Validate one contract artifact and return a JSON-serializable result.

    Checks run in a stable order: secret scan, schema, top identity, semantic
    artifact rules, related-artifact lineage and optional knowledge grounding.
    """

    issues: list[ValidationIssue] = []
    _secret_scan(document, issues)
    _native_scan(document, issues)
    if not isinstance(document, Mapping):
        _add(issues, "invalid_json", "$", "artifact must be a JSON object")
        return ValidationResult(False, expected_kind, _dedupe_issues(issues))
    kind = document.get("artifact_kind")
    if not isinstance(kind, str) or kind not in METADATA:
        _add(issues, "unknown_artifact_kind", "$.artifact_kind", "artifact kind is not registered")
        return ValidationResult(False, kind if isinstance(kind, str) else expected_kind, _dedupe_issues(issues))
    if expected_kind is not None and kind != expected_kind:
        _add(issues, "unknown_artifact_kind", "$.artifact_kind", "artifact kind does not match expected kind")
    _timestamp_scan(document, issues)
    _decimal_scan(document, issues)
    _schema_validate(document, kind, issues)
    _validate_snapshot_binding(document, issues)
    if verify_hash:
        try:
            digest = content_hash(document, kind)
            meta = METADATA[kind]
            hash_field, id_field = meta["content_hash_field"].lstrip("/"), meta["id_field"].lstrip("/")
            if document.get(hash_field) != digest:
                _add(issues, "hash_mismatch", "$." + hash_field, "content hash does not match semantic projection")
            if document.get(id_field) != meta["id_prefix"] + digest:
                _add(issues, "hash_mismatch", "$." + id_field, "top-level identity does not match semantic projection")
        except (CanonicalizationError, TypeError, ValueError):
            _add(issues, "hash_mismatch", "$", "artifact identity cannot be computed")
    related = _related_map(related_artifacts)
    if kind == "market_snapshot":
        _validate_market(document, issues)
    elif kind == "market_observation_frame":
        _validate_observation(document, related, issues)
    elif kind == "knowledge_evidence_package":
        _validate_evidence_basics(document, issues)
        if knowledge_dir is not None:
            _validate_kb_lineage(document, knowledge_dir, issues)
    elif kind == "knowledge_citation":
        _validate_citation_basics(document, issues)
        if knowledge_dir is not None:
            _validate_kb_lineage(document, knowledge_dir, issues)
    elif kind in {"normalized_model_request", "normalized_model_response", "model_call_metadata"}:
        _validate_model(document, related, issues)
    elif kind == "pa_analysis_result":
        _validate_result(document, related, issues)
    elif kind in {"analysis_run_decision_event", "analysis_run_outcome_event"}:
        _validate_event(document, related, issues)
    elif kind == "policy_outcome_matrix":
        _validate_policy(document, issues)
    elif kind == "analysis_request":
        _validate_request(document, related, issues)
    if knowledge_dir is not None and kind != "knowledge_evidence_package":
        related_evidence = related.get("knowledge_evidence_package")
        if related_evidence is not None:
            _validate_kb_lineage(related_evidence, knowledge_dir, issues)
    return ValidationResult(not _dedupe_issues(issues), kind, _dedupe_issues(issues))


__all__ = [
    "REQUIRED_POLICY_OUTCOMES",
    "ValidationIssue",
    "ValidationResult",
    "validate_artifact",
]
