#!/usr/bin/env python3
"""Small, repeatable PA knowledge-base builder.

The builder keeps two layers:
1. source-faithful page/semantic chunks with provenance; and
2. curated normalized PA records that point back to those chunks.

It deliberately rebuilds the small corpus instead of maintaining a dependency
graph. A malformed source is recorded and does not stop the other documents.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

try:
    import pypdf
    from pypdf import PdfReader
    PYPDF_VERSION = getattr(pypdf, "__version__", "unknown")
    PYPDF_IMPORT_ERROR = None
except Exception as exc:  # pragma: no cover - the runtime check reports this cleanly
    PdfReader = None  # type: ignore[assignment,misc]
    PYPDF_VERSION = None
    PYPDF_IMPORT_ERROR = f"{type(exc).__name__}: {exc}"

try:
    import jsonschema
    try:
        JSONSCHEMA_VERSION = importlib.metadata.version("jsonschema")
    except importlib.metadata.PackageNotFoundError:
        JSONSCHEMA_VERSION = "unknown"
    JSONSCHEMA_IMPORT_ERROR = None
except Exception as exc:  # pragma: no cover - runtime dependency check reports this
    jsonschema = None  # type: ignore[assignment]
    JSONSCHEMA_VERSION = None
    JSONSCHEMA_IMPORT_ERROR = f"{type(exc).__name__}: {exc}"


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE_DIR = ROOT / "策略"
DEFAULT_OUTPUT_DIR = ROOT / "knowledge" / "data"
CONFIG_DIR = ROOT / "knowledge" / "config"
SCHEMA_DIR = ROOT / "knowledge" / "schemas"
KB_VERSION = "0.1.0"
PYTHON_MIN_VERSION = (3, 10)
OCR_LANGUAGES = ("eng", "chi_sim")

# Exit codes are part of the public CLI contract.  A warning means that a
# usable snapshot was committed but at least one source was partial/failed.
EXIT_SUCCESS = 0
EXIT_WARNING = 1
EXIT_USAGE = 2
EXIT_MISSING_DEPENDENCY = 3
EXIT_QUALITY_FAILURE = 4
EXIT_RUNTIME_FAILURE = 5


class CliError(Exception):
    """An expected CLI failure with a stable process exit code."""

    def __init__(self, message: str, exit_code: int, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.exit_code = exit_code
        self.details = details or {}

DOC_ID_BY_FILENAME = {
    "Rose的策略.pdf": "rose-strategy",
    "价格行为学 - Rose 的 Caveman (野人) 交易策略 - forecho's Blog.pdf": "rose-caveman",
    "价格行为学 - Rose 的 Gap Bar - forecho's Blog.pdf": "rose-gap-bar",
    "价格行为学 - Rose 的 MTR (主趋势反转) - forecho's Blog.pdf": "rose-mtr",
    "价格行为学 - Rose 的 MTR (主趋势反转) - forecho's Blog 2.pdf": "rose-mtr",
    "价格行为学 - Rose 的 Mother Bar (孕线) 策略 - forecho's Blog.pdf": "rose-mother-bar",
    "（公开版）PA日内交易setup清单-南桥价格行为.pdf": "nanqiao-checklist",
}

RECORD_SOURCE_PREFIXES = {
    "rose-zone-": "rose-strategy",
    "rose-three-": "rose-strategy",
    "rose-sentiment-": "rose-strategy",
    "rose-mother-bar-scalp": "rose-strategy",
    "rose-fofs-": "rose-strategy",
    "rose-range-": "rose-strategy",
    "rose-flag-": "rose-strategy",
    "rose-missed-": "rose-strategy",
    "rose-caveman": "rose-caveman",
    "rose-gap-bar": "rose-gap-bar",
    "rose-mother-bar": "rose-mother-bar",
    "mtr-": "rose-mtr",
    "nanqiao-": "nanqiao-checklist",
}

REQUIRED_CHUNK_FIELDS = [
    "chunk_id",
    "source_file",
    "source_sha256",
    "page_start",
    "page_end",
    "title_path",
    "parent_chunk_id",
    "adjacent_chunk_ids",
    "source_text",
]

REQUIRED_RECORD_FIELDS = [
    "id",
    "type",
    "name",
    "aliases",
    "tags",
    "source_refs",
    "market_context",
    "direction",
    "timeframes",
    "prerequisites",
    "recognition_features",
    "trigger",
    "entry",
    "invalidation_stop",
    "targets_exit",
    "risk_notes",
    "prohibited",
    "exceptions",
    "examples",
    "interpretation_notes",
    "confidence",
    "knowledge_base_version",
]

NORMALIZED_LOGIC_FIELDS = [
    "market_context",
    "recognition_features",
    "trigger",
    "entry",
    "invalidation_stop",
    "targets_exit",
    "risk_notes",
    "prohibited",
    "exceptions",
    "examples",
]

# Normalized execution fields are deliberately represented as claim objects
# in generated records.  The declaration type is part of the data contract,
# rather than an informal annotation hidden in prose.
CLAIM_FIELDS = [
    *NORMALIZED_LOGIC_FIELDS,
    "prerequisites",
    "interpretation_notes",
]
DECLARATION_TYPES = {
    "source_assertion",
    "derived_interpretation",
    "source_not_stated",
}
ARTIFACT_SCHEMAS = {
    "manifest.json": "manifest-0.1.0.schema.json",
    "chunks.json": "chunks-0.1.0.schema.json",
    "pa_records.json": "pa_records-0.1.0.schema.json",
    "terminology.json": "terminology-0.1.0.schema.json",
    "processing_issues.json": "processing_issues-0.1.0.schema.json",
    "visual_index.json": "visual_index-0.1.0.schema.json",
    "validation/quality_report.json": "quality_report-0.1.0.schema.json",
}
DIAGNOSTIC_SCHEMA_FILENAME = "diagnostic-0.1.0.schema.json"


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def load_configuration() -> tuple[dict[str, dict[str, Any]], dict[str, Any], dict[str, Any]]:
    """Load source-scoped handwritten configuration deterministically.

    Each file under ``knowledge/config/sources`` owns one document and its
    records.  The older aggregate files remain readable for migration and
    backwards compatibility, but are intentionally not the source of truth
    once source-scoped files exist.
    """
    source_dir = CONFIG_DIR / "sources"
    source_files = sorted(source_dir.glob("*.json")) if source_dir.is_dir() else []
    if not source_files:
        # Compatibility fallback for an untouched pre-G006 checkout.  A
        # normal repository build always uses source-scoped files.
        return (
            load_json(CONFIG_DIR / "document_specs.json"),
            load_json(CONFIG_DIR / "pa_records.json"),
            load_json(CONFIG_DIR / "terminology.json"),
        )

    doc_specs: dict[str, dict[str, Any]] = {}
    records: list[dict[str, Any]] = []
    scoped_terms: list[dict[str, Any]] = []
    scoped_conflicts: list[dict[str, Any]] = []
    for path in source_files:
        payload = load_json(path)
        if not isinstance(payload, dict):
            raise ValueError(f"source config must be an object: {path}")
        source_id = payload.get("source_id") or path.stem
        if not isinstance(source_id, str) or not source_id:
            raise ValueError(f"source config has invalid source_id: {path}")
        document = payload.get("document_spec") or payload.get("document") or {}
        if not isinstance(document, dict):
            raise ValueError(f"source config document_spec must be an object: {path}")
        doc_spec = dict(document)
        doc_spec.setdefault("source_id", source_id)
        doc_specs[source_id] = doc_spec
        source_records = payload.get("records", [])
        if not isinstance(source_records, list):
            raise ValueError(f"source config records must be a list: {path}")
        for record in source_records:
            if not isinstance(record, dict):
                raise ValueError(f"source config record must be an object: {path}")
            item = dict(record)
            item["source_id"] = source_id
            records.append(item)
        terminology = payload.get("terminology", {})
        if not isinstance(terminology, dict):
            raise ValueError(f"source config terminology must be an object: {path}")
        for term in terminology.get("terms", []):
            if not isinstance(term, dict):
                raise ValueError(f"source config term must be an object: {path}")
            scoped_terms.append(dict(term))
        for conflict in terminology.get("conflicts", []):
            if not isinstance(conflict, dict):
                raise ValueError(f"source config conflict must be an object: {path}")
            scoped_conflicts.append(dict(conflict))

    # Terminology is aggregated from source-owned sections.  The historical
    # aggregate file is only a fallback when no source-scoped config exists.
    dedupe = lambda rows: list({json.dumps(row, ensure_ascii=False, sort_keys=True): row for row in rows}.values())
    terminology = {"terms": dedupe(scoped_terms), "conflicts": dedupe(scoped_conflicts)}
    return doc_specs, {"records": records}, terminology


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=False) + "\n",
        encoding="utf-8",
    )


def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def normalize_text(value: str) -> str:
    value = unicodedata.normalize("NFKC", value or "")
    value = value.replace("\r\n", "\n").replace("\r", "\n")
    value = value.replace("\u00a0", " ")
    value = re.sub(r"[ \t]+\n", "\n", value)
    value = re.sub(r"\n{3,}", "\n\n", value)
    return value.strip()


def slugify(value: str) -> str:
    original = normalize_text(value)
    value = original.lower()
    value = re.sub(r"[^a-z0-9]+", "-", value).strip("-")
    return value or "source-" + hashlib.sha256(original.encode("utf-8")).hexdigest()[:10]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def run_tool(args: list[str], timeout: int = 120) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    except FileNotFoundError as exc:
        return subprocess.CompletedProcess(args, 127, "", str(exc))
    except subprocess.TimeoutExpired as exc:
        return subprocess.CompletedProcess(args, 124, exc.stdout or "", f"timeout: {exc}")


def runtime_dependency_report() -> dict[str, Any]:
    """Describe the runtime required by build/update before any output write."""
    python_version = ".".join(str(part) for part in sys.version_info[:3])
    report: dict[str, Any] = {
        "python": {
            "required": ">=3.10",
            "version": python_version,
            "ok": sys.version_info[:2] >= PYTHON_MIN_VERSION,
        },
        "python_packages": {
            "pypdf": {
                "required": "requirements.txt",
                "version": PYPDF_VERSION,
                "ok": PdfReader is not None,
                "error": PYPDF_IMPORT_ERROR,
            },
            "jsonschema": {
                "required": "requirements.txt",
                "version": JSONSCHEMA_VERSION,
                "ok": jsonschema is not None,
                "error": JSONSCHEMA_IMPORT_ERROR,
            },
        },
        "executables": {},
        "tesseract_languages": {
            "required": list(OCR_LANGUAGES),
            "available": [],
            "ok": False,
        },
        "missing": [],
    }

    for executable in ("pdfinfo", "pdfimages", "pdftoppm", "tesseract"):
        path = shutil.which(executable)
        report["executables"][executable] = {"path": path, "ok": path is not None}
        if path is None:
            report["missing"].append(executable)

    if not report["python"]["ok"]:
        report["missing"].append("python>=3.10")
    if not report["python_packages"]["pypdf"]["ok"]:
        report["missing"].append("pypdf (install requirements.txt)")
    if not report["python_packages"]["jsonschema"]["ok"]:
        report["missing"].append("jsonschema (install requirements.txt)")

    tesseract_path = report["executables"]["tesseract"]["path"]
    if tesseract_path:
        languages = run_tool([tesseract_path, "--list-langs"], timeout=30)
        available = {
            line.strip()
            for line in languages.stdout.splitlines()
            if line.strip() and not line.lower().startswith("list of available languages")
        }
        report["tesseract_languages"]["available"] = sorted(available)
        report["tesseract_languages"]["ok"] = languages.returncode == 0 and all(
            language in available for language in OCR_LANGUAGES
        )
        if languages.returncode != 0:
            report["tesseract_languages"]["error"] = normalize_text(languages.stderr) or f"tesseract exited {languages.returncode}"
        missing_languages = [language for language in OCR_LANGUAGES if language not in available]
        if missing_languages:
            report["missing"].append(f"tesseract language data ({', '.join(missing_languages)})")
    else:
        report["missing"].append("tesseract language data (tesseract is not installed)")

    return report


def require_runtime_dependencies() -> dict[str, Any]:
    report = runtime_dependency_report()
    if report["missing"]:
        missing = ", ".join(report["missing"])
        raise CliError(
            f"missing runtime dependencies: {missing}",
            EXIT_MISSING_DEPENDENCY,
            {"dependencies": report},
        )
    return report


def parse_pdfinfo(path: Path) -> tuple[dict[str, Any], list[str]]:
    proc = run_tool(["pdfinfo", str(path)])
    errors: list[str] = []
    if proc.returncode != 0:
        errors.append(normalize_text(proc.stderr) or f"pdfinfo exited {proc.returncode}")
    info: dict[str, Any] = {}
    for line in proc.stdout.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        value = value.strip()
        if key.strip() == "Pages":
            try:
                info["page_count"] = int(value)
            except ValueError:
                info["page_count_raw"] = value
        elif key.strip() in {"Page size", "PDF version", "File size"}:
            info[key.strip().lower().replace(" ", "_")] = value
    if "page_count" not in info:
        errors.append("pdfinfo did not provide a page count")
    return info, errors


def image_page_counts(path: Path) -> tuple[dict[int, int], list[str]]:
    proc = run_tool(["pdfimages", "-list", str(path)])
    if proc.returncode != 0:
        return {}, [normalize_text(proc.stderr) or f"pdfimages exited {proc.returncode}"]
    counts: Counter[int] = Counter()
    for line in proc.stdout.splitlines():
        parts = line.split()
        if len(parts) >= 2 and parts[0].isdigit() and parts[1].isdigit():
            counts[int(parts[0])] += 1
    return dict(counts), []


def extract_pdf_pages(path: Path) -> tuple[list[str], list[str], dict[str, Any]]:
    """Return normalized page text, extraction errors, and reader metadata."""
    if PdfReader is None:
        return [], ["pypdf is not available"], {}
    try:
        reader = PdfReader(str(path), strict=False)
        pages: list[str] = []
        errors: list[str] = []
        for number, page in enumerate(reader.pages, start=1):
            try:
                pages.append(normalize_text(page.extract_text() or ""))
            except Exception as exc:  # keep other pages moving
                pages.append("")
                errors.append(f"page {number}: {type(exc).__name__}: {exc}")
        return pages, errors, {"reader_page_count": len(reader.pages), "reader": "pypdf"}
    except Exception as exc:
        return [], [f"pypdf: {type(exc).__name__}: {exc}"], {}


def ocr_page(
    pdf_path: Path,
    page_number: int,
    render_dir: Path,
) -> tuple[str, str | None, str | None]:
    """Render one page and OCR it; return text, render path, error."""
    if shutil.which("pdftoppm") is None:
        return "", None, "pdftoppm is not installed"
    if shutil.which("tesseract") is None:
        return "", None, "tesseract is not installed"
    render_dir.mkdir(parents=True, exist_ok=True)
    prefix = render_dir / f"page-{page_number:03d}"
    render = run_tool(
        [
            "pdftoppm",
            "-png",
            "-r",
            "180",
            "-f",
            str(page_number),
            "-l",
            str(page_number),
            "-singlefile",
            str(pdf_path),
            str(prefix),
        ],
        timeout=180,
    )
    image_path = prefix.with_suffix(".png")
    if render.returncode != 0 or not image_path.exists():
        return "", None, normalize_text(render.stderr) or "page render failed"
    ocr = run_tool(
        ["tesseract", str(image_path), "stdout", "-l", "chi_sim+eng", "--psm", "6"],
        timeout=180,
    )
    if ocr.returncode != 0:
        return "", str(image_path), normalize_text(ocr.stderr) or "tesseract failed"
    return normalize_text(ocr.stdout), str(image_path), None


def source_id_for_path(path: Path) -> tuple[str, bool]:
    if path.name in DOC_ID_BY_FILENAME:
        return DOC_ID_BY_FILENAME[path.name], True
    return slugify(path.stem), False


def select_source_paths(
    source_dir: Path,
    only: list[str] | None,
    *,
    allow_empty: bool = False,
) -> list[Path]:
    if not source_dir.exists() or not source_dir.is_dir():
        raise CliError(f"source directory does not exist or is not a directory: {source_dir}", EXIT_USAGE)
    candidates = sorted(source_dir.glob("*.pdf"), key=lambda path: path.name)
    if not candidates and not allow_empty:
        raise CliError(f"source directory contains no PDF files: {source_dir}", EXIT_USAGE)
    if only is None:
        return candidates
    if not only:
        raise CliError("--only requires at least one source id or filename fragment", EXIT_USAGE)
    selected = [
        path
        for path in candidates
        if any(token.lower() in path.name.lower() or token.lower() in source_id_for_path(path)[0].lower() for token in only)
    ]
    if not selected:
        raise CliError(f"--only did not match a PDF source: {', '.join(only)}", EXIT_USAGE)
    return selected


def validate_partial_output_target(
    output_dir: Path,
    selected_source_ids: set[str],
    only: list[str] | None,
) -> None:
    """Prevent a partial build from replacing a formal or unrelated snapshot."""
    if only is None:
        return
    if output_dir == DEFAULT_OUTPUT_DIR.resolve():
        raise CliError(
            "--only refuses to write the formal knowledge/data output; pass an explicit isolated --output-dir",
            EXIT_USAGE,
        )
    if not output_dir.exists():
        return
    if not output_dir.is_dir():
        raise CliError(f"output path is not a directory: {output_dir}", EXIT_USAGE)

    manifest_path = output_dir / "manifest.json"
    if manifest_path.exists():
        try:
            manifest = load_json(manifest_path)
        except (OSError, json.JSONDecodeError) as exc:
            raise CliError(
                f"refusing partial build because existing manifest is unreadable: {manifest_path}",
                EXIT_RUNTIME_FAILURE,
            ) from exc
        existing_source_ids = {doc.get("source_id") for doc in manifest.get("documents", [])}
        if existing_source_ids != selected_source_ids:
            raise CliError(
                "refusing partial build over an output containing a different source set; use a new isolated --output-dir",
                EXIT_USAGE,
                {
                    "existing_source_ids": sorted(value for value in existing_source_ids if value),
                    "selected_source_ids": sorted(selected_source_ids),
                },
            )
    elif any(output_dir.iterdir()):
        raise CliError(
            "refusing partial build over a non-empty directory without a manifest; use a new isolated --output-dir",
            EXIT_USAGE,
        )


def remove_path(path: Path) -> None:
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
    elif path.exists() or path.is_symlink():
        path.unlink()


def commit_staged_output(staging_dir: Path, output_dir: Path, run_id: str) -> None:
    """Swap a validated directory into place while retaining rollback ability."""
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    if output_dir.exists() and not output_dir.is_dir():
        raise OSError(f"output path is not a directory: {output_dir}")
    if not output_dir.exists():
        os.replace(staging_dir, output_dir)
        return

    backup_dir = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.backup-", dir=output_dir.parent))
    backup_dir.rmdir()
    try:
        os.replace(output_dir, backup_dir)
        try:
            os.replace(staging_dir, output_dir)
        except Exception:
            if output_dir.exists():
                remove_path(output_dir)
            os.replace(backup_dir, output_dir)
            raise
    except Exception:
        if backup_dir.exists() and not output_dir.exists():
            os.replace(backup_dir, output_dir)
        raise
    else:
        remove_path(backup_dir)


def document_result(
    path: Path,
    doc_spec: dict[str, Any],
    temp_root: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    source_id, known = source_id_for_path(path)
    digest = sha256_file(path)
    pdfinfo, pdfinfo_errors = parse_pdfinfo(path)
    page_count = pdfinfo.get("page_count")
    image_counts, image_errors = image_page_counts(path)
    errors = [*pdfinfo_errors, *image_errors]
    base: dict[str, Any] = {
        "source_id": source_id,
        "source_file": str(path.relative_to(ROOT).as_posix()) if path.is_relative_to(ROOT) else str(path),
        "file_name": path.name,
        "title": doc_spec.get("title", path.stem),
        "known_initial_source": known,
        # A file can be extracted before its semantic configuration and
        # normalized records exist.  Keep that state explicit so validation
        # cannot mistake page text for a complete knowledge source.
        "semantic_configuration_status": "configured" if known else "pending",
        "normalization_status": "configured" if known else "pending",
        "sha256": digest,
        "file_size_bytes": path.stat().st_size,
        "page_count": page_count,
        "page_size": pdfinfo.get("page_size"),
        "pdf_version": pdfinfo.get("pdf_version"),
        "image_pages": sorted(image_counts),
        "image_count": sum(image_counts.values()),
        "extraction_methods": [],
        "extraction_errors": [],
        "status": "failed",
        "attempted": True,
        "visual_review": {
            "rendered_for_ocr_pages": [],
            "related_visual_notes_present": bool(doc_spec.get("chunk_specs")),
        },
    }
    if page_count is None:
        base["extraction_errors"] = errors
        base["failure_kind"] = "pdf_structure_unreadable"
        if doc_spec.get("external_recovery"):
            base["supplemental_recovery"] = doc_spec["external_recovery"]
        return base, []

    pdf_pages, extraction_errors, reader_info = extract_pdf_pages(path)
    if not pdf_pages:
        base["extraction_errors"] = [*errors, *extraction_errors]
        base["failure_kind"] = "text_extraction_failed"
        return base, []
    if len(pdf_pages) != page_count:
        errors.append(f"page count mismatch: pdfinfo={page_count}, pypdf={len(pdf_pages)}")
        page_count = len(pdf_pages)
        base["page_count"] = page_count
    base["extraction_methods"].append(reader_info.get("reader", "pypdf"))
    base["extraction_methods"].append("pdfimages")
    if image_counts:
        base["extraction_methods"].append("visual-page-linkage")

    pages: list[dict[str, Any]] = []
    threshold = int(doc_spec.get("ocr_when_text_chars_below", 80))
    render_root = temp_root / source_id
    for number, pdf_text in enumerate(pdf_pages, start=1):
        image_count = image_counts.get(number, 0)
        ocr_text = ""
        render_path: str | None = None
        ocr_error: str | None = None
        ocr_attempted = False
        if image_count and len(pdf_text) < threshold:
            ocr_attempted = True
            ocr_text, render_path, ocr_error = ocr_page(path, number, render_root)
            if render_path:
                base["visual_review"]["rendered_for_ocr_pages"].append(number)
            if ocr_text:
                base["extraction_methods"].append("tesseract")
        if pdf_text and ocr_text:
            content = f"{pdf_text}\n\n[OCR supplemental]\n{ocr_text}"
            text_sources = ["pdf_text", "ocr"]
        elif ocr_text:
            content = ocr_text
            text_sources = ["ocr"]
        else:
            content = pdf_text
            text_sources = ["pdf_text"] if pdf_text else []
        page_errors: list[str] = []
        if ocr_error:
            page_errors.append(ocr_error)
        pages.append(
            {
                "page_number": number,
                "pdf_text": pdf_text,
                "ocr_text": ocr_text,
                "content": normalize_text(content),
                "text_sources": text_sources,
                "pdf_text_chars": len(pdf_text),
                "ocr_text_chars": len(ocr_text),
                "content_chars": len(normalize_text(content)),
                "image_count": image_count,
                "ocr_attempted": ocr_attempted,
                "ocr_error": ocr_error,
                "errors": page_errors,
            }
        )

    base["extraction_errors"] = [*errors, *extraction_errors]
    base["ocr_pages"] = [page["page_number"] for page in pages if page["ocr_text"]]
    base["empty_text_pages"] = [page["page_number"] for page in pages if not page["content"]]
    base["text_characters"] = sum(page["content_chars"] for page in pages)
    base["status"] = "partial" if base["extraction_errors"] or base["empty_text_pages"] else "success"
    if base["status"] == "partial" and not base["extraction_errors"] and base["empty_text_pages"]:
        base["extraction_errors"].append(
            f"{len(base['empty_text_pages'])} page(s) have no text layer/OCR; visual references retained"
        )
    return base, pages


def page_block(page: dict[str, Any]) -> str:
    content = normalize_text(page.get("content", ""))
    if content:
        return f"[Page {page['page_number']}]\n{content}"
    return f"[Page {page['page_number']}]\n[Visual-only page; no extractable text]"


def _compiled_patterns(values: Any, *, field: str) -> list[re.Pattern[str]]:
    """Compile an explicitly configured retrieval line-pattern list."""
    if values is None:
        return []
    if not isinstance(values, list) or not all(isinstance(value, str) and value for value in values):
        raise ValueError(f"{field} must be a non-empty string list")
    try:
        return [re.compile(value, re.IGNORECASE) for value in values]
    except re.error as exc:
        raise ValueError(f"{field} contains an invalid regular expression: {exc}") from exc


def _strip_visual_notes(source_text: str) -> str:
    """Remove the optional visual-note section from lexical evidence."""
    lines = source_text.splitlines()
    kept: list[str] = []
    in_visual_notes = False
    for line in lines:
        if line.strip() == "[Visual notes]":
            in_visual_notes = True
            continue
        if in_visual_notes and line.startswith("[") and line.endswith("]"):
            in_visual_notes = False
        if not in_visual_notes:
            kept.append(line)
    return normalize_text("\n".join(kept))


def retrieval_page_block(
    page: dict[str, Any],
    spec: dict[str, Any],
    policy: dict[str, Any],
) -> str:
    """Build a source-faithful, noise-filtered block for an opted-in chunk.

    The raw page extraction is intentionally untouched.  This function only
    controls the retrieval child chunk text and requires positive, per-chunk
    OCR anchors so an opted-in document cannot accidentally index an entire
    chart screenshot.
    """
    page_number = page["page_number"]
    pdf_text = normalize_text(page.get("pdf_text", ""))
    parts = [f"[Page {page_number}]", pdf_text or "[No PDF text]"]

    keep_patterns = _compiled_patterns(
        spec.get("ocr_keep_patterns", []),
        field=f"chunk spec {spec.get('key', '<unknown>')}.ocr_keep_patterns",
    )
    drop_patterns = _compiled_patterns(
        policy.get("ocr_drop_patterns", []),
        field="retrieval_source_text_policy.ocr_drop_patterns",
    )
    strip_patterns = _compiled_patterns(
        policy.get("ocr_strip_patterns", []),
        field="retrieval_source_text_policy.ocr_strip_patterns",
    )
    time_pattern_values = policy.get("ocr_time_patterns", [])
    time_patterns = _compiled_patterns(
        time_pattern_values,
        field="retrieval_source_text_policy.ocr_time_patterns",
    )
    date_patterns = _compiled_patterns(
        policy.get("ocr_date_patterns", []),
        field="retrieval_source_text_policy.ocr_date_patterns",
    )
    kept_lines: list[str] = []
    for raw_line in normalize_text(page.get("ocr_text", "")).splitlines():
        line = unicodedata.normalize("NFKC", raw_line).strip()
        if not line or not keep_patterns or not any(pattern.search(line) for pattern in keep_patterns):
            continue
        if any(pattern.search(line) for pattern in drop_patterns):
            continue
        if any(pattern.search(line) for pattern in time_patterns + date_patterns):
            continue
        for pattern in strip_patterns:
            line = pattern.sub("", line)
        line = " ".join(line.split()).strip(" |,;:")
        # A positive anchor is necessary but not sufficient: after coordinate
        # stripping, discard a line that contains no alphanumeric/CJK signal.
        if not any(character.isalnum() or "\u3400" <= character <= "\u9fff" for character in line):
            continue
        if line not in kept_lines:
            kept_lines.append(line)
    if kept_lines:
        parts.extend(["[OCR rule lines]", "\n".join(kept_lines)])

    if policy.get("include_visual_notes", True):
        field = str(policy.get("visual_notes_field", "retrieval_visual_notes"))
        notes = normalize_text(str(spec.get(field) or ""))
        if not notes:
            raise ValueError(
                f"chunk spec {spec.get('key', '<unknown>')} must provide non-empty {field} "
                "when retrieval visual notes are enabled"
            )
        parts.extend(["[Visual notes]", notes])
    return "\n".join(parts)


def page_excerpt_block(page: dict[str, Any], page_excerpts: dict[str, Any] | None) -> str:
    """Return a page marker plus validated source-text excerpts.

    Source-scoped chunk specs may provide ``page_excerpts`` when a semantic
    unit spans pages that also contain adjacent examples.  Selectors are
    exact normalized substrings of the extracted page text; refusing a
    missing selector keeps the generated chunk source-faithful instead of
    silently inventing or dropping text.
    """
    if page_excerpts is None:
        return page_block(page)
    number = page["page_number"]
    raw_selectors = page_excerpts.get(str(number))
    if raw_selectors is None and number in page_excerpts:
        raw_selectors = page_excerpts[number]
    if not isinstance(raw_selectors, list) or not raw_selectors:
        raise ValueError(
            f"page_excerpts[{number}] must be a non-empty list of extracted-text selectors"
        )
    content = normalize_text(page.get("content", ""))
    matches: list[tuple[int, str]] = []
    for index, selector in enumerate(raw_selectors):
        if not isinstance(selector, str) or not normalize_text(selector):
            raise ValueError(f"page_excerpts[{number}][{index}] must be a non-empty string")
        normalized_selector = normalize_text(selector)
        offset = content.find(normalized_selector)
        if offset < 0:
            raise ValueError(
                f"page_excerpts[{number}][{index}] is not present in extracted page text"
            )
        matches.append((offset, normalized_selector))

    # Preserve page reading order even when config was authored in a more
    # convenient semantic order, and avoid emitting an overlapping selector
    # twice.  Every selected page still carries its continuity marker.
    selected: list[str] = []
    end = -1
    for offset, selector in sorted(matches, key=lambda item: (item[0], item[1])):
        if offset < end:
            continue
        selected.append(selector)
        end = offset + len(selector)
    return f"[Page {number}]\n" + "\n".join(selected)


def make_chunk(
    doc: dict[str, Any],
    pages: list[dict[str, Any]],
    spec: dict[str, Any],
    parent_chunk_id: str,
    retrieval: bool,
    retrieval_source_text_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    start, end = spec["pages"]
    selected = [page for page in pages if start <= page["page_number"] <= end]
    page_excerpts = spec.get("page_excerpts")
    if page_excerpts is not None:
        if not isinstance(page_excerpts, dict):
            raise ValueError(f"chunk spec {spec.get('key', '<unknown>')}: page_excerpts must be an object")
        expected_pages = {str(number) for number in range(start, end + 1)}
        configured_pages = {str(number) for number in page_excerpts}
        missing_pages = sorted(expected_pages - configured_pages, key=int)
        extra_pages = sorted(
            configured_pages - expected_pages,
            key=lambda value: (not value.isdigit(), int(value) if value.isdigit() else value),
        )
        if missing_pages or extra_pages:
            details = []
            if missing_pages:
                details.append(f"missing pages {missing_pages}")
            if extra_pages:
                details.append(f"out-of-range pages {extra_pages}")
            raise ValueError(
                f"chunk spec {spec.get('key', '<unknown>')}: page_excerpts has " + ", ".join(details)
            )
    if retrieval and retrieval_source_text_policy and retrieval_source_text_policy.get("enabled"):
        if page_excerpts is not None:
            raise ValueError(
                f"chunk spec {spec.get('key', '<unknown>')}: page_excerpts cannot be combined "
                "with retrieval_source_text_policy"
            )
        source_text = "\n\n".join(
            retrieval_page_block(page, spec, retrieval_source_text_policy)
            for page in selected
        )
    else:
        source_text = "\n\n".join(page_excerpt_block(page, page_excerpts) for page in selected)
    source_id = doc["source_id"]
    key = spec["key"]
    chunk_id = f"{source_id}:{slugify(key)}"
    related_images = [page["page_number"] for page in selected if page.get("image_count")]
    sources = sorted({source for page in selected for source in page.get("text_sources", [])})
    retrieval_visual_notes = ""
    if retrieval and retrieval_source_text_policy and retrieval_source_text_policy.get("enabled"):
        notes_field = str(retrieval_source_text_policy.get("visual_notes_field", "retrieval_visual_notes"))
        retrieval_visual_notes = normalize_text(str(spec.get(notes_field) or ""))
    return {
        "chunk_id": chunk_id,
        "chunk_key": key,
        "source_id": source_id,
        "source_file": doc["source_file"],
        "source_sha256": doc["sha256"],
        "page_start": start,
        "page_end": end,
        "page_range": [start, end],
        "title_path": spec["title_path"],
        "parent_chunk_id": parent_chunk_id,
        "adjacent_chunk_ids": [],
        "chunk_kind": spec.get("kind", "section"),
        "is_retrieval_chunk": retrieval,
        "is_cross_page": start < end,
        "semantic_boundary": spec["boundary_reason"],
        "extraction_sources": sources,
        "related_image_pages": related_images,
        "visual_notes": spec.get("visual_notes", ""),
        "retrieval_visual_notes": retrieval_visual_notes,
        "source_text": source_text,
        "source_text_sha256": sha256_text(source_text),
        "source_locator": f"{doc['source_file']} pages {start}-{end}",
        "provenance_status": "source_faithful_pdf",
    }


def make_chunks(doc: dict[str, Any], pages: list[dict[str, Any]], doc_spec: dict[str, Any]) -> list[dict[str, Any]]:
    # Unknown sources are intentionally page-extracted only.  Do not create a
    # root/context chunk that could be mistaken for a configured semantic
    # retrieval unit; the validator reports the source as incomplete until a
    # document spec and normalized record mapping are added.
    if (
        not pages
        or doc.get("status") == "failed"
        or doc.get("semantic_configuration_status") != "configured"
    ):
        return []
    page_count = int(doc.get("page_count") or len(pages))
    root_id = f"{doc['source_id']}:root"
    root_spec = {
        "key": "root",
        "pages": [1, page_count],
        "title_path": [doc.get("title", doc["source_id"]), "Document context rollup"],
        "kind": "document_context",
        "boundary_reason": "Document-level source context rollup; not a fixed-size retrieval chunk.",
        "visual_notes": "Use child semantic chunks for retrieval and this rollup for parent context.",
    }
    retrieval_policy = doc_spec.get("retrieval_source_text_policy")
    if retrieval_policy is not None and (
        not isinstance(retrieval_policy, dict) or not retrieval_policy.get("enabled")
    ):
        retrieval_policy = None
    chunks = [make_chunk(doc, pages, root_spec, "", False)]
    covered: set[int] = set()
    spec_keys: set[str] = set()
    for spec in doc_spec.get("chunk_specs", []):
        start, end = spec["pages"]
        start = max(1, min(page_count, int(start)))
        end = max(start, min(page_count, int(end)))
        fixed = dict(spec)
        fixed["pages"] = [start, end]
        fixed["key"] = str(spec["key"])
        chunks.append(make_chunk(doc, pages, fixed, root_id, True, retrieval_policy))
        covered.update(range(start, end + 1))
        spec_keys.add(str(spec["key"]))

    # Blank separator/chart pages not assigned to a named strategy are still
    # preserved as context, but are not treated as fixed-length retrieval data.
    missing = [number for number in range(1, page_count + 1) if number not in covered]
    for number in missing:
        key = f"page-context-{number:03d}"
        context_spec = {
            "key": key,
            "pages": [number, number],
            "title_path": [doc.get("title", doc["source_id"]), f"Page {number} context"],
            "kind": "page_context",
            "boundary_reason": "Unassigned page preserved as page context; not a fixed-size retrieval split.",
            "visual_notes": "Retained so page-level chart/text provenance is not discarded.",
        }
        chunks.append(make_chunk(doc, pages, context_spec, root_id, False))

    ordered = [chunks[0]] + sorted(chunks[1:], key=lambda chunk: (chunk["page_start"], chunk["page_end"], chunk["chunk_id"]))
    retrieval = [chunk for chunk in ordered if chunk["is_retrieval_chunk"]]
    for index, chunk in enumerate(retrieval):
        neighbors: list[str] = []
        if index:
            neighbors.append(retrieval[index - 1]["chunk_id"])
        if index + 1 < len(retrieval):
            neighbors.append(retrieval[index + 1]["chunk_id"])
        chunk["adjacent_chunk_ids"] = neighbors
    return ordered


def infer_record_source(record_id: str) -> str | None:
    for prefix, source_id in sorted(RECORD_SOURCE_PREFIXES.items(), key=lambda item: len(item[0]), reverse=True):
        if record_id.startswith(prefix):
            return source_id
    return None


def record_source_refs(
    record_cfg: dict[str, Any],
    source_id: str,
    chunk_lookup: dict[tuple[str, str], dict[str, Any]],
    docs: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    refs: list[dict[str, Any]] = []
    for key in record_cfg.get("chunk_keys", []):
        chunk = chunk_lookup.get((source_id, key))
        if not chunk:
            continue
        refs.append(
            {
                "chunk_id": chunk["chunk_id"],
                "source_id": chunk["source_id"],
                "source_file": chunk["source_file"],
                "source_sha256": chunk["source_sha256"],
                "page_start": chunk["page_start"],
                "page_end": chunk["page_end"],
                "title_path": chunk["title_path"],
                "source_locator": chunk["source_locator"],
                "provenance_status": chunk["provenance_status"],
            }
        )
    if not refs and record_cfg.get("supplemental_source"):
        supplemental = dict(record_cfg["supplemental_source"])
        refs.append(
            {
                "chunk_id": None,
                "source_id": source_id,
                "source_file": supplemental.get("source_file") or docs.get(source_id, {}).get("source_file"),
                "source_sha256": docs.get(source_id, {}).get("sha256"),
                "page_start": supplemental.get("page_start"),
                "page_end": supplemental.get("page_end"),
                "title_path": [record_cfg["name"], "supplemental external source"],
                "source_locator": supplemental.get("locator"),
                "external_url": supplemental.get("url"),
                "provenance_status": "supplemental_external_due_to_pdf_failure",
            }
        )
    return refs


def infer_declaration_type(statement: str) -> str:
    """Choose a conservative declaration type for migrated handwritten text."""
    not_stated_markers = (
        "未给出",
        "未提供",
        "未明确",
        "没有给出",
        "没有独立",
        "不能被本记录替代",
        "不伪造",
        "本地 PDF",
    )
    if any(marker in statement for marker in not_stated_markers):
        return "source_not_stated"
    # Existing normalized prose is generally an interpretation of a source
    # diagram/table rather than a verbatim quotation.  Source-scoped files
    # may opt into source_assertion explicitly when a future document pass
    # verifies exact wording.
    return "derived_interpretation"


def normalize_claim_item(
    item: Any,
    record_id: str,
    field: str,
    index: int,
    default_chunk_keys: list[str],
) -> dict[str, Any]:
    """Normalize legacy prose or an explicit claim config into claim metadata."""
    if isinstance(item, dict):
        claim = dict(item)
        statement = claim.get("statement") or claim.get("claim") or claim.get("label")
        if not statement and claim.get("pages"):
            statement = f"Example on pages {claim['pages']}"
        claim["statement"] = normalize_text(str(statement or ""))
        declaration = claim.get("declaration_type") or claim.get("claim_type")
        claim["declaration_type"] = declaration or infer_declaration_type(claim["statement"])
    else:
        claim = {
            "statement": normalize_text(str(item or "")),
            "declaration_type": infer_declaration_type(normalize_text(str(item or ""))),
        }
    claim["claim_id"] = claim.get("claim_id") or f"{record_id}:{field}:{index + 1}"
    if claim["declaration_type"] in {"derived_interpretation", "source_not_stated"}:
        claim["rationale"] = normalize_text(
            str(claim.get("rationale") or (
                "Normalized interpretation of source-linked material; this is not verbatim source wording."
                if claim["declaration_type"] == "derived_interpretation"
                else "The referenced source does not state a concrete rule for this field."
            ))
        )
    keys = claim.get("supporting_chunk_keys") or claim.get("chunk_keys") or default_chunk_keys
    if isinstance(keys, str):
        keys = [keys]
    claim["supporting_chunk_keys"] = [str(key) for key in keys or []]
    # Legacy example metadata (label/pages/url) is retained as non-claim
    # fields so source-scoped editors do not lose useful context.
    return claim


def claim_refs_for_keys(
    source_id: str,
    keys: list[str],
    chunk_lookup: dict[tuple[str, str], dict[str, Any]],
    fallback_refs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    refs: list[dict[str, Any]] = []
    for key in keys:
        chunk = chunk_lookup.get((source_id, key))
        if chunk:
            refs.append(
                {
                    "chunk_id": chunk["chunk_id"],
                    "source_id": chunk["source_id"],
                    "source_file": chunk["source_file"],
                    "source_sha256": chunk["source_sha256"],
                    "page_start": chunk["page_start"],
                    "page_end": chunk["page_end"],
                    "title_path": chunk["title_path"],
                    "source_locator": chunk["source_locator"],
                    "provenance_status": chunk["provenance_status"],
                }
            )
    return refs or [dict(ref) for ref in fallback_refs]


def build_claims(
    record: dict[str, Any],
    source_id: str,
    source_refs: list[dict[str, Any]],
    chunk_lookup: dict[tuple[str, str], dict[str, Any]],
) -> None:
    record_id = str(record.get("id", "unknown"))
    default_keys = [str(key) for key in record.get("chunk_keys", [])]
    for field in CLAIM_FIELDS:
        values = record.get(field, [])
        if not isinstance(values, list):
            continue
        claims: list[dict[str, Any]] = []
        for index, item in enumerate(values):
            claim = normalize_claim_item(item, record_id, field, index, default_keys)
            keys = claim.pop("supporting_chunk_keys", [])
            claim["source_refs"] = claim_refs_for_keys(source_id, keys, chunk_lookup, source_refs)
            claims.append(claim)
        record[field] = claims


def build_records(
    record_config: dict[str, Any],
    docs: dict[str, dict[str, Any]],
    chunks: list[dict[str, Any]],
    selected_source_ids: set[str],
) -> list[dict[str, Any]]:
    chunk_lookup = {(chunk["source_id"], chunk["chunk_key"]): chunk for chunk in chunks}
    records: list[dict[str, Any]] = []
    for config in record_config.get("records", []):
        source_id = config.get("source_id") or infer_record_source(config["id"])
        if not source_id or source_id not in selected_source_ids:
            continue
        refs = record_source_refs(config, source_id, chunk_lookup, docs)
        if config.get("chunk_keys") and not refs:
            continue
        if not refs:
            continue
        record = dict(config)
        record.pop("chunk_keys", None)
        record.pop("supplemental_source", None)
        record["source_id"] = source_id
        record["source_refs"] = refs
        record["source_files"] = sorted({ref["source_file"] for ref in refs if ref.get("source_file")})
        source_chunks = [chunk_lookup[(source_id, key)] for key in config.get("chunk_keys", []) if (source_id, key) in chunk_lookup]
        excerpt = "\n\n".join(chunk["source_text"] for chunk in source_chunks)
        record["source_excerpt"] = normalize_text(excerpt)[:5000]
        source_doc = docs.get(source_id, {})
        build_claims(record, source_id, refs, chunk_lookup)
        if source_requires_reverification(source_doc):
            # A changed known PDF must not silently carry forward a static
            # normalized interpretation.  Keep the record for auditability,
            # but make it inactive until its source is explicitly reverified.
            record["active"] = False
            record["record_status"] = "invalidated"
            record["source_fidelity"] = "pending_source_reverification"
            record["reverification_required"] = True
            record["invalidation_reason"] = source_doc.get(
                "invalidation_reason", "source_sha256_changed_since_previous_snapshot"
            )
        else:
            record["active"] = True
            record["record_status"] = "active"
            record["source_fidelity"] = "source_faithful_with_normalization" if source_chunks else "supplemental_external_due_to_pdf_failure"
            record["reverification_required"] = False
        record["knowledge_base_version"] = KB_VERSION
        records.append(record)
    return records


LIFECYCLE_CHANGE_STATUSES = {"added", "changed", "deleted", "renamed", "unchanged"}

# A source-content hash in the source-scoped document configuration is the
# deliberately explicit re-verification signal.  Merely seeing the same
# bytes again after an earlier change is not enough to restore a normalized
# record: an editor must record that the current content was checked.
SOURCE_REVERIFICATION_HASH_KEY = "verified_source_sha256"
SOURCE_REVERIFICATION_PENDING = "pending_source_reverification"
SOURCE_REVERIFICATION_VERIFIED = "source_reverified"


def configured_reverification_hash(doc_spec: dict[str, Any] | None) -> str | None:
    """Return a valid configured content hash used to clear invalidation.

    Source-scoped configuration is intentionally free-form, so malformed or
    absent values simply do not count as an explicit signal.  A matching
    SHA-256 is required; an arbitrary config edit or a no-op update cannot
    reactivate a changed source.
    """
    if not isinstance(doc_spec, dict):
        return None
    value = doc_spec.get(SOURCE_REVERIFICATION_HASH_KEY)
    if not isinstance(value, str):
        return None
    candidate = normalize_text(value).lower()
    return candidate if re.fullmatch(r"[0-9a-f]{64}", candidate) else None


def source_requires_reverification(document: dict[str, Any] | None) -> bool:
    """Whether a document's normalized records must remain inactive.

    The explicit fields are persisted in the manifest and survive a no-op
    update.  ``change_status == changed`` remains a compatibility fallback for
    snapshots written before the persistent fields existed.
    """
    if not isinstance(document, dict):
        return False
    # New snapshots always persist the source-scoped fields.  When present,
    # they are authoritative: a matching verified hash on the same ``changed``
    # update must not be re-invalidated by the legacy change-status fallback.
    if "source_reverification_required" in document or "source_reverification_status" in document:
        return bool(
            document.get("source_reverification_required") is True
            or document.get("source_reverification_status") == SOURCE_REVERIFICATION_PENDING
        )
    # Older snapshots had no persistent state fields; retain their changed
    # status as a compatibility fallback when carrying state forward once.
    return bool(document.get("reverification_required") is True or document.get("change_status") == "changed")


def apply_source_reverification_state(
    previous: dict[str, Any] | None,
    documents: list[dict[str, Any]],
    doc_specs: dict[str, dict[str, Any]],
    lifecycle_changes: list[dict[str, Any]],
) -> None:
    """Persist source invalidation until an explicit config hash matches.

    State is attached to each current manifest document so records and the
    validator consume the same source-scoped truth.  A same-SHA rename uses
    ``previous_source_id`` to inherit pending state; rename itself is never a
    re-verification signal.
    """
    previous_docs = {doc.get("source_id"): doc for doc in _manifest_documents(previous)}
    changes_by_source = {
        change.get("source_id"): change
        for change in lifecycle_changes
        if change.get("source_id")
    }
    for document in documents:
        source_id = document.get("source_id")
        change = changes_by_source.get(source_id, {})
        previous_source_id = change.get("previous_source_id") or source_id
        previous_document = previous_docs.get(previous_source_id)
        doc_spec = doc_specs.get(str(source_id))
        current_sha = document.get("sha256")
        configured_sha = configured_reverification_hash(doc_spec)

        # This is the only path that clears a persisted pending state.  The
        # signal is source-scoped and content-bound, so it is reproducible and
        # cannot be triggered by an unchanged update alone.
        if configured_sha and configured_sha == str(current_sha).lower():
            document["source_reverification_status"] = SOURCE_REVERIFICATION_VERIFIED
            document["source_reverification_required"] = False
            document["source_reverified_sha256"] = current_sha
            document["source_reverification_signal"] = SOURCE_REVERIFICATION_HASH_KEY
            document.pop("invalidation_reason", None)
            continue

        inherited_pending = source_requires_reverification(previous_document)
        if change.get("status") == "changed" or inherited_pending:
            document["source_reverification_status"] = SOURCE_REVERIFICATION_PENDING
            document["source_reverification_required"] = True
            document["invalidation_reason"] = (
                previous_document.get("invalidation_reason")
                if isinstance(previous_document, dict) and previous_document.get("invalidation_reason")
                else "source_sha256_changed_since_previous_snapshot"
            )
            document.pop("source_reverified_sha256", None)
            document.pop("source_reverification_signal", None)
            continue

        document["source_reverification_status"] = "not_required"
        document["source_reverification_required"] = False
        document.pop("source_reverified_sha256", None)
        document.pop("source_reverification_signal", None)


def _manifest_documents(previous: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not previous:
        return []
    documents = previous.get("documents", [])
    if not isinstance(documents, list):
        raise TypeError("manifest.documents must be a list")
    return [document for document in documents if isinstance(document, dict)]


def build_manifest_change(previous: dict[str, Any] | None, document: dict[str, Any]) -> str:
    """Return the same-file lifecycle status for a single document.

    Rename detection needs the complete current/previous set and is handled by
    :func:`apply_lifecycle_changes`; this helper remains intentionally small for
    callers that only have one document available.
    """
    if not previous:
        return "added"
    previous_docs = _manifest_documents(previous)
    previous_by_file = {item.get("source_file"): item for item in previous_docs}
    previous_by_id = {item.get("source_id"): item for item in previous_docs}
    old = previous_by_file.get(document.get("source_file")) or previous_by_id.get(document.get("source_id"))
    if not old:
        return "added"
    if old.get("sha256") != document.get("sha256"):
        return "changed"
    return "unchanged"


def apply_lifecycle_changes(
    previous: dict[str, Any] | None,
    documents: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Annotate current documents and return a complete lifecycle change log.

    Matching is deterministic and deliberately conservative: a same source id
    or path is an update; otherwise an identical SHA-256 is a rename; all
    unmatched previous documents are deletions.  Every change carries the
    relevant current/previous digest so a manifest is sufficient to audit the
    update without reading stale output files.
    """
    previous_docs = _manifest_documents(previous)
    old_by_id = {item.get("source_id"): item for item in previous_docs if item.get("source_id")}
    old_by_file = {item.get("source_file"): item for item in previous_docs if item.get("source_file")}
    old_by_sha: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in previous_docs:
        if item.get("sha256"):
            old_by_sha[str(item["sha256"])].append(item)
    matched_old_ids: set[str] = set()
    changes: list[dict[str, Any]] = []

    for document in sorted(documents, key=lambda item: (str(item.get("source_id", "")), str(item.get("source_file", "")))):
        source_id = document.get("source_id")
        source_file = document.get("source_file")
        digest = document.get("sha256")
        old = old_by_id.get(source_id) or old_by_file.get(source_file)
        status = "added"
        change: dict[str, Any] = {
            "source_id": source_id,
            "source_file": source_file,
            "sha256": digest,
            "source_sha256": digest,
        }
        if old:
            matched_old_ids.add(str(old.get("source_id")))
            if old.get("sha256") == digest:
                status = "unchanged" if (
                    old.get("source_id") == source_id and old.get("source_file") == source_file
                ) else "renamed"
            else:
                status = "changed"
            change.update(
                {
                    "previous_source_id": old.get("source_id"),
                    "previous_source_file": old.get("source_file"),
                    "previous_sha256": old.get("sha256"),
                    "previous_source_sha256": old.get("sha256"),
                }
            )
        else:
            # Use an unmatched equal digest as a simple, unambiguous rename
            # signal.  If duplicate files share a digest, consume them in
            # stable source-id order.
            candidates = [
                item for item in sorted(old_by_sha.get(str(digest), []), key=lambda value: str(value.get("source_id", "")))
                if str(item.get("source_id")) not in matched_old_ids
            ]
            if candidates:
                old = candidates[0]
                matched_old_ids.add(str(old.get("source_id")))
                status = "renamed"
                change.update(
                    {
                        "previous_source_id": old.get("source_id"),
                        "previous_source_file": old.get("source_file"),
                        "previous_sha256": old.get("sha256"),
                        "previous_source_sha256": old.get("sha256"),
                    }
                )
        document["change_status"] = status
        # Keep an alias for callers that use lifecycle terminology rather than
        # extraction status.  The existing ``status`` field remains the
        # extraction outcome (success/partial/failed).
        document["lifecycle_status"] = status
        if status == "renamed":
            document["renamed_from_source_id"] = old.get("source_id") if old else None
            document["renamed_from_source_file"] = old.get("source_file") if old else None
        change["status"] = status
        change["change_status"] = status
        changes.append(change)

    for old in sorted(previous_docs, key=lambda item: (str(item.get("source_id", "")), str(item.get("source_file", "")))):
        old_id = str(old.get("source_id"))
        if old_id in matched_old_ids:
            continue
        changes.append(
            {
                "source_id": old.get("source_id"),
                "source_file": old.get("source_file"),
                "sha256": old.get("sha256"),
                "source_sha256": old.get("sha256"),
                "previous_source_id": old.get("source_id"),
                "previous_source_file": old.get("source_file"),
                "previous_sha256": old.get("sha256"),
                "previous_source_sha256": old.get("sha256"),
                "status": "deleted",
                "change_status": "deleted",
            }
        )
    return changes


def lifecycle_change_summary(changes: list[dict[str, Any]]) -> dict[str, int]:
    counts = {status: 0 for status in sorted(LIFECYCLE_CHANGE_STATUSES)}
    for change in changes:
        status = change.get("status") or change.get("change_status")
        if status in counts:
            counts[status] += 1
    return counts


def tokenize_query(value: str) -> list[str]:
    value = normalize_text(value).lower()
    terms = re.findall(r"[a-z0-9]+", value)
    cjk = "".join(re.findall(r"[\u3400-\u9fff]", value))
    terms.extend(list(cjk))
    terms.extend(cjk[index : index + 2] for index in range(max(0, len(cjk) - 1)))
    return sorted({term for term in terms if term})


def claim_statement(value: Any) -> str:
    if isinstance(value, dict):
        return normalize_text(str(value.get("statement") or value.get("claim") or value.get("label") or ""))
    return normalize_text(str(value or ""))


def claim_texts(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    return [text for item in values if (text := claim_statement(item))]


def searchable_record_text(record: dict[str, Any]) -> str:
    fields = [
        record.get("name", ""),
        " ".join(record.get("aliases", [])),
        " ".join(record.get("tags", [])),
        record.get("source_excerpt", ""),
        " ".join(claim_texts(record.get("market_context", []))),
        " ".join(claim_texts(record.get("recognition_features", []))),
        " ".join(claim_texts(record.get("trigger", []))),
        " ".join(claim_texts(record.get("entry", []))),
        " ".join(claim_texts(record.get("targets_exit", []))),
    ]
    return normalize_text(" ".join(fields)).lower()


def search_knowledge(output_dir: Path, query: str, limit: int = 5) -> dict[str, Any]:
    if type(limit) is not int or limit < 1:
        raise CliError("query limit must be an integer greater than zero", EXIT_USAGE)
    records_path = output_dir / "pa_records.json"
    chunks_path = output_dir / "chunks.json"
    if not records_path.exists() or not chunks_path.exists():
        raise RuntimeError(f"knowledge output not found under {output_dir}; run build first")
    records = load_json(records_path).get("records", [])
    chunks = load_json(chunks_path).get("chunks", [])
    terms = tokenize_query(query)
    normalized_query = normalize_text(query).lower()
    scored: list[tuple[float, dict[str, Any]]] = []
    for record in records:
        # Inactive records are retained for auditability but never participate
        # in retrieval after their source changed or was otherwise invalidated.
        if record.get("active", True) is False or record.get("record_status") == "invalidated":
            continue
        text = searchable_record_text(record)
        score = 0.0
        if normalized_query and normalized_query in text:
            score += 12.0
        for term in terms:
            if term in text:
                score += 1.0 if len(term) == 1 else 2.5
        if score:
            scored.append((score, record))
    scored.sort(key=lambda item: (-item[0], item[1]["id"]))
    results: list[dict[str, Any]] = []
    chunk_by_id = {chunk["chunk_id"]: chunk for chunk in chunks}
    for score, record in scored[:limit]:
        citations: list[dict[str, Any]] = []
        for ref in record.get("source_refs", []):
            citation = dict(ref)
            chunk = chunk_by_id.get(ref.get("chunk_id"))
            if chunk:
                citation["related_image_pages"] = chunk.get("related_image_pages", [])
                citation["adjacent_chunk_ids"] = chunk.get("adjacent_chunk_ids", [])
            citations.append(citation)
        results.append(
            {
                "record_id": record["id"],
                "name": record["name"],
                "type": record["type"],
                "score": round(score, 3),
                "logic": {
                    "market_context": record.get("market_context", []),
                    "recognition_features": record.get("recognition_features", []),
                    "trigger": record.get("trigger", []),
                    "entry": record.get("entry", []),
                    "invalidation_stop": record.get("invalidation_stop", []),
                    "targets_exit": record.get("targets_exit", []),
                    "risk_notes": record.get("risk_notes", []),
                },
                "citations": citations,
                "source_fidelity": record.get("source_fidelity"),
            }
        )

    if not results:
        chunk_scored: list[tuple[float, dict[str, Any]]] = []
        for chunk in chunks:
            text = normalize_text(
                " ".join(chunk.get("title_path", [])) + " " + chunk.get("source_text", "")
            ).lower()
            score = 12.0 if normalized_query in text else 0.0
            score += sum(1.0 if len(term) == 1 else 2.0 for term in terms if term in text)
            if score:
                chunk_scored.append((score, chunk))
        chunk_scored.sort(key=lambda item: (-item[0], item[1]["chunk_id"]))
        for score, chunk in chunk_scored[:limit]:
            results.append(
                {
                    "record_id": None,
                    "name": " / ".join(chunk.get("title_path", [])),
                    "type": "source_chunk",
                    "score": round(score, 3),
                    "logic": {"source_excerpt": chunk.get("source_text", "")[:1800]},
                    "citations": [
                        {
                            "chunk_id": chunk["chunk_id"],
                            "source_file": chunk["source_file"],
                            "page_start": chunk["page_start"],
                            "page_end": chunk["page_end"],
                            "title_path": chunk["title_path"],
                            "related_image_pages": chunk.get("related_image_pages", []),
                        }
                    ],
                    "source_fidelity": chunk.get("provenance_status"),
                }
            )
    return {"query": query, "count": len(results), "results": results}


def _schema_error_text(error: Any) -> str:
    path = ".".join(str(part) for part in getattr(error, "absolute_path", []))
    prefix = f"{path}: " if path else ""
    return prefix + str(getattr(error, "message", error))


def validate_json_artifact(value: Any, schema_filename: str) -> list[str]:
    """Validate one generated artifact against its versioned JSON Schema."""
    if jsonschema is None:
        return ["jsonschema dependency is unavailable"]
    schema_path = SCHEMA_DIR / schema_filename
    try:
        schema = load_json(schema_path)
        validator_cls = jsonschema.validators.validator_for(schema)
        validator_cls.check_schema(schema)
        validator = validator_cls(schema)
        return [_schema_error_text(error) for error in sorted(validator.iter_errors(value), key=_schema_error_text)]
    except Exception as exc:
        return [f"{schema_filename}: {type(exc).__name__}: {exc}"]


def validate_artifact_files(output_dir: Path) -> dict[str, list[str]]:
    """Load and schema-validate each explicitly generated JSON artifact."""
    errors: dict[str, list[str]] = {}
    for relative_path, schema_filename in ARTIFACT_SCHEMAS.items():
        path = output_dir / relative_path
        if not path.exists():
            # The quality report is assembled by validate_output itself and
            # is checked against its schema immediately before return.
            if relative_path == "validation/quality_report.json":
                continue
            errors[relative_path] = ["artifact is missing"]
            continue
        try:
            value = load_json(path)
        except Exception as exc:
            errors[relative_path] = [f"unreadable JSON: {type(exc).__name__}: {exc}"]
            continue
        artifact_errors = validate_json_artifact(value, schema_filename)
        if artifact_errors:
            errors[relative_path] = artifact_errors
    extracted_root = output_dir / "extracted"
    if extracted_root.is_dir():
        for diagnostic_path in sorted(extracted_root.glob("*/diagnostic.json")):
            relative_path = diagnostic_path.relative_to(output_dir).as_posix()
            try:
                value = load_json(diagnostic_path)
            except Exception as exc:
                errors[relative_path] = [f"unreadable JSON: {type(exc).__name__}: {exc}"]
                continue
            artifact_errors = validate_json_artifact(value, DIAGNOSTIC_SCHEMA_FILENAME)
            if artifact_errors:
                errors[relative_path] = artifact_errors
    return errors


def schema_value_errors(chunks: list[dict[str, Any]], records: list[dict[str, Any]]) -> list[str]:
    """Validate the generated values against the small local schema.

    The project intentionally has no external JSON-schema dependency.  This
    covers the type, required-value, range, and hash invariants that matter for
    the generated corpus while keeping the CLI self-contained.
    """
    errors: list[str] = []
    for chunk in chunks:
        cid = chunk.get("chunk_id", "unknown")
        string_fields = ["chunk_id", "source_file", "source_sha256", "source_text", "semantic_boundary"]
        for field in string_fields:
            value = chunk.get(field)
            if not isinstance(value, str) or not value:
                errors.append(f"chunk {cid}: {field} must be a non-empty string")
        if isinstance(chunk.get("source_sha256"), str) and not re.fullmatch(r"[0-9a-f]{64}", chunk["source_sha256"]):
            errors.append(f"chunk {cid}: source_sha256 is not a SHA-256 hex digest")
        for field in ("page_start", "page_end"):
            value = chunk.get(field)
            if type(value) is not int or value < 1:
                errors.append(f"chunk {cid}: {field} must be an integer >= 1")
        if type(chunk.get("page_start")) is int and type(chunk.get("page_end")) is int and chunk["page_end"] < chunk["page_start"]:
            errors.append(f"chunk {cid}: page_end precedes page_start")
        if not isinstance(chunk.get("title_path"), list) or not chunk["title_path"] or not all(isinstance(item, str) and item for item in chunk["title_path"]):
            errors.append(f"chunk {cid}: title_path must be a non-empty string list")
        if not isinstance(chunk.get("parent_chunk_id"), str):
            errors.append(f"chunk {cid}: parent_chunk_id must be a string")
        if not isinstance(chunk.get("adjacent_chunk_ids"), list) or not all(isinstance(item, str) for item in chunk["adjacent_chunk_ids"]):
            errors.append(f"chunk {cid}: adjacent_chunk_ids must be a string list")

    for record in records:
        rid = record.get("id", "unknown")
        for field in ("id", "type", "name", "knowledge_base_version"):
            value = record.get(field)
            if not isinstance(value, str) or not value:
                errors.append(f"record {rid}: {field} must be a non-empty string")
        for field in ("aliases", "tags", "source_refs"):
            value = record.get(field)
            if not isinstance(value, list):
                errors.append(f"record {rid}: {field} must be a list")
        confidence = record.get("confidence")
        if type(confidence) not in (int, float) or not 0 <= confidence <= 1:
            errors.append(f"record {rid}: confidence must be a number in [0, 1]")
    return errors


def claim_overlap(statement: str, source_text: str) -> bool:
    """Require a strict lexical link for claims declared as source-derived.

    A one-character CJK statement is not evidence.  CJK claims need one
    contiguous two-character overlap; ASCII claims need an exact statement or
    an alphanumeric token of length >= 2.  This prevents unrelated single
    characters such as ``买`` or ``入/买`` from passing provenance checks.
    """
    normalized_statement = normalize_text(statement).lower()
    normalized_source = normalize_text(source_text).lower()
    if not normalized_statement or not normalized_source:
        return False
    words = [word for word in re.findall(r"[a-z0-9]{2,}", normalized_statement) if word]
    cjk = "".join(re.findall(r"[\u3400-\u9fff]", normalized_statement))
    # Do not let a one-character CJK statement pass through the exact branch.
    # ASCII tokens and two-or-more CJK characters establish the minimum claim
    # specificity required for exact matching.
    if not words and len(cjk) < 2:
        return False
    if normalized_statement in normalized_source:
        return True
    if any(word in normalized_source for word in words):
        return True
    if len(cjk) >= 2 and any(cjk[index : index + 2] in normalized_source for index in range(len(cjk) - 1)):
        return True
    return False


def normalized_retrieval_text(source_text: str) -> str:
    """Normalize retrieval text for exact/substring duplication checks."""
    without_notes = _strip_visual_notes(source_text or "")
    without_markers = re.sub(r"(?m)^\[Page \d+\]\s*$", "", without_notes)
    return normalize_text(without_markers).lower()


def validate_claim_provenance(
    records: list[dict[str, Any]],
    chunks: list[dict[str, Any]],
    docs: dict[str, dict[str, Any]],
) -> list[str]:
    """Validate declaration types, claim refs, hashes and source linkage."""
    errors: list[str] = []
    chunk_by_id = {chunk.get("chunk_id"): chunk for chunk in chunks}
    seen_claim_ids: set[str] = set()
    for record in records:
        record_id = str(record.get("id", "unknown"))
        for field in CLAIM_FIELDS:
            values = record.get(field, [])
            if not isinstance(values, list):
                continue
            for index, claim in enumerate(values):
                path = f"{record_id}.{field}[{index}]"
                if not isinstance(claim, dict):
                    errors.append(f"{path}: claim must be an object")
                    continue
                claim_id = claim.get("claim_id")
                if not isinstance(claim_id, str) or not claim_id:
                    errors.append(f"{path}: claim_id must be a non-empty string")
                elif claim_id in seen_claim_ids:
                    errors.append(f"{path}: duplicate claim_id {claim_id}")
                else:
                    seen_claim_ids.add(claim_id)
                statement = claim.get("statement")
                if not isinstance(statement, str) or not normalize_text(statement):
                    errors.append(f"{path}: statement must be a non-empty string")
                declaration = claim.get("declaration_type")
                if declaration not in DECLARATION_TYPES:
                    errors.append(f"{path}: invalid declaration_type {declaration!r}")
                if declaration in {"derived_interpretation", "source_not_stated"} and not normalize_text(str(claim.get("rationale") or "")):
                    errors.append(f"{path}: {declaration} requires a non-empty rationale")
                refs = claim.get("source_refs")
                if not isinstance(refs, list) or not refs:
                    errors.append(f"{path}: source_refs must be a non-empty list")
                    continue
                visual_page = claim.get("visual_page")
                if visual_page is not None and type(visual_page) is not int:
                    errors.append(f"{path}: visual_page must be an integer when provided")
                referenced_source_text = ""
                visual_page_supported = False
                for ref_index, ref in enumerate(refs):
                    ref_path = f"{path}.source_refs[{ref_index}]"
                    if not isinstance(ref, dict):
                        errors.append(f"{ref_path}: citation must be an object")
                        continue
                    chunk_id = ref.get("chunk_id")
                    if chunk_id is None:
                        # Supplemental external citations have no local text or
                        # page coordinates.  They can describe a derived or
                        # explicitly-not-stated claim, but must never be
                        # promoted to a source assertion.  This check is per
                        # citation so a mixed local/external list cannot hide
                        # an external ref behind one valid local ref.
                        if declaration == "source_assertion":
                            errors.append(f"{ref_path}: source_assertion requires a local chunk/page citation")
                        if ref.get("provenance_status") != "supplemental_external_due_to_pdf_failure":
                            errors.append(f"{ref_path}: supplemental citation has invalid provenance_status")
                        external_url = ref.get("external_url")
                        if not isinstance(external_url, str) or not normalize_text(external_url):
                            errors.append(f"{ref_path}: supplemental citation requires a non-empty external_url")
                        if "page_start" not in ref or "page_end" not in ref or ref.get("page_start") is not None or ref.get("page_end") is not None:
                            errors.append(f"{ref_path}: supplemental citation page_start/page_end must be null")
                        source_doc = docs.get(str(ref.get("source_id")))
                        if not source_doc:
                            errors.append(f"{ref_path}: supplemental citation source_id is missing from manifest")
                        else:
                            if ref.get("source_file") != source_doc.get("source_file"):
                                errors.append(f"{ref_path}: supplemental source_file disagrees with manifest")
                            if ref.get("source_sha256") != source_doc.get("sha256"):
                                errors.append(f"{ref_path}: supplemental source_sha256 disagrees with manifest")
                        continue
                    chunk = chunk_by_id.get(chunk_id)
                    if not chunk:
                        errors.append(f"{ref_path}: citation points to missing chunk {chunk_id}")
                        continue
                    for key in ("source_id", "source_file", "source_sha256", "page_start", "page_end", "title_path", "source_locator", "provenance_status"):
                        if ref.get(key) != chunk.get(key):
                            errors.append(f"{ref_path}: {key} disagrees with chunk")
                    source_doc = docs.get(str(chunk.get("source_id")))
                    if not source_doc:
                        errors.append(f"{ref_path}: citation source_id is missing from manifest")
                    else:
                        if ref.get("source_sha256") != source_doc.get("sha256"):
                            errors.append(f"{ref_path}: source_sha256 disagrees with manifest")
                        page_count = source_doc.get("page_count")
                        start = ref.get("page_start")
                        end = ref.get("page_end")
                        if source_doc.get("status") == "failed" or type(page_count) is not int:
                            errors.append(f"{ref_path}: citation source has no usable local page range")
                        elif not (type(start) is int and type(end) is int and 1 <= start <= end <= page_count):
                            errors.append(f"{ref_path}: page range is outside source")
                    include_visual_notes = False
                    if type(visual_page) is int:
                        start = ref.get("page_start")
                        end = ref.get("page_end")
                        if isinstance(start, int) and isinstance(end, int) and start <= visual_page <= end:
                            if visual_page in chunk.get("related_image_pages", []):
                                include_visual_notes = True
                                visual_page_supported = True
                    chunk_text = str(chunk.get("source_text") or "")
                    lexical_text = chunk_text if include_visual_notes else _strip_visual_notes(chunk_text)
                    if include_visual_notes:
                        visual_note = normalize_text(
                            str(chunk.get("retrieval_visual_notes") or chunk.get("visual_notes") or "")
                        )
                        if visual_note:
                            lexical_text += "\n" + visual_note
                    referenced_source_text += "\n" + lexical_text
                if visual_page is not None and not visual_page_supported:
                    errors.append(
                        f"{path}: visual_page {visual_page} does not resolve to any cited chunk image page"
                    )
                # Unsupported free-form strings should not pass merely because
                # a declaration type was supplied.  External supplemental
                # records have no local source text and are validated through
                # their explicit URL instead.
                if declaration in {"source_assertion", "derived_interpretation"} and referenced_source_text:
                    if not claim_overlap(str(statement), referenced_source_text):
                        errors.append(
                            f"{path}: {declaration} statement has no lexical support in referenced source"
                        )
    return errors


def validate_output(output_dir: Path) -> dict[str, Any]:
    manifest = load_json(output_dir / "manifest.json")
    chunks = load_json(output_dir / "chunks.json").get("chunks", [])
    records = load_json(output_dir / "pa_records.json").get("records", [])
    docs = {doc["source_id"]: doc for doc in manifest.get("documents", [])}
    checks: list[dict[str, Any]] = []
    warnings: list[str] = []

    artifact_schema_errors = validate_artifact_files(output_dir)
    checks.append(
        {
            "name": "versioned JSON schemas validate generated artifacts",
            "status": "pass" if not artifact_schema_errors else "fail",
            "details": {
                "schemas": {
                    relative: ARTIFACT_SCHEMAS[relative]
                    for relative in ARTIFACT_SCHEMAS
                    if relative != "validation/quality_report.json"
                },
                "errors": artifact_schema_errors,
            },
        }
    )

    statuses = [doc.get("status") for doc in docs.values()]
    checks.append(
        {
            "name": "all source PDFs attempted",
            "status": "pass" if len(docs) == len(set(docs)) and all(doc.get("attempted") for doc in docs.values()) else "fail",
            "details": {"attempted": len(docs), "statuses": dict(Counter(statuses))},
        }
    )
    extracted_root = output_dir / "extracted"
    extracted_source_ids = {
        child.name for child in extracted_root.iterdir() if child.is_dir()
    } if extracted_root.is_dir() else set()
    manifest_source_ids = set(docs)
    checks.append(
        {
            "name": "manifest and extracted directories agree",
            "status": "pass" if extracted_source_ids == manifest_source_ids else "fail",
            "details": {
                "manifest_source_ids": sorted(manifest_source_ids),
                "extracted_source_ids": sorted(extracted_source_ids),
                "missing_extracted_directories": sorted(manifest_source_ids - extracted_source_ids),
                "unexpected_extracted_directories": sorted(extracted_source_ids - manifest_source_ids),
            },
        }
    )

    # Lifecycle metadata is intentionally self-describing.  Older snapshots
    # (before G005) may not have a ``changes`` list, so validate it when
    # present and otherwise leave the historical output readable.
    lifecycle_errors: list[str] = []
    lifecycle_changes = manifest.get("changes", [])
    if lifecycle_changes is not None:
        if not isinstance(lifecycle_changes, list):
            lifecycle_errors.append("manifest.changes must be a list")
        else:
            for index, change in enumerate(lifecycle_changes):
                if not isinstance(change, dict):
                    lifecycle_errors.append(f"change {index} must be an object")
                    continue
                status = change.get("status") or change.get("change_status")
                if status not in LIFECYCLE_CHANGE_STATUSES:
                    lifecycle_errors.append(f"change {index}: invalid status {status!r}")
                digest = change.get("sha256")
                if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
                    lifecycle_errors.append(f"change {index}: sha256 must be a SHA-256 hex digest")
                if status in {"changed", "renamed"}:
                    previous_digest = change.get("previous_sha256")
                    if not isinstance(previous_digest, str) or not re.fullmatch(r"[0-9a-f]{64}", previous_digest):
                        lifecycle_errors.append(f"change {index}: previous_sha256 must be a SHA-256 hex digest")
    checks.append(
        {
            "name": "manifest lifecycle changes and SHA-256 statuses",
            "status": "pass" if not lifecycle_errors else "fail",
            "details": {
                "changes": len(lifecycle_changes) if isinstance(lifecycle_changes, list) else 0,
                "summary": manifest.get("change_summary", {}),
                "errors": lifecycle_errors,
            },
        }
    )

    # A newly encountered source may be safely extracted to pages, but it is
    # not a usable knowledge source until semantic chunk boundaries and record
    # mappings are configured.  This check deliberately fails overall quality
    # so ``build`` returns the G004 quality-failure code and does not publish a
    # partial formal snapshot.
    records_by_source: Counter[str] = Counter(str(record.get("source_id")) for record in records)
    semantic_chunks_by_source: Counter[str] = Counter(
        str(chunk.get("source_id")) for chunk in chunks if chunk.get("is_retrieval_chunk")
    )
    incomplete_sources: list[dict[str, Any]] = []
    for source_id, doc in docs.items():
        semantic_status = doc.get("semantic_configuration_status")
        normalization_status = doc.get("normalization_status")
        # Legacy manifests use known_initial_source as their implicit
        # configuration marker.
        if semantic_status is None:
            semantic_status = "configured" if doc.get("known_initial_source", True) else "pending"
        if normalization_status is None:
            normalization_status = "configured" if doc.get("known_initial_source", True) else "pending"
        if semantic_status != "configured" or normalization_status != "configured":
            incomplete_sources.append(
                {
                    "source_id": source_id,
                    "source_file": doc.get("source_file"),
                    "semantic_configuration_status": semantic_status,
                    "normalization_status": normalization_status,
                    "retrieval_chunks": semantic_chunks_by_source.get(source_id, 0),
                    "normalized_records": records_by_source.get(source_id, 0),
                    "reason": "semantic configuration and normalized records are pending",
                }
            )
    checks.append(
        {
            "name": "all sources have semantic configuration and normalization",
            "status": "pass" if not incomplete_sources else "fail",
            "details": {"incomplete_sources": incomplete_sources},
        }
    )

    changed_source_ids = {
        str(doc.get("source_id")) for doc in docs.values() if doc.get("change_status") == "changed"
    }
    pending_source_ids = {
        str(doc.get("source_id")) for doc in docs.values() if source_requires_reverification(doc)
    }
    stale_active_records: list[str] = []
    for record in records:
        if str(record.get("source_id")) in pending_source_ids and (
            record.get("active", True) is not False
            or record.get("record_status") != "invalidated"
            or record.get("source_fidelity") == "source_faithful_with_normalization"
        ):
            stale_active_records.append(str(record.get("id", "unknown")))
    checks.append(
        {
            "name": "changed-source records require re-verification",
            "status": "pass" if not stale_active_records else "fail",
            "details": {
                "changed_source_ids": sorted(changed_source_ids),
                "pending_source_ids": sorted(pending_source_ids),
                "stale_active_records": stale_active_records,
            },
        }
    )
    schema_path = ROOT / "knowledge" / "schema.json"
    schema_errors: list[str] = []
    try:
        schema_index = load_json(schema_path)
        declared = schema_index.get("schemas", {})
        for relative_path, schema_filename in ARTIFACT_SCHEMAS.items():
            if declared.get(relative_path) != f"schemas/{schema_filename}":
                schema_errors.append(f"schema index mapping missing or stale: {relative_path}")
            schema_file = SCHEMA_DIR / schema_filename
            if not schema_file.exists():
                schema_errors.append(f"versioned schema missing: {schema_file}")
        if schema_index.get("schemas", {}).get("extracted/<source_id>/diagnostic.json") != f"schemas/{DIAGNOSTIC_SCHEMA_FILENAME}":
            schema_errors.append("schema index mapping missing or stale: extracted/<source_id>/diagnostic.json")
        if not (SCHEMA_DIR / DIAGNOSTIC_SCHEMA_FILENAME).exists():
            schema_errors.append(f"versioned schema missing: {SCHEMA_DIR / DIAGNOSTIC_SCHEMA_FILENAME}")
    except Exception as exc:
        schema_errors.append(f"schema index load/shape error: {type(exc).__name__}: {exc}")
    checks.append(
        {
            "name": "declared schema covers generated layers",
            "status": "pass" if not schema_errors else "fail",
            "details": {"schema_file": str(schema_path.relative_to(ROOT)), "errors": schema_errors},
        }
    )
    schema_value_issues = schema_value_errors(chunks, records)
    checks.append(
        {
            "name": "generated values satisfy schema types and ranges",
            "status": "pass" if not schema_value_issues else "fail",
            "details": {"errors": schema_value_issues},
        }
    )
    hash_mismatches: list[str] = []
    missing_source_files: list[str] = []
    for doc in docs.values():
        source_path = ROOT / doc["source_file"]
        if not source_path.exists():
            missing_source_files.append(doc["source_file"])
        elif sha256_file(source_path) != doc.get("sha256"):
            hash_mismatches.append(doc["source_file"])
    checks.append(
        {
            "name": "manifest source hashes match local files",
            "status": "pass" if not hash_mismatches and not missing_source_files else "fail",
            "details": {"hash_mismatches": hash_mismatches, "missing_source_files": missing_source_files},
        }
    )
    if any(status == "failed" for status in statuses):
        warnings.extend(
            f"{doc['source_file']}: {doc.get('failure_kind', 'processing failed')}" for doc in docs.values() if doc.get("status") == "failed"
        )

    missing_chunk_fields = {
        chunk.get("chunk_id", "unknown"): [field for field in REQUIRED_CHUNK_FIELDS if field not in chunk]
        for chunk in chunks
        if any(field not in chunk for field in REQUIRED_CHUNK_FIELDS)
    }
    page_range_errors: list[str] = []
    for chunk in chunks:
        doc = docs.get(chunk.get("source_id"))
        if not doc or doc.get("status") == "failed":
            continue
        page_count = doc.get("page_count") or 0
        if not isinstance(chunk.get("page_start"), int) or not isinstance(chunk.get("page_end"), int):
            page_range_errors.append(chunk.get("chunk_id", "unknown"))
        elif chunk["page_start"] < 1 or chunk["page_end"] < chunk["page_start"] or chunk["page_end"] > page_count:
            page_range_errors.append(chunk.get("chunk_id", "unknown"))
    checks.append(
        {
            "name": "source chunk schema and page ranges",
            "status": "pass" if not missing_chunk_fields and not page_range_errors else "fail",
            "details": {"missing_fields": missing_chunk_fields, "page_range_errors": page_range_errors},
        }
    )

    chunk_by_id = {chunk.get("chunk_id"): chunk for chunk in chunks}
    duplicate_chunk_ids = [chunk_id for chunk_id, count in Counter(chunk.get("chunk_id") for chunk in chunks).items() if count > 1]
    duplicate_record_ids = [record_id for record_id, count in Counter(record.get("id") for record in records).items() if count > 1]
    relation_errors: list[str] = []
    provenance_errors: list[str] = []
    continuity_errors: list[str] = []
    for chunk in chunks:
        chunk_id = chunk.get("chunk_id") or "unknown"
        source_doc = docs.get(chunk.get("source_id"))
        if source_doc:
            if chunk.get("source_file") != source_doc.get("source_file"):
                provenance_errors.append(f"{chunk_id}: source_file does not match manifest")
            if chunk.get("source_sha256") != source_doc.get("sha256"):
                provenance_errors.append(f"{chunk_id}: source_sha256 does not match manifest")
        else:
            provenance_errors.append(f"{chunk_id}: source_id is missing from manifest")
        parent_id = chunk.get("parent_chunk_id")
        if chunk_id.endswith(":root"):
            if parent_id not in ("", None):
                relation_errors.append(f"{chunk_id}: root must not have a parent")
        elif not parent_id or parent_id not in chunk_by_id:
            relation_errors.append(f"{chunk_id}: missing parent {parent_id!r}")
        elif chunk_by_id[parent_id].get("source_id") != chunk.get("source_id"):
            relation_errors.append(f"{chunk_id}: parent belongs to another source")
        adjacent = chunk.get("adjacent_chunk_ids", [])
        if len(adjacent) != len(set(adjacent)):
            relation_errors.append(f"{chunk_id}: duplicate adjacent chunk id")
        for adjacent_id in adjacent:
            neighbor = chunk_by_id.get(adjacent_id)
            if not neighbor:
                relation_errors.append(f"{chunk_id}: missing adjacent chunk {adjacent_id}")
            elif neighbor.get("source_id") != chunk.get("source_id"):
                relation_errors.append(f"{chunk_id}: adjacent chunk belongs to another source")
            elif adjacent_id == chunk_id:
                relation_errors.append(f"{chunk_id}: cannot be adjacent to itself")
            elif chunk_id not in neighbor.get("adjacent_chunk_ids", []):
                relation_errors.append(f"{chunk_id}: adjacency is not reciprocal with {adjacent_id}")
        if chunk.get("page_range") != [chunk.get("page_start"), chunk.get("page_end")]:
            relation_errors.append(f"{chunk_id}: page_range disagrees with page_start/page_end")
        if chunk.get("is_cross_page"):
            markers = {int(value) for value in re.findall(r"^\[Page (\d+)\]$", chunk.get("source_text", ""), flags=re.MULTILINE)}
            expected = set(range(chunk.get("page_start", 0), chunk.get("page_end", 0) + 1))
            missing_markers = sorted(expected - markers)
            if missing_markers:
                continuity_errors.append(f"{chunk_id}: missing page markers {missing_markers}")
    checks.append(
        {
            "name": "chunk parent/adjacent relations and cross-page continuity",
            "status": "pass" if not duplicate_chunk_ids and not duplicate_record_ids and not relation_errors and not provenance_errors and not continuity_errors else "fail",
            "details": {
                "duplicate_chunk_ids": duplicate_chunk_ids,
                "duplicate_record_ids": duplicate_record_ids,
                "relation_errors": relation_errors,
                "provenance_errors": provenance_errors,
                "continuity_errors": continuity_errors,
            },
        }
    )

    semantic_issues = [
        chunk.get("chunk_id", "unknown")
        for chunk in chunks
        if chunk.get("is_retrieval_chunk") and (not chunk.get("semantic_boundary") or "fixed-size" in chunk.get("semantic_boundary", ""))
    ]
    retrieval_chunks = [chunk for chunk in chunks if chunk.get("is_retrieval_chunk")]
    empty_retrieval = [chunk.get("chunk_id", "unknown") for chunk in retrieval_chunks if not chunk.get("source_text")]
    duplicate_texts = [
        digest
        for digest, count in Counter(chunk.get("source_text_sha256") for chunk in retrieval_chunks).items()
        if count > 1
    ]
    retrieval_texts = {
        str(chunk.get("chunk_id", "unknown")): normalized_retrieval_text(str(chunk.get("source_text") or ""))
        for chunk in retrieval_chunks
    }
    min_overlap_chars = 80
    exact_retrieval_pairs: list[list[str]] = []
    contained_retrieval_pairs: list[list[str]] = []
    retrieval_ids = sorted(retrieval_texts)
    for index, left_id in enumerate(retrieval_ids):
        left = retrieval_texts[left_id]
        if len(left) < min_overlap_chars:
            continue
        for right_id in retrieval_ids[index + 1 :]:
            right = retrieval_texts[right_id]
            if len(right) < min_overlap_chars:
                continue
            pair = [left_id, right_id]
            if left == right:
                exact_retrieval_pairs.append(pair)
            elif left in right or right in left:
                contained_retrieval_pairs.append(pair)
    checks.append(
        {
            "name": "semantic chunks preserve text and non-mechanical boundaries",
            "status": "pass"
            if not semantic_issues
            and not empty_retrieval
            and not duplicate_texts
            and not exact_retrieval_pairs
            and not contained_retrieval_pairs
            else "fail",
            "details": {
                "semantic_issues": semantic_issues,
                "empty_retrieval": empty_retrieval,
                "duplicate_retrieval_text_hashes": duplicate_texts,
                "exact_retrieval_pairs": exact_retrieval_pairs,
                "contained_retrieval_pairs": contained_retrieval_pairs,
                "normalized_overlap_min_chars": min_overlap_chars,
                "short_retrieval_chunks_ignored": sorted(
                    chunk_id for chunk_id, text in retrieval_texts.items() if len(text) < min_overlap_chars
                ),
            },
        }
    )

    coverage: dict[str, Any] = {}
    coverage_failures: list[str] = []
    for source_id, doc in docs.items():
        if doc.get("status") == "failed" or not doc.get("page_count"):
            continue
        page_numbers: set[int] = set()
        for chunk in chunks:
            if chunk.get("source_id") == source_id and chunk.get("chunk_id") != f"{source_id}:root":
                page_numbers.update(range(chunk["page_start"], chunk["page_end"] + 1))
        missing = [number for number in range(1, doc["page_count"] + 1) if number not in page_numbers]
        coverage[source_id] = {"page_count": doc["page_count"], "pages_with_semantic_or_context_chunk": len(page_numbers), "missing_pages": missing}
        if missing:
            coverage_failures.append(source_id)
    checks.append(
        {
            "name": "successful document page coverage",
            "status": "pass" if not coverage_failures else "fail",
            "details": coverage,
        }
    )

    record_field_errors: dict[str, list[str]] = {}
    record_logic_shape_errors: dict[str, list[str]] = {}
    citation_errors: list[str] = []
    record_integrity_errors: list[str] = []
    for record in records:
        record_id = record.get("id", "unknown")
        missing = [field for field in REQUIRED_RECORD_FIELDS if field not in record or record[field] in (None, "", [])]
        if missing:
            record_field_errors[record_id] = missing
        bad_logic_shape = [field for field in NORMALIZED_LOGIC_FIELDS if not isinstance(record.get(field), list)]
        if bad_logic_shape:
            record_logic_shape_errors[record_id] = bad_logic_shape
        refs = record.get("source_refs")
        if not isinstance(refs, list) or not refs:
            citation_errors.append(record_id)
            continue

        source_id = record.get("source_id")
        source_id_key = source_id if isinstance(source_id, str) else str(source_id)
        if source_id_key not in docs:
            record_integrity_errors.append(f"{record_id}: source_id is missing from manifest")
        ref_source_ids = {
            ref.get("source_id")
            for ref in refs
            if isinstance(ref, dict) and isinstance(ref.get("source_id"), str)
        }
        if ref_source_ids != {source_id_key}:
            record_integrity_errors.append(
                f"{record_id}: source_refs source_id set {sorted(str(value) for value in ref_source_ids)} "
                f"does not match record source_id {source_id!r}"
            )
        ref_source_files = {
            ref.get("source_file")
            for ref in refs
            if isinstance(ref, dict) and ref.get("source_file")
        }
        record_source_files = record.get("source_files")
        if not isinstance(record_source_files, list) or set(record_source_files) != ref_source_files:
            record_integrity_errors.append(
                f"{record_id}: source_files does not match non-empty source_ref source_file set"
            )
    checks.append(
        {
            "name": "normalized PA record fields and citations",
            "status": "pass" if not record_field_errors and not record_logic_shape_errors and not citation_errors and not record_integrity_errors else "fail",
            "details": {
                "record_count": len(records),
                "missing_fields": record_field_errors,
                "logic_fields_not_lists": record_logic_shape_errors,
                "missing_source_refs": citation_errors,
                "record_integrity_errors": record_integrity_errors,
            },
        }
    )

    claim_provenance_errors = validate_claim_provenance(records, chunks, docs)
    checks.append(
        {
            "name": "claim declaration types and claim-level provenance",
            "status": "pass" if not claim_provenance_errors else "fail",
            "details": {"errors": claim_provenance_errors},
        }
    )

    visual_records = [
        record["id"]
        for record in records
        if any(ref.get("chunk_id") and ref.get("page_start") is not None for ref in record.get("source_refs", []))
        and any(chunk.get("related_image_pages") for chunk in chunks if chunk.get("chunk_id") in {ref.get("chunk_id") for ref in record.get("source_refs", [])})
    ]
    checks.append(
        {
            "name": "chart/page visual linkage",
            "status": "pass" if visual_records else "warn",
            "details": {"records_with_related_image_pages": visual_records},
        }
    )

    visual_ref_errors: list[str] = []
    for chunk in chunks:
        source_doc = docs.get(str(chunk.get("source_id")))
        image_pages = set(source_doc.get("image_pages", []) if source_doc else [])
        start, end = chunk.get("page_start"), chunk.get("page_end")
        for page in chunk.get("related_image_pages", []):
            if type(page) is not int or not isinstance(start, int) or not isinstance(end, int) or not start <= page <= end:
                visual_ref_errors.append(f"{chunk.get('chunk_id', 'unknown')}: related image page is outside chunk range")
            if image_pages and page not in image_pages:
                visual_ref_errors.append(f"{chunk.get('chunk_id', 'unknown')}: related image page is absent from manifest image_pages")
    visual_index_path = output_dir / "visual_index.json"
    if visual_index_path.exists():
        try:
            visual_index = load_json(visual_index_path)
            index_docs = {item.get("source_id"): item for item in visual_index.get("documents", [])}
            if set(index_docs) != set(docs):
                visual_ref_errors.append("visual_index documents do not match manifest documents")
            for source_id, item in index_docs.items():
                if source_id not in docs:
                    continue
                valid_pages = set(docs[source_id].get("image_pages", []))
                for page in item.get("chunk_visual_refs", []):
                    if page not in valid_pages:
                        visual_ref_errors.append(f"visual_index {source_id}: chunk_visual_refs page {page} is not an image page")
        except Exception as exc:
            visual_ref_errors.append(f"visual_index load error: {type(exc).__name__}: {exc}")
    checks.append(
        {
            "name": "visual references resolve to source image pages",
            "status": "pass" if not visual_ref_errors else "fail",
            "details": {"errors": visual_ref_errors},
        }
    )

    sample_requirements = {
        "rose-zone-buy-sell": ["买入区域", "卖出区域"],
        "rose-caveman": ["Caveman", "信号棒", "止损"],
        "rose-gap-bar": ["Gap Bar", "斐波那契"],
        "rose-mother-bar": ["Mother Bar", "0.66", "止损"],
        "rose-fofs-swing": ["FOFS", "TTR"],
        "nanqiao-h2-l2": ["H2", "L2", "1 tick"],
        "nanqiao-discipline-checklist": ["10 秒", "强制退出"],
    }
    record_by_id = {record["id"]: record for record in records}
    sample_missing: dict[str, list[str]] = {}
    sample_missing_records: list[str] = []
    sample_skipped_for_partial_build: list[str] = []
    for record_id, terms in sample_requirements.items():
        record = record_by_id.get(record_id)
        if not record:
            expected_source_id = infer_record_source(record_id)
            if expected_source_id and expected_source_id not in docs:
                sample_skipped_for_partial_build.append(record_id)
            else:
                sample_missing_records.append(record_id)
            continue
        source_text = record.get("source_excerpt", "").lower()
        missing = [term for term in terms if term.lower() not in source_text]
        if missing:
            sample_missing[record_id] = missing
    checks.append(
        {
            "name": "source-to-normalized sample cross-check",
            "status": "pass" if not sample_missing and not sample_missing_records else "fail",
            "details": {
                "checked_records": [record_id for record_id in sample_requirements if record_id in record_by_id],
                "missing_records": sample_missing_records,
                "skipped_for_partial_build": sample_skipped_for_partial_build,
                "missing_terms": sample_missing,
            },
        }
    )

    citation_integrity_errors: list[str] = []
    for record in records:
        record_id = record.get("id", "unknown")
        for ref in record.get("source_refs", []):
            chunk_id = ref.get("chunk_id")
            if chunk_id is None:
                if ref.get("provenance_status") != "supplemental_external_due_to_pdf_failure" or not ref.get("external_url"):
                    citation_integrity_errors.append(f"{record_id}: malformed supplemental citation")
                continue
            chunk = chunk_by_id.get(chunk_id)
            if not chunk:
                citation_integrity_errors.append(f"{record_id}: citation points to missing chunk {chunk_id}")
                continue
            for field in ("source_id", "source_file", "source_sha256", "page_start", "page_end", "title_path", "source_locator", "provenance_status"):
                if ref.get(field) != chunk.get(field):
                    citation_integrity_errors.append(f"{record_id}/{chunk_id}: citation field {field} disagrees with chunk")
            source_doc = docs.get(chunk.get("source_id"))
            if source_doc and source_doc.get("status") != "failed":
                if not (1 <= chunk.get("page_start", 0) <= chunk.get("page_end", 0) <= source_doc.get("page_count", 0)):
                    citation_integrity_errors.append(f"{record_id}/{chunk_id}: citation page range is outside source")
    checks.append(
        {
            "name": "normalized citations resolve to exact source chunks",
            "status": "pass" if not citation_integrity_errors else "fail",
            "details": {"errors": citation_integrity_errors, "citation_count": sum(len(record.get("source_refs", [])) for record in records)},
        }
    )

    terminology_errors: list[str] = []
    try:
        generated_terminology = load_json(output_dir / "terminology.json")
        for index, term in enumerate(generated_terminology.get("terms", [])):
            for field in ("term_en", "term_zh", "aliases", "source_ids", "note"):
                if field not in term or term[field] in (None, "", []):
                    terminology_errors.append(f"term {index}: missing {field}")
            for source_id in term.get("source_ids", []):
                if source_id not in docs:
                    terminology_errors.append(f"term {index}: unknown source {source_id}")
        for index, conflict in enumerate(generated_terminology.get("conflicts", [])):
            for field in ("topic", "sources", "description"):
                if field not in conflict or conflict[field] in (None, "", []):
                    terminology_errors.append(f"conflict {index}: missing {field}")
            for source_id in conflict.get("sources", []):
                if source_id not in docs:
                    terminology_errors.append(f"conflict {index}: unknown source {source_id}")
    except Exception as exc:
        terminology_errors.append(f"terminology load/shape error: {type(exc).__name__}: {exc}")
    checks.append(
        {
            "name": "terminology aliases and conflict register",
            "status": "pass" if not terminology_errors else "fail",
            "details": {
                "errors": terminology_errors,
                "terms": len(generated_terminology.get("terms", [])) if "generated_terminology" in locals() else 0,
                "conflicts": len(generated_terminology.get("conflicts", [])) if "generated_terminology" in locals() else 0,
            },
        }
    )

    query_checks: list[dict[str, Any]] = []
    query_requirements = [
        ("Mother Bar", "rose-mother-bar"),
        ("Caveman", "rose-caveman"),
        ("Gap Bar", "rose-gap-bar"),
        ("H2 L2", "nanqiao-checklist"),
        ("区间突破", "rose-strategy"),
        ("MTR", "rose-mtr"),
        ("FOFS", "rose-strategy"),
    ]
    skipped_queries: list[str] = []
    for query, required_source_id in query_requirements:
        if required_source_id not in docs:
            skipped_queries.append(query)
            continue
        if source_requires_reverification(docs[required_source_id]):
            # Changed-source normalized records are intentionally inactive
            # until re-verification.  Do not let a different source's broad
            # lexical hit make this source's representative query fail.
            skipped_queries.append(f"{query} (source pending re-verification)")
            continue
        try:
            result = search_knowledge(output_dir, query, limit=3)
            has_citation = any(item.get("citations") for item in result.get("results", []))
            required_source_file = docs[required_source_id].get("source_file")
            required_source_hit = any(
                citation.get("source_file") == required_source_file
                for item in result.get("results", [])
                for citation in item.get("citations", [])
            )
            query_checks.append(
                {
                    "query": query,
                    "status": "pass" if result.get("count") and has_citation and required_source_hit else "fail",
                    "count": result.get("count"),
                    "required_source_hit": required_source_hit,
                    "record_ids": [item.get("record_id") for item in result.get("results", [])],
                }
            )
        except Exception as exc:
            query_checks.append({"query": query, "status": "fail", "error": str(exc)})
    # A partial/renamed fixture may contain no canonical source ids at all; in
    # that case every representative query is intentionally skipped.  The
    # dedicated semantic-completeness check above still rejects genuinely
    # unknown sources, while an otherwise configured renamed source can be
    # published without inventing a canonical query requirement for it.
    checks.append({"name": "representative retrieval queries return citations", "status": "pass" if not query_checks or all(item["status"] == "pass" for item in query_checks) else "fail", "details": {"executed": query_checks, "skipped_for_partial_build": skipped_queries}})

    source_issue_count = sum(len(doc.get("extraction_errors", [])) for doc in docs.values())
    if source_issue_count:
        warnings.append(f"{source_issue_count} source extraction issue(s) recorded; successful documents continue to be indexed")
    if any(record.get("source_fidelity") == "supplemental_external_due_to_pdf_failure" for record in records):
        warnings.append("MTR normalized record is supplemental external data because the local PDF is structurally unreadable")

    overall = "pass" if not any(check["status"] == "fail" for check in checks) else "fail"
    report = {
        "schema_version": KB_VERSION,
        "knowledge_base_version": KB_VERSION,
        "generated_at": now_iso(),
        "overall_status": overall,
        "checks": checks,
        "warnings": warnings,
        "summary": {
            "documents": len(docs),
            "successful_or_partial_documents": sum(status in {"success", "partial"} for status in statuses),
            "partial_documents": sum(status == "partial" for status in statuses),
            "failed_documents": sum(status == "failed" for status in statuses),
            "chunks": len(chunks),
            "retrieval_chunks": sum(chunk.get("is_retrieval_chunk") for chunk in chunks),
            "normalized_records": len(records),
            "ocr_pages": sum(len(doc.get("ocr_pages", [])) for doc in docs.values()),
        },
    }
    # The report is generated by this function, so its schema check is
    # performed against the assembled value and is not itself a source of
    # quality truth.  This still catches malformed report structure before a
    # build is committed or a validate command reports success.
    report_schema_errors = validate_json_artifact(report, ARTIFACT_SCHEMAS["validation/quality_report.json"])
    report["checks"].append(
        {
            "name": "quality report validates against its versioned schema",
            "status": "pass" if not report_schema_errors else "fail",
            "details": {"errors": report_schema_errors},
        }
    )
    if report_schema_errors:
        report["overall_status"] = "fail"
    return report


def write_quality_markdown(output_dir: Path, report: dict[str, Any]) -> None:
    summary = report.get("summary", {})
    checks = report.get("checks", [])
    lines = [
        "# PA knowledge base quality report",
        "",
        f"- KB version: `{report.get('knowledge_base_version', 'unknown')}`",
        f"- Generated: `{report.get('generated_at', now_iso())}`",
        f"- Overall: **{report.get('overall_status', 'fail')}**",
        "",
        "## Summary",
        "",
    ]
    for key, value in summary.items():
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Checks", ""])
    for check in checks:
        lines.append(f"- `{check['status']}` {check['name']}: `{json.dumps(check['details'], ensure_ascii=False)}`")
    if report.get("warnings"):
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- {warning}" for warning in report["warnings"])
    (output_dir / "validation").mkdir(parents=True, exist_ok=True)
    (output_dir / "validation" / "quality_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def report_exit_code(report: dict[str, Any]) -> int:
    if report.get("overall_status") == "fail":
        return EXIT_QUALITY_FAILURE
    summary = report.get("summary", {})
    if summary.get("partial_documents", 0) or summary.get("failed_documents", 0):
        return EXIT_WARNING
    return EXIT_SUCCESS


def build(
    source_dir: Path,
    output_dir: Path,
    only: list[str] | None = None,
    mode: str = "build",
) -> dict[str, Any]:
    source_dir = source_dir.expanduser().resolve()
    output_dir = output_dir.expanduser().resolve()
    selected = select_source_paths(source_dir, only, allow_empty=mode == "update")
    selected_source_ids = {source_id_for_path(path)[0] for path in selected}
    validate_partial_output_target(output_dir, selected_source_ids, only)
    dependency_report = require_runtime_dependencies()

    doc_specs, record_config, terminology = load_configuration()
    previous = load_json(output_dir / "manifest.json") if (output_dir / "manifest.json").exists() else None
    previous_documents = _manifest_documents(previous)
    configured_record_sources = {
        config.get("source_id") or infer_record_source(config.get("id", ""))
        for config in record_config.get("records", [])
    }
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    staging_dir = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.staging-", dir=output_dir.parent))
    render_root = Path(tempfile.mkdtemp(prefix="kb-render-"))

    try:
        documents: list[dict[str, Any]] = []
        pages_by_source: dict[str, list[dict[str, Any]]] = {}
        chunks: list[dict[str, Any]] = []
        # A same-SHA rename of a configured source inherits its semantic spec
        # and normalized mappings under the new source id.  A genuinely new
        # source remains pending and is rejected by validation below.
        rename_aliases: dict[str, str] = {}
        for path in selected:
            source_id, _ = source_id_for_path(path)
            digest = sha256_file(path)
            doc_spec = doc_specs.get(source_id)
            if doc_spec is None:
                candidates = [
                    old
                    for old in sorted(previous_documents, key=lambda item: str(item.get("source_id", "")))
                    if old.get("sha256") == digest
                    and old.get("source_id") in doc_specs
                    and old.get("source_id") not in rename_aliases.values()
                ]
                if candidates:
                    inherited_source_id = str(candidates[0]["source_id"])
                    rename_aliases[source_id] = inherited_source_id
                    doc_spec = doc_specs[inherited_source_id]
            inherited_source_id = rename_aliases.get(source_id)
            if doc_spec is None:
                doc_spec = {"title": path.stem, "chunk_specs": [], "ocr_when_text_chars_below": 80}
            doc, pages = document_result(path, doc_spec, render_root)
            # Keep fixture/mocked document_result implementations compatible
            # with the lifecycle metadata introduced here.
            doc.setdefault("source_id", source_id)
            doc.setdefault("source_file", str(path.relative_to(ROOT).as_posix()) if path.is_relative_to(ROOT) else str(path))
            doc.setdefault("sha256", digest)
            doc.setdefault("known_initial_source", source_id in doc_specs)
            doc["semantic_configuration_status"] = "configured" if doc_spec.get("chunk_specs") or source_id in doc_specs or inherited_source_id else "pending"
            inherited_record_configured = inherited_source_id in configured_record_sources if inherited_source_id else False
            doc["normalization_status"] = "configured" if source_id in configured_record_sources or inherited_record_configured else "pending"
            if inherited_source_id:
                doc["semantic_configuration_source_id"] = inherited_source_id
                doc["renamed_from_source_id"] = inherited_source_id
            documents.append(doc)
            pages_by_source[doc["source_id"]] = pages
            if pages:
                chunks.extend(make_chunks(doc, pages, doc_spec))

        lifecycle_changes = apply_lifecycle_changes(previous, documents)
        # Keep source re-verification state in the current manifest document.
        # Renamed sources use the previous source's config/spec for this
        # purpose, but the rename itself does not clear a pending state.
        effective_doc_specs = dict(doc_specs)
        for renamed_source_id, previous_source_id in rename_aliases.items():
            if previous_source_id in doc_specs:
                effective_doc_specs[renamed_source_id] = doc_specs[previous_source_id]
        apply_source_reverification_state(previous, documents, effective_doc_specs, lifecycle_changes)
        selected_ids = {doc["source_id"] for doc in documents}
        docs_by_id = {doc["source_id"]: doc for doc in documents}
        effective_record_config = {"records": list(record_config.get("records", []))}
        for renamed_source_id, previous_source_id in sorted(rename_aliases.items()):
            for config in record_config.get("records", []):
                configured_source = config.get("source_id") or infer_record_source(config.get("id", ""))
                if configured_source != previous_source_id:
                    continue
                clone = dict(config)
                clone["source_id"] = renamed_source_id
                effective_record_config["records"].append(clone)
        records = build_records(effective_record_config, docs_by_id, chunks, selected_ids)
        filtered_terms: list[dict[str, Any]] = []
        for term in terminology.get("terms", []):
            selected_term_sources = [
                rename_aliases.get(source_id, source_id)
                for source_id in term.get("source_ids", [])
                if rename_aliases.get(source_id, source_id) in selected_ids
            ]
            if selected_term_sources:
                filtered_term = dict(term)
                filtered_term["source_ids"] = sorted(set(selected_term_sources))
                filtered_terms.append(filtered_term)
        filtered_conflicts: list[dict[str, Any]] = []
        for conflict in terminology.get("conflicts", []):
            selected_conflict_sources = [
                rename_aliases.get(source_id, source_id)
                for source_id in conflict.get("sources", [])
                if rename_aliases.get(source_id, source_id) in selected_ids
            ]
            if selected_conflict_sources:
                filtered_conflict = dict(conflict)
                filtered_conflict["sources"] = sorted(set(selected_conflict_sources))
                filtered_conflicts.append(filtered_conflict)
        generated_terminology = {"terms": filtered_terms, "conflicts": filtered_conflicts, "knowledge_base_version": KB_VERSION}

        (staging_dir / "extracted").mkdir(parents=True, exist_ok=True)
        for source_id, pages in pages_by_source.items():
            if pages:
                write_jsonl(staging_dir / "extracted" / source_id / "pages.jsonl", pages)
                full_text = "\n\n".join(page_block(page) for page in pages)
                (staging_dir / "extracted" / source_id / "document.txt").parent.mkdir(parents=True, exist_ok=True)
                (staging_dir / "extracted" / source_id / "document.txt").write_text(full_text + "\n", encoding="utf-8")
            else:
                write_json(staging_dir / "extracted" / source_id / "diagnostic.json", docs_by_id[source_id])

        manifest = {
            "schema_version": KB_VERSION,
            "knowledge_base_version": KB_VERSION,
            "generated_at": now_iso(),
            "mode": mode,
            "source_dir": str(source_dir.relative_to(ROOT).as_posix()) if source_dir.is_relative_to(ROOT) else str(source_dir),
            "documents": documents,
            # Keep the full change ledger in the manifest even though deleted
            # sources are intentionally absent from active ``documents`` and
            # all generated layers.
            "changes": lifecycle_changes,
            "change_summary": lifecycle_change_summary(lifecycle_changes),
            "lifecycle": {
                "mode": mode,
                "run_id": run_id,
                "previous_manifest_generated_at": previous.get("generated_at") if previous else None,
                "changes": lifecycle_changes,
            },
            "processing_summary": {
                "attempted": len(documents),
                "success": sum(doc.get("status") == "success" for doc in documents),
                "partial": sum(doc.get("status") == "partial" for doc in documents),
                "failed": sum(doc.get("status") == "failed" for doc in documents),
            },
        }
        processing_issues = [
            {
                "source_id": doc["source_id"],
                "source_file": doc["source_file"],
                "status": doc["status"],
                "errors": doc.get("extraction_errors", []),
                "failure_kind": doc.get("failure_kind"),
                "supplemental_recovery": doc.get("supplemental_recovery"),
            }
            for doc in documents
            if doc.get("status") != "success" or doc.get("extraction_errors")
        ]
        write_json(staging_dir / "manifest.json", manifest)
        write_json(staging_dir / "chunks.json", {"schema_version": KB_VERSION, "knowledge_base_version": KB_VERSION, "chunks": chunks})
        write_json(staging_dir / "pa_records.json", {"schema_version": KB_VERSION, "knowledge_base_version": KB_VERSION, "records": records})
        generated_terminology["schema_version"] = KB_VERSION
        write_json(staging_dir / "terminology.json", generated_terminology)
        write_json(staging_dir / "processing_issues.json", {"schema_version": KB_VERSION, "knowledge_base_version": KB_VERSION, "issues": processing_issues})
        visual_index = {
            "schema_version": KB_VERSION,
            "knowledge_base_version": KB_VERSION,
            "generated_at": now_iso(),
            "documents": [
                {
                    "source_id": doc["source_id"],
                    "source_file": doc["source_file"],
                    "status": doc["status"],
                    "page_count": doc.get("page_count"),
                    "image_pages": doc.get("image_pages", []),
                    "image_count": doc.get("image_count", 0),
                    "ocr_pages": doc.get("ocr_pages", []),
                    "rendered_for_ocr_pages": doc.get("visual_review", {}).get("rendered_for_ocr_pages", []),
                    "chunk_visual_refs": sorted({page for chunk in chunks if chunk.get("source_id") == doc["source_id"] for page in chunk.get("related_image_pages", [])}),
                }
                for doc in documents
            ],
        }
        write_json(staging_dir / "visual_index.json", visual_index)
        report = validate_output(staging_dir)
        write_json(staging_dir / "validation" / "quality_report.json", report)
        write_quality_markdown(staging_dir, report)

        exit_code = report_exit_code(report)
        result: dict[str, Any] = {
            "mode": mode,
            "output_dir": str(output_dir),
            "documents": manifest["processing_summary"],
            "chunks": len(chunks),
            "records": len(records),
            "quality_status": report["overall_status"],
            "warnings": report.get("warnings", []),
            "committed": False,
            "exit_code": exit_code,
            "runtime": {
                "python": dependency_report["python"]["version"],
                "pypdf": dependency_report["python_packages"]["pypdf"]["version"],
                "executables": {
                    name: item["path"] for name, item in dependency_report["executables"].items()
                },
            },
        }
        if exit_code == EXIT_QUALITY_FAILURE:
            result["status"] = "quality-fail"
            result["warnings"] = [
                *result["warnings"],
                "formal output was not replaced because quality_status=fail",
            ]
            return result

        commit_staged_output(staging_dir, output_dir, run_id)
        staging_dir = None  # ownership moved to output_dir
        result["committed"] = True
        result["status"] = "warning" if exit_code == EXIT_WARNING else "success"
        return result
    finally:
        if staging_dir is not None and staging_dir.exists():
            remove_path(staging_dir)
        if render_root.exists():
            remove_path(render_root)


def positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer greater than zero") from exc
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be an integer greater than zero")
    return parsed


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build and query the local PA knowledge base")
    subparsers = parser.add_subparsers(dest="command", required=True)

    for name in ("build", "update"):
        sub = subparsers.add_parser(name, help="extract, chunk, normalize, and validate")
        sub.add_argument("--source-dir", default=str(DEFAULT_SOURCE_DIR), help="PDF directory")
        sub.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="generated output directory")
        sub.add_argument("--only", nargs="+", metavar="SOURCE", help="source id or filename fragment; requires an isolated --output-dir")

    query = subparsers.add_parser("query", help="search normalized records and return citations")
    query.add_argument("text", nargs="+", help="query text")
    query.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    query.add_argument("--limit", type=positive_int, default=5)

    validate = subparsers.add_parser("validate", help="run schema/provenance/query checks")
    validate.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    return parser.parse_args(argv)


def print_cli_error(error: CliError) -> None:
    payload: dict[str, Any] = {"error": str(error), "exit_code": error.exit_code}
    payload.update(error.details)
    print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(sys.argv[1:] if argv is None else argv)
        if args.command in {"build", "update"}:
            result = build(
                Path(args.source_dir),
                Path(args.output_dir),
                only=args.only,
                mode=args.command,
            )
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return int(result["exit_code"])
        if args.command == "query":
            result = search_knowledge(Path(args.output_dir).expanduser().resolve(), " ".join(args.text), args.limit)
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return EXIT_SUCCESS
        if args.command == "validate":
            output_dir = Path(args.output_dir).expanduser().resolve()
            report = validate_output(output_dir)
            write_json(output_dir / "validation" / "quality_report.json", report)
            write_quality_markdown(output_dir, report)
            print(json.dumps(report, ensure_ascii=False, indent=2))
            return report_exit_code(report)
        return EXIT_USAGE
    except CliError as exc:
        print_cli_error(exc)
        return exc.exit_code
    except Exception as exc:
        # Keep process code 1 reserved for usable source-warning snapshots.
        message = f"{type(exc).__name__}: {exc}" if str(exc) else type(exc).__name__
        print_cli_error(CliError(message, EXIT_RUNTIME_FAILURE))
        return EXIT_RUNTIME_FAILURE


if __name__ == "__main__":
    raise SystemExit(main())
