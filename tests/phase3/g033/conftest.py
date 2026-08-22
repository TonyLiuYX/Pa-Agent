from __future__ import annotations

import hashlib
import http.client
import importlib.util
import io
import json
import threading
import zipfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Iterator

from pypdf import PdfWriter

from pa_engine.web.config import CURATED_SYMBOLS, WebConfig
from pa_engine.web.server import create_server
from pa_engine.web.supervisor import RuntimeSupervisor
from pa_engine.web.workspace import ProductWorkspace


MODEL_CANARY = "G033_MODEL_CANARY_f83b7c29"
ALPACA_ID_CANARY = "G033_ALPACA_ID_CANARY_a25e81d4"
ALPACA_SECRET_CANARY = "G033_ALPACA_SECRET_CANARY_06c4fe91"
PAGE_TOKEN = "G033_PAGE_TOKEN_TEST_ONLY"


_G032_CONFTST = Path(__file__).resolve().parents[1] / "g032" / "conftest.py"
_fixture_spec = importlib.util.spec_from_file_location("g033_g032_fixture_runtime", _G032_CONFTST)
assert _fixture_spec is not None and _fixture_spec.loader is not None
_fixture_module = importlib.util.module_from_spec(_fixture_spec)
_fixture_spec.loader.exec_module(_fixture_module)
fixture_runtime_factory = _fixture_module.fixture_runtime_factory


class OfflineAssetValidator:
    def validate_universe(self, key_id: str, secret_key: str) -> list[dict[str, Any]]:
        assert key_id and secret_key
        return [
            {
                "symbol": symbol,
                "asset_class": "us_equity",
                "status": "active",
                "tradable": True,
                "exchange": "NASDAQ",
            }
            for symbol in CURATED_SYMBOLS
        ]


def make_workspace(
    tmp_path: Path,
    *,
    registry: Any | None = None,
    configured: bool = False,
) -> tuple[WebConfig, ProductWorkspace]:
    config = WebConfig(
        database_path=str(tmp_path / "runtime.db"),
        workspace_path=str(tmp_path / ".pa-workspace"),
        port=0,
    )
    workspace = ProductWorkspace(
        config,
        built_in_registry=registry,
        asset_validator=OfflineAssetValidator(),
    )
    if configured:
        workspace.save_credentials(
            {"provider": "alpaca", "key_id": ALPACA_ID_CANARY, "secret_key": ALPACA_SECRET_CANARY}
        )
        workspace.save_credentials({"provider": "deepseek", "api_key": MODEL_CANARY})
    return config, workspace


@contextmanager
def running_product_server(
    tmp_path: Path,
    *,
    configured: bool = False,
    runtime_factory: Callable[..., Any] | None = None,
    preflight_database: bool = False,
) -> Iterator[tuple[Any, RuntimeSupervisor, ProductWorkspace]]:
    config, workspace = make_workspace(tmp_path, configured=configured)
    supervisor = RuntimeSupervisor(
        config,
        workspace=workspace,
        runtime_factory=runtime_factory or fixture_runtime_factory,
        preflight_database=preflight_database,
    )
    server = create_server(config, supervisor=supervisor, workspace=workspace, page_token=PAGE_TOKEN)
    thread = threading.Thread(target=server.serve_forever, name="g033-test-http", daemon=True)
    thread.start()
    try:
        yield server, supervisor, workspace
    finally:
        server.shutdown()
        server.server_close()
        supervisor.close()
        thread.join(timeout=3)


def mutation_headers(server: Any, *, content_type: str = "application/json") -> dict[str, str]:
    return {
        "Origin": f"http://127.0.0.1:{server.server_port}",
        "X-PA-Page-Token": PAGE_TOKEN,
        "Content-Type": content_type,
    }


def http_request(
    server: Any,
    method: str,
    path: str,
    *,
    payload: Any = None,
    raw_body: bytes | None = None,
    headers: dict[str, str] | None = None,
) -> tuple[int, dict[str, str], Any, bytes]:
    connection = http.client.HTTPConnection(server.server_address[0], server.server_address[1], timeout=4)
    if raw_body is not None:
        body = raw_body
    elif payload is None:
        body = None
    else:
        body = json.dumps(payload).encode("utf-8")
    request_headers = dict(headers or {})
    if body is not None:
        request_headers.setdefault("Content-Length", str(len(body)))
    connection.request(method, path, body=body, headers=request_headers)
    response = connection.getresponse()
    data = response.read()
    response_headers = {key.lower(): value for key, value in response.getheaders()}
    connection.close()
    try:
        document: Any = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        document = data.decode("utf-8", "replace")
    return response.status, response_headers, document, data


def _json_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _one_page_pdf() -> bytes:
    writer = PdfWriter()
    writer.add_blank_page(width=612, height=792)
    output = io.BytesIO()
    writer.write(output)
    return output.getvalue()


def build_setup_bundle(
    *,
    slug: str = "opening-reversal",
    mutate: Callable[[dict[str, Any]], None] | None = None,
) -> bytes:
    """Return a minimal but fully grounded Setup Bundle v1 archive."""

    pdf = _one_page_pdf()
    source_sha = hashlib.sha256(pdf).hexdigest()
    source_id = "user-source-" + source_sha[:12]
    record_id = f"user-{source_sha[:12]}-{slug}"
    strategy_id = "pa:" + record_id
    chunk_id = source_id + ":p1"
    text = "Open only after the first reversal trigger closes."
    ref = {
        "chunk_id": chunk_id,
        "source_id": source_id,
        "source_file": "source.pdf",
        "source_sha256": source_sha,
        "page_start": 1,
        "page_end": 1,
        "title_path": ["Opening reversal"],
        "source_locator": "source.pdf#page=1",
        "provenance_status": "verified_local_pdf",
    }
    manifest = {
        "schema_version": "0.1.0",
        "knowledge_base_version": "0.1.0",
        "generated_at": "2026-08-22T00:00:00Z",
        "documents": [
            {
                "source_id": source_id,
                "source_file": "source.pdf",
                "file_name": "source.pdf",
                "sha256": source_sha,
                "status": "success",
                "attempted": True,
                "semantic_configuration_status": "configured",
                "normalization_status": "configured",
                "source_reverification_status": "not_required",
                "source_reverification_required": False,
                "page_count": 1,
                "image_pages": [],
                "extraction_errors": [],
            }
        ],
        "changes": [],
        "change_summary": {},
        "processing_summary": {"attempted": 1, "success": 1, "partial": 0, "failed": 0},
    }
    chunks = {
        "schema_version": "0.1.0",
        "knowledge_base_version": "0.1.0",
        "chunks": [
            {
                "chunk_id": chunk_id,
                "chunk_key": chunk_id,
                "source_id": source_id,
                "source_file": "source.pdf",
                "source_sha256": source_sha,
                "page_start": 1,
                "page_end": 1,
                "page_range": [1, 1],
                "title_path": ["Opening reversal"],
                "parent_chunk_id": "",
                "adjacent_chunk_ids": [],
                "source_text": text,
                "source_text_sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
                "semantic_boundary": "one complete setup rule",
                "provenance_status": "verified_local_pdf",
                "source_locator": "source.pdf#page=1",
                "related_image_pages": [],
                "is_retrieval_chunk": True,
            }
        ],
    }
    empty_fields = {
        field: []
        for field in (
            "market_context",
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
        )
    }
    empty_fields["trigger"] = [
        {
            "claim_id": f"{record_id}:trigger:1",
            "statement": text,
            "declaration_type": "source_assertion",
            "source_refs": [dict(ref)],
        }
    ]
    records = {
        "schema_version": "0.1.0",
        "knowledge_base_version": "0.1.0",
        "records": [
            {
                "id": record_id,
                "source_id": source_id,
                "type": "setup",
                "name": "Opening Reversal",
                "aliases": ["OR"],
                "tags": ["intraday", "reversal"],
                "source_refs": [dict(ref)],
                "direction": ["long", "short"],
                "timeframes": ["1min", "5min"],
                "confidence": 0.9,
                "knowledge_base_version": "0.1.0",
                "active": True,
                "record_status": "active",
                "reverification_required": False,
                **empty_fields,
            }
        ],
    }
    files: dict[str, Any] = {
        "source.pdf": pdf,
        "manifest.json": manifest,
        "chunks.json": chunks,
        "pa_records.json": records,
        "bundle.json": {},
    }
    if mutate is not None:
        mutate(files)
    bundle_override = files.pop("bundle.json")
    encoded = {
        name: value if isinstance(value, bytes) else _json_bytes(value)
        for name, value in files.items()
    }
    effective_source_sha = hashlib.sha256(encoded["source.pdf"]).hexdigest()
    bundle = {
        "schema_version": "pa-setup-bundle.v1",
        "bundle_version": "1.0.0",
        "bundle_id": "bundle-" + effective_source_sha[:16],
        "source": {"file": "source.pdf", "sha256": effective_source_sha, "page_count": 1},
        "files": {name: hashlib.sha256(data).hexdigest() for name, data in encoded.items()},
        "setup_ids": [strategy_id],
    }
    if isinstance(bundle_override, dict):
        bundle.update(bundle_override)
    encoded["bundle.json"] = _json_bytes(bundle)
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as output:
        for name in ("bundle.json", "source.pdf", "manifest.json", "chunks.json", "pa_records.json"):
            output.writestr(name, encoded[name])
    return archive.getvalue()
