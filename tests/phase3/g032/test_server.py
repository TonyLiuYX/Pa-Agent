from __future__ import annotations

import http.client
import json
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import pytest

from pa_engine.web.config import WebConfig
from pa_engine.web.server import _display_url, create_server
from pa_engine.web.supervisor import RuntimeSupervisor

from .conftest import configured_workspace, fixture_runtime_factory


PAGE_TOKEN = "G032_PAGE_TOKEN_TEST_ONLY"
MODEL_CANARY = "G032_MODEL_CANARY_7f3c"
ALPACA_ID_CANARY = "G032_ALPACA_ID_CANARY_9a1d"
ALPACA_SECRET_CANARY = "G032_ALPACA_SECRET_CANARY_4b8e"


@contextmanager
def running_server(tmp_path: Path) -> Iterator[tuple[Any, RuntimeSupervisor]]:
    config = WebConfig(database_path=str(tmp_path / "runtime.db"), workspace_path=str(tmp_path / "workspace"), port=0, step_interval_seconds=1.0)
    workspace = configured_workspace(config)
    supervisor = RuntimeSupervisor(config, workspace=workspace, runtime_factory=fixture_runtime_factory, preflight_database=False)
    server = create_server(config, supervisor=supervisor, workspace=workspace, page_token=PAGE_TOKEN)
    thread = threading.Thread(target=server.serve_forever, name="g032-test-http", daemon=True)
    thread.start()
    try:
        yield server, supervisor
    finally:
        server.shutdown()
        server.server_close()
        supervisor.close()
        thread.join(timeout=3)


def http_request(server: Any, method: str, path: str, *, payload: Any = None, headers: dict[str, str] | None = None):
    connection = http.client.HTTPConnection(server.server_address[0], server.server_address[1], timeout=3)
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    request_headers = dict(headers or {})
    if body is not None:
        request_headers.setdefault("Content-Length", str(len(body)))
    connection.request(method, path, body=body, headers=request_headers)
    response = connection.getresponse()
    raw = response.read()
    response_headers = {key.lower(): value for key, value in response.getheaders()}
    connection.close()
    try:
        document = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        document = raw.decode("utf-8", "replace")
    return response.status, response_headers, document, raw


def start_payload(**overrides: Any) -> dict[str, Any]:
    value = {
        "symbol": "AAPL",
        "timeframe": "1min",
        "execution_policy": "paper",
    }
    value.update(overrides)
    return value


def mutation_headers(server: Any, *, token: str = PAGE_TOKEN, origin: str | None = None, content_type: str = "application/json") -> dict[str, str]:
    return {
        "Origin": origin or f"http://127.0.0.1:{server.server_port}",
        "X-PA-Page-Token": token,
        "Content-Type": content_type,
    }


def wait_for(supervisor: RuntimeSupervisor, lifecycle: str, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if supervisor.lifecycle == lifecycle:
            return
        time.sleep(0.01)
    pytest.fail(f"supervisor did not reach {lifecycle!r}; got {supervisor.lifecycle!r}")


def assert_no_canaries(raw: bytes | str) -> None:
    text = raw.decode("utf-8", "replace") if isinstance(raw, bytes) else raw
    assert MODEL_CANARY not in text
    assert ALPACA_ID_CANARY not in text
    assert ALPACA_SECRET_CANARY not in text


def test_static_page_and_health_are_loopback_paper_surfaces(tmp_path: Path):
    with running_server(tmp_path) as (server, _supervisor):
        status, headers, document, raw = http_request(server, "GET", "/")
        assert status == 200
        assert "PAPER TRADING ONLY" in raw.decode("utf-8")
        assert "LOCAL / LOOPBACK ONLY" in raw.decode("utf-8")
        assert "LOCAL / 127.0.0.1" not in raw.decode("utf-8")
        assert "__PA_PAGE_TOKEN__" not in raw.decode("utf-8")
        assert_no_canaries(raw)
        assert headers["content-type"].startswith("text/html")

        status, headers, document, raw = http_request(server, "GET", "/api/health")
        assert status == 200
        assert document["labels"]["paper_only"] is True
        assert document["labels"]["iex_only"] is True
        assert document["labels"]["market_data_usage"] == "internal_prototype"
        assert document["iex_only"] is True
        assert document["market_data_usage"] == "internal_prototype"
        assert document["no_live_orders"] is True
        assert headers.get("access-control-allow-origin") is None
        assert_no_canaries(raw)

        status, _headers, _document, raw = http_request(server, "GET", "/static/app.js")
        assert status == 200
        static_text = raw.decode("utf-8")
        assert "localStorage" not in static_text
        assert "sessionStorage" not in static_text
        assert "/api/order" not in static_text
        assert "/api/download" not in static_text


def test_mutation_boundary_duplicate_start_and_idempotent_stop(tmp_path: Path, capsys: pytest.CaptureFixture[str]):
    with running_server(tmp_path) as (server, supervisor):
        origin_headers = mutation_headers(server)

        status, _headers, document, _raw = http_request(
            server,
            "POST",
            "/api/start",
            payload=start_payload(),
            headers={**origin_headers, "X-PA-Page-Token": "wrong"},
        )
        assert status == 403
        assert document["error"]["code"] == "invalid_page_token"

        status, _headers, document, _raw = http_request(
            server,
            "POST",
            "/api/start",
            payload=start_payload(),
            headers={**origin_headers, "Origin": "http://example.test"},
        )
        assert status == 403
        assert document["error"]["code"] == "foreign_origin"

        status, _headers, document, _raw = http_request(
            server,
            "POST",
            "/api/start",
            payload=start_payload(database_path="/tmp/attacker.db"),
            headers=origin_headers,
        )
        assert status == 400
        assert document["error"]["code"] == "database_path_forbidden"

        status, _headers, document, _raw = http_request(
            server,
            "POST",
            "/api/start",
            payload=start_payload(model_key="UNSELECTED_PROVIDER_CANARY"),
            headers=origin_headers,
        )
        assert status == 400
        assert document["error"]["code"] == "start_secrets_forbidden"

        status, _headers, document, raw = http_request(
            server,
            "POST",
            "/api/start",
            payload=start_payload(),
            headers=origin_headers,
        )
        assert status == 202
        assert document["lifecycle"] == "starting"
        assert_no_canaries(raw)

        status, _headers, duplicate, raw = http_request(
            server,
            "POST",
            "/api/start",
            payload=start_payload(),
            headers=origin_headers,
        )
        assert status == 409
        assert duplicate["error"]["code"] == "already_running"
        assert_no_canaries(raw)
        wait_for(supervisor, "running")

        status, _headers, snapshot, raw = http_request(server, "GET", "/api/snapshot")
        assert status == 200
        assert snapshot["bars"]["closed_count"] == 63
        assert snapshot["bars"]["forming"]["is_closed"] is False
        assert snapshot["paper_only"] is True
        assert snapshot["market_data_usage"] == "internal_prototype"
        assert_no_canaries(raw)

        status, _headers, stopped, raw = http_request(server, "POST", "/api/stop", payload={}, headers=origin_headers)
        assert status == 200
        assert stopped["lifecycle"] == "stopped"
        assert_no_canaries(raw)
        assert supervisor.runtime_present is False

        status, _headers, stopped_again, raw = http_request(server, "POST", "/api/stop", payload={}, headers=origin_headers)
        assert status == 200
        assert stopped_again["accepted"] is False
        assert stopped_again["lifecycle"] == "stopped"
        assert_no_canaries(raw)
        captured = capsys.readouterr()
        assert_no_canaries(captured.out + captured.err)


def test_ipv6_startup_url_is_bracketed_and_valid():
    assert _display_url(("::1", 8765)) == "http://[::1]:8765/"
    assert _display_url(("127.0.0.1", 8765)) == "http://127.0.0.1:8765/"


def test_non_json_and_cors_like_requests_are_rejected(tmp_path: Path):
    with running_server(tmp_path) as (server, _supervisor):
        headers = mutation_headers(server, content_type="text/plain")
        status, _headers, document, raw = http_request(server, "POST", "/api/stop", payload="{}", headers=headers)
        assert status == 415
        assert document["error"]["code"] == "content_type_required"
        assert_no_canaries(raw)

        status, _headers, document, raw = http_request(
            server,
            "OPTIONS",
            "/api/start",
            headers={"Origin": f"http://127.0.0.1:{server.server_port}"},
        )
        assert status == 405
        assert document["error"]["code"] == "cors_disabled"
        assert_no_canaries(raw)

        for path in ("/api/order", "/api/download", "/api/trades"):
            status, _headers, _document, _raw = http_request(server, "GET", path)
            assert status == 404
