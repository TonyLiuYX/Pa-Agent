from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote

from .conftest import (
    ALPACA_ID_CANARY,
    ALPACA_SECRET_CANARY,
    MODEL_CANARY,
    build_setup_bundle,
    fixture_runtime_factory,
    http_request,
    mutation_headers,
    running_product_server,
)


SECRETS = (MODEL_CANARY, ALPACA_ID_CANARY, ALPACA_SECRET_CANARY)


def _assert_clean(value: Any) -> None:
    text = value.decode("utf-8", "replace") if isinstance(value, bytes) else json.dumps(value, ensure_ascii=False, default=str)
    for secret in SECRETS:
        assert secret not in text


def _wait(supervisor, lifecycle: str, timeout: float = 3.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if supervisor.lifecycle == lifecycle:
            return
        time.sleep(0.01)
    raise AssertionError(f"expected lifecycle {lifecycle}, got {supervisor.lifecycle}")


def _save_credentials(server) -> list[bytes]:
    values: list[bytes] = []
    for payload in (
        {"provider": "alpaca", "key_id": ALPACA_ID_CANARY, "secret_key": ALPACA_SECRET_CANARY},
        {"provider": "deepseek", "api_key": MODEL_CANARY},
    ):
        status, _headers, document, raw = http_request(
            server,
            "POST",
            "/api/settings/credentials",
            payload=payload,
            headers=mutation_headers(server),
        )
        assert status == 200
        assert set(document["credentials"]) == {"alpaca", "deepseek", "openai"}
        _assert_clean(raw)
        values.append(raw)
    return values


def test_five_deep_links_and_read_apis_are_bounded_product_surfaces(tmp_path: Path):
    with running_product_server(tmp_path) as (server, _supervisor, _workspace):
        for route in ("/", "/trade", "/strategies", "/create-setup", "/settings"):
            status, headers, _document, raw = http_request(server, "GET", route)
            assert status == 200
            assert headers["content-type"].startswith("text/html")
            assert "frame-ancestors 'none'" in headers["content-security-policy"]
            assert b"MARKET SPINE" in raw

        status, _headers, bootstrap, _raw = http_request(server, "GET", "/api/bootstrap")
        assert status == 200
        assert bootstrap["routes"] == ["/", "/trade", "/strategies", "/create-setup", "/settings"]
        assert bootstrap["strategy_counts"]["selected"] == 21
        assert bootstrap["settings"]["credentials"] == {"alpaca": False, "deepseek": False, "openai": False}

        status, _headers, home, _raw = http_request(server, "GET", "/api/home")
        assert status == 200 and home["available"] is False and home["series"] == []

        status, _headers, strategies, _raw = http_request(server, "GET", "/api/strategies")
        assert status == 200 and strategies["counts"]["total"] == 21
        strategy_id = strategies["items"][0]["strategy_id"]
        status, _headers, detail, _raw = http_request(server, "GET", "/api/strategies/" + quote(strategy_id, safe=""))
        assert status == 200 and detail["strategy_id"] == strategy_id and "claims" in detail

        status, _headers, kit, _raw = http_request(server, "GET", "/api/setup-kit")
        assert status == 200 and set(kit["prompts"]) == {"zh-CN", "en-US"}


def test_settings_start_selection_import_and_runtime_lock_follow_new_api_contract(tmp_path: Path):
    captured_requests: list[dict[str, Any]] = []

    def factory(config, launch, web_config):
        captured_requests.append(launch.request.public_config())
        return fixture_runtime_factory(config, launch, web_config)

    with running_product_server(tmp_path, runtime_factory=factory) as (server, supervisor, workspace):
        responses = _save_credentials(server)
        status, _headers, settings, raw = http_request(server, "GET", "/api/settings")
        assert status == 200
        assert settings["credentials"] == {"alpaca": True, "deepseek": True, "openai": False}
        assert len(settings["alpaca_assets"]) == 6
        _assert_clean(raw)

        status, _headers, error, _raw = http_request(
            server,
            "POST",
            "/api/start",
            payload={
                "symbol": "NVDA",
                "timeframe": "5min",
                "execution_policy": "advisory",
                "api_key": MODEL_CANARY,
            },
            headers=mutation_headers(server),
        )
        assert status == 400 and error["error"]["code"] == "start_secrets_forbidden"

        start_body = {"symbol": "NVDA", "timeframe": "5min", "execution_policy": "advisory"}
        status, _headers, started, raw = http_request(
            server, "POST", "/api/start", payload=start_body, headers=mutation_headers(server)
        )
        assert status == 202 and started["lifecycle"] == "starting"
        assert captured_requests == [] or captured_requests == [start_body]
        _assert_clean(raw)

        for path, payload in (
            ("/api/strategies/selection", {"strategy_ids": workspace.strategies()["selected_ids"][:1]}),
            ("/api/settings/model", {"active_provider": "openai", "deepseek_reasoning": "enabled", "openai_speed": "fast"}),
            ("/api/settings/credentials/clear", {"provider": "deepseek"}),
        ):
            status, _headers, document, _raw = http_request(
                server, "POST", path, payload=payload, headers=mutation_headers(server)
            )
            assert status == 409 and document["error"]["code"] == "configuration_locked"

        status, _headers, language, _raw = http_request(
            server,
            "POST",
            "/api/preferences",
            payload={"locale": "en-US"},
            headers=mutation_headers(server),
        )
        assert status == 200 and language["locale"] == "en-US"

        _wait(supervisor, "running")
        assert captured_requests == [start_body]
        status, _headers, stopped, raw = http_request(
            server, "POST", "/api/stop", payload={}, headers=mutation_headers(server)
        )
        assert status == 200 and stopped["lifecycle"] == "stopped"
        _assert_clean(raw)
        assert workspace.settings()["credentials"] == {"alpaca": True, "deepseek": True, "openai": False}

        archive = build_setup_bundle()
        status, _headers, imported, raw = http_request(
            server,
            "POST",
            "/api/setups/import",
            raw_body=archive,
            headers=mutation_headers(server, content_type="application/zip"),
        )
        assert status == 201 and imported["validation"]["ok"] is True
        _assert_clean(raw)
        assert workspace.strategies()["counts"] == {"total": 22, "built_in": 21, "custom": 1, "selected": 21}
        for response in responses:
            _assert_clean(response)


def test_secret_canaries_never_reach_http_static_logs_sqlite_workspace_or_report(tmp_path: Path, capsys):
    captured: list[dict[str, Any]] = []

    def factory(config, launch, web_config):
        captured.append({"request": launch.request.public_config(), "repr": repr(launch)})
        return fixture_runtime_factory(config, launch, web_config)

    public_documents: list[Any] = []
    with running_product_server(
        tmp_path,
        runtime_factory=factory,
        preflight_database=True,
    ) as (server, supervisor, workspace):
        _save_credentials(server)
        for path in ("/", "/settings", "/static/app.js", "/static/app.css", "/api/bootstrap", "/api/settings"):
            status, _headers, document, raw = http_request(server, "GET", path)
            assert status == 200
            _assert_clean(raw)
            public_documents.append(document)

        exact_start = {"symbol": "AAPL", "timeframe": "1min", "execution_policy": "advisory"}
        status, _headers, document, raw = http_request(
            server, "POST", "/api/start", payload=exact_start, headers=mutation_headers(server)
        )
        assert status == 202
        _assert_clean(raw)
        _wait(supervisor, "running")
        assert captured == [{"request": exact_start, "repr": "RuntimeLaunch(symbol='AAPL', timeframe='1min', execution_policy='advisory', provider='deepseek', selected=21, credentials=<runtime-only>)"}]

        status, _headers, snapshot, raw = http_request(server, "GET", "/api/snapshot")
        assert status == 200
        _assert_clean(raw)
        public_documents.append(snapshot)
        http_request(server, "POST", "/api/stop", payload={}, headers=mutation_headers(server))

        settings = workspace.settings()
        _assert_clean(settings)

    captured_logs = capsys.readouterr()
    _assert_clean(captured_logs.out + captured_logs.err)
    for file_path in (
        tmp_path / "runtime.db",
        *[path for path in (tmp_path / ".pa-workspace").rglob("*") if path.is_file()],
    ):
        if file_path.exists():
            data = file_path.read_bytes()
            for secret in SECRETS:
                assert secret.encode("utf-8") not in data

    report = tmp_path / "public-report.json"
    report.write_text(json.dumps({"responses": public_documents, "runtime": captured}, ensure_ascii=False), encoding="utf-8")
    _assert_clean(report.read_bytes())
