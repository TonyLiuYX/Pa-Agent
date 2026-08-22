"""Standard-library loopback HTTP server for the Paper Desk workspace."""

from __future__ import annotations

import argparse
from html import escape as html_escape
import json
import mimetypes
import secrets
import socket
import sys
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import unquote, urlsplit

from .config import StartRequest, WebConfig, WebConfigError, validate_loopback_host
from .setup_bundle import MAX_ARCHIVE_BYTES
from .supervisor import RuntimeSupervisor, SupervisorError
from .workspace import ProductWorkspace


_STATIC_ROOT = Path(__file__).resolve().parent / "static"
_JSON_CONTENT_TYPE = "application/json; charset=utf-8"
_MAX_JSON_BODY_BYTES = 256 * 1024
_APP_ROUTES = frozenset({"/", "/trade", "/strategies", "/create-setup", "/settings"})
_JSON_MUTATIONS = frozenset(
    {
        "/api/start",
        "/api/stop",
        "/api/preferences",
        "/api/strategies/selection",
        "/api/settings/model",
        "/api/settings/credentials",
        "/api/settings/credentials/clear",
    }
)


def _labels() -> dict[str, Any]:
    return {
        "paper_only": True,
        "paper_only_text": "PAPER TRADING ONLY — no live orders",
        "iex_only": True,
        "market_feed": "alpaca:iex:single_exchange",
        "market_data_usage": "internal_prototype",
        "no_live_orders": True,
    }


def _json_bytes(value: Mapping[str, Any]) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _display_url(address: tuple[str, int] | Any) -> str:
    host = str(address[0])
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    return f"http://{host}:{int(address[1])}/"


class WebApplication:
    """Product and HTTP state; runtime mutations stay on the owner thread."""

    def __init__(
        self,
        config: WebConfig,
        supervisor: RuntimeSupervisor | Any | None = None,
        *,
        workspace: ProductWorkspace | None = None,
        page_token: str | None = None,
    ) -> None:
        self.config = config
        if supervisor is None:
            self.workspace = workspace or ProductWorkspace(config)
            self.supervisor = RuntimeSupervisor(config, workspace=self.workspace)
        else:
            self.supervisor = supervisor
            self.workspace = workspace or getattr(supervisor, "workspace", None) or ProductWorkspace(config)
        self.page_token = page_token or secrets.token_urlsafe(24)

    def page(self) -> bytes:
        document = (_STATIC_ROOT / "index.html").read_text(encoding="utf-8")
        return document.replace("__PA_PAGE_TOKEN__", html_escape(self.page_token, quote=True)).encode("utf-8")

    def static(self, name: str) -> tuple[bytes, str] | None:
        if not name or Path(name).is_absolute() or len(Path(name).parts) != 1 or name.startswith("."):
            return None
        path = _STATIC_ROOT / name
        if not path.is_file() or path.suffix not in {".css", ".js", ".svg", ".png", ".ico"}:
            return None
        content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        if path.suffix == ".js":
            content_type = "text/javascript"
        return path.read_bytes(), content_type + ("; charset=utf-8" if path.suffix in {".css", ".js", ".svg"} else "")

    @staticmethod
    def _snapshot_value(snapshot: Any) -> dict[str, Any]:
        if hasattr(snapshot, "to_dict"):
            value = snapshot.to_dict()
        else:
            value = dict(snapshot) if isinstance(snapshot, Mapping) else {}
        value.setdefault("paper_only", True)
        value.setdefault("iex_only", True)
        value.setdefault("market_data_usage", "internal_prototype")
        value.setdefault("no_live_orders", True)
        value.setdefault("labels", _labels())
        return value

    def health(self) -> dict[str, Any]:
        value = self.supervisor.health()
        result = dict(value) if isinstance(value, Mapping) else {"ok": False}
        result.update({"labels": _labels(), "iex_only": True, "market_data_usage": "internal_prototype", "no_live_orders": True})
        result["page_token_required_for_mutation"] = True
        return result

    def snapshot(self) -> dict[str, Any]:
        return self._snapshot_value(self.supervisor.snapshot())

    def bootstrap(self) -> dict[str, Any]:
        value = self.workspace.bootstrap()
        value["runtime"] = {
            "lifecycle": self.snapshot().get("lifecycle", "idle"),
            "configuration_locked": bool(getattr(self.supervisor, "configuration_locked", False)),
        }
        value["settings"] = self.workspace.settings()
        return value


class _WebHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], application: WebApplication) -> None:
        self.application = application
        self.address_family = socket.AF_INET6 if ":" in address[0] else socket.AF_INET
        super().__init__(address, _RequestHandler)


class _RequestHandler(BaseHTTPRequestHandler):
    server: _WebHTTPServer

    def log_message(self, format: str, *args: Any) -> None:
        # Bodies are never logged; URL paths carry no credential material.
        sys.stderr.write("pa_engine.web: " + (format % args) + "\n")

    def _write(self, status: int, body: bytes, content_type: str, *, cache_control: str = "no-store", disposition: str | None = None) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", cache_control)
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Content-Security-Policy", "default-src 'self'; connect-src 'self'; img-src 'self' data:; style-src 'self'; script-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'")
        if disposition:
            self.send_header("Content-Disposition", disposition)
        self.end_headers()
        self.wfile.write(body)

    def _write_json(self, status: int, value: Mapping[str, Any]) -> None:
        self._write(status, _json_bytes(value), _JSON_CONTENT_TYPE)

    def _error(self, status: int, code: str, message: str) -> None:
        self._write_json(
            status,
            {
                "error": {"code": str(code), "message": str(message)[:240]},
                "paper_only": True,
                "iex_only": True,
                "market_data_usage": "internal_prototype",
                "no_live_orders": True,
                "labels": _labels(),
            },
        )

    def _same_origin(self) -> bool:
        origin = self.headers.get("Origin")
        if not isinstance(origin, str) or not origin:
            return False
        parsed = urlsplit(origin)
        if parsed.scheme != "http" or parsed.path or parsed.query or parsed.fragment or parsed.username or parsed.password:
            return False
        try:
            origin_port = parsed.port or 80
        except ValueError:
            return False
        return origin_port == self.server.server_port and (parsed.hostname or "").lower() == str(self.server.server_address[0]).lower()

    def _mutation_guard(self, *, content_types: frozenset[str]) -> bool:
        if not self._same_origin():
            self._error(HTTPStatus.FORBIDDEN, "foreign_origin", "mutation requires the current loopback page origin")
            return False
        token = self.headers.get("X-PA-Page-Token") or self.headers.get("X-Page-Token")
        if token != self.server.application.page_token:
            self._error(HTTPStatus.FORBIDDEN, "invalid_page_token", "mutation requires the current page token")
            return False
        content_type = self.headers.get("Content-Type", "").lower().split(";", 1)[0].strip()
        if content_type not in content_types:
            expected = "application/json" if "application/json" in content_types else "application/zip"
            self._error(HTTPStatus.UNSUPPORTED_MEDIA_TYPE, "content_type_required", f"mutation body must use {expected}")
            return False
        return True

    def _read_body(self, maximum: int) -> bytes:
        raw_length = self.headers.get("Content-Length")
        try:
            length = int(raw_length or "0")
        except ValueError:
            raise WebConfigError("invalid_body", "request body length is invalid") from None
        if length < 0 or length > maximum:
            raise WebConfigError("body_too_large", "request body exceeds the fixed limit")
        raw = self.rfile.read(length)
        if len(raw) != length:
            raise WebConfigError("invalid_body", "request body ended early")
        return raw

    def _read_json(self) -> Any:
        try:
            return json.loads(self._read_body(_MAX_JSON_BODY_BYTES).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise WebConfigError("invalid_json", "request body is not valid JSON") from None

    def do_GET(self) -> None:  # noqa: N802 - stdlib handler API
        path = urlsplit(self.path).path
        app = self.server.application
        try:
            if path in _APP_ROUTES:
                self._write(HTTPStatus.OK, app.page(), "text/html; charset=utf-8")
                return
            if path == "/api/health":
                self._write_json(HTTPStatus.OK, app.health())
                return
            if path == "/api/bootstrap":
                self._write_json(HTTPStatus.OK, app.bootstrap())
                return
            if path == "/api/home":
                self._write_json(HTTPStatus.OK, app.workspace.home())
                return
            if path == "/api/snapshot":
                self._write_json(HTTPStatus.OK, app.snapshot())
                return
            if path == "/api/strategies":
                self._write_json(HTTPStatus.OK, app.workspace.strategies())
                return
            if path.startswith("/api/strategies/"):
                strategy_id = unquote(path.removeprefix("/api/strategies/"))
                self._write_json(HTTPStatus.OK, app.workspace.strategy_detail(strategy_id))
                return
            if path == "/api/settings":
                self._write_json(HTTPStatus.OK, app.workspace.settings())
                return
            if path == "/api/setup-kit":
                self._write_json(HTTPStatus.OK, app.workspace.setup_kit())
                return
            if path.startswith("/static/"):
                asset = app.static(path.removeprefix("/static/"))
                if asset is None:
                    self._error(HTTPStatus.NOT_FOUND, "not_found", "resource not found")
                else:
                    self._write(HTTPStatus.OK, asset[0], asset[1], cache_control="no-cache")
                return
            self._error(HTTPStatus.NOT_FOUND, "not_found", "resource not found")
        except WebConfigError as exc:
            status = HTTPStatus.NOT_FOUND if exc.code == "strategy_not_found" else HTTPStatus.BAD_REQUEST
            self._error(status, exc.code, str(exc))
        except Exception:
            self._error(HTTPStatus.INTERNAL_SERVER_ERROR, "web_error", "request could not be completed")

    def do_POST(self) -> None:  # noqa: N802 - stdlib handler API
        path = urlsplit(self.path).path
        if path not in _JSON_MUTATIONS and path != "/api/setups/import":
            self._error(HTTPStatus.NOT_FOUND, "not_found", "resource not found")
            return
        content_types = frozenset({"application/json"}) if path in _JSON_MUTATIONS else frozenset({"application/zip", "application/x-zip-compressed"})
        if not self._mutation_guard(content_types=content_types):
            return
        app = self.server.application
        try:
            if path == "/api/setups/import":
                archive = self._read_body(MAX_ARCHIVE_BYTES)
                result = app.supervisor.mutate_configuration(app.workspace.import_setup, archive)
                self._write_json(HTTPStatus.OK if result.get("idempotent") else HTTPStatus.CREATED, result)
                return
            payload = self._read_json()
            if path == "/api/start":
                result = app.supervisor.request_start(StartRequest.from_payload(payload))
                self._write_json(HTTPStatus.ACCEPTED, result)
            elif path == "/api/stop":
                result = app.supervisor.request_stop()
                self._write_json(HTTPStatus.OK, result)
            elif path == "/api/preferences":
                self._write_json(HTTPStatus.OK, app.workspace.update_preferences(payload))
            elif path == "/api/strategies/selection":
                self._write_json(HTTPStatus.OK, app.supervisor.mutate_configuration(app.workspace.update_selection, payload))
            elif path == "/api/settings/model":
                self._write_json(HTTPStatus.OK, app.supervisor.mutate_configuration(app.workspace.update_model, payload))
            elif path == "/api/settings/credentials":
                self._write_json(HTTPStatus.OK, app.supervisor.mutate_configuration(app.workspace.save_credentials, payload))
            elif path == "/api/settings/credentials/clear":
                self._write_json(HTTPStatus.OK, app.supervisor.mutate_configuration(app.workspace.clear_credentials, payload))
        except WebConfigError as exc:
            self._error(HTTPStatus.BAD_REQUEST, exc.code, str(exc))
        except SupervisorError as exc:
            conflicts = {"already_running", "lease_conflict", "service_closed", "configuration_locked", "database_configuration_conflict", "strategy_selection_conflict", "execution_policy_conflict"}
            self._error(HTTPStatus.CONFLICT if exc.code in conflicts else HTTPStatus.BAD_REQUEST, exc.code, str(exc))
        except Exception:
            self._error(HTTPStatus.INTERNAL_SERVER_ERROR, "web_error", "request could not be completed")

    def do_OPTIONS(self) -> None:  # noqa: N802 - explicitly no CORS surface
        self._error(HTTPStatus.METHOD_NOT_ALLOWED, "cors_disabled", "CORS is not enabled")


def create_server(
    config: WebConfig | None = None,
    *,
    supervisor: RuntimeSupervisor | Any | None = None,
    workspace: ProductWorkspace | None = None,
    page_token: str | None = None,
) -> _WebHTTPServer:
    effective = config or WebConfig()
    host = validate_loopback_host(effective.host)
    owns_supervisor = supervisor is None
    application = WebApplication(effective, supervisor, workspace=workspace, page_token=page_token)
    try:
        return _WebHTTPServer((host, effective.port), application)
    except Exception:
        if owns_supervisor:
            application.supervisor.close()
        raise


def serve(config: WebConfig | None = None) -> None:
    effective = config or WebConfig()
    server = create_server(effective)
    address = server.server_address
    print(f"pa_engine.web listening at {_display_url(address)}")
    print("PAPER TRADING ONLY — no live orders. Alpaca Basic IEX / internal_prototype.")
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        try:
            server.application.supervisor.close()
        finally:
            server.server_close()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m pa_engine.web",
        description="Loopback-only Paper Desk for advisory and automatic paper trading.",
        epilog="PAPER TRADING ONLY — no live orders. Alpaca Basic IEX / internal_prototype.",
    )
    parser.add_argument("--host", default="127.0.0.1", help="loopback bind host (127.0.0.1 or ::1 only)")
    parser.add_argument("--port", type=int, default=8765, help="HTTP port (0 selects an ephemeral local port)")
    parser.add_argument("--database", "--db", dest="database_path", default="runtime.db", help="SQLite runtime database path")
    parser.add_argument("--workspace", dest="workspace_path", default=".pa-workspace", help="non-secret local Paper Desk workspace")
    parser.add_argument("--knowledge-dir", default=None, help="optional packaged knowledge snapshot directory")
    parser.add_argument("--startup-history-seconds", type=int, default=7 * 86400)
    parser.add_argument("--step-interval-seconds", type=float, default=1.0, help="owner polling interval; minimum 1 second")
    parser.add_argument("--min-closed-bars", type=int, default=3)
    parser.add_argument("--stale-after-seconds", type=int, default=60)
    parser.add_argument("--queue-capacity", type=int, default=200)
    parser.add_argument("--max-in-flight", type=int, default=40)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        config = WebConfig(
            host=args.host,
            port=args.port,
            database_path=args.database_path,
            workspace_path=args.workspace_path,
            knowledge_dir=args.knowledge_dir,
            startup_history_seconds=args.startup_history_seconds,
            step_interval_seconds=args.step_interval_seconds,
            minimum_closed_bars=args.min_closed_bars,
            stale_after_seconds=args.stale_after_seconds,
            queue_capacity=args.queue_capacity,
            max_in_flight=args.max_in_flight,
        )
    except WebConfigError as exc:
        print(json.dumps({"error": {"code": exc.code, "message": str(exc)}, "labels": _labels()}, ensure_ascii=False), file=sys.stderr)
        return 2
    try:
        serve(config)
    except WebConfigError as exc:
        print(json.dumps({"error": {"code": exc.code, "message": str(exc)}, "labels": _labels()}, ensure_ascii=False), file=sys.stderr)
        return 2
    except OSError:
        print(json.dumps({"error": {"code": "bind_failed", "message": "loopback web server could not bind the requested port"}, "labels": _labels()}, ensure_ascii=False), file=sys.stderr)
        return 2
    return 0


__all__ = ["WebApplication", "create_server", "main", "serve"]
