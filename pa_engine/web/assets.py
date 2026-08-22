"""Read-only Alpaca Assets validation for the curated trading candidates."""

from __future__ import annotations

import json
import socket
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable, Mapping

from .config import CURATED_SYMBOLS, WebConfigError


ASSETS_BASE_URL = "https://api.alpaca.markets/v2/assets"


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001 - urllib protocol
        return None


class AlpacaAssetValidator:
    """Validate a fixed symbol universe without persisting auth headers."""

    def __init__(
        self,
        *,
        opener: Any | None = None,
        timeout: float = 10.0,
        fetcher: Callable[[str, str, str], Mapping[str, Any]] | None = None,
    ) -> None:
        self._opener = opener or urllib.request.build_opener(_NoRedirect())
        self._timeout = float(timeout)
        self._fetcher = fetcher

    def __repr__(self) -> str:
        return "AlpacaAssetValidator(feed='iex', credentials=<call-only>)"

    def _fetch(self, symbol: str, key_id: str, secret_key: str) -> Mapping[str, Any]:
        if self._fetcher is not None:
            value = self._fetcher(symbol, key_id, secret_key)
            if not isinstance(value, Mapping):
                raise WebConfigError("asset_validation_failed", "Alpaca asset response is invalid")
            return value
        url = ASSETS_BASE_URL + "/" + urllib.parse.quote(symbol, safe="")
        request = urllib.request.Request(
            url,
            method="GET",
            headers={
                "Accept": "application/json",
                "APCA-API-KEY-ID": key_id,
                "APCA-API-SECRET-KEY": secret_key,
                "User-Agent": "pa-engine-paper-desk/1",
            },
        )
        try:
            with self._opener.open(request, timeout=self._timeout) as response:
                raw = response.read(256 * 1024 + 1)
        except urllib.error.HTTPError as exc:
            code = "alpaca_authentication" if exc.code in {401, 403} else "alpaca_assets_unavailable"
            raise WebConfigError(code, "Alpaca Assets validation failed") from None
        except (urllib.error.URLError, TimeoutError, socket.timeout, OSError):
            raise WebConfigError("alpaca_assets_unavailable", "Alpaca Assets validation is temporarily unavailable") from None
        if len(raw) > 256 * 1024:
            raise WebConfigError("asset_validation_failed", "Alpaca asset response exceeds the fixed limit")
        try:
            value = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise WebConfigError("asset_validation_failed", "Alpaca asset response is invalid") from None
        if not isinstance(value, Mapping):
            raise WebConfigError("asset_validation_failed", "Alpaca asset response is invalid")
        return value

    @staticmethod
    def _project(symbol: str, value: Mapping[str, Any]) -> dict[str, Any]:
        asset_class = value.get("class", value.get("asset_class"))
        exchange = str(value.get("exchange") or "").upper()
        valid = (
            str(value.get("symbol") or "").upper() == symbol
            and asset_class == "us_equity"
            and value.get("status") == "active"
            and value.get("tradable") is True
            and exchange not in {"", "OTC"}
        )
        if not valid:
            raise WebConfigError("asset_not_tradable", f"{symbol} is not an active, tradable non-OTC U.S. equity on Alpaca")
        return {
            "symbol": symbol,
            "asset_class": "us_equity",
            "status": "active",
            "tradable": True,
            "exchange": exchange,
        }

    def validate_universe(self, key_id: str, secret_key: str) -> list[dict[str, Any]]:
        return [self._project(symbol, self._fetch(symbol, key_id, secret_key)) for symbol in CURATED_SYMBOLS]


__all__ = ["ASSETS_BASE_URL", "AlpacaAssetValidator"]
