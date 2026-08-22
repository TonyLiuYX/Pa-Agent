"""Secret-safe OpenAI GPT-5.6 Luna smoke command for G030.

The live path reads ``OPENAI_API_KEY`` only from the process environment and
prints a small sanitized result.  ``--dry-run`` builds the exact Responses
payload without reading a key or making a network call.
"""

from __future__ import annotations

import argparse
import json
import os
from typing import Any

from pa_engine.providers.base import make_normalized_model_request

from .adapter import OpenAIAdapter, build_transport_payload
from .config import OPENAI_ADAPTER_VERSION, OPENAI_MODEL, OPENAI_PROVIDER, OpenAIConfig


SMOKE_CANDIDATE: dict[str, Any] = {
    "schema_version": "0.1.0",
    "branch": "no_entry",
    "reason": None,
    "direction": None,
    "entry_price": None,
    "stop_price": None,
    "target_price": None,
    "N": 60,
    "X": None,
    "grounding": {"reason": None, "entry_price": None, "stop_price": None, "target_price": None},
    "confidence": None,
    "uncertainty": {"level": "high", "codes": ["no_actionable_evidence"], "details": "no actionable setup"},
    "shared_market_view_ref": {
        "schema_version": "0.1.0",
        "market_snapshot_id": "mkt_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "market_snapshot_hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "observation_frame_id": "obs_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "observation_frame_hash": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "shared_market_view_id": "smv_6ac4a4c1a28d70fefa39d0b9cabf20683da9c02c005ac1923f89673d9929d100",
        "shared_market_view_hash": "6ac4a4c1a28d70fefa39d0b9cabf20683da9c02c005ac1923f89673d9929d100",
    },
    "second_event_ref": None,
}


def make_smoke_request() -> dict[str, Any]:
    return make_normalized_model_request(
        provider=OPENAI_PROVIDER,
        model=OPENAI_MODEL,
        adapter=OPENAI_ADAPTER_VERSION,
        correlation_id="g031-openai-live-smoke",
        reasoning_mode="enabled",
        requested_capabilities=("structured_json", "reasoning_mode"),
        prompt_template_version="g031-openai-smoke-prompt-0.1.0",
        instructions=(
            "Return exactly the complete JSON object supplied in the user context. "
            "It is already a valid G018 no_entry candidate; do not add markdown, commentary, or fields."
        ),
        context_text=json.dumps(SMOKE_CANDIDATE, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
        parameters={"temperature": "0", "max_output_tokens": 1024},
    )


def _safe_result(outcome: Any) -> dict[str, Any]:
    value = outcome.to_dict() if hasattr(outcome, "to_dict") else outcome
    if isinstance(value, dict):
        response = value.get("response")
        metadata = value.get("call_metadata")
        openai = metadata.get("extensions", {}).get("openai", {}) if isinstance(metadata, dict) else {}
        return {
            "classification": value.get("classification"),
            "retryable": value.get("retryable"),
            "request_semantic_hash": value.get("request_semantic_hash"),
            "response_status": response.get("status") if isinstance(response, dict) else None,
            "finish_reason": response.get("finish_reason") if isinstance(response, dict) else None,
            "error_code": response.get("error", {}).get("code") if isinstance(response, dict) and isinstance(response.get("error"), dict) else None,
            "usage": metadata.get("usage") if isinstance(metadata, dict) else None,
            "effective_service_tier": openai.get("effective_service_tier") if isinstance(openai, dict) else None,
        }
    return {"classification": "internal_error", "retryable": False}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Secret-safe OpenAI GPT-5.6 Luna Responses smoke")
    parser.add_argument("--speed", choices=("standard", "fast"), default="standard")
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--dry-run", action="store_true", help="build payload without reading a key or making a network call")
    args = parser.parse_args(argv)
    request = make_smoke_request()
    config = OpenAIConfig(timeout_seconds=args.timeout, speed=args.speed)
    if args.dry_run:
        print(json.dumps({"ok": True, "speed": args.speed, "payload": build_transport_payload(request, service_tier=config.requested_service_tier)}, sort_keys=True))
        return 0
    key = os.environ.get("OPENAI_API_KEY")
    if not key:
        print(json.dumps({"ok": False, "error": "OPENAI_API_KEY is not set"}, sort_keys=True))
        return 2
    try:
        outcome = OpenAIAdapter(api_key=key, config=config).complete(request)
        print(json.dumps({"ok": outcome.is_success, "speed": args.speed, **_safe_result(outcome)}, sort_keys=True, ensure_ascii=False))
        return 0 if outcome.is_success else 1
    except Exception:
        print(json.dumps({"ok": False, "error": "smoke command failed"}, sort_keys=True))
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = ["SMOKE_CANDIDATE", "main", "make_smoke_request"]
