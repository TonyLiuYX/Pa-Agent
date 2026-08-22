"""Secret-safe DeepSeek live smoke command.

Examples::

    python3 -m pa_engine.providers.deepseek.smoke --dry-run --thinking disabled
    DEEPSEEK_API_KEY=... python3 -m pa_engine.providers.deepseek.smoke --thinking enabled

The command reads the key only from the process environment, never prints it,
and does not write a file.  ``--dry-run`` builds the exact transport payload
without making a network call, so CI can exercise the command without a key.
Actual enabled/disabled network evidence is intentionally deferred to G030.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

from pa_engine.providers.base import make_normalized_model_request

from .adapter import DeepSeekAdapter, build_transport_payload
from .config import DEEPSEEK_MODEL, DEEPSEEK_PROVIDER, DeepSeekConfig


# A complete, schema-valid no-entry candidate is embedded in the offline
# smoke prompt.  This makes a credentialed G030 smoke deterministic enough to
# distinguish transport success from an otherwise valid JSON-but-unknown
# model answer; no strategy/account data or runtime secret is included.
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


def make_smoke_request(*, thinking: str) -> dict[str, Any]:
    max_output_tokens = 4096 if thinking == "enabled" else 1024
    return make_normalized_model_request(
        provider=DEEPSEEK_PROVIDER,
        model=DEEPSEEK_MODEL,
        adapter="g022-deepseek-v4-flash-0.1.0",
        correlation_id="g022-live-smoke",
        reasoning_mode=thinking,
        requested_capabilities=("structured_json", "reasoning_mode"),
        prompt_template_version="g022-smoke-prompt-0.1.0",
        instructions=(
            "Return exactly the complete JSON object supplied in the user context. "
            "It is already a valid G018 no_entry candidate; do not add markdown, commentary, or fields."
        ),
        # Keep the user message itself parseable by G018 so the expected model
        # response is self-contained and can be checked without a live market
        # snapshot or strategy profile.
        context_text=json.dumps(SMOKE_CANDIDATE, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
        parameters={"temperature": "0", "max_output_tokens": max_output_tokens},
    )


def _safe_result(outcome: Any) -> dict[str, Any]:
    if hasattr(outcome, "to_dict"):
        value = outcome.to_dict()
    else:
        value = outcome
    # AdapterOutcome.to_dict already redacts known secrets.  Keep this final
    # projection intentionally small for shell logs and G030 evidence.
    if isinstance(value, dict):
        response = value.get("response")
        metadata = value.get("call_metadata")
        return {
            "classification": value.get("classification"),
            "retryable": value.get("retryable"),
            "request_semantic_hash": value.get("request_semantic_hash"),
            "response_status": response.get("status") if isinstance(response, dict) else None,
            "finish_reason": response.get("finish_reason") if isinstance(response, dict) else None,
            "error_code": response.get("error", {}).get("code") if isinstance(response, dict) and isinstance(response.get("error"), dict) else None,
            "usage": metadata.get("usage") if isinstance(metadata, dict) else None,
        }
    return {"classification": "internal_error", "retryable": False}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Secret-safe DeepSeek V4 Flash smoke")
    parser.add_argument("--thinking", choices=("enabled", "disabled"), default="disabled")
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--dry-run", action="store_true", help="build payload without reading a key or making a network call")
    args = parser.parse_args(argv)
    request = make_smoke_request(thinking=args.thinking)
    if args.dry_run:
        print(json.dumps({"ok": True, "thinking": args.thinking, "payload": build_transport_payload(request)}, sort_keys=True))
        return 0
    key = os.environ.get("DEEPSEEK_API_KEY")
    if not key:
        print(json.dumps({"ok": False, "error": "DEEPSEEK_API_KEY is not set"}, sort_keys=True))
        return 2
    try:
        adapter = DeepSeekAdapter(api_key=key, config=DeepSeekConfig(timeout_seconds=args.timeout))
        outcome = adapter.complete(request)
        print(json.dumps({"ok": outcome.is_success, **_safe_result(outcome)}, sort_keys=True, ensure_ascii=False))
        return 0 if outcome.is_success else 1
    except Exception:
        # No exception text crosses this CLI boundary: urllib/provider errors
        # may carry request details and are not useful in smoke evidence.
        print(json.dumps({"ok": False, "error": "smoke command failed"}, sort_keys=True))
        return 1


if __name__ == "__main__":  # pragma: no cover - exercised through subprocess
    raise SystemExit(main())


__all__ = ["SMOKE_CANDIDATE", "main", "make_smoke_request"]
