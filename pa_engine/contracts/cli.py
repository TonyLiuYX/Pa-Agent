"""Language-neutral JSON CLI for contract validation and hashing."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .canonical import content_hash
from .registry import METADATA, artifact_kinds
from .validation import validate_artifact


def _json_out(value: Any) -> None:
    sys.stdout.write(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")


def _load(path: str) -> Any:
    with Path(path).open("r", encoding="utf-8") as fh:
        return json.load(fh, parse_constant=lambda x: (_ for _ in ()).throw(ValueError(f"invalid constant {x}")))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m pa_engine.contracts")
    sub = parser.add_subparsers(dest="command", required=True)
    validate = sub.add_parser("validate")
    validate.add_argument("--file", required=True)
    validate.add_argument("--kind")
    validate.add_argument("--knowledge-dir")
    validate.add_argument("--related", action="append", default=[], metavar="KIND=PATH")
    hashed = sub.add_parser("hash")
    hashed.add_argument("--file", required=True)
    hashed.add_argument("--kind", required=True, choices=artifact_kinds())
    return parser


def _related(specs: list[str]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for spec in specs:
        if "=" not in spec:
            raise ValueError("--related must be KIND=PATH")
        kind, path = spec.split("=", 1)
        if not kind or not path or kind not in METADATA:
            raise ValueError("unknown related artifact kind")
        result[kind] = _load(path)
    return result


def main(argv: list[str] | None = None) -> int:
    try:
        args = _parser().parse_args(argv)
    except SystemExit as exc:
        # argparse has already emitted usage; expose the contract exit code.
        if int(exc.code or 0) != 0:
            _json_out({"error": "usage", "message": "invalid command arguments"})
        return int(exc.code)
    try:
        document = _load(args.file)
    except Exception as exc:
        _json_out({"error": "read_or_json_error", "message": str(exc)})
        return 2
    if args.command == "hash":
        try:
            digest = content_hash(document, args.kind)
            meta = METADATA[args.kind]
            result = {
                "artifact_kind": args.kind,
                "content_hash": digest,
                "id": meta["id_prefix"] + digest,
            }
            _json_out(result)
            return 0
        except Exception as exc:
            _json_out({"error": "read_or_json_error", "message": str(exc)})
            return 2
    try:
        related = _related(args.related)
        result = validate_artifact(document, expected_kind=args.kind, knowledge_dir=args.knowledge_dir, related_artifacts=related)
        _json_out(result.to_dict())
        return 0 if result.valid else 1
    except Exception as exc:
        _json_out({"error": "read_or_json_error", "message": str(exc)})
        return 2


__all__ = ["main"]
