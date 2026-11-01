"""Check schema, feature providers, variant coverage and generated source hashes."""
from __future__ import annotations

import ast
import importlib
import json
from .specs import ROOT, catalog, catalog_document, catalog_markdown, load_specs, VARIANTS
from .registry import FEATURE_REGISTRY

CAPABILITIES = {"bars", "prior_session", "previous_close", "daily_atr14", "liquidity_history", "classified_trades", "depth", "key_levels",
    "prior_volume_profile", "premarket", "volume_baseline", "parent_detection", "prior_tpo", "news", "exact_vwap",
    "historical_vwap", "equity_metadata", "synced_market", "daily_context", "short_bars", "ib", "consensus", "tpo",
    "volume_profile", "same_time14", "first_half_hour", "price_observation"}


def provider_fields():
    # No dynamic execution or test fixtures are accepted as feature providers.
    fields = set()
    for name in ("features.py", "engine.py"):
        tree = ast.parse((ROOT / "recognition" / name).read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Dict):
                fields.update(k.value for k in node.keys if isinstance(k, ast.Constant) and isinstance(k.value, str))
            if isinstance(node, ast.keyword) and node.arg:
                fields.add(node.arg)
            if isinstance(node, ast.Subscript) and isinstance(node.slice, ast.Constant) and isinstance(node.slice.value, str):
                fields.add(node.slice.value)
    fields.update(prefix + "_" + edge for prefix in ("imbalance", "imbalance150") for edge in ("edge", "risk"))
    fields.update({"stacked", "stacked150"})  # flags assigned by the same ratio loop
    return fields


def validate_specs(check_generated=True, require_jsonschema=False):
    errors = []
    rows = catalog()
    specs = load_specs(include_variants=True)
    discovered = provider_fields()
    provided = set(FEATURE_REGISTRY)
    for name, feature in FEATURE_REGISTRY.items():
        if name not in discovered:
            errors.append("registered feature has no implementation: " + name)
        if set(feature.requirements) - CAPABILITIES:
            errors.append("registered feature has unknown capabilities: " + name)
        components = feature.provider.split(".")
        provider = importlib.import_module(".".join(components[:2]))
        for component in components[2:]:
            provider = getattr(provider, component, None)
        if not callable(provider):
            errors.append("registered provider is not callable: " + name)
    for spec in specs:
        if set(spec["requirements"]) - CAPABILITIES:
            errors.append(spec["setup_id"] + " unknown data capability")
        captured = set()
        for index, st in enumerate(spec["stages"]):
            if st.get("same_frame") and index == 0:
                errors.append(spec["setup_id"] + " first stage cannot share a preceding frame")
            if "same_frame" in st and not isinstance(st["same_frame"], bool):
                errors.append(spec["setup_id"] + " invalid same_frame flag")
            for feature, op, value in st["tests"]:
                if feature not in provided:
                    errors.append(spec["setup_id"] + " feature has no provider: " + feature)
                if op not in {"eq", "gt", "ge", "lt", "le"}:
                    errors.append(spec["setup_id"] + " invalid predicate operation")
                if isinstance(value, str) and value.startswith("@") and value[1:] not in captured:
                    errors.append(spec["setup_id"] + " references capture before its definition")
            for target, feature in st["capture"].items():
                if feature not in provided:
                    errors.append(spec["setup_id"] + " capture has no provider: " + feature)
                captured.add(target)
        for feature, operation, value in spec["cancel"]:
            if feature not in provided:
                errors.append(spec["setup_id"] + " cancellation has no registered provider: " + feature)
        continuation = spec.get("continuation_scales", [])
        if any(not isinstance(s, int) or isinstance(s, bool) or s < 0 for s in continuation):
            errors.append(spec["setup_id"] + " invalid continuation scale")
    if len({s["setup_id"] for s in specs}) != 50:
        errors.append("expected 50 stable setup IDs")
    if any(r["variant_status"][v] != "implemented_experimental" for r in rows for v in r["variants"]):
        errors.append("specified variant lacks implementation")
    generated = catalog_document()
    if check_generated:
        path = ROOT / "recognition" / "catalog.json"
        if not path.exists() or json.loads(path.read_text()) != generated:
            errors.append("recognition/catalog.json is stale; regenerate with build.py or recognition catalog")
        readable = ROOT / "recognition" / "catalog.md"
        if not readable.exists() or readable.read_text(encoding="utf-8") != catalog_markdown(generated):
            errors.append("recognition/catalog.md is stale; regenerate with build.py or recognition catalog")
    schema_status = "not_executed"
    try:
        from jsonschema import Draft202012Validator
        schema = json.loads((ROOT / "schema" / "recognition.schema.json").read_text())
        Draft202012Validator.check_schema(schema)
        validator = Draft202012Validator(schema)
        for error in validator.iter_errors(generated):
            errors.append("recognition schema: " + error.message)
        schema_status = "passed" if not errors else "failed"
    except ImportError:
        if require_jsonschema:
            errors.append("jsonschema required but unavailable")
    except (ValueError, OSError) as exc:
        errors.append("recognition schema load: " + str(exc))
    return {"status": "failed" if errors else "passed", "setup_count": len(rows), "graph_count": len(specs),
            "variant_count": len(VARIANTS), "schema_checks": schema_status, "errors": errors,
            "market_validation": "not_run"}
