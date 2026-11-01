"""Public JSON contracts with offline validation and no network schema resolution."""
from __future__ import annotations

import json
from pathlib import Path
from .config import ROOT, canonical

SCHEMAS = {
    'market': 'market-event.schema.json', 'detection': 'detection.schema.json',
    'profile': 'paper.schema.json', 'account': 'paper.schema.json',
    'order': 'paper.schema.json', 'fill': 'paper.schema.json',
    'position': 'paper.schema.json', 'account_event': 'paper.schema.json',
    'annotation': 'annotation.schema.json', 'dataset': 'evaluation-dataset.schema.json',
    'envelope': 'runtime-envelope.schema.json', 'report': 'run-report.schema.json',
    'config': 'runtime-config.schema.json',
}


def validate_contract(kind, value, *, require_jsonschema=False):
    """Reject non-JSON numbers always; jsonschema is optional for core runtime."""
    value=json.loads(canonical(value))
    try:
        from jsonschema import Draft202012Validator, FormatChecker, ValidationError
    except ImportError:
        if require_jsonschema:
            raise RuntimeError('jsonschema is required for complete contract verification') from None
        return False
    schema=json.loads((ROOT/'schema'/SCHEMAS[kind]).read_text())
    if SCHEMAS[kind]=='paper.schema.json': schema={**schema,'oneOf':[{'$ref':'#/$defs/'+kind}]}
    try:
        Draft202012Validator(schema,format_checker=FormatChecker()).validate(value)
    except ValidationError:
        raise ValueError("invalid "+kind+" contract") from None
    return True


def envelope(run_id, row):
    return {'schema_version':'1.0.0','run_id':run_id, **row}
