#!/bin/sh
set -eu

PROJECT_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
PYTHON="$PROJECT_ROOT/.venv/bin/python"

if [ ! -x "$PYTHON" ]; then
    echo "missing $PYTHON; run: bash scripts/bootstrap_env.sh" >&2
    exit 1
fi

echo "== Python test suite =="
"$PYTHON" -m pytest -q "$PROJECT_ROOT/tests"

echo "== Packaged strategy registry =="
"$PYTHON" - "$PROJECT_ROOT" <<'PY'
import sys
import tempfile
from pathlib import Path

root = Path(sys.argv[1])
sys.path.insert(0, str(root))

from pa_engine.web.config import WebConfig
from pa_engine.web.workspace import ProductWorkspace

with tempfile.TemporaryDirectory() as directory:
    runtime_root = Path(directory)
    workspace = ProductWorkspace(
        WebConfig(
            database_path=str(runtime_root / "runtime.db"),
            workspace_path=str(runtime_root / "workspace"),
        )
    )
    bootstrap = workspace.bootstrap()
    strategies = workspace.strategies()
    assert bootstrap["labels"]["paper_only"] is True
    assert bootstrap["labels"]["no_live_orders"] is True
    assert len(strategies["items"]) == 21, strategies["items"]
    print(f"Paper Desk bootstrap loaded {len(strategies['items'])} executable strategies")
PY

echo "== Cloud lint and production build =="
cd "$PROJECT_ROOT/paper-desk-cloud"
npm ci
npm run lint
npm run build
npm audit --omit=dev --audit-level=high

echo "== Public release verification passed =="
