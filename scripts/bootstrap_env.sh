#!/bin/sh
set -euo pipefail

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
VENV="$ROOT/.venv"

echo "== bootstrap: PA knowledge-base environment =="
echo "repo root: $ROOT"

if [ -d "$VENV/bin" ]; then
    echo "== reusing existing venv: $VENV =="
else
    echo "== creating venv: python3 -m venv $VENV =="
    python3 -m venv "$VENV"
fi

echo "== installing pinned dependencies from requirements-lock.txt =="
"$VENV/bin/python" -m pip install -r "$ROOT/requirements-lock.txt"

echo "== bootstrap complete =="
echo "install the browser used by UI tests: $VENV/bin/python -m playwright install chromium"
echo "then verify the public release: bash scripts/verify_public.sh"
