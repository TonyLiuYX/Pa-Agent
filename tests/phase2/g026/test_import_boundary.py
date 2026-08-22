from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
EXIT = ROOT / "pa_engine" / "paper" / "exit"


def test_exit_imports_stay_in_memory_and_out_of_forbidden_layers():
    forbidden = {"sqlite3", "requests", "urllib", "socket", "broker", "deepseek", "openai", "persistence"}
    for path in EXIT.glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [alias.name.lower() for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                if node.level:
                    continue
                names = [(node.module or "").lower()]
            else:
                continue
            assert not any(any(token in name for token in forbidden) for name in names), (path, names)


def test_exit_source_does_not_implement_account_math_or_model_calls():
    for path in EXIT.glob("*.py"):
        source = path.read_text(encoding="utf-8").lower()
        assert "calculate_settlement" not in source
        assert "quantity_for_balance" not in source
        assert "deepseek" not in source
        assert "openai" not in source
