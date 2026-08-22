import ast
from pathlib import Path


ROOT = Path(__file__).parents[3] / "pa_engine" / "paper" / "core"


def test_core_imports_only_stdlib_and_local_modules():
    forbidden = {"feed", "timer", "database", "db", "broker", "sqlite", "alpaca", "requests", "httpx"}
    for path in ROOT.glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [node.module or ""]
            else:
                continue
            assert not any(any(token in name.lower().split(".") for token in forbidden) for name in names), (path, names)
