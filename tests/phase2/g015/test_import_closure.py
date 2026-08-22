"""Static import closure guard for the vendor-neutral market package."""
from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
MARKET_DIR = ROOT / "pa_engine" / "market"
FORBIDDEN_ROOTS = {
    "account", "alpaca", "anthropic", "broker", "deepseek", "http", "httpx", "model",
    "network", "openai", "paper", "polygon", "requests", "socket", "strategy", "urllib", "vendor",
    "websocket", "websockets", "yfinance", "aiohttp",
}


def _imported_modules(tree: ast.AST) -> set[str]:
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
    return modules


def test_market_package_imports_only_contracts_and_stdlib_modules():
    files = sorted(MARKET_DIR.glob("*.py"))
    assert files, "market package must contain implementation modules"
    for path in files:
        modules = _imported_modules(ast.parse(path.read_text(encoding="utf-8"), filename=str(path)))
        forbidden = {
            module
            for module in modules
            if module.split(".", 1)[0].lower() in FORBIDDEN_ROOTS
            or any(part.lower() in FORBIDDEN_ROOTS for part in module.split("."))
        }
        assert not forbidden, (path.name, sorted(forbidden))


def test_market_package_has_no_network_or_strategy_call_symbols():
    forbidden_tokens = {"requests", "httpx", "socket", "alpaca", "aiohttp", "openai", "anthropic", "deepseek", "polygon", "strategy", "paper_account", "yfinance"}
    for path in sorted(MARKET_DIR.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        names = {node.id.lower() for node in ast.walk(tree) if isinstance(node, ast.Name)}
        attrs = {node.attr.lower() for node in ast.walk(tree) if isinstance(node, ast.Attribute)}
        assert not (names | attrs) & forbidden_tokens, (path.name, sorted((names | attrs) & forbidden_tokens))
