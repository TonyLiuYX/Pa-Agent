"""Import-boundary checks for the G027 storage layer and its test seam.

The storage package must stay persistence-self-contained: only stdlib,
``pa_engine.contracts`` and ``pa_engine.storage`` itself may be imported by
storage modules.  Upstream layers (paper, strategy, providers, market_sources,
market_view) must remain storage-free, and ``sqlite3`` may only appear in the
storage package.  The phase-2/g027 tests are the integration seam and may
import anything from ``pa_engine``.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
PA_ENGINE = ROOT / "pa_engine"
STORAGE = PA_ENGINE / "storage"
UPSTREAM_ROOTS = (
    "pa_engine.paper",
    "pa_engine.strategy",
    "pa_engine.providers",
    "pa_engine.market_sources",
    "pa_engine.market_view",
)
FORBIDDEN_STORAGE_ROOTS = (
    "pa_engine.paper",
    "pa_engine.strategy",
    "pa_engine.providers",
    "pa_engine.market_sources",
    "pa_engine.market_view",
    "pa_engine.strategies",
    "requests",
    "urllib",
    "socket",
    "openai",
    "deepseek",
    "alpaca",
    "websockets",
)
ALLOWED_STORAGE_ROOTS = ("pa_engine.contracts", "pa_engine.storage")
TEST_G027 = ROOT / "tests" / "phase2" / "g027"


def _module_root(module: str) -> str:
    return module.split(".", 1)[0]


def _relative_resolution(path: Path, level: int, module: str | None) -> str:
    package_parts = ["pa_engine", *path.relative_to(PA_ENGINE).parts[:-1]]
    package_parts = package_parts[: max(0, len(package_parts) - (level - 1))]
    base = ".".join(package_parts)
    return f"{base}.{module}" if module else base


def _imports(path: Path) -> list[tuple[str, str]]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    modules: list[tuple[str, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.extend((alias.name, path.name) for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0:
            modules.append((node.module or "", path.name))
        elif isinstance(node, ast.ImportFrom):
            modules.append((_relative_resolution(path, node.level, node.module), path.name))
    return modules


def test_storage_modules_import_only_stdlib_contracts_and_storage():
    for path in STORAGE.glob("*.py"):
        for module, _source in _imports(path):
            root = _module_root(module)
            assert root in sys.stdlib_module_names or module.startswith(ALLOWED_STORAGE_ROOTS), (
                path,
                module,
            )


def test_storage_modules_never_import_upstream_or_network_layers():
    for path in STORAGE.glob("*.py"):
        for module, _source in _imports(path):
            root = _module_root(module)
            assert not any(root == forbidden or module.startswith(forbidden + ".") for forbidden in FORBIDDEN_STORAGE_ROOTS), (
                path,
                module,
            )


def test_upstream_layers_never_import_pa_engine_storage():
    for root in UPSTREAM_ROOTS:
        for path in (PA_ENGINE / root).rglob("*.py"):
            for module, _source in _imports(path):
                assert module != "pa_engine.storage" and not module.startswith("pa_engine.storage."), (
                    path,
                    module,
                )


def test_sqlite3_is_imported_only_by_storage_modules():
    for path in PA_ENGINE.rglob("*.py"):
        for module, _source in _imports(path):
            if _module_root(module) == "sqlite3":
                assert STORAGE in path.parents, (path, module)


def test_g027_tests_are_the_pa_engine_integration_seam():
    for path in TEST_G027.glob("*.py"):
        if path.name in {"__init__.py", "test_import_boundary.py"}:
            continue
        for module, _source in _imports(path):
            root = _module_root(module)
            assert root in sys.stdlib_module_names or root == "pytest" or module.startswith("pa_engine."), (
                path,
                module,
            )
        imports = [
            module
            for module, _source in _imports(path)
            if module.startswith("pa_engine.")
        ]
        assert imports, path
