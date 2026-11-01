"""Shared release identity and explicit distribution boundary.

Never discover release files with a recursive walk of the repository root.
This allowlist excludes user runs and credentials even when they are nearby.
"""
from __future__ import annotations
import json
from pathlib import Path
import re

ROOT_FILES = {"README.md", "AGENTS.md", "CHANGELOG.md", "release.json", "requirements-runtime.txt"}
DIRECTORY_PATTERNS = {
    "setups": ("SET-*.md",), "data": ("sources.jsonl", "evidence.jsonl", "visuals.jsonl", "relations.jsonl", "taxonomy.yaml"),
    "assets/diagrams": ("SET-*.svg",), "indexes": ("*.md",), "exports": ("setups.json", "setups.csv"),
    "schema": ("*.schema.json", "*.md"), "scripts": ("*.py", "requirements*.txt"),
    "recognition": ("*.py", "*.md", "catalog.json", "audit_links.json"),
    "evaluation": ("*.py", "*.md", "tests/*.py", "tests/raw_fixtures.json", "tests/raw_negative_designs.json", "tests/fixtures/checkpoint_v1.json"),
    "runtime": ("*.py", "*.md", "tests/*.py"), "paper": ("*.py", "*.md"), "connectors": ("*.py", "*.md", "requirements.txt"),
    "config": ("runtime.example.json",), "templates": ("*.md",),
    "research": ("*.md", "state.json", "development-checkpoint.json", "search-log.jsonl", "candidates.jsonl", "semantic-review.jsonl", "merge-decisions.jsonl"),
    "reports": ("*.md", "recognition-validation.json", "recognition-smoke.json", "recognition-situations.json", "validation-results.json", "rebuild-results.json", "ui-results.json", "runtime-validation.json", "workbench-validation.json", "soak-results.json", "soak-validation.json", "release-validation.json", "previews/*.png"),
    "site": ("*.html", "setups/SET-*.html", "source/SRC-*.html"),
    "workbench": ("index.html", "app.js", "style.css", "README.md", "tests/*.py", "vendor/lightweight-charts.js", "vendor/LICENSE.lightweight-charts", "vendor/NOTICE.lightweight-charts", "vendor/manifest.json"),
}
GENERATED_FILES = {"indexes/catalog.md", "indexes/aliases.md", "indexes/comparisons.md", "indexes/sources.md", "indexes/taxonomy.md", "recognition/catalog.json", "recognition/catalog.md"}
SECRET_NAMES = re.compile(r"(^|[._-])(secrets?|credentials?|password|token|private[-_]?key|api[-_]?key|env)($|[._-])", re.I)


def release_metadata(root):
    path=Path(root)/"release.json"
    if path.is_symlink():raise ValueError("release metadata must not be a symlink")
    row=json.loads(path.read_text(encoding="utf-8"))
    if not re.fullmatch(r"\d+\.\d+\.\d+",row["version"]):raise ValueError("release version must be semver")
    from datetime import date
    date.fromisoformat(row["content_date"])
    return row


def is_generated(relative):
    return relative in GENERATED_FILES or relative.startswith(("site/", "exports/"))


def safe_relative(relative):
    path=Path(relative)
    return not path.is_absolute() and bool(path.parts) and all(
        part not in {"..", "__pycache__", "node_modules"} and not part.startswith(".") and not SECRET_NAMES.search(part)
        for part in path.parts) and path.suffix.lower() not in {".db", ".sqlite", ".sqlite3", ".zip", ".pyc"}


def delivery_files(root, include_generated=True):
    """Return sorted, allowlisted regular files; never follow a symlink."""
    root=Path(root).resolve(); found=set()
    for name in ROOT_FILES:
        path=root/name
        if path.is_symlink():raise ValueError("delivery file is a symlink: "+name)
        if path.is_file():found.add(path)
    for directory,patterns in DIRECTORY_PATTERNS.items():
        base=root/directory
        if base.is_symlink():raise ValueError("delivery directory is a symlink: "+directory)
        for pattern in patterns:
            for path in base.glob(pattern):
                rel=path.relative_to(root).as_posix()
                if path.is_file() and safe_relative(rel) and (include_generated or not is_generated(rel)):
                    if path.is_symlink() or any(parent.is_symlink() for parent in path.parents if parent!=root and parent.is_relative_to(root)):
                        raise ValueError("delivery file is a symlink: "+rel)
                    found.add(path)
    return sorted(found,key=lambda path:path.relative_to(root).as_posix())


def allowed_manifest_path(root, relative):
    """Membership checked against the same policy used by build and validate."""
    if not safe_relative(relative):return False
    return relative in {path.relative_to(Path(root).resolve()).as_posix() for path in delivery_files(root)}
