from __future__ import annotations

import copy
import io
import json
import zipfile
from pathlib import Path

import pytest

import pa_engine.web.setup_bundle as setup_bundle_module
from pa_engine.web.config import WebConfigError
from pa_engine.web.setup_bundle import REQUIRED_FILES, SetupBundleImporter

from .conftest import build_setup_bundle, make_workspace


def test_valid_bundle_is_grounded_atomic_idempotent_and_custom_defaults_off(tmp_path: Path):
    _config, workspace = make_workspace(tmp_path)
    archive = build_setup_bundle()

    imported = workspace.import_setup(archive)
    assert imported["imported"] is True
    assert imported["validation"] == {"ok": True, "checks": 12}
    strategy_id = imported["bundle"]["setup_ids"][0]
    assert strategy_id.startswith("pa:user-")

    catalog = workspace.strategies()
    assert catalog["counts"] == {"total": 22, "built_in": 21, "custom": 1, "selected": 21}
    custom = next(item for item in catalog["items"] if item["strategy_id"] == strategy_id)
    assert custom["custom"] is True
    assert custom["selected"] is False
    assert workspace.registry.load_packet(strategy_id).ok is True

    target = Path(workspace.config.workspace_path) / "setups" / imported["bundle"]["bundle_id"]
    assert {path.name for path in target.iterdir()} == REQUIRED_FILES
    assert (target / "source.pdf").is_file()

    repeated = workspace.import_setup(archive)
    assert repeated["imported"] is False
    assert repeated["idempotent"] is True
    assert workspace.strategies()["counts"]["total"] == 22


@pytest.mark.parametrize(
    ("mutate", "code"),
    [
        (
            lambda files: files["bundle.json"].update(
                {"files": {"source.pdf": "0" * 64, "manifest.json": "0" * 64, "chunks.json": "0" * 64, "pa_records.json": "0" * 64}}
            ),
            "bundle_hash_mismatch",
        ),
        (
            lambda files: files["chunks.json"]["chunks"][0].update({"page_start": 2, "page_end": 2, "page_range": [2, 2]}),
            "bundle_chunk_invalid",
        ),
        (
            lambda files: files["pa_records.json"]["records"][0]["trigger"][0]["source_refs"][0].update({"chunk_id": "missing"}),
            "bundle_reference_invalid",
        ),
        (
            lambda files: files["pa_records.json"]["records"][0]["trigger"][0].update({"declaration_type": "invented"}),
            "bundle_schema_invalid",
        ),
        (
            lambda files: files["chunks.json"]["chunks"].append(copy.deepcopy(files["chunks.json"]["chunks"][0])),
            "bundle_chunk_invalid",
        ),
        (
            lambda files: files["chunks.json"]["chunks"][0].update({"page_range": [1, 2]}),
            "bundle_chunk_invalid",
        ),
        (
            lambda files: files["pa_records.json"]["records"].append(
                copy.deepcopy(files["pa_records.json"]["records"][0])
            ),
            "bundle_record_invalid",
        ),
    ],
)
def test_semantic_rejections_leave_no_partial_install(tmp_path: Path, mutate, code: str):
    importer = SetupBundleImporter(tmp_path / ".pa-workspace")
    with pytest.raises(WebConfigError) as error:
        importer.import_archive(build_setup_bundle(mutate=mutate))
    assert error.value.code == code
    setup_root = tmp_path / ".pa-workspace" / "setups"
    assert not setup_root.exists() or list(setup_root.iterdir()) == []


def test_zip_traversal_unknown_files_and_strategy_conflicts_are_rejected_atomically(tmp_path: Path):
    valid = build_setup_bundle()
    with zipfile.ZipFile(io.BytesIO(valid)) as source:
        values = {name: source.read(name) for name in source.namelist()}
    bad = io.BytesIO()
    with zipfile.ZipFile(bad, "w") as output:
        for name, value in values.items():
            output.writestr(name, value)
        output.writestr("../escape.py", b"print('no')")

    importer = SetupBundleImporter(tmp_path / ".pa-workspace")
    with pytest.raises(WebConfigError) as traversal:
        importer.import_archive(bad.getvalue())
    assert traversal.value.code == "bundle_file_set_invalid"
    assert not (tmp_path / "escape.py").exists()

    strategy_id = next(item.removeprefix("pa:") for item in json.loads(values["bundle.json"])["setup_ids"])
    with pytest.raises(WebConfigError) as collision:
        importer.import_archive(valid, existing_strategy_ids=["pa:" + strategy_id])
    assert collision.value.code == "strategy_id_conflict"
    setup_root = tmp_path / ".pa-workspace" / "setups"
    assert not setup_root.exists() or list(setup_root.iterdir()) == []


def test_archive_and_extracted_size_limits_reject_before_any_write(tmp_path: Path, monkeypatch):
    valid = build_setup_bundle()
    importer = SetupBundleImporter(tmp_path / ".pa-workspace")

    monkeypatch.setattr(setup_bundle_module, "MAX_ARCHIVE_BYTES", len(valid) - 1)
    with pytest.raises(WebConfigError) as archive_limit:
        importer.import_archive(valid)
    assert archive_limit.value.code == "bundle_too_large"

    monkeypatch.setattr(setup_bundle_module, "MAX_ARCHIVE_BYTES", 50 * 1024 * 1024)
    monkeypatch.setattr(setup_bundle_module, "MAX_EXTRACTED_BYTES", 1)
    with pytest.raises(WebConfigError) as extracted_limit:
        importer.import_archive(valid)
    assert extracted_limit.value.code == "bundle_too_large"
    assert not (tmp_path / ".pa-workspace" / "setups").exists()
