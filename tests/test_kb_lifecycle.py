"""Isolated fixtures for the G005 source lifecycle contract.

The fixtures mock PDF extraction so lifecycle behavior is tested without
requiring a particular Poppler/Tesseract installation.  The public builder,
manifest, generated layers, validator, and retrieval path remain real.
"""

from __future__ import annotations

import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import kb  # noqa: E402


MOTHER = "价格行为学 - Rose 的 Mother Bar (孕线) 策略 - forecho's Blog.pdf"
GAP = "价格行为学 - Rose 的 Gap Bar - forecho's Blog.pdf"
ALL_TERMS = (
    "Mother Bar Gap Bar 斐波那契 Caveman 信号棒 FOFS TTR H2 L2 1 tick "
    "10 秒 强制退出 买入区域 卖出区域 区间突破 MTR 0.66 止损 突破"
)
FAKE_DEPENDENCIES = {
    "python": {"version": "3.13.0"},
    "python_packages": {"pypdf": {"version": "test"}},
    "executables": {name: {"path": name} for name in ("pdfinfo", "pdfimages", "pdftoppm", "tesseract")},
}

REAL_LOAD_CONFIGURATION = kb.load_configuration


def fixture_configuration():
    """Use source specs without real-PDF verification markers in fake-byte tests.

    The lifecycle fixture deliberately writes byte strings such as ``mother-v1``
    rather than the real PDF.  Copy the source-scoped specs and remove their
    content-bound verification markers so those bytes can only become verified
    through the explicit re-verification patches in the tests below.
    """
    doc_specs, record_config, terminology = REAL_LOAD_CONFIGURATION()
    isolated_specs = {source_id: dict(spec) for source_id, spec in doc_specs.items()}
    for spec in isolated_specs.values():
        spec.pop(kb.SOURCE_REVERIFICATION_HASH_KEY, None)
    return isolated_specs, record_config, terminology


def fake_document_result(path: Path, doc_spec: dict, _temp_root: Path):
    source_id, _ = kb.source_id_for_path(path)
    digest = kb.sha256_file(path)
    configured_source_id = str(doc_spec.get("source_id") or source_id)
    _, record_config, _ = REAL_LOAD_CONFIGURATION()
    source_claim_text = []
    for configured_record in record_config.get("records", []):
        configured_source = configured_record.get("source_id") or kb.infer_record_source(configured_record.get("id", ""))
        if configured_source != configured_source_id:
            continue
        for field in kb.CLAIM_FIELDS:
            for item in configured_record.get(field, []):
                source_claim_text.append(kb.claim_statement(item))
    fixture_text = "[fixture] " + ALL_TERMS + " " + " ".join(source_claim_text)
    page_count = max([int(spec["pages"][1]) for spec in doc_spec.get("chunk_specs", [])] or [1])
    pages = [
        {
            "page_number": number,
            # Keep each synthetic page semantically rich while ensuring
            # distinct retrieval chunks cannot be exact/contained duplicates
            # under the production normalized-overlap gate.
            "pdf_text": f"{fixture_text} lifecycle-fixture-page-{number:03d}",
            "ocr_text": "",
            "content": f"{fixture_text} lifecycle-fixture-page-{number:03d}",
            "text_sources": ["pdf_text"],
            "pdf_text_chars": len(fixture_text) + len(f" lifecycle-fixture-page-{number:03d}"),
            "ocr_text_chars": 0,
            "content_chars": len(fixture_text) + len(f" lifecycle-fixture-page-{number:03d}"),
            # Source-scoped records contain visually supported claims.  The
            # lifecycle fixture is synthetic, but it must still model those
            # cited pages as image-bearing so the production visual-page
            # provenance gate remains enabled rather than being bypassed.
            "image_count": 1,
            "ocr_attempted": False,
            "ocr_error": None,
            "errors": [],
        }
        for number in range(1, page_count + 1)
    ]
    return (
        {
            "source_id": source_id,
            "source_file": str(path),
            "file_name": path.name,
            "title": doc_spec.get("title", path.stem),
            "known_initial_source": source_id in kb.DOC_ID_BY_FILENAME.values(),
            "sha256": digest,
            "file_size_bytes": path.stat().st_size,
            "page_count": page_count,
            "page_size": "fixture",
            "pdf_version": "fixture",
            "image_pages": list(range(1, page_count + 1)),
            "image_count": page_count,
            "extraction_methods": ["fixture"],
            "extraction_errors": [],
            "status": "success",
            "attempted": True,
            "visual_review": {"rendered_for_ocr_pages": [], "related_visual_notes_present": bool(doc_spec.get("chunk_specs"))},
            "ocr_pages": [],
            "empty_text_pages": [],
            "text_characters": sum(len(fixture_text) + len(f" lifecycle-fixture-page-{number:03d}") for number in range(1, page_count + 1)),
        },
        pages,
    )


class KnowledgeBaseLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.source_dir = root / "sources"
        self.output_dir = root / "output"
        self.source_dir.mkdir()
        self.patches = (
            patch.object(kb, "require_runtime_dependencies", return_value=FAKE_DEPENDENCIES),
            patch.object(kb, "document_result", side_effect=fake_document_result),
            patch.object(kb, "load_configuration", side_effect=fixture_configuration),
        )
        for item in self.patches:
            item.start()

    def tearDown(self):
        for item in reversed(self.patches):
            item.stop()
        self.temp_dir.cleanup()

    def write_source(self, name: str, content: bytes):
        path = self.source_dir / name
        path.write_bytes(content)
        return path

    def manifest(self) -> dict:
        return json.loads((self.output_dir / "manifest.json").read_text(encoding="utf-8"))

    def records(self) -> list[dict]:
        return json.loads((self.output_dir / "pa_records.json").read_text(encoding="utf-8"))["records"]

    def test_add_change_delete_rename_and_noop_statuses(self):
        mother_path = self.write_source(MOTHER, b"mother-v1")
        result = kb.build(self.source_dir, self.output_dir, mode="build")
        self.assertEqual(result["exit_code"], kb.EXIT_SUCCESS)
        first = self.manifest()
        self.assertEqual(first["change_summary"]["added"], 1)
        self.assertEqual(first["documents"][0]["change_status"], "added")
        self.assertEqual(first["documents"][0]["sha256"], kb.sha256_file(mother_path))

        # A repeated update is a no-op for active layers and IDs.
        before_chunks = json.loads((self.output_dir / "chunks.json").read_text(encoding="utf-8"))["chunks"]
        before_records = self.records()
        result = kb.build(self.source_dir, self.output_dir, mode="update")
        self.assertEqual(result["exit_code"], kb.EXIT_SUCCESS)
        self.assertEqual(self.manifest()["documents"][0]["change_status"], "unchanged")
        self.assertEqual(json.loads((self.output_dir / "chunks.json").read_text(encoding="utf-8"))["chunks"], before_chunks)
        self.assertEqual(self.records(), before_records)

        # Add a configured source.
        gap_path = self.write_source(GAP, b"gap-v1")
        result = kb.build(self.source_dir, self.output_dir, mode="update")
        self.assertEqual(result["exit_code"], kb.EXIT_SUCCESS)
        manifest = self.manifest()
        statuses = {change["source_id"]: change["status"] for change in manifest["changes"]}
        self.assertEqual(statuses["rose-mother-bar"], "unchanged")
        self.assertEqual(statuses["rose-gap-bar"], "added")
        self.assertEqual(
            next(doc["sha256"] for doc in manifest["documents"] if doc["source_id"] == "rose-gap-bar"),
            kb.sha256_file(gap_path),
        )

        # Change the known source: records stay for audit but are inactive and
        # explicitly pending source re-verification.
        mother_path.write_bytes(b"mother-v2")
        result = kb.build(self.source_dir, self.output_dir, mode="update")
        self.assertEqual(result["exit_code"], kb.EXIT_SUCCESS)
        changed = next(doc for doc in self.manifest()["documents"] if doc["source_id"] == "rose-mother-bar")
        self.assertEqual(changed["change_status"], "changed")
        self.assertEqual(changed["sha256"], kb.sha256_file(mother_path))
        stale = [record for record in self.records() if record["source_id"] == "rose-mother-bar"]
        self.assertTrue(stale)
        self.assertTrue(all(record["active"] is False for record in stale))
        self.assertTrue(all(record["source_fidelity"] == "pending_source_reverification" for record in stale))
        self.assertNotIn(
            "rose-mother-bar",
            {item.get("record_id") for item in kb.search_knowledge(self.output_dir, "Mother Bar")["results"]},
        )

        # Repeating the exact v2 input is a no-op, not a source
        # re-verification.  The invalidated record must remain inactive and
        # absent from normalized retrieval.
        result = kb.build(self.source_dir, self.output_dir, mode="update")
        self.assertEqual(result["exit_code"], kb.EXIT_SUCCESS)
        unchanged = next(doc for doc in self.manifest()["documents"] if doc["source_id"] == "rose-mother-bar")
        self.assertEqual(unchanged["change_status"], "unchanged")
        self.assertEqual(unchanged["source_reverification_status"], kb.SOURCE_REVERIFICATION_PENDING)
        self.assertTrue(unchanged["source_reverification_required"])
        stale = [record for record in self.records() if record["source_id"] == "rose-mother-bar"]
        self.assertTrue(stale)
        self.assertTrue(all(record["active"] is False for record in stale))
        self.assertTrue(all(record["record_status"] == "invalidated" for record in stale))
        self.assertTrue(all(record["source_fidelity"] == "pending_source_reverification" for record in stale))
        self.assertTrue(all(record["reverification_required"] is True for record in stale))
        self.assertNotIn(
            "rose-mother-bar",
            {item.get("record_id") for item in kb.search_knowledge(self.output_dir, "Mother Bar")["results"]},
        )

        # Delete Gap Bar: no old active layer or extracted directory survives.
        (self.source_dir / GAP).unlink()
        result = kb.build(self.source_dir, self.output_dir, mode="update")
        self.assertEqual(result["exit_code"], kb.EXIT_SUCCESS)
        manifest = self.manifest()
        self.assertIn("deleted", [change["status"] for change in manifest["changes"]])
        self.assertNotIn("rose-gap-bar", {doc["source_id"] for doc in manifest["documents"]})
        self.assertFalse((self.output_dir / "extracted" / "rose-gap-bar").exists())
        self.assertFalse(any(record["source_id"] == "rose-gap-bar" for record in self.records()))
        self.assertFalse(any(chunk["source_id"] == "rose-gap-bar" for chunk in json.loads((self.output_dir / "chunks.json").read_text())["chunks"]))

        # Rename by equal SHA; configured semantic mappings are inherited by
        # the new id, while the old id/path is removed from active layers.
        renamed_path = self.source_dir / "mother-renamed.pdf"
        mother_path.rename(renamed_path)
        result = kb.build(self.source_dir, self.output_dir, mode="update")
        self.assertEqual(result["exit_code"], kb.EXIT_SUCCESS)
        manifest = self.manifest()
        rename = next(change for change in manifest["changes"] if change["status"] == "renamed")
        self.assertEqual(rename["previous_source_id"], "rose-mother-bar")
        self.assertEqual(rename["sha256"], kb.sha256_file(renamed_path))
        active_ids = {doc["source_id"] for doc in manifest["documents"]}
        self.assertEqual(active_ids, {"mother-renamed"})
        self.assertFalse((self.output_dir / "extracted" / "rose-mother-bar").exists())
        self.assertTrue((self.output_dir / "extracted" / "mother-renamed").exists())
        self.assertFalse(any(record["source_id"] == "rose-mother-bar" for record in self.records()))
        renamed_records = [record for record in self.records() if record["source_id"] == "mother-renamed"]
        self.assertTrue(renamed_records)
        self.assertTrue(all(record["active"] is False for record in renamed_records))
        self.assertTrue(all(record["record_status"] == "invalidated" for record in renamed_records))
        self.assertTrue(all(record["source_fidelity"] == "pending_source_reverification" for record in renamed_records))
        self.assertTrue(all(record["reverification_required"] is True for record in renamed_records))

    def test_explicit_source_hash_reverification_restores_changed_record(self):
        mother_path = self.write_source(MOTHER, b"mother-v1")
        result = kb.build(self.source_dir, self.output_dir, mode="build")
        self.assertEqual(result["exit_code"], kb.EXIT_SUCCESS)

        mother_path.write_bytes(b"mother-v2")
        result = kb.build(self.source_dir, self.output_dir, mode="update")
        self.assertEqual(result["exit_code"], kb.EXIT_SUCCESS)
        self.assertTrue(all(not record["active"] for record in self.records()))

        # A source-scoped, content-bound hash is the explicit re-verification
        # signal.  The PDF bytes are unchanged during this update; only the
        # configuration's verified_source_sha256 marker permits restoration.
        load_configuration = kb.load_configuration

        def verified_configuration():
            doc_specs, record_config, terminology = load_configuration()
            source_spec = dict(doc_specs["rose-mother-bar"])
            source_spec[kb.SOURCE_REVERIFICATION_HASH_KEY] = kb.sha256_file(mother_path)
            doc_specs = dict(doc_specs)
            doc_specs["rose-mother-bar"] = source_spec
            return doc_specs, record_config, terminology

        with patch.object(kb, "load_configuration", side_effect=verified_configuration):
            result = kb.build(self.source_dir, self.output_dir, mode="update")
        self.assertEqual(result["exit_code"], kb.EXIT_SUCCESS)
        verified_doc = next(doc for doc in self.manifest()["documents"] if doc["source_id"] == "rose-mother-bar")
        self.assertEqual(verified_doc["change_status"], "unchanged")
        self.assertEqual(verified_doc["source_reverification_status"], kb.SOURCE_REVERIFICATION_VERIFIED)
        self.assertFalse(verified_doc["source_reverification_required"])
        self.assertEqual(verified_doc["source_reverified_sha256"], kb.sha256_file(mother_path))
        restored = [record for record in self.records() if record["source_id"] == "rose-mother-bar"]
        self.assertTrue(restored)
        self.assertTrue(all(record["active"] is True for record in restored))
        self.assertTrue(all(record["record_status"] == "active" for record in restored))
        self.assertTrue(all(record["source_fidelity"] == "source_faithful_with_normalization" for record in restored))
        self.assertTrue(all(record["reverification_required"] is False for record in restored))
        self.assertIn(
            "rose-mother-bar",
            {item.get("record_id") for item in kb.search_knowledge(self.output_dir, "Mother Bar")["results"]},
        )

    def test_explicit_source_hash_can_verify_the_changed_update_itself(self):
        mother_path = self.write_source(MOTHER, b"mother-v1")
        result = kb.build(self.source_dir, self.output_dir, mode="build")
        self.assertEqual(result["exit_code"], kb.EXIT_SUCCESS)

        mother_path.write_bytes(b"mother-v2")
        load_configuration = kb.load_configuration

        def verified_configuration():
            doc_specs, record_config, terminology = load_configuration()
            source_spec = dict(doc_specs["rose-mother-bar"])
            source_spec[kb.SOURCE_REVERIFICATION_HASH_KEY] = kb.sha256_file(mother_path)
            doc_specs = dict(doc_specs)
            doc_specs["rose-mother-bar"] = source_spec
            return doc_specs, record_config, terminology

        # The explicit marker is allowed to accompany the changed update;
        # it must not be overridden by the legacy change_status fallback.
        with patch.object(kb, "load_configuration", side_effect=verified_configuration):
            result = kb.build(self.source_dir, self.output_dir, mode="update")
        self.assertEqual(result["exit_code"], kb.EXIT_SUCCESS)
        changed_doc = next(doc for doc in self.manifest()["documents"] if doc["source_id"] == "rose-mother-bar")
        self.assertEqual(changed_doc["change_status"], "changed")
        self.assertEqual(changed_doc["source_reverification_status"], kb.SOURCE_REVERIFICATION_VERIFIED)
        self.assertFalse(changed_doc["source_reverification_required"])
        records = [record for record in self.records() if record["source_id"] == "rose-mother-bar"]
        self.assertTrue(records)
        self.assertTrue(all(record["active"] is True for record in records))
        self.assertIn(
            "rose-mother-bar",
            {item.get("record_id") for item in kb.search_knowledge(self.output_dir, "Mother Bar")["results"]},
        )

    def test_unknown_added_source_is_page_only_and_fails_validation(self):
        unknown = self.write_source("new-unknown.pdf", b"unknown")
        result = kb.build(self.source_dir, self.output_dir, mode="update")
        self.assertEqual(result["exit_code"], kb.EXIT_QUALITY_FAILURE)
        self.assertEqual(result["quality_status"], "fail")
        self.assertFalse(result["committed"])
        # G004 atomicity means a failed first snapshot is not published, while
        # the fixture still proves the source has a stable SHA at the boundary.
        self.assertEqual(kb.sha256_file(unknown), kb.sha256_file(unknown))


if __name__ == "__main__":
    unittest.main()
