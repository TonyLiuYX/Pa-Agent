import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
PYTHON = sys.executable
SCRIPT = ROOT / "scripts" / "kb.py"
OUTPUT = ROOT / "knowledge" / "data"
MTR_FILENAME = "价格行为学 - Rose 的 MTR (主趋势反转) - forecho's Blog 2.pdf"
MTR_SHA256 = "9ade5b6c706cdeff09fba8effadffc1d0bda3c4a2004774232bae24ed094e5be"
sys.path.insert(0, str(ROOT / "scripts"))
import kb  # noqa: E402


def snapshot_tree(path: Path) -> dict[str, bytes]:
    return {
        str(item.relative_to(path)): item.read_bytes()
        for item in path.rglob("*")
        if item.is_file()
    }


class KnowledgeBaseSmokeTests(unittest.TestCase):
    def test_generated_layers_and_required_provenance(self):
        manifest = json.loads((OUTPUT / "manifest.json").read_text(encoding="utf-8"))
        chunks = json.loads((OUTPUT / "chunks.json").read_text(encoding="utf-8"))["chunks"]
        records = json.loads((OUTPUT / "pa_records.json").read_text(encoding="utf-8"))["records"]
        self.assertGreaterEqual(len(manifest["documents"]), 1)
        self.assertTrue(chunks)
        self.assertTrue(records)
        for chunk in chunks:
            for field in ("chunk_id", "source_file", "page_start", "page_end", "title_path", "parent_chunk_id"):
                self.assertIn(field, chunk)
        for record in records:
            self.assertTrue(record["source_refs"])
            self.assertTrue(record["entry"])
            self.assertTrue(record["invalidation_stop"])
            self.assertTrue(record["targets_exit"])

    def test_query_returns_source_citation(self):
        proc = subprocess.run(
            [PYTHON, str(SCRIPT), "query", "Mother Bar", "突破", "止损"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
        result = json.loads(proc.stdout)
        self.assertGreater(result["count"], 0)
        self.assertTrue(result["results"][0]["citations"])
        self.assertTrue(result["results"][0]["citations"][0]["source_file"])

    def test_quality_report_passes_provenance_and_conflict_checks(self):
        report = json.loads((OUTPUT / "validation" / "quality_report.json").read_text(encoding="utf-8"))
        self.assertEqual(report["overall_status"], "pass")
        check_status = {check["name"]: check["status"] for check in report["checks"]}
        for name in (
            "generated values satisfy schema types and ranges",
            "chunk parent/adjacent relations and cross-page continuity",
            "normalized citations resolve to exact source chunks",
            "terminology aliases and conflict register",
        ):
            self.assertEqual(check_status.get(name), "pass", name)


class KnowledgeBaseCliContractTests(unittest.TestCase):
    def run_cli(self, *arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [PYTHON, str(SCRIPT), *arguments],
            cwd=ROOT,
            capture_output=True,
            text=True,
        )

    def assert_runtime_failure(self, proc: subprocess.CompletedProcess[str]) -> None:
        self.assertEqual(proc.returncode, kb.EXIT_RUNTIME_FAILURE, proc.stderr)
        self.assertNotIn("Traceback", proc.stderr)
        payload = json.loads(proc.stderr)
        self.assertEqual(payload["exit_code"], kb.EXIT_RUNTIME_FAILURE)
        self.assertTrue(payload["error"])

    def assert_malformed_manifest_does_not_change_build_snapshot(self, command: str) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir) / "kb"
            shutil.copytree(OUTPUT, output_dir)
            manifest_path = output_dir / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["documents"] = None
            manifest_path.write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")
            before = snapshot_tree(output_dir)

            proc = self.run_cli(
                command,
                "--source-dir",
                str(ROOT / "策略"),
                "--output-dir",
                str(output_dir),
                "--only",
                "rose-gap-bar",
            )

            self.assert_runtime_failure(proc)
            self.assertEqual(snapshot_tree(output_dir), before)

    def test_temp_directory_end_to_end_build_is_isolated(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir) / "mother-bar-kb"
            proc = self.run_cli(
                "build",
                "--source-dir",
                str(ROOT / "策略"),
                "--output-dir",
                str(output_dir),
                "--only",
                "rose-mother-bar",
            )
            self.assertEqual(proc.returncode, kb.EXIT_SUCCESS, proc.stderr)
            result = json.loads(proc.stdout)
            self.assertTrue(result["committed"])
            self.assertEqual(result["quality_status"], "pass")
            manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual([doc["source_id"] for doc in manifest["documents"]], ["rose-mother-bar"])
            extracted_ids = {path.name for path in (output_dir / "extracted").iterdir() if path.is_dir()}
            self.assertEqual(extracted_ids, {"rose-mother-bar"})

    def test_only_refuses_to_replace_existing_full_snapshot(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir) / "full-kb"
            shutil.copytree(OUTPUT, output_dir)
            before = snapshot_tree(output_dir)
            proc = self.run_cli(
                "build",
                "--source-dir",
                str(ROOT / "策略"),
                "--output-dir",
                str(output_dir),
                "--only",
                "rose-mother-bar",
            )
            self.assertEqual(proc.returncode, kb.EXIT_USAGE, proc.stderr)
            self.assertEqual(snapshot_tree(output_dir), before)
            manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(len(manifest["documents"]), 6)
            extracted_ids = {path.name for path in (output_dir / "extracted").iterdir() if path.is_dir()}
            self.assertEqual(extracted_ids, {doc["source_id"] for doc in manifest["documents"]})

    def test_missing_dependency_returns_nonzero_before_writing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            source_dir = Path(temp_dir) / "source"
            source_dir.mkdir()
            shutil.copy2(ROOT / "策略" / "Rose的策略.pdf", source_dir / "sample.pdf")
            output_dir = Path(temp_dir) / "output"
            with patch.object(kb, "PdfReader", None), patch.object(kb, "PYPDF_VERSION", None):
                code = kb.main(
                    [
                        "build",
                        "--source-dir",
                        str(source_dir),
                        "--output-dir",
                        str(output_dir),
                    ]
                )
            self.assertEqual(code, kb.EXIT_MISSING_DEPENDENCY)
            self.assertFalse(output_dir.exists())

    def test_validate_warning_and_quality_failure_exit_codes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir) / "kb"
            shutil.copytree(OUTPUT, output_dir)
            valid_proc = self.run_cli("validate", "--output-dir", str(output_dir))
            self.assertEqual(valid_proc.returncode, kb.EXIT_SUCCESS, valid_proc.stderr)
            valid_report = json.loads(valid_proc.stdout)
            self.assertEqual(valid_report["overall_status"], "pass")
            self.assertEqual(valid_report["warnings"], [])

            records_path = output_dir / "pa_records.json"
            records = json.loads(records_path.read_text(encoding="utf-8"))
            records["records"][0]["confidence"] = 2
            records_path.write_text(json.dumps(records, ensure_ascii=False), encoding="utf-8")
            quality_proc = self.run_cli("validate", "--output-dir", str(output_dir))
            self.assertEqual(quality_proc.returncode, kb.EXIT_QUALITY_FAILURE, quality_proc.stderr)
            quality_report = json.loads(quality_proc.stdout)
            self.assertEqual(quality_report["overall_status"], "fail")

    def test_invalid_parameter_and_missing_output_exit_codes(self):
        invalid_proc = self.run_cli("query", "Mother", "--limit", "0")
        self.assertEqual(invalid_proc.returncode, kb.EXIT_USAGE)

        missing_proc = self.run_cli("query", "Mother", "--output-dir", str(ROOT / "tmp" / "g004-no-such-output"))
        self.assertEqual(missing_proc.returncode, kb.EXIT_RUNTIME_FAILURE)

    def test_build_malformed_snapshot_returns_runtime_failure(self):
        self.assert_malformed_manifest_does_not_change_build_snapshot("build")

    def test_update_malformed_snapshot_returns_runtime_failure(self):
        self.assert_malformed_manifest_does_not_change_build_snapshot("update")

    def test_query_malformed_snapshot_returns_runtime_failure(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir) / "kb"
            shutil.copytree(OUTPUT, output_dir)
            records_path = output_dir / "pa_records.json"
            records = json.loads(records_path.read_text(encoding="utf-8"))
            records["records"] = None
            records_path.write_text(json.dumps(records, ensure_ascii=False), encoding="utf-8")

            proc = self.run_cli("query", "Mother", "--output-dir", str(output_dir))

            self.assert_runtime_failure(proc)

    def test_validate_malformed_snapshot_returns_runtime_failure(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir) / "kb"
            shutil.copytree(OUTPUT, output_dir)
            manifest_path = output_dir / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["documents"] = None
            manifest_path.write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")
            before = snapshot_tree(output_dir)

            proc = self.run_cli("validate", "--output-dir", str(output_dir))

            self.assert_runtime_failure(proc)
            self.assertEqual(snapshot_tree(output_dir), before)

    def test_partial_build_unreadable_manifest_returns_runtime_failure(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir) / "kb"
            shutil.copytree(OUTPUT, output_dir)
            (output_dir / "manifest.json").write_text("{not-json", encoding="utf-8")
            before = snapshot_tree(output_dir)

            proc = self.run_cli(
                "build",
                "--source-dir",
                str(ROOT / "策略"),
                "--output-dir",
                str(output_dir),
                "--only",
                "rose-gap-bar",
            )

            self.assert_runtime_failure(proc)
            self.assertEqual(snapshot_tree(output_dir), before)

    def test_quality_failure_does_not_replace_previous_snapshot(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            source_dir = Path(temp_dir) / "source"
            source_dir.mkdir()
            source_path = source_dir / MTR_FILENAME
            shutil.copy2(ROOT / "策略" / source_path.name, source_path)
            output_dir = Path(temp_dir) / "output"
            shutil.copytree(OUTPUT, output_dir)
            before = snapshot_tree(output_dir)

            fake_dependencies = {
                "python": {"version": "3.14.3"},
                "python_packages": {"pypdf": {"version": "test"}},
                "executables": {
                    "pdfinfo": {"path": "pdfinfo"},
                    "pdfimages": {"path": "pdfimages"},
                    "pdftoppm": {"path": "pdftoppm"},
                    "tesseract": {"path": "tesseract"},
                },
            }
            fake_doc = {
                "source_id": "rose-mtr",
                "source_file": str(source_path),
                "sha256": kb.sha256_file(source_path),
                "status": "failed",
                "attempted": True,
                "failure_kind": "injected",
                "page_count": None,
                "image_pages": [],
                "image_count": 0,
                "visual_review": {"rendered_for_ocr_pages": []},
            }
            failed_report = {
                "knowledge_base_version": kb.KB_VERSION,
                "generated_at": "test",
                "overall_status": "fail",
                "checks": [{"status": "fail", "name": "injected", "details": {}}],
                "warnings": [],
                "summary": {
                    "documents": 1,
                    "successful_or_partial_documents": 0,
                    "partial_documents": 0,
                    "failed_documents": 1,
                    "chunks": 0,
                    "retrieval_chunks": 0,
                    "normalized_records": 0,
                    "ocr_pages": 0,
                },
            }
            with patch.object(kb, "require_runtime_dependencies", return_value=fake_dependencies), patch.object(
                kb, "document_result", return_value=(fake_doc, []),
            ), patch.object(kb, "validate_output", return_value=failed_report):
                result = kb.build(source_dir, output_dir)

            self.assertEqual(result["exit_code"], kb.EXIT_QUALITY_FAILURE)
            self.assertFalse(result["committed"])
            self.assertEqual(snapshot_tree(output_dir), before)


class KnowledgeBaseG013AcceptanceTests(unittest.TestCase):
    """Final six-source snapshot acceptance checks (G013)."""

    def test_full_snapshot_counts_provenance_and_complete_source_queries(self):
        manifest = json.loads((OUTPUT / "manifest.json").read_text(encoding="utf-8"))
        chunks_payload = json.loads((OUTPUT / "chunks.json").read_text(encoding="utf-8"))
        records_payload = json.loads((OUTPUT / "pa_records.json").read_text(encoding="utf-8"))
        issues = json.loads((OUTPUT / "processing_issues.json").read_text(encoding="utf-8"))
        report = json.loads((OUTPUT / "validation" / "quality_report.json").read_text(encoding="utf-8"))

        expected_sources = {
            "rose-strategy",
            "rose-caveman",
            "rose-gap-bar",
            "rose-mtr",
            "rose-mother-bar",
            "nanqiao-checklist",
        }
        documents = manifest["documents"]
        self.assertEqual(len(documents), 6)
        self.assertEqual({doc["source_id"] for doc in documents}, expected_sources)
        self.assertTrue(all(doc["status"] == "success" for doc in documents))
        self.assertEqual(manifest["processing_summary"], {"attempted": 6, "success": 6, "partial": 0, "failed": 0})

        mtr = next(doc for doc in documents if doc["source_id"] == "rose-mtr")
        self.assertEqual(mtr["file_name"], MTR_FILENAME)
        self.assertEqual(mtr["sha256"], MTR_SHA256)
        self.assertEqual(mtr["page_count"], 7)
        self.assertFalse((OUTPUT / "extracted" / "rose-mtr" / "diagnostic.json").exists())
        self.assertFalse(any(item.get("source_fidelity", "").startswith("supplemental") for item in records_payload["records"]))
        self.assertEqual(issues["issues"], [])

        chunks = chunks_payload["chunks"]
        records = records_payload["records"]
        self.assertEqual(len(chunks), 82)
        self.assertEqual(sum(chunk.get("is_retrieval_chunk", False) for chunk in chunks), 65)
        self.assertEqual(len(records), 24)
        active_ids = {record["id"] for record in records if record.get("active", True) and record.get("record_status") != "invalidated"}
        self.assertEqual(len(active_ids), 24)

        check_status = {check["name"]: check["status"] for check in report["checks"]}
        for name in (
            "versioned JSON schemas validate generated artifacts",
            "chunk parent/adjacent relations and cross-page continuity",
            "semantic chunks preserve text and non-mechanical boundaries",
            "claim declaration types and claim-level provenance",
            "normalized citations resolve to exact source chunks",
            "representative retrieval queries return citations",
        ):
            self.assertEqual(check_status.get(name), "pass", name)
        self.assertEqual(report["overall_status"], "pass")
        self.assertEqual(report["warnings"], [])
        relation_details = next(check["details"] for check in report["checks"] if check["name"] == "chunk parent/adjacent relations and cross-page continuity")
        self.assertEqual(relation_details["duplicate_chunk_ids"], [])
        self.assertEqual(relation_details["duplicate_record_ids"], [])
        semantic_details = next(check["details"] for check in report["checks"] if check["name"] == "semantic chunks preserve text and non-mechanical boundaries")
        self.assertEqual(semantic_details["duplicate_retrieval_text_hashes"], [])

        # Every source has an independent query whose result exposes the full
        # market-context -> trigger -> entry -> invalidation/exit logic chain.
        source_queries = {
            "rose-strategy": "FOFS 失败的失败是波段",
            "rose-caveman": "Caveman（野人）策略",
            "rose-gap-bar": "Gap Bar 顺势回调策略",
            "rose-mtr": "MTR（主趋势反转）失败突破与被套盘",
            "rose-mother-bar": "Mother Bar（孕线）区域反转与突破",
            "nanqiao-checklist": "趋势回调 H2 / L2（顺大逆小）",
        }
        for source_id, query in source_queries.items():
            result = kb.search_knowledge(OUTPUT, query, limit=5)
            matching = [
                item
                for item in result["results"]
                if any(citation.get("source_id") == source_id for citation in item.get("citations", []))
            ]
            self.assertTrue(matching, f"{source_id}: {query}")
            logic = matching[0]["logic"]
            for field in ("market_context", "trigger", "entry", "invalidation_stop", "targets_exit"):
                self.assertTrue(logic.get(field), f"{source_id}/{field}")

        golden = json.loads((ROOT / "tests" / "fixtures" / "golden_queries.json").read_text(encoding="utf-8"))
        golden_ids = {record_id for case in golden for record_id in case["expected_record_ids"]}
        self.assertEqual(golden_ids, active_ids)


if __name__ == "__main__":
    unittest.main()
