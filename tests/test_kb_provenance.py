"""G006 schema, claim provenance, and independent golden checks."""

from __future__ import annotations

import copy
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "knowledge" / "data"
SCRIPT = ROOT / "scripts" / "kb.py"
sys.path.insert(0, str(ROOT / "scripts"))
import kb  # noqa: E402


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


class KnowledgeBaseProvenanceTests(unittest.TestCase):
    def copy_output(self) -> tuple[tempfile.TemporaryDirectory, Path]:
        holder = tempfile.TemporaryDirectory()
        output = Path(holder.name) / "kb"
        shutil.copytree(OUTPUT, output)
        return holder, output

    def validate_nonzero(self, output: Path) -> dict:
        proc = subprocess.run(
            [sys.executable, str(SCRIPT), "validate", "--output-dir", str(output)],
            cwd=ROOT,
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(proc.returncode, kb.EXIT_SUCCESS, proc.stderr)
        if proc.stdout.strip():
            return json.loads(proc.stdout)
        return json.loads(proc.stderr)

    def add_synthetic_external_fixture(self, output: Path) -> tuple[dict, Path]:
        """Add a schema-compliant external derived claim and diagnostic.

        The final six-source snapshot intentionally has no failed-source
        diagnostic or supplemental MTR record.  These fixtures exercise the
        same external provenance contract in an isolated copy instead of
        depending on either legacy artifact.
        """
        manifest = read_json(output / "manifest.json")
        document = manifest["documents"][0]
        external_ref = {
            "chunk_id": None,
            "source_id": document["source_id"],
            "source_file": document["source_file"],
            "source_sha256": document["sha256"],
            "page_start": None,
            "page_end": None,
            "title_path": ["Synthetic external fixture"],
            "source_locator": "synthetic external diagnostic",
            "external_url": "https://example.invalid/synthetic-external",
            "provenance_status": "supplemental_external_due_to_pdf_failure",
        }
        records_path = output / "pa_records.json"
        records = read_json(records_path)
        record = records["records"][0]
        claim = {
            "claim_id": "synthetic-external:trigger:1",
            "statement": "Synthetic external derived claim",
            "declaration_type": "derived_interpretation",
            "rationale": "Synthetic fixture used to test external provenance boundaries.",
            "source_refs": [external_ref],
        }
        record["trigger"].append(claim)
        records_path.write_text(json.dumps(records, ensure_ascii=False), encoding="utf-8")

        diagnostic_path = output / "extracted" / document["source_id"] / "diagnostic.json"
        diagnostic = {
            "source_id": document["source_id"],
            "source_file": document["source_file"],
            "file_name": document["file_name"],
            "title": "Synthetic external diagnostic",
            "sha256": document["sha256"],
            "status": "failed",
            "attempted": True,
            "page_count": None,
            "image_pages": [],
            "extraction_errors": ["synthetic external fixture"],
        }
        diagnostic_path.write_text(json.dumps(diagnostic, ensure_ascii=False), encoding="utf-8")
        self.assertEqual(kb.validate_json_artifact(records, kb.ARTIFACT_SCHEMAS["pa_records.json"]), [])
        self.assertEqual(kb.validate_json_artifact(diagnostic, kb.DIAGNOSTIC_SCHEMA_FILENAME), [])
        return claim, diagnostic_path

    def test_every_generated_artifact_directly_matches_versioned_schema(self):
        for relative_path, schema_filename in kb.ARTIFACT_SCHEMAS.items():
            value = read_json(OUTPUT / relative_path)
            errors = kb.validate_json_artifact(value, schema_filename)
            self.assertEqual(errors, [], f"{relative_path}: {errors}")
        for path in (OUTPUT / "extracted").glob("*/diagnostic.json"):
            errors = kb.validate_json_artifact(read_json(path), kb.DIAGNOSTIC_SCHEMA_FILENAME)
            self.assertEqual(errors, [], str(path))

    def test_required_field_or_type_mutations_fail_schema_validation(self):
        mutations = {
            "manifest.json": lambda value: value.pop("generated_at"),
            "chunks.json": lambda value: value["chunks"][0].__setitem__("page_start", "wrong"),
            "pa_records.json": lambda value: value["records"][0].__setitem__("trigger", "wrong"),
            "terminology.json": lambda value: value["terms"][0].__setitem__("term_en", 9),
            "processing_issues.json": lambda value: value.__setitem__("issues", "wrong"),
            "visual_index.json": lambda value: value.__setitem__("documents", "wrong"),
        }
        for relative_path, mutate in mutations.items():
            holder, output = self.copy_output()
            try:
                path = output / relative_path
                value = read_json(path)
                mutate(value)
                path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
                report = self.validate_nonzero(output)
                if "overall_status" in report:
                    self.assertEqual(report["overall_status"], "fail", relative_path)
                    check = next(item for item in report["checks"] if item["name"] == "versioned JSON schemas validate generated artifacts")
                    self.assertEqual(check["status"], "fail", relative_path)
            finally:
                holder.cleanup()

    def test_quality_report_schema_rejects_mutation_without_circular_truth(self):
        report = read_json(OUTPUT / "validation" / "quality_report.json")
        report.pop("checks")
        self.assertTrue(kb.validate_json_artifact(report, kb.ARTIFACT_SCHEMAS["validation/quality_report.json"]))

    def test_manifest_reverification_fields_are_schema_typed_when_present(self):
        mutations = {
            "source_reverification_status": 9,
            "source_reverification_required": "wrong",
            "source_reverified_sha256": "wrong",
            "source_reverification_signal": 9,
            "invalidation_reason": 9,
        }
        for field, invalid_value in mutations.items():
            holder, output = self.copy_output()
            try:
                path = output / "manifest.json"
                manifest = read_json(path)
                manifest["documents"][0][field] = invalid_value
                path.write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")
                report = self.validate_nonzero(output)
                self.assertEqual(report["overall_status"], "fail", field)
                check = next(item for item in report["checks"] if item["name"] == "versioned JSON schemas validate generated artifacts")
                self.assertEqual(check["status"], "fail", field)
            finally:
                holder.cleanup()

    def test_diagnostic_required_field_mutation_fails(self):
        holder, output = self.copy_output()
        try:
            _, path = self.add_synthetic_external_fixture(output)
            value = read_json(path)
            value.pop("source_id")
            path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
            proc = subprocess.run(
                [sys.executable, str(SCRIPT), "validate", "--output-dir", str(output)],
                cwd=ROOT,
                capture_output=True,
                text=True,
            )
            self.assertEqual(proc.returncode, kb.EXIT_QUALITY_FAILURE, proc.stderr)
        finally:
            holder.cleanup()

    def test_claims_have_declaration_type_and_resolvable_refs(self):
        records = read_json(OUTPUT / "pa_records.json")["records"]
        declaration_types = set()
        for record in records:
            for field in kb.CLAIM_FIELDS:
                for claim in record[field]:
                    self.assertIsInstance(claim, dict)
                    self.assertTrue(claim["statement"])
                    self.assertIn(claim["declaration_type"], kb.DECLARATION_TYPES)
                    declaration_types.add(claim["declaration_type"])
                    self.assertTrue(claim["source_refs"])
                    if claim["declaration_type"] in {"derived_interpretation", "source_not_stated"}:
                        self.assertTrue(claim.get("rationale"))
        self.assertIn("source_assertion", declaration_types)
        self.assertIn("derived_interpretation", declaration_types)

    def test_synthetic_external_derived_claim_and_diagnostic_are_accepted(self):
        holder, output = self.copy_output()
        try:
            self.add_synthetic_external_fixture(output)
            proc = subprocess.run(
                [sys.executable, str(SCRIPT), "validate", "--output-dir", str(output)],
                cwd=ROOT,
                capture_output=True,
                text=True,
            )
            self.assertEqual(proc.returncode, kb.EXIT_SUCCESS, proc.stderr)
            report = json.loads(proc.stdout)
            self.assertEqual(report["overall_status"], "pass")
            claim_check = next(item for item in report["checks"] if item["name"] == "claim declaration types and claim-level provenance")
            self.assertEqual(claim_check["status"], "pass")
        finally:
            holder.cleanup()

    def test_external_supplemental_cannot_be_declared_source_assertion(self):
        holder, output = self.copy_output()
        try:
            self.add_synthetic_external_fixture(output)
            path = output / "pa_records.json"
            records = read_json(path)
            claim = next(item for item in records["records"][0]["trigger"] if item["claim_id"] == "synthetic-external:trigger:1")
            claim["declaration_type"] = "source_assertion"
            claim.pop("rationale", None)

            schema_errors = kb.validate_json_artifact(records, kb.ARTIFACT_SCHEMAS["pa_records.json"])
            self.assertTrue(schema_errors)

            path.write_text(json.dumps(records, ensure_ascii=False), encoding="utf-8")
            proc = subprocess.run(
                [sys.executable, str(SCRIPT), "validate", "--output-dir", str(output)],
                cwd=ROOT,
                capture_output=True,
                text=True,
            )
            self.assertEqual(proc.returncode, kb.EXIT_QUALITY_FAILURE, proc.stderr)
            report = json.loads(proc.stdout)
            self.assertEqual(report["overall_status"], "fail")
            claim_check = next(item for item in report["checks"] if item["name"] == "claim declaration types and claim-level provenance")
            self.assertEqual(claim_check["status"], "fail")
            self.assertTrue(any("source_assertion" in error for error in claim_check["details"]["errors"]))
        finally:
            holder.cleanup()

    def test_mixed_local_and_external_refs_cannot_be_source_assertion(self):
        holder, output = self.copy_output()
        try:
            self.add_synthetic_external_fixture(output)
            records_path = output / "pa_records.json"
            chunks = read_json(output / "chunks.json")["chunks"]
            records = read_json(records_path)
            claim = next(item for item in records["records"][0]["trigger"] if item["claim_id"] == "synthetic-external:trigger:1")
            local_chunk = next(
                chunk
                for chunk in chunks
                if chunk["source_id"] == claim["source_refs"][0]["source_id"] and chunk["chunk_id"] != f"{claim['source_refs'][0]['source_id']}:root"
            )
            claim["declaration_type"] = "source_assertion"
            claim.pop("rationale", None)
            claim["statement"] = local_chunk["source_text"][:80]
            claim["source_refs"].append(
                {
                    key: local_chunk[key]
                    for key in (
                        "chunk_id",
                        "source_id",
                        "source_file",
                        "source_sha256",
                        "page_start",
                        "page_end",
                        "title_path",
                        "source_locator",
                        "provenance_status",
                    )
                }
            )

            schema_errors = kb.validate_json_artifact(records, kb.ARTIFACT_SCHEMAS["pa_records.json"])
            self.assertTrue(schema_errors)

            records_path.write_text(json.dumps(records, ensure_ascii=False), encoding="utf-8")
            proc = subprocess.run(
                [sys.executable, str(SCRIPT), "validate", "--output-dir", str(output)],
                cwd=ROOT,
                capture_output=True,
                text=True,
            )
            self.assertEqual(proc.returncode, kb.EXIT_QUALITY_FAILURE, proc.stderr)
            report = json.loads(proc.stdout)
            self.assertEqual(report["overall_status"], "fail")
            claim_check = next(item for item in report["checks"] if item["name"] == "claim declaration types and claim-level provenance")
            self.assertEqual(claim_check["status"], "fail")
            self.assertTrue(any("source_assertion" in error for error in claim_check["details"]["errors"]))
        finally:
            holder.cleanup()

    def test_provenance_negative_matrix_fails_overall_validation(self):
        for label in (
            "unsupported trigger",
            "wrong page",
            "duplicate id",
            "broken link",
            "wrong visual ref",
            "wrong source hash",
            "orphan source chunk",
            "orphan source record",
        ):
            holder, output = self.copy_output()
            try:
                if label in {"unsupported trigger", "wrong page", "broken link"}:
                    path = output / "pa_records.json"
                elif label in {"duplicate id", "wrong visual ref", "wrong source hash", "orphan source chunk"}:
                    path = output / "chunks.json"
                else:
                    path = output / "pa_records.json"
                value = read_json(path)
                if label == "duplicate id":
                    value["chunks"][1]["chunk_id"] = value["chunks"][0]["chunk_id"]
                elif label == "wrong visual ref":
                    value["chunks"][1]["related_image_pages"] = [999]
                elif label == "wrong source hash":
                    value["chunks"][0]["source_sha256"] = "0" * 64
                elif label == "orphan source chunk":
                    template = next(chunk for chunk in value["chunks"] if chunk["chunk_id"].endswith(":root"))
                    rogue = dict(template)
                    rogue.update(
                        {
                            "chunk_id": "rogue:root",
                            "chunk_key": "root",
                            "source_id": "rogue-source",
                            "source_file": "synthetic/rogue.pdf",
                            "source_sha256": "0" * 64,
                            "page_start": 1,
                            "page_end": 1,
                            "page_range": [1, 1],
                            "title_path": ["Rogue source"],
                            "parent_chunk_id": "",
                            "adjacent_chunk_ids": [],
                            "source_text": "Synthetic rogue source chunk",
                            "source_locator": "synthetic rogue source",
                            "semantic_boundary": "Synthetic source root context; not a fixed-size retrieval chunk.",
                            "provenance_status": "local_pdf_text",
                            "is_retrieval_chunk": False,
                            "is_cross_page": False,
                            "related_image_pages": [],
                        }
                    )
                    value["chunks"].append(rogue)
                elif label == "orphan source record":
                    template = value["records"][0]
                    rogue = copy.deepcopy(template)
                    rogue["id"] = "rogue-record"
                    rogue["source_id"] = "rogue-source"
                    rogue["source_files"] = ["rogue"]
                    for field in kb.CLAIM_FIELDS:
                        for index, claim in enumerate(rogue.get(field, []), start=1):
                            claim["claim_id"] = f"rogue-record:{field}:{index}"
                    value["records"].append(rogue)
                else:
                    if label == "unsupported trigger":
                        target = next(item for item in value["records"] if item["id"] == "nanqiao-breakout-failure-breakout")
                        target["trigger"][0]["statement"] = "unsupported trigger test string"
                    elif label == "wrong page":
                        value["records"][0]["source_refs"][0]["page_start"] = 999
                    elif label == "broken link":
                        value["records"][0]["source_refs"][0]["chunk_id"] = "missing:chunk"
                path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
                report = self.validate_nonzero(output)
                if "overall_status" in report:
                    self.assertEqual(report["overall_status"], "fail", label)
                    if label == "orphan source chunk":
                        relation_check = next(item for item in report["checks"] if item["name"] == "chunk parent/adjacent relations and cross-page continuity")
                        relation_errors = relation_check["details"].get("provenance_errors", []) + relation_check["details"].get("relation_errors", [])
                        self.assertTrue(any("rogue" in error and "manifest" in error for error in relation_errors))
            finally:
                holder.cleanup()

    def test_golden_query_uses_static_expected_ids_claims_and_citations(self):
        golden = read_json(ROOT / "tests" / "fixtures" / "golden_queries.json")
        for case in golden:
            result = kb.search_knowledge(OUTPUT, case["query"], limit=5)
            ids = [item.get("record_id") for item in result["results"]]
            for expected_id in case["expected_record_ids"]:
                self.assertIn(expected_id, ids)
                record_result = next(item for item in result["results"] if item.get("record_id") == expected_id)
                for field in ("market_context", "trigger", "entry", "invalidation_stop", "targets_exit"):
                    self.assertTrue(record_result["logic"].get(field), f"{case['query']}/{expected_id}/{field}")
            for claim in case["expected_claims"]:
                record_result = next(item for item in result["results"] if item.get("record_id") == claim["record_id"])
                matching_claims = [item for item in record_result["logic"].get(claim["field"], []) if item.get("statement") == claim["statement"]]
                statements = [item.get("statement") for item in matching_claims]
                self.assertIn(claim["statement"], statements)
                self.assertTrue(matching_claims[0].get("source_refs"))
                self.assertIn(matching_claims[0].get("declaration_type"), kb.DECLARATION_TYPES)
                if matching_claims[0]["declaration_type"] in {"derived_interpretation", "source_not_stated"}:
                    self.assertTrue(matching_claims[0].get("rationale"))
            for citation in case["expected_citations"]:
                record_result = next(item for item in result["results"] if item.get("record_id") == citation["record_id"])
                self.assertTrue(any(
                    ref.get("chunk_id") == citation["chunk_id"]
                    and ref.get("page_start") == citation["page_start"]
                    and ref.get("page_end") == citation["page_end"]
                    for ref in record_result["citations"]
                ))

    def test_source_scoped_configuration_has_one_owner_per_file(self):
        source_dir = ROOT / "knowledge" / "config" / "sources"
        files = sorted(source_dir.glob("*.json"))
        self.assertEqual({path.stem for path in files}, set(kb.load_configuration()[0]))
        for path in files:
            value = read_json(path)
            self.assertEqual(value["source_id"], path.stem)
            self.assertTrue(value["document_spec"])
            self.assertTrue(all(record["source_id"] == path.stem for record in value["records"]))
            self.assertIn("terminology", value)
            self.assertIsInstance(value["terminology"].get("terms"), list)
            self.assertIsInstance(value["terminology"].get("conflicts"), list)

    def test_scoped_terminology_is_used_without_shared_aggregate(self):
        source_dir = ROOT / "knowledge" / "config" / "sources"
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir) / "config"
            shutil.copytree(source_dir, config_dir / "sources")
            (config_dir / "terminology.json").write_text("not-used", encoding="utf-8")
            original = kb.CONFIG_DIR
            try:
                kb.CONFIG_DIR = config_dir
                _, _, terminology = kb.load_configuration()
            finally:
                kb.CONFIG_DIR = original
            self.assertTrue(terminology["terms"])
            self.assertTrue(terminology["conflicts"])


if __name__ == "__main__":
    unittest.main()
