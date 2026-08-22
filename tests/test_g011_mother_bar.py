"""G011 source-scoped Mother Bar extraction, provenance and query checks."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "kb.py"
SOURCE_DIR = ROOT / "策略"
CONFIG_PATH = ROOT / "knowledge" / "config" / "sources" / "rose-mother-bar.json"
GOLDEN_PATH = ROOT / "tests" / "fixtures" / "g011_mother_bar_golden.json"


class MotherBarG011Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_dir = tempfile.TemporaryDirectory(prefix="g011-mother-bar-")
        cls.output_dir = Path(cls.temp_dir.name) / "knowledge"
        proc = subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "build",
                "--source-dir",
                str(SOURCE_DIR),
                "--output-dir",
                str(cls.output_dir),
                "--only",
                "rose-mother-bar",
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            raise AssertionError(f"isolated G011 build failed: {proc.returncode}\n{proc.stdout}\n{proc.stderr}")
        cls.build_result = json.loads(proc.stdout)
        cls.manifest = json.loads((cls.output_dir / "manifest.json").read_text(encoding="utf-8"))
        cls.chunks = json.loads((cls.output_dir / "chunks.json").read_text(encoding="utf-8"))["chunks"]
        cls.records = json.loads((cls.output_dir / "pa_records.json").read_text(encoding="utf-8"))["records"]
        cls.visual_index = json.loads((cls.output_dir / "visual_index.json").read_text(encoding="utf-8"))
        cls.report = json.loads((cls.output_dir / "validation" / "quality_report.json").read_text(encoding="utf-8"))

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temp_dir.cleanup()

    def query(self, text: str) -> dict:
        proc = subprocess.run(
            [sys.executable, str(SCRIPT), "query", *text.split(), "--output-dir", str(self.output_dir)],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
        return json.loads(proc.stdout)

    def test_config_is_source_scoped_and_semantically_ordered(self):
        config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        self.assertEqual(config["source_id"], "rose-mother-bar")
        specs = config["document_spec"]["chunk_specs"]
        self.assertEqual(
            [(spec["key"], spec["pages"]) for spec in specs],
            [
                ("definition-zones", [1, 2]),
                ("contexts-confirmation-risk", [3, 3]),
                ("example", [4, 5]),
                ("conclusion", [6, 6]),
            ],
        )
        self.assertEqual(len(config["records"]), 1)
        self.assertEqual(config["records"][0]["source_id"], "rose-mother-bar")

    def test_isolated_build_has_six_page_and_visual_coverage(self):
        self.assertEqual(self.build_result["exit_code"], 0)
        self.assertEqual(self.build_result["quality_status"], "pass")
        self.assertEqual(self.manifest["documents"][0]["page_count"], 6)
        self.assertEqual(self.manifest["documents"][0]["status"], "success")
        self.assertEqual(self.manifest["documents"][0]["image_pages"], [1, 2, 4, 5])
        self.assertEqual(self.visual_index["documents"][0]["image_pages"], [1, 2, 4, 5])
        self.assertEqual(self.visual_index["documents"][0]["chunk_visual_refs"], [1, 2, 4, 5])

        retrieval = [chunk for chunk in self.chunks if chunk["is_retrieval_chunk"]]
        self.assertEqual(len(retrieval), 4)
        self.assertEqual({page for chunk in retrieval for page in range(chunk["page_start"], chunk["page_end"] + 1)}, set(range(1, 7)))
        self.assertEqual({chunk["chunk_key"] for chunk in retrieval}, {"definition-zones", "contexts-confirmation-risk", "example", "conclusion"})
        self.assertEqual(
            {chunk["chunk_key"]: chunk["related_image_pages"] for chunk in retrieval},
            {"definition-zones": [1, 2], "contexts-confirmation-risk": [], "example": [4, 5], "conclusion": []},
        )

        coverage = next(check for check in self.report["checks"] if check["name"] == "successful document page coverage")
        self.assertEqual(coverage["status"], "pass")
        self.assertEqual(coverage["details"]["rose-mother-bar"]["missing_pages"], [])

    def test_retrieval_chunks_are_unique_and_excerpt_is_not_truncated(self):
        retrieval = [chunk for chunk in self.chunks if chunk["is_retrieval_chunk"]]
        hashes = [chunk["source_text_sha256"] for chunk in retrieval]
        self.assertEqual(len(hashes), len(set(hashes)), "same-page duplicate retrieval chunk survived")
        for index, left in enumerate(retrieval):
            for right in retrieval[index + 1 :]:
                self.assertNotIn(left["source_text"], right["source_text"])
                self.assertNotIn(right["source_text"], left["source_text"])
            self.assertLess(len(left["source_text"]), 5000)
        self.assertLess(len(self.records[0]["source_excerpt"]), 5000)

    def test_claims_keep_directional_and_derived_provenance(self):
        record = self.records[0]
        soft_stops = record["invalidation_stop"]
        soft_stop = next(claim for claim in soft_stops if "连续出现两根强劲" in claim["statement"])
        self.assertEqual(soft_stop["declaration_type"], "source_assertion")
        self.assertIn("卖出区做空", soft_stop["statement"])
        self.assertNotIn("买入区做多", soft_stop["statement"])

        no_symmetric = next(claim for claim in soft_stops if "完全对称" in claim["statement"])
        self.assertEqual(no_symmetric["declaration_type"], "source_not_stated")
        self.assertTrue(no_symmetric["rationale"])

        no_waiting_rule = next(claim for claim in record["entry"] if "等待结构证据" in claim["statement"])
        self.assertEqual(no_waiting_rule["declaration_type"], "source_not_stated")
        self.assertTrue(no_waiting_rule["source_refs"])

        derived_management = next(claim for claim in record["targets_exit"] if "顺势管理" in claim["statement"])
        self.assertEqual(derived_management["declaration_type"], "derived_interpretation")
        self.assertTrue(derived_management["rationale"])

        conclusion_claim = next(
            claim
            for claim in record["interpretation_notes"]
            if "微观结构" in claim["statement"] and claim["declaration_type"] == "source_assertion"
        )
        self.assertEqual(conclusion_claim["declaration_type"], "source_assertion")
        self.assertEqual(
            [(ref["chunk_id"], ref["page_start"], ref["page_end"]) for ref in conclusion_claim["source_refs"]],
            [("rose-mother-bar:conclusion", 6, 6)],
        )

        for field in ("market_context", "prerequisites", "recognition_features", "trigger", "entry", "invalidation_stop", "targets_exit", "risk_notes", "prohibited", "exceptions", "examples", "interpretation_notes"):
            for claim in record[field]:
                self.assertIn(claim["declaration_type"], {"source_assertion", "derived_interpretation", "source_not_stated"})
                self.assertTrue(claim["source_refs"], f"{field} claim lacks source refs")

    def test_golden_queries_return_claims_and_exact_citations(self):
        golden = json.loads(GOLDEN_PATH.read_text(encoding="utf-8"))
        record = self.records[0]
        for case in golden:
            result = self.query(case["query"])
            ids = [item["record_id"] for item in result["results"]]
            for expected_id in case["expected_record_ids"]:
                self.assertIn(expected_id, ids, case["query"])
            item = next(item for item in result["results"] if item["record_id"] == "rose-mother-bar")
            for expected in case["expected_claims"]:
                claims = item["logic"][expected["field"]]
                match = next((claim for claim in claims if claim["statement"] == expected["statement"]), None)
                self.assertIsNotNone(match, f"missing claim for {case['query']}")
                self.assertEqual(match["declaration_type"], expected["declaration_type"])
            for expected in case["expected_citations"]:
                self.assertTrue(
                    any(
                        citation.get("chunk_id") == expected["chunk_id"]
                        and citation.get("page_start") == expected["page_start"]
                        and citation.get("page_end") == expected["page_end"]
                        for citation in item["citations"]
                    ),
                    f"missing citation for {case['query']}",
                )

        # The merged p3 chunk must serve both query families without creating
        # a second exact-copy retrieval chunk.
        for query in ("趋势 第一 二条腿", "突破 1.1 陷阱区"):
            result = self.query(query)
            item = next(item for item in result["results"] if item["record_id"] == "rose-mother-bar")
            self.assertTrue(any(ref["chunk_id"] == "rose-mother-bar:contexts-confirmation-risk" and ref["page_start"] == 3 for ref in item["citations"]))


if __name__ == "__main__":
    unittest.main()
