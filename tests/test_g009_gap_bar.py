"""G009 source-scoped acceptance tests for the Rose Gap Bar PDF."""

from __future__ import annotations

import json
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "kb.py"
SOURCE_DIR = ROOT / "策略"
FIXTURE = ROOT / "tests" / "fixtures" / "g009_gap_bar_golden.json"
sys.path.insert(0, str(ROOT / "scripts"))
import kb  # noqa: E402


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


class GapBarSourceAcceptanceTests(unittest.TestCase):
    """Build one isolated Gap Bar snapshot and check source-specific behavior."""

    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.TemporaryDirectory(prefix="g009-gap-bar-")
        cls.output = Path(cls.temp_dir.name) / "knowledge"
        proc = subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "build",
                "--source-dir",
                str(SOURCE_DIR),
                "--output-dir",
                str(cls.output),
                "--only",
                "rose-gap-bar",
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
        )
        if proc.returncode != kb.EXIT_SUCCESS:
            raise AssertionError(f"isolated Gap Bar build failed: {proc.stderr or proc.stdout}")
        cls.manifest = read_json(cls.output / "manifest.json")
        cls.chunks = read_json(cls.output / "chunks.json")["chunks"]
        cls.records = read_json(cls.output / "pa_records.json")["records"]
        cls.record = next(record for record in cls.records if record["id"] == "rose-gap-bar")
        cls.fixture = read_json(FIXTURE)

    @classmethod
    def tearDownClass(cls):
        cls.temp_dir.cleanup()

    def query(self, text: str) -> dict:
        proc = subprocess.run(
            [sys.executable, str(SCRIPT), "query", *text.split(), "--output-dir", str(self.output)],
            cwd=ROOT,
            capture_output=True,
            text=True,
        )
        self.assertEqual(proc.returncode, kb.EXIT_SUCCESS, proc.stderr)
        return json.loads(proc.stdout)

    def test_four_page_coverage_and_non_overlapping_retrieval_chunks(self):
        document = next(item for item in self.manifest["documents"] if item["source_id"] == "rose-gap-bar")
        self.assertEqual(document["page_count"], 4)
        retrieval = [chunk for chunk in self.chunks if chunk["is_retrieval_chunk"]]
        self.assertEqual(
            [(chunk["chunk_key"], chunk["page_start"], chunk["page_end"]) for chunk in retrieval],
            [("definition", 1, 2), ("execution", 3, 3), ("summary", 4, 4)],
        )
        covered = {
            page
            for chunk in retrieval
            for page in range(chunk["page_start"], chunk["page_end"] + 1)
        }
        self.assertEqual(covered, {1, 2, 3, 4})

        # The old config repeated p2 and p4 in adjacent retrieval chunks.  A
        # retrieval chunk must not be a full-text duplicate or exact substring
        # of another retrieval chunk after text normalization.
        texts = [kb.normalize_text(chunk["source_text"]) for chunk in retrieval]
        self.assertEqual(len(texts), len(set(texts)))
        for left_index, left in enumerate(texts):
            for right_index, right in enumerate(texts):
                if left_index == right_index:
                    continue
                self.assertNotIn(left, right)

    def test_visual_direction_and_record_citations_cover_execution_pages(self):
        chunks_by_key = {chunk["chunk_key"]: chunk for chunk in self.chunks}
        self.assertEqual(chunks_by_key["definition"]["related_image_pages"], [2])
        self.assertEqual(chunks_by_key["execution"]["related_image_pages"], [3])
        self.assertIn("连续看涨", chunks_by_key["execution"]["visual_notes"])
        self.assertNotIn("看跌", chunks_by_key["execution"]["visual_notes"])
        self.assertEqual(chunks_by_key["summary"]["related_image_pages"], [])

        citation_pages = {
            page
            for ref in self.record["source_refs"]
            for page in range(ref["page_start"], ref["page_end"] + 1)
        }
        self.assertTrue({2, 3, 4}.issubset(citation_pages))
        self.assertEqual(
            [ref["chunk_id"] for ref in self.record["source_refs"]],
            ["rose-gap-bar:definition", "rose-gap-bar:execution", "rose-gap-bar:summary"],
        )

    def test_golden_positive_and_negative_queries(self):
        for case in self.fixture:
            result = self.query(case["query"])
            record_ids = [item.get("record_id") for item in result["results"]]
            for expected_id in case["expected_record_ids"]:
                self.assertIn(expected_id, record_ids, case["id"])

            if case["kind"] == "positive":
                record_result = next(
                    item for item in result["results"] if item.get("record_id") == "rose-gap-bar"
                )
                for expected_claim in case["expected_claims"]:
                    claims = record_result["logic"][expected_claim["field"]]
                    self.assertIn(expected_claim["statement"], [claim["statement"] for claim in claims])
                for expected_citation in case["expected_citations"]:
                    self.assertTrue(
                        any(
                            citation.get("chunk_id") == expected_citation["chunk_id"]
                            and citation.get("page_start") == expected_citation["page_start"]
                            and citation.get("page_end") == expected_citation["page_end"]
                            for citation in record_result["citations"]
                        ),
                        case["id"],
                    )
            else:
                # The query is deliberately about unsupported fixed risk
                # rules.  It may retrieve the record, but those fields must
                # remain explicit source_not_stated claims, never assertions.
                for expected_claim in case["expected_source_not_stated"]:
                    claims = self.record[expected_claim["field"]]
                    matching = next(
                        claim for claim in claims if claim["statement"] == expected_claim["statement"]
                    )
                    self.assertEqual(matching["declaration_type"], "source_not_stated")
                    self.assertNotEqual(matching["declaration_type"], case["forbidden_declaration_type"])

    def test_source_specific_rules_and_visual_pages_are_preserved(self):
        recognition = [claim["statement"] for claim in self.record["recognition_features"]]
        trigger = [claim["statement"] for claim in self.record["trigger"]]
        definition_text = " ".join(recognition)
        self.assertIn("强趋势单 K", self.record["prerequisites"][0]["statement"])
        self.assertIn("连续同向趋势 K", self.record["prerequisites"][0]["statement"])
        self.assertIn("隐含供需区", definition_text)
        self.assertTrue(any("0.66" in statement and "50%" in statement for statement in trigger))
        self.assertTrue(any("33%" in statement and "50%" in statement for statement in trigger))

        visual_index = read_json(self.output / "visual_index.json")
        visual_document = next(item for item in visual_index["documents"] if item["source_id"] == "rose-gap-bar")
        self.assertEqual(visual_document["page_count"], 4)
        self.assertEqual(visual_document["image_pages"], [2, 3])
        self.assertEqual(visual_document["chunk_visual_refs"], [2, 3])


if __name__ == "__main__":
    unittest.main()
