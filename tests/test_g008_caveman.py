import json
import re
import subprocess
import sys
import tempfile
import unicodedata
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "kb.py"
FIXTURE = ROOT / "tests" / "fixtures" / "g008_caveman_golden.json"
SOURCE_DIR = ROOT / "策略"


class G008CavemanSourceQATests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
        cls.temp_dir = tempfile.TemporaryDirectory(prefix="g008-caveman-test-")
        cls.output_dir = Path(cls.temp_dir.name) / "kb"
        cls.build = subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "build",
                "--source-dir",
                str(SOURCE_DIR),
                "--output-dir",
                str(cls.output_dir),
                "--only",
                "rose-caveman",
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
        )
        if cls.build.returncode == 0:
            cls.build_payload = json.loads(cls.build.stdout)
            cls.manifest = json.loads((cls.output_dir / "manifest.json").read_text(encoding="utf-8"))
            cls.chunks = json.loads((cls.output_dir / "chunks.json").read_text(encoding="utf-8"))["chunks"]
            cls.records = json.loads((cls.output_dir / "pa_records.json").read_text(encoding="utf-8"))["records"]
            cls.terminology = json.loads((cls.output_dir / "terminology.json").read_text(encoding="utf-8"))
            cls.visual_index = json.loads((cls.output_dir / "visual_index.json").read_text(encoding="utf-8"))
        else:
            cls.build_payload = {}
            cls.manifest = {"documents": []}
            cls.chunks = []
            cls.records = []
            cls.terminology = {"conflicts": []}
            cls.visual_index = {"documents": []}

    @classmethod
    def tearDownClass(cls):
        cls.temp_dir.cleanup()

    def require_build(self):
        self.assertEqual(self.build.returncode, 0, self.build.stderr or self.build.stdout)

    def test_source_build_has_14_page_visual_coverage(self):
        self.require_build()
        fixture = self.fixture
        self.assertEqual(self.build_payload["quality_status"], "pass")
        self.assertEqual(self.build_payload["chunks"], 15)  # root + 14 retrieval chunks
        docs = self.manifest["documents"]
        self.assertEqual(len(docs), 1)
        doc = docs[0]
        self.assertEqual(doc["source_id"], fixture["source_id"])
        self.assertEqual(doc["page_count"], fixture["page_count"])
        self.assertEqual(doc["image_pages"], fixture["visual_pages"])
        indexed = self.visual_index["documents"][0]
        self.assertEqual(indexed["chunk_visual_refs"], fixture["visual_pages"])
        retrieval = [chunk for chunk in self.chunks if chunk["is_retrieval_chunk"]]
        self.assertEqual(len(retrieval), 14)
        self.assertEqual(
            {page for chunk in retrieval for page in chunk["related_image_pages"]},
            set(fixture["visual_pages"]),
        )

    def test_p2_visual_transcription_and_positive_wedge_direction(self):
        self.require_build()
        p2 = next(chunk for chunk in self.chunks if chunk["chunk_key"] == "p2-core-visual")
        for required in self.fixture["p2_visual_required"]:
            self.assertIn(required, p2["visual_notes"])
        self.assertIn("EMA", p2["visual_notes"])
        self.assertNotIn("Wedge Top 的位置可能失败", p2["visual_notes"])
        record = self.records[0]
        wedge_claims = [
            claim
            for claim in record["market_context"]
            if "Wedge Top" in claim["statement"]
        ]
        self.assertEqual(len(wedge_claims), 1)
        self.assertEqual(wedge_claims[0]["declaration_type"], "source_assertion")
        self.assertEqual(wedge_claims[0]["source_refs"][0]["page_start"], 2)
        self.assertEqual(wedge_claims[0].get("visual_page"), 2)
        self.assertEqual(wedge_claims[0].get("visual_locator"), "p2 图片 Wedge Top 位置标注")
        two_r_claim = next(
            claim
            for claim in record["targets_exit"]
            if "most of the time 2R targets" in claim["statement"]
        )
        self.assertEqual(two_r_claim["declaration_type"], "source_assertion")
        self.assertEqual(two_r_claim["source_refs"][0]["page_start"], 2)
        self.assertEqual(two_r_claim.get("visual_page"), 2)
        self.assertEqual(two_r_claim.get("visual_locator"), "p2 图片 most-of-the-time 2R targets 文字")
        filter_text = " ".join(
            claim["statement"]
            for field in ("recognition_features", "risk_notes", "prohibited")
            for claim in record[field]
        )
        for required in ("第二推", "均线", "TR上沿", "TR下沿", "二次入场", "顺势"):
            self.assertIn(required, filter_text)

    def test_nine_cases_are_independent_directional_trigger_chunks(self):
        self.require_build()
        fixture_cases = self.fixture["case_chunks"]
        retrieval = {chunk["chunk_key"]: chunk for chunk in self.chunks if chunk["is_retrieval_chunk"]}
        self.assertEqual(set(retrieval) & set(fixture_cases), set(fixture_cases))
        self.assertNotIn("examples", retrieval)
        self.assertNotIn("case-1-9", retrieval)
        for key, expected in fixture_cases.items():
            chunk = retrieval[key]
            self.assertEqual(chunk["page_range"], expected["page_range"])
            self.assertTrue(any(expected["direction"] in part for part in chunk["title_path"]))
            self.assertIn(expected["direction"], chunk["source_text"])
            self.assertIn(expected["trigger"], chunk["source_text"])
            markers = {
                int(page)
                for page in re.findall(r"^\[Page (\d+)\]$", chunk["source_text"], flags=re.MULTILINE)
            }
            self.assertEqual(markers, set(range(chunk["page_start"], chunk["page_end"] + 1)))
        examples = self.records[0]["examples"]
        self.assertEqual(len(examples), 9)
        self.assertEqual(
            {claim["pages"][0] for claim in examples},
            {5, 6, 7, 8, 9, 11, 12, 13},
        )
        self.assertTrue(all(claim["declaration_type"] == "source_assertion" for claim in examples))

    def test_retrieval_excerpts_have_no_exact_or_substring_duplicates(self):
        self.require_build()
        retrieval = [chunk for chunk in self.chunks if chunk["is_retrieval_chunk"]]

        def normalized(value):
            return " ".join(unicodedata.normalize("NFKC", value).split())

        texts = {chunk["chunk_key"]: normalized(chunk["source_text"]) for chunk in retrieval}
        self.assertEqual(len(texts), len(set(texts.values())))
        for key, text in texts.items():
            for other_key, other_text in texts.items():
                if key >= other_key:
                    continue
                self.assertFalse(
                    text in other_text or other_text in text,
                    f"mechanical retrieval substring overlap: {key} vs {other_key}",
                )
        invalid = next(chunk for chunk in retrieval if chunk["chunk_key"] == "invalid-signal-case")
        self.assertIn("bar 52", invalid["source_text"])
        self.assertNotIn("案例一:Caveman 1", invalid["source_text"])

    def test_same_source_conflicts_keep_both_sides_and_locators(self):
        self.require_build()
        expected = self.fixture["conflicts"]
        conflicts = {conflict["topic"]: conflict for conflict in self.terminology["conflicts"]}
        self.assertEqual(set(conflicts), set(expected))
        for topic, labels in expected.items():
            conflict = conflicts[topic]
            self.assertEqual([conflict["side_a"]["label"], conflict["side_b"]["label"]], labels)
            self.assertEqual(conflict["sources"], ["rose-caveman"])
            self.assertIn("page", conflict["side_a"]["source_locator"])
            self.assertIn("page", conflict["side_b"]["source_locator"])
        self.assertIn("p2", conflicts["信号棒与触发棒的时序"]["description"])
        self.assertIn("p3-p4", conflicts["目标风险单位"]["description"])

    def test_special_query_returns_full_filter_entry_stop_target_logic(self):
        self.require_build()
        query = self.fixture["query"]
        proc = subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "query",
                query["text"],
                "--output-dir",
                str(self.output_dir),
                "--limit",
                "3",
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        result = json.loads(proc.stdout)
        self.assertGreaterEqual(result["count"], 1)
        hit = next(item for item in result["results"] if item["record_id"] == query["record_id"])
        logic = hit["logic"]
        searchable_logic = " ".join(
            claim["statement"]
            for field in ("market_context", "recognition_features", "trigger", "entry", "risk_notes")
            for claim in logic[field]
        )
        for required in query["required_logic_terms"]:
            self.assertIn(required, searchable_logic)
        self.assertTrue(logic["entry"])
        self.assertTrue(logic["invalidation_stop"])
        self.assertTrue(logic["targets_exit"])
        cited_pages = {
            page
            for citation in hit["citations"]
            for page in range(citation["page_start"], citation["page_end"] + 1)
        }
        self.assertTrue(set(query["required_citation_pages"]).issubset(cited_pages))
        self.assertTrue(any(citation["chunk_id"] == "rose-caveman:p2-core-visual" for citation in hit["citations"]))


if __name__ == "__main__":
    unittest.main()
