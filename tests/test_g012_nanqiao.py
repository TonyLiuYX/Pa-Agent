"""G012 source-scoped acceptance tests for the 17-page Nanqiao checklist.

The tests deliberately build an isolated snapshot so they exercise the real
PDF extraction, semantic configuration, provenance normalization, and query
path without touching ``knowledge/data``.
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "kb.py"
CONFIG_PATH = ROOT / "knowledge" / "config" / "sources" / "nanqiao-checklist.json"
GOLDEN_PATH = ROOT / "tests" / "fixtures" / "g012_nanqiao_golden.json"
SOURCE_DIR = ROOT / "策略"
sys.path.insert(0, str(ROOT / "scripts"))
import kb  # noqa: E402


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


class G012NanqiaoSourceQATests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.holder = tempfile.TemporaryDirectory(prefix="g012-nanqiao-")
        cls.output = Path(cls.holder.name) / "kb"
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
                "nanqiao-checklist",
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            raise RuntimeError(f"isolated Nanqiao build failed ({proc.returncode}): {proc.stdout}\n{proc.stderr}")
        cls.build_result = json.loads(proc.stdout)
        cls.manifest = read_json(cls.output / "manifest.json")
        cls.chunks = read_json(cls.output / "chunks.json")["chunks"]
        cls.records = read_json(cls.output / "pa_records.json")["records"]
        cls.config = read_json(CONFIG_PATH)

    @classmethod
    def tearDownClass(cls):
        cls.holder.cleanup()

    def chunk(self, key: str) -> dict:
        return next(item for item in self.chunks if item.get("chunk_key") == key)

    def record(self, record_id: str) -> dict:
        return next(item for item in self.records if item.get("id") == record_id)

    def query(self, text: str) -> dict:
        proc = subprocess.run(
            [sys.executable, str(SCRIPT), "query", *text.split(), "--output-dir", str(self.output), "--limit", "5"],
            cwd=ROOT,
            capture_output=True,
            text=True,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        return json.loads(proc.stdout)

    def test_isolated_cli_build_and_page_record_coverage(self):
        self.assertEqual(self.build_result["quality_status"], "pass")
        self.assertEqual(self.build_result["exit_code"], 0)
        self.assertEqual(self.manifest["documents"][0]["page_count"], 17)
        self.assertEqual(self.manifest["documents"][0]["status"], "success")
        self.assertEqual(len(self.records), 12)
        self.assertEqual(sum(record["type"] == "setup" for record in self.records), 11)
        self.assertEqual(sum(record["type"] == "discipline" for record in self.records), 1)
        coverage = self.config["document_spec"]["page_coverage"]
        self.assertEqual(coverage["processed_pages"], list(range(1, 18)))
        self.assertEqual(set(coverage["visual_pages"]), set(self.manifest["documents"][0]["image_pages"]))
        self.assertEqual(coverage["text_only_pages"], [1, 9, 15, 17])
        block_pages = {
            page
            for page in range(1, 18)
            for block in self.config["document_spec"]["visual_blocks"]
            if page in block["pages"]
        }
        self.assertEqual(block_pages - set(coverage["text_only_pages"]), set(coverage["visual_pages"]))

    def test_retrieval_page_blocks_are_unique_and_visual_claims_are_disambiguated(self):
        blocks = {item["id"]: item for item in self.config["document_spec"]["visual_blocks"]}
        expected = {
            "p3-h2-l2-chart": "breakout-h2-l2",
            "p3-breakout-failure-table": "breakout-failure-breakout",
            "p4-failure-failure-top": "breakout-failure-breakout",
            "p4-key-level-heading": "key-level-reversal",
            "p12-trapped-continuation": "trapped-pullback",
            "p12-opposite1-table": "always-in-opposite-1",
            "p13-opposite1-chart": "always-in-opposite-1",
            "p13-third-bar-chart": "always-in-third-bar",
            "p14-ema20-chart": "always-in-ema20",
            "p14-breakout-heading": "always-in-breakout-scalp",
        }
        for block_id, setup_key in expected.items():
            self.assertEqual(blocks[block_id]["setup_key"], setup_key)

        retrieval = [chunk for chunk in self.chunks if chunk["is_retrieval_chunk"]]
        self.assertEqual(len(retrieval), 17)
        self.assertEqual([chunk["chunk_key"] for chunk in retrieval], [f"page-{page:02d}-semantic" for page in range(1, 18)])
        self.assertEqual(sorted((chunk["page_start"], chunk["page_end"]) for chunk in retrieval), [(page, page) for page in range(1, 18)])
        self.assertEqual(len({chunk["source_text_sha256"] for chunk in retrieval}), len(retrieval))
        for left_index, left in enumerate(retrieval):
            self.assertEqual(left["page_start"], left["page_end"])
            for right in retrieval[left_index + 1 :]:
                self.assertNotIn(left["source_text"], right["source_text"])
                self.assertNotIn(right["source_text"], left["source_text"])

        # A same-page chart/table pair deliberately shares one neutral page
        # chunk; exact setup ownership remains at the claim visual metadata.
        self.assertEqual(self.chunk("page-03-semantic")["related_image_pages"], [3])
        self.assertEqual(self.chunk("page-04-semantic")["related_image_pages"], [4])
        self.assertEqual(self.chunk("page-12-semantic")["related_image_pages"], [12])
        self.assertEqual(self.chunk("page-13-semantic")["related_image_pages"], [13])
        self.assertEqual(self.chunk("page-14-semantic")["related_image_pages"], [14])
        self.assertEqual(self.chunk("page-16-semantic")["related_image_pages"], [16])
        self.assertIn("p3-h2-l2-chart", self.chunk("page-03-semantic")["visual_notes"])
        self.assertIn("p3-breakout-failure-table", self.chunk("page-03-semantic")["visual_notes"])
        self.assertIn("p4-failure-failure-top", self.chunk("page-04-semantic")["visual_notes"])
        self.assertIn("p4-key-level-heading", self.chunk("page-04-semantic")["visual_notes"])
        self.assertIn("p12-trapped-continuation", self.chunk("page-12-semantic")["visual_notes"])
        self.assertIn("p12-opposite1-table", self.chunk("page-12-semantic")["visual_notes"])
        self.assertIn("p13-opposite1-chart", self.chunk("page-13-semantic")["visual_notes"])
        self.assertIn("p13-third-bar-chart", self.chunk("page-13-semantic")["visual_notes"])
        self.assertIn("p14-ema20-chart", self.chunk("page-14-semantic")["visual_notes"])
        self.assertIn("p14-breakout-heading", self.chunk("page-14-semantic")["visual_notes"])
        self.assertIn("p14→p15→p16", self.chunk("page-16-semantic")["visual_notes"])

    def test_critical_visual_claims_have_exact_page_citations_and_status(self):
        expected = {
            ("nanqiao-h2-l2", "invalidation_stop", "图示止损:信号 K 高点"): (3, "p3-h2-l2-chart", "p3 upper chart green annotation"),
            ("nanqiao-h2-l2", "targets_exit", "图示止盈:固定盈亏比 1:1、1:2"): (3, "p3-h2-l2-chart", "p3 upper chart green annotation"),
            ("nanqiao-h2-l2", "targets_exit", "图示止盈:拿到前低"): (3, "p3-h2-l2-chart", "p3 upper chart green annotation"),
            ("nanqiao-breakout-failure-breakout", "invalidation_stop", "图示止损:K14 高点"): (4, "p4-failure-failure-top", "p4 upper 失败的失败 annotation"),
            ("nanqiao-point3-limit", "targets_exit", "止盈:根据当日平均波动幅度,决定止盈多少点"): (8, "p8-point3-chart", "p8 green Point 3 annotation"),
            ("nanqiao-key-level-reversal", "invalidation_stop", "图示头肩结构止损:右肩极值外 1 tick"): (5, "p5-key-level-charts", "p5 lower green head-and-shoulders box"),
            ("nanqiao-trapped-pullback", "entry", "图示回踩突破点挂顺势限价单(p12 上部 Trapped 延续图)"): (12, "p12-trapped-continuation", "p12 upper chart green callouts"),
            ("nanqiao-always-in-opposite-1", "entry", "图示强势 scalp:在 K18 高点挂限价单做空"): (13, "p13-opposite1-chart", "p13 upper green 强势 scalp box"),
            ("nanqiao-always-in-ema20", "targets_exit", "图示止盈:根据 ATR 数值,固定止盈 5 个点"): (14, "p14-ema20-chart", "p14 lower EMA20 chart green box"),
        }
        for (record_id, field, statement), (page, visual_block, visual_locator) in expected.items():
            claims = self.record(record_id)[field]
            claim = next(item for item in claims if item["statement"] == statement)
            self.assertEqual(claim["declaration_type"], "source_assertion")
            self.assertEqual(claim.get("visual_page"), page)
            self.assertEqual(claim.get("visual_block"), visual_block)
            self.assertEqual(claim.get("visual_locator"), visual_locator)
            self.assertTrue(claim["source_refs"])
            self.assertTrue(any(ref["page_start"] <= page <= ref["page_end"] for ref in claim["source_refs"]))

    def test_cross_page_setup_refs_follow_adjacent_page_chain(self):
        chunks_by_id = {chunk["chunk_id"]: chunk for chunk in self.chunks}
        expected_pages = {
            "nanqiao-h2-l2": [2, 3],
            "nanqiao-breakout-failure-breakout": [3, 4],
            "nanqiao-key-level-reversal": [4, 5, 6],
            "nanqiao-point3-limit": [7, 8],
            "nanqiao-trend-50-pullback": [9, 10],
            "nanqiao-trapped-pullback": [11, 12],
            "nanqiao-always-in-opposite-1": [12, 13],
            "nanqiao-always-in-breakout-scalp": [14, 15, 16],
        }
        for record_id, pages in expected_pages.items():
            refs = self.record(record_id)["source_refs"]
            self.assertEqual([ref["page_start"] for ref in refs], pages)
            for left, right in zip(refs, refs[1:]):
                self.assertIn(right["chunk_id"], chunks_by_id[left["chunk_id"]]["adjacent_chunk_ids"])
            self.assertTrue(all(chunks_by_id[ref["chunk_id"]]["parent_chunk_id"].endswith(":root") for ref in refs))

    def test_discipline_metadata_is_source_supported_derived_claim(self):
        claim = self.record("nanqiao-discipline-checklist")["interpretation_notes"][0]
        self.assertEqual(claim["declaration_type"], "derived_interpretation")
        self.assertIn("纪律检查表", claim["statement"])
        self.assertIn("按系统交易", claim["statement"])
        self.assertIn("后续 agent 自动调用", claim["rationale"])
        self.assertEqual(claim["source_refs"][0]["page_start"], 17)
        self.assertEqual(claim["source_refs"][0]["page_end"], 17)

    def test_actionable_claims_are_structured_and_unstated_targets_are_explicit(self):
        for record in self.records:
            for field in ("entry", "invalidation_stop", "targets_exit", "prohibited", "exceptions"):
                self.assertTrue(record[field], f"{record['id']} missing {field}")
                for claim in record[field]:
                    self.assertIn(claim["declaration_type"], kb.DECLARATION_TYPES)
                    self.assertTrue(claim["source_refs"])
                    if claim["declaration_type"] in {"derived_interpretation", "source_not_stated"}:
                        self.assertTrue(claim.get("rationale"))
        unstated = [
            claim
            for record in self.records
            for claim in record["targets_exit"]
            if claim["declaration_type"] == "source_not_stated"
        ]
        self.assertTrue(unstated)
        self.assertTrue(any("未给" in claim["statement"] for claim in unstated))

    def test_three_setup_classes_use_static_golden_queries(self):
        golden = read_json(GOLDEN_PATH)
        self.assertGreaterEqual(len(golden), 3)
        categories = set()
        for case in golden:
            categories.add(case["category"])
            result = self.query(case["query"])
            ids = [item.get("record_id") for item in result["results"]]
            for expected_id in case["expected_record_ids"]:
                self.assertIn(expected_id, ids, case["query"])
            for expected_claim in case["expected_claims"]:
                hit = next(item for item in result["results"] if item.get("record_id") == expected_claim["record_id"])
                statements = [
                    claim.get("statement")
                    for values in hit["logic"].values()
                    if isinstance(values, list)
                    for claim in values
                    if isinstance(claim, dict)
                ]
                self.assertIn(expected_claim["statement"], statements)
            for citation in case["expected_citations"]:
                hit = next(item for item in result["results"] if item.get("record_id") == citation["record_id"])
                self.assertTrue(
                    any(
                        ref.get("chunk_id") == citation["chunk_id"]
                        and ref.get("page_start") == citation["page_start"]
                        and ref.get("page_end") == citation["page_end"]
                        for ref in hit["citations"]
                    )
                )
        self.assertTrue({"breakout", "limit", "always_in"}.issubset(categories))


if __name__ == "__main__":
    unittest.main()
