"""G007 source-scoped acceptance checks for Rose的策略.pdf."""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "kb.py"
FIXTURE = ROOT / "tests" / "fixtures" / "g007_rose_strategy_golden.json"
SOURCE_DIR = ROOT / "策略"
sys.path.insert(0, str(ROOT / "scripts"))
import kb  # noqa: E402


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


class G007RoseStrategyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.fixture = read_json(FIXTURE)
        cls.temp_dir = tempfile.TemporaryDirectory(prefix="g007-rose-")
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
                "rose-strategy",
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
        )
        if proc.returncode != kb.EXIT_SUCCESS:
            raise AssertionError(f"G007 isolated build failed ({proc.returncode}): {proc.stdout}\n{proc.stderr}")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temp_dir.cleanup()

    def test_isolated_build_has_31_pages_and_20_visual_pages(self):
        manifest = read_json(self.output / "manifest.json")
        self.assertEqual([doc["source_id"] for doc in manifest["documents"]], ["rose-strategy"])
        doc = manifest["documents"][0]
        self.assertEqual(doc["source_file"], self.fixture["source_file"])
        self.assertEqual(doc["page_count"], self.fixture["expected_page_count"])
        self.assertEqual(doc["status"], "success")
        self.assertEqual(doc["image_pages"], self.fixture["expected_visual_pages"])

        report = read_json(self.output / "validation" / "quality_report.json")
        self.assertEqual(report["overall_status"], "pass")
        coverage = next(item for item in report["checks"] if item["name"] == "successful document page coverage")
        self.assertEqual(coverage["status"], "pass")
        self.assertEqual(coverage["details"]["rose-strategy"]["missing_pages"], [])

    def test_retrieval_chunks_are_rule_direction_case_split_and_bounded(self):
        chunks = read_json(self.output / "chunks.json")["chunks"]
        retrieval = [chunk for chunk in chunks if chunk["is_retrieval_chunk"]]
        expected = {f"rose-strategy:{key}" for key in self.fixture["expected_retrieval_chunks"]}
        self.assertEqual({chunk["chunk_id"] for chunk in retrieval}, expected)
        self.assertGreaterEqual(len(retrieval), 20)
        # The prior source snapshot had 14k–16k OCR/UI retrieval blocks.  A
        # page-bounded rule/case split must keep each retrieval item below it.
        self.assertLess(max(len(chunk["source_text"]) for chunk in retrieval), 10000)
        self.assertEqual(
            {chunk["page_start"] for chunk in retrieval},
            {4, 5, 7, 8, 10, 11, 13, 14, 16, 17, 18, 19, 21, 22, 23, 25, 26, 27, 28, 30},
        )

    def test_raw_pages_and_root_keep_complete_extraction(self):
        pages = {
            page["page_number"]: page
            for page in (
                json.loads(line)
                for line in (self.output / "extracted" / "rose-strategy" / "pages.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            )
        }
        self.assertEqual(pages[21]["ocr_text_chars"], 6286)
        self.assertEqual(pages[23]["ocr_text_chars"], 6240)
        self.assertIn("TradingView", pages[21]["content"])
        self.assertIn("UTC", pages[21]["content"])
        chunks = read_json(self.output / "chunks.json")["chunks"]
        root = next(chunk for chunk in chunks if chunk["chunk_id"] == "rose-strategy:root")
        self.assertIn(pages[21]["content"], root["source_text"])
        self.assertIn(pages[23]["content"], root["source_text"])

    def test_retrieval_text_is_noise_free_and_preserves_page_rules(self):
        chunks = read_json(self.output / "chunks.json")["chunks"]
        retrieval = [chunk for chunk in chunks if chunk["is_retrieval_chunk"]]
        self.assertLess(max(len(chunk["source_text"]) for chunk in retrieval), 2000)
        forbidden = re.compile(r"TradingView|UTC|\bVol\b|\bRTH\b|B[-_]ADJ", re.IGNORECASE)
        axis_only = re.compile(r"^\s*[0-9]{1,2},[0-9]{3}(?:\.[0-9]+)?\s*$")
        required = {
            "zones-sell": ["Sell Zone", "买入解套"],
            "zones-buy": ["Buy Zone", "卖出区域", "买入区域"],
            "three-pushes-shape": ["三推", "第一推"],
            "three-pushes-distance": ["第一推", "第二推"],
            "sentiment-definition": ["Sentiment", "情绪", "目标"],
            "sentiment-pullback": ["第二推", "回调点"],
            "mother-bar-levels": ["0.66", "1.11", "0.89", "0.79", "0.21", "50%"],
            "mother-bar-fibonacci": ["斐波那契", "0.618"],
            "fofs-construction": ["FOFS", "TTR", "中线", "1MM"],
            "fofs-midpoint-breakout": ["中线", "AB"],
            "fofs-example-ttr": ["TTR", "中线突破入场"],
            "fofs-example-abcd": ["AB/CD", "1MM"],
            "range-method": ["区间", "上/下半区", "突破"],
            "range-bull-case": ["多头", "上半区", "突破单"],
            "range-control-case": ["多空腿", "上/下半区"],
            "flag-overview": ["旗形", "大影线", "内包线", "第二次"],
            "flag-bull-case-1": ["下影线", "内包线", "多头突破单"],
            "flag-bear-case": ["上影线", "内包线", "空头卖出"],
            "flag-bull-case-2": ["下影线", "内包线", "突破单"],
            "missed-trend-doji": ["十字星", "1MM/2MM"],
        }
        by_key = {chunk["chunk_key"]: chunk for chunk in retrieval}
        self.assertEqual(set(by_key), set(required))
        for key, terms in required.items():
            chunk = by_key[key]
            text = chunk["source_text"] + "\n" + chunk.get("retrieval_visual_notes", "")
            self.assertIn(f"[Page {chunk['page_start']}]", chunk["source_text"])
            self.assertIn("[Visual notes]", chunk["source_text"])
            self.assertFalse(forbidden.search(chunk["source_text"]), key)
            self.assertFalse(any(axis_only.match(line) for line in chunk["source_text"].splitlines()), key)
            for term in terms:
                self.assertIn(term.lower(), text.lower(), f"{key}: {term}")

    def test_strict_claim_overlap_rejects_single_cjk_fallback(self):
        self.assertFalse(kb.claim_overlap("买", "买入区域"))
        self.assertFalse(kb.claim_overlap("买/斜", "买入区域和斜率"))
        self.assertFalse(kb.claim_overlap("入/买", "入场与买入区域"))
        self.assertTrue(kb.claim_overlap("买入", "买入区域"))
        self.assertTrue(kb.claim_overlap("AB=CD", "AB=CD 量测"))

    def test_duplicate_and_contained_retrieval_texts_fail_validation(self):
        source = self.output
        for mode in ("exact", "contained"):
            with self.subTest(mode=mode):
                temp = Path(tempfile.mkdtemp(prefix=f"g007-{mode}-"))
                try:
                    isolated = temp / "knowledge"
                    shutil.copytree(source, isolated)
                    payload = read_json(isolated / "chunks.json")
                    retrieval = [chunk for chunk in payload["chunks"] if chunk["is_retrieval_chunk"]]
                    retrieval.sort(key=lambda chunk: len(kb.normalized_retrieval_text(chunk["source_text"])), reverse=True)
                    if mode == "exact":
                        retrieval[1]["source_text"] = retrieval[0]["source_text"]
                    else:
                        retrieval[1]["source_text"] = retrieval[0]["source_text"] + "\nextra rule context"
                    (isolated / "chunks.json").write_text(
                        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
                    )
                    report = kb.validate_output(isolated)
                    check = next(
                        check for check in report["checks"]
                        if check["name"] == "semantic chunks preserve text and non-mechanical boundaries"
                    )
                    self.assertEqual(check["status"], "fail")
                    self.assertTrue(
                        check["details"]["exact_retrieval_pairs"]
                        or check["details"]["contained_retrieval_pairs"]
                    )
                finally:
                    shutil.rmtree(temp)

    def test_invalid_visual_page_fails_provenance(self):
        temp = Path(tempfile.mkdtemp(prefix="g007-visual-page-"))
        try:
            isolated = temp / "knowledge"
            shutil.copytree(self.output, isolated)
            payload = read_json(isolated / "pa_records.json")
            claim = next(
                claim
                for record in payload["records"]
                for claim in record.get("trigger", [])
                if claim.get("claim_id") == "rose-fofs-swing:trigger:1"
            )
            claim["visual_page"] = 999
            (isolated / "pa_records.json").write_text(
                json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
            )
            report = kb.validate_output(isolated)
            check = next(
                check for check in report["checks"]
                if check["name"] == "claim declaration types and claim-level provenance"
            )
            self.assertEqual(check["status"], "fail")
            self.assertTrue(any("visual_page" in error for error in check["details"]["errors"]))
        finally:
            shutil.rmtree(temp)

    def test_visual_index_and_records_cover_all_eight_themes(self):
        visual_index = read_json(self.output / "visual_index.json")
        doc = visual_index["documents"][0]
        self.assertEqual(doc["source_id"], "rose-strategy")
        self.assertEqual(doc["chunk_visual_refs"], self.fixture["expected_visual_pages"])

        records = read_json(self.output / "pa_records.json")["records"]
        self.assertEqual(
            {record["id"] for record in records},
            {
                "rose-zone-buy-sell",
                "rose-three-pushes",
                "rose-sentiment-line",
                "rose-mother-bar-scalp",
                "rose-fofs-swing",
                "rose-range-breakout",
                "rose-flag-pullback",
                "rose-missed-trend",
            },
        )
        for record in records:
            self.assertTrue(record["source_refs"], record["id"])
            self.assertTrue(any(ref["page_start"] in self.fixture["expected_visual_pages"] for ref in record["source_refs"]))
            for field in kb.CLAIM_FIELDS:
                for claim in record[field]:
                    self.assertIn(claim["declaration_type"], kb.DECLARATION_TYPES)
                    self.assertTrue(claim["source_refs"], f"{record['id']}.{field}")
                    if claim["declaration_type"] in {"derived_interpretation", "source_not_stated"}:
                        self.assertTrue(claim.get("rationale"))

    def test_eight_static_golden_queries_return_claims_and_exact_pages(self):
        records_by_id = {
            record["id"]: record
            for record in read_json(self.output / "pa_records.json")["records"]
        }
        for case in self.fixture["cases"]:
            with self.subTest(theme=case["theme"]):
                result = kb.search_knowledge(self.output, case["query"], limit=5)
                result_by_id = {item.get("record_id"): item for item in result["results"]}
                for record_id in case["expected_record_ids"]:
                    self.assertIn(record_id, result_by_id, result)
                for expected in case["expected_claims"]:
                    item = result_by_id[expected["record_id"]]
                    # The stable CLI query contract exposes the core trading
                    # logic fields; prerequisite/prohibited/exception claims
                    # remain in the generated normalized record.  Check the
                    # query payload when present and fall back to that static
                    # generated record for the other normalized fields.
                    claims = item["logic"].get(expected["field"])
                    if claims is None:
                        claims = records_by_id[expected["record_id"]].get(expected["field"], [])
                    matched = [claim for claim in claims if claim.get("statement") == expected["statement"]]
                    self.assertTrue(matched, expected)
                    self.assertEqual(matched[0].get("declaration_type"), expected["declaration_type"])
                for expected in case["expected_citations"]:
                    item = result_by_id[expected["record_id"]]
                    self.assertTrue(
                        any(
                            citation.get("chunk_id") == expected["chunk_id"]
                            and citation.get("page_start") == expected["page_start"]
                            and citation.get("page_end") == expected["page_end"]
                            for citation in item["citations"]
                        ),
                        expected,
                    )

    def test_negative_queries_do_not_invent_unstated_parameters(self):
        for case in self.fixture["negative_queries"]:
            with self.subTest(query=case["query"]):
                result = kb.search_knowledge(self.output, case["query"], limit=5)
                statements = [
                    claim.get("statement", "")
                    for item in result["results"]
                    for claims in item.get("logic", {}).values()
                    if isinstance(claims, list)
                    for claim in claims
                    if isinstance(claim, dict)
                ]
                for forbidden in case["forbidden_statements"]:
                    self.assertFalse(any(forbidden in statement for statement in statements), forbidden)


if __name__ == "__main__":
    unittest.main()
