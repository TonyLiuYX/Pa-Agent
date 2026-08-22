"""Independent G010 checks for the replacement Rose MTR PDF."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "策略" / "价格行为学 - Rose 的 MTR (主趋势反转) - forecho's Blog 2.pdf"
EXPECTED_SHA256 = "9ade5b6c706cdeff09fba8effadffc1d0bda3c4a2004774232bae24ed094e5be"

import sys

sys.path.insert(0, str(ROOT / "scripts"))
import kb  # noqa: E402


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


class MtrSourceRecoveryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.holder = tempfile.TemporaryDirectory(prefix="g010-mtr-")
        cls.output = Path(cls.holder.name) / "kb"
        cls.result = kb.build(ROOT / "策略", cls.output, only=["rose-mtr"], mode="build")
        if cls.result["exit_code"] != kb.EXIT_SUCCESS:
            raise AssertionError(cls.result)
        cls.manifest = read_json(cls.output / "manifest.json")
        cls.chunks = read_json(cls.output / "chunks.json")["chunks"]
        cls.records = read_json(cls.output / "pa_records.json")["records"]
        cls.report = read_json(cls.output / "validation" / "quality_report.json")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.holder.cleanup()

    def test_actual_locator_maps_to_canonical_source(self):
        self.assertEqual(kb.source_id_for_path(SOURCE), ("rose-mtr", True))
        document = self.manifest["documents"][0]
        self.assertEqual(document["source_id"], "rose-mtr")
        self.assertEqual(document["source_file"], SOURCE.relative_to(ROOT).as_posix())
        self.assertEqual(document["file_name"], SOURCE.name)
        self.assertEqual(document["sha256"], EXPECTED_SHA256)
        self.assertEqual(document["page_count"], 7)
        self.assertEqual(document["status"], "success")
        self.assertEqual(document["empty_text_pages"], [])
        self.assertEqual(document["image_pages"], [2, 3, 4, 5, 6])

    def test_every_page_has_semantic_text_coverage_and_visual_refs(self):
        retrieval = [chunk for chunk in self.chunks if chunk["is_retrieval_chunk"]]
        self.assertEqual(
            {chunk["chunk_key"] for chunk in retrieval},
            {
                "intro-core",
                "trap-confirmation",
                "video-example",
                "example-one-two",
                "example-two-three",
                "risk-management",
                "summary",
            },
        )
        covered_pages = {
            page
            for chunk in retrieval
            for page in range(chunk["page_start"], chunk["page_end"] + 1)
        }
        self.assertEqual(covered_pages, set(range(1, 8)))
        self.assertTrue(all(chunk["source_text"].strip() for chunk in retrieval))

        visual_pages = {
            page
            for chunk in retrieval
            for page in chunk.get("related_image_pages", [])
        }
        self.assertEqual(visual_pages, {2, 3, 4, 5, 6})
        for page in range(2, 7):
            owners = [chunk for chunk in retrieval if page in chunk.get("related_image_pages", [])]
            self.assertTrue(owners, page)
            self.assertTrue(all(chunk["page_start"] <= page <= chunk["page_end"] for chunk in owners))

    def test_record_replaces_supplemental_placeholder_with_local_claims(self):
        config = read_json(ROOT / "knowledge" / "config" / "sources" / "rose-mtr.json")
        self.assertEqual(config["document_spec"]["file_name"], SOURCE.name)
        self.assertEqual(config["document_spec"]["verified_source_sha256"], EXPECTED_SHA256)
        self.assertNotIn("external_recovery", config["document_spec"])
        self.assertEqual([item["id"] for item in config["records"]], ["rose-mtr"])
        self.assertNotIn("mtr-recovery", [item["id"] for item in config["records"]])

        self.assertEqual(len(self.records), 1)
        record = self.records[0]
        self.assertEqual(record["id"], "rose-mtr")
        self.assertEqual(record["source_fidelity"], "source_faithful_with_normalization")
        self.assertTrue(all(ref["chunk_id"] for ref in record["source_refs"]))
        self.assertTrue(all(ref["source_id"] == "rose-mtr" for ref in record["source_refs"]))
        self.assertTrue(all("external_url" not in ref for ref in record["source_refs"]))

        claim_fields = (
            "market_context",
            "prerequisites",
            "recognition_features",
            "trigger",
            "entry",
            "invalidation_stop",
            "targets_exit",
            "risk_notes",
            "prohibited",
            "exceptions",
            "examples",
        )
        for field in claim_fields:
            claims = record[field]
            self.assertTrue(claims, field)
            for claim in claims:
                self.assertIn(claim["declaration_type"], kb.DECLARATION_TYPES, field)
                if claim["declaration_type"] in {"derived_interpretation", "source_not_stated"}:
                    self.assertTrue(claim.get("rationale"), field)
                self.assertTrue(claim["source_refs"], field)
                for ref in claim["source_refs"]:
                    self.assertIsNotNone(ref["chunk_id"])
                    self.assertEqual(ref["source_id"], "rose-mtr")
                    self.assertEqual(ref["source_sha256"], EXPECTED_SHA256)

        # Entry/stop/target rules are executable source rules and must remain
        # exact local assertions.  Other normalized fields may be an explicit
        # derived interpretation when they combine separate source sentences.
        for field in ("entry", "invalidation_stop", "targets_exit"):
            self.assertTrue(all(claim["declaration_type"] == "source_assertion" for claim in record[field]), field)
        prerequisite = next(
            claim for claim in record["prerequisites"] if claim["statement"].startswith("识别信号K线")
        )
        self.assertEqual(prerequisite["declaration_type"], "derived_interpretation")
        self.assertTrue(prerequisite["rationale"])

        self.assertEqual(record["interpretation_notes"][0]["declaration_type"], "derived_interpretation")
        self.assertFalse(any(item.get("source_fidelity", "").startswith("supplemental") for item in self.records))

    def test_source_scoped_validate_and_golden_queries_pass(self):
        self.assertEqual(self.report["overall_status"], "pass")
        coverage = next(item for item in self.report["checks"] if item["name"] == "successful document page coverage")
        self.assertEqual(coverage["status"], "pass")
        self.assertEqual(coverage["details"]["rose-mtr"]["missing_pages"], [])
        visual = next(item for item in self.report["checks"] if item["name"] == "visual references resolve to source image pages")
        self.assertEqual(visual["status"], "pass")
        provenance = next(item for item in self.report["checks"] if item["name"] == "claim declaration types and claim-level provenance")
        self.assertEqual(provenance["status"], "pass")

        golden = read_json(ROOT / "tests" / "fixtures" / "g010_mtr_golden.json")
        for case in golden:
            result = kb.search_knowledge(self.output, case["query"], limit=5)
            ids = [item.get("record_id") for item in result["results"]]
            for expected_id in case["expected_record_ids"]:
                self.assertIn(expected_id, ids, case["query"])
            for expected_claim in case["expected_claims"]:
                item = next(
                    item for item in result["results"] if item.get("record_id") == expected_claim["record_id"]
                )
                claims = item["logic"].get(expected_claim["field"], [])
                statements = [claim.get("statement") for claim in claims]
                self.assertIn(expected_claim["statement"], statements, expected_claim)
                expected_chunk_id = expected_claim.get("expected_chunk_id")
                if expected_chunk_id:
                    matching_claim = next(
                        claim for claim in claims if claim.get("statement") == expected_claim["statement"]
                    )
                    self.assertTrue(
                        any(
                            ref.get("chunk_id") == expected_chunk_id
                            and ref.get("page_start") == expected_claim["expected_page_start"]
                            and ref.get("page_end") == expected_claim["expected_page_end"]
                            for ref in matching_claim.get("source_refs", [])
                        ),
                        expected_claim,
                    )
            for expected_citation in case["expected_citations"]:
                item = next(
                    item for item in result["results"] if item.get("record_id") == expected_citation["record_id"]
                )
                self.assertTrue(
                    any(
                        citation.get("chunk_id") == expected_citation["chunk_id"]
                        and citation.get("page_start") == expected_citation["page_start"]
                        and citation.get("page_end") == expected_citation["page_end"]
                        for citation in item["citations"]
                    ),
                    expected_citation,
                )

        result = kb.search_knowledge(self.output, "MTR", limit=5)
        self.assertTrue(result["results"])
        self.assertEqual(result["results"][0]["record_id"], "rose-mtr")
        cited_visual_pages = {
            page
            for citation in result["results"][0]["citations"]
            for page in citation.get("related_image_pages", [])
        }
        self.assertTrue({2, 3, 4, 5, 6}.issubset(cited_visual_pages))


if __name__ == "__main__":
    unittest.main()
