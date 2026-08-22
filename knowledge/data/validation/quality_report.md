# PA knowledge base quality report

- KB version: `0.1.0`
- Generated: `2026-08-16T15:45:50+00:00`
- Overall: **pass**

## Summary

- documents: 6
- successful_or_partial_documents: 6
- partial_documents: 0
- failed_documents: 0
- chunks: 82
- retrieval_chunks: 65
- normalized_records: 24
- ocr_pages: 26

## Checks

- `pass` versioned JSON schemas validate generated artifacts: `{"schemas": {"manifest.json": "manifest-0.1.0.schema.json", "chunks.json": "chunks-0.1.0.schema.json", "pa_records.json": "pa_records-0.1.0.schema.json", "terminology.json": "terminology-0.1.0.schema.json", "processing_issues.json": "processing_issues-0.1.0.schema.json", "visual_index.json": "visual_index-0.1.0.schema.json"}, "errors": {}}`
- `pass` all source PDFs attempted: `{"attempted": 6, "statuses": {"success": 6}}`
- `pass` manifest and extracted directories agree: `{"manifest_source_ids": ["nanqiao-checklist", "rose-caveman", "rose-gap-bar", "rose-mother-bar", "rose-mtr", "rose-strategy"], "extracted_source_ids": ["nanqiao-checklist", "rose-caveman", "rose-gap-bar", "rose-mother-bar", "rose-mtr", "rose-strategy"], "missing_extracted_directories": [], "unexpected_extracted_directories": []}`
- `pass` manifest lifecycle changes and SHA-256 statuses: `{"changes": 6, "summary": {"added": 0, "changed": 0, "deleted": 0, "renamed": 0, "unchanged": 6}, "errors": []}`
- `pass` all sources have semantic configuration and normalization: `{"incomplete_sources": []}`
- `pass` changed-source records require re-verification: `{"changed_source_ids": [], "pending_source_ids": [], "stale_active_records": []}`
- `pass` declared schema covers generated layers: `{"schema_file": "knowledge/schema.json", "errors": []}`
- `pass` generated values satisfy schema types and ranges: `{"errors": []}`
- `pass` manifest source hashes match local files: `{"hash_mismatches": [], "missing_source_files": []}`
- `pass` source chunk schema and page ranges: `{"missing_fields": {}, "page_range_errors": []}`
- `pass` chunk parent/adjacent relations and cross-page continuity: `{"duplicate_chunk_ids": [], "duplicate_record_ids": [], "relation_errors": [], "provenance_errors": [], "continuity_errors": []}`
- `pass` semantic chunks preserve text and non-mechanical boundaries: `{"semantic_issues": [], "empty_retrieval": [], "duplicate_retrieval_text_hashes": [], "exact_retrieval_pairs": [], "contained_retrieval_pairs": [], "normalized_overlap_min_chars": 80, "short_retrieval_chunks_ignored": ["nanqiao-checklist:page-06-semantic", "nanqiao-checklist:page-10-semantic", "rose-mother-bar:conclusion", "rose-strategy:fofs-example-abcd", "rose-strategy:fofs-example-ttr", "rose-strategy:sentiment-pullback", "rose-strategy:three-pushes-shape", "rose-strategy:zones-buy", "rose-strategy:zones-sell"]}`
- `pass` successful document page coverage: `{"rose-strategy": {"page_count": 31, "pages_with_semantic_or_context_chunk": 31, "missing_pages": []}, "rose-caveman": {"page_count": 14, "pages_with_semantic_or_context_chunk": 14, "missing_pages": []}, "rose-gap-bar": {"page_count": 4, "pages_with_semantic_or_context_chunk": 4, "missing_pages": []}, "rose-mtr": {"page_count": 7, "pages_with_semantic_or_context_chunk": 7, "missing_pages": []}, "rose-mother-bar": {"page_count": 6, "pages_with_semantic_or_context_chunk": 6, "missing_pages": []}, "nanqiao-checklist": {"page_count": 17, "pages_with_semantic_or_context_chunk": 17, "missing_pages": []}}`
- `pass` normalized PA record fields and citations: `{"record_count": 24, "missing_fields": {}, "logic_fields_not_lists": {}, "missing_source_refs": [], "record_integrity_errors": []}`
- `pass` claim declaration types and claim-level provenance: `{"errors": []}`
- `pass` chart/page visual linkage: `{"records_with_related_image_pages": ["nanqiao-h2-l2", "nanqiao-breakout-failure-breakout", "nanqiao-key-level-reversal", "nanqiao-range-extreme-limit", "nanqiao-point3-limit", "nanqiao-trend-50-pullback", "nanqiao-trapped-pullback", "nanqiao-always-in-opposite-1", "nanqiao-always-in-third-bar", "nanqiao-always-in-ema20", "nanqiao-always-in-breakout-scalp", "rose-caveman", "rose-gap-bar", "rose-mother-bar", "rose-mtr", "rose-zone-buy-sell", "rose-three-pushes", "rose-sentiment-line", "rose-mother-bar-scalp", "rose-fofs-swing", "rose-range-breakout", "rose-flag-pullback", "rose-missed-trend"]}`
- `pass` visual references resolve to source image pages: `{"errors": []}`
- `pass` source-to-normalized sample cross-check: `{"checked_records": ["rose-zone-buy-sell", "rose-caveman", "rose-gap-bar", "rose-mother-bar", "rose-fofs-swing", "nanqiao-h2-l2", "nanqiao-discipline-checklist"], "missing_records": [], "skipped_for_partial_build": [], "missing_terms": {}}`
- `pass` normalized citations resolve to exact source chunks: `{"errors": [], "citation_count": 70}`
- `pass` terminology aliases and conflict register: `{"errors": [], "terms": 34, "conflicts": 8}`
- `pass` representative retrieval queries return citations: `{"executed": [{"query": "Mother Bar", "status": "pass", "count": 3, "required_source_hit": true, "record_ids": ["rose-mother-bar", "rose-mother-bar-scalp", "nanqiao-always-in-opposite-1"]}, {"query": "Caveman", "status": "pass", "count": 1, "required_source_hit": true, "record_ids": ["rose-caveman"]}, {"query": "Gap Bar", "status": "pass", "count": 3, "required_source_hit": true, "record_ids": ["rose-gap-bar", "nanqiao-always-in-opposite-1", "nanqiao-always-in-third-bar"]}, {"query": "H2 L2", "status": "pass", "count": 2, "required_source_hit": true, "record_ids": ["nanqiao-h2-l2", "nanqiao-trend-50-pullback"]}, {"query": "区间突破", "status": "pass", "count": 3, "required_source_hit": true, "record_ids": ["rose-range-breakout", "nanqiao-discipline-checklist", "nanqiao-point3-limit"]}, {"query": "MTR", "status": "pass", "count": 1, "required_source_hit": true, "record_ids": ["rose-mtr"]}, {"query": "FOFS", "status": "pass", "count": 1, "required_source_hit": true, "record_ids": ["rose-fofs-swing"]}], "skipped_for_partial_build": []}`
- `pass` quality report validates against its versioned schema: `{"errors": []}`
