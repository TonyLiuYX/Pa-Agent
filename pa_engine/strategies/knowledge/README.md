# Exact strategy knowledge packets (G016)

The registry is local-only and reads only `knowledge/data/manifest.json`,
`chunks.json` and `pa_records.json`.  Its snapshot identity is the SHA-256 of
the canonical JSON projection described by
`g016.manifest-chunks-records.v1`/`0.1.0`.  Volatile manifest lifecycle fields
are omitted, documents/chunks/records are sorted by their stable IDs, and
floats are converted to canonical decimal strings only in the hash
projection.  The current registry pins snapshot
`kbs_fb71f20c8f5c80fc8b96554a7d3b9eb1d328fd0ee571d9b7701a4a3d753f4b0c` and
contains 21 active `type=setup` records.

Each successful `load_packet("pa:<record_id>")` returns one packet, one
`strategy_profile_ref`, and one G014 `knowledge_evidence_package`.  A packet
has `record_type="setup"`, `record_status="active"`, `executable=true`, one
lossless `primary_record`, the fixed-order `CLAIM_FIELDS` arrays with
`field`/one-based `ordinal`, all referenced retrieval `chunk_excerpts`, and
deterministic claim/global visual refs.  `supplemental_context` is explicitly
`executable=false`: its `records` remain lossless concept/discipline records,
while context-level `chunk_excerpts` and `visual_refs` contain the complete
grounding union.  Supplemental records never enter the executable evidence
package.  Packet identity is `packet_id=spk_<packet_hash>`; content hashing
excludes only the packet ID/hash and applies float normalization.

The profile reference binds one strategy, record, packet and snapshot.  Its
self identity is `profile_ref_id=spr_<profile_ref_hash>` and it intentionally
does not include evidence IDs/hashes.  `bind_strategy_profile_ref` deep-copies
and rehashes a G014 artifact extension; `validate_profile_lineage` rejects
owner/snapshot/profile swaps, nested model-request context swaps, hash-valid
evidence forgeries and unused citations.  For full grounding use
`registry.validate_packet(packet)` and
`registry.validate_profile_lineage(profile_ref, packet=..., artifacts=...)`;
these methods bind against the registry's immutable snapshot and authoritative
packet/profile pair.  The standalone `validate_packet` with only a
`SnapshotIdentity` checks identity/shape pins, not source bytes.

Stable non-executable outcome codes include `strategy_not_registered`,
`record_not_found`, `record_inactive`, `record_pending`, `non_setup_record`,
`snapshot_mismatch`, `profile_binding_mismatch`, `cross_strategy_claim`,
`claim_declaration_mismatch`, and `grounding_failure`.  Version/schema and
duplicate-source failures are typed `snapshot_mismatch`/`snapshot_invalid`.
`StrategyKnowledgeError` carries `code`, `message`, and JSON-native
`details`; `PacketLoadOutcome` is deep-copy safe.  A bounded cache uses
`(snapshot_hash, strategy_id)` keys and is suitable for the expected sub-200
strategy count (including synthetic 200-entry checks).

G018 consumes `PacketLoadOutcome.packet`, `profile_ref`, and
`evidence_package`.  G018 owns market matching and grounded decision semantics;
G016 does not search the global KB, choose entries, schedule calls, or create
paper accounts.
