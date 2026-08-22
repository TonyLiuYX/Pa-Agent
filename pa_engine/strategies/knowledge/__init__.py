"""Public exact strategy knowledge packet API for Phase 2."""

from .errors import PacketLoadOutcome, ProfileBindingResult, StrategyKnowledgeError
from .packet import CLAIM_FIELDS, packet_content_hash, validate_packet
from .profile import bind_strategy_profile_ref, validate_profile_lineage
from .registry import DEFAULT_REGISTRY_PATH, StrategyRegistry, build_registry, load_default_registry
from .snapshot import SnapshotIdentity, compute_snapshot_identity

__all__ = [
    "CLAIM_FIELDS",
    "DEFAULT_REGISTRY_PATH",
    "PacketLoadOutcome",
    "ProfileBindingResult",
    "SnapshotIdentity",
    "StrategyKnowledgeError",
    "StrategyRegistry",
    "bind_strategy_profile_ref",
    "build_registry",
    "compute_snapshot_identity",
    "load_default_registry",
    "packet_content_hash",
    "validate_packet",
    "validate_profile_lineage",
]
