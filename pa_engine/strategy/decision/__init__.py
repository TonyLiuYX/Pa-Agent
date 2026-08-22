"""G018 grounded strategy-decision profile.

Public flow::

    candidate = parse_candidate(raw_json)
    outcome = finalize_decision(candidate, registry=..., packet=..., ...)

``candidate`` is a compact strict JSON-native object.  ``outcome`` contains a
frozen G014 ``pa_analysis_result`` and one deterministic scheduler instruction
only when all authoritative packet, profile, evidence and market checks pass.
"""

from .errors import DecisionFinalization, DecisionIssue, DecisionValidationError, DecisionValidationResult
from .grounding import grounding_validate, validate_grounding
from .instruction import (
    INSTRUCTION_TYPE,
    INSTRUCTION_VERSION,
    build_scheduler_instruction,
    instruction_hash,
    make_instruction,
    make_scheduler_instruction,
    validate_instruction,
    verify_instruction,
)
from .mapper import (
    ENGINE_VERSION,
    finalize_artifacts,
    finalize_candidate,
    finalize_decision,
    ground_and_map,
    map_candidate,
    map_candidate_to_normalized_model_candidate,
    map_candidate_to_g014,
    map_g018_candidate_to_g014,
    map_to_normalized_model_candidate,
    map_to_g014_result,
)
from .refs import (
    make_second_event_ref,
    make_shared_market_view_ref,
    market_view_ref_digest,
    market_view_ref_for,
    shared_market_view_ref_for,
    validate_market_view_ref,
    validate_second_event_ref,
)
from .schema import (
    BRANCHES,
    CANDIDATE_FIELDS,
    DECISION_SCHEMA_VERSION,
    DECLARATIONS,
    DIRECTIONS,
    GROUNDING_FIELDS,
    GROUNDING_REF_FIELDS,
    MAX_DELAY_SECONDS,
    MIN_DELAY_SECONDS,
    REASON_MAX_CODEPOINTS,
    SCHEMA_PATH,
    SCHEMA_VERSION,
    UNCERTAINTY_CODES,
    canonical_positive_decimal,
    make_model_output_ref,
    model_output_digest,
    parse_candidate,
    parse_json_native,
    parse_model_output,
    parse_untrusted_candidate,
    second_event_digest,
    validate_candidate,
    validate_model_output,
    validate_model_output_ref,
    validate_untrusted_candidate,
)


__all__ = [
    "BRANCHES",
    "CANDIDATE_FIELDS",
    "DECISION_SCHEMA_VERSION",
    "DECLARATIONS",
    "DIRECTIONS",
    "DecisionFinalization",
    "DecisionIssue",
    "DecisionValidationError",
    "DecisionValidationResult",
    "ENGINE_VERSION",
    "GROUNDING_FIELDS",
    "GROUNDING_REF_FIELDS",
    "INSTRUCTION_TYPE",
    "INSTRUCTION_VERSION",
    "MAX_DELAY_SECONDS",
    "MIN_DELAY_SECONDS",
    "REASON_MAX_CODEPOINTS",
    "SCHEMA_VERSION",
    "SCHEMA_PATH",
    "UNCERTAINTY_CODES",
    "build_scheduler_instruction",
    "canonical_positive_decimal",
    "finalize_candidate",
    "finalize_decision",
    "finalize_artifacts",
    "ground_and_map",
    "grounding_validate",
    "instruction_hash",
    "make_instruction",
    "make_model_output_ref",
    "make_second_event_ref",
    "make_scheduler_instruction",
    "make_shared_market_view_ref",
    "map_candidate",
    "map_candidate_to_normalized_model_candidate",
    "map_candidate_to_g014",
    "map_g018_candidate_to_g014",
    "map_to_normalized_model_candidate",
    "map_to_g014_result",
    "market_view_ref_digest",
    "market_view_ref_for",
    "parse_candidate",
    "parse_json_native",
    "parse_model_output",
    "parse_untrusted_candidate",
    "model_output_digest",
    "second_event_digest",
    "shared_market_view_ref_for",
    "validate_candidate",
    "validate_grounding",
    "validate_instruction",
    "validate_market_view_ref",
    "validate_model_output",
    "validate_model_output_ref",
    "validate_untrusted_candidate",
    "validate_second_event_ref",
    "verify_instruction",
]
