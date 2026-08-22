# G018 grounded strategy decision profile

G018 owns the untrusted model-output boundary between G017 and later strategy
scheduling.  The profile is intentionally separate from the frozen G014
`candidate` object: it carries exactly one `branch` (`no_entry`,
`enter_immediately`, or `wait_for_price`), canonical Decimal prices, mutually
exclusive `N`/`X`, claim/evidence/citation/market grounding references,
confidence/uncertainty, one G021-style `shared_market_view_ref`, and (only for
`enter_immediately`) a compact internal `second_event_ref`.

The shared view contains a deterministic `smv_...` ID/hash derived from the
actual G014 snapshot/frame pair. Immediate candidates additionally require a
trusted upstream event (`event_id`, `event_hash`/`content_hash`, and canonical
`reference_price`) passed separately to `finalize_decision`; a price-only or
model-generated event reference is never accepted.

All prices use the G014 positive-Decimal lexical form.  No float, NaN,
Infinity, bool-as-int, duplicate JSON key, unknown field, multiline reason or
non-canonical decimal is accepted.  Actionable levels are always declared as
`derived_interpretation`; `source_not_stated` stop/target/entry support is a
hard rejection.  A reason is one non-empty line of at most 240 Unicode code
points.

`finalize_decision` requires the authoritative G016 `StrategyRegistry` and
calls both `validate_packet` and `validate_profile_lineage`.  It also validates
the supplied G014 market snapshot/observation pair, then uses
`finalize_artifact` and `validate_artifact(..., related_artifacts=...)` for the
result.  Its result extension carries a compact `g018_model_output_ref` with a
content-derived `moc_...` candidate identity for G022.  Rejections are typed
and carry no scheduler instruction.
The public `map_candidate_to_normalized_model_candidate` helper produces the
exact frozen G014 candidate expected in `normalized_model_response.candidate`;
the surrounding response's `extensions.g018_model_output_ref` must equal the
candidate's compact model-output reference.

The scheduler instruction is content-derived and excludes account, quantity,
R and P&L fields.  It contains the branch, strategy/profile/result lineage,
market view, second-event reference, direction/levels and exactly one of
`next_call_after_seconds` (N) or `entry_wait_seconds` (X).

Each instruction carries the complete deep-copied `strategy_profile_ref` as
well as root `strategy_id`, `profile_ref_id` and `profile_ref_hash`. Standalone
`validate_instruction` checks the G016 profile self-shape and these internal
bindings; callers with an authoritative profile/registry can pass
`expected_profile_ref=` and `registry=` for exact lineage validation. The
finalizer always checks against the profile already accepted by G016.

```python
from pa_engine.strategy.decision import finalize_decision, parse_candidate

candidate = parse_candidate(raw_json)
outcome = finalize_decision(
    candidate,
    registry=registry,
    packet=packet,
    profile_ref=profile_ref,
    evidence_package=evidence,
    market_snapshot=market_snapshot,
    observation_frame=observation_frame,
    analysis_request=request,
    normalized_model_request=model_request,
    normalized_model_response=model_response,
    second_event=trusted_event,  # required for enter_immediately
)
if outcome.valid:
    submit_to_scheduler(outcome.instruction)
```
