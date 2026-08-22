# G021 shared market view

`SharedMarketViewBuilder` consumes one G019-validated Alpaca IEX/raw,
regular-session `market_snapshot` and one sequence of G020 frozen
`SecondAggregateEvent` values.  Post-freeze G020 reconciliation events are
retained as a new non-executable view context; they never replace the frozen
execution/current-price reference. The join is half-open at `cutover_at`:
history bars ending at the cutover are retained, live seconds ending at or
before it are overlap, and live intervals that begin at or after it are the
only intervals aggregated into new candles.  A straddling interval fails
closed.

The builder emits an immutable `SharedMarketView` with exact Decimal text,
closed G014 snapshot/observation lineage, separate forming-candle metadata,
the latest trusted second-event reference plus embedded validated G020 source
evidence, six scheduler readiness facts, raw basis and an identity derived
from all of those values. Sparse no-trade intervals are recorded as non-
terminal metadata, while confirmed gaps remain fail-closed. `to_decision_ref()` returns the
small G018-compatible snapshot+observation pair reference; it is intentionally
distinct from the richer `mview_…` identity.

`SharedMarketViewPublisher.publish_from` constructs one view and fans it to a
batch of subscribers.  No network, credential, model, strategy, account or
persistence state is used by this package.
