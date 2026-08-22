# G015 market validation and observations

G015 consumes the frozen `pa_engine.contracts` G014 artifacts and keeps the
market seam provider-neutral.  `validate_market_snapshot` performs the G014
schema/identity pass plus exact market checks; `assess_market` applies the
frozen policy rows and `build_observation_frame` emits only objective OHLCV
primitives.

The primary assessment call is `assess_market(snapshot, policy, request=None)`;
when a request is provided it is always validated and any invalid request is a
pre-provider terminal `invalid_market_data` assessment.  Callers preferring
the argument order from the request-first pipeline can use
`assess_market_bundle(snapshot, request, policy)`.

## Feed identity

`MarketFeedRef` is the immutable `0.1.0` value with exactly
`schema_version`, `provider`, `feed` and `coverage`.  Each value is a
lower-case slug.  Its canonical bytes are G014 canonical JSON and its SHA-256
hash is stable under object-key reordering.  The optional
`extensions.market_feed` object is copied into observation-frame extensions so
it remains hash-bound.  G014 fixtures with `{}` extensions remain valid unless
`require_feed=True` or an `expected_feed` is supplied.  Downstream G019–G021
should require and match the same reference (for the first path,
`ALPACA_IEX_FEED = {schema_version: "0.1.0", provider: "alpaca", feed:
"iex", coverage: "single_exchange"}`); omitted, malformed or mismatched feed
identity terminates before a provider/model call.

## Exact time and data policy

UTC timestamps use the G014 `Z` grammar with one through nine fractional
digits.  Comparisons parse all nine digits as integer nanoseconds, so `.0Z` and
no fraction denote the same instant and `.0000001Z` remains distinct from
`.0000002Z`.  This comparison equivalence does not relax frozen G014
cross-artifact lineage: a request must reuse the snapshot's original UTC
lexeme where G014 related equality requires it; G015 never rewrites those
strings.  Coverage expected/actual/missing intervals are checked as an exact
ordered union, with no fabricated or unreported gaps.  Minute/hour bars must
have their exact fixed duration.  Day/week
bars require only positive ordered UTC intervals: no 24-hour/168-hour rule is
invented across sessions or daylight-saving changes.  Direct second
timeframes remain rejected by the frozen G014 schema.

Structural failures map to the frozen outcomes `malformed_ohlcv`,
`ohlc_invariant`, `duplicate_bars`, `out_of_order_bars` and `future_bars`.
Invalid request/policy/feed/cross-lineage metadata terminates pre-provider
with the existing `invalid_market_data` contract code; no new persisted G014
quality codes are introduced.  No interpolation or synthetic gap bars are
created.

Policy assessment filters `is_closed=true` and excludes a final incomplete bar
before counting history.  Too few closed bars caused by that final bar produce
`incomplete_final_bar_insufficient`; otherwise they produce
`insufficient_closed_history`.  Partial coverage is then allowed or rejected,
staleness is compared exactly against `analysis_as_of`, and a sufficient
incomplete final bar contributes `incomplete_final_bar_sufficient`.  Continue
outcomes can accumulate.  Rows are evaluated in frozen precedence order: the
first terminal row (ordinary insufficiency, partial disallowed, or stale data)
stops all lower rows; a partial-allowed row can therefore be followed by stale
or incomplete-insufficient.  A terminal outcome always prevents a model call.
Warning and uncertainty unions are deterministic.

Producer quality metadata is checked for consistency before policy use:
`invalid`/`invalid_market_data` is structural, `partial_data` requires partial
coverage, and `incomplete_last_bar_excluded` requires a final incomplete bar.
`stale_data` and `insufficient_history` are accepted only when the exact
staleness/history policy computes the same condition; contradictory claims are
rejected as malformed rather than copied into an accepted frame.

## Observation formulas

All arithmetic stays in `Decimal` with sufficient local precision and output is
canonical fixed-point text (no float, exponent, trailing zero or `-0`).  For
each closed bar:

* `direction` compares close/open (`bullish`, `bearish`, `doji`).
* `range = high-low`; `body = abs(close-open)`.
* `upper_wick = high-max(open,close)`;
  `lower_wick = min(open,close)-low`.
* `inside` means the current range is contained by the previous with at least
  one strict edge; `outside` is the inverse.  Exact-equal and partial
  intersections are `overlap`; disjoint ranges are `none`.
* A full-range gap is `up` when `current.low > previous.high`, `down` when
  `current.high < previous.low`, otherwise `none`.
* A rolling high/low uses the preceding N contiguous closed bars, excluding
  the current bar; insufficient history is represented by nulls.

Relations and rolling windows never bridge a missing or non-contiguous
interval.  Lookbacks are validated as positive integers, de-duplicated and
sorted.  Session, timezone and adjustment policy stay structurally
provider-neutral at G015; downstream provider adapters may impose raw,
regular-session/New-York and adjustment expectations.
