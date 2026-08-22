# Paper math v0.1.0

This package is the independent, in-memory paper-account seam for Phase 2.
Each `strategy_id` is initialized once at USD `1000.00`; no strategy shares
balance or positions with another strategy, even when symbols and directions
match.  A strategy can have at most one pending/open trade in this slice.
`PaperAccount` has no restore-balance argument; fresh-account hydration is
owned by G027.

The math policy is deliberately exact:

- quantity is `balance / entry` at eight fractional-share places using
  `ROUND_DOWN`;
- money, balances and applied/net P&L are USD cents using `ROUND_HALF_UP`;
- displayed planned `R` is four places using `ROUND_HALF_UP`;
- gross P&L is retained without cent rounding, then losses follow
  cap → cent-quantize → re-clamp → non-negative balance;
- fees and slippage are exactly `Decimal("0.00")` in every settlement/event,
  and no order-book, margin, borrow,
  dividend, split, broker, or real-money behavior is represented.

Immediate entries use the exact post-response fill reference.  The levels are
revalidated against that reference, so drift outside `stop < entry < target`
or `target < entry < stop` emits a typed rejection with no balance mutation.
Conditional fills must equal the requested entry level exactly.  Instruction
transitions accept only the exact G018 branch names and a matching
`strategy_id`.

`PaperEvent` and `TransitionResult` are typed domain values only.  G027 owns
the persisted runtime-event envelope.  Caller-supplied idempotency keys bind to
a stable content hash: replaying an identical request is a no-op, while a
different request under the same key is a conflict and is never executed.
Event payloads and calculation/settlement mappings are recursively immutable;
`to_dict()` returns detached mutable projections for serialization.
