# G021 deterministic fixture scope

Focused tests use the accepted G019 recorded IEX response and deterministic
G020 local-second events. `candle_vectors.json` records exact 1m/5m/hour plus
G019-calendar-aligned 1Day/1Week Decimal vectors, `session_edges.json` fixes
regular-session/DST/cutover boundaries, and `gap_revision_cases.json` drives
sparse no-trade, confirmed connection-gap, and immutable reconciliation cases.
The fixture boundary intentionally keeps no API credentials, strategy state,
model output, or account data.
