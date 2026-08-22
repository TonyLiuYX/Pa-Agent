# Provider adapter guide (G017)

The provider boundary accepts and returns the frozen G014 JSON artifacts:
`normalized_model_request`, `normalized_model_response`, and
`model_call_metadata`.  A transport adapter implements `ModelAdapter.complete`
and returns an `AdapterOutcome`; it does not expose SDK request objects,
provider-specific thinking fields, prompts, account balances, or market data.

Use `make_normalized_model_request` to construct requests.  The only
provider-neutral thinking switch is `extensions.reasoning_mode`, with the
value `enabled` or `disabled`; a later adapter may translate that value to its
own wire format.  Request semantic identity is the G014 content hash, so
correlation IDs and call telemetry are not replay keys.

`ScriptedAdapter` supplies deterministic sequences for tests.  `ReplayAdapter`
loads validated request/response pairs from memory or JSON and looks them up by
semantic request identity.  Missing or contradictory recordings are surfaced as
typed terminal outcomes/errors rather than retried as provider failures.

Each concrete adapter is the request boundary: it validates the normalized
request, checks known runtime secret values, and only then evaluates requested
capabilities.  `ProviderNeutralModelGateway` validates the G014 request and
transparently invokes the adapter; it does not preflight capability outcomes or
bypass the adapter's secret check.

Cancellation and late-response helpers carry an adapter-only `generation`
number on `AdapterOutcome`; it is not written into G014 artifacts or hashes.

Runtime credentials belong only in `RuntimeCredentialResolver`.  Keep the
returned handle at the transport boundary, never put it in a contract or
fixture.  Pass its `secret_values` to a deterministic adapter or the contract
constructor when a test needs to assert end-to-end redaction, and use
`redact_secrets`/`redacted_json` for diagnostic values.
Timeout and rate-limit outcomes are retryable. Server/provider/transport
outcomes are retryable only when the adapter explicitly marks them
`transient=true`; an unclassified provider-like error is terminal by default.
The `scripted_error` convenience helper marks its default server/provider/
transport vector transient, while an explicit `retryable` bit never substitutes
for that evidence.
Authentication, malformed response, cancellation, late response,
configuration, capability, replay, and exhaustion outcomes are terminal.

The fixed runtime adapters are `pa_engine.providers.deepseek` (G022) and
`pa_engine.providers.openai` (G031).  Runtime selection constructs exactly one
of them; the OpenAI adapter uses `OPENAI_API_KEY`, the fixed
`gpt-5.6-luna`/`reasoning.effort=max` Responses request, and the service-tier
choice documented in its package README.
