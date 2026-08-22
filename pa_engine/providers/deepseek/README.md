# DeepSeek V4 Flash adapter (G022)

`DeepSeekAdapter` implements the G017 `ModelAdapter` boundary for the
documented Chat Completions endpoint:

`POST https://api.deepseek.com/chat/completions`

Each call sends one request to the fixed
`https://api.deepseek.com/chat/completions` endpoint with
`model: "deepseek-v4-flash"`, two text messages from the normalized request,
`response_format: {"type": "json_object"}`, and `thinking: {"type":
"enabled"|"disabled"}`.  G022 does not expose a reasoning-effort choice;
DeepSeek's documented enabled default is used.  When thinking is **enabled**,
the payload omits `temperature`, `top_p`, `presence_penalty`, and
`frequency_penalty` because DeepSeek documents those fields as unsupported or
no-ops in thinking mode.  When thinking is **disabled**, the normalized G017
temperature is sent as `temperature`.  `extensions.reasoning_mode` remains the
only provider-neutral thinking field and is translated only while building this
transport payload.

The adapter makes exactly one HTTP attempt.  Timeout and rate-limit outcomes
are retryable according to G017; 400/401/402/422 are terminal request,
authentication, balance, or parameter failures; 500/503 server failures are
marked transient.  Injected transport statuses must be real integers in the
HTTP range 100..599; malformed/out-of-range values become terminal transport
failures.  The default urllib transport disables all HTTP redirects, so every
301/302/303/307/308 response is one terminal provider attempt and never sends
the Authorization header to a second URL.  Scheduler retry timing belongs to
G023.

The response mapper first requires the official non-streaming envelope:
`object` must be `chat.completion`, `model` must be `deepseek-v4-flash`, and
`choices` must contain exactly one mapping with `index: 0`.  Optional `id`,
`created`, usage, and system-fingerprint fields are retained when valid but
are not required for candidate lineage.  It then parses the assistant
`content` through the strict G018
candidate parser.  It maps the accepted candidate to the frozen G014
normalized candidate and stores the exact raw candidate in
`extensions.g018_model_output_ref`, which lets G018 bind the later finalizer.
Malformed, truncated (`length`), tool-call, content-filter, and unsupported
output is a terminal `invalid_model_response` outcome and never becomes a
decision; unknown or secret-bearing finish reasons are not retained in
telemetry.  The official `insufficient_system_resource` finish reason is
different: it indicates an inference-system interruption, so the adapter
returns a candidate-free `server` outcome with explicit `transient: true` and
`retryable: true` while preserving finish, usage, request-id, timestamp, and
optional DeepSeek telemetry for G023's retry decision.

Usage's three core counters stay in `model_call_metadata.usage`.  DeepSeek's
optional cache and reasoning counters are retained under
`model_call_metadata.extensions.deepseek` and are `null` when absent.  The
reasoning content itself is never persisted or logged.

The endpoint is not a configurable live host; custom transports are the test
seam.  Credentials are runtime-only.  Pass `api_key=` or a G017
`RuntimeCredentialResolver`; the key is sent only in the Authorization header
and is excluded from requests, responses, metadata, reprs, exceptions and
smoke output.  One adapter instance may be used concurrently by multiple
strategy calls.

## Smoke command

Build a payload without a key or network access:

```bash
python3 -m pa_engine.providers.deepseek.smoke --dry-run --thinking disabled
```

Run a user-supplied live smoke without persisting the key (G030 owns the
credentialed evidence):

```bash
DEEPSEEK_API_KEY='…' python3 -m pa_engine.providers.deepseek.smoke --thinking enabled
```

The command prints only sanitized classification/lineage/usage fields.  It
does not write files or print the environment value.

The wire mapping follows DeepSeek's [Chat Completions API](https://api-docs.deepseek.com/api/create-chat-completion/),
[Thinking Mode](https://api-docs.deepseek.com/guides/thinking_mode), and
[JSON Output](https://api-docs.deepseek.com/guides/json_mode) guides.  Error
and rate-limit status handling follows the provider's [error-code](https://api-docs.deepseek.com/quick_start/error_codes)
and [rate-limit](https://api-docs.deepseek.com/quick_start/rate_limit) docs.
