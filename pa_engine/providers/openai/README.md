# OpenAI GPT-5.6 Luna adapter (G031)

`OpenAIAdapter` implements the provider-neutral G017 boundary with the
standard-library Responses API transport:

`POST https://api.openai.com/v1/responses`

The wire request fixes `model: "gpt-5.6-luna"`,
`reasoning: {"effort": "max"}`, strict JSON-schema output for the G018
decision candidate, `store: false`, and a stateless text input.  The only
choice is `speed=standard|fast`, mapped exactly to
`service_tier=default|fast`.  `fast` is an API service tier and has no fixed
latency multiplier guarantee.

Each adapter call makes exactly one HTTP attempt.  Redirects are refused, and
the scheduler owns retry timing.  Runtime credentials are accepted only from
`OPENAI_API_KEY` or a G017 runtime resolver and are never retained in a
normalized artifact, prompt, SQLite row, repr, exception, or smoke result.

Successful Responses objects must be completed, report a valid effective
service tier and usage counters, and contain exactly one completed assistant
message with exactly one `output_text` content block.  Reasoning items may be
present, but refusals, tool calls, multiple assistant messages, incomplete
messages and ambiguous text are rejected.  The selected text is parsed and
validated by the existing G018 candidate parser before it becomes a normalized
response.

## G030 smoke

Offline payload check:

```bash
python3 -m pa_engine.providers.openai.smoke --dry-run --speed standard
```

Credentialed evidence is intentionally owned by G030:

```bash
OPENAI_API_KEY='…' python3 -m pa_engine.providers.openai.smoke --speed standard
```

The command prints only a sanitized classification, lineage, usage and
effective-tier projection; it never prints the key or raw response.
