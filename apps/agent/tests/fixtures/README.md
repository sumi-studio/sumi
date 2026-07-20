# Chat provider fixture provenance

Every `.sse` file in this directory declares its source kind in
`provenance.json`. Existing Kimi/GLM cases and provider-specific finish reasons
are deterministic synthetic contract fixtures; they are not represented as
live captures. `opencode_kimi_k2_7_code_text.sse` is a sanitized live curl
capture. Complete normalized event/final-message snapshots live separately
under `../snapshots/`.

The near-term live default is OpenCode Zen Go. Its capture preserves the
provider's `reasoning_content` deltas, usage/cost placement, `[DONE]`, and the
post-DONE cost trailer; `[DONE]` remains the canonical normalized terminal.
The request body is the complete `build_request` output fixed by
`opencode_live_capture_request` in `chat_send_matrix.json`; the provenance test
keeps the recorded body equal to that production builder output. Capture only
after explicitly loading the `opencode-go` credential from the local OpenCode
auth store without printing it:

```sh
curl -sS -N --max-time 60 \
  -H "Authorization: Bearer $OPENCODE_GO_API_KEY" \
  -H "Content-Type: application/json" \
  https://opencode.ai/zen/go/v1/chat/completions \
  --data-binary '{"max_tokens":64,"messages":[{"content":[{"text":"Reply with exactly fixture-ok","type":"text"}],"role":"user"}],"model":"kimi-k2.7-code","stream":true,"stream_options":{"include_usage":true}}'
```

Moonshot direct, Z.ai direct, and Umans raw/live evidence remains mandatory but
is deferred as a release-blocking T25 provider-release gate because those
credentials were unavailable during T8. A missing credential, the OpenCode
gateway capture, or a synthetic fixture does not satisfy that gate. Use the
corresponding documented base URL, model, and credential variable. Tool capture uses one deterministic
`echo_value(value: string)` tool, `tool_choice:"required"`, and a 128-token
limit. Reasoning capture uses the preset's production thinking control and the
prompt `Reply with exactly OK after reasoning.`. Do not infer one provider's
dialect from another provider or gateway.

Store the unmodified response body as a raw capture before generating expected
normalized snapshots. Sanitization may replace API secrets (which must not be
present in a response), response/request IDs, timestamps, and user-specific
account identifiers. It must preserve SSE event order, line boundaries, JSON
field placement, usage placement, reasoning field names, and provider finish
reasons. Record the UTC capture time, endpoint origin, model, request case,
capture command with a `$..._API_KEY` placeholder, sanitization operations, and
SHA-256 of the pre-sanitized capture in `provenance.json`. Never store or print
the credential or Authorization header.

Provider-specific live gates are ignored by the ordinary test suite and must be
selected explicitly with `cargo test <live_gate_name> -- --ignored`. A selected
gate requires its preset's non-empty credential environment variable and fails
when the credential is missing; `SUMI_ENV_FILE` may explicitly name a dotenv
file to load. Live tests are not substitutes for checked-in raw capture
provenance.
