# Chat provider fixture provenance

Every `.sse` file in this directory declares its source kind in
`provenance.json`. Existing Kimi/GLM cases and provider-specific finish reasons
are deterministic synthetic contract fixtures; they are not represented as
live captures. `opencode_kimi_k2_7_code_text.sse` is a sanitized historical live
curl capture. Complete normalized event/final-message snapshots live separately
under `../snapshots/`.

## T25 release live proof

The T25 provider-release live dispatcher is the local development-only Codex
OAuth bridge for OpenAI Responses:

- non-ignored gate: `provider::tests::live_codex_responses_provider_release_gate`
- bridge: `scripts/dev/codex-responses-proxy.py`
- opt-in: `SUMI_LIVE_TEST=1`
- required environment:
  - `SUMI_CODEX_RESPONSES_BASE_URL` — loopback bridge URL, e.g. `http://127.0.0.1:8765`
  - `SUMI_CODEX_RESPONSES_PROXY_SECRET` — shared startup secret for the bridge
  - `SUMI_CODEX_RESPONSES_MODEL` — optional, defaults to `gpt-5.6-sol`; if set, must be non-empty. The release gate uses Sol because its first canonical tool-use turn must yield encrypted reasoning context for the required preservation/replay proof.

The gate unconditionally requires the first turn to return and preserve non-empty encrypted provider context, then replay it into the second turn.

Start the bridge and export its secret:

```sh
python3 scripts/dev/codex-responses-proxy.py --auth-file ~/.codex/auth.json
# It prints PROXY_SECRET=<secret>
export SUMI_CODEX_RESPONSES_BASE_URL=http://127.0.0.1:8765
export SUMI_CODEX_RESPONSES_PROXY_SECRET=<secret>
```

The Sumi adapter sends the ordinary `Authorization: Bearer <proxy-secret>`
header. The bridge validates the bearer token in constant time before reading
the request body, then replaces it with Codex OAuth credentials for the
upstream request only. The bridge mutates the public OpenAI Responses request
shape: it removes `max_output_tokens`, forces `stream=true`/`store=false`, and
injects Codex OAuth headers. This validates the Responses adapter against the
ChatGPT Codex subscription endpoint, not the public OpenAI API-key contract,
and is not a production provider path.

Run the release gate without any credential value in argv:

```sh
SUMI_LIVE_TEST=1 cargo test --manifest-path apps/agent/Cargo.toml \
  provider::tests::live_codex_responses_provider_release_gate -- --nocapture
```

A missing or empty `SUMI_CODEX_RESPONSES_BASE_URL` or
`SUMI_CODEX_RESPONSES_PROXY_SECRET` fails the gate. The gate must complete a
real `store:false` two-turn exchange: exactly one `echo_value` tool-call/result
round-trip, preservation/replay of non-empty encrypted provider context from
the first turn into the second turn, and non-empty expected second-turn text.
It must emit bounded evidence without tokens or raw secrets.

## Fixture and durable restart proofs (mandatory and distinct)

OpenAI Responses fixture and durable round-trip proof remains required and is a
separate acceptance contract from the live bridge. The bridge mutates the
public request shape, so it cannot substitute for fixture or durable restart
proof. The provenance ledger binds the fixture tests in
`apps/agent/tests/fixtures/provenance.json` and the durable round-trip tests in
`store::event_writer`.

## Post-deadline and release-blocking provider evidence

OpenCode Zen Go is confirmed unavailable for the T25 deadline and is moved to
post-deadline provider-qualification debt; the Codex OAuth bridge OpenAI
Responses live proof replaces it for the T25 provider-release gate.

Direct Moonshot, Z.ai, and Umans live/raw proofs remain credential-gated
developer/provider-qualification probes for the Chat Completions track. They
are not completed, deleted, or substituted by the Responses bridge, and are
not blockers for the Responses-only Cloud release gate.

The OpenCode capture script below is retained for future qualification. Capture
only after explicitly loading the `opencode-go` credential from the local
OpenCode auth store without printing it:

```sh
(
  set +x
  set -eu
  umask 077
  LC_ALL=C
  export LC_ALL
  case ${OPENCODE_GO_API_KEY-} in
    '') printf '%s\n' \
      'OPENCODE_GO_API_KEY must be a non-empty OpenCode Bearer token' >&2
      exit 1
      ;;
    *[!A-Za-z0-9._~-]*)
      printf '%s\n' \
        'OPENCODE_GO_API_KEY contains a byte outside [A-Za-z0-9._~-]' >&2
      exit 1
      ;;
  esac

  curl_config=
  raw_tmp=
  cleanup() {
    [ -z "$curl_config" ] || rm -f -- "$curl_config"
    [ -z "$raw_tmp" ] || rm -f -- "$raw_tmp"
  }
  trap cleanup EXIT HUP INT TERM

  repo_root=$(git rev-parse --show-toplevel)
  repo_root=$(cd -- "$repo_root" && pwd -P)
  quarantine_dir="$repo_root/target/provider-captures/opencode-go"

  install -d -m 0700 -- \
    "$repo_root/target/provider-captures" "$quarantine_dir"
  quarantine_dir=$(cd -- "$quarantine_dir" && pwd -P)
  case "$quarantine_dir" in
    "$repo_root"/*) ;;
    *) exit 1 ;;
  esac

  curl_config=$(mktemp /tmp/sumi-opencode-curl.XXXXXX)
  raw_tmp=$(mktemp \
    "$quarantine_dir/opencode-kimi-k2-7-code.XXXXXX.tmp")
  case "$raw_tmp" in
    "$repo_root"/*) raw_tmp_relative=${raw_tmp#"$repo_root"/} ;;
    *) exit 1 ;;
  esac
  git -C "$repo_root" check-ignore --quiet --no-index -- \
    "$raw_tmp_relative"

  printf 'header = "Authorization: Bearer %s"\n' \
    "$OPENCODE_GO_API_KEY" >"$curl_config"
  printf 'header = "Content-Type: application/json"\n' >>"$curl_config"
  chmod 0600 "$curl_config"

  curl --disable --config "$curl_config" \
    --silent --show-error --no-buffer --fail-with-body --max-time 60 \
    --output "$raw_tmp" \
    https://opencode.ai/zen/go/v1/chat/completions \
    --data-binary '{"max_tokens":64,"messages":[{"content":[{"text":"Reply with exactly fixture-ok","type":"text"}],"role":"user"}],"model":"kimi-k2.7-code","stream":true,"stream_options":{"include_usage":true}}'

  chmod 0600 "$raw_tmp"
  captured_at=$(date -u '+%Y%m%dT%H%M%SZ')
  raw_capture_bytes=$(wc -c <"$raw_tmp")
  raw_capture_sha256=$(sha256sum -- "$raw_tmp")
  raw_capture_sha256=${raw_capture_sha256%% *}
  raw_capture="${raw_tmp%.tmp}-${captured_at}-${raw_capture_sha256}.raw.sse"
  [ ! -e "$raw_capture" ]
  case "$raw_capture" in
    "$repo_root"/*) raw_capture_relative=${raw_capture#"$repo_root"/} ;;
    *) exit 1 ;;
  esac
  git -C "$repo_root" check-ignore --quiet --no-index -- \
    "$raw_capture_relative"
  mv -- "$raw_tmp" "$raw_capture"
  raw_tmp=

  printf 'quarantined raw capture: %s\n' "$raw_capture"
  printf 'captured at: %s; bytes: %s; pre-sanitization SHA-256: %s\n' \
    "$captured_at" "$raw_capture_bytes" "$raw_capture_sha256"
)
```

Keep `--disable` as curl's first option: the mode-0600 config keeps the
credential out of argv and disables user `.curlrc` tracing. Run the command from
any CWD inside the repository worktree; it fails outside a worktree. The command
accepts only a non-empty
`[A-Za-z0-9._~-]` OpenCode Bearer token. This set covers OpenCode's current
ASCII key shape while rejecting whitespace/control bytes, quotes, backslashes,
and curl-config directive injection before any config or capture file is
created. Before curl writes a response body, the command verifies that the exact
repository-relative temporary path is ignored. It derives the final path only
after hashing, then verifies that exact prospective path is also ignored before
publishing. Both checks fail closed unless the path is below the resolved
repository root. Success atomically publishes only a mode-0600 raw file in its
mode-0700 directory. Transport failures and HTTP 401/429/5xx leave both existing
captures and the tracked fixture unchanged because `--fail-with-body` writes
only to the trapped temporary file. Never print the config, environment,
headers, or command trace.

Moonshot direct, Z.ai direct, and Umans raw/live evidence remains a
release-blocking missing obligation for the Chat Completions provider track and
is not replaced by the Responses bridge or by any fixture. When those
credentials become available, capture each direct provider using its documented
base URL, model, and credential variable. For each pending direct-provider
capture, tool capture uses one deterministic `echo_value(value: string)` tool,
`tool_choice:"required"`, and a 128-token limit. Reasoning capture uses the
preset's production thinking control and the prompt
`Reply with exactly OK after reasoning.`. Do not infer one provider's dialect
from another provider or gateway.

## Promotion

Keep the unmodified response body only in the ignored quarantine. Promotion is
a separate, deliberate review:

1. Create and manually sanitize a separate mode-0600 candidate under
   `target/provider-captures/sanitized-candidates/`. The repository has no
   automatic capture sanitizer. Never place or copy the raw file under the
   tracked fixture directory.
2. Record every replacement and fail promotion unless an inspection accounts
   for secrets/Authorization material, request/response IDs, timestamps, and
   user/account/organization identifiers without exposing them in argv,
   terminal output, logs, or provenance. Preserve SSE order/framing, JSON field
   placement, usage/cost, reasoning fields, finish reasons, `[DONE]`, and its
   trailer.
3. In a disposable worktree, install only the sanitized candidate plus proposed
   provenance and run the full normalization/provenance suite:

   ```sh
   repo_root=$(git rev-parse --show-toplevel)
   cargo test --manifest-path "$repo_root/apps/agent/Cargo.toml"
   ```

4. Update `provenance.json` with UTC time, endpoint/model/case, raw bytes and
   pre-sanitization SHA-256 printed above, a capture command with only a
   `$..._API_KEY` placeholder, and the sanitization list. After the candidate,
   provenance inspection, and tests pass, atomically rename a temporary copy of
   that candidate—not the raw file—over the tracked fixture, then rerun the
   tests on the final tree.

Never store or print the credential or Authorization header. The temporary curl
config must never be retained as provenance.

## Test selection

The non-ignored `live_codex_responses_provider_release_gate` is the T25 release
dispatcher. It returns without external communication unless `SUMI_LIVE_TEST=1`;
with that opt-in it runs through the local Codex OAuth bridge and fails on a
missing or empty `SUMI_CODEX_RESPONSES_BASE_URL` or
`SUMI_CODEX_RESPONSES_PROXY_SECRET`. The two OpenCode Go live gates
(`live_opencode_go_two_turn_tool_reasoning_gate` and
`live_opencode_go_provider_release_gate`) are ignored post-debt probes; they
are selected explicitly with `cargo test <live_gate_name> -- --ignored`.
The direct Moonshot, Z.ai, and Umans live gates
(`live_kimi_k3_direct_two_turn_tool_reasoning_gate`,
`live_glm_5_2_direct_two_turn_tool_reasoning_gate`,
`live_umans_direct_two_turn_tool_reasoning_gate`) are ignored
credential-gated probes for release-blocking missing Chat Completions evidence.
Direct Moonshot/Z.ai/Umans evidence remains a release-blocking missing
obligation for the Chat Completions provider track; the Responses bridge and
OpenCode Go are not substitutes for those future proofs. `SUMI_ENV_FILE` may
explicitly name a dotenv file to load. Live tests are not substitutes for
checked-in raw capture provenance.
