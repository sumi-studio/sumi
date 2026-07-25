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

OpenCode Zen Go is the only mandatory live provider proof for the T25
provider-release slice. The non-ignored `live_opencode_go_provider_release_gate`
runs `opencode-go` when `SUMI_LIVE_TEST=1` and must complete a real two-turn
exchange: one `echo_value` tool-call/result round-trip and replayable reasoning
on both turns. A missing or empty `OPENCODE_GO_API_KEY` fails the gate; a skip,
synthetic fixture, or gateway capture is not live success.

Moonshot direct, Z.ai direct, and Umans raw/live evidence remains explicitly
deferred and is not replaced by the OpenCode gate or by any fixture. When those
credentials become available, capture each direct provider using its documented
base URL, model, and credential variable. For each deferred direct-provider
capture, tool capture uses one deterministic `echo_value(value: string)` tool,
`tool_choice:"required"`, and a 128-token limit. Reasoning capture uses the
preset's production thinking control and the prompt `Reply with exactly OK after
reasoning.`. Do not infer one provider's dialect from another provider or
gateway.

OpenAI Responses fixture and durable round-trip proof remains required. A live
Responses proof through ChatGPT/Codex login is optional and is not part of this
packet.

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

The non-ignored `live_opencode_go_provider_release_gate` is the T25 release
dispatcher. It returns without external communication unless `SUMI_LIVE_TEST=1`;
with that opt-in it runs `opencode-go` and fails on a missing or empty
`OPENCODE_GO_API_KEY`. The four provider-specific live gates
(`live_opencode_go_two_turn_tool_reasoning_gate`,
`live_kimi_k3_direct_two_turn_tool_reasoning_gate`,
`live_glm_5_2_direct_two_turn_tool_reasoning_gate`,
`live_umans_direct_two_turn_tool_reasoning_gate`) remain ignored development
gates selected explicitly with `cargo test <live_gate_name> -- --ignored`.
Direct Moonshot/Z.ai/Umans evidence remains deferred; OpenCode Go is not a
substitute for those future proofs. `SUMI_ENV_FILE` may explicitly name a
dotenv file to load. Live tests are not substitutes for checked-in raw capture
provenance.
