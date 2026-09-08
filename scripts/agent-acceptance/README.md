# Real-agent artifact delivery acceptance

This opt-in runner checks one real journey: a fresh synthetic Human asks its PA to create a CSV, correct it in a second turn, and deliver it to an owned shared Messaging channel in a third turn. The Human downloads the actual message attachment and reloads Direct Chat. It requires completed tool results, exact downloaded bytes, stable message identity after reload, and the selected deployment images. It does not accept the model's claim as proof of delivery.

This is specifically for the existing **Docker context `default`, Compose project `sumi-dev`** deployment on the machine running the script. It is not a generic remote/Workers harness. The selected browser origin must reach that deployment and appear in the API's allowed browser origins. It never builds, deploys, changes provider configuration, or cleans Docker images/volumes.

## Prerequisites

- A running `sumi-dev-api-1`, `sumi-dev-runtime-provisioner-1`, `sumi-dev-web-1`, and `sumi-dev-postgres-1`, with real provider credentials already configured. Running this scenario consumes provider usage and writes retained synthetic data.
- The complete four-image `images.json` produced by `scripts/operations/build-dogfood-images`, for the exact deployed full commit SHA. It must be a regular file owned by the invoking user, without group/other permissions. All four immutable image IDs and lazy-started runtime/executor/broker images are checked.
- Existing web dependencies (including `@playwright/test`) installed under `apps/web`, and an explicitly selected local Chromium-compatible executable. The runner does not install dependencies or browsers.
- An owned executable built from **`apps/api/cmd/e2e-session-cookie` at the deployed revision**, or an already verified compatible build of that same command. There is no private-only issuer implementation. For example, after checking disk capacity, from `apps/api` at that revision:

  ```bash
  go build -o /tmp/sumi-acceptance-session-issuer ./cmd/e2e-session-cookie
  chmod 700 /tmp/sumi-acceptance-session-issuer
  ```

  Do not build an arbitrary current checkout and assume session compatibility. The runner reads the deployed API's signing secret into memory and passes it only to this local issuer through its environment. It supplies only its newly created Human/PA IDs and does **not** set the issuer's optional database/provisioning variables. Never substitute an untrusted executable. The issued session lasts 15 minutes.

## Run deliberately

From the repository root, replacing placeholders with inspected deployment values:

```bash
node scripts/agent-acceptance/artifact-delivery.mjs --run \
  --sha FULL_DEPLOYED_SHA \
  --manifest /absolute/path/to/images.json \
  --origin http://DEPLOYMENT_HOST:5173 \
  --issuer /tmp/sumi-acceptance-session-issuer \
  --provider opencode-go \
  --model kimi-k2.7-code \
  --browser /usr/bin/google-chrome
```

Provider/model are expected values, not configuration overrides. Every observed final model reply must match them. A mismatch fails the run; it does not silently switch the deployment model. Other providers remain unverified until this actual journey passes with them.

With no arguments the runner reports `SKIPPED` and performs no external operations. Local checks also perform no external operations:

```bash
node --check scripts/agent-acceptance/artifact-delivery.mjs
node scripts/agent-acceptance/artifact-delivery.mjs --self-check
node --test scripts/agent-acceptance/artifact-delivery.test.mjs
```

## What changes and what proves success

The runner atomically inserts only a new synthetic Human, PA, employment, and wrapping key. Workspace creation, targeted PA invitation, Messaging/Direct Chat installation and channel creation use normal browser UI/API. It does not insert memberships, messages or attachments through SQL.

The model must complete `write_file → read_file`, then `edit_file → read_file`, then invitation acceptance and Messaging open/write to the selected channel. Operations are sequential for this narrow scenario. A different valid strategy can fail this probe without implying a general product defect. The normal attachment card must download `item,count\napples,3\n` as 20 UTF-8 bytes; evidence records its SHA256. Reload must reproduce the final assistant message without another Human command.

Existing runtime review remains in force. The only automated approval is the existing synthetic correction's exact `edit_file` operation, path, old/new string, single resource scope and no extra arguments. It clicks the uniquely matching normal Human approval card and verifies the outbound `approve_once` for the exact request ID and completed held tool call. Any other approval reports `BLOCKED / HUMAN_APPROVAL_REQUIRED`. No standing permission or reviewer configuration changes. The successful baseline below required **zero approvals**, so it does not validate that optional UI branch.

Each model turn is bounded to 240 seconds, readiness to 60 seconds, downloads/reload to 30 seconds, browser operations to 10 seconds, navigation/process operations to 30 seconds, and fetches to 8 seconds. There are no unbounded retries. Unexpected errors are reported using a bounded error code rather than dumping secrets or frames.

## Evidence, cleanup, and status

Each run writes a fresh owner-only `/tmp/sumi-artifact-delivery-acceptance-*/evidence.json`. Evidence includes owned fixture IDs, deployment IDs, actual model/provider, public synthetic replies, completed tool metadata, exact CSV content/hash, and cleanup outcomes. It excludes cookies, credentials, raw WebSocket frames, reasoning, traces, HARs and screenshots. Treat the evidence as local operational data; publish an allowlisted summary when needed.

Cleanup checks that the session still belongs to the generated Human, disables only its personal Direct Chat and the generated Workspace's Messaging installation through normal APIs, revokes its session, and closes the browser. It attempts to recover an installation's lost POST response by listing only that new owner. Each cleanup result is recorded even after a failed model turn.

**Retained:** synthetic identity, Workspace/channel/membership, transcript, workspace file, uploaded attachment and runtime state. These remain identified in evidence for normal lifecycle handling; cleanup is not deletion or rollback. An interrupted process or failed session setup may leave retained fixtures, and the evidence's phase/cleanup fields must be inspected. The runner never resumes an older fixture or deletes pre-existing data. Runtime idle reclamation remains the application's responsibility.

- `PASSED` (exit 0): scenario and all required cleanup confirmations passed.
- `PASSED_CLEANUP_INCOMPLETE` (exit 1): scenario passed but cleanup was not fully confirmed; do not call the whole run accepted.
- `BLOCKED` (exit 1): an approval outside the exact synthetic correction requires a Human decision. No approval bypass/retry.
- `FAILED` (exit 1): precondition, tool/result, actual bytes, deployment, model, or other scenario check failed. Inspect `failure`, `lastWorkPhase`, `lastReply` and cleanup. Preflight failures before evidence creation emit a JSON status with `fixture: not-created`.
- `SKIPPED` (exit 0): no opt-in run requested; this proves nothing about product behavior.

Baseline: [2026-09-08 accepted delivery evidence](../../docs/agent/evidence/artifact-delivery-2026-09-08.json), deployment `7d898b0d28c78a9e4d732890dd4a7c5a78d664f0`, actual `opencode-go/kimi-k2.7-code`. That live run preceded this portable checked-in version; syntax/pure checks of this version are not a new live acceptance.

This scenario does not cover Firebase signup/login for an existing person, physical iOS, runtime generation restart, compaction, arbitrary artifact types, or the rest of Issue #362.
