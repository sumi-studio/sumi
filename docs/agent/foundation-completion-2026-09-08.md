# Issue #362 — implementation and acceptance ledger

The user expanded the engineering goal on 2026-09-08 to fully address
[Issue #362](https://github.com/sumi-studio/sumi/issues/362). PRs #373 and #374
are an intermediate milestone, not completion of that goal. This ledger
reconciles the original audit with current implementation and observable use.
It does not turn every comparative item into a required mechanism.

Product direction remains the user's instructions and the English README
Description. In particular, useful methods such as notes, reflection and
procedure reuse remain optional. Their compulsory use is not an acceptance
condition. No test of personhood is proposed here.

## Baseline and accepted milestone

Current shared deployment: `5f3fa8130775a359706ba055c7fda73f81b80f20` (PR #377).
The private-file acceptance below was performed on the preceding `8cbba0b8`
milestone. A subsequent recipient-delivery run on #376 passed after repairing
reviewer stream cancellation.

- #373 releases a temporary provider-overflow restriction after an actual
  durable L0→L1 replacement. Its 37 assembler tests and independent review
  passed; ordinary hydration alone does not remove the restriction.
- #374 connects private workspace `write_file` and `edit_file` through the
  production reviewed executor. All 144 executor tests, three deployment
  boundary checks, an actual-image Docker mutation check, independent review
  and both existing CI jobs passed.
- All four shared images were built from the same merged commit. API, web and
  provisioner were updated with their existing mounts, provider configuration
  and data. Postgres, Firebase and LiveKit containers remained in place.
- A fresh owned test identity submitted one browser command to the configured
  `opencode-go/kimi-k2.7-code`. Actual successful tool events showed file
  creation, original read, unique replacement and changed read. Reload retained
  the same reply ID, sequence and text without a second command. The fixture
  installation was disabled and its session revoked.

This is one successful real-model scenario, not a universal quality guarantee.
It does not establish recipient-visible artifact delivery, a generation
restart, physical iOS behavior, or the rest of #362. Existing-account startup
was not exercised by the fresh identity. Earlier long-context evaluation also
retains its documented semantic limitations.

A [checked-in evidence extract](evidence/workspace-text-2026-09-08.json) contains the synthetic tool sequence and acceptance boundaries.

Local full evidence for this milestone (contains only the owned synthetic test):
`/tmp/sumi-workspace-live-acceptance-4ZsvXt/evidence.json`.
Cutover metadata is in
`/tmp/sumi-workspace-cutover-8cbba0b8/cutover.json`; adjacent private resolved
configuration and backups contain secrets and must not be published.

## Recipient delivery and review recovery

PR #376 fixes an actual delivery blocker: closing one provider stream after a
reviewer read tool cancelled the shared review token, so the next round reported
`reviewer cancelled`. Each stream now owns a child token. The real HTTP/SSE
regression verifies read-tool continuation and parent cancellation; all three CI
jobs passed on the exact reviewed commit before merge.

The configured real model then executed eight successful tools across three
Human turns: write/read, edit/read, invitation list/accept and Messaging open/write.
The Human-facing attachment card downloaded `item,count\napples,3\n` (20 bytes),
matching the corrected CSV. The same final reply survived reload without another
command. Test installations were disabled, the session revoked and browsers closed.
Existing shared data/configuration were retained; the HTTPS entry returned 200.

[Allowlisted synthetic evidence](evidence/artifact-delivery-2026-09-08.json) records
actual tool sequences, download bytes/hash and the acceptance limits. The original
local evidence is `/tmp/sumi-artifact-delivery-acceptance-xT6mhc/evidence.json`.
This scenario did not request additional Human approval and does not establish a
generation restart, physical iOS behavior, or every existing-account login path.

## Existing-person restart after active-state recovery

PR #377 passed all three CI jobs and was deployed after graceful runtime shutdown
and consistent private backups. Migration 21 was verified on an existing synthetic
PA: all 15 indexes exist and earlier migration checksums are unchanged. The
original volume and history remain in place.

The configured real model identified the previously created report without the new
prompt supplying its filename or corrected count, then read the exact 20-byte CSV.
The old reply and new reply both survived browser reload; the one new command was
not resubmitted. The owned installation was disabled and the session revoked.
[Acceptance evidence](evidence/active-state-recovery-2026-09-08.json) records the
bounded scenario. This is one remembered report, not a general memory-quality claim.

Deployment preparation exposed a backup SQL UUID/domain mismatch, repaired before
any volume archive or new runtime startup. Two browser probe failures were harness
assumptions (volatile events omit sequence numbers; command IDs are not restricted
to UUIDv7); native events showed both read operations completed. The final probe
also verified reload and cleanup. These failed probes are not application failures
or additional evidence of general recovery coverage.

## Audit items and remaining acceptance

The user also explicitly requested native GPT-6 Astra support through ChatGPT
login on 2026-09-08. This includes a usable connection and reconnection flow,
account-scoped inference credentials, model/effort selection, and the existing
Sumi memory/tool loop running through the native backend. The development-only
Codex OAuth proxy is not acceptance of that request. Provider-native steering
and asynchronous tools require separate wire-level and real-backend evidence;
selecting the model does not establish those capabilities. Sumi remains the
agent harness, including its own history and tools.

Status here describes capability, not whether a historical issue happens to
be open. “Partial” is not closure. Precise source references remain in the
original audit and the current implementation; this table names the behavior
still needed to close each row.

| ID | Current assessment | Remaining acceptance |
| --- | --- | --- |
| M01 | L0→L1 runs; upper-layer replacement boundary settled, implementation pending | Follow [the user's chronological replacement boundary](memory-boundaries-2026-09-08.md) using the same parent context. L2-internal reintegration remains a distinct operation. |
| M02 | Full-parent fork and chronological replacement repaired; semantic quality partial | Evaluate meaning, uncertainty, corrections and unfinished details against originals; do not equate compression ratio with fidelity. |
| M03 | Ordinary silent trimming repaired; capacity boundary remains | Accumulated L1 must not eventually make every subsequent request unrecoverable. |
| M04 | Original history read/search and optional files available | Establish voluntary revision/strategic forgetting and recoverable source references without imposing an automatic memory-rewriting ritual. |
| M05 | Maintenance failure isolation implemented | Exercise timeout, later user input, and retry from the later full parent context. |
| A01 | Shared notification intents exist; PA ingress absent | Deliver an authorized shared event to the same PA without an open browser, with replay/dedup and optional response. |
| A02 | Receipt timestamp/delta implemented | Project authenticated speaker, place, source identity and occurrence time as distinct from receipt time. |
| A03 | Durable commands exist; undertaking continuity incomplete | An optional undertaking can retain request, correction, artifact, question and delivery outcome across interruptions. No compulsory task-record creation. |
| A04 | Reminder records exist; scheduled wake absent | Due/canceled/overdue markers cause the appropriate single admission across restart, without manufacturing a reply. |
| T01 | Tool progress/control exist; inference waits for completion | One real long-running operation returns a durable handle, allows other conversation and later result recovery without repeating the effect. |
| T02 | Corrected artifact delivered and downloaded through the actual shared app | Real model created/corrected CSV and sent it to the owned channel; the Human downloaded exactly 20 matching bytes. Prior review cancellation produced a truthful failure and was repaired in #376. Retain these cases as repeatable opt-in acceptance. |
| T03 | No production external connector | Use one authorized real external capability and continue unrelated conversation when it fails. Scope concrete credentials/resources before implementation. |
| T04 | Messaging images/text supported; intake/formats incomplete | Actual UI upload and content-based answer; unsupported content must be distinguished from content actually read. |
| T05 | Helper delegation absent | One bounded attributable helper job supports message, result, cancellation and reconnect without cloning the secretary's continuation. |
| T06 | Optional procedure files possible; extension lifecycle absent | Verify voluntary save/find/revise/reuse first; add discovery only for a demonstrated extension need. Never require reflection or skill use. |
| R01 | Some restart phases supported | Recover additional ordinary durable phases from their actual evidence, without guessing outcomes of emitted effects. |
| R02 | Transient health transport failures now retry | Implemented with real-socket regression coverage: transient transport failure retains the runtime, while identity/epoch/protocol failure remains fenced. Deployed in #375; shared fault-injection acceptance is not yet claimed. |
| R03 | Active work and input admission protect cold-idle lifetime | Implemented and independently reviewed; focused Go race tests cover active work, accepted input, completed idle work and admission/reaping. Deployed in #375. |
| R04 | Warm prevents idle stopping but does not restore | Explicit intended presence survives exit/reboot with bounded restoration and observable failure. |
| R05 | Active-state reconstruction deployed and verified on an existing PA in #377 | All three CI jobs passed; local suite 2,210 passed, 20 ignored. Native migration 21 added all 15 indexes with previous migration checksums intact. Existing conversation context identified the old report and read its corrected bytes; old/new replies survived reload. Large active suffixes remain proportional work. |
| P01 | No provider-native steering/async path | Verify an actual documented supported provider contract before adopting it; internal async/steering is not evidence of native support. |
| P02 | Model overrides partial; effort not wired | Explicit supported effort/budget reaches the provider; incompatible configuration is explained before sending. Model switching is a separate remaining path. |
| P03 | Harmless incoming reasoning metadata projected to canonical fields | Implemented; incremental response, canonical context serialization and next-request tests pass. Required structure and outgoing rules remain checked. Deployed in #375. |
| P04 | Provider Retry-After respected | Implemented; loopback HTTP and controlled-time tests cover delay, steering, cancellation and fallback. Delays over five minutes end automatic retry instead of resending early. Deployed in #375. |
| P05 | Per-response limits only | Account for an explicitly bounded undertaking/wake across restart, stop further admissions truthfully and support replenishment. |
| U01 | Exact-call review exists; standing permission UI absent | Grant, view, narrow and revoke understandable scopes; recheck queued actions and retain effect truth. |
| U02 | Cards render; shared action round trip absent | Human and PA act on the same authorized object/version with mobile/keyboard feedback and an attributable resulting event. |
| U03 | Questions/polls exist without answer wake | An addressable question and later answer resume the originating context while unrelated activity remains possible. |
| Q01 | Separate real-model evidence exists | Maintain a small reproducible opt-in set for correction, wait, interruption, actual artifacts and delivery; distinguish blocked/skipped/failed. |
| Q02 | Rust CI now active; relevance review continues | The Rust regression workflow passed its first GitHub Actions run on the supported Rust 1.88 toolchain. Local suite: 2,213 passed, 20 ignored (includes subprocess entry points and opt-in live-provider/performance cases). Continue reviewing relevance and explicit non-run reporting. |

## Comparative items are reconciled, not multiplied into machinery

C01–C04 concern memory and experience; their remaining work maps to M01–M05.
C05/C06 concern methods the person may choose: existing files are already useful
primitives, and no required self-improvement loop will be added to close them.
C07–C11 map to shared perception, continuity and scheduled return (A01–A04).
C12–C15 require actual waiting, recovery, delivery and answers (T01/T02, R01–R05,
U03). C16 requires manageable permission (U01). C17–C21 concern real artifacts,
content, shared objects, optional capabilities and bounded help (T02–T06/U02).
C22–C25 concern actual model capabilities, resources, observable state and
behavioral evidence (P01–P05/Q01/Q02), not a personality score.

## Closure criterion

The subsequent user-authorized milestone is
[Cloudflare Workers deployment](../roadmap.md). It follows this goal; it does
not replace the remaining foundation requirements with a hosting migration.

Every remaining row needs either an implemented, tested user-facing path or a
specific product decision supported by the user's direction; “not implemented”
cannot silently become “out of scope.” Design questions remain visible and are
brought to the user through Decision Inbox while independent work continues.

The end-to-end acceptance is the same secretary taking one real undertaking
through correction, a detour, a question, external waiting, long conversation,
interruption, a real artifact and delivery. It must also be possible to remain
quiet, decline a method, or change an optional practice. Closing a run, a PR or
this table does not by itself prove the undertaking complete.
