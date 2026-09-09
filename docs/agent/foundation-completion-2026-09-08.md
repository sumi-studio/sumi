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

Observed shared deployment after #389 (2026-09-09): API
`b6d271b4e177cdbf27e0064ed69fb0000e7905f3` (#386), runtime/provisioner
`bc49022a7bfef70b659a5e6c6478ae165698ffa1` (including #381/#383), and Web
`055780646c749508bb9a6f75ffce4daaff43b505` (#389). These are deliberately
recorded separately: the later Web-only updates did not replace the runtime.
This does not mean that all of #362
or native account sign-in acceptance is complete.
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

## Attention, activity log and native ChatGPT candidate

The initial #379/#380 cutover preserved existing histories, recorded five native pre-external-event
boundaries and converted 319 API event rows with all five heads matching their
native histories. Command files were unchanged. Two never-initialized volumes
remained empty; an unregistered historical database was backed up unchanged.

The first actual Attention probe accepted its Workspace invitation through the
real tool, closed the DirectChat socket, stopped its owned runtime and sent an
ordinary DM without a mention. The delivery row remained unacknowledged through
retries and the probe timed out without a real Messaging reply. Diagnosis found
an actual UUID v4 command durably appended at sequence 2, while the delivery
receipt column accepted only UUID v7. Thus a missing DB acknowledgement did not
mean that no command had been appended. The fixture delivery tests had used v7
receipts and missed this integration mismatch. Public execution events also
showed that sequence 2 was processed: the fixture's earlier blanket instruction
not to send messages caused review to block the later DM reply. The setup prompt
has therefore been scoped to invitation acceptance; this is a fixture correction,
not permission to bypass action review. This is a failed acceptance,
despite passing CI. Its isolated installations were disabled, session revoked and runtime
stopped. Reminder and cancellation scenarios were not reached. Local evidence:
`/tmp/sumi-attention-live-acceptance-41RNf9/evidence.json`.

The #382 candidate corrected the receipt column with migration 0038 and is
running with healthy API, web and provisioner services. A second fresh probe
recorded the UUID v4 receipt successfully and the PA opened the DM. It proposed
the requested reply, but review rejected it solely because the source participant
was not a designated Human approver. This is a product mismatch between an
ordinary conversational request and a grant of elevated authority. The probe
timed out, then disabled its owned installations, revoked its session and stopped
its runtime. Local evidence:
`/tmp/sumi-attention-live-acceptance-GJk5Fn/evidence.json`.

The next correction preserves authenticated Messaging source metadata separately
from message content and evaluates ordinary replies within the PA's existing
permissions. Participant speech must not grant employer rights or elevated
approval. At that point, actual reply, reminder and cancellation acceptance
remained open;
the later runs below record the results.

The activity-log component checks cover chronological interleaving, replay and
scroll position at desktop and mobile widths. They do not establish physical
iOS keyboard behavior. Native ChatGPT connection and Astra inference are
implemented, but actual Human authorization and an actual Astra response remain
unverified. The user's ChatGPT credentials are not borrowed from another harness.

Upper memory replacement was merged through #381 and is included in the current
shared deployment. Its staged semantic evidence is described below. The earlier
combined Rust suite passed 2,260 tests with 20 ignored; this does
not establish semantic fidelity or long-run cache behavior with a real model.

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
| M01 | L0→L1, L1→L2 and distinct L2-only reintegration are wired; staged real-provider evidence below | Stronger runtime-driven semantic acceptance must compare actual replacements and paginated original history against [the chronological boundary](memory-boundaries-2026-09-08.md). The prior staged run is not a natural long session. |
| M02 | Full-parent fork and chronological replacement repaired; semantic quality partial | Evaluate meaning, uncertainty, corrections and unfinished details against originals; do not equate compression ratio with fidelity. |
| M03 | Upper-layer capacity scheduling exists; ordinary silent trimming repaired | Verify recovery under repeated upper-layer pressure including a failed or non-shrinking attempt. Scheduling thresholds alone do not guarantee capacity or semantic retention. |
| M04 | Original history read/search and optional files available | Establish voluntary revision/strategic forgetting and recoverable source references without imposing an automatic memory-rewriting ritual. |
| M05 | Maintenance timeout and later-parent retry verified through actual driver/adapter/store | Local HTTP integration covers unchanged target/settings, backoff, ordinary response and completed-only promotion. Real-model semantics and full Session ingress remain outside this test. |
| A01 | Actual DM wake, ordinary reply and replay verified after request/approval repair | Broaden interruption and duplicate-delivery acceptance while retaining optional response and existing permissions. |
| A02 | Receipt timestamp/delta and source projection implemented | Verify authenticated speaker, place, source identity and occurrence time through actual shared event delivery. |
| A03 | Durable commands exist; undertaking continuity incomplete | An optional undertaking can retain request, correction, artifact, question and delivery outcome across interruptions. No compulsory task-record creation. |
| A04 | Due reminder survives Cold stop and wakes the same PA; cancelled marker suppressed | Actual run verified one due admission and zero cancelled admissions. Whole-host reboot and admission/DB-ack crash injection remain outside this scenario. |
| T01 | Durable workspace processes deployed through #398; same-PA cold-stop continuation, output read, report and reconnect/no-replay verified | Retain this opt-in real-model scenario. One synthetic process does not establish every failure or resource-limit case. |
| T02 | Corrected artifact delivered and downloaded through the actual shared app | Real model created/corrected CSV and sent it to the owned channel; the Human downloaded exactly 20 matching bytes. Prior review cancellation produced a truthful failure and was repaired in #376. Retain these cases as repeatable opt-in acceptance. |
| T03 | Reviewed public HTTPS document reading accepted in a real Kimi scenario (#399) | Authenticated connectors and full browser interaction remain separate; ordinary fetch-failure continuation passed. |
| T04 | Browser DM upload and content-based text/image replies accepted (#400) | Unsupported formats return explicit unread errors; broader formats and native Astra remain outside this acceptance. |
| T05 | Helper delegation absent | One bounded attributable helper job supports message, result, cancellation and reconnect without cloning the secretary's continuation. |
| T06 | Optional procedure files possible; extension lifecycle absent | Verify voluntary save/find/revise/reuse first; add discovery only for a demonstrated extension need. Never require reflection or skill use. |
| R01 | Some restart phases supported | Recover additional ordinary durable phases from their actual evidence, without guessing outcomes of emitted effects. |
| R02 | Transient health transport failures now retry | Implemented with real-socket regression coverage: transient transport failure retains the runtime, while identity/epoch/protocol failure remains fenced. Deployed in #375; shared fault-injection acceptance is not yet claimed. |
| R03 | Active work and input admission protect cold-idle lifetime | Implemented and independently reviewed; focused Go race tests cover active work, accepted input, completed idle work and admission/reaping. Deployed in #375. |
| R04 | Warm restoration deployed in #386; host service recovery configured in #387 | Runtime exit and API restart restore the same owned PA without browser demand; Cold remains stopped. Actual whole-WSL reboot remains unverified. See the receipt-linked acceptance below. |
| R05 | Active-state reconstruction deployed and verified on an existing PA in #377 | All three CI jobs passed; local suite 2,210 passed, 20 ignored. Native migration 21 added all 15 indexes with previous migration checksums intact. Existing conversation context identified the old report and read its corrected bytes; old/new replies survived reload. Large active suffixes remain proportional work. |
| P01 | No provider-native steering/async path | Verify an actual documented supported provider contract before adopting it; internal async/steering is not evidence of native support. |
| P02 | ChatGPT connection, Astra effort and idle switching implemented in #380 | Verify actual authorization, inference and switching; configured values and synthetic protocol tests alone do not establish live activation. |
| P03 | Harmless incoming reasoning metadata projected to canonical fields | Implemented; incremental response, canonical context serialization and next-request tests pass. Required structure and outgoing rules remain checked. Deployed in #375. |
| P04 | Provider Retry-After respected | Implemented; loopback HTTP and controlled-time tests cover delay, steering, cancellation and fallback. Delays over five minutes end automatic retry instead of resending early. Deployed in #375. |
| P05 | Per-response limits only | Account for an explicitly bounded undertaking/wake across restart, stop further admissions truthfully and support replenishment. |
| U01 | Exact-call review exists; standing permission UI absent | Grant, view, narrow and revoke understandable scopes; recheck queued actions and retain effect truth. |
| U02 | Cards render; shared action round trip absent | Human and PA act on the same authorized object/version with mobile/keyboard feedback and an attributable resulting event. |
| U03 | Actual text reply and poll-vote Attention continuations accepted | Same PA used the reply reference and selected poll option in a persisted Messaging response. Physical mobile interaction and native Astra remain unverified. |
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

### Latest observed acceptance — 2026-09-08

Candidate #382 at `0b88996a` passed its current Rust, API/Postgres and web CI.
The shared-environment run `wt1r55` produced a real PA-authored Messaging DM
reply with no DirectChat socket, and replay matched through event 46. A real
one-minute reply-later event also produced its requested Messaging reply.
The overall run failed at cancellation: the next message was admitted as
command 5, but the retained public event log ends at event 88, after the
preceding reminder's write and before its proposed resolve tool ran. This
is not yet evidence of a cancellation implementation failure; runtime
termination or lost progress still needs diagnosis. Fixture cleanup completed.
Evidence: `/tmp/sumi-attention-live-acceptance-wt1r55/evidence.json` and
`/tmp/sumi-attention-wt1r55-diagnosis.json`.

The staged upper-memory real-provider test passed all mechanical checks
(4 provider streams, including store close/reopen). The parent answered
October 19; the selected old L1 and reintegrated L2 retained provisional
October 12; after a later correction and reopen the parent answered October
21. All preserved the uncertain venue and original note location. This is
a synthetic staged scenario, not a natural long-running session or a
completed independent semantic acceptance. Normalized provider counters
reported cache reads of 71,168 and 34,304 on the two forks; these do not
prove longitudinal cache savings. Trace: `/tmp/sumi-upper-real-session-20260908/trace.json`.

The subsequent diagnostic run `kk9dbT` passed DM delivery, replay, an actual
one-minute reminder, cancellation through real tools, and suppression past the
cancelled deadline. All four images remained pinned; cleanup revoked the
fixture session, disabled its apps, and stopped only its owned runtime.
[Selected synthetic evidence](evidence/attention-delivery-2026-09-08.json)
records these results. The earlier `wt1r55` stall is still unexplained, so
this rerun does not establish that intermittent lost progress is fixed.

Independent review found the staged upper-memory replacements faithful to the
selected sources. It also found original-history verification incomplete:
the earlier trace recorded only the first recall page. The test now follows
pagination and checks actual original facts and separation from later history;
its focused offline validation passed (one scenario, no model calls). This does not retroactively
strengthen the previous real-model trace.

Correction after receipt-level review: `kk9dbT`'s runner status was PASSED,
but it did not require resolving the original due reminder. The final tool
sequence contains no such resolve receipt; only the separately created
cancellation marker was resolved. The selected artifact now labels this
PARTIAL_ACCEPTANCE. Delivery and separate cancellation are proven; completion
of the original reminder workflow is not. The runner now requires both its
write receipt and its matching resolve receipt before advancing. At that point,
no fresh
real run had validated that stronger condition; the following runs did.

Diagnostic-only shared update `ba3eeb75` passed all three CI jobs and routine
cutover checks, preserving existing mounts and the ChatGPT encryption key.
No history conversion or schema migration ran. Configuration and PostgreSQL
backups were taken; PA volumes were retained in place. The actual-model
run below requires the original reminder resolve receipt.

The strengthened run `zzT9jd` completed on `ba3eeb75`. Its original reminder
resolve call `messaging_8` started at event 93 and returned resolved=true for
the matching marker at event 94. The separate cancellation was suppressed
before its due time and remained unadmitted after the deadline. DM delivery,
replay, unchanged image bindings and fixture cleanup also passed.
[Resolution evidence](evidence/attention-resolution-2026-09-08.json) records
the matched receipts. The earlier partial artifact is retained as such;
the unexplained earlier stall is not claimed fixed by this passing scenario.

## Provider call identity repair

A scripted provider reusing `echo_0` on a later response caused the real Session
and sequential execution worker to reject the second durable operation and lose
ownership. This is a reproduced harness failure, not an observed live-provider
duplicate. New incoming calls now receive an internal ID scoped to their
assistant message. The original provider ID remains available for wire pairing,
including approvals, recovery and memory transformations. Opaque native items
are unchanged.

Both cross-command and same-command reuse regressions pass. Exact request-body
comparison passes for Chat Completions, Responses, Anthropic, native replay and
ChatGPT Responses. The combined Rust library suite passes 2,225 tests with 19
ignored opt-in/subprocess cases; Go and TypeScript contract checks also pass.
This repair is deployed through #383; current real-model acceptance follows.

## Current deployment acceptance — 2026-09-09 JST

PR #383 merged after all three exact-head CI checks and independent review.
The shared deployment now includes #381 and #383, retaining existing
configuration, encryption key, mounts and histories. No history reset or schema
migration was needed for this update.

The actual Kimi run `1hDq1o` completed with exit 0. The original due marker
was resolved at events 105–106, following its actual Messaging write; the
separate cancellation marker was resolved at 137–138 and remained unadmitted
past its deadline. DM delivery with the DirectChat socket closed, log replay,
pinned images and owned-fixture cleanup also passed.
[Selected evidence](evidence/attention-identity-2026-09-09.json) preserves those
receipts. This does not prove native ChatGPT/Astra activation, physical iOS
quality, natural long-run memory fidelity, or the cause of the earlier stall.

## Warm restoration and current Direct Chat acceptance — 2026-09-09

[Native acceptance report](https://github.com/sumi-studio/sumi/issues/362#issuecomment-5596637290)
records the owned PA's automatic restoration after runtime exit and API restart,
plus a 40.4-second Cold observation exceeding the 30-second restoration sweep.
The real model created/read a file, then identified it after restoration without
the continuation prompt supplying its name or marker, edited it, and read the
exact updated 28 bytes. Fresh replay matched all 57 persisted event sequences
and bodies. Volatile unsequenced events were excluded. This bounded scenario
is not evidence of general memory fidelity or whole-WSL restart recovery.

The host's Docker restart policy and tmpfiles configuration were applied and
verified against #387. Existing files, histories and configuration were retained.
The test installation was disabled, its session revoked and revocation verified,
and only its own Cold containers stopped.

Direct Chat #385 repaired the browser's provider-call identity parsing, operation
row updates and scroll restoration. #388 refined typography, tool rows and the
composer from installed Codex App implementation evidence. #389 added measured
height/opacity opening and closing using its 300ms easing; reduced-motion
preferences disable these transitions. All three CI jobs and independent review
passed before each merge. The integrated browser journey covers send, steer,
approval, abort, replay, keyboard disclosure activation and retained panel DOM.
Its stale fixture was repaired to supply the required output audience, without
relaxing the production parser.

The deployed #389 UI rendered the owned saved conversation at 390px, opened and
closed the result panel, reported 0.3s normal/0s reduced-motion transitions, and
had no horizontal overflow. No model command was sent. The owned session was
then revoked and its installation disabled again. Actual physical iOS behavior,
live Codex pixel matching and animation of content growth while already open
remain unverified or unimplemented; these are not claimed by the opening and
closing check. Web returned HTTP 200 and API `/health` returned `status: ok`.

## Overdue reminder probe and reviewer evidence defect

The first stopped-runtime reminder probe did not reach reminder creation. Its
explicitly requested invitation acceptance was blocked by execution review after
a successful invitation-list result. The reviewer claimed the operation was not
available and its ID conflicted with the preceding call. Native public events
show two distinct internal IDs and two distinct provider IDs. The parent registry
contains the operation; the reviewer receives a read-only inspection subset.
These observations identify missing/ambiguous review evidence, not an actual
call-ID collision. The PA's final description of an approval pending was also
inaccurate: the recorded outcome was block, with no approval request.

[Selected synthetic evidence](evidence/reviewer-capability-2026-09-09.json)
records the failed scenario and successful cleanup. The probe now captures
public tool-result messages, including pre-execution rejection, so a missing
execution-end event does not hide the reason. This does not establish overdue
reminder acceptance; that scenario must run after the evidence repair.

## Durable process continuation: observed, acceptance still open

PR #397 introduced independently running workspace processes and completion
Attention. On its shared deployment (`9b814576`), one owned synthetic PA started
a delayed file-producing process, answered an unrelated arithmetic question,
and was cold-stopped while the process kept running. The completion event then
woke the same PA, which read stdout and posted the expected digest to Messaging.
An independent read-only inspection of its workspace confirmed the exact artifact
bytes and one execution-counter entry.

The initial probe is still recorded as `FAILED`: it incorrectly classified a
single status inspection **after** completion had been received as polling for
completion. The predicate now checks for status/output execution starts before
receipt of that operation's completion event, including failed calls. This
correction does not establish the reconnect/no-replay phase, which was never
reached. The original evidence remains unchanged at
`/tmp/sumi-process-attention-lerj65x_/evidence.json`; the independent artifact
check is adjacent in `artifact-audit.json`.

The actual continuation also exposed repeated argument failures before a
successful stdout read. Provider-payload tests retain the required `stream`
field and its enum in the OpenCode Go, Responses and Anthropic requests.
The rejection content, however, only told the model to regenerate its arguments;
internal diagnostic details were not included in the Chat Completions tool
result text. Actionable rejection recovery and a fresh end-to-end acceptance
remain required. This run used the configured Kimi model, not native Astra.

### Successful rerun after #398

The fresh run on `661aa0ac` passed the complete scenario, including a cold stop
while the independent process was running, automatic completion delivery to the
same PA, successful stdout read before the exact Messaging report, and independent
artifact-byte/hash and single-execution checks. A hello-only reconnect sent no
command; command identities and the completion receipt stayed unchanged, and the
execution counter still contained one entry. The probe exited successfully after
disabling its owned installations, revoking its session and stopping the owned
generation. Stored history, artifact and completion receipt were retained.

[Allowlisted evidence](evidence/workspace-process-2026-09-09.json) records this
acceptance and its limits. Full local evidence is
`/tmp/sumi-process-attention-4vm5i_fd/evidence.json`. The preceding failed record
is unchanged. #398's 38 assembler tests, actual provider-schema checks, independent
review and all three CI jobs passed before merge; the runtime check used Kimi,
not native Astra.

The deployment helper initially checked for stopped PA containers before their
already-requested shutdowns had finished. It stopped before starting new services.
Native container state was checked and deployment resumed from the stopped
checkpoint, preserving all mounts and settings. The helper now waits a bounded
time for that completion; this was a deployment-script correction, not a reason
to weaken process isolation or remove data.

## Public document reading and ordinary fetch failure

PR #399 merged as `f1ad7e5106b1bcbfe50860323ee4dc44909c6c56`; all three CI
jobs and independent review passed. All four shared images now use the reviewed
source `c3fbabb3dc9a8f41a81388c990632331cb48ea0f`, whose tree matches the merge.
The cutover retained existing configuration, volumes and histories.

The owned real Kimi scenario fetched RFC 2606 through the normally reviewed
`public_url_read`, then answered from its 8,008-byte body. A separate `.invalid`
URL produced a normal `dns_failed` tool error; the PA stated that no content had
been read and answered a subsequent unrelated question. Hello-only reconnect
submitted zero commands, with the existing command set unchanged. Cleanup passed.

The first probe failed because it required the model to preserve `#section-2`;
the model fetched the exact same document without that fragment. Native events
confirmed the correct document and hash. The corrected, independently reviewed
probe accepts only that document with or without the fragment, while checking
that the result preserves the actual tool argument. The original failure remains
unchanged; a fresh run passed. This does not establish live fragment retention.

[Selected evidence](evidence/public-web-2026-09-09.json) records the source, hashes,
error and continuation. Full local evidence is
`/tmp/sumi-public-web-kjtebw1c/evidence.json`. This is one plaintext real-model
scenario, not browser rendering, authenticated reading, PDF support or native
Astra acceptance. HTML extraction and destination/redirect limits have local
tests; they were not exercised by this live document.

## Browser Messaging attachment intake

PR #400 merged as `5decea03673283ef0a730748afd2e432ae2825e8`; its reviewed
source `8fcf99456dd528246bc7d3d2b821f442bea90c15` is deployed across all four
shared images. All three CI jobs, 25 attachment tests, two configuration tests
and independent review passed. Existing volumes and settings were retained.

The same owned Human/PA pair used an ordinary DM. A real browser uploaded and
sent `note.txt`, then `picture.png`, through the existing composer. Each upload
and send occurred once. Attention delivered the authentic Human messages; the
PA opened each attachment and replied through Messaging. The text reply contained
the attachment-only number and quantity. The image reply identified its pixel-only
number, blue circle and upper-right position. Filenames, alternative text and
message prompts did not contain these answers. Attachment bytes/hashes, tool
results, persisted replies and command identities were checked. Histories remain;
owned installations were disabled, the session revoked and the Cold runtime stopped.

The first run failed in the probe because API epoch `"13"` and command epoch `13`
were compared as different types. Exact numeric comparison fixed that mismatch;
the original failed record remains unchanged. Normal recovery retained the earlier
message, and the subsequent text reply also answered that earlier attachment.

[Selected evidence](evidence/attachment-intake-2026-09-09.json) preserves both
current answers and their receipts. Full local evidence is
`/tmp/sumi-attachment-intake-074724bw/evidence.json`. This verifies one Kimi
text/image journey through desktop Chrome, not physical iOS, all image formats,
PDF extraction or native Astra. No live provider request was captured: the
actual adapter serialization regression and the live image-specific answer
are separate evidence.

## Reconciled reminder, reply and poll acceptance

The table above now reflects three earlier successful real-model runs whose
results had not yet replaced the stale pending assessments:

- [Reminder restart](evidence/reminder-restart-2026-09-09.json): the same PA
  restarted from generation 0 to 1 after its Cold runtime was stopped. The due
  marker admitted one command; the cancelled marker admitted none. This is
  admission evidence, not a claim about reminder-response quality or host reboot.
- [Reply continuation](evidence/reply-attention-2026-09-09.json): a Human answer
  retained its original-message reference through Attention, and the PA's
  subsequent Messaging reply confirmed the selected afternoon preference.
- [Poll continuation](evidence/poll-attention-2026-09-09.json): the persisted
  afternoon vote arrived as source metadata, with no fabricated Human speech,
  and the PA replied to the original poll with the correct choice.

All three runs completed their owned-fixture cleanup. These are historical
acceptances at their recorded revisions, not new reruns on #400. The earlier
failed probes remain failed.

## Maintenance timeout and later-parent retry

`memory_adapter_timeout_retries_from_later_actual_parent` exercises a real
Chat Completions adapter against a loopback server that stalls only the first
maintenance request. The actual response-header timeout releases the memory
job. A later authenticated turn is seeded, the parent still completes an
ordinary adapter request, and the existing 30-second backoff prevents an
immediate fork. The next retry uses the actual later parent snapshot, unchanged
model/settings and the same selected target. A held retry cannot promote; its
completed result can, while the later correction remains.

The focused test passed in 31.17 seconds. Production retry timing and policy
were unchanged. This is driver/adapter/store integration with synthetic
endpoint answers and a seeded later turn, not full Session command admission
or real-model semantic fidelity. Initial runs stopped on sandbox loopback
permission and a test-only wire-content parser assumption; neither established
a product failure.

## Interrupted undertaking: observed recovery gap

The integrated attachment → poll → CSV undertaking did not complete. The
first `write_file` emitted its start but no result; the file was absent on a
read-only volume check. Its original stall cause is not established.
Restoring the same PA recorded a truthful indeterminate result, then closed
the original command without another inference. That is a continuation gap,
not successful completion of the requested work.

A single subsequent Human progress question reached the same Messaging DM.
The PA checked the missing file and used a workspace process to create and
read back the 90-byte CSV. The process succeeded and its container was removed.
The diagnostic runner incorrectly rejected the resulting `process_completed`
input as an extra command and stopped the PA during its next response. There
was no delivered progress reply or attachment. This probe therefore does not
establish end-to-end acceptance, and its original failure record is retained.

PR #403 was merged as `57ced374` after independent review and successful Rust,
API/Postgres and Web CI. It resumes the original command after a recovered
tool turn, retaining indeterminate results instead of replaying old effects.
It has not yet been deployed to the shared environment.
Continuation after a partial new assistant message, and recovery with
unclassified controls mixed into the recovery steps, remain separate gaps.
Graceful RuntimeShutdown also currently shares the explicit Abort closure path;
its final Store writes need not appear in the disconnected API event tail.
The tail alone therefore does not prove that command 33 remains open in Store.

A subsequent read-only inspection of the stopped fixture's native Store
confirmed commands 32 and 33 are both applied/finished. Events 809–812 contain
MessageEnd, TurnEnd, AgentEnd and command disposition after the API tail at
808. The original workspace CSV exists with 90 bytes and SHA-256
`a5a91dcb315c5090fd9859e49ee73f843962ff29a966faf7bb7e633785d41dea`.
Evidence: `/tmp/sumi-original-store-inspection.json`. The next acceptance uses
an ordinary new correction in the same undertaking; it must not revive either
historical command. The shared deployment remains `8fcf9945` pending the
separate runtime-stop continuation repair and its acceptance.
