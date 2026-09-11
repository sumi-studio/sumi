# Client operations through the same Sumi

Status: architecture proposal for the next slice. The CLI and browser-operation
bridge described below are not implemented. This document does not establish a
release or live acceptance result.

## Current implementation and reusable paths

The current Feedback implementation slice lets the human report a problem in
place, capture or attach visual evidence, and submit the report with diagnostic
snapshots. Browser observations and server-collected state are separate evidence
sources. These are human-operated reporting capabilities; they do not give the
PA access to the user's screen or let it operate the browser. Validation and
deployment of this slice are tracked with its implementation.

Ordinary conversation already reaches the user's existing PA through the shared
web agent store (`apps/web/src/agent/store.ts`) and Direct Chat transport. The PA
already has a Feedback adapter (`apps/agent/src/tools/feedback.rs`) that opens,
creates and replies to reports through authenticated local control. Messaging
can upload images and return their bytes as model image content through
`open_attachment` (`apps/agent/src/tools/messaging.rs`). Direct Chat currently
accepts only empty attachment arrays.

Feedback access currently belongs to the author and configured development
recipients. The human does not inherit access to a report authored by their PA.
Consequently, asking Sumi to call `feedback create` is not yet a joint reporting
flow visible in the human's Inbox.

There is no agent-callable browser operation channel or `sumi` operations CLI.
Bash runs in an isolated executor with no network, cleared environment and
sanitized inherited descriptors. The runtime owns local-control credentials;
those credentials are not available to shell commands. A CLI needs a deliberate
connection through that boundary, not just a script around an HTTP request.

## Intended experience

The human opens Feedback without leaving the screen where the problem occurred
and talks with the same Sumi. Sumi can inspect explicitly shared screen context,
request a screenshot while sharing is active, ask a follow-up question, and
prepare a report containing its observations and the human's account. The human
can inspect and edit that draft before sending it through their Feedback app.

This uses the existing PA, conversation and memory. Feedback is context for the
ongoing conversation, not another persona or an independent agent task. An
unavailable browser, unsupported capture API or failed evidence upload must
leave text conversation and reporting usable.

## CLI first, with operation documentation

Expose a small `sumi` CLI through the existing bash tool. The initial proposed
commands are concrete operations, not a generic remote JavaScript executor:

| Proposed command | Result |
| --- | --- |
| `sumi client inspect` | Shared app context, capture availability and observation time |
| `sumi client screenshot --output evidence.png` | One image from the active sharing session, with capture metadata |
| `sumi feedback draft --title-file title.txt --body-file report.md` | An editable draft presented in the human's current Feedback mode |

Results should be bounded JSON with request IDs and explicit success or failure.
A screenshot command writes an image to the PA workspace and reports its
location, dimensions and capture time. Model image ingestion must consume the
actual bytes through the existing image-content machinery; returning a filename
or printing base64 as shell text is not evidence that Sumi saw the image.

Ship an operation reference with `sumi help` and a discoverable skill explaining
when and how to inspect, capture, read image evidence, prepare a draft and handle
ordinary failures. Include short runnable examples and each operation's inputs,
outputs and limits. This is the operations wiki the PA can consult as needed;
it does not prescribe a mandatory reporting workflow. No separate model-facing
tool is needed for every CLI subcommand.

The first CLI transport is a narrow executor-to-runtime broker. It forwards
only registered operations, associates each request with the authenticated PA
and originating shell invocation, and applies operation-level authorization.
Approval of an outer bash command must not silently authorize every nested app
mutation. Reuse the existing bound-operation descriptors and execution review
for the concrete inner operation. Keep runtime credentials and the general
local-control socket out of the shell. This broker is new implementation work;
existing executor RPC is a boundary to extend, not an already usable CLI API.

## Browser connection and capture

A browser session registers its active Feedback context with the API. The API
binds it to the authenticated human and their PA; model arguments cannot select
an arbitrary user. Where multiple tabs exist, use the explicitly active Feedback
session rather than guessing the target. Requests carry an operation ID, session
identity and deadline. Responses must match that request and cannot fulfill a
request after logout, session replacement or expiry.

`inspect` returns a defined app-context snapshot: current app route, viewport,
relevant visible state and sharing status. It does not dump the entire DOM,
storage, hidden form values or unrelated conversation. Its observations are
client evidence, distinct from server diagnostic snapshots. Expanding what can
be inspected should follow an actual debugging need.

The human starts screen sharing with a browser gesture and chooses the source.
While that stream remains active, a screenshot request can capture a frame
without reopening the picker. Closing Feedback or stopping sharing ends the
session and its tracks. A stopped stream returns an ordinary operation error;
it is never restarted silently. Browser capture requires transient user
activation and source selection, and its grant cannot be persisted across new
capture requests. See the [W3C Screen Capture specification](https://www.w3.org/TR/screen-capture/).

A one-shot capture button used for human reporting is therefore not by itself
an agent screenshot service. The bridge must deliberately manage an active
sharing stream, evidence transfer and cancellation. Recording can use the same
stream through [MediaRecorder](https://www.w3.org/TR/mediastream-recording/), but
recording upload does not establish model video understanding. Start agent
inspection acceptance with still images.

## Draft authorship and evidence

Keep the first joint reporting flow simple: Sumi prepares a draft, the browser
shows it to the human, and the existing human-authorized Feedback endpoint sends
it. The PA does not receive the human's credentials or silently become the human
author. Attach selected evidence through Feedback's own authorized evidence
storage; a private Messaging attachment link does not automatically grant
Feedback recipients access.

The draft records the human's description separately from Sumi's observations,
with capture timestamps and the selected diagnostic snapshots. No conversation
history is attached implicitly. Use a stable draft/request ID so a lost response
does not create repeated drafts or submissions. If direct PA submission with
shared human access becomes necessary, add explicit collaborator membership to
Feedback instead of treating PA ownership as human ownership.

## Staged acceptance

1. **Same conversation and draft:** talk from Feedback through the existing PA;
   a follow-up retains conversation context, Sumi prepares an editable draft,
   and submission appears in the human's Inbox with the human as author. Leaving
   Feedback does not create or reset a PA conversation.
2. **Client bridge:** inspect the selected authenticated tab through the CLI;
   verify wrong-session responses, logout, disconnect and timeout cannot deliver
   stale observations. Text reporting still works when the bridge is absent.
3. **Visual evidence:** start sharing from a real browser gesture, request an
   image through bash/CLI, and have the real model identify information present
   only in the image. Stop sharing and verify another request fails normally.
   Submit selected evidence and verify a development recipient can open it.
4. **Operational reliability:** lose a response during draft creation and report
   submission, retry with the same IDs, and verify one draft/report. Exercise
   unsupported capture and upload failures without losing the report text.

The next implementation decision is whether to ship the same-conversation draft
stage first or include the scoped CLI broker in that slice. The complete screen
inspection experience requires both the browser bridge and image ingestion;
adding operation names to a skill alone does not supply either capability.
