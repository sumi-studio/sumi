# Conversations and the secretary's life log

## User direction

On 2026-09-08, the user clarified that conversations, including one-to-one
conversations with a secretary, generally belong in Workspace Messaging DMs.
The surface currently called DirectChat is expected to become primarily a
life log. Its existing name does not define the intended product boundary.

The user further clarified that once Attention is connected, this surface will
mainly serve debugging, verification, and transparency. Prioritize natural
conversation through Messaging and an inspectable record of received inputs
and actions; this clarification is not a request for an elaborate new primary
life-log interface.

The secretary's experience of receiving a notification should therefore not
disappear from that log merely because the notification originated elsewhere.
The log should make the source, speaker, place, and time understandable instead
of presenting the experience as a Human message in an unrelated conversation.

## Correction to the staged implementation

The staged Attention implementation treated DirectChat as an exclusive
Human–secretary conversation and filtered all `secretary` audience events out
of the browser stream. That product premise was an assumption by the
implementing orchestrator, not a user requirement. The user rejected it before
the Attention changes were deployed.

Do not treat that filter, its cursor tests, or immutable run-audience routing as
evidence that the product should hide these experiences. Reconsider the model
at the integration boundary, including whether it unnecessarily delays input
from another place until a run ends.

## Distinctions to preserve

- An experience has a source: who acted, where, and when. A Messaging DM is a
  conversation in its own right, not an inferior route to DirectChat.
- An outgoing message has a destination. Recording an experience in the life
  log does not post a reply into the originating Messaging conversation.
- A sender is not automatically an approver. Source attribution must not grant
  the sender authority over the secretary's tools or another person's data.
- A life log still has authorized viewers. This clarification does not itself
  define new access rights across people or expose credentials or private model
  reasoning.

## Integration still to establish

The application must connect ordinary Messaging DM input, explicit shared-place
mentions, and reminders to the same continuing secretary, and represent those
experiences with their actual origins. Review the existing DM notification
rules before choosing delivery eligibility; do not require an explicit mention
in a normal DM merely because the first Attention slice used mentions.

The API projection and browser presentation need a coherent source-aware
representation. Simply removing the filter while leaving all incoming messages
styled as messages from the Human would preserve the original misunderstanding.
Actual replies should use the destination's normal Messaging path.

This document records product direction and a correction. It does not claim
that the revised integration or UI has been implemented or accepted.

## Requested presentation and interaction quality

The user subsequently requested improving the current screen, with ChatGPT Web
as a reference for readability, neutral appearance, and usability, and Codex
desktop as a reference for information architecture and interaction. Preserve
the current colors. Thought and tool presentation are important in their own
right, not secondary details to leave behind a generic completion label.

The user explicitly rejected collecting tools separately from prose because it
destroys the visible chronology. Preserve the actual order of prose blocks,
available display summaries, operations, and outcomes. A run-wide overview may
aid navigation but must not replace the chronological account. Detail expansion
must retain the surrounding context. Do not fabricate model reasoning, times,
success, or a delivery destination not established by actual events.

The user also reported lost scroll position and forced jumps to the top.
Acceptance must exercise replay, lazy rendering, streaming updates, viewport
changes, and detail expansion in a browser. Reading older content should retain
the reader's position; following new output is appropriate while already at
the end. A successful text replay test alone does not establish usable layout
or scrolling.
