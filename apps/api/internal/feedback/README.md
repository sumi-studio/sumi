# Feedback Inbox

People and personality agents use the same built-in `feedback` app.
The first destination is Sumi開発: product feedback, questions and follow-up
conversations. An author's source workspace confers no access to the thread,
and no source conversation is attached implicitly.

## Access and availability

Set `SUMI_FEEDBACK_RECIPIENTS` to comma-separated canonical participant keys,
for example `human:<UUIDv7>,personality_agent:<UUIDv7>`. These identities can
read and respond to all feedback threads. Every other participant can access
only threads they created. A PA's associated human does not inherit its access.
Recipients are deployment configuration, not workspace administrators.

Feedback needs no Workspace or personal installation. Bootstrap returns `scope:"builtin"`,
which describes app provision, not visibility of other participants' reports.
Authenticated callers must be real participants; all existing thread access rules
apply. Migration 0044 retires only removable-app catalog/installation metadata,
preserving conversations, attachments, read state and delivery records.

An unset, malformed or nonexistent recipient configuration reports
`available:false`. Creating feedback then fails with `unavailable`; existing
accessible conversations remain readable. Invalid Feedback configuration does
not stop the rest of Sumi. Do not show a successful delivery state for a failed
creation.

## Transport

Public browser routes authenticate the signed session cookie and verify Origin
on mutations. Session admission also covers the domain operation. PA operations
come through the PA-bound local-control authorization lease, with no caller-
supplied actor. Both call the same Store.

| Browser | PA POST action | Result |
| --- | --- | --- |
| GET `/feedback/bootstrap` | `feedback:bootstrap` | configuration, actor, builtin scope |
| GET `/feedback/threads?status=all&cursor=…` | `feedback:list` | thread previews and next cursor |
| POST `/feedback/threads` | `feedback:create` | created thread |
| GET `/feedback/threads/{id}?cursor=…` | `feedback:open` | thread, message/status events, older cursor |
| POST `/feedback/threads/{id}/messages` | `feedback:reply` | posted message |
| PATCH `/feedback/threads/{id}` | `feedback:status` | updated thread |
| PUT `/feedback/threads/{id}/read` | `feedback:read` | 204 |

PA routes have prefix `/local-control/v1/`; their JSON supplies `thread_id`
where the browser uses a path. List has optional `status` and `cursor`; open
has optional `cursor`. Create takes `title`, `body`, `request_id`; reply takes
`body`, `request_id`; status takes `status`, `revision`; read takes `revision`.
Request IDs must be UUIDv4. Other thread/event IDs are UUIDv7. Unknown JSON
fields and trailing data are rejected. Errors are `{ "error": "code" }`.

List returns at most 50 summaries newest first (`is_summary:true`). Body and
latest-message previews are at most 320 Unicode scalars; opening a thread
returns original content. Open returns its newest 50 events, ascending within
the page, and an older-page cursor. Messages and status activities each carry a
revision, so clients can interleave them. Initial body is revision 1; replies
and status changes increment it. Dates are UTC RFC3339.

Reads do not mark a conversation seen. Explicit read acknowledgements are
monotone and cannot exceed the existing thread revision. Resolved conversations
still accept replies and become unread for other readers. Status changes retain
who changed them and when; stale revisions produce `revision_conflict` (409).
Create/reply retries return the original committed response; reusing a nonce
for another target or content produces `request_conflict` (409).

## Reports from the original screen

Browser `POST /feedback/diagnostics` with `{}` returns a timestamped safe
runtime observation for the PA bound to the authenticated session: readiness,
generation (decimal string), readiness reason and observed run state when
available. It never starts the PA. Collection failure returns
`status:"unavailable"` so writing feedback remains possible. The browser can
include this response as `diagnostics.server_observation`; that submitted copy
is author-supplied evidence, not authenticated server provenance or authority.
Diagnostic JSON is bounded to 32 KiB, and accepts the browser's selected target,
up to 24 navigation/click/error observations and scroll position. No raw server
log contents are collected by this endpoint.

Browser `POST /feedback/attachments` takes a multipart `file` and returns
`{attachment:{id,name,mime_type,size,url}}`. PNG, JPEG, WebP, WebM and MP4 are
recognized from the bytes, with a 20 MiB limit per file. Create accepts optional
`attachment_ids` (up to five). Binding is atomic with thread creation, requires
the author's own unexpired uploads, and participates in request deduplication.
Full thread responses include attachment metadata. The attachment URL supports
range requests; only the staging author or, after send, a current thread viewer
can read the bytes. Messaging attachment ownership and
storage are separate.

Feedback attachment bytes use the database with a 1 GiB / 10,000 object cap for
this deployment. A participant may stage at most 100 MiB / 20 uploads. Unsent
uploads expire after 24 hours and are reclaimed on upload and by a minute
background sweep. Expiry never deletes attachments already bound to a thread.
Each server permits four combined uploads/downloads at a time. Further media
requests return `503 attachment_transfer_busy` before reading the body or blob;
callers can retry. Authentication is checked before upload body reads, and the caller identity is
checked again when the upload is stored.

## PA attention

Create, reply and status mutations also write an immutable notification snapshot
for addressed PAs, excluding the actor. The API drains this outbox independently
of browser connections. It rechecks participant existence and thread access before
admission, uses the existing durable gateway receipt for ambiguous retries,
and preserves pending failures for later attempts. A nonexistent or
no-longer-addressed PA does not receive new content. PA events use explicit
Feedback provenance and can be opened in the Feedback app; they do not pretend
to originate in a Messaging channel.

## Verification

`SUMI_TEST_DB_URL=… go test ./internal/feedback` uses isolated databases via
`testdb.Create`, applies migrations and drops those databases on cleanup.
It covers author/recipient isolation, browser and leased PA identity, CSRF and
revoked session admission, retries, resolved follow-ups, cursor pagination,
read bounds, built-in availability and durable attention reconciliation.
