# Bringing a Cloud secretary back to Sumi Local

This is the recoverable path that lets a signed-in owner move their Cloud
secretary home — to the same Local install that surrendered it, or to a
fresh Local target — without minting a second secretary or losing the
identity and state of the one that already exists. It covers the Cloud
return session (`apps/api/internal/returnsession`), the Local command
(`apps/api/cmd/local-move`, run through
`deploy/local-host/sumi-local-move`), and the owner-facing Cloud surface
(`apps/web/src/components/secretary-return.tsx`).

It is the reverse of the registration move (`docs/local-cloud-move.md`):
here Cloud is the **source**, so the session seals it and serves the
bundle instead of staging an upload.

## What moves

The portable bundle carries core state: journal (including notes), inputs,
turns, plans, operations, approvals, reminders, outbox and memory chunks.
The owner also explicitly selects the working file store; nothing is preselected:

- **`local`**: copy an immutable capture of the Cloud workspace into Local's
  working directory. The Cloud copy remains read-only; this is not ongoing
  synchronization or a backup service.
- **`cloud`**: keep Cloud as the working store. Local receives a scoped file
  credential, and the person and secretary use that same Cloud workspace.
  File operations require connectivity. The Local PTY is unavailable because
  that Cloud store is not mounted as a Local directory.

Before sealing, the return waits for admitted file effects and terminal writers
to reach the required durable cut. A pending terminal or uncertain writer is a
reason to finish/reconcile it, never permission to copy a changing tree. For
`local`, immutable-capture capability must be configured on filesvc; an
unavailable capture cannot fall back to walking the live workspace.

Jobs, model connections and credentials, account and workspace membership, and
usage records do not move in the core bundle. The Cloud-file credential above
is issued separately for that file mode. A persona with unresolved model
selection arrives with `needs_rebinding`; see "Model connection" below.

The supported packaged receiver is Linux/amd64, with Node and PostgreSQL as
specified in [Local host](local-host.md). Current packs include the prebuilt
`local-move` binary and install a `sumi-local-move` command beside `sumi-local`;
no source checkout or Go compiler is needed. This is not a native Mac receiver.

## The flow

1. In Cloud settings the owner chooses **秘書をローカルに戻す** (bring the
   secretary back to Local). The app asks Cloud for a session through the
   owner's live browser session — never with ids in the request body — and
   records the selected file mode and shows the return URL:
   `https://<api>/api/secretary-return/sessions/<session_id>#grant=<grant>`.
   The grant is in the fragment, so it never reaches a request line or an
   access log; Cloud stores only its hash. The app keeps the URL in the
   tab's session storage (`sessionStorage`) so a lost create answer can
   be recovered while the tab lives, and offers a copy button.
2. On the Local host where the secretary should live:
   `sumi-local-move return`, then paste the URL. The command checks its
   own persona slot first:

   | Slot holds | Meaning | What happens |
   | --- | --- | --- |
   | nothing (`absent`) | fresh target | ordinary staged import |
   | this secretary's surrendered copy (`surrendered`) | the install that sent it away | reclaim via `ImportReturning` over the recorded lineage |
   | any other secretary | occupied | refused before any request reaches Cloud |

   It then tells Cloud which placement and slot it is (the destination
   binding) — one return URL serves one Local placement; a second install
   pasting it is refused — and only then does Cloud seal the secretary.
   From that commit Cloud stops answering for it.
3. The command downloads the sealed bundle, stages the import, activates
   it locally, and reports the activation proof. Cloud completes the
   source transfer: Cloud authority ends, Local authority stands.
4. On a fresh target the install's `SUMI_PERSONA_ID` is retargeted at the
   returned secretary — only after activation committed on both sides,
   and only if the config file still names the slot the return was
   checked against. An operator edit in between is refused, not
   overwritten; `return-status`/`return-resume` finish the retarget later.

`sumi-local-move return-status` shows the local import ledger, the
recorded outcome and, when Cloud is reachable, the session status and
preflight. `sumi-local-move return-resume` continues after any
interruption. Exit status 3 means "not finished yet; resume later".

## Deadlines and cancelling

- A session admits a destination binding for 1 hour (`admit_until`).
  The deadline bounds the binding — once a destination is bound, admission
  is durable and the session cannot expire even while the seal is still in
  flight. Expiry is never treated as evidence that the destination did not
  activate, and a sealed session keeps serving proofs after it.
- `sumi-local-move return-cancel` works until activation commits on the
  Local side. If the bundle never landed, the command writes the
  tombstone and reports the retirement proof; Cloud unseals only on that
  proof. A **staged** copy is retired the same way. An **active** one is
  refused — it is the live secretary now.
- Cancelling on the same surrendered install removes the incoming carried
  rows and leaves the slot inert under the forward transfer's hold with
  its placement-local history (jobs, usage facts, reservations) intact;
  it does not restore the old conversation snapshot. Cloud becomes active
  again only through the retirement proof — a committed activation always
  wins over a cancel in flight.
- The owner can also cancel from the browser. Before the destination
  binds, it closes the session outright (`cancelled` — nothing moved).
  Once the binding has committed it marks `cancelling`: intent, not
  authority. The bind's seal then runs under the session row lock — the
  cancel either lands first (the seal is refused: the gate only seals
  while the session is `awaiting`/`sealed`) or waits behind the seal and
  sees it committed. With no export committed the next read closes the
  session `cancelled` outright — no retire proof is needed because the
  destination provably holds nothing; with a committed seal the
  destination's retire proof resolves it to `aborted`. Either way a
  racing seal can never strand the secretary under a dead session.
- The grant keeps status access after every deadline and terminal status:
  a sealed source must always be able to serve the proof the destination
  earned.
- Nothing aborts because Cloud is unreachable: the secretary stays
  sealed, the command says so and exits 3. A destination lost **after**
  activation is an explicit recovery limitation — recovery there needs
  its own authority proof and is not implemented.

## Model connection

The carried model intent is enforced, not erased. If the secretary
arrived on Cloud with an unresolved selection, the returned copy is
`needs_rebinding`: it cannot answer until a connection is chosen. A
packaged Local has no human-bound connection selection, so the operator
chooses explicitly:

```
sumi-local-move return-resume --use-config-model
```

`--use-config-model` clears the carried intent through the authorized
path so the install's configured provider is used. Nothing silently
routes the secretary to a different model — without the flag the command
stops and explains.

## Recovery, concretely

| What happened | What converges it |
| --- | --- |
| Return URL pasted into a second Local | the second `return` is refused before Cloud seals; the first keeps the session |
| Local occupied by a different live secretary | refused before any request reaches Cloud; nothing binds. An inert surrendered shell of another secretary is authored history — preserved, not disqualifying |
| Cancel or the deadline lands while the bind's seal is in flight | the session goes `cancelling`, never terminal; the row lock serializes the seal — if it committed, the retire proof resolves to `aborted`; if it never did, the session closes `cancelled` (nothing ever moved) |
| A tool approval is still pending when the return runs | `preflight.pending_approvals` shows it; the imported persona is unbound on a fresh Local so activation refuses until a human decides it — the command says to decide it on Cloud then `return-resume`, or `return-cancel` and return again once decided |
| A new return after one already settled | the finished record is archived to `state-<session>.json` and the new return proceeds |
| Download/import interrupted | `return-resume` re-reads the sealed session and imports again |
| Activation committed, report lost | `return-resume` re-posts the proof; the source completes idempotently |
| Report committed, local record lost | `return-status`/`return-resume` reads the import ledger and the session |
| Cancel raced an in-flight activation | the committed activation wins; the source completes |
| Cloud sealed, then crashed before the session update | the next status read or the sweep promotes from the export ledger |
| `config.env` changed between check and retarget | refused; the secretary stays active and the retarget waits |
| The create answer was lost | the app replays `POST /sessions` (409) and recovers the open session's view |
| Two `sumi-local-move` processes | the second refuses (one lock in `<state-home>` covers move and return) |

`Service.Run`/`Service.Sweep` on the Cloud server expires open admissions
and reconciles sessions from the portable ledger so a lost answer does
not strand one.

## Local files

`<state-home>/return/state.json` (0600, directory 0700) records the
session URL, grant, persona id, destination binding and the staged
config-retarget intent, replaced atomically. Authority is always re-read
from the Local import ledger and the Cloud session, never from this file
alone. The grant lives only here and in the pasted URL — never in argv,
logs or Cloud rows (Cloud stores its hash).

## Server gate

`cmd/server` mounts the return routes and starts the sweep only when
`SUMI_TRANSFER_PUBLIC_BASE_URL` is set (the same gate as the registration
move). Unset means the feature does not exist: no routes, no sweep, and
the settings surface hides it when the routes answer 404.

Mounted is not admitted. `SUMI_RETURN_FILE_MODES` selects which implemented
file modes new sessions may choose: `local,cloud`, or either mode alone. Unset
keeps `FilePolicyUndecided`: owner create and destination binding refuse new
moves with `409 file_policy_undecided`. Invalid nonempty values fail server
startup. The public base URL alone never admits a move.

Enable only the modes whose API/provisioner/filesvc/Local receiver have passed
integrated acceptance. For `local`, filesvc additionally requires the
`FILESV_CAPTURE_META_URL` and matching object backend configuration described in
`apps/files/cmd/filesvc/main.go`; incomplete configuration fails startup and
absent configuration leaves capture unavailable. The mode gate does not create
that capability. The user's file choice remains per session.

Status, cancellation, download and proof reports remain available when new-move
admission is disabled. Keep `SUMI_TRANSFER_PUBLIC_BASE_URL` configured during
recovery; unsetting it removes those routes too.

## Migration

`0060_return_sessions` adds the `return_sessions` table — bookkeeping
around the portable ledger (`core_transfers`), not a second copy of it.
`0063_return_file_modes` adds the selected file mode and storage-epoch binding.
Terminal coordination also requires `0062_terminal_sessions`. filesvc maintains
its capture/barrier tables in its own database at startup.
Receipts, proofs and persona authority stay in the portable contract;
`transfer_id = session_id::text`. The down migration drops the table;
running it while a session is open abandons that session's bookkeeping
(the portable transfer itself is unaffected and still resolves through
its proofs).

## Integration contracts

- **Owner proof** — `returnsession.OwnerProof`: return the human and the
  secretary a live, verified browser session proves for this request;
  identity never comes from the request body. `cmd/server` adapts
  `BrowserSessionCookie` + `HMACUserSessionVerifier` into it.
- **CSRF/origin** — mutating owner routes run inside the same
  origin-and-CSRF wrapper as every browser mutation.
- **Mount** — `returnsession.NewServer(svc, proof, publicBaseURL)`;
  mount only with a real adapter and run the sweep. The grant is not the
  core state administrator token and must never be accepted as one.
- **Files policy** — the owner chooses `local` or `cloud` from the modes the
  deployment enables. Session binding, preflight and client copy retain that
  choice. The portable state contract still carries core state; capture/import
  and the Cloud-file credential are the file-mode-specific paths.
