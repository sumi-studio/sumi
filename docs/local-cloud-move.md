# Bringing a Local secretary into a Sumi Cloud registration

This is the recoverable path that lets someone registering for Sumi Cloud
bring the secretary already running on their Local placement, before Cloud
mints a different one. It covers the Cloud transfer session
(`apps/api/internal/transfersession`) and the Local command
(`apps/api/cmd/local-move`, run through `deploy/local-host/sumi-local-move`).

What is implemented here is the transfer itself. The registration screen,
the authentication adapter, the account transaction that uses the carried
secretary, the server mount and the Direct Chat surface are separate
integrations (see "Integration contracts").

## What moves

Only core state moves: journal (including notes), inputs, turns, plans,
operations, approvals, reminders, outbox and memory chunks. Files in the
Local workspace, jobs, model connections and credentials, account and
workspace membership, and usage records do not. Every session view carries
`state_only: true` and the `not_included` list; the command prints it before
sealing. Credentials never enter the bundle, so the secretary cannot answer
in Cloud until a model connection is selected there.

## The flow

1. During registration a live, verified flow proves a credential (the
   Firebase UID). The browser asks Cloud for a session and shows the move
   URL:
   `https://<api>/api/secretary-transfer/sessions/<session_id>#grant=<grant>`.
   The grant is in the fragment, so it never reaches a request line or an
   access log.
2. On the Local host: `sumi-local-move start`, then paste the URL. The
   command seals the secretary for the Cloud placement named by the session
   (from then on it does not answer on Local and new messages are refused),
   uploads the sealed state, and waits.
3. Cloud stages the import (nothing runs yet). The person finishes
   registration; the account transaction claims the staged secretary for the
   proven credential and binds it to the new human (`provisioned`).
4. Cloud activates the staged secretary (`activated`); Cloud authority
   starts at that commit.
5. The command reads the activation proof from the session and completes the
   Local side (`transferred`).

`sumi-local-move status` shows Local authority, the Cloud session status and
its deadlines. `sumi-local-move resume` continues after any interruption.
Exit status 3 means "not finished yet; run resume later".

## Deadlines and cancelling

- A session admits a bundle for 1 hour (`admit_until`). A staged secretary
  waits 24 hours for its account (`claim_until`). After either deadline Cloud
  marks the session `expired` and deletes any staged copy.
- `sumi-local-move cancel` works until the account transaction provisions
  the session. If the secretary was sealed, the command sends the sealed
  bundle's persona id and transfer key so Cloud can record a tombstone even
  when the bundle never arrived; the secretary becomes active on Local again
  only with Cloud's retirement proof. After provisioning, cancel is refused
  and `resume` finishes the move.
- The grant keeps status access after every deadline, so a sealed Local
  source can always read the proof it earned. Upload admission is what
  expires.
- Nothing aborts because Cloud is unreachable or reports a different
  placement id: the secretary stays sealed and the command says so.

## Recovery, concretely

| What happened | What converges it |
| --- | --- |
| Upload connection cut, or the Local process stopped | `resume` re-seals idempotently (same cut) and uploads again |
| Upload committed but its response was lost | the next status read shows `staged`; a duplicate upload returns the current view |
| Cloud crashed between the import and the session update | the next status read, upload or sweep promotes the session |
| A cancel or expiry landed while an import was running | the import finishes, Cloud retires the late stage before answering; the sweep retires it after a crash |
| Account transaction committed, activation did not | the session stays `provisioned`; the next status read or sweep activates it |
| Two `sumi-local-move` processes | the second refuses (lock in `<state-home>/move`) |

`Service.Sweep` (or `Service.Run`) should run periodically on the Cloud
server so expiry and owed activation proceed without a Local poll.

## Local files

`<state-home>/move/state.json` (0600, directory 0700) records the session
URL, grant, persona id and destination placement, replaced atomically.
Authority is always re-read from the Local ledger and the Cloud session.
Finished moves are kept as `state-<session>.json` when a new one starts.

The wrapper reads `config.env` like `sumi-local` and passes the database URL
through the environment. It uses `SUMI_LOCAL_MOVE_BIN`, a
`sumi-local-move-bin` installed next to it, or builds from the source
checkout into `<state-home>/run`. `sumi-local install` does not install it
yet.

## Integration contracts

- **Registrant proof** — `transfersession.RegistrantProof`: return the
  credential a live, verified registration flow proved for this request,
  applying its browser binding; an error for unproven, expired, closed or
  consumed flows. Session creation additionally refuses a credential that
  already has an account.
- **Account transaction** — inside the transaction that has just verified a
  live flow awaiting account creation: `ClaimInTx(tx, sessionID, subject)`
  returns the carried persona id to use as the agent id; create the human and
  bind the claimed credential to it; then `ProvisionInTx(tx, claim, humanID)`
  binds the persona and records the activation obligation. After commit call
  `Service.Reconcile(sessionID)`.
- **Mount** — `transfersession.NewServer(svc, proof, publicBaseURL)`; mount
  its routes only with a real adapter, and run the sweep.
- **Model connection and Direct Chat** — the carried inputs are processed by
  a core host pointed at Cloud state; showing them and the replies in Direct
  Chat, and asking for a model connection after arrival, belong to those
  surfaces.
