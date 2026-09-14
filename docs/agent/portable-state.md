# Portable secretary state

A secretary that began on one placement (for example a Local Sumi) can move to
another (for example Sumi Cloud) and continue as the same individual: the same
persona id, journal, memory notes, recorded decisions, reminders and unfinished
work, with exactly one placement able to run it at any time. This is the
M19 foundation for the Local→Cloud journey in the engineering plan (§4.2, §5).
It is portability for state the new architecture creates, not an importer for
the old Rust/SQLite data.

Code: `apps/api/internal/portable` (state service), migration
`0049_core_transfer`. Proof: `go test ./internal/portable/` (two real
PostgreSQL databases) and `apps/core/scripts/e2e-portable.mjs` (two state
services, two databases, the real Node core).

## Transfer steps

All routes are under `/internal/core` and require the state service's
admin/service secret. A persona token cannot seal, export, import or activate.

| Step | Where | Route | Result |
|---|---|---|---|
| Placement | either | `GET /placement` | This placement's durable id. A seal addresses its bundle to exactly one. |
| Seal | source | `POST /personas/{p}/transfers/{t}/seal` `{destination_id}` | `active → sealed`. The writer generation is bumped past every holder and the lease is parked, so the running core is fenced at its next state call. New inputs get `409`; replays of accepted inputs still answer. The receipt lists the cut, the destination and what continues. Replaying with a different `destination_id` is a `409`; retargeting means a new transfer id. |
| Export | source | `GET /personas/{p}/transfers/{t}/bundle` | NDJSON stream of the cut. Byte-identical on repeat. The header names the destination and carries the transfer key. |
| Import | destination | `POST /transfers/import?human_id=` | `staged`. One transaction: rows, trailer digest and counts, cut positions, reference checks, lease epoch floor, ledger. Any failure leaves nothing. A bundle addressed to another placement is refused (`422`), so an ordinary retry can never stage two copies. A retired transfer is refused forever. Same transfer, content and `human_id` again → `200` with the recorded receipt; a different `human_id` is a `409`. |
| Activate | destination | `POST /personas/{p}/transfers/{t}/activate` | `staged → active`, and mints the `activate_proof`. Replay returns the same proof. After a retire, `409`. |
| Complete | source | `POST /personas/{p}/transfers/{t}/complete` `{activate_proof}` | `sealed → transferred`, only with the destination's `activate_proof`. A missing proof is `400`; a value the destination never produced is `409`, and the source stays sealed. Source rows stay as history. |
| Retire | destination | `POST /personas/{p}/transfers/{t}/retire` `{destination_id, transfer_key?}` | Commits "this transfer never runs here": deletes a staged copy, or writes a tombstone when the bundle never arrived (the `transfer_key` from the bundle header is required then). `destination_id` must name this placement — a retire dispatched to the wrong service is refused. Mints the `retire_proof`. Replay returns the same proof; a bare tombstone (nothing ever imported) may be *corrected* by a re-retire naming the real `persona_id`/`transfer_key`, which re-mints the proof but never lifts the foreclosure. An activated transfer cannot retire (`409`). Retire on the transfer's own source is refused. |
| Abort | source | `POST /personas/{p}/transfers/{t}/abort` `{retire_proof}` | `sealed → active` with the destination's `retire_proof`; the next writer gets a newer generation. Without proof the source stays sealed. There is no force path — `{force:true}` is rejected `400`. |
| Status | either | `GET /transfers/{export\|import}/{t}` | The ledger answer after a lost response, including whichever proof the destination committed (`activate_proof` or `retire_proof`). |

`authority` is checked inside the same transactions as the writer generation
(`requireGeneration`, `AcquireWriter`, `SubmitInput`), so it is a database fence,
not a label. A staged or transferred persona admits no mutation even from a
caller that presents a matching generation.

The destination's first writer acquires `generation_high_water + 1` and runs
ordinary recovery: carried running turns are interrupted, their inputs are
requeued, and the recorded plan continues. Operations already done before the
move are answered from the ledger instead of running again.

**One continuing secretary.** Source authority can only end on destination
evidence, so no lost response, retry or partition creates two writers:

- Activation and retirement serialize on the destination's transfer ledger
  row: exactly one commits; the loser gets `409` and can read the winner from
  Status.
- `Complete` requires the `activate_proof` the destination minted when it
  activated. `Abort` requires the `retire_proof` it minted when it retired.
  Both are HMAC-SHA256 of `action:transfer_id:persona_id:destination_id`
  under the transfer key that travels in the bundle header, computed over the
  producing service's own placement id — a value the destination only
  publishes after it commits, so it works as commit evidence. A proof minted
  by the wrong placement names that placement and fails the source's check
  against the recorded destination. It is not an identity proof: anyone
  holding the bundle already holds the whole life and could mint any proof.
  Deliberate forgery is out of scope; the gates exist so honest calls cannot
  end authority by accident.
- While the destination is unreachable the source stays `sealed`: parked,
  visible, singular. When it answers again, Status returns whichever proof
  committed — `activate_proof` → `complete`, `retire_proof` → `abort`.
- A destination that is gone *permanently* cannot produce a retire proof, so
  the source stays sealed indefinitely. That is the honest availability
  tradeoff this contract takes today: recovering from a truly lost placement
  means deciding who may declare it dead, which has identity consequences —
  it is a separate product decision, not a request flag on `abort`.

## Bundle format v1

```
{"record":"header","format":"sumi.portable-secretary","format_version":1,"transfer_id":…,"persona_id":…,"destination_id":…,"transfer_key":…,"sealed_at":…,"sections":[{"name":"core","contract":"core.v1"}],"cut":{"generation_high_water":…,"latest_event_seq":…,"latest_outbox_seq":…},"secrets":"none","not_included":[…]}
{"record":"row","section":"core","table":"core_personas","data":{…}}
{"record":"row","section":"core","table":"core_inputs","data":{…}}
…
{"record":"trailer","rows":{"core_events":…,…},"content_sha256":"<sha256 of every byte before this line>"}
```

- `core.v1` carries `core_personas` (id, display name, birth time), `core_inputs`,
  `core_turns` (including `commit_request`), `core_turn_plans`, `core_events`,
  `core_operations`, `core_schedules`, `core_outbox`, with exactly the columns in
  `contract.go`. Unknown or missing columns are refused on both sides.
- `destination_id` is the destination's placement id (`GET /placement`); an
  import anywhere else is refused. `transfer_key` is a per-transfer random key
  the source mints at the seal; the destination stores it at import and uses it
  to mint the proofs. It authorizes evidence for this one transfer only.
- Not carried: the writer lease (only its generation, as the epoch floor),
  placement authority, the human binding — the destination binds the
  persona to its own authenticated human — and `core_jobs`. A job is
  runner-owned execution bound to the placement that queued it; carrying a
  claim could run the same work twice, so job rows stay behind. A job's
  terminal notification is an ordinary `job:<job_id>` input and does cross
  in `core_inputs` — the result still reaches the moved secretary.
- A reader refuses a format version, section or contract it does not implement.
  There is no compatibility layer; version 1 is the only version.
- The digest detects truncation and corruption. It is not authentication: the
  authenticated service channel is what makes the bundle trustworthy.
- `TestEveryPersonaTableHasAPortabilityDecision` fails when any table that
  references `core_personas` is neither carried nor declared placement-local.

## Refusals that protect the secretary

- **Unresolved effects.** Seal refuses while any operation is `running`: its
  external result is unknown, and moving it could repeat or lose the effect.
  The same rule covers non-terminal jobs (`queued`, `running`,
  `cancel_requested`): job submission takes a share lock on the persona row
  and requires `authority = 'active'`, so a submit either lands inside the
  cut — and the seal then refuses — or is refused on the sealed persona.
  A job can never slip between the check and the commit, and a claim never
  starts work for a non-active persona.
- **Second copy.** Import refuses a persona id already present in the
  destination, whatever its authority. A transfer never overwrites a secretary
  and never creates a duplicate of one that is already there.
- **Broken references.** Seal and import check references foreign keys do not
  enforce: input↔turn, event/operation↔turn, operation↔recorded plan position,
  running turn↔claimed input, wake input↔schedule, outbox↔turn/input,
  contiguous journal and outbox sequences, and no generation at or above the
  cut epoch.

## Secrets

Version 1 writes no credential, token or key into a bundle (`"secrets":"none"`).
Core state currently holds none. The bundle still contains private life content
(messages, notes, decisions), so it must only travel over the authenticated
service channel. Encrypting a bundle at rest (a downloadable file) is not
implemented and must be decided before any such file exists.

## Extension contract for other state

These kinds of state are listed in `not_included` and are **not** covered by a
transfer today. Each needs a section, owned by the module that defines it,
before a transfer may claim to preserve it:

| Section | Owner | What the section must provide |
|---|---|---|
| `files` | fabric-cloud (M11) | Scope↔persona/workspace binding; manifest of path, version and content SHA-256; bytes transferred separately and verified against the manifest before staging completes; version floor so destination CAS versions never go backwards; relative paths and links contained in the declared root. **At the seal, executor writes must actually stop** (unmount or stop the executor): CAS fences API writers only, and a direct POSIX write after the cut would be lost. Large content may pre-copy before the seal and send only the final delta at the cut. |
| `jobs` | jobs-results (M09) | Today `core_jobs` is placement-local and the seal refuses while any job is non-terminal, so in-flight work can neither be lost nor duplicated; a finished job's `job:<job_id>` notification input does travel with the cut. The section still owed: carrying terminal job *records* for history, and a path that lets a move proceed with in-flight jobs — drained or recorded source-bound with result reconciliation — rather than blocking. Completion authority after the move would belong to the destination; a late source result is reconciled, not executed again. |
| `memory_projection` | M06 | Encrypted originals may be re-encrypted for the destination. Search projections may be rebuilt there instead of carried. The journal and notes already travel in `core`. |
| `approvals` | M08 | Pending human approvals carried as evidence, re-validated at the destination against the same operation, target and current permissions before use. |
| `connections` | M08 / D9 | Connection metadata and provider context references only. Secrets never travel in a bundle; the receipt lists each connection needing reauthorization. |
| `account_and_workspace` | koseki / workspace (M21) | Not imported. The destination's authenticated account decides human, employer and membership; local roles never become Cloud permissions. |
| `usage` | M14 | Records, if carried, must not change the destination contract or payer. |

Every section must: scope rows to the persona, use a deterministic order, define
exact columns, add its reference checks to seal and import, make its own writers
honor `authority` (staging executes nothing), and add its tables to the coverage
test.

## Not yet covered

- The account flow (M21): binding a transfer to the consenting Cloud account,
  signup offering "bring my local secretary", and the D10 choices for a
  destination that already has a secretary. The import route currently trusts
  the service secret and a caller-supplied `human_id`.
- Delivery: the transfer does not redirect ingress. Inputs sent to a sealed or
  transferred source get `409` and stay with the sender.
- Incremental pre-copy for large lives; the whole cut is one import transaction.
- Returning a transferred persona to its origin (Cloud→Local exit). The format
  is symmetric, but importing into a placement that still holds the tombstoned
  row is refused.
- Real model continuity quality: the e2e uses the mock provider.
