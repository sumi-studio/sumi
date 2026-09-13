# Concurrent work-item workflow

How multiple engineers — including concurrent model workers sharing one
GitHub account — hand off tasks through GitHub without losing track of
who owns what.

## Source of truth

- **Issues are the canonical record of an outcome and its assignment.**
  Nobody should have to read an agent's chat log to learn who owns a
  task or whether it finished.
- **PRs are the canonical record of implementation and verification.**
  Substantive review notes and validation evidence live on the PR.
- The Decision Inbox assists decisions and notices; it does not carry a
  second copy of task state.

## States

Work state is a `state:*` label on the issue:

| Label                | Meaning                                             |
| -------------------- | --------------------------------------------------- |
| `state:ready`        | A currently-valid outcome; safe to claim            |
| `state:in-progress`  | Claimed; the claim ledger names the owner           |
| `state:review`       | Implementation PR exists; awaiting acceptance        |
| `state:blocked`      | Stopped; a comment gives the reason and unblock condition |
| *(closed)*           | Done. Reopen is normal if the outcome regresses     |

States are iterative, not one-way gates. Review can return to
in-progress, in-progress can return to ready on reprioritization, and a
closed issue can be reopened — leave a comment with the reason and the
owner. `Ready` holds only outcomes that are currently valid for the
release; the triage labels (`ready-for-agent`, `needs-triage`, …) are
the *intake* axis and stay separate.

Reuse an existing issue when it already covers the outcome; link and
update rather than filing a duplicate. Old issues are context, not
automatic scope.

## Worker identity

All model workers run under one GitHub account, so `assignee` cannot
distinguish them. Identify yourself everywhere as
`model/harness/session`, e.g. `swe-2/devin-cli/alpha-core-20260913` or
`opus5/codex/<chat>`. Use that string as `--owner` in the ledger and put
it in the first comment of work you take.

## Claiming: `scripts/operations/task-ledger`

GitHub has no atomic claim primitive (label edits are not
compare-and-swap), so concurrent claimants on one host serialize through
a small flock(1)-guarded ledger:

```sh
export SUMI_TASK_LEDGER_DIR=/home/yohaku/sumi-local/task-ledger   # shared team ledger on this WSL
scripts/operations/task-ledger next --label state:ready           # open, unclaimed work (empty = stay quiet)
scripts/operations/task-ledger claim 123 --owner swe-2/devin-cli/xyz --worktree "$PWD"
scripts/operations/task-ledger renew 123 --owner swe-2/devin-cli/xyz
scripts/operations/task-ledger release 123 --owner swe-2/devin-cli/xyz --reason review --note "PR #456"
scripts/operations/task-ledger list                               # who holds what
```

Semantics:

- `claim` is atomic across concurrent invocations; a held issue exits 3
  and names the holder, lease expiry, and worktree.
- A lease expiring does **not** transfer the claim. `reclaim` refuses a
  live lease, requires `--evidence` describing what you checked, and the
  tool itself reports the recorded PID's liveness, whether the recorded
  worktree still exists, and any open PRs referencing the issue.
- `release --reason` maps to the state transitions above and keeps the
  history, so a released issue can be claimed again — the ledger-level
  form of reopening. Tracker label mapping: `ready` → `state:ready`,
  `review` → `state:review`, `blocked` → `state:blocked`,
  `abandoned` → `state:ready` (dropped work returns to the pool), and
  `done` → all `state:*` labels removed. Done means closed, and closing
  is the acceptor's call — the ledger never closes the issue itself; it
  prints the `gh issue close` suggestion for whoever verifies the work.
  Every sync removes the *other* `state:*` labels too, so an issue
  never carries two contradictory states.
- With `--gh-sync`, claim/release also swap the `state:*` labels via
  `gh` for callers with tracker write access. Without it, the tool
  prints the equivalent `gh` commands to stderr for whoever owns
  tracker mutation — the local transition still commits.
- The ledger is advisory about tracker existence: `claim` does not
  verify the issue exists or is open — it records worker intent.
- The ledger is per-host (the shared WSL). It is not a distributed
  system; if workers ever run on separate hosts, promote the ledger to a
  shared location or a compare-and-swap primitive first.

## Packet contents for a delegated issue

An issue that is `state:ready` should carry (template:
`.github/ISSUE_TEMPLATE/work-item.md`): outcome and use, basis and prior
decisions, owner and edit scope, dependencies, how to verify, related
PRs. Prescribing line-level implementation steps is not part of the
convention unless a specific constraint requires it.
