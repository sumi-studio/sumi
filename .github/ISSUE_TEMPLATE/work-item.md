---
name: Work item
about: A delegated outcome for an engineer (human or model). See docs/agents/workflow.md.
title: ""
labels: []
assignees: []
---

## Outcome

What must be true when this is done, and one concrete way a user or the
system benefits. States (Ready / In progress / Review / Done / Blocked)
are labels, not sections — keep this part about the work itself.

## Basis and decisions

Why this work, which plans/ADRs/packets it comes from, and decisions
already taken that constrain the design. Separate facts from proposals.

## Owner and edit scope

Who runs it (model + session, e.g. `swe-2/devin-cli/<id>`), which
worktree/branch, and which directories or contracts they own. Name
what is explicitly out of scope.

## Dependencies

Other issues, PRs, or external facts this needs first. Leave empty if none.

## How to verify

Commands, checks, or observable behavior that demonstrate the outcome —
including what must be real (PG, workerd, browser) versus mocked.

## Related PRs

Linked when implementation starts.
