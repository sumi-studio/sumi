# Sumi roadmap

This records the user's current sequence, not an obligation to preserve the
existing implementation or historical architecture.

## 1. Complete the agent foundation work in Issue #362

Deliver and verify the capabilities tracked in
[the current acceptance ledger](agent/foundation-completion-2026-09-08.md).
The shared deployment of PRs #373/#374 is an intermediate milestone. Continue
through the remaining memory, Attention, recovery, tool, permission and
real-use acceptance work before declaring this goal complete.

## 2. Deploy Sumi using Cloudflare Workers

On 2026-09-08 the user requested adding Cloudflare Workers to the roadmap and
explicitly authorized proceeding through deployment after the current #362
goal, once the agent foundation is in suitable shape.

- Reconcile the resulting implementation with the then-current official
  Workers capabilities and operating limits. Determine the placement of the
  application, continuing agent execution, durable state, connections and
  isolated tools from that evidence; do not assume an existing container
  deployment can be moved unchanged.
- Implement the selected deployment shape and deploy it. A feasibility report
  or a demonstration endpoint alone does not finish this milestone.
- Verify the same everyday capabilities in the deployed environment: login,
  conversation and retained state, background work and recovery, tool effects,
  permission boundaries and useful delivery to people.
- Record operating cost/resource assumptions, observability and the actual
  recovery procedure. Evaluate any data-preservation requirements the user
  has introduced by that time rather than inheriting today's absence of
  backward-compatibility requirements indefinitely.

The choice of a particular Workers feature, supporting service, data store or
execution split remains open. This milestone does not mean moving Decision
Inbox again; it concerns Sumi's deployment.
