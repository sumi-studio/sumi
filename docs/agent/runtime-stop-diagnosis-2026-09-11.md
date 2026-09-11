# Agent stops and message delivery — 2026-09-11

## Observed incident and limits

The dogfood PA was running again when inspected, with no OOM flag and no
container restart. The previous container had been removed and its stop event
was no longer available. The earlier API discarded the process monitor's exit
reason. We cannot establish why that particular runtime stopped.

The deployed idle timeout was four hours, not the source default of five
minutes. Idle reclamation requires a cold PA, an expired inactivity timer, and
no in-flight run, pending approval, or unacknowledged command. Messaging
Attention admission refreshes activity as well as Direct Chat. A process
running a long task is protected while the task runs; completion does not
itself reset the inactivity timer.

## Confirmed defects

- Channel messages selected by the existing `all` or keyword notification
  setting were discarded before creating an Attention outbox entry, unless
  they were DMs or explicit mentions. This occurred even while the PA was
  running. Attention now follows the notification decision and preserves the
  existing mute, membership, self-notification, and delivery-time access checks.
- The provisioned-process monitor retired its runtime after three consecutive
  inspection errors. Retirement fenced the current lease and reconciled/stopped
  the runtime even when it was still healthy. Losing observation was treated as
  sufficient evidence to terminate execution.
- Supervisor inspection could treat an unsuccessful Docker query as a
  successfully observed missing role. Inspection now fails as an observation
  error instead of reporting a false inactive state.

The second defect is a mechanism for unnecessary stops, not proof that it
caused the reported incident. Inspection transport failures must remain
observation failures. An explicit shutdown or a confirmed inactive/different
execution epoch remains a separate lifecycle event.

The monitor now retains ownership and retries observation without fencing or
stopping the runtime. It records observation loss/recovery without logging
private diagnostics. Read-only Docker inspection respects caller cancellation,
and canceled queued requests do not start another inspection. Lifecycle
mutations retain their separate cleanup ownership.

## Delivery while stopped

With the API and database running, eligible Messaging events are stored in an
outbox. Delivery starts the recipient runtime on demand and retries failures.
An idle PA therefore need not miss eligible messages. The API itself being
unavailable is different: it cannot accept new sends during that outage.

Outbox admission is not evidence that the model read or replied to a message.
Use runtime command acknowledgement and subsequent conversation events to
distinguish those stages. Previously discarded channel events have no outbox
record; this change does not silently replay old conversations.
