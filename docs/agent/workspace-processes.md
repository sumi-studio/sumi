# Workspace processes

The `process` tool starts a command without keeping a model turn open. The same
secretary can continue a conversation while it runs. Its terminal result enters
Attention as `workspace_operation / process_completed`, so no browser connection
or user status request is needed to deliver it.

## Execution and ownership

`start` accepts an executable, arguments, a relative working directory, and a
timeout. It returns after durable acceptance. Use `/bin/bash -c` explicitly when
shell expansion is needed. `status`, `read_output`, and `cancel` address the
returned operation ID. Reading a result never starts the command again.

The existing bound-tool execution path reviews the command. The internal call
identity supplies the idempotency key; model arguments cannot choose it or the
acting PA. The API obtains the owner and current epoch from authenticated local
control. Repeating the same internal call returns the same operation; changing
its arguments conflicts.

The runtime provisioner owns the operation container separately from PA runtime
generations. Cold idle teardown, closing Direct Chat, or aborting a conversation
does not cancel accepted work. Cancellation is explicit and operation-scoped.
The provisioner uses Docker directly for these typed operations; generation
lifecycle commands retain their existing supervisor path. Neither route exposes
a Docker socket to the PA or its tool executor.

## Environment and limits

The deployment's pinned `SUMI_AGENT_IMAGE_TAG` selects the existing agent image.
It provides shell/basic utilities; additional build tools are not assumed. The
container has the PA's workspace volume and a 32 MiB scratch filesystem. It has
no network, inherited provider credentials, identity volume, control sockets, or
Docker socket. Its root filesystem is read-only. It runs as UID/GID 10002 with
all capabilities dropped and no new privileges, limited to one CPU, 384 MiB
memory, and 128 processes.

The initial capacity is one active operation per PA and four overall. Capacity
exhaustion is a tool error; existing work and ordinary conversation continue.
Timeout defaults to 600 seconds and is at most 3600 seconds. The container's own
timeout also runs while the provisioner is unavailable.

Each output stream retains at most 1 MiB. `stdout_bytes` and `stderr_bytes` count
retained bytes, not the total bytes the program emitted. `read_output` uses byte
offsets and a 4–65536-byte limit (default 16384). Ordinary sequential UTF-8 reads
preserve complete code points; non-text bytes are represented as lossy text.
Truncation is explicit. Docker's temporary logs are separately capped at 16 MiB.
The workspace has no storage quota: these output limits do not bound files a
command writes into its workspace.

## Recovery and delivery

The private provisioner state directory contains accepted operations, bounded
output snapshots, terminal results, and completion admission receipts. Terminal
output stays on disk and is loaded only when read, rather than accumulating in
the provisioner's memory. The
observer reconciles existing containers after restart. An unconfirmed launch is
not blindly repeated: if its container is missing, the result is indeterminate.
A terminal container is removed only after its terminal snapshot is durable;
workspace files and the operation record remain.

The API reconciles a completion with the durable command log before waking the
same PA. A lost acknowledgment therefore retries the same event, rather than
starting another command or adding another completion. Completion metadata is an
external event from the PA's own operation, not a human message or new authority.
Raw stdout/stderr are read separately with `read_output`. Delivery proves receipt
by the agent gateway; it does not prove the secretary has read or acted on it.

## Verification

Focused tests cover accepted-call replay, store reconstruction, cancellation,
output bounds, unavailable logs, and authenticated API admission. The opt-in
Docker test uses an already present image and its own temporary workspace and
operation containers:

```sh
cd apps/api
SUMI_TEST_PROCESS_DOCKER=1 SUMI_TEST_PROCESS_IMAGE_TAG=<full-source-revision> \
  go test ./internal/runtimeprovision -run '^TestProcessDockerIntegration$' -count=1 -v
```

That test does not prove model behavior. Acceptance also requires a real-model
journey: start actual work, receive a separate conversation reply while it runs,
disconnect, observe automatic completion handling by the same PA, verify the
artifact/report, and reconnect without a second execution.
