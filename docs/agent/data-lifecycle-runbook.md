# Data lifecycle and KMS runbook

## Authority and ordering

`apps/api` is the sole Cloud tombstone authority. The supervisor authenticates
as the target tenant/agent, creates the tombstone, fences the generation using
the T26-issued lease and the T27 physical-reap proof, then asks the lifecycle
worker to complete the logical suffix. The worker must never invent a fence,
generation, or physical receipt.

The sequence is `requested -> fenced -> live_purged -> backup_expired`. A
crash repeats the current stage only. `backup_expired` describes backup
retention completion; it never authorizes a restored old conversation to run.

## Restore procedure

Before mounting an agent DB or exposing transcript, search, export, provider,
tool, or artifact access, query the authenticated control-plane tombstones for
that tenant/agent. For an old conversation tombstone, reapply key destruction,
delete only its conversation artifact subtree through the authenticated broker,
and reset the DB to the recorded replacement conversation. Do this for every
state, including `live_purged` and `backup_expired`; then boot the replacement
identity. Keep workspace and every other tenant/agent subtree untouched.

## KMS rotation and revocation

Cloud runtimes resolve the current agent key from KMS on every current-key
operation. Rotate by registering the new KMS agent key, switching KMS current
key, rewrapping active data-key rows, and only then disabling the old key.
If KMS unwrap, identity binding, TLS, or a retired-key unwrap fails, stop
admission and investigate; do not fall back to an environment wrapping key.

## Evidence commands

```sh
cargo test --manifest-path apps/agent/Cargo.toml store::lifecycle::tests --lib
cargo test --manifest-path apps/agent/Cargo.toml store::kms::tests --lib
cargo test --manifest-path apps/agent/Cargo.toml --test lifecycle_command_boundary
(cd apps/api && go test ./internal/store ./internal/handler)
```

The release restore gate additionally restores an old agent DB and artifact
subtree at each tombstone state, proves the old data is removed before command
admission, and proves a second tenant/agent DB, artifact subtree, and workspace
are unchanged.
