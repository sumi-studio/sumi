# Dogfood image build and cutover

This runbook fixes the image identity used by a dogfood cutover. The build
step creates exactly four local images from one clean raw Git commit/tree and
writes an immutable-build evidence manifest. It does not push, start, stop,
migrate, restart, or run Compose.

Database backup/restore rehearsal and workload quiescence are separate release
gates. A `COMPLETE` image manifest does not satisfy either gate and must not be
used as evidence that they passed.

## Build the four exact-SHA images

First merge this operations slice. Then choose the final accepted `main` commit
`S`; `S` must itself contain the build and validation scripts below. An older
accepted commit that predates these scripts cannot be built by checking out
that commit and invoking a script only present in a later worktree. Use a clean
checkout at exact `S`. The requested tag is the full
lowercase commit SHA, not a branch, date, short SHA, release alias, or `latest`.
The build host must provide Git 2.40 or newer; the script checks this before it
uses the pinned-tree `git check-attr --source` boundary.

```sh
S='REPLACE_WITH_FINAL_40_HEX_MAIN_SHA'
git switch --detach "${S}"
SHA="$(env -i PATH="${PATH}" LC_ALL=C GIT_CONFIG_NOSYSTEM=1 \
  GIT_CONFIG_GLOBAL=/dev/null GIT_NO_REPLACE_OBJECTS=1 \
  git rev-parse --verify 'HEAD^{commit}')"
test "${#SHA}" -eq 40
test "${SHA}" = "${S}"
EVIDENCE_DIR=/absolute/path/to/owner-only/dogfood-manifests
install -d -m 0700 "${EVIDENCE_DIR}"

scripts/operations/build-dogfood-images \
  --commit "${SHA}" \
  --tag "${SHA}" \
  --manifest "${EVIDENCE_DIR}/images-${SHA}.json" \
  --dry-run

scripts/operations/build-dogfood-images \
  --commit "${SHA}" \
  --tag "${SHA}" \
  --manifest "${EVIDENCE_DIR}/images-${SHA}.json"
```

The manifest is published atomically with mode `0600` only after all four
builds and immutable-IID inspections succeed. `COMPLETE` attests only the four
captured immutable image IDs and the exact raw source commit, tree,
Dockerfiles, requested tag/reference inputs, and OCI source/revision labels.
Requested tags are explicitly non-authoritative mutable handles; `COMPLETE`
does not assert that any tag currently points at a captured ID. On any partial
build or immutable inspection mismatch there is no `COMPLETE` manifest.

Every Git revision, tree, status, and export operation runs with an empty
inherited environment, replacement objects disabled, system/global config
disabled, and only script-controlled Git directory/index/worktree values. The
script resolves and carries the raw expected tree ID separately, exports it
into one owner-only temporary context, re-hashes that exported tree, and uses
the same context for all four builds.
Ignored and untracked files—including local credentials, `.tfvars`, and native
build products—never enter the context, and an edit to the live checkout after
export cannot change a later image. The context is removed on success and on
failure. The repository currently uses no submodules, symbolic links, or Git
LFS. The script fails closed if any appears: a plain tracked-tree export would
otherwise omit submodule content, retain path-indirecting links, or preserve
LFS pointers instead of proving one contained context of regular tracked files.

Each build writes an isolated Docker `--iidfile`. Inspection addresses that
canonical image ID, not its mutable tag, and consumes one bounded JSON evidence
object. A concurrent retag therefore cannot make immutable build evidence
false. The script refuses a requested reference that already exists and
rechecks immediately before each `docker build --tag` assignment. The full-SHA
namespace is intended to be new. Docker provides no atomic "assign only if
absent" primitive here, so an unrelated daemon actor can still win the narrow
check/assignment race; do not treat this script as a daemon lock.

The current Dockerfiles contain upstream image/package selectors that are not
content-digest pinned. The source SHA and labels therefore prove which Sumi
tree was built, but they do not make independent rebuilds byte-for-byte
reproducible. Treat the recorded image IDs as the identity of this local build;
for registry promotion, the verified RepoDigests identify the published bytes.

The script accepts only these references, all with the same tag:

```text
ghcr.io/sumi-studio/sumi-api:${SHA}
ghcr.io/sumi-studio/sumi-agent:${SHA}
ghcr.io/sumi-studio/sumi-provisioner:${SHA}
ghcr.io/sumi-studio/sumi-web:${SHA}
```

It deliberately has no push or Compose operation. Publishing these images
requires a separately reviewed and explicitly authorized registry workflow.

## Bind one tag across the stack

In the same shell used for validation and cutover, bind every service and the
lazily provisioned agent runtime to the one manifest tag:

```sh
export SUMI_DOGFOOD_IMAGE_TAG="${SHA}"
export SUMI_API_IMAGE_TAG="${SUMI_DOGFOOD_IMAGE_TAG}"
export SUMI_AGENT_IMAGE_TAG="${SUMI_DOGFOOD_IMAGE_TAG}"
export SUMI_PROVISIONER_IMAGE_TAG="${SUMI_DOGFOOD_IMAGE_TAG}"
export SUMI_WEB_IMAGE_TAG="${SUMI_DOGFOOD_IMAGE_TAG}"

test "${SUMI_API_IMAGE_TAG}" = "${SUMI_AGENT_IMAGE_TAG}"
test "${SUMI_API_IMAGE_TAG}" = "${SUMI_PROVISIONER_IMAGE_TAG}"
test "${SUMI_API_IMAGE_TAG}" = "${SUMI_WEB_IMAGE_TAG}"
```

Do not continue with an absent variable, `latest`, a short SHA, or unequal
values. The provisioner forwards `SUMI_AGENT_IMAGE_TAG` and
`SUMI_AGENT_IMAGE_PULL_POLICY` into every PAID-specific agent Compose project;
setting only the control-plane tags does not pin the agent runtime.

## Reviewed local-image path (`--pull never`)

This path uses the four locally inspected images and makes registry access
irrelevant to image selection. Keep the launcher-required owner-only Docker
configuration file in place even when pulls are disabled; that file mount is a
separate provisioner boundary.

Local Docker tags remain mutable after the manifest is written. Immediately
before the reviewed cutover, use the closed validator to require the exact
schema/version/revision/tree, exactly one of each known role, canonical IIDs,
four distinct IIDs, the exact requested repositories/tags/Dockerfiles, no
unknown keys, and current reference-to-IID equality. Each Docker inspection has
a five-second timeout and 64-KiB output bound. A successful result is only a
time-of-check fact; it does not lock Docker or exclude unrelated daemon actors.
Minimize the interval between this check and use, and fail closed on any
intervening daemon activity.

```sh
MANIFEST="${EVIDENCE_DIR}/images-${SHA}.json"
TREE="$(env -i PATH="${PATH}" LC_ALL=C GIT_CONFIG_NOSYSTEM=1 \
  GIT_CONFIG_GLOBAL=/dev/null GIT_NO_REPLACE_OBJECTS=1 \
  git rev-parse --verify "${SHA}^{tree}")"
scripts/operations/verify-dogfood-image-bindings \
  --manifest "${MANIFEST}" --commit "${SHA}" --tree "${TREE}" --tag "${SHA}"
```

```sh
export SUMI_AGENT_IMAGE_PULL_POLICY=never
export SUMI_DOCKER_CONFIG_FILE=/absolute/path/to/owner-only/config.json
export SUMI_LOCAL_COMPOSE_PROJECT=sumi-dev
export SUMI_LOCAL_ENV_FILE=/absolute/path/to/deploy/local/.env.local
export SUMI_LOCAL_RUNTIME_ENV_FILE=/absolute/path/to/deploy/local/.env.runtime
export SUMI_LOCAL_COMPOSE_OVERRIDE_FILE=
readonly SUMI_LOCAL_COMPOSE_PROJECT SUMI_LOCAL_ENV_FILE \
  SUMI_LOCAL_RUNTIME_ENV_FILE SUMI_LOCAL_COMPOSE_OVERRIDE_FILE

umask 077
scripts/dev/compose-stack --firebase real config > "${EVIDENCE_DIR}/compose-${SHA}.yaml"
sha256sum "${SUMI_LOCAL_ENV_FILE}" "${SUMI_LOCAL_RUNTIME_ENV_FILE}" \
  > "${EVIDENCE_DIR}/compose-inputs-${SHA}.sha256"

# Immediately before use: fail if config inputs or mutable tag bindings changed.
sha256sum --check "${EVIDENCE_DIR}/compose-inputs-${SHA}.sha256"
scripts/operations/verify-dogfood-image-bindings \
  --manifest "${MANIFEST}" --commit "${SHA}" --tree "${TREE}" --tag "${SHA}"
scripts/dev/compose-stack --firebase real up --pull never
```

`--pull never` governs the control-plane Compose invocation. The explicit
`SUMI_AGENT_IMAGE_PULL_POLICY=never` separately governs the allocator,
prepare, runtime, executor, and broker services created later by the
provisioner. Both settings are required for a fully local cutover.

Do not run `deploy/agent/compose.yaml` directly. Under the lifecycle accepted
in PR #329, the provisioner and supervisor own the exact-generation sequence:

1. `prepare` synchronously joins and removes an old project before allocator
   and prepare issue a new epoch.
2. `activate` starts only executor, broker, and runtime with `--no-deps`; it
   cannot rerun the allocator.
3. `abort` and exact-epoch stop remove the complete project and attest the
   reaped generation only after observed emptiness.
4. `inspect-epoch` and `reconcile` use the non-secret lifecycle descriptor;
   reconciliation fails closed when a live generation cannot be fenced.

Image selection happens before this lifecycle. It does not weaken the epoch,
cleanup, containment, or authenticated Ready gates.

## Registry immutable path

Use this path only after a separate authorized publication workflow has:

- pushed the four images represented by the reviewed local manifest;
- verified each registry RepoDigest and OCI revision/source label;
- recorded those four digests in a registry promotion attestation; and
- proved that the registry prevents reassignment of the full-SHA tags.

The current Compose files select images by tag, not by digest. A full SHA tag
is therefore immutable only when registry policy makes it so. Do not treat tag
shape alone as immutability.

With that registry evidence reviewed, retain the exact same four tag variables
and select pulls explicitly:

```sh
export SUMI_AGENT_IMAGE_PULL_POLICY=always
export SUMI_DOCKER_CONFIG_FILE=/absolute/path/to/owner-only/config.json
export SUMI_LOCAL_COMPOSE_PROJECT=sumi-dev
export SUMI_LOCAL_ENV_FILE=/absolute/path/to/deploy/local/.env.local
export SUMI_LOCAL_RUNTIME_ENV_FILE=/absolute/path/to/deploy/local/.env.runtime
export SUMI_LOCAL_COMPOSE_OVERRIDE_FILE=
readonly SUMI_LOCAL_COMPOSE_PROJECT SUMI_LOCAL_ENV_FILE \
  SUMI_LOCAL_RUNTIME_ENV_FILE SUMI_LOCAL_COMPOSE_OVERRIDE_FILE

umask 077
scripts/dev/compose-stack --firebase real config > "${EVIDENCE_DIR}/compose-${SHA}.yaml"
sha256sum "${SUMI_LOCAL_ENV_FILE}" "${SUMI_LOCAL_RUNTIME_ENV_FILE}" \
  > "${EVIDENCE_DIR}/compose-inputs-${SHA}.sha256"
sha256sum --check "${EVIDENCE_DIR}/compose-inputs-${SHA}.sha256"
scripts/dev/compose-stack --firebase real up --pull always
```

If any pulled image ID, RepoDigest, source label, revision label, or tag differs
from the promotion attestation, stop. Do not fall back to a local cache,
`missing`, `latest`, or a mixed set.

## Gates outside this document

Before either cutover path, the release owner must separately confirm:

- a backup was created from the intended database and a restore rehearsal
  proved it usable;
- the required quiescence boundary is in force for schema/runtime transition;
- migration authorization and execution, if any, follows its own reviewed
  workflow; and
- rollback/recovery authority and the exact accepted image manifest are
  available to the operator.

This document does not authorize backup mutation, restore, migration, registry
push, service teardown, deployment, or recovery. The commands in the two
cutover sections are operator paths to use only after those independent gates
and authorizations are satisfied.
