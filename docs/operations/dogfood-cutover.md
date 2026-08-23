# Dogfood image build and cutover

This runbook fixes the image identity used by a dogfood cutover. The build
step creates exactly four local images from one clean Git commit and writes an
attestation manifest. It does not push, start, stop, migrate, restart, or run
Compose.

Database backup/restore rehearsal and workload quiescence are separate release
gates. A `COMPLETE` image manifest does not satisfy either gate and must not be
used as evidence that they passed.

## Build the four exact-SHA images

Use a clean checkout at the exact accepted `main` commit. The tag is the full
lowercase commit SHA, not a branch, date, short SHA, release alias, or `latest`.

```sh
git switch main
git pull --ff-only
SHA="$(git rev-parse HEAD)"
test "${#SHA}" -eq 40
test -z "$(git status --porcelain)"
mkdir -p .dogfood-manifests

scripts/operations/build-dogfood-images \
  --commit "${SHA}" \
  --tag "${SHA}" \
  --manifest ".dogfood-manifests/images-${SHA}.json" \
  --dry-run

scripts/operations/build-dogfood-images \
  --commit "${SHA}" \
  --tag "${SHA}" \
  --manifest ".dogfood-manifests/images-${SHA}.json"
```

The manifest is published atomically with mode `0600` only after all four
builds and inspections succeed. It records image IDs, available RepoDigests,
and the OCI source/revision labels. A local image commonly has no RepoDigest
until it has been pushed; an empty `repo_digests` array is explicit, not a
registry attestation. On any partial build or inspection mismatch there is no
`COMPLETE` manifest.

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

```sh
export SUMI_AGENT_IMAGE_PULL_POLICY=never
export SUMI_DOCKER_CONFIG_FILE=/absolute/path/to/owner-only/config.json

scripts/dev/compose-stack config
scripts/dev/compose-stack up --pull never
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

scripts/dev/compose-stack config
scripts/dev/compose-stack up --pull always
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
