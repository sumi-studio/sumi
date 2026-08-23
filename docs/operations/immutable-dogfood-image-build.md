# Immutable dogfood image build

This runbook covers only the local build and evidence-validation slice for the
four dogfood images: `api`, `agent`, `provisioner`, and `web`. It does
not push images, modify a registry, start or stop Docker workloads, or perform
a cutover.

## Preconditions

- Use a clean checkout whose `HEAD` is the accepted, full lowercase 40-digit
  Git commit `S`.
- `S` must contain the build and validation scripts used below.
- This build path is Linux-only because bounded Docker inspection terminates a
  dedicated process group.
- Git 2.40 or newer, Node.js 20.19 or newer (the repository engine minimum),
  and Docker must be on `PATH`.
- Repository-local `$GIT_DIR/info/attributes` must not exist. System, global,
  and repository-configured attribute files are ignored; the accepted tree's
  committed `.gitattributes` files are authoritative.
- The local Docker `default` context must address the intended daemon.
  `DOCKER_HOST`, ambient Docker contexts, client credentials, client proxy
  settings, and the caller's Docker config are not used. Public base-image
  access must therefore be available through the local daemon without ambient
  client configuration.
- Create the evidence directory with owner-only permissions. The manifest path
  must not already exist.
- Ensure operational exclusivity for the four requested mutable references
  during this build. The builder checks each reference before use, but no
  daemon-wide lock exists and an external actor can create a non-atomic race.

```bash
S="$(git rev-parse HEAD)"
test "$(git rev-parse --verify "${S}^{commit}")" = "${S}"
test -z "$(git status --porcelain=v1 --untracked-files=all)"

EVIDENCE_DIR=/absolute/owner-only/path
install -d -m 0700 "${EVIDENCE_DIR}"
MANIFEST="${EVIDENCE_DIR}/dogfood-images-${S}.json"
test ! -e "${MANIFEST}"
```

## Review the exact build

The dry run verifies the commit, tree, Dockerfiles, worktree cleanliness,
unsupported tree entries, and evidence-directory permissions. It does not call
Docker or write a manifest.

```bash
scripts/operations/build-dogfood-images \
  --commit "${S}" \
  --tag "${S}" \
  --manifest "${MANIFEST}" \
  --dry-run
```

## Build and publish local evidence

The builder first bounds the raw tree to at most 2,048 entries, 512 bytes per
path, 4 MiB per blob, and 64 MiB total. It then materializes one private Docker
context directly from the raw Git tree and blob objects for `S`, streaming
the same limits and blob sizes a second time. It refuses symlinks, gitlinks,
and filtered paths such as Git LFS; working-tree line-ending and encoding
conversions are not applied.

Every Docker operation uses a new owner-only, empty client configuration and
explicitly selects `--context default`. Each role is built with the same
context tree, its pinned Dockerfile, the requested mutable exact-SHA
reference, and the OCI revision and source labels.

```bash
scripts/operations/build-dogfood-images \
  --commit "${S}" \
  --tag "${S}" \
  --manifest "${MANIFEST}"
```

The command writes no evidence on a partial failure. After all four builds
produce distinct canonical immutable image IDs and pass bounded label
inspection, it atomically publishes one owner-only schema-v2 `COMPLETE`
manifest.

## Validate the evidence

Derive the raw tree from the same accepted commit and validate the closed
manifest schema:

```bash
TREE="$(git rev-parse --verify "${S}^{tree}")"
scripts/operations/verify-dogfood-image-bindings \
  --manifest "${MANIFEST}" \
  --commit "${S}" \
  --tree "${TREE}" \
  --tag "${S}" \
  --manifest-only
```

To additionally check the four mutable local tags against the recorded
immutable IDs at the time of inspection, omit `--manifest-only`:

```bash
scripts/operations/verify-dogfood-image-bindings \
  --manifest "${MANIFEST}" \
  --commit "${S}" \
  --tree "${TREE}" \
  --tag "${S}"
```

That live check is intentionally bounded and non-atomic. The immutable IDs and
raw build-input bindings in the manifest are authoritative; mutable tags are
not. The manifest attests the exact raw source tree, selected Dockerfiles, OCI
labels, and resulting immutable image IDs. It does not attest reproducible
dependency resolution, network responses, base-image identity, daemon state,
builder implementation, or broader build-environment identity.
