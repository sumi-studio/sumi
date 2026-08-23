import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import {
  chmod,
  copyFile,
  mkdir,
  mkdtemp,
  readdir,
  readFile,
  rm,
  stat,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const sourceRoot = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const sourceScript = join(
  sourceRoot,
  "scripts/operations/build-dogfood-images",
);

async function git(cwd, args) {
  return execFileAsync("git", args, { cwd });
}

async function createFixture() {
  const container = await mkdtemp(join(tmpdir(), "sumi-image-build-test-"));
  const root = join(container, "repo");
  const state = join(container, "state");
  const bin = join(container, "bin");
  await mkdir(join(root, "scripts/operations"), { recursive: true });
  await mkdir(state);
  await mkdir(bin);
  await copyFile(
    sourceScript,
    join(root, "scripts/operations/build-dogfood-images"),
  );
  await chmod(join(root, "scripts/operations/build-dogfood-images"), 0o755);
  for (const role of ["api", "agent", "provisioner", "web"]) {
    await mkdir(join(root, "deploy", role), { recursive: true });
    await writeFile(join(root, "deploy", role, "Dockerfile"), "FROM scratch\n");
  }
  await writeFile(
    join(bin, "docker"),
    `#!/usr/bin/env bash
set -euo pipefail
printf '%s\\0' "$@" >> "\${FAKE_DOCKER_LOG}"
printf '\\n' >> "\${FAKE_DOCKER_LOG}"
if [[ "\${1:-}" == build ]]; then
  dockerfile=
  while (($#)); do
    if [[ "$1" == --file ]]; then dockerfile="$2"; break; fi
    shift
  done
  if [[ -n "\${FAKE_FAIL_ROLE:-}" && "$dockerfile" == "deploy/\${FAKE_FAIL_ROLE}/Dockerfile" ]]; then
    exit 42
  fi
  exit 0
fi
if [[ "\${1:-}" == image && "\${2:-}" == inspect && "$#" -eq 3 ]]; then
  repository="\${3%:*}"
  role="\${repository##*-}"
  [[ "\${FAKE_EXISTING_ROLE:-}" == "$role" ]]
  exit
fi
[[ "\${1:-}" == image && "\${2:-}" == inspect && "\${3:-}" == --format ]]
format="$4"
reference="$5"
repository="\${reference%:*}"
role="\${repository##*-}"
case "$role" in
  api) hex=a ;;
  agent) hex=b ;;
  provisioner) hex=c ;;
  web) hex=d ;;
  *) exit 90 ;;
esac
digest="$(printf '%064s' '' | tr ' ' "$hex")"
case "$format" in
  '{{.Id}}') printf 'sha256:%s\\n' "$digest" ;;
  *org.opencontainers.image.revision*)
    if [[ "\${FAKE_BAD_LABEL_ROLE:-}" == "$role" ]]; then printf 'bad\\n'; else printf '%s\\n' "\${EXPECTED_SHA}"; fi ;;
  *org.opencontainers.image.source*) printf '%s\\n' 'https://github.com/sumi-studio/sumi' ;;
  '{{join .RepoTags ","}}')
    if [[ "\${FAKE_MIXED_TAG_ROLE:-}" == "$role" ]]; then printf '%s,%s:latest\\n' "$reference" "$repository"; else printf '%s\\n' "$reference"; fi ;;
  '{{join .RepoDigests ","}}')
    if [[ "\${FAKE_BAD_DIGEST_ROLE:-}" == "$role" ]]; then printf 'ghcr.io/other/image@sha256:%s\\n' "$digest"; else printf '%s@sha256:%s\\n' "$repository" "$digest"; fi ;;
  *) exit 91 ;;
esac
`,
    { mode: 0o755 },
  );
  await git(root, ["init", "--quiet"]);
  await git(root, ["config", "user.email", "tests@sumi.invalid"]);
  await git(root, ["config", "user.name", "Sumi tests"]);
  await git(root, ["add", "."]);
  await git(root, ["commit", "--quiet", "-m", "fixture"]);
  const { stdout } = await git(root, ["rev-parse", "HEAD"]);
  const sha = stdout.trim();
  return {
    container,
    root,
    state,
    sha,
    script: join(root, "scripts/operations/build-dogfood-images"),
    manifest: join(state, "manifest.json"),
    log: join(state, "docker.log"),
    env: {
      ...process.env,
      PATH: `${bin}:${process.env.PATH}`,
      FAKE_DOCKER_LOG: join(state, "docker.log"),
      EXPECTED_SHA: sha,
    },
  };
}

async function run(fixture, extraArgs = [], extraEnv = {}) {
  return execFileAsync(
    fixture.script,
    [
      "--commit",
      fixture.sha,
      "--tag",
      fixture.sha,
      "--manifest",
      fixture.manifest,
      ...extraArgs,
    ],
    { cwd: fixture.root, env: { ...fixture.env, ...extraEnv } },
  );
}

async function withFixture(fn) {
  const fixture = await createFixture();
  try {
    await fn(fixture);
  } finally {
    await rm(fixture.container, { recursive: true, force: true });
  }
}

test("dry-run constructs four exact builds without invoking Docker or writing a manifest", async () => {
  await withFixture(async (fixture) => {
    const result = await run(fixture, ["--dry-run"]);
    const expected = ["api", "agent", "provisioner", "web"]
      .map(
        (role) =>
          `DRY-RUN: docker build --file deploy/${role}/Dockerfile --label org.opencontainers.image.revision=${fixture.sha} --label org.opencontainers.image.source=https://github.com/sumi-studio/sumi --tag ghcr.io/sumi-studio/sumi-${role}:${fixture.sha} .`,
      )
      .join("\n");
    assert.equal(result.stdout.trim(), expected);
    await assert.rejects(stat(fixture.log), { code: "ENOENT" });
    await assert.rejects(stat(fixture.manifest), { code: "ENOENT" });
  });
});

test("a partial build failure publishes no COMPLETE manifest", async () => {
  await withFixture(async (fixture) => {
    await assert.rejects(run(fixture, [], { FAKE_FAIL_ROLE: "agent" }), {
      code: 42,
    });
    await assert.rejects(stat(fixture.manifest), { code: "ENOENT" });
    assert.deepEqual(await readdir(fixture.state), ["docker.log"]);
    assert.doesNotMatch(await readFile(fixture.log, "utf8"), /COMPLETE/);
  });
});

test("tag mismatch and floating tags are refused before Docker", async () => {
  await withFixture(async (fixture) => {
    const other = fixture.sha.replace(/^./, fixture.sha[0] === "a" ? "b" : "a");
    await assert.rejects(
      execFileAsync(
        fixture.script,
        [
          "--commit",
          fixture.sha,
          "--tag",
          other,
          "--manifest",
          fixture.manifest,
        ],
        { cwd: fixture.root, env: fixture.env },
      ),
      /tag must exactly equal the source commit/,
    );
    await assert.rejects(
      execFileAsync(
        fixture.script,
        [
          "--commit",
          fixture.sha,
          "--tag",
          "latest",
          "--manifest",
          fixture.manifest,
        ],
        { cwd: fixture.root, env: fixture.env },
      ),
      /floating tags are refused/,
    );
    await assert.rejects(stat(fixture.log), { code: "ENOENT" });
  });
});

test("mixed Compose image tag variables are refused", async () => {
  await withFixture(async (fixture) => {
    await assert.rejects(
      run(fixture, ["--dry-run"], { SUMI_AGENT_IMAGE_TAG: "latest" }),
      /SUMI_AGENT_IMAGE_TAG does not match/,
    );
  });
});

test("an existing local exact-SHA tag is never reassigned", async () => {
  await withFixture(async (fixture) => {
    await assert.rejects(
      run(fixture, [], { FAKE_EXISTING_ROLE: "provisioner" }),
      /immutable image tag already exists locally/,
    );
    await assert.rejects(stat(fixture.manifest), { code: "ENOENT" });
  });
});

test("dirty or incorrectly rooted source trees are refused", async (t) => {
  await t.test("dirty", async () => {
    await withFixture(async (fixture) => {
      await writeFile(join(fixture.root, "dirty.txt"), "dirty\n");
      await assert.rejects(
        run(fixture, ["--dry-run"]),
        /worktree is not clean/,
      );
    });
  });
  await t.test("wrong working directory", async () => {
    await withFixture(async (fixture) => {
      await assert.rejects(
        execFileAsync(
          fixture.script,
          [
            "--commit",
            fixture.sha,
            "--tag",
            fixture.sha,
            "--manifest",
            fixture.manifest,
            "--dry-run",
          ],
          { cwd: fixture.state, env: fixture.env },
        ),
        /run from the repository root/,
      );
    });
  });
});

test("wrong image labels, digests, and mixed RepoTags fail closed", async (t) => {
  for (const [name, env, message] of [
    ["label", { FAKE_BAD_LABEL_ROLE: "agent" }, /wrong revision label/],
    ["digest", { FAKE_BAD_DIGEST_ROLE: "web" }, /unexpected RepoDigest/],
    ["RepoTags", { FAKE_MIXED_TAG_ROLE: "api" }, /floating, or mixed RepoTags/],
  ]) {
    await t.test(name, async () => {
      await withFixture(async (fixture) => {
        await assert.rejects(run(fixture, [], env), message);
        await assert.rejects(stat(fixture.manifest), { code: "ENOENT" });
      });
    });
  }
});

test("shell metacharacters in a tag cannot execute", async () => {
  await withFixture(async (fixture) => {
    const marker = join(fixture.state, "injected");
    await assert.rejects(
      execFileAsync(
        fixture.script,
        [
          "--commit",
          fixture.sha,
          "--tag",
          `$(touch ${marker})`,
          "--manifest",
          fixture.manifest,
        ],
        { cwd: fixture.root, env: fixture.env },
      ),
      /full lowercase Git SHA/,
    );
    await assert.rejects(stat(marker), { code: "ENOENT" });
  });
});

test("successful inspection writes the exact complete mode-safe manifest", async () => {
  await withFixture(async (fixture) => {
    await run(fixture);
    const manifest = JSON.parse(await readFile(fixture.manifest, "utf8"));
    const repositories = {
      api: "ghcr.io/sumi-studio/sumi-api",
      agent: "ghcr.io/sumi-studio/sumi-agent",
      provisioner: "ghcr.io/sumi-studio/sumi-provisioner",
      web: "ghcr.io/sumi-studio/sumi-web",
    };
    const hexes = { api: "a", agent: "b", provisioner: "c", web: "d" };
    assert.deepEqual(manifest, {
      schema_version: 1,
      status: "COMPLETE",
      source: {
        repository: "https://github.com/sumi-studio/sumi",
        revision: fixture.sha,
      },
      tag: fixture.sha,
      images: ["api", "agent", "provisioner", "web"].map((name) => ({
        name,
        reference: `${repositories[name]}:${fixture.sha}`,
        id: `sha256:${hexes[name].repeat(64)}`,
        repo_digests: [
          `${repositories[name]}@sha256:${hexes[name].repeat(64)}`,
        ],
        labels: {
          "org.opencontainers.image.revision": fixture.sha,
          "org.opencontainers.image.source":
            "https://github.com/sumi-studio/sumi",
        },
      })),
    });
    assert.equal((await stat(fixture.manifest)).mode & 0o777, 0o600);
    assert.equal((await stat(fixture.manifest)).nlink, 1);
  });
});
