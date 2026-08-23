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
    join(root, ".gitignore"),
    "*.tfvars\n*.so\ncredentials.env\n",
  );
  await writeFile(join(root, "tracked.txt"), "pinned\n");
  await writeFile(
    join(bin, "docker"),
    `#!/usr/bin/env bash
set -euo pipefail
printf '%s\\0' "$@" >> "\${FAKE_DOCKER_LOG}"
printf '\\n' >> "\${FAKE_DOCKER_LOG}"
role_hex() {
  case "$1" in
    api) printf a ;;
    agent) printf b ;;
    provisioner) printf c ;;
    web) printf d ;;
    *) exit 90 ;;
  esac
}
role_id() {
  hex="$(role_hex "$1")"
  printf 'sha256:%064s' '' | tr ' ' "$hex"
}
if [[ "\${1:-}" == build ]]; then
  dockerfile= iidfile= reference= context="\${!#}"
  while (($#)); do
    case "$1" in
      --file) dockerfile="$2"; shift ;;
      --iidfile) iidfile="$2"; shift ;;
      --tag) reference="$2"; shift ;;
    esac
    shift
  done
  role="$(basename "$(dirname "$dockerfile")")"
  [[ "$dockerfile" == "$context/deploy/$role/Dockerfile" ]]
  printf '%s\\n' "$context" >> "\${FAKE_CONTEXT_LOG}"
  [[ "$(< "$context/tracked.txt")" == pinned ]]
  if [[ "\${FAKE_ASSERT_EXCLUDED:-}" == 1 ]]; then
    [[ ! -e "$context/private.tfvars" && ! -e "$context/native.so" && ! -e "$context/credentials.env" ]]
  fi
  if [[ "\${FAKE_MIDRUN_EDIT:-}" == 1 && "$role" == api ]]; then
    printf 'edited during build\\n' > "\${LIVE_REPO_ROOT}/tracked.txt"
  fi
  if [[ -n "\${FAKE_FAIL_ROLE:-}" && "$dockerfile" == "deploy/\${FAKE_FAIL_ROLE}/Dockerfile" ]]; then
    exit 42
  fi
  if [[ "\${FAKE_FAIL_ROLE:-}" == "$role" ]]; then exit 42; fi
  if [[ "\${FAKE_BAD_IID_ROLE:-}" == "$role" ]]; then
    printf '%s\\n%s\\n' "$(role_id "$role")" 'injected'
  else
    role_id "$role"
    printf '\\n'
  fi > "$iidfile"
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
subject="$5"
if [[ "$format" == '{{.Id}}' ]]; then
  repository="\${subject%:*}"
  role="\${repository##*-}"
  if [[ "\${FAKE_REMOVE_TAG_ROLE:-}" == "$role" ]]; then exit 1; fi
  if [[ "\${FAKE_RETAG_ROLE:-}" == "$role" ]]; then printf 'sha256:%064s\\n' '' | tr ' ' e; else role_id "$role"; printf '\\n'; fi
  exit 0
fi
[[ "$format" == '{{json .}}' ]]
hex="\${subject#sha256:}"
case "\${hex:0:1}" in
  a) role=api ;;
  b) role=agent ;;
  c) role=provisioner ;;
  d) role=web ;;
  *) exit 92 ;;
esac
repository="ghcr.io/sumi-studio/sumi-$role"
reference="$repository:\${EXPECTED_SHA}"
id="$subject"
if [[ "\${FAKE_IID_MISMATCH_ROLE:-}" == "$role" ]]; then id="sha256:$(printf '%064s' '' | tr ' ' e)"; fi
revision="\${EXPECTED_SHA}"
if [[ "\${FAKE_BAD_LABEL_ROLE:-}" == "$role" ]]; then revision=bad; fi
digest="$repository@sha256:$hex"
if [[ "\${FAKE_BAD_DIGEST_ROLE:-}" == "$role" ]]; then digest="ghcr.io/other/image@sha256:$hex"; fi
tags="[\\"$reference\\"]"
if [[ "\${FAKE_MIXED_TAG_ROLE:-}" == "$role" ]]; then tags="[\\"$reference\\",\\"$repository:latest\\"]"; fi
object="{\\"Id\\":\\"$id\\",\\"Config\\":{\\"Labels\\":{\\"org.opencontainers.image.revision\\":\\"$revision\\",\\"org.opencontainers.image.source\\":\\"https://github.com/sumi-studio/sumi\\"}},\\"RepoTags\\":$tags,\\"RepoDigests\\":[\\"$digest\\"]}"
if [[ "\${FAKE_MIXED_INSPECT_ROLE:-}" == "$role" ]]; then printf '%s\\n%s\\n' "$object" "$object"; else printf '%s\\n' "$object"; fi
`,
    { mode: 0o755 },
  );
  await git(root, ["init", "--quiet"]);
  await git(root, ["config", "user.email", "tests@sumi.invalid"]);
  await git(root, ["config", "user.name", "Sumi tests"]);
  await git(root, ["add", "."]);
  await git(root, ["commit", "--quiet", "-m", "fixture"]);
  await writeFile(join(root, "private.tfvars"), "secret\n");
  await writeFile(join(root, "native.so"), "secret\n");
  await writeFile(join(root, "credentials.env"), "secret\n");
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
      FAKE_CONTEXT_LOG: join(state, "contexts.log"),
      LIVE_REPO_ROOT: root,
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

async function refreshFixtureCommit(fixture, message) {
  await git(fixture.root, ["commit", "--quiet", "-m", message]);
  const { stdout } = await git(fixture.root, ["rev-parse", "HEAD"]);
  fixture.sha = stdout.trim();
  fixture.env.EXPECTED_SHA = fixture.sha;
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
    const expected = [
      `DRY-RUN: export Git tree ${fixture.sha} to /PRIVATE/EXACT-CONTEXT`,
      ...["api", "agent", "provisioner", "web"].map(
        (role) =>
          `DRY-RUN: docker build --iidfile /PRIVATE/IID-${role} --file /PRIVATE/EXACT-CONTEXT/deploy/${role}/Dockerfile --label org.opencontainers.image.revision=${fixture.sha} --label org.opencontainers.image.source=https://github.com/sumi-studio/sumi --tag ghcr.io/sumi-studio/sumi-${role}:${fixture.sha} /PRIVATE/EXACT-CONTEXT`,
      ),
    ].join("\n");
    assert.equal(result.stdout.trim(), expected);
    await assert.rejects(stat(fixture.log), { code: "ENOENT" });
    await assert.rejects(stat(fixture.manifest), { code: "ENOENT" });
    assert.deepEqual(await readdir(fixture.state), []);
  });
});

test("a partial build failure publishes no COMPLETE manifest", async () => {
  await withFixture(async (fixture) => {
    await assert.rejects(run(fixture, [], { FAKE_FAIL_ROLE: "agent" }), {
      code: 42,
    });
    await assert.rejects(stat(fixture.manifest), { code: "ENOENT" });
    assert.doesNotMatch(await readFile(fixture.log, "utf8"), /COMPLETE/);
    const context = (
      await readFile(join(fixture.state, "contexts.log"), "utf8")
    )
      .trim()
      .split("\n")[0];
    await assert.rejects(stat(dirname(context)), { code: "ENOENT" });
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

test("all builds use one exported tree and exclude live edits and ignored secrets", async () => {
  await withFixture(async (fixture) => {
    await run(fixture, [], {
      FAKE_ASSERT_EXCLUDED: "1",
      FAKE_MIDRUN_EDIT: "1",
    });
    assert.equal(
      await readFile(join(fixture.root, "tracked.txt"), "utf8"),
      "edited during build\n",
    );
    const contexts = (
      await readFile(join(fixture.state, "contexts.log"), "utf8")
    )
      .trim()
      .split("\n");
    assert.equal(contexts.length, 4);
    assert.equal(new Set(contexts).size, 1);
    await assert.rejects(stat(dirname(contexts[0])), { code: "ENOENT" });
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

test("submodule and Git LFS trees fail closed before export", async (t) => {
  await t.test("submodule", async () => {
    await withFixture(async (fixture) => {
      const child = join(fixture.container, "child");
      await mkdir(child);
      await git(child, ["init", "--quiet"]);
      await git(child, ["config", "user.email", "tests@sumi.invalid"]);
      await git(child, ["config", "user.name", "Sumi tests"]);
      await writeFile(join(child, "child.txt"), "child\n");
      await git(child, ["add", "."]);
      await git(child, ["commit", "--quiet", "-m", "child"]);
      await git(fixture.root, [
        "-c",
        "protocol.file.allow=always",
        "submodule",
        "add",
        "--quiet",
        child,
        "vendor/submodule",
      ]);
      await refreshFixtureCommit(fixture, "add gitlink");
      await assert.rejects(run(fixture, ["--dry-run"]), /contains submodules/);
      await assert.rejects(stat(fixture.log), { code: "ENOENT" });
    });
  });
  await t.test("Git LFS", async () => {
    await withFixture(async (fixture) => {
      await writeFile(
        join(fixture.root, ".gitattributes"),
        "tracked.txt filter=lfs\n",
      );
      await git(fixture.root, ["add", ".gitattributes"]);
      await refreshFixtureCommit(fixture, "add lfs policy");
      await assert.rejects(
        run(fixture, ["--dry-run"]),
        /contains Git LFS paths/,
      );
      await assert.rejects(stat(fixture.log), { code: "ENOENT" });
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

test("mixed immutable-ID inspection responses and iidfile attacks fail closed", async (t) => {
  for (const [name, env, message] of [
    [
      "mixed inspection",
      { FAKE_MIXED_INSPECT_ROLE: "api" },
      /invalid immutable-ID inspection evidence/,
    ],
    [
      "iid mismatch",
      { FAKE_IID_MISMATCH_ROLE: "agent" },
      /invalid immutable-ID inspection evidence/,
    ],
    [
      "iid injection",
      { FAKE_BAD_IID_ROLE: "web" },
      /non-canonical or mixed iidfile/,
    ],
  ]) {
    await t.test(name, async () => {
      await withFixture(async (fixture) => {
        await assert.rejects(run(fixture, [], env), message);
        await assert.rejects(stat(fixture.manifest), { code: "ENOENT" });
      });
    });
  }
});

test("tag retag and removal before COMPLETE handoff are rejected", async (t) => {
  for (const [name, env, message] of [
    ["retag", { FAKE_RETAG_ROLE: "provisioner" }, /retagged before COMPLETE/],
    ["removal", { FAKE_REMOVE_TAG_ROLE: "web" }, /disappeared before COMPLETE/],
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
