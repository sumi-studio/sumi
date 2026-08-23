import assert from "node:assert/strict";
import { execFile, spawn } from "node:child_process";
import {
  chmod,
  copyFile,
  mkdir,
  mkdtemp,
  readdir,
  readFile,
  rm,
  stat,
  symlink,
  truncate,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import test from "node:test";
import { fileURLToPath, pathToFileURL } from "node:url";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const sourceRoot = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const sourceScript = join(
  sourceRoot,
  "scripts/operations/build-dogfood-images",
);
const sourceValidator = join(
  sourceRoot,
  "scripts/operations/verify-dogfood-image-bindings",
);
const sourceDockerWrapper = join(
  sourceRoot,
  "scripts/operations/dogfood-docker.mjs",
);
const TREE_LIMITS = {
  entries: 2_048,
  pathBytes: 512,
  blobBytes: 4 * 1024 * 1024,
  aggregateBytes: 64 * 1024 * 1024,
};

async function git(cwd, args) {
  return execFileAsync("git", args, { cwd });
}

async function treeMetrics(fixture) {
  const { stdout } = await git(fixture.root, [
    "ls-tree",
    "-r",
    "-z",
    "--long",
    fixture.sha,
  ]);
  let entries = 0;
  let aggregateBytes = 0;
  for (const record of stdout.split("\0")) {
    if (!record) continue;
    const [metadata, path] = record.split("\t");
    const [, type, , size] = metadata.trim().split(/ +/);
    assert.equal(type, "blob", `unexpected fixture entry: ${path}`);
    entries += 1;
    aggregateBytes += Number(size);
  }
  return { entries, aggregateBytes };
}

function pathWithBytes(length) {
  const components = [];
  let remaining = length;
  while (remaining > 240) {
    components.push("p".repeat(200));
    remaining -= 201;
  }
  components.push("p".repeat(remaining));
  const path = components.join("/");
  assert.equal(Buffer.byteLength(path), length);
  return path;
}

async function addSparseFile(root, path, size) {
  const absolute = join(root, path);
  await mkdir(dirname(absolute), { recursive: true });
  await writeFile(absolute, "");
  await truncate(absolute, size);
}

async function commitEntryCount(fixture, target) {
  const metrics = await treeMetrics(fixture);
  assert.ok(metrics.entries <= target);
  for (let index = metrics.entries; index < target; index += 1) {
    const path = join(fixture.root, "bounds", `entry-${index}`);
    await mkdir(dirname(path), { recursive: true });
    await writeFile(path, "");
  }
  await git(fixture.root, ["add", "bounds"]);
  await refreshFixtureCommit(fixture, `set entry count to ${target}`);
}

async function commitPathLength(fixture, target) {
  const path = pathWithBytes(target);
  await addSparseFile(fixture.root, path, 0);
  await git(fixture.root, ["add", path]);
  await refreshFixtureCommit(fixture, `add ${target}-byte path`);
}

async function commitBlobSize(fixture, target) {
  await addSparseFile(fixture.root, "bounds/blob", target);
  await git(fixture.root, ["add", "bounds/blob"]);
  await refreshFixtureCommit(fixture, `add ${target}-byte blob`);
}

async function commitAggregateSize(fixture, target) {
  const metrics = await treeMetrics(fixture);
  assert.ok(metrics.aggregateBytes <= target);
  let remaining = target - metrics.aggregateBytes;
  let index = 0;
  while (remaining > 0) {
    const size = Math.min(remaining, TREE_LIMITS.blobBytes);
    const path = `bounds/aggregate-${index++}`;
    await addSparseFile(fixture.root, path, size);
    remaining -= size;
  }
  await git(fixture.root, ["add", "bounds"]);
  await refreshFixtureCommit(fixture, `set aggregate size to ${target}`);
}

async function createFixture() {
  const container = await mkdtemp(join(tmpdir(), "sumi-image-build-test-"));
  const root = join(container, "repo");
  const state = join(container, "state");
  const bin = join(container, "bin");
  await mkdir(join(root, "scripts/operations"), { recursive: true });
  await mkdir(state, { mode: 0o700 });
  await mkdir(bin);
  await copyFile(
    sourceScript,
    join(root, "scripts/operations/build-dogfood-images"),
  );
  await copyFile(
    sourceValidator,
    join(root, "scripts/operations/verify-dogfood-image-bindings"),
  );
  await copyFile(
    sourceDockerWrapper,
    join(root, "scripts/operations/dogfood-docker.mjs"),
  );
  await chmod(join(root, "scripts/operations/build-dogfood-images"), 0o755);
  await chmod(
    join(root, "scripts/operations/verify-dogfood-image-bindings"),
    0o755,
  );
  await chmod(join(root, "scripts/operations/dogfood-docker.mjs"), 0o755);
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
fixture_root="$(cd "$(dirname "$0")/.." && pwd -P)"
state="$fixture_root/state"
controls="$state/controls"
control() {
  if [[ -f "$controls/$1" ]]; then
    command cat -- "$controls/$1"
  fi
}
FAKE_ALIAS_ALL_BINDINGS="$(control FAKE_ALIAS_ALL_BINDINGS)"
FAKE_APPEAR_BEFORE_ROLE="$(control FAKE_APPEAR_BEFORE_ROLE)"
FAKE_ASSERT_EXCLUDED="$(control FAKE_ASSERT_EXCLUDED)"
FAKE_BAD_IID_ROLE="$(control FAKE_BAD_IID_ROLE)"
FAKE_BAD_LABEL_ROLE="$(control FAKE_BAD_LABEL_ROLE)"
FAKE_CANCEL_BUILD_ROLE="$(control FAKE_CANCEL_BUILD_ROLE)"
FAKE_CANCEL_INSPECT_ROLE="$(control FAKE_CANCEL_INSPECT_ROLE)"
FAKE_DOCKER_SECRET="$(control FAKE_DOCKER_SECRET)"
FAKE_EXISTING_ROLE="$(control FAKE_EXISTING_ROLE)"
FAKE_FAIL_ROLE="$(control FAKE_FAIL_ROLE)"
FAKE_FLOOD_STDERR_ROLE="$(control FAKE_FLOOD_STDERR_ROLE)"
FAKE_FLOOD_STDOUT_ROLE="$(control FAKE_FLOOD_STDOUT_ROLE)"
FAKE_HANG_BINDING_ROLE="$(control FAKE_HANG_BINDING_ROLE)"
FAKE_HANG_IID_ROLE="$(control FAKE_HANG_IID_ROLE)"
FAKE_IID_MISMATCH_ROLE="$(control FAKE_IID_MISMATCH_ROLE)"
FAKE_INSPECT_EXIT_125_ROLE="$(control FAKE_INSPECT_EXIT_125_ROLE)"
FAKE_MIDRUN_EDIT="$(control FAKE_MIDRUN_EDIT)"
FAKE_MIXED_INSPECT_ROLE="$(control FAKE_MIXED_INSPECT_ROLE)"
FAKE_RETAG_AFTER_API="$(control FAKE_RETAG_AFTER_API)"
FAKE_REQUIRE_STDIN_ROLE="$(control FAKE_REQUIRE_STDIN_ROLE)"
FAKE_STDIN_TOKEN="$(control FAKE_STDIN_TOKEN)"
FAKE_UNTRUST_PRIVATE_ROOT_ROLE="$(control FAKE_UNTRUST_PRIVATE_ROOT_ROLE)"

[[ "\${1:-}" == --config && -n "\${2:-}" ]]
client_config="$2"
shift 2
[[ "\${1:-}" == --context && "\${2:-}" == default ]]
shift 2
[[ "$client_config" == "\${DOCKER_CONFIG:-}" ]]
[[ "$client_config" == "\${HOME:-}" ]]
[[ "$(stat -Lc '%a' -- "$client_config")" == 700 ]]
[[ -z "$(find "$client_config" -mindepth 1 -maxdepth 1 -print -quit)" ]]
printf '%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\t%s\\n' \
  "\${DOCKER_HOST-}" "\${DOCKER_CONTEXT-}" "\${DOCKER_CONFIG-}" "\${HOME-}" \
  "\${HTTP_PROXY-}" "\${HTTPS_PROXY-}" "\${ALL_PROXY-}" "\${NO_PROXY-}" "$client_config" \
  >> "$state/docker-authority.log"
printf '%s\\0' "$@" >> "$state/docker.log"
printf '\\n' >> "$state/docker.log"
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
  printf '%s\\n' "$context" >> "$state/contexts.log"
  [[ "$(< "$context/tracked.txt")" == pinned ]]
  if [[ "$FAKE_REQUIRE_STDIN_ROLE" == "$role" ]]; then
    IFS= read -r stdin_token
    [[ "$stdin_token" == "$FAKE_STDIN_TOKEN" ]]
  fi
  if [[ "$FAKE_UNTRUST_PRIVATE_ROOT_ROLE" == "$role" ]]; then
    private_root="$(dirname "$context")"
    moved_root="$state/untrusted-private-root"
    [[ ! -e "$moved_root" && ! -L "$moved_root" ]]
    mv -- "$private_root" "$moved_root"
    ln -s -- "$moved_root" "$private_root"
  fi
  if [[ "$FAKE_CANCEL_BUILD_ROLE" == "$role" ]]; then
    printf '%s\\n' "$$" > "$state/cancel-build-leader.pid"
    printf '%s\\n' "$PPID" > "$state/cancel-build-wrapper.pid"
    (
      trap '' INT TERM
      printf '%s\\n' "$BASHPID" > "$state/cancel-build-helper.pid"
      exec sleep 60
    ) &
    trap 'exit 143' TERM
    trap 'exit 130' INT
    wait
  fi
  if [[ "$FAKE_ASSERT_EXCLUDED" == 1 ]]; then
    [[ ! -e "$context/private.tfvars" && ! -e "$context/native.so" && ! -e "$context/credentials.env" ]]
  fi
  if [[ "$FAKE_MIDRUN_EDIT" == 1 && "$role" == api ]]; then
    printf 'edited during build\\n' > "$fixture_root/repo/tracked.txt"
  fi
  if [[ "$FAKE_RETAG_AFTER_API" == 1 && "$role" == api ]]; then
    : > "$state/retagged-after-api"
  fi
  if [[ "$FAKE_FAIL_ROLE" == "$role" ]]; then exit 42; fi
  if [[ "$FAKE_BAD_IID_ROLE" == "$role" ]]; then
    printf '%s\\n%s\\n' "$(role_id "$role")" 'injected'
  else
    role_id "$role"
    printf '\\n'
  fi > "$iidfile"
  : > "$state/built-$role"
  exit 0
fi
[[ "\${1:-}" == image && "\${2:-}" == inspect && "\${3:-}" == --format ]]
format="$4"
subject="$5"
[[ "$format" == '{{json .}}' ]]
if [[ "$subject" == sha256:* ]]; then
  hex="\${subject#sha256:}"
  case "\${hex:0:1}" in
    a) role=api ;;
    b) role=agent ;;
    c) role=provisioner ;;
    d) role=web ;;
    *) exit 92 ;;
  esac
  id="$subject"
  hang_role="$FAKE_HANG_IID_ROLE"
else
  repository="\${subject%:*}"
  role="\${repository##*-}"
  if [[ "$FAKE_INSPECT_EXIT_125_ROLE" == "$role" ]]; then
    printf '%s' "$FAKE_DOCKER_SECRET" >&2
    exit 125
  fi
  if [[ "$FAKE_EXISTING_ROLE" == "$role" ]]; then
    :
  elif [[ "$FAKE_APPEAR_BEFORE_ROLE" == "$role" ]]; then
    marker="$state/appeared-$role"
    if [[ ! -e "$marker" ]]; then
      : > "$marker"
      printf 'Error response from daemon: No such image: %s\\n' "$subject" >&2
      exit 1
    fi
  elif [[ ! -e "$state/built-$role" ]]; then
    printf 'Error response from daemon: No such image: %s\\n' "$subject" >&2
    exit 1
  fi
  id="$(role_id "$role")"
  hang_role="$FAKE_HANG_BINDING_ROLE"
  if [[ "$FAKE_ALIAS_ALL_BINDINGS" == 1 ]]; then
    id="$(role_id api)"
  elif [[ "$FAKE_RETAG_AFTER_API" == 1 && "$role" == api && -e "$state/retagged-after-api" ]]; then
    id="sha256:$(printf '%064s' '' | tr ' ' e)"
  fi
fi
if [[ "$FAKE_CANCEL_INSPECT_ROLE" == "$role" ]]; then
  printf '%s\\n' "$$" > "$state/cancel-inspect-leader.pid"
  printf '%s\\n' "$PPID" > "$state/cancel-inspect-wrapper.pid"
  (
    trap '' INT TERM
    printf '%s\\n' "$BASHPID" > "$state/cancel-inspect-helper.pid"
    exec </dev/null >/dev/null 2>/dev/null
    exec sleep 60
  ) &
  trap 'exit 143' TERM
  trap 'exit 130' INT
  wait
fi
if [[ "$hang_role" == "$role" ]]; then
  printf '%s\\n' "$$" > "$state/hanging-docker.pid"
  printf '%s' "$FAKE_DOCKER_SECRET" >&2
  trap '' TERM
  exec sleep 60
fi
if [[ "$FAKE_FLOOD_STDOUT_ROLE" == "$role" ]]; then
  exec node -e 'process.stdout.write(process.argv[1].repeat(100000))' "$FAKE_DOCKER_SECRET"
fi
if [[ "$FAKE_FLOOD_STDERR_ROLE" == "$role" ]]; then
  exec node -e 'process.stderr.write(process.argv[1].repeat(10000))' "$FAKE_DOCKER_SECRET"
fi
if [[ "$FAKE_IID_MISMATCH_ROLE" == "$role" ]]; then id="sha256:$(printf '%064s' '' | tr ' ' e)"; fi
revision="$(< "$state/expected-sha")"
if [[ "$FAKE_BAD_LABEL_ROLE" == "$role" ]]; then revision=bad; fi
object="{\\"Id\\":\\"$id\\",\\"Config\\":{\\"Labels\\":{\\"org.opencontainers.image.revision\\":\\"$revision\\",\\"org.opencontainers.image.source\\":\\"https://github.com/sumi-studio/sumi\\"}}}"
if [[ "$FAKE_MIXED_INSPECT_ROLE" == "$role" ]]; then printf '%s\\n%s\\n' "$object" "$object"; else printf '%s\\n' "$object"; fi
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
  await writeFile(join(state, "expected-sha"), `${sha}\n`);
  const { stdout: treeStdout } = await git(root, [
    "rev-parse",
    `${sha}^{tree}`,
  ]);
  return {
    container,
    root,
    state,
    sha,
    tree: treeStdout.trim(),
    script: join(root, "scripts/operations/build-dogfood-images"),
    manifest: join(state, "manifest.json"),
    log: join(state, "docker.log"),
    env: {
      ...process.env,
      PATH: `${bin}:${process.env.PATH}`,
    },
  };
}

async function setFakeControls(fixture, values = {}) {
  const controls = join(fixture.state, "controls");
  await rm(controls, { recursive: true, force: true });
  await mkdir(controls, { mode: 0o700 });
  for (const [name, value] of Object.entries(values)) {
    if (name.startsWith("FAKE_")) {
      await writeFile(join(controls, name), String(value));
    }
  }
}

async function assertHangingDockerWasKilled(fixture) {
  const pid = Number(
    (await readFile(join(fixture.state, "hanging-docker.pid"), "utf8")).trim(),
  );
  assert.ok(Number.isInteger(pid) && pid > 1);
  assert.throws(
    () => process.kill(pid, 0),
    (error) => error?.code === "ESRCH",
    `Docker process ${pid} survived the bounded helper`,
  );
}

async function waitForFile(path, timeoutMs = 2_000) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    try {
      return await readFile(path, "utf8");
    } catch (error) {
      if (error?.code !== "ENOENT" || Date.now() >= deadline) throw error;
      await new Promise((resolvePromise) => setTimeout(resolvePromise, 10));
    }
  }
}

function collectChild(child) {
  const stdout = [];
  const stderr = [];
  child.stdout.on("data", (chunk) => stdout.push(chunk));
  child.stderr.on("data", (chunk) => stderr.push(chunk));
  return new Promise((resolvePromise, reject) => {
    child.once("error", reject);
    child.once("close", (code, signal) => {
      resolvePromise({
        code,
        signal,
        stdout: Buffer.concat(stdout).toString("utf8"),
        stderr: Buffer.concat(stderr).toString("utf8"),
      });
    });
  });
}

async function childCompletionWithin(completion, timeoutMs, message) {
  let timer;
  try {
    return await Promise.race([
      completion,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(message)), timeoutMs);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

function assertProcessGone(pid, description) {
  assert.ok(Number.isInteger(pid) && pid > 1, `invalid ${description} PID`);
  assert.throws(
    () => process.kill(pid, 0),
    (error) => error?.code === "ESRCH",
    `${description} ${pid} survived external cancellation`,
  );
}

function killProcess(pid, signal = "SIGKILL") {
  if (!Number.isInteger(pid) || pid <= 1) return;
  try {
    process.kill(pid, signal);
  } catch {}
}

function killProcessGroup(pid, signal = "SIGKILL") {
  if (!Number.isInteger(pid) || pid <= 1) return;
  try {
    process.kill(-pid, signal);
  } catch {}
}

async function run(fixture, extraArgs = [], extraEnv = {}) {
  await setFakeControls(fixture, extraEnv);
  const processEnv = Object.fromEntries(
    Object.entries(extraEnv).filter(([name]) => !name.startsWith("FAKE_")),
  );
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
    { cwd: fixture.root, env: { ...fixture.env, ...processEnv } },
  );
}

async function runWithGitVersion(fixture, version) {
  const fakeGitBin = join(fixture.container, "fake-git-bin");
  await mkdir(fakeGitBin, { recursive: true });
  const { stdout } = await execFileAsync("sh", ["-c", "command -v git"]);
  const realGit = stdout.trim();
  await writeFile(
    join(fakeGitBin, "git"),
    `#!/usr/bin/env bash
set -euo pipefail
if [[ "\${!#}" == version ]]; then
  printf '%s\\n' ${JSON.stringify(version)}
  exit 0
fi
exec ${JSON.stringify(realGit)} "$@"
`,
    { mode: 0o755 },
  );
  return run(fixture, ["--dry-run"], {
    PATH: `${fakeGitBin}:${fixture.env.PATH}`,
  });
}

async function refreshFixtureCommit(fixture, message) {
  await git(fixture.root, ["commit", "--quiet", "-m", message]);
  const { stdout } = await git(fixture.root, ["rev-parse", "HEAD"]);
  fixture.sha = stdout.trim();
  const { stdout: treeStdout } = await git(fixture.root, [
    "rev-parse",
    `${fixture.sha}^{tree}`,
  ]);
  fixture.tree = treeStdout.trim();
  await writeFile(join(fixture.state, "expected-sha"), `${fixture.sha}\n`);
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
      `DRY-RUN: export raw Git commit ${fixture.sha} tree ${fixture.tree} to /PRIVATE/EXACT-CONTEXT`,
      ...["api", "agent", "provisioner", "web"].map(
        (role) =>
          `DRY-RUN: docker --config /PRIVATE/EMPTY-DOCKER-CONFIG --context default build --iidfile /PRIVATE/IID-${role} --file /PRIVATE/EXACT-CONTEXT/deploy/${role}/Dockerfile --label org.opencontainers.image.revision=${fixture.sha} --label org.opencontainers.image.source=https://github.com/sumi-studio/sumi --tag ghcr.io/sumi-studio/sumi-${role}:${fixture.sha} /PRIVATE/EXACT-CONTEXT`,
      ),
    ].join("\n");
    assert.equal(result.stdout.trim(), expected);
    await assert.rejects(stat(fixture.log), { code: "ENOENT" });
    await assert.rejects(stat(fixture.manifest), { code: "ENOENT" });
    assert.deepEqual((await readdir(fixture.state)).sort(), [
      "controls",
      "expected-sha",
    ]);
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

test("EXIT cleanup preserves a primary failure and fails closed for an untrusted private root", async (t) => {
  await t.test("preserves an existing Docker failure", async () => {
    await withFixture(async (fixture) => {
      await assert.rejects(
        run(fixture, [], {
          FAKE_FAIL_ROLE: "api",
          FAKE_UNTRUST_PRIVATE_ROOT_ROLE: "api",
        }),
        (error) => {
          assert.equal(error.code, 42);
          assert.match(
            error.stderr,
            /refusing to clean an untrusted private build root/,
          );
          return true;
        },
      );
      await assert.rejects(stat(fixture.manifest), { code: "ENOENT" });
    });
  });

  await t.test(
    "turns an otherwise successful exit into a failure",
    async () => {
      await withFixture(async (fixture) => {
        await assert.rejects(
          run(fixture, [], { FAKE_UNTRUST_PRIVATE_ROOT_ROLE: "api" }),
          (error) => {
            assert.equal(error.code, 1);
            assert.match(
              error.stderr,
              /refusing to clean an untrusted private build root/,
            );
            return true;
          },
        );
        await stat(fixture.manifest);
      });
    },
  );
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

test("Git version preflight accepts supported vendor suffixes and rejects old or malformed versions", async (t) => {
  for (const version of [
    "git version 2.40",
    "git version 2.40.0",
    "git version 2.47.1.windows.1",
    "git version 2.47.1 (Apple Git-147)",
    "git version 3.0.0-rc1",
  ]) {
    await t.test(version, async () => {
      await withFixture(async (fixture) => {
        await runWithGitVersion(fixture, version);
      });
    });
  }
  for (const version of ["git version 2.39.9", "git version 2.x"]) {
    await t.test(`reject ${version}`, async () => {
      await withFixture(async (fixture) => {
        await assert.rejects(
          runWithGitVersion(fixture, version),
          /Git 2\.40 or newer is required/,
        );
      });
    });
  }
});

test("an existing local exact-SHA tag is never reassigned", async () => {
  await withFixture(async (fixture) => {
    await assert.rejects(
      run(fixture, [], { FAKE_EXISTING_ROLE: "provisioner" }),
      /requested mutable reference already exists locally/,
    );
    await assert.rejects(stat(fixture.manifest), { code: "ENOENT" });
  });
});

test("only verified Docker not-found permits mutable-reference assignment", async (t) => {
  await t.test("verified absence", async () => {
    await withFixture(async (fixture) => {
      await run(fixture);
      await stat(fixture.manifest);
    });
  });

  await t.test("Docker exit 125", async () => {
    await withFixture(async (fixture) => {
      const secret = "must-not-escape-exit-125-inspection";
      let failure;
      try {
        await run(fixture, [], {
          FAKE_INSPECT_EXIT_125_ROLE: "api",
          FAKE_DOCKER_SECRET: secret,
        });
      } catch (error) {
        failure = error;
      }
      assert.ok(failure, "Docker exit 125 was mistaken for verified absence");
      assert.match(
        failure.stderr,
        /cannot inspect requested mutable reference safely/,
      );
      assert.doesNotMatch(failure.stdout, new RegExp(secret));
      assert.doesNotMatch(failure.stderr, new RegExp(secret));
      await assert.rejects(stat(join(fixture.state, "built-api")), {
        code: "ENOENT",
      });
      await assert.rejects(stat(fixture.manifest), { code: "ENOENT" });
    });
  });
});

test("a tag appearing after initial preflight is refused immediately before assignment", async () => {
  await withFixture(async (fixture) => {
    await assert.rejects(
      run(fixture, [], { FAKE_APPEAR_BEFORE_ROLE: "api" }),
      /requested mutable reference appeared before build/,
    );
    await assert.rejects(stat(fixture.manifest), { code: "ENOENT" });
  });
});

test("Docker authority is fixed to an empty-config local default context", async () => {
  await withFixture(async (fixture) => {
    const hostileConfig = join(fixture.state, "hostile-docker-config");
    await mkdir(hostileConfig, { mode: 0o700 });
    await writeFile(
      join(hostileConfig, "config.json"),
      JSON.stringify({
        currentContext: "remote-attacker",
        proxies: { default: { httpProxy: "http://attacker.invalid" } },
      }),
    );
    await run(fixture, [], {
      DOCKER_HOST: "tcp://attacker.invalid:2376",
      DOCKER_CONTEXT: "remote-attacker",
      DOCKER_CONFIG: hostileConfig,
      HTTP_PROXY: "http://attacker.invalid",
      HTTPS_PROXY: "http://attacker.invalid",
      ALL_PROXY: "socks5://attacker.invalid",
      NO_PROXY: "*",
    });

    const rows = (
      await readFile(join(fixture.state, "docker-authority.log"), "utf8")
    )
      .split("\n")
      .filter(Boolean);
    assert.ok(rows.length >= 12);
    for (const row of rows) {
      const [
        dockerHost,
        dockerContext,
        dockerConfig,
        home,
        httpProxy,
        httpsProxy,
        allProxy,
        noProxy,
        configArgument,
      ] = row.split("\t");
      assert.equal(dockerHost, "");
      assert.equal(dockerContext, "");
      assert.equal(httpProxy, "");
      assert.equal(httpsProxy, "");
      assert.equal(allProxy, "");
      assert.equal(noProxy, "");
      assert.equal(dockerConfig, home);
      assert.equal(dockerConfig, configArgument);
      assert.match(dockerConfig, /^\/tmp\/sumi-dogfood-docker\./);
      assert.notEqual(dockerConfig, hostileConfig);
    }
    assert.match(
      await readFile(join(hostileConfig, "config.json"), "utf8"),
      /remote-attacker/,
    );
  });
});

test("public builder preserves parent stdin for an interactive Docker build", async () => {
  await withFixture(async (fixture) => {
    const token = "interactive-build-token";
    await setFakeControls(fixture, {
      FAKE_REQUIRE_STDIN_ROLE: "api",
      FAKE_STDIN_TOKEN: token,
    });
    const child = spawn(
      fixture.script,
      [
        "--commit",
        fixture.sha,
        "--tag",
        fixture.sha,
        "--manifest",
        fixture.manifest,
      ],
      {
        cwd: fixture.root,
        env: fixture.env,
        stdio: ["pipe", "pipe", "pipe"],
      },
    );
    const completion = collectChild(child);
    child.stdin.end(`${token}\n`);
    try {
      const result = await childCompletionWithin(
        completion,
        3_000,
        "public builder did not complete its interactive Docker build",
      );
      assert.equal(result.code, 0, result.stderr);
      await stat(fixture.manifest);
    } finally {
      if (child.exitCode === null && child.signalCode === null) {
        child.kill("SIGKILL");
      }
    }
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

test("a tracked path matched by ignore rules remains in the exact exported tree", async () => {
  await withFixture(async (fixture) => {
    await writeFile(join(fixture.root, "tracked-input.tfvars"), "nonsecret\n");
    await git(fixture.root, ["add", "--force", "tracked-input.tfvars"]);
    await refreshFixtureCommit(fixture, "track an ignored build input");

    await run(fixture);

    const manifest = JSON.parse(await readFile(fixture.manifest, "utf8"));
    assert.equal(manifest.status, "COMPLETE");
    assert.equal(manifest.source.tree, fixture.tree);
  });
});

test("the build context preserves raw blob bytes despite checkout conversion attributes", async () => {
  await withFixture(async (fixture) => {
    await writeFile(
      join(fixture.root, ".gitattributes"),
      "tracked.txt text eol=crlf\n",
    );
    await writeFile(join(fixture.root, "line\nbreak\tname"), "raw-name\n");
    await git(fixture.root, ["add", ".gitattributes", "line\nbreak\tname"]);
    await refreshFixtureCommit(fixture, "add checkout conversion policy");
    const { stdout: raw } = await git(fixture.root, [
      "cat-file",
      "blob",
      `${fixture.sha}:tracked.txt`,
    ]);
    assert.equal(raw, "pinned\n");

    await run(fixture);

    const manifest = JSON.parse(await readFile(fixture.manifest, "utf8"));
    assert.equal(manifest.source.tree, fixture.tree);
  });
});

test("raw tree limits accept exact bounds and reject one byte or entry beyond", async (t) => {
  const cases = [
    {
      name: "entry count",
      limit: TREE_LIMITS.entries,
      prepare: commitEntryCount,
      error: /raw tree entry count exceeds 2048/,
    },
    {
      name: "path bytes",
      limit: TREE_LIMITS.pathBytes,
      prepare: commitPathLength,
      error: /raw tree path exceeds 512 bytes/,
    },
    {
      name: "blob bytes",
      limit: TREE_LIMITS.blobBytes,
      prepare: commitBlobSize,
      error: /raw tree blob exceeds 4194304 bytes/,
    },
    {
      name: "aggregate bytes",
      limit: TREE_LIMITS.aggregateBytes,
      prepare: commitAggregateSize,
      error: /raw tree aggregate exceeds 67108864 bytes/,
    },
  ];
  for (const { name, limit, prepare, error } of cases) {
    await t.test(`${name}: exact bound`, async () => {
      await withFixture(async (fixture) => {
        await prepare(fixture, limit);
        await run(fixture, ["--dry-run"]);
        await assert.rejects(stat(fixture.log), { code: "ENOENT" });
      });
    });
    await t.test(`${name}: one beyond`, async () => {
      await withFixture(async (fixture) => {
        await prepare(fixture, limit + 1);
        await assert.rejects(run(fixture), error);
        await assert.rejects(stat(fixture.log), { code: "ENOENT" });
        await assert.rejects(stat(fixture.manifest), { code: "ENOENT" });
      });
    });
  }
});

test("streaming export rechecks raw blob size before Docker", async () => {
  await withFixture(async (fixture) => {
    const fakeGitBin = join(fixture.container, "mutating-git-bin");
    await mkdir(fakeGitBin);
    const { stdout: realGitStdout } = await execFileAsync("sh", [
      "-c",
      "command -v git",
    ]);
    const realGit = realGitStdout.trim();
    const { stdout: objectStdout } = await git(fixture.root, [
      "rev-parse",
      `${fixture.sha}:tracked.txt`,
    ]);
    const object = objectStdout.trim();
    await writeFile(
      join(fakeGitBin, "git"),
      `#!/usr/bin/env bash
set -euo pipefail
if [[ " $* " == *" cat-file blob ${object} "* ]]; then
  ${JSON.stringify(realGit)} "$@"
  printf x
  exit 0
fi
exec ${JSON.stringify(realGit)} "$@"
`,
      { mode: 0o755 },
    );

    await assert.rejects(
      run(fixture, [], { PATH: `${fakeGitBin}:${fixture.env.PATH}` }),
      /materialized blob size differs from raw Git tree/,
    );
    await assert.rejects(stat(fixture.log), { code: "ENOENT" });
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
  await t.test("failed cleanliness inspection", async () => {
    await withFixture(async (fixture) => {
      const fakeGitBin = join(fixture.container, "failing-status-git-bin");
      await mkdir(fakeGitBin);
      const { stdout: realGitStdout } = await execFileAsync("sh", [
        "-c",
        "command -v git",
      ]);
      const realGit = realGitStdout.trim();
      await writeFile(
        join(fakeGitBin, "git"),
        `#!/usr/bin/env bash
set -euo pipefail
if [[ " $* " == *" status --porcelain=v1 --untracked-files=all "* ]]; then
  exit 23
fi
exec ${JSON.stringify(realGit)} "$@"
`,
        { mode: 0o755 },
      );

      await assert.rejects(
        run(fixture, ["--dry-run"], {
          PATH: `${fakeGitBin}:${fixture.env.PATH}`,
        }),
        /cannot verify worktree cleanliness/,
      );
      await assert.rejects(stat(fixture.manifest), { code: "ENOENT" });
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

test("evidence parent must be owner-only", async () => {
  await withFixture(async (fixture) => {
    await chmod(fixture.state, 0o755);
    await assert.rejects(
      run(fixture, ["--dry-run"]),
      /parent must be owner-only/,
    );
  });
});

test("submodule, symlink, and Git LFS trees fail closed before export", async (t) => {
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
  for (const [name, target] of [
    ["path-escaping symlink", "../../outside-secret"],
    ["benign symlink", "tracked.txt"],
  ]) {
    await t.test(name, async () => {
      await withFixture(async (fixture) => {
        await symlink(
          target,
          join(fixture.root, `fixture-${name.replaceAll(" ", "-")}`),
        );
        await git(fixture.root, ["add", "."]);
        await refreshFixtureCommit(fixture, `add ${name}`);
        await assert.rejects(
          run(fixture, ["--dry-run"]),
          /contains symbolic links/,
        );
        await assert.rejects(stat(fixture.log), { code: "ENOENT" });
      });
    });
  }
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
        /contains filtered paths.*Git LFS/,
      );
      await assert.rejects(stat(fixture.log), { code: "ENOENT" });
    });
  });
  for (const driver of ["unset", "unspecified"]) {
    await t.test(`Git LFS pointer with filter=${driver}`, async () => {
      await withFixture(async (fixture) => {
        await writeFile(
          join(fixture.root, ".gitattributes"),
          `tracked.txt filter=${driver}\n`,
        );
        await writeFile(
          join(fixture.root, "tracked.txt"),
          [
            "version https://git-lfs.github.com/spec/v1",
            `oid sha256:${"a".repeat(64)}`,
            "size 1",
            "",
          ].join("\n"),
        );
        await git(fixture.root, ["add", ".gitattributes", "tracked.txt"]);
        await refreshFixtureCommit(fixture, `add ${driver} filter driver`);

        await assert.rejects(
          run(fixture, ["--dry-run"]),
          /contains filtered paths.*Git LFS/,
        );
        await assert.rejects(stat(fixture.log), { code: "ENOENT" });
      });
    });
  }
  await t.test("repository info attributes cannot hide Git LFS", async () => {
    await withFixture(async (fixture) => {
      await writeFile(
        join(fixture.root, ".gitattributes"),
        "tracked.txt filter=lfs\n",
      );
      await git(fixture.root, ["add", ".gitattributes"]);
      await refreshFixtureCommit(fixture, "add lfs policy");
      await writeFile(
        join(fixture.root, ".git/info/attributes"),
        "tracked.txt -filter\n",
      );

      await assert.rejects(
        run(fixture, ["--dry-run"]),
        /repository-local Git attributes are not allowed/,
      );
      await assert.rejects(stat(fixture.log), { code: "ENOENT" });
    });
  });
  await t.test(
    "repository config cannot inject a global attributes file",
    async () => {
      await withFixture(async (fixture) => {
        const ambientAttributes = join(
          fixture.state,
          "ambient-global-attributes",
        );
        await writeFile(ambientAttributes, "tracked.txt filter=lfs\n");
        await git(fixture.root, [
          "config",
          "core.attributesFile",
          ambientAttributes,
        ]);

        await run(fixture, ["--dry-run"]);
        await assert.rejects(stat(fixture.log), { code: "ENOENT" });
      });
    },
  );
});

test("wrong immutable image labels fail closed", async () => {
  await withFixture(async (fixture) => {
    await assert.rejects(
      run(fixture, [], { FAKE_BAD_LABEL_ROLE: "agent" }),
      /wrong revision label/,
    );
    await assert.rejects(stat(fixture.manifest), { code: "ENOENT" });
  });
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

test("immutable-IID inspection times out without exposing Docker output", async () => {
  await withFixture(async (fixture) => {
    const secret = "must-not-escape-iid-inspect-stderr";
    await setFakeControls(fixture, {
      FAKE_HANG_IID_ROLE: "api",
      FAKE_DOCKER_SECRET: secret,
    });
    const started = Date.now();
    let failure;
    try {
      await execFileAsync(
        fixture.script,
        [
          "--commit",
          fixture.sha,
          "--tag",
          fixture.sha,
          "--manifest",
          fixture.manifest,
        ],
        {
          cwd: fixture.root,
          env: fixture.env,
          timeout: 9_000,
        },
      );
      assert.fail("non-returning immutable-IID inspection unexpectedly passed");
    } catch (error) {
      failure = error;
    }
    assert.ok(
      Date.now() - started < 8_000,
      "IID inspection timeout was not bounded",
    );
    assert.match(
      failure.stderr,
      /Docker inspection exceeded its hard deadline/,
    );
    assert.doesNotMatch(failure.stderr, new RegExp(secret));
    assert.doesNotMatch(failure.stdout, new RegExp(secret));
    await assertHangingDockerWasKilled(fixture);
    await assert.rejects(stat(fixture.manifest), { code: "ENOENT" });
  });
});

test("post-SIGKILL process-group waiting is bounded and fails closed", async () => {
  const moduleUrl = pathToFileURL(sourceDockerWrapper).href;
  const program = `
    import assert from "node:assert/strict";
    import process from "node:process";
    import { terminateProcessGroup } from ${JSON.stringify(moduleUrl)};

    const signals = [];
    process.kill = (pid, signal) => {
      assert.equal(pid, -4242);
      signals.push(signal);
      return true;
    };
    await assert.rejects(
      terminateProcessGroup({ pid: 4242 }),
      /Docker process group did not exit after SIGKILL/,
    );
    assert.deepEqual(signals.slice(0, 2), ["SIGTERM", "SIGKILL"]);
    assert.ok(signals.slice(2).length > 0);
    assert.ok(signals.slice(2).every((signal) => signal === 0));
    process.stdout.write("bounded process-group failure observed\\n");
  `;
  const result = await execFileAsync(
    process.execPath,
    ["--input-type=module", "--eval", program],
    { timeout: 5_000 },
  );
  assert.match(result.stdout, /bounded process-group failure observed/);
});

test("external SIGTERM kills bounded inspection and removes private config", async () => {
  await withFixture(async (fixture) => {
    const secret = "must-not-escape-cancelled-inspection";
    await setFakeControls(fixture, {
      FAKE_HANG_IID_ROLE: "api",
      FAKE_DOCKER_SECRET: secret,
    });
    const wrapper = join(fixture.root, "scripts/operations/dogfood-docker.mjs");
    const immutableId = `sha256:${"a".repeat(64)}`;
    const child = spawn(
      wrapper,
      [
        "verify-iid",
        immutableId,
        fixture.sha,
        "https://github.com/sumi-studio/sumi",
      ],
      {
        cwd: fixture.root,
        env: fixture.env,
        stdio: ["ignore", "pipe", "pipe"],
      },
    );
    const completion = collectChild(child);
    let dockerPid;
    let configDirectory;
    try {
      dockerPid = Number(
        (await waitForFile(join(fixture.state, "hanging-docker.pid"))).trim(),
      );
      const authorityRows = (
        await waitForFile(join(fixture.state, "docker-authority.log"))
      )
        .split("\n")
        .filter(Boolean);
      configDirectory = authorityRows.at(-1).split("\t").at(-1);
      assert.match(configDirectory, /^\/tmp\/sumi-dogfood-docker\./);

      child.kill("SIGTERM");
      const result = await completion;
      assert.notEqual(result.code, 0);
      assert.doesNotMatch(result.stdout, new RegExp(secret));
      assert.doesNotMatch(result.stderr, new RegExp(secret));
      assert.throws(
        () => process.kill(dockerPid, 0),
        (error) => error?.code === "ESRCH",
        `Docker process ${dockerPid} survived external cancellation`,
      );
      await assert.rejects(stat(configDirectory), { code: "ENOENT" });
    } finally {
      if (child.exitCode === null && child.signalCode === null) {
        child.kill("SIGKILL");
      }
      if (Number.isInteger(dockerPid)) {
        try {
          process.kill(-dockerPid, "SIGKILL");
        } catch {}
      }
      if (configDirectory?.startsWith("/tmp/sumi-dogfood-docker.")) {
        await rm(configDirectory, { recursive: true, force: true });
      }
    }
  });
});

test("inspection cancellation kills a redirected same-group helper after leader exit", async () => {
  await withFixture(async (fixture) => {
    await setFakeControls(fixture, { FAKE_CANCEL_INSPECT_ROLE: "api" });
    const wrapper = join(fixture.root, "scripts/operations/dogfood-docker.mjs");
    const immutableId = `sha256:${"a".repeat(64)}`;
    const child = spawn(
      wrapper,
      [
        "verify-iid",
        immutableId,
        fixture.sha,
        "https://github.com/sumi-studio/sumi",
      ],
      {
        cwd: fixture.root,
        env: fixture.env,
        stdio: ["ignore", "pipe", "pipe"],
      },
    );
    const completion = collectChild(child);
    let leaderPid;
    let helperPid;
    let wrapperPid;
    let configDirectory;
    let processGroupGone = false;
    try {
      leaderPid = Number(
        (
          await waitForFile(join(fixture.state, "cancel-inspect-leader.pid"))
        ).trim(),
      );
      helperPid = Number(
        (
          await waitForFile(join(fixture.state, "cancel-inspect-helper.pid"))
        ).trim(),
      );
      wrapperPid = Number(
        (
          await waitForFile(join(fixture.state, "cancel-inspect-wrapper.pid"))
        ).trim(),
      );
      const authorityRows = (
        await waitForFile(join(fixture.state, "docker-authority.log"))
      )
        .split("\n")
        .filter(Boolean);
      configDirectory = authorityRows.at(-1).split("\t").at(-1);

      assert.equal(wrapperPid, child.pid);
      child.kill("SIGTERM");
      const result = await childCompletionWithin(
        completion,
        1_500,
        "inspection wrapper did not complete external cancellation",
      );
      assert.notEqual(result.code, 0);
      await new Promise((resolvePromise) => setTimeout(resolvePromise, 400));
      assertProcessGone(leaderPid, "Docker inspection leader");
      assertProcessGone(helperPid, "redirected same-group inspection helper");
      processGroupGone = true;
      await assert.rejects(stat(configDirectory), { code: "ENOENT" });
    } finally {
      if (!processGroupGone) {
        if (child.exitCode === null && child.signalCode === null) {
          killProcess(child.pid);
        }
        killProcess(wrapperPid);
        killProcessGroup(leaderPid);
      }
      await Promise.race([
        completion.catch(() => {}),
        new Promise((resolvePromise) => setTimeout(resolvePromise, 500)),
      ]);
      if (configDirectory?.startsWith("/tmp/sumi-dogfood-docker.")) {
        await rm(configDirectory, { recursive: true, force: true });
      }
    }
  });
});

test("build cancellation kills a same-group helper after the Docker leader exits", async () => {
  await withFixture(async (fixture) => {
    await setFakeControls(fixture, { FAKE_CANCEL_BUILD_ROLE: "api" });
    const wrapper = join(fixture.root, "scripts/operations/dogfood-docker.mjs");
    const child = spawn(
      wrapper,
      [
        "build",
        "--iidfile",
        join(fixture.state, "cancelled.iid"),
        "--file",
        join(fixture.root, "deploy/api/Dockerfile"),
        "--tag",
        `ghcr.io/sumi-studio/sumi-api:${fixture.sha}`,
        fixture.root,
      ],
      {
        cwd: fixture.root,
        env: fixture.env,
        stdio: ["ignore", "pipe", "pipe"],
      },
    );
    const completion = collectChild(child);
    let leaderPid;
    let helperPid;
    let wrapperPid;
    let configDirectory;
    let processGroupGone = false;
    try {
      leaderPid = Number(
        (
          await waitForFile(join(fixture.state, "cancel-build-leader.pid"))
        ).trim(),
      );
      helperPid = Number(
        (
          await waitForFile(join(fixture.state, "cancel-build-helper.pid"))
        ).trim(),
      );
      wrapperPid = Number(
        (
          await waitForFile(join(fixture.state, "cancel-build-wrapper.pid"))
        ).trim(),
      );
      const authorityRows = (
        await waitForFile(join(fixture.state, "docker-authority.log"))
      )
        .split("\n")
        .filter(Boolean);
      configDirectory = authorityRows.at(-1).split("\t").at(-1);

      assert.equal(wrapperPid, child.pid);
      child.kill("SIGTERM");
      const result = await childCompletionWithin(
        completion,
        2_000,
        "Docker wrapper did not finish after cancelling the complete build process group",
      );
      assert.equal(result.code, 143);
      assertProcessGone(leaderPid, "Docker leader");
      assertProcessGone(helperPid, "same-group Docker helper");
      processGroupGone = true;
      await assert.rejects(stat(configDirectory), { code: "ENOENT" });
    } finally {
      if (!processGroupGone) {
        if (child.exitCode === null && child.signalCode === null) {
          killProcess(child.pid);
        }
        killProcess(wrapperPid);
        killProcessGroup(leaderPid);
      }
      await Promise.race([
        completion.catch(() => {}),
        new Promise((resolvePromise) => setTimeout(resolvePromise, 500)),
      ]);
      if (configDirectory?.startsWith("/tmp/sumi-dogfood-docker.")) {
        await rm(configDirectory, { recursive: true, force: true });
      }
    }
  });
});

test("public builder forwards SIGTERM and waits for the Docker process group", async () => {
  await withFixture(async (fixture) => {
    await setFakeControls(fixture, { FAKE_CANCEL_BUILD_ROLE: "api" });
    const child = spawn(
      fixture.script,
      [
        "--commit",
        fixture.sha,
        "--tag",
        fixture.sha,
        "--manifest",
        fixture.manifest,
      ],
      {
        cwd: fixture.root,
        env: fixture.env,
        stdio: ["ignore", "pipe", "pipe"],
      },
    );
    const completion = collectChild(child);
    let leaderPid;
    let helperPid;
    let wrapperPid;
    let configDirectory;
    let processesGone = false;
    try {
      leaderPid = Number(
        (
          await waitForFile(join(fixture.state, "cancel-build-leader.pid"))
        ).trim(),
      );
      helperPid = Number(
        (
          await waitForFile(join(fixture.state, "cancel-build-helper.pid"))
        ).trim(),
      );
      wrapperPid = Number(
        (
          await waitForFile(join(fixture.state, "cancel-build-wrapper.pid"))
        ).trim(),
      );
      const authorityRows = (
        await waitForFile(join(fixture.state, "docker-authority.log"))
      )
        .split("\n")
        .filter(Boolean);
      configDirectory = authorityRows.at(-1).split("\t").at(-1);

      child.kill("SIGTERM");
      const result = await childCompletionWithin(
        completion,
        2_500,
        "public builder did not await cancellation of its Docker wrapper",
      );
      assert.equal(result.code, 143);
      assertProcessGone(wrapperPid, "Docker wrapper");
      assertProcessGone(leaderPid, "Docker leader");
      assertProcessGone(helperPid, "same-group Docker helper");
      processesGone = true;
      await assert.rejects(stat(configDirectory), { code: "ENOENT" });
      await assert.rejects(stat(fixture.manifest), { code: "ENOENT" });
    } finally {
      if (!processesGone) {
        if (child.exitCode === null && child.signalCode === null) {
          killProcess(child.pid);
        }
        killProcess(wrapperPid);
        killProcessGroup(leaderPid);
      }
      await Promise.race([
        completion.catch(() => {}),
        new Promise((resolvePromise) => setTimeout(resolvePromise, 500)),
      ]);
      if (configDirectory?.startsWith("/tmp/sumi-dogfood-docker.")) {
        await rm(configDirectory, { recursive: true, force: true });
      }
    }
  });
});

test("immutable-IID inspection bounds stdout and stderr without leaking either", async (t) => {
  for (const stream of ["STDOUT", "STDERR"]) {
    await t.test(stream.toLowerCase(), async () => {
      await withFixture(async (fixture) => {
        const secret = `must-not-escape-large-${stream.toLowerCase()}`;
        let failure;
        try {
          await run(fixture, [], {
            [`FAKE_FLOOD_${stream}_ROLE`]: "api",
            FAKE_DOCKER_SECRET: secret,
          });
          assert.fail(`oversized Docker ${stream} unexpectedly passed`);
        } catch (error) {
          failure = error;
        }
        assert.match(failure.stderr, /exceeded its output limit/);
        assert.doesNotMatch(failure.stderr, new RegExp(secret));
        assert.doesNotMatch(failure.stdout, new RegExp(secret));
        await assert.rejects(stat(fixture.manifest), { code: "ENOENT" });
      });
    });
  }
});

test("retag after API build cannot falsify immutable COMPLETE evidence", async () => {
  await withFixture(async (fixture) => {
    await run(fixture, [], { FAKE_RETAG_AFTER_API: "1" });
    const manifest = JSON.parse(await readFile(fixture.manifest, "utf8"));
    assert.equal(manifest.status, "COMPLETE");
    assert.equal(manifest.mutable_handles_authoritative, false);
    await assert.rejects(
      execFileAsync(
        join(fixture.root, "scripts/operations/verify-dogfood-image-bindings"),
        [
          "--manifest",
          fixture.manifest,
          "--commit",
          fixture.sha,
          "--tree",
          fixture.tree,
          "--tag",
          fixture.sha,
        ],
        {
          cwd: fixture.root,
          env: fixture.env,
        },
      ),
      /does not match immutable IID/,
    );
  });
});

test("live binding rejects four tags aliased to one component IID", async () => {
  await withFixture(async (fixture) => {
    await run(fixture);
    await setFakeControls(fixture, { FAKE_ALIAS_ALL_BINDINGS: "1" });
    await assert.rejects(
      execFileAsync(
        join(fixture.root, "scripts/operations/verify-dogfood-image-bindings"),
        [
          "--manifest",
          fixture.manifest,
          "--commit",
          fixture.sha,
          "--tree",
          fixture.tree,
          "--tag",
          fixture.sha,
        ],
        {
          cwd: fixture.root,
          env: fixture.env,
        },
      ),
      /does not match immutable IID/,
    );
  });
});

test("live binding inspection times out without exposing Docker output", async () => {
  await withFixture(async (fixture) => {
    await run(fixture);
    const secret = "must-not-escape-docker-stderr";
    await setFakeControls(fixture, {
      FAKE_HANG_BINDING_ROLE: "agent",
      FAKE_DOCKER_SECRET: secret,
    });
    const started = Date.now();
    let failure;
    try {
      await execFileAsync(
        join(fixture.root, "scripts/operations/verify-dogfood-image-bindings"),
        [
          "--manifest",
          fixture.manifest,
          "--commit",
          fixture.sha,
          "--tree",
          fixture.tree,
          "--tag",
          fixture.sha,
        ],
        {
          cwd: fixture.root,
          env: fixture.env,
        },
      );
      assert.fail("non-returning Docker inspection unexpectedly passed");
    } catch (error) {
      failure = error;
    }
    assert.ok(Date.now() - started < 10_000, "binding timeout was not bounded");
    assert.match(
      failure.stderr,
      /cannot inspect requested reference safely \(timeout\)/,
    );
    assert.doesNotMatch(failure.stderr, /requested reference is absent/);
    assert.doesNotMatch(failure.stderr, new RegExp(secret));
    assert.doesNotMatch(failure.stdout, new RegExp(secret));
    await assertHangingDockerWasKilled(fixture);
  });
});

test("live binding distinguishes verified absence from inspection unavailability", async (t) => {
  await t.test("verified absence", async () => {
    await withFixture(async (fixture) => {
      await run(fixture);
      await rm(join(fixture.state, "built-agent"));
      await assert.rejects(
        execFileAsync(
          join(
            fixture.root,
            "scripts/operations/verify-dogfood-image-bindings",
          ),
          [
            "--manifest",
            fixture.manifest,
            "--commit",
            fixture.sha,
            "--tree",
            fixture.tree,
            "--tag",
            fixture.sha,
          ],
          { cwd: fixture.root, env: fixture.env },
        ),
        (error) => {
          assert.match(error.stderr, /requested reference is absent/);
          assert.doesNotMatch(
            error.stderr,
            /cannot inspect requested reference safely/,
          );
          return true;
        },
      );
    });
  });

  await t.test("Docker CLI unavailable", async () => {
    await withFixture(async (fixture) => {
      await run(fixture);
      const emptyPath = join(fixture.state, "empty-path");
      await mkdir(emptyPath);
      await assert.rejects(
        execFileAsync(
          process.execPath,
          [
            join(
              fixture.root,
              "scripts/operations/verify-dogfood-image-bindings",
            ),
            "--manifest",
            fixture.manifest,
            "--commit",
            fixture.sha,
            "--tree",
            fixture.tree,
            "--tag",
            fixture.sha,
          ],
          {
            cwd: fixture.root,
            env: { ...fixture.env, PATH: emptyPath },
          },
        ),
        (error) => {
          assert.match(
            error.stderr,
            /cannot inspect requested reference safely \(unavailable\)/,
          );
          assert.doesNotMatch(error.stderr, /requested reference is absent/);
          return true;
        },
      );
    });
  });
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

test("Git replacement and inherited Git injection cannot relabel alternate bytes", async () => {
  await withFixture(async (fixture) => {
    const accepted = fixture.sha;
    const acceptedTree = fixture.tree;
    await writeFile(join(fixture.root, "tracked.txt"), "alternate\n");
    await git(fixture.root, ["add", "tracked.txt"]);
    await git(fixture.root, ["commit", "--quiet", "-m", "alternate tree"]);
    const { stdout } = await git(fixture.root, ["rev-parse", "HEAD"]);
    const alternate = stdout.trim();
    await git(fixture.root, ["replace", accepted, alternate]);
    await git(fixture.root, ["checkout", "--detach", "--force", accepted]);
    assert.equal(
      await readFile(join(fixture.root, "tracked.txt"), "utf8"),
      "alternate\n",
    );
    await assert.rejects(run(fixture, ["--dry-run"]), /worktree is not clean/);
    await assert.rejects(stat(fixture.manifest), { code: "ENOENT" });

    await execFileAsync("git", ["checkout", "--detach", "--force", accepted], {
      cwd: fixture.root,
      env: { ...process.env, GIT_NO_REPLACE_OBJECTS: "1" },
    });
    assert.equal(
      await readFile(join(fixture.root, "tracked.txt"), "utf8"),
      "pinned\n",
    );
    await run(fixture, [], {
      GIT_DIR: join(fixture.state, "spoof.git"),
      GIT_WORK_TREE: fixture.state,
      GIT_INDEX_FILE: join(fixture.state, "spoof.index"),
      GIT_OBJECT_DIRECTORY: join(fixture.state, "spoof.objects"),
      GIT_ALTERNATE_OBJECT_DIRECTORIES: join(
        fixture.state,
        "alternate.objects",
      ),
      GIT_GRAFT_FILE: join(fixture.state, "spoof.grafts"),
      GIT_NAMESPACE: "spoof",
      GIT_CONFIG_COUNT: "1",
      GIT_CONFIG_KEY_0: "core.worktree",
      GIT_CONFIG_VALUE_0: fixture.state,
      GIT_CONFIG_SYSTEM: join(fixture.state, "spoof.system.config"),
      GIT_CONFIG_GLOBAL: join(fixture.state, "spoof.global.config"),
    });
    const manifest = JSON.parse(await readFile(fixture.manifest, "utf8"));
    assert.equal(manifest.source.revision, accepted);
    assert.equal(manifest.source.tree, acceptedTree);
  });
});

test("closed manifest validation rejects extras, duplicate roles, and malformed IIDs", async (t) => {
  await withFixture(async (fixture) => {
    await run(fixture);
    const original = JSON.parse(await readFile(fixture.manifest, "utf8"));
    const validator = join(
      fixture.root,
      "scripts/operations/verify-dogfood-image-bindings",
    );
    for (const [name, mutate, pattern] of [
      [
        "unknown key",
        (value) => {
          value.extra = true;
        },
        /missing or unknown keys/,
      ],
      [
        "duplicate role",
        (value) => {
          value.images[1].role = "api";
        },
        /duplicate, missing, or unknown/,
      ],
      [
        "missing role",
        (value) => {
          value.images.pop();
        },
        /exactly four roles/,
      ],
      [
        "extra role",
        (value) => {
          value.images.push({ ...value.images[0], role: "worker" });
        },
        /exactly four roles/,
      ],
      [
        "malformed IID",
        (value) => {
          value.images[2].id = "sha256:abc";
        },
        /non-canonical immutable IID/,
      ],
      [
        "duplicate IID",
        (value) => {
          value.images[1].id = value.images[0].id;
        },
        /duplicate immutable IID/,
      ],
      [
        "wrong tree",
        (value) => {
          value.source.tree = "f".repeat(40);
        },
        /source identity/,
      ],
    ]) {
      await t.test(name, async () => {
        const candidate = structuredClone(original);
        mutate(candidate);
        const path = join(fixture.state, `${name.replaceAll(" ", "-")}.json`);
        await writeFile(path, `${JSON.stringify(candidate)}\n`, {
          mode: 0o600,
        });
        await assert.rejects(
          execFileAsync(
            validator,
            [
              "--manifest",
              path,
              "--commit",
              fixture.sha,
              "--tree",
              fixture.tree,
              "--tag",
              fixture.sha,
              "--manifest-only",
            ],
            { cwd: fixture.root, env: fixture.env },
          ),
          pattern,
        );
      });
    }
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
      schema_version: 2,
      status: "COMPLETE",
      evidence_scope: "IMMUTABLE_IMAGE_IDS_AND_BUILD_INPUTS_ONLY",
      mutable_handles_authoritative: false,
      source: {
        repository: "https://github.com/sumi-studio/sumi",
        revision: fixture.sha,
        tree: fixture.tree,
      },
      build: { context_tree: fixture.tree, requested_tag: fixture.sha },
      images: ["api", "agent", "provisioner", "web"].map((role) => ({
        role,
        id: `sha256:${hexes[role].repeat(64)}`,
        dockerfile: `deploy/${role}/Dockerfile`,
        requested_reference: `${repositories[role]}:${fixture.sha}`,
        labels: {
          "org.opencontainers.image.revision": fixture.sha,
          "org.opencontainers.image.source":
            "https://github.com/sumi-studio/sumi",
        },
      })),
    });
    assert.equal((await stat(fixture.manifest)).mode & 0o777, 0o600);
    assert.equal((await stat(fixture.manifest)).nlink, 1);
    const validated = await execFileAsync(
      join(fixture.root, "scripts/operations/verify-dogfood-image-bindings"),
      [
        "--manifest",
        fixture.manifest,
        "--commit",
        fixture.sha,
        "--tree",
        fixture.tree,
        "--tag",
        fixture.sha,
        "--manifest-only",
      ],
      { cwd: fixture.root, env: fixture.env },
    );
    assert.match(validated.stdout, /immutable evidence validated/);
  });
});

test("root operability and the runbook expose the supported build contract", async () => {
  const packageJson = JSON.parse(
    await readFile(join(sourceRoot, "package.json"), "utf8"),
  );
  assert.match(
    packageJson.scripts["test:operability"],
    /scripts\/operations\/build-dogfood-images\.test\.mjs/,
  );
  assert.equal(packageJson.engines.node, ">=20.19");

  const runbook = await readFile(
    join(sourceRoot, "docs/operations/immutable-dogfood-image-build.md"),
    "utf8",
  );
  assert.match(runbook, /Linux-only/);
  assert.match(runbook, /Node\.js 20\.19 or newer/);
  assert.match(runbook, /local Docker `default` context/);
  assert.match(runbook, /does not attest reproducible\s+dependency/);
  assert.match(runbook, /operational exclusivity/);
  assert.match(runbook, /non-atomic race/);
});
