import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import {
  chmod,
  mkdir,
  mkdtemp,
  readFile,
  rm,
  stat,
  symlink,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const repositoryRoot = resolve(
  dirname(fileURLToPath(import.meta.url)),
  "../..",
);

async function source(path) {
  return readFile(resolve(repositoryRoot, path), "utf8");
}

test("Compose pulls published Sumi images from GHCR", async () => {
  const [
    local,
    realFirebase,
    agent,
    agentPrepare,
    firebase,
    composeLauncher,
    supervisor,
    firebaseCheck,
  ] = await Promise.all([
    source("deploy/local/compose.dev.yaml"),
    source("deploy/local/compose.real-firebase.yaml"),
    source("deploy/agent/compose.yaml"),
    source("deploy/agent/compose.prepare.yaml"),
    source("deploy/firebase/compose.yaml"),
    source("scripts/dev/compose-stack"),
    source("deploy/agent/supervisor"),
    source("scripts/dev/firebase-auth-emulator-check"),
  ]);

  for (const compose of [local, realFirebase, agent, agentPrepare, firebase]) {
    assert.doesNotMatch(compose, /^\s+build:/m);
    const ghcrImages =
      compose.match(/^\s+image: ghcr\.io\/sumi-studio\//gm) ?? [];
    // Every GHCR image is pulled on start. The per-agent compose files let a
    // developer opt into reusing a pinned local build
    // (SUMI_AGENT_IMAGE_PULL_POLICY, default always); production keeps always.
    const alwaysPulls =
      compose.match(
        /^\s+pull_policy: (?:always|\$\{SUMI_AGENT_IMAGE_PULL_POLICY:-always\})$/gm,
      ) ?? [];
    assert.ok(ghcrImages.length > 0);
    assert.equal(alwaysPulls.length, ghcrImages.length);
  }

  assert.match(local, /sumi-api:\$\{SUMI_API_IMAGE_TAG:-latest\}/);
  assert.match(
    local,
    /sumi-provisioner:\$\{SUMI_PROVISIONER_IMAGE_TAG:-latest\}/,
  );
  assert.match(local, /sumi-web:\$\{SUMI_WEB_IMAGE_TAG:-latest\}/);
  assert.match(local, /sumi-firebase:\$\{SUMI_FIREBASE_IMAGE_TAG:-latest\}/);
  assert.match(realFirebase, /sumi-api:\$\{SUMI_API_IMAGE_TAG:-latest\}/);
  assert.match(agent, /sumi-agent:\$\{SUMI_AGENT_IMAGE_TAG:-latest\}/);
  assert.match(firebase, /sumi-firebase:\$\{SUMI_FIREBASE_IMAGE_TAG:-latest\}/);
  assert.doesNotMatch(composeLauncher, /--build/);
  assert.doesNotMatch(supervisor, /--build/);
  assert.match(firebaseCheck, /docker build/);
  assert.match(firebaseCheck, /local-check-/);
  assert.match(firebaseCheck, /up --detach --pull never/);
});

test("the API image and runbook bind the offline schema-30 rollback operator", async () => {
  const [dockerfile, rollbackRunbook] = await Promise.all([
    source("deploy/api/Dockerfile"),
    source("docs/schema30-prewrite-rollback.md"),
  ]);

  assert.match(
    dockerfile,
    /go build -o \/usr\/local\/bin\/sumi-schema30-rollback \.\/cmd\/schema30-rollback/,
  );
  assert.match(
    dockerfile,
    /COPY --from=build \/usr\/local\/bin\/sumi-schema30-rollback \/usr\/local\/bin\/sumi-schema30-rollback/,
  );

  const commandBlocks = [
    ...rollbackRunbook.matchAll(/```bash\n([\s\S]*?)```/g),
  ].map((match) => match[1]);
  assert.equal(commandBlocks.length, 2);
  for (const command of commandBlocks) {
    assert.match(command, /^set \+x\nset -euo pipefail$/m);
    assert.match(command, /^set -euo pipefail$/m);
    assert.match(command, /\^sha256:\[0-9a-f\]\{64\}\$/);
    assert.match(command, /\[\[ -v SUMI_DB_URL \]\]/);
    assert.doesNotMatch(command, /\$\{SUMI_DB_URL/);
    assert.match(command, /docker image inspect --format/);
    assert.match(command, /docker network inspect/);
    assert.ok(
      command.indexOf("docker image inspect") < command.indexOf("docker run"),
    );
  }
});

test("runtime provisioner receives a file-scoped Docker config", async () => {
  const [local, provisionerDockerfile] = await Promise.all([
    source("deploy/local/compose.dev.yaml"),
    source("deploy/provisioner/Dockerfile"),
  ]);
  const provisioner = local.slice(
    local.indexOf("  runtime-provisioner:"),
    local.indexOf("\n  web:"),
  );

  assert.match(provisioner, /DOCKER_CONFIG: \/run\/sumi\/docker-config/);
  assert.match(
    provisioner,
    /source: \$\{SUMI_DOCKER_CONFIG_FILE:\?SUMI_DOCKER_CONFIG_FILE is required\}/,
  );
  assert.match(provisioner, /target: \/run\/sumi\/docker-config\/config\.json/);
  assert.match(provisioner, /read_only: true/);
  assert.match(provisioner, /create_host_path: false/);
  assert.doesNotMatch(provisioner, /\/root\/\.docker/);
  assert.match(
    provisionerDockerfile,
    /install -d -m 0700 \/run\/sumi\/docker-config/,
  );
});

test("the local media server is opt-in and carries no repository credential", async () => {
  const [local, launcher] = await Promise.all([
    source("deploy/local/compose.dev.yaml"),
    source("scripts/dev/compose-stack"),
  ]);
  const livekit = local.slice(
    local.indexOf("  livekit:"),
    local.indexOf("\n  runtime-provisioner:"),
  );

  // Nothing here may sign a room token: every LiveKit credential falls back to
  // empty, and the service exists only in the profile the launcher enables for
  // exactly the credential pair the API also requires.
  for (const [, fallback] of local.matchAll(
    /\$\{SUMI_LIVEKIT_API_(?:KEY|SECRET):-([^}]*)\}/g,
  )) {
    assert.equal(fallback, "");
  }
  assert.match(livekit, /^\s+profiles: \["calls"\]$/m);
  assert.match(
    livekit,
    /\$\{SUMI_LIVEKIT_API_KEY:-\}: \$\{SUMI_LIVEKIT_API_SECRET:-\}/,
  );
  assert.match(
    livekit,
    /SUMI_LIVEKIT_API_KEY and SUMI_LIVEKIT_API_SECRET are required/,
  );
  assert.match(livekit, /:7880:7880/);
  assert.match(livekit, /:7881:7881/);
  assert.match(livekit, /:7882:7882\/udp/);
  assert.match(launcher, /COMPOSE_PROFILES_ARGUMENTS=\(--profile calls\)/);
  assert.match(
    launcher,
    /fail "SUMI_LIVEKIT_API_KEY and SUMI_LIVEKIT_API_SECRET must be set together"/,
  );
});

test("Jenkins rebuilds the provisioner for every source tree embedded in it", async () => {
  const [jenkinsfile, provisionerDockerfile] = await Promise.all([
    source("Jenkinsfile"),
    source("deploy/provisioner/Dockerfile"),
  ]);

  assert.match(provisionerDockerfile, /COPY apps\/api\/ \.\//);
  assert.match(
    provisionerDockerfile,
    /COPY --from=build \/usr\/local\/bin\/sumi-runtime-provisioner/,
  );
  assert.match(provisionerDockerfile, /\/opt\/sumi\/deploy\/agent\/supervisor/);
  assert.match(jenkinsfile, /provisioner:\s*\['apps\/api', 'deploy\/agent'\]/);
  assert.match(
    jenkinsfile,
    /watchedDirs\.addAll\(extraWatchedDirsByImage\[name\] \?: \[\]\)/,
  );
});

test("Compose gives runtime only the logical executor workspace address", async () => {
  const [compose, entrypoint] = await Promise.all([
    source("deploy/agent/compose.yaml"),
    source("deploy/agent/container-entrypoint"),
  ]);
  const runtime = compose.slice(
    compose.indexOf("  runtime:"),
    compose.indexOf("\n  executor:"),
  );
  assert.match(runtime, /SUMI_WORKSPACE: \/workspace/);
  assert.doesNotMatch(runtime, /workspace:\/workspace/);

  const runtimeEntrypoint = entrypoint.slice(
    entrypoint.indexOf("  runtime)"),
    entrypoint.indexOf("\n  executor)"),
  );
  assert.match(runtimeEntrypoint, /SUMI_WORKSPACE \\/);
  assert.match(runtimeEntrypoint, /"SUMI_WORKSPACE=\$\{SUMI_WORKSPACE\}"/);
  assert.match(
    runtimeEntrypoint,
    /exec env -i "\$\{runtime_environment\[@\]\}"/,
  );
});

test("the supported launcher gates API, executor, runtime Ready, then Vite", async () => {
  const launcher = await source("scripts/dev/real-stack");
  const apiStart = launcher.indexOf('log "starting API"');
  const apiGate = launcher.search(
    /wait_for_http "\$\{API_ORIGIN\}\/health" "\$\{API_PID\}" "API"/,
  );
  const executorStart = launcher.indexOf(
    'log "starting authenticated tool executor"',
  );
  const executorGate = launcher.search(
    /wait_for_socket "\$\{EXECUTOR_SOCKET\}" "\$\{EXECUTOR_PID\}"/,
  );
  const runtimeEnvironmentStart = launcher.indexOf(
    "declare -a runtime_environment=(",
  );
  const runtimeStart = launcher.indexOf(
    'log "starting production PersonalityAgent"',
  );
  const runtimeGate = launcher.indexOf("wait_for_runtime_ready \\");
  const viteStart = launcher.search(/log "starting Vite at \$\{WEB_ORIGIN\}"/);

  for (const position of [
    apiStart,
    apiGate,
    executorStart,
    executorGate,
    runtimeEnvironmentStart,
    runtimeStart,
    runtimeGate,
    viteStart,
  ]) {
    assert.notEqual(position, -1);
  }
  assert.ok(apiGate < executorStart);
  assert.ok(executorStart < executorGate);
  assert.ok(executorGate < runtimeEnvironmentStart);
  assert.ok(runtimeEnvironmentStart < runtimeStart);
  assert.ok(runtimeStart < runtimeGate);
  assert.ok(runtimeGate < viteStart);
  const executorBlock = launcher.slice(executorStart, runtimeEnvironmentStart);
  const runtimeBlock = launcher.slice(runtimeEnvironmentStart, viteStart);
  assert.match(launcher, /generateKeyPairSync\("ed25519"\)/);
  assert.match(launcher, /embeddedPublic\.equals\(publicBytes\)/);
  assert.match(
    executorBlock,
    /SUMI_EXECUTOR_CALL_AUTHORITY_PUBLIC_KEY=\$\{SUMI_EXECUTOR_CALL_AUTHORITY_PUBLIC_KEY\}/,
  );
  assert.doesNotMatch(
    executorBlock,
    /SUMI_EXECUTOR_CALL_AUTHORITY_PRIVATE_KEY=\$\{/,
  );
  assert.match(
    runtimeBlock,
    /SUMI_EXECUTOR_CALL_AUTHORITY_PRIVATE_KEY=\$\{SUMI_EXECUTOR_CALL_AUTHORITY_PRIVATE_KEY\}/,
  );
  assert.doesNotMatch(
    runtimeBlock,
    /SUMI_EXECUTOR_CALL_AUTHORITY_PUBLIC_KEY=\$\{/,
  );
  assert.match(launcher, /state\.local_control\?\.state === "ready"/);
  assert.match(launcher, /state\.local_control\?\.integrity\?\.mac/);
  assert.match(
    launcher,
    /SUMI_AUTH_PERSONALITY_AGENT_ID must equal SUMI_PERSONALITY_AGENT_ID/,
  );
  assert.match(launcher, /SUMI_BROWSER_SESSION_AUDIENCE=sumi:web/);
  assert.match(launcher, /SUMI_BROWSER_WS_ALLOWED_ORIGINS=\$\{WEB_ORIGIN\}/);
  assert.match(launcher, /SUMI_AUTH_ALLOW_INSECURE_COOKIES=true/);
  const apiBlock = launcher.slice(apiStart, executorStart);
  for (const [apiVariable, launcherVariable] of [
    [
      "SUMI_MESSAGING_ATTACHMENT_WORKSPACE_QUOTA_BYTES",
      "MESSAGING_ATTACHMENT_WORKSPACE_QUOTA_BYTES",
    ],
    [
      "SUMI_MESSAGING_ATTACHMENT_WORKSPACE_QUOTA_OBJECTS",
      "MESSAGING_ATTACHMENT_WORKSPACE_QUOTA_OBJECTS",
    ],
    [
      "SUMI_MESSAGING_ATTACHMENT_TOTAL_QUOTA_BYTES",
      "MESSAGING_ATTACHMENT_TOTAL_QUOTA_BYTES",
    ],
    [
      "SUMI_MESSAGING_ATTACHMENT_TOTAL_QUOTA_OBJECTS",
      "MESSAGING_ATTACHMENT_TOTAL_QUOTA_OBJECTS",
    ],
  ]) {
    assert.match(
      apiBlock,
      new RegExp(`"${apiVariable}=\\$\\{${launcherVariable}\\}"`),
    );
  }
  assert.match(launcher, /"SUMI_PUBLIC_LISTEN=\$\{SUMI_PUBLIC_LISTEN\}"/);
  assert.match(launcher, /100\.64\.0\.0\/10/);
  assert.match(
    launcher,
    /configuration file must not be readable by group or other/,
  );
  assert.match(launcher, /url\.hostname !== publicHost/);
  assert.match(
    launcher,
    /assert_exact_tcp_listener "\$\{SUMI_PUBLIC_LISTEN\}" "\$\{API_PORT\}" "API"/,
  );
  assert.match(
    launcher,
    /SUMI_GATEWAY_URL=ws:\/\/\$\{LOOPBACK_GATEWAY_LISTEN\}\/agent\/ws/,
  );
  assert.ok(
    launcher.includes(`assert_exact_tcp_listener \\
        "\${LOOPBACK_GATEWAY_LISTEN}" \\
        8082 \\
        "loopback gateway relay"`),
  );
  assert.match(launcher, /"SUMI_DEV_HOST=\$\{PUBLIC_HOST\}"/);
  assert.match(launcher, /"SUMI_DEV_API_ORIGIN=\$\{API_ORIGIN\}"/);
  assert.doesNotMatch(
    launcher,
    /SUMI_MODEL_API_KEY_ENV \\\n+\s+SUMI_EXECUTION_REVIEWER_MODEL_PRESET/,
  );
  assert.match(
    launcher,
    /\[\[ -z "\$\{reviewer_api_key_env\}" \]\] && continue/,
  );
});

test("make dev delegates to the real-stack launcher, not raw Turbo tasks", async () => {
  const [makefile, packageJSON] = await Promise.all([
    source("Makefile"),
    source("package.json"),
  ]);
  assert.match(
    makefile,
    /dev: ## Start the supported authenticated local Sumi stack/,
  );
  const scripts = JSON.parse(packageJSON).scripts;
  assert.equal(scripts.dev, "bash scripts/dev/real-stack");
  assert.equal(scripts["dev:workspaces"], "turbo run dev");
});

test("the allocator exception is bounded to one locked disposable generation", async () => {
  const launcher = await source("scripts/dev/real-stack");
  assert.match(
    launcher,
    /flock -n 9 \|\| fail "another local Sumi stack owns the fixed development ports"/,
  );
  assert.match(
    launcher,
    /RUNTIME_ROOT="\$\(mktemp -d "\$\{TMPDIR:-\/tmp\}\/sumi-real-stack\.XXXXXXXX"\)"/,
  );
  assert.match(launcher, /SUMI_RPC_GENERATION=0/);
  assert.match(
    launcher,
    /runtime_parent="\$\(realpath -m -- "\$\(dirname -- "\$\{RUNTIME_ROOT\}"\)"\)"/,
  );
  assert.match(
    launcher,
    /temporary_root="\$\(realpath -m -- "\$\{TMPDIR:-\/tmp\}"\)"/,
  );
  assert.match(launcher, /rm -rf -- "\$\{RUNTIME_ROOT\}"/);
  assert.match(launcher, /fail "a required Sumi process exited"/);
  assert.doesNotMatch(launcher, /--supervisor-allocate/);
});

/** Lifts one top-level function out of the launcher so a test can drive it. */
function launcherFunction(launcher, name) {
  const start = launcher.indexOf(`\n${name}() {\n`);
  assert.ok(start > 0);
  const end = launcher.indexOf("\n}\n", start);
  assert.ok(end > start);
  return launcher.slice(start + 1, end + 3);
}

async function runBash(script, environment) {
  const options = {
    env: {
      PATH: process.env.PATH,
      DISPOSABLE_RUNTIME_ROOT: "/tmp/sumi-real-stack.disposable",
      HOME: "/home/example",
      ...environment,
    },
  };
  const preamble = [
    "set -Eeuo pipefail",
    'fail() { printf "fail: %s\\n" "$*" >&2; exit 3; }',
  ].join("\n");
  try {
    return {
      ...(await execFileAsync(
        "bash",
        ["-c", `${preamble}\n${script}`],
        options,
      )),
      code: 0,
    };
  } catch (error) {
    return { stdout: error.stdout, stderr: error.stderr, code: error.code };
  }
}

/**
 * Expands the configured attachment root definition, so this test covers the
 * default and override selection without provisioning a real user-state path.
 */
function attachmentRootFor(launcher, environment) {
  const start = launcher.indexOf('readonly CONFIGURED_PERSISTENT_STATE_ROOT="');
  const end = launcher.indexOf(
    "\n",
    launcher.indexOf(
      '\nPERSISTENT_STATE_ROOT="$(provision_persistent_state_root',
    ),
  );
  assert.ok(start > 0 && end > start);
  const script = [
    'RUNTIME_ROOT="$DISPOSABLE_RUNTIME_ROOT"',
    launcher.slice(start, end),
    [
      'printf %s "$',
      '{CONFIGURED_PERSISTENT_STATE_ROOT}/messaging-attachments"',
    ].join(""),
  ].join("\n");
  return runBash(script, environment);
}

/** Runs the launcher's own provisioner against a real path on disk. */
function provisionStateRoot(launcher, root) {
  const script = [
    launcherFunction(launcher, "validate_persistent_state_root_ancestors"),
    launcherFunction(launcher, "provision_persistent_state_root"),
    `provision_persistent_state_root ${JSON.stringify(root)}`,
  ].join("\n");
  return runBash(script, {});
}

test("uploaded attachment bytes outlive the disposable runtime root", async () => {
  const launcher = await source("scripts/dev/real-stack");

  // Postgres survives a restart in a named Compose volume, so attachment bytes
  // must not sit in the tree that shutdown deletes; otherwise the rows outlive
  // the objects they name and every stored image comes back missing.
  assert.match(launcher, /rm -rf -- "\$\{RUNTIME_ROOT\}"/);
  const disposableDirectories = launcher.match(
    /mkdir -m 0700 \\\n(?:\s+"\$\{[A-Z_]+\}" \\\n)*\s+"\$\{[A-Z_]+\}"\n/,
  );
  assert.ok(disposableDirectories);
  assert.doesNotMatch(disposableDirectories[0], /MESSAGING_ATTACHMENT_DIR/);

  // The disposable-boundary documentation already drifted from the launcher
  // once, telling developers that shutdown removes all state. Keep the
  // exception named where they read about shutdown.
  const guide = await source("docs/local-development.md");
  assert.doesNotMatch(guide, /deletes all\s+state on shutdown/);
  assert.match(guide, /SUMI_REAL_STACK_STATE_ROOT/);
  assert.match(guide, /sumi\/real-stack\/messaging-attachments/);

  for (const [environment, expected] of [
    [{}, "/home/example/.local/state/sumi/real-stack/messaging-attachments"],
    [
      { XDG_STATE_HOME: "/home/example/state" },
      "/home/example/state/sumi/real-stack/messaging-attachments",
    ],
    [
      {
        SUMI_REAL_STACK_STATE_ROOT: "/srv/sumi-dev",
        XDG_STATE_HOME: "/home/example/state",
      },
      "/srv/sumi-dev/messaging-attachments",
    ],
  ]) {
    const resolved = await attachmentRootFor(launcher, environment);
    assert.equal(resolved.code, 0);
    assert.equal(resolved.stdout, expected);
  }
});

test("a relative state root is refused instead of splitting the store", async () => {
  const launcher = await source("scripts/dev/real-stack");

  // The API runs from apps/api and resolves SUMI_MESSAGING_ATTACHMENT_ROOT from
  // there, while the launcher creates directories from the invocation
  // directory. A relative root would therefore point the two at different
  // places, and would move the store every time the stack is started from
  // somewhere else.
  assert.match(
    launcher.slice(launcher.indexOf('log "starting API"')),
    /cd "\$\{REPOSITORY_ROOT\}\/apps\/api"/,
  );

  for (const root of ["sumi-dev-state", "./sumi-dev-state", "../sumi-dev"]) {
    const refused = await provisionStateRoot(launcher, root);
    assert.equal(refused.code, 3);
    assert.match(refused.stderr, /must be an absolute path/);
    assert.match(
      refused.stderr,
      /SUMI_REAL_STACK_STATE_ROOT or XDG_STATE_HOME/,
    );
  }
});

test("the launcher never re-modes a state root it did not create", async () => {
  const launcher = await source("scripts/dev/real-stack");
  assert.match(
    launcher,
    /^PERSISTENT_STATE_ROOT="\$\(provision_persistent_state_root "\$\{CONFIGURED_PERSISTENT_STATE_ROOT\}"\)"$/m,
  );

  const scratch = await mkdtemp(join(tmpdir(), "sumi-state-root-test."));
  try {
    // A root the launcher creates itself is private from the first byte.
    const created = join(scratch, "new-parent", "nested", "real-stack");
    const fresh = await provisionStateRoot(launcher, created);
    assert.equal(fresh.code, 0);
    assert.equal((await stat(created)).mode & 0o777, 0o700);
    assert.equal((await stat(join(scratch, "new-parent"))).mode & 0o777, 0o700);
    assert.equal(
      (await stat(join(scratch, "new-parent", "nested"))).mode & 0o777,
      0o700,
    );

    // A root that already exists may be $HOME, $TMPDIR, or any other directory
    // with duties of its own. Forcing it to 0700 would strip access the rest of
    // the system depends on, so the launcher refuses and leaves it untouched.
    const shared = join(scratch, "shared");
    await mkdir(shared, { mode: 0o755 });
    await chmod(shared, 0o755);
    const refused = await provisionStateRoot(launcher, shared);
    assert.equal(refused.code, 3);
    assert.match(refused.stderr, /mode 0700/);
    assert.equal((await stat(shared)).mode & 0o777, 0o755);

    // An existing root that already satisfies the contract is accepted as is.
    const private_ = join(scratch, "private");
    await mkdir(private_, { mode: 0o700 });
    await chmod(private_, 0o700);
    const reused = await provisionStateRoot(launcher, private_);
    assert.equal(reused.code, 0);
    assert.equal((await stat(private_)).mode & 0o777, 0o700);

    // A symlink is refused too: its target is someone else's directory.
    const linked = join(scratch, "linked");
    await symlink(private_, linked);
    const rejected = await provisionStateRoot(launcher, linked);
    assert.equal(rejected.code, 3);
    assert.match(rejected.stderr, /must be a real directory/);

    // Normalizing at the provisioner's entrance keeps a trailing slash from
    // making bash inspect the symlink target instead of the link itself.
    const trailingSlashRejected = await provisionStateRoot(
      launcher,
      `${linked}/`,
    );
    assert.equal(trailingSlashRejected.code, 3);
    assert.match(trailingSlashRejected.stderr, /must be a real directory/);
  } finally {
    await rm(scratch, { force: true, recursive: true });
  }
});

test("a state root under a writable non-sticky ancestor is refused", async () => {
  const launcher = await source("scripts/dev/real-stack");
  const scratch = await mkdtemp(join(tmpdir(), "sumi-state-root-test."));
  try {
    const writableAncestor = join(scratch, "writable");
    const root = join(writableAncestor, "real-stack");
    await mkdir(writableAncestor, { mode: 0o777 });
    await chmod(writableAncestor, 0o777);
    await mkdir(root, { mode: 0o700 });
    await chmod(root, 0o700);

    const refused = await provisionStateRoot(launcher, root);
    assert.equal(refused.code, 3);
    assert.match(
      refused.stderr,
      new RegExp(`ancestor ${writableAncestor} is writable by group or other`),
    );
  } finally {
    await rm(scratch, { force: true, recursive: true });
  }
});

test("a state root under an ancestor owned by another user is refused", async () => {
  const launcher = await source("scripts/dev/real-stack");
  const script = [
    launcherFunction(launcher, "validate_persistent_state_root_ancestors"),
    "stat() {",
    '  case "$4" in',
    '    /foreign-owner) printf "755 4242\\n" ;;',
    '    /) printf "755 0\\n" ;;',
    '    *) fail "unexpected stat path: $4" ;;',
    "  esac",
    "}",
    "validate_persistent_state_root_ancestors /foreign-owner/real-stack",
  ].join("\n");

  const refused = await runBash(script, {});
  assert.equal(refused.code, 3);
  assert.match(refused.stderr, /ancestor \/foreign-owner.*owner uid 4242/);
});

test("a root-owned writable sticky ancestor is accepted", async () => {
  const launcher = await source("scripts/dev/real-stack");
  const script = [
    launcherFunction(launcher, "validate_persistent_state_root_ancestors"),
    "stat() {",
    '  case "$4" in',
    '    /tmp) printf "1777 0\\n" ;;',
    '    /) printf "755 0\\n" ;;',
    '    *) fail "unexpected stat path: $4" ;;',
    "  esac",
    "}",
    "validate_persistent_state_root_ancestors /tmp/real-stack",
  ].join("\n");

  const accepted = await runBash(script, {});
  assert.equal(accepted.code, 0);
});

test("a state root under a writable sticky ancestor is accepted", async () => {
  const launcher = await source("scripts/dev/real-stack");
  const scratch = await mkdtemp(join(tmpdir(), "sumi-state-root-test."));
  try {
    const stickyAncestor = join(scratch, "sticky");
    const root = join(stickyAncestor, "real-stack");
    await mkdir(stickyAncestor, { mode: 0o777 });
    await chmod(stickyAncestor, 0o1777);
    await mkdir(root, { mode: 0o700 });
    await chmod(root, 0o700);

    const accepted = await provisionStateRoot(launcher, root);
    assert.equal(accepted.code, 0);
  } finally {
    await rm(scratch, { force: true, recursive: true });
  }
});

test("a state root with a symlink ancestor passes its canonical path to the API", async () => {
  const launcher = await source("scripts/dev/real-stack");
  const scratch = await mkdtemp(join(tmpdir(), "sumi-state-root-test."));
  try {
    const realParent = join(scratch, "real-parent");
    const linkedParent = join(scratch, "linked-parent");
    const configuredRoot = join(linkedParent, "real-stack");
    const canonicalRoot = join(realParent, "real-stack");
    await mkdir(realParent, { mode: 0o700 });
    await chmod(realParent, 0o700);
    await symlink(realParent, linkedParent);

    const provisioned = await provisionStateRoot(launcher, configuredRoot);
    assert.equal(provisioned.code, 0);
    assert.equal(provisioned.stdout, `${canonicalRoot}\n`);

    // The launcher constructs the API root from the provisioner's returned
    // real path, rather than from the configured spelling with its link.
    assert.match(
      launcher,
      /PERSISTENT_STATE_ROOT="\$\(provision_persistent_state_root "\$\{CONFIGURED_PERSISTENT_STATE_ROOT\}"\)"/,
    );
    assert.match(
      launcher,
      /readonly MESSAGING_ATTACHMENT_DIR="\$\{PERSISTENT_STATE_ROOT\}\/messaging-attachments"/,
    );
    assert.match(
      launcher,
      /"SUMI_MESSAGING_ATTACHMENT_ROOT=\$\{MESSAGING_ATTACHMENT_DIR\}"/,
    );
    assert.equal(
      `${provisioned.stdout.trim()}/messaging-attachments`,
      `${canonicalRoot}/messaging-attachments`,
    );
  } finally {
    await rm(scratch, { force: true, recursive: true });
  }
});
