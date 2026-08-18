import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
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

/**
 * Runs the launcher's own attachment root block under a controlled environment,
 * so the test measures where the bytes actually land and which values the block
 * refuses, rather than how either happens to be spelled.
 */
async function attachmentRootFor(launcher, environment) {
  const start = launcher.indexOf('readonly PERSISTENT_STATE_ROOT="');
  const end = launcher.indexOf(
    "\n",
    launcher.indexOf('readonly MESSAGING_ATTACHMENT_DIR="'),
  );
  assert.ok(start > 0 && end > start);
  const script = [
    "set -euo pipefail",
    'fail() { printf "fail: %s\\n" "$*" >&2; exit 3; }',
    'RUNTIME_ROOT="$DISPOSABLE_RUNTIME_ROOT"',
    launcher.slice(start, end),
    'printf %s "$MESSAGING_ATTACHMENT_DIR"',
  ].join("\n");
  const options = {
    env: {
      PATH: process.env.PATH,
      DISPOSABLE_RUNTIME_ROOT: "/tmp/sumi-real-stack.disposable",
      HOME: "/home/example",
      ...environment,
    },
  };
  try {
    return {
      ...(await execFileAsync("bash", ["-c", script], options)),
      code: 0,
    };
  } catch (error) {
    return { stdout: error.stdout, stderr: error.stderr, code: error.code };
  }
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

  for (const environment of [
    { SUMI_REAL_STACK_STATE_ROOT: "sumi-dev-state" },
    { SUMI_REAL_STACK_STATE_ROOT: "./sumi-dev-state" },
    { SUMI_REAL_STACK_STATE_ROOT: "../sumi-dev-state" },
    { XDG_STATE_HOME: "state" },
  ]) {
    const refused = await attachmentRootFor(launcher, environment);
    assert.equal(refused.code, 3);
    assert.equal(refused.stdout, "");
    assert.match(refused.stderr, /must be an absolute path/);
    assert.match(
      refused.stderr,
      /SUMI_REAL_STACK_STATE_ROOT or XDG_STATE_HOME/,
    );
  }
});
