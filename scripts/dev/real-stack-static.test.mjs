import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const repositoryRoot = resolve(
  dirname(fileURLToPath(import.meta.url)),
  "../..",
);

async function source(path) {
  return readFile(resolve(repositoryRoot, path), "utf8");
}

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
  const apiGate = launcher.search(
    /wait_for_http "\$\{API_ORIGIN\}\/health" "\$\{API_PID\}" "API"/,
  );
  const executorStart = launcher.indexOf(
    'log "starting authenticated tool executor"',
  );
  const executorGate = launcher.search(
    /wait_for_socket "\$\{EXECUTOR_SOCKET\}" "\$\{EXECUTOR_PID\}"/,
  );
  const runtimeStart = launcher.indexOf(
    'log "starting production PersonalityAgent"',
  );
  const runtimeGate = launcher.indexOf("wait_for_runtime_ready \\");
  const viteStart = launcher.search(/log "starting Vite at \$\{WEB_ORIGIN\}"/);

  for (const position of [
    apiGate,
    executorStart,
    executorGate,
    runtimeStart,
    runtimeGate,
    viteStart,
  ]) {
    assert.notEqual(position, -1);
  }
  assert.ok(apiGate < executorStart);
  assert.ok(executorStart < executorGate);
  assert.ok(executorGate < runtimeStart);
  assert.ok(runtimeStart < runtimeGate);
  assert.ok(runtimeGate < viteStart);
  assert.match(launcher, /state\.local_control\?\.state === "ready"/);
  assert.match(launcher, /state\.local_control\?\.integrity\?\.mac/);
  assert.match(
    launcher,
    /SUMI_AUTH_PERSONALITY_AGENT_ID must equal SUMI_PERSONALITY_AGENT_ID/,
  );
  assert.match(launcher, /SUMI_BROWSER_SESSION_AUDIENCE=sumi:web/);
  assert.match(launcher, /SUMI_BROWSER_WS_ALLOWED_ORIGINS=\$\{WEB_ORIGIN\}/);
  assert.match(launcher, /SUMI_AUTH_ALLOW_INSECURE_COOKIES=true/);
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
