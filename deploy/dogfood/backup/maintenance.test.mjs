import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import {
  chmod,
  mkdir,
  mkdtemp,
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

const run = promisify(execFile);
const directory = dirname(fileURLToPath(import.meta.url));
const personalityAgentID = "0190abcd-1234-7abc-8def-0123456789ab";
const personalityAgentProject = `sumi-${personalityAgentID.replaceAll("-", "")}`;
const agentContainers = {
  runtime: "1".repeat(64),
  executor: "2".repeat(64),
  broker: "3".repeat(64),
};

test("versioned maintenance stops and exactly resumes every writer class", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "sumi-maintenance-helper-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const work = join(root, "work");
  const envFile = join(root, "operator.env");
  const docker = join(root, "docker");
  const database = join(root, "database");
  const dockerConfigDirectory = join(root, "docker-config");
  const dockerConfig = join(dockerConfigDirectory, "config.json");
  const apiState = join(root, "api-state");
  const provisionerState = join(root, "provisioner-state");
  const ingressState = join(root, "ingress-state");
  const log = join(root, "docker.log");
  const snapshotID = "20260810T120005Z-aaaaaaaaaaaa";
  await mkdir(work);
  await mkdir(dockerConfigDirectory);
  await writeFile(dockerConfig, "{}\n", { mode: 0o600 });
  await writeFile(envFile, "SUMI_APP_SHA=fake\n", { mode: 0o600 });
  await writeFile(apiState, "running\n");
  await writeFile(provisionerState, "running\n");
  await writeFile(ingressState, "running\n");
  await writeFile(log, "");
  await writeFile(
    database,
    `#!/usr/bin/env bash
set -Eeuo pipefail
if [[ "\${SUMI_TEST_DATABASE_FAIL:-0}" == 1 ]]; then exit 9; fi
if [[ "\${1:-}" == agent-ids ]]; then printf '%s\\n' '${personalityAgentID}'; fi
`,
    { mode: 0o700 },
  );
  await writeFile(
    docker,
    `#!/usr/bin/env bash
set -Eeuo pipefail
printf '%s\\n' "$*" >> "\${SUMI_TEST_DOCKER_LOG}"
arguments="$*"
if [[ "\${arguments}" == *"context inspect"* ]]; then
  printf 'unix:///var/run/docker.sock\\n'
elif [[ "\${arguments}" == *"ps --status running -q api"* ]]; then
  if [[ "$(<"\${SUMI_TEST_API_STATE}")" == running ]]; then printf 'api-container-id\\n'; fi
elif [[ "\${arguments}" == *"ps --status running -q runtime-provisioner"* ]]; then
  if [[ "$(<"\${SUMI_TEST_PROVISIONER_STATE}")" == running ]]; then printf 'provisioner-container-id\\n'; fi
elif [[ "\${arguments}" == *"ps --status running -q cloudflared"* ]]; then
  if [[ "$(<"\${SUMI_TEST_INGRESS_STATE}")" == running ]]; then printf 'ingress-container-id\\n'; fi
elif [[ "\${arguments}" == *"ps --filter label=com.docker.compose.project -q"* ]]; then
  printf '%s\\n%s\\n%s\\n' '${agentContainers.runtime}' '${agentContainers.executor}' '${agentContainers.broker}'
elif [[ "\${arguments}" == *"ps --filter label=com.docker.compose.service=runtime -q"* ]]; then
  printf '%s\\n' '${agentContainers.runtime}'
elif [[ "\${arguments}" == *"ps --filter label=com.docker.compose.service=executor -q"* ]]; then
  printf '%s\\n' '${agentContainers.executor}'
elif [[ "\${arguments}" == *"ps --filter label=com.docker.compose.service=broker -q"* ]]; then
  printf '%s\\n' '${agentContainers.broker}'
elif [[ "\${arguments}" == *"ps --filter label=com.docker.compose.project=${personalityAgentProject} -q"* ]]; then
  printf '%s\\n%s\\n%s\\n' '${agentContainers.runtime}' '${agentContainers.executor}' '${agentContainers.broker}'
elif [[ "\${arguments}" == *"inspect --format {{.Id}}"* ]]; then
  container_id="\${!#}"
  case "\${container_id}" in
    '${agentContainers.runtime}') service=runtime;;
    '${agentContainers.executor}') service=executor;;
    '${agentContainers.broker}') service=broker;;
    *) exit 2;;
  esac
  printf '%s\\t%s\\t%s\\trunning\\n' "\${container_id}" '${personalityAgentProject}' "\${service}"
elif [[ "\${arguments}" == *"inspect --format {{index .Config.Labels \\"com.docker.compose.project\\"}}\\t{{index .Config.Labels \\"com.docker.compose.service\\"}}"* ]]; then
  container_id="\${!#}"
  case "\${container_id}" in
    '${agentContainers.runtime}') service=runtime;;
    '${agentContainers.executor}') service=executor;;
    '${agentContainers.broker}') service=broker;;
    *) exit 2;;
  esac
  printf '%s\\t%s\\n' '${personalityAgentProject}' "\${service}"
elif [[ "\${arguments}" == *"inspect --format {{index .Config.Labels \\"com.docker.compose.project\\"}}"* ]]; then
  printf '%s\\n' '${personalityAgentProject}'
elif [[ "\${arguments}" == *"inspect --format {{.State.Status}}"* ]]; then
  printf 'running\\n'
elif [[ "\${arguments}" == *"stop cloudflared"* ]]; then
  printf 'stopped\\n' > "\${SUMI_TEST_INGRESS_STATE}"
elif [[ "\${arguments}" == *"stop api"* ]]; then
  printf 'stopped\\n' > "\${SUMI_TEST_API_STATE}"
elif [[ "\${arguments}" == *"stop runtime-provisioner"* ]]; then
  printf 'stopped\\n' > "\${SUMI_TEST_PROVISIONER_STATE}"
elif [[ "\${arguments}" == *"up -d --no-deps runtime-provisioner"* ]]; then
  printf 'running\\n' > "\${SUMI_TEST_PROVISIONER_STATE}"
elif [[ "\${arguments}" == *"up -d --no-deps api"* ]]; then
  printf 'running\\n' > "\${SUMI_TEST_API_STATE}"
elif [[ "\${arguments}" == *"up -d --no-deps cloudflared"* ]]; then
  printf 'running\\n' > "\${SUMI_TEST_INGRESS_STATE}"
elif [[ "\${arguments}" == *"exec -T api"* ]]; then
  [[ "$(<"\${SUMI_TEST_API_STATE}")" == running && "$(<"\${SUMI_TEST_PROVISIONER_STATE}")" == running ]]
fi
`,
  );
  await chmod(docker, 0o700);
  const environment = {
    ...process.env,
    SUMI_DOGFOOD_OPERATOR_ENV_FILE: envFile,
    SUMI_DOCKER_BIN: docker,
    SUMI_DOGFOOD_DOCKER_CONTEXT: "dogfood-test",
    SUMI_DOCKER_CONFIG_FILE: dockerConfig,
    SUMI_BACKUP_WORK_ROOT: work,
    SUMI_DATABASE_HELPER: database,
    SUMI_RESUME_HELPER: resolve(directory, "resume-api.sh"),
    SUMI_TEST_API_STATE: apiState,
    SUMI_TEST_PROVISIONER_STATE: provisionerState,
    SUMI_TEST_INGRESS_STATE: ingressState,
    SUMI_TEST_DOCKER_LOG: log,
  };

  await run("bash", [resolve(directory, "quiesce-api.sh"), snapshotID], {
    env: environment,
  });
  assert.equal((await readFile(apiState, "utf8")).trim(), "stopped");
  assert.equal((await readFile(provisionerState, "utf8")).trim(), "stopped");
  assert.equal((await readFile(ingressState, "utf8")).trim(), "stopped");
  assert.ok((await stat(join(work, ".maintenance", snapshotID))).isDirectory());
  await run("bash", [resolve(directory, "resume-api.sh"), snapshotID], {
    env: environment,
  });
  assert.equal((await readFile(apiState, "utf8")).trim(), "running");
  assert.equal((await readFile(provisionerState, "utf8")).trim(), "running");
  assert.equal((await readFile(ingressState, "utf8")).trim(), "running");
  await assert.rejects(stat(join(work, ".maintenance", snapshotID)));
  assert.match(
    await readFile(log, "utf8"),
    new RegExp(
      `stop cloudflared[\\s\\S]*stop runtime-provisioner` +
        `[\\s\\S]*stop --time 120 ${agentContainers.runtime}` +
        `[\\s\\S]*stop --time 120 ${agentContainers.broker}` +
        `[\\s\\S]*stop --time 120 ${agentContainers.executor}` +
        `[\\s\\S]*stop api` +
        `[\\s\\S]*up -d --no-deps runtime-provisioner` +
        `[\\s\\S]*up -d --no-deps api[\\s\\S]*exec -T api` +
        `[\\s\\S]*start ${agentContainers.executor}` +
        `[\\s\\S]*start ${agentContainers.broker}` +
        `[\\s\\S]*start ${agentContainers.runtime}` +
        `[\\s\\S]*up -d --no-deps cloudflared`,
    ),
  );

  await writeFile(log, "");
  await writeFile(apiState, "stopped\n");
  const invalidSnapshotID = "20260810T120006Z-aaaaaaaaaaaa";
  await assert.rejects(
    run("bash", [resolve(directory, "quiesce-api.sh"), invalidSnapshotID], {
      env: environment,
    }),
    /requires exactly one running ingress, API, and runtime provisioner/,
  );
  assert.doesNotMatch(await readFile(log, "utf8"), /stop |up -d/);
  await assert.rejects(stat(join(work, ".maintenance", invalidSnapshotID)));

  await writeFile(log, "");
  await writeFile(apiState, "running\n");
  await writeFile(provisionerState, "running\n");
  await writeFile(ingressState, "running\n");
  const failedInventorySnapshotID = "20260810T120007Z-aaaaaaaaaaaa";
  await assert.rejects(
    run(
      "bash",
      [resolve(directory, "quiesce-api.sh"), failedInventorySnapshotID],
      { env: { ...environment, SUMI_TEST_DATABASE_FAIL: "1" } },
    ),
  );
  assert.equal((await readFile(apiState, "utf8")).trim(), "running");
  assert.equal((await readFile(provisionerState, "utf8")).trim(), "running");
  assert.equal((await readFile(ingressState, "utf8")).trim(), "running");
  await assert.rejects(
    stat(join(work, ".maintenance", failedInventorySnapshotID)),
  );
});

test("database maintenance rejects host-only URLs and uses the internal Compose client", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "sumi-database-adapter-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const envFile = join(root, "operator.env");
  const docker = join(root, "docker");
  const dockerConfigDirectory = join(root, "docker-config");
  const dockerConfig = join(dockerConfigDirectory, "config.json");
  const log = join(root, "docker.log");
  await writeFile(envFile, "SUMI_APP_SHA=fake\n", { mode: 0o600 });
  await mkdir(dockerConfigDirectory);
  await writeFile(dockerConfig, "{}\n", { mode: 0o600 });
  await writeFile(log, "");
  await writeFile(
    docker,
    `#!/usr/bin/env bash
set -Eeuo pipefail
printf '%s\\n' "$*" >> "\${SUMI_TEST_DOCKER_LOG}"
if [[ "$*" == *"context inspect"* ]]; then printf 'unix:///var/run/docker.sock\\n'; fi
`,
  );
  await chmod(docker, 0o700);
  const environment = {
    ...process.env,
    SUMI_DOGFOOD_OPERATOR_ENV_FILE: envFile,
    SUMI_DOCKER_BIN: docker,
    SUMI_DOGFOOD_DOCKER_CONTEXT: "dogfood-test",
    SUMI_DOCKER_CONFIG_FILE: dockerConfig,
    SUMI_DB_URL: "postgres://sumi:secret@postgres:5432/sumi",
    SUMI_TEST_DOCKER_LOG: log,
  };
  await run("bash", [resolve(directory, "compose-database.sh"), "agent-ids"], {
    env: environment,
  });
  assert.match(
    await readFile(log, "utf8"),
    /run --rm --no-deps -T[\s\S]*database-client/,
  );
  await writeFile(log, "");
  await run(
    "bash",
    [resolve(directory, "compose-database.sh"), "scratch-object-count"],
    {
      env: {
        ...environment,
        SUMI_RESTORE_DB_URL:
          "postgres://sumi:secret@scratch-postgres:5432/sumi",
      },
    },
  );
  const scratchInvocation = await readFile(log, "utf8");
  assert.ok(scratchInvocation.includes(String.raw`\$\$pg_catalog\$\$`));
  assert.ok(scratchInvocation.includes(String.raw`\$\$public\$\$`));
  await assert.rejects(
    run("bash", [resolve(directory, "compose-database.sh"), "agent-ids"], {
      env: {
        ...environment,
        SUMI_DB_URL: "postgres://sumi:secret@127.0.0.1:5432/sumi",
      },
    }),
    /host-only database/,
  );
  const adapter = await readFile(
    resolve(directory, "compose-database.sh"),
    "utf8",
  );
  for (const catalog of [
    "pg_namespace",
    "pg_class",
    "pg_proc",
    "pg_type",
    "pg_constraint",
  ]) {
    assert.match(
      adapter,
      new RegExp(`scratch-object-count[\\s\\S]*${catalog}`),
    );
  }
  assert.doesNotMatch(adapter, /to_regnamespace\(current_schema\(\)\)/);
});

test("compose migration adapter preserves the selected binary and database URL", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "sumi-migrate-adapter-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const envFile = join(root, "operator.env");
  const docker = join(root, "docker");
  const dockerConfigDirectory = join(root, "docker-config");
  const dockerConfig = join(dockerConfigDirectory, "config.json");
  const log = join(root, "docker.log");
  await writeFile(envFile, "SUMI_APP_SHA=fake\n", { mode: 0o600 });
  await mkdir(dockerConfigDirectory);
  await writeFile(dockerConfig, "{}\n", { mode: 0o600 });
  await writeFile(log, "");
  await writeFile(
    docker,
    `#!/usr/bin/env bash
set -Eeuo pipefail
printf '%s\\n' "$*" > "\${SUMI_TEST_DOCKER_LOG}"
if [[ "$*" == *"context inspect"* ]]; then printf 'unix:///var/run/docker.sock\\n'; fi
`,
  );
  await chmod(docker, 0o700);
  await run("bash", [resolve(directory, "compose-migrate.sh"), "verify"], {
    env: {
      ...process.env,
      SUMI_DOGFOOD_OPERATOR_ENV_FILE: envFile,
      SUMI_DOCKER_BIN: docker,
      SUMI_DOGFOOD_DOCKER_CONTEXT: "dogfood-test",
      SUMI_DOCKER_CONFIG_FILE: dockerConfig,
      SUMI_DB_URL: "postgres://scratch.invalid/sumi",
      SUMI_TEST_DOCKER_LOG: log,
    },
  });
  assert.match(
    await readFile(log, "utf8"),
    /run --rm --no-deps migrate verify/,
  );
  assert.doesNotMatch(await readFile(log, "utf8"), /scratch\.invalid/);
});
