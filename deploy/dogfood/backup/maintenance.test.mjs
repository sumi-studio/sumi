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

test("versioned maintenance helpers stop exactly one API and retain a crash marker until ready", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "sumi-maintenance-helper-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const work = join(root, "work");
  const envFile = join(root, "operator.env");
  const docker = join(root, "docker");
  const dockerConfigDirectory = join(root, "docker-config");
  const dockerConfig = join(dockerConfigDirectory, "config.json");
  const state = join(root, "docker-state");
  const log = join(root, "docker.log");
  const snapshotID = "20260810T120005Z-aaaaaaaaaaaa";
  await mkdir(work);
  await mkdir(dockerConfigDirectory);
  await writeFile(dockerConfig, "{}\n", { mode: 0o600 });
  await writeFile(envFile, "SUMI_APP_SHA=fake\n", { mode: 0o600 });
  await writeFile(state, "running\n");
  await writeFile(log, "");
  await writeFile(
    docker,
    `#!/usr/bin/env bash
set -Eeuo pipefail
printf '%s\\n' "$*" >> "\${SUMI_TEST_DOCKER_LOG}"
arguments="$*"
if [[ "\${arguments}" == *"context inspect"* ]]; then
  printf 'unix:///var/run/docker.sock\\n'
elif [[ "\${arguments}" == *"ps --status running -q api"* ]]; then
  if [[ "$(<"\${SUMI_TEST_DOCKER_STATE}")" == running ]]; then printf 'container-id\\n'; fi
elif [[ "\${arguments}" == *"stop api"* ]]; then
  printf 'stopped\\n' > "\${SUMI_TEST_DOCKER_STATE}"
elif [[ "\${arguments}" == *"up -d --no-deps api"* ]]; then
  printf 'running\\n' > "\${SUMI_TEST_DOCKER_STATE}"
elif [[ "\${arguments}" == *"exec -T api"* ]]; then
  [[ "$(<"\${SUMI_TEST_DOCKER_STATE}")" == running ]]
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
    SUMI_TEST_DOCKER_STATE: state,
    SUMI_TEST_DOCKER_LOG: log,
  };

  await run("bash", [resolve(directory, "quiesce-api.sh"), snapshotID], {
    env: environment,
  });
  assert.equal((await readFile(state, "utf8")).trim(), "stopped");
  assert.ok((await stat(join(work, ".maintenance", snapshotID))).isDirectory());
  await run("bash", [resolve(directory, "resume-api.sh"), snapshotID], {
    env: environment,
  });
  assert.equal((await readFile(state, "utf8")).trim(), "running");
  await assert.rejects(stat(join(work, ".maintenance", snapshotID)));
  assert.match(
    await readFile(log, "utf8"),
    /stop api[\s\S]*up -d --no-deps api[\s\S]*exec -T api/,
  );
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
