import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import {
  chmod,
  link,
  mkdir,
  mkdtemp,
  readFile,
  rm,
  stat,
  symlink,
  writeFile,
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
const launcher = join(repositoryRoot, "scripts/dev/compose-stack");
const baseCompose = join(repositoryRoot, "deploy/local/compose.dev.yaml");
const realCompose = join(
  repositoryRoot,
  "deploy/local/compose.real-firebase.yaml",
);

test("real Firebase mode selects both Compose files without rendering credentials", async () => {
  const fixture = await createFixture(0o600);
  try {
    const result = await runLauncher(fixture, "real");
    assert.equal(result.stdout, "");
    assert.doesNotMatch(result.stderr, /test-client-secret|test-refresh-token/);
    assert.deepEqual(await dockerArguments(fixture), [
      "compose",
      "-p",
      "sumi-dev",
      "-f",
      baseCompose,
      "-f",
      realCompose,
      "config",
      "--quiet",
    ]);
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("config remains quiet without a Docker credential file", async () => {
  const fixture = await createFixture(0o600);
  try {
    await runLauncher(fixture, "emulator");
    assert.deepEqual(await dockerArguments(fixture), [
      "compose",
      "-p",
      "sumi-dev",
      "-f",
      baseCompose,
      "config",
      "--quiet",
    ]);
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("up validates and forwards a file-scoped Docker config", async () => {
  const fixture = await createFixture(0o600);
  try {
    await runLauncher(fixture, "emulator", ["up", "runtime-provisioner"], {
      SUMI_DOCKER_CONFIG_FILE: fixture.dockerConfigFile,
    });
    assert.deepEqual(await dockerArguments(fixture), [
      "compose",
      "-p",
      "sumi-dev",
      "-f",
      baseCompose,
      "up",
      "runtime-provisioner",
    ]);
    assert.equal(
      (await readFile(fixture.dockerConfigEnvLog, "utf8")).trim(),
      fixture.dockerConfigFile,
    );
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("up rejects an unsafe Docker config file", async (t) => {
  await t.test("world-readable", async () => {
    const fixture = await createFixture(0o600);
    try {
      await chmod(fixture.dockerConfigFile, 0o644);
      await assert.rejects(
        runLauncher(fixture, "emulator", ["up"], {
          SUMI_DOCKER_CONFIG_FILE: fixture.dockerConfigFile,
        }),
        /mode 0400 or 0600/,
      );
    } finally {
      await rm(fixture.root, { recursive: true, force: true });
    }
  });

  await t.test("symlink", async () => {
    const fixture = await createFixture(0o600);
    try {
      const linkedConfig = join(fixture.root, "linked-config.json");
      await symlink(fixture.dockerConfigFile, linkedConfig);
      await assert.rejects(
        runLauncher(fixture, "emulator", ["up"], {
          SUMI_DOCKER_CONFIG_FILE: linkedConfig,
        }),
        /regular non-symlink/,
      );
    } finally {
      await rm(fixture.root, { recursive: true, force: true });
    }
  });

  await t.test("hard link", async () => {
    const fixture = await createFixture(0o600);
    try {
      await link(
        fixture.dockerConfigFile,
        join(fixture.root, "config-copy.json"),
      );
      await assert.rejects(
        runLauncher(fixture, "emulator", ["up"], {
          SUMI_DOCKER_CONFIG_FILE: fixture.dockerConfigFile,
        }),
        /with one link/,
      );
    } finally {
      await rm(fixture.root, { recursive: true, force: true });
    }
  });
});

test("down remains available without a Docker credential file", async () => {
  const fixture = await createFixture(0o600);
  try {
    await runLauncher(fixture, "emulator", ["down"]);
    assert.deepEqual(await dockerArguments(fixture), [
      "compose",
      "-p",
      "sumi-dev",
      "-f",
      baseCompose,
      "down",
    ]);
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("real Firebase down removes only the selected project's staged ADC volume", async () => {
  const fixture = await createFixture(0o600);
  try {
    await runLauncher(fixture, "real", ["down"], {
      SUMI_LOCAL_COMPOSE_PROJECT: "sumi-feature_153",
    });
    assert.deepEqual(await dockerArguments(fixture), [
      "volume",
      "rm",
      "sumi-feature_153_firebase-adc-runtime",
    ]);
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("real Firebase mode rejects a host ADC readable by other users", async () => {
  const fixture = await createFixture(0o644);
  try {
    await assert.rejects(runLauncher(fixture, "real"), (error) => {
      assert.equal(error.code, 1);
      assert.match(error.stderr, /mode 0400 or 0600/);
      assert.doesNotMatch(
        error.stderr,
        /test-client-secret|test-refresh-token/,
      );
      return true;
    });
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("Docker Compose accepts the merged real-Firebase configuration", {
  // biome-ignore lint/suspicious/noUndeclaredEnvVars: opt-in local integration gate, not a cached Turbo task
  skip: process.env.SUMI_TEST_COMPOSE_CONFIG !== "1",
}, async () => {
  const fixture = await createFixture(0o600, { fakeDocker: false });
  try {
    const result = await runLauncher(fixture, "real");
    assert.equal(result.stdout, "");
    assert.doesNotMatch(result.stderr, /test-client-secret|test-refresh-token/);
  } finally {
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("the staged ADC is readable by API UID 65532 with mode 0400", {
  // biome-ignore lint/suspicious/noUndeclaredEnvVars: explicit local Docker integration gate
  skip: process.env.SUMI_TEST_ADC_STAGING !== "1",
}, async () => {
  const fixture = await createFixture(0o600, { fakeDocker: false });
  try {
    const existing = await execFileAsync(
      "docker",
      [
        "ps",
        "-a",
        "--filter",
        "label=com.docker.compose.project=sumi-dev",
        "--format",
        "{{.ID}}",
      ],
      { cwd: repositoryRoot },
    );
    assert.equal(
      existing.stdout.trim(),
      "",
      "refusing to reuse an active sumi-dev Compose project",
    );
    const up = await runLauncher(fixture, "real", ["up", "firebase-adc-init"]);
    assert.doesNotMatch(
      `${up.stdout}\n${up.stderr}`,
      /test-client-secret|test-refresh-token/,
    );
    const container = await execFileAsync(
      "docker",
      [
        "ps",
        "-a",
        "--filter",
        "label=com.docker.compose.project=sumi-dev",
        "--filter",
        "label=com.docker.compose.service=firebase-adc-init",
        "--format",
        "{{.Image}}",
      ],
      { cwd: repositoryRoot },
    );
    const image = container.stdout.trim();
    assert.notEqual(image, "");
    await execFileAsync(
      "docker",
      [
        "run",
        "--rm",
        "--user",
        "65532:65532",
        "--volume",
        "sumi-dev_firebase-adc-runtime:/run/sumi/firebase:ro",
        "--entrypoint",
        "/busybox/sh",
        image,
        "-ceu",
        'file=/run/sumi/firebase/application_default_credentials.json; test -r "$file"; test "$(stat -c %u:%a "$file")" = 65532:400',
      ],
      { cwd: repositoryRoot },
    );
    assert.equal((await stat(fixture.adcFile)).mode & 0o777, 0o600);
  } finally {
    await runLauncher(fixture, "real", ["down", "-v"]).catch(() => undefined);
    await rm(fixture.root, { recursive: true, force: true });
  }
});

async function createFixture(adcMode, { fakeDocker = true } = {}) {
  const root = await mkdtemp(join(tmpdir(), "sumi-compose-stack-"));
  const bin = join(root, "bin");
  const envFile = join(root, "local.env");
  const runtimeEnvFile = join(root, "runtime.env");
  const adcFile = join(root, "adc.json");
  const dockerConfigFile = join(root, "config.json");
  const dockerLog = join(root, "docker.args");
  const dockerConfigEnvLog = join(root, "docker-config.env");
  await mkdir(bin);
  if (fakeDocker) {
    await writeExecutable(
      join(bin, "docker"),
      `#!/bin/sh
if [ "$1" = network ] && [ "$2" = inspect ]; then
  printf '%s\\n' 'sumi-control-plane:bridge:true'
  exit 0
fi
printf '%s\\n' "$@" > "$SUMI_TEST_DOCKER_LOG"
printf '%s\\n' "$SUMI_DOCKER_CONFIG_FILE" > "$SUMI_TEST_DOCKER_CONFIG_ENV_LOG"
`,
    );
  }
  await writeExecutable(
    join(bin, "ip"),
    "#!/bin/sh\nprintf '%s\\n' '1: lo inet 127.0.0.1/8 scope host lo'\n",
  );
  await writeFile(
    adcFile,
    JSON.stringify({
      type: "authorized_user",
      client_id: "test-client-id",
      client_secret: "test-client-secret",
      refresh_token: "test-refresh-token",
    }),
    { mode: adcMode },
  );
  await chmod(adcFile, adcMode);
  await writeFile(dockerConfigFile, "{}\n", { mode: 0o600 });
  await chmod(dockerConfigFile, 0o600);
  await writeFile(
    envFile,
    [
      "SUMI_DEV_BIND_HOST=127.0.0.1",
      "SUMI_AUTH_FIREBASE_PROJECT_ID=sumi-studio",
      "VITE_FIREBASE_API_KEY=test-public-api-key",
      "VITE_FIREBASE_AUTH_DOMAIN=sumi-studio.firebaseapp.com",
      "VITE_FIREBASE_PROJECT_ID=sumi-studio",
      "VITE_FIREBASE_APP_ID=test-public-app-id",
      `SUMI_FIREBASE_ADC_FILE=${adcFile}`,
      "",
    ].join("\n"),
    { mode: 0o600 },
  );
  await writeFile(
    runtimeEnvFile,
    [
      "SUMI_EXECUTION_REVIEWER_MODEL_PRESET=kimi-k3",
      "SUMI_EXECUTION_REVIEWER_MODEL_API_KEY_ENV=SUMI_TEST_EXECUTION_REVIEWER_KEY",
      "SUMI_TEST_EXECUTION_REVIEWER_KEY=test-execution-reviewer-key",
      "SUMI_ESCALATION_REVIEWER_MODEL_PRESET=glm-5.2",
      "SUMI_ESCALATION_REVIEWER_MODEL_API_KEY_ENV=SUMI_TEST_ESCALATION_REVIEWER_KEY",
      "SUMI_TEST_ESCALATION_REVIEWER_KEY=test-escalation-reviewer-key",
      "",
    ].join("\n"),
    { mode: 0o600 },
  );
  return {
    root,
    bin,
    envFile,
    runtimeEnvFile,
    adcFile,
    dockerConfigFile,
    dockerLog,
    dockerConfigEnvLog,
  };
}

async function runLauncher(fixture, mode, action = ["config"], extraEnv = {}) {
  return execFileAsync(launcher, ["--firebase", mode, ...action], {
    cwd: repositoryRoot,
    env: {
      ...process.env,
      PATH: `${fixture.bin}:${process.env.PATH}`,
      SUMI_LOCAL_ENV_FILE: fixture.envFile,
      SUMI_LOCAL_RUNTIME_ENV_FILE: fixture.runtimeEnvFile,
      SUMI_TEST_DOCKER_LOG: fixture.dockerLog,
      SUMI_TEST_DOCKER_CONFIG_ENV_LOG: fixture.dockerConfigEnvLog,
      FIREBASE_AUTH_EMULATOR_HOST: "must-be-cleared",
      VITE_FIREBASE_AUTH_EMULATOR_URL: "must-be-cleared",
      ...extraEnv,
    },
  });
}

async function dockerArguments(fixture) {
  return (await readFile(fixture.dockerLog, "utf8")).trim().split("\n");
}

async function writeExecutable(path, contents) {
  await writeFile(path, contents, { mode: 0o700 });
  await chmod(path, 0o700);
}
