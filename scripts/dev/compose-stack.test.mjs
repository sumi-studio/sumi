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

test("emulator mode selects only the base Compose file", async () => {
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
  const dockerLog = join(root, "docker.args");
  await mkdir(bin);
  if (fakeDocker) {
    await writeExecutable(
      join(bin, "docker"),
      `#!/bin/sh\nprintf '%s\\n' "$@" > "$SUMI_TEST_DOCKER_LOG"\n`,
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
  return { root, bin, envFile, runtimeEnvFile, adcFile, dockerLog };
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
