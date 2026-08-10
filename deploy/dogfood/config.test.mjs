import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import {
  chmod,
  mkdir,
  mkdtemp,
  readFile,
  rm,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const run = promisify(execFile);
const directory = dirname(fileURLToPath(import.meta.url));

test("operator configuration accepts only exact images and protected real inputs", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "sumi-dogfood-config-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const state = join(root, "state");
  await mkdir(state);
  await chmod(state, 0o711);
  const operationLock = join(state, ".operations.lock");
  await writeFile(operationLock, "", { mode: 0o600 });
  const postgres = await secret(root, "postgres");
  const dockerConfig = await registryConfig(root);
  const firebase = await secret(root, "firebase");
  const tunnel = await secret(root, "tunnel");
  const environment = {
    ...process.env,
    SUMI_API_IMAGE: `ghcr.io/sumi-studio/sumi-api@sha256:${"1".repeat(64)}`,
    SUMI_PROVISIONER_IMAGE: `ghcr.io/sumi-studio/sumi-runtime-provisioner@sha256:${"2".repeat(64)}`,
    SUMI_POSTGRES_IMAGE: `postgres:17-alpine@sha256:${"3".repeat(64)}`,
    SUMI_CLOUDFLARED_IMAGE: `cloudflare/cloudflared@sha256:${"4".repeat(64)}`,
    SUMI_APP_SHA: "5".repeat(40),
    SUMI_CANONICAL_HOST: "workspace.example.com",
    SUMI_CLOUDFLARE_ZONE: "example.com",
    SUMI_DOGFOOD_STATE_ROOT: state,
    SUMI_DOGFOOD_DOCKER_CONTEXT: "default",
    SUMI_DOGFOOD_OPERATION_LOCK: operationLock,
    SUMI_POSTGRES_PASSWORD_FILE: postgres,
    SUMI_DOCKER_CONFIG_FILE: dockerConfig,
    SUMI_FIREBASE_ADC_FILE: firebase,
    SUMI_CLOUDFLARE_TUNNEL_TOKEN_FILE: tunnel,
    SUMI_DB_URL:
      "postgres://sumi:not-a-real-secret@postgres:5432/sumi?sslmode=disable",
    SUMI_LOCAL_CONTROL_TENANT_ID: "dogfood",
    SUMI_AGENT_TOKEN_SECRET: signingSecret(1),
    SUMI_BROWSER_SESSION_SECRET: signingSecret(2),
    SUMI_PROVIDER_API_KEY: "provider-secret",
    SUMI_APPROVAL_SECRET_DIGEST_KEY: "6".repeat(64),
  };

  await run(process.execPath, [resolve(directory, "validate-env.mjs")], {
    env: environment,
  });
  await assert.rejects(
    run(process.execPath, [resolve(directory, "validate-env.mjs")], {
      env: {
        ...environment,
        SUMI_API_IMAGE: "ghcr.io/sumi-studio/sumi-api:latest",
      },
    }),
    /image@sha256 digest/,
  );
  await assert.rejects(
    run(process.execPath, [resolve(directory, "validate-env.mjs")], {
      env: {
        ...environment,
        SUMI_DB_URL:
          "postgres://sumi:different@postgres:5432/sumi?sslmode=disable",
      },
    }),
    /password differs/,
  );
  await assert.rejects(
    run(process.execPath, [resolve(directory, "validate-env.mjs")], {
      env: { ...environment, SUMI_AGENT_TOKEN_SECRET: "not-base64" },
    }),
    /canonical base64/,
  );
  await chmod(tunnel, 0o644);
  await assert.rejects(
    run(process.execPath, [resolve(directory, "validate-env.mjs")], {
      env: environment,
    }),
    /no group\/other permissions/,
  );
});

test("origin preflight uses a local explicit Docker context and performs no mutation", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "sumi-dogfood-preflight-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const state = join(root, "state");
  const bin = join(root, "bin");
  const log = join(root, "docker.log");
  await mkdir(state);
  await chmod(state, 0o711);
  const operationLock = join(state, ".operations.lock");
  await writeFile(operationLock, "", { mode: 0o600 });
  await mkdir(bin);
  const postgres = await secret(root, "postgres");
  const dockerConfig = await registryConfig(root);
  const firebase = await secret(root, "firebase");
  const tunnel = await secret(root, "tunnel");
  const operator = join(root, "operator.env");
  await writeFile(
    operator,
    [
      `SUMI_API_IMAGE=ghcr.io/sumi-studio/sumi-api@sha256:${"1".repeat(64)}`,
      `SUMI_PROVISIONER_IMAGE=ghcr.io/sumi-studio/sumi-runtime-provisioner@sha256:${"2".repeat(64)}`,
      `SUMI_POSTGRES_IMAGE=postgres:17-alpine@sha256:${"3".repeat(64)}`,
      `SUMI_CLOUDFLARED_IMAGE=cloudflare/cloudflared@sha256:${"4".repeat(64)}`,
      `SUMI_APP_SHA=${"5".repeat(40)}`,
      "SUMI_CANONICAL_HOST=workspace.example.com",
      "SUMI_CLOUDFLARE_ZONE=example.com",
      "SUMI_DOGFOOD_DOCKER_CONTEXT=dogfood-test",
      `SUMI_DOGFOOD_STATE_ROOT=${state}`,
      `SUMI_DOGFOOD_OPERATION_LOCK=${operationLock}`,
      `SUMI_POSTGRES_PASSWORD_FILE=${postgres}`,
      `SUMI_DOCKER_CONFIG_FILE=${dockerConfig}`,
      `SUMI_FIREBASE_ADC_FILE=${firebase}`,
      `SUMI_CLOUDFLARE_TUNNEL_TOKEN_FILE=${tunnel}`,
      "SUMI_DB_URL=postgres://sumi:not-a-real-secret@postgres:5432/sumi?sslmode=disable",
      "SUMI_LOCAL_CONTROL_TENANT_ID=dogfood",
      `SUMI_AGENT_TOKEN_SECRET=${signingSecret(1)}`,
      `SUMI_BROWSER_SESSION_SECRET=${signingSecret(2)}`,
      `SUMI_APPROVAL_SECRET_DIGEST_KEY=${"6".repeat(64)}`,
      "SUMI_PROVIDER_API_KEY=provider-secret",
      "",
    ].join("\n"),
    { mode: 0o600 },
  );
  const docker = join(bin, "docker");
  const rendered = JSON.stringify({
    services: {
      api: {
        image: `api@sha256:${"1".repeat(64)}`,
        ports: [],
        deploy: { replicas: 1, update_config: { order: "stop-first" } },
        healthcheck: { test: ["CMD", "wget", "/ready"] },
        volumes: [{ source: state, target: "/var/lib/sumi" }],
      },
      cloudflared: {
        image: `cloudflared@sha256:${"4".repeat(64)}`,
        ports: [],
        command: [
          "tunnel",
          "run",
          "--token-file",
          "/run/secrets/cloudflare_tunnel_token",
        ],
        depends_on: { api: { condition: "service_healthy" } },
      },
      "runtime-provisioner": {
        image: `provisioner@sha256:${"2".repeat(64)}`,
        ports: [],
        environment: { DOCKER_CONFIG: "/run/sumi/docker-config" },
        volumes: [
          {
            source: dockerConfig,
            target: "/run/sumi/docker-config/config.json",
            read_only: true,
          },
        ],
      },
      postgres: {
        image: `postgres@sha256:${"3".repeat(64)}`,
        ports: [],
        volumes: [
          { source: "postgres-data", target: "/var/lib/postgresql/data" },
        ],
      },
      "database-client": {
        image: `postgres@sha256:${"3".repeat(64)}`,
        ports: [],
        profiles: ["maintenance"],
        read_only: true,
        cap_drop: ["ALL"],
        networks: { data: null },
        volumes: [],
      },
    },
    networks: { data: { internal: true } },
  });
  await writeFile(
    docker,
    `#!/usr/bin/env bash
set -Eeuo pipefail
printf '%s\\n' "$*" >> "\${SUMI_TEST_DOCKER_LOG}"
if [[ "$*" == *"context inspect"* ]]; then
  printf 'unix:///var/run/docker.sock\\n'
elif [[ "$*" == *"config --format json"* ]]; then
  printf '%s\\n' '${rendered}'
else
  printf 'mutating Docker command reached during preflight\\n' >&2
  exit 9
fi
`,
  );
  await chmod(docker, 0o700);

  const result = await run(
    "bash",
    [resolve(directory, "deploy-origin.sh"), operator, "--check"],
    {
      env: {
        ...process.env,
        PATH: `${bin}:${process.env.PATH ?? ""}`,
        SUMI_TEST_DOCKER_LOG: log,
      },
    },
  );
  assert.match(result.stdout, /no image pull, migration, restart, or deploy/);
  assert.doesNotMatch(await readFile(log, "utf8"), /pull|stop|up -d|run --rm/);

  await writeFile(
    operator,
    `${await readFile(operator, "utf8")}PATH=/tmp/untrusted\n`,
    { mode: 0o600 },
  );
  await assert.rejects(
    run("bash", [resolve(directory, "deploy-origin.sh"), operator, "--check"], {
      env: {
        ...process.env,
        PATH: `${bin}:${process.env.PATH ?? ""}`,
        SUMI_TEST_DOCKER_LOG: log,
      },
    }),
    /outside the SUMI_ namespace/,
  );
});

async function secret(root, name) {
  const path = join(root, name);
  await writeFile(path, "not-a-real-secret\n", { mode: 0o600 });
  return path;
}

async function registryConfig(root) {
  const directory = join(root, "docker");
  await mkdir(directory);
  const path = join(directory, "config.json");
  const auth = Buffer.from("dogfood:not-a-real-token").toString("base64");
  await writeFile(
    path,
    `${JSON.stringify({ auths: { "ghcr.io": { auth } } })}\n`,
    {
      mode: 0o600,
    },
  );
  return path;
}

function signingSecret(byte) {
  return Buffer.alloc(32, byte).toString("base64");
}
