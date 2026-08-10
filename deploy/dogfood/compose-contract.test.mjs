import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const directory = dirname(fileURLToPath(import.meta.url));

test("dogfood compose has one stop-first API and no published ports", async () => {
  const compose = await readFile(resolve(directory, "compose.yaml"), "utf8");
  assert.match(
    compose,
    /api:[\s\S]*?deploy:\s*\n\s+replicas: 1[\s\S]*?order: stop-first/,
  );
  assert.doesNotMatch(compose, /^\s+ports:/m);
  assert.match(compose, /http:\/\/127\.0\.0\.1:8080\/ready/);
  assert.match(
    compose,
    /cloudflared:[\s\S]*?api: \{ condition: service_healthy \}/,
  );
  assert.match(compose, /SUMI_AGENT_IMAGE_TAG: \$\{SUMI_APP_SHA/);
  assert.doesNotMatch(compose, /latest/);
  assert.match(
    compose,
    /--token-file, \/run\/secrets\/cloudflare_tunnel_token/,
  );
  assert.match(compose, /postgres-data:\/var\/lib\/postgresql\/data/);
  assert.match(
    compose,
    /database-client:[\s\S]*?profiles: \[maintenance\][\s\S]*?networks: \[data\]/,
  );
  assert.match(compose, /data:\s*\n\s+internal: true/);
  assert.match(
    compose,
    /DOCKER_CONFIG: \/run\/sumi\/docker-config[\s\S]*?target: \/run\/sumi\/docker-config\/config\.json/,
  );
});

test("operator template does not contain a usable secret", async () => {
  const template = await readFile(
    resolve(directory, "operator.env.example"),
    "utf8",
  );
  for (const name of [
    "SUMI_AGENT_TOKEN_SECRET",
    "SUMI_BROWSER_SESSION_SECRET",
    "SUMI_PROVIDER_API_KEY",
  ]) {
    assert.match(template, new RegExp(`^${name}=<[^>]+>$`, "m"));
  }
  assert.match(template, /^SUMI_DOCKER_CONFIG_FILE=\//m);
  assert.match(template, /^SUMI_CLOUDFLARE_TUNNEL_TOKEN_FILE=\//m);
});

test("operator preflight is explicitly non-mutating", async () => {
  const deploy = await readFile(resolve(directory, "deploy-origin.sh"), "utf8");
  assert.match(deploy, /--profile maintenance config --format json/);
  const preflight = deploy.search(/if \[\[ "\$\{mode\}" == "--check" \]\]/);
  const lock = deploy.indexOf("/usr/bin/flock --nonblock");
  const pull = deploy.search(/"\$\{compose\[@\]\}" pull/);
  assert.ok(preflight > 0 && lock > preflight && pull > lock);
  const stop = deploy.search(/"\$\{compose\[@\]\}" stop api/);
  const migrate = deploy.search(/"\$\{compose\[@\]\}" run --rm migrate apply/);
  const start = deploy.search(
    /"\$\{compose\[@\]\}" up -d --no-deps --force-recreate api/,
  );
  assert.ok(stop > pull && migrate > stop && start > migrate);
  assert.match(
    deploy,
    /no image pull, migration, restart, or deploy was performed/,
  );
});

test("host preparation rejects pre-existing symlinked durable paths", async () => {
  const prepare = await readFile(resolve(directory, "prepare-host.sh"), "utf8");
  assert.match(prepare, /\[\[ ! -L "\$\{path\}" \]\]/);
  assert.match(prepare, /\[\[ ! -L "\$\{operation_lock\}" \]\]/);
  const rejection = prepare.indexOf("refusing symlinked state directory");
  const installation = prepare.indexOf("install -d -o 0 -g 0 -m 0711");
  assert.ok(rejection > 0 && installation > rejection);
});

test("cutover record cannot omit recovery and restart evidence", async () => {
  const record = JSON.parse(
    await readFile(resolve(directory, "cutover-record.template.json"), "utf8"),
  );
  assert.equal(record.status, "not_cut_over");
  assert.equal(record.backup_rehearsal.snapshot_id, null);
  assert.equal(record.backup_rehearsal.restored_at, null);
  assert.equal(record.backup_rehearsal.restored_agent_volume_map_sha256, null);
  assert.deepEqual(record.protected_data, [
    "postgres_control_plane",
    "messaging_metadata",
    "messaging_read_state",
    "messaging_attachments",
    "api_command_log",
    "api_runtime_state",
    "personality_agent_private_volumes",
  ]);
  assert.equal(record.restart_smoke.result, null);
  assert.equal(record.edge.deployment_id, null);
});
