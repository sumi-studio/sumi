import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import {
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

const run = promisify(execFile);
const edgeDirectory = dirname(fileURLToPath(import.meta.url));

test("asset staging removes the dormant MCP sandbox and adds static policy", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "sumi-edge-stage-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const source = join(root, "source");
  const destination = join(root, "staged");
  await mkdir(source);
  await writeFile(join(source, "index.html"), "<!doctype html>\n");
  await writeFile(join(source, "mcp-app-sandbox.html"), "must not ship\n");
  await run("bash", [
    resolve(edgeDirectory, "stage-assets.sh"),
    source,
    destination,
  ]);

  assert.match(
    await readFile(join(destination, "index.html"), "utf8"),
    /doctype/,
  );
  await assert.rejects(stat(join(destination, "mcp-app-sandbox.html")));
  assert.match(
    await readFile(join(destination, ".assetsignore"), "utf8"),
    /mcp-app-sandbox/,
  );
  assert.match(
    await readFile(join(destination, "_headers"), "utf8"),
    /Content-Security-Policy/,
  );
  assert.match(
    await readFile(join(destination, "_headers"), "utf8"),
    /\/\*[\s\S]*Cache-Control: no-store[\s\S]*\/assets\/\*[\s\S]*immutable/,
  );
});

test("asset staging rejects symlinks instead of uploading their targets", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "sumi-edge-symlink-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const source = join(root, "source");
  await mkdir(source);
  await writeFile(join(root, "outside"), "must not upload\n");
  await symlink(join(root, "outside"), join(source, "linked"));
  await assert.rejects(
    run("bash", [
      resolve(edgeDirectory, "stage-assets.sh"),
      source,
      join(root, "staged"),
    ]),
    /static source contains a symlink/,
  );
});

test("wrangler rendering binds one exact route to one immutable app SHA", async (t) => {
  const generated = resolve(edgeDirectory, "wrangler.generated.json");
  t.after(() => rm(generated, { force: true }));
  await run("node", [resolve(edgeDirectory, "render-config.mjs")], {
    env: {
      ...process.env,
      SUMI_CANONICAL_HOST: "workspace.example.com",
      SUMI_CLOUDFLARE_ZONE: "example.com",
      SUMI_APP_SHA: "0123456789abcdef0123456789abcdef01234567",
    },
  });
  const config = JSON.parse(await readFile(generated, "utf8"));
  assert.deepEqual(config.routes, [
    { pattern: "workspace.example.com/*", zone_name: "example.com" },
  ]);
  assert.equal(
    config.vars.SUMI_APP_SHA,
    "0123456789abcdef0123456789abcdef01234567",
  );
});
