import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const run = promisify(execFile);
const directory = dirname(fileURLToPath(import.meta.url));

test("dedicated smoke runner cannot report success when real inputs are absent", async () => {
  await assert.rejects(
    run("bash", [resolve(directory, "run.sh")], {
      env: { PATH: process.env.PATH ?? "" },
    }),
    (error) => {
      assert.equal(error.code, 2);
      assert.match(error.stderr, /NOT COVERED/);
      return true;
    },
  );
});

test("browser smoke owns API restart, Tunnel restart, cursor catch-up, and nonce replay", async () => {
  const source = await readFile(
    resolve(directory, "../../../apps/web/e2e/dogfood-restart.spec.ts"),
    "utf8",
  );
  for (const evidence of [
    "SUMI_DOGFOOD_RESTART_API_HELPER",
    "SUMI_DOGFOOD_RESTART_TUNNEL_HELPER",
    "caught_up",
    "discarding a successful receipt",
    "client_nonce",
  ]) {
    assert.match(source, new RegExp(evidence));
  }
});
