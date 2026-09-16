#!/usr/bin/env node
// Verify a deployed Sumi files service and, optionally, one JuiceFS client's
// view of the same scope. Writes only under <scope>/.sumi-probe/ and removes
// what it wrote. Tokens are read from files and never printed.
//
//   node scripts/operations/files-cloud-probe.mjs \
//     --api http://127.0.0.1:8780 --token-file ~/.config/sumi-files/probe-token \
//     --scope <scope> [--other-scope <scope the token must NOT reach>] \
//     [--mount-dir /var/lib/sumi-files/mnt/<scope>]   # a client mount of the same scope
//
// PASS requires every executed check to pass. Without --mount-dir only the API
// is verified; the cross-client check needs a host that mounts the volume.
import { createHash, randomBytes } from "node:crypto";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { join } from "node:path";

const args = {};
for (let i = 2; i < process.argv.length; i += 2) {
  const k = process.argv[i];
  if (!k?.startsWith("--")) {
    console.error(`unexpected argument ${k}`);
    process.exit(2);
  }
  args[k.slice(2)] = process.argv[i + 1];
}
for (const req of ["api", "token-file", "scope"]) {
  if (!args[req]) {
    console.error(`--${req} is required`);
    process.exit(2);
  }
}
const api = args.api.replace(/\/+$/, "");
const token = readFileSync(args["token-file"], "utf8").trim();
const scope = args.scope;
const results = [];
const record = (name, ok, detail) => {
  results.push(ok);
  console.log(`${ok ? "PASS" : "FAIL"}\t${name}\t${detail}`);
};
const sha = (b) => createHash("sha256").update(b).digest("hex");

async function call(
  method,
  op,
  { sc = scope, query = {}, body, headers = {}, auth = true } = {},
) {
  const url = new URL(`${api}/v1/files/${encodeURIComponent(sc)}/${op}`);
  for (const [k, v] of Object.entries(query)) url.searchParams.set(k, v);
  const h = { ...headers };
  if (auth) h.authorization = `Bearer ${token}`;
  const res = await fetch(url, {
    method,
    headers: h,
    body,
    signal: AbortSignal.timeout(60_000),
  });
  const buf = Buffer.from(await res.arrayBuffer());
  return { status: res.status, buf, headers: res.headers };
}

const stamp = `${new Date().toISOString().replace(/[:.]/g, "")}-${randomBytes(3).toString("hex")}`;
const apiPath = `.sumi-probe/api-${stamp}.bin`;
const fsPath = `.sumi-probe/fs-${stamp}.txt`;
const apiBytes = randomBytes(64 * 1024);

try {
  const health = await fetch(`${api}/healthz`, {
    signal: AbortSignal.timeout(10_000),
  });
  record("healthz", health.status === 200, `status=${health.status}`);

  const w = await call("PUT", "write", {
    query: { path: apiPath },
    headers: { "if-version": "none" },
    body: apiBytes,
  });
  record(
    "api-write",
    w.status === 200,
    `status=${w.status} ${w.status === 200 ? "" : w.buf.toString().slice(0, 160)}`,
  );

  const r = await call("GET", "read", { query: { path: apiPath } });
  record(
    "api-read-back",
    r.status === 200 && sha(r.buf) === sha(apiBytes),
    `status=${r.status} version=${r.headers.get("x-file-version")}`,
  );

  const anon = await call("GET", "read", {
    query: { path: apiPath },
    auth: false,
  });
  record("refuse-no-token", anon.status === 403, `status=${anon.status}`);

  const reserved = await call("GET", "stat", {
    query: { path: ".filesv-op-probe/x" },
  });
  record(
    "refuse-reserved-name",
    reserved.status === 400,
    `status=${reserved.status}`,
  );

  if (args["other-scope"]) {
    const other = await call("GET", "list", {
      sc: args["other-scope"],
      query: { path: "/" },
    });
    record(
      "refuse-ungranted-scope",
      other.status === 403,
      `status=${other.status}`,
    );
  }

  if (args["mount-dir"]) {
    const dir = args["mount-dir"];
    const local = join(dir, apiPath);
    const seen = existsSync(local) ? readFileSync(local) : null;
    record(
      "mount-sees-api-write",
      seen !== null && sha(seen) === sha(apiBytes),
      seen === null
        ? `absent at ${local}`
        : `sha ${sha(seen) === sha(apiBytes) ? "matches" : "differs"}`,
    );
    const text = `written through the mount at ${stamp}\n`;
    mkdirSync(join(dir, ".sumi-probe"), { recursive: true });
    writeFileSync(join(dir, fsPath), text, { flush: true });
    const back = await call("GET", "read", { query: { path: fsPath } });
    record(
      "api-sees-mount-write",
      back.status === 200 && back.buf.toString() === text,
      `status=${back.status}`,
    );
    rmSync(join(dir, fsPath), { force: true });
  }
} catch (err) {
  record("probe-run", false, err instanceof Error ? err.message : String(err));
} finally {
  await call("DELETE", "remove", {
    query: { path: apiPath },
    headers: { "if-version": "any" },
  }).catch(() => {});
}

const ok = results.length > 0 && results.every(Boolean);
console.log(ok ? "RESULT\tPASS" : "RESULT\tFAIL");
process.exit(ok ? 0 : 1);
