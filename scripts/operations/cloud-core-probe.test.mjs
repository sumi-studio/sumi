import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtemp, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import test from "node:test";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const probe = resolve(
  dirname(fileURLToPath(import.meta.url)),
  "cloud-core-probe.mjs",
);
const PERSONA = "01a0a3b5-31c3-751a-9a3f-aa8d9817ba8d";
const WAKE = "wake-credential-for-probe-test-012345";

// A stand-in for the deployed Worker: the routes the probe touches, with a
// switchable do-runtime-auth outcome — "good" answers like a DO whose
// installed runtime secret authenticates through the binding, "bad" like
// one holding a wrong secret (503 unauthorized).
async function stubCore(mode) {
  const server = createServer((req, res) => {
    const authed = req.headers.authorization === `Bearer ${WAKE}`;
    const json = (status, body) => {
      res.writeHead(status, { "content-type": "application/json" });
      res.end(JSON.stringify(body));
    };
    if (req.url === "/health") return json(200, { ok: true });
    if (req.url === "/health/state") {
      if (!authed) return json(401, { error: "unauthorized" });
      return json(200, { ok: true, via: "binding", state_status: 200 });
    }
    const m = /^\/personas\/([^/]+)\/(wake|check)$/.exec(req.url ?? "");
    if (!m) return json(404, { error: "not found" });
    if (!authed) return json(401, { error: "unauthorized" });
    if (m[2] === "check") {
      if (mode === "bad")
        return json(503, { ok: false, via: "binding", reason: "unauthorized" });
      return json(200, { ok: true, via: "binding", state_status: 200 });
    }
    if (mode === "bad")
      return json(503, { ok: false, persona: m[1], reason: "unauthorized" });
    return json(200, { ok: true, persona: m[1], coalesced: false });
  });
  await new Promise((r) => server.listen(0, "127.0.0.1", r));
  return { server, url: `http://127.0.0.1:${server.address().port}` };
}

async function run(args) {
  const dir = await mkdtemp(join(tmpdir(), "cloud-core-probe-test-"));
  const tokenFile = join(dir, "wake-token");
  await writeFile(tokenFile, WAKE);
  return await new Promise((resolvePromise, reject) => {
    const child = spawn(
      process.execPath,
      [probe, "--wake-token-file", tokenFile, ...args],
      { stdio: ["ignore", "pipe", "pipe"] },
    );
    let out = "";
    let err = "";
    child.stdout.on("data", (c) => (out += c));
    child.stderr.on("data", (c) => (err += c));
    child.on("error", reject);
    child.on("close", (code) => resolvePromise({ code, out, err }));
  });
}

test("without --persona the probe refuses before claiming readiness", async () => {
  const { server, url } = await stubCore("good");
  try {
    const r = await run(["--core", url]);
    assert.notEqual(r.code, 0, "must not exit 0");
    assert.match(r.err, /--persona is required/);
    assert.doesNotMatch(r.out, /"summary":"PASS"/);
  } finally {
    server.close();
  }
});

test("a malformed --persona is refused", async () => {
  const { server, url } = await stubCore("good");
  try {
    const r = await run(["--core", url, "--persona", "not-a-persona"]);
    assert.notEqual(r.code, 0);
    assert.match(r.err, /uuidv7/);
    assert.doesNotMatch(r.out, /"summary":"PASS"/);
  } finally {
    server.close();
  }
});

test("a healthy deployment passes all checks including do-runtime-auth", async () => {
  const { server, url } = await stubCore("good");
  try {
    const r = await run(["--core", url, "--persona", PERSONA]);
    assert.equal(r.code, 0, r.out + r.err);
    assert.match(r.out, /"check":"do-runtime-auth","ok":true/);
    assert.match(r.out, /"summary":"PASS"/);
  } finally {
    server.close();
  }
});

test("a wrong installed runtime secret fails do-runtime-auth despite healthy routes", async () => {
  const { server, url } = await stubCore("bad");
  try {
    const r = await run(["--core", url, "--persona", PERSONA]);
    assert.equal(r.code, 1, r.out + r.err);
    assert.match(r.out, /"check":"do-runtime-auth","ok":false/);
    assert.match(r.out, /"summary":"FAIL","failed":\[[^\]]*"do-runtime-auth"/);
  } finally {
    server.close();
  }
});
