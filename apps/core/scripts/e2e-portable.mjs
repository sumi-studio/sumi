#!/usr/bin/env node
/**
 * E2E: one secretary moves between two placements and continues there.
 *
 * Two REAL Go state services (state-dev, which mounts the transfer routes)
 * on two separate REAL PostgreSQL databases, with different service secrets,
 * driven by the real Node core process. The model is the mock provider: this
 * proves state continuity and fencing, not model recall quality.
 *
 *   1. Local: the core answers messages, writes a memory note, sets a reminder.
 *   2. Local: while a slow turn is in flight the transfer seals the cut. The
 *      running core is fenced at its next state call and stops; new inputs
 *      and new writers are refused.
 *   3. The bundle streams Local → Cloud. It is addressed to Cloud's
 *      placement id, so importing it back into Local is refused. Truncated
 *      and altered copies are refused and leave nothing behind; the real
 *      one stages. Staging runs nothing. Importing the same bundle again
 *      replays the receipt.
 *   4. Cloud activates and produces an activate_proof. Local cannot
 *      complete without it — the proof is read back from Cloud's transfer
 *      status (the lost-response path), then Local completes.
 *   5. Cloud: the core continues the same life — the carried journal and
 *      outbox are identical, the interrupted input runs again as attempt 2,
 *      the queued input is answered, the carried reminder fires, and new work
 *      appends after the cut. Local stays fenced.
 *
 * Requires SUMI_TEST_DB_URL on a server that allows CREATE DATABASE.
 * SUMI_PORTABLE_EVIDENCE_DIR (optional) receives the bundle and a summary.
 *   SUMI_TEST_DB_URL=postgres://sumi:sumi-dev@127.0.0.1:55432/sumi?sslmode=disable \
 *     node scripts/e2e-portable.mjs
 */
import { spawn, spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { isDeepStrictEqual } from "node:util";

const DB_URL = process.env.SUMI_TEST_DB_URL ?? process.env.SUMI_DB_URL;
if (!DB_URL) {
  console.error("e2e-portable: SUMI_TEST_DB_URL required — real PostgreSQL");
  process.exit(2);
}
const EVIDENCE = process.env.SUMI_PORTABLE_EVIDENCE_DIR;
const API_DIR = resolve(import.meta.dirname, "../../api");
const CORE_DIR = resolve(import.meta.dirname, "..");
const run = randomUUID().replaceAll("-", "").slice(0, 10);
// SUMI_PORTABLE_DB_PREFIX namespaces the two databases when parallel
// worktrees own distinct prefixes on a shared PostgreSQL.
const DB_PREFIX = process.env.SUMI_PORTABLE_DB_PREFIX ?? "sumi_portable_repair";
const LOCAL_DB = withDatabase(DB_URL, `${DB_PREFIX}_local_${run}`);
const CLOUD_DB = withDatabase(DB_URL, `${DB_PREFIX}_cloud_${run}`);
const LOCAL_ADMIN = `local-admin-${randomUUID()}`;
const CLOUD_ADMIN = `cloud-admin-${randomUUID()}`;
const LOCAL_PORT = Number(process.env.SUMI_PORTABLE_LOCAL_PORT ?? 9430);
const CLOUD_PORT = Number(process.env.SUMI_PORTABLE_CLOUD_PORT ?? 9431);
const LOCAL = `http://127.0.0.1:${LOCAL_PORT}`;
const CLOUD = `http://127.0.0.1:${CLOUD_PORT}`;
const TRANSFER = `e2e-move-${run}`;

let passed = 0;
const children = [];
process.on("exit", () => {
  for (const c of children) c.kill("SIGKILL");
});

function log(...a) {
  console.log("[portable]", ...a);
}
function check(cond, msg, detail) {
  if (!cond) {
    console.error(`[portable] not ok - ${msg}`);
    if (detail !== undefined)
      console.error(
        "[portable] detail:",
        typeof detail === "string" ? detail : JSON.stringify(detail, null, 1),
      );
    process.exit(1);
  }
  passed++;
  console.log(`[portable] ok ${passed} - ${msg}`);
}

function withDatabase(url, name) {
  const q = url.indexOf("?");
  const base = q >= 0 ? url.slice(0, q) : url;
  return base.slice(0, base.lastIndexOf("/") + 1) + name + (q >= 0 ? url.slice(q) : "");
}

function uuidv7() {
  const now = Date.now().toString(16).padStart(12, "0");
  const r = randomUUID().replaceAll("-", "");
  return `${now.slice(0, 8)}-${now.slice(8, 12)}-7${r.slice(13, 16)}-${((parseInt(r.slice(16, 18), 16) & 0x3f) | 0x80).toString(16).padStart(2, "0")}${r.slice(18, 20)}-${r.slice(20, 32)}`;
}

async function req(base, method, path, token, body, ndjson = false) {
  const res = await fetch(base + path, {
    method,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": ndjson ? "application/x-ndjson" : "application/json",
    },
    body: body === undefined ? undefined : ndjson ? body : JSON.stringify(body),
  });
  const text = await res.text();
  let json = null;
  try {
    json = JSON.parse(text);
  } catch {
    /* ndjson or empty */
  }
  return { status: res.status, json, text };
}

async function waitFor(label, fn, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  let last;
  while (Date.now() < deadline) {
    last = await fn();
    if (last === true) return;
    await new Promise((r) => setTimeout(r, 250));
  }
  check(false, `timed out waiting: ${label}`, last);
}

function startService(label, dbUrl, port, admin) {
  const proc = spawn(bin, [], {
    env: {
      ...process.env,
      SUMI_DB_URL: dbUrl,
      SUMI_DB_CREATE: "1",
      SUMI_CORE_STATE_TOKEN: admin,
      SUMI_STATE_LISTEN: `127.0.0.1:${port}`,
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  proc.stderr.on("data", (d) => process.stderr.write(`[${label}] ${d}`));
  children.push(proc);
  return proc;
}

function coreEnv(base, personaId, token, holder) {
  return {
    ...process.env,
    SUMI_STATE_URL: base,
    SUMI_PERSONA_ID: personaId,
    SUMI_PERSONA_TOKEN: token,
    SUMI_MODEL_PROVIDER: "mock",
    SUMI_HOLDER_ID: holder,
    SUMI_LEASE_TTL_MS: "4000",
  };
}

function startCore(label, env) {
  const proc = spawn(process.execPath, ["src/host/local.ts"], {
    cwd: CORE_DIR,
    env,
    stdio: ["ignore", "pipe", "pipe"],
  });
  let output = "";
  const tee = (d) => {
    output += d;
    process.stdout.write(`[${label}] ${d}`);
  };
  proc.stdout.on("data", tee);
  proc.stderr.on("data", tee);
  children.push(proc);
  const exited = new Promise((res) => proc.on("exit", (code, signal) => res({ code, signal })));
  return { proc, exited, output: () => output };
}

async function submit(base, personaId, token, inputId, text) {
  const r = await req(base, "POST", `/internal/core/personas/${personaId}/inputs`, token, {
    input_id: inputId,
    kind: "message",
    payload: { text },
    actor_kind: "human",
    actor_id: "owner",
    source_surface: "e2e-portable",
  });
  return r;
}

async function allPages(base, path, token) {
  const out = [];
  let after = 0;
  for (;;) {
    const r = await req(base, "GET", `${path}?after_seq=${after}&limit=1000`, token);
    check(r.status === 200, `read ${path}`, r.text);
    const page = r.json.events ?? r.json.outbox ?? r.json.entries ?? r.json;
    const rows = Array.isArray(page) ? page : [];
    if (rows.length === 0) return out;
    out.push(...rows);
    after = rows[rows.length - 1].seq;
  }
}

async function inputStatus(base, personaId, token, inputId) {
  const r = await req(base, "GET", `/internal/core/personas/${personaId}/inputs/${encodeURIComponent(inputId)}`, token);
  return r.status === 200 ? r.json : null;
}

// --- build and start two placements --------------------------------------
const binDir = mkdtempSync(join(tmpdir(), "sumi-portable-e2e-"));
const bin = join(binDir, "state-dev");
log("building state-dev…");
const build = spawnSync("go", ["build", "-buildvcs=false", "-o", bin, "./cmd/state-dev"], {
  cwd: API_DIR,
  stdio: "inherit",
});
check(build.status === 0, "state-dev builds");
log("databases:", LOCAL_DB.replace(/\/\/[^@]*@/, "//***@"), CLOUD_DB.replace(/\/\/[^@]*@/, "//***@"));
startService("local-state", LOCAL_DB, LOCAL_PORT, LOCAL_ADMIN);
startService("cloud-state", CLOUD_DB, CLOUD_PORT, CLOUD_ADMIN);
for (const base of [LOCAL, CLOUD]) {
  await waitFor(`${base} health`, async () => {
    try {
      return (await fetch(`${base}/health`)).ok;
    } catch {
      return false;
    }
  }, 30_000);
}
check(true, "local and cloud state services are up on separate databases");

// --- 1. the local secretary lives ------------------------------------------
const pid = uuidv7();
const created = await req(LOCAL, "POST", "/internal/core/personas", LOCAL_ADMIN, {
  persona_id: pid,
  display_name: "Portable e2e secretary",
});
check(created.status === 201, "local persona created", created.text);
const localToken = created.json.persona_token;

for (const [id, text] of [
  ["in-hello", "hello from the local machine"],
  ["in-note", '!journal.note {"text":"The user takes tea at 15:00"}'],
  ["in-remind", '!schedule.set {"wake_at":"+15000","payload":{"text":"tea time reminder"}}'],
]) {
  const r = await submit(LOCAL, pid, localToken, id, text);
  check(r.status === 201, `local input ${id} accepted`, r.text);
}
const first = spawnSync(process.execPath, ["src/host/local.ts", "--once"], {
  cwd: CORE_DIR,
  env: { ...coreEnv(LOCAL, pid, localToken, "local-core"), SUMI_ONCE_IDLE_MS: "1500" },
  stdio: "inherit",
  timeout: 90_000,
});
check(first.status === 0, "local core drained its first three messages");
let st = await req(LOCAL, "GET", `/internal/core/personas/${pid}/state`, localToken);
check(st.json.queued_inputs === 0 && st.json.pending_schedules === 1, "local reminder is pending", st.json);

// --- 2. seal while a slow turn is in flight --------------------------------
check((await submit(LOCAL, pid, localToken, "in-slow", "!slow 8000 finishing before the move")).status === 201, "slow input accepted");
const localCore = startCore("local-core", coreEnv(LOCAL, pid, localToken, "local-core"));
await waitFor("local slow turn running", async () => {
  const s = await req(LOCAL, "GET", `/internal/core/personas/${pid}/state`, localToken);
  return s.json?.running_turn ? true : s.json;
}, 20_000);
check((await submit(LOCAL, pid, localToken, "in-queued", "please check my calendar after the move")).status === 201, "queued input accepted before the seal");

const cloudPlacement = await req(CLOUD, "GET", "/internal/core/placement", CLOUD_ADMIN);
check(cloudPlacement.status === 200 && typeof cloudPlacement.json.placement_id === "string", "cloud exposes its placement id", cloudPlacement.text);
const localPlacement = await req(LOCAL, "GET", "/internal/core/placement", LOCAL_ADMIN);
check(
  localPlacement.status === 200 && localPlacement.json.placement_id !== cloudPlacement.json.placement_id,
  "the two placements have distinct durable ids",
  { local: localPlacement.json, cloud: cloudPlacement.json },
);

const noDest = await req(LOCAL, "POST", `/internal/core/personas/${pid}/transfers/${TRANSFER}/seal`, LOCAL_ADMIN);
check(noDest.status === 400, "seal without a destination is refused", noDest.text);
const seal = await req(LOCAL, "POST", `/internal/core/personas/${pid}/transfers/${TRANSFER}/seal`, LOCAL_ADMIN, {
  destination_id: cloudPlacement.json.placement_id,
});
check(seal.status === 200 && seal.json.status === "sealed", "local seal bound to cloud", seal.text);
check(seal.json.destination_id === cloudPlacement.json.placement_id, "seal receipt names the destination", seal.json);
const cont = seal.json.continuity;
check(
  cont.running_turns === 1 && cont.claimed_inputs === 1 && cont.queued_inputs === 1 && cont.notes === 1 && cont.pending_schedules === 1,
  "seal receipt lists the in-flight turn, queued input, note and reminder",
  cont,
);
const sealReplay = await req(LOCAL, "POST", `/internal/core/personas/${pid}/transfers/${TRANSFER}/seal`, LOCAL_ADMIN, {
  destination_id: cloudPlacement.json.placement_id,
});
check(sealReplay.status === 200 && sealReplay.json.sealed_at === seal.json.sealed_at, "seal replay returns the same cut");
const sealSelf = await req(LOCAL, "POST", `/internal/core/personas/${pid}/transfers/${TRANSFER}/seal`, LOCAL_ADMIN, {
  destination_id: localPlacement.json.placement_id,
});
check(sealSelf.status === 400, "a seal cannot address the source placement itself", sealSelf.text);
const sealOther = await req(LOCAL, "POST", `/internal/core/personas/${pid}/transfers/${TRANSFER}/seal`, LOCAL_ADMIN, {
  destination_id: uuidv7(),
});
check(sealOther.status === 409, "seal replayed with a different destination is a conflict", sealOther.text);
const personaTokenSeal = await req(LOCAL, "POST", `/internal/core/personas/${pid}/transfers/other-transfer/seal`, localToken);
check(personaTokenSeal.status === 401, "a persona token cannot drive a transfer");

const localExit = await Promise.race([
  localCore.exited,
  new Promise((r) => setTimeout(() => r(null), 25_000)),
]);
check(localExit !== null, "the running local core stopped by itself after the seal", localCore.output().slice(-2000));
check(
  localExit.code === 0 && localCore.output().includes("lost writer fence"),
  "the local core stopped because its writer generation was fenced",
  { exit: localExit, tail: localCore.output().slice(-1500) },
);
const lateAcquire = await req(LOCAL, "POST", `/internal/core/personas/${pid}/writer/acquire`, localToken, { holder_id: "local-core-2", ttl_ms: 4000 });
check(lateAcquire.status === 409, "no new local writer after the seal", lateAcquire.text);
const lateInput = await submit(LOCAL, pid, localToken, "in-late", "sent to local after the seal");
check(lateInput.status === 409, "new local input refused explicitly after the seal", lateInput.text);
const replayInput = await submit(LOCAL, pid, localToken, "in-queued", "please check my calendar after the move");
check(replayInput.status === 200, "an already-accepted input still replays its receipt", replayInput.text);

// --- 3. stream the bundle ---------------------------------------------------
const bundleRes = await req(LOCAL, "GET", `/internal/core/personas/${pid}/transfers/${TRANSFER}/bundle`, LOCAL_ADMIN);
check(bundleRes.status === 200, "local bundle streamed", bundleRes.text.slice(0, 300));
const bundle = bundleRes.text;
const lines = bundle.trimEnd().split("\n");
const header = JSON.parse(lines[0]);
const trailer = JSON.parse(lines[lines.length - 1]);
check(header.format === "sumi.portable-secretary" && header.format_version === 1 && header.secrets === "none", "bundle header declares format v1 and no secrets", header);
check(
  header.destination_id === cloudPlacement.json.placement_id && /^[0-9a-f]{64}$/.test(header.transfer_key ?? ""),
  "bundle is addressed to cloud's placement and carries a transfer key",
  { destination_id: header.destination_id },
);
check(!bundle.includes(LOCAL_ADMIN) && !bundle.includes(localToken) && !bundle.includes("local-core"), "bundle carries no service secret, persona token or lease holder");
const exportStatus = await req(LOCAL, "GET", `/internal/core/transfers/export/${TRANSFER}`, LOCAL_ADMIN);
check(exportStatus.json.content_sha256 === trailer.content_sha256, "local ledger recorded the exported digest");

// The bundle is addressed to Cloud; Local must refuse to stage it, so an
// ordinary "the response was lost, try the other service" retry can never
// leave a second staged copy.
let r = await req(LOCAL, "POST", "/internal/core/transfers/import", LOCAL_ADMIN, bundle, true);
check(r.status === 422 && r.json.error.includes("addressed to placement"), "local refuses a bundle addressed to cloud", r.text);

const truncated = lines.slice(0, -1).join("\n") + "\n";
r = await req(CLOUD, "POST", "/internal/core/transfers/import", CLOUD_ADMIN, truncated, true);
check(r.status === 422 && r.json.error.includes("truncated"), "cloud refuses a bundle cut before its trailer", r.text);
r = await req(CLOUD, "POST", "/internal/core/transfers/import", CLOUD_ADMIN, bundle.replace("tea at 15:00", "tea at 16:00"), true);
check(r.status === 422 && r.json.error.includes("digest"), "cloud refuses an altered bundle", r.text);
r = await req(CLOUD, "GET", `/internal/core/personas/${pid}/state`, CLOUD_ADMIN);
check(r.status === 404, "refused bundles left no persona on cloud", r.text);

const imported = await req(CLOUD, "POST", "/internal/core/transfers/import", CLOUD_ADMIN, bundle, true);
check(imported.status === 201 && imported.json.status === "staged", "cloud staged the bundle", imported.text);
check(imported.json.content_sha256 === trailer.content_sha256, "cloud verified the same digest");
const cloudPersona = await req(CLOUD, "POST", "/internal/core/personas", CLOUD_ADMIN, { persona_id: pid });
check(cloudPersona.status === 200 && cloudPersona.json.created === false && cloudPersona.json.persona.authority === "staged", "cloud persona exists as staged, not re-created", cloudPersona.text);
const cloudToken = cloudPersona.json.persona_token;
check(cloudToken !== localToken, "cloud issues its own persona token");
r = await req(CLOUD, "POST", `/internal/core/personas/${pid}/writer/acquire`, cloudToken, { holder_id: "cloud-core", ttl_ms: 4000 });
check(r.status === 409, "staged persona cannot acquire a writer", r.text);
r = await submit(CLOUD, pid, cloudToken, "in-early", "sent to cloud before activation");
check(r.status === 409, "staged persona refuses inputs", r.text);
r = await req(CLOUD, "POST", "/internal/core/transfers/import", CLOUD_ADMIN, bundle, true);
check(r.status === 200 && r.json.content_sha256 === trailer.content_sha256, "re-importing the same bundle replays the receipt", r.text);

// --- 4. activate cloud, complete local --------------------------------------
// Without destination evidence the source must not end its authority: a
// complete carrying no proof, a wrong proof, or the source's own digest is
// refused and the secretary stays sealed rather than stranded.
r = await req(LOCAL, "POST", `/internal/core/personas/${pid}/transfers/${TRANSFER}/complete`, LOCAL_ADMIN, {});
check(r.status === 400, "complete without an activate_proof is refused", r.text);
r = await req(LOCAL, "POST", `/internal/core/personas/${pid}/transfers/${TRANSFER}/complete`, LOCAL_ADMIN, {
  activate_proof: imported.json.content_sha256,
});
check(r.status === 409, "complete with a fabricated proof is refused", r.text);
r = await req(LOCAL, "GET", `/internal/core/personas/${pid}/state`, localToken);
check(r.json.persona.authority === "sealed", "local stayed sealed through the refused completes", r.json);

const activated = await req(CLOUD, "POST", `/internal/core/personas/${pid}/transfers/${TRANSFER}/activate`, CLOUD_ADMIN);
check(activated.status === 200 && activated.json.status === "activated", "cloud activated", activated.text);
check(/^[0-9a-f]{64}$/.test(activated.json.activate_proof ?? ""), "cloud produced an activate_proof", activated.json);
const activatedReplay = await req(CLOUD, "POST", `/internal/core/personas/${pid}/transfers/${TRANSFER}/activate`, CLOUD_ADMIN);
check(
  activatedReplay.status === 200 && activatedReplay.json.activate_proof === activated.json.activate_proof,
  "activate replay returns the same proof",
  activatedReplay.text,
);

// The lost-response path: pretend the activate answer never arrived. The
// proof is discoverable from the destination's transfer status, and only
// then can the source complete.
const importStatus = await req(CLOUD, "GET", `/internal/core/transfers/import/${TRANSFER}`, CLOUD_ADMIN);
check(
  importStatus.status === 200 && importStatus.json.status === "activated" &&
    importStatus.json.activate_proof === activated.json.activate_proof,
  "the activate proof is readable from cloud's transfer status",
  importStatus.text,
);
r = await req(LOCAL, "POST", `/internal/core/personas/${pid}/transfers/${TRANSFER}/complete`, LOCAL_ADMIN, {
  activate_proof: importStatus.json.activate_proof,
});
check(r.status === 200 && r.json.status === "completed", "local completed with cloud's activate_proof", r.text);
const completeReplay = await req(LOCAL, "POST", `/internal/core/personas/${pid}/transfers/${TRANSFER}/complete`, LOCAL_ADMIN, {
  activate_proof: importStatus.json.activate_proof,
});
check(completeReplay.status === 200 && completeReplay.json.status === "completed", "complete replay returns the same receipt", completeReplay.text);
st = await req(LOCAL, "GET", `/internal/core/personas/${pid}/state`, localToken);
check(st.json.persona.authority === "transferred", "local persona is marked transferred");
const localEvents = await allPages(LOCAL, `/internal/core/personas/${pid}/events`, localToken);
const localOutbox = await allPages(LOCAL, `/internal/core/personas/${pid}/outbox`, localToken);

// --- 5. the same secretary continues on cloud -------------------------------
const cloudCore = startCore("cloud-core", coreEnv(CLOUD, pid, cloudToken, "cloud-core"));
await waitFor("cloud finished the carried work", async () => {
  const s = await req(CLOUD, "GET", `/internal/core/personas/${pid}/state`, cloudToken);
  const slow = await inputStatus(CLOUD, pid, cloudToken, "in-slow");
  const queued = await inputStatus(CLOUD, pid, cloudToken, "in-queued");
  const done =
    s.json?.queued_inputs === 0 &&
    s.json?.running_turn === null &&
    s.json?.pending_schedules === 0 &&
    slow?.input?.status === "done" &&
    queued?.input?.status === "done";
  return done ? true : { state: s.json, slow: slow?.input?.status, queued: queued?.input?.status };
}, 90_000);
check((await submit(CLOUD, pid, cloudToken, "in-after", "first message in the cloud")).status === 201, "cloud accepts new input after activation");
await waitFor("cloud answered the first new message", async () => {
  const a = await inputStatus(CLOUD, pid, cloudToken, "in-after");
  return a?.input?.status === "done" ? true : a;
}, 30_000);
cloudCore.proc.kill("SIGTERM");
await cloudCore.exited;

const cloudEvents = await allPages(CLOUD, `/internal/core/personas/${pid}/events`, cloudToken);
const cloudOutbox = await allPages(CLOUD, `/internal/core/personas/${pid}/outbox`, cloudToken);
check(localEvents.length === header.cut.latest_event_seq, "local journal ends exactly at the cut", { local: localEvents.length, cut: header.cut });
check(isDeepStrictEqual(cloudEvents.slice(0, localEvents.length), localEvents), "carried journal on cloud is identical to local, entry by entry");
check(isDeepStrictEqual(cloudOutbox.slice(0, localOutbox.length), localOutbox), "carried outbox on cloud is identical to local");
check(
  cloudEvents.length > localEvents.length && cloudEvents.every((e, i) => e.seq === i + 1),
  "cloud journal continues after the cut without gaps",
  { local: localEvents.length, cloud: cloudEvents.length },
);
const teaNotes = cloudEvents.filter((e) => e.kind === "note" && e.payload?.text === "The user takes tea at 15:00");
check(teaNotes.length === 1, "the memory note exists exactly once after the move", teaNotes);
const slow = await inputStatus(CLOUD, pid, cloudToken, "in-slow");
check(slow.turn?.attempt === 2 && slow.turn?.status === "done", "interrupted input completed on cloud as attempt 2", slow);
const wakes = cloudEvents.filter((e) => e.kind === "input_received" && e.payload?.kind === "wake");
check(wakes.length === 1 && wakes[0].seq > localEvents.length, "the carried reminder fired once, on cloud", wakes);
const after = await inputStatus(CLOUD, pid, cloudToken, "in-after");
check(after.turn?.status === "done", "new cloud input answered", after);
st = await req(LOCAL, "GET", `/internal/core/personas/${pid}/state`, localToken);
check(st.json.latest_event_seq === localEvents.length && st.json.persona.authority === "transferred", "local did not change after the move");
r = await req(LOCAL, "POST", `/internal/core/personas/${pid}/writer/acquire`, localToken, { holder_id: "local-core", ttl_ms: 4000 });
check(r.status === 409, "local still cannot run the moved secretary", r.text);

if (EVIDENCE) {
  mkdirSync(EVIDENCE, { recursive: true });
  writeFileSync(join(EVIDENCE, "bundle.ndjson"), bundle);
  writeFileSync(
    join(EVIDENCE, "summary.json"),
    JSON.stringify(
      {
        checks_passed: passed,
        persona_id: pid,
        transfer_id: TRANSFER,
        databases: [LOCAL_DB.replace(/\/\/[^@]*@/, "//***@"), CLOUD_DB.replace(/\/\/[^@]*@/, "//***@")],
        seal_receipt: seal.json,
        import_receipt: imported.json,
        local_events: localEvents.length,
        cloud_events: cloudEvents.length,
        local_core_exit: localExit,
        bundle_bytes: Buffer.byteLength(bundle),
        bundle_lines: lines.length,
      },
      null,
      2,
    ),
  );
  log("evidence written to", EVIDENCE);
}
log(`PASS: ${passed} checks. databases left for inspection: ${LOCAL_DB.slice(LOCAL_DB.lastIndexOf("/") + 1).split("?")[0]}, ${CLOUD_DB.slice(CLOUD_DB.lastIndexOf("/") + 1).split("?")[0]}`);
process.exit(0);
