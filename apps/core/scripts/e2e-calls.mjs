#!/usr/bin/env node
/**
 * e2e-calls.mjs — real-stack call participation slice.
 *
 * Topology: real PostgreSQL + real api server (browser session lane AND core
 * internal lane on one binary) + real Node core with the scripted 'fixture'
 * model + call-runner (rtc-node participant) + real headless Chrome driving
 * the actual messaging UI, against a real self-hosted LiveKit.
 *
 * Postgres/LiveKit are external services provided via env; every Sumi process
 * is a child of this script.
 *
 *   SUMI_E2E_DB_URL            postgres://... (required)
 *   SUMI_LIVEKIT_URL           ws://host:port (RTC)
 *   SUMI_LIVEKIT_API_URL       http://host:port (RoomService/twirp)
 *   SUMI_LIVEKIT_API_KEY / SUMI_LIVEKIT_API_SECRET  (required)
 *   SUMI_E2E_API_PORT          api listen port (default 11575)
 *   SUMI_E2E_CHROME            chrome path (default /usr/bin/google-chrome)
 *   SUMI_E2E_EVIDENCE          artifact dir (default ./e2e-evidence)
 *   SUMI_E2E_SKIP_BROWSER=1    skip the Chrome leg
 *   SUMI_E2E_PLAYWRIGHT        require() specifier for @playwright/test
 *
 * Steps:
 *   A  browser human joins a DM call (real UI or the same REST route)
 *   B  call_started input -> fixture model -> call.join -> bridge claims,
 *      mints a ticket, joins LiveKit as personality_agent:<id>#e<epoch>
 *   C  browser fake-mic audio -> VAD -> fixture STT -> call_utterance inputs
 *   D  model call.say -> utterance -> TTS publish; the BROWSER measures
 *      nonzero samples on the remote <audio> element
 *   E  utterances mapped to silence produce no call.say
 *   F  SIGKILL the runner mid-call -> new claim supersedes (epoch+1),
 *      non-terminal utterances go 'unknown', nothing is replayed
 *   G  a hand-joined stale-epoch actor is removed by the observer
 *   H  the human removes the secretary -> session revoked, call ends
 */
import { spawn, execFileSync, spawnSync } from "node:child_process";
import { createRequire } from "node:module";
import { createHash, createHmac, randomBytes } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const APPS = path.resolve(import.meta.dirname, "../..");
const API_DIR = path.join(APPS, "api");
const CORE_DIR = path.join(APPS, "core");
const WEB_DIR = path.join(APPS, "web");

const DB_URL = req("SUMI_E2E_DB_URL");
const API_PORT = Number(process.env.SUMI_E2E_API_PORT ?? 11575);
const WEB_PORT = 5173; // vite dev port is fixed by vite.config.ts
const LK_URL = req("SUMI_LIVEKIT_URL");
const LK_API = req("SUMI_LIVEKIT_API_URL");
const LK_KEY = req("SUMI_LIVEKIT_API_KEY");
const LK_SECRET = req("SUMI_LIVEKIT_API_SECRET");
const CHROME = process.env.SUMI_E2E_CHROME ?? "/usr/bin/google-chrome";
const SKIP_BROWSER = process.env.SUMI_E2E_SKIP_BROWSER === "1";
const EVIDENCE =
  process.env.SUMI_E2E_EVIDENCE ?? path.join(CORE_DIR, "e2e-evidence");
const API_BASE = `http://127.0.0.1:${API_PORT}`;
const WEB_BASE = `http://127.0.0.1:${WEB_PORT}`;
const BROWSER_SECRET_B64 = Buffer.from(
  "e2e-calls-browser-session-secret-32b",
).toString("base64");
const CORE_TOKEN = "e2e-core-state-token-0123456789abcdef";
const FB_EMULATOR_HOST = process.env.FIREBASE_AUTH_EMULATOR_HOST ?? "127.0.0.1:11577";

const results = [];
const procs = [];
let failed = false;

function req(name) {
  const v = process.env[name];
  if (!v) {
    console.error(`missing required env ${name}`);
    process.exit(2);
  }
  return v;
}
function note(name, ok, detail) {
  results.push({ name, ok: !!ok, detail });
  console.log(
    `${ok ? "PASS" : "FAIL"}  ${name}${detail ? ` — ${detail}` : ""}`,
  );
  if (!ok) failed = true;
}
/**
 * Synthetic microphone fixture for headless Chrome's
 * --use-file-for-fake-audio-capture: a PCM16 mono WAV whose loop contains
 * short speech-like bursts separated by long silences, so the
 * bridge VAD sees discrete utterances instead of the default continuous
 * test tone (which would make every emission an instant barge-in).
 */
function writeFakeMicWav(file) {
  const rate = 48000;
  const seconds = 60;
  const burstSeconds = 1.6;
  const total = rate * seconds;
  const data = Buffer.alloc(total * 2);
  // Speech-like bursts every 15s starting at 6s, so the bridge hears
  // discrete utterances whenever the secretary joins (the file loops).
  const bursts = [6, 21, 36, 51].map((s) => [
    Math.floor(s * rate),
    Math.floor((s + burstSeconds) * rate),
  ]);
  for (let i = 0; i < total; i++) {
    let v = 0;
    if (bursts.some(([a, b]) => i >= a && i < b)) {
      const t = i / rate;
      // Syllable-like amplitude modulation over two tones.
      const env = 0.5 + 0.5 * Math.sin(2 * Math.PI * 6 * t);
      v = Math.round(
        14000 * env * (Math.sin(2 * Math.PI * 350 * t) + 0.6 * Math.sin(2 * Math.PI * 900 * t)) / 1.6,
      );
    }
    data.writeInt16LE(v, i * 2);
  }
  const header = Buffer.alloc(44);
  header.write("RIFF", 0);
  header.writeUInt32LE(36 + data.length, 4);
  header.write("WAVE", 8);
  header.write("fmt ", 12);
  header.writeUInt32LE(16, 16);
  header.writeUInt16LE(1, 20); // PCM
  header.writeUInt16LE(1, 22); // mono
  header.writeUInt32LE(rate, 24);
  header.writeUInt32LE(rate * 2, 28); // byte rate
  header.writeUInt16LE(2, 32); // block align
  header.writeUInt16LE(16, 34); // bits
  header.write("data", 36);
  header.writeUInt32LE(data.length, 40);
  fs.writeFileSync(file, Buffer.concat([header, data]));
}
function launch(name, cmd, args, opts = {}) {
  fs.mkdirSync(EVIDENCE, { recursive: true });
  const fd = fs.openSync(path.join(EVIDENCE, `${name}.log`), "a");
  const p = spawn(cmd, args, { ...opts, stdio: ["ignore", fd, fd] });
  procs.push({ name, p });
  p.on("exit", (code) => console.log(`[exit] ${name} code=${code}`));
  return p;
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function waitHTTP(url, ms) {
  const deadline = Date.now() + ms;
  let last = "no attempt";
  while (Date.now() < deadline) {
    try {
      const r = await fetch(url);
      if (r.status < 500) return r;
      last = `status ${r.status}`;
    } catch (e) {
      last = e.message;
    }
    await sleep(300);
  }
  throw new Error(`timeout waiting for ${url} (${last})`);
}

async function j(method, url, body, headers = {}) {
  const r = await fetch(url, {
    method,
    headers: { "Content-Type": "application/json", ...headers },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await r.text();
  let json;
  try {
    json = JSON.parse(text);
  } catch {
    json = { raw: text };
  }
  return { status: r.status, json };
}

function uuidv7() {
  const b = randomBytes(16);
  const ms = BigInt(Date.now());
  b[0] = Number((ms >> 40n) & 0xffn);
  b[1] = Number((ms >> 32n) & 0xffn);
  b[2] = Number((ms >> 24n) & 0xffn);
  b[3] = Number((ms >> 16n) & 0xffn);
  b[4] = Number((ms >> 8n) & 0xffn);
  b[5] = Number(ms & 0xffn);
  b[6] = (b[6] & 0x0f) | 0x70;
  b[8] = (b[8] & 0x3f) | 0x80;
  const h = [...b].map((x) => x.toString(16).padStart(2, "0")).join("");
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}

function mintJoinToken(identity, room, ttlSec = 300) {
  const b64 = (o) => Buffer.from(JSON.stringify(o)).toString("base64url");
  const header = b64({ alg: "HS256", typ: "JWT" });
  const now = Math.floor(Date.now() / 1000);
  const payload = b64({
    iss: LK_KEY,
    sub: identity,
    name: identity,
    exp: now + ttlSec,
    nbf: now - 10,
    video: {
      roomJoin: true,
      room,
      canPublish: true,
      canSubscribe: true,
      canPublishData: true,
    },
  });
  const sig = createHmac("sha256", LK_SECRET)
    .update(`${header}.${payload}`)
    .digest("base64url");
  return `${header}.${payload}.${sig}`;
}

function adminToken(grant) {
  const b64 = (o) => Buffer.from(JSON.stringify(o)).toString("base64url");
  const now = Math.floor(Date.now() / 1000);
  const h = b64({ alg: "HS256", typ: "JWT" });
  const p = b64({ iss: LK_KEY, exp: now + 60, nbf: now - 10, video: grant });
  const sig = createHmac("sha256", LK_SECRET).update(`${h}.${p}`).digest("base64url");
  return `${h}.${p}.${sig}`;
}

async function roomService(method, body) {
  // Grant must match the method: ListRooms needs roomList, the rest need
  // roomAdmin bound to the room name.
  const grant =
    method === "ListRooms"
      ? { roomList: true }
      : { roomAdmin: true, room: ROOM_NAME };
  const r = await fetch(`${LK_API}/twirp/livekit.RoomService/${method}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${adminToken(grant)}`,
    },
    body: JSON.stringify(body ?? {}),
  });
  return { status: r.status, json: await r.json().catch(() => ({})) };
}

const scoped = (p) =>
  `${p}?workspace_id=${WORKSPACE_ID}&installation_id=${INSTALLATION_ID}&authority_epoch=${EPOCH}`;
const cookieH = () => ({
  Cookie: `sumi_session=${SESSION}`,
  Origin: WEB_BASE,
});
const coreAPI = (method, p, body, headers = {}) =>
  j(
    method,
    `${API_BASE}/internal/core/personas/${encodeURIComponent(PERSONA_ID)}${p}`,
    body,
    { Authorization: `Bearer ${PERSONA_TOKEN}`, ...headers },
  );

let PERSONA_ID = "";
let PERSONA_TOKEN = "";
let SESSION = "";
let WORKSPACE_ID = "";
let INSTALLATION_ID = "";
let EPOCH = "";
let DM_ID = "";
let ROOM_NAME = "";
let ROOM_SID = "";
let browser;

async function main() {
  fs.mkdirSync(EVIDENCE, { recursive: true });
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "sumi-e2e-calls-"));
  const fakeMicWav = path.join(tmp, "fake-mic.wav");
  writeFakeMicWav(fakeMicWav);

  // --------------------------------------------------------- fixtures
  const fixturePath = path.join(tmp, "fixture-script.json");
  fs.writeFileSync(
    fixturePath,
    JSON.stringify({
      rules: [
        {
          when: { kind: "call_started" },
          do: [{ tool: "call.join", args: { place_id: "$place_id" } }],
        },
        {
          when: { kind: "call_utterance", text_regex: "check-one-marker" },
          do: [
            {
              tool: "call.say",
              args: {
                session_id: "$session_id",
                text: "fixture reply: I hear you, human",
              },
            },
          ],
        },
        // Default posture: listen, stay silent (no leave rule — the human's
        // removal drives the exit in step H).
        { when: { kind: "call_utterance" }, do: [] },
      ],
    }),
  );
  const sttPath = path.join(tmp, "stt-script.json");
  fs.writeFileSync(
    sttPath,
    JSON.stringify({
      default: [
        "sumi check-one-marker please",
        "just ambient noise here",
        "more ambient noise",
        "yet more ambient",
        "background continues",
        "still background",
        "hum hum hum",
        "la la la",
      ],
    }),
  );

  // --------------------------------------------------------- build + api
  console.log("== build api server + session issuer");
  execFileSync("go", ["build", "-o", path.join(tmp, "api"), "./cmd/server"], {
    cwd: API_DIR,
    stdio: "inherit",
  });
  execFileSync(
    "go",
    ["build", "-o", path.join(tmp, "session-issuer"), "./cmd/e2e-session-cookie"],
    { cwd: API_DIR, stdio: "inherit" },
  );
  execFileSync(
    "go",
    ["build", "-o", path.join(tmp, "seed-member"), "./cmd/e2e-seed-member"],
    { cwd: API_DIR, stdio: "inherit" },
  );

  PERSONA_ID = uuidv7();
  const cmdLogDir = path.join(tmp, "cmd-log");
  const runtimeDir = path.join(tmp, "agent-runtime");
  fs.mkdirSync(cmdLogDir, { recursive: true });
  fs.mkdirSync(runtimeDir, { recursive: true, mode: 0o700 });
  launch("api", path.join(tmp, "api"), [], {
    env: {
      ...process.env,
      SUMI_COMMAND_LOG_DIR: cmdLogDir,
      SUMI_AGENT_RUNTIME_STATE_DIR: runtimeDir,
      SUMI_DB_URL: DB_URL,
      PORT: String(API_PORT),
      SUMI_PUBLIC_LOOPBACK_LISTEN: `127.0.0.1:${API_PORT}`,
      ENV: "development",
      SUMI_BROWSER_SESSION_SECRET: BROWSER_SECRET_B64,
      SUMI_BROWSER_SESSION_AUDIENCE: "sumi-e2e",
      SUMI_BROWSER_WS_ALLOWED_ORIGINS: WEB_BASE,
      FIREBASE_AUTH_EMULATOR_HOST: FB_EMULATOR_HOST,
      SUMI_AUTH_FIREBASE_PROJECT_ID: "sumi-e2e-calls",
      SUMI_AUTH_TENANT_ID: "e2e-calls-tenant",
      SUMI_AUTH_ALLOW_INSECURE_COOKIES: "true",
      SUMI_AGENT_WRAPPING_KEY_ID: "e2e-calls-wrapping",
      SUMI_CORE_STATE_TOKEN: CORE_TOKEN,
      SUMI_LIVEKIT_URL: LK_URL,
      SUMI_LIVEKIT_API_URL: LK_API,
      SUMI_LIVEKIT_API_KEY: LK_KEY,
      SUMI_LIVEKIT_API_SECRET: LK_SECRET,
    },
  });
  await waitHTTP(`${API_BASE}/health`, 30_000);
  note("api.up", true);

  // --------------------------------------------------------- provision
  const issuerEnv = {
    ...process.env,
    SUMI_BROWSER_SESSION_SECRET: BROWSER_SECRET_B64,
    SUMI_BROWSER_SESSION_AUDIENCE: "sumi-e2e",
    SUMI_E2E_SESSION_DATABASE_URL: DB_URL,
    SUMI_E2E_SESSION_PROVISION_SECRETARY: "1",
    SUMI_E2E_SESSION_TENANT_ID: "e2e-calls-tenant",
    SUMI_E2E_SESSION_USER_ID: uuidv7(),
    SUMI_E2E_SESSION_PERSONALITY_AGENT_ID: PERSONA_ID,
    SUMI_E2E_SESSION_DISPLAY_NAME: "E2E Human",
  };
  const issued = spawnSync(path.join(tmp, "session-issuer"), [], {
    env: issuerEnv,
    encoding: "utf8",
  });
  if (issued.status !== 0) {
    console.log(issued.stdout, issued.stderr);
    throw new Error("session issuer failed");
  }
  SESSION = issued.stdout.trim();
  note("provision.session", true, `pa=${PERSONA_ID.slice(0, 8)}`);

  const mkPersona = await j(
    "POST",
    `${API_BASE}/internal/core/personas`,
    { persona_id: PERSONA_ID, display_name: "E2E Secretary" },
    { Authorization: `Bearer ${CORE_TOKEN}` },
  );
  if (mkPersona.status >= 300)
    throw new Error(`persona create: ${mkPersona.status} ${JSON.stringify(mkPersona.json)}`);
  PERSONA_TOKEN = mkPersona.json.persona_token;
  note("provision.persona", true);

  const ws = await j("POST", `${API_BASE}/workspaces`, { name: "call-e2e" }, cookieH());
  if (ws.status >= 300)
    throw new Error(`workspace: ${ws.status} ${JSON.stringify(ws.json)}`);
  WORKSPACE_ID = ws.json.workspace_id;

  const inst = await j(
    "POST",
    `${API_BASE}/app-installations`,
    {
      owner: { kind: "workspace", workspace_id: WORKSPACE_ID },
      app_id: "messaging",
      operation_id: crypto.randomUUID(),
    },
    cookieH(),
  );
  if (inst.status >= 300)
    throw new Error(`install: ${inst.status} ${JSON.stringify(inst.json)}`);
  INSTALLATION_ID = inst.json.installation_id;
  EPOCH = inst.json.authority_epoch;

  // The secretary's workspace membership is seeded by the labeled fixture
  // binary — the product path is the targeted invite accepted over the
  // legacy local-control lane, which this core-only stack does not run.
  const seed = spawnSync(path.join(tmp, "seed-member"), [], {
    env: {
      ...process.env,
      SUMI_E2E_SEED_DATABASE_URL: DB_URL,
      SUMI_E2E_SEED_WORKSPACE_ID: WORKSPACE_ID,
      SUMI_E2E_SEED_MEMBER_KIND: "personality_agent",
      SUMI_E2E_SEED_MEMBER_ID: PERSONA_ID,
    },
    encoding: "utf8",
  });
  if (seed.status !== 0)
    throw new Error(`seed member: ${seed.stderr || seed.stdout}`);
  note("provision.pa_member", true);

  const dm = await j(
    "POST",
    `${API_BASE}${scoped("/messaging/dms")}`,
    { participant: { kind: "personality_agent", personality_agent_id: PERSONA_ID } },
    cookieH(),
  );
  if (dm.status >= 300)
    throw new Error(`dm: ${dm.status} ${JSON.stringify(dm.json)}`);
  DM_ID = dm.json.dm_id;
  ROOM_NAME = DM_ID; // LiveKit room name is the raw place id
  note("provision.messaging", true, `dm=${DM_ID.slice(0, 8)}`);

  // --------------------------------------------------------- core + runner
  const coreEnv = {
    ...process.env,
    SUMI_STATE_URL: API_BASE,
    SUMI_PERSONA_ID: PERSONA_ID,
    SUMI_PERSONA_TOKEN: PERSONA_TOKEN,
    SUMI_WORKSPACE_ROOT: path.join(tmp, "persona-root"),
    SUMI_MODEL_PROVIDER: "fixture",
    SUMI_MODEL_FIXTURE: fixturePath,
  };
  launch("core", "node", ["--experimental-strip-types", "src/host/local.ts"], {
    cwd: CORE_DIR,
    env: coreEnv,
  });

  const runnerEnv = {
    ...coreEnv,
    SUMI_CALL_RUNNER_ID: "e2e-runner-a",
    SUMI_CALL_POLL_MS: "800",
    SUMI_CALL_LEASE_MS: "15000",
    SUMI_CALL_STT_FIXTURE: sttPath,
    SUMI_CALL_VAD_MAX_MS: "4000",
    SUMI_CALL_VAD_HANGOVER_MS: "700",
  };
  let runner = launch("runner-1", "node", [
    "--experimental-strip-types",
    "src/host/call-runner.ts",
  ], { cwd: CORE_DIR, env: runnerEnv });

  // --------------------------------------------------------- web + browser
  launch(
    "web",
    "node",
    [
      path.join(WEB_DIR, "node_modules/vite/bin/vite.js"),
      "--port",
      String(WEB_PORT),
      "--strictPort",
    ],
    {
      cwd: WEB_DIR,
      env: {
        ...process.env,
        SUMI_DEV_API_ORIGIN: API_BASE,
        VITE_FIREBASE_API_KEY: "e2e-fake-api-key",
        VITE_FIREBASE_AUTH_DOMAIN: "sumi-e2e-calls.firebaseapp.com",
        VITE_FIREBASE_PROJECT_ID: "sumi-e2e-calls",
        VITE_FIREBASE_APP_ID: "1:0:web:e2e",
        VITE_FIREBASE_AUTH_EMULATOR_URL: `http://${FB_EMULATOR_HOST}`,
      },
    },
  );
  await waitHTTP(`${WEB_BASE}/`, 60_000);
  note("web.up", true);

  let page;
  if (!SKIP_BROWSER) {
    const webRequire = createRequire(path.join(WEB_DIR, "package.json"));
    const { chromium } = webRequire(
      process.env.SUMI_E2E_PLAYWRIGHT ?? "@playwright/test",
    );
    browser = await chromium.launch({
      executablePath: CHROME,
      headless: true,
      args: [
        "--use-fake-ui-for-media-stream",
        "--use-fake-device-for-media-stream",
        `--use-file-for-fake-audio-capture=${fakeMicWav}`,
        "--autoplay-policy=no-user-gesture-required",
        "--no-sandbox",
      ],
    });
    const context = await browser.newContext({
      permissions: ["microphone"],
    });
    await context.addCookies([
      {
        name: "sumi_session",
        value: SESSION,
        url: WEB_BASE,
      },
    ]);
    page = await context.newPage();
    page.on("console", (m) => {
      if (m.type() === "error") console.log("[page-err]", m.text());
    });
  }

  // --------------------------------------------------------- A: call starts
  let callStarted = false;
  if (page) {
    try {
      await page.goto(`${WEB_BASE}/w/${WORKSPACE_ID}/messaging`, {
        waitUntil: "domcontentloaded",
        timeout: 30_000,
      });
      await page.waitForSelector("text=Sumi", { timeout: 30_000 });
      await page.getByText("Sumi").first().click();
      const btn = page.getByRole("button", {
        name: /通話を開始|通話に参加/,
      });
      await btn.waitFor({ timeout: 20_000 });
      await btn.click();
      callStarted = true;
    } catch (e) {
      console.log("[browser] UI call start failed, REST fallback:", e.message);
      await page
        .screenshot({ path: path.join(EVIDENCE, "call-start-fail.png") })
        .catch(() => {});
    }
  }
  if (!callStarted) {
    const r = await j(
      "POST",
      `${API_BASE}${scoped(`/messaging/places/${DM_ID}/call/token`)}`,
      {},
      cookieH(),
    );
    if (r.status >= 300)
      throw new Error(`call token: ${r.status} ${JSON.stringify(r.json)}`);
    // The REST path mints a ticket but does NOT join — join with rtc-node so
    // the room opens and the webhook fires.
    const { Room } = await import("@livekit/rtc-node");
    const humanRoom = new Room();
    await humanRoom.connect(r.json.url, r.json.token, {
      autoSubscribe: true,
      dynacast: false,
    });
    // Keep the human in the room for the whole test.
    procs.push({ name: "human-room", p: { kill: () => humanRoom.disconnect() } });
  }
  note("A.call_started", true, `via ${callStarted ? "browser-ui" : "rest+rtc"}`);

  // Continuous remote-audio monitor: the secretary's track exists only
  // while an utterance is being emitted, so sample every <audio> element
  // for the life of the call instead of measuring after the fact.
  if (page) {
    await page.evaluate(() => {
      window.__sumiMaxPeak = 0;
      window.__sumiSawAudioEl = false;
      const ctx = new AudioContext();
      const hooked = new WeakSet();
      setInterval(() => {
        for (const el of document.querySelectorAll("audio")) {
          if (
            !el.srcObject ||
            !el.srcObject.getAudioTracks().length ||
            hooked.has(el)
          ) {
            continue;
          }
          hooked.add(el);
          window.__sumiSawAudioEl = true;
          const an = ctx.createAnalyser();
          an.fftSize = 2048;
          ctx.createMediaStreamSource(el.srcObject).connect(an);
          const buf = new Float32Array(an.fftSize);
          const tick = () => {
            if (!el.srcObject) return;
            an.getFloatTimeDomainData(buf);
            for (const v of buf) {
              window.__sumiMaxPeak = Math.max(
                window.__sumiMaxPeak,
                Math.abs(v),
              );
            }
            setTimeout(tick, 40);
          };
          tick();
        }
      }, 150);
    });
  }

  {
    const deadline = Date.now() + 30_000;
    while (Date.now() < deadline) {
      const r = await roomService("ListRooms", {});
      const hit = (r.json.rooms ?? []).find((x) => x.name === ROOM_NAME);
      if (hit) {
        ROOM_SID = hit.sid;
        break;
      }
      await sleep(500);
    }
    note("A.room_open", !!ROOM_SID, ROOM_SID ?? "no room");
  }

  // --------------------------------------------------------- B: join
  let secEpoch = 0;
  {
    const deadline = Date.now() + 90_000;
    while (Date.now() < deadline) {
      const r = await roomService("ListParticipants", { room: ROOM_NAME });
      const me = (r.json.participants ?? []).find((p) =>
        p.identity.startsWith(`personality_agent:${PERSONA_ID}#e`),
      );
      if (me) {
        secEpoch = Number(me.identity.split("#e")[1]);
        break;
      }
      await sleep(700);
    }
    note(
      "B.secretary_joined",
      secEpoch > 0,
      secEpoch ? `identity #e${secEpoch}` : "not in room",
    );
  }
  if (page)
    await page
      .screenshot({ path: path.join(EVIDENCE, "call-active.png") })
      .catch(() => {});

  // --------------------------------------------------------- C/D/E
  const events = async () => {
    const r = await coreAPI("GET", `/events?limit=300`);
    return r.json.events ?? [];
  };
  let sawUtterance = false;
  let sawSay = false;
  let emitted = 0;
  {
    const deadline = Date.now() + 120_000;
    while (Date.now() < deadline && !(sawUtterance && emitted > 0)) {
      const ev = await events();
      sawUtterance ||= ev.some(
        (e) =>
          e.kind === "input_received" && e.payload?.kind === "call_utterance",
      );
      sawSay ||= ev.some(
        (e) => e.kind === "tool_call" && e.payload?.tool === "call.say",
      );
      const logFile = path.join(EVIDENCE, "runner-1.log");
      if (fs.existsSync(logFile)) {
        const log = fs.readFileSync(logFile, "utf8");
        emitted = (log.match(/"status":"emitted"/g) ?? []).length;
      }
      await sleep(1000);
    }
    note("C.utterance_input", sawUtterance);
    note("D.call_say_intent", sawSay);
    note("D.emitted", emitted > 0, `emitted×${emitted}`);
  }

  if (page) {
    const aud = await page.evaluate(() => ({
      ok: (window.__sumiMaxPeak ?? 0) > 0.005,
      peak: window.__sumiMaxPeak ?? 0,
      sawElement: window.__sumiSawAudioEl ?? false,
    }));
    note("D.audible_in_browser", aud.ok, JSON.stringify(aud));
  }

  // --------------------------------------------------------- E: ambient
  // Wait for a second (ambient) segment, then assert every call.say in
  // the journal traces to the marker utterance — ambient speech must
  // produce cognition without producing speech.
  {
    const logFile = path.join(EVIDENCE, "runner-1.log");
    const segments = () => {
      if (!fs.existsSync(logFile)) return [];
      return [...fs.readFileSync(logFile, "utf8").matchAll(
        /speech segment transcribed .*"text":"([^"]*)"/g,
      )].map((m) => m[1]);
    };
    const deadline = Date.now() + 30_000;
    while (Date.now() < deadline && segments().length < 2) {
      await sleep(1000);
    }
    const seg = segments();
    const ambient = seg.filter((t) => !/check-one-marker/.test(t));
    const ev = await events();
    const sayCount = ev.filter(
      (e) => e.kind === "tool_call" && e.payload?.tool === "call.say",
    ).length;
    note(
      "E.no_ambient_say",
      ambient.length > 0 && sayCount === seg.length - ambient.length,
      `segments=${seg.length} ambient=${ambient.length} says=${sayCount}`,
    );
  }

  // --------------------------------------------------------- F: kill+restart
  const preKillEpoch = secEpoch;
  runner.kill("SIGKILL");
  await sleep(500);
  runner = launch("runner-2", "node", [
    "--experimental-strip-types",
    "src/host/call-runner.ts",
  ], {
    cwd: CORE_DIR,
    env: { ...runnerEnv, SUMI_CALL_RUNNER_ID: "e2e-runner-b" },
  });

  {
    const deadline = Date.now() + 90_000;
    let newEpoch = 0;
    while (Date.now() < deadline) {
      const r = await roomService("ListParticipants", { room: ROOM_NAME });
      const me = (r.json.participants ?? []).find((p) =>
        p.identity.startsWith(`personality_agent:${PERSONA_ID}#e`),
      );
      if (me) {
        const e = Number(me.identity.split("#e")[1]);
        if (e > preKillEpoch) {
          newEpoch = e;
          break;
        }
      }
      await sleep(800);
    }
    note(
      "F.reclaimed_new_epoch",
      newEpoch > 0,
      newEpoch ? `#e${preKillEpoch} -> #e${newEpoch}` : "no rejoin",
    );
    // Nothing may be replayed: runner-2 must not emit an utterance id that
    // runner-1 already dequeued/emitted. A NEW call.say after reclaim is
    // legitimate and its new id is fine.
    const emittedIds = (file) => {
      const p = path.join(EVIDENCE, file);
      if (!fs.existsSync(p)) return new Set();
      const log = fs.readFileSync(p, "utf8");
      const ids = new Set();
      for (const m of log.matchAll(
        /"utterance_id":"([^"]+)","status":"(dequeued|emitting|emitted)"/g,
      )) {
        ids.add(m[1]);
      }
      return ids;
    };
    const r1ids = emittedIds("runner-1.log");
    const r2ids = emittedIds("runner-2.log");
    const replayed = [...r2ids].filter((id) => r1ids.has(id));
    note(
      "F.no_stale_replay",
      replayed.length === 0,
      replayed.length ? `replayed: ${replayed.join(",")}` : "no old utterance re-emitted",
    );
  }

  // --------------------------------------------------------- G: stale actor
  {
    const stale = `personality_agent:${PERSONA_ID}#e1`;
    const { Room } = await import("@livekit/rtc-node");
    const room = new Room();
    let joinOk = true;
    try {
      await room.connect(LK_URL, mintJoinToken(stale, ROOM_NAME), {
        autoSubscribe: false,
        dynacast: false,
      });
    } catch (e) {
      joinOk = false;
      console.log("[probe] stale join refused:", e.message);
    }
    let removed = false;
    let tRemove = -1;
    const t0 = Date.now();
    if (joinOk) {
      const deadline = Date.now() + 30_000;
      while (Date.now() < deadline) {
        const r = await roomService("ListParticipants", { room: ROOM_NAME });
        const still = (r.json.participants ?? []).some(
          (p) => p.identity === stale,
        );
        if (!still) {
          removed = true;
          tRemove = Date.now() - t0;
          break;
        }
        await sleep(500);
      }
    }
    note(
      "G.stale_actor_removed",
      removed || !joinOk,
      joinOk ? `removed after ${tRemove}ms` : "join refused",
    );
    try {
      await room.disconnect();
    } catch {}
  }

  // --------------------------------------------------------- H: removal
  {
    const r = await j(
      "POST",
      `${API_BASE}${scoped(`/messaging/places/${DM_ID}/call/participants/remove`)}`,
      {
        participant: {
          kind: "personality_agent",
          id: PERSONA_ID,
        },
      },
      cookieH(),
    );
    let gone = false;
    const deadline = Date.now() + 15_000;
    while (Date.now() < deadline) {
      const parts =
        (await roomService("ListParticipants", { room: ROOM_NAME })).json
          .participants ?? [];
      if (
        !parts.some((p) =>
          p.identity.startsWith(`personality_agent:${PERSONA_ID}`),
        )
      ) {
        gone = true;
        break;
      }
      await sleep(500);
    }
    note("H.human_removal", r.status < 300 && gone, `route=${r.status}`);
  }

  // --------------------------------------------------------- journal
  const ev = await events();
  fs.writeFileSync(
    path.join(EVIDENCE, "journal-events.json"),
    JSON.stringify(ev, null, 2),
  );
  const kinds = [
    ...new Set(
      ev
        .filter((e) => e.kind === "input_received")
        .map((e) => e.payload?.kind)
        .filter(Boolean),
    ),
  ];
  note(
    "journal.call_inputs",
    kinds.includes("call_started") || kinds.includes("call_utterance"),
    kinds.join(","),
  );
  if (page)
    await page
      .screenshot({ path: path.join(EVIDENCE, "final.png") })
      .catch(() => {});
}

main()
  .catch((e) => {
    failed = true;
    console.error("FATAL", e);
  })
  .finally(async () => {
    if (browser) await browser.close().catch(() => {});
    for (const { p } of procs) {
      try {
        p.kill("SIGTERM");
      } catch {}
    }
    await sleep(800);
    for (const { p } of procs) {
      try {
        p.kill("SIGKILL");
      } catch {}
    }
    console.log("\n=== VERDICT ===");
    for (const r of results)
      console.log(`${r.ok ? "PASS" : "FAIL"} ${r.name} ${r.detail ?? ""}`);
    process.exit(failed ? 1 : 0);
  });
