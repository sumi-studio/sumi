import { type ChildProcess, spawn, spawnSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import type { BrowserContext } from "@playwright/test";
import type { WorkspaceBrowserBuild } from "./workspace-stack";

/**
 * The Cloud browser journey's stack, on the product path:
 *
 *   person (Playwright Chrome) -> Vite web app (/browser) -> production Go
 *   server (cmd/server: /api/cloud-browser, browser tools, core state)
 *   -> browser Worker (ProfileBrowser DO) -> remote Chrome
 *   secretary (real Core Secretary, scripted provider) -> same API -> same DO
 *
 * Modes:
 *   local  wrangler dev with the test config and scripts/local-pool.mjs
 *          (headless Chrome standing in for Browser Run; NOT Cloud evidence)
 *   cloud  an already deployed isolated test Worker (Browser Run); the test
 *          API must be reachable from it (its SUMI_STATE_URL)
 *
 * Fixtures, all identified: e2e-session-cookie mints the Human + secretary
 * binding + signed cookie; SUMI_CORE_STATE_TOKEN creates the persona; the Jev
 * endpoint is the loopback contract double (local) unless a live key is used.
 * Ports stay inside the packet's owned range 19400-19439.
 */

const supportDirectory = dirname(fileURLToPath(import.meta.url));
const repositoryRoot = resolve(supportDirectory, "../../../..");
const apiDirectory = resolve(repositoryRoot, "apps/api");
const coreDirectory = resolve(repositoryRoot, "apps/core");
const webDirectory = resolve(repositoryRoot, "apps/web");
const browserCloudDirectory = resolve(repositoryRoot, "apps/browser-cloud");

export const PORTS = { pool: 19401, worker: 19402, inspector: 19403, api: 19410, web: 19411, jev: 19412 };

function uuidv7(): string {
  const now = Date.now().toString(16).padStart(12, "0");
  const r = crypto.randomUUID().replaceAll("-", "");
  return `${now.slice(0, 8)}-${now.slice(8, 12)}-7${r.slice(13, 16)}-${((Number.parseInt(r.slice(16, 18), 16) & 0x3f) | 0x80).toString(16).padStart(2, "0")}${r.slice(18, 20)}-${r.slice(20, 32)}`;
}

async function waitForHTTP(url: string, proc: ChildProcess | undefined, timeoutMs: number) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (proc && proc.exitCode !== null) throw new Error(`process exited ${proc.exitCode} before ${url} was ready`);
    try {
      if ((await fetch(url)).ok) return;
    } catch {
      // not up yet
    }
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${url}`);
    await new Promise((r) => setTimeout(r, 250));
  }
}

export interface StackOptions {
  mode: "local" | "cloud";
  databaseURL: string;
  artifacts: string;
  /** cloud: https origin of the deployed test Worker. */
  workerURL?: string;
  /** cloud: file holding the test runtime token (never printed). */
  tokenFile?: string;
  /** Jev endpoint the Worker uses (local: the loopback double). */
  jevEndpoint?: string;
}

type Pending = { resolve(value: unknown): void; reject(error: Error): void };

/** The harness side of apps/core/scripts/cloud-browser-journey-secretary.mjs. */
export class ScriptedSecretary {
  private seq = 0;
  private readonly pending = new Map<number, Pending>();
  private readonly child: ChildProcess;
  readonly ready: Promise<void>;

  constructor(env: NodeJS.ProcessEnv, log: (line: string) => void) {
    this.child = spawn(process.execPath, [join(coreDirectory, "scripts/cloud-browser-journey-secretary.mjs")], {
      cwd: coreDirectory,
      env,
      stdio: ["pipe", "pipe", "pipe"],
    });
    this.child.stderr?.on("data", (d) => log(`[secretary] ${d}`));
    let ready: () => void;
    this.ready = new Promise((r) => {
      ready = r;
    });
    createInterface({ input: this.child.stdout as NodeJS.ReadableStream }).on("line", (line) => {
      let message: { id?: number; ready?: boolean; result?: unknown; error?: string };
      try {
        message = JSON.parse(line);
      } catch {
        log(`[secretary] ${line}`);
        return;
      }
      if (message.ready) ready();
      if (message.id === undefined) return;
      const waiter = this.pending.get(message.id);
      this.pending.delete(message.id);
      if (message.error) waiter?.reject(new Error(message.error));
      else waiter?.resolve(message.result);
    });
  }

  /** One durable Core tool call; `wait` polls job.status until it ends. */
  call<T = Record<string, unknown>>(call: string, args: Record<string, unknown> = {}, wait = false, timeoutMs = 60_000): Promise<T> {
    const id = ++this.seq;
    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`secretary ${call} timed out`));
      }, timeoutMs + 30_000);
      this.pending.set(id, {
        resolve: (v) => {
          clearTimeout(timer);
          resolve(v as T);
        },
        reject: (e) => {
          clearTimeout(timer);
          reject(e);
        },
      });
      this.child.stdin?.write(`${JSON.stringify({ id, call, args, wait, timeoutMs })}\n`);
    });
  }

  stop(): void {
    this.child.stdin?.write(`${JSON.stringify({ stop: true })}\n`);
    setTimeout(() => {
      if (this.child.exitCode === null) this.child.kill("SIGKILL");
    }, 3_000).unref();
  }
}

export class CloudBrowserStack {
  readonly children: ChildProcess[] = [];
  worker?: ChildProcess;
  secretary?: ScriptedSecretary;
  private readonly log: string[] = [];

  constructor(
    readonly options: StackOptions,
    readonly apiURL: string,
    readonly webURL: string,
    readonly workerURL: string,
    readonly runtimeToken: string,
    readonly sessionCookie: string,
    readonly humanID: string,
    readonly personaID: string,
    readonly personaToken: string,
    readonly runtimeDirectory: string,
    private readonly workerArgs: string[],
  ) {}

  note(line: string): void {
    this.log.push(line);
    if (this.log.length > 4000) this.log.splice(0, 1000);
  }

  logs(): string {
    return this.log.join("");
  }

  async installSession(context: BrowserContext): Promise<void> {
    await context.addCookies([
      { name: "sumi_session", value: this.sessionCookie, domain: "127.0.0.1", path: "/", httpOnly: true, secure: false, sameSite: "Lax" },
    ]);
  }

  /** The fixture site's own server-side record (bearer-protected test route). */
  async fixtureReport(): Promise<{ orders: { token: string }[]; attempts: { token: string; accepted: number }[]; memos: string[] }> {
    const response = await fetch(`${this.workerURL}/__fixture/report`, { headers: { Authorization: `Bearer ${this.runtimeToken}` } });
    if (!response.ok) throw new Error(`fixture report ${response.status}`);
    return response.json() as never;
  }

  /** Local stand-in only: live remote browsers. */
  async poolSessions(): Promise<{ sessionId: string }[]> {
    return (await fetch(`http://127.0.0.1:${PORTS.pool}/sessions`)).json() as never;
  }

  /** Local stand-in only: kill every remote browser without telling its host. */
  async killBrowsers(): Promise<number> {
    let killed = 0;
    for (const s of await this.poolSessions()) {
      const r = (await (await fetch(`http://127.0.0.1:${PORTS.pool}/kill/${s.sessionId}`, { method: "POST" })).json()) as { killed: boolean };
      if (r.killed) killed++;
    }
    return killed;
  }

  startWorker(): void {
    const worker = spawn(join(browserCloudDirectory, "node_modules/.bin/wrangler"), this.workerArgs, {
      cwd: browserCloudDirectory,
      env: { ...process.env, WRANGLER_SEND_METRICS: "false", CI: "1" },
      stdio: ["ignore", "pipe", "pipe"],
    });
    worker.stdout?.on("data", (d) => this.note(`[worker] ${d}`));
    worker.stderr?.on("data", (d) => this.note(`[worker] ${d}`));
    this.worker = worker;
    this.children.push(worker);
  }

  /** Local: restart the Worker process (the Durable Object loses its memory;
   * the remote browser keeps running in the pool). */
  async restartWorker(): Promise<void> {
    const old = this.worker;
    if (old && old.exitCode === null) {
      old.kill("SIGTERM");
      await new Promise((r) => old.once("exit", r));
    }
    this.startWorker();
    await waitForHTTP(`${this.workerURL}/health`, this.worker, 60_000);
  }

  async profileState(): Promise<Record<string, unknown>> {
    const response = await fetch(`${this.apiURL}/api/cloud-browser`, { headers: { Cookie: `sumi_session=${this.sessionCookie}` } });
    return response.json() as never;
  }

  async stop(): Promise<void> {
    this.secretary?.stop();
    for (const child of [...this.children].reverse()) if (child.exitCode === null) child.kill("SIGTERM");
    await new Promise((r) => setTimeout(r, 1500));
    for (const child of this.children) if (child.exitCode === null) child.kill("SIGKILL");
    rmSync(this.runtimeDirectory, { recursive: true, force: true });
  }
}

export async function startCloudBrowserStack(build: WorkspaceBrowserBuild, options: StackOptions): Promise<CloudBrowserStack> {
  const runtimeDirectory = mkdtempSync(join(tmpdir(), "sumi-cloud-browser-20260927-run-"));
  const apiURL = `http://127.0.0.1:${PORTS.api}`;
  const webURL = `http://127.0.0.1:${PORTS.web}`;
  const workerURL = options.mode === "local" ? `http://127.0.0.1:${PORTS.worker}` : (options.workerURL ?? "");
  if (options.mode === "cloud" && !/^https:\/\/sumi-cloud-browser-test-20260927[a-z0-9.-]*\.workers\.dev$/.test(workerURL))
    throw new Error("cloud mode needs the isolated test Worker origin");
  const runtimeToken =
    options.mode === "cloud" ? readFileSync(options.tokenFile ?? "", "utf8").trim() : randomBytes(36).toString("base64url");
  const sessionSecret = randomBytes(48).toString("base64");
  const sessionAudience = "sumi:web";
  const adminToken = `e2e-admin-${crypto.randomUUID().replaceAll("-", "")}`;
  const humanID = uuidv7();
  const personaID = uuidv7();
  const children: ChildProcess[] = [];
  const log: string[] = [];
  // Until the stack exists, lines wait here; afterwards they go to the stack.
  let sink = (line: string) => void log.push(line);
  const note = (line: string) => sink(line);
  let stack: CloudBrowserStack | undefined;

  try {
    let workerArgs: string[] = [];
    if (options.mode === "local") {
      const pool = spawn(process.execPath, [join(browserCloudDirectory, "scripts/local-pool.mjs")], {
        env: { ...process.env, POOL_PORT: String(PORTS.pool), FIXTURE_ORIGIN: `127.0.0.1:${PORTS.worker}` },
        stdio: ["ignore", "pipe", "pipe"],
      });
      pool.stderr?.on("data", (d) => note(`[pool] ${d}`));
      children.push(pool);
      const envFile = join(runtimeDirectory, "worker.env");
      writeFileSync(
        envFile,
        [
          `SUMI_BROWSER_CLOUD_TOKEN=${runtimeToken}`,
          `LOCAL_POOL=http://127.0.0.1:${PORTS.pool}`,
          `SUMI_STATE_URL=${apiURL}`,
          `SUMI_BROWSER_IDLE_GRACE_MS=8000`,
          `SUMI_BROWSER_KEEPALIVE_MS=30000`,
          ...(options.jevEndpoint ? [`SUMI_JEV_ENDPOINT=${options.jevEndpoint}`] : []),
          "",
        ].join("\n"),
        { mode: 0o600 },
      );
      workerArgs = [
        "dev",
        "-c",
        "wrangler.test.jsonc",
        "--ip",
        "127.0.0.1",
        "--port",
        String(PORTS.worker),
        "--inspector-port",
        String(PORTS.inspector),
        "--persist-to",
        join(runtimeDirectory, "wrangler-state"),
        "--env-file",
        envFile,
        "--show-interactive-dev-session=false",
        "--log-level",
        "log",
      ];
    }

    const modelKey = randomBytes(32).toString("base64");
    const api = spawn(build.apiServer, [], {
      cwd: apiDirectory,
      env: {
        ...process.env,
        PORT: String(PORTS.api),
        SUMI_PUBLIC_LOOPBACK_LISTEN: `127.0.0.1:${PORTS.api}`,
        SUMI_DB_URL: options.databaseURL,
        SUMI_BROWSER_EVENT_DIR: join(runtimeDirectory, "gateway"),
        SUMI_COMMAND_LOG_DIR: join(runtimeDirectory, "commands"),
        SUMI_BROWSER_SESSION_SECRET: sessionSecret,
        SUMI_BROWSER_SESSION_AUDIENCE: sessionAudience,
        SUMI_BROWSER_WS_ALLOWED_ORIGINS: webURL,
        SUMI_AUTH_ALLOW_INSECURE_COOKIES: "true",
        SUMI_AUTH_FIREBASE_PROJECT_ID: "sumi-studio",
        SUMI_AUTH_TENANT_ID: "e2e-cloud-browser",
        FIREBASE_AUTH_EMULATOR_HOST: "127.0.0.1:9",
        SUMI_CORE_STATE_TOKEN: adminToken,
        SUMI_BROWSER_CLOUD_URL: workerURL,
        SUMI_BROWSER_CLOUD_TOKEN: runtimeToken,
        SUMI_MODEL_CONNECTION_KEY: modelKey,
      },
      stdio: ["ignore", "pipe", "pipe"],
    });
    api.stderr?.on("data", (d) => note(`[api] ${d}`));
    api.stdout?.on("data", (d) => note(`[api] ${d}`));
    children.push(api);
    await waitForHTTP(`${apiURL}/health`, api, 30_000);

    const issued = spawnSync(build.sessionIssuer, [], {
      cwd: apiDirectory,
      env: {
        ...process.env,
        SUMI_BROWSER_SESSION_SECRET: sessionSecret,
        SUMI_BROWSER_SESSION_AUDIENCE: sessionAudience,
        SUMI_E2E_SESSION_TENANT_ID: "e2e-cloud-browser",
        SUMI_E2E_SESSION_USER_ID: humanID,
        SUMI_E2E_SESSION_PERSONALITY_AGENT_ID: personaID,
        SUMI_E2E_SESSION_PROVISION_SECRETARY: "1",
        SUMI_E2E_SESSION_DATABASE_URL: options.databaseURL,
        SUMI_E2E_SESSION_DISPLAY_NAME: "Cloud Browser E2E Human",
      },
      encoding: "utf8",
    });
    if (issued.status !== 0) throw new Error(`session issuer: ${issued.stderr}`);
    const cookie = issued.stdout.trim();
    if (!cookie || cookie.length > 4096 || /\s/.test(cookie)) throw new Error("session issuer returned an invalid cookie");

    const persona = await fetch(`${apiURL}/internal/core/personas`, {
      method: "POST",
      headers: { Authorization: `Bearer ${adminToken}`, "Content-Type": "application/json" },
      body: JSON.stringify({ persona_id: personaID, human_id: humanID, display_name: "すみ（E2E 秘書）" }),
    });
    if (persona.status !== 201) throw new Error(`createPersona: ${persona.status} ${await persona.text()}`);
    const personaToken = ((await persona.json()) as { persona_token: string }).persona_token;

    const vite = spawn(
      process.execPath,
      [resolve(webDirectory, "node_modules/vite/bin/vite.js"), "--host", "127.0.0.1", "--port", String(PORTS.web), "--strictPort"],
      {
        cwd: webDirectory,
        env: { ...process.env, SUMI_DEV_API_ORIGIN: apiURL, SUMI_DEV_BROWSER_CLOUD_ORIGIN: workerURL },
        stdio: ["ignore", "pipe", "pipe"],
      },
    );
    vite.stderr?.on("data", (d) => note(`[web] ${d}`));
    children.push(vite);

    stack = new CloudBrowserStack(
      options,
      apiURL,
      webURL,
      workerURL,
      runtimeToken,
      cookie,
      humanID,
      personaID,
      personaToken,
      runtimeDirectory,
      workerArgs,
    );
    stack.children.push(...children);
    for (const line of log) stack.note(line);
    const created = stack;
    sink = (line) => created.note(line);
    if (options.mode === "local") {
      stack.startWorker();
      await waitForHTTP(`http://127.0.0.1:${PORTS.pool}/sessions`, children[0], 20_000);
    }
    await waitForHTTP(`${workerURL}/health`, stack.worker, 90_000);
    await waitForHTTP(`${webURL}/`, vite, 30_000);
    const current = stack;
    stack.secretary = new ScriptedSecretary(
      { ...process.env, CB_API_URL: apiURL, CB_PERSONA: personaID, CB_PERSONA_TOKEN: personaToken, CB_HUMAN: humanID },
      (line) => current.note(line),
    );
    await stack.secretary.ready;
    return stack;
  } catch (error) {
    if (stack) {
      log.push(stack.logs());
      await stack.stop();
    }
    else {
      for (const child of children.reverse()) if (child.exitCode === null) child.kill("SIGKILL");
      rmSync(runtimeDirectory, { recursive: true, force: true });
    }
    throw new Error(`${error instanceof Error ? error.message : error}\n${log.join("").slice(-4000)}`);
  }
}
