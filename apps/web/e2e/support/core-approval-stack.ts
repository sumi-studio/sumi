import { type ChildProcess, spawn, spawnSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import { once } from "node:events";
import { createServer } from "node:net";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import type { BrowserContext } from "@playwright/test";
import type { WorkspaceBrowserBuild } from "./real-agent-stack";

/**
 * The mounted-browser approval journey's stack: the production Go server
 * (cmd/server) with its real /me/approvals surface, the Vite dev server, a
 * real signed browser session for a Human whose secretary is bound to a core
 * persona, and the real TypeScript core host (mock provider — directives in
 * the input text) that parks and resumes the same durable operation.
 *
 * Fixtures, all identified: the e2e-session-cookie issuer mints the Human +
 * secretary binding + signed cookie; the model is the scripted MockProvider;
 * SUMI_CORE_STATE_TOKEN gives the harness the same admin persona-management
 * surface an operator would use. Firebase is configured but never contacted —
 * no login exchange happens.
 */

const supportDirectory = dirname(fileURLToPath(import.meta.url));
const repositoryRoot = resolve(supportDirectory, "../../../..");
const apiDirectory = resolve(repositoryRoot, "apps/api");
const coreDirectory = resolve(repositoryRoot, "apps/core");
const webDirectory = resolve(repositoryRoot, "apps/web");

function uuidv7(): string {
  const now = Date.now().toString(16).padStart(12, "0");
  const r = crypto.randomUUID().replaceAll("-", "");
  return `${now.slice(0, 8)}-${now.slice(8, 12)}-7${r.slice(13, 16)}-${((parseInt(r.slice(16, 18), 16) & 0x3f) | 0x80).toString(16).padStart(2, "0")}${r.slice(18, 20)}-${r.slice(20, 32)}`;
}

async function ephemeralPort(): Promise<number> {
  const server = createServer();
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("no port");
  server.close();
  await once(server, "close");
  return address.port;
}

async function waitForHTTP(url: string, proc: ChildProcess, timeoutMs: number) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (proc.exitCode !== null) {
      throw new Error(`process exited ${proc.exitCode} before ${url} was ready`);
    }
    try {
      if ((await fetch(url)).ok) return;
    } catch {
      // not up yet
    }
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${url}`);
    await new Promise((r) => setTimeout(r, 250));
  }
}

export class CoreApprovalStack {
  private readonly children: ChildProcess[] = [];
  private stopped = false;

  private constructor(
    readonly apiURL: string,
    readonly webURL: string,
    readonly sessionCookie: string,
    readonly humanID: string,
    readonly personaID: string,
    private readonly personaToken: string,
    private readonly adminToken: string,
    private readonly runtimeDirectory: string,
    private readonly coreEnv: NodeJS.ProcessEnv,
  ) {}

  /** The e2e-session-cookie fixture's signed sumi_session value. */
  async installSession(context: BrowserContext): Promise<void> {
    await context.addCookies([
      {
        name: "sumi_session",
        value: this.sessionCookie,
        domain: "127.0.0.1",
        path: "/",
        httpOnly: true,
        secure: false,
        sameSite: "Lax",
      },
    ]);
  }

  /** Submit a secretary input exactly as the attention intake would. */
  async submitInput(text: string): Promise<string> {
    const inputID = `in-${crypto.randomUUID()}`;
    const response = await fetch(
      `${this.apiURL}/internal/core/personas/${this.personaID}/inputs`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${this.personaToken}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          input_id: inputID,
          kind: "message",
          payload: { text },
          actor_kind: "human",
          actor_id: this.humanID,
          source_surface: "e2e",
        }),
      },
    );
    if (response.status !== 201) {
      throw new Error(`submit input: ${response.status} ${await response.text()}`);
    }
    return inputID;
  }

  /** One real core pass: acquire, recover, drain, exit — the parked turn resumes. */
  runCoreOnce(label: string): void {
    const host = join(coreDirectory, "src/host/local.ts");
    const result = spawnSync(process.execPath, [host, "--once"], {
      env: this.coreEnv,
      cwd: coreDirectory,
      encoding: "utf8",
      timeout: 60_000,
    });
    if (result.status !== 0) {
      throw new Error(
        `core --once (${label}) exited ${result.status}\n${result.stdout}\n${result.stderr}`,
      );
    }
  }

  /** The session human's inbox, exactly as the browser fetches it. */
  async listApprovals(): Promise<Record<string, unknown>[]> {
    const response = await fetch(`${this.apiURL}/me/approvals`, {
      headers: { Cookie: `sumi_session=${this.sessionCookie}` },
    });
    if (!response.ok) throw new Error(`inbox: ${response.status}`);
    return ((await response.json()) as { approvals: never[] }).approvals;
  }

  /** The persona's durable outbox — where a granted send becomes visible. */
  async outbox(): Promise<{ kind: string; payload: Record<string, unknown> }[]> {
    const response = await fetch(
      `${this.apiURL}/internal/core/personas/${this.personaID}/outbox?after_seq=0`,
      { headers: { Authorization: `Bearer ${this.personaToken}` } },
    );
    if (!response.ok) throw new Error(`outbox: ${response.status}`);
    return ((await response.json()) as { outbox: never[] }).outbox;
  }

  async stop(): Promise<void> {
    if (this.stopped) return;
    this.stopped = true;
    for (const child of [...this.children].reverse()) {
      if (child.exitCode === null) child.kill("SIGKILL");
    }
    const { rm } = await import("node:fs/promises");
    await rm(this.runtimeDirectory, { recursive: true, force: true });
  }
}

export async function startCoreApprovalStack(
  build: WorkspaceBrowserBuild,
  databaseURL: string,
): Promise<CoreApprovalStack> {
  if (!databaseURL.trim()) {
    throw new Error("a disposable empty Postgres database URL is required");
  }
  const runtimeDirectory = mkdtempSync(
    join(tmpdir(), "sumi-approval-browser-runtime-"),
  );
  const [apiPort, webPort] = await Promise.all([ephemeralPort(), ephemeralPort()]);
  const apiURL = `http://127.0.0.1:${apiPort}`;
  const webURL = `http://127.0.0.1:${webPort}`;
  const sessionSecret = randomBytes(48).toString("base64");
  const sessionAudience = "sumi:web";
  const adminToken = `e2e-admin-${crypto.randomUUID().replaceAll("-", "")}`;
  const humanID = uuidv7();
  const personaID = uuidv7();
  const children: ChildProcess[] = [];

  try {
    const api = spawn(build.apiServer, [], {
      cwd: apiDirectory,
      env: {
        ...process.env,
        PORT: String(apiPort),
        SUMI_PUBLIC_LOOPBACK_LISTEN: `127.0.0.1:${apiPort}`,
        SUMI_DB_URL: databaseURL,
        SUMI_AGENT_RUNTIME_STATE_DIR: join(runtimeDirectory, "gateway"),
        SUMI_COMMAND_LOG_DIR: join(runtimeDirectory, "commands"),
        SUMI_BROWSER_SESSION_SECRET: sessionSecret,
        SUMI_BROWSER_SESSION_AUDIENCE: sessionAudience,
        SUMI_BROWSER_WS_ALLOWED_ORIGINS: webURL,
        SUMI_AUTH_ALLOW_INSECURE_COOKIES: "true",
        SUMI_AUTH_FIREBASE_PROJECT_ID: "sumi-studio",
        SUMI_AUTH_TENANT_ID: "e2e-approvals",
        FIREBASE_AUTH_EMULATOR_HOST: "127.0.0.1:9",
        SUMI_AGENT_WRAPPING_KEY_ID: `e2e-${crypto.randomUUID().slice(0, 8)}`,
        SUMI_CORE_STATE_TOKEN: adminToken,
      },
      stdio: ["ignore", "pipe", "pipe"],
    });
    api.stderr.on("data", (d) => process.stderr.write(`[approval-api] ${d}`));
    children.push(api);
    await waitForHTTP(`${apiURL}/health`, api, 30_000);

    const sessionCookie = spawnSync(build.sessionIssuer, [], {
      cwd: apiDirectory,
      env: {
        ...process.env,
        SUMI_BROWSER_SESSION_SECRET: sessionSecret,
        SUMI_BROWSER_SESSION_AUDIENCE: sessionAudience,
        SUMI_E2E_SESSION_TENANT_ID: "e2e-approvals",
        SUMI_E2E_SESSION_USER_ID: humanID,
        SUMI_E2E_SESSION_PERSONALITY_AGENT_ID: personaID,
        SUMI_E2E_SESSION_PROVISION_SECRETARY: "1",
        SUMI_E2E_SESSION_DATABASE_URL: databaseURL,
        SUMI_E2E_SESSION_DISPLAY_NAME: "Approval E2E Human",
      },
      encoding: "utf8",
    });
    if (sessionCookie.status !== 0) {
      throw new Error(`session issuer: ${sessionCookie.stderr}`);
    }
    const cookie = sessionCookie.stdout.trim();
    if (!cookie || cookie.length > 4096 || /\s/.test(cookie)) {
      throw new Error("session issuer returned an invalid cookie");
    }

    const persona = await fetch(`${apiURL}/internal/core/personas`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${adminToken}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        persona_id: personaID,
        human_id: humanID,
        display_name: "E2E Secretary",
      }),
    });
    if (persona.status !== 201) {
      throw new Error(`createPersona: ${persona.status} ${await persona.text()}`);
    }
    const personaToken = (
      (await persona.json()) as { persona_token: string }
    ).persona_token;

    const vite = spawn(
      process.execPath,
      [
        resolve(webDirectory, "node_modules/vite/bin/vite.js"),
        "--host", "127.0.0.1",
        "--port", String(webPort),
        "--strictPort",
      ],
      {
        cwd: webDirectory,
        env: { ...process.env, SUMI_DEV_API_ORIGIN: apiURL },
        stdio: ["ignore", "pipe", "pipe"],
      },
    );
    vite.stderr.on("data", (d) => process.stderr.write(`[approval-web] ${d}`));
    children.push(vite);
    await waitForHTTP(`${webURL}/`, vite, 30_000);

    const stack = new CoreApprovalStack(
      apiURL, webURL, cookie, humanID, personaID, personaToken, adminToken,
      runtimeDirectory,
      {
        ...process.env,
        SUMI_STATE_URL: apiURL,
        SUMI_PERSONA_ID: personaID,
        SUMI_PERSONA_TOKEN: personaToken,
        SUMI_MODEL_PROVIDER: "mock",
        SUMI_LEASE_TTL_MS: "3000",
      },
    );
    stack.children.push(...children);
    return stack;
  } catch (error) {
    for (const child of children.reverse()) {
      if (child.exitCode === null) child.kill("SIGKILL");
    }
    const { rm } = await import("node:fs/promises");
    await rm(runtimeDirectory, { recursive: true, force: true });
    throw error;
  }
}
