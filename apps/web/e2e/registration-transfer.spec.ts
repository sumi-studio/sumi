import { type ChildProcess, spawn, spawnSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import {
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  rmSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, type Page, test } from "@playwright/test";

/**
 * The registration secretary-move journey on the real stack: the production
 * Go server (cmd/server) on a disposable Cloud Postgres, a second disposable
 * Postgres as the Local placement, the Firebase Auth emulator, the
 * development mailbox sender, the Vite web app in Chrome, and the real
 * sumi-local-move command.
 *
 *   SUMI_LCR_E2E_DB_URL        disposable empty Cloud Postgres database
 *   SUMI_LCR_E2E_LOCAL_DB_URL  disposable Local-placement Postgres database
 *   FIREBASE_AUTH_EMULATOR_HOST  Firebase Auth emulator (project sumi-studio)
 *   SUMI_LCR_E2E_API_PORT      loopback API port
 *   SUMI_LCR_E2E_WEB_PORT      loopback Vite port
 *   SUMI_LCR_E2E_BINARIES      dir holding server, migrate, local-move and
 *                              e2e-registration-seed builds
 */

test.describe.configure({ mode: "serial", timeout: 300_000 });
test.use({ actionTimeout: 15_000 });

const supportDirectory = dirname(fileURLToPath(import.meta.url));
const webDirectory = resolve(supportDirectory, "..");
const projectID = "sumi-studio";
const wrappingKeyID = "e2e-lcr/v1";

interface Mail {
  file: string;
  to: string;
  subject: string;
  text: string;
  code: string;
}

class Stack {
  readonly children: ChildProcess[] = [];
  readonly seenMail = new Set<string>();
  apiLog = "";
  constructor(
    readonly apiURL: string,
    readonly webURL: string,
    readonly mailbox: string,
    readonly runtime: string,
    readonly binaries: string,
    readonly databaseURL: string,
    readonly localDatabaseURL: string,
    readonly emulatorHost: string,
  ) {}

  /** Runs one seeded binary against a database, returning trimmed stdout. */
  seed(
    binary: string,
    args: string[],
    databaseURL: string,
    extraEnv: Record<string, string> = {},
  ): string {
    const result = spawnSync(join(this.binaries, binary), args, {
      env: {
        ...process.env,
        SUMI_E2E_REG_SEED_DATABASE_URL: databaseURL,
        ...extraEnv,
      },
      encoding: "utf8",
      timeout: 60_000,
    });
    if (result.status !== 0)
      throw new Error(`${binary} ${args}: ${result.stderr}`);
    return result.stdout.trim();
  }

  migrate(databaseURL: string) {
    const result = spawnSync(join(this.binaries, "migrate"), [], {
      env: { ...process.env, SUMI_DB_URL: databaseURL },
      encoding: "utf8",
      timeout: 60_000,
    });
    if (result.status !== 0) throw new Error(`migrate: ${result.stderr}`);
  }

  /** The real Local command, with its own state home on the local DB. */
  mover(personaID: string): ChildProcess {
    const home = join(this.runtime, "local-home");
    mkdirSync(home, { recursive: true, mode: 0o700 });
    const child = spawn(
      join(this.binaries, "local-move"),
      ["start", "--wait", "4m"],
      {
        env: {
          ...process.env,
          SUMI_LOCAL_HOME: home,
          SUMI_DB_URL: this.localDatabaseURL,
          SUMI_PERSONA_ID: personaID,
        },
        stdio: ["pipe", "pipe", "pipe"],
      },
    );
    child.stderr.on("data", (chunk: Buffer) =>
      process.stderr.write(`[local-move] ${chunk}`),
    );
    this.children.push(child);
    return child;
  }

  async nextMail(to: string): Promise<Mail> {
    const deadline = Date.now() + 30_000;
    for (;;) {
      let names: string[] = [];
      try {
        names = readdirSync(this.mailbox).sort();
      } catch {
        // The sender creates the directory on first delivery.
      }
      for (const file of names) {
        if (this.seenMail.has(file)) continue;
        let message: { to: string; subject: string; text: string };
        try {
          message = JSON.parse(readFileSync(join(this.mailbox, file), "utf8"));
        } catch {
          continue;
        }
        this.seenMail.add(file);
        if (message.to !== to) continue;
        const code = /\b(\d{6})\b/.exec(message.subject)?.[1];
        if (!code) throw new Error(`unexpected mail: ${message.text}`);
        return { file, ...message, code };
      }
      if (Date.now() > deadline) throw new Error(`no mail for ${to}`);
      await new Promise((done) => setTimeout(done, 200));
    }
  }

  stop() {
    for (const child of [...this.children].reverse()) {
      if (child.exitCode === null) child.kill("SIGTERM");
    }
    rmSync(this.runtime, { recursive: true, force: true });
  }
}

async function waitForHTTP(
  url: string,
  child: ChildProcess,
  timeoutMs: number,
) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (child.exitCode !== null)
      throw new Error(`exited before ${url} was ready`);
    try {
      if ((await fetch(url)).ok) return;
    } catch {
      // not up yet
    }
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${url}`);
    await new Promise((done) => setTimeout(done, 250));
  }
}

function required(name: string): string {
  const value = process.env[name]?.trim();
  if (!value)
    throw new Error(`${name} is required for the registration-move journey`);
  return value;
}

async function startStack(): Promise<Stack> {
  const databaseURL = required("SUMI_LCR_E2E_DB_URL");
  const localDatabaseURL = required("SUMI_LCR_E2E_LOCAL_DB_URL");
  const emulatorHost = required("FIREBASE_AUTH_EMULATOR_HOST");
  const apiPort = required("SUMI_LCR_E2E_API_PORT");
  const webPort = required("SUMI_LCR_E2E_WEB_PORT");
  const binaries = required("SUMI_LCR_E2E_BINARIES");
  const runtime = mkdtempSync(join(tmpdir(), "sumi-lcr-e2e-"));
  const apiURL = `http://127.0.0.1:${apiPort}`;
  const webURL = `http://127.0.0.1:${webPort}`;
  const stack = new Stack(
    apiURL,
    webURL,
    join(runtime, "mailbox"),
    runtime,
    binaries,
    databaseURL,
    localDatabaseURL,
    emulatorHost,
  );
  try {
    stack.migrate(localDatabaseURL);
    const api = spawn(join(binaries, "server"), [], {
      env: {
        ...process.env,
        PORT: apiPort,
        SUMI_PUBLIC_LOOPBACK_LISTEN: `127.0.0.1:${apiPort}`,
        SUMI_DB_URL: databaseURL,
        SUMI_AGENT_RUNTIME_STATE_DIR: join(runtime, "gateway"),
        SUMI_COMMAND_LOG_DIR: join(runtime, "commands"),
        SUMI_BROWSER_SESSION_SECRET: randomBytes(48).toString("base64"),
        SUMI_BROWSER_SESSION_AUDIENCE: "sumi:web",
        SUMI_BROWSER_WS_ALLOWED_ORIGINS: webURL,
        SUMI_AUTH_ALLOW_INSECURE_COOKIES: "true",
        SUMI_AUTH_FIREBASE_PROJECT_ID: projectID,
        SUMI_AUTH_TENANT_ID: "e2e-lcr",
        SUMI_AGENT_WRAPPING_KEY_ID: wrappingKeyID,
        FIREBASE_AUTH_EMULATOR_HOST: emulatorHost,
        SUMI_AUTH_EMAIL_CHALLENGE_KEY: randomBytes(32).toString("base64"),
        SUMI_AUTH_EMAIL_CHALLENGE_KEY_ID: "e2e-email.v1",
        SUMI_AUTH_EMAIL_SENDER: "dev-mailbox",
        SUMI_AUTH_EMAIL_DEV_MAILBOX_DIR: stack.mailbox,
        SUMI_AUTH_EMAIL_LINK_ORIGIN: webURL,
        SUMI_TRANSFER_PUBLIC_BASE_URL: apiURL,
      },
      stdio: ["ignore", "pipe", "pipe"],
    });
    const record = (chunk: Buffer) => {
      stack.apiLog += chunk.toString();
    };
    api.stdout.on("data", record);
    api.stderr.on("data", record);
    stack.children.push(api);
    await waitForHTTP(`${apiURL}/health`, api, 60_000);

    const vite = spawn(
      process.execPath,
      [
        resolve(webDirectory, "node_modules/vite/bin/vite.js"),
        "--host",
        "127.0.0.1",
        "--port",
        webPort,
        "--strictPort",
      ],
      {
        cwd: webDirectory,
        env: {
          ...process.env,
          SUMI_DEV_API_ORIGIN: apiURL,
          VITE_FIREBASE_AUTH_EMULATOR_URL: `http://${emulatorHost}`,
        },
        stdio: ["ignore", "pipe", "pipe"],
      },
    );
    vite.stderr.on("data", (chunk: Buffer) =>
      process.stderr.write(`[lcr-e2e-web] ${chunk}`),
    );
    stack.children.push(vite);
    await waitForHTTP(`${webURL}/`, vite, 60_000);
    return stack;
  } catch (error) {
    stack.stop();
    throw new Error(`${String(error)}\n${stack.apiLog.slice(-4000)}`);
  }
}

async function sessionUser(page: Page): Promise<string | null> {
  return page.evaluate(async () => {
    const response = await fetch("/auth/session", { credentials: "include" });
    const body = (await response.json()) as {
      authenticated: boolean;
      user?: { id: string };
    };
    return body.authenticated ? (body.user?.id ?? null) : null;
  });
}

async function waitForExit(
  child: ChildProcess,
  timeoutMs: number,
): Promise<number> {
  const deadline = Date.now() + timeoutMs;
  while (child.exitCode === null) {
    if (Date.now() > deadline) throw new Error("local-move did not exit");
    await new Promise((done) => setTimeout(done, 200));
  }
  return child.exitCode;
}

test("invited registration brings the Local secretary through the real move", async ({
  page,
}) => {
  const stack = await startStack();
  try {
    const email = `mover-${randomBytes(4).toString("hex")}@example.test`;
    const token = stack.seed(
      "e2e-registration-seed",
      ["invite"],
      stack.databaseURL,
      { SUMI_E2E_REG_SEED_EMAIL: email },
    );
    const personaID = stack.seed(
      "e2e-registration-seed",
      ["secretary"],
      stack.localDatabaseURL,
    );

    await page.goto(`${stack.webURL}/#invite=${token}`);
    // The invitation resolves to the sign-up intent; the move choice lives on
    // the sign-in path's create-account confirmation.
    await expect(
      page.getByRole("button", { name: "ログインして移動を選ぶ" }),
    ).toBeVisible({ timeout: 30_000 });
    await page.getByRole("button", { name: "ログインして移動を選ぶ" }).click();
    await page.getByPlaceholder("メールアドレス").fill(email);
    await page.getByRole("button", { name: "メールでログイン" }).click();
    await expect(
      page.getByRole("heading", { name: "確認コードを入力" }),
    ).toBeVisible();
    const mail = await stack.nextMail(email);
    await page.getByRole("textbox", { name: "確認コード" }).fill(mail.code);

    // The create-account confirmation offers the explicit secretary choice.
    await expect(
      page.getByRole("button", { name: "Localの秘書を引き継ぐ" }),
    ).toBeVisible({ timeout: 30_000 });
    await page.getByRole("button", { name: "Localの秘書を引き継ぐ" }).click();

    // The move URL is shown with the real command line and the state-only
    // boundary is disclosed.
    const command = page.locator("code", { hasText: "sumi-local-move start" });
    await expect(command).toBeVisible({ timeout: 30_000 });
    await expect(
      page.getByText("移動するのは秘書の状態のみです"),
    ).toBeVisible();
    const commandText = await command.textContent();
    const moveURL = /'(https?:\/\/[^']+)'/.exec(commandText ?? "")?.[1];
    if (!moveURL) throw new Error(`no move URL in ${commandText}`);

    // The real Local command: bind, seal, export, upload.
    const mover = stack.mover(personaID);
    mover.stdin?.write(`${moveURL}\n`);
    await expect(
      page.getByRole("button", { name: "この秘書で登録を完了" }),
    ).toBeVisible({ timeout: 60_000 });
    await page.getByRole("button", { name: "この秘書で登録を完了" }).click();

    // Signed in as the new Human; the Local command finished with the
    // Cloud-issued retirement proof, not a timeout.
    await expect
      .poll(() => sessionUser(page), { timeout: 30_000 })
      .not.toBeNull();
    const moverExit = await waitForExit(mover, 60_000);
    if (moverExit !== 0) throw new Error(`local-move exited ${moverExit}`);
    await expect(page.locator("#login-title")).toBeHidden({
      timeout: 30_000,
    });

    // Identity continuity at rest: the same persona is bound to the one new
    // Human on Cloud with the carried state, and Local authority is gone.
    stack.seed(
      "e2e-registration-seed",
      ["verify", personaID],
      stack.databaseURL,
    );
    stack.seed(
      "e2e-registration-seed",
      ["verify-local", personaID],
      stack.localDatabaseURL,
    );
  } finally {
    stack.stop();
  }
});
