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
 *   SUMI_LCR_E2E_API_PORT      loopback API port (each test stack adds 10)
 *   SUMI_LCR_E2E_WEB_PORT      loopback Vite port (each test stack adds 10)
 *   SUMI_LCR_E2E_BINARIES      dir holding server, migrate, local-move and
 *                              e2e-registration-seed builds
 *
 * Each test gets its own database beside the named one (dbAt appends a
 * suffix) so DB-level identity assertions stay scoped to one registration.
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

/** Rewrites a Postgres URL's database name — per-test isolation. */
function dbAt(base: string, name: string): string {
  const url = new URL(base);
  url.pathname = `/${name}`;
  return url.toString();
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

interface StackOptions {
  /** Slot shifts the API/web ports so test stacks coexist: slot n uses
   * base+2n. Databases default to the env base with a `-s{n}` suffix. */
  slot: number;
  /** When false the API starts without SUMI_TRANSFER_PUBLIC_BASE_URL — the
   * disabled-feature deployment mode. */
  transfer?: boolean;
  databaseName?: string;
  localDatabaseName?: string;
}

async function startStack(options: StackOptions): Promise<Stack> {
  const databaseURL = dbAt(
    required("SUMI_LCR_E2E_DB_URL"),
    options.databaseName ?? `lcr_e2e_s${options.slot}`,
  );
  const localDatabaseURL = dbAt(
    required("SUMI_LCR_E2E_LOCAL_DB_URL"),
    options.localDatabaseName ?? `lcr_e2e_l${options.slot}`,
  );
  const emulatorHost = required("FIREBASE_AUTH_EMULATOR_HOST");
  const apiPort = String(
    Number(required("SUMI_LCR_E2E_API_PORT")) + options.slot * 2,
  );
  const webPort = String(
    Number(required("SUMI_LCR_E2E_WEB_PORT")) + options.slot * 2,
  );
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
        // The deployment switch under test: absent means the transfer routes
        // are not mounted at all.
        ...(options.transfer === false
          ? {}
          : { SUMI_TRANSFER_PUBLIC_BASE_URL: apiURL }),
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

    // SUMI_LCR_E2E_PREVIEW_DIST switches the web side from the dev server to
    // `vite preview` over a real production build — the mode where StrictMode
    // double-mounts disappear and cross-tab cookie rotation is the honest
    // race. The directory must be a build baked with the emulator config.
    const previewDist = process.env.SUMI_LCR_E2E_PREVIEW_DIST?.trim();
    const vite = spawn(
      process.execPath,
      [
        resolve(webDirectory, "node_modules/vite/bin/vite.js"),
        ...(previewDist ? ["preview"] : []),
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
          ...(previewDist ? { SUMI_WEB_DIST_DIR: previewDist } : {}),
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

/** The default invited path: invitation → sign-up intent → email → code →
 * the secretary choice is presented BEFORE any account exists. */
async function inviteSignUpToChoice(
  page: Page,
  stack: Stack,
  email: string,
  token: string,
): Promise<Mail> {
  await page.goto(`${stack.webURL}/#invite=${token}`);
  await page.getByPlaceholder("メールアドレス").fill(email);
  await page.getByRole("button", { name: "メールで新規登録" }).click();
  await expect(
    page.getByRole("heading", { name: "確認コードを入力" }),
  ).toBeVisible();
  const mail = await stack.nextMail(email);
  await page.getByRole("textbox", { name: "確認コード" }).fill(mail.code);
  // The create-account confirmation offers the explicit secretary choice.
  await expect(
    page.getByRole("button", { name: "Localの秘書を引き継ぐ" }),
  ).toBeVisible({ timeout: 30_000 });
  return mail;
}

/** Runs the real Local command against the shown move URL and waits for the
 * arrival state — the finish button appears. */
async function runLocalMove(page: Page, stack: Stack, personaID: string) {
  const command = page.locator("code", { hasText: "sumi-local-move start" });
  await expect(command).toBeVisible({ timeout: 30_000 });
  await expect(page.getByText("移動するのは秘書の状態のみです")).toBeVisible();
  const commandText = await command.textContent();
  const moveURL = /'(https?:\/\/[^']+)'/.exec(commandText ?? "")?.[1];
  if (!moveURL) throw new Error(`no move URL in ${commandText}`);
  const mover = stack.mover(personaID);
  mover.stdin?.write(`${moveURL}\n`);
  await expect(
    page.getByRole("button", { name: "この秘書で登録を完了" }),
  ).toBeVisible({ timeout: 60_000 });
  return mover;
}

test("invited registration offers the move before creating and brings the Local secretary", async ({
  page,
}) => {
  const stack = await startStack({ slot: 0 });
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

    // F297: the default invited sign-up path reaches the choice directly —
    // no secondary sign-in link, and resolve has created no account yet.
    await inviteSignUpToChoice(page, stack, email, token);
    await page.getByRole("button", { name: "Localの秘書を引き継ぐ" }).click();

    // The real Local command: bind, seal, export, upload.
    const mover = await runLocalMove(page, stack, personaID);
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
    stack.seed("e2e-registration-seed", ["verify", personaID], stack.databaseURL);
    stack.seed(
      "e2e-registration-seed",
      ["verify-local", personaID],
      stack.localDatabaseURL,
    );
  } finally {
    stack.stop();
  }
});

test("one transient 401 during transfer polling recovers without a reload", async ({
  page,
}) => {
  const stack = await startStack({ slot: 1 });
  try {
    const email = `retry-${randomBytes(4).toString("hex")}@example.test`;
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

    await inviteSignUpToChoice(page, stack, email, token);

    // F295: the shared CSRF cookie rotates under a sibling tab's traffic, so
    // an ordinary poll can land one 401 that says nothing about the flow.
    // Inject exactly one, then let the request through: the client must
    // refetch CSRF and retry once instead of wedging the move.
    let injected = false;
    await page.route(
      "**/api/secretary-transfer/registrant/session",
      async (route) => {
        if (!injected) {
          injected = true;
          await route.fulfill({
            status: 401,
            contentType: "application/json",
            body: JSON.stringify({ error: "unauthorized" }),
          });
          return;
        }
        await route.continue();
      },
    );
    await page.getByRole("button", { name: "Localの秘書を引き継ぐ" }).click();

    const mover = await runLocalMove(page, stack, personaID);
    await page.getByRole("button", { name: "この秘書で登録を完了" }).click();

    await expect
      .poll(() => sessionUser(page), { timeout: 30_000 })
      .not.toBeNull();
    const moverExit = await waitForExit(mover, 60_000);
    if (moverExit !== 0) throw new Error(`local-move exited ${moverExit}`);
    stack.seed("e2e-registration-seed", ["verify", personaID], stack.databaseURL);
  } finally {
    stack.stop();
  }
});

test("a persistent 401 stops polling with a truthful state, not a loop", async ({
  page,
}) => {
  const stack = await startStack({ slot: 5 });
  try {
    const email = `lost-${randomBytes(4).toString("hex")}@example.test`;
    const token = stack.seed(
      "e2e-registration-seed",
      ["invite"],
      stack.databaseURL,
      { SUMI_E2E_REG_SEED_EMAIL: email },
    );

    await inviteSignUpToChoice(page, stack, email, token);
    await page.getByRole("button", { name: "Localの秘書を引き継ぐ" }).click();
    await expect(
      page.locator("code", { hasText: "sumi-local-move start" }),
    ).toBeVisible({ timeout: 30_000 });

    // F295's other half: a 401 that survives the one retry is a real refusal.
    // Polling must stop and say the sign-in lapsed — never loop forever or
    // treat a dead flow as alive.
    let refused = 0;
    await page.route(
      "**/api/secretary-transfer/registrant/session",
      async (route) => {
        refused += 1;
        await route.fulfill({
          status: 401,
          contentType: "application/json",
          body: JSON.stringify({ error: "unauthorized" }),
        });
      },
    );
    await expect(
      page.getByText(
        "ログインの有効期限が切れました。このページを閉じても移動は継続します。",
      ),
    ).toBeVisible({ timeout: 30_000 });
    // Let any tick already in flight land, then watch a full poll window.
    await page.waitForTimeout(2_000);
    const refusedAtStop = refused;
    await page.waitForTimeout(3_000);
    expect(refused).toBe(refusedAtStop);
    // The finish CTA is not offered on a dead proof.
    await expect(
      page.getByRole("button", { name: "この秘書で登録を完了" }),
    ).toHaveCount(0);
  } finally {
    stack.stop();
  }
});

test("a sibling tab's refused sign-up does not destroy an in-progress move", async ({
  page,
  context,
}) => {
  const stack = await startStack({ slot: 2 });
  try {
    const email = `tabs-${randomBytes(4).toString("hex")}@example.test`;
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

    // Tab A reaches the move and is waiting on the Local command.
    await inviteSignUpToChoice(page, stack, email, token);
    await page.getByRole("button", { name: "Localの秘書を引き継ぐ" }).click();
    await expect(
      page.locator("code", { hasText: "sumi-local-move start" }),
    ).toBeVisible({ timeout: 30_000 });

    // Tab B signs up on the same invite and gets its code wrong once — a
    // deterministic 4xx refusal. F294: it must not sign the shared Firebase
    // credential out from under tab A's in-progress confirmation.
    const tabB = await context.newPage();
    await tabB.goto(`${stack.webURL}/#invite=${token}`);
    await tabB.getByPlaceholder("メールアドレス").fill(email);
    await tabB.getByRole("button", { name: "メールで新規登録" }).click();
    await expect(
      tabB.getByRole("heading", { name: "確認コードを入力" }),
    ).toBeVisible();
    await stack.nextMail(email); // consume tab B's challenge
    await tabB.getByRole("textbox", { name: "確認コード" }).fill("000000");
    await expect(
      tabB.getByText(/確認コードが正しくありません/),
    ).toBeVisible();

    // Tab A's move panel — and its Firebase capability — survived the
    // sibling refusal. The Local command can still complete the move.
    await expect(
      page.locator("code", { hasText: "sumi-local-move start" }),
    ).toBeVisible();
    const mover = await runLocalMove(page, stack, personaID);
    await page.getByRole("button", { name: "この秘書で登録を完了" }).click();
    await expect
      .poll(() => sessionUser(page), { timeout: 30_000 })
      .not.toBeNull();
    const moverExit = await waitForExit(mover, 60_000);
    if (moverExit !== 0) throw new Error(`local-move exited ${moverExit}`);
    stack.seed("e2e-registration-seed", ["verify", personaID], stack.databaseURL);
  } finally {
    stack.stop();
  }
});

test("without the transfer surface invited sign-up provisions directly", async ({
  page,
}) => {
  const stack = await startStack({ slot: 3, transfer: false });
  try {
    const email = `direct-${randomBytes(4).toString("hex")}@example.test`;
    const token = stack.seed(
      "e2e-registration-seed",
      ["invite"],
      stack.databaseURL,
      { SUMI_E2E_REG_SEED_EMAIL: email },
    );

    // F296: no transfer routes are mounted, so the invited sign-up resolves
    // straight to an account — no choice panel, no doomed CTA.
    await page.goto(`${stack.webURL}/#invite=${token}`);
    await page.getByPlaceholder("メールアドレス").fill(email);
    await page.getByRole("button", { name: "メールで新規登録" }).click();
    await expect(
      page.getByRole("heading", { name: "確認コードを入力" }),
    ).toBeVisible();
    const mail = await stack.nextMail(email);
    await page.getByRole("textbox", { name: "確認コード" }).fill(mail.code);
    await expect
      .poll(() => sessionUser(page), { timeout: 30_000 })
      .not.toBeNull();
    await expect(page.locator("#login-title")).toBeHidden({
      timeout: 30_000,
    });
    await expect(
      page.getByRole("button", { name: "Localの秘書を引き継ぐ" }),
    ).toHaveCount(0);

    // The route surface itself is absent — a plain mux 404, not the mounted
    // handlers' JSON "transfer session not found".
    const transferStatus = await page.evaluate(async () => {
      const csrf = await fetch("/auth/csrf", { credentials: "include" });
      const { csrf_token } = (await csrf.json()) as { csrf_token: string };
      const response = await fetch(
        "/api/secretary-transfer/registrant/session",
        {
          method: "POST",
          credentials: "include",
          headers: {
            "Content-Type": "application/json",
            "X-CSRF-Token": csrf_token,
          },
          body: JSON.stringify({ flow_id: "none", nonce: "none" }),
        },
      );
      return response.status;
    });
    expect(transferStatus).toBe(404);
  } finally {
    stack.stop();
  }
});

test("without the transfer surface the sign-in confirmation shows no move CTA", async ({
  page,
}) => {
  const stack = await startStack({ slot: 4, transfer: false });
  try {
    const email = `cta-${randomBytes(4).toString("hex")}@example.test`;
    const token = stack.seed(
      "e2e-registration-seed",
      ["invite"],
      stack.databaseURL,
      { SUMI_E2E_REG_SEED_EMAIL: email },
    );

    // A person on the sign-in intent still reaches the create-account
    // confirmation; the Local CTA must never render when the API has no
    // transfer routes — the choice collapses to the plain new-account ask.
    await page.goto(`${stack.webURL}/#invite=${token}`);
    await page.getByRole("button", { name: "ログイン" }).click();
    await page.getByPlaceholder("メールアドレス").fill(email);
    await page.getByRole("button", { name: "メールでログイン" }).click();
    await expect(
      page.getByRole("heading", { name: "確認コードを入力" }),
    ).toBeVisible();
    const mail = await stack.nextMail(email);
    await page.getByRole("textbox", { name: "確認コード" }).fill(mail.code);
    const createNew = page.getByRole("button", {
      name: "新しい秘書で登録する",
    });
    await expect(createNew).toBeVisible({ timeout: 30_000 });
    await expect(
      page.getByRole("button", { name: "Localの秘書を引き継ぐ" }),
    ).toHaveCount(0);
    await createNew.click();
    await expect
      .poll(() => sessionUser(page), { timeout: 30_000 })
      .not.toBeNull();
  } finally {
    stack.stop();
  }
});
