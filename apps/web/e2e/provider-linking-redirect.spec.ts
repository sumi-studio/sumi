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
import {
  type Browser,
  type BrowserContext,
  expect,
  type Page,
  test,
} from "@playwright/test";

/**
 * Settings account linking and reauthentication on the real stack: the
 * production Go server (cmd/server) on a disposable Postgres database, the
 * Firebase Auth emulator's fake IdP widget, the development mailbox sender,
 * and the Vite web app in Chrome. Provider trips must stay in the same tab —
 * no popup may be opened anywhere in these journeys.
 *
 *   SUMI_PROVIDER_E2E_DB_URL          disposable empty Postgres database
 *   FIREBASE_AUTH_EMULATOR_HOST       Firebase Auth emulator (project sumi-studio)
 *   SUMI_PROVIDER_E2E_API_PORT        loopback API port
 *   SUMI_PROVIDER_E2E_WEB_PORT        loopback Vite port
 *   SUMI_PROVIDER_E2E_BUILD_DIR       optional prebuilt server + seed binaries
 *   SUMI_PROVIDER_E2E_SCREENSHOT_DIR  optional absolute screenshot directory
 */

test.describe.configure({ mode: "serial", timeout: 300_000 });
test.use({ actionTimeout: 15_000 });

const supportDirectory = dirname(fileURLToPath(import.meta.url));
const apiDirectory = resolve(supportDirectory, "../../api");
const webDirectory = resolve(supportDirectory, "..");
const projectID = "sumi-studio";
const wrappingKeyID = "e2e-provider-redirect/v1";

interface Mail {
  file: string;
  to: string;
  subject: string;
  text: string;
  code: string;
  link: string;
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
    readonly emulatorHost: string,
  ) {}

  seedAccount(label: string): { humanID: string; email: string } {
    const suffix = randomBytes(4).toString("hex");
    const email = `${label}-${suffix}@example.test`;
    const result = spawnSync(join(this.binaries, "e2e-auth-email-seed"), [], {
      env: {
        ...process.env,
        FIREBASE_AUTH_EMULATOR_HOST: this.emulatorHost,
        SUMI_E2E_AUTH_SEED_DATABASE_URL: this.databaseURL,
        SUMI_E2E_AUTH_SEED_WRAPPING_KEY_ID: wrappingKeyID,
        SUMI_E2E_AUTH_SEED_PROJECT_ID: projectID,
        SUMI_E2E_AUTH_SEED_FIREBASE_UID: `e2e-${label}-${suffix}`,
        SUMI_E2E_AUTH_SEED_EMAIL: email,
      },
      encoding: "utf8",
    });
    if (result.status !== 0) throw new Error(`seed: ${result.stderr}`);
    return { humanID: result.stdout.trim(), email };
  }

  /** Waits for the next unseen development-mailbox message to this address. */
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
          continue; // still being written
        }
        this.seenMail.add(file);
        if (message.to !== to) continue;
        const code = /\b(\d{6})\b/.exec(message.subject)?.[1];
        const link = /(http:\/\/\S+\/email-sign-in#\S+)/.exec(
          message.text,
        )?.[1];
        if (!code || !link) throw new Error(`unexpected mail: ${message.text}`);
        return { file, ...message, code, link };
      }
      if (Date.now() > deadline) throw new Error(`no mail for ${to}`);
      await new Promise((done) => setTimeout(done, 200));
    }
  }

  /** Expires a pending provider operation so the server rules it expired. */
  expireProviderOperation() {
    const result = spawnSync(
      "psql",
      [
        this.databaseURL,
        "-c",
        "UPDATE provider_operations SET expires_at = now() - interval '1 second' WHERE status = 'pending'",
      ],
      { encoding: "utf8" },
    );
    if (result.status !== 0) throw new Error(`expire: ${result.stderr}`);
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
    throw new Error(`${name} is required for the provider-redirect journey`);
  return value;
}

async function startStack(): Promise<Stack> {
  const databaseURL = required("SUMI_PROVIDER_E2E_DB_URL");
  const emulatorHost = required("FIREBASE_AUTH_EMULATOR_HOST");
  const apiPort = required("SUMI_PROVIDER_E2E_API_PORT");
  const webPort = required("SUMI_PROVIDER_E2E_WEB_PORT");
  const runtime = mkdtempSync(join(tmpdir(), "sumi-provider-e2e-"));
  let binaries = process.env.SUMI_PROVIDER_E2E_BUILD_DIR?.trim() ?? "";
  if (!binaries) {
    binaries = join(runtime, "bin");
    const build = spawnSync(
      "go",
      [
        "build",
        "-buildvcs=false",
        "-o",
        `${binaries}/`,
        "./cmd/server",
        "./cmd/e2e-auth-email-seed",
      ],
      { cwd: apiDirectory, encoding: "utf8", timeout: 300_000 },
    );
    if (build.status !== 0) throw new Error(`go build: ${build.stderr}`);
  }
  const apiURL = `http://127.0.0.1:${apiPort}`;
  const webURL = `http://127.0.0.1:${webPort}`;
  const stack = new Stack(
    apiURL,
    webURL,
    join(runtime, "mailbox"),
    runtime,
    binaries,
    databaseURL,
    emulatorHost,
  );
  try {
    const api = spawn(join(binaries, "server"), [], {
      cwd: apiDirectory,
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
        SUMI_AUTH_TENANT_ID: "e2e-provider-redirect",
        SUMI_AGENT_WRAPPING_KEY_ID: wrappingKeyID,
        FIREBASE_AUTH_EMULATOR_HOST: emulatorHost,
        SUMI_AUTH_EMAIL_CHALLENGE_KEY: randomBytes(32).toString("base64"),
        SUMI_AUTH_EMAIL_CHALLENGE_KEY_ID: "e2e-email.v1",
        SUMI_AUTH_EMAIL_SENDER: "dev-mailbox",
        SUMI_AUTH_EMAIL_DEV_MAILBOX_DIR: stack.mailbox,
        SUMI_AUTH_EMAIL_LINK_ORIGIN: webURL,
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
    vite.stderr.on("data", (chunk) =>
      process.stderr.write(`[provider-e2e-web] ${chunk}`),
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

async function expectSignedIn(page: Page, humanID: string) {
  await expect.poll(() => sessionUser(page), { timeout: 30_000 }).toBe(humanID);
  await expect(page.locator("#login-title")).toBeHidden({ timeout: 30_000 });
}

async function signInWithEmailCode(
  page: Page,
  stack: Stack,
  email: string,
  humanID: string,
) {
  await page.goto(`${stack.webURL}/`);
  await page.getByPlaceholder("メールアドレス").fill(email);
  await page.getByRole("button", { name: "メールでログイン" }).click();
  await expect(
    page.getByRole("heading", { name: "確認コードを入力" }),
  ).toBeVisible();
  const mail = await stack.nextMail(email);
  await page.getByRole("textbox", { name: "確認コード" }).fill(mail.code);
  await expectSignedIn(page, humanID);
}

async function openSettings(page: Page) {
  await page.getByRole("button", { name: "設定" }).click();
  await expect(
    page.getByRole("heading", { name: "ログイン方法" }),
  ).toBeVisible();
}

/**
 * The page must be on the emulator's fake IdP widget. Creates a new fake
 * account with the given email and returns to the app — all in one tab.
 */
async function emulatorLinkProvider(page: Page, email: string) {
  await page.waitForURL(/emulator\/auth\/handler/, { timeout: 20_000 });
  await page.locator("#add-account-button").click();
  await page.locator("#email-input").fill(email);
  await page.locator("#display-name-input").fill("E2E Persona");
  await page.locator("#sign-in").click();
  await page.waitForURL(`${stack.webURL}/**`, { timeout: 30_000 });
}

/** Reuses an existing fake IdP account (same provider subject). */
async function emulatorReuseProviderAccount(page: Page, email: string) {
  await page.waitForURL(/emulator\/auth\/handler/, { timeout: 20_000 });
  const account = page.locator(".js-reuse-account", { hasText: email });
  await expect(account).toBeVisible();
  await account.click();
  await page.waitForURL(`${stack.webURL}/**`, { timeout: 30_000 });
}

let stack: Stack;
let screenshots: string;

async function shot(page: Page, name: string) {
  await page.screenshot({ path: join(screenshots, `${name}.png`) });
}

async function freshPage(
  browser: Browser,
): Promise<{ context: BrowserContext; page: Page }> {
  const context = await browser.newContext({
    viewport: { width: 420, height: 860 },
  });
  return { context, page: await context.newPage() };
}

test.beforeAll(async () => {
  const testInfo = test.info();
  testInfo.setTimeout(420_000);
  screenshots =
    process.env.SUMI_PROVIDER_E2E_SCREENSHOT_DIR?.trim() ||
    testInfo.outputPath("screenshots");
  mkdirSync(screenshots, { recursive: true });
  stack = await startStack();
});

test.afterAll(async () => {
  const testInfo = test.info();
  if (!stack) return;
  await testInfo.attach("api-log", {
    body: stack.apiLog,
    contentType: "text/plain",
  });
  stack.stop();
});

test("linking Google from settings stays in one tab and returns to the action", async ({
  browser,
}) => {
  const account = stack.seedAccount("link");
  const { context, page } = await freshPage(browser);
  let popups = 0;
  page.on("popup", () => {
    popups += 1;
  });
  await signInWithEmailCode(page, stack, account.email, account.humanID);
  await openSettings(page);
  await shot(page, "01-settings");

  await page.getByRole("button", { name: "Googleを追加" }).click();
  // The whole tab leaves for the provider; no popup may be involved.
  await emulatorLinkProvider(page, `persona-${account.email}`);

  // The return reopens the settings action and finishes the link.
  await expect(
    page.getByRole("status").filter({ hasText: "Googleを追加しました" }),
  ).toBeVisible({ timeout: 45_000 });
  await shot(page, "02-linked");
  expect(popups).toBe(0);
  await expect(
    page.getByRole("button", { name: "Googleの解除を開始" }),
  ).toBeVisible();
  await context.close();
});

test("a cancelled provider return settles the operation and keeps settings usable", async ({
  browser,
}) => {
  const account = stack.seedAccount("cancel");
  const { context, page } = await freshPage(browser);
  await signInWithEmailCode(page, stack, account.email, account.humanID);
  await openSettings(page);
  await page.getByRole("button", { name: "GitHubを追加" }).click();
  await page.waitForURL(/emulator\/auth\/handler/, { timeout: 20_000 });

  // Backing out before choosing an account returns to the app; the pending
  // operation must be settled as cancelled, not left hanging.
  await page.goBack();
  await expect(
    page.getByRole("alert").filter({ hasText: "認証をキャンセルしました。" }),
  ).toBeVisible({ timeout: 45_000 });
  await shot(page, "03-cancelled");
  await expect(
    page.getByRole("button", { name: "GitHubを追加" }),
  ).toBeVisible();
  await context.close();
});

test("a provider credential already linked elsewhere is refused with a clear error", async ({
  browser,
}) => {
  const personaEmail = `shared-${randomBytes(4).toString("hex")}@example.test`;
  const first = stack.seedAccount("first");
  const second = stack.seedAccount("second");

  const firstBrowser = await freshPage(browser);
  await signInWithEmailCode(
    firstBrowser.page,
    stack,
    first.email,
    first.humanID,
  );
  await openSettings(firstBrowser.page);
  await firstBrowser.page.getByRole("button", { name: "Googleを追加" }).click();
  await emulatorLinkProvider(firstBrowser.page, personaEmail);
  await expect(
    firstBrowser.page
      .getByRole("status")
      .filter({ hasText: "Googleを追加しました" }),
  ).toBeVisible({ timeout: 45_000 });

  const secondBrowser = await freshPage(browser);
  await signInWithEmailCode(
    secondBrowser.page,
    stack,
    second.email,
    second.humanID,
  );
  await openSettings(secondBrowser.page);
  await secondBrowser.page
    .getByRole("button", { name: "Googleを追加" })
    .click();
  // The emulator lists the persona created above; reusing it carries the same
  // provider subject, which Firebase reports as credential-already-in-use.
  await emulatorReuseProviderAccount(secondBrowser.page, personaEmail);
  await expect(
    secondBrowser.page.getByRole("alert").filter({
      hasText: "このログイン方法は別のアカウントで使用されています。",
    }),
  ).toBeVisible({ timeout: 45_000 });
  await shot(secondBrowser.page, "04-credential-in-use");
  await expect(
    secondBrowser.page.getByRole("button", { name: "Googleを追加" }),
  ).toBeVisible();
  await firstBrowser.context.close();
  await secondBrowser.context.close();
});

test("unlinking a provider reauthenticates through the other linked method and resumes in place", async ({
  browser,
}) => {
  const account = stack.seedAccount("unlink");
  const { context, page } = await freshPage(browser);
  let popups = 0;
  page.on("popup", () => {
    popups += 1;
  });
  await signInWithEmailCode(page, stack, account.email, account.humanID);
  await openSettings(page);

  // Two linked methods: unlinking one reauthenticates through the other.
  const googlePersona = `google-${account.email}`;
  await page.getByRole("button", { name: "Googleを追加" }).click();
  await emulatorLinkProvider(page, googlePersona);
  await expect(
    page.getByRole("status").filter({ hasText: "Googleを追加しました" }),
  ).toBeVisible({ timeout: 45_000 });
  await page.getByRole("button", { name: "GitHubを追加" }).click();
  await emulatorLinkProvider(page, `github-${account.email}`);
  await expect(
    page.getByRole("status").filter({ hasText: "GitHubを追加しました" }),
  ).toBeVisible({ timeout: 45_000 });

  await page.getByRole("button", { name: "GitHubの解除を開始" }).click();
  await page.getByRole("button", { name: "再認証して解除" }).click();
  // The reauth leaves through Google — the same fake account — in this tab.
  await emulatorReuseProviderAccount(page, googlePersona);

  await expect(
    page.getByRole("status").filter({ hasText: "GitHubを解除しました" }),
  ).toBeVisible({ timeout: 45_000 });
  await shot(page, "05-unlinked");
  expect(popups).toBe(0);
  await expect(page.getByRole("button", { name: "GitHubを追加" })).toBeVisible({
    timeout: 10_000,
  });
  await context.close();
});

test("an expired server operation after the return is named, not replayed", async ({
  browser,
}) => {
  const account = stack.seedAccount("expired");
  const { context, page } = await freshPage(browser);
  await signInWithEmailCode(page, stack, account.email, account.humanID);
  await openSettings(page);
  await page.getByRole("button", { name: "GitHubを追加" }).click();
  await page.waitForURL(/emulator\/auth\/handler/, { timeout: 20_000 });

  // The person lingers at the provider past the server-side TTL.
  stack.expireProviderOperation();
  await page.locator("#add-account-button").click();
  await page.locator("#email-input").fill(`expired-${account.email}`);
  await page.locator("#sign-in").click();
  await page.waitForURL(`${stack.webURL}/**`, { timeout: 30_000 });

  // The link reached Firebase but the operation expired: settings must show
  // an honest failure and keep the change resumable rather than pretending
  // success.
  await expect(page.getByRole("alert")).toBeVisible({ timeout: 45_000 });
  await shot(page, "06-expired");
  await context.close();
});
