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
  type Route,
  test,
} from "@playwright/test";

/**
 * The code-first email sign-in journey on the real stack: the production Go
 * server (cmd/server) on a disposable Postgres database, the Firebase Auth
 * emulator, the development mailbox sender, and the Vite web app in Chrome.
 *
 *   SUMI_AUTH_EMAIL_E2E_DB_URL          disposable empty Postgres database
 *   FIREBASE_AUTH_EMULATOR_HOST         Firebase Auth emulator (project sumi-studio)
 *   SUMI_AUTH_EMAIL_E2E_API_PORT        loopback API port
 *   SUMI_AUTH_EMAIL_E2E_WEB_PORT        loopback Vite port
 *   SUMI_AUTH_EMAIL_E2E_BUILD_DIR       optional prebuilt server + seed binaries
 *   SUMI_AUTH_EMAIL_E2E_SCREENSHOT_DIR  optional absolute screenshot directory
 */

test.describe.configure({ mode: "serial", timeout: 240_000 });
test.use({ actionTimeout: 15_000 });

const supportDirectory = dirname(fileURLToPath(import.meta.url));
const apiDirectory = resolve(supportDirectory, "../../api");
const webDirectory = resolve(supportDirectory, "..");
const projectID = "sumi-studio";
const wrappingKeyID = "e2e-auth-email/v1";

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
    throw new Error(`${name} is required for the email-code browser journey`);
  return value;
}

async function startStack(): Promise<Stack> {
  const databaseURL = required("SUMI_AUTH_EMAIL_E2E_DB_URL");
  const emulatorHost = required("FIREBASE_AUTH_EMULATOR_HOST");
  const apiPort = required("SUMI_AUTH_EMAIL_E2E_API_PORT");
  const webPort = required("SUMI_AUTH_EMAIL_E2E_WEB_PORT");
  const runtime = mkdtempSync(join(tmpdir(), "sumi-auth-email-e2e-"));
  let binaries = process.env.SUMI_AUTH_EMAIL_E2E_BUILD_DIR?.trim() ?? "";
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
        SUMI_AUTH_TENANT_ID: "e2e-auth-email",
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
      process.stderr.write(`[email-e2e-web] ${chunk}`),
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

async function startCode(page: Page, webURL: string, email: string) {
  await page.goto(`${webURL}/`);
  await page.getByPlaceholder("メールアドレス").fill(email);
  await page.getByRole("button", { name: "メールでログイン" }).click();
  await expect(
    page.getByRole("heading", { name: "確認コードを入力" }),
  ).toBeVisible();
}

/**
 * Sends the intercepted request to the real server, so it commits, and then
 * resets the browser's connection: neither the body nor the session cookie
 * reaches the page.
 */
async function commitWithoutDelivery(route: Route): Promise<number> {
  const request = route.request();
  const headers: Record<string, string> = {};
  for (const [name, value] of Object.entries(await request.allHeaders())) {
    if (
      !name.startsWith(":") &&
      !["host", "content-length", "connection", "accept-encoding"].includes(
        name,
      )
    ) {
      headers[name] = value;
    }
  }
  const response = await fetch(request.url(), {
    method: request.method(),
    headers,
    body: request.postData() ?? undefined,
  });
  await response.arrayBuffer();
  await route.abort("connectionreset");
  return response.status;
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
    process.env.SUMI_AUTH_EMAIL_E2E_SCREENSHOT_DIR?.trim() ||
    testInfo.outputPath("screenshots");
  mkdirSync(screenshots, { recursive: true });
  stack = await startStack();
});

test.afterAll(async () => {
  const testInfo = test.info();
  if (!stack) return;
  const secrets = [...stack.seenMail]
    .map((file) => {
      try {
        return readFileSync(join(stack.mailbox, file), "utf8");
      } catch {
        return "";
      }
    })
    .flatMap((raw) => /token=([A-Za-z0-9_-]{43})/.exec(raw)?.slice(1) ?? []);
  await testInfo.attach("api-log", {
    body: stack.apiLog,
    contentType: "text/plain",
  });
  stack.stop();
  for (const token of secrets) {
    expect(stack.apiLog).not.toContain(token);
  }
  expect(stack.apiLog).not.toContain("email-sign-in#");
});

test("code entry signs in an existing account after a wrong code and a resend", async ({
  browser,
}) => {
  const account = stack.seedAccount("code");
  const { context, page } = await freshPage(browser);
  await page.goto(`${stack.webURL}/`);
  await shot(page, "01-login-card");
  await startCode(page, stack.webURL, account.email);
  const first = await stack.nextMail(account.email);
  const input = page.getByRole("textbox", { name: "確認コード" });
  await expect(input).toHaveAttribute("autocomplete", "one-time-code");
  await expect(input).toHaveAttribute("inputmode", "numeric");
  await expect(
    page.getByRole("button", { name: /再送信（\d+秒）/ }),
  ).toBeDisabled();
  await shot(page, "02-code-sent");

  const wrong = first.code === "000000" ? "111111" : "000000";
  await input.fill(wrong);
  await expect(page.getByRole("alert")).toHaveText(
    "確認コードが正しくありません。あと4回入力できます。",
  );
  await shot(page, "03-wrong-code");

  const resend = page.getByRole("button", { name: "コードを再送信" });
  await expect(resend).toBeEnabled({ timeout: 40_000 });
  await resend.click();
  await expect(page.getByText(/メールを再送信しました/)).toBeVisible();
  const second = await stack.nextMail(account.email);
  // A live code with time and attempts left is re-sent, so either mail works.
  expect(second.code).toBe(first.code);
  await shot(page, "04-resent");

  // Paste with a space, as copied from some mail clients.
  await input.fill(`${second.code.slice(0, 3)} ${second.code.slice(3)}`);
  await expectSignedIn(page, account.humanID);
  await shot(page, "05-signed-in");
  await context.close();
});

test("another browser must choose before it takes over, and the original tab says so", async ({
  browser,
}) => {
  const account = stack.seedAccount("adopt");
  const original = await freshPage(browser);
  await startCode(original.page, stack.webURL, account.email);
  const mail = await stack.nextMail(account.email);

  const other = await freshPage(browser);
  await other.page.goto(mail.link);
  await expect(
    other.page.getByRole("heading", { name: "メールのリンクで続ける" }),
  ).toBeVisible();
  const proceed = other.page.getByRole("button", {
    name: "このブラウザで続ける",
  });
  await expect(proceed).toBeVisible();
  expect(new URL(other.page.url()).hash).toBe("");
  await shot(other.page, "06-link-other-browser-choice");
  expect(await sessionUser(other.page)).toBeNull();

  await proceed.click();
  await expectSignedIn(other.page, account.humanID);

  await expect(original.page.getByRole("alert")).toHaveText(
    "このログインは別のブラウザで続行されました。こちらで続ける場合は、もう一度メールアドレスを入力してください。",
    { timeout: 20_000 },
  );
  expect(await sessionUser(original.page)).toBeNull();
  await shot(original.page, "07-original-continued-elsewhere");
  await original.context.close();

  // The authenticated browser opens a link for a different account.
  const second = stack.seedAccount("switch");
  const starter = await freshPage(browser);
  await startCode(starter.page, stack.webURL, second.email);
  const switchMail = await stack.nextMail(second.email);
  await other.page.goto(switchMail.link);
  await expect(
    other.page.getByRole("heading", { name: "アカウントを切り替えますか？" }),
  ).toBeVisible();
  const switchButton = other.page.getByRole("button", {
    name: "現在のセッションを終了して切り替える",
  });
  await expect(switchButton).toBeVisible();
  expect(await sessionUser(other.page)).toBe(account.humanID);
  await shot(other.page, "08-link-switch-account-choice");
  await switchButton.click();
  await expectSignedIn(other.page, second.humanID);
  await shot(other.page, "09-switched");
  await starter.context.close();
  await other.context.close();
});

test("opening a link elsewhere and cancelling leaves the code usable", async ({
  browser,
}) => {
  const account = stack.seedAccount("scan");
  const app = await freshPage(browser);
  await startCode(app.page, stack.webURL, account.email);
  const mail = await stack.nextMail(account.email);

  const scanner = await freshPage(browser);
  await scanner.page.goto(mail.link);
  await scanner.page
    .getByRole("button", { name: "キャンセル（元の画面でコードを入力する）" })
    .click();
  await expect(scanner.page.getByPlaceholder("メールアドレス")).toBeVisible();
  expect(await sessionUser(scanner.page)).toBeNull();
  await scanner.context.close();

  await app.page.getByRole("textbox", { name: "確認コード" }).fill(mail.code);
  await expectSignedIn(app.page, account.humanID);
  await app.context.close();
});

test("a committed sign-in whose body was lost after the cookie arrived needs no replay", async ({
  browser,
}) => {
  const account = stack.seedAccount("lost-body");
  const { context, page } = await freshPage(browser);
  let resolves = 0;
  await page.route("**/auth/flows/resolve", async (route) => {
    resolves += 1;
    if (resolves === 1) {
      const response = await route.fetch();
      await route.fulfill({ response, body: "{" });
      return;
    }
    await route.continue();
  });
  await startCode(page, stack.webURL, account.email);
  const mail = await stack.nextMail(account.email);
  await page.getByRole("textbox", { name: "確認コード" }).fill(mail.code);
  await expectSignedIn(page, account.humanID);
  expect(resolves).toBe(1);
  await context.close();
});

test("a committed sign-in whose cookie never arrived is replayed for the same account", async ({
  browser,
}) => {
  const account = stack.seedAccount("lost-cookie");
  const { context, page } = await freshPage(browser);
  const committed: number[] = [];
  let resolves = 0;
  await page.route("**/auth/flows/resolve", async (route) => {
    resolves += 1;
    if (resolves === 1) {
      committed.push(await commitWithoutDelivery(route));
      return;
    }
    await route.continue();
  });
  await startCode(page, stack.webURL, account.email);
  const mail = await stack.nextMail(account.email);
  await page.getByRole("textbox", { name: "確認コード" }).fill(mail.code);
  await expectSignedIn(page, account.humanID);
  expect(committed).toEqual([200]);
  expect(resolves).toBe(2);
  await context.close();
});

test("when the replay is lost too, the kept flow recovers the same account", async ({
  browser,
}) => {
  const account = stack.seedAccount("lost-twice");
  const { context, page } = await freshPage(browser);
  const committed: number[] = [];
  let resolves = 0;
  await page.route("**/auth/flows/resolve", async (route) => {
    resolves += 1;
    if (resolves <= 2) {
      committed.push(await commitWithoutDelivery(route));
      return;
    }
    await route.continue();
  });
  await startCode(page, stack.webURL, account.email);
  const mail = await stack.nextMail(account.email);
  await page.getByRole("textbox", { name: "確認コード" }).fill(mail.code);
  await expect(page.getByRole("alert")).toHaveText(
    "Sumi のセッションを開始できませんでした。",
  );
  expect(await sessionUser(page)).toBeNull();
  await shot(page, "11-completion-response-lost");
  // The status poll finds the completed flow without a session and replays it.
  await expectSignedIn(page, account.humanID);
  expect(committed).toEqual([200, 200]);
  expect(resolves).toBe(3);
  await context.close();
});

test("the link in the same browser completes without a choice and the code tab follows", async ({
  browser,
}) => {
  const account = stack.seedAccount("same");
  const { context, page } = await freshPage(browser);
  await startCode(page, stack.webURL, account.email);
  const mail = await stack.nextMail(account.email);

  const linkPage = await context.newPage();
  await linkPage.goto(mail.link);
  await expectSignedIn(linkPage, account.humanID);
  await shot(linkPage, "10-link-same-browser-signed-in");
  await expectSignedIn(page, account.humanID);
  await context.close();
});
