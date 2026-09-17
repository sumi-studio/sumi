import { type ChildProcess, spawn, spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
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
 * The no-sender login journey on the real stack: the production Go server
 * WITHOUT the SUMI_AUTH_EMAIL_* quartet (email routes unmounted), the
 * Firebase Auth emulator, and the Vite web app in Chrome. Proves OAuth and
 * the invited registration choice stay usable, the email control is never
 * offered, capability-read failure is explicit and retryable, and saved
 * email state (pending code, pending link) degrades to a deliberate
 * unavailable panel instead of a dead form.
 *
 *   SUMI_UI_E2E_DB_URL            disposable empty Postgres database
 *   FIREBASE_AUTH_EMULATOR_HOST   Firebase Auth emulator (project sumi-studio)
 *   SUMI_UI_E2E_API_PORT          loopback API port
 *   SUMI_UI_E2E_WEB_PORT          loopback Vite port
 *   SUMI_UI_E2E_BUILD_DIR         optional prebuilt server + seed binaries
 *   SUMI_UI_E2E_SCREENSHOT_DIR    optional absolute screenshot directory
 */

test.describe.configure({ mode: "serial", timeout: 240_000 });
test.use({ actionTimeout: 15_000 });
test.use({
  launchOptions: {
    executablePath:
      process.env.SUMI_UI_E2E_CHROME?.trim() ||
      "/ms-playwright/chromium-1148/chrome-linux/chrome",
    args: ["--no-sandbox", "--disable-dev-shm-usage"],
  },
});

const supportDirectory = dirname(fileURLToPath(import.meta.url));
const apiDirectory = resolve(supportDirectory, "../../api");
const webDirectory = resolve(supportDirectory, "..");
const projectID = "sumi-studio";
const wrappingKeyID = "e2e-auth-email/v1";

class Stack {
  readonly children: ChildProcess[] = [];
  apiLog = "";
  constructor(
    readonly apiURL: string,
    readonly webURL: string,
    readonly runtime: string,
    readonly binaries: string,
    readonly databaseURL: string,
    readonly emulatorHost: string,
  ) {}

  /** Issues an enrollment invite for the email and returns its token. */
  seedInvite(email: string): string {
    const result = spawnSync(
      join(this.binaries, "e2e-registration-seed"),
      ["invite"],
      {
        env: {
          ...process.env,
          SUMI_E2E_REG_SEED_DATABASE_URL: this.databaseURL,
          SUMI_E2E_REG_SEED_EMAIL: email,
        },
        encoding: "utf8",
      },
    );
    if (result.status !== 0) throw new Error(`seed invite: ${result.stderr}`);
    return result.stdout.trim();
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
    throw new Error(`${name} is required for the no-sender browser journey`);
  return value;
}

async function startStack(): Promise<Stack> {
  const databaseURL = required("SUMI_UI_E2E_DB_URL");
  const emulatorHost = required("FIREBASE_AUTH_EMULATOR_HOST");
  const apiPort = required("SUMI_UI_E2E_API_PORT");
  const webPort = required("SUMI_UI_E2E_WEB_PORT");
  const runtime = mkdtempSync(join(tmpdir(), "sumi-ui-e2e-"));
  let binaries = process.env.SUMI_UI_E2E_BUILD_DIR?.trim() ?? "";
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
        "./cmd/e2e-registration-seed",
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
    runtime,
    binaries,
    databaseURL,
    emulatorHost,
  );
  try {
    // Deliberately no SUMI_AUTH_EMAIL_* — the deployment offers no email
    // sign-in, and the UI must never pretend otherwise.
    const api = spawn(join(binaries, "server"), [], {
      cwd: apiDirectory,
      env: {
        ...process.env,
        PORT: apiPort,
        SUMI_PUBLIC_LOOPBACK_LISTEN: `127.0.0.1:${apiPort}`,
        SUMI_DB_URL: databaseURL,
        SUMI_AGENT_RUNTIME_STATE_DIR: join(runtime, "gateway"),
        SUMI_COMMAND_LOG_DIR: join(runtime, "commands"),
        SUMI_BROWSER_SESSION_SECRET: Buffer.from(
          crypto.getRandomValues(new Uint8Array(48)),
        ).toString("base64"),
        SUMI_BROWSER_SESSION_AUDIENCE: "sumi:web",
        SUMI_BROWSER_WS_ALLOWED_ORIGINS: webURL,
        SUMI_AUTH_ALLOW_INSECURE_COOKIES: "true",
        SUMI_AUTH_FIREBASE_PROJECT_ID: projectID,
        SUMI_AUTH_TENANT_ID: "e2e-ui",
        SUMI_AGENT_WRAPPING_KEY_ID: wrappingKeyID,
        FIREBASE_AUTH_EMULATOR_HOST: emulatorHost,
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
    vite.stderr.on("data", (chunk) =>
      process.stderr.write(`[ui-e2e-web] ${chunk}`),
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

/** The emulator's fake IdP widget: create a fake account, return to the app. */
async function emulatorSignIn(page: Page, email: string) {
  await page.waitForURL(/emulator\/auth\/handler/, { timeout: 20_000 });
  await page.locator("#add-account-button").click();
  await page.locator("#email-input").fill(email);
  await page.locator("#display-name-input").fill("E2E Persona");
  await page.locator("#sign-in").click();
  await page.waitForURL(`${stack.webURL}/**`, { timeout: 30_000 });
}

test.beforeAll(async () => {
  const testInfo = test.info();
  testInfo.setTimeout(420_000);
  screenshots =
    process.env.SUMI_UI_E2E_SCREENSHOT_DIR?.trim() ||
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

test("the no-sender deployment never renders an email submit and keeps OAuth usable", async ({
  browser,
}) => {
  const { context, page } = await freshPage(browser);
  await page.goto(`${stack.webURL}/`);

  // The server capability read drives this: /auth/methods answered without
  // email_code, so the email form and divider never mount.
  await expect(
    page.getByText(/メールでのログイン・新規登録は現在利用できません/),
  ).toBeVisible({ timeout: 20_000 });
  await expect(page.getByPlaceholder("メールアドレス")).toHaveCount(0);
  await expect(
    page.getByRole("button", { name: "メールでログイン" }),
  ).toHaveCount(0);
  await shot(page, "01-no-email-signin");

  await expect(
    page.getByRole("button", { name: "Googleで続ける" }),
  ).toBeEnabled();
  await expect(
    page.getByRole("button", { name: "GitHubで続ける" }),
  ).toBeEnabled();
  await context.close();
});

test("a failed capability read is explicit, retryable, and never blocks OAuth", async ({
  browser,
}) => {
  const { context, page } = await freshPage(browser);
  // Explicit failure case: the capability read cannot complete at all.
  await page.route("**/auth/methods", (route) => route.abort("failed"));
  await page.goto(`${stack.webURL}/`);

  await expect(
    page.getByText(/メールでのログインが利用できるか確認できませんでした/),
  ).toBeVisible({ timeout: 20_000 });
  await expect(page.getByPlaceholder("メールアドレス")).toHaveCount(0);
  await expect(
    page.getByRole("button", { name: "Googleで続ける" }),
  ).toBeEnabled();
  await shot(page, "02-capability-failed");

  // Retry with the read succeeding — the real answer here is email-off.
  await page.unroute("**/auth/methods");
  await page.getByRole("button", { name: "再試行" }).click();
  await expect(
    page.getByText(/メールでのログイン・新規登録は現在利用できません/),
  ).toBeVisible({ timeout: 20_000 });
  await expect(page.getByPlaceholder("メールアドレス")).toHaveCount(0);
  await shot(page, "03-capability-retried");
  await context.close();
});

test("invited OAuth sign-up reaches the secretary choice without any email step", async ({
  browser,
}) => {
  const email = `e2e-oauth-${Date.now()}@example.test`;
  const invite = stack.seedInvite(email);
  const { context, page } = await freshPage(browser);
  await page.goto(`${stack.webURL}/#invite=${invite}`);

  await expect(
    page.getByText(/メールでのログイン・新規登録は現在利用できません/),
  ).toBeVisible({ timeout: 20_000 });
  await expect(page.getByPlaceholder("メールアドレス")).toHaveCount(0);
  await shot(page, "04-invite-no-email");

  await page.getByRole("button", { name: "Googleで続ける" }).click();
  await emulatorSignIn(page, email);

  // The new-vs-carried-secretary choice renders; no email step intervened.
  await expect(
    page.getByRole("button", { name: "新しい秘書で登録する" }),
  ).toBeVisible({ timeout: 30_000 });
  await expect(
    page.getByRole("button", { name: "Localの秘書を引き継ぐ" }),
  ).toBeVisible();
  await shot(page, "05-secretary-choice");

  await page.getByRole("button", { name: "新しい秘書で登録する" }).click();
  await expect
    .poll(() => sessionUser(page), { timeout: 30_000 })
    .not.toBeNull();
  await expect(page.locator("#login-title")).toBeHidden({ timeout: 30_000 });
  await shot(page, "06-signed-in");
  await context.close();
});

test("a saved code step on a now-disabled deployment is blocked with a working exit", async ({
  browser,
}) => {
  const { context, page } = await freshPage(browser);
  await page.goto(`${stack.webURL}/`);
  // Simulate a code step saved while email was still offered.
  await page.evaluate(() => {
    const state = "A".repeat(24);
    localStorage.setItem("sumi.auth.email-flow-active.v1", state);
    localStorage.setItem(
      `sumi.auth.email-flow.v1.${state}`,
      JSON.stringify({
        flowId: "00000000-0000-7000-8000-000000000000",
        nonce: "n".repeat(43),
        intent: "sign_in",
        provider: "email_code",
        email: "stale@example.test",
        stage: "code_sent",
        expiresAt: new Date(Date.now() + 30 * 60_000).toISOString(),
      }),
    );
  });
  await page.reload();

  await expect(page.getByText(/このコードは確認できません/)).toBeVisible({
    timeout: 20_000,
  });
  // The actual controls — input, submit, resend — never mount. (getByLabel
  // would also substring-match the section heading "確認コードを入力".)
  await expect(page.locator("#sumi-auth-code")).toHaveCount(0);
  await expect(
    page.getByRole("button", { name: "確認して続ける" }),
  ).toHaveCount(0);
  await expect(
    page.getByRole("button", { name: /コードを再送信/ }),
  ).toHaveCount(0);
  // Nothing claims a code was sent.
  await expect(page.getByText(/確認コードを送信しました/)).toHaveCount(0);
  await shot(page, "07-stale-code-blocked");

  await page.getByRole("button", { name: "別の方法でログイン" }).click();
  await expect(
    page.getByText(/メールでのログイン・新規登録は現在利用できません/),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Googleで続ける" }),
  ).toBeEnabled();
  await context.close();
});

test("a pending email link on a disabled deployment gets a deliberate exit", async ({
  browser,
}) => {
  const { context, page } = await freshPage(browser);
  await page.goto(`${stack.webURL}/`);
  await page.evaluate(() => {
    sessionStorage.setItem(
      "sumi.auth.email-link.v1",
      JSON.stringify({
        challengeId: "00000000-0000-4000-8000-000000000000",
        token: "t".repeat(43),
      }),
    );
  });
  await page.reload();

  // The link can never resolve here — the panel says so and offers a close.
  await expect(page.getByText(/このリンクでは続けられません/)).toBeVisible({
    timeout: 20_000,
  });
  await shot(page, "08-stale-link");
  await page.getByRole("button", { name: "閉じる" }).click();
  await expect(
    page.getByText(/メールでのログイン・新規登録は現在利用できません/),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Googleで続ける" }),
  ).toBeEnabled();
  await context.close();
});
