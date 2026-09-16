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

  seedAccount(label: string): {
    humanID: string;
    email: string;
    firebaseUID: string;
  } {
    const suffix = randomBytes(4).toString("hex");
    const email = `${label}-${suffix}@example.test`;
    const firebaseUID = `e2e-${label}-${suffix}`;
    const result = spawnSync(join(this.binaries, "e2e-auth-email-seed"), [], {
      env: {
        ...process.env,
        FIREBASE_AUTH_EMULATOR_HOST: this.emulatorHost,
        SUMI_E2E_AUTH_SEED_DATABASE_URL: this.databaseURL,
        SUMI_E2E_AUTH_SEED_WRAPPING_KEY_ID: wrappingKeyID,
        SUMI_E2E_AUTH_SEED_PROJECT_ID: projectID,
        SUMI_E2E_AUTH_SEED_FIREBASE_UID: firebaseUID,
        SUMI_E2E_AUTH_SEED_EMAIL: email,
      },
      encoding: "utf8",
    });
    if (result.status !== 0) throw new Error(`seed: ${result.stderr}`);
    return { humanID: result.stdout.trim(), email, firebaseUID };
  }

  /** Runs one SQL statement and returns its unaligned, tuples-only output. */
  sql(statement: string): string {
    const result = spawnSync(
      "psql",
      [this.databaseURL, "-v", "ON_ERROR_STOP=1", "-At", "-c", statement],
      { encoding: "utf8" },
    );
    if (result.status !== 0) throw new Error(`sql: ${result.stderr}`);
    return result.stdout.trim();
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

  /** Expires this account's pending provider operation. */
  expireProviderOperation(firebaseUID: string) {
    this.sql(
      `UPDATE provider_operations SET expires_at = now() - interval '1 second' WHERE status = 'pending' AND firebase_uid = '${firebaseUID}'`,
    );
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

/** The UID on the live Firebase auth object, or null. */
async function firebaseLiveUid(page: Page): Promise<string | null> {
  return page.evaluate(async () => {
    const mod = (await import("/src/auth/firebase.ts")) as {
      getFirebaseAuth: () => { currentUser: { uid: string } | null };
    };
    return mod.getFirebaseAuth().currentUser?.uid ?? null;
  });
}

/** The UID Firebase persists in IndexedDB for this origin, or null. */
async function firebasePersistedUid(page: Page): Promise<string | null> {
  return page.evaluate(async () => {
    const db = await new Promise<IDBDatabase | null>((resolve) => {
      const request = indexedDB.open("firebaseLocalStorageDb");
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => resolve(null);
    });
    if (!db?.objectStoreNames.contains("firebaseLocalStorage")) {
      return null;
    }
    const store = db
      .transaction("firebaseLocalStorage", "readonly")
      .objectStore("firebaseLocalStorage");
    // Both requests must be queued while the transaction is still live; an
    // awaited get() issued after getAllKeys resolves would hit an inactive
    // transaction and fail closed.
    const keysRequest = store.getAllKeys();
    const valuesRequest = store.getAll();
    const [keys, values] = await Promise.all([
      new Promise<unknown>((resolve) => {
        keysRequest.onsuccess = () => resolve(keysRequest.result);
        keysRequest.onerror = () => resolve(null);
      }),
      new Promise<unknown>((resolve) => {
        valuesRequest.onsuccess = () => resolve(valuesRequest.result);
        valuesRequest.onerror = () => resolve(null);
      }),
    ]);
    if (!Array.isArray(keys) || !Array.isArray(values)) return null;
    for (let index = 0; index < keys.length; index += 1) {
      const key = keys[index];
      if (typeof key !== "string" || !key.startsWith("firebase:authUser")) {
        continue;
      }
      // The store's keyPath is fbase_key; records are {fbase_key, value}
      // where value is the serialized user JSON.
      const record = values[index] as
        | { value?: unknown; uid?: string }
        | null;
      const raw = record && "value" in record ? record.value : record;
      const user =
        typeof raw === "string"
          ? (JSON.parse(raw) as { uid?: string })
          : (raw as { uid?: string } | null);
      return user?.uid ?? null;
    }
    return null;
  });
}

/**
 * Holds the reads a settling unlink makes once armed — the Firebase lookup
 * and the authoritative provider-method read — while "解除を確定中" shows. The
 * real response is fetched up front so the delayed delivery still succeeds
 * after a sign-out tears the session down.
 */
async function holdSettlingReads(page: Page) {
  let release: () => void = () => {};
  const held = new Promise<void>((done) => {
    release = done;
  });
  let armed = false;
  let seen = false;
  const handler = async (route: Parameters<Parameters<Page["route"]>[1]>[0]) => {
    const settling =
      armed &&
      (await page
        .evaluate(
          () => document.body.textContent?.includes("解除を確定中") ?? false,
        )
        .catch(() => false));
    if (!settling) {
      await route.continue();
      return;
    }
    seen = true;
    const response = await route.fetch().catch(() => null);
    await held;
    if (response) await route.fulfill({ response }).catch(() => undefined);
    else await route.abort().catch(() => undefined);
  };
  await page.route("**/accounts:lookup**", handler);
  await page.route("**/auth/providers", handler);
  return {
    arm: () => {
      armed = true;
    },
    seen: () => seen,
    release: () => release(),
  };
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
  stack.expireProviderOperation(account.firebaseUID);
  await page.locator("#add-account-button").click();
  await page.locator("#email-input").fill(`expired-${account.email}`);
  await page.locator("#sign-in").click();
  await page.waitForURL(`${stack.webURL}/**`, { timeout: 30_000 });

  // The link reached Firebase but the operation expired: settings must name
  // the expiry rather than pretend success or leave a raw error code.
  await expect(
    page.getByRole("alert").filter({
      hasText: "変更の有効期限が切れました。もう一度お試しください。",
    }),
  ).toBeVisible({ timeout: 45_000 });
  await shot(page, "06-expired");
  await context.close();
});

test("a logout during the post-unlink lookup is honoured, not overwritten", async ({
  browser,
}) => {
  const account = stack.seedAccount("logout-race");
  const { context, page } = await freshPage(browser);
  await signInWithEmailCode(page, stack, account.email, account.humanID);
  await openSettings(page);

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

  // Hold the read that settles the unlink, then log out: the transition must
  // win over the late completion.
  const settle = await holdSettlingReads(page);
  await page.getByRole("button", { name: "GitHubの解除を開始" }).click();
  await page.getByRole("button", { name: "再認証して解除" }).click();
  settle.arm();
  await emulatorReuseProviderAccount(page, googlePersona);

  // Wait until the settling unlink is actually inside the held read, then log
  // out with an ordinary click: nothing in the busy settings popover may
  // cover the button.
  await expect.poll(() => settle.seen(), { timeout: 30_000 }).toBe(true);
  const logout = page.getByRole("button", { name: "ログアウト" });
  if (!(await logout.isVisible().catch(() => false))) {
    await page.getByRole("button", { name: "設定" }).click();
  }
  await shot(page, "07a-logout-while-settling");
  await logout.click();
  await expect(page.locator("#login-title")).toBeVisible({
    timeout: 30_000,
  });
  settle.release();
  await expect.poll(() => sessionUser(page), { timeout: 15_000 }).toBeNull();

  // The old Firebase identity must not be reinstalled by the late
  // completion — neither live nor persisted, not now and not after a reload.
  await expect
    .poll(() => firebaseLiveUid(page), { timeout: 15_000 })
    .toBeNull();
  await expect
    .poll(() => firebasePersistedUid(page), { timeout: 15_000 })
    .toBeNull();
  await page.reload();
  await expect.poll(() => sessionUser(page), { timeout: 15_000 }).toBeNull();
  await expect
    .poll(() => firebaseLiveUid(page), { timeout: 15_000 })
    .toBeNull();
  await expect
    .poll(() => firebasePersistedUid(page), { timeout: 15_000 })
    .toBeNull();
  await shot(page, "07-logout-race");
  await context.close();
});

test("an unfinished link survives another account taking over the browser", async ({
  browser,
}) => {
  const first = stack.seedAccount("first");
  const second = stack.seedAccount("second");
  const { context, page } = await freshPage(browser);
  await signInWithEmailCode(page, stack, first.email, first.humanID);
  await openSettings(page);
  await page.getByRole("button", { name: "Googleを追加" }).click();
  await page.waitForURL(/emulator\/auth\/handler/, { timeout: 20_000 });
  // The first tab is now at the provider; its unfinished link is durable.

  // A second tab on the same browser signs out and becomes another account.
  const takeover = await context.newPage();
  await takeover.goto(`${stack.webURL}/`);
  await openSettings(takeover);
  await takeover.getByRole("button", { name: "ログアウト" }).click();
  await expect(takeover.locator("#login-title")).toBeVisible({
    timeout: 30_000,
  });
  await signInWithEmailCode(takeover, stack, second.email, second.humanID);
  await openSettings(takeover);
  // The second account's own change must not see or destroy the first's.
  await takeover.getByRole("button", { name: "GitHubを追加" }).click();
  await emulatorLinkProvider(takeover, `github-${second.email}`);
  await expect(
    takeover.getByRole("status").filter({ hasText: "GitHubを追加しました" }),
  ).toBeVisible({ timeout: 45_000 });

  const firstRecord = await takeover.evaluate(() => {
    for (let index = 0; index < localStorage.length; index += 1) {
      const key = localStorage.key(index);
      if (!key?.startsWith("sumi.auth.provider-pending.v2/")) continue;
      const value = JSON.parse(localStorage.getItem(key) ?? "null") as {
        humanId?: string;
        provider?: string;
      } | null;
      if (value?.provider === "google.com") return value;
    }
    return null;
  });
  expect(firstRecord?.humanId).toBe(first.humanID);

  // When the first account signs back in, its own pending change resumes
  // and completes normally — the other account's work never interfered.
  await takeover.getByRole("button", { name: "ログアウト" }).click();
  await expect(takeover.locator("#login-title")).toBeVisible({
    timeout: 30_000,
  });
  await signInWithEmailCode(takeover, stack, first.email, first.humanID);
  await openSettings(takeover);
  await expect(
    takeover.getByText("Googleで認証を続けてください"),
  ).toBeVisible();
  await takeover.getByRole("button", { name: "Googleで認証を続ける" }).click();
  await emulatorLinkProvider(takeover, `google-${first.email}`);
  await expect(
    takeover.getByRole("status").filter({ hasText: "Googleを追加しました" }),
  ).toBeVisible({ timeout: 45_000 });
  await shot(takeover, "08-cross-account-pending");
  await context.close();
});

test("a cancelled unlink reauthentication abandons the unsent intent", async ({
  browser,
}) => {
  const account = stack.seedAccount("cancel-reauth");
  const { context, page } = await freshPage(browser);
  await signInWithEmailCode(page, stack, account.email, account.humanID);
  await openSettings(page);

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
  await page.waitForURL(/emulator\/auth\/handler/, { timeout: 20_000 });
  // Backing out before authenticating cancels the reauth: no unlink request
  // ever reached the server, so the intent is abandoned — not left durable.
  await page.goBack();
  await expect(
    page.getByRole("alert").filter({ hasText: "認証をキャンセルしました。" }),
  ).toBeVisible({ timeout: 45_000 });

  // A restart must not resurrect the unsent intent or block other changes.
  await page.reload();
  await openSettings(page);
  await expect(page.getByText(/再開できます/)).toBeHidden();
  await expect(
    page.getByRole("button", { name: "GitHubの解除を開始" }),
  ).toBeEnabled();
  await expect(
    page.getByRole("button", { name: "Googleの解除を開始" }),
  ).toBeEnabled();
  await shot(page, "09-cancelled-reauth");
  await context.close();
});

test("a lost start reply shows recovery copy, not a raw network error", async ({
  browser,
}) => {
  const account = stack.seedAccount("lost-reply");
  const { context, page } = await freshPage(browser);
  await signInWithEmailCode(page, stack, account.email, account.humanID);
  await openSettings(page);

  // The operation-start reply never arrives; the request may have committed.
  await page.route("**/auth/providers/operations", (route) => route.abort());
  await page.getByRole("button", { name: "Googleを追加" }).click();
  await expect(
    page.getByRole("alert").filter({
      hasText:
        "結果をまだ確認できません。接続を確認して「再開」を押してください。",
    }),
  ).toBeVisible({ timeout: 45_000 });
  await shot(page, "10-lost-reply");
  await context.close();
});

/** Links Google then GitHub from open settings; returns the Google persona. */
async function linkGoogleAndGitHub(page: Page, email: string): Promise<string> {
  const googlePersona = `google-${email}`;
  await page.getByRole("button", { name: "Googleを追加" }).click();
  await emulatorLinkProvider(page, googlePersona);
  await expect(
    page.getByRole("status").filter({ hasText: "Googleを追加しました" }),
  ).toBeVisible({ timeout: 45_000 });
  await page.getByRole("button", { name: "GitHubを追加" }).click();
  await emulatorLinkProvider(page, `github-${email}`);
  await expect(
    page.getByRole("status").filter({ hasText: "GitHubを追加しました" }),
  ).toBeVisible({ timeout: 45_000 });
  return googlePersona;
}

test("a logout in another tab signs this tab out, even while an unlink is settling", async ({
  browser,
}) => {
  const account = stack.seedAccount("cross-tab-logout");
  const { context, page } = await freshPage(browser);
  await signInWithEmailCode(page, stack, account.email, account.humanID);
  await openSettings(page);
  const googlePersona = await linkGoogleAndGitHub(page, account.email);

  const settle = await holdSettlingReads(page);
  await page.getByRole("button", { name: "GitHubの解除を開始" }).click();
  await page.getByRole("button", { name: "再認証して解除" }).click();
  settle.arm();
  await emulatorReuseProviderAccount(page, googlePersona);
  await expect.poll(() => settle.seen(), { timeout: 30_000 }).toBe(true);

  // Tab B on the same browser logs out explicitly. Tab A is left untouched.
  const tabB = await context.newPage();
  await tabB.goto(`${stack.webURL}/`);
  await openSettings(tabB);
  await tabB.getByRole("button", { name: "ログアウト" }).click();
  await expect(tabB.locator("#login-title")).toBeVisible({ timeout: 30_000 });

  // Tab A follows the explicit logout on its own, without a reload.
  await expect(page.locator("#login-title")).toBeVisible({ timeout: 15_000 });
  settle.release();
  // Its late completion cannot keep or reinstall the old identity.
  await expect
    .poll(() => firebaseLiveUid(page), { timeout: 15_000 })
    .toBeNull();
  for (let second = 0; second < 5; second += 1) {
    expect(await firebasePersistedUid(page)).toBeNull();
    expect(await firebaseLiveUid(page)).toBeNull();
    await page.waitForTimeout(1_000);
  }
  await expect(page.locator("#login-title")).toBeVisible();
  await shot(page, "11-cross-tab-logout");

  // A deliberate sign-in afterwards, in either tab, is not undone by the
  // earlier logout.
  await signInWithEmailCode(tabB, stack, account.email, account.humanID);
  for (let second = 0; second < 5; second += 1) {
    expect(await firebasePersistedUid(tabB)).toBe(account.firebaseUID);
    await tabB.waitForTimeout(1_000);
  }
  await tabB.reload();
  await expectSignedIn(tabB, account.humanID);
  // currentUser restores asynchronously from the persisted session.
  await expect
    .poll(() => firebaseLiveUid(tabB), { timeout: 15_000 })
    .toBe(account.firebaseUID);
  await context.close();
});

test("another browser's provider removal shows on the next settings open and the method can be added again", async ({
  browser,
}) => {
  const account = stack.seedAccount("other-browser");
  const first = await freshPage(browser);
  await signInWithEmailCode(first.page, stack, account.email, account.humanID);
  await openSettings(first.page);
  const googlePersona = await linkGoogleAndGitHub(first.page, account.email);

  // A second browser signs in while both providers are linked.
  const second = await freshPage(browser);
  await signInWithEmailCode(second.page, stack, account.email, account.humanID);
  await openSettings(second.page);
  await expect(
    second.page.getByRole("button", { name: "GitHubの解除を開始" }),
  ).toBeVisible({ timeout: 15_000 });
  await second.page.keyboard.press("Escape");

  // The first browser removes GitHub.
  await first.page.getByRole("button", { name: "GitHubの解除を開始" }).click();
  await first.page.getByRole("button", { name: "再認証して解除" }).click();
  await emulatorReuseProviderAccount(first.page, googlePersona);
  await expect(
    first.page.getByRole("status").filter({ hasText: "GitHubを解除しました" }),
  ).toBeVisible({ timeout: 45_000 });

  // Reopening settings in the second browser shows the method as removed,
  // with no fresh sign-in.
  await openSettings(second.page);
  await expect(
    second.page.getByRole("button", { name: "GitHubを追加" }),
  ).toBeVisible({ timeout: 15_000 });
  await expect(
    second.page.getByRole("button", { name: "GitHubの解除を開始" }),
  ).toBeHidden();
  await shot(second.page, "12-other-browser-removal");

  // Its cached Firebase user must not block adding the method again.
  await second.page.getByRole("button", { name: "GitHubを追加" }).click();
  await emulatorLinkProvider(second.page, `github-again-${account.email}`);
  await expect(
    second.page.getByRole("status").filter({ hasText: "GitHubを追加しました" }),
  ).toBeVisible({ timeout: 45_000 });
  await first.context.close();
  await second.context.close();
});

test("an unlink whose browser record was lost no longer blocks provider changes", async ({
  browser,
}) => {
  const account = stack.seedAccount("orphan-unlink");
  // The server still holds a pending unlink, but its nonce lived only in a
  // browser whose site data was cleared; it expired long ago.
  const nonceHash = randomBytes(32).toString("hex");
  const operationID = stack.sql(
    "SELECT overlay(gen_random_uuid()::text placing '7' from 15 for 1)",
  );
  stack.sql(
    `INSERT INTO provider_operations
       (operation_id, nonce_hash, human_id, firebase_uid, provider, operation, decision_path, expires_at)
     VALUES ('${operationID}', decode('${nonceHash}', 'hex'), '${account.humanID}',
       '${account.firebaseUID}', 'github.com', 'unlink', 'account_settings', now() - interval '1 hour')`,
  );

  const { context, page } = await freshPage(browser);
  await signInWithEmailCode(page, stack, account.email, account.humanID);
  await openSettings(page);
  await page.getByRole("button", { name: "Googleを追加" }).click();
  await emulatorLinkProvider(page, `google-${account.email}`);
  await expect(
    page.getByRole("status").filter({ hasText: "Googleを追加しました" }),
  ).toBeVisible({ timeout: 45_000 });
  await shot(page, "13-orphan-unlink-recovered");
  // The uncertain unlink was reconciled from the live account, not dropped.
  expect(
    stack.sql(
      `SELECT status || '/' || terminal_outcome FROM provider_operations WHERE operation_id = '${operationID}'`,
    ),
  ).toBe("failed/expired");
  await context.close();
});
