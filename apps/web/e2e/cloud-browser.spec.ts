import { appendFileSync, mkdirSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { type Browser, type BrowserContext, expect, type Page, test } from "@playwright/test";
import { FIXTURE_KEY, fillDefaults, choice, startJevFixture } from "../../desktop/test/jev-fixture.mjs";
import { CloudBrowserStack, PORTS, startCloudBrowserStack } from "./support/cloud-browser-stack";
import { buildWorkspaceBrowserStack, removeWorkspaceBrowserBuild, type WorkspaceBrowserBuild } from "./support/real-agent-stack";

/**
 * The secretary's Cloud browser, end to end on the product path: the person
 * in Web Sumi (/browser) and the secretary (real Core tools) on the same
 * remote tab. Opt-in:
 *
 *   CLOUD_BROWSER_E2E_DB_URL      disposable empty Postgres database
 *   CLOUD_BROWSER_E2E_ARTIFACTS   absolute directory for receipts/screenshots
 *   CLOUD_BROWSER_E2E_MODE        local (default) | cloud
 *   CLOUD_BROWSER_E2E_WORKER_URL  cloud: isolated test Worker origin
 *   CLOUD_BROWSER_E2E_TOKEN_FILE  cloud: test runtime token file
 *   CLOUD_BROWSER_E2E_JEV_KEY_FILE optional: a real Jev key for one synthetic
 *                                 goal (read, never printed or stored here)
 */

const databaseURL = process.env.CLOUD_BROWSER_E2E_DB_URL ?? "";
const artifacts = resolve(process.env.CLOUD_BROWSER_E2E_ARTIFACTS ?? "/tmp/sumi-cloud-browser-e2e");
const mode = process.env.CLOUD_BROWSER_E2E_MODE === "cloud" ? "cloud" : "local";
const APP = "http://app.sumi-fixture.test";
const DOCS = "http://docs.sumi-fixture.test";

test.describe.configure({ mode: "serial", timeout: 900_000 });
test.skip(!databaseURL, "set CLOUD_BROWSER_E2E_DB_URL to run the Cloud browser journey");

type Json = Record<string, any>;
type Target = { id: string; name: string; role: string; tag: string; bounds: { x: number; y: number; width: number; height: number } };

let build: WorkspaceBrowserBuild;
let stack: CloudBrowserStack;
let jev: Awaited<ReturnType<typeof startJevFixture>> | undefined;
let context: BrowserContext;
let page: Page;
let pagerGoal = 30;

const receipts = join(artifacts, "receipts.jsonl");
/** Records evidence with tokens, tickets and keys removed. */
function receipt(step: string, value: unknown) {
  const text = JSON.stringify({ at: new Date().toISOString(), step, value }, (key, v) =>
    /token|ticket|api_key|authorization|cookie|jwt/i.test(key) ? "[redacted]" : v,
  );
  appendFileSync(receipts, `${text}\n`);
}

async function shot(name: string) {
  await page.screenshot({ path: join(artifacts, `${name}.png`) });
}

const frame = () => page.getByTestId("cloud-browser-frame");
const controlPill = () => page.getByTestId("cloud-browser-control");

async function clickRemote(bounds: Target["bounds"]) {
  const box = await frame().boundingBox();
  if (!box) throw new Error("no frame");
  const x = box.x + ((bounds.x + bounds.width / 2) * box.width) / 1280;
  const y = box.y + ((bounds.y + bounds.height / 2) * box.height) / 800;
  await page.mouse.click(x, y);
}

async function addressBar(url: string) {
  const bar = page.getByLabel("アドレス");
  await bar.click();
  await bar.fill(url);
  await bar.press("Enter");
}

async function activeTabTitle(pattern: RegExp) {
  await expect(page.getByRole("tab", { selected: true })).toContainText(pattern, { timeout: 30_000 });
}

async function handBack() {
  const button = page.getByRole("button", { name: "秘書に戻す" });
  if (await button.isVisible()) await button.click();
  await expect(controlPill()).toContainText("秘書が操作できます");
}

// ---------- secretary helpers (real Core tools) ----------

async function tabs(): Promise<Json[]> {
  const value = await stack.secretary!.call<Json>("browser.tabs", {});
  return value.tabs ?? [];
}

async function sharedTab(name: RegExp, want: (t: Json) => boolean = (t) => t.available): Promise<Json> {
  for (let i = 0; i < 60; i++) {
    const found = (await tabs()).find((t) => name.test(String(t.name)) && want(t));
    if (found) return found;
    await page.waitForTimeout(500);
  }
  throw new Error(`secretary never saw ${name}`);
}

async function observe(attachment: string): Promise<{ job: Json; value: Json }> {
  const result = await stack.secretary!.call<Json>("browser.observe", { attachment_id: attachment }, true);
  if (result.job?.status !== "done") throw new Error(`observe did not complete: ${result.job?.status} ${JSON.stringify(result.job?.result ?? result).slice(0, 800)}`);
  return { job: result.job, value: result.job.result.value };
}

async function act(attachment: string, binding: Json, action: Json, guard = false): Promise<Json> {
  const result = await stack.secretary!.call<Json>(
    "browser.act",
    { attachment_id: attachment, binding, action, ...(guard ? { guard: true } : {}) },
    true,
  );
  return result.job;
}

const target = (observation: Json, name: RegExp): Target => {
  const t = (observation.targets as Target[]).find((x) => name.test(x.name));
  if (!t) throw new Error(`no target ${name} in ${JSON.stringify((observation.targets as Target[]).map((x) => x.name))}`);
  return t;
};

// ---------- setup ----------

test.beforeAll(async ({ browser }: { browser: Browser }) => {
  mkdirSync(artifacts, { recursive: true, mode: 0o700 });
  writeFileSync(receipts, "");
  build = await buildWorkspaceBrowserStack();
  if (mode === "local") {
    jev = await startJevFixture({
      port: PORTS.jev,
      // Rule-based stand-in: press the control named Next until the goal page
      // is visible. The real Jev decides for itself (see the live goal).
      policy: (body: Json) => {
        const q = body.questions;
        const ops = Object.keys(q.operation.criteria);
        const text = String(body.state.page.text);
        const answers: Json = {};
        const next = q.click_target ? Object.entries(q.click_target.criteria).find(([, d]) => /next/i.test(String(d))) : undefined;
        let op = "BLOCKED";
        if (text.includes(`Page ${pagerGoal}`)) op = "DONE";
        else if (next && ops.includes("CLICK")) {
          op = "CLICK";
          answers.click_target = choice(Object.keys(q.click_target.criteria), next[0]);
        }
        answers.operation = choice(ops, ops.includes(op) ? op : ops[0]);
        return fillDefaults(q, answers);
      },
    });
  }
  stack = await startCloudBrowserStack(build, {
    mode,
    databaseURL,
    artifacts,
    workerURL: process.env.CLOUD_BROWSER_E2E_WORKER_URL,
    tokenFile: process.env.CLOUD_BROWSER_E2E_TOKEN_FILE,
    jevEndpoint: jev ? `http://127.0.0.1:${PORTS.jev}/` : undefined,
  });
  context = await browser.newContext({ viewport: { width: 1440, height: 1000 }, locale: "ja-JP" });
  await stack.installSession(context);
  page = await context.newPage();
  page.setDefaultTimeout(30_000);
  page.on("console", (m) => {
    if (m.type() === "error") stack.note(`[page] ${m.text()}\n`);
  });
});

test.afterAll(async () => {
  writeFileSync(join(artifacts, "stack.log"), redactLog(stack?.logs() ?? ""));
  await context?.close();
  await stack?.stop();
  jev?.close();
  if (build) await removeWorkspaceBrowserBuild(build);
});

function redactLog(text: string): string {
  return text.replace(/sbt1\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g, "sbt1.[redacted]").replace(/Bearer [A-Za-z0-9._-]+/g, "Bearer [redacted]");
}

// ---------- journey ----------

let appAttachment = "";
let appTab = "";

test("the person opens Web Sumi, prepares the secretary's browser and sees its screen", async () => {
  await page.goto(`${stack.webURL}/browser`);
  await expect(page.getByRole("heading", { name: /ブラウザを用意します/ })).toBeVisible({ timeout: 30_000 });
  await shot("01-prepare");
  await page.getByRole("button", { name: "ブラウザを用意する" }).click();
  await expect(frame()).toBeVisible({ timeout: 60_000 });
  await expect(page.getByTestId("cloud-browser-status")).toHaveCount(0, { timeout: 60_000 });
  const size = await frame().evaluate((img: HTMLImageElement) => ({ w: img.naturalWidth, h: img.naturalHeight }));
  receipt("entry", { frame: size });
  expect(size).toEqual({ w: 1280, h: 800 });
  // The rail offers the app once the API reports it configured.
  await expect(page.getByRole("button", { name: "秘書のブラウザ" })).toBeVisible();
  await shot("02-live-blank");
});

test("the person browses, signs in manually on the shared screen and shares the tab", async () => {
  await addressBar(`${APP}/`);
  await activeTabTitle(/sign in/);
  await page.getByRole("button", { name: "秘書と共有" }).click();
  await page.getByRole("button", { name: "共有する" }).click();
  await expect(page.getByRole("button", { name: "共有中" })).toBeVisible({ timeout: 15_000 });
  await page.keyboard.press("Escape");
  const tab = await sharedTab(/sign in/);
  appAttachment = tab.attachment_id;
  appTab = tab.tab.tabId;
  receipt("shared-tab", { name: tab.name, available: tab.available, layers: tab.operation_layers, profileId: tab.tab.profileId });
  expect(tab.tab.profileId).toBe("cloud");
  // Direct browser tools need no Jev key.
  expect(tab.operation_layers).toEqual({ direct: true, jev: "not_configured" });

  const { value } = await observe(appAttachment);
  // The fixture's fields are wrapped by their labels; find the password
  // field by its input type.
  const password = (value.targets as (Target & { type?: string })[]).find((t) => t.type === "password");
  if (!password) throw new Error("no password field");
  const signIn = target(value, /sign in/i);
  // The person types the password on the shared screen (a synthetic one on
  // the synthetic fixture) and presses Sign in.
  await clickRemote(password.bounds);
  await expect(controlPill()).toContainText("あなたが操作中");
  await page.keyboard.type("synthetic-pass");
  await clickRemote(signIn.bounds);
  await activeTabTitle(/dashboard/);
  await shot("03-person-signed-in");
});

let dashboardBinding: Json = {};

test("direct path without Jev: the secretary acts on the tab the person sees (same viewport)", async () => {
  await handBack();
  const first = await observe(appAttachment);
  receipt("observation-text", String(first.value.text).slice(0, 600));
  expect(first.value.text).toMatch(/Signed in as\s*Synthetic Ada/);
  const count = target(first.value, /Count \+1/);
  // Frame-vs-agent: the person's frame is the same 1280x800 viewport the
  // secretary's target bounds are measured in; crop the person's frame at
  // the secretary's bounds.
  const box = await frame().boundingBox();
  if (!box) throw new Error("no frame");
  const sx = box.width / 1280;
  const sy = box.height / 800;
  await page.screenshot({
    path: join(artifacts, "04-agent-target-in-person-frame.png"),
    clip: { x: box.x + count.bounds.x * sx - 6, y: box.y + count.bounds.y * sy - 6, width: count.bounds.width * sx + 12, height: count.bounds.height * sy + 12 },
  });
  const job = await act(appAttachment, first.value.binding, { kind: "click", target: count.id });
  expect(job.status).toBe("done");
  const after = await observe(appAttachment);
  expect(after.value.text).toMatch(/In-memory counter \(not saved anywhere\):\s*1/);
  // Same contract as Local: the page operation clicks the element (DOM click).
  expect(after.value.text).toMatch(/last target:\s*count \((?:trusted|synthetic)\)/);
  receipt("frame-vs-agent", { viewport: { w: 1280, h: 800 }, displayed: { w: box.width, h: box.height }, target: count, act: job.status });
  dashboardBinding = after.value.binding;
  await shot("05-after-secretary-click");
});

test("a person's input between observation and action wins: the stale action is refused", async () => {
  const obs = await observe(appAttachment);
  const count = target(obs.value, /Count \+1/);
  // The person scrolls the page (takes control), then hands back.
  const box = await frame().boundingBox();
  await page.mouse.move(box!.x + 200, box!.y + 200);
  await page.mouse.wheel(0, 120);
  await expect(controlPill()).toContainText("あなたが操作中");
  await handBack();
  const job = await act(appAttachment, obs.value.binding, { kind: "click", target: count.id });
  receipt("race-refused", { status: job.status, result: job.result });
  expect(job.status).toBe("failed");
  expect(JSON.stringify(job.result)).toMatch(/page_changed|stale_observation/);
  const check = await observe(appAttachment);
  expect(check.value.text).toMatch(/In-memory counter \(not saved anywhere\):\s*1/);
});

test("Japanese text through the IME path reaches the remote page and the secretary reads it", async () => {
  await addressBar(`${APP}/memo`);
  await activeTabTitle(/memo/);
  await handBack();
  const obs = await observe(appAttachment);
  // The shared page operation names targets by aria-label, placeholder or
  // text, not label[for] (same as Local), so the memo field is found by tag.
  const memo = (obs.value.targets as Target[]).find((t) => t.tag === "textarea");
  if (!memo) throw new Error("no memo field");
  const save = target(obs.value, /Save memo/);
  await clickRemote(memo.bounds);
  // Synthetic IME composition on the person's side: composition updates,
  // then commit. Only the committed text crosses to the remote page.
  const cdp = await context.newCDPSession(page);
  await cdp.send("Input.imeSetComposition", { text: "とうきょう", selectionStart: 5, selectionEnd: 5 });
  await cdp.send("Input.imeSetComposition", { text: "東京", selectionStart: 2, selectionEnd: 2 });
  await cdp.send("Input.insertText", { text: "東京で打ち合わせ" });
  await page.keyboard.press("End");
  await page.keyboard.type(" 3F");
  await shot("06-ime-typed");
  await clickRemote(save.bounds);
  await handBack();
  let text = "";
  for (let i = 0; i < 20 && !text.includes("東京で打ち合わせ 3F"); i++) {
    text = String((await observe(appAttachment)).value.text);
    if (!text.includes("東京で打ち合わせ 3F")) await page.waitForTimeout(500);
  }
  expect(text).toMatch(/Saved memo:\s*東京で打ち合わせ 3F/);
  const report = await stack.fixtureReport();
  receipt("ime", { memos: report.memos });
  expect(report.memos.at(-1)).toBe("東京で打ち合わせ 3F");
  await shot("07-ime-saved");
});

test("second origin, storage and scroll on a new tab (keyboard and wheel)", async () => {
  await addressBar(`${APP}/`);
  await activeTabTitle(/dashboard/);
  await handBack();
  const obs = await observe(appAttachment);
  // Profile state the secretary makes: theme in localStorage, a note in IndexedDB.
  for (const name of [/Use dark theme/, /Save note to IndexedDB/]) {
    const o = await observe(appAttachment);
    expect((await act(appAttachment, o.value.binding, { kind: "click", target: target(o.value, name).id })).status).toBe("done");
  }
  void obs;
  const check = await observe(appAttachment);
  expect(check.value.text).toMatch(/localStorage theme:\s*dark/);
  expect(check.value.text).toMatch(/IndexedDB note:\s*fixture note v1/);

  await page.getByRole("button", { name: "新しいタブ" }).click();
  await expect(page.getByRole("tab")).toHaveCount(2, { timeout: 15_000 });
  await addressBar(`${DOCS}/`);
  await activeTabTitle(/Fixture docs/);
  // Share read-only so the secretary can find the button for the person.
  await page.getByRole("button", { name: "秘書と共有" }).click();
  await page.getByLabel("操作も任せる").uncheck();
  await page.getByRole("button", { name: "共有する" }).click();
  await expect(page.getByRole("button", { name: "共有中" })).toBeVisible({ timeout: 15_000 });
  await page.keyboard.press("Escape");
  const docs = await sharedTab(/Fixture docs/);
  expect(docs.allow_actions).toBe(false);
  const docsObs = await observe(docs.attachment_id);
  await clickRemote(target(docsObs.value, /Mark section read/).bounds);
  await expect.poll(async () => String((await observe(docs.attachment_id)).value.text), { timeout: 10_000 }).toMatch(/localStorage docs_last:\s*read v1/);
  // A read-only share refuses actions.
  const readOnly = await observe(docs.attachment_id);
  const refused = await stack.secretary!.call<Json>("browser.act", {
    attachment_id: docs.attachment_id,
    binding: readOnly.value.binding,
    action: { kind: "click", target: target(readOnly.value, /Mark section read/).id },
  });
  receipt("read-only-share", { refused: refused.error ?? refused.job?.status ?? refused });
  expect(JSON.stringify(refused)).not.toContain('"status":"done"');
  // Keyboard and wheel scrolling by the person. Observations carry the
  // visible text (same as Local), so the scrolled view no longer shows the top.
  await page.keyboard.press("PageDown");
  const box = await frame().boundingBox();
  await page.mouse.move(box!.x + 300, box!.y + 300);
  await page.mouse.wheel(0, 600);
  await page.waitForTimeout(800);
  const docsAfter = await observe(docs.attachment_id);
  expect(docsAfter.value.text).not.toMatch(/localStorage docs_last/);
  expect(docsAfter.value.text).toMatch(/Section (?:[5-9]|[1-3]\d)\b/);
  await shot("08-docs-scrolled");
  await page.getByRole("tab", { name: /dashboard/ }).click();
  await activeTabTitle(/dashboard/);
  await handBack();
});

test("Jev goal runs asynchronously and the person's takeover stops it before its next action", async () => {
  test.skip(mode === "cloud" && !process.env.CLOUD_BROWSER_E2E_JEV_KEY_FILE, "no Jev endpoint in this mode");
  const key = mode === "local" ? FIXTURE_KEY : (await import("node:fs")).readFileSync(process.env.CLOUD_BROWSER_E2E_JEV_KEY_FILE!, "utf8").trim();
  await page.getByRole("button", { name: /Jev/ }).click();
  await page.getByLabel("Jev の API キー").fill(key);
  await page.getByRole("button", { name: "保存", exact: true }).click();
  await expect(page.getByRole("button", { name: /Jev 設定済み/ })).toBeVisible({ timeout: 15_000 });
  await page.keyboard.press("Escape");
  const withJev = await sharedTab(/sign in|dashboard/, (t) => t.attachment_id === appAttachment && t.operation_layers.jev === "available");
  receipt("jev-available", withJev.operation_layers);

  await addressBar(`${APP}/pager?p=1`);
  await activeTabTitle(/Page 1/);
  await handBack();
  pagerGoal = 30;
  const started = await stack.secretary!.call<Json>("browser.goal", {
    attachment_id: appAttachment,
    goal: "Press Next until the page shows Page 30.",
    max_steps: 30,
  });
  const jobId = started.job.job_id;
  let progress: Json = {};
  for (let i = 0; i < 120; i++) {
    const s = await stack.secretary!.call<Json>("job.status", { job_id: jobId });
    progress = s.job?.result?.progress ?? {};
    if ((progress.actions_dispatched ?? 0) >= 2) break;
    await page.waitForTimeout(250);
  }
  if ((progress.actions_dispatched ?? 0) < 2) {
    const s = await stack.secretary!.call<Json>("job.status", { job_id: jobId });
    throw new Error(`goal did not progress: ${JSON.stringify(s.job ?? s).slice(0, 1200)}`);
  }
  await expect(page.getByText(/秘書が作業中/)).toBeVisible();
  await shot("09-goal-running");
  // The person clicks on the page: control moves before the click is sent.
  const box = await frame().boundingBox();
  await page.mouse.click(box!.x + box!.width * 0.8, box!.y + box!.height * 0.8);
  await expect(page.getByText(/あなたが操作中です/)).toBeVisible();
  const ended = await stack.secretary!.call<Json>("job.status", { job_id: jobId }, true);
  receipt("takeover-during-goal", { status: ended.job.status, outcome: ended.job.result?.value?.goal_outcome, actions: ended.job.result?.value?.actions_dispatched });
  expect(ended.job.status).toBe("cancelled");
  expect(ended.job.result.value.goal_outcome).toBe("stopped_by_person");
  expect(ended.job.result.value.actions_dispatched).toBeLessThan(30);
  await shot("10-goal-stopped-by-person");
  await handBack();

  // The secretary's own cancellation.
  const again = await stack.secretary!.call<Json>("browser.goal", { attachment_id: appAttachment, goal: "Press Next until the page shows Page 30.", max_steps: 30 });
  for (let i = 0; i < 120; i++) {
    const s = await stack.secretary!.call<Json>("job.status", { job_id: again.job.job_id });
    if ((s.job?.result?.progress?.actions_dispatched ?? 0) >= 1) break;
    await page.waitForTimeout(250);
  }
  await stack.secretary!.call("job.cancel", { job_id: again.job.job_id });
  const cancelled = await stack.secretary!.call<Json>("job.status", { job_id: again.job.job_id }, true);
  receipt("secretary-cancel", { status: cancelled.job.status, outcome: cancelled.job.result?.value?.goal_outcome });
  expect(cancelled.job.status).toBe("cancelled");
  expect(cancelled.job.cancel_requested_at).toBeTruthy();

  // A goal that completes.
  const obs = await observe(appAttachment);
  const current = Number(/Page (\d+)/.exec(String(obs.value.text))?.[1] ?? 1);
  pagerGoal = current + 2;
  const done = await stack.secretary!.call<Json>(
    "browser.goal",
    { attachment_id: appAttachment, goal: `Press Next until the page shows Page ${pagerGoal}.`, max_steps: 6 },
    true,
  );
  receipt("goal-done", { status: done.job.status, outcome: done.job.result?.value?.goal_outcome, layer: done.job.result?.value?.operation_layer });
  expect(done.job.status).toBe("done");
  if (mode === "local") expect(jev!.requests.every((r: Json) => !r.raw.includes(FIXTURE_KEY) && !r.raw.includes(stack.humanID))).toBe(true);
});

test("revocation withdraws the tab from the secretary at once", async () => {
  await page.getByRole("button", { name: "共有中" }).click();
  await page.getByRole("button", { name: "共有をやめる" }).click();
  await expect(page.getByRole("button", { name: "秘書と共有" })).toBeVisible({ timeout: 15_000 });
  const after = (await tabs()).find((t) => t.attachment_id === appAttachment);
  let refused: Json | undefined;
  try {
    refused = await stack.secretary!.call<Json>("browser.observe", { attachment_id: appAttachment });
  } catch (error) {
    refused = { error: String(error) };
  }
  receipt("revoked", { listed: !!after, observe: refused?.error ?? refused?.job?.status ?? refused });
  expect(after?.available ?? false).toBe(false);
  expect(JSON.stringify(refused)).not.toContain('"status":"done"');
  // Share again for the rest of the journey (a new grant on the same tab).
  await page.getByRole("button", { name: "秘書と共有" }).click();
  await page.getByRole("button", { name: "共有する" }).click();
  await expect(page.getByRole("button", { name: "共有中" })).toBeVisible({ timeout: 15_000 });
  await page.keyboard.press("Escape");
  const again = await sharedTab(/./, (t) => t.available && t.tab.tabId === appTab && t.attachment_id !== appAttachment);
  appAttachment = again.attachment_id;
});

test("the viewer reconnects to the live browser without losing page memory", async () => {
  await page.reload();
  await expect(frame()).toBeVisible({ timeout: 30_000 });
  await expect(page.getByTestId("cloud-browser-status")).toHaveCount(0, { timeout: 30_000 });
  await addressBar(`${APP}/`);
  await activeTabTitle(/dashboard/);
  await handBack();
  const o = await observe(appAttachment);
  expect((await act(appAttachment, o.value.binding, { kind: "click", target: target(o.value, /Count \+1/).id })).status).toBe("done");
  if (mode === "local") {
    // Host restart while the remote browser stays up: reconnect, same page.
    await stack.restartWorker();
    await expect(page.getByTestId("cloud-browser-recovery")).toContainText("再接続しました", { timeout: 60_000 });
    const same = await observe(appAttachment);
    receipt("host-reconnect", { counter: /In-memory counter \(not saved anywhere\):\s*(\d+)/.exec(String(same.value.text))?.[1] });
    expect(same.value.text).toMatch(/In-memory counter \(not saved anywhere\):\s*1/);
    await shot("11-reconnected");
  }
});

test("an effect whose outcome is unknown after a browser loss is reported, never replayed", async () => {
  test.skip(mode === "cloud", "needs the local pool's kill hook (no such mouth in Cloud)");
  const o = await observe(appAttachment);
  const token = crypto.randomUUID();
  const url = `${APP}/order/place?token=${token}&item=Synthetic%20notebook`;
  const pending = act(appAttachment, o.value.binding, { kind: "navigate", url });
  // The fixture commits the order, then answers after 6 s; lose the browser meanwhile.
  for (let i = 0; i < 40; i++) {
    if ((await stack.fixtureReport()).attempts.some((a) => a.token === token)) break;
    await page.waitForTimeout(100);
  }
  const killed = await stack.killBrowsers();
  const job = await pending;
  await expect(page.getByTestId("cloud-browser-recovery")).toContainText("届いたかは確認できません", { timeout: 60_000 });
  await shot("12-uncertain-effect");
  await page.waitForTimeout(8_000);
  const report = await stack.fixtureReport();
  const attempts = report.attempts.filter((a) => a.token === token);
  receipt("uncertain-effect", { killed, job: { status: job.status, result: job.result }, attempts });
  expect(attempts).toHaveLength(1);
  expect(job.status).not.toBe("done");
});

test("close and recreate: cookies, two origins, IndexedDB and tabs return; stale tickets and observations are refused", async () => {
  const before = await observe(appAttachment);
  // A ticket used once cannot open a second viewer.
  const reuse = await page.evaluate(async () => {
    const csrf = (await (await fetch("/auth/csrf", { credentials: "include" })).json()).csrf_token;
    const profile = (await (await fetch("/api/cloud-browser", { credentials: "include" })).json()).profiles[0].profile_id;
    const t = await (
      await fetch(`/api/cloud-browser/profiles/${profile}/viewer-ticket`, { method: "POST", credentials: "include", headers: { "X-CSRF-Token": csrf } })
    ).json();
    const open = () =>
      new Promise<string>((resolve) => {
        const ws = new WebSocket(`${location.origin.replace("http", "ws")}/browser-cloud/viewer`, ["sumi.browser.v1", t.ticket]);
        ws.onmessage = () => {
          ws.close();
          resolve("opened");
        };
        ws.onerror = () => resolve("refused");
        ws.onclose = () => resolve("refused");
      });
    const first = await open();
    const second = await open();
    return { first, second };
  });
  receipt("ticket-reuse", reuse);
  expect(reuse).toEqual({ first: "opened", second: "refused" });

  // The person leaves; the browser saves and sleeps after the idle grace.
  await page.goto(`${stack.webURL}/`);
  const deadline = Date.now() + 120_000;
  let state: Json = {};
  for (;;) {
    state = await stack.profileState();
    const profile = state.profiles?.[0];
    const sleeping = profile?.state === "sleeping" && (mode === "cloud" || (await stack.poolSessions()).length === 0);
    if (sleeping) break;
    if (Date.now() > deadline) throw new Error(`profile did not sleep: ${JSON.stringify(profile)}`);
    await page.waitForTimeout(1_000);
  }
  receipt("slept", { state: state.profiles[0].state, checkpoint_bytes: state.profiles[0].checkpoint_bytes, tab_ids: state.profiles[0].tab_ids });

  await page.goto(`${stack.webURL}/browser`);
  await expect(frame()).toBeVisible({ timeout: 90_000 });
  await expect(page.getByTestId("cloud-browser-recovery")).toContainText("復元しました", { timeout: 90_000 });
  await expect(page.getByRole("tab")).toHaveCount(2, { timeout: 30_000 });
  await shot("13-restored");
  const restoredTab = await sharedTab(/./, (t) => t.available && t.attachment_id === appAttachment);
  expect(restoredTab.tab.tabId).toBe(appTab);
  // The old observation belongs to the previous browser.
  const stale = await act(appAttachment, before.value.binding, { kind: "click", target: target(before.value, /Count \+1/).id });
  expect(stale.status).toBe("failed");
  const after = await observe(appAttachment);
  receipt("restored", {
    signedIn: String(after.value.text).includes("Signed in as"),
    theme: /localStorage theme:\s*(\w+)/.exec(String(after.value.text))?.[1],
    idb: /IndexedDB note:\s*([\w ]+)/.exec(String(after.value.text))?.[1],
    counter: /In-memory counter \(not saved anywhere\):\s*(\d+)/.exec(String(after.value.text))?.[1],
    staleAct: stale.result,
  });
  expect(after.value.text).toMatch(/Signed in as\s*Synthetic Ada/);
  expect(after.value.text).toMatch(/localStorage theme:\s*dark/);
  expect(after.value.text).toMatch(/IndexedDB note:\s*fixture note v1/);
  // Page memory is not a checkpoint: said in the UI, true here.
  expect(after.value.text).toMatch(/In-memory counter \(not saved anywhere\):\s*0/);
  // The second origin's tab returns at its scroll position, with its storage.
  const docs = (await tabs()).find((t) => /Fixture docs/.test(String(t.name)));
  expect(docs, "the docs tab is restored with its grant").toBeTruthy();
  const scrolled = await observe(docs!.attachment_id);
  expect(scrolled.value.text).not.toMatch(/localStorage docs_last/);
  await page.getByRole("tab", { name: /Fixture docs/ }).click();
  await activeTabTitle(/Fixture docs/);
  await page.getByTestId("cloud-browser-frame").click({ position: { x: 640, y: 400 } });
  await page.keyboard.press("Home");
  await expect.poll(async () => String((await observe(docs!.attachment_id)).value.text), { timeout: 10_000 }).toMatch(/localStorage docs_last:\s*read v1/);
  receipt("restored-docs", { scrollRestored: true, docsLast: "read v1" });
  await page.getByRole("tab", { name: /dashboard/ }).click();
  await activeTabTitle(/dashboard/);
});

test("reset wipes the profile and closes the viewer", async () => {
  await page.getByRole("button", { name: "ブラウザをリセット" }).click();
  await page.getByRole("button", { name: "リセット", exact: true }).click();
  await expect(page.getByRole("heading", { name: /ブラウザを用意します|リセットされました/ })).toBeVisible({ timeout: 30_000 });
  const state = await stack.profileState();
  receipt("reset", { profiles: state.profiles?.length ?? 0 });
  expect((await tabs()).filter((t) => t.available)).toHaveLength(0);
  await shot("14-reset");
});
