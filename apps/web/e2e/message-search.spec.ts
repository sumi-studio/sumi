import { randomBytes, randomUUID } from "node:crypto";
import { mkdir, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, test, type Page } from "@playwright/test";
import {
  buildWorkspaceBrowserStack,
  removeWorkspaceBrowserBuild,
  startWorkspaceBrowserStack,
  type WorkspaceBrowserBuild,
} from "./support/real-agent-stack";

// Message-search end-to-end evidence on the real Workspace browser stack:
// production API + Postgres + Vite + real Chrome. The search -> jump path is
// exercised through the UI; bulk history is seeded through the same production
// REST surface so the target message sits outside the initially loaded page.

test.describe.configure({ timeout: 300_000 });
test.use({ actionTimeout: 10_000 });

const artifactDirectory =
  process.env.SUMI_E2E_ARTIFACT_DIR?.trim() || "test-results/message-search";
const humanBID = "0198f0f4-9b72-7000-8000-00000000e2e1";
const personalityAgentID = "0198f0f4-9b72-7000-8000-000000000001";
const oldNeedle = "quartz-needle-e2e-old-message";
const japaneseNeedle = "検証用の日本語メッセージ星印";
const deletedNeedle = "deleted-needle-e2e-message";
const dmSecret = "dm-secret-e2e-token";
const threadNeedle = "thread-needle-e2e-message";

let build: WorkspaceBrowserBuild | undefined;

test.beforeAll(async () => {
  test.setTimeout(180_000);
  build = await buildWorkspaceBrowserStack();
});

test.afterAll(async () => {
  if (build) await removeWorkspaceBrowserBuild(build);
});

test("Human searches shared messages and reaches the original message", async ({
  context,
  page,
}) => {
  if (!build) throw new Error("Workspace browser binaries were not built");
  const databaseURL = process.env.SUMI_WORKSPACE_E2E_DB_URL?.trim();
  if (!databaseURL) {
    throw new Error(
      "SUMI_WORKSPACE_E2E_DB_URL must name a disposable empty Postgres database",
    );
  }
  const stack = await startWorkspaceBrowserStack(build, databaseURL);
  const artifact = async (name: string) => {
    await mkdir(artifactDirectory, { recursive: true });
    await page.screenshot({ path: join(artifactDirectory, name) });
  };
  try {
    await stack.installSession(context);
    await page.goto(stack.webURL);

    const workspace = await createWorkspace(page, "Search Studio");
    const installation = await installMessaging(page);
    const scopeQuery =
      `workspace_id=${workspace.workspaceID}` +
      `&installation_id=${installation.installationID}` +
      `&authority_epoch=${installation.authorityEpoch}`;

    // A second Human gives the DM boundary a place the Secretary must not
    // read. Provisioning uses the same test-only issuer binary, still scoped
    // to this disposable database.
    await provisionHuman(build, databaseURL, humanBID, "Workspace E2E Human B");
    await seedMember(databaseURL, workspace.workspaceID, "human", humanBID);
    await seedMember(
      databaseURL,
      workspace.workspaceID,
      "personality_agent",
      personalityAgentID,
    );

    await page.getByRole("button", { name: "開く", exact: true }).click();
    await expect(
      page.getByText("場所はまだありません", { exact: true }),
    ).toBeVisible();

    const channelID = await createChannel(page, "search-general");
    const api = context.request;
    const post = (path: string, data: Record<string, unknown>) =>
      api.post(`${stack.apiURL}${path}?${scopeQuery}`, {
        headers: { Origin: stack.webURL },
        data,
      });

    // The needle messages sit far above the most recent page so the loaded
    // history cannot contain them before the jump. 80 fillers leave a real
    // unloaded range between the jump window and the newest page.
    const needleMessageID = await sendMessage(
      post,
      channelID,
      `${oldNeedle} tail`,
    );
    await sendMessage(post, channelID, `${japaneseNeedle} の続き`);
    const doomed = await sendMessage(post, channelID, deletedNeedle);
    for (let index = 0; index < 80; index++) {
      await sendMessage(post, channelID, `filler message ${index}`);
    }
    const thread = await post(`/messaging/places/${channelID}/threads`, {
      name: "検証スレッド",
      client_nonce: randomUUID(),
    });
    expect(thread.status()).toBe(201);
    const threadID = asString(asRecord(await thread.json()).thread_id);
    await sendMessage(post, threadID, `${threadNeedle} in-thread`);
    const dm = await api.post(`${stack.apiURL}/messaging/dms?${scopeQuery}`, {
      headers: { Origin: stack.webURL },
      data: { participant: { kind: "human", human_id: humanBID } },
    });
    expect(dm.status()).toBe(200);
    const dmWire = asRecord(await dm.json());
    const dmID = asString(dmWire.dm_id);
    await sendMessage(post, dmID, `${dmSecret} body`);
    const deleted = await api.delete(
      `${stack.apiURL}/messaging/places/${channelID}/messages/${doomed}?${scopeQuery}`,
      { headers: { Origin: stack.webURL } },
    );
    expect(deleted.status()).toBe(200);

    await mkdir(artifactDirectory, { recursive: true });
    await writeFile(
      join(artifactDirectory, "search-e2e-fixture.json"),
      JSON.stringify(
        {
          workspace_id: workspace.workspaceID,
          installation_id: installation.installationID,
          authority_epoch: installation.authorityEpoch,
          channel_id: channelID,
          thread_id: threadID,
          dm_id: dmID,
          personality_agent_id: personalityAgentID,
          human_b_id: humanBID,
          old_needle: oldNeedle,
          japanese_needle: japaneseNeedle,
          deleted_needle: deletedNeedle,
          dm_secret: dmSecret,
        },
        null,
        2,
      ),
    );

    await page.reload();
    await page.getByRole("button", { name: "search-general" }).click();
    await expect(
      page.getByText("filler message 79", { exact: true }),
    ).toBeVisible();
    // Older than one page: the needle is not part of the loaded window yet.
    await expect(page.getByText(oldNeedle, { exact: false })).toHaveCount(0);

    const search = page.getByPlaceholder("検索");
    await search.fill("quartz-needle");
    const resultsPanel = page.getByText("「quartz-needle」の検索結果");
    await expect(resultsPanel).toBeVisible();
    const hit = page.getByRole("button").filter({ hasText: oldNeedle });
    await expect(hit).toBeVisible();
    await artifact("01-search-results.png");
    await hit.click();

    // Acceptance: the original message loads and is scrolled into view — with
    // the actual following conversation, not a disconnected window. seq 2
    // (the Japanese message) must already be readable next to the target.
    const viewport = page.locator('[data-slot="conversation-viewport"]');
    await expect(
      viewport.locator(`[data-message-id="${needleMessageID}"]`),
    ).toBeVisible();
    await expect(
      viewport.getByText(`${japaneseNeedle} の続き`, { exact: true }),
    ).toBeVisible();
    await artifact("02-jumped-to-old-message.png");

    // Between the jump window (seq 1..25) and the newest page (seq 34..83)
    // the unloaded range must surface as a truthful, pageable marker — never
    // as silently adjacent rows. Scroll down until it mounts.
    const gapButton = page.getByRole("button", {
      name: /途中の\d+件を読み込む/,
    });
    for (let scrolls = 0; scrolls < 30; scrolls += 1) {
      if ((await gapButton.count()) > 0) break;
      await viewport.evaluate((element) => {
        element.scrollTop += element.clientHeight * 0.8;
      });
      await page.waitForTimeout(120);
    }
    await expect(gapButton).toHaveText("途中の8件を読み込む");
    await artifact("03-gap-row.png");

    // Clicking fills the missing range: the windows join into one contiguous
    // history anchored where the person was reading.
    await gapButton.click();
    await expect(gapButton).toHaveCount(0);
    await expect(
      page.getByText("filler message 24", { exact: true }),
    ).toBeVisible();
    await artifact("04-gap-filled.png");

    await search.fill("日本語メッセージ");
    await expect(
      page.getByText("「日本語メッセージ」の検索結果"),
    ).toBeVisible();
    const japaneseHit = page
      .getByRole("button")
      .filter({ hasText: japaneseNeedle });
    await expect(japaneseHit).toBeVisible();
    await japaneseHit.click();
    await expect(
      viewport.getByText(`${japaneseNeedle} の続き`, { exact: true }),
    ).toBeVisible();
    await artifact("05-japanese-result.png");

    await search.fill("zzz-no-match-token");
    await expect(
      page.getByText("一致するメッセージはありません", { exact: true }),
    ).toBeVisible();
    await artifact("06-no-results.png");

    // Deleted content must not come back through search.
    await search.fill(deletedNeedle);
    await expect(
      page.getByText("一致するメッセージはありません", { exact: true }),
    ).toBeVisible();

    // A thread result must navigate into the thread place, not just name it.
    await search.fill("thread-needle");
    const threadHit = page
      .getByRole("button")
      .filter({ hasText: threadNeedle });
    await expect(threadHit).toBeVisible();
    await threadHit.click();
    await expect(
      viewport.getByText(`${threadNeedle} in-thread`),
    ).toBeVisible();
    await artifact("07-thread-result.png");

    // A Human participant can still find and reach DM history.
    await search.fill(dmSecret);
    const dmHit = page.getByRole("button").filter({ hasText: dmSecret });
    await expect(dmHit).toBeVisible();
    await dmHit.click();
    await expect(viewport.getByText(`${dmSecret} body`)).toBeVisible();
    await artifact("08-dm-result.png");

    // Rapid query switching must not resurrect the previous result list.
    await page.getByRole("button", { name: "search-general" }).click();
    await search.fill(oldNeedle);
    await search.fill("zzz-no-match-token");
    await expect(
      page.getByText("一致するメッセージはありません", { exact: true }),
    ).toBeVisible();
    await expect(
      page.getByRole("button").filter({ hasText: oldNeedle }),
    ).toHaveCount(0);
  } catch (error) {
    console.error(stack.diagnostics());
    await artifact("99-failure.png").catch(() => undefined);
    throw error;
  } finally {
    await stack.stop();
  }
});

function asRecord(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("expected a JSON object");
  }
  return value as Record<string, unknown>;
}

function asString(value: unknown): string {
  if (typeof value !== "string" || value === "") {
    throw new Error("expected a non-empty string");
  }
  return value;
}

const specDirectory = dirname(fileURLToPath(import.meta.url));
const apiDirectory = resolve(specDirectory, "../../api");

async function createWorkspace(page: Page, name: string) {
  await page.getByRole("textbox", { name: "新しいWorkspaceの名前" }).fill(name);
  const responsePromise = page.waitForResponse(
    (response) =>
      response.request().method() === "POST" &&
      new URL(response.url()).pathname === "/workspaces",
  );
  await page.getByRole("button", { name: "作成して開く" }).click();
  const response = await responsePromise;
  expect(response.status()).toBe(201);
  return { workspaceID: asString(asRecord(await response.json()).workspace_id) };
}

async function installMessaging(page: Page) {
  await page.getByRole("button", { name: "アプリ", exact: true }).click();
  const responsePromise = page.waitForResponse(
    (response) =>
      response.request().method() === "POST" &&
      new URL(response.url()).pathname === "/app-installations",
  );
  await page.getByRole("button", { name: "インストール" }).click();
  const response = await responsePromise;
  expect(response.status()).toBe(201);
  const installation = asRecord(await response.json());
  return {
    installationID: asString(installation.installation_id),
    authorityEpoch: asString(installation.authority_epoch),
  };
}

async function createChannel(page: Page, channel: string): Promise<string> {
  await page.getByTitle("チャンネルを作成").click();
  const dialog = page.getByRole("dialog", { name: "チャンネルを作成" });
  const responsePromise = page.waitForResponse(
    (response) =>
      response.request().method() === "POST" &&
      new URL(response.url()).pathname === "/messaging/channels",
  );
  await dialog.getByRole("textbox", { name: "名前", exact: true }).fill(channel);
  await dialog.getByRole("button", { name: "作成", exact: true }).click();
  const response = await responsePromise;
  expect(response.status()).toBe(201);
  return asString(asRecord(await response.json()).channel_id);
}

async function sendMessage(
  post: (path: string, data: Record<string, unknown>) => Promise<{
    status(): number;
    headers(): Record<string, string>;
    json(): Promise<unknown>;
  }>,
  placeID: string,
  content: string,
): Promise<string> {
  // Bulk seeding can exceed the mutation admission burst (64 tokens, 4/s
  // refill); honor Retry-After instead of failing the fixture.
  const nonce = randomUUID();
  for (let attempt = 0; attempt < 20; attempt += 1) {
    const response = await post(`/messaging/places/${placeID}/messages`, {
      content,
      client_nonce: nonce,
    });
    if (response.status() !== 429) {
      expect(response.status()).toBe(201);
      return asString(asRecord(await response.json()).message_id);
    }
    const retryAfter = Number(response.headers()["retry-after"]);
    await new Promise((resolve) =>
      setTimeout(
        resolve,
        Number.isFinite(retryAfter) && retryAfter > 0
          ? retryAfter * 1000
          : 500,
      ),
    );
  }
  throw new Error("message send stayed rate limited");
}

async function provisionHuman(
  stackBuild: WorkspaceBrowserBuild,
  databaseURL: string,
  userID: string,
  displayName: string,
): Promise<void> {
  const { execFile } = await import("node:child_process");
  await new Promise<void>((resolvePromise, reject) => {
    execFile(
      stackBuild.sessionIssuer,
      [],
      {
        env: {
          PATH: process.env.PATH ?? "",
          SUMI_BROWSER_SESSION_SECRET: randomBytes(48).toString("base64"),
          SUMI_BROWSER_SESSION_AUDIENCE: "sumi:web",
          SUMI_E2E_SESSION_TENANT_ID: `e2e-search-${randomUUID()}`,
          SUMI_E2E_SESSION_USER_ID: userID,
          SUMI_E2E_SESSION_PERSONALITY_AGENT_ID: personalityAgentID,
          SUMI_E2E_SESSION_DATABASE_URL: databaseURL,
          SUMI_E2E_SESSION_DISPLAY_NAME: displayName,
        },
      },
      (error) => (error ? reject(error) : resolvePromise()),
    );
  });
}

async function seedMember(
  databaseURL: string,
  workspaceID: string,
  kind: "human" | "personality_agent",
  memberID: string,
): Promise<void> {
  const { execFile } = await import("node:child_process");
  await new Promise<void>((resolvePromise, reject) => {
    execFile(
      "go",
      ["run", "./cmd/e2e-seed-member"],
      {
        cwd: apiDirectory,
        env: {
          PATH: process.env.PATH ?? "",
          HOME: process.env.HOME ?? "",
          GOPATH: process.env.GOPATH ?? "",
          GOCACHE: process.env.GOCACHE ?? "",
          GOMODCACHE: process.env.GOMODCACHE ?? "",
          SUMI_E2E_SEED_DATABASE_URL: databaseURL,
          SUMI_E2E_SEED_WORKSPACE_ID: workspaceID,
          SUMI_E2E_SEED_MEMBER_KIND: kind,
          SUMI_E2E_SEED_MEMBER_ID: memberID,
        },
      },
      (error, _stdout, stderr) =>
        error ? reject(new Error(stderr || error.message)) : resolvePromise(),
    );
  });
}
