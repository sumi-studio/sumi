import { execFile } from "node:child_process";
import { lstat } from "node:fs/promises";
import { isAbsolute } from "node:path";
import { promisify } from "node:util";
import { type Browser, expect, type Page, test } from "@playwright/test";

const run = promisify(execFile);
const configuration = {
  baseURL: process.env.SUMI_DOGFOOD_SMOKE_BASE_URL ?? "",
  storageState: process.env.SUMI_DOGFOOD_SMOKE_STORAGE_STATE ?? "",
  placeID: process.env.SUMI_DOGFOOD_SMOKE_PLACE_ID ?? "",
  messagingPath: process.env.SUMI_DOGFOOD_SMOKE_MESSAGING_PATH ?? "",
  restartAPI: process.env.SUMI_DOGFOOD_RESTART_API_HELPER ?? "",
  restartTunnel: process.env.SUMI_DOGFOOD_RESTART_TUNNEL_HELPER ?? "",
};
const missing = Object.entries(configuration)
  .filter(([, value]) => value.trim() === "")
  .map(([name]) => name);

test.describe("dedicated dogfood restart recovery", () => {
  test.describe.configure({ mode: "serial" });
  test.skip(
    missing.length > 0,
    `NOT COVERED: real dogfood restart inputs are absent (${missing.join(", ")})`,
  );
  test.setTimeout(240_000);

  test.beforeAll(async () => {
    const url = new URL(configuration.baseURL);
    if (url.protocol !== "https:" || url.username || url.password) {
      throw new Error(
        "SUMI_DOGFOOD_SMOKE_BASE_URL must be a credential-free HTTPS origin",
      );
    }
    if (
      !/^\/(?:c|dm|group)\/[A-Za-z0-9_-]+$/.test(configuration.messagingPath)
    ) {
      throw new Error(
        "SUMI_DOGFOOD_SMOKE_MESSAGING_PATH must be one canonical shipped Messaging route",
      );
    }
    await requireProtectedFile(configuration.storageState, false);
    await requireProtectedFile(configuration.restartAPI, true);
    await requireProtectedFile(configuration.restartTunnel, true);
  });

  test("Messaging WebApp shows API loss, then replays another client's outage commit exactly once", async ({
    browser,
  }) => {
    await exerciseMessagingRecovery(browser, configuration.restartAPI, "api");
  });

  test("Messaging WebApp converges through a named Tunnel restart", async ({
    browser,
  }) => {
    await exerciseMessagingRecovery(
      browser,
      configuration.restartTunnel,
      "tunnel",
    );
  });

  test("Direct Chat WebApp shows API loss, then cursor-replays another client's command", async ({
    browser,
  }) => {
    const observerContext = await authenticatedContext(browser);
    const writerContext = await authenticatedContext(browser);
    try {
      const observer = await observerContext.newPage();
      const writer = await writerContext.newPage();
      await Promise.all([openDirectChat(observer), openDirectChat(writer)]);

      await observerContext.setOffline(true);
      await expect(directStatus(observer)).toHaveAttribute(
        "data-sumi-connection-state",
        "closed",
      );
      await expect(
        observer.getByText("再接続中", { exact: true }),
      ).toBeVisible();

      const restart = run(configuration.restartAPI, [], { timeout: 120_000 });
      await expect(directStatus(writer)).toHaveAttribute(
        "data-sumi-connection-state",
        "closed",
        { timeout: 30_000 },
      );
      await restart;
      await waitForDirectConnected(writer);

      const text = `dogfood direct replay ${uniqueNonce("direct")}`;
      const composer = writer.getByRole("textbox", {
        name: "メッセージ",
        exact: true,
      });
      await composer.fill(text);
      await writer.getByRole("button", { name: "送信", exact: true }).click();
      await expect(writer.getByText(text, { exact: true })).toHaveCount(1);

      await observerContext.setOffline(false);
      await waitForDirectConnected(observer);
      await expect(observer.getByText(text, { exact: true })).toHaveCount(1, {
        timeout: 60_000,
      });
    } finally {
      await Promise.all([observerContext.close(), writerContext.close()]);
    }
  });

  test("same Messaging nonce returns one durable message after a discarded receipt", async ({
    browser,
  }) => {
    const context = await authenticatedContext(browser);
    try {
      const page = await context.newPage();
      await openMessaging(page);
      const nonce = uniqueNonce("discarded-receipt");
      const firstStatus = await sendAndDiscardReceipt(
        page,
        configuration.placeID,
        nonce,
      );
      expect(firstStatus).toBe(201);
      const replay = await sendMessage(page, configuration.placeID, nonce);
      expect(replay.status).toBe(200);

      const messages = await history(page, configuration.placeID);
      const matches = messages.filter(
        (message) => message.client_nonce === nonce,
      );
      expect(matches).toHaveLength(1);
      expect(matches[0]?.message_id).toBe(replay.messageID);
      expect(matches[0]?.seq).toBe(replay.seq);
      await waitForMessagingConnected(page);
    } finally {
      await context.close();
    }
  });
});

async function exerciseMessagingRecovery(
  browser: Browser,
  restartHelper: string,
  label: string,
) {
  const observerContext = await authenticatedContext(browser);
  const writerContext = await authenticatedContext(browser);
  try {
    const observer = await observerContext.newPage();
    const writer = await writerContext.newPage();
    await Promise.all([openMessaging(observer), openMessaging(writer)]);

    // Keep one shipped client offline after the shared failure has recovered.
    // The second shipped client can then commit while the observer remains in
    // its outage, forcing the observer's own cursor replay to surface it.
    await observerContext.setOffline(true);
    await expect(messagingSurface(observer)).toHaveAttribute(
      "data-sumi-connection-state",
      "reconnecting",
    );
    await expect(
      observer.getByText(/再接続中… 新しいメッセージ/),
    ).toBeVisible();

    const restart = run(restartHelper, [], { timeout: 120_000 });
    await expect(messagingSurface(writer)).toHaveAttribute(
      "data-sumi-connection-state",
      "reconnecting",
      { timeout: 30_000 },
    );
    await restart;
    await waitForMessagingConnected(writer);

    const text = `dogfood ${label} replay ${uniqueNonce(label)}`;
    const composer = writer.locator('textarea[aria-label$="へメッセージ"]');
    await expect(composer).toBeVisible();
    await composer.fill(text);
    await writer.getByRole("button", { name: "送信", exact: true }).click();
    await expect(writer.getByText(text, { exact: true })).toHaveCount(1);

    await observerContext.setOffline(false);
    await waitForMessagingConnected(observer);
    await expect(observer.getByText(text, { exact: true })).toHaveCount(1, {
      timeout: 60_000,
    });
  } finally {
    await Promise.all([observerContext.close(), writerContext.close()]);
  }
}

function authenticatedContext(browser: Browser) {
  return browser.newContext({
    baseURL: configuration.baseURL,
    storageState: configuration.storageState,
  });
}

async function openMessaging(page: Page) {
  const response = await page.goto(configuration.messagingPath, {
    waitUntil: "domcontentloaded",
  });
  expect(response?.ok()).toBe(true);
  await waitForMessagingConnected(page);
}

async function openDirectChat(page: Page) {
  const response = await page.goto("/direct", {
    waitUntil: "domcontentloaded",
  });
  expect(response?.ok()).toBe(true);
  await waitForDirectConnected(page);
}

function messagingSurface(page: Page) {
  return page.locator('[data-sumi-surface="messaging"]');
}

function directStatus(page: Page) {
  return page.locator('[data-sumi-surface="direct-chat"]');
}

async function waitForMessagingConnected(page: Page) {
  await expect(messagingSurface(page)).toHaveAttribute(
    "data-sumi-connection-state",
    "connected",
    { timeout: 60_000 },
  );
}

async function waitForDirectConnected(page: Page) {
  await expect(directStatus(page)).toHaveAttribute(
    "data-sumi-connection-state",
    "connected",
    { timeout: 60_000 },
  );
  await expect(directStatus(page)).toHaveAttribute(
    "data-sumi-ready-state",
    "ready",
    { timeout: 60_000 },
  );
}

async function sendAndDiscardReceipt(
  page: Page,
  placeID: string,
  nonce: string,
) {
  return page.evaluate(
    async ({ selectedPlaceID, clientNonce }) => {
      const response = await fetch(
        `/messaging/places/${encodeURIComponent(selectedPlaceID)}/messages`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            content: `dogfood smoke ${clientNonce}`,
            urgency: "",
            reply_to: "",
            client_nonce: clientNonce,
            attachments: [],
          }),
        },
      );
      await response.arrayBuffer();
      return response.status;
    },
    { selectedPlaceID: placeID, clientNonce: nonce },
  );
}

async function sendMessage(page: Page, placeID: string, nonce: string) {
  const result = await page.evaluate(
    async ({ selectedPlaceID, clientNonce }) => {
      const response = await fetch(
        `/messaging/places/${encodeURIComponent(selectedPlaceID)}/messages`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            content: `dogfood smoke ${clientNonce}`,
            urgency: "",
            reply_to: "",
            client_nonce: clientNonce,
            attachments: [],
          }),
        },
      );
      return {
        status: response.status,
        body: (await response.json()) as unknown,
      };
    },
    { selectedPlaceID: placeID, clientNonce: nonce },
  );
  if (
    (result.status !== 200 && result.status !== 201) ||
    typeof result.body !== "object" ||
    result.body === null
  ) {
    throw new Error(`send failed: ${JSON.stringify(result)}`);
  }
  const body = result.body as Record<string, unknown>;
  if (typeof body.message_id !== "string" || !Number.isSafeInteger(body.seq)) {
    throw new Error(
      `send returned an invalid receipt: ${JSON.stringify(result)}`,
    );
  }
  return {
    status: result.status,
    messageID: body.message_id,
    seq: Number(body.seq),
  };
}

async function history(page: Page, placeID: string) {
  const body = await page.evaluate(async (selectedPlaceID) => {
    const response = await fetch(
      `/messaging/places/${encodeURIComponent(selectedPlaceID)}/messages?limit=100`,
      { cache: "no-store" },
    );
    if (!response.ok) throw new Error(`history failed: ${response.status}`);
    return response.json() as Promise<unknown>;
  }, placeID);
  if (
    typeof body !== "object" ||
    body === null ||
    !Array.isArray((body as Record<string, unknown>).messages)
  ) {
    throw new Error(
      `history returned an invalid body: ${JSON.stringify(body)}`,
    );
  }
  return (body as { messages: Record<string, unknown>[] }).messages;
}

function uniqueNonce(prefix: string) {
  return `${prefix}-${Date.now()}-${crypto.randomUUID()}`;
}

async function requireProtectedFile(path: string, executable: boolean) {
  if (!isAbsolute(path)) throw new Error(`${path} is not absolute`);
  const info = await lstat(path);
  if (!info.isFile() || info.isSymbolicLink() || (info.mode & 0o077) !== 0) {
    throw new Error(`${path} must be a protected regular non-symlink`);
  }
  if (executable && (info.mode & 0o100) === 0) {
    throw new Error(`${path} must be owner-executable`);
  }
}
