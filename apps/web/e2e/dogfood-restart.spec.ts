import { execFile } from "node:child_process";
import { lstat } from "node:fs/promises";
import { isAbsolute } from "node:path";
import { promisify } from "node:util";
import { expect, type Page, test } from "@playwright/test";

const run = promisify(execFile);
const configuration = {
  baseURL: process.env.SUMI_DOGFOOD_SMOKE_BASE_URL ?? "",
  storageState: process.env.SUMI_DOGFOOD_SMOKE_STORAGE_STATE ?? "",
  placeID: process.env.SUMI_DOGFOOD_SMOKE_PLACE_ID ?? "",
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
  test.setTimeout(180_000);

  test.beforeAll(async () => {
    const url = new URL(configuration.baseURL);
    if (url.protocol !== "https:" || url.username || url.password) {
      throw new Error(
        "SUMI_DOGFOOD_SMOKE_BASE_URL must be a credential-free HTTPS origin",
      );
    }
    await requireProtectedFile(configuration.storageState, false);
    await requireProtectedFile(configuration.restartAPI, true);
    await requireProtectedFile(configuration.restartTunnel, true);
  });

  test("API restart closes the live socket and cursor replay catches the intervening commit once", async ({
    browser,
  }) => {
    const context = await browser.newContext({
      baseURL: configuration.baseURL,
      storageState: configuration.storageState,
    });
    try {
      const observer = await context.newPage();
      const writer = await context.newPage();
      await Promise.all([visitOrigin(observer), visitOrigin(writer)]);

      const baseline = await openAndCatchUp(observer, configuration.placeID, 0);
      await run(configuration.restartAPI, [], { timeout: 120_000 });
      await expect
        .poll(() => socketCloseCount(observer), { timeout: 30_000 })
        .toBeGreaterThan(0);
      await waitForHealth(writer);

      const nonce = uniqueNonce("api-restart");
      const receipt = await sendMessage(writer, configuration.placeID, nonce);
      const frames = await reconnectAndCatchUp(
        observer,
        configuration.placeID,
        baseline,
      );
      assertCaughtUpExactlyOnce(frames, nonce, receipt.seq);
    } finally {
      await context.close();
    }
  });

  test("named Tunnel connector restart also converges through the same cursor contract", async ({
    browser,
  }) => {
    const context = await browser.newContext({
      baseURL: configuration.baseURL,
      storageState: configuration.storageState,
    });
    try {
      const observer = await context.newPage();
      const writer = await context.newPage();
      await Promise.all([visitOrigin(observer), visitOrigin(writer)]);

      const baseline = await openAndCatchUp(observer, configuration.placeID, 0);
      await run(configuration.restartTunnel, [], { timeout: 120_000 });
      await expect
        .poll(() => socketCloseCount(observer), { timeout: 30_000 })
        .toBeGreaterThan(0);
      await waitForHealth(writer);

      const nonce = uniqueNonce("tunnel-restart");
      const receipt = await sendMessage(writer, configuration.placeID, nonce);
      const frames = await reconnectAndCatchUp(
        observer,
        configuration.placeID,
        baseline,
      );
      assertCaughtUpExactlyOnce(frames, nonce, receipt.seq);
    } finally {
      await context.close();
    }
  });

  test("discarding a successful receipt and retrying the same nonce returns one durable message", async ({
    browser,
  }) => {
    const context = await browser.newContext({
      baseURL: configuration.baseURL,
      storageState: configuration.storageState,
    });
    try {
      const page = await context.newPage();
      await visitOrigin(page);
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
    } finally {
      await context.close();
    }
  });
});

type SocketFrame = Record<string, unknown>;
type SmokeWindow = Window &
  typeof globalThis & {
    __sumiDogfoodSocket?: {
      socket: WebSocket;
      frames: SocketFrame[];
      closes: number;
    };
  };

async function visitOrigin(page: Page) {
  const response = await page.goto("/", { waitUntil: "domcontentloaded" });
  expect(response?.ok()).toBe(true);
}

async function openAndCatchUp(page: Page, placeID: string, cursor: number) {
  await openSocket(page, placeID, cursor);
  await expect
    .poll(
      async () => {
        const frames = await socketFrames(page);
        return frames.some(
          (frame) => frame.type === "caught_up" && frame.place_id === placeID,
        );
      },
      { timeout: 30_000 },
    )
    .toBe(true);
  const caughtUp = (await socketFrames(page)).findLast(
    (frame) => frame.type === "caught_up" && frame.place_id === placeID,
  );
  const latest = caughtUp?.latest_seq;
  if (!Number.isSafeInteger(latest) || Number(latest) < cursor) {
    throw new Error(`invalid caught_up frame: ${JSON.stringify(caughtUp)}`);
  }
  return Number(latest);
}

async function reconnectAndCatchUp(
  page: Page,
  placeID: string,
  cursor: number,
) {
  await openAndCatchUp(page, placeID, cursor);
  return socketFrames(page);
}

async function openSocket(page: Page, placeID: string, cursor: number) {
  await page.evaluate(
    ({ selectedPlaceID, selectedCursor }) => {
      const state: NonNullable<SmokeWindow["__sumiDogfoodSocket"]> = {
        socket: new WebSocket(new URL("/messaging/ws", window.location.href)),
        frames: [],
        closes: 0,
      };
      (window as SmokeWindow).__sumiDogfoodSocket = state;
      state.socket.addEventListener("open", () => {
        state.socket.send(
          JSON.stringify({
            type: "hello",
            cursors: { [selectedPlaceID]: selectedCursor },
          }),
        );
      });
      state.socket.addEventListener("message", (event) => {
        if (typeof event.data !== "string") return;
        state.frames.push(JSON.parse(event.data) as SocketFrame);
      });
      state.socket.addEventListener("close", () => {
        state.closes++;
      });
    },
    { selectedPlaceID: placeID, selectedCursor: cursor },
  );
}

function socketFrames(page: Page) {
  return page.evaluate(
    () => (window as SmokeWindow).__sumiDogfoodSocket?.frames ?? [],
  );
}

function socketCloseCount(page: Page) {
  return page.evaluate(
    () => (window as SmokeWindow).__sumiDogfoodSocket?.closes ?? 0,
  );
}

function assertCaughtUpExactlyOnce(
  frames: SocketFrame[],
  nonce: string,
  minimumSeq: number,
) {
  const messages = frames.filter((frame) => {
    if (
      frame.type !== "event" ||
      typeof frame.event !== "object" ||
      frame.event === null
    )
      return false;
    const event = frame.event as Record<string, unknown>;
    if (
      event.type !== "message_created" ||
      typeof event.message !== "object" ||
      event.message === null
    )
      return false;
    return (event.message as Record<string, unknown>).client_nonce === nonce;
  });
  expect(messages).toHaveLength(1);
  const barrier = frames.findLast(
    (frame) =>
      frame.type === "caught_up" && frame.place_id === configuration.placeID,
  );
  expect(Number(barrier?.latest_seq)).toBeGreaterThanOrEqual(minimumSeq);
}

async function waitForHealth(page: Page) {
  await expect
    .poll(
      () =>
        page.evaluate(async () => {
          try {
            const response = await fetch("/health", { cache: "no-store" });
            return response.status;
          } catch {
            return 0;
          }
        }),
      { timeout: 60_000 },
    )
    .toBe(200);
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
      {
        cache: "no-store",
      },
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
  return (body as { messages: Array<Record<string, unknown>> }).messages;
}

async function requireProtectedFile(path: string, executable: boolean) {
  if (!isAbsolute(path))
    throw new Error(`smoke input must be absolute: ${path}`);
  const info = await lstat(path);
  if (!info.isFile() || info.isSymbolicLink())
    throw new Error(`smoke input must be a regular non-symlink: ${path}`);
  if ((info.mode & 0o077) !== 0)
    throw new Error(`smoke input grants group/other permissions: ${path}`);
  if (executable && (info.mode & 0o100) === 0)
    throw new Error(`smoke helper is not owner-executable: ${path}`);
}

function uniqueNonce(prefix: string) {
  return `${prefix}-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}
