import { expect, test } from "@playwright/test";
import {
  buildRealAgentStack,
  firstProviderResponse,
  firstUserMessage,
  type RealAgentBuild,
  removeRealAgentBuild,
  secondProviderResponse,
  secondUserMessage,
  startRealAgentStack,
} from "./support/real-agent-stack";

test.describe.configure({ timeout: 420_000 });

let build: RealAgentBuild | undefined;

test.beforeAll(async () => {
  test.setTimeout(420_000);
  build = await buildRealAgentStack();
});

test.afterAll(async () => {
  if (build) await removeRealAgentBuild(build);
});

test("Chrome direct chat reaches the real production agent for two contextual turns", async ({
  context,
  page,
}) => {
  if (!build) throw new Error("real-agent binaries were not built");

  const assistantMessageEndSequence = new Map<string, number>();
  let directChatSocketSeen = false;
  page.on("websocket", (socket) => {
    if (new URL(socket.url()).pathname !== "/direct-chat/ws") return;
    directChatSocketSeen = true;
    socket.on("framereceived", ({ payload }) => {
      if (typeof payload !== "string") return;
      try {
        const frame = JSON.parse(payload) as {
          type?: string;
          envelope?: {
            seq?: unknown;
            event?: { type?: string; message?: unknown };
          };
        };
        const event =
          frame.type === "event" ? frame.envelope?.event : undefined;
        const sequence = frame.envelope?.seq;
        if (
          event?.type !== "message_end" ||
          !Number.isSafeInteger(sequence) ||
          Number(sequence) <= 0
        ) {
          return;
        }
        const serialized = JSON.stringify(event.message);
        for (const response of [
          firstProviderResponse,
          secondProviderResponse,
        ]) {
          if (serialized.includes(response)) {
            assistantMessageEndSequence.set(response, Number(sequence));
          }
        }
      } catch {
        // The production browser decoder owns malformed-frame behavior. This
        // observer records only positive durable assistant MessageEnd seqs.
      }
    });
  });

  const stack = await startRealAgentStack(build);
  try {
    await stack.installSession(context);
    const cookies = await context.cookies(stack.apiURL);
    expect(
      cookies.some(
        (cookie) => cookie.name === "sumi_session" && cookie.httpOnly,
      ),
    ).toBe(true);

    await page.goto(stack.webURL);
    await expect(
      page.getByText("エージェント利用可能", { exact: true }),
    ).toBeVisible({ timeout: 45_000 });
    await expect.poll(() => directChatSocketSeen).toBe(true);

    const composer = page.getByRole("textbox", {
      name: "メッセージ",
      exact: true,
    });
    await composer.fill(firstUserMessage);
    await page.getByRole("button", { name: "送信", exact: true }).click();
    await expect(
      page.getByText(firstProviderResponse, { exact: true }),
    ).toBeVisible({ timeout: 30_000 });
    await expect
      .poll(() => assistantMessageEndSequence.get(firstProviderResponse))
      .toBeGreaterThan(0);
    await expect(
      page.getByRole("button", { name: "送信", exact: true }),
    ).toBeVisible({ timeout: 30_000 });

    await composer.fill(secondUserMessage);
    await page.getByRole("button", { name: "送信", exact: true }).click();
    await expect(
      page.getByText(secondProviderResponse, { exact: true }),
    ).toBeVisible({ timeout: 30_000 });
    await expect
      .poll(() => assistantMessageEndSequence.get(secondProviderResponse))
      .toBeGreaterThan(0);

    await expect(
      page.getByText(firstProviderResponse, { exact: true }),
    ).toBeVisible();
    await expect(
      page.getByText(secondProviderResponse, { exact: true }),
    ).toBeVisible();
    expect(stack.provider.requestCount).toBe(2);
    expect(stack.provider.requests).toHaveLength(2);
    expect(stack.provider.contextVerified).toBe(true);
    expect(
      assistantMessageEndSequence.get(secondProviderResponse),
    ).toBeGreaterThan(
      assistantMessageEndSequence.get(firstProviderResponse) ?? 0,
    );
  } finally {
    await stack.stop();
  }
});
