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
  const databaseURL = process.env.SUMI_DIRECT_CHAT_E2E_DB_URL?.trim();
  if (!databaseURL) {
    throw new Error(
      "SUMI_DIRECT_CHAT_E2E_DB_URL must name a disposable empty Postgres database",
    );
  }

  const assistantMessageEndSequence = new Map<string, number>();
  let directChatSocketSeen = false;
  let directChatSocketCount = 0;
  let directChatSocketCloseCount = 0;
  let commandSent = false;
  let postCommandSocketCloseCount = 0;
  const directChatDiagnostics: string[] = [];
  const recordDirectChatDiagnostic = (value: string) => {
    if (directChatDiagnostics.length < 256) directChatDiagnostics.push(value);
  };
  page.on("websocket", (socket) => {
    if (new URL(socket.url()).pathname !== "/direct-chat/ws") return;
    directChatSocketSeen = true;
    directChatSocketCount++;
    const socketNumber = directChatSocketCount;
    recordDirectChatDiagnostic(`socket ${socketNumber} opened`);
    socket.on("close", () => {
      directChatSocketCloseCount++;
      if (commandSent) postCommandSocketCloseCount++;
      recordDirectChatDiagnostic(`socket ${socketNumber} closed`);
    });
    socket.on("framesent", ({ payload }) => {
      if (typeof payload !== "string") return;
      try {
        const frame = JSON.parse(payload) as { type?: unknown };
        recordDirectChatDiagnostic(
          `socket ${socketNumber} sent ${String(frame.type)}`,
        );
        if (frame.type === "command") commandSent = true;
      } catch {
        recordDirectChatDiagnostic(
          `socket ${socketNumber} sent malformed JSON`,
        );
      }
    });
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
        recordDirectChatDiagnostic(
          frame.type === "event"
            ? `socket ${socketNumber} received event ${String(event?.type)} seq=${String(sequence)}`
            : `socket ${socketNumber} received ${String(frame.type)}`,
        );
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

  const stack = await startRealAgentStack(build, databaseURL);
  let primaryError: Error | undefined;
  try {
    await stack.installSession(context);
    const cookies = await context.cookies(stack.apiURL);
    expect(
      cookies.some(
        (cookie) => cookie.name === "sumi_session" && cookie.httpOnly,
      ),
    ).toBe(true);

    await page.goto(stack.webURL);
    await page.getByRole("button", { name: "直通", exact: true }).click();
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
    await expect(page.getByText(firstUserMessage, { exact: true })).toHaveCount(
      1,
    );
    await expect(
      page.getByText(secondUserMessage, { exact: true }),
    ).toHaveCount(1);
    expect(stack.provider.requestCount).toBe(2);
    expect(stack.provider.requests).toHaveLength(2);
    expect(stack.provider.contextVerified).toBe(true);
    expect(directChatSocketCount - directChatSocketCloseCount).toBe(1);
    expect(postCommandSocketCloseCount).toBe(0);
    expect(
      assistantMessageEndSequence.get(secondProviderResponse),
    ).toBeGreaterThan(
      assistantMessageEndSequence.get(firstProviderResponse) ?? 0,
    );
  } catch (error) {
    primaryError = new Error(
      `${error instanceof Error ? error.message : String(error)}\n\nDirect-chat frame diagnostics (types and sequences only):\n${directChatDiagnostics.join("\n")}\n\nRedacted child diagnostics:\n${stack.diagnostics()}`,
      { cause: error },
    );
  }
  let cleanupError: Error | undefined;
  try {
    await stack.stop();
  } catch (error) {
    cleanupError = error instanceof Error ? error : new Error(String(error));
  }
  if (primaryError && cleanupError) {
    throw new AggregateError(
      [primaryError, cleanupError],
      primaryError.message,
      { cause: primaryError },
    );
  }
  if (primaryError) throw primaryError;
  if (cleanupError) throw cleanupError;
});
