import {
  expect,
  type Page,
  type Request,
  type Response,
  type Route,
  test,
} from "@playwright/test";
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
test.use({ actionTimeout: 15_000 });

let build: RealAgentBuild | undefined;

test.beforeAll(async () => {
  test.setTimeout(420_000);
  build = await buildRealAgentStack();
});

test.afterAll(async () => {
  if (build) await removeRealAgentBuild(build);
});

test("two real Chrome pages own Direct Chat through the production Participant lifecycle", async ({
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

  // Drop the first command frame before the native send. The production
  // DirectChatSocket has already retained that exact idempotency key, so its
  // normal reconnect path—not fixture code—must retry and admit it once.
  await context.addInitScript(() => {
    const state = {
      armed: false,
      commandAttempts: [] as string[],
      suppressed: 0,
    };
    const browserWindow = window as Window & {
      __sumiArmDirectChatRetry?: () => void;
      __sumiDirectChatRetryState?: typeof state;
    };
    browserWindow.__sumiArmDirectChatRetry = () => {
      state.armed = true;
    };
    browserWindow.__sumiDirectChatRetryState = state;
    const nativeSend = WebSocket.prototype.send;
    WebSocket.prototype.send = function send(
      data: string | ArrayBufferLike | Blob | ArrayBufferView,
    ) {
      if (
        typeof data === "string" &&
        new URL(this.url).pathname === "/direct-chat/ws"
      ) {
        try {
          const frame = JSON.parse(data) as {
            type?: unknown;
            idempotency_key?: unknown;
          };
          if (
            frame.type === "command" &&
            typeof frame.idempotency_key === "string"
          ) {
            state.commandAttempts.push(frame.idempotency_key);
            if (state.armed) {
              state.armed = false;
              state.suppressed += 1;
              this.close(4001, "deterministic pending retry");
              return;
            }
          }
        } catch {
          // The native implementation remains authoritative for malformed
          // data; this seam observes only production command envelopes.
        }
      }
      nativeSend.call(this, data);
    };
  });

  const assistantMessageEndSequence = new Map<string, number>();
  const socketObservations: SocketObservation[] = [];
  const directChatDiagnostics: string[] = [];
  const recordDiagnostic = (value: string) => {
    if (directChatDiagnostics.length < 512) directChatDiagnostics.push(value);
  };
  observeDirectChat(
    page,
    "first",
    socketObservations,
    assistantMessageEndSequence,
    recordDiagnostic,
  );
  const secondPage = await context.newPage();
  observeDirectChat(
    secondPage,
    "second",
    socketObservations,
    assistantMessageEndSequence,
    recordDiagnostic,
  );

  const stack = await startRealAgentStack(build, databaseURL);
  let primaryError: Error | undefined;
  try {
    await stack.installSession(context);
    const cookies = await context.cookies(stack.webURL);
    expect(
      cookies.some(
        (cookie) => cookie.name === "sumi_session" && cookie.httpOnly,
      ),
    ).toBe(true);

    const firstSession = waitForSessionBootstrap(page);
    const secondSession = waitForSessionBootstrap(secondPage);
    await Promise.all([page.goto(stack.webURL), secondPage.goto(stack.webURL)]);
    const [firstSessionResponse, secondSessionResponse] = await Promise.all([
      firstSession,
      secondSession,
    ]);
    const webOrigin = new URL(stack.webURL).origin;
    const sessionUserID = await expectNormalSession(
      firstSessionResponse,
      webOrigin,
    );
    expect(await expectNormalSession(secondSessionResponse, webOrigin)).toBe(
      sessionUserID,
    );
    expect(new URL(stack.apiURL).origin).not.toBe(webOrigin);

    await expect(railButton(page)).toHaveCount(0);
    await expect(railButton(secondPage)).toHaveCount(0);

    const installResponse = page.waitForResponse(isInstallResponse);
    const firstMenu = await openParticipantApps(page);
    await directChatRow(firstMenu)
      .getByRole("button", { name: "導入" })
      .click();
    const installed = expectInstallation(
      await responseJSON(await installResponse, 201, webOrigin),
      sessionUserID,
      "enabled",
      "1",
    );
    expect(installed.installationID).toMatch(UUIDv7Pattern);
    await expect(railButton(page)).toBeVisible();
    await expect(railButton(secondPage)).toBeVisible({ timeout: 30_000 });
    await closePopovers(page);

    await Promise.all([openDirectChat(page), openDirectChat(secondPage)]);
    await expectSocketScope(
      socketObservations,
      "first",
      installed.installationID,
      "1",
    );
    await expectSocketScope(
      socketObservations,
      "second",
      installed.installationID,
      "1",
    );

    await page.evaluate(() => {
      const arm = (window as Window & { __sumiArmDirectChatRetry?: () => void })
        .__sumiArmDirectChatRetry;
      if (!arm) throw new Error("pending-retry seam was not installed");
      arm();
    });
    await sendMessage(page, firstUserMessage);
    await expect(
      page.getByText(firstProviderResponse, { exact: true }),
    ).toBeVisible({ timeout: 30_000 });
    await expect(
      secondPage.getByText(firstProviderResponse, { exact: true }),
    ).toBeVisible({ timeout: 30_000 });
    await expect
      .poll(async () => pendingRetryState(page))
      .toMatchObject({ suppressed: 1, attemptCount: 2, uniqueKeys: 1 });
    await expect
      .poll(() => assistantMessageEndSequence.get(firstProviderResponse))
      .toBeGreaterThan(0);
    await expect(page.getByText(firstUserMessage, { exact: true })).toHaveCount(
      1,
    );
    expect(stack.provider.requestCount).toBe(1);

    const delayedSecondPageList =
      await holdNextParticipantInstallationList(secondPage);
    const delayedRefreshResponse = secondPage.waitForResponse(
      isParticipantInstallationListResponse,
    );
    const secondRefreshMenu = await openParticipantApps(secondPage);
    await secondRefreshMenu
      .getByRole("button", { name: "個人用アプリを更新" })
      .click();
    await delayedSecondPageList.reached;

    let disableRequestSeen = false;
    const observeDisableRequest = (request: Request) => {
      if (
        request.method() === "PUT" &&
        /^\/app-installations\/[^/]+\/state$/.test(
          new URL(request.url()).pathname,
        )
      ) {
        disableRequestSeen = true;
      }
    };
    page.on("request", observeDisableRequest);
    const disableResponse = page.waitForResponse(isLifecycleStateResponse);
    const disableMenu = await openParticipantApps(page);
    await directChatRow(disableMenu)
      .getByRole("button", { name: "無効化" })
      .click();
    await page.waitForTimeout(200);
    expect(disableRequestSeen).toBe(false);

    delayedSecondPageList.release();
    expect((await delayedRefreshResponse).status()).toBe(200);
    const disabled = expectInstallation(
      await responseJSON(await disableResponse, 200, webOrigin),
      sessionUserID,
      "disabled",
      "2",
      installed.installationID,
    );
    page.off("request", observeDisableRequest);
    await expect
      .poll(delayedSecondPageList.requestCount)
      .toBeGreaterThanOrEqual(2);
    await delayedSecondPageList.dispose();
    expect(disabled.installationID).toBe(installed.installationID);
    await closePopovers(page);
    await closePopovers(secondPage);
    await expectLifecycle(page, "直通は無効になっています");
    await expectLifecycle(secondPage, "直通は無効になっています");
    await expect(railButton(page)).toHaveCount(0);
    await expect(railButton(secondPage)).toHaveCount(0);
    await expectScopeSocketsClosed(
      socketObservations,
      installed.installationID,
      "1",
    );
    expect(
      await probeDirectChatScope(page, installed.installationID, "1"),
    ).toEqual({ opened: false, ready: false });

    const enableResponse = secondPage.waitForResponse(isLifecycleStateResponse);
    await secondPage
      .getByRole("button", { name: "有効にする", exact: true })
      .click();
    const enabled = expectInstallation(
      await responseJSON(await enableResponse, 200, webOrigin),
      sessionUserID,
      "enabled",
      "2",
      installed.installationID,
    );
    await expectChatReady(secondPage);
    await expect(railButton(secondPage)).toBeVisible();
    await expectSocketScope(
      socketObservations,
      "second",
      enabled.installationID,
      "2",
    );
    expect(
      await probeDirectChatScope(page, enabled.installationID, "1"),
    ).toEqual({ opened: false, ready: false });
    await expectChatReady(page);
    await expect(railButton(page)).toBeVisible({ timeout: 30_000 });
    await expectSocketScope(
      socketObservations,
      "first",
      enabled.installationID,
      "2",
    );

    page.once("dialog", (dialog) => void dialog.accept());
    const uninstallResponse = page.waitForResponse(isUninstallResponse);
    const uninstallMenu = await openParticipantApps(page);
    await directChatRow(uninstallMenu)
      .getByRole("button", { name: "Direct Chatをアンインストール" })
      .click();
    expect((await uninstallResponse).status()).toBe(204);
    await closePopovers(page);
    await expectLifecycle(page, "直通はまだ導入されていません");
    await expectLifecycle(secondPage, "直通はまだ導入されていません");
    await expect(railButton(page)).toHaveCount(0);
    await expect(railButton(secondPage)).toHaveCount(0);
    await expectScopeSocketsClosed(
      socketObservations,
      enabled.installationID,
      "2",
    );
    expect(
      await probeDirectChatScope(page, enabled.installationID, "2"),
    ).toEqual({ opened: false, ready: false });

    const reinstallResponse = secondPage.waitForResponse(isInstallResponse);
    await secondPage
      .getByRole("button", { name: "直通を導入", exact: true })
      .click();
    const reinstalled = expectInstallation(
      await responseJSON(await reinstallResponse, 201, webOrigin),
      sessionUserID,
      "enabled",
      "1",
    );
    expect(reinstalled.installationID).toMatch(UUIDv7Pattern);
    expect(reinstalled.installationID).not.toBe(installed.installationID);
    await expectChatReady(secondPage);
    await expectSocketScope(
      socketObservations,
      "second",
      reinstalled.installationID,
      "1",
    );
    await expectChatReady(page);
    await expect(railButton(page)).toBeVisible({ timeout: 30_000 });
    await expectSocketScope(
      socketObservations,
      "first",
      reinstalled.installationID,
      "1",
    );

    await sendMessage(secondPage, secondUserMessage);
    for (const currentPage of [page, secondPage]) {
      await expect(
        currentPage.getByText(secondProviderResponse, { exact: true }),
      ).toBeVisible({ timeout: 30_000 });
      await expect(
        currentPage.getByText(firstProviderResponse, { exact: true }),
      ).toBeVisible();
      await expect(
        currentPage.getByText(secondUserMessage, { exact: true }),
      ).toHaveCount(1);
    }
    await expect
      .poll(() => assistantMessageEndSequence.get(secondProviderResponse))
      .toBeGreaterThan(0);
    expect(stack.provider.requestCount).toBe(2);
    expect(stack.provider.requests).toHaveLength(2);
    expect(stack.provider.contextVerified).toBe(true);
    expect(
      assistantMessageEndSequence.get(secondProviderResponse),
    ).toBeGreaterThan(
      assistantMessageEndSequence.get(firstProviderResponse) ?? 0,
    );

    for (const observation of socketObservations) {
      expect(observation.host).toBe(new URL(stack.webURL).host);
      expect(observation.pathname).toBe("/direct-chat/ws");
    }
  } catch (error) {
    const pageStates = await Promise.all(
      [page, secondPage].map(async (currentPage) => ({
        url: currentPage.url(),
        text: await currentPage
          .locator("body")
          .innerText()
          .catch(() => "<page body unavailable>"),
      })),
    );
    primaryError = new Error(
      `${error instanceof Error ? error.message : String(error)}\n\nPage state:\n${pageStates
        .map(
          (state, index) =>
            `page ${index + 1}: ${state.url}\n${state.text.slice(0, 8_192)}`,
        )
        .join(
          "\n\n",
        )}\n\nDirect-chat frame diagnostics (types and sequences only):\n${directChatDiagnostics.join("\n")}\n\nRedacted child diagnostics:\n${stack.diagnostics()}`,
      { cause: error },
    );
  }
  const cleanupErrors: Error[] = [];
  try {
    await secondPage.close();
  } catch (error) {
    cleanupErrors.push(
      error instanceof Error ? error : new Error(String(error)),
    );
  }
  try {
    await stack.stop();
  } catch (error) {
    cleanupErrors.push(
      error instanceof Error ? error : new Error(String(error)),
    );
  }
  if (primaryError && cleanupErrors.length > 0) {
    throw new AggregateError(
      [primaryError, ...cleanupErrors],
      primaryError.message,
      { cause: primaryError },
    );
  }
  if (primaryError) throw primaryError;
  if (cleanupErrors.length > 0) {
    throw new AggregateError(cleanupErrors, "real-agent cleanup failed");
  }
});

interface SocketObservation {
  page: "first" | "second";
  installationID: string;
  authorityEpoch: string;
  host: string;
  pathname: string;
  closed: boolean;
}

const UUIDv7Pattern =
  /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

function observeDirectChat(
  page: Page,
  pageName: "first" | "second",
  observations: SocketObservation[],
  assistantSequences: Map<string, number>,
  diagnostic: (value: string) => void,
) {
  page.on("websocket", (socket) => {
    const url = new URL(socket.url());
    if (url.pathname !== "/direct-chat/ws") return;
    const observation: SocketObservation = {
      page: pageName,
      installationID: url.searchParams.get("installation_id") ?? "",
      authorityEpoch: url.searchParams.get("authority_epoch") ?? "",
      host: url.host,
      pathname: url.pathname,
      closed: false,
    };
    observations.push(observation);
    const number = observations.length;
    diagnostic(
      `${pageName} socket ${number} opened ${observation.installationID}@${observation.authorityEpoch}`,
    );
    socket.on("close", () => {
      observation.closed = true;
      diagnostic(`${pageName} socket ${number} closed`);
    });
    socket.on("framesent", ({ payload }) => {
      if (typeof payload !== "string") return;
      try {
        const frame = JSON.parse(payload) as { type?: unknown };
        diagnostic(`${pageName} socket ${number} sent ${String(frame.type)}`);
      } catch {
        diagnostic(`${pageName} socket ${number} sent malformed JSON`);
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
        diagnostic(
          frame.type === "event"
            ? `${pageName} socket ${number} received event ${String(event?.type)} seq=${String(sequence)}`
            : `${pageName} socket ${number} received ${String(frame.type)}`,
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
            assistantSequences.set(response, Number(sequence));
          }
        }
      } catch {
        // The production browser decoder owns malformed-frame behavior. This
        // observer records only positive durable assistant MessageEnd seqs.
      }
    });
  });
}

function waitForSessionBootstrap(page: Page): Promise<Response> {
  return page.waitForResponse(
    (response) =>
      response.request().method() === "GET" &&
      new URL(response.url()).pathname === "/auth/session",
  );
}

async function expectNormalSession(
  response: Response,
  webOrigin: string,
): Promise<string> {
  expect(response.status()).toBe(200);
  expect(new URL(response.url()).origin).toBe(webOrigin);
  const body = asRecord(await response.json());
  expect(body.authenticated).toBe(true);
  expect(typeof body.authority_binding_id).toBe("string");
  return asString(asRecord(body.user).id);
}

async function openParticipantApps(page: Page) {
  const settings = page.getByRole("dialog", { name: "設定" });
  if (!(await settings.isVisible().catch(() => false))) {
    await page.getByRole("button", { name: "設定" }).click();
    await expect(settings).toBeVisible();
  }
  const apps = page.getByRole("dialog", { name: "個人用アプリ" });
  if (!(await apps.isVisible().catch(() => false))) {
    await settings.getByRole("button", { name: "個人用アプリ" }).click();
    await expect(apps).toBeVisible();
  }
  return apps;
}

function directChatRow(menu: ReturnType<Page["getByRole"]>) {
  return menu.getByText("Direct Chat", { exact: true }).locator("xpath=../..");
}

async function closePopovers(page: Page) {
  await page.keyboard.press("Escape");
  await page.keyboard.press("Escape");
}

function railButton(page: Page) {
  return page.getByRole("button", { name: "直通", exact: true });
}

async function openDirectChat(page: Page) {
  await railButton(page).click();
  await expectChatReady(page);
}

async function expectChatReady(page: Page) {
  await expect(
    page.getByText("エージェント利用可能", { exact: true }),
  ).toBeVisible({ timeout: 45_000 });
}

async function expectLifecycle(page: Page, title: string) {
  await expect(page.getByRole("heading", { name: title })).toBeVisible({
    timeout: 30_000,
  });
}

async function sendMessage(page: Page, message: string) {
  const composer = page.getByRole("textbox", {
    name: "メッセージ",
    exact: true,
  });
  await composer.fill(message);
  await page.getByRole("button", { name: "送信", exact: true }).click();
}

async function pendingRetryState(page: Page) {
  return page.evaluate(() => {
    const state = (
      window as Window & {
        __sumiDirectChatRetryState?: {
          commandAttempts: string[];
          suppressed: number;
        };
      }
    ).__sumiDirectChatRetryState;
    if (!state) throw new Error("pending-retry state is unavailable");
    return {
      suppressed: state.suppressed,
      attemptCount: state.commandAttempts.length,
      uniqueKeys: new Set(state.commandAttempts).size,
    };
  });
}

async function expectSocketScope(
  observations: SocketObservation[],
  page: SocketObservation["page"],
  installationID: string,
  authorityEpoch: string,
) {
  await expect
    .poll(() =>
      observations.some(
        (observation) =>
          observation.page === page &&
          observation.installationID === installationID &&
          observation.authorityEpoch === authorityEpoch &&
          !observation.closed,
      ),
    )
    .toBe(true);
}

async function expectScopeSocketsClosed(
  observations: SocketObservation[],
  installationID: string,
  authorityEpoch: string,
) {
  await expect
    .poll(() => {
      const matching = observations.filter(
        (observation) =>
          observation.installationID === installationID &&
          observation.authorityEpoch === authorityEpoch,
      );
      return matching.length >= 2 && matching.every((entry) => entry.closed);
    })
    .toBe(true);
}

async function probeDirectChatScope(
  page: Page,
  installationID: string,
  authorityEpoch: string,
) {
  return page.evaluate(
    ({ installationID: id, authorityEpoch: epoch }) =>
      new Promise<{ opened: boolean; ready: boolean }>((resolve, reject) => {
        const url = new URL("/direct-chat/ws", window.location.origin);
        url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
        url.searchParams.set("installation_id", id);
        url.searchParams.set("authority_epoch", epoch);
        const socket = new WebSocket(url);
        let opened = false;
        let ready = false;
        const timeout = window.setTimeout(() => {
          socket.close();
          reject(new Error("obsolete Direct Chat scope did not close"));
        }, 10_000);
        socket.onopen = () => {
          opened = true;
          socket.send(JSON.stringify({ type: "hello", last_event_seq: 0 }));
        };
        socket.onmessage = (event) => {
          if (typeof event.data !== "string") return;
          try {
            const frame = JSON.parse(event.data) as {
              type?: unknown;
              status?: unknown;
            };
            if (
              frame.type === "direct_chat_status" &&
              frame.status === "ready"
            ) {
              ready = true;
            }
          } catch {
            // A production rejection may close without a protocol frame.
          }
        };
        socket.onclose = () => {
          window.clearTimeout(timeout);
          resolve({ opened, ready });
        };
        socket.onerror = () => {
          // The close event reports the expected handshake rejection.
        };
      }),
    { installationID, authorityEpoch },
  );
}

async function holdNextParticipantInstallationList(page: Page): Promise<{
  reached: Promise<void>;
  release: () => void;
  requestCount: () => number;
  dispose: () => Promise<void>;
}> {
  const pattern = "**/app-installations?**";
  const reached = deferred<void>();
  const release = deferred<void>();
  let requestCount = 0;
  let held = false;
  const handler = async (route: Route) => {
    if (!isParticipantInstallationListRequest(route.request())) {
      await route.continue();
      return;
    }
    requestCount += 1;
    if (held) {
      await route.continue();
      return;
    }
    held = true;
    const response = await route.fetch();
    reached.resolve(undefined);
    await release.promise;
    await route.fulfill({ response });
  };
  await page.route(pattern, handler);
  return {
    reached: reached.promise,
    release: () => release.resolve(undefined),
    requestCount: () => requestCount,
    dispose: () => page.unroute(pattern, handler),
  };
}

function isInstallResponse(response: Response) {
  return (
    response.request().method() === "POST" &&
    new URL(response.url()).pathname === "/app-installations"
  );
}

function isLifecycleStateResponse(response: Response) {
  return (
    response.request().method() === "PUT" &&
    /^\/app-installations\/[^/]+\/state$/.test(new URL(response.url()).pathname)
  );
}

function isUninstallResponse(response: Response) {
  return (
    response.request().method() === "DELETE" &&
    /^\/app-installations\/[^/]+$/.test(new URL(response.url()).pathname)
  );
}

function isParticipantInstallationListResponse(response: Response) {
  return isParticipantInstallationList(
    response.request().method(),
    response.url(),
  );
}

function isParticipantInstallationListRequest(request: Request) {
  return isParticipantInstallationList(request.method(), request.url());
}

function isParticipantInstallationList(method: string, rawURL: string) {
  const url = new URL(rawURL);
  return (
    method === "GET" &&
    url.pathname === "/app-installations" &&
    url.searchParams.get("owner_kind") === "participant" &&
    url.searchParams.get("participant_kind") === "human"
  );
}

async function responseJSON(
  response: Response,
  status: number,
  webOrigin: string,
) {
  expect(response.status()).toBe(status);
  expect(new URL(response.url()).origin).toBe(webOrigin);
  return response.json();
}

function expectInstallation(
  value: unknown,
  humanID: string,
  state: "enabled" | "disabled",
  authorityEpoch: string,
  expectedInstallationID?: string,
) {
  const installation = asRecord(value);
  const owner = asRecord(installation.owner);
  const participant = asRecord(owner.participant);
  expect(owner.kind).toBe("participant");
  expect(participant.kind).toBe("human");
  expect(participant.human_id).toBe(humanID);
  expect(installation.app_id).toBe("direct-chat");
  expect(installation.state).toBe(state);
  expect(installation.authority_epoch).toBe(authorityEpoch);
  const installationID = asString(installation.installation_id);
  if (expectedInstallationID !== undefined) {
    expect(installationID).toBe(expectedInstallationID);
  }
  return { installationID };
}

function asRecord(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("expected JSON object");
  }
  return value as Record<string, unknown>;
}

function asString(value: unknown): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error("expected non-empty string");
  }
  return value;
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((settle) => {
    resolve = settle;
  });
  return { promise, resolve };
}
