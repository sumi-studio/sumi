import { expect, test } from "@playwright/test";
import {
  buildWorkspaceBrowserStack,
  removeWorkspaceBrowserBuild,
  startWorkspaceBrowserStack,
  type WorkspaceBrowserBuild,
} from "./support/real-agent-stack";

test.describe.configure({ timeout: 300_000 });
test.use({ actionTimeout: 10_000 });

let build: WorkspaceBrowserBuild | undefined;

test.beforeAll(async () => {
  test.setTimeout(180_000);
  build = await buildWorkspaceBrowserStack();
});

test.afterAll(async () => {
  if (build) await removeWorkspaceBrowserBuild(build);
});

test("Human with membership 0 creates isolated Workspaces and uses installed Messaging", async ({
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
  const liveWorkspaceIds = new Set<string>();
  const liveMessagingScopes = new Set<string>();
  const malformedMessagingSocketScopes: string[] = [];
  const liveMessageContents = new Set<string>();
  const websocketErrors: string[] = [];
  const expectedRejectedWebSocketURLs = new Set<string>();
  let primaryError: Error | undefined;
  try {
    page.on("websocket", (socket) => {
      if (!socket.url().includes("/messaging/ws")) return;
      socket.on("framereceived", (frame) => {
        if (typeof frame.payload !== "string") return;
        try {
          const payload = asRecord(JSON.parse(frame.payload) as unknown);
          if (payload.type === "hello_ack") {
            const query = new URL(socket.url()).searchParams;
            const workspaceIds = query.getAll("workspace_id");
            const installationIds = query.getAll("installation_id");
            const authorityEpochs = query.getAll("authority_epoch");
            if (
              workspaceIds.length !== 1 ||
              installationIds.length !== 1 ||
              authorityEpochs.length !== 1
            ) {
              malformedMessagingSocketScopes.push(socket.url());
              return;
            }
            const workspaceId = workspaceIds[0];
            if (workspaceId) liveWorkspaceIds.add(workspaceId);
            const installationId = installationIds[0];
            const authorityEpoch = authorityEpochs[0];
            if (workspaceId && installationId && authorityEpoch) {
              liveMessagingScopes.add(
                `${workspaceId}:${installationId}:${authorityEpoch}`,
              );
            }
          }
          if (payload.type === "event") {
            const event = asRecord(payload.event);
            if (event.type === "message_created") {
              const message = asRecord(event.message);
              liveMessageContents.add(asString(message.content));
            }
          }
        } catch {
          // Production client owns protocol validation; this observer records
          // only the frames needed to prove the live journey.
        }
      });
      socket.on("socketerror", (error) => {
        if (expectedRejectedWebSocketURLs.has(socket.url())) return;
        websocketErrors.push(String(error));
      });
    });
    await stack.installSession(context);
    await page.goto(stack.webURL);

    await expect(
      page.getByRole("heading", { name: "どこで一緒に働きますか" }),
    ).toBeVisible();
    await expect(
      page.getByText("まだWorkspaceに参加していません"),
    ).toBeVisible();
    await expect(page.getByText("0件", { exact: true })).toBeVisible();

    const alpha = await createWorkspace(page, "Alpha Studio");
    await expect(page).toHaveURL(`${stack.webURL}/w/${alpha.workspaceID}`);
    await expect(page.getByRole("heading", { name: "概要" })).toBeVisible();

    await page.reload();
    await expect(page).toHaveURL(`${stack.webURL}/w/${alpha.workspaceID}`);
    await expect(page.getByRole("heading", { name: "概要" })).toBeVisible();
    await expect(
      page.getByText("Alpha Studio", { exact: true }).first(),
    ).toBeVisible();

    await page.getByRole("button", { name: "参加者と招待" }).click();
    await expect(
      page.getByText("Human · 0000e2e0", { exact: true }),
    ).toBeVisible();

    const currentInvitePath = `/workspaces/${alpha.workspaceID}/invites/current-agent`;
    const createCurrentInviteResponse = page.waitForResponse(
      (response) =>
        response.request().method() === "POST" &&
        new URL(response.url()).pathname === currentInvitePath,
    );
    await page.getByRole("button", { name: "招待する" }).click();
    const currentInviteCreated = await createCurrentInviteResponse;
    expect(currentInviteCreated.status()).toBe(201);
    const currentInvite = assertTargetedInvitePayload(
      await currentInviteCreated.json(),
      alpha.workspaceID,
    );
    await expect(
      page.getByText("招待済み・承諾待ち", { exact: true }),
    ).toBeVisible();
    await expect(
      page.getByText("Direct Chatで招待を確認してもらってください。", {
        exact: true,
      }),
    ).toBeVisible();

    const reloadedCurrentInviteResponse = page.waitForResponse(
      (response) =>
        response.request().method() === "GET" &&
        new URL(response.url()).pathname === currentInvitePath,
    );
    await page.reload();
    const currentInviteReloaded = await reloadedCurrentInviteResponse;
    expect(currentInviteReloaded.status()).toBe(200);
    expect(
      assertTargetedInvitePayload(
        await currentInviteReloaded.json(),
        alpha.workspaceID,
      ).inviteID,
    ).toBe(currentInvite.inviteID);
    await page.getByRole("button", { name: "参加者と招待" }).click();
    await expect(
      page.getByText("招待済み・承諾待ち", { exact: true }),
    ).toBeVisible();

    const replay = await page.evaluate(async (path) => {
      const response = await fetch(path, {
        method: "POST",
        credentials: "include",
        cache: "no-store",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
        },
        body: "{}",
      });
      return {
        status: response.status,
        body: (await response.json()) as unknown,
      };
    }, currentInvitePath);
    expect(replay.status).toBe(200);
    expect(
      assertTargetedInvitePayload(replay.body, alpha.workspaceID).inviteID,
    ).toBe(currentInvite.inviteID);

    const revokeCurrentInviteResponse = page.waitForResponse(
      (response) =>
        response.request().method() === "DELETE" &&
        new URL(response.url()).pathname ===
          `/workspaces/${alpha.workspaceID}/invites/${currentInvite.inviteID}`,
    );
    await page
      .getByRole("button", {
        name: "Direct Chatの相手への招待を取り消す",
      })
      .click();
    expect((await revokeCurrentInviteResponse).status()).toBe(204);
    await expect(page.getByRole("button", { name: "招待する" })).toBeVisible();
    const [currentAfterRevoke, registryAfterRevoke] = await Promise.all([
      page.request.get(`${stack.apiURL}${currentInvitePath}`),
      page.request.get(
        `${stack.apiURL}/workspaces/${alpha.workspaceID}/invites`,
      ),
    ]);
    expect(currentAfterRevoke.status()).toBe(404);
    expect(registryAfterRevoke.status()).toBe(200);
    const registry = asRecord(await registryAfterRevoke.json());
    const activeInviteIDs = asArray(registry.invites).map((entry) =>
      asString(asRecord(entry).invite_id),
    );
    expect(activeInviteIDs).not.toContain(currentInvite.inviteID);

    const createInviteResponse = page.waitForResponse(
      (response) =>
        response.request().method() === "POST" &&
        new URL(response.url()).pathname.endsWith("/invites"),
    );
    await page.getByRole("button", { name: "招待を作成" }).click();
    expect((await createInviteResponse).status()).toBe(201);
    await expect(
      page.getByRole("textbox", { name: "招待コード" }),
    ).toBeVisible();
    const revokeInviteResponse = page.waitForResponse(
      (response) =>
        response.request().method() === "DELETE" &&
        new URL(response.url()).pathname.includes("/invites/"),
    );
    await page.getByRole("button", { name: /を取り消す$/ }).click();
    expect((await revokeInviteResponse).status()).toBe(204);
    await expect(
      page.getByText("有効な招待はありません。", { exact: true }),
    ).toBeVisible();

    await page.getByRole("button", { name: "ロール", exact: true }).click();
    await page.getByRole("button", { name: "ロールを作成" }).click();
    await page.getByRole("textbox", { name: "ロール名" }).fill("Observer");
    await page.getByRole("button", { name: "作成", exact: true }).click();
    await expect(page.getByRole("heading", { name: "Observer" })).toBeVisible();

    const alphaInstallation = await installMessaging(page);
    const { installationID: alphaInstallationID, authorityEpoch: alphaEpoch } =
      alphaInstallation;
    await page.getByRole("button", { name: "開く", exact: true }).click();
    await expect.poll(() => liveWorkspaceIds.has(alpha.workspaceID)).toBe(true);
    await expect(
      page.getByText("場所はまだありません", { exact: true }),
    ).toBeVisible();
    await createChannelAndSend(page, "alpha-general", "alpha-only-message");
    await expect
      .poll(() => liveMessageContents.has("alpha-only-message"))
      .toBe(true);
    await page.reload();
    await expect(
      page.getByText("alpha-only-message", { exact: true }),
    ).toBeVisible();

    await openWorkspaceList(page);
    const beta = await createWorkspace(page, "Beta Studio");
    await installMessaging(page);
    await page.getByRole("button", { name: "開く", exact: true }).click();
    await expect.poll(() => liveWorkspaceIds.has(beta.workspaceID)).toBe(true);
    await expect(page.getByText("alpha-general", { exact: true })).toHaveCount(
      0,
    );
    await expect(
      page.getByText("alpha-only-message", { exact: true }),
    ).toHaveCount(0);
    await createChannelAndSend(page, "beta-general", "beta-only-message");
    await expect
      .poll(() => liveMessageContents.has("beta-only-message"))
      .toBe(true);

    await switchWorkspace(page, "Alpha Studio");
    await page.getByRole("button", { name: "Messaging", exact: true }).click();
    await page.getByText("alpha-general", { exact: true }).click();
    await expect(
      page.getByText("alpha-only-message", { exact: true }),
    ).toBeVisible();
    await expect(page.getByText("beta-general", { exact: true })).toHaveCount(
      0,
    );
    await expect(
      page.getByText("beta-only-message", { exact: true }),
    ).toHaveCount(0);

    await page.getByRole("button", { name: "Workspace", exact: true }).click();
    await page.getByRole("button", { name: "アプリ", exact: true }).click();
    const disabled = page.waitForResponse(
      (response) =>
        response.request().method() === "PUT" &&
        new URL(response.url()).pathname ===
          `/app-installations/${alphaInstallationID}/state`,
    );
    await page.getByRole("button", { name: "無効にする" }).click();
    const disabledInstallation = asRecord(await (await disabled).json());
    const disabledEpoch = asString(disabledInstallation.authority_epoch);
    expect(disabledEpoch).not.toBe(alphaEpoch);
    await expectExactScopeStatus(
      page,
      stack.apiURL,
      alpha.workspaceID,
      alphaInstallationID,
      alphaEpoch,
      404,
    );
    await expectExactScopeWebSocketReject(
      page,
      stack.apiURL,
      alpha.workspaceID,
      alphaInstallationID,
      alphaEpoch,
      expectedRejectedWebSocketURLs,
    );
    await page.goto(`${stack.webURL}/w/${alpha.workspaceID}/messaging`);
    await expect(
      page.getByText("Messagingは無効になっています", { exact: true }),
    ).toBeVisible();
    const reenabled = page.waitForResponse(
      (response) =>
        response.request().method() === "PUT" &&
        new URL(response.url()).pathname ===
          `/app-installations/${alphaInstallationID}/state`,
    );
    await page.getByRole("button", { name: "有効にする" }).click();
    const reenabledInstallation = asRecord(await (await reenabled).json());
    const alphaCurrentEpoch = asString(reenabledInstallation.authority_epoch);
    expect(alphaCurrentEpoch).toBe(disabledEpoch);
    await expectExactScopeStatus(
      page,
      stack.apiURL,
      alpha.workspaceID,
      alphaInstallationID,
      alphaEpoch,
      404,
    );
    await expectExactScopeStatus(
      page,
      stack.apiURL,
      alpha.workspaceID,
      alphaInstallationID,
      alphaCurrentEpoch,
      200,
    );
    await page.getByText("alpha-general", { exact: true }).click();
    await expect
      .poll(() =>
        liveMessagingScopes.has(
          `${alpha.workspaceID}:${alphaInstallationID}:${alphaCurrentEpoch}`,
        ),
      )
      .toBe(true);
    await expect(
      page.getByText("alpha-only-message", { exact: true }),
    ).toBeVisible();

    await page.getByRole("button", { name: "Workspace", exact: true }).click();
    await page.getByRole("button", { name: "アプリ", exact: true }).click();
    page.once("dialog", (dialog) => void dialog.accept());
    await page.getByRole("button", { name: "アンインストール" }).click();
    await expectExactScopeStatus(
      page,
      stack.apiURL,
      alpha.workspaceID,
      alphaInstallationID,
      alphaCurrentEpoch,
      404,
    );
    await page.goto(`${stack.webURL}/w/${alpha.workspaceID}/messaging`);
    await expect(
      page.getByText("Messagingはまだインストールされていません", {
        exact: true,
      }),
    ).toBeVisible();
    const replacementInstallation = page.waitForResponse(
      (response) =>
        response.request().method() === "POST" &&
        new URL(response.url()).pathname === "/app-installations",
    );
    await page.getByRole("button", { name: "Messagingをインストール" }).click();
    const replacementResponse = await replacementInstallation;
    expect(replacementResponse.status()).toBe(201);
    const replacement = asRecord(await replacementResponse.json());
    const replacementInstallationID = asString(replacement.installation_id);
    const replacementEpoch = asString(replacement.authority_epoch);
    expect(replacementInstallationID).not.toBe(alphaInstallationID);
    await expectExactScopeStatus(
      page,
      stack.apiURL,
      alpha.workspaceID,
      alphaInstallationID,
      alphaCurrentEpoch,
      404,
    );
    await expectExactScopeStatus(
      page,
      stack.apiURL,
      alpha.workspaceID,
      replacementInstallationID,
      replacementEpoch,
      200,
    );
    await page.getByText("alpha-general", { exact: true }).click();
    await expect
      .poll(() =>
        liveMessagingScopes.has(
          `${alpha.workspaceID}:${replacementInstallationID}:${replacementEpoch}`,
        ),
      )
      .toBe(true);
    await expect(
      page.getByText("alpha-only-message", { exact: true }),
    ).toBeVisible();

    await switchWorkspace(page, "Beta Studio");
    await page.getByRole("button", { name: "Messaging", exact: true }).click();
    await page.getByText("beta-general", { exact: true }).click();
    await expect(
      page.getByText("beta-only-message", { exact: true }),
    ).toBeVisible();
    await expect(
      page.getByText("alpha-only-message", { exact: true }),
    ).toHaveCount(0);
    expect(beta.workspaceID).not.toBe(alpha.workspaceID);
    expect(malformedMessagingSocketScopes).toEqual([]);
    expect(websocketErrors).toEqual([]);
  } catch (error) {
    const visibleText = await page
      .locator("body")
      .innerText()
      .catch(() => "<page body unavailable>");
    primaryError = new Error(
      `${error instanceof Error ? error.message : String(error)}\n\nBrowser URL:\n${page.url()}\n\nVisible page text:\n${visibleText.slice(0, 8_192)}\n\nRedacted child diagnostics:\n${stack.diagnostics()}`,
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
      {
        cause: primaryError,
      },
    );
  }
  if (primaryError) throw primaryError;
  if (cleanupError) throw cleanupError;
});

async function createWorkspace(
  page: import("@playwright/test").Page,
  name: string,
) {
  await page.getByRole("textbox", { name: "新しいWorkspaceの名前" }).fill(name);
  const responsePromise = page.waitForResponse(
    (response) =>
      response.request().method() === "POST" &&
      new URL(response.url()).pathname === "/workspaces",
  );
  await page.getByRole("button", { name: "作成して開く" }).click();
  const response = await responsePromise;
  expect(response.status()).toBe(201);
  const body = asRecord(await response.json());
  return { workspaceID: asString(body.workspace_id) };
}

async function installMessaging(page: import("@playwright/test").Page) {
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

async function createChannelAndSend(
  page: import("@playwright/test").Page,
  channel: string,
  message: string,
) {
  await page.getByTitle("チャンネルを作成").click();
  const dialog = page.getByRole("dialog", { name: "チャンネルを作成" });
  await dialog
    .getByRole("textbox", { name: "名前", exact: true })
    .fill(channel);
  await dialog.getByRole("button", { name: "作成", exact: true }).click();
  const composer = page.getByRole("textbox", {
    name: `#${channel} へメッセージ`,
  });
  await composer.fill(message);
  await composer.press("Enter");
  await expect(page.getByText(message, { exact: true })).toBeVisible();
}

async function openWorkspaceList(page: import("@playwright/test").Page) {
  await page.getByRole("button", { name: "Workspaceを切り替える" }).click();
  await page.getByRole("button", { name: "Workspace一覧" }).click();
  await expect(
    page.getByRole("heading", { name: "どこで一緒に働きますか" }),
  ).toBeVisible();
}

async function switchWorkspace(
  page: import("@playwright/test").Page,
  workspaceName: string,
) {
  await page.getByRole("button", { name: "Workspaceを切り替える" }).click();
  const switcher = page.getByRole("dialog", {
    name: "Workspaceを切り替える",
  });
  await switcher.getByRole("button").filter({ hasText: workspaceName }).click();
  await expect(page.getByRole("heading", { name: "概要" })).toBeVisible();
}

async function expectExactScopeStatus(
  page: import("@playwright/test").Page,
  apiURL: string,
  workspaceID: string,
  installationID: string,
  authorityEpoch: string,
  expected: number,
) {
  const query = new URLSearchParams({
    workspace_id: workspaceID,
    installation_id: installationID,
    authority_epoch: authorityEpoch,
  });
  const response = await page.request.get(
    `${apiURL}/messaging/bootstrap?${query.toString()}`,
  );
  expect(response.status()).toBe(expected);
}

async function expectExactScopeWebSocketReject(
  page: import("@playwright/test").Page,
  apiURL: string,
  workspaceID: string,
  installationID: string,
  authorityEpoch: string,
  expectedRejectedWebSocketURLs: Set<string>,
) {
  const query = new URLSearchParams({
    workspace_id: workspaceID,
    installation_id: installationID,
    authority_epoch: authorityEpoch,
  });
  const websocketURL = `${apiURL.replace(/^http/, "ws")}/messaging/ws?${query.toString()}`;
  expectedRejectedWebSocketURLs.add(websocketURL);
  const rejected = await page.evaluate(async (url) => {
    return await new Promise<boolean>((resolve) => {
      const socket = new WebSocket(url);
      const timeout = window.setTimeout(() => {
        socket.close();
        resolve(false);
      }, 5_000);
      socket.addEventListener("open", () => {
        window.clearTimeout(timeout);
        socket.close();
        resolve(false);
      });
      socket.addEventListener("close", () => {
        window.clearTimeout(timeout);
        resolve(true);
      });
    });
  }, websocketURL);
  expect(rejected).toBe(true);
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

function asArray(value: unknown): unknown[] {
  if (!Array.isArray(value)) throw new Error("expected JSON array");
  return value;
}

function assertTargetedInvitePayload(
  value: unknown,
  workspaceID: string,
): { inviteID: string } {
  const body = asRecord(value);
  expect(Object.keys(body).sort()).toEqual([
    "created_at",
    "expires_at",
    "invite_id",
    "kind",
    "workspace_id",
  ]);
  expect(body.kind).toBe("targeted_personality_agent");
  expect(body.workspace_id).toBe(workspaceID);
  for (const forbidden of [
    "personality_agent_id",
    "target_id",
    "target_kind",
    "code",
    "code_hash",
  ]) {
    expect(body).not.toHaveProperty(forbidden);
  }
  return { inviteID: asString(body.invite_id) };
}
