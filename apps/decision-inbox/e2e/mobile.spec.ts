import { expect, test } from "@playwright/test";

test("expired offline session clears saved private data", async ({ page }) => {
  await page.goto("/");
  await page.evaluate(() => {
    localStorage.setItem(
      "sumi-decision-inbox:v1:session",
      JSON.stringify({
        expiresAt: new Date(Date.now() - 1_000).toISOString(),
        vapidPublicKey: "saved-key",
        pushSubscriptionCount: 0,
      }),
    );
    localStorage.setItem(
      "sumi-decision-inbox:v1:list:pending",
      JSON.stringify([{ id: "saved-private-decision" }]),
    );
  });
  await page.evaluate(() => navigator.serviceWorker.ready);
  await page.context().setOffline(true);
  await page.reload();
  await expect(
    page.getByRole("heading", { name: "Decision inbox" }),
  ).toBeVisible();
  await expect(
    page.getByText("saved-private-decision", { exact: false }),
  ).not.toBeVisible();
  expect(
    await page.evaluate(() =>
      Object.keys(localStorage).filter((key) =>
        key.startsWith("sumi-decision-inbox:v1:"),
      ),
    ),
  ).toEqual([]);
  await page.context().setOffline(false);
});

test("a successful bootstrap clears an earlier private cached inbox", async ({
  page,
  request,
}) => {
  const minted = await request.post("/api/publisher/bootstrap-tokens", {
    headers: { Authorization: "Bearer local-publisher-token-change-me" },
    data: { expiresInSeconds: 600 },
  });
  expect(minted.status()).toBe(201);
  const { bootstrapToken } = (await minted.json()) as {
    bootstrapToken: string;
  };
  await page.addInitScript(() => {
    localStorage.setItem(
      "sumi-decision-inbox:v1:session",
      JSON.stringify({ expiresAt: "2099-01-01T00:00:00.000Z" }),
    );
    localStorage.setItem(
      "sumi-decision-inbox:v1:list:pending",
      JSON.stringify([{ id: "other-user-decision" }]),
    );
    localStorage.setItem(
      "sumi-decision-inbox:v1:request:other-user-decision",
      JSON.stringify({ id: "other-user-decision" }),
    );
  });
  await page.goto(`/#bootstrap=${encodeURIComponent(bootstrapToken)}`);
  await expect(
    page.getByRole("heading", { name: "Needs your call" }),
  ).toBeVisible();
  expect(
    await page.evaluate(() => ({
      session: localStorage.getItem("sumi-decision-inbox:v1:session"),
      cachedInbox: localStorage.getItem("sumi-decision-inbox:v1:list:pending"),
      oldDetail: localStorage.getItem(
        "sumi-decision-inbox:v1:request:other-user-decision",
      ),
    })),
  ).toEqual({
    session: expect.stringMatching(/^\{(?:(?!csrfToken).)*\}$/),
    cachedInbox: expect.not.stringContaining("other-user-decision"),
    oldDetail: null,
  });
});

test("a manual private code clears an earlier private cached inbox", async ({
  page,
  request,
}) => {
  const minted = await request.post("/api/publisher/bootstrap-tokens", {
    headers: { Authorization: "Bearer local-publisher-token-change-me" },
    data: { expiresInSeconds: 600 },
  });
  expect(minted.status()).toBe(201);
  const { bootstrapToken } = (await minted.json()) as {
    bootstrapToken: string;
  };
  await page.addInitScript(() => {
    localStorage.setItem(
      "sumi-decision-inbox:v1:list:pending",
      JSON.stringify([{ id: "other-user-decision" }]),
    );
    localStorage.setItem(
      "sumi-decision-inbox:v1:request:other-user-decision",
      JSON.stringify({ id: "other-user-decision" }),
    );
  });
  await page.goto("/");
  await page.getByLabel("One-time code").fill(bootstrapToken);
  await page.getByRole("button", { name: "Open inbox" }).click();
  await expect(
    page.getByRole("heading", { name: "Needs your call" }),
  ).toBeVisible();
  expect(
    await page.evaluate(() => ({
      cachedInbox: localStorage.getItem("sumi-decision-inbox:v1:list:pending"),
      oldDetail: localStorage.getItem(
        "sumi-decision-inbox:v1:request:other-user-decision",
      ),
    })),
  ).toEqual({
    cachedInbox: expect.not.stringContaining("other-user-decision"),
    oldDetail: null,
  });
});

test("an offline cached session revalidates before it can send a decision", async ({
  page,
  request,
}) => {
  const created = await request.post("/api/publisher/requests", {
    headers: {
      Authorization: "Bearer local-publisher-token-change-me",
      "Idempotency-Key": `playwright-reconnect-${Date.now()}`,
    },
    data: {
      title: "Reconnect before deciding",
      body: "This decision must recover the in-memory CSRF token first.",
      source: "Codex · Developer Workspace",
      choices: [
        { id: "continue", label: "Continue", tone: "positive" },
        { id: "wait", label: "Wait", tone: "neutral" },
      ],
      allowFreeText: false,
      expiresAt: new Date(Date.now() + 3_600_000).toISOString(),
    },
  });
  expect(created.status()).toBe(201);
  const {
    request: { id },
  } = (await created.json()) as { request: { id: string } };
  const minted = await request.post("/api/publisher/bootstrap-tokens", {
    headers: { Authorization: "Bearer local-publisher-token-change-me" },
    data: { expiresInSeconds: 600 },
  });
  const { bootstrapToken } = (await minted.json()) as {
    bootstrapToken: string;
  };
  await page.goto(`/#bootstrap=${encodeURIComponent(bootstrapToken)}`);
  await page.getByText("Reconnect before deciding").click();
  await expect(
    page.getByRole("heading", { name: "Reconnect before deciding" }),
  ).toBeVisible();
  await expect(
    page.evaluate(
      (requestId) =>
        localStorage.getItem(`sumi-decision-inbox:v1:request:${requestId}`),
      id,
    ),
  ).resolves.toBeTruthy();
  await page.evaluate(() => navigator.serviceWorker.ready);
  await page.context().setOffline(true);
  await page.reload();
  await expect(page.getByRole("button", { name: "Continue" })).toBeDisabled();
  await page.context().setOffline(false);
  await expect(page.getByRole("button", { name: "Continue" })).toBeEnabled();
  await page.getByRole("button", { name: "Continue" }).click();
  await page.getByRole("button", { name: "Send decision" }).click();
  await expect(page.getByRole("heading", { name: "Continue" })).toBeVisible();
  await expect(page).toHaveURL(new RegExp(`/requests/${id}$`));
});

test("an open cached session expires without a reload", async ({ page }) => {
  const id = "cached-expiry-decision-12345";
  await page.goto("/");
  await expect(
    page.getByRole("heading", { name: "Decision inbox" }),
  ).toBeVisible();
  await page.evaluate((cachedId) => {
    const expiresAt = new Date(Date.now() + 6_000).toISOString();
    localStorage.setItem(
      "sumi-decision-inbox:v1:session",
      JSON.stringify({ expiresAt, pushSubscriptionCount: 0 }),
    );
    localStorage.setItem(
      `sumi-decision-inbox:v1:request:${cachedId}`,
      JSON.stringify({
        id: cachedId,
        title: "Cached decision disappears at expiry",
        body: "This private state must be revoked without a reload.",
        source: "Codex · Developer Workspace",
        choices: [
          { id: "continue", label: "Continue", tone: "positive" },
          { id: "wait", label: "Wait", tone: "neutral" },
        ],
        allowFreeText: false,
        status: "pending",
        expiresAt: new Date(Date.now() + 3_600_000).toISOString(),
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
      }),
    );
    history.replaceState({}, "", `/requests/${cachedId}`);
  }, id);
  await page.evaluate(() => navigator.serviceWorker.ready);
  await page.context().setOffline(true);
  await page.reload();
  await expect(
    page.getByRole("heading", { name: "Cached decision disappears at expiry" }),
  ).toBeVisible();
  await page.waitForTimeout(6_500);
  await page.evaluate(() => window.dispatchEvent(new Event("focus")));
  await expect(
    page.getByRole("heading", { name: "Decision inbox" }),
  ).toBeVisible({ timeout: 8_000 });
  expect(
    await page.evaluate(() =>
      Object.keys(localStorage).filter((key) =>
        key.startsWith("sumi-decision-inbox:v1:"),
      ),
    ),
  ).toEqual([]);
  await page.context().setOffline(false);
});

test("mobile Human can open and resolve an exact request", async ({
  page,
  request,
}, testInfo) => {
  const expiresAt = new Date(Date.now() + 3_600_000).toISOString();
  const created = await request.post("/api/publisher/requests", {
    headers: {
      Authorization: "Bearer local-publisher-token-change-me",
      "Idempotency-Key": `playwright-${Date.now()}`,
    },
    data: {
      title: "Choose today’s integration window",
      body: "The checked release head is ready. Decide whether Codex should integrate it now or wait for the morning review window.",
      source: "Codex · Developer Workspace",
      choices: [
        { id: "now", label: "Integrate now", tone: "positive" },
        { id: "morning", label: "Wait until morning", tone: "neutral" },
        { id: "stop", label: "Stop this change", tone: "destructive" },
      ],
      allowFreeText: true,
      expiresAt,
    },
  });
  expect(created.status()).toBe(201);
  const createdBody = (await created.json()) as { request: { id: string } };

  await page.goto("/#bootstrap=local-human-bootstrap-change-me");
  await expect(
    page.getByRole("heading", { name: "Needs your call" }),
  ).toBeVisible();
  await expect(
    page.getByText("Choose today’s integration window"),
  ).toBeVisible();
  expect(await page.evaluate(() => Notification.permission)).toBe("default");
  await page.screenshot({
    path: testInfo.outputPath("decision-inbox-mobile-list.png"),
    fullPage: true,
  });

  await page.getByText("Choose today’s integration window").click();
  await expect(page).toHaveURL(
    new RegExp(`/requests/${createdBody.request.id}$`),
  );
  await expect(
    page.getByRole("heading", { name: "Choose today’s integration window" }),
  ).toBeVisible();
  await page.screenshot({
    path: testInfo.outputPath("decision-inbox-mobile-detail.png"),
    fullPage: true,
  });

  await page.getByRole("button", { name: "Integrate now" }).click();
  await page
    .getByLabel("Add a short note optional")
    .fill("Use the checked release head.");
  await page.getByRole("button", { name: "Send decision" }).click();
  await expect(
    page.getByRole("heading", { name: "Integrate now" }),
  ).toBeVisible();
  await expect(page.getByText("Use the checked release head.")).toBeVisible();
  await page.screenshot({
    path: testInfo.outputPath("decision-inbox-mobile-resolved.png"),
    fullPage: true,
  });

  await page.evaluate(() => navigator.serviceWorker.ready);
  await page.context().setOffline(true);
  await page.reload();
  await expect(
    page.getByText(
      "Offline · Showing the last saved view. Actions are paused.",
    ),
  ).toBeVisible();
  await expect(
    page.getByRole("heading", { name: "Choose today’s integration window" }),
  ).toBeVisible();
  await page.screenshot({
    path: testInfo.outputPath("decision-inbox-mobile-offline.png"),
    fullPage: true,
  });
  await page.context().setOffline(false);
});

test("renewal tap replaces an expired browser Push subscription", async ({
  page,
  request,
}) => {
  const pending = await request.post("/api/publisher/requests", {
    headers: {
      Authorization: "Bearer local-publisher-token-change-me",
      "Idempotency-Key": `playwright-push-${Date.now()}`,
    },
    data: {
      title: "Keep push setup visible",
      body: "A pending request keeps the compact repair notice visible.",
      source: "Codex · Developer Workspace",
      choices: [
        { id: "now", label: "Continue", tone: "positive" },
        { id: "later", label: "Wait", tone: "neutral" },
      ],
      allowFreeText: false,
      expiresAt: new Date(Date.now() + 3_600_000).toISOString(),
    },
  });
  expect(pending.status()).toBe(201);
  const minted = await request.post("/api/publisher/bootstrap-tokens", {
    headers: { Authorization: "Bearer local-publisher-token-change-me" },
    data: { expiresInSeconds: 600 },
  });
  expect(minted.status()).toBe(201);
  const { bootstrapToken } = (await minted.json()) as {
    bootstrapToken: string;
  };
  const submittedEndpoints: string[] = [];
  page.on("request", (outgoing) => {
    if (
      outgoing.method() === "POST" &&
      outgoing.url().endsWith("/api/human/push-subscriptions")
    ) {
      submittedEndpoints.push(
        (outgoing.postDataJSON() as { endpoint: string }).endpoint,
      );
    }
  });

  await page.addInitScript(() => {
    const staleEndpoint = "https://push.example.invalid/e2e-stale";
    const freshEndpoint = "https://push.example.invalid/e2e-fresh";
    const events: string[] = [];
    let current: {
      endpoint: string;
      toJSON: () => PushSubscriptionJSON;
      unsubscribe: () => Promise<boolean>;
    } | null;
    const stale = {
      endpoint: staleEndpoint,
      toJSON: () => ({
        endpoint: staleEndpoint,
        expirationTime: Date.now() - 1_000,
        keys: {
          auth: "e2e-stale-auth-key",
          p256dh: "e2e-stale-p256dh-key-material",
        },
      }),
      unsubscribe: async () => {
        events.push("unsubscribe:stale");
        current = null;
        return true;
      },
    };
    const fresh = {
      endpoint: freshEndpoint,
      toJSON: () => ({
        endpoint: freshEndpoint,
        expirationTime: null,
        keys: {
          auth: "e2e-fresh-auth-key",
          p256dh: "e2e-fresh-p256dh-key-material",
        },
      }),
      unsubscribe: async () => true,
    };
    current = stale;
    const registration = {
      pushManager: {
        getSubscription: async () => current,
        subscribe: async () => {
          events.push("subscribe:fresh");
          current = fresh;
          return fresh;
        },
      },
    };
    Object.defineProperty(window, "Notification", {
      configurable: true,
      value: {
        permission: "granted",
        requestPermission: async () => "granted",
      },
    });
    Object.defineProperty(window, "PushManager", {
      configurable: true,
      value: function PushManager() {},
    });
    Object.defineProperty(navigator, "serviceWorker", {
      configurable: true,
      value: {
        ready: Promise.resolve(registration),
        register: async () => registration,
      },
    });
    (
      window as Window & { __decisionInboxPushEvents?: string[] }
    ).__decisionInboxPushEvents = events;
  });

  await page.goto(`/#bootstrap=${encodeURIComponent(bootstrapToken)}`);
  await expect(
    page.getByRole("heading", { name: "Needs your call" }),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Renew notifications" }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Settings" }).click();
  await expect(
    page.getByText("Subscription needs to be renewed"),
  ).toBeVisible();
  expect(submittedEndpoints).toEqual([
    "https://push.example.invalid/e2e-stale",
  ]);

  await page.getByRole("button", { name: "Enable on this device" }).click();
  await expect(
    page.getByText("Push notifications are on for this device."),
  ).toBeVisible();
  expect(submittedEndpoints).toEqual([
    "https://push.example.invalid/e2e-stale",
    "https://push.example.invalid/e2e-fresh",
  ]);
  expect(
    await page.evaluate(
      () =>
        (window as Window & { __decisionInboxPushEvents?: string[] })
          .__decisionInboxPushEvents,
    ),
  ).toEqual(["unsubscribe:stale", "subscribe:fresh"]);
});
