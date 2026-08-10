import { expect, test } from "@playwright/test";

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
});

test("renewal tap replaces an expired browser Push subscription", async ({
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
