import { beforeEach, describe, expect, it, vi } from "vitest";

interface FakeClient {
  url: string;
  focused: boolean;
  navigate: ReturnType<typeof vi.fn>;
  focus: ReturnType<typeof vi.fn>;
}

function client(url: string, focused: boolean): FakeClient {
  return {
    url,
    focused,
    navigate: vi.fn(async () => undefined),
    focus: vi.fn(async () => undefined),
  };
}

const handlers = new Map<string, (event: unknown) => void>();
const shown: Array<{ title: string; options: Record<string, unknown> }> = [];
let windows: FakeClient[] = [];
const openWindow = vi.fn(async () => null);

const fakeSelf = {
  location: { origin: "https://sumi.test" },
  addEventListener: (type: string, handler: (event: unknown) => void) => {
    handlers.set(type, handler);
  },
  skipWaiting: vi.fn(),
  registration: {
    showNotification: vi.fn(
      async (title: string, options: Record<string, unknown>) => {
        shown.push({ title, options });
      },
    ),
    pushManager: { subscribe: vi.fn(async () => undefined) },
  },
  clients: {
    matchAll: vi.fn(async () => windows),
    claim: vi.fn(async () => undefined),
    openWindow,
  },
};

vi.stubGlobal("self", fakeSelf);
await import("../../public/sw.js");

async function deliver(payload: unknown): Promise<void> {
  const pending: Promise<unknown>[] = [];
  handlers.get("push")?.({
    data: { json: () => payload },
    waitUntil: (work: Promise<unknown>) => pending.push(work),
  });
  await Promise.all(pending);
}

async function clickLast(): Promise<void> {
  const pending: Promise<unknown>[] = [];
  const notification = shown.at(-1);
  handlers.get("notificationclick")?.({
    notification: {
      close: vi.fn(),
      data: notification?.options.data,
    },
    waitUntil: (work: Promise<unknown>) => pending.push(work),
  });
  await Promise.all(pending);
}

const POINTER = {
  workspace_id: "workspace-1",
  place_id: "channel-1",
  place_kind: "channel",
};

describe("generic push Service Worker", () => {
  beforeEach(() => {
    shown.length = 0;
    windows = [];
    openWindow.mockClear();
  });

  it("ignores server-authored display content and uses fixed generic copy", async () => {
    await deliver({
      ...POINTER,
      title: "participant name",
      body: "private message body",
      attachment: "secret.pdf",
    });

    expect(shown).toHaveLength(1);
    expect(shown[0]).toMatchObject({
      title: "Sumi",
      options: {
        body: "新しいメッセージがあります",
        data: { url: "/w/workspace-1/messaging/c/channel-1" },
      },
    });
    expect(JSON.stringify(shown[0])).not.toMatch(
      /participant name|private message body|secret\.pdf/,
    );
  });

  it("suppresses only a focused Messaging view for the same Workspace", async () => {
    windows = [
      client("https://sumi.test/w/workspace-1/messaging/c/other", true),
    ];
    await deliver(POINTER);
    expect(shown).toHaveLength(0);

    windows = [
      client("https://sumi.test/w/workspace-2/messaging/c/other", true),
    ];
    await deliver(POINTER);
    expect(shown).toHaveLength(1);
  });

  it("routes a click through an encoded app-owned path", async () => {
    await deliver({
      workspace_id: "workspace/one",
      place_id: "dm/one",
      place_kind: "dm",
    });
    windows = [client("https://sumi.test/direct", false)];
    await clickLast();

    expect(windows[0]?.navigate).toHaveBeenCalledWith(
      "/w/workspace%2Fone/messaging/dm/dm%2Fone",
    );
    expect(windows[0]?.focus).toHaveBeenCalled();
  });

  it("opens a new window when existing clients cannot navigate", async () => {
    await deliver(POINTER);
    const broken = client("https://sumi.test/direct", false);
    broken.navigate.mockRejectedValue(new Error("closed"));
    windows = [broken];
    await clickLast();

    expect(openWindow).toHaveBeenCalledWith(
      "/w/workspace-1/messaging/c/channel-1",
    );
  });

  it("rejects malformed pointers and registers no offline fetch handler", async () => {
    await deliver({ ...POINTER, place_kind: "https://evil.test" });
    await deliver({ title: "missing pointer" });
    expect(shown).toHaveLength(0);
    expect(handlers.has("fetch")).toBe(false);
  });
});
