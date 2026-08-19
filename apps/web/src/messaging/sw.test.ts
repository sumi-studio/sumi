import { beforeEach, describe, expect, it, vi } from "vitest";

/**
 * Service Worker（public/sw.js）の提示の判断。ここは「タブが無いときの
 * 通知層」で、判定そのもの（誰を呼ぶか）はサーバー側にある。見るのは二つ:
 * 通知が指す住所がアプリの route と同じ規則で作られていること、そして
 * 黙るのが「同じ知らせが既に画面に見えている」ときだけであること。
 */

interface FakeClient {
  url: string;
  focused: boolean;
  navigate: (url: string) => Promise<unknown>;
  focus: () => Promise<unknown>;
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
// listener は import した瞬間に登録される。
await import("../../public/sw.js");

/** push イベントを一つ流し、waitUntil に渡された仕事が終わるまで待つ。 */
async function deliver(payload: unknown): Promise<void> {
  const handler = handlers.get("push");
  if (!handler)
    throw new Error("the service worker registered no push handler");
  const pending: Array<Promise<unknown>> = [];
  handler({
    data: payload === undefined ? null : { json: () => payload },
    waitUntil: (work: Promise<unknown>) => pending.push(work),
  });
  await Promise.all(pending);
}

async function clickFirstNotification(): Promise<void> {
  const handler = handlers.get("notificationclick");
  if (!handler) throw new Error("no notificationclick handler");
  const pending: Array<Promise<unknown>> = [];
  const notification = shown[shown.length - 1];
  handler({
    notification: { close: vi.fn(), data: notification.options.data },
    waitUntil: (work: Promise<unknown>) => pending.push(work),
  });
  await Promise.all(pending);
}

const CALL = {
  workspace_id: "workspace-1",
  place_id: "channel-1",
  place_kind: "channel",
  title: "#general — Yohaku",
  body: "見てもらえますか",
  reason: "mention",
  seq: 7,
};

describe("service worker push presentation", () => {
  beforeEach(() => {
    shown.length = 0;
    windows = [];
    openWindow.mockClear();
  });

  it("points the notification at the place's real route inside its Workspace", async () => {
    await deliver(CALL);

    expect(shown).toHaveLength(1);
    expect(shown[0].options.data).toMatchObject({
      // 通知は place の住所を指す。Workspace を含まない住所は存在しない route で、
      // 押しても開けない（それが起きていた）。
      url: "/w/workspace-1/messaging/c/channel-1",
      reason: "mention",
    });

    // 押されたら、その窓をその住所へ連れて行く。
    windows = [client("https://sumi.test/w/workspace-2/messaging", false)];
    await clickFirstNotification();
    expect(windows[0].navigate).toHaveBeenCalledWith(
      "/w/workspace-1/messaging/c/channel-1",
    );
  });

  it("stays quiet only when that Workspace's Messaging is on screen", async () => {
    // 同じ Workspace の Messaging が前面。同じ出来事は WebSocket で既にその窓へ
    // 届いていて、タブ内の通知層が提示を決めている。ここで重ねると二回鳴る。
    windows = [
      client("https://sumi.test/w/workspace-1/messaging/c/other", true),
    ];
    await deliver(CALL);
    expect(shown).toHaveLength(0);

    // 別 Workspace の Messaging が前面。その窓はこの Workspace の scoped な
    // event を受け取らないので、ここで黙ると呼びかけはどこにも出ない。
    windows = [client("https://sumi.test/w/workspace-2/messaging/c/x", true)];
    await deliver(CALL);
    expect(shown).toHaveLength(1);

    // Messaging 以外の画面が前面でも同じ。
    windows = [client("https://sumi.test/w/workspace-1/settings", true)];
    await deliver(CALL);
    expect(shown).toHaveLength(2);

    // 同じ Workspace の Messaging を開いていても、前面でなければ見えていない。
    windows = [client("https://sumi.test/w/workspace-1/messaging", false)];
    await deliver(CALL);
    expect(shown).toHaveLength(3);
  });

  it("still calls when the payload cannot say which Workspace it came from", async () => {
    // 抑止は「見えていることが確かめられたとき」だけ。分からないときに黙るのは、
    // 呼びかけを落とす側に倒すことになる。
    windows = [client("https://sumi.test/w/workspace-1/messaging", true)];
    await deliver({ ...CALL, workspace_id: "" });
    expect(shown).toHaveLength(1);
    // 行き先が決められない通知は Messaging の根にも飛ばさず、"/" に落ちる。
    expect(shown[0].options.data).toMatchObject({ url: "/" });
  });

  it("ignores a payload it cannot read", async () => {
    await deliver(undefined);
    await deliver({ body: "title のない呼びかけ" });
    expect(shown).toHaveLength(0);
  });
});
