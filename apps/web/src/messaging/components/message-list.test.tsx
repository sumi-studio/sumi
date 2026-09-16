// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { createRef } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { Message, Place, PlaceKey } from "../model";
import { bindMessagingSessionIdentity, useMessaging } from "../store";
import { MessageList, type MessageListHandle } from "./message-list";

const VIEWPORT_HEIGHT = 240;
const PLACE: Place = { kind: "channel", channelId: "ch-jump-targets" };
const KEY: PlaceKey = "channel:ch-jump-targets";
const SELF = { kind: "human", humanId: "self" } as const;

function message(seq: number, deleted = false): Message {
  return {
    messageId: `jump-${seq}`,
    place: PLACE,
    seq,
    author: SELF,
    content: deleted ? "" : `Message ${seq}`,
    mentions: [],
    urgency: "normal",
    reactions: [],
    attachments: [],
    poll: null,
    replyTo: null,
    createdAt: seq * 60_000,
    editedAt: null,
    deleted,
  };
}

function seedStore(messages: Message[]) {
  useMessaging.setState({
    activePlaceKey: KEY,
    messagesByPlace: { [KEY]: messages },
    pendingByPlace: {},
    unreadLineByPlace: {},
    hasMoreByPlace: {},
    loadingGapsByPlace: {},
    self: SELF,
    selfKey: "human:self",
    membersByKey: {
      "human:self": {
        participant: SELF,
        displayName: "Self",
        tagline: "",
      },
    },
    noteReadUpTo: vi.fn(),
    setReplyTarget: vi.fn(),
    startEdit: vi.fn(),
    submitEdit: vi.fn(),
    cancelEdit: vi.fn(),
    editDraft: "",
    editConflict: null,
    editFailure: null,
    editSavedWithPendingChanges: false,
    editSession: null,
    deleteFailedMessageIds: new Set(),
    setEditDraft: vi.fn(),
    reloadEditConflict: vi.fn(),
    deleteMessage: vi.fn(),
    createReplyLater: vi.fn(),
    retrySend: vi.fn(),
    toggleReaction: vi.fn(),
    capabilities: {
      status: false,
      replyLater: false,
      reactions: false,
      notifications: false,
      threads: false,
    },
    editingMessageId: null,
    replyLaterById: {},
    loadOlder: vi.fn(),
    loadGap: vi.fn(),
  });
}

function mountList(messages: Message[]) {
  seedStore(messages);
  const handle = createRef<MessageListHandle>();
  render(
    <MessageList
      handleRef={handle}
      revealedAttachmentIds={new Set()}
      onRevealAttachment={() => {}}
      onOpenImage={() => {}}
    />,
  );
  return handle;
}

function recordScrollTo(viewport: HTMLElement) {
  const tops: number[] = [];
  const apply = viewport.scrollTo.bind(viewport);
  Object.defineProperty(viewport, "scrollTo", {
    configurable: true,
    value(options: ScrollToOptions | number) {
      const top = typeof options === "number" ? options : options.top;
      if (typeof top === "number") tops.push(top);
      apply(options as ScrollToOptions);
    },
  });
  return tops;
}

/** place入室直後の位置決めtimer([0,120,300]ms)が済むまで待つ。 */
async function settleInitialPositioning() {
  await act(async () => {
    await new Promise((resolve) => setTimeout(resolve, 400));
  });
}

beforeEach(() => {
  Object.defineProperty(HTMLElement.prototype, "offsetHeight", {
    configurable: true,
    get() {
      if (this.dataset.slot === "conversation-viewport") {
        return VIEWPORT_HEIGHT;
      }
      if (this.dataset.index !== undefined) {
        return Number(this.dataset.index) % 2 === 0 ? 72 : 36;
      }
      return 0;
    },
  });
  Object.defineProperty(HTMLElement.prototype, "offsetWidth", {
    configurable: true,
    get() {
      return 800;
    },
  });
  Object.defineProperty(HTMLElement.prototype, "clientHeight", {
    configurable: true,
    get() {
      return this.dataset.slot === "conversation-viewport"
        ? VIEWPORT_HEIGHT
        : 0;
    },
  });
  Object.defineProperty(HTMLElement.prototype, "clientWidth", {
    configurable: true,
    get() {
      return this.getAttribute("role") === "log" ? 800 : 0;
    },
  });
  Object.defineProperty(HTMLElement.prototype, "scrollHeight", {
    configurable: true,
    get() {
      if (this.dataset.slot !== "conversation-viewport") return 0;
      return Number.parseFloat(
        (this.firstElementChild as HTMLElement | null)?.style.height ?? "0",
      );
    },
  });
  Object.defineProperty(HTMLElement.prototype, "scrollTo", {
    configurable: true,
    value(this: HTMLElement, options: ScrollToOptions) {
      if (typeof options.top === "number") this.scrollTop = options.top;
      this.dispatchEvent(new Event("scroll"));
    },
  });
});

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  bindMessagingSessionIdentity(null);
});

describe("MessageList deleted-target jumps", () => {
  it("re-scrolls to the deletion marker when the same target is selected again", async () => {
    bindMessagingSessionIdentity("self");
    const messages = Array.from({ length: 40 }, (_, index) =>
      message(index + 1, index + 1 === 20),
    );
    const handle = mountList(messages);
    await settleInitialPositioning();
    const viewport = screen.getByRole("region") as HTMLElement;
    const scrollTops = recordScrollTo(viewport);

    act(() => handle.current?.jumpToSeq(20));
    const marker = () =>
      screen.queryByText("このメッセージは削除されています", { exact: true });
    await waitFor(() => expect(marker()).toBeInTheDocument());
    await waitFor(() => expect(scrollTops.length).toBeGreaterThan(0));
    const firstScrollCount = scrollTops.length;

    // 人が読み進めて離れたあと、同じ削除対象をもう一度選ぶ。wheelを伴う
    // 実操作なのでfollowが外れ、戻る力は働かない。
    fireEvent.wheel(viewport, { deltaY: -400 });
    act(() => {
      viewport.scrollTop = 0;
      fireEvent.scroll(viewport);
    });
    act(() => handle.current?.jumpToSeq(20));

    // 旧実装は同一seqのsetStateがbail-outしてscrollを発行しなかった。
    await waitFor(() =>
      expect(scrollTops.length).toBeGreaterThan(firstScrollCount),
    );
  });

  it("still scrolls when a rows update cancels the first animation frame", async () => {
    bindMessagingSessionIdentity("self");
    const messages = Array.from({ length: 40 }, (_, index) =>
      message(index + 1, index + 1 === 20),
    );
    const handle = mountList(messages);
    await settleInitialPositioning();
    const viewport = screen.getByRole("region") as HTMLElement;
    const scrollTops = recordScrollTo(viewport);

    // frameを手動制御する: marker scrollのRAFが発火する前にrows更新を挟む。
    const pendingFrames = new Map<number, FrameRequestCallback>();
    let nextFrame = 0;
    vi.spyOn(window, "requestAnimationFrame").mockImplementation((callback) => {
      const id = ++nextFrame;
      pendingFrames.set(id, callback);
      return id;
    });
    vi.spyOn(window, "cancelAnimationFrame").mockImplementation((id) => {
      pendingFrames.delete(id);
    });

    act(() => handle.current?.jumpToSeq(20));
    // 標識行はまだwindow外だがitemsには入っているのでscroll用のframeが立つ。
    expect(pendingFrames.size).toBeGreaterThan(0);

    // 標識のscroll frameが走る前に、別の行更新がframeを取り消す。
    const armed = [...pendingFrames.keys()];
    act(() => {
      useMessaging.setState({
        messagesByPlace: { [KEY]: [...messages, message(41)] },
      });
    });
    const disarmed = armed.filter((id) => !pendingFrames.has(id));
    expect(disarmed.length).toBeGreaterThan(0);
    // 旧実装はeffect本体で完了を記録済みのため再armせず、scrollは永久に来ない。
    expect(pendingFrames.size).toBeGreaterThan(0);

    // 残っているframeを全部走らせるとmarkerへのscrollが発行される。
    for (const callback of [...pendingFrames.values()]) {
      callback(performance.now());
    }
    expect(scrollTops.length).toBeGreaterThan(0);
  });
});

describe("MessageList gap fill outcome", () => {
  const messages = [
    message(1),
    message(2),
    message(10),
    message(11),
    message(12),
  ];

  it("reports a failed fill on the gap row and lets the retry fill it", async () => {
    bindMessagingSessionIdentity("self");
    mountList(messages);
    const loadGap = vi
      .fn()
      .mockResolvedValueOnce("failed" as const)
      .mockImplementationOnce(async () => {
        // 成功時はstoreが区間をmergeする——ここでは同じ結果を再現する。
        const merged = [
          ...messages,
          ...[3, 4, 5, 6, 7, 8, 9].map((seq) => message(seq)),
        ].sort((a, b) => a.seq - b.seq);
        useMessaging.setState({ messagesByPlace: { [KEY]: merged } });
        return "filled" as const;
      });
    act(() => useMessaging.setState({ loadGap }));
    await settleInitialPositioning();

    const gapButton = screen.getByRole("button", {
      name: "この間の会話を読み込む",
    });
    fireEvent.click(gapButton);
    // 旧実装は失敗を握りつぶして元の文言へ戻るだけだった——失敗はその行の上に
    // 正直に出て、行は押せるまま残る。
    const retry = await screen.findByRole("button", {
      name: "読み込めませんでした · 再試行",
    });
    expect(loadGap).toHaveBeenCalledExactlyOnceWith(KEY, 10);

    fireEvent.click(retry);
    await waitFor(() =>
      expect(screen.queryByText("Message 5", { exact: true })).not.toBeNull(),
    );
    expect(loadGap).toHaveBeenCalledTimes(2);
    // 埋まったgap行は消える——失敗表示も一緒に消える。
    expect(
      screen.queryByRole("button", { name: /読み込めませんでした/ }),
    ).toBeNull();
    expect(
      screen.queryByRole("button", { name: "この間の会話を読み込む" }),
    ).toBeNull();
  });

  it("does not report a cancelled fill as a failure", async () => {
    bindMessagingSessionIdentity("self");
    mountList(messages);
    // place/session差し替えで無効化された要求は"cancelled"——失敗とは出さない。
    const loadGap = vi.fn().mockResolvedValue("cancelled" as const);
    act(() => useMessaging.setState({ loadGap }));
    await settleInitialPositioning();

    fireEvent.click(
      screen.getByRole("button", {
        name: "この間の会話を読み込む",
      }),
    );
    await waitFor(() => expect(loadGap).toHaveBeenCalled());
    await waitFor(() =>
      expect(
        screen.getByRole("button", {
          name: "この間の会話を読み込む",
        }),
      ).toBeInTheDocument(),
    );
    expect(
      screen.queryByRole("button", { name: /読み込めませんでした/ }),
    ).toBeNull();
  });
});

describe("MessageList reply quote to a deleted original", () => {
  it("scrolls to the deletion marker instead of dead-ending", async () => {
    bindMessagingSessionIdentity("self");
    const messages = [
      message(1),
      message(2),
      message(3, true),
      { ...message(4), replyTo: "jump-3" },
      message(5),
    ];
    mountList(messages);
    await settleInitialPositioning();
    const viewport = screen.getByRole("region") as HTMLElement;
    const scrollTops = recordScrollTo(viewport);

    // 旧実装はflashMessageへ直結していて、tombstoneは行として存在しないので
    // scrollToMessageがfalseで終わり、標識もscrollも起きなかった。
    fireEvent.click(screen.getByTitle("Self の返信元へ移動"));
    await waitFor(() =>
      expect(
        screen.getByText("このメッセージは削除されています", { exact: true }),
      ).toBeInTheDocument(),
    );
    await waitFor(() => expect(scrollTops.length).toBeGreaterThan(0));
  });
});
