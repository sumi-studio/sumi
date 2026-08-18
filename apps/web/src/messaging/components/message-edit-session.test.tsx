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
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MockMessagingServer } from "../mock-server";
import type {
  MemberProfile,
  Message,
  ParticipantRef,
  PlaceKey,
} from "../model";
import {
  bindMessagingSessionIdentity,
  installMessagingBackend,
  useMessaging,
} from "../store";
import { MessageList } from "./message-list";

/**
 * 編集セッション（対象IDと書きかけの本文）が、仮想リストの行の寿命から
 * 独立していることの裏。行はスクロールでいつでも消える。
 */

vi.mock("../place-route", () => ({
  placePath: (workspaceId: string, key: string) => `/w/${workspaceId}/${key}`,
}));

const VIEWPORT_HEIGHT = 240;
const MESSAGE_COUNT = 200;
/** 最下部から遠く、最初の描画窓には入らないメッセージ。 */
const OFFSCREEN_INDEX = 3;

const SELF: ParticipantRef = { kind: "human", humanId: "human-a" };
const PLACE: PlaceKey = "channel:channel-a";

const membersByKey: Record<string, MemberProfile> = {
  "human:human-a": { participant: SELF, displayName: "余白", tagline: "" },
};

function makeMessages(count: number): Message[] {
  return Array.from({ length: count }, (_, index) => ({
    messageId: `message-${index}`,
    place: { kind: "channel", channelId: "channel-a" } as const,
    seq: index + 1,
    author: SELF,
    content: `本文 ${index}`,
    mentions: [],
    urgency: "normal" as const,
    reactions: [],
    attachments: [],
    replyTo: null,
    // グルーピングでまとまらないよう十分に離す。
    createdAt: Date.UTC(2026, 0, 1) + index * 3_600_000,
    editedAt: null,
    deleted: false,
  }));
}

function seedStore(messages: Message[]) {
  useMessaging.setState({
    ready: true,
    self: SELF,
    selfKey: "human:human-a",
    membersByKey,
    activePlaceKey: PLACE,
    messagesByPlace: { [PLACE]: messages },
    pendingByPlace: {},
    // 既読が先頭まで進んでいれば noteReadUpTo はサーバーを呼ばない。
    lastReadByPlace: { [PLACE]: messages.length },
    unreadLineByPlace: {},
    hasMoreByPlace: { [PLACE]: false },
    replyLaterById: {},
    editingMessageId: null,
    editDraft: "",
    replyTargetId: null,
    capabilities: {
      status: false,
      replyLater: false,
      reactions: false,
      notifications: false,
    },
  });
}

beforeEach(() => {
  Object.defineProperty(HTMLElement.prototype, "offsetHeight", {
    configurable: true,
    get() {
      if (this.dataset.slot === "conversation-viewport") {
        return VIEWPORT_HEIGHT;
      }
      return this.dataset.index !== undefined ? 60 : 0;
    },
  });
  Object.defineProperty(HTMLElement.prototype, "offsetWidth", {
    configurable: true,
    get: () => 800,
  });
  Object.defineProperty(HTMLElement.prototype, "clientHeight", {
    configurable: true,
    get() {
      return this.dataset.slot === "conversation-viewport"
        ? VIEWPORT_HEIGHT
        : 0;
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
  useMessaging.getState().cancelEdit();
});

function row(messageId: string): HTMLElement | null {
  return document.querySelector<HTMLElement>(
    `[data-message-id="${messageId}"]`,
  );
}

describe("編集セッションは仮想リストの行より長生きする", () => {
  it("描画窓の外のメッセージでも編集を始めれば編集欄が現れる", async () => {
    const messages = makeMessages(MESSAGE_COUNT);
    seedStore(messages);
    render(<MessageList />);

    const target = messages[OFFSCREEN_INDEX].messageId;
    // 最初の描画窓は最新側。対象の行はまだ存在しない。
    await waitFor(() => {
      expect(row(messages[MESSAGE_COUNT - 1].messageId)).toBeInTheDocument();
    });
    expect(row(target)).not.toBeInTheDocument();

    act(() => useMessaging.getState().startEdit(target));

    // 対象まで運ばれ、その位置に編集欄が開く。
    await waitFor(() => {
      expect(screen.getByLabelText("メッセージを編集")).toBeVisible();
    });
    expect(screen.getByLabelText("メッセージを編集")).toHaveValue(
      `本文 ${OFFSCREEN_INDEX}`,
    );
    expect(row(target)).toBeInTheDocument();
  });

  it("編集中に行が描画窓から外れても書きかけは残る", async () => {
    const messages = makeMessages(MESSAGE_COUNT);
    seedStore(messages);
    render(<MessageList />);

    const target = messages[OFFSCREEN_INDEX].messageId;
    act(() => useMessaging.getState().startEdit(target));
    const textarea = await screen.findByLabelText("メッセージを編集");
    fireEvent.change(textarea, { target: { value: "書きかけの続き" } });
    expect(useMessaging.getState().editDraft).toBe("書きかけの続き");

    // 最新側へ飛ばして対象の行を捨てさせる。
    const viewport = document.querySelector<HTMLElement>(
      '[data-slot="conversation-viewport"]',
    );
    if (!viewport) throw new Error("viewport not rendered");
    fireEvent.wheel(viewport, { deltaY: 4_000 });
    viewport.scrollTop = 9_000;
    fireEvent.scroll(viewport);
    await waitFor(() => {
      expect(screen.queryByLabelText("メッセージを編集")).toBeNull();
    });
    // 行が消えても編集セッションは生きている。
    expect(useMessaging.getState().editingMessageId).toBe(target);
    expect(useMessaging.getState().editDraft).toBe("書きかけの続き");

    // 戻ると元本文ではなく書きかけが再び出る。
    viewport.scrollTop = 0;
    fireEvent.scroll(viewport);
    const again = await screen.findByLabelText("メッセージを編集");
    expect(again).toHaveValue("書きかけの続き");
  });
});

describe("編集セッションのタイムライン整合性", () => {
  afterEach(() => {
    bindMessagingSessionIdentity(null);
  });

  async function bootStore() {
    bindMessagingSessionIdentity("message-edit-session");
    const backend = new MockMessagingServer();
    installMessagingBackend(backend);
    useMessaging.getState().init();
    await waitFor(() => expect(useMessaging.getState().ready).toBe(true));
    useMessaging.getState().selectPlace("channel:ch-general");
    await waitFor(() =>
      expect(
        useMessaging.getState().messagesByPlace["channel:ch-general"],
      ).not.toHaveLength(0),
    );
    return backend;
  }

  it("編集中の対象が message_deleted で消えると composer を通常状態へ戻す", async () => {
    const backend = await bootStore();
    const target = useMessaging
      .getState()
      .messagesByPlace["channel:ch-general"]?.find(
        (message) =>
          message.author.kind === "human" &&
          message.author.humanId === "h-yohaku",
      );
    if (!target) throw new Error("target message was not loaded");

    act(() => useMessaging.getState().startEdit(target.messageId));
    expect(useMessaging.getState()).toMatchObject({
      editingMessageId: target.messageId,
      editDraft: target.content,
    });

    await backend.deleteMessage(target.place, target.messageId);

    expect(useMessaging.getState()).toMatchObject({
      editingMessageId: null,
      editDraft: "",
    });
  });

  it("別メッセージの message_deleted では編集セッションを維持する", async () => {
    const backend = await bootStore();
    const messages = (
      useMessaging.getState().messagesByPlace["channel:ch-general"] ?? []
    ).filter(
      (message) =>
        message.author.kind === "human" &&
        message.author.humanId === "h-yohaku",
    );
    const target = messages[0];
    const other = messages[1];
    if (!target || !other) throw new Error("test messages were not loaded");

    act(() => useMessaging.getState().startEdit(target.messageId));
    act(() => useMessaging.getState().setEditDraft("保存前の書きかけ"));
    await backend.deleteMessage(other.place, other.messageId);

    expect(useMessaging.getState()).toMatchObject({
      editingMessageId: target.messageId,
      editDraft: "保存前の書きかけ",
    });
  });
});
