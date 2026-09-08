// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import { memo } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MessagingAPIError } from "../api-backend";
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
  usePlaceNavigate: () => () => undefined,
}));

/**
 * 行の描画回数。MessageItem は memo なので、この外側の memo が描き直された
 * 回数＝「その行に渡る props が変わった回数」。編集欄の1キーストロークで
 * 可視行すべてを描き直していないことを、ここで数える。
 */
const rowRenders = new Map<string, number>();
vi.mock("./message-item", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./message-item")>();
  const Counted = memo(function CountedMessageItem(
    props: React.ComponentProps<typeof actual.MessageItem>,
  ) {
    rowRenders.set(
      props.message.messageId,
      (rowRenders.get(props.message.messageId) ?? 0) + 1,
    );
    return <actual.MessageItem {...props} />;
  });
  return { ...actual, MessageItem: Counted };
});

const VIEWPORT_HEIGHT = 240;
const MESSAGE_COUNT = 200;
/** 最下部から遠く、最初の描画窓には入らないメッセージ。 */
const OFFSCREEN_INDEX = 3;

const SELF: ParticipantRef = { kind: "human", humanId: "human-a" };
const SUMI: ParticipantRef = {
  kind: "personality_agent",
  personalityAgentId: "sumi-a",
};
const PLACE: PlaceKey = "channel:channel-a";

const membersByKey: Record<string, MemberProfile> = {
  "human:human-a": { participant: SELF, displayName: "余白", tagline: "" },
  "personality_agent:sumi-a": {
    participant: SUMI,
    displayName: "墨",
    tagline: "秘書",
  },
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
    poll: null,
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
    draftByPlace: {},
    editingMessageId: null,
    editDraft: "",
    capabilities: {
      status: false,
      replyLater: false,
      reactions: false,
      notifications: false,
    },
  });
}

function conflictWire(message: Message) {
  return {
    message_id: message.messageId,
    place:
      message.place.kind === "channel"
        ? { kind: "channel", channel_id: message.place.channelId }
        : message.place.kind === "thread"
          ? { kind: "thread", thread_id: message.place.threadId }
          : { kind: message.place.kind, dm_id: message.place.dmId },
    seq: message.seq,
    author:
      message.author.kind === "human"
        ? { kind: "human", human_id: message.author.humanId }
        : {
            kind: "personality_agent",
            personality_agent_id: message.author.personalityAgentId,
          },
    content: message.content,
    mentions: [],
    urgency: message.urgency,
    reactions: [],
    attachments: [],
    poll: message.poll,
    reply_to: null,
    client_nonce: "conflict-nonce",
    created_at: new Date(message.createdAt).toISOString(),
    edited_at: message.editedAt
      ? new Date(message.editedAt).toISOString()
      : null,
    revision: message.revision,
    deleted: message.deleted,
  };
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
  rowRenders.clear();
});

function row(messageId: string): HTMLElement | null {
  return document.querySelector<HTMLElement>(
    `[data-message-id="${messageId}"]`,
  );
}

/** 添付の開閉は画面側の状態。編集セッションの試験では常に閉じたまま。 */
function renderList() {
  return render(
    <MessageList
      revealedAttachmentIds={new Set()}
      onRevealAttachment={() => undefined}
      onOpenImage={() => undefined}
    />,
  );
}

describe("編集セッションは仮想リストの行より長生きする", () => {
  it("描画窓の外のメッセージでも編集を始めれば編集欄が現れる", async () => {
    const messages = makeMessages(MESSAGE_COUNT);
    seedStore(messages);
    renderList();

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
    renderList();

    const target = messages[OFFSCREEN_INDEX].messageId;
    act(() => useMessaging.getState().startEdit(target));
    const textarea = (await screen.findByLabelText(
      "メッセージを編集",
    )) as HTMLTextAreaElement;
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

  it("編集欄は開いた回に一度だけフォーカスし、行の再マウントでは奪わない", async () => {
    const messages = makeMessages(MESSAGE_COUNT);
    seedStore(messages);
    renderList();
    // 編集欄が focus() を呼んだ回数を数える。仮想リストは自分の都合で
    // viewport へフォーカスを移すことがあるので、activeElement ではなく
    // 「編集欄が奪いにいったか」を見る。
    const focusedTextareas: HTMLElement[] = [];
    const originalFocus = HTMLElement.prototype.focus;
    vi.spyOn(HTMLElement.prototype, "focus").mockImplementation(function (
      this: HTMLElement,
      options?: FocusOptions,
    ) {
      if (this instanceof HTMLTextAreaElement) focusedTextareas.push(this);
      originalFocus.call(this, options);
    });

    const target = messages[OFFSCREEN_INDEX].messageId;
    act(() => useMessaging.getState().startEdit(target));
    const textarea = await screen.findByLabelText("メッセージを編集");
    expect(focusedTextareas).toEqual([textarea]);

    // 最新側へ飛ばして行を捨て、戻して再マウントさせる。
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
    viewport.scrollTop = 0;
    fireEvent.scroll(viewport);
    const remounted = await screen.findByLabelText("メッセージを編集");
    // 同じ編集の回。別の場所（composer など）に居る caret を奪い返さない。
    expect(remounted).not.toBe(textarea);
    expect(focusedTextareas).toEqual([textarea]);
    expect(remounted).not.toHaveFocus();

    // 取り消して開き直せば、新しい回として再びフォーカスする。
    act(() => useMessaging.getState().cancelEdit());
    await waitFor(() => {
      expect(screen.queryByLabelText("メッセージを編集")).toBeNull();
    });
    act(() => useMessaging.getState().startEdit(target));
    const reopened = await screen.findByLabelText("メッセージを編集");
    expect(focusedTextareas).toEqual([textarea, reopened]);
  });

  it("編集欄の1キーストロークで描き直すのは編集行だけ", async () => {
    const messages = makeMessages(MESSAGE_COUNT);
    seedStore(messages);
    renderList();
    // 最新側の描画窓にある行を編集する（可視行が複数ある状態で数える）。
    const target = messages[MESSAGE_COUNT - 2].messageId;
    await waitFor(() => {
      expect(row(target)).toBeInTheDocument();
    });
    act(() => useMessaging.getState().startEdit(target));
    const textarea = await screen.findByLabelText("メッセージを編集");
    const before = new Map(rowRenders);
    expect(before.size).toBeGreaterThan(1);

    fireEvent.change(textarea, { target: { value: "一文字足す" } });
    expect(useMessaging.getState().editDraft).toBe("一文字足す");

    const rerendered = [...rowRenders].filter(
      ([id, count]) => count !== before.get(id),
    );
    expect(rerendered.map(([id]) => id)).toEqual([target]);
  });

  it("インライン編集中の @ 補完は候補を選ぶと表示名を挿入する", async () => {
    const messages = makeMessages(MESSAGE_COUNT);
    seedStore(messages);
    renderList();

    act(() =>
      useMessaging.getState().startEdit(messages[OFFSCREEN_INDEX].messageId),
    );
    const textarea = (await screen.findByLabelText(
      "メッセージを編集",
    )) as HTMLTextAreaElement;
    fireEvent.change(textarea, { target: { value: "@" } });

    const suggestions = screen.getByTestId("mention-suggestions");
    expect(suggestions).toHaveTextContent("墨");
    fireEvent.mouseDown(
      within(suggestions).getByRole("button", { name: /墨/ }),
    );

    await waitFor(() => expect(textarea).toHaveValue("@墨 "));
  });

  it("候補表示後にキャレットを@から外してTabしても古い範囲を置換しない", async () => {
    const messages = makeMessages(MESSAGE_COUNT);
    seedStore(messages);
    renderList();

    act(() =>
      useMessaging.getState().startEdit(messages[OFFSCREEN_INDEX].messageId),
    );
    const textarea = (await screen.findByLabelText(
      "メッセージを編集",
    )) as HTMLTextAreaElement;
    fireEvent.change(textarea, { target: { value: "@" } });
    expect(screen.getByTestId("mention-suggestions")).toBeVisible();

    textarea.setSelectionRange(0, 0);
    fireEvent.keyUp(textarea, { key: "Home" });
    fireEvent.keyDown(textarea, { key: "Tab" });

    expect(useMessaging.getState().editDraft).toBe("@");
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

  it("成功応答とechoを失った編集は同一PATCHの409正本で確定し、偽の競合を出さない", async () => {
    const backend = await bootStore();
    const target = useMessaging
      .getState()
      .messagesByPlace["channel:ch-general"]?.find(
        (message) =>
          message.author.kind === "human" &&
          message.author.humanId === "h-yohaku",
      );
    if (!target) throw new Error("target message was not loaded");
    const submitted = "応答を失った保存本文";
    const committed = {
      ...target,
      content: submitted,
      editedAt: Date.UTC(2026, 7, 23, 12, 0, 0),
      revision: (target.revision ?? 1) + 1,
    };
    const edit = vi
      .spyOn(backend, "editMessage")
      // サーバーでは確定したが、REST応答もWS echoも届かなかった窓を再現する。
      .mockRejectedValueOnce(new Error("edit response lost"))
      .mockRejectedValueOnce(
        new MessagingAPIError("edit_conflict", 409, {
          message: conflictWire(committed),
        }),
      );
    renderList();

    act(() => useMessaging.getState().startEdit(target.messageId));
    const textarea = await screen.findByLabelText("メッセージを編集");
    fireEvent.change(textarea, { target: { value: submitted } });
    fireEvent.click(screen.getByRole("button", { name: "保存" }));
    await waitFor(() =>
      expect(useMessaging.getState().editFailure).toBe(
        "保存できませんでした。もう一度お試しください。",
      ),
    );

    fireEvent.click(screen.getByRole("button", { name: "保存" }));

    await waitFor(() =>
      expect(useMessaging.getState().editingMessageId).toBeNull(),
    );
    expect(edit).toHaveBeenNthCalledWith(
      2,
      target.place,
      target.messageId,
      submitted,
      target.revision ?? 1,
    );
    expect(
      useMessaging
        .getState()
        .messagesByPlace["channel:ch-general"]?.find(
          (message) => message.messageId === target.messageId,
        ),
    ).toMatchObject({ content: submitted, revision: committed.revision });
    expect(screen.queryByText("別の場所で編集されました")).toBeNull();
    expect(useMessaging.getState().editConflict).toBeNull();
  });

  it("同一本文のWS N+2が先行したら遅延2xx N+1でdraftと競合を消さない", async () => {
    const backend = await bootStore();
    const target = useMessaging
      .getState()
      .messagesByPlace["channel:ch-general"]?.find(
        (message) =>
          message.author.kind === "human" &&
          message.author.humanId === "h-yohaku",
      );
    if (!target) throw new Error("target message was not loaded");
    const submitted = "同じ本文でも版が二つ先";
    const acknowledged = {
      ...target,
      content: submitted,
      revision: (target.revision ?? 1) + 1,
    };
    const projected = {
      ...acknowledged,
      revision: (target.revision ?? 1) + 2,
    };
    const emit = (
      backend as unknown as {
        emit(event: { type: "message_edited"; message: Message }): void;
      }
    ).emit.bind(backend);
    let resolveSave: ((message: Message) => void) | undefined;
    const save = new Promise<Message>((resolve) => {
      resolveSave = resolve;
    });
    vi.spyOn(backend, "editMessage").mockImplementationOnce(() => save);
    renderList();

    act(() => useMessaging.getState().startEdit(target.messageId));
    const textarea = await screen.findByLabelText("メッセージを編集");
    fireEvent.change(textarea, { target: { value: submitted } });
    fireEvent.click(screen.getByRole("button", { name: "保存" }));
    act(() => emit({ type: "message_edited", message: projected }));

    expect(screen.getByRole("alert")).toHaveTextContent(
      "別の場所で編集されました",
    );
    await act(async () => {
      resolveSave?.(acknowledged);
      await save;
    });

    expect(useMessaging.getState()).toMatchObject({
      editingMessageId: target.messageId,
      editDraft: submitted,
      editBaseRevision: acknowledged.revision,
      editConflict: {
        content: projected.content,
        revision: projected.revision,
      },
    });
    expect(screen.getByLabelText("メッセージを編集")).toHaveValue(submitted);
    expect(screen.getByRole("alert")).toHaveTextContent(
      "別の場所で編集されました",
    );
    expect(
      useMessaging
        .getState()
        .messagesByPlace["channel:ch-general"]?.find(
          (message) => message.messageId === target.messageId,
        ),
    ).toMatchObject(projected);
  });

  it("WS N+2が再試行409より先行したらN+1 lost ACKでdraftと競合を消さない", async () => {
    const backend = await bootStore();
    const target = useMessaging
      .getState()
      .messagesByPlace["channel:ch-general"]?.find(
        (message) =>
          message.author.kind === "human" &&
          message.author.humanId === "h-yohaku",
      );
    if (!target) throw new Error("target message was not loaded");
    const submitted = "応答を失ったN+1本文";
    const acknowledged = {
      ...target,
      content: submitted,
      revision: (target.revision ?? 1) + 1,
    };
    const projected = {
      ...target,
      content: "その後に確定したN+2本文",
      revision: (target.revision ?? 1) + 2,
    };
    const emit = (
      backend as unknown as {
        emit(event: { type: "message_edited"; message: Message }): void;
      }
    ).emit.bind(backend);
    let rejectRetry: ((error: unknown) => void) | undefined;
    const retry = new Promise<Message>((_resolve, reject) => {
      rejectRetry = reject;
    });
    vi.spyOn(backend, "editMessage")
      .mockRejectedValueOnce(new Error("edit response lost"))
      .mockImplementationOnce(() => retry);
    renderList();

    act(() => useMessaging.getState().startEdit(target.messageId));
    const textarea = await screen.findByLabelText("メッセージを編集");
    fireEvent.change(textarea, { target: { value: submitted } });
    fireEvent.click(screen.getByRole("button", { name: "保存" }));
    await waitFor(() =>
      expect(useMessaging.getState().editFailure).not.toBeNull(),
    );
    fireEvent.click(screen.getByRole("button", { name: "保存" }));
    act(() => emit({ type: "message_edited", message: projected }));

    await act(async () => {
      rejectRetry?.(
        new MessagingAPIError("edit_conflict", 409, {
          message: conflictWire(acknowledged),
        }),
      );
      await retry.catch(() => undefined);
    });

    expect(useMessaging.getState()).toMatchObject({
      editingMessageId: target.messageId,
      editDraft: submitted,
      editBaseRevision: acknowledged.revision,
      editConflict: {
        content: projected.content,
        revision: projected.revision,
      },
    });
    expect(screen.getByLabelText("メッセージを編集")).toHaveValue(submitted);
    expect(screen.getByRole("alert")).toHaveTextContent(
      "別の場所で編集されました",
    );
    expect(
      useMessaging
        .getState()
        .messagesByPlace["channel:ch-general"]?.find(
          (message) => message.messageId === target.messageId,
        ),
    ).toMatchObject(projected);
  });

  it("DELETE の失敗は無音にせず、その行に出して再試行できる", async () => {
    const backend = await bootStore();
    // 未読ライン付近（入室時の位置決めの先）にある自分の最後の発言を使う。
    const target = (
      useMessaging.getState().messagesByPlace["channel:ch-general"] ?? []
    )
      .filter(
        (message) =>
          message.author.kind === "human" &&
          message.author.humanId === "h-yohaku",
      )
      .at(-1);
    if (!target) throw new Error("target message was not loaded");
    renderList();
    await waitFor(() => expect(row(target.messageId)).toBeInTheDocument());

    const remove = vi
      .spyOn(backend, "deleteMessage")
      .mockRejectedValueOnce(new MessagingAPIError("forbidden", 403));
    act(() => useMessaging.getState().deleteMessage(target.messageId));
    await waitFor(() =>
      expect(
        useMessaging.getState().deleteFailedMessageIds.has(target.messageId),
      ).toBe(true),
    );
    const notice = await waitFor(() => {
      const element = row(target.messageId);
      if (!element) throw new Error("target row is not rendered");
      return within(element).getByRole("alert");
    });
    expect(notice).toHaveTextContent("削除できませんでした");
    // 本文は残ったまま（削除は起きていない）。
    expect(
      useMessaging
        .getState()
        .messagesByPlace["channel:ch-general"]?.find(
          (message) => message.messageId === target.messageId,
        )?.deleted,
    ).toBe(false);

    // 再試行で失敗表示は消え、成功すれば tombstone になる。
    fireEvent.click(within(notice).getByRole("button", { name: "もう一度" }));
    expect(remove).toHaveBeenCalledTimes(2);
    await waitFor(() =>
      expect(
        useMessaging
          .getState()
          .messagesByPlace["channel:ch-general"]?.find(
            (message) => message.messageId === target.messageId,
          )?.deleted,
      ).toBe(true),
    );
    expect(
      useMessaging.getState().deleteFailedMessageIds.has(target.messageId),
    ).toBe(false);
    // tombstone は行にならない。失敗表示ごと消える。
    await waitFor(() => expect(row(target.messageId)).toBeNull());
  });
});
