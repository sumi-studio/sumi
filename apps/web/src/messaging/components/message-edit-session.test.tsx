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
}));

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

function conflictWire(message: Message) {
  return {
    message_id: message.messageId,
    place:
      message.place.kind === "channel"
        ? { kind: "channel", channel_id: message.place.channelId }
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

  it("インライン編集中の @ 補完は候補を選ぶと表示名を挿入する", async () => {
    const messages = makeMessages(MESSAGE_COUNT);
    seedStore(messages);
    render(<MessageList />);

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
    render(<MessageList />);

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

  it("編集中に対象の message_edited を受けると書きかけを残して保存を止める", async () => {
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
    act(() => useMessaging.getState().setEditDraft("自分の書きかけ"));
    await backend.editMessage(
      target.place,
      target.messageId,
      "別の場所の本文",
      target.revision ?? 1,
    );

    expect(useMessaging.getState()).toMatchObject({
      editingMessageId: target.messageId,
      editDraft: "自分の書きかけ",
      editConflict: {
        content: "別の場所の本文",
        revision: 2,
      },
    });
    const edit = vi.spyOn(backend, "editMessage");
    act(() => useMessaging.getState().submitEdit());
    expect(edit).not.toHaveBeenCalled();
  });

  it("409の現在メッセージを再読込の版として使い、次の保存を通す", async () => {
    const backend = await bootStore();
    const target = useMessaging
      .getState()
      .messagesByPlace["channel:ch-general"]?.find(
        (message) =>
          message.author.kind === "human" &&
          message.author.humanId === "h-yohaku",
      );
    if (!target) throw new Error("target message was not loaded");
    const current = {
      ...target,
      content: "サーバで確定した本文",
      editedAt: Date.UTC(2026, 7, 18, 12, 0, 0),
      revision: (target.revision ?? 1) + 1,
    };
    const edit = vi
      .spyOn(backend, "editMessage")
      .mockRejectedValueOnce(
        new MessagingAPIError("edit_conflict", 409, {
          message: conflictWire(current),
        }),
      )
      .mockResolvedValueOnce({
        ...current,
        content: "再読込後の保存",
        revision: (current.revision ?? 1) + 1,
      });

    act(() => useMessaging.getState().startEdit(target.messageId));
    act(() => useMessaging.getState().setEditDraft("自分の書きかけ"));
    act(() => useMessaging.getState().submitEdit());

    await waitFor(() => {
      expect(useMessaging.getState().editConflict).toEqual({
        content: "サーバで確定した本文",
        revision: 2,
      });
    });
    expect(
      useMessaging
        .getState()
        .messagesByPlace["channel:ch-general"]?.find(
          (message) => message.messageId === target.messageId,
        ),
    ).toMatchObject({ content: "サーバで確定した本文", revision: 2 });

    act(() => useMessaging.getState().reloadEditConflict());
    act(() => useMessaging.getState().setEditDraft("再読込後の保存"));
    act(() => useMessaging.getState().submitEdit());

    await waitFor(() =>
      expect(edit).toHaveBeenLastCalledWith(
        target.place,
        target.messageId,
        "再読込後の保存",
        2,
      ),
    );
    await waitFor(() =>
      expect(useMessaging.getState().editingMessageId).toBeNull(),
    );
  });

  it("WS切断中の409 message_deletedはtombstoneを反映して編集を閉じ、再保存しない", async () => {
    const backend = await bootStore();
    const target = useMessaging
      .getState()
      .messagesByPlace["channel:ch-general"]?.find(
        (message) =>
          message.author.kind === "human" &&
          message.author.humanId === "h-yohaku",
      );
    if (!target) throw new Error("target message was not loaded");
    const tombstone = {
      ...target,
      content: "",
      mentions: [],
      reactions: [],
      attachments: [],
      deleted: true,
      revision: (target.revision ?? 1) + 1,
    };
    // mockはWS eventをemitしない。PATCHの終端応答だけで収束することを見る。
    const edit = vi.spyOn(backend, "editMessage").mockRejectedValueOnce(
      new MessagingAPIError("message_deleted", 409, {
        message: conflictWire(tombstone),
      }),
    );

    act(() => useMessaging.getState().startEdit(target.messageId));
    act(() => useMessaging.getState().setEditDraft("WS切断中の保存"));
    act(() => useMessaging.getState().submitEdit());

    await waitFor(() => {
      expect(useMessaging.getState().editingMessageId).toBeNull();
      expect(
        useMessaging
          .getState()
          .messagesByPlace["channel:ch-general"]?.find(
            (message) => message.messageId === target.messageId,
          ),
      ).toMatchObject({
        deleted: true,
        content: "",
        revision: tombstone.revision,
      });
    });

    act(() => useMessaging.getState().submitEdit());
    expect(edit).toHaveBeenCalledOnce();
  });

  it("404 not_foundは対象seqを再取得してtombstoneを反映する", async () => {
    const backend = await bootStore();
    const target = useMessaging
      .getState()
      .messagesByPlace["channel:ch-general"]?.find(
        (message) =>
          message.author.kind === "human" &&
          message.author.humanId === "h-yohaku",
      );
    if (!target) throw new Error("target message was not loaded");
    const tombstone = {
      ...target,
      content: "",
      mentions: [],
      reactions: [],
      attachments: [],
      deleted: true,
      revision: (target.revision ?? 1) + 1,
    };
    vi.spyOn(backend, "editMessage").mockRejectedValueOnce(
      new MessagingAPIError("not_found", 404),
    );
    const fetch = vi
      .spyOn(backend, "fetchMessages")
      .mockResolvedValueOnce([tombstone]);

    act(() => useMessaging.getState().startEdit(target.messageId));
    act(() => useMessaging.getState().setEditDraft("消えた対象への保存"));
    act(() => useMessaging.getState().submitEdit());

    await waitFor(() => {
      expect(useMessaging.getState().editingMessageId).toBeNull();
      expect(fetch).toHaveBeenCalledWith(target.place, {
        beforeSeq: target.seq + 1,
        limit: 1,
      });
      expect(
        useMessaging
          .getState()
          .messagesByPlace["channel:ch-general"]?.find(
            (message) => message.messageId === target.messageId,
          ),
      ).toMatchObject({ deleted: true, revision: tombstone.revision });
    });
  });

  it("未知の編集失敗は無視せず編集欄に表示する", async () => {
    const backend = await bootStore();
    const target = useMessaging
      .getState()
      .messagesByPlace["channel:ch-general"]?.find(
        (message) =>
          message.author.kind === "human" &&
          message.author.humanId === "h-yohaku",
      );
    if (!target) throw new Error("target message was not loaded");
    vi.spyOn(backend, "editMessage").mockRejectedValueOnce(
      new MessagingAPIError("unexpected_edit_response", 418),
    );

    act(() => useMessaging.getState().startEdit(target.messageId));
    act(() => useMessaging.getState().setEditDraft("失敗を表示する"));
    act(() => useMessaging.getState().submitEdit());

    await waitFor(() =>
      expect(useMessaging.getState()).toMatchObject({
        editingMessageId: target.messageId,
        editFailure: "保存できませんでした。もう一度お試しください。",
      }),
    );
  });

  it("revision 3のWS後に遅れて届くrevision 2の409で競合本文と編集基準を戻さない", async () => {
    const backend = await bootStore();
    const target = useMessaging
      .getState()
      .messagesByPlace["channel:ch-general"]?.find(
        (message) =>
          message.author.kind === "human" &&
          message.author.humanId === "h-yohaku",
      );
    if (!target) throw new Error("target message was not loaded");
    const emit = (
      backend as unknown as {
        emit(event: { type: "message_edited"; message: Message }): void;
      }
    ).emit.bind(backend);
    const revision2 = {
      ...target,
      content: "revision 2 の競合本文",
      revision: 2,
    };
    const revision3 = {
      ...target,
      content: "revision 3 のWS本文",
      revision: 3,
    };
    let rejectFirstSave: ((error: unknown) => void) | undefined;
    const firstSave = new Promise<Message>((_resolve, reject) => {
      rejectFirstSave = reject;
    });
    const edit = vi
      .spyOn(backend, "editMessage")
      .mockImplementationOnce(() => firstSave)
      .mockResolvedValueOnce({
        ...revision3,
        content: "revision 3から保存",
        revision: 4,
      });

    act(() => useMessaging.getState().startEdit(target.messageId));
    act(() => useMessaging.getState().setEditDraft("自分の書きかけ"));
    act(() => useMessaging.getState().submitEdit());
    act(() => emit({ type: "message_edited", message: revision3 }));
    await act(async () => {
      rejectFirstSave?.(
        new MessagingAPIError("edit_conflict", 409, {
          message: conflictWire(revision2),
        }),
      );
      await firstSave.catch(() => undefined);
    });

    await waitFor(() => {
      expect(useMessaging.getState().editConflict).toEqual({
        content: "revision 3 のWS本文",
        revision: 3,
      });
    });
    expect(
      useMessaging
        .getState()
        .messagesByPlace["channel:ch-general"]?.find(
          (message) => message.messageId === target.messageId,
        ),
    ).toMatchObject({ content: "revision 3 のWS本文", revision: 3 });

    act(() => useMessaging.getState().reloadEditConflict());
    act(() => useMessaging.getState().setEditDraft("revision 3から保存"));
    act(() => useMessaging.getState().submitEdit());

    await waitFor(() =>
      expect(edit).toHaveBeenLastCalledWith(
        target.place,
        target.messageId,
        "revision 3から保存",
        3,
      ),
    );
  });

  it("revision 3の取り込み後にrevision 2のmessage_editedが届いても本文を戻さない", async () => {
    const backend = await bootStore();
    const target = useMessaging
      .getState()
      .messagesByPlace["channel:ch-general"]?.find(
        (message) =>
          message.author.kind === "human" &&
          message.author.humanId === "h-yohaku",
      );
    if (!target) throw new Error("target message was not loaded");
    const emit = (
      backend as unknown as {
        emit(event: { type: "message_edited"; message: Message }): void;
      }
    ).emit.bind(backend);

    act(() =>
      emit({
        type: "message_edited",
        message: { ...target, content: "revision 3", revision: 3 },
      }),
    );
    act(() =>
      emit({
        type: "message_edited",
        message: { ...target, content: "revision 2", revision: 2 },
      }),
    );

    expect(
      useMessaging
        .getState()
        .messagesByPlace["channel:ch-general"]?.find(
          (message) => message.messageId === target.messageId,
        ),
    ).toMatchObject({ content: "revision 3", revision: 3 });
  });

  it("WS切断中の成功応答で本文とrevisionを反映し、次の編集もそのrevisionを送る", async () => {
    const backend = await bootStore();
    const target = useMessaging
      .getState()
      .messagesByPlace["channel:ch-general"]?.find(
        (message) =>
          message.author.kind === "human" &&
          message.author.humanId === "h-yohaku",
      );
    if (!target) throw new Error("target message was not loaded");
    const committed = {
      ...target,
      content: "WSなしで確定した本文",
      editedAt: Date.UTC(2026, 7, 19, 12, 0, 0),
      revision: (target.revision ?? 1) + 1,
    };
    const afterRetry = {
      ...committed,
      content: "次の編集も成功",
      revision: (committed.revision ?? 1) + 1,
    };
    // mockはlive eventをemitしない。PATCH成功応答だけでtimelineが収束することを見る。
    const edit = vi
      .spyOn(backend, "editMessage")
      .mockResolvedValueOnce(committed)
      .mockResolvedValueOnce(afterRetry);

    act(() => useMessaging.getState().startEdit(target.messageId));
    act(() => useMessaging.getState().setEditDraft(committed.content));
    act(() => useMessaging.getState().submitEdit());

    await waitFor(() => {
      expect(
        useMessaging
          .getState()
          .messagesByPlace["channel:ch-general"]?.find(
            (message) => message.messageId === target.messageId,
          ),
      ).toMatchObject({
        content: committed.content,
        revision: committed.revision,
      });
      expect(useMessaging.getState().editingMessageId).toBeNull();
    });

    act(() => useMessaging.getState().startEdit(target.messageId));
    act(() => useMessaging.getState().setEditDraft(afterRetry.content));
    act(() => useMessaging.getState().submitEdit());

    await waitFor(() =>
      expect(edit).toHaveBeenLastCalledWith(
        target.place,
        target.messageId,
        afterRetry.content,
        committed.revision,
      ),
    );
  });

  it("保存中の追記は成功応答後も残し、次の保存は確定revisionを基準にする", async () => {
    const backend = await bootStore();
    const target = useMessaging
      .getState()
      .messagesByPlace["channel:ch-general"]?.find(
        (message) =>
          message.author.kind === "human" &&
          message.author.humanId === "h-yohaku",
      );
    if (!target) throw new Error("target message was not loaded");
    const submitted = "送った版";
    const appended = "送った版の追記";
    const committed = {
      ...target,
      content: submitted,
      editedAt: Date.UTC(2026, 7, 19, 12, 30, 0),
      revision: (target.revision ?? 1) + 1,
    };
    let resolveSave: ((message: Message) => void) | undefined;
    const save = new Promise<Message>((resolve) => {
      resolveSave = resolve;
    });
    const edit = vi
      .spyOn(backend, "editMessage")
      .mockImplementationOnce(() => save);

    act(() => useMessaging.getState().startEdit(target.messageId));
    act(() => useMessaging.getState().setEditDraft(submitted));
    act(() => useMessaging.getState().submitEdit());
    act(() => useMessaging.getState().setEditDraft(appended));

    await act(async () => {
      resolveSave?.(committed);
      await save;
    });

    await waitFor(() => {
      expect(useMessaging.getState()).toMatchObject({
        editingMessageId: target.messageId,
        editDraft: appended,
        editBaseRevision: committed.revision,
        editSavedWithPendingChanges: true,
      });
      expect(
        useMessaging
          .getState()
          .messagesByPlace["channel:ch-general"]?.find(
            (message) => message.messageId === target.messageId,
          ),
      ).toMatchObject({ content: submitted, revision: committed.revision });
    });

    act(() => useMessaging.getState().submitEdit());
    await waitFor(() =>
      expect(edit).toHaveBeenLastCalledWith(
        target.place,
        target.messageId,
        appended,
        committed.revision,
      ),
    );
  });

  it("追記中に自分のmessage_editedが先に届いても、ACKで競合を残さず確定revisionへ進める", async () => {
    const backend = await bootStore();
    const target = useMessaging
      .getState()
      .messagesByPlace["channel:ch-general"]?.find(
        (message) =>
          message.author.kind === "human" &&
          message.author.humanId === "h-yohaku",
      );
    if (!target) throw new Error("target message was not loaded");
    const submitted = "先に送った本文";
    const appended = "先に送った本文と追記";
    const committed = {
      ...target,
      content: submitted,
      revision: (target.revision ?? 1) + 1,
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

    act(() => useMessaging.getState().startEdit(target.messageId));
    act(() => useMessaging.getState().setEditDraft(submitted));
    act(() => useMessaging.getState().submitEdit());
    act(() => useMessaging.getState().setEditDraft(appended));
    act(() => emit({ type: "message_edited", message: committed }));

    expect(useMessaging.getState().editConflict).toBeNull();

    await act(async () => {
      resolveSave?.(committed);
      await save;
    });

    expect(useMessaging.getState()).toMatchObject({
      editingMessageId: target.messageId,
      editDraft: appended,
      editBaseRevision: committed.revision,
      editConflict: null,
      editSavedWithPendingChanges: true,
    });
  });

  it("保存中の二度目のsubmitはPATCHを送らない", async () => {
    const backend = await bootStore();
    const target = useMessaging
      .getState()
      .messagesByPlace["channel:ch-general"]?.find(
        (message) =>
          message.author.kind === "human" &&
          message.author.humanId === "h-yohaku",
      );
    if (!target) throw new Error("target message was not loaded");
    let resolveSave: ((message: Message) => void) | undefined;
    const save = new Promise<Message>((resolve) => {
      resolveSave = resolve;
    });
    const edit = vi
      .spyOn(backend, "editMessage")
      .mockImplementationOnce(() => save);

    act(() => useMessaging.getState().startEdit(target.messageId));
    act(() => useMessaging.getState().setEditDraft("一度だけ送る"));
    act(() => useMessaging.getState().submitEdit());
    act(() => useMessaging.getState().submitEdit());

    expect(edit).toHaveBeenCalledOnce();

    await act(async () => {
      resolveSave?.({
        ...target,
        content: "一度だけ送る",
        revision: (target.revision ?? 1) + 1,
      });
      await save;
    });
  });

  it("保存中の他者編集と409は従来どおり競合として残す", async () => {
    const backend = await bootStore();
    const target = useMessaging
      .getState()
      .messagesByPlace["channel:ch-general"]?.find(
        (message) =>
          message.author.kind === "human" &&
          message.author.humanId === "h-yohaku",
      );
    if (!target) throw new Error("target message was not loaded");
    const otherEdit = {
      ...target,
      content: "他者の本文",
      revision: (target.revision ?? 1) + 1,
    };
    const emit = (
      backend as unknown as {
        emit(event: { type: "message_edited"; message: Message }): void;
      }
    ).emit.bind(backend);
    let rejectSave: ((error: unknown) => void) | undefined;
    const save = new Promise<Message>((_resolve, reject) => {
      rejectSave = reject;
    });
    vi.spyOn(backend, "editMessage").mockImplementationOnce(() => save);

    act(() => useMessaging.getState().startEdit(target.messageId));
    act(() => useMessaging.getState().setEditDraft("自分の本文"));
    act(() => useMessaging.getState().submitEdit());
    act(() => emit({ type: "message_edited", message: otherEdit }));
    await act(async () => {
      rejectSave?.(
        new MessagingAPIError("edit_conflict", 409, {
          message: conflictWire(otherEdit),
        }),
      );
      await save.catch(() => undefined);
    });

    expect(useMessaging.getState()).toMatchObject({
      editingMessageId: target.messageId,
      editDraft: "自分の本文",
      editConflict: {
        content: otherEdit.content,
        revision: otherEdit.revision,
      },
    });
  });

  it("保存中に取消して別の編集を始めても、先の成功は新しいセッションを閉じない", async () => {
    const backend = await bootStore();
    const [first, second] = (
      useMessaging.getState().messagesByPlace["channel:ch-general"] ?? []
    ).filter(
      (message) =>
        message.author.kind === "human" &&
        message.author.humanId === "h-yohaku",
    );
    if (!first || !second) throw new Error("test messages were not loaded");

    let resolveFirstSave: ((message: Message) => void) | undefined;
    const firstSave = new Promise<Message>((resolve) => {
      resolveFirstSave = resolve;
    });
    const edit = vi
      .spyOn(backend, "editMessage")
      .mockImplementationOnce(() => firstSave);

    act(() => useMessaging.getState().startEdit(first.messageId));
    act(() => useMessaging.getState().setEditDraft("先の保存"));
    act(() => useMessaging.getState().submitEdit());
    expect(edit).toHaveBeenCalledWith(
      first.place,
      first.messageId,
      "先の保存",
      first.revision ?? 1,
    );

    act(() => useMessaging.getState().cancelEdit());
    act(() => useMessaging.getState().startEdit(second.messageId));
    act(() => useMessaging.getState().setEditDraft("新しい書きかけ"));

    await act(async () => {
      resolveFirstSave?.({
        ...first,
        content: "先の保存",
        revision: (first.revision ?? 1) + 1,
      });
      await firstSave;
    });

    expect(useMessaging.getState()).toMatchObject({
      editingMessageId: second.messageId,
      editDraft: "新しい書きかけ",
      editBaseRevision: second.revision ?? 1,
    });
  });
});
