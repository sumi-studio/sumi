import { afterEach, describe, expect, it, vi } from "vitest";
import { MessagingAPIError } from "./api-backend";
import { MockMessagingServer } from "./mock-server";
import type { Message } from "./model";
import {
  bindMessagingSessionIdentity,
  installMessagingBackend,
  useMessaging,
} from "./store";

function ownMessage(): Message {
  const target = useMessaging
    .getState()
    .messagesByPlace["channel:ch-general"]?.find(
      (message) =>
        message.author.kind === "human" &&
        message.author.humanId === "h-yohaku",
    );
  if (!target) throw new Error("target message was not loaded");
  return target;
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

describe("編集セッションのタイムライン整合性", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    bindMessagingSessionIdentity(null);
  });

  async function bootStore() {
    bindMessagingSessionIdentity("message-edit-session");
    const backend = new MockMessagingServer();
    installMessagingBackend(backend);
    useMessaging.getState().init();
    await vi.waitFor(() => expect(useMessaging.getState().ready).toBe(true));
    useMessaging.getState().selectPlace("channel:ch-general");
    await vi.waitFor(() =>
      expect(
        useMessaging.getState().messagesByPlace["channel:ch-general"],
      ).not.toHaveLength(0),
    );
    return backend;
  }

  it.each([
    "cancel",
    "save",
  ])("inline edit %s leaves the queued composer reply, text and attachment intact", async (finish) => {
    const backend = await bootStore();
    const key = "channel:ch-general";
    const messages = useMessaging.getState().messagesByPlace[key];
    const target = messages.find(
      (message) =>
        message.author.kind === "human" &&
        message.author.humanId === "h-yohaku",
    );
    const reply = messages.find(
      (message) => message.messageId !== target?.messageId,
    );
    if (!target || !reply) throw new Error("test messages were not loaded");
    useMessaging.getState().setDraft(key, "案内への返信を書きかけです");
    useMessaging.getState().setReplyTarget(reply.messageId);
    useMessaging
      .getState()
      .addDraftAttachments([
        new File(["invitation"], "案内.txt", { type: "text/plain" }),
      ]);
    await vi.waitFor(() =>
      expect(
        useMessaging.getState().draftByPlace[key].attachments[0].status,
      ).toBe("ready"),
    );
    const draft = useMessaging.getState().draftByPlace[key];

    useMessaging.getState().startEdit(target.messageId);
    expect(useMessaging.getState().draftByPlace[key]).toEqual(draft);
    useMessaging.getState().setEditDraft("以前の投稿の誤字を直しました");
    if (finish === "cancel") useMessaging.getState().cancelEdit();
    else useMessaging.getState().submitEdit();
    await vi.waitFor(() =>
      expect(useMessaging.getState().editingMessageId).toBeNull(),
    );
    expect(
      useMessaging
        .getState()
        .messagesByPlace[key].find(
          (message) => message.messageId === target.messageId,
        )?.content,
    ).toBe(finish === "save" ? "以前の投稿の誤字を直しました" : target.content);
    expect(useMessaging.getState().draftByPlace[key]).toEqual(draft);

    const send = vi.spyOn(backend, "sendMessage");
    useMessaging
      .getState()
      .send(useMessaging.getState().draftByPlace[key].text, "normal");
    expect(send).toHaveBeenCalledWith(
      expect.objectContaining({
        content: "案内への返信を書きかけです",
        replyTo: reply.messageId,
        attachments: [draft.attachments[0].attachment?.attachmentId],
      }),
    );
  });

  it("編集中の対象が message_deleted で消えると composer を通常状態へ戻す", async () => {
    const backend = await bootStore();
    const target = ownMessage();

    useMessaging.getState().startEdit(target.messageId);
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

    useMessaging.getState().startEdit(target.messageId);
    useMessaging.getState().setEditDraft("保存前の書きかけ");
    await backend.deleteMessage(other.place, other.messageId);

    expect(useMessaging.getState()).toMatchObject({
      editingMessageId: target.messageId,
      editDraft: "保存前の書きかけ",
    });
  });

  it("編集中に対象の message_edited を受けると書きかけを残して保存を止める", async () => {
    const backend = await bootStore();
    const target = ownMessage();

    useMessaging.getState().startEdit(target.messageId);
    useMessaging.getState().setEditDraft("自分の書きかけ");
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
    useMessaging.getState().submitEdit();
    expect(edit).not.toHaveBeenCalled();
  });

  it("catch-up が message_created で運ぶ他所の新しい版も、submit を待たず競合として止める", async () => {
    const backend = await bootStore();
    const target = ownMessage();
    const emit = (
      backend as unknown as {
        emit(event: { type: "message_created"; message: Message }): void;
      }
    ).emit.bind(backend);

    useMessaging.getState().startEdit(target.messageId);
    useMessaging.getState().setEditDraft("切断中の書きかけ");
    // 再接続の catch-up は現在版を message_created として再生する。
    emit({
      type: "message_created",
      message: {
        ...target,
        content: "別のタブで保存された本文",
        revision: (target.revision ?? 1) + 1,
      },
    });

    expect(useMessaging.getState()).toMatchObject({
      editingMessageId: target.messageId,
      editDraft: "切断中の書きかけ",
      editConflict: {
        content: "別のタブで保存された本文",
        revision: (target.revision ?? 1) + 1,
      },
    });
    const edit = vi.spyOn(backend, "editMessage");
    useMessaging.getState().submitEdit();
    expect(edit).not.toHaveBeenCalled();
  });

  it("409の現在メッセージを再読込の版として使い、次の保存を通す", async () => {
    const backend = await bootStore();
    const target = ownMessage();
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

    useMessaging.getState().startEdit(target.messageId);
    useMessaging.getState().setEditDraft("自分の書きかけ");
    useMessaging.getState().submitEdit();

    await vi.waitFor(() => {
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

    useMessaging.getState().reloadEditConflict();
    useMessaging.getState().setEditDraft("再読込後の保存");
    useMessaging.getState().submitEdit();

    await vi.waitFor(() =>
      expect(edit).toHaveBeenLastCalledWith(
        target.place,
        target.messageId,
        "再読込後の保存",
        2,
      ),
    );
    await vi.waitFor(() =>
      expect(useMessaging.getState().editingMessageId).toBeNull(),
    );
  });

  it("同一本文でも409正本が期待次版を越えていれば競合として残す", async () => {
    const backend = await bootStore();
    const target = ownMessage();
    const submitted = "偶然同じになった本文";
    const later = {
      ...target,
      content: submitted,
      revision: (target.revision ?? 1) + 2,
    };
    vi.spyOn(backend, "editMessage").mockRejectedValueOnce(
      new MessagingAPIError("edit_conflict", 409, {
        message: conflictWire(later),
      }),
    );

    useMessaging.getState().startEdit(target.messageId);
    useMessaging.getState().setEditDraft(submitted);
    useMessaging.getState().submitEdit();

    await vi.waitFor(() =>
      expect(useMessaging.getState().editConflict).toEqual({
        content: submitted,
        revision: later.revision,
      }),
    );
    expect(useMessaging.getState().editingMessageId).toBe(target.messageId);
  });

  it("WS切断中の409 message_deletedはtombstoneを反映して編集を閉じ、再保存しない", async () => {
    const backend = await bootStore();
    const target = ownMessage();
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

    useMessaging.getState().startEdit(target.messageId);
    useMessaging.getState().setEditDraft("WS切断中の保存");
    useMessaging.getState().submitEdit();

    await vi.waitFor(() => {
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

    useMessaging.getState().submitEdit();
    expect(edit).toHaveBeenCalledOnce();
  });

  it("404 not_foundは対象seqを再取得してtombstoneを反映する", async () => {
    const backend = await bootStore();
    const target = ownMessage();
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

    useMessaging.getState().startEdit(target.messageId);
    useMessaging.getState().setEditDraft("消えた対象への保存");
    useMessaging.getState().submitEdit();

    await vi.waitFor(() => {
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
    const target = ownMessage();
    vi.spyOn(backend, "editMessage").mockRejectedValueOnce(
      new MessagingAPIError("unexpected_edit_response", 418),
    );

    useMessaging.getState().startEdit(target.messageId);
    useMessaging.getState().setEditDraft("失敗を表示する");
    useMessaging.getState().submitEdit();

    await vi.waitFor(() =>
      expect(useMessaging.getState()).toMatchObject({
        editingMessageId: target.messageId,
        editFailure: "保存できませんでした。もう一度お試しください。",
      }),
    );
  });

  it("revision 3のWS後に遅れて届くrevision 2の409で競合本文と編集基準を戻さない", async () => {
    const backend = await bootStore();
    const target = ownMessage();
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

    useMessaging.getState().startEdit(target.messageId);
    useMessaging.getState().setEditDraft("自分の書きかけ");
    useMessaging.getState().submitEdit();
    emit({ type: "message_edited", message: revision3 });
    rejectFirstSave?.(
      new MessagingAPIError("edit_conflict", 409, {
        message: conflictWire(revision2),
      }),
    );
    await firstSave.catch(() => undefined);

    // Drain the store's promise continuations before checking unchanged state.
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
    await vi.waitFor(() => {
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

    useMessaging.getState().reloadEditConflict();
    useMessaging.getState().setEditDraft("revision 3から保存");
    useMessaging.getState().submitEdit();

    await vi.waitFor(() =>
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
    const target = ownMessage();
    const emit = (
      backend as unknown as {
        emit(event: { type: "message_edited"; message: Message }): void;
      }
    ).emit.bind(backend);

    emit({
      type: "message_edited",
      message: { ...target, content: "revision 3", revision: 3 },
    });
    emit({
      type: "message_edited",
      message: { ...target, content: "revision 2", revision: 2 },
    });

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
    const target = ownMessage();
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

    useMessaging.getState().startEdit(target.messageId);
    useMessaging.getState().setEditDraft(committed.content);
    useMessaging.getState().submitEdit();

    await vi.waitFor(() => {
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

    useMessaging.getState().startEdit(target.messageId);
    useMessaging.getState().setEditDraft(afterRetry.content);
    useMessaging.getState().submitEdit();

    await vi.waitFor(() =>
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
    const target = ownMessage();
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

    useMessaging.getState().startEdit(target.messageId);
    useMessaging.getState().setEditDraft(submitted);
    useMessaging.getState().submitEdit();
    useMessaging.getState().setEditDraft(appended);

    resolveSave?.(committed);
    await save;

    await vi.waitFor(() => {
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

    useMessaging.getState().submitEdit();
    await vi.waitFor(() =>
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
    const target = ownMessage();
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

    useMessaging.getState().startEdit(target.messageId);
    useMessaging.getState().setEditDraft(submitted);
    useMessaging.getState().submitEdit();
    useMessaging.getState().setEditDraft(appended);
    emit({ type: "message_edited", message: committed });

    expect(useMessaging.getState().editConflict).toBeNull();

    resolveSave?.(committed);
    await save;

    await vi.waitFor(() => {
      expect(useMessaging.getState()).toMatchObject({
        editingMessageId: target.messageId,
        editDraft: appended,
        editBaseRevision: committed.revision,
        editConflict: null,
        editSavedWithPendingChanges: true,
      });
    });
  });

  it("保存中の二度目のsubmitはPATCHを送らない", async () => {
    const backend = await bootStore();
    const target = ownMessage();
    let resolveSave: ((message: Message) => void) | undefined;
    const save = new Promise<Message>((resolve) => {
      resolveSave = resolve;
    });
    const edit = vi
      .spyOn(backend, "editMessage")
      .mockImplementationOnce(() => save);

    useMessaging.getState().startEdit(target.messageId);
    useMessaging.getState().setEditDraft("一度だけ送る");
    useMessaging.getState().submitEdit();
    useMessaging.getState().submitEdit();

    expect(edit).toHaveBeenCalledOnce();

    resolveSave?.({
      ...target,
      content: "一度だけ送る",
      revision: (target.revision ?? 1) + 1,
    });
    await save;
    await vi.waitFor(() =>
      expect(useMessaging.getState().editingMessageId).toBeNull(),
    );
    expect(edit).toHaveBeenCalledOnce();
  });

  it("保存中の他者編集と409は従来どおり競合として残す", async () => {
    const backend = await bootStore();
    const target = ownMessage();
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

    useMessaging.getState().startEdit(target.messageId);
    useMessaging.getState().setEditDraft("自分の本文");
    useMessaging.getState().submitEdit();
    emit({ type: "message_edited", message: otherEdit });
    rejectSave?.(
      new MessagingAPIError("edit_conflict", 409, {
        message: conflictWire(otherEdit),
      }),
    );
    await save.catch(() => undefined);

    // Drain the store's promise continuations before checking unchanged state.
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
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

    useMessaging.getState().startEdit(first.messageId);
    useMessaging.getState().setEditDraft("先の保存");
    useMessaging.getState().submitEdit();
    expect(edit).toHaveBeenCalledWith(
      first.place,
      first.messageId,
      "先の保存",
      first.revision ?? 1,
    );

    useMessaging.getState().cancelEdit();
    useMessaging.getState().startEdit(second.messageId);
    useMessaging.getState().setEditDraft("新しい書きかけ");

    resolveFirstSave?.({
      ...first,
      content: "先の保存",
      revision: (first.revision ?? 1) + 1,
    });
    await firstSave;

    // The backend promise resolves before the store applies its acknowledgement.
    await vi.waitFor(() => {
      expect(
        useMessaging
          .getState()
          .messagesByPlace["channel:ch-general"]?.find(
            (message) => message.messageId === first.messageId,
          ),
      ).toMatchObject({
        content: "先の保存",
        revision: (first.revision ?? 1) + 1,
      });
    });
    expect(useMessaging.getState()).toMatchObject({
      editingMessageId: second.messageId,
      editDraft: "新しい書きかけ",
      editBaseRevision: second.revision ?? 1,
    });
  });

  it("保存中に取消して同じメッセージを開き直すと、先の成功ACKは開き直した編集の base だけ進める", async () => {
    const backend = await bootStore();
    const target = ownMessage();
    const committedRevision = (target.revision ?? 1) + 1;

    let resolveFirstSave: ((message: Message) => void) | undefined;
    const firstSave = new Promise<Message>((resolve) => {
      resolveFirstSave = resolve;
    });
    const edit = vi
      .spyOn(backend, "editMessage")
      .mockImplementationOnce(() => firstSave)
      .mockImplementationOnce(
        async (_place, _messageId, content, revision) => ({
          ...target,
          content,
          revision: revision + 1,
        }),
      );

    useMessaging.getState().startEdit(target.messageId);
    useMessaging.getState().setEditDraft("先の保存");
    useMessaging.getState().submitEdit();
    // 「保存中…」の間に取り消して、同じメッセージをもう一度開く。
    // ACK 前なので編集欄は旧本文・旧 revision で開く。
    useMessaging.getState().cancelEdit();
    useMessaging.getState().startEdit(target.messageId);
    expect(useMessaging.getState()).toMatchObject({
      editDraft: target.content,
      editBaseRevision: target.revision ?? 1,
    });
    useMessaging.getState().setEditDraft("開き直して直した本文");

    resolveFirstSave?.({
      ...target,
      content: "先の保存",
      revision: committedRevision,
    });
    await firstSave;

    // 書きかけは残し、base だけが確定 revision へ進む。競合にはならない。
    await vi.waitFor(() => {
      expect(useMessaging.getState()).toMatchObject({
        editingMessageId: target.messageId,
        editDraft: "開き直して直した本文",
        editBaseRevision: committedRevision,
        editConflict: null,
      });
    });
    useMessaging.getState().submitEdit();
    await vi.waitFor(() =>
      expect(edit).toHaveBeenLastCalledWith(
        target.place,
        target.messageId,
        "開き直して直した本文",
        committedRevision,
      ),
    );
    await vi.waitFor(() =>
      expect(useMessaging.getState().editingMessageId).toBeNull(),
    );
  });

  it("開き直した編集に自分の echo が先に届いて競合になっても、ACK が畳んで base を進める", async () => {
    const backend = await bootStore();
    const target = ownMessage();
    const committed = {
      ...target,
      content: "先の保存",
      revision: (target.revision ?? 1) + 1,
    };
    const emit = (
      backend as unknown as {
        emit(event: { type: "message_edited"; message: Message }): void;
      }
    ).emit.bind(backend);
    let resolveFirstSave: ((message: Message) => void) | undefined;
    const firstSave = new Promise<Message>((resolve) => {
      resolveFirstSave = resolve;
    });
    vi.spyOn(backend, "editMessage").mockImplementationOnce(() => firstSave);

    useMessaging.getState().startEdit(target.messageId);
    useMessaging.getState().setEditDraft("先の保存");
    useMessaging.getState().submitEdit();
    useMessaging.getState().cancelEdit();
    useMessaging.getState().startEdit(target.messageId);
    useMessaging.getState().setEditDraft("開き直しの書きかけ");

    // 開き直した session は送信中ではないので、自分の echo を echo と見分けられず
    // いったん競合になる（本文と送信中 session でしか照合できない）。
    emit({ type: "message_edited", message: committed });
    expect(useMessaging.getState().editConflict).toEqual({
      content: committed.content,
      revision: committed.revision,
    });

    resolveFirstSave?.(committed);
    await firstSave;

    // 成功 ACK R は base..R に他者の編集が無いことの確定。R 以下の競合は畳む。
    await vi.waitFor(() => {
      expect(useMessaging.getState()).toMatchObject({
        editingMessageId: target.messageId,
        editDraft: "開き直しの書きかけ",
        editBaseRevision: committed.revision,
        editConflict: null,
      });
    });
  });
});
