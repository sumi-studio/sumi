import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MessagingAPIError } from "./api-backend";
import type {
  ConnectionState,
  Message,
  MessagingBackend,
  Place,
  ReactionMutationResult,
  ServerEvent,
  ThreadSummary,
} from "./model";
import {
  bindMessagingSessionIdentity,
  installMessagingBackend,
  useMessaging,
} from "./store";

const place: Place = { kind: "channel", channelId: "channel-1" };
const placeKey = "channel:channel-1" as const;

/**
 * 履歴を持っている状態を作る。storeが履歴を受け付けるのはheldなplaceだけなので、
 * stateへ直接置くのではなく画面を開く経路（selectPlace → loadPlace）で作る。
 * 読み込みのfetchは記録から外し、後続のresyncだけを見えるようにする。
 */
async function holdLoaded(
  harness: StubBackend,
  messages: Message[],
): Promise<void> {
  harness.history = messages;
  useMessaging.getState().selectPlace(placeKey);
  await harness.settle();
  harness.fetches.splice(0);
}

/**
 * reaction eventはmessage全体を運ばない、という契約のstore側の裏。編集を
 * 巻き戻さないこと、切断中に落ちたreactionが再接続で戻ることを見る。
 */
describe("reaction convergence in the messaging store", () => {
  let session = 0;

  beforeEach(() => {
    bindMessagingSessionIdentity(`reaction-test-${++session}`);
  });

  afterEach(() => {
    bindMessagingSessionIdentity(null);
  });

  it("patches only reactions and re-reads the loaded window on caught_up", async () => {
    const harness = new StubBackend();
    installMessagingBackend(harness);
    useMessaging.getState().init();
    await harness.bootstrapped;
    await holdLoaded(harness, [message(1, "編集前"), message(2, "隣")]);

    // 編集がcommit/publishしたあとに、編集前のcontentを見ていたreaction eventが
    // 遅れて届く。reactionだけが乗るので編集は生き残る。
    harness.emit({ type: "message_edited", message: message(1, "編集後", 99) });
    harness.emit({
      type: "reaction_updated",
      place,
      messageId: "message-1",
      reactions: [{ emoji: "👍", participants: [self] }],
    });
    const afterEdit = useMessaging.getState().messagesByPlace[placeKey] ?? [];
    expect(afterEdit[0]).toMatchObject({
      content: "編集後",
      editedAt: 99,
      reactions: [{ emoji: "👍", participants: [self] }],
    });

    // 未ロードのmessageへのreactionはstateを触らない。
    const untouched = useMessaging.getState().messagesByPlace;
    harness.emit({
      type: "reaction_updated",
      place,
      messageId: "message-404",
      reactions: [{ emoji: "🎉", participants: [self] }],
    });
    expect(useMessaging.getState().messagesByPlace).toBe(untouched);

    // 切断中に落ちたreactionはcatch-upでreplayされない（seqが進まないので）。
    // caught_upでロード済みwindowを読み直して収束する。
    harness.history = [
      { ...message(1, "編集後", 99), reactions: [] },
      {
        ...message(2, "隣"),
        reactions: [{ emoji: "🎉", participants: [self] }],
      },
    ];
    harness.emit({ type: "caught_up", place });
    await harness.settle();
    expect(harness.fetches).toEqual([{ beforeSeq: 3, limit: 2 }]);
    const resynced = useMessaging.getState().messagesByPlace[placeKey] ?? [];
    expect(resynced[0]).toMatchObject({ content: "編集後", reactions: [] });
    expect(resynced[1]?.reactions).toEqual([
      { emoji: "🎉", participants: [self] },
    ]);

    // 変化がなければmessageの参照は保たれる（無駄な再描画を作らない）。
    const stable = useMessaging.getState().messagesByPlace[placeKey];
    harness.emit({ type: "caught_up", place });
    await harness.settle();
    expect(useMessaging.getState().messagesByPlace[placeKey]).toBe(stable);

    // loadOlderで200件を超えていても、最古のロード済みmessageまで
    // before_seqで遡り、切断中にreactionを落としたままにしない。
    const loaded = Array.from({ length: 205 }, (_, index) =>
      message(index + 1, `message ${index + 1}`),
    );
    harness.history = loaded.map((entry) =>
      entry.seq === 1
        ? {
            ...entry,
            reactions: [{ emoji: "👀", participants: [self] }],
          }
        : entry,
    );
    harness.fetches.length = 0;
    useMessaging.setState({ messagesByPlace: { [placeKey]: loaded } });
    harness.emit({ type: "caught_up", place });
    await harness.settle();

    expect(harness.fetches).toEqual([
      { beforeSeq: 206, limit: 200 },
      { beforeSeq: 6, limit: 5 },
    ]);
    expect(
      useMessaging.getState().messagesByPlace[placeKey]?.[0]?.reactions,
    ).toEqual([{ emoji: "👀", participants: [self] }]);
  });

  it("re-reads each loaded range without traversing unloaded gaps", async () => {
    const harness = new StubBackend();
    installMessagingBackend(harness);
    useMessaging.getState().init();
    await harness.bootstrapped;

    const loaded = [
      ...Array.from({ length: 50 }, (_, index) =>
        message(index + 1, `old ${index + 1}`),
      ),
      ...Array.from({ length: 50 }, (_, index) =>
        message(index + 951, `new ${index + 951}`),
      ),
    ];
    harness.history = loaded.map((entry) => {
      if (entry.seq === 1) {
        return {
          ...entry,
          reactions: [{ emoji: "👀", participants: [self] }],
        };
      }
      if (entry.seq === 951) {
        return {
          ...entry,
          reactions: [{ emoji: "🎉", participants: [self] }],
        };
      }
      return entry;
    });
    useMessaging.setState({ messagesByPlace: { [placeKey]: loaded } });

    harness.emit({ type: "caught_up", place });
    await harness.settle();

    expect(harness.fetches).toEqual([
      { beforeSeq: 1001, limit: 50 },
      { beforeSeq: 51, limit: 50 },
    ]);
    const resynced = useMessaging.getState().messagesByPlace[placeKey] ?? [];
    expect(resynced.find((entry) => entry.seq === 1)?.reactions).toEqual([
      { emoji: "👀", participants: [self] },
    ]);
    expect(resynced.find((entry) => entry.seq === 951)?.reactions).toEqual([
      { emoji: "🎉", participants: [self] },
    ]);
  });

  it("replays live reaction updates over an older resync snapshot", async () => {
    const harness = new StubBackend();
    installMessagingBackend(harness);
    useMessaging.getState().init();
    await harness.bootstrapped;
    useMessaging.setState({
      messagesByPlace: { [placeKey]: [message(1, "message")] },
    });
    harness.history = [message(1, "message")];
    harness.holdFetches = true;

    harness.emit({ type: "caught_up", place });
    await harness.settle();
    expect(harness.heldFetchCount).toBe(1);

    harness.emit({
      type: "reaction_updated",
      place,
      messageId: "message-1",
      reactions: [{ emoji: "👍", participants: [self] }],
    });
    harness.releaseFetches();
    await harness.settle();

    expect(
      useMessaging.getState().messagesByPlace[placeKey]?.[0]?.reactions,
    ).toEqual([{ emoji: "👍", participants: [self] }]);
  });

  it("does not replay an earlier generation over a newer resync snapshot", async () => {
    const harness = new StubBackend();
    installMessagingBackend(harness);
    useMessaging.getState().init();
    await harness.bootstrapped;
    useMessaging.setState({
      messagesByPlace: { [placeKey]: [message(1, "message")] },
    });
    harness.history = [message(1, "message")];
    harness.holdFetches = true;

    // Resync A captures the old snapshot. E1 then arrives live and is queued
    // only for A.
    harness.emit({ type: "caught_up", place });
    await harness.settle();
    harness.emit({
      type: "reaction_updated",
      place,
      messageId: "message-1",
      reactions: [{ emoji: "👍", participants: [self] }],
    });

    // Before resync B starts, the server has committed the later E2 state.
    // Its echo is deliberately absent; B's snapshot is the only E2 evidence.
    harness.history = [
      {
        ...message(1, "message"),
        reactions: [{ emoji: "🎉", participants: [self] }],
      },
    ];
    harness.emit({ type: "caught_up", place });
    await harness.settle();
    // B is queued behind A and therefore has no chance to inherit A's journal.
    expect(harness.heldFetchCount).toBe(1);

    harness.releaseFetches();
    await harness.settle();
    // B starts with a fresh journal only after A has settled.
    expect(harness.heldFetchCount).toBe(1);
    harness.releaseFetches();
    await harness.settle();

    expect(
      useMessaging.getState().messagesByPlace[placeKey]?.[0]?.reactions,
    ).toEqual([{ emoji: "🎉", participants: [self] }]);
  });

  it("replays newer live state after a delayed canonical toggle ACK", async () => {
    const harness = new StubBackend();
    const acknowledgement = deferred<ReactionMutationResult>();
    harness.toggleResults.push(acknowledgement.promise);
    installMessagingBackend(harness);
    useMessaging.getState().init();
    await harness.bootstrapped;
    const target = message(1, "message");
    useMessaging.setState({ messagesByPlace: { [placeKey]: [target] } });

    useMessaging.getState().toggleReaction(target, "👍");
    await harness.settle();
    expect(harness.toggleReaction).toHaveBeenCalledTimes(1);

    const newer = [
      { emoji: "👍", participants: [self] },
      { emoji: "🎉", participants: [other] },
    ];
    harness.emit({
      type: "reaction_updated",
      place,
      messageId: target.messageId,
      reactions: newer,
    });
    acknowledgement.resolve({
      messageId: target.messageId,
      reactions: [{ emoji: "👍", participants: [self] }],
    });
    await harness.settle();

    expect(
      useMessaging.getState().messagesByPlace[placeKey]?.[0]?.reactions,
    ).toEqual(newer);
  });

  it("converges when an older published WS frame is delivered during a toggle", async () => {
    const harness = new StubBackend();
    const acknowledgement = deferred<ReactionMutationResult>();
    harness.toggleResults.push(acknowledgement.promise);
    installMessagingBackend(harness);
    useMessaging.getState().init();
    await harness.bootstrapped;
    const target = message(1, "message");
    useMessaging.setState({ messagesByPlace: { [placeKey]: [target] } });

    useMessaging.getState().toggleReaction(target, "👍");
    await harness.settle();
    const olderPublished = [{ emoji: "🎉", participants: [other] }];
    harness.emit({
      type: "reaction_updated",
      place,
      messageId: target.messageId,
      reactions: olderPublished,
    });
    const ownCommitted = [
      ...olderPublished,
      { emoji: "👍", participants: [self] },
    ];
    acknowledgement.resolve({
      messageId: target.messageId,
      reactions: ownCommitted,
    });
    await harness.settle();

    // Without a wire revision, replaying the frame delivered during the
    // request can temporarily restore O. The server enqueues our own E_A after
    // O on the same WS before returning the HTTP response, so WS FIFO converges
    // to E_A; disconnect/overflow instead converges through caught_up resync.
    expect(
      useMessaging.getState().messagesByPlace[placeKey]?.[0]?.reactions,
    ).toEqual(olderPublished);
    harness.emit({
      type: "reaction_updated",
      place,
      messageId: target.messageId,
      reactions: ownCommitted,
    });
    expect(
      useMessaging.getState().messagesByPlace[placeKey]?.[0]?.reactions,
    ).toEqual(ownCommitted);
  });

  it("serializes concurrent local toggles", async () => {
    const harness = new StubBackend();
    const first = deferred<ReactionMutationResult>();
    const second = deferred<ReactionMutationResult>();
    harness.toggleResults.push(first.promise, second.promise);
    installMessagingBackend(harness);
    useMessaging.getState().init();
    await harness.bootstrapped;
    const target = message(1, "message");
    useMessaging.setState({ messagesByPlace: { [placeKey]: [target] } });

    useMessaging.getState().toggleReaction(target, "👍");
    useMessaging.getState().toggleReaction(target, "👍");
    await harness.settle();
    expect(harness.toggleReaction).toHaveBeenCalledTimes(1);

    first.resolve({
      messageId: target.messageId,
      reactions: [{ emoji: "👍", participants: [self] }],
    });
    await harness.settle();
    expect(harness.toggleReaction).toHaveBeenCalledTimes(2);

    second.resolve({ messageId: target.messageId, reactions: [] });
    await harness.settle();
    expect(
      useMessaging.getState().messagesByPlace[placeKey]?.[0]?.reactions,
    ).toEqual([]);
  });

  it("orders resync after an in-flight local toggle", async () => {
    const harness = new StubBackend();
    const acknowledgement = deferred<ReactionMutationResult>();
    harness.toggleResults.push(acknowledgement.promise);
    harness.holdFetches = true;
    installMessagingBackend(harness);
    useMessaging.getState().init();
    await harness.bootstrapped;
    const target = message(1, "message");
    useMessaging.setState({ messagesByPlace: { [placeKey]: [target] } });

    useMessaging.getState().toggleReaction(target, "👍");
    await harness.settle();
    harness.emit({
      type: "reaction_updated",
      place,
      messageId: target.messageId,
      reactions: [
        { emoji: "👍", participants: [self] },
        { emoji: "🎉", participants: [other] },
      ],
    });
    harness.history = [
      {
        ...target,
        reactions: [{ emoji: "👀", participants: [other] }],
      },
    ];
    harness.emit({ type: "caught_up", place });
    await harness.settle();
    expect(harness.fetches).toEqual([]);

    acknowledgement.resolve({
      messageId: target.messageId,
      reactions: [{ emoji: "👍", participants: [self] }],
    });
    await harness.settle();
    expect(harness.heldFetchCount).toBe(1);

    harness.releaseFetches();
    await harness.settle();
    expect(
      useMessaging.getState().messagesByPlace[placeKey]?.[0]?.reactions,
    ).toEqual([{ emoji: "👀", participants: [other] }]);
  });

  it("waits for an active resync before sending a local toggle", async () => {
    const harness = new StubBackend();
    harness.holdFetches = true;
    harness.toggleResults.push({
      messageId: "message-1",
      reactions: [{ emoji: "👍", participants: [self] }],
    });
    installMessagingBackend(harness);
    useMessaging.getState().init();
    await harness.bootstrapped;
    const target = message(1, "message");
    harness.history = [target];
    useMessaging.setState({ messagesByPlace: { [placeKey]: [target] } });

    harness.emit({ type: "caught_up", place });
    await harness.settle();
    expect(harness.heldFetchCount).toBe(1);

    useMessaging.getState().toggleReaction(target, "👍");
    await harness.settle();
    expect(harness.toggleReaction).not.toHaveBeenCalled();

    harness.releaseFetches();
    await harness.settle();
    expect(harness.toggleReaction).toHaveBeenCalledTimes(1);
    expect(
      useMessaging.getState().messagesByPlace[placeKey]?.[0]?.reactions,
    ).toEqual([{ emoji: "👍", participants: [self] }]);
  });

  it("does not restore reactions on a tombstone from a delayed toggle", async () => {
    const harness = new StubBackend();
    const acknowledgement = deferred<ReactionMutationResult>();
    harness.toggleResults.push(acknowledgement.promise);
    installMessagingBackend(harness);
    useMessaging.getState().init();
    await harness.bootstrapped;
    const target = message(1, "message");
    await holdLoaded(harness, [target]);

    useMessaging.getState().toggleReaction(target, "👍");
    await harness.settle();
    harness.emit({
      type: "message_deleted",
      message: { ...target, content: "", deleted: true, reactions: [] },
    });
    harness.emit({
      type: "reaction_updated",
      place,
      messageId: target.messageId,
      reactions: [{ emoji: "👍", participants: [self] }],
    });
    acknowledgement.resolve({
      messageId: target.messageId,
      reactions: [{ emoji: "👍", participants: [self] }],
    });
    await harness.settle();

    expect(
      useMessaging.getState().messagesByPlace[placeKey]?.[0],
    ).toMatchObject({ deleted: true, reactions: [] });
  });

  it("projects a replayed thread tombstone as a deletion", async () => {
    const harness = new StubBackend();
    const thread = threadSummary();
    harness.threadSummaries = [
      {
        ...thread,
        revision: 2,
        messageCount: 1,
        lastMessageAt: 2,
        lastMessage: "survives",
      },
    ];
    installMessagingBackend(harness);
    useMessaging.getState().init();
    await harness.bootstrapped;
    useMessaging.setState({ threadsById: { [thread.threadId]: thread } });

    // WebSocket replay represents the offline deletion as message_created
    // containing a tombstone, not as a message_deleted event.
    harness.emit({
      type: "message_created",
      message: {
        ...message(2, ""),
        place: { kind: "thread", threadId: thread.threadId },
        deleted: true,
      },
      notify: null,
    });
    await harness.settle();

    expect(harness.fetchThread).toHaveBeenCalledWith(thread.threadId);
    expect(useMessaging.getState().threadsById[thread.threadId]).toMatchObject({
      messageCount: 1,
      lastMessageAt: 2,
      lastMessage: "survives",
    });
  });

  it("keeps a tombstone when message_deleted precedes the PATCH response and replay", async () => {
    const harness = new StubBackend();
    const committed = deferred<Message>();
    harness.editMessage = vi.fn(async () => await committed.promise);
    installMessagingBackend(harness);
    useMessaging.getState().init();
    await harness.bootstrapped;
    const target = { ...message(1, "編集前"), revision: 1 };
    await holdLoaded(harness, [target]);

    useMessaging.getState().startEdit(target.messageId);
    useMessaging.getState().setEditDraft("PATCHで確定した本文");
    useMessaging.getState().submitEdit();
    expect(harness.editMessage).toHaveBeenCalledOnce();

    const tombstone = {
      ...target,
      content: "",
      mentions: [],
      reactions: [],
      attachments: [],
      deleted: true,
      revision: 3,
    };
    harness.emit({ type: "message_deleted", message: tombstone });
    committed.resolve({
      ...target,
      content: "PATCHで確定した本文",
      revision: 2,
    });
    await harness.settle();

    // reconnectのcatch-upはmessage_createdで運ばれるが、旧編集eventも同じ
    // 単調適用を通るためtombstoneを覆えない。
    harness.emit({
      type: "message_created",
      message: { ...target, content: "replayされた古い編集", revision: 2 },
      notify: null,
    });

    expect(
      useMessaging.getState().messagesByPlace[placeKey]?.[0],
    ).toMatchObject({ content: "", deleted: true, revision: 3 });
  });

  it("keeps a reaction published before a successful edit response", async () => {
    const harness = new StubBackend();
    const committed = deferred<Message>();
    harness.editMessage = vi.fn(async () => await committed.promise);
    installMessagingBackend(harness);
    useMessaging.getState().init();
    await harness.bootstrapped;
    const target = { ...message(1, "編集前"), revision: 1 };
    await holdLoaded(harness, [target]);

    useMessaging.getState().startEdit(target.messageId);
    useMessaging.getState().setEditDraft("PATCHで確定した本文");
    useMessaging.getState().submitEdit();
    harness.emit({
      type: "reaction_updated",
      place,
      messageId: target.messageId,
      reactions: [{ emoji: "👍", participants: [other] }],
    });
    committed.resolve({
      ...target,
      content: "PATCHで確定した本文",
      revision: 2,
      reactions: [],
    });
    await harness.settle();

    expect(
      useMessaging.getState().messagesByPlace[placeKey]?.[0],
    ).toMatchObject({
      content: "PATCHで確定した本文",
      revision: 2,
      reactions: [{ emoji: "👍", participants: [other] }],
    });
  });

  it("keeps a reaction published before a 409 edit-conflict response", async () => {
    const harness = new StubBackend();
    const target = { ...message(1, "編集前"), revision: 1 };
    const conflict = new MessagingAPIError("edit_conflict", 409);
    Object.defineProperty(conflict, "currentMessage", {
      value: {
        ...target,
        content: "別クライアントの編集",
        revision: 2,
        reactions: [],
      },
    });
    harness.editMessage = vi.fn(async () => {
      throw conflict;
    });
    installMessagingBackend(harness);
    useMessaging.getState().init();
    await harness.bootstrapped;
    await holdLoaded(harness, [target]);

    useMessaging.getState().startEdit(target.messageId);
    useMessaging.getState().setEditDraft("競合する編集");
    useMessaging.getState().submitEdit();
    harness.emit({
      type: "reaction_updated",
      place,
      messageId: target.messageId,
      reactions: [{ emoji: "🎉", participants: [other] }],
    });
    await harness.settle();

    expect(
      useMessaging.getState().messagesByPlace[placeKey]?.[0],
    ).toMatchObject({
      content: "別クライアントの編集",
      revision: 2,
      reactions: [{ emoji: "🎉", participants: [other] }],
    });
  });

  it("discards toggle ACKs and queued mutations from an earlier session", async () => {
    const oldHarness = new StubBackend();
    const acknowledgement = deferred<ReactionMutationResult>();
    oldHarness.toggleResults.push(acknowledgement.promise, {
      messageId: "message-1",
      reactions: [],
    });
    installMessagingBackend(oldHarness);
    useMessaging.getState().init();
    await oldHarness.bootstrapped;
    const oldTarget = message(1, "old session");
    useMessaging.setState({
      messagesByPlace: { [placeKey]: [oldTarget] },
    });
    useMessaging.getState().toggleReaction(oldTarget, "👍");
    useMessaging.getState().toggleReaction(oldTarget, "👍");
    await oldHarness.settle();
    expect(oldHarness.toggleReaction).toHaveBeenCalledTimes(1);

    bindMessagingSessionIdentity("reaction-test-toggle-new-session");
    const newHarness = new StubBackend();
    installMessagingBackend(newHarness);
    useMessaging.getState().init();
    await newHarness.bootstrapped;
    useMessaging.setState({
      messagesByPlace: {
        [placeKey]: [
          {
            ...message(1, "new session"),
            reactions: [{ emoji: "🎉", participants: [other] }],
          },
        ],
      },
    });

    acknowledgement.resolve({
      messageId: oldTarget.messageId,
      reactions: [{ emoji: "👍", participants: [self] }],
    });
    await oldHarness.settle();

    expect(oldHarness.toggleReaction).toHaveBeenCalledTimes(1);
    expect(
      useMessaging.getState().messagesByPlace[placeKey]?.[0],
    ).toMatchObject({
      content: "new session",
      reactions: [{ emoji: "🎉", participants: [other] }],
    });
  });

  it("discards a reaction snapshot from an earlier session", async () => {
    const oldHarness = new StubBackend();
    installMessagingBackend(oldHarness);
    useMessaging.getState().init();
    await oldHarness.bootstrapped;
    useMessaging.setState({
      messagesByPlace: { [placeKey]: [message(1, "old session")] },
    });
    oldHarness.history = [
      {
        ...message(1, "old session"),
        reactions: [{ emoji: "👀", participants: [self] }],
      },
    ];
    oldHarness.holdFetches = true;
    oldHarness.emit({ type: "caught_up", place });
    await oldHarness.settle();
    expect(oldHarness.heldFetchCount).toBe(1);

    bindMessagingSessionIdentity("reaction-test-new-session");
    const newHarness = new StubBackend();
    installMessagingBackend(newHarness);
    useMessaging.getState().init();
    await newHarness.bootstrapped;
    useMessaging.setState({
      messagesByPlace: {
        [placeKey]: [
          {
            ...message(1, "new session"),
            reactions: [{ emoji: "🎉", participants: [self] }],
          },
        ],
      },
    });

    oldHarness.releaseFetches();
    await oldHarness.settle();

    expect(
      useMessaging.getState().messagesByPlace[placeKey]?.[0],
    ).toMatchObject({
      content: "new session",
      reactions: [{ emoji: "🎉", participants: [self] }],
    });
  });

  it("does not apply a delayed deletion ACK to the replacement session", async () => {
    const oldHarness = new StubBackend();
    const deleted = deferred<Message>();
    oldHarness.deleteMessage = vi.fn(async () => await deleted.promise);
    installMessagingBackend(oldHarness);
    useMessaging.getState().init();
    await oldHarness.bootstrapped;
    const target = {
      ...message(1, "old session"),
      author: other,
      mentions: [self],
      revision: 1,
    };
    useMessaging.setState({
      activePlaceKey: placeKey,
      messagesByPlace: { [placeKey]: [target] },
      lastReadByPlace: { [placeKey]: 0 },
      unreadCountByPlace: { [placeKey]: 1 },
      mentionCountByPlace: { [placeKey]: 1 },
    });
    useMessaging.getState().deleteMessage(target.messageId);
    expect(oldHarness.deleteMessage).toHaveBeenCalledOnce();

    bindMessagingSessionIdentity("reaction-test-delete-new-session");
    const newHarness = new StubBackend();
    installMessagingBackend(newHarness);
    useMessaging.getState().init();
    await newHarness.bootstrapped;
    useMessaging.setState({
      activePlaceKey: placeKey,
      messagesByPlace: { [placeKey]: [{ ...target, content: "new session" }] },
      lastReadByPlace: { [placeKey]: 0 },
      unreadCountByPlace: { [placeKey]: 1 },
      mentionCountByPlace: { [placeKey]: 1 },
    });

    deleted.resolve({
      ...target,
      content: "",
      mentions: [],
      reactions: [],
      attachments: [],
      deleted: true,
      revision: 2,
    });
    await oldHarness.settle();

    expect(useMessaging.getState()).toMatchObject({
      unreadCountByPlace: { [placeKey]: 1 },
      mentionCountByPlace: { [placeKey]: 1 },
    });
    expect(
      useMessaging.getState().messagesByPlace[placeKey]?.[0],
    ).toMatchObject({ content: "new session", deleted: false });
  });
});

const self = { kind: "human", humanId: "human-1" } as const;
const other = { kind: "human", humanId: "human-2" } as const;

function deferred<T>(): {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (reason?: unknown) => void;
} {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((settle, fail) => {
    resolve = settle;
    reject = fail;
  });
  return { promise, resolve, reject };
}

function message(
  seq: number,
  content: string,
  editedAt: number | null = null,
): Message {
  return {
    messageId: `message-${seq}`,
    place,
    seq,
    author: self,
    content,
    mentions: [],
    urgency: "normal",
    reactions: [],
    attachments: [],
    poll: null,
    replyTo: null,
    createdAt: 1,
    editedAt,
    deleted: false,
  };
}

function threadSummary(): ThreadSummary {
  return {
    threadId: "thread-1",
    revision: 1,
    workspaceId: "workspace-1",
    parentPlace: { kind: "channel", channelId: "channel-1" },
    parentMessageId: "message-1",
    name: "Thread",
    messageCount: 2,
    lastMessageAt: 1,
    lastMessage: "deleted message",
    participants: [self],
    latestSeq: 2,
  };
}

class StubBackend implements MessagingBackend {
  readonly capabilities = {
    status: false,
    replyLater: false,
    reactions: true,
    polls: true,
    notifications: false,
  } as const;
  history: Message[] = [];
  threadSummaries: ThreadSummary[] = [];
  readonly fetches: { beforeSeq?: number; limit?: number }[] = [];
  readonly toggleResults: (
    | ReactionMutationResult
    | Promise<ReactionMutationResult>
  )[] = [];
  readonly pollVoteResults: (Message | Promise<Message>)[] = [];
  holdFetches = false;
  private heldFetches: {
    response: Message[];
    resolve: (messages: Message[]) => void;
  }[] = [];
  private listener: ((event: ServerEvent) => void) | null = null;
  private resolveBootstrapped!: () => void;
  readonly bootstrapped = new Promise<void>((resolve) => {
    this.resolveBootstrapped = resolve;
  });

  emit(event: ServerEvent): void {
    this.listener?.(event);
  }

  get heldFetchCount(): number {
    return this.heldFetches.length;
  }

  releaseFetches(): void {
    for (const fetch of this.heldFetches.splice(0)) {
      fetch.resolve(fetch.response);
    }
  }

  /** resyncのawait chain（fetch → set）を流し切る。 */
  async settle(): Promise<void> {
    await new Promise((resolve) => setTimeout(resolve, 0));
  }

  async bootstrap(): ReturnType<MessagingBackend["bootstrap"]> {
    // Store request fencing validates the response in its own microtask before
    // applying bootstrap.  Resolve the test readiness marker after that chain.
    queueMicrotask(() =>
      queueMicrotask(() => queueMicrotask(() => this.resolveBootstrapped())),
    );
    return {
      self,
      workspaces: [],
      channels: [
        {
          channelId: "channel-1",
          workspaceId: "workspace-1",
          revision: 1,
          name: "general",
          topic: "",
          visibility: "public",
          voice: false,
        },
      ],
      dms: [],
      members: [],
      statuses: [],
      readMarkers: [],
      unreadSummaries: [],
      replyLaterMarkers: [],
      notificationSetting: {
        owner: self,
        defaults: { level: "all" },
        perPlace: [],
        keywords: [],
      },
      employedAgents: [],
    };
  }

  async fetchMessages(
    _place: Place,
    options: { beforeSeq?: number; limit?: number } = {},
  ): Promise<Message[]> {
    this.fetches.push(options);
    const beforeSeq = options.beforeSeq ?? Number.POSITIVE_INFINITY;
    const limit = options.limit ?? 50;
    const eligible = this.history.filter((entry) => entry.seq < beforeSeq);
    const response = eligible.slice(Math.max(0, eligible.length - limit));
    if (!this.holdFetches) return response;
    return new Promise((resolve) => {
      this.heldFetches.push({ response, resolve });
    });
  }

  fetchThreads = vi.fn(async (_parent: Place) => this.threadSummaries);

  fetchThread = vi.fn(async (threadId: string) => {
    const thread = this.threadSummaries.find(
      (summary) => summary.threadId === threadId,
    );
    if (!thread) throw new Error("thread not found");
    return thread;
  });

  async searchMessages(): Promise<import("./model").MessageSearchResult[]> {
    return [];
  }

  createChannel = vi.fn();
  ensureDM = vi.fn();
  createGroupDM = vi.fn();
  updateChannel = vi.fn();
  duplicateChannel = vi.fn();
  fetchPresence = vi.fn(async () => ({
    statuses: [],
    replyLaterMarkers: [],
  }));

  async uploadAttachment(): Promise<never> {
    throw new Error("uploadAttachment is not part of this test");
  }

  async updateDraftAttachment(): Promise<never> {
    throw new Error("updateDraftAttachment is not part of this test");
  }
  attachmentURL(attachmentId: string): string {
    return `/test/attachments/${attachmentId}`;
  }
  sendMessage = vi.fn();
  editMessage = async (): ReturnType<MessagingBackend["editMessage"]> => {
    throw new Error("editMessage is not part of this test");
  };
  deleteMessage = vi.fn(async (): Promise<Message> => {
    throw new Error("deleteMessage is not part of this test");
  });
  markRead = vi.fn(async () => undefined);
  setStatus = vi.fn(async () => {
    throw new Error("unused");
  });
  createReplyLater = vi.fn(async () => {
    throw new Error("unused");
  });
  resolveReplyLater = vi.fn(async () => {
    throw new Error("unused");
  });
  toggleReaction = vi.fn(
    async (
      _place: Place,
      messageId: string,
      _emoji: string,
    ): Promise<ReactionMutationResult> =>
      await (this.toggleResults.shift() ?? { messageId, reactions: [] }),
  );
  votePoll = vi.fn(
    async (_place: Place, _messageId: string, _optionIds: string[]) =>
      await (this.pollVoteResults.shift() ?? message(1, "message")),
  );
  async setNotificationSetting(): ReturnType<
    MessagingBackend["setNotificationSetting"]
  > {
    throw new Error("unused");
  }
  sendTyping = vi.fn();

  subscribe(listener: (event: ServerEvent) => void): () => void {
    this.listener = listener;
    return () => {
      this.listener = null;
    };
  }

  subscribeConnection(_listener: (state: ConnectionState) => void): () => void {
    return () => undefined;
  }

  dispose(): void {
    this.listener = null;
  }
}

function pollMessage(revision = 0, allowMulti = true): Message {
  return {
    ...message(1, "poll message"),
    revision: 1,
    poll: {
      question: "which?",
      allowMulti,
      closesAt: null,
      revision,
      options: [
        { optionId: "a", text: "A", voters: [] },
        { optionId: "b", text: "B", voters: [] },
      ],
    },
  };
}

function requiredPoll(message: Message): NonNullable<Message["poll"]> {
  if (!message.poll) throw new Error("poll fixture missing");
  return message.poll;
}

describe("poll convergence in the messaging store", () => {
  let session = 0;

  beforeEach(() => {
    bindMessagingSessionIdentity(`poll-test-${++session}`);
  });

  afterEach(() => {
    bindMessagingSessionIdentity(null);
  });

  it("applies poll-only live state and never rolls it back through resync or edit", async () => {
    const harness = new StubBackend();
    installMessagingBackend(harness);
    useMessaging.getState().init();
    await harness.bootstrapped;
    const initial = {
      ...pollMessage(0),
      content: "edited content",
      editedAt: 9,
    };
    await holdLoaded(harness, [initial]);
    harness.history = [
      {
        ...initial,
        poll: {
          ...requiredPoll(initial),
          revision: 1,
          options: [
            { optionId: "a", text: "A", voters: [self] },
            { optionId: "b", text: "B", voters: [] },
          ],
        },
      },
    ];
    harness.holdFetches = true;

    harness.emit({ type: "caught_up", place });
    await harness.settle();
    harness.emit({
      type: "poll_updated",
      place,
      messageId: initial.messageId,
      poll: {
        ...requiredPoll(initial),
        revision: 3,
        options: [
          { optionId: "a", text: "A", voters: [] },
          { optionId: "b", text: "B", voters: [other] },
        ],
      },
    });
    harness.releaseFetches();
    await harness.settle();

    harness.emit({
      type: "message_edited",
      message: {
        ...initial,
        content: "newer edit",
        revision: 2,
        poll: {
          ...requiredPoll(initial),
          revision: 2,
          options: [
            { optionId: "a", text: "A", voters: [self] },
            { optionId: "b", text: "B", voters: [] },
          ],
        },
      },
    });
    harness.emit({
      type: "poll_updated",
      place,
      messageId: initial.messageId,
      poll: { ...requiredPoll(initial), revision: 1 },
    });

    const projected = useMessaging.getState().messagesByPlace[placeKey]?.[0];
    expect(projected).toMatchObject({ content: "newer edit", revision: 2 });
    expect(projected?.poll?.revision).toBe(3);
    expect(projected?.poll?.options[1]?.voters).toEqual([other]);
  });

  it("serializes whole-selection intents and lets a later vote run after failure", async () => {
    const harness = new StubBackend();
    const first = deferred<Message>();
    const second = deferred<Message>();
    harness.pollVoteResults.push(first.promise, second.promise);
    installMessagingBackend(harness);
    useMessaging.getState().init();
    await harness.bootstrapped;
    const initial = pollMessage();
    await holdLoaded(harness, [initial]);

    const firstTask = useMessaging.getState().votePoll(initial, ["a"]);
    const firstFailure = expect(firstTask).rejects.toThrow("first failed");
    const secondTask = useMessaging.getState().votePoll(initial, ["a", "b"]);
    await harness.settle();
    expect(harness.votePoll).toHaveBeenCalledTimes(1);
    expect(harness.votePoll).toHaveBeenLastCalledWith(place, "message-1", [
      "a",
    ]);
    expect(
      useMessaging.getState().pollVoteByMessage[initial.messageId],
    ).toEqual(
      expect.objectContaining({ optionIds: ["a", "b"], pending: true }),
    );

    first.reject(new Error("first failed"));
    await firstFailure;
    await harness.settle();
    expect(harness.votePoll).toHaveBeenCalledTimes(2);
    expect(harness.votePoll).toHaveBeenLastCalledWith(place, "message-1", [
      "a",
      "b",
    ]);

    second.resolve({
      ...initial,
      poll: {
        ...requiredPoll(initial),
        revision: 2,
        options: [
          { optionId: "a", text: "A", voters: [self] },
          { optionId: "b", text: "B", voters: [self] },
        ],
      },
    });
    await secondTask;
    expect(
      useMessaging.getState().messagesByPlace[placeKey]?.[0]?.poll,
    ).toMatchObject({
      revision: 2,
      options: [{ voters: [self] }, { voters: [self] }],
    });
    expect(
      useMessaging.getState().pollVoteByMessage[initial.messageId],
    ).toBeUndefined();
  });

  it("does not let a lower-revision HTTP acknowledgement roll back live state", async () => {
    const harness = new StubBackend();
    const acknowledgement = deferred<Message>();
    harness.pollVoteResults.push(acknowledgement.promise);
    installMessagingBackend(harness);
    useMessaging.getState().init();
    await harness.bootstrapped;
    const initial = pollMessage();
    useMessaging.setState({ messagesByPlace: { [placeKey]: [initial] } });

    const task = useMessaging.getState().votePoll(initial, ["a"]);
    harness.emit({
      type: "poll_updated",
      place,
      messageId: initial.messageId,
      poll: {
        ...requiredPoll(initial),
        revision: 2,
        options: [
          { optionId: "a", text: "A", voters: [] },
          { optionId: "b", text: "B", voters: [other] },
        ],
      },
    });
    acknowledgement.resolve({
      ...initial,
      poll: {
        ...requiredPoll(initial),
        revision: 1,
        options: [
          { optionId: "a", text: "A", voters: [self] },
          { optionId: "b", text: "B", voters: [] },
        ],
      },
    });
    await task;

    const poll = useMessaging.getState().messagesByPlace[placeKey]?.[0]?.poll;
    expect(poll?.revision).toBe(2);
    expect(poll?.options[1]?.voters).toEqual([other]);
  });

  it("surfaces only the latest failed intent and retries its whole selection", async () => {
    const harness = new StubBackend();
    const failure = deferred<Message>();
    const retry = deferred<Message>();
    harness.pollVoteResults.push(failure.promise, retry.promise);
    installMessagingBackend(harness);
    useMessaging.getState().init();
    await harness.bootstrapped;
    const initial = pollMessage();
    useMessaging.setState({ messagesByPlace: { [placeKey]: [initial] } });

    const failedTask = useMessaging.getState().votePoll(initial, ["b"]);
    const assertion = expect(failedTask).rejects.toThrow("offline");
    failure.reject(new Error("offline"));
    await assertion;
    expect(
      useMessaging.getState().pollVoteByMessage[initial.messageId],
    ).toEqual(
      expect.objectContaining({
        optionIds: ["b"],
        pending: false,
        failed: true,
      }),
    );

    const retryTask = useMessaging.getState().votePoll(initial, ["b"]);
    retry.resolve({
      ...initial,
      poll: {
        ...requiredPoll(initial),
        revision: 1,
        options: [
          { optionId: "a", text: "A", voters: [] },
          { optionId: "b", text: "B", voters: [self] },
        ],
      },
    });
    await retryTask;
    expect(harness.votePoll).toHaveBeenNthCalledWith(2, place, "message-1", [
      "b",
    ]);
    expect(
      useMessaging.getState().pollVoteByMessage[initial.messageId],
    ).toBeUndefined();
  });

  it("fences old-session acknowledgements and queued intents", async () => {
    const oldHarness = new StubBackend();
    const acknowledgement = deferred<Message>();
    oldHarness.pollVoteResults.push(acknowledgement.promise, pollMessage(7));
    installMessagingBackend(oldHarness);
    useMessaging.getState().init();
    await oldHarness.bootstrapped;
    const oldMessage = pollMessage();
    useMessaging.setState({ messagesByPlace: { [placeKey]: [oldMessage] } });
    void useMessaging
      .getState()
      .votePoll(oldMessage, ["a"])
      .catch(() => undefined);
    void useMessaging
      .getState()
      .votePoll(oldMessage, ["a", "b"])
      .catch(() => undefined);
    await oldHarness.settle();

    bindMessagingSessionIdentity("poll-test-replacement-session");
    const newHarness = new StubBackend();
    installMessagingBackend(newHarness);
    useMessaging.getState().init();
    await newHarness.bootstrapped;
    const replacement = {
      ...pollMessage(5),
      content: "replacement session",
      poll: {
        ...requiredPoll(pollMessage(5)),
        options: [
          { optionId: "a", text: "A", voters: [] },
          { optionId: "b", text: "B", voters: [other] },
        ],
      },
    };
    useMessaging.setState({ messagesByPlace: { [placeKey]: [replacement] } });

    acknowledgement.resolve({
      ...oldMessage,
      poll: {
        ...requiredPoll(oldMessage),
        revision: 6,
        options: [
          { optionId: "a", text: "A", voters: [self] },
          { optionId: "b", text: "B", voters: [] },
        ],
      },
    });
    await oldHarness.settle();

    expect(oldHarness.votePoll).toHaveBeenCalledTimes(1);
    expect(useMessaging.getState().messagesByPlace[placeKey]?.[0]).toEqual(
      replacement,
    );
  });

  it("keeps a tombstone terminal against poll events and delayed vote ACKs", async () => {
    const harness = new StubBackend();
    const acknowledgement = deferred<Message>();
    harness.pollVoteResults.push(acknowledgement.promise, pollMessage(10));
    installMessagingBackend(harness);
    useMessaging.getState().init();
    await harness.bootstrapped;
    const initial = pollMessage();
    await holdLoaded(harness, [initial]);
    void useMessaging
      .getState()
      .votePoll(initial, ["a"])
      .catch(() => undefined);
    void useMessaging
      .getState()
      .votePoll(initial, ["a", "b"])
      .catch(() => undefined);
    await harness.settle();

    harness.emit({
      type: "message_deleted",
      message: {
        ...initial,
        content: "",
        deleted: true,
        revision: 2,
        poll: null,
      },
    });
    harness.emit({
      type: "poll_updated",
      place,
      messageId: initial.messageId,
      poll: { ...requiredPoll(initial), revision: 8 },
    });
    acknowledgement.resolve({
      ...initial,
      poll: { ...requiredPoll(initial), revision: 9 },
    });
    await harness.settle();

    expect(harness.votePoll).toHaveBeenCalledTimes(1);
    expect(
      useMessaging.getState().messagesByPlace[placeKey]?.[0],
    ).toMatchObject({ deleted: true, poll: null });
    expect(
      useMessaging.getState().pollVoteByMessage[initial.messageId],
    ).toBeUndefined();
  });

  it("retries a failed poll send with the same nonce and preserved declaration", async () => {
    const harness = new StubBackend();
    harness.sendMessage
      .mockRejectedValueOnce(new Error("offline"))
      .mockResolvedValueOnce({
        clientNonce: "ignored by assertion",
        messageId: "message-1",
        seq: 1,
        created: true,
      });
    installMessagingBackend(harness);
    useMessaging.getState().init();
    await harness.bootstrapped;
    useMessaging.getState().selectPlace(placeKey);
    await harness.settle();
    const declaration = {
      question: "which?",
      allowMulti: true,
      closesAt: null,
      options: ["A", "B"],
    };

    expect(useMessaging.getState().send("context", "urgent", declaration)).toBe(
      true,
    );
    await harness.settle();
    const pending = useMessaging.getState().pendingByPlace[placeKey]?.[0];
    expect(pending).toMatchObject({
      content: "context",
      urgency: "urgent",
      poll: declaration,
      failed: true,
    });
    if (!pending) throw new Error("pending poll missing");

    useMessaging.getState().retrySend(pending.clientNonce);
    await harness.settle();
    const first = harness.sendMessage.mock.calls[0]?.[0];
    const second = harness.sendMessage.mock.calls[1]?.[0];
    expect(second).toMatchObject({
      clientNonce: first?.clientNonce,
      content: "context",
      urgency: "urgent",
      poll: declaration,
    });
  });
});
