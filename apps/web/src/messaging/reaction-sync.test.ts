import { describe, expect, it, vi } from "vitest";
import type {
  ConnectionState,
  Message,
  MessagingBackend,
  Place,
  ServerEvent,
} from "./model";
import { installMessagingBackend, useMessaging } from "./store";

const place: Place = { kind: "channel", channelId: "channel-1" };
const placeKey = "channel:channel-1" as const;

/**
 * reaction eventはmessage全体を運ばない、という契約のstore側の裏。編集を
 * 巻き戻さないこと、切断中に落ちたreactionが再接続で戻ることを見る。
 */
describe("reaction convergence in the messaging store", () => {
  it("patches only reactions and re-reads the loaded window on caught_up", async () => {
    const harness = new StubBackend();
    installMessagingBackend(harness);
    useMessaging.getState().init();
    await harness.bootstrapped;
    useMessaging.setState({
      messagesByPlace: { [placeKey]: [message(1, "編集前"), message(2, "隣")] },
    });

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
  });
});

const self = { kind: "human", humanId: "human-1" } as const;

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
    replyTo: null,
    createdAt: 1,
    editedAt,
    deleted: false,
  };
}

class StubBackend implements MessagingBackend {
  readonly capabilities = {
    status: false,
    replyLater: false,
    reactions: true,
  } as const;
  history: Message[] = [];
  readonly fetches: { beforeSeq?: number; limit?: number }[] = [];
  private listener: ((event: ServerEvent) => void) | null = null;
  private resolveBootstrapped!: () => void;
  readonly bootstrapped = new Promise<void>((resolve) => {
    this.resolveBootstrapped = resolve;
  });

  emit(event: ServerEvent): void {
    this.listener?.(event);
  }

  /** resyncのawait chain（fetch → set）を流し切る。 */
  async settle(): Promise<void> {
    await new Promise((resolve) => setTimeout(resolve, 0));
  }

  async bootstrap(): ReturnType<MessagingBackend["bootstrap"]> {
    queueMicrotask(() => this.resolveBootstrapped());
    return {
      self,
      workspaces: [],
      channels: [
        {
          channelId: "channel-1",
          workspaceId: "workspace-1",
          name: "general",
          topic: "",
          visibility: "public",
        },
      ],
      dms: [],
      members: [],
      statuses: [],
      readMarkers: [],
      unreadSummaries: [],
      replyLaterMarkers: [],
      employedAgents: [],
    };
  }

  async fetchMessages(
    _place: Place,
    options: { beforeSeq?: number; limit?: number } = {},
  ): Promise<Message[]> {
    this.fetches.push(options);
    return this.history;
  }

  sendMessage = vi.fn();
  editMessage = vi.fn(async () => undefined);
  deleteMessage = vi.fn(async () => undefined);
  markRead = vi.fn(async () => undefined);
  setStatus = vi.fn(async () => undefined);
  createReplyLater = vi.fn(async () => undefined);
  resolveReplyLater = vi.fn(async () => undefined);
  toggleReaction = vi.fn(async () => undefined);
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
