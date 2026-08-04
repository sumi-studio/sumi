import { secureRandomUUID } from "../lib/random-uuid";
import { hasDisplayMention } from "./mention";
import type {
  ChannelSummary,
  ConnectionState,
  DmSummary,
  MemberProfile,
  Message,
  MessagingBackend,
  NotificationSetting,
  NotificationSettingInput,
  NotifyReason,
  ParticipantRef,
  ParticipantStatus,
  Place,
  PlaceKey,
  ReactionSummary,
  ReadMarker,
  ReplyLaterMarker,
  SendMessageInput,
  SendReceipt,
  ServerEvent,
  StatusKind,
  UnreadSummary,
  WorkspaceSummary,
} from "./model";
import {
  parsePlaceKey,
  participantKey,
  placeKey,
  sameParticipant,
} from "./model";

/**
 * インメモリのモックbackend。実API（WS + REST）と同じMessagingBackend境界を
 * 実装し、UIの作り込みを契約合意と切り離して進めるためのもの。
 *
 * 模擬agentは特別な経路を持たない。人間と同じ道具 — typing、Status、
 * ReplyLater、通常のメッセージ送信 — を使って振る舞う。人格は複製しない
 * ので、同じagentへの呼びかけは直列に処理される（nextFreeAtで表現）。
 */

const SELF: ParticipantRef = { kind: "human", humanId: "h-yohaku" };
const HARU: ParticipantRef = { kind: "human", humanId: "h-haru" };
const SUMI: ParticipantRef = {
  kind: "personality_agent",
  personalityAgentId: "a-sumi",
};
const KURO: ParticipantRef = {
  kind: "personality_agent",
  personalityAgentId: "a-kuro",
};

const WORKSPACES: WorkspaceSummary[] = [
  { workspaceId: "ws-sumi", name: "Sumi Studio" },
];

const CHANNELS: ChannelSummary[] = [
  {
    channelId: "ch-general",
    workspaceId: "ws-sumi",
    name: "general",
    topic: "雑談と全体連絡",
    visibility: "public",
  },
  {
    channelId: "ch-dev",
    workspaceId: "ws-sumi",
    name: "dev",
    topic: "開発の相談と進捗",
    visibility: "public",
  },
  {
    channelId: "ch-design",
    workspaceId: "ws-sumi",
    name: "design",
    topic: "デザインレビュー",
    visibility: "public",
  },
];

const DMS: DmSummary[] = [
  { dmId: "dm-sumi", kind: "dm", participants: [SELF, SUMI] },
  { dmId: "dm-haru", kind: "dm", participants: [SELF, HARU] },
];

const MEMBERS: MemberProfile[] = [
  { participant: SELF, displayName: "yohaku", tagline: "Founder / デザイン" },
  { participant: HARU, displayName: "Haru", tagline: "エンジニア" },
  { participant: SUMI, displayName: "Sumi", tagline: "yohakuの秘書" },
  { participant: KURO, displayName: "Kuro", tagline: "開発" },
];

interface AgentPersona {
  ref: ParticipantRef;
  replies: string[];
  /** busy中の応答予約に添える一言。人間が押すのと同じボタン。 */
  replyLaterNote: string;
}

const PERSONAS: AgentPersona[] = [
  {
    ref: SUMI,
    replies: [
      "確認しました。こちらで進めておきますね。",
      "承知しました。関連する経緯もあとでまとめて共有します。",
      "なるほど。2点だけ確認させてください — 期限と優先度はどうしますか？",
      "読みました。次に着手する前に一度状況を整理して返します。",
    ],
    replyLaterNote: "別の対応中です。終わり次第返信します",
  },
  {
    ref: KURO,
    replies: [
      "見ました。再現手順を確認してから返します。",
      "了解です。該当のブランチを確認します。",
      "それ、昨日のデプロイと関係あるかもしれません。ログを見てきます。",
      "対応できます。今のタスクが終わり次第着手します。",
    ],
    replyLaterNote: "デプロイ対応中です。落ち着いたら返信します",
  },
];

interface SeedSpec {
  author: ParticipantRef;
  content: string;
  minutesAgo: number;
  mentions?: ParticipantRef[];
  urgency?: Message["urgency"];
  reactions?: ReactionSummary[];
}

function seedMessages(place: Place, specs: SeedSpec[]): Message[] {
  const now = Date.now();
  return specs.map((spec, index) => ({
    messageId: secureRandomUUID(),
    place,
    seq: index + 1,
    author: spec.author,
    content: spec.content,
    mentions: spec.mentions ?? [],
    urgency: spec.urgency ?? "normal",
    reactions: spec.reactions ?? [],
    replyTo: null,
    createdAt: now - spec.minutesAgo * 60_000,
    editedAt: null,
    deleted: false,
  }));
}

const DAY = 24 * 60;

function buildSeedHistory(): Map<string, Message[]> {
  const history = new Map<string, Message[]>();
  history.set(
    placeKey({ kind: "channel", channelId: "ch-general" }),
    seedMessages({ kind: "channel", channelId: "ch-general" }, [
      {
        author: HARU,
        content: "今日から新しいスプリントですね",
        minutesAgo: DAY + 190,
      },
      {
        author: HARU,
        content: "ボードの整理しておきました",
        minutesAgo: DAY + 188,
      },
      {
        author: SELF,
        content: "ありがとう！あとで見ます",
        minutesAgo: DAY + 120,
      },
      {
        author: SUMI,
        content:
          "スプリントの持ち越しタスクは3件でした。#dev に詳細を書いています。",
        minutesAgo: DAY + 118,
      },
      { author: HARU, content: "助かる〜", minutesAgo: DAY + 117 },
      { author: SELF, content: "昼どこか行く？", minutesAgo: 260 },
      { author: HARU, content: "そば！", minutesAgo: 258 },
      { author: HARU, content: "12:30に下で", minutesAgo: 257 },
      { author: SELF, content: "👍", minutesAgo: 255 },
      {
        author: SUMI,
        content:
          "14:00の定例、資料のリンクを貼っておきます: sumi://docs/weekly",
        minutesAgo: 40,
      },
      {
        author: HARU,
        content: "ありがとう、目を通しておきます",
        minutesAgo: 12,
      },
      { author: HARU, content: "会議室は3Fに変更だそうです", minutesAgo: 11 },
    ]),
  );
  history.set(
    placeKey({ kind: "channel", channelId: "ch-dev" }),
    seedMessages({ kind: "channel", channelId: "ch-dev" }, [
      {
        author: KURO,
        content:
          "スプリント持ち越し: #141 認証リダイレクト、#143 WS再接続、#145 画像アップロード",
        minutesAgo: DAY + 115,
      },
      { author: HARU, content: "#143 は自分が見ます", minutesAgo: DAY + 90 },
      {
        author: KURO,
        content: "了解です。#141 はこちらで進めます。",
        minutesAgo: DAY + 88,
      },
      {
        author: HARU,
        content: "staging に #143 の修正を出しました。再接続10回連続で確認OK",
        minutesAgo: 300,
      },
      {
        author: KURO,
        content:
          "確認しました。エッジケースとしてタブ休止からの復帰も試すと良さそうです。",
        minutesAgo: 295,
      },
      { author: HARU, content: "たしかに。追試します", minutesAgo: 293 },
      {
        author: KURO,
        content:
          "#141 の原因が分かりました。セッションcookieのaudience不一致です。",
        minutesAgo: 90,
      },
      {
        author: KURO,
        content: "修正PRを出しています。レビューお願いできますか？",
        minutesAgo: 88,
        mentions: [SELF],
        urgency: "urgent",
      },
      { author: HARU, content: "先に見始めてます", minutesAgo: 60 },
      {
        author: KURO,
        content:
          "助かります。合わせてデプロイ手順も1行変わるのでPR本文を見てください。",
        minutesAgo: 58,
      },
      { author: HARU, content: "本番反映は夕方でいい？", minutesAgo: 45 },
      {
        author: KURO,
        content: "はい、17時以降が安全です。",
        minutesAgo: 44,
        reactions: [{ emoji: "👍", participants: [HARU, SELF] }],
      },
    ]),
  );
  history.set(
    placeKey({ kind: "channel", channelId: "ch-design" }),
    seedMessages({ kind: "channel", channelId: "ch-design" }, [
      {
        author: SELF,
        content: "トークン整理の案、Figmaに置きました",
        minutesAgo: DAY + 30,
      },
      {
        author: HARU,
        content: "spacing が気持ちいいです",
        minutesAgo: DAY + 20,
      },
      {
        author: SELF,
        content: "次はメッセージングの画面に入ります",
        minutesAgo: 150,
      },
    ]),
  );
  history.set(
    placeKey({ kind: "dm", dmId: "dm-sumi" }),
    seedMessages({ kind: "dm", dmId: "dm-sumi" }, [
      { author: SELF, content: "今日の予定まとめておいて", minutesAgo: 200 },
      {
        author: SUMI,
        content:
          "14:00 定例、16:00 Haruさんと1on1、他は空いています。定例の資料は共有済みです。",
        minutesAgo: 198,
      },
    ]),
  );
  history.set(
    placeKey({ kind: "dm", dmId: "dm-haru" }),
    seedMessages({ kind: "dm", dmId: "dm-haru" }, [
      {
        author: HARU,
        content: "例の件、あとで相談させてください",
        minutesAgo: 400,
      },
      { author: SELF, content: "もちろん", minutesAgo: 398 },
    ]),
  );
  return history;
}

function initialReadMarkers(
  history: Map<string, Message[]>,
): Map<string, number> {
  const markers = new Map<string, number>();
  for (const [key, messages] of history) {
    const last = messages[messages.length - 1];
    if (!last) {
      markers.set(key, 0);
      continue;
    }
    if (key === "channel:ch-general") {
      markers.set(key, Math.max(0, last.seq - 3));
    } else if (key === "channel:ch-dev") {
      markers.set(key, Math.max(0, last.seq - 6));
    } else {
      markers.set(key, last.seq);
    }
  }
  return markers;
}

function resolveMentionsAtAdmission(content: string): ParticipantRef[] {
  return MEMBERS.filter((member) =>
    hasDisplayMention(content, member.displayName),
  ).map((member) => member.participant);
}

const SEND_LATENCY_MS = 160;
const TYPING_DELAY_MS = 650;
const TYPING_INTERVAL_MS = 3_000;
const REPLY_LATER_REMIND_MS = 9_000;

export class MockMessagingServer implements MessagingBackend {
  readonly capabilities = {
    status: true,
    replyLater: true,
    reactions: true,
    notifications: true,
  } as const;
  private readonly listeners = new Set<(event: ServerEvent) => void>();
  private readonly history = buildSeedHistory();
  private readonly readMarkers: Map<string, number>;
  private readonly statuses = new Map<string, ParticipantStatus>();
  private readonly replyLaterMarkers = new Map<string, ReplyLaterMarker>();
  /** モックもサーバー役なので、通知判定は送信時にこちら側で行う。 */
  private notificationSetting: NotificationSetting = {
    owner: SELF,
    defaults: { level: "all" },
    perPlace: [],
    keywords: [],
  };
  /** 同じagentへの呼びかけは直列に処理される（人格は複製しない）。 */
  private readonly agentNextFreeAt = new Map<string, number>();

  constructor() {
    this.readMarkers = initialReadMarkers(this.history);
    this.statuses.set(participantKey(KURO), {
      participant: KURO,
      status: "busy",
      note: "デプロイ対応中",
      expiresAt: null,
    });
  }

  dispose(): void {
    this.listeners.clear();
  }

  async bootstrap() {
    const readMarkers: ReadMarker[] = [];
    const unreadSummaries: UnreadSummary[] = [];
    for (const [key, lastReadSeq] of this.readMarkers) {
      const place = parsePlaceKey(key);
      if (!place) continue;
      readMarkers.push({ place, lastReadSeq });
      const messages = this.history.get(key) ?? [];
      unreadSummaries.push({
        place,
        latestSeq: messages.at(-1)?.seq ?? 0,
        unreadCount: messages.filter(
          (message) =>
            message.seq > lastReadSeq &&
            !message.deleted &&
            !sameParticipant(message.author, SELF),
        ).length,
        mentionCount: messages.filter(
          (message) =>
            message.seq > lastReadSeq &&
            !message.deleted &&
            message.urgency !== "fyi" &&
            message.mentions.some((mention) => sameParticipant(mention, SELF)),
        ).length,
      });
    }
    return {
      self: SELF,
      workspaces: WORKSPACES,
      channels: CHANNELS,
      dms: DMS,
      members: MEMBERS,
      statuses: [...this.statuses.values()],
      readMarkers,
      unreadSummaries,
      replyLaterMarkers: [...this.replyLaterMarkers.values()],
      notificationSetting: this.notificationSetting,
      employedAgents: [SUMI],
    };
  }

  async setNotificationSetting(input: NotificationSettingInput): Promise<void> {
    this.notificationSetting = { owner: SELF, ...input };
  }

  /**
   * 受信者（このモックでは常にSELF）を呼ぶかどうか。優先度は
   * dm > mention > keyword > all、muteはすべてを抑制する——実サーバーと同じ規則。
   */
  private notifyFor(message: Message): { reason: NotifyReason } | null {
    if (sameParticipant(message.author, SELF)) return null;
    const key = placeKey(message.place);
    const override = this.notificationSetting.perPlace.find(
      (entry) => placeKey(entry.place) === key,
    );
    const level = override?.level ?? this.notificationSetting.defaults.level;
    if (level === "mute") return null;
    if (message.place.kind !== "channel") return { reason: "dm" };
    if (message.mentions.some((ref) => sameParticipant(ref, SELF))) {
      return { reason: "mention" };
    }
    const content = message.content.toLowerCase();
    if (
      this.notificationSetting.keywords.some(
        (keyword) => keyword && content.includes(keyword.toLowerCase()),
      )
    ) {
      return { reason: "keyword" };
    }
    return level === "all" ? { reason: "all" } : null;
  }

  async fetchMessages(
    place: Place,
    options?: { beforeSeq?: number; limit?: number },
  ): Promise<Message[]> {
    const messages = this.history.get(placeKey(place)) ?? [];
    const beforeSeq = options?.beforeSeq ?? Number.POSITIVE_INFINITY;
    const limit = options?.limit ?? 50;
    const slice = messages.filter((message) => message.seq < beforeSeq);
    return slice.slice(Math.max(0, slice.length - limit));
  }

  sendMessage(input: SendMessageInput): Promise<SendReceipt> {
    // idempotency: 同じclientNonceの再送はcommit済みmessageのreceiptを返すだけ。
    const existing = (this.history.get(placeKey(input.place)) ?? []).find(
      (entry) => entry.clientNonce === input.clientNonce,
    );
    if (existing) {
      return Promise.resolve({
        messageId: existing.messageId,
        seq: existing.seq,
      });
    }
    return new Promise((resolve) => {
      window.setTimeout(() => {
        const message = this.appendMessage({
          place: input.place,
          author: SELF,
          content: input.content,
          // mentionはclientから信用せず、現在のmembershipからadmission時に解決する。
          mentions: resolveMentionsAtAdmission(input.content),
          urgency: input.urgency,
          replyTo: input.replyTo,
          clientNonce: input.clientNonce,
        });
        // 送信者自身にもmessage_createdをechoし、楽観的描画を確定へ置換する。
        this.emit({
          type: "message_created",
          message: { ...message },
          notify: this.notifyFor(message),
        });
        this.scheduleAgentResponses(message);
        resolve({ messageId: message.messageId, seq: message.seq });
      }, SEND_LATENCY_MS);
    });
  }

  async editMessage(
    place: Place,
    messageId: string,
    content: string,
  ): Promise<void> {
    const messages = this.history.get(placeKey(place)) ?? [];
    const message = messages.find((entry) => entry.messageId === messageId);
    if (!message || message.deleted || !sameParticipant(message.author, SELF)) {
      return;
    }
    message.content = content;
    message.mentions = resolveMentionsAtAdmission(content);
    message.editedAt = Date.now();
    this.emit({ type: "message_edited", message: { ...message } });
  }

  async deleteMessage(place: Place, messageId: string): Promise<void> {
    const messages = this.history.get(placeKey(place)) ?? [];
    const message = messages.find((entry) => entry.messageId === messageId);
    if (!message || message.deleted || !sameParticipant(message.author, SELF)) {
      return;
    }
    // tombstone化: contentは残さず、消えた事実とseqだけが残る。
    message.deleted = true;
    message.content = "";
    this.emit({
      type: "message_deleted",
      message: { ...message },
    });
  }

  async markRead(place: Place, lastReadSeq: number): Promise<void> {
    const key = placeKey(place);
    const current = this.readMarkers.get(key) ?? 0;
    if (lastReadSeq > current) this.readMarkers.set(key, lastReadSeq);
  }

  async setStatus(status: StatusKind, note: string): Promise<void> {
    const next: ParticipantStatus = {
      participant: SELF,
      status,
      note,
      expiresAt: null,
    };
    this.statuses.set(participantKey(SELF), next);
    this.emit({ type: "status_updated", status: next });
  }

  async createReplyLater(
    place: Place,
    messageId: string,
    remindAt: number,
  ): Promise<void> {
    const existing = [...this.replyLaterMarkers.values()].find(
      (marker) =>
        !marker.resolved &&
        marker.messageId === messageId &&
        sameParticipant(marker.participant, SELF),
    );
    if (existing) return;
    const marker: ReplyLaterMarker = {
      markerId: secureRandomUUID(),
      participant: SELF,
      place,
      messageId,
      note: "後で返信します",
      remindAt,
      resolved: false,
    };
    this.replyLaterMarkers.set(marker.markerId, marker);
    this.emit({ type: "reply_later_created", marker });
  }

  async resolveReplyLater(markerId: string): Promise<void> {
    const marker = this.replyLaterMarkers.get(markerId);
    if (!marker || marker.resolved) return;
    marker.resolved = true;
    this.emit({ type: "reply_later_resolved", markerId });
  }

  async toggleReaction(
    place: Place,
    messageId: string,
    emoji: string,
  ): Promise<void> {
    this.applyReaction(place, messageId, SELF, emoji);
  }

  /** リアクションのトグル。人間もagentも同じ道具として通る経路。 */
  private applyReaction(
    place: Place,
    messageId: string,
    participant: ParticipantRef,
    emoji: string,
  ): void {
    const messages = this.history.get(placeKey(place)) ?? [];
    const message = messages.find((entry) => entry.messageId === messageId);
    if (!message || message.deleted) return;
    const summary = message.reactions.find((entry) => entry.emoji === emoji);
    if (!summary) {
      message.reactions.push({ emoji, participants: [participant] });
    } else if (
      summary.participants.some((ref) => sameParticipant(ref, participant))
    ) {
      summary.participants = summary.participants.filter(
        (ref) => !sameParticipant(ref, participant),
      );
      if (summary.participants.length === 0) {
        message.reactions = message.reactions.filter(
          (entry) => entry.emoji !== emoji,
        );
      }
    } else {
      summary.participants.push(participant);
    }
    this.emit({ type: "reaction_updated", message: { ...message } });
  }

  sendTyping(_place: Place): void {
    // 自分のtypingは他参加者向け。モックでは表示相手がいないため何もしない。
  }

  subscribe(
    listener: (event: ServerEvent) => void,
    _options?: { sinceByPlace?: Record<PlaceKey, number> },
  ): () => void {
    // モックは常時ライブ接続のためcursor catch-upは不要。実装はWS再接続時に
    // sinceByPlaceの次seqからdurable eventをreplayする。
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  subscribeConnection(listener: (state: ConnectionState) => void): () => void {
    listener("connected");
    return () => {};
  }

  private emit(event: ServerEvent): void {
    for (const listener of this.listeners) listener(event);
  }

  private appendMessage(input: {
    place: Place;
    author: ParticipantRef;
    content: string;
    mentions: ParticipantRef[];
    urgency: Message["urgency"];
    replyTo: string | null;
    clientNonce?: string;
  }): Message {
    const key = placeKey(input.place);
    const messages = this.history.get(key) ?? [];
    const lastSeq = messages[messages.length - 1]?.seq ?? 0;
    const message: Message = {
      messageId: secureRandomUUID(),
      place: input.place,
      seq: lastSeq + 1,
      author: input.author,
      content: input.content,
      mentions: input.mentions,
      urgency: input.urgency,
      reactions: [],
      replyTo: input.replyTo,
      createdAt: Date.now(),
      editedAt: null,
      deleted: false,
      clientNonce: input.clientNonce,
    };
    messages.push(message);
    this.history.set(key, messages);
    return message;
  }

  /**
   * 呼びかけ（mention / DM）に応じた模擬agentの振る舞い。
   * busyなら人間と同じ「後で返信します」を押し、リマインド時刻に返信して
   * 予約を解決する。空いていればtypingを見せてから返信する。
   */
  private scheduleAgentResponses(trigger: Message): void {
    for (const persona of PERSONAS) {
      if (!this.isCalled(trigger, persona.ref)) continue;
      // FYI（返信不要）は文章で返さず、読んだ合図の👀だけ置く。
      if (trigger.urgency === "fyi") {
        window.setTimeout(
          () => {
            this.applyReaction(
              trigger.place,
              trigger.messageId,
              persona.ref,
              "👀",
            );
          },
          1_200 + Math.random() * 1_000,
        );
        continue;
      }
      const key = participantKey(persona.ref);
      const status = this.statuses.get(key);
      if (status?.status === "busy") {
        this.scheduleBusyResponse(persona, trigger);
        continue;
      }
      const now = Date.now();
      const readyAt = Math.max(now, this.agentNextFreeAt.get(key) ?? now);
      const replyAt = readyAt + TYPING_DELAY_MS + 1_400 + Math.random() * 1_600;
      this.agentNextFreeAt.set(key, replyAt + 500);
      this.scheduleTypingUntil(
        persona.ref,
        trigger.place,
        readyAt + TYPING_DELAY_MS,
        replyAt,
      );
      window.setTimeout(() => {
        this.appendAndEmitReply(persona, trigger);
      }, replyAt - now);
    }
  }

  private scheduleBusyResponse(persona: AgentPersona, trigger: Message): void {
    const marker: ReplyLaterMarker = {
      markerId: secureRandomUUID(),
      participant: persona.ref,
      place: trigger.place,
      messageId: trigger.messageId,
      note: persona.replyLaterNote,
      remindAt: Date.now() + REPLY_LATER_REMIND_MS,
      resolved: false,
    };
    window.setTimeout(() => {
      this.replyLaterMarkers.set(marker.markerId, marker);
      this.emit({ type: "reply_later_created", marker });
    }, 1_100);
    window.setTimeout(() => {
      const typingAt = Date.now();
      this.scheduleTypingUntil(
        persona.ref,
        trigger.place,
        typingAt,
        typingAt + 1_600,
      );
      window.setTimeout(() => {
        this.appendAndEmitReply(persona, trigger);
        marker.resolved = true;
        this.emit({ type: "reply_later_resolved", markerId: marker.markerId });
        const status: ParticipantStatus = {
          participant: persona.ref,
          status: "available",
          note: "",
          expiresAt: null,
        };
        this.statuses.set(participantKey(persona.ref), status);
        this.emit({ type: "status_updated", status });
      }, 1_700);
    }, REPLY_LATER_REMIND_MS);
  }

  private appendAndEmitReply(persona: AgentPersona, trigger: Message): void {
    const replyPool = persona.replies;
    const content = replyPool[Math.floor(Math.random() * replyPool.length)];
    const message = this.appendMessage({
      place: trigger.place,
      author: persona.ref,
      content,
      mentions: trigger.urgency === "fyi" ? [] : [trigger.author],
      urgency: "normal",
      replyTo: trigger.messageId,
    });
    this.emit({
      type: "message_created",
      message: { ...message },
      notify: this.notifyFor(message),
    });
  }

  private scheduleTypingUntil(
    participant: ParticipantRef,
    place: Place,
    startAt: number,
    endAt: number,
  ): void {
    for (let at = startAt; at < endAt; at += TYPING_INTERVAL_MS) {
      window.setTimeout(() => {
        this.emit({ type: "typing", place, participant });
      }, at - Date.now());
    }
  }

  private isCalled(trigger: Message, agent: ParticipantRef): boolean {
    if (trigger.mentions.some((ref) => sameParticipant(ref, agent))) {
      return true;
    }
    const place = trigger.place;
    if (place.kind !== "dm") return false;
    const dm = DMS.find((entry) => entry.dmId === place.dmId);
    return dm?.participants.some((ref) => sameParticipant(ref, agent)) ?? false;
  }
}
