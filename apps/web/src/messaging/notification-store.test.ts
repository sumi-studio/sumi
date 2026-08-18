// @vitest-environment jsdom

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type {
  ConnectionState,
  Message,
  MessagingBackend,
  NotificationSetting,
  NotificationSettingInput,
  Place,
  PlaceKey,
  ServerEvent,
} from "./model";
import { resetNotificationAudio } from "./notifications";
import {
  bindMessagingSessionIdentity,
  installMessagingBackend,
  notificationLevelFor,
  useMessaging,
} from "./store";

const SELF = { kind: "human", humanId: "human-1" } as const;
const OTHER = { kind: "human", humanId: "human-2" } as const;
const CHANNEL: Place = { kind: "channel", channelId: "channel-1" };
const CHANNEL_KEY: PlaceKey = "channel:channel-1";

/** MessagingBackendの最小実装。設定の送信と、event配送だけを見る。 */
class StubBackend implements MessagingBackend {
  readonly capabilities = {
    status: true,
    replyLater: true,
    reactions: true,
    notifications: true,
  } as const;
  readonly settingWrites: NotificationSettingInput[] = [];
  rejectSettingWrites = false;
  /**
   * サーバー側の現在値。PUTが「解決した順」に置き換わるので、送信が入れ替わると
   * ここに古いsnapshotが残る——それが今回の再現したい壊れ方。
   */
  serverSetting: NotificationSetting = {
    owner: SELF,
    defaults: { level: "mentions" },
    perPlace: [{ place: CHANNEL, level: "all" }],
    keywords: ["デプロイ"],
  };
  /** 本物のサーバーはkeywordを正規化して返す。手元がそれを取り込むかを見る。 */
  normalizeKeywords: ((keywords: string[]) => string[]) | null = null;
  /** trueの間、届いたPUTを解決させずに溜める。遅いbackendの代わり。 */
  holdWrites = false;
  private heldWrites: (() => void)[] = [];
  private listener: ((event: ServerEvent) => void) | null = null;

  get inFlight(): number {
    return this.heldWrites.length;
  }

  releaseWrites(): void {
    const held = this.heldWrites.splice(0, this.heldWrites.length);
    for (const resolve of held) resolve();
  }

  async bootstrap(): ReturnType<MessagingBackend["bootstrap"]> {
    return {
      self: SELF,
      workspaces: [{ workspaceId: "ws", name: "Sumi" }],
      channels: [
        {
          channelId: "channel-1",
          workspaceId: "ws",
          name: "dev",
          topic: "",
          visibility: "public",
          voice: false,
        },
      ],
      dms: [],
      members: [
        { participant: SELF, displayName: "yohaku", tagline: "" },
        { participant: OTHER, displayName: "Kuro", tagline: "" },
      ],
      statuses: [],
      readMarkers: [{ place: CHANNEL, lastReadSeq: 0 }],
      unreadSummaries: [
        {
          place: CHANNEL,
          latestSeq: 0,
          unreadCount: 0,
          mentionCount: 0,
        },
      ],
      replyLaterMarkers: [],
      notificationSetting: this.serverSetting,
      employedAgents: [],
    };
  }

  async fetchMessages(): Promise<Message[]> {
    return [];
  }
  async searchMessages(): Promise<import("./model").MessageSearchResult[]> {
    return [];
  }
  async fetchPresence(): ReturnType<MessagingBackend["fetchPresence"]> {
    return { statuses: [], replyLaterMarkers: [] };
  }
  async createChannel(): ReturnType<MessagingBackend["createChannel"]> {
    throw new Error("unused");
  }
  async ensureDM(): ReturnType<MessagingBackend["ensureDM"]> {
    throw new Error("unused");
  }
  async createGroupDM(): ReturnType<MessagingBackend["createGroupDM"]> {
    throw new Error("unused");
  }
  async updateChannelTopic(): ReturnType<
    MessagingBackend["updateChannelTopic"]
  > {
    throw new Error("unused");
  }
  async uploadAttachment(): Promise<never> {
    throw new Error("uploadAttachment is not part of this test");
  }

  async updateDraftAttachment(): Promise<never> {
    throw new Error("updateDraftAttachment is not part of this test");
  }
  attachmentURL(attachmentId: string): string {
    return `/test/attachments/${attachmentId}`;
  }
  async sendMessage() {
    return {
      clientNonce: "notification-test",
      messageId: "m",
      seq: 1,
      created: true,
    };
  }
  async editMessage(): ReturnType<MessagingBackend["editMessage"]> {
    throw new Error("unused");
  }
  async deleteMessage(): ReturnType<MessagingBackend["deleteMessage"]> {
    throw new Error("unused");
  }
  async markRead(): Promise<void> {}
  async setStatus(): ReturnType<MessagingBackend["setStatus"]> {
    throw new Error("unused");
  }
  async createReplyLater(): ReturnType<MessagingBackend["createReplyLater"]> {
    throw new Error("unused");
  }
  async resolveReplyLater(): ReturnType<MessagingBackend["resolveReplyLater"]> {
    throw new Error("unused");
  }
  async toggleReaction(): ReturnType<MessagingBackend["toggleReaction"]> {
    throw new Error("unused");
  }
  async setNotificationSetting(
    input: NotificationSettingInput,
  ): Promise<NotificationSetting> {
    this.settingWrites.push(input);
    // 成否はPUTが届いた時点で決まる。溜めている間の切り替えに引きずられない。
    const rejects = this.rejectSettingWrites;
    if (this.holdWrites) {
      await new Promise<void>((resolve) => this.heldWrites.push(resolve));
    }
    if (rejects) throw new Error("rejected");
    this.serverSetting = {
      owner: SELF,
      defaults: input.defaults,
      perPlace: input.perPlace,
      keywords: this.normalizeKeywords
        ? this.normalizeKeywords(input.keywords)
        : input.keywords,
    };
    return this.serverSetting;
  }
  sendTyping(): void {}
  subscribe(listener: (event: ServerEvent) => void): () => void {
    this.listener = listener;
    return () => {
      this.listener = null;
    };
  }
  subscribeConnection(listener: (state: ConnectionState) => void): () => void {
    listener("connected");
    return () => undefined;
  }
  dispose(): void {}

  emit(event: ServerEvent): void {
    this.listener?.(event);
  }
}

function incoming(overrides: Partial<Message> = {}): Message {
  return {
    messageId: "message-1",
    place: CHANNEL,
    seq: 1,
    author: OTHER,
    content: "デプロイの件です",
    mentions: [],
    urgency: "normal",
    reactions: [],
    attachments: [],
    replyTo: null,
    createdAt: 1,
    editedAt: null,
    deleted: false,
    ...overrides,
  };
}

class FakeNotification {
  static permission = "granted";
  static readonly constructed: {
    title: string;
    options: NotificationOptions;
  }[] = [];
  onclick: (() => void) | null = null;
  close = vi.fn();
  constructor(title: string, options: NotificationOptions) {
    FakeNotification.constructed.push({ title, options });
  }
}

let backend: StubBackend;

async function bootedStore(): Promise<StubBackend> {
  bindMessagingSessionIdentity("human-1");
  const stub = new StubBackend();
  installMessagingBackend(stub);
  useMessaging.getState().init();
  await vi.waitFor(() => expect(useMessaging.getState().ready).toBe(true));
  return stub;
}

beforeEach(async () => {
  FakeNotification.constructed.length = 0;
  vi.stubGlobal("Notification", FakeNotification);
  vi.stubGlobal("focus", vi.fn());
  backend = await bootedStore();
});

afterEach(() => {
  bindMessagingSessionIdentity(null);
  resetNotificationAudio();
  localStorage.clear();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("notification settings in the store", () => {
  it("adopts the setting bootstrap already carries", () => {
    const state = useMessaging.getState();
    expect(state.notificationDefaultLevel).toBe("mentions");
    expect(state.notificationKeywords).toEqual(["デプロイ"]);
    expect(notificationLevelFor(state, CHANNEL_KEY)).toBe("all");
    expect(notificationLevelFor(state, "channel:unknown")).toBe("mentions");
  });

  it("writes the whole setting when one place changes", async () => {
    const result = useMessaging
      .getState()
      .setPlaceNotificationLevel(CHANNEL_KEY, "mute");

    expect(notificationLevelFor(useMessaging.getState(), CHANNEL_KEY)).toBe(
      "mute",
    );
    await vi.waitFor(() => expect(backend.settingWrites).toHaveLength(1));
    expect(backend.settingWrites[0]).toEqual({
      defaults: { level: "mentions" },
      perPlace: [{ place: CHANNEL, level: "mute" }],
      keywords: ["デプロイ"],
    });
    await expect(result).resolves.toBe("confirmed");
  });

  it("keeps the defaults and keywords editable through the same path", async () => {
    const first = useMessaging.getState().setNotificationDefaultLevel("all");
    const latest = useMessaging
      .getState()
      .setNotificationKeywords(["リリース", "Kuro"]);

    // 全置換なので、続けて変えた分は最新のsnapshot1本にまとめて送れば足りる。
    await vi.waitFor(() =>
      expect(backend.serverSetting.keywords).toEqual(["リリース", "Kuro"]),
    );
    expect(backend.settingWrites).toHaveLength(1);
    expect(backend.settingWrites[0]).toMatchObject({
      defaults: { level: "all" },
      keywords: ["リリース", "Kuro"],
    });
    expect(backend.serverSetting.defaults).toEqual({ level: "all" });
    expect(useMessaging.getState().notificationKeywords).toEqual([
      "リリース",
      "Kuro",
    ]);
    await expect(first).resolves.toBe("superseded");
    await expect(latest).resolves.toBe("confirmed");
  });

  it("並べて送るので、遅れて届いた古いsnapshotがサーバーを巻き戻さない", async () => {
    backend.holdWrites = true;
    useMessaging.getState().setNotificationDefaultLevel("all");
    await vi.waitFor(() => expect(backend.inFlight).toBe(1));

    // 1本目が飛んでいる最中の変更。列の後ろに並ぶので、まだ送られない。
    useMessaging.getState().setNotificationKeywords(["リリース"]);
    expect(backend.settingWrites).toHaveLength(1);

    backend.holdWrites = false;
    backend.releaseWrites();

    await vi.waitFor(() => expect(backend.settingWrites).toHaveLength(2));
    // サーバーに最後に残るのは新しい方。手元と食い違わない。
    expect(backend.serverSetting.keywords).toEqual(["リリース"]);
    expect(backend.serverSetting.defaults).toEqual({ level: "all" });
    expect(useMessaging.getState().notificationKeywords).toEqual(["リリース"]);
    expect(useMessaging.getState().notificationDefaultLevel).toBe("all");
  });

  it("追い越された書き込みの失敗では、後から成功した設定を巻き戻さない", async () => {
    backend.holdWrites = true;
    backend.rejectSettingWrites = true;
    useMessaging.getState().setNotificationDefaultLevel("all");
    await vi.waitFor(() => expect(backend.inFlight).toBe(1));

    // 後続は成功する。失敗するのは追い越された古い1本だけ。
    backend.rejectSettingWrites = false;
    useMessaging.getState().setNotificationKeywords(["リリース"]);
    backend.holdWrites = false;
    backend.releaseWrites();

    await vi.waitFor(() =>
      expect(backend.serverSetting.keywords).toEqual(["リリース"]),
    );
    expect(useMessaging.getState().notificationKeywords).toEqual(["リリース"]);
    expect(useMessaging.getState().notificationDefaultLevel).toBe("all");
  });

  it("はサーバーが正規化して返した確定値を手元の正本にする", async () => {
    backend.normalizeKeywords = (keywords) =>
      keywords.map((keyword) => keyword.trim()).filter(Boolean);

    useMessaging.getState().setNotificationKeywords(["  リリース  ", "   "]);

    await vi.waitFor(() =>
      expect(useMessaging.getState().notificationKeywords).toEqual([
        "リリース",
      ]),
    );
  });

  it("puts a rejected change back, rather than showing a setting that is not in force", async () => {
    backend.rejectSettingWrites = true;
    const result = useMessaging
      .getState()
      .setPlaceNotificationLevel(CHANNEL_KEY, "mute");
    expect(notificationLevelFor(useMessaging.getState(), CHANNEL_KEY)).toBe(
      "mute",
    );
    await vi.waitFor(() =>
      expect(notificationLevelFor(useMessaging.getState(), CHANNEL_KEY)).toBe(
        "all",
      ),
    );
    await expect(result).resolves.toBe("failed");
  });

  it("does not let queued writes cross a messaging session boundary", async () => {
    const previousBackend = backend;
    previousBackend.holdWrites = true;
    useMessaging.getState().setNotificationDefaultLevel("all");
    await vi.waitFor(() => expect(previousBackend.inFlight).toBe(1));
    // This second old-session snapshot is queued behind the held request.
    useMessaging.getState().setNotificationKeywords(["前アカウント"]);

    bindMessagingSessionIdentity("human-2");
    const nextBackend = new StubBackend();
    nextBackend.serverSetting = {
      owner: SELF,
      defaults: { level: "mentions" },
      perPlace: [{ place: CHANNEL, level: "all" }],
      keywords: ["新アカウント"],
    };
    installMessagingBackend(nextBackend);
    useMessaging.getState().init();
    await vi.waitFor(() => expect(useMessaging.getState().ready).toBe(true));

    // Bring the new session to generation 2 as well. A generation counter that
    // was merely reset would let the old queued generation-2 task pass.
    useMessaging.getState().setNotificationDefaultLevel("all");
    useMessaging.getState().setNotificationKeywords(["新しい確定値"]);
    await vi.waitFor(() =>
      expect(nextBackend.serverSetting.keywords).toEqual(["新しい確定値"]),
    );
    expect(nextBackend.settingWrites).toHaveLength(1);

    previousBackend.holdWrites = false;
    previousBackend.releaseWrites();
    await vi.waitFor(() =>
      expect(previousBackend.serverSetting.defaults.level).toBe("all"),
    );
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(nextBackend.settingWrites).toHaveLength(1);
    expect(useMessaging.getState().notificationKeywords).toEqual([
      "新しい確定値",
    ]);

    // A later failure must roll back to this session's confirmed value, never
    // to the response returned by the request that crossed the boundary.
    nextBackend.rejectSettingWrites = true;
    useMessaging.getState().setNotificationDefaultLevel("mute");
    await vi.waitFor(() =>
      expect(useMessaging.getState().notificationDefaultLevel).toBe("all"),
    );
    expect(useMessaging.getState().notificationKeywords).toEqual([
      "新しい確定値",
    ]);
  });

  it("keeps the sound preference on the device, not in the shared setting", () => {
    useMessaging.getState().setNotificationSoundEnabled(false);
    expect(useMessaging.getState().notificationSoundEnabled).toBe(false);
    expect(backend.settingWrites).toHaveLength(0);
    expect(localStorage.getItem("sumi.messaging.notification-sound")).toBe(
      "off",
    );
  });
});

describe("presenting an incoming message", () => {
  it("calls the person when the server said so and the tab is elsewhere", () => {
    vi.spyOn(document, "hasFocus").mockReturnValue(false);

    backend.emit({
      type: "message_created",
      message: incoming(),
      notify: { reason: "keyword" },
    });

    expect(FakeNotification.constructed).toHaveLength(1);
    expect(FakeNotification.constructed[0]?.title).toBe("#dev — Kuro");
    expect(FakeNotification.constructed[0]?.options.body).toBe(
      "デプロイの件です",
    );
  });

  it("stays quiet when the server did not call this person", () => {
    vi.spyOn(document, "hasFocus").mockReturnValue(false);

    backend.emit({
      type: "message_created",
      message: incoming(),
      notify: null,
    });

    expect(FakeNotification.constructed).toHaveLength(0);
  });

  it("never notifies the sender about their own message", () => {
    vi.spyOn(document, "hasFocus").mockReturnValue(false);

    backend.emit({
      type: "message_created",
      message: incoming({ author: SELF }),
      // 万一サーバーが付けてきても、自分の発言では呼ばない。
      notify: { reason: "all" },
    });

    expect(FakeNotification.constructed).toHaveLength(0);
  });

  it("does not stack a desktop notification on the visible conversation", () => {
    vi.spyOn(document, "hasFocus").mockReturnValue(true);
    useMessaging.setState({ activePlaceKey: CHANNEL_KEY });

    backend.emit({
      type: "message_created",
      message: incoming(),
      notify: { reason: "mention" },
    });

    expect(FakeNotification.constructed).toHaveLength(0);
  });

  it("presents a called message after the current place is explicitly cleared", () => {
    const startTone = vi.fn();
    const gainNode = () => ({
      gain: {
        value: 0,
        setValueAtTime: vi.fn(),
        exponentialRampToValueAtTime: vi.fn(),
      },
      connect: vi.fn(),
    });
    class FakeAudioContext {
      currentTime = 0;
      destination = {};
      resume = vi.fn();
      createGain = vi.fn(gainNode);
      createOscillator = vi.fn(() => ({
        type: "",
        frequency: { setValueAtTime: vi.fn() },
        connect: vi.fn(),
        start: startTone,
        stop: vi.fn(),
      }));
    }
    vi.stubGlobal("AudioContext", FakeAudioContext);
    resetNotificationAudio();
    vi.spyOn(document, "hasFocus").mockReturnValue(true);
    useMessaging.setState({
      activePlaceKey: CHANNEL_KEY,
      editingMessageId: "editing-in-channel",
      replyTargetId: "replying-in-channel",
    });

    useMessaging.getState().clearPlaceSelection();
    backend.emit({
      type: "message_created",
      message: incoming(),
      notify: { reason: "mention" },
    });

    expect(useMessaging.getState()).toMatchObject({
      activePlaceKey: null,
      editingMessageId: null,
      replyTargetId: null,
    });
    expect(startTone).toHaveBeenCalledTimes(2);
    expect(FakeNotification.constructed).toHaveLength(0);
  });
});
