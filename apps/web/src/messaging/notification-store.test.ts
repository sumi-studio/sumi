// @vitest-environment jsdom

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type {
  ConnectionState,
  Message,
  MessagingBackend,
  NotificationSettingInput,
  Place,
  PlaceKey,
  ServerEvent,
} from "./model";
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
  private listener: ((event: ServerEvent) => void) | null = null;

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
      notificationSetting: {
        owner: SELF,
        defaults: { level: "mentions" },
        perPlace: [{ place: CHANNEL, level: "all" }],
        keywords: ["デプロイ"],
      },
      employedAgents: [],
    };
  }

  async fetchMessages(): Promise<Message[]> {
    return [];
  }
  async searchMessages(): ReturnType<MessagingBackend["searchMessages"]> {
    return [];
  }
  async createChannel(): ReturnType<MessagingBackend["createChannel"]> {
    throw new Error("not used");
  }
  async ensureDM(): ReturnType<MessagingBackend["ensureDM"]> {
    throw new Error("not used");
  }
  async createGroupDM(): ReturnType<MessagingBackend["createGroupDM"]> {
    throw new Error("not used");
  }
  async updateChannel(): ReturnType<MessagingBackend["updateChannel"]> {
    throw new Error("not used");
  }
  async duplicateChannel(): ReturnType<MessagingBackend["duplicateChannel"]> {
    throw new Error("not used");
  }
  async uploadAttachment(): ReturnType<MessagingBackend["uploadAttachment"]> {
    throw new Error("not used");
  }
  async updateAttachment(): ReturnType<MessagingBackend["updateAttachment"]> {
    throw new Error("not used");
  }
  async sendMessage() {
    return { messageId: "m", seq: 1 };
  }
  async editMessage(): Promise<void> {}
  async deleteMessage(): Promise<void> {}
  async markRead(): Promise<void> {}
  async setStatus(): Promise<void> {}
  async createReplyLater(): Promise<void> {}
  async resolveReplyLater(): Promise<void> {}
  async toggleReaction(): Promise<void> {}
  async setNotificationSetting(input: NotificationSettingInput): Promise<void> {
    this.settingWrites.push(input);
    if (this.rejectSettingWrites) throw new Error("rejected");
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
    useMessaging.getState().setPlaceNotificationLevel(CHANNEL_KEY, "mute");

    expect(notificationLevelFor(useMessaging.getState(), CHANNEL_KEY)).toBe(
      "mute",
    );
    await vi.waitFor(() => expect(backend.settingWrites).toHaveLength(1));
    expect(backend.settingWrites[0]).toEqual({
      defaults: { level: "mentions" },
      perPlace: [{ place: CHANNEL, level: "mute" }],
      keywords: ["デプロイ"],
    });
  });

  it("keeps the defaults and keywords editable through the same path", async () => {
    useMessaging.getState().setNotificationDefaultLevel("all");
    useMessaging.getState().setNotificationKeywords(["リリース", "Kuro"]);

    await vi.waitFor(() => expect(backend.settingWrites).toHaveLength(2));
    expect(backend.settingWrites[1]).toMatchObject({
      defaults: { level: "all" },
      keywords: ["リリース", "Kuro"],
    });
    expect(useMessaging.getState().notificationKeywords).toEqual([
      "リリース",
      "Kuro",
    ]);
  });

  it("puts a rejected change back, rather than showing a setting that is not in force", async () => {
    backend.rejectSettingWrites = true;
    useMessaging.getState().setPlaceNotificationLevel(CHANNEL_KEY, "mute");
    expect(notificationLevelFor(useMessaging.getState(), CHANNEL_KEY)).toBe(
      "mute",
    );
    await vi.waitFor(() =>
      expect(notificationLevelFor(useMessaging.getState(), CHANNEL_KEY)).toBe(
        "all",
      ),
    );
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
});
