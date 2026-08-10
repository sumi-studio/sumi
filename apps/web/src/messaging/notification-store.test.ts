// @vitest-environment jsdom

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type {
  ConnectionState,
  MemberProfile,
  Message,
  MessagingBackend,
  NotificationSetting,
  NotificationSettingInput,
  Place,
  PlaceKey,
  RoleAssignment,
  ServerEvent,
  WorkspaceRole,
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
    threads: true,
    polls: true,
  } as const;
  readonly settingWrites: NotificationSettingInput[] = [];
  readonly renewalWrites: string[][] = [];
  rejectSettingWrites = false;
  serverSetting: NotificationSetting = {
    owner: SELF,
    defaults: { level: "mentions" },
    perPlace: [{ place: CHANNEL, level: "all" }],
    keywords: ["デプロイ"],
  };
  normalizeKeywords: ((keywords: string[]) => string[]) | null = null;
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
      threads: [],
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
      roles: [],
      roleAssignments: [],
      permissions: {},
      notificationSetting: this.serverSetting,
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
  async fetchThreads(): ReturnType<MessagingBackend["fetchThreads"]> {
    return [];
  }
  async createThread(): ReturnType<MessagingBackend["createThread"]> {
    throw new Error("not used");
  }
  async votePoll(): ReturnType<MessagingBackend["votePoll"]> {
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
  async renewAttachments(
    attachmentIds: string[],
  ): ReturnType<MessagingBackend["renewAttachments"]> {
    this.renewalWrites.push([...attachmentIds]);
  }
  async sendMessage() {
    return {
      clientNonce: "notification-test",
      messageId: "m",
      seq: 1,
      created: true,
    };
  }
  async editMessage(): Promise<void> {}
  async deleteMessage(): Promise<void> {}
  async markRead(): Promise<void> {}
  async fetchPresence(): ReturnType<MessagingBackend["fetchPresence"]> {
    return { statuses: [], replyLaterMarkers: [] };
  }
  async setStatus(): ReturnType<MessagingBackend["setStatus"]> {
    throw new Error("not used");
  }
  async updateProfile(): Promise<MemberProfile> {
    return { participant: SELF, displayName: "yohaku", tagline: "" };
  }
  async fetchRoles() {
    return { roles: [], roleAssignments: [], permissions: {} };
  }
  async createRole(): Promise<WorkspaceRole> {
    throw new Error("not used");
  }
  async updateRole(): Promise<WorkspaceRole> {
    throw new Error("not used");
  }
  async deleteRole(): Promise<void> {}
  async setMemberRoles(): Promise<RoleAssignment> {
    return { participant: SELF, roleIds: [] };
  }
  async createReplyLater(): ReturnType<MessagingBackend["createReplyLater"]> {
    throw new Error("not used");
  }
  async resolveReplyLater(): ReturnType<MessagingBackend["resolveReplyLater"]> {
    throw new Error("not used");
  }
  async setReaction(): ReturnType<MessagingBackend["setReaction"]> {
    throw new Error("not used");
  }
  async setNotificationSetting(
    input: NotificationSettingInput,
  ): Promise<NotificationSetting> {
    this.settingWrites.push(input);
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
  });

  it("serializes full replacements so an older snapshot cannot win", async () => {
    backend.holdWrites = true;
    useMessaging.getState().setNotificationDefaultLevel("all");
    await vi.waitFor(() => expect(backend.inFlight).toBe(1));

    useMessaging.getState().setNotificationKeywords(["リリース"]);
    expect(backend.settingWrites).toHaveLength(1);

    backend.holdWrites = false;
    backend.releaseWrites();
    await vi.waitFor(() => expect(backend.settingWrites).toHaveLength(2));

    expect(backend.serverSetting).toMatchObject({
      defaults: { level: "all" },
      keywords: ["リリース"],
    });
    expect(useMessaging.getState().notificationKeywords).toEqual(["リリース"]);
  });

  it("does not let an obsolete failed write roll back a later success", async () => {
    backend.holdWrites = true;
    backend.rejectSettingWrites = true;
    useMessaging.getState().setNotificationDefaultLevel("all");
    await vi.waitFor(() => expect(backend.inFlight).toBe(1));

    backend.rejectSettingWrites = false;
    useMessaging.getState().setNotificationKeywords(["リリース"]);
    backend.holdWrites = false;
    backend.releaseWrites();

    await vi.waitFor(() =>
      expect(backend.serverSetting.keywords).toEqual(["リリース"]),
    );
    expect(useMessaging.getState()).toMatchObject({
      notificationDefaultLevel: "all",
      notificationKeywords: ["リリース"],
    });
  });

  it("adopts the server-normalized confirmed snapshot", async () => {
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

  it("does not let queued writes cross a messaging session boundary", async () => {
    const previousBackend = backend;
    previousBackend.holdWrites = true;
    useMessaging.getState().setNotificationDefaultLevel("all");
    await vi.waitFor(() => expect(previousBackend.inFlight).toBe(1));
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

describe("attachment draft leases in the store", () => {
  it("forwards renewal ids to the installed backend", async () => {
    await useMessaging
      .getState()
      .renewAttachments(["attachment-1", "attachment-2"]);

    expect(backend.renewalWrites).toEqual([["attachment-1", "attachment-2"]]);
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
