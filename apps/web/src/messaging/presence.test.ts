import { afterEach, describe, expect, it, vi } from "vitest";
import type {
  ConnectionState,
  Message,
  MessagingBackend,
  ParticipantRef,
  ParticipantStatus,
  Place,
  ReplyLaterMarker,
  SendReceipt,
  ServerEvent,
  StatusKind,
} from "./model";
import {
  bindMessagingSessionIdentity,
  installMessagingBackend,
  useMessaging,
} from "./store";

const SELF: ParticipantRef = { kind: "human", humanId: "human-1" };
const OTHER: ParticipantRef = { kind: "human", humanId: "human-2" };
const CHANNEL: Place = { kind: "channel", channelId: "channel-1" };

function status(
  participant: ParticipantRef,
  kind: StatusKind,
  note = "",
  expiresAt: number | null = null,
): ParticipantStatus {
  return { participant, status: kind, note, expiresAt };
}

function marker(
  markerId: string,
  participant: ParticipantRef,
  remindAt: number | null = null,
  resolved = false,
): ReplyLaterMarker {
  return {
    markerId,
    participant,
    place: CHANNEL,
    messageId: "message-1",
    note: "後で返信します",
    remindAt,
    resolved,
  };
}

function targetMessage(): Message {
  return {
    messageId: "message-1",
    place: CHANNEL,
    seq: 1,
    author: OTHER,
    content: "hello",
    mentions: [],
    urgency: "normal",
    reactions: [],
    replyTo: null,
    createdAt: 0,
    editedAt: null,
    deleted: false,
  };
}

/**
 * status/reply-laterだけを動かせる最小のbackend。socketのechoは呼ばない限り
 * 起きないので、「RESTの成功だけで収束するか」をそのまま観察できる。
 */
class FakePresenceBackend implements MessagingBackend {
  readonly capabilities = {
    status: true,
    replyLater: true,
    reactions: true,
  } as const;
  presence: {
    statuses: ParticipantStatus[];
    replyLaterMarkers: ReplyLaterMarker[];
  } = { statuses: [], replyLaterMarkers: [] };
  nextStatus: ParticipantStatus = status(SELF, "available");
  nextMarker: ReplyLaterMarker = marker("marker-self", SELF, 1);
  nextPresenceFetch: Promise<{
    statuses: ParticipantStatus[];
    replyLaterMarkers: ReplyLaterMarker[];
  }> | null = null;
  presenceFetches = 0;
  private listener: ((event: ServerEvent) => void) | null = null;
  private connectionListener: ((state: ConnectionState) => void) | null = null;

  async bootstrap(): ReturnType<MessagingBackend["bootstrap"]> {
    return {
      self: SELF,
      workspaces: [{ workspaceId: "workspace-1", name: "Sumi" }],
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
      members: [
        { participant: SELF, displayName: "Yohaku", tagline: "" },
        { participant: OTHER, displayName: "Haru", tagline: "" },
      ],
      statuses: this.presence.statuses,
      readMarkers: [],
      unreadSummaries: [],
      replyLaterMarkers: this.presence.replyLaterMarkers,
      employedAgents: [],
    };
  }

  async fetchPresence(): ReturnType<MessagingBackend["fetchPresence"]> {
    this.presenceFetches += 1;
    if (this.nextPresenceFetch) {
      const pending = this.nextPresenceFetch;
      this.nextPresenceFetch = null;
      return pending;
    }
    return this.presence;
  }

  async fetchMessages(): Promise<Message[]> {
    return [];
  }
  async sendMessage(): Promise<SendReceipt> {
    throw new Error("unused");
  }
  async editMessage(): Promise<void> {}
  async deleteMessage(): Promise<void> {}
  async markRead(): Promise<void> {}
  async setStatus(): Promise<ParticipantStatus> {
    return this.nextStatus;
  }
  async createReplyLater(): Promise<ReplyLaterMarker> {
    return this.nextMarker;
  }
  async resolveReplyLater(): Promise<ReplyLaterMarker> {
    return { ...this.nextMarker, resolved: true };
  }
  async toggleReaction(): Promise<void> {}
  sendTyping(): void {}

  subscribe(listener: (event: ServerEvent) => void): () => void {
    this.listener = listener;
    return () => {
      this.listener = null;
    };
  }

  subscribeConnection(listener: (state: ConnectionState) => void): () => void {
    this.connectionListener = listener;
    listener("reconnecting");
    return () => {
      this.connectionListener = null;
    };
  }

  dispose(): void {
    this.listener = null;
    this.connectionListener = null;
  }

  emit(event: ServerEvent): void {
    this.listener?.(event);
  }

  emitConnection(state: ConnectionState): void {
    this.connectionListener?.(state);
  }
}

async function startMessaging(
  backend: FakePresenceBackend,
): Promise<FakePresenceBackend> {
  bindMessagingSessionIdentity("human-1");
  installMessagingBackend(backend);
  useMessaging.getState().init();
  await vi.waitFor(() => expect(useMessaging.getState().ready).toBe(true));
  // 最初のconnectedはbootstrapで受け取った現在値そのもの。
  backend.emitConnection("connected");
  return backend;
}

describe("messaging presence convergence", () => {
  afterEach(() => {
    vi.useRealTimers();
    bindMessagingSessionIdentity(null);
  });

  it("re-syncs statuses and open markers after a reconnect", async () => {
    const backend = new FakePresenceBackend();
    backend.presence = {
      statuses: [status(OTHER, "busy", "取り込み中")],
      replyLaterMarkers: [marker("marker-open", OTHER)],
    };
    await startMessaging(backend);
    expect(useMessaging.getState().statusByKey["human:human-2"]?.status).toBe(
      "busy",
    );
    expect(backend.presenceFetches).toBe(0);

    // 切断中に相手がavailableへ戻し、開いていたmarkerを解決する。
    // どちらもvolatile/非replayなので、cursorのcatch-upでは戻ってこない。
    backend.presence = {
      statuses: [status(OTHER, "available")],
      replyLaterMarkers: [],
    };
    backend.emitConnection("reconnecting");
    backend.emitConnection("connected");

    await vi.waitFor(() => {
      const state = useMessaging.getState();
      expect(state.statusByKey["human:human-2"]?.status).toBe("available");
      expect(state.replyLaterById["marker-open"]).toBeUndefined();
    });
    expect(backend.presenceFetches).toBe(1);
  });

  it("replays live presence events that arrive during the snapshot fetch", async () => {
    const backend = new FakePresenceBackend();
    backend.presence = {
      statuses: [status(OTHER, "available")],
      replyLaterMarkers: [marker("marker-open", OTHER)],
    };
    await startMessaging(backend);

    let resolvePresence!: (presence: {
      statuses: ParticipantStatus[];
      replyLaterMarkers: ReplyLaterMarker[];
    }) => void;
    backend.nextPresenceFetch = new Promise((resolve) => {
      resolvePresence = resolve;
    });
    backend.emitConnection("reconnecting");
    backend.emitConnection("connected");
    await vi.waitFor(() => expect(backend.presenceFetches).toBe(1));

    // These land after the server captured the snapshot but before its response
    // reaches us. They must survive the wholesale replacement below.
    backend.emit({
      type: "status_updated",
      status: status(OTHER, "busy", "live update"),
    });
    backend.emit({
      type: "reply_later_created",
      marker: marker("marker-new", OTHER),
    });
    backend.emit({ type: "reply_later_resolved", markerId: "marker-open" });

    resolvePresence({
      statuses: [status(OTHER, "available")],
      replyLaterMarkers: [marker("marker-open", OTHER)],
    });
    await vi.waitFor(() => {
      const state = useMessaging.getState();
      expect(state.statusByKey["human:human-2"]?.status).toBe("busy");
      expect(state.replyLaterById["marker-new"]).toBeDefined();
      expect(state.replyLaterById["marker-open"]?.resolved).toBe(true);
    });
  });

  it("converges from the REST acknowledgement without a socket echo", async () => {
    const backend = new FakePresenceBackend();
    backend.nextStatus = status(SELF, "busy", "取り込み中");
    backend.nextMarker = marker("marker-self", SELF, 1_800_000);
    await startMessaging(backend);

    useMessaging.getState().setStatus("busy", "取り込み中");
    await vi.waitFor(() =>
      expect(useMessaging.getState().statusByKey["human:human-1"]).toEqual(
        status(SELF, "busy", "取り込み中"),
      ),
    );

    useMessaging.getState().createReplyLater(targetMessage());
    await vi.waitFor(() =>
      expect(useMessaging.getState().replyLaterById["marker-self"]).toEqual(
        marker("marker-self", SELF, 1_800_000),
      ),
    );

    useMessaging.getState().resolveReplyLater("marker-self");
    await vi.waitFor(() =>
      expect(
        useMessaging.getState().replyLaterById["marker-self"]?.resolved,
      ).toBe(true),
    );

    // 後着のechoは同じmarkerを運ぶ。解決済みが未解決へ戻ってはいけない。
    backend.emit({
      type: "reply_later_created",
      marker: marker("marker-self", SELF),
    });
    const settled = useMessaging.getState().replyLaterById["marker-self"];
    expect(settled?.resolved).toBe(true);
    // 相手向けwireにはremind_atが載らない。一度知った自分の予定は消さない。
    expect(settled?.remindAt).toBe(1_800_000);
  });

  it("replays a status REST acknowledgement over an older presence snapshot", async () => {
    const backend = new FakePresenceBackend();
    backend.presence = {
      statuses: [status(SELF, "available")],
      replyLaterMarkers: [],
    };
    await startMessaging(backend);

    let resolvePresence!: (presence: {
      statuses: ParticipantStatus[];
      replyLaterMarkers: ReplyLaterMarker[];
    }) => void;
    backend.nextPresenceFetch = new Promise((resolve) => {
      resolvePresence = resolve;
    });
    backend.emitConnection("reconnecting");
    backend.emitConnection("connected");
    await vi.waitFor(() => expect(backend.presenceFetches).toBe(1));

    backend.nextStatus = status(SELF, "busy", "canonical ACK");
    useMessaging.getState().setStatus("busy", "canonical ACK");
    await vi.waitFor(() =>
      expect(useMessaging.getState().statusByKey["human:human-1"]).toEqual(
        backend.nextStatus,
      ),
    );

    resolvePresence({
      statuses: [status(SELF, "available")],
      replyLaterMarkers: [],
    });
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(useMessaging.getState().statusByKey["human:human-1"]).toEqual(
      backend.nextStatus,
    );
  });

  it("replays reply-later REST acknowledgements over an older snapshot", async () => {
    const backend = new FakePresenceBackend();
    await startMessaging(backend);

    let resolvePresence!: (presence: {
      statuses: ParticipantStatus[];
      replyLaterMarkers: ReplyLaterMarker[];
    }) => void;
    backend.nextPresenceFetch = new Promise((resolve) => {
      resolvePresence = resolve;
    });
    backend.emitConnection("reconnecting");
    backend.emitConnection("connected");
    await vi.waitFor(() => expect(backend.presenceFetches).toBe(1));

    backend.nextMarker = marker("marker-during-resync", SELF, 1_800_000);
    useMessaging.getState().createReplyLater(targetMessage());
    await vi.waitFor(() =>
      expect(
        useMessaging.getState().replyLaterById["marker-during-resync"],
      ).toEqual(backend.nextMarker),
    );
    useMessaging.getState().resolveReplyLater("marker-during-resync");
    await vi.waitFor(() =>
      expect(
        useMessaging.getState().replyLaterById["marker-during-resync"]
          ?.resolved,
      ).toBe(true),
    );

    resolvePresence({ statuses: [], replyLaterMarkers: [] });
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(
      useMessaging.getState().replyLaterById["marker-during-resync"],
    ).toMatchObject({ resolved: true, remindAt: 1_800_000 });
  });

  it("drops a status once expires_at is reached", async () => {
    const backend = new FakePresenceBackend();
    await startMessaging(backend);

    vi.useFakeTimers();
    const expiresAt = Date.now() + 60_000;
    backend.emit({
      type: "status_updated",
      status: status(OTHER, "busy", "1分だけ", expiresAt),
    });
    expect(useMessaging.getState().statusByKey["human:human-2"]?.status).toBe(
      "busy",
    );

    vi.advanceTimersByTime(59_000);
    expect(useMessaging.getState().statusByKey["human:human-2"]).toBeDefined();

    vi.advanceTimersByTime(1_000);
    expect(
      useMessaging.getState().statusByKey["human:human-2"],
    ).toBeUndefined();
  });

  it("never adopts a status that already expired", async () => {
    const backend = new FakePresenceBackend();
    backend.presence = {
      statuses: [status(OTHER, "busy", "期限切れ", Date.now() - 1_000)],
      replyLaterMarkers: [],
    };
    await startMessaging(backend);
    expect(
      useMessaging.getState().statusByKey["human:human-2"],
    ).toBeUndefined();
  });
});
