// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";
import { MockMessagingServer } from "./mock-server";
import type {
  ConnectionState,
  Message,
  MessagingBackend,
  ParticipantRef,
  ParticipantStatus,
  Place,
  ReplyLaterMarker,
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
const CHANNEL: Place = { kind: "channel", channelId: "ch-general" };

function status(
  participant: ParticipantRef,
  kind: StatusKind,
  note = "",
  expiresAt: number | null = null,
): ParticipantStatus {
  return {
    participant,
    status: kind,
    note,
    expiresAt,
    baseStatus: null,
    baseNote: "",
  };
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
    attachments: [],
    poll: null,
    replyTo: null,
    createdAt: 0,
    editedAt: null,
    deleted: false,
  };
}

/** Controlled transport: no mutation echo unless the test emits one. */
class ControlledPresenceBackend extends MockMessagingServer {
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
  private eventListener: ((event: ServerEvent) => void) | null = null;
  private connectionListener: ((state: ConnectionState) => void) | null = null;

  override async bootstrap(): ReturnType<MessagingBackend["bootstrap"]> {
    const snapshot = await super.bootstrap();
    return {
      ...snapshot,
      self: SELF,
      statuses: this.presence.statuses,
      replyLaterMarkers: this.presence.replyLaterMarkers,
    };
  }

  override async fetchPresence(): ReturnType<
    MessagingBackend["fetchPresence"]
  > {
    this.presenceFetches += 1;
    if (this.nextPresenceFetch) {
      const pending = this.nextPresenceFetch;
      this.nextPresenceFetch = null;
      return pending;
    }
    return this.presence;
  }

  override async setStatus(): ReturnType<MessagingBackend["setStatus"]> {
    return this.nextStatus;
  }

  override async createReplyLater(): ReturnType<
    MessagingBackend["createReplyLater"]
  > {
    return this.nextMarker;
  }

  override async resolveReplyLater(): ReturnType<
    MessagingBackend["resolveReplyLater"]
  > {
    return { ...this.nextMarker, resolved: true };
  }

  override subscribe(listener: (event: ServerEvent) => void): () => void {
    this.eventListener = listener;
    return () => {
      this.eventListener = null;
    };
  }

  override subscribeConnection(
    listener: (state: ConnectionState) => void,
  ): () => void {
    this.connectionListener = listener;
    listener("reconnecting");
    return () => {
      this.connectionListener = null;
    };
  }

  override dispose(): void {
    super.dispose();
    this.eventListener = null;
    this.connectionListener = null;
  }

  pushEvent(event: ServerEvent): void {
    this.eventListener?.(event);
  }

  emitConnection(state: ConnectionState): void {
    this.connectionListener?.(state);
  }
}

async function startMessaging(
  backend: ControlledPresenceBackend,
): Promise<void> {
  bindMessagingSessionIdentity("human-1");
  installMessagingBackend(backend);
  useMessaging.getState().init();
  await vi.waitFor(() => expect(useMessaging.getState().ready).toBe(true));
  backend.emitConnection("connected");
}

describe("messaging presence convergence", () => {
  afterEach(() => {
    vi.useRealTimers();
    bindMessagingSessionIdentity(null);
  });

  it("replaces volatile presence with the authoritative reconnect snapshot", async () => {
    const backend = new ControlledPresenceBackend();
    backend.presence = {
      statuses: [status(OTHER, "busy", "取り込み中")],
      replyLaterMarkers: [marker("marker-open", OTHER)],
    };
    await startMessaging(backend);

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

  it("replays live updates and clears that race an older snapshot", async () => {
    const backend = new ControlledPresenceBackend();
    backend.presence = {
      statuses: [status(SELF, "available"), status(OTHER, "busy")],
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

    backend.pushEvent({
      type: "status_updated",
      status: status(SELF, "busy", "live update"),
    });
    backend.pushEvent({ type: "status_cleared", participant: OTHER });
    backend.pushEvent({
      type: "reply_later_created",
      marker: marker("marker-new", OTHER),
    });
    backend.pushEvent({
      type: "reply_later_resolved",
      markerId: "marker-open",
    });

    resolvePresence({
      statuses: [status(SELF, "available"), status(OTHER, "busy")],
      replyLaterMarkers: [marker("marker-open", OTHER)],
    });
    await vi.waitFor(() => {
      const state = useMessaging.getState();
      expect(state.statusByKey["human:human-1"]?.status).toBe("busy");
      expect(state.statusByKey["human:human-2"]).toBeUndefined();
      expect(state.replyLaterById["marker-new"]).toBeDefined();
      expect(state.replyLaterById["marker-open"]?.resolved).toBe(true);
    });
  });

  it("converges from REST acknowledgements without a socket echo", async () => {
    const backend = new ControlledPresenceBackend();
    backend.nextStatus = status(SELF, "busy", "取り込み中");
    backend.nextMarker = marker("marker-self", SELF, 1_800_000);
    await startMessaging(backend);

    useMessaging.getState().setStatus("busy", "取り込み中");
    await vi.waitFor(() =>
      expect(useMessaging.getState().statusByKey["human:human-1"]).toEqual(
        backend.nextStatus,
      ),
    );

    useMessaging.getState().createReplyLater(targetMessage());
    await vi.waitFor(() =>
      expect(useMessaging.getState().replyLaterById["marker-self"]).toEqual(
        backend.nextMarker,
      ),
    );
    useMessaging.getState().resolveReplyLater("marker-self");
    await vi.waitFor(() =>
      expect(
        useMessaging.getState().replyLaterById["marker-self"]?.resolved,
      ).toBe(true),
    );

    backend.pushEvent({
      type: "reply_later_created",
      marker: marker("marker-self", SELF),
    });
    const settled = useMessaging.getState().replyLaterById["marker-self"];
    expect(settled?.resolved).toBe(true);
    expect(settled?.remindAt).toBe(1_800_000);
  });

  it("drops an expired status and never adopts one already expired", async () => {
    const backend = new ControlledPresenceBackend();
    backend.presence = {
      statuses: [status(OTHER, "busy", "stale", Date.now() - 1)],
      replyLaterMarkers: [],
    };
    await startMessaging(backend);
    expect(
      useMessaging.getState().statusByKey["human:human-2"],
    ).toBeUndefined();

    vi.useFakeTimers();
    const expiresAt = Date.now() + 60_000;
    backend.pushEvent({
      type: "status_updated",
      status: status(OTHER, "busy", "1分だけ", expiresAt),
    });
    vi.advanceTimersByTime(59_000);
    expect(useMessaging.getState().statusByKey["human:human-2"]).toBeDefined();
    vi.advanceTimersByTime(1_000);
    expect(
      useMessaging.getState().statusByKey["human:human-2"],
    ).toBeUndefined();
  });
});
