import { afterEach, describe, expect, it, vi } from "vitest";
import { useCall } from "./call/call-store";
import { MockMessagingServer } from "./mock-server";
import type { ConnectionState } from "./model";
import {
  bindMessagingSessionIdentity,
  getMessagingSessionIdentity,
  installMessagingBackend,
  refreshMessagingMemberProfiles,
  useMessaging,
} from "./store";

describe("messaging session boundary", () => {
  afterEach(() => {
    bindMessagingSessionIdentity(null);
    vi.unstubAllGlobals();
  });

  it("disposes private state before a different signed-in human can render", () => {
    bindMessagingSessionIdentity("human-a");
    useMessaging.setState({
      ready: true,
      self: { kind: "human", humanId: "human-a" },
      selfKey: "human:human-a",
      channels: [
        {
          channelId: "private-a",
          workspaceId: "workspace",
          name: "A",
          topic: "",
          visibility: "private",
          voice: false,
        },
      ],
      messagesByPlace: {
        "channel:private-a": [
          {
            messageId: "message-a",
            place: { kind: "channel", channelId: "private-a" },
            seq: 1,
            author: { kind: "human", humanId: "human-a" },
            content: "A only",
            mentions: [],
            urgency: "normal",
            reactions: [],
            attachments: [],
            replyTo: null,
            createdAt: 1,
            editedAt: null,
            deleted: false,
          },
        ],
      },
    });
    useCall.setState({
      activePlaceKey: "channel:private-a",
      phase: "connected",
      audioPlaybackBlocked: true,
    });

    bindMessagingSessionIdentity(null);
    bindMessagingSessionIdentity("human-b");

    expect(getMessagingSessionIdentity()).toBe("human-b");
    expect(useMessaging.getState()).toMatchObject({
      ready: false,
      self: null,
      selfKey: "",
      channels: [],
      messagesByPlace: {},
      activePlaceKey: null,
      connection: "disconnected",
    });
    expect(useCall.getState()).toMatchObject({
      activePlaceKey: null,
      phase: "idle",
      audioPlaybackBlocked: false,
    });
  });

  it("atomically refreshes Human and contextual agent presentation profiles", async () => {
    bindMessagingSessionIdentity("human-a");
    const server = new MockMessagingServer();
    const snapshot = await server.bootstrap();
    vi.spyOn(server, "bootstrap").mockResolvedValue({
      ...snapshot,
      self: { kind: "human", humanId: "human-a" },
      members: [
        {
          participant: { kind: "human", humanId: "human-a" },
          displayName: "After",
          tagline: "",
        },
        {
          participant: {
            kind: "personality_agent",
            personalityAgentId: "agent-a",
          },
          displayName: "Sumi（After）",
          tagline: "",
        },
      ],
    });
    installMessagingBackend(server);
    const messagesByPlace = {
      "channel:private-a": [
        {
          messageId: "message-a",
          place: { kind: "channel" as const, channelId: "private-a" },
          seq: 1,
          author: { kind: "human" as const, humanId: "human-a" },
          content: "A only",
          mentions: [],
          urgency: "normal" as const,
          reactions: [],
          attachments: [],
          replyTo: null,
          createdAt: 1,
          editedAt: null,
          deleted: false,
        },
      ],
    };
    useMessaging.setState({
      ready: true,
      self: { kind: "human", humanId: "human-a" },
      selfKey: "human:human-a",
      membersByKey: {
        "human:human-a": {
          participant: { kind: "human", humanId: "human-a" },
          displayName: "Before",
          tagline: "",
        },
        "personality_agent:agent-a": {
          participant: {
            kind: "personality_agent",
            personalityAgentId: "agent-a",
          },
          displayName: "Sumi（Before）",
          tagline: "",
        },
      },
      messagesByPlace,
    });

    await refreshMessagingMemberProfiles();

    expect(useMessaging.getState().membersByKey).toMatchObject({
      "human:human-a": { displayName: "After" },
      "personality_agent:agent-a": { displayName: "Sumi（After）" },
    });
    expect(useMessaging.getState().messagesByPlace).toBe(messagesByPlace);
  });

  it("WebSocketが再接続するたびvolatile call snapshotを読み直す", async () => {
    bindMessagingSessionIdentity("reconnect-human");
    const server = new ReconnectingMockServer();
    installMessagingBackend(server);
    const fetchMock = vi.fn(
      async () =>
        new Response(JSON.stringify({ calls: [] }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
    );
    vi.stubGlobal("fetch", fetchMock);

    useMessaging.getState().init();
    await vi.waitFor(() => expect(server.connectionListener).not.toBeNull());

    server.emitConnection("connected");
    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledOnce());

    server.emitConnection("reconnecting");
    server.emitConnection("connected");

    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
  });
});

class ReconnectingMockServer extends MockMessagingServer {
  connectionListener: ((state: ConnectionState) => void) | null = null;

  subscribeConnection(listener: (state: ConnectionState) => void): () => void {
    this.connectionListener = listener;
    listener("reconnecting");
    return () => {
      this.connectionListener = null;
    };
  }

  emitConnection(state: ConnectionState): void {
    this.connectionListener?.(state);
  }
}
