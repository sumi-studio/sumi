// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiMessagingBackend } from "./api-backend";
import type { ServerEvent } from "./model";

const channel = { kind: "channel", channelId: "channel-1" } as const;
const bootstrap = {
  self: { kind: "human", human_id: "human-1" },
  workspaces: [{ workspace_id: "workspace-1", name: "Sumi" }],
  channels: [
    {
      channel_id: "channel-1",
      workspace_id: "workspace-1",
      name: "general",
      topic: "",
      visibility: "public",
    },
  ],
  dms: [],
  members: [
    {
      participant: { kind: "human", human_id: "human-1" },
      display_name: "Yohaku",
    },
  ],
  read_markers: [{ place: channelWire(), last_read_seq: 0 }],
  unread_summaries: [
    {
      place: channelWire(),
      latest_seq: 0,
      unread_count: 0,
      mention_count: 0,
    },
  ],
};

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("ApiMessagingBackend", () => {
  it("uses the browser session REST surface for bootstrap, history, send, and read", async () => {
    const fetchMock = vi.fn(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const path = String(input);
        if (path === "/messaging/bootstrap") return json(bootstrap);
        if (path.includes("/messages?") && init?.method === "GET") {
          return json({ messages: [messageWire(1, "hello")] });
        }
        if (path.endsWith("/messages") && init?.method === "POST") {
          return json({ message_id: "message-2", seq: 2 }, 201);
        }
        if (path.endsWith("/read-through") && init?.method === "PUT") {
          return new Response(null, { status: 204 });
        }
        throw new Error(`unexpected request ${path}`);
      },
    );
    vi.stubGlobal("fetch", fetchMock);
    const backend = new ApiMessagingBackend();

    const snapshot = await backend.bootstrap();
    expect(snapshot.channels[0]?.name).toBe("general");
    expect(snapshot.self).toEqual({ kind: "human", humanId: "human-1" });
    expect(await backend.fetchMessages(channel, { limit: 50 })).toHaveLength(1);
    await expect(
      backend.sendMessage({
        place: channel,
        content: "sent",
        urgency: "normal",
        replyTo: null,
        clientNonce: "nonce-1",
        attachments: [],
      }),
    ).resolves.toEqual({ messageId: "message-2", seq: 2 });
    await backend.markRead(channel, 2);

    expect(fetchMock).toHaveBeenCalledWith(
      "/messaging/places/channel-1/read-through",
      expect.objectContaining({
        method: "PUT",
        credentials: "include",
        body: JSON.stringify({ seq: 2 }),
      }),
    );
  });

  it("uploads an attachment as multipart and sends its id with the message", async () => {
    const requests: { path: string; init?: RequestInit }[] = [];
    const fetchMock = vi.fn(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const path = String(input);
        requests.push({ path, init });
        if (path === "/messaging/attachments") {
          return json(
            {
              attachment_id: "attachment-1",
              filename: "shot.png",
              mime: "image/png",
              size: 2048,
            },
            201,
          );
        }
        if (path.endsWith("/messages") && init?.method === "POST") {
          return json({ message_id: "message-2", seq: 2 }, 201);
        }
        throw new Error(`unexpected request ${path}`);
      },
    );
    vi.stubGlobal("fetch", fetchMock);
    const backend = new ApiMessagingBackend();

    const file = new File([new Uint8Array([1, 2, 3])], "shot.png", {
      type: "image/png",
    });
    const attachment = await backend.uploadAttachment(file);
    expect(attachment).toEqual({
      attachmentId: "attachment-1",
      filename: "shot.png",
      mime: "image/png",
      size: 2048,
      // 取得先は境界がこの形に決める。可視性はサーバーが検査する。
      url: "/messaging/attachments/attachment-1",
    });
    const upload = requests[0];
    expect(upload.init?.method).toBe("POST");
    expect(upload.init?.credentials).toBe("include");
    // boundary付きContent-Typeはブラウザに決めさせる。
    expect(upload.init?.headers).not.toHaveProperty("Content-Type");
    const form = upload.init?.body as FormData;
    expect(form).toBeInstanceOf(FormData);
    expect((form.get("file") as File).name).toBe("shot.png");

    await backend.sendMessage({
      place: channel,
      content: "見て",
      urgency: "normal",
      replyTo: null,
      clientNonce: "nonce-1",
      attachments: [attachment.attachmentId],
    });
    expect(JSON.parse(String(requests[1].init?.body))).toEqual({
      content: "見て",
      urgency: "normal",
      reply_to: "",
      client_nonce: "nonce-1",
      attachments: ["attachment-1"],
    });
  });

  it("projects the attachments a message carries", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        if (String(input) === "/messaging/bootstrap") return json(bootstrap);
        return json({
          messages: [
            {
              ...messageWire(1, "見て"),
              attachments: [
                {
                  attachment_id: "attachment-1",
                  filename: "shot.png",
                  mime: "image/png",
                  size: 2048,
                },
              ],
            },
          ],
        });
      }),
    );
    const backend = new ApiMessagingBackend();

    const [message] = await backend.fetchMessages(channel, { limit: 50 });

    expect(message.attachments).toEqual([
      {
        attachmentId: "attachment-1",
        filename: "shot.png",
        mime: "image/png",
        size: 2048,
        url: "/messaging/attachments/attachment-1",
      },
    ]);
  });

  it("opens one messaging socket, sends cursors, and projects message_created", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => json(bootstrap)),
    );
    vi.stubGlobal("WebSocket", FakeWebSocket);
    const backend = new ApiMessagingBackend();
    await backend.bootstrap();
    const events: ServerEvent[] = [];
    backend.subscribe((event) => events.push(event), {
      sinceByPlace: { "channel:channel-1": 4 },
    });
    const socket = FakeWebSocket.instances[0];
    expect(socket).toBeDefined();
    socket?.open();
    expect(JSON.parse(socket?.sent[0] ?? "{}")).toEqual({
      type: "hello",
      cursors: { "channel-1": 4 },
    });
    socket?.message({
      type: "event",
      event: {
        type: "message_created",
        place_id: "channel-1",
        message: messageWire(5, "live"),
      },
    });
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({
      type: "message_created",
      message: { seq: 5, content: "live", place: channel },
    });
  });
});

class FakeWebSocket extends EventTarget {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSED = 3;
  static instances: FakeWebSocket[] = [];
  readyState = FakeWebSocket.CONNECTING;
  readonly sent: string[] = [];
  readonly url: string | URL;

  constructor(url: string | URL) {
    super();
    this.url = url;
    FakeWebSocket.instances = [this];
  }

  send(data: string): void {
    this.sent.push(data);
  }

  close(): void {
    this.readyState = FakeWebSocket.CLOSED;
    this.dispatchEvent(new Event("close"));
  }

  open(): void {
    this.readyState = FakeWebSocket.OPEN;
    this.dispatchEvent(new Event("open"));
  }

  message(value: unknown): void {
    this.dispatchEvent(
      new MessageEvent("message", { data: JSON.stringify(value) }),
    );
  }
}

function channelWire() {
  return { kind: "channel", channel_id: "channel-1" };
}

function messageWire(seq: number, content: string) {
  return {
    message_id: `message-${seq}`,
    place: channelWire(),
    seq,
    author: { kind: "human", human_id: "human-1" },
    content,
    mentions: [],
    attachments: [],
    urgency: "normal",
    reply_to: null,
    client_nonce: `nonce-${seq}`,
    created_at: "2026-08-01T10:00:00Z",
    edited_at: null,
    deleted: false,
  };
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}
