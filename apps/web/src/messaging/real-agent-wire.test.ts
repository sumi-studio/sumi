import { describe, expect, it } from "vitest";
import { hasExactOpenMessageWireShape } from "./real-agent-wire";

describe("real-agent の messages 形状アサーション", () => {
  it("revision を必須の wire key として受け入れる", () => {
    expect(hasExactOpenMessageWireShape(messageWire())).toBe(true);
  });

  it("revision を欠く旧い wire を拒否する", () => {
    expect(
      hasExactOpenMessageWireShape(
        Object.fromEntries(
          Object.entries(messageWire()).filter(([key]) => key !== "revision"),
        ),
      ),
    ).toBe(false);
  });
});

function messageWire() {
  return {
    message_id: "0198f0f4-9b72-7000-8000-000000000001",
    place: { kind: "channel", channel_id: "channel-a" },
    seq: 1,
    author: {
      kind: "human",
      human_id: "0198f0f4-9b72-7000-8000-000000000002",
    },
    content: "revision を含む",
    mentions: [],
    urgency: "normal",
    reactions: [],
    attachments: [],
    reply_to: null,
    client_nonce: "nonce",
    created_at: "2026-08-18T00:00:00Z",
    edited_at: null,
    revision: 1,
    deleted: false,
  };
}
