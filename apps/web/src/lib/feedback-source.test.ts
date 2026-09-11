import { expect, it } from "vitest";
import type { ChatItem } from "../agent/model";
import { userItemSourceLabel, userItemText } from "./user-item-text";
it("labels a feedback reply without changing its body or sender", () => {
  const item: Extract<ChatItem, { kind: "user" }> = {
    kind: "user",
    id: "feedback-1",
    text: "修正しました",
    attachments: [],
    timestamp: "2026-09-11T10:00:00Z",
    delivery: "durable",
    source: {
      version: 2,
      tenant_id: "test",
      personality_agent_id: "018f47a2-9b3c-7def-8abc-0123456789ab",
      actor: { kind: "human", principal_id: "author", display_name: "開発者" },
      source: {
        surface: "feedback",
        kind: "feedback_reply",
        event_id: "018f47a2-9b3c-7def-8abc-0123456789ac",
        thread_id: "018f47a2-9b3c-7def-8abc-0123456789ad",
        title: "通知について",
        revision: 2,
        occurred_at: "2026-09-11T10:00:00Z",
      },
    },
  };
  expect(userItemSourceLabel(item)).toBe("Feedback · 通知について · 開発者");
  expect(userItemText(item)).toBe("修正しました");
});
