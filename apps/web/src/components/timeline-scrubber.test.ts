import { describe, expect, it } from "vitest";
import type { ChatItem } from "../agent/model";
import { createConversationTimeline } from "./timeline-scrubber";

const user = (id: string): ChatItem => ({
  kind: "user",
  id,
  text: id,
  attachments: [],
  timestamp: null,
  delivery: "durable",
});

describe("paged conversation navigation", () => {
  it("keeps every navigation position while older body pages arrive", () => {
    const index = ["old", "middle", "latest"].map((id) => ({ id, title: id }));
    const before = createConversationTimeline(
      [user("latest")],
      ["latest"],
      index,
    );
    const after = createConversationTimeline(
      [user("middle"), user("latest")],
      ["latest"],
      index,
    );
    expect(before).toEqual(after);
    expect(after.messageIds).toEqual(["old", "middle", "latest"]);
    expect(after.visibleRange).toEqual([2, 2]);
  });

  it("keeps the history index when jumping and adds live messages without duplicates", () => {
    const index = ["old", "middle", "latest"].map((id) => ({ id, title: id }));
    const timeline = createConversationTimeline(
      [user("old"), user("latest"), user("new")],
      ["old"],
      index,
    );
    expect(timeline.messageIds).toEqual(["old", "middle", "latest", "new"]);
    expect(timeline.visibleRange).toEqual([0, 0]);
  });
});
