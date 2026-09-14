/**
 * Micro-benchmark: decomposes the per-delta cost of the direct-chat stream
 * path (reducer + projections) at short and lifetime-scale histories.
 * Run: pnpm --filter @sumi/web exec tsx scripts/conversation-perf-bench.ts
 */
import type {
  BrowserEventEnvelope,
  PublicAssistantMessage,
  PublicMessage,
} from "@sumi/api-client";
import {
  collectAgentCopyText,
  projectConversation,
} from "../src/agent/projection";
import {
  ConversationProjector,
  collectTimelineExchanges,
} from "../src/agent/projector";
import {
  type AgentSession,
  createAgentSession,
  reduceEnvelope,
} from "../src/agent/reducer";
import { createConversationTimeline } from "../src/components/timeline-scrubber";

const Timestamp = "2026-09-14T12:00:00Z";
let seq = 0;
const nextSeq = () => ++seq;
const messageId = (n: number) =>
  `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`;

const userMessage = (text: string): PublicMessage => ({
  role: "user",
  content: [{ type: "text", text }],
  timestamp: Timestamp,
});
const assistantMessage = (text: string): PublicAssistantMessage => ({
  role: "assistant",
  content: text ? [{ type: "text", text, wire_item_index: 0 }] : [],
  model: "gpt-5.6-terra",
  provider: "openai",
  origin: {
    provider_instance_id: "default",
    protocol: "open_ai_responses",
    model: "gpt-5.6-terra",
  },
  usage: {
    input: 1,
    output: 1,
    cache_read: 0,
    cache_write: 0,
    reasoning: 0,
    total_tokens: 2,
  },
  stop_reason: "stop",
  error_message: null,
  provider_code: null,
  interrupted: false,
  timestamp: Timestamp,
});

function buildSession(turns: number): AgentSession {
  let session = createAgentSession();
  const push = (event: BrowserEventEnvelope["event"]) => {
    const result = reduceEnvelope(
      session,
      { audience: "direct_chat", seq: nextSeq(), event },
      { id: () => "bench" },
    );
    session = result.session;
  };
  for (let turn = 0; turn < turns; turn += 1) {
    const userId = messageId(turn * 4);
    const assistantId = messageId(turn * 4 + 1);
    push({ type: "agent_start" });
    push({
      type: "message_start",
      message_id: userId,
      message: userMessage(`記録の質問 ${turn}`),
    });
    push({
      type: "message_end",
      message_id: userId,
      message: userMessage(`記録の質問 ${turn}`),
    });
    push({
      type: "message_start",
      message_id: assistantId,
      message: assistantMessage(""),
    });
    push({
      type: "reasoning_summary",
      message_id: assistantId,
      content_index: 0,
      wire_item_index: 0,
      content: `考えたこと ${turn}`,
    });
    push({
      type: "message_end",
      message_id: assistantId,
      message: {
        ...assistantMessage(""),
        content: [
          {
            type: "text",
            text: `応答 ${turn} の前半です。`.repeat(4),
            wire_item_index: 1,
          },
          {
            type: "text",
            text: `応答 ${turn} の後半です。`.repeat(4),
            wire_item_index: 2,
          },
        ],
      },
    });
    push({ type: "agent_end" });
  }
  // Begin a streamed reply.
  const replyId = messageId(1_000_000);
  push({ type: "agent_start" });
  push({
    type: "message_start",
    message_id: replyId,
    message: assistantMessage(""),
  });
  const result = reduceEnvelope(
    session,
    {
      audience: "direct_chat",
      event: {
        type: "message_update",
        message_id: replyId,
        event: { type: "text_start", content_index: 0 },
      },
    },
    { id: () => "bench" },
  );
  return { ...result.session, conversation: result.session.conversation };
}

function bench(name: string, fn: () => void, iterations: number) {
  fn();
  const times: number[] = [];
  for (let i = 0; i < iterations; i += 1) {
    const start = performance.now();
    fn();
    times.push(performance.now() - start);
  }
  times.sort((a, b) => a - b);
  const median = times[Math.floor(times.length / 2)];
  const p90 = times[Math.floor(times.length * 0.9)];
  console.log(
    `${name}: median=${median.toFixed(3)}ms p90=${p90.toFixed(3)}ms n=${iterations}`,
  );
}

for (const turns of [24, 1600]) {
  let session = buildSession(turns);
  const model = session.conversation;
  const replyId = messageId(1_000_000);
  console.log(
    `\n=== turns=${turns} entries=${model.entryOrder.length} runs=${model.runOrder.length}`,
  );

  const projector = new ConversationProjector();
  projector.update(model);

  const delta = (): BrowserEventEnvelope => ({
    audience: "direct_chat",
    event: {
      type: "message_update",
      message_id: replyId,
      event: { type: "text_delta", content_index: 0, delta: "x" },
    },
  });

  bench(
    "reduce+projector text_delta",
    () => {
      session = reduceEnvelope(session, delta(), { id: () => "bench" }).session;
      projector.update(session.conversation);
    },
    200,
  );
  bench("projectConversation", () => void projectConversation(model), 200);
  bench("collectAgentCopyText", () => void collectAgentCopyText(model), 200);
  const { exchanges, itemIndexById } = collectTimelineExchanges(
    projector.items,
  );
  bench(
    "createConversationTimeline",
    () =>
      void createConversationTimeline(exchanges, itemIndexById, [], undefined),
    200,
  );
}
