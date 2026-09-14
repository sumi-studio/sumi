import "@sumi/ui/globals.css";
import type {
  BrowserEventEnvelope,
  PublicAssistantMessage,
  PublicMessage,
} from "@sumi/api-client";
import {
  Profiler,
  type ProfilerOnRenderCallback,
  StrictMode,
  useMemo,
} from "react";
import { createRoot } from "react-dom/client";
import {
  createConversationStore,
  type DirectChatTransport,
} from "../src/agent/store";
import { ChatScreen } from "../src/components/chat-screen";
import type {
  DirectChatHistoryAPI,
  DirectChatHistoryPage,
  DirectChatIndexEntry,
  DurableHistoryEvent,
} from "../src/lib/direct-chat-history";
import type {
  DirectChatConnectionState,
  DirectChatInstallationBinding,
  DirectChatReadyState,
  DirectChatServerFrame,
} from "../src/lib/direct-chat-socket";

/**
 * Streaming-performance harness: mounts the real ChatScreen against a real
 * conversation store fed by a synthetic transport and a generated history
 * page. window.__stream drives a lifetime-scale log plus a streamed reply;
 * window.__perf records every React commit and per-frame processing time so
 * a spec can compare short and long histories without wall-clock assertions.
 */

interface CommitRecord {
  phase: string;
  actualDuration: number;
  baseDuration: number;
  t: number;
}
interface FrameRecord {
  seq: number | null;
  type: string;
  emitMs: number;
  commitCountBefore: number;
}
const perf: {
  commits: CommitRecord[];
  frames: FrameRecord[];
} = { commits: [], frames: [] };

const onRender: ProfilerOnRenderCallback = (
  _id,
  phase,
  actualDuration,
  baseDuration,
) => {
  perf.commits.push({
    phase,
    actualDuration,
    baseDuration,
    t: performance.now(),
  });
};

class FakeTransport implements DirectChatTransport {
  private frames = new Set<(frame: DirectChatServerFrame) => void>();
  private connections = new Set<(state: DirectChatConnectionState) => void>();
  private readies = new Set<(state: DirectChatReadyState) => void>();
  replayCursor = 0;
  bindInstallation(_binding: DirectChatInstallationBinding) {}
  suspendInstallation() {}
  connect() {
    for (const listener of this.connections) listener("connected");
    for (const listener of this.readies) listener("ready");
  }
  close() {}
  setReplayCursor(seq: number) {
    this.replayCursor = seq;
  }
  sendCommand() {
    return true;
  }
  onFrame(listener: (frame: DirectChatServerFrame) => void) {
    this.frames.add(listener);
    return () => this.frames.delete(listener);
  }
  onConnection(listener: (state: DirectChatConnectionState) => void) {
    this.connections.add(listener);
    return () => this.connections.delete(listener);
  }
  onReady(listener: (state: DirectChatReadyState) => void) {
    this.readies.add(listener);
    return () => this.readies.delete(listener);
  }
  emit(envelope: BrowserEventEnvelope) {
    const record: FrameRecord = {
      seq: "seq" in envelope ? envelope.seq : null,
      type: envelope.event.type,
      emitMs: 0,
      commitCountBefore: perf.commits.length,
    };
    perf.frames.push(record);
    const start = performance.now();
    for (const listener of this.frames) listener({ type: "event", envelope });
    record.emitMs = performance.now() - start;
  }
}

const transport = new FakeTransport();
let seq = 0;
const nextSeq = () => ++seq;
const messageId = (n: number) =>
  `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`;

const Timestamp = "2026-09-14T12:00:00Z";

function userMessage(text: string): PublicMessage {
  return {
    role: "user",
    content: [{ type: "text", text }],
    timestamp: Timestamp,
  };
}

function assistantMessage(text: string): PublicAssistantMessage {
  return {
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
  };
}

/** One exchange = user message + reasoning summary + two assistant blocks. */
function buildHistoryPage(turns: number): DirectChatHistoryPage {
  const events: DurableHistoryEvent[] = [];
  const index: DirectChatIndexEntry[] = [];
  const push = (event: DurableHistoryEvent["event"]) =>
    events.push({
      audience: "direct_chat",
      seq: nextSeq(),
      event,
    });
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
    index.push({
      id: userId,
      seq,
      title: `記録の質問 ${turn}`,
      preview: `応答 ${turn}`,
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
  return {
    events,
    context: [],
    latestSeq: seq,
    beforeSeq: null,
    hasMore: false,
    index,
    activeRun: null,
    pendingApprovals: [],
    commandDispositions: [],
  };
}

const historyAPI: DirectChatHistoryAPI = {
  async page(_binding, query) {
    const turns = Number(
      new URLSearchParams(globalThis.location?.search).get("turns") ?? 0,
    );
    if (query.beforeSeq !== undefined || query.aroundMessageId) {
      return {
        events: [],
        context: [],
        latestSeq: seq,
        beforeSeq: null,
        hasMore: false,
        index: [],
        activeRun: null,
        pendingApprovals: [],
        commandDispositions: [],
      };
    }
    return buildHistoryPage(turns);
  },
};

const store = createConversationStore({
  transport,
  historyAPI,
  idempotencyKey: () => messageId(900_000 + Math.floor(Math.random() * 90_000)),
  reducerId: () => "harness",
});

const deltas = { messageId: "", text: "" };

const streamApi = {
  stats() {
    const conversation = store.getState().conversation;
    return {
      entries: conversation.entryOrder.length,
      runs: conversation.runOrder.length,
    };
  },
  beginReply() {
    deltas.messageId = messageId(1_000_000 + seq);
    deltas.text = "";
    transport.emit({
      audience: "direct_chat",
      seq: nextSeq(),
      event: { type: "agent_start" },
    });
    transport.emit({
      audience: "direct_chat",
      seq: nextSeq(),
      event: {
        type: "message_start",
        message_id: deltas.messageId,
        message: assistantMessage(""),
      },
    });
    transport.emit({
      audience: "direct_chat",
      event: {
        type: "message_update",
        message_id: deltas.messageId,
        event: { type: "text_start", content_index: 0 },
      },
    });
  },
  /** Emit one text delta; returns the synchronous emit time in ms. */
  delta(chunk: string) {
    deltas.text += chunk;
    const frame = perf.frames.length;
    transport.emit({
      audience: "direct_chat",
      event: {
        type: "message_update",
        message_id: deltas.messageId,
        event: { type: "text_delta", content_index: 0, delta: chunk },
      },
    });
    return perf.frames[frame].emitMs;
  },
  endReply() {
    transport.emit({
      audience: "direct_chat",
      seq: nextSeq(),
      event: {
        type: "message_end",
        message_id: deltas.messageId,
        message: assistantMessage(deltas.text),
      },
    });
    transport.emit({
      audience: "direct_chat",
      seq: nextSeq(),
      event: { type: "agent_end" },
    });
  },
  resetPerf() {
    perf.commits.length = 0;
    perf.frames.length = 0;
  },
  perf() {
    return perf;
  },
  rafDelay() {
    return new Promise<number>((resolve) => {
      const start = performance.now();
      requestAnimationFrame(() => resolve(performance.now() - start));
    });
  },
};

(globalThis as Record<string, unknown>).__stream = streamApi;
(globalThis as Record<string, unknown>).__perf = perf;

function App() {
  const screen = useMemo(
    () => (
      <ChatScreen
        installationId="installation-1"
        authorityEpoch="1"
        store={store}
      />
    ),
    [],
  );
  return (
    <Profiler id="chat-screen" onRender={onRender}>
      <div style={{ height: "100dvh" }}>{screen}</div>
    </Profiler>
  );
}

const root = document.getElementById("root");
if (!root) throw new Error("missing #root");
createRoot(root).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
