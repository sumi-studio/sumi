import type { BrowserServerFrame, Command } from "@sumi/api-client";
import { createFileRoute } from "@tanstack/react-router";
import { useEffect, useMemo, useRef, useState } from "react";
import {
  type ConnectionState,
  ConversationSocket,
  eventType,
} from "../lib/conversation-socket";

export const Route = createFileRoute("/")({ component: Home });

type FeedItem = {
  id: string;
  kind: "assistant" | "tool" | "steer" | "approval" | "system";
  text: string;
  requestID?: string;
};

function Home() {
  const conversationID = import.meta.env.VITE_CONVERSATION_ID ?? "default";
  const socket = useMemo(() => new ConversationSocket(conversationID), []);
  const [state, setState] = useState<ConnectionState>("connecting");
  const [input, setInput] = useState("");
  const [feed, setFeed] = useState<FeedItem[]>([]);
  const streamingText = useRef("");
  const streamingMessageID = useRef<string | undefined>(undefined);

  useEffect(() => {
    const offState = socket.onState(setState);
    const offFrame = socket.onFrame((frame) =>
      applyFrame(frame, streamingText, streamingMessageID, setFeed),
    );
    socket.connect();
    return () => {
      offFrame();
      offState();
      socket.close();
    };
  }, [socket]);

  const send = () => {
    const text = input.trim();
    if (!text) return;
    const command: Command = { type: "user_message", text, attachments: [] };
    if (!socket.sendCommand(command)) {
      return;
    }
    setFeed((items) => [
      ...items,
      {
        id: crypto.randomUUID(),
        kind: "system",
        text:
          state === "open" && streamingText.current ? `Steer: ${text}` : text,
      },
    ]);
    setInput("");
  };

  return (
    <main className="mx-auto flex min-h-dvh max-w-3xl flex-col bg-white px-5 py-6 text-zinc-900">
      <header className="mb-6 flex items-center justify-between border-b pb-4">
        <div>
          <h1 className="text-xl font-semibold">Sumi</h1>
          <p className="text-sm text-zinc-500">{conversationID}</p>
        </div>
        <span className="text-sm text-zinc-500">{state}</span>
      </header>
      <section className="flex-1 space-y-4" aria-live="polite">
        {feed.length === 0 && (
          <p className="text-zinc-500">会話を開始してください。</p>
        )}
        {feed.map((item) => (
          <FeedItemView
            item={item}
            socket={socket}
            state={state}
            key={item.id}
          />
        ))}
      </section>
      <form
        className="mt-8 flex gap-2 border-t pt-4"
        onSubmit={(event) => {
          event.preventDefault();
          send();
        }}
      >
        <label className="sr-only" htmlFor="message">
          メッセージ
        </label>
        <textarea
          id="message"
          className="min-h-12 flex-1 resize-none rounded-lg border p-3"
          value={input}
          onChange={(event) => setInput(event.target.value)}
          placeholder="メッセージ…"
        />
        <button
          className="rounded-lg bg-zinc-900 px-4 text-white disabled:opacity-40"
          disabled={state !== "open" || !input.trim()}
          type="submit"
        >
          {streamingText.current ? "Steer" : "送信"}
        </button>
        {streamingText.current && (
          <button
            className="rounded-lg border px-3 disabled:opacity-40"
            disabled={state !== "open"}
            type="button"
            onClick={() => socket.sendCommand({ type: "abort" })}
          >
            停止
          </button>
        )}
      </form>
    </main>
  );
}

function FeedItemView({
  item,
  socket,
  state,
}: {
  item: FeedItem;
  socket: ConversationSocket;
  state: ConnectionState;
}) {
  const disabled = state !== "open";

  if (item.kind === "approval" && item.requestID) {
    const requestID = item.requestID;
    return (
      <article className="rounded-lg border border-amber-300 bg-amber-50 p-4">
        <p className="font-medium">🔐 承認が必要です</p>
        <p className="my-2 text-sm">{item.text}</p>
        <div className="flex gap-2">
          <button
            type="button"
            className="rounded bg-zinc-900 px-3 py-1 text-white disabled:opacity-40"
            disabled={disabled}
            onClick={() =>
              socket.sendCommand({
                type: "approval_decision",
                request_id: requestID,
                decision: { type: "approve_once" },
              })
            }
          >
            今回のみ
          </button>
          <button
            type="button"
            className="rounded border px-3 py-1 disabled:opacity-40"
            disabled={disabled}
            onClick={() =>
              socket.sendCommand({
                type: "approval_decision",
                request_id: requestID,
                decision: { type: "deny" },
              })
            }
          >
            拒否
          </button>
        </div>
      </article>
    );
  }
  return (
    <article
      className={
        item.kind === "tool"
          ? "rounded-lg bg-zinc-100 p-3 text-sm"
          : "whitespace-pre-wrap"
      }
    >
      {item.kind === "tool"
        ? item.text
        : item.kind === "steer"
          ? `Steered (${item.text})`
          : item.text}
    </article>
  );
}

function applyFrame(
  frame: BrowserServerFrame,
  streamingText: React.MutableRefObject<string>,
  streamingMessageID: React.MutableRefObject<string | undefined>,
  setFeed: React.Dispatch<React.SetStateAction<FeedItem[]>>,
) {
  if (frame.type === "command_rejected") {
    setFeed((items) => [
      ...items,
      {
        id: crypto.randomUUID(),
        kind: "system",
        text: `Command rejected: ${frame.reject_reason}`,
      },
    ]);
    return;
  }
  if (frame.type === "command_accepted") {
    // The UI already renders user messages optimistically; the server is
    // acknowledging durable append. No further action is required.
    return;
  }
  if (frame.type !== "event") return;
  const event = frame.envelope.event as unknown as Record<string, unknown>;
  const type = eventType(frame.envelope);
  if (type === "message_update") {
    const messageID = String(event.message_id);
    const stream = event.event as { type?: string; delta?: string };
    if (stream.type === "text_delta" && typeof stream.delta === "string") {
      if (streamingMessageID.current !== messageID) {
        streamingText.current = "";
        streamingMessageID.current = messageID;
      }
      streamingText.current += stream.delta;
      const id = `stream-${messageID}`;
      setFeed((items) => [
        ...items.filter((item) => item.id !== id),
        { id, kind: "assistant", text: streamingText.current },
      ]);
    }
    return;
  }
  if (type === "message_end") {
    const message = event.message as {
      role?: string;
      content?: Array<{ type?: string; text?: string }>;
    };
    if (message.role !== "assistant") return;
    const text = message.content
      ?.filter((content) => content.type === "text")
      .map((content) => content.text ?? "")
      .join("");
    if (text) {
      const id = `message-${String(event.message_id)}`;
      setFeed((items) => [
        ...items.filter(
          (item) => !item.id.startsWith("stream-") && item.id !== id,
        ),
        { id, kind: "assistant", text },
      ]);
    }
    streamingText.current = "";
    streamingMessageID.current = undefined;
  }
  if (type === "tool_execution_start" || type === "tool_execution_end") {
    const label =
      type === "tool_execution_start"
        ? `Tool started: ${String(event.tool_name)}`
        : `Tool finished: ${String(event.tool_call_id)}`;
    setFeed((items) => [
      ...items,
      { id: crypto.randomUUID(), kind: "tool", text: label },
    ]);
  } else if (type === "steered") {
    setFeed((items) => [
      ...items,
      { id: crypto.randomUUID(), kind: "steer", text: String(event.mode) },
    ]);
  } else if (type === "approval_requested") {
    const request = event.request as { id?: string; action?: unknown };
    setFeed((items) => [
      ...items,
      {
        id: crypto.randomUUID(),
        kind: "approval",
        requestID: request.id,
        text: JSON.stringify(request.action ?? request),
      },
    ]);
  } else if (type === "approval_resolved") {
    const requestID = String(event.request_id);
    setFeed((items) =>
      items.filter(
        (item) => item.kind !== "approval" || item.requestID !== requestID,
      ),
    );
  }
}
