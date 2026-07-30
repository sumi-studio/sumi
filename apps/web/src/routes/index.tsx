import { createFileRoute } from "@tanstack/react-router";
import { useEffect, useMemo, useState } from "react";
import {
  type DirectChatConnectionState,
  type DirectChatReadyState,
  DirectChatSocket,
} from "../lib/direct-chat-socket";
import { DirectChatTimeline, type FeedItem } from "../lib/direct-chat-timeline";

export const Route = createFileRoute("/")({ component: Home });

function Home() {
  const socket = useMemo(() => new DirectChatSocket(), []);
  const timeline = useMemo(() => new DirectChatTimeline(), []);
  const [connection, setConnection] = useState<DirectChatConnectionState>("connecting");
  const [ready, setReady] = useState<DirectChatReadyState>("unknown");
  const [input, setInput] = useState("");
  const [feed, setFeed] = useState<FeedItem[]>([]);

  useEffect(() => {
    const offConnection = socket.onConnection(setConnection);
    const offReady = socket.onReady(setReady);
    const offFrame = socket.onFrame((frame) => setFeed([...timeline.apply(frame)]));
    socket.connect();
    return () => { offFrame(); offReady(); offConnection(); socket.close(); };
  }, [socket, timeline]);

  const send = () => {
    const text = input.trim();
    if (!text || ready !== "ready") return;
    if (!socket.sendCommand({ type: "user_message", text, attachments: [] })) return;
    setInput("");
  };
  const streaming = feed.some((item) => item.id.startsWith("stream-"));

  return (
    <main className="mx-auto flex min-h-dvh max-w-3xl flex-col bg-white px-5 py-6 text-zinc-900">
      <header className="mb-6 flex items-center justify-between border-b pb-4">
        <div>
          <h1 className="text-xl font-semibold">Sumi</h1>
          <p className="text-sm text-zinc-500">Direct chat</p>
        </div>
        <div className="text-right text-sm text-zinc-500">
          <p>{connection}</p>
          <p>agent: {ready === "ready" ? "Ready" : ready === "not_ready" ? "Unavailable" : "Checking"}</p>
        </div>
      </header>
      <section className="flex-1 space-y-4" aria-live="polite">
        {feed.length === 0 && <p className="text-zinc-500">会話を開始してください。</p>}
        {feed.map((item) => <FeedItemView item={item} socket={socket} connected={connection === "connected"} key={item.id} />)}
      </section>
      <form className="mt-8 flex gap-2 border-t pt-4" onSubmit={(event) => { event.preventDefault(); send(); }}>
        <label className="sr-only" htmlFor="message">メッセージ</label>
        <textarea id="message" className="min-h-12 flex-1 resize-none rounded-lg border p-3" value={input} onChange={(event) => setInput(event.target.value)} placeholder="メッセージ…" />
        <button className="rounded-lg bg-zinc-900 px-4 text-white disabled:opacity-40" disabled={connection !== "connected" || ready !== "ready" || !input.trim()} type="submit">
          {streaming ? "Steer" : "送信"}
        </button>
        {streaming && <button className="rounded-lg border px-3 disabled:opacity-40" disabled={connection !== "connected"} type="button" onClick={() => socket.sendCommand({ type: "abort" })}>停止</button>}
      </form>
    </main>
  );
}

function FeedItemView({ item, socket, connected }: { item: FeedItem; socket: DirectChatSocket; connected: boolean }) {
  if (item.kind === "approval" && item.requestID) {
    const requestID = item.requestID;
    return <article className="rounded-lg border border-amber-300 bg-amber-50 p-4">
      <p className="font-medium">🔐 承認が必要です</p>
      <p className="my-2 text-sm">{item.text}</p>
      <div className="flex gap-2">
        <button type="button" className="rounded bg-zinc-900 px-3 py-1 text-white disabled:opacity-40" disabled={!connected} onClick={() => socket.sendCommand({ type: "approval_decision", request_id: requestID, decision: { type: "approve_once" } })}>今回のみ</button>
        <button type="button" className="rounded border px-3 py-1 disabled:opacity-40" disabled={!connected} onClick={() => socket.sendCommand({ type: "approval_decision", request_id: requestID, decision: { type: "deny" } })}>拒否</button>
      </div>
    </article>;
  }
  return <article className={item.kind === "tool" ? "rounded-lg bg-zinc-100 p-3 text-sm" : "whitespace-pre-wrap"}>
    {item.kind === "steer" ? `Steered (${item.text})` : item.text}
  </article>;
}
