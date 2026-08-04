import { ChevronRight, MessagesSquare } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import type { Message, ThreadSummary } from "../model";
import { usePlaceNavigate } from "../place-route";
import { useMessaging } from "../store";

/**
 * メッセージ単位のスレッド導線。message-item.tsx は並行して別の作業が入る
 * ファイルなので、ロジックはここに閉じ、向こうには2箇所の差し込みだけを残す。
 */

function threadOf(
  threadsById: Record<string, ThreadSummary>,
  messageId: string,
): ThreadSummary | undefined {
  for (const thread of Object.values(threadsById)) {
    if (thread.parentMessageId === messageId) return thread;
  }
  return undefined;
}

/** 起点メッセージに出す「N件のメッセージ ›」。押すとスレッドへ移動する。 */
export function MessageThreadChip({ message }: { message: Message }) {
  const threadsById = useMessaging((state) => state.threadsById);
  const placeNavigate = usePlaceNavigate();
  const thread = threadOf(threadsById, message.messageId);
  if (!thread) return null;
  return (
    <button
      type="button"
      onClick={() => placeNavigate(`thread:${thread.threadId}`)}
      className="mt-1 flex items-center gap-1.5 rounded-md border border-border bg-muted/30 px-2 py-1 text-[11.5px] transition-colors hover:border-muted-foreground/40"
    >
      <MessagesSquare className="size-3 shrink-0 text-muted-foreground" />
      <span className="truncate font-medium">{thread.name}</span>
      <span className="text-muted-foreground tabular-nums">
        {thread.messageCount}件のメッセージ
      </span>
      <ChevronRight className="size-3 shrink-0 text-muted-foreground" />
    </button>
  );
}

/**
 * メッセージ操作の「スレッドを作成」。すでにスレッドを持つメッセージには
 * 出さない（1メッセージから生えるスレッドは1本）。
 */
export function MessageThreadAction({ message }: { message: Message }) {
  const allowThreads = useMessaging((state) => state.capabilities.threads);
  const threadsById = useMessaging((state) => state.threadsById);
  const createThread = useMessaging((state) => state.createThread);
  const activePlaceKey = useMessaging((state) => state.activePlaceKey);
  const placeNavigate = usePlaceNavigate();
  const [open, setOpen] = useState(false);
  const [name, setName] = useState("");
  const [busy, setBusy] = useState(false);
  const [failed, setFailed] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (open) inputRef.current?.focus();
  }, [open]);

  useEffect(() => {
    if (!open) return;
    const closeOnOutside = (event: MouseEvent) => {
      if (!containerRef.current?.contains(event.target as Node)) setOpen(false);
    };
    window.addEventListener("mousedown", closeOnOutside);
    return () => window.removeEventListener("mousedown", closeOnOutside);
  }, [open]);

  if (!allowThreads || message.deleted) return null;
  // スレッドは今のところチャンネル配下だけ。DMの中に脇道は作らない。
  if (!activePlaceKey?.startsWith("channel:")) return null;
  if (threadOf(threadsById, message.messageId)) return null;

  const submit = async (event: React.FormEvent) => {
    event.preventDefault();
    const trimmed = name.trim();
    if (!trimmed || busy) return;
    setBusy(true);
    setFailed(false);
    try {
      const key = await createThread(
        activePlaceKey,
        trimmed,
        message.messageId,
      );
      setOpen(false);
      setName("");
      setBusy(false);
      placeNavigate(key);
    } catch {
      setFailed(true);
      setBusy(false);
    }
  };

  return (
    <div ref={containerRef} className="relative">
      <button
        type="button"
        title="スレッドを作成"
        aria-label="スレッドを作成"
        aria-expanded={open}
        onClick={() => {
          setName(message.content.slice(0, 60));
          setOpen((value) => !value);
        }}
        className="flex size-7 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
      >
        <MessagesSquare className="size-3.5" />
      </button>
      {open ? (
        <form
          onSubmit={submit}
          className="absolute top-full right-0 z-30 mt-1 w-64 space-y-1.5 rounded-lg border border-border bg-background p-2 shadow-md"
        >
          <p className="font-medium text-[11px] text-muted-foreground">
            スレッドを作成 — この発言から枝分かれします
          </p>
          <input
            ref={inputRef}
            value={name}
            disabled={busy}
            maxLength={100}
            onChange={(event) => setName(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === "Escape") setOpen(false);
            }}
            placeholder="スレッドの名前"
            aria-label="スレッドの名前"
            className="w-full rounded-md border border-border bg-background px-2 py-1 text-[12.5px] outline-none placeholder:text-muted-foreground/60 focus-visible:border-ring/60 disabled:opacity-50"
          />
          {failed ? (
            <p className="text-[11px] text-rose-500">
              スレッドを作成できませんでした
            </p>
          ) : null}
          <div className="flex justify-end">
            <button
              type="submit"
              disabled={busy || !name.trim()}
              className="rounded-md bg-primary px-2 py-1 font-medium text-[12px] text-primary-foreground hover:opacity-90 disabled:opacity-50"
            >
              作成
            </button>
          </div>
        </form>
      ) : null}
    </div>
  );
}
