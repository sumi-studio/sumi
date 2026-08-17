import { MessagesSquare } from "lucide-react";
import { useState } from "react";
import type { Message } from "../model";
import { usePlaceNavigate } from "../place-route";
import { useMessaging } from "../store";

function threadFor(
  messageId: string,
  threads: ReturnType<typeof useMessaging.getState>["threadsById"],
) {
  return Object.values(threads).find(
    (thread) => thread.parentMessageId === messageId,
  );
}

export function MessageThreadChip({ message }: { message: Message }) {
  const threads = useMessaging((state) => state.threadsById);
  const navigate = usePlaceNavigate();
  const thread = threadFor(message.messageId, threads);
  if (!thread) return null;
  return (
    <button
      type="button"
      onClick={() => navigate(`thread:${thread.threadId}`)}
      className="mt-1 flex items-center gap-1 rounded border px-2 py-1 text-xs text-muted-foreground hover:bg-accent"
    >
      <MessagesSquare className="size-3" />
      {thread.name} · {thread.messageCount}件
    </button>
  );
}

export function MessageThreadAction({ message }: { message: Message }) {
  const canUseThreads = useMessaging((state) => state.capabilities.threads);
  const activePlaceKey = useMessaging((state) => state.activePlaceKey);
  const threads = useMessaging((state) => state.threadsById);
  const createThread = useMessaging((state) => state.createThread);
  const navigate = usePlaceNavigate();
  const [open, setOpen] = useState(false);
  const [name, setName] = useState("");
  if (
    !canUseThreads ||
    !activePlaceKey?.startsWith("channel:") ||
    message.deleted ||
    threadFor(message.messageId, threads)
  )
    return null;
  const submit = async () => {
    if (!name.trim()) return;
    navigate(
      await createThread(activePlaceKey, name.trim(), message.messageId),
    );
  };
  return (
    <span className="relative">
      <button
        type="button"
        title="スレッドを作成"
        aria-label="スレッドを作成"
        onClick={() => {
          setName(message.content.slice(0, 60));
          setOpen(!open);
        }}
        className="flex size-7 items-center justify-center rounded-md text-muted-foreground hover:bg-accent"
      >
        <MessagesSquare className="size-3.5" />
      </button>
      {open ? (
        <span className="absolute top-full right-0 z-30 flex w-64 gap-1 rounded border bg-background p-2 shadow">
          <input
            autoFocus
            maxLength={100}
            value={name}
            onChange={(event) => setName(event.target.value)}
            className="min-w-0 flex-1 rounded border px-2 text-xs"
          />
          <button
            type="button"
            onClick={() => void submit()}
            className="rounded bg-primary px-2 text-xs text-primary-foreground"
          >
            作成
          </button>
        </span>
      ) : null}
    </span>
  );
}
