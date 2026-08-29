import { MessagesSquare } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { isImeComposing } from "../../lib/ime";
import { secureRandomUUID } from "../../lib/random-uuid";
import { clampCodePoints, codePointLength } from "../../lib/text-length";
import type { Message } from "../model";
import { usePlaceNavigate } from "../place-route";
import { useMessaging } from "../store";
import { useOverlayPanel } from "./overlay";

const THREAD_NAME_MAX_CODE_POINTS = 100;
const THREAD_NAME_SUGGESTION_CODE_POINTS = 60;

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
  const [creating, setCreating] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const creatingRef = useRef(false);
  const createNonceRef = useRef<string | null>(null);
  const initializedDraftRef = useRef(false);
  const overlay = useOverlayPanel<HTMLButtonElement>({
    open,
    onOpenChange: setOpen,
  });

  useEffect(() => {
    if (open) inputRef.current?.focus();
  }, [open]);

  if (
    !canUseThreads ||
    !activePlaceKey?.startsWith("channel:") ||
    message.deleted ||
    threadFor(message.messageId, threads)
  )
    return null;
  const submit = async () => {
    const trimmed = name.trim();
    if (
      creatingRef.current ||
      !trimmed ||
      codePointLength(trimmed) > THREAD_NAME_MAX_CODE_POINTS
    ) {
      return;
    }
    const nonce = createNonceRef.current ?? secureRandomUUID();
    createNonceRef.current = nonce;
    creatingRef.current = true;
    setCreating(true);
    setError(null);
    try {
      navigate(
        await createThread(activePlaceKey, trimmed, message.messageId, nonce),
      );
      createNonceRef.current = null;
      initializedDraftRef.current = false;
      setName("");
      overlay.close();
    } catch {
      setError("スレッドを作成できませんでした。再試行してください。");
    } finally {
      creatingRef.current = false;
      setCreating(false);
    }
  };
  return (
    <div className="relative">
      <button
        type="button"
        title="スレッドを作成"
        aria-label="スレッドを作成"
        aria-haspopup="dialog"
        {...overlay.triggerProps}
        onClick={() => {
          if (!open && !initializedDraftRef.current) {
            initializedDraftRef.current = true;
            setName(
              clampCodePoints(
                message.content,
                THREAD_NAME_SUGGESTION_CODE_POINTS,
              ),
            );
          }
          overlay.toggle();
        }}
        className="flex size-7 items-center justify-center rounded-md text-muted-foreground hover:bg-accent"
      >
        <MessagesSquare className="size-3.5" />
      </button>
      {open ? (
        <div
          {...overlay.panelProps}
          role="dialog"
          aria-label="スレッドを作成"
          className="absolute top-full right-0 z-30 mt-1 w-64 rounded-lg border border-border bg-background p-2 shadow-md"
        >
          <form
            className="space-y-1.5"
            onSubmit={(event) => {
              event.preventDefault();
              void submit();
            }}
          >
            <p
              id={`thread-description-${message.messageId}`}
              className="font-medium text-[11px] text-muted-foreground"
            >
              スレッドを作成 — この発言から枝分かれします
            </p>
            <input
              ref={inputRef}
              value={name}
              disabled={creating}
              onChange={(event) => {
                createNonceRef.current = null;
                setError(null);
                setName(
                  clampCodePoints(
                    event.target.value,
                    THREAD_NAME_MAX_CODE_POINTS,
                  ),
                );
              }}
              onKeyDown={(event) => {
                if (event.key !== "Enter") return;
                event.preventDefault();
                if (!isImeComposing(event)) void submit();
              }}
              placeholder="スレッドの名前"
              aria-label="スレッドの名前"
              aria-describedby={`thread-description-${message.messageId}`}
              className="w-full rounded-md border border-border bg-background px-2 py-1 text-[12.5px] outline-none placeholder:text-muted-foreground/60 focus-visible:border-ring/60 disabled:opacity-50"
            />
            {error ? (
              <p role="alert" className="text-[11px] text-destructive">
                {error}
              </p>
            ) : null}
            <div className="flex justify-end">
              <button
                type="submit"
                disabled={creating || !name.trim()}
                className="rounded-md bg-primary px-2 py-1 font-medium text-[12px] text-primary-foreground hover:opacity-90 disabled:opacity-50"
              >
                {creating ? "作成中…" : "作成"}
              </button>
            </div>
          </form>
        </div>
      ) : null}
    </div>
  );
}
