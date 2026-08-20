import { MessagesSquare, Plus, X } from "lucide-react";
import { useRef, useState } from "react";
import { secureRandomUUID } from "../../lib/random-uuid";
import type { PlaceKey } from "../model";
import { placeKey } from "../model";
import { usePlaceNavigate } from "../place-route";
import { useMessaging } from "../store";

export function ThreadPanel({
  parentKey,
  onClose,
}: {
  parentKey: PlaceKey;
  onClose: () => void;
}) {
  const threadsById = useMessaging((state) => state.threadsById);
  const createThread = useMessaging((state) => state.createThread);
  const navigate = usePlaceNavigate();
  const [name, setName] = useState("");
  const [creating, setCreating] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const creatingRef = useRef(false);
  const createNonceRef = useRef<string | null>(null);
  const threads = Object.values(threadsById)
    .filter((thread) => placeKey(thread.parentPlace) === parentKey)
    .sort((a, b) => (b.lastMessageAt ?? 0) - (a.lastMessageAt ?? 0));
  const submit = async () => {
    if (creatingRef.current) return;
    const trimmed = name.trim();
    if (!trimmed) return;
    creatingRef.current = true;
    setCreating(true);
    setError(null);
    try {
      const nonce = createNonceRef.current ?? secureRandomUUID();
      createNonceRef.current = nonce;
      const key = await createThread(parentKey, trimmed, null, nonce);
      createNonceRef.current = null;
      setName("");
      navigate(key);
      onClose();
    } catch {
      setError("スレッドを作成できませんでした。再試行してください。");
    } finally {
      creatingRef.current = false;
      setCreating(false);
    }
  };
  return (
    <aside className="flex w-72 shrink-0 flex-col border-border/70 border-l bg-muted/20">
      <div className="flex h-12 items-center gap-2 border-border/70 border-b px-3">
        <MessagesSquare className="size-4" />
        <strong className="text-sm">スレッド</strong>
        <button
          type="button"
          className="ml-auto"
          aria-label="スレッド一覧を閉じる"
          onClick={onClose}
        >
          <X className="size-4" />
        </button>
      </div>
      <div className="flex gap-1 p-2">
        <input
          className="min-w-0 flex-1 rounded border bg-background px-2 py-1 text-sm"
          maxLength={100}
          value={name}
          onChange={(event) => {
            createNonceRef.current = null;
            setError(null);
            setName(event.target.value);
          }}
          placeholder="スレッド名"
          disabled={creating}
        />
        <button
          type="button"
          aria-label="スレッドを作成"
          className="rounded border px-2"
          onClick={() => void submit()}
          disabled={creating}
        >
          <Plus className="size-4" />
        </button>
      </div>
      {error ? (
        <p role="alert" className="px-3 pb-2 text-xs text-destructive">
          {error}
        </p>
      ) : null}
      <div className="min-h-0 flex-1 overflow-y-auto p-1">
        {threads.map((thread) => (
          <button
            key={thread.threadId}
            type="button"
            onClick={() => {
              navigate(`thread:${thread.threadId}`);
              onClose();
            }}
            className="block w-full rounded px-2 py-2 text-left hover:bg-accent"
          >
            <span className="block truncate text-sm font-medium">
              {thread.name}
            </span>
            <span className="block truncate text-xs text-muted-foreground">
              {thread.lastMessage || `${thread.messageCount}件のメッセージ`}
            </span>
          </button>
        ))}
      </div>
    </aside>
  );
}
