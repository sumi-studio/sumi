import { MessagesSquare, Plus, Search, X } from "lucide-react";
import { useEffect, useMemo, useRef, useState } from "react";
import type { PlaceKey, ThreadSummary } from "../model";
import { participantKey } from "../model";
import { usePlaceNavigate } from "../place-route";
import { useMessaging } from "../store";
import { ParticipantAvatar } from "./participant-avatar";

/**
 * チャンネル配下のスレッド一覧。閲覧は親チャンネルのメンバー全員できるので、
 * ここには「自分が参加しているもの」ではなくその場所の脇道が全部並ぶ。
 */

const RELATIVE_FORMAT = new Intl.DateTimeFormat("ja-JP", {
  month: "numeric",
  day: "numeric",
});

function activityLabel(at: number | null, now: number): string {
  if (at === null) return "まだ発言はありません";
  const minutes = Math.round((now - at) / 60_000);
  if (minutes < 1) return "たった今";
  if (minutes < 60) return `${minutes}分前`;
  if (minutes < 24 * 60) return `${Math.round(minutes / 60)}時間前`;
  return RELATIVE_FORMAT.format(at);
}

/** スレッド一覧に並ぶ1行。名前・最新の発言・参加者アバター。 */
function ThreadRow({
  thread,
  onOpen,
}: {
  thread: ThreadSummary;
  onOpen: () => void;
}) {
  const membersByKey = useMessaging((state) => state.membersByKey);
  const unread = useMessaging(
    (state) => state.unreadCountByPlace[`thread:${thread.threadId}`] ?? 0,
  );
  const shown = thread.participants.slice(0, 4);
  return (
    <button
      type="button"
      onClick={onOpen}
      className="flex w-full items-start gap-2 rounded-md px-2 py-2 text-left transition-colors hover:bg-accent/60"
    >
      <MessagesSquare className="mt-0.5 size-3.5 shrink-0 text-muted-foreground" />
      <span className="min-w-0 flex-1">
        <span className="flex items-center gap-1.5">
          <span className="min-w-0 flex-1 truncate font-medium text-[13px]">
            {thread.name}
          </span>
          {unread > 0 ? (
            <span className="shrink-0 rounded-full bg-muted-foreground/20 px-1.5 py-px font-semibold text-[10px] tabular-nums">
              {unread > 99 ? "99+" : unread}
            </span>
          ) : null}
        </span>
        <span className="mt-0.5 block truncate text-[11.5px] text-muted-foreground">
          {thread.lastMessage || "まだ発言はありません"}
        </span>
        <span className="mt-1 flex items-center gap-1.5">
          <span className="flex -space-x-1.5">
            {shown.map((ref) => {
              const key = participantKey(ref);
              return (
                <ParticipantAvatar
                  key={key}
                  participantKey={key}
                  name={membersByKey[key]?.displayName ?? "?"}
                  size={16}
                />
              );
            })}
          </span>
          <span className="text-[10.5px] text-muted-foreground/80 tabular-nums">
            {thread.messageCount}件 ·{" "}
            {activityLabel(thread.lastMessageAt, Date.now())}
          </span>
        </span>
      </span>
    </button>
  );
}

function CreateThreadForm({
  parentKey,
  onDone,
  onCancel,
}: {
  parentKey: PlaceKey;
  onDone: (key: PlaceKey) => void;
  onCancel: () => void;
}) {
  const createThread = useMessaging((state) => state.createThread);
  const [name, setName] = useState("");
  const [busy, setBusy] = useState(false);
  const [failed, setFailed] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    inputRef.current?.focus();
  }, []);

  const submit = async (event: React.FormEvent) => {
    event.preventDefault();
    const trimmed = name.trim();
    if (!trimmed || busy) return;
    setBusy(true);
    setFailed(false);
    try {
      onDone(await createThread(parentKey, trimmed, null));
    } catch {
      setFailed(true);
      setBusy(false);
    }
  };

  return (
    <form onSubmit={submit} className="space-y-1.5 px-2 pb-2">
      <input
        ref={inputRef}
        value={name}
        disabled={busy}
        maxLength={100}
        onChange={(event) => setName(event.target.value)}
        onKeyDown={(event) => {
          if (event.key === "Escape") onCancel();
        }}
        placeholder="スレッドの名前"
        aria-label="スレッドの名前"
        className="w-full rounded-md border border-border bg-background px-2 py-1.5 text-[12.5px] outline-none placeholder:text-muted-foreground/60 focus-visible:border-ring/60 disabled:opacity-50"
      />
      {failed ? (
        <p className="text-[11px] text-rose-500">
          スレッドを作成できませんでした
        </p>
      ) : null}
      <div className="flex justify-end gap-1.5">
        <button
          type="button"
          onClick={onCancel}
          className="rounded-md px-2 py-1 text-[12px] text-muted-foreground hover:bg-accent"
        >
          キャンセル
        </button>
        <button
          type="submit"
          disabled={busy || !name.trim()}
          className="rounded-md bg-primary px-2 py-1 font-medium text-[12px] text-primary-foreground hover:opacity-90 disabled:opacity-50"
        >
          作成
        </button>
      </div>
    </form>
  );
}

export function ThreadPanel({
  parentKey,
  onClose,
}: {
  parentKey: PlaceKey;
  onClose: () => void;
}) {
  const threadsById = useMessaging((state) => state.threadsById);
  const loadThreads = useMessaging((state) => state.loadThreads);
  const placeNavigate = usePlaceNavigate();
  const [query, setQuery] = useState("");
  const [creating, setCreating] = useState(false);

  useEffect(() => {
    void loadThreads(parentKey).catch(() => undefined);
  }, [loadThreads, parentKey]);

  const threads = useMemo(() => {
    const all = Object.values(threadsById).filter(
      (thread) =>
        thread.parentPlace.kind === "channel" &&
        `channel:${thread.parentPlace.channelId}` === parentKey,
    );
    const needle = query.trim().toLowerCase();
    const matched = needle
      ? all.filter((thread) => thread.name.toLowerCase().includes(needle))
      : all;
    return matched.sort(
      (a, b) => (b.lastMessageAt ?? 0) - (a.lastMessageAt ?? 0),
    );
  }, [threadsById, parentKey, query]);

  const open = (key: PlaceKey) => {
    placeNavigate(key);
    onClose();
  };

  return (
    <aside className="flex w-72 shrink-0 flex-col border-border/70 border-l bg-muted/20">
      <div className="flex h-12 shrink-0 items-center gap-2 border-border/70 border-b px-3">
        <span className="min-w-0 flex-1 truncate font-semibold text-[13px]">
          スレッド
        </span>
        <button
          type="button"
          title="スレッドを作成"
          aria-label="スレッドを作成"
          onClick={() => setCreating((value) => !value)}
          className="flex size-7 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
        >
          <Plus className="size-4" />
        </button>
        <button
          type="button"
          title="閉じる"
          aria-label="スレッド一覧を閉じる"
          onClick={onClose}
          className="flex size-7 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
        >
          <X className="size-3.5" />
        </button>
      </div>
      <div className="shrink-0 p-2">
        <span className="flex items-center gap-1.5 rounded-md border border-border bg-background px-2 py-1.5">
          <Search className="size-3.5 shrink-0 text-muted-foreground" />
          <input
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="スレッドを名前で探す"
            aria-label="スレッドを名前で探す"
            className="min-w-0 flex-1 bg-transparent text-[12.5px] outline-none placeholder:text-muted-foreground/60"
          />
        </span>
      </div>
      {creating ? (
        <CreateThreadForm
          parentKey={parentKey}
          onDone={(key) => {
            setCreating(false);
            open(key);
          }}
          onCancel={() => setCreating(false)}
        />
      ) : null}
      <div className="scrollbar-ui min-h-0 flex-1 overflow-y-auto px-1 pb-2">
        {threads.length === 0 ? (
          <div className="flex flex-col items-center gap-2 px-4 py-10 text-center">
            <MessagesSquare className="size-7 text-muted-foreground/40" />
            <p className="font-medium text-[13px]">スレッドはありません。</p>
            <p className="text-[11.5px] text-muted-foreground">
              話が長くなりそうなときは、脇道をスレッドに移すと本流が読みやすくなります。
            </p>
            <button
              type="button"
              onClick={() => setCreating(true)}
              className="mt-1 rounded-md bg-primary px-2.5 py-1 font-medium text-[12px] text-primary-foreground hover:opacity-90"
            >
              スレッドを作成
            </button>
          </div>
        ) : (
          threads.map((thread) => (
            <ThreadRow
              key={thread.threadId}
              thread={thread}
              onOpen={() => open(`thread:${thread.threadId}`)}
            />
          ))
        )}
      </div>
    </aside>
  );
}
