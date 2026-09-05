import { MessagesSquare, Plus, Search, X } from "lucide-react";
import {
  type RefObject,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { isImeComposing } from "../../lib/ime";
import { clampCodePoints, codePointLength } from "../../lib/text-length";
import type { PlaceKey, ThreadSummary } from "../model";
import { participantKey, placeKey } from "../model";
import { usePlaceNavigate } from "../place-route";
import { useMessaging } from "../store";
import { ParticipantAvatar } from "./participant-avatar";

const THREAD_NAME_MAX_CODE_POINTS = 100;
const RELATIVE_FORMAT = new Intl.DateTimeFormat("ja-JP", {
  month: "numeric",
  day: "numeric",
});

function activityLabel(at: number | null, now: number): string {
  if (at === null) return "まだ発言はありません";
  const minutes = Math.max(0, Math.round((now - at) / 60_000));
  if (minutes < 1) return "たった今";
  if (minutes < 60) return `${minutes}分前`;
  if (minutes < 24 * 60) return `${Math.round(minutes / 60)}時間前`;
  return RELATIVE_FORMAT.format(at);
}

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
  const shownParticipants = thread.participants.slice(0, 4);

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
          {shownParticipants.length > 0 ? (
            <span
              className="flex -space-x-1.5"
              title={`参加者 ${shownParticipants.length}人を表示`}
            >
              {shownParticipants.map((participant) => {
                const key = participantKey(participant);
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
          ) : null}
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
  const [creating, setCreating] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const creatingRef = useRef(false);

  useEffect(() => {
    inputRef.current?.focus();
  }, []);

  const submit = async () => {
    const trimmed = name.trim();
    if (
      creatingRef.current ||
      !trimmed ||
      codePointLength(trimmed) > THREAD_NAME_MAX_CODE_POINTS
    ) {
      return;
    }
    creatingRef.current = true;
    setCreating(true);
    setError(null);
    try {
      const key = await createThread(parentKey, trimmed, null);
      onDone(key);
    } catch {
      setError("スレッドを作成できませんでした。再試行してください。");
    } finally {
      creatingRef.current = false;
      setCreating(false);
    }
  };

  return (
    <form
      className="space-y-1.5 px-2 pb-2"
      onSubmit={(event) => {
        event.preventDefault();
        void submit();
      }}
    >
      <input
        ref={inputRef}
        value={name}
        disabled={creating}
        onChange={(event) => {
          setError(null);
          setName(
            clampCodePoints(event.target.value, THREAD_NAME_MAX_CODE_POINTS),
          );
        }}
        onKeyDown={(event) => {
          if (event.key === "Escape" && !isImeComposing(event)) {
            event.preventDefault();
            onCancel();
            return;
          }
          if (event.key !== "Enter") return;
          event.preventDefault();
          if (!isImeComposing(event)) void submit();
        }}
        placeholder="スレッドの名前"
        aria-label="スレッドの名前"
        className="w-full rounded-md border border-border bg-background px-2 py-1.5 text-[12.5px] outline-none placeholder:text-muted-foreground/60 focus-visible:border-ring/60 disabled:opacity-50"
      />
      {error ? (
        <p role="alert" className="text-[11px] text-destructive">
          {error}
        </p>
      ) : null}
      <div className="flex justify-end gap-1.5">
        <button
          type="button"
          onClick={onCancel}
          disabled={creating}
          className="rounded-md px-2 py-1 text-[12px] text-muted-foreground hover:bg-accent"
        >
          キャンセル
        </button>
        <button
          type="submit"
          disabled={creating || !name.trim()}
          className="rounded-md bg-primary px-2 py-1 font-medium text-[12px] text-primary-foreground hover:opacity-90 disabled:opacity-50"
        >
          {creating ? "作成中…" : "作成"}
        </button>
      </div>
    </form>
  );
}

type ThreadListState = "loading" | "loaded" | "error";

export function ThreadPanel({
  parentKey,
  onClose,
  returnFocusRef,
}: {
  parentKey: PlaceKey;
  onClose: () => void;
  returnFocusRef?: RefObject<HTMLButtonElement | null>;
}) {
  const threadsById = useMessaging((state) => state.threadsById);
  const parentLoaded = useMessaging(
    (state) => state.threadsLoadedForPlace[parentKey] ?? false,
  );
  const loadThreads = useMessaging((state) => state.loadThreads);
  const navigate = usePlaceNavigate();
  const [query, setQuery] = useState("");
  const [creating, setCreating] = useState(false);
  const [listState, setListState] = useState<ThreadListState>(() =>
    parentLoaded ? "loaded" : "loading",
  );
  const createTriggerRef = useRef<HTMLButtonElement>(null);
  const loadGenerationRef = useRef(0);

  const load = useCallback(() => {
    const generation = ++loadGenerationRef.current;
    if (parentLoaded) {
      setListState("loaded");
      return;
    }
    setListState("loading");
    void loadThreads(parentKey).then(
      () => {
        if (loadGenerationRef.current === generation) setListState("loaded");
      },
      () => {
        if (loadGenerationRef.current === generation) setListState("error");
      },
    );
  }, [loadThreads, parentKey, parentLoaded]);

  useEffect(() => {
    load();
    return () => {
      loadGenerationRef.current += 1;
    };
  }, [load]);

  const allThreads = useMemo(
    () =>
      Object.values(threadsById)
        .filter((thread) => placeKey(thread.parentPlace) === parentKey)
        .sort((a, b) => (b.lastMessageAt ?? 0) - (a.lastMessageAt ?? 0)),
    [threadsById, parentKey],
  );
  const threads = useMemo(() => {
    const needle = query.trim().toLocaleLowerCase("ja-JP");
    return needle
      ? allThreads.filter((thread) =>
          thread.name.toLocaleLowerCase("ja-JP").includes(needle),
        )
      : allThreads;
  }, [allThreads, query]);

  const openThread = (key: PlaceKey) => {
    navigate(key);
    onClose();
  };
  const closeCreateForm = useCallback(() => {
    createTriggerRef.current?.focus();
    setCreating(false);
  }, []);

  return (
    <aside className="flex w-72 shrink-0 flex-col border-border/70 border-l bg-muted/20">
      <div className="flex h-12 shrink-0 items-center gap-2 border-border/70 border-b px-3">
        <strong className="min-w-0 flex-1 truncate text-[13px]">
          スレッド
        </strong>
        <button
          ref={createTriggerRef}
          type="button"
          title="スレッドを作成"
          aria-label="スレッドを作成"
          aria-expanded={creating}
          onClick={() => setCreating(true)}
          className="flex size-7 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
        >
          <Plus className="size-4" />
        </button>
        <button
          type="button"
          title="閉じる"
          aria-label="スレッド一覧を閉じる"
          onClick={() => {
            returnFocusRef?.current?.focus();
            onClose();
          }}
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
            openThread(key);
          }}
          onCancel={closeCreateForm}
        />
      ) : null}
      <div className="scrollbar-ui min-h-0 flex-1 overflow-y-auto px-1 pb-2">
        {listState === "loading" ? (
          <p
            role="status"
            className="px-3 py-8 text-center text-sm text-muted-foreground"
          >
            スレッドを読み込み中…
          </p>
        ) : listState === "error" ? (
          <div className="px-4 py-8 text-center">
            <p role="alert" className="text-sm text-destructive">
              スレッドを読み込めませんでした
            </p>
            <button
              type="button"
              onClick={load}
              className="mt-2 rounded-md border px-2.5 py-1 text-xs hover:bg-accent"
            >
              再試行
            </button>
          </div>
        ) : allThreads.length === 0 ? (
          <div className="flex flex-col items-center gap-2 px-4 py-10 text-center">
            <MessagesSquare className="size-7 text-muted-foreground/40" />
            <p className="font-medium text-[13px]">スレッドはありません</p>
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
        ) : threads.length === 0 ? (
          <p className="px-3 py-8 text-center text-sm text-muted-foreground">
            一致するスレッドはありません
          </p>
        ) : (
          threads.map((thread) => (
            <ThreadRow
              key={thread.threadId}
              thread={thread}
              onOpen={() => openThread(`thread:${thread.threadId}`)}
            />
          ))
        )}
      </div>
    </aside>
  );
}
