import { CompactMessageResponse } from "@sumi/ui/ai-elements/compact-message-response";
import {
  ArrowDown,
  ArrowLeft,
  ArrowUp,
  Check,
  ChevronDown,
  Inbox,
  MessageSquarePlus,
  RotateCcw,
  Search,
} from "lucide-react";
import {
  type CSSProperties,
  Fragment,
  useCallback,
  useEffect,
  useLayoutEffect,
  useRef,
  useState,
} from "react";
import { isImeComposing } from "../lib/ime";
import { secureRandomUUID } from "../lib/random-uuid";
import {
  type Author,
  type Bootstrap,
  type Detail,
  errorMessage,
  type FeedbackClient,
  type Filter,
  feedbackClient,
  type Thread,
} from "./api";
import {
  captureFeedbackDiagnostics,
  type FeedbackDiagnostics,
  readServedRelease,
} from "./diagnostics";
import { clearDraft, draftKey, loadDraft, saveDraft } from "./drafts";
import "./inbox.css";

export interface InboxLocation {
  thread?: string;
  compose?: boolean;
}
interface Props {
  actor: string;
  location: InboxLocation;
  navigate(location: InboxLocation): void;
  client?: FeedbackClient;
}

export function FeedbackInbox({
  actor,
  location,
  navigate,
  client = feedbackClient,
}: Props) {
  const [bootstrap, setBootstrap] = useState<Bootstrap | null>(null);
  const [threads, setThreads] = useState<Thread[]>([]);
  const loadedThreads = useRef(threads);
  loadedThreads.current = threads;
  const [cursor, setCursor] = useState<string | null>(null);
  const [filter, setFilter] = useState<Filter>("all");
  const [search, setSearch] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [moreBusy, setMoreBusy] = useState(false);
  const generation = useRef(0);
  const refreshing = useRef(false);
  const paging = useRef(false);
  const container = useRef<HTMLDivElement>(null);
  const [viewportHeight, setViewportHeight] = useState<number>();
  useEffect(() => {
    const viewport = window.visualViewport;
    if (!viewport) return;
    const update = () => {
      const top = container.current?.getBoundingClientRect().top ?? 0;
      setViewportHeight(
        Math.max(200, viewport.height - Math.max(0, top - viewport.offsetTop)),
      );
    };
    update();
    viewport.addEventListener("resize", update);
    viewport.addEventListener("scroll", update);
    return () => {
      viewport.removeEventListener("resize", update);
      viewport.removeEventListener("scroll", update);
    };
  }, []);
  const refresh = useCallback(
    async (initial = false) => {
      if (!initial && (refreshing.current || paging.current)) return;
      refreshing.current = true;
      const version = ++generation.current;
      try {
        const boot = await client.bootstrap();
        if (version !== generation.current) return;
        setBootstrap(boot);
        if (!boot.enabled || !boot.available) {
          setLoading(false);
          return;
        }
        const page = await client.list(filter);
        if (version !== generation.current) return;
        const currentVersions = new Set(
          page.threads.map((thread) => `${thread.id}:${thread.revision}`),
        );
        const listGap =
          loadedThreads.current.length > 0 &&
          !loadedThreads.current.some((thread) =>
            currentVersions.has(`${thread.id}:${thread.revision}`),
          );
        if (initial || listGap || loadedThreads.current.length === 0)
          setCursor(page.next_cursor ?? null);
        setThreads((previous) => {
          // Keep deliberately loaded older pages while refreshing the newest page.
          const ids = new Set(page.threads.map((thread) => thread.id));
          const oldest = page.threads.at(-1)?.updated_at;
          const retained =
            initial || !oldest
              ? []
              : previous.filter(
                  (thread) =>
                    !ids.has(thread.id) &&
                    Date.parse(thread.updated_at) < Date.parse(oldest) &&
                    (filter === "all" || thread.status === filter),
                );
          return [...page.threads, ...retained];
        });

        setError("");
      } catch (cause) {
        if (version === generation.current) setError(errorMessage(cause));
      } finally {
        if (version === generation.current) {
          setLoading(false);
          refreshing.current = false;
        }
      }
    },
    [client, filter],
  );
  useEffect(() => {
    refreshing.current = false;
    paging.current = false;
    setLoading(true);
    setThreads([]);
    setCursor(null);
    void refresh(true);
    const timer = window.setInterval(() => {
      if (document.visibilityState === "visible") void refresh();
    }, 12_000);
    const onFocus = () => void refresh();
    window.addEventListener("focus", onFocus);
    return () => {
      ++generation.current;
      clearInterval(timer);
      window.removeEventListener("focus", onFocus);
    };
  }, [refresh]);
  async function more() {
    if (!cursor || moreBusy) return;
    const version = generation.current;
    setMoreBusy(true);
    paging.current = true;
    try {
      const page = await client.list(filter, cursor);
      if (version !== generation.current) return;
      setThreads((previous) => {
        const ids = new Set(previous.map((item) => item.id));
        return [
          ...previous,
          ...page.threads.filter((item) => !ids.has(item.id)),
        ].sort(
          (left, right) =>
            Date.parse(right.updated_at) - Date.parse(left.updated_at) ||
            right.id.localeCompare(left.id),
        );
      });
      setCursor(page.next_cursor ?? null);
    } catch (cause) {
      if (version === generation.current) setError(errorMessage(cause));
    } finally {
      setMoreBusy(false);
      paging.current = false;
    }
  }
  const visible = threads.filter((thread) =>
    `${thread.title} ${thread.latest_message?.body ?? thread.body}`
      .toLocaleLowerCase()
      .includes(search.toLocaleLowerCase()),
  );
  const detailVisible = Boolean(location.thread || location.compose);
  const unavailable = bootstrap && (!bootstrap.available || !bootstrap.enabled);
  return (
    <div
      ref={container}
      className={`feedback-app${detailVisible ? " feedback-has-detail" : ""}`}
      style={
        viewportHeight
          ? ({ "--feedback-height": `${viewportHeight}px` } as CSSProperties)
          : undefined
      }
    >
      <aside className="feedback-sidebar" aria-label="フィードバック一覧">
        <header className="feedback-sidebar-heading">
          <div>
            <h1>Feedback</h1>
            <p>{bootstrap?.recipient_name ?? "Sumi開発"}と話す</p>
          </div>
          <button
            type="button"
            className="feedback-icon-button"
            title="新しいフィードバック"
            aria-label="新しいフィードバック"
            disabled={!bootstrap?.available || !bootstrap?.enabled}
            onClick={() => navigate({ compose: true })}
          >
            <MessageSquarePlus size={20} />
          </button>
        </header>
        <div className="feedback-search">
          <Search size={16} />
          <input
            type="search"
            aria-label="読み込んだ会話を検索"
            placeholder="会話を探す"
            value={search}
            onChange={(event) => setSearch(event.target.value)}
          />
        </div>
        <fieldset className="feedback-filters" aria-label="会話の状態">
          {(
            [
              ["all", "すべて"],
              ["open", "進行中"],
              ["resolved", "解決済み"],
            ] as const
          ).map(([value, label]) => (
            <button
              type="button"
              key={value}
              aria-pressed={filter === value}
              onClick={() => setFilter(value)}
            >
              {label}
            </button>
          ))}
        </fieldset>
        <div className="feedback-thread-list">
          {error && <Notice text={error} onRetry={() => void refresh(true)} />}
          {loading && threads.length === 0 && (
            <p className="feedback-list-note" role="status">
              会話を読み込んでいます…
            </p>
          )}
          {unavailable && (
            <p className="feedback-list-note" role="status">
              {!bootstrap.enabled
                ? "Feedbackを有効にしてください。"
                : "送信先の準備中です。まだ送信できません。"}
            </p>
          )}
          {!loading && !error && !unavailable && visible.length === 0 && (
            <div className="feedback-list-empty">
              <Inbox size={24} />
              <p>
                {search
                  ? "一致する会話はありません"
                  : filter === "all"
                    ? "ここから会話が始まります"
                    : "この状態の会話はありません"}
              </p>
              {!search && filter === "all" && (
                <span>
                  気づいたことや相談したいことを、
                  <br />
                  そのまま届けてください。
                </span>
              )}
            </div>
          )}
          {visible.map((thread) => (
            <a
              key={thread.id}
              className={`feedback-thread-link${thread.id === location.thread ? " is-selected" : ""}`}
              href={`/feedback?thread=${encodeURIComponent(thread.id)}`}
              aria-current={thread.id === location.thread ? "page" : undefined}
              onClick={(event) => {
                if (
                  event.metaKey ||
                  event.ctrlKey ||
                  event.shiftKey ||
                  event.altKey
                )
                  return;
                event.preventDefault();
                navigate({ thread: thread.id });
              }}
            >
              <div className="feedback-thread-meta">
                <span>{thread.author.display_name}</span>
                <time dateTime={thread.updated_at}>
                  {shortDate(thread.updated_at)}
                </time>
              </div>
              <div className="feedback-thread-title">
                {thread.unread && (
                  <span
                    className="feedback-unread"
                    role="img"
                    aria-label="未読"
                  />
                )}
                <strong>{thread.title}</strong>
                {thread.status === "resolved" && (
                  <Check size={14} aria-label="解決済み" />
                )}
              </div>
              <p>{thread.latest_message?.body ?? thread.body}</p>
            </a>
          ))}
          {cursor && (
            <button
              type="button"
              className="feedback-load-more"
              disabled={moreBusy}
              onClick={() => void more()}
            >
              {moreBusy ? "読み込み中…" : "さらに読み込む"}
              <ChevronDown size={14} />
            </button>
          )}
          {search && (
            <p className="feedback-list-note">
              読み込んだ会話から検索しています。
            </p>
          )}
        </div>
      </aside>
      <main className="feedback-main">
        {location.compose ? (
          <NewThread
            key={`${actor}:new`}
            actor={actor}
            recipient={bootstrap?.recipient_name ?? "Sumi開発"}
            enabled={Boolean(bootstrap?.enabled && bootstrap.available)}
            client={client}
            onBack={() => navigate({})}
            onCreated={(thread) => {
              navigate({ thread: thread.id });
              void refresh(true);
            }}
          />
        ) : location.thread ? (
          <Conversation
            key={`${actor}:${location.thread}`}
            actor={actor}
            id={location.thread}
            client={client}
            recipient={bootstrap?.recipient_name ?? "Sumi開発"}
            onBack={() => navigate({})}
            onChanged={() => void refresh()}
          />
        ) : (
          <section className="feedback-welcome">
            <span className="feedback-welcome-icon">
              <MessageSquarePlus size={26} />
            </span>
            <h2>気づいたことから、話そう。</h2>
            <p>
              うまくいかなかったことも、こうだったらいいなも。
              <br />
              Sumi開発との会話が、ここにまとまります。
            </p>
            <button
              type="button"
              className="feedback-primary"
              disabled={!bootstrap?.available || !bootstrap?.enabled}
              onClick={() => navigate({ compose: true })}
            >
              フィードバックを書く
            </button>
          </section>
        )}
      </main>
    </div>
  );
}
function NewThread({
  actor,
  recipient,
  enabled,
  client,
  onBack,
  onCreated,
}: {
  actor: string;
  recipient: string;
  enabled: boolean;
  client: FeedbackClient;
  onBack(): void;
  onCreated(thread: Thread): void;
}) {
  const key = draftKey(actor, "new");
  const [draft, setDraft] = useState(() => {
    const existing = loadDraft(key);
    if (
      existing.submitted ||
      (existing.diagnostics && (existing.title.trim() || existing.body.trim()))
    )
      return existing;
    let diagnostics: FeedbackDiagnostics | undefined;
    try {
      diagnostics = captureFeedbackDiagnostics();
    } catch {
      /* A broken browser API must not block a report. */
    }
    const next = { ...existing, diagnostics };
    saveDraft(key, next);
    return next;
  });
  const submitted = useRef(Boolean(draft.submitted));
  useEffect(() => {
    if (submitted.current || draft.diagnostics?.served_release) return;
    let active = true;
    void readServedRelease().then((release) => {
      if (!active || submitted.current || !release) return;
      setDraft((previous) => {
        if (submitted.current || !previous.diagnostics) return previous;
        const next = {
          ...previous,
          diagnostics: { ...previous.diagnostics, served_release: release },
        };
        saveDraft(key, next);
        return next;
      });
    });
    return () => {
      active = false;
    };
  }, [key, draft.diagnostics?.served_release]);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const mounted = useRef(true);
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);
  const update = (field: "title" | "body", value: string) => {
    const next = { ...draft, [field]: value, requestId: secureRandomUUID() };
    setDraft(next);
    saveDraft(key, next);
  };
  async function submit() {
    if (busy || !enabled || !draft.title.trim() || !draft.body.trim()) return;
    if ([...draft.title].length > 160 || [...draft.body].length > 20_000) {
      setError("件名は160文字、本文は20,000文字以内で入力してください。");
      return;
    }
    submitted.current = true;
    saveDraft(key, { ...draft, submitted: true });
    setBusy(true);
    setError("");
    try {
      const thread = await client.create(
        draft.title,
        draft.body,
        draft.requestId,
        draft.diagnostics,
      );
      clearDraft(key, draft.requestId);
      if (mounted.current) onCreated(thread);
    } catch (cause) {
      if (mounted.current) setError(errorMessage(cause));
    } finally {
      if (mounted.current) setBusy(false);
    }
  }
  return (
    <>
      <header className="feedback-detail-header">
        <button
          type="button"
          className="feedback-icon-button"
          aria-label="一覧に戻る"
          onClick={onBack}
        >
          <ArrowLeft size={19} />
        </button>
        <span>新しいフィードバック</span>
      </header>
      <form
        className="feedback-new"
        onSubmit={(event) => {
          event.preventDefault();
          void submit();
        }}
      >
        <div className="feedback-new-recipient">
          宛先 <strong>{recipient}</strong>
        </div>
        <label htmlFor="feedback-title" className="feedback-field-label">
          件名
        </label>
        <input
          id="feedback-title"
          className="feedback-title-input"
          placeholder="何について話しましょう？"
          value={draft.title}
          disabled={busy}
          onChange={(event) => update("title", event.target.value)}
          onKeyDown={(event) => {
            if (event.key === "Enter" && isImeComposing(event))
              event.preventDefault();
          }}
        />
        <label htmlFor="feedback-body" className="feedback-field-label">
          内容
        </label>
        <textarea
          id="feedback-body"
          className="feedback-new-body"
          placeholder="気づいたことや相談したいことを自由に。関連するページのリンクも貼れます。"
          value={draft.body}
          disabled={busy}
          onChange={(event) => update("body", event.target.value)}
        />
        {draft.diagnostics ? (
          <Diagnostics details={draft.diagnostics} composing />
        ) : (
          <p className="feedback-diagnostics">
            診断情報を取得できませんでした。本文だけ送信できます。
          </p>
        )}
        <div className="feedback-compose-footer">
          <span>共有先：あなた・{recipient}</span>
          <button
            className="feedback-primary"
            type="submit"
            disabled={
              busy || !enabled || !draft.title.trim() || !draft.body.trim()
            }
          >
            {busy ? "送信中…" : "送信する"}
            <ArrowUp size={16} />
          </button>
        </div>
        {error && <Notice text={error} />}
      </form>
    </>
  );
}
function Conversation({
  actor,
  id,
  client,
  recipient,
  onBack,
  onChanged,
}: {
  actor: string;
  id: string;
  client: FeedbackClient;
  recipient: string;
  onBack(): void;
  onChanged(): void;
}) {
  const [detail, setDetail] = useState<Detail | null>(null);
  const [error, setError] = useState("");
  const [statusBusy, setStatusBusy] = useState(false);
  const [olderBusy, setOlderBusy] = useState(false);
  const [following, setFollowing] = useState(true);
  const scroll = useRef<HTMLDivElement>(null);
  const stick = useRef(true);
  const mounted = useRef(true);
  const sequence = useRef(0);
  const refreshing = useRef(false);
  const readRevision = useRef(0);
  const previousHeight = useRef<number | null>(null);
  const changed = useRef(onChanged);
  changed.current = onChanged;
  const markRead = useCallback(
    async (data: Detail) => {
      if (
        !stick.current ||
        document.visibilityState === "hidden" ||
        data.thread.revision <= readRevision.current
      )
        return;
      const revision = data.thread.revision;
      try {
        await client.read(id, revision);
        if (mounted.current) {
          readRevision.current = Math.max(readRevision.current, revision);
          changed.current();
        }
      } catch {
        /* Retry the same observed boundary on the next refresh. */
      }
    },
    [client, id],
  );
  const refresh = useCallback(
    async (force = false) => {
      if (refreshing.current && !force) return;
      refreshing.current = true;
      const current = ++sequence.current;
      try {
        const latest = await client.open(id);
        if (!mounted.current || current !== sequence.current) return;
        setDetail((previous) => {
          if (!previous) return latest;
          const firstRevision = Math.min(
            ...latest.messages.map((item) => item.revision),
            ...(latest.activities ?? []).map((item) => item.revision),
          );
          return {
            ...latest,
            messages: [
              ...previous.messages.filter(
                (item) => item.revision < firstRevision,
              ),
              ...latest.messages,
            ],
            activities: [
              ...(previous.activities ?? []).filter(
                (item) => item.revision < firstRevision,
              ),
              ...(latest.activities ?? []),
            ],
            next_cursor:
              firstRevision > previous.thread.revision + 1
                ? latest.next_cursor
                : previous.next_cursor,
          };
        });
        setError("");
        void markRead(latest);
      } catch (cause) {
        if (mounted.current && current === sequence.current)
          setError(errorMessage(cause));
      } finally {
        if (current === sequence.current) refreshing.current = false;
      }
    },
    [client, id, markRead],
  );
  useEffect(() => {
    mounted.current = true;
    refreshing.current = false;
    void refresh();
    const timer = window.setInterval(() => {
      if (document.visibilityState === "visible") void refresh();
    }, 10_000);
    const onFocus = () => void refresh();
    window.addEventListener("focus", onFocus);
    return () => {
      mounted.current = false;
      ++sequence.current;
      clearInterval(timer);
      window.removeEventListener("focus", onFocus);
    };
  }, [refresh]);
  // biome-ignore lint/correctness/useExhaustiveDependencies: Reconcile the viewport after the rendered event list changes.
  useLayoutEffect(() => {
    const node = scroll.current;
    if (!node) return;
    if (previousHeight.current !== null) {
      node.scrollTop += node.scrollHeight - previousHeight.current;
      previousHeight.current = null;
    } else if (stick.current) node.scrollTop = node.scrollHeight;
  }, [detail]);
  async function loadOlder() {
    if (!detail?.next_cursor || olderBusy) return;
    setOlderBusy(true);
    try {
      const older = await client.open(id, detail.next_cursor);
      if (!mounted.current) return;
      previousHeight.current = scroll.current?.scrollHeight ?? null;
      setDetail((current) =>
        current
          ? {
              ...current,
              messages: uniqueById([...older.messages, ...current.messages]),
              activities: uniqueById([
                ...(older.activities ?? []),
                ...(current.activities ?? []),
              ]),
              next_cursor: older.next_cursor,
            }
          : older,
      );
    } catch (cause) {
      if (mounted.current) setError(errorMessage(cause));
    } finally {
      if (mounted.current) setOlderBusy(false);
    }
  }
  async function changeStatus() {
    if (!detail || statusBusy) return;
    setStatusBusy(true);
    try {
      await client.status(
        id,
        detail.thread.status === "open" ? "resolved" : "open",
        detail.thread.revision,
      );
      await refresh(true);
      changed.current();
    } catch (cause) {
      await refresh(true);
      if (mounted.current) setError(errorMessage(cause));
    } finally {
      if (mounted.current) setStatusBusy(false);
    }
  }
  const entries = detail
    ? [
        ...detail.messages.map((message) => ({
          ...message,
          kind: "message" as const,
        })),
        ...(detail.activities ?? []).map((activity) => ({
          ...activity,
          kind: "status" as const,
        })),
      ].sort((a, b) => a.revision - b.revision)
    : [];
  const gapIndex = entries.findIndex(
    (entry, index) =>
      index > 0 && entry.revision > entries[index - 1].revision + 1,
  );
  return (
    <>
      <header className="feedback-detail-header">
        <button
          type="button"
          className="feedback-icon-button feedback-mobile-back"
          aria-label="一覧に戻る"
          onClick={onBack}
        >
          <ArrowLeft size={19} />
        </button>
        <div className="feedback-detail-heading">
          <strong>{detail?.thread.title ?? "会話"}</strong>
          <span>
            {recipient}
            {detail?.thread.status === "resolved" ? " · 解決済み" : ""}
          </span>
        </div>
        {detail && (
          <button
            type="button"
            className="feedback-status-button"
            disabled={statusBusy}
            onClick={() => void changeStatus()}
          >
            {detail.thread.status === "open" ? (
              <Check size={15} />
            ) : (
              <RotateCcw size={14} />
            )}
            <span>
              {detail.thread.status === "open" ? "解決にする" : "再開する"}
            </span>
          </button>
        )}
      </header>
      <div
        ref={scroll}
        className="feedback-conversation"
        onScroll={() => {
          const node = scroll.current;
          if (!node) return;
          const near =
            node.scrollHeight - node.scrollTop - node.clientHeight < 64;
          stick.current = near;
          setFollowing(near);
          if (near && detail) void markRead(detail);
        }}
      >
        {!detail && !error && (
          <p className="feedback-list-note" role="status">
            会話を読み込んでいます…
          </p>
        )}
        {detail && (
          <div className="feedback-messages">
            <article className="feedback-original">
              <h2>{detail.thread.title}</h2>
              <AuthorLine
                author={detail.thread.author}
                time={detail.thread.created_at}
              />
              <MessageBody>{detail.thread.body}</MessageBody>
              {detail.thread.diagnostics && (
                <Diagnostics details={detail.thread.diagnostics} />
              )}
            </article>
            {detail.next_cursor && gapIndex === -1 && (
              <button
                type="button"
                className="feedback-older"
                disabled={olderBusy}
                onClick={() => void loadOlder()}
              >
                {olderBusy ? "読み込み中…" : "前のやりとりを読む"}
              </button>
            )}
            {entries.map((entry, index) => (
              <Fragment key={entry.id}>
                {index === gapIndex && detail.next_cursor && (
                  <button
                    type="button"
                    className="feedback-older"
                    disabled={olderBusy}
                    onClick={() => void loadOlder()}
                  >
                    {olderBusy ? "読み込み中…" : "間のやりとりを読む"}
                  </button>
                )}
                {entry.kind === "message" ? (
                  <article key={entry.id} className="feedback-message">
                    <AuthorLine author={entry.author} time={entry.created_at} />
                    <MessageBody>{entry.body}</MessageBody>
                  </article>
                ) : (
                  <p key={entry.id} className="feedback-status-event">
                    {entry.status === "resolved" ? (
                      <Check size={14} />
                    ) : (
                      <RotateCcw size={13} />
                    )}
                    <span>
                      {entry.author.display_name}が
                      {entry.status === "resolved"
                        ? "解決済みにしました"
                        : "会話を再開しました"}
                    </span>
                    <time dateTime={entry.created_at}>
                      {shortDate(entry.created_at)}
                    </time>
                  </p>
                )}
              </Fragment>
            ))}
          </div>
        )}
      </div>
      {!following && (
        <button
          type="button"
          className="feedback-jump"
          onClick={() => {
            stick.current = true;
            setFollowing(true);
            scroll.current?.scrollTo({
              top: scroll.current.scrollHeight,
              behavior: "instant",
            });
            if (detail) void markRead(detail);
          }}
        >
          <ArrowDown size={14} />
          最新へ
        </button>
      )}
      <div className="feedback-bottom">
        {error && <Notice text={error} onRetry={() => void refresh(true)} />}
        {detail && (
          <ReplyComposer
            actor={actor}
            id={id}
            client={client}
            onSent={async () => {
              stick.current = true;
              setFollowing(true);
              await refresh(true);
              changed.current();
            }}
          />
        )}
      </div>
    </>
  );
}
function ReplyComposer({
  actor,
  id,
  client,
  onSent,
}: {
  actor: string;
  id: string;
  client: FeedbackClient;
  onSent(): Promise<void>;
}) {
  const key = draftKey(actor, id);
  const [draft, setDraft] = useState(() => loadDraft(key));
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const mounted = useRef(true);
  const input = useRef<HTMLTextAreaElement>(null);
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);
  // biome-ignore lint/correctness/useExhaustiveDependencies: Measure the textarea after its controlled content changes.
  useLayoutEffect(() => {
    const node = input.current;
    if (node) {
      node.style.height = "auto";
      node.style.height = `${Math.min(node.scrollHeight, 160)}px`;
    }
  }, [draft.body]);
  async function submit() {
    if (busy || !draft.body.trim()) return;
    if ([...draft.body].length > 20_000) {
      setError("20,000文字以内で入力してください。");
      return;
    }
    saveDraft(key, draft);
    setBusy(true);
    setError("");
    try {
      await client.reply(id, draft.body, draft.requestId);
      clearDraft(key, draft.requestId);
      if (mounted.current) {
        setDraft(loadDraft(key));
        await onSent();
        input.current?.focus();
      }
    } catch (cause) {
      if (mounted.current) setError(errorMessage(cause));
    } finally {
      if (mounted.current) setBusy(false);
    }
  }
  return (
    <>
      <form
        className="feedback-reply"
        onSubmit={(event) => {
          event.preventDefault();
          void submit();
        }}
      >
        <textarea
          ref={input}
          rows={1}
          aria-label="返信"
          placeholder="返信する…"
          value={draft.body}
          disabled={busy}
          onChange={(event) => {
            const next = {
              ...draft,
              body: event.target.value,
              requestId: secureRandomUUID(),
            };
            setDraft(next);
            saveDraft(key, next);
          }}
          onKeyDown={(event) => {
            if (
              event.key === "Enter" &&
              (event.metaKey || event.ctrlKey) &&
              !isImeComposing(event)
            ) {
              event.preventDefault();
              void submit();
            }
          }}
        />
        <button
          className="feedback-send"
          type="submit"
          aria-label={busy ? "返信を送信中" : "返信を送信"}
          disabled={busy || !draft.body.trim()}
        >
          <ArrowUp size={19} />
        </button>
      </form>
      <div className="feedback-reply-hint">
        改行はEnter · 送信は⌘ / Ctrl + Enter
      </div>
      {error && <Notice text={error} />}
    </>
  );
}
function AuthorLine({ author, time }: { author: Author; time: string }) {
  return (
    <div className="feedback-author">
      <span className="feedback-avatar" aria-hidden="true">
        {Array.from(author.display_name)[0] ?? "?"}
      </span>
      <strong>{author.display_name}</strong>
      <time dateTime={time} title={new Date(time).toLocaleString("ja-JP")}>
        {shortDate(time, true)}
      </time>
    </div>
  );
}
function MessageBody({ children }: { children: string }) {
  return (
    <div className="feedback-message-body">
      <CompactMessageResponse>{children}</CompactMessageResponse>
    </div>
  );
}
function Notice({ text, onRetry }: { text: string; onRetry?: () => void }) {
  return (
    <div className="feedback-notice" role="alert">
      <span>{text}</span>
      {onRetry && (
        <button type="button" onClick={onRetry}>
          再試行
        </button>
      )}
    </div>
  );
}
function uniqueById<T extends { id: string }>(items: T[]): T[] {
  return [...new Map(items.map((item) => [item.id, item])).values()];
}
function shortDate(value: string, includeTime = false) {
  const date = new Date(value);
  const today = new Date();
  return date.toLocaleString("ja-JP", {
    ...(date.toDateString() !== today.toDateString()
      ? {
          month: "numeric",
          day: "numeric",
          ...(date.getFullYear() !== today.getFullYear()
            ? { year: "numeric" as const }
            : {}),
        }
      : {}),
    ...(includeTime || date.toDateString() === today.toDateString()
      ? { hour: "2-digit", minute: "2-digit" }
      : {}),
  });
}

function Diagnostics({
  details,
  composing = false,
}: {
  details: FeedbackDiagnostics;
  composing?: boolean;
}) {
  return (
    <details className="feedback-diagnostics">
      <summary>
        {composing ? "診断情報を自動添付します" : "添付された診断情報"}
      </summary>
      <p>
        開いていた画面・接続状態・ブラウザ環境を共有します。会話本文や認証情報は含みません。
      </p>
      <pre>{JSON.stringify(details, null, 2)}</pre>
    </details>
  );
}
