import { Clock, Hash, Pencil, Users } from "lucide-react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { AppRail } from "../../shell/app-rail";
import { type PlaceKey, participantKey, type ReplyLaterMarker } from "../model";
import { usePlaceNavigate } from "../place-route";
import { useMessaging } from "../store";
import { usePlaceDisplay } from "../use-place-name";
import { Composer } from "./composer";
import { ConnectionBanner } from "./connection-banner";
import { MemberList } from "./member-list";
import { MessageList, type MessageListHandle } from "./message-list";
import { Sidebar } from "./sidebar";

interface PendingJump {
  placeKey: PlaceKey;
  messageId?: string;
  seq?: number;
}

function relativeTime(target: number, now: number): string {
  const delta = target - now;
  const minutes = Math.round(Math.abs(delta) / 60_000);
  if (delta <= 0) return "リマインド中";
  if (minutes < 1) return "まもなく";
  if (minutes < 60) return `${minutes}分後`;
  return `${Math.round(minutes / 60)}時間後`;
}

/**
 * ヘッダーのchannelトピック。クリックでその場のinputになり、Enterで保存、
 * Escapeで破棄する。権限はサーバーのmembershipモデルに従う（v0: workspaceの
 * activeメンバーなら誰でも編集できる）。
 */
function ChannelTopic({
  channelId,
  topic,
}: {
  channelId: string;
  topic: string;
}) {
  const updateChannelTopic = useMessaging((state) => state.updateChannelTopic);
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState(topic);
  const [busy, setBusy] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (editing) inputRef.current?.focus();
  }, [editing]);

  if (!editing) {
    return (
      <button
        type="button"
        title="トピックを編集"
        onClick={() => {
          setDraft(topic);
          setEditing(true);
        }}
        className="group flex min-w-0 items-center gap-2 text-left"
      >
        <span className="h-4 w-px shrink-0 bg-border" />
        {topic ? (
          <span className="truncate text-[12px] text-muted-foreground group-hover:text-foreground">
            {topic}
          </span>
        ) : (
          <span className="shrink-0 text-[12px] text-muted-foreground/60 group-hover:text-foreground">
            トピックを追加
          </span>
        )}
        <Pencil className="size-3 shrink-0 text-muted-foreground opacity-0 transition-opacity group-hover:opacity-100" />
      </button>
    );
  }

  const submit = async () => {
    if (busy) return;
    setBusy(true);
    try {
      await updateChannelTopic(channelId, draft.trim());
      setEditing(false);
    } catch {
      // 保存できなかったときは入力を失わず編集中のまま残す。
    } finally {
      setBusy(false);
    }
  };

  return (
    <span className="flex min-w-0 items-center gap-2">
      <span className="h-4 w-px shrink-0 bg-border" />
      <input
        ref={inputRef}
        value={draft}
        disabled={busy}
        maxLength={200}
        onChange={(event) => setDraft(event.target.value)}
        onKeyDown={(event) => {
          if (event.key === "Enter") {
            event.preventDefault();
            void submit();
          } else if (event.key === "Escape") {
            setEditing(false);
          }
        }}
        onBlur={() => {
          if (!busy) setEditing(false);
        }}
        placeholder="トピックを設定"
        className="w-72 max-w-full rounded-md border border-border bg-background px-2 py-0.5 text-[12px] outline-none focus-visible:border-ring/60 disabled:opacity-50"
      />
    </span>
  );
}

function TypingIndicator() {
  const activePlaceKey = useMessaging((state) => state.activePlaceKey);
  const typingByPlace = useMessaging((state) => state.typingByPlace);
  const membersByKey = useMessaging((state) => state.membersByKey);
  const [now, setNow] = useState(() => Date.now());

  const entries = activePlaceKey ? (typingByPlace[activePlaceKey] ?? {}) : {};
  const active = Object.entries(entries).filter(
    ([, expiresAt]) => expiresAt > now,
  );

  useEffect(() => {
    if (active.length === 0) return;
    const timer = window.setInterval(() => setNow(Date.now()), 1_000);
    return () => window.clearInterval(timer);
  }, [active.length]);

  const names = active
    .map(([key]) => membersByKey[key]?.displayName ?? "誰か")
    .join("、");

  return (
    <div className="h-5 shrink-0 px-4 sm:px-6">
      {names ? (
        <span className="text-[11px] text-muted-foreground">
          <span className="font-medium">{names}</span> が入力中…
        </span>
      ) : null}
    </div>
  );
}

function ReplyLaterMenu({ onJump }: { onJump: (jump: PendingJump) => void }) {
  const replyLaterById = useMessaging((state) => state.replyLaterById);
  const selfKey = useMessaging((state) => state.selfKey);
  const resolveReplyLater = useMessaging((state) => state.resolveReplyLater);
  const [open, setOpen] = useState(false);
  const [now, setNow] = useState(() => Date.now());

  // リマインドの予定が入っているのは本人のmarkerだけ。相手の「後で返信します」は
  // messageの側に見えていればよく、こちらのknock対象にはならない。
  const markers = useMemo(
    () =>
      Object.values(replyLaterById)
        .filter(
          (marker): marker is ReplyLaterMarker & { remindAt: number } =>
            !marker.resolved &&
            participantKey(marker.participant) === selfKey &&
            marker.remindAt !== null,
        )
        .sort((a, b) => a.remindAt - b.remindAt),
    [replyLaterById, selfKey],
  );
  const dueCount = markers.filter((marker) => marker.remindAt <= now).length;

  useEffect(() => {
    if (markers.length === 0) return;
    const timer = window.setInterval(() => setNow(Date.now()), 30_000);
    return () => window.clearInterval(timer);
  }, [markers.length]);

  return (
    <div className="relative">
      <button
        type="button"
        onClick={() => setOpen((value) => !value)}
        title="後で返信"
        className={`relative flex size-8 items-center justify-center rounded-md transition-colors hover:bg-accent ${
          open ? "bg-accent text-foreground" : "text-muted-foreground"
        }`}
      >
        <Clock className="size-4" />
        {markers.length > 0 ? (
          <span
            className={`absolute -top-0.5 -right-0.5 flex size-4 items-center justify-center rounded-full font-semibold text-[9px] text-white ${
              dueCount > 0 ? "bg-rose-500" : "bg-muted-foreground"
            }`}
          >
            {markers.length}
          </span>
        ) : null}
      </button>
      {open ? (
        <div className="absolute top-full right-0 z-20 mt-1 w-72 rounded-lg border border-border bg-background p-1 shadow-md">
          <p className="px-2 pt-1.5 pb-1 font-medium text-[11px] text-muted-foreground">
            後で返信 — 忘れないように knock します
          </p>
          {markers.length === 0 ? (
            <p className="px-2 pb-2 text-[12px] text-muted-foreground/70">
              予約はありません
            </p>
          ) : (
            markers.map((marker) => {
              const due = marker.remindAt <= now;
              return (
                <div
                  key={marker.markerId}
                  className="flex items-center gap-2 rounded-md px-2 py-1.5 hover:bg-accent/60"
                >
                  <button
                    type="button"
                    onClick={() => {
                      setOpen(false);
                      onJump({
                        placeKey: `${marker.place.kind}:${
                          marker.place.kind === "channel"
                            ? marker.place.channelId
                            : marker.place.dmId
                        }`,
                        messageId: marker.messageId,
                      });
                    }}
                    className="min-w-0 flex-1 text-left"
                  >
                    <span className="block truncate text-[12.5px]">
                      {marker.note}
                    </span>
                    <span
                      className={`block text-[11px] ${
                        due
                          ? "font-medium text-rose-500"
                          : "text-muted-foreground"
                      }`}
                    >
                      {relativeTime(marker.remindAt, now)}
                    </span>
                  </button>
                  <button
                    type="button"
                    onClick={() => resolveReplyLater(marker.markerId)}
                    className="shrink-0 rounded px-1.5 py-0.5 text-[11px] text-muted-foreground hover:bg-accent hover:text-foreground"
                  >
                    完了
                  </button>
                </div>
              );
            })
          )}
        </div>
      ) : null}
    </div>
  );
}

function ReplyLaterKnock({ onJump }: { onJump: (jump: PendingJump) => void }) {
  const replyLaterById = useMessaging((state) => state.replyLaterById);
  const selfKey = useMessaging((state) => state.selfKey);
  const resolveReplyLater = useMessaging((state) => state.resolveReplyLater);
  const [now, setNow] = useState(() => Date.now());
  const [dismissed, setDismissed] = useState<Record<string, boolean>>({});

  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 5_000);
    return () => window.clearInterval(timer);
  }, []);

  const due = Object.values(replyLaterById).filter(
    (marker) =>
      !marker.resolved &&
      participantKey(marker.participant) === selfKey &&
      marker.remindAt !== null &&
      marker.remindAt <= now &&
      !dismissed[marker.markerId],
  );
  const marker = due[0];
  if (!marker) return null;

  return (
    <div className="fixed right-4 bottom-4 z-30 w-80 rounded-xl border border-border bg-background p-3 shadow-lg">
      <p className="flex items-center gap-1.5 font-medium text-[13px]">
        <Clock className="size-3.5 text-rose-500" />
        後で返信の時間です
      </p>
      <p className="mt-1 truncate text-[12.5px] text-muted-foreground">
        {marker.note}
      </p>
      <div className="mt-2 flex items-center gap-1.5">
        <button
          type="button"
          onClick={() =>
            onJump({
              placeKey: `${marker.place.kind}:${
                marker.place.kind === "channel"
                  ? marker.place.channelId
                  : marker.place.dmId
              }`,
              messageId: marker.messageId,
            })
          }
          className="rounded-md bg-primary px-2.5 py-1 font-medium text-[12px] text-primary-foreground hover:opacity-90"
        >
          移動して返信
        </button>
        <button
          type="button"
          onClick={() => resolveReplyLater(marker.markerId)}
          className="rounded-md px-2 py-1 text-[12px] text-muted-foreground hover:bg-accent"
        >
          完了にする
        </button>
        <button
          type="button"
          onClick={() =>
            setDismissed((entry) => ({ ...entry, [marker.markerId]: true }))
          }
          className="ml-auto rounded-md px-2 py-1 text-[12px] text-muted-foreground hover:bg-accent"
        >
          閉じる
        </button>
      </div>
    </div>
  );
}

export function MessagingScreen({ placeKey }: { placeKey?: PlaceKey }) {
  const init = useMessaging((state) => state.init);
  const ready = useMessaging((state) => state.ready);
  const canReplyLater = useMessaging((state) => state.capabilities.replyLater);
  const activePlaceKey = useMessaging((state) => state.activePlaceKey);
  const selectPlace = useMessaging((state) => state.selectPlace);
  const placeNavigate = usePlaceNavigate();
  const loadPlaceAround = useMessaging((state) => state.loadPlaceAround);
  const messagesByPlace = useMessaging((state) => state.messagesByPlace);
  const display = usePlaceDisplay(activePlaceKey);
  const unreadCountByPlace = useMessaging((state) => state.unreadCountByPlace);
  const mentionCountByPlace = useMessaging(
    (state) => state.mentionCountByPlace,
  );
  const channels = useMessaging((state) => state.channels);
  const dms = useMessaging((state) => state.dms);
  const listRef = useRef<MessageListHandle>(null);
  const [membersOpen, setMembersOpen] = useState(true);
  const [pendingJump, setPendingJump] = useState<PendingJump | null>(null);

  useEffect(() => {
    init();
  }, [init]);

  // URLが現在地の正本。route paramのplaceをstoreへ同期する。
  // selectPlaceは未知のplaceを黙って捨てるので、place一覧が変わったら
  // （live event・再接続後の突き合わせ）もう一度同期し直す。
  // biome-ignore lint/correctness/useExhaustiveDependencies: place一覧の変化を選び直しのトリガーにする
  useEffect(() => {
    if (!ready || !placeKey) return;
    if (placeKey !== activePlaceKey) selectPlace(placeKey);
  }, [ready, placeKey, activePlaceKey, selectPlace, channels, dms]);

  // タブタイトルへ未読を集約する。ウィンドウが裏にあっても件数が見える。
  useEffect(() => {
    let unread = 0;
    for (const [key, count] of Object.entries(unreadCountByPlace)) {
      unread +=
        key.startsWith("dm:") || key.startsWith("group_dm:")
          ? count
          : (mentionCountByPlace[key] ?? 0);
    }
    document.title = unread > 0 ? `(${unread}) Sumi` : "Sumi";
  }, [unreadCountByPlace, mentionCountByPlace]);

  // permalink（/c/:id?m=seq）で開かれたら該当メッセージへジャンプする。
  useEffect(() => {
    if (!ready || !placeKey) return;
    const params = new URLSearchParams(window.location.search);
    const rawSeq = params.get("m");
    const seq = rawSeq === null ? null : Number(rawSeq);
    if (seq !== null && Number.isSafeInteger(seq) && seq > 0) {
      setPendingJump({ placeKey, seq });
      void loadPlaceAround(placeKey, seq);
      window.history.replaceState(null, "", window.location.pathname);
    }
  }, [ready, placeKey, loadPlaceAround]);

  const requestJump = useCallback(
    (jump: PendingJump) => {
      placeNavigate(jump.placeKey);
      setPendingJump(jump);
    },
    [placeNavigate],
  );

  // 対象placeのメッセージが手元に揃った時点でジャンプを実行する。
  useEffect(() => {
    if (!pendingJump) return;
    if (activePlaceKey !== pendingJump.placeKey) return;
    const messages = messagesByPlace[pendingJump.placeKey];
    if (!messages) return;
    const targetAvailable = pendingJump.messageId
      ? messages.some((message) => message.messageId === pendingJump.messageId)
      : pendingJump.seq !== undefined
        ? messages.some((message) => message.seq === pendingJump.seq)
        : false;
    if (!targetAvailable) return;
    const frame = window.requestAnimationFrame(() => {
      if (pendingJump.messageId) {
        listRef.current?.jumpToMessage(pendingJump.messageId);
      } else if (pendingJump.seq !== undefined) {
        listRef.current?.jumpToSeq(pendingJump.seq);
      }
      setPendingJump(null);
    });
    return () => window.cancelAnimationFrame(frame);
  }, [pendingJump, activePlaceKey, messagesByPlace]);

  if (!ready) {
    return (
      <div className="flex h-dvh items-center justify-center bg-background text-muted-foreground text-sm">
        読み込み中…
      </div>
    );
  }

  return (
    <div className="flex h-dvh bg-background text-foreground">
      <AppRail activeAppId="home" />
      <Sidebar />
      {/* ヘッダーはコンテンツ列の全幅に固定し、メンバーパネルはその下で開閉する。
          開閉でヘッダー内のボタンが動かないための構造（ポインタの下でUIを動かさない）。 */}
      <div className="flex min-w-0 flex-1 flex-col">
        <header className="flex h-12 shrink-0 items-center gap-2 border-border/70 border-b px-4 sm:px-5">
          {display?.kind === "channel" ? (
            <Hash className="size-4 shrink-0 text-muted-foreground" />
          ) : null}
          <span className="truncate font-semibold text-[14.5px]">
            {display?.name ?? ""}
          </span>
          {display?.kind === "channel" && activePlaceKey ? (
            <ChannelTopic
              channelId={activePlaceKey.slice("channel:".length)}
              topic={display.topic}
            />
          ) : display?.topic ? (
            <>
              <span className="h-4 w-px shrink-0 bg-border" />
              <span className="truncate text-[12px] text-muted-foreground">
                {display.topic}
              </span>
            </>
          ) : null}
          <span className="ml-auto flex items-center gap-1">
            {canReplyLater ? <ReplyLaterMenu onJump={requestJump} /> : null}
            <button
              type="button"
              title="メンバーリスト"
              onClick={() => setMembersOpen((value) => !value)}
              className={`flex size-8 items-center justify-center rounded-md transition-colors hover:bg-accent ${
                membersOpen ? "text-foreground" : "text-muted-foreground"
              }`}
            >
              <Users className="size-4" />
            </button>
          </span>
        </header>
        <ConnectionBanner />
        <div className="flex min-h-0 flex-1">
          <main className="flex min-w-0 flex-1 flex-col">
            <MessageList handleRef={listRef} />
            <TypingIndicator />
            <Composer />
          </main>
          {membersOpen ? <MemberList /> : null}
        </div>
      </div>
      {canReplyLater ? <ReplyLaterKnock onJump={requestJump} /> : null}
    </div>
  );
}
