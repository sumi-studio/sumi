import { Bell, Clock, Hash, Users, X } from "lucide-react";
import {
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { CallBanner } from "../call/call-banner";
import { CallFailureNotice } from "../call/call-failure-notice";
import { CallStage } from "../call/call-stage";
import { CallStartButtons } from "../call/call-start-buttons";
import { IncomingCall } from "../call/incoming-call";
import { type PlaceKey, participantKey, type ReplyLaterMarker } from "../model";
import {
  dismissPermissionPrompt,
  isPermissionPromptDismissed,
  type NotificationPermissionState,
  notificationCountForPlace,
  notificationPermission,
  requestNotificationPermission,
} from "../notifications";
import { usePlaceNavigate } from "../place-route";
import { enablePushSubscription, isPushSupported } from "../push";
import { getMessagingScope, useMessaging } from "../store";
import { usePlaceDisplay } from "../use-place-name";
import { Composer } from "./composer";
import { ConnectionBanner } from "./connection-banner";
import { ImageViewer } from "./image-viewer";
import { MemberList } from "./member-list";
import type { ImageViewerRequest } from "./message-attachments";
import { MessageList, type MessageListHandle } from "./message-list";
import { MessageSearch } from "./message-search";
import { NotificationSettingsMenu } from "./notification-settings";
import { useOverlayPanel, useWheelPassthrough } from "./overlay";
import { Sidebar } from "./sidebar";

interface PendingJump {
  placeKey: PlaceKey;
  messageId?: string;
  seq?: number;
}

interface ViewingImage {
  placeKey: PlaceKey;
  request: ImageViewerRequest;
}

const NO_REVEALED_ATTACHMENTS: ReadonlySet<string> = new Set();

function relativeTime(target: number, now: number): string {
  const delta = target - now;
  const minutes = Math.round(Math.abs(delta) / 60_000);
  if (delta <= 0) return "リマインド中";
  if (minutes < 1) return "まもなく";
  if (minutes < 60) return `${minutes}分後`;
  return `${Math.round(minutes / 60)}時間後`;
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

/**
 * 通知許可を求める導線。ブラウザのダイアログは一度断られると出し直せないので、
 * 押されるまで待つ控えめなバナーにしておく。閉じたら二度と出さない。
 */
function NotificationPermissionBanner() {
  const enabled = useMessaging((state) => state.capabilities.notifications);
  const [permission, setPermission] = useState<NotificationPermissionState>(
    () => notificationPermission(),
  );
  const [dismissed, setDismissed] = useState(() =>
    isPermissionPromptDismissed(),
  );

  if (!enabled || !isPushSupported() || dismissed || permission !== "default") {
    return null;
  }

  return (
    <div className="flex shrink-0 items-center gap-2 border-border/70 border-b bg-accent/40 px-4 py-1.5 sm:px-5">
      <Bell className="size-3.5 shrink-0 text-muted-foreground" />
      <span className="min-w-0 flex-1 truncate text-[12px] text-muted-foreground">
        呼ばれたときだけ通知します。ブラウザの通知を許可しますか？
      </span>
      <button
        type="button"
        onClick={() => {
          void requestNotificationPermission().then((next) => {
            setPermission(next);
            if (next === "granted") void enablePushSubscription();
          });
        }}
        className="shrink-0 rounded-md bg-primary px-2 py-0.5 font-medium text-[12px] text-primary-foreground hover:opacity-90"
      >
        許可する
      </button>
      <button
        type="button"
        aria-label="通知の案内を閉じる"
        onClick={() => {
          dismissPermissionPrompt();
          setDismissed(true);
        }}
        className="shrink-0 rounded p-0.5 text-muted-foreground hover:bg-accent hover:text-foreground"
      >
        <X className="size-3.5" />
      </button>
    </div>
  );
}

function ReplyLaterMenu({ onJump }: { onJump: (jump: PendingJump) => void }) {
  const replyLaterById = useMessaging((state) => state.replyLaterById);
  const selfKey = useMessaging((state) => state.selfKey);
  const resolveReplyLater = useMessaging((state) => state.resolveReplyLater);
  const [open, setOpen] = useState(false);
  const [now, setNow] = useState(() => Date.now());
  const overlay = useOverlayPanel<HTMLButtonElement>({
    open,
    onOpenChange: setOpen,
  });

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
        title="後で返信"
        aria-haspopup="dialog"
        {...overlay.triggerProps}
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
        <div
          {...overlay.panelProps}
          role="dialog"
          aria-label="後で返信"
          className="absolute top-full right-0 z-20 mt-1 w-72 rounded-lg border border-border bg-background p-1 shadow-md"
        >
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
  const passthroughRef = useWheelPassthrough<HTMLDivElement>();

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
    <div
      ref={passthroughRef}
      className="fixed right-4 bottom-4 z-30 w-80 rounded-xl border border-border bg-background p-3 shadow-lg"
    >
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
  const ready = useMessaging((state) => state.ready);
  const canReplyLater = useMessaging((state) => state.capabilities.replyLater);
  const activePlaceKey = useMessaging((state) => state.activePlaceKey);
  const selectPlace = useMessaging((state) => state.selectPlace);
  const clearPlaceSelection = useMessaging(
    (state) => state.clearPlaceSelection,
  );
  const placeNavigate = usePlaceNavigate();
  const loadPlaceAround = useMessaging((state) => state.loadPlaceAround);
  const messagesByPlace = useMessaging((state) => state.messagesByPlace);
  const unreadCountByPlace = useMessaging((state) => state.unreadCountByPlace);
  const mentionCountByPlace = useMessaging(
    (state) => state.mentionCountByPlace,
  );
  const channels = useMessaging((state) => state.channels);
  const dms = useMessaging((state) => state.dms);
  const workspaces = useMessaging((state) => state.workspaces);
  const selectedPlaceKey =
    placeKey &&
    (channels.some((channel) => placeKey === `channel:${channel.channelId}`) ||
      dms.some((dm) => placeKey === `${dm.kind}:${dm.dmId}`))
      ? placeKey
      : null;
  const display = usePlaceDisplay(selectedPlaceKey);
  const canNotify = useMessaging((state) => state.capabilities.notifications);
  const notificationLevelByPlace = useMessaging(
    (state) => state.notificationLevelByPlace,
  );
  const notificationDefaultLevel = useMessaging(
    (state) => state.notificationDefaultLevel,
  );
  const listRef = useRef<MessageListHandle>(null);
  const [membersOpen, setMembersOpen] = useState(true);
  const [pendingJump, setPendingJump] = useState<PendingJump | null>(null);
  const [viewingImage, setViewingImage] = useState<ViewingImage | null>(null);
  const [revealedForPlace, setRevealedForPlace] = useState<{
    placeKey: PlaceKey | null;
    attachmentIds: ReadonlySet<string>;
  }>({ placeKey: null, attachmentIds: new Set() });
  const attachmentURL = useMessaging((state) => state.attachmentURL);

  const revealAttachment = useCallback(
    (attachmentId: string) => {
      if (!selectedPlaceKey) return;
      setRevealedForPlace((current) => {
        const attachmentIds =
          current.placeKey === selectedPlaceKey
            ? current.attachmentIds
            : new Set<string>();
        if (attachmentIds.has(attachmentId)) return current;
        return {
          placeKey: selectedPlaceKey,
          attachmentIds: new Set(attachmentIds).add(attachmentId),
        };
      });
    },
    [selectedPlaceKey],
  );

  const openImage = useCallback(
    (request: ImageViewerRequest) => {
      if (selectedPlaceKey)
        setViewingImage({ placeKey: selectedPlaceKey, request });
    },
    [selectedPlaceKey],
  );
  const revealedAttachmentIds =
    revealedForPlace.placeKey === selectedPlaceKey
      ? revealedForPlace.attachmentIds
      : NO_REVEALED_ATTACHMENTS;

  // URLが現在地の正本。route paramのplaceをstoreへ同期する。
  // homeまたはbootstrapに存在しないplace URLは「未選択」が正本。表示だけを
  // 隠すのではなくcurrent placeを解除し、通知判定や編集状態にも同じ現在地を渡す。
  useLayoutEffect(() => {
    if (!ready) return;
    if (!selectedPlaceKey) {
      clearPlaceSelection();
      setPendingJump(null);
      return;
    }
    if (selectedPlaceKey !== activePlaceKey) selectPlace(selectedPlaceKey);
  }, [
    ready,
    selectedPlaceKey,
    activePlaceKey,
    selectPlace,
    clearPlaceSelection,
  ]);

  // 開示とビューアーはこのplaceを見ている画面だけの状態。履歴行の仮想化では
  // 忘れず、placeを離れたら永続化せずに捨てる。
  useEffect(() => {
    if (viewingImage && viewingImage.placeKey !== selectedPlaceKey) {
      setViewingImage(null);
    }
    if (revealedForPlace.placeKey !== selectedPlaceKey) {
      setRevealedForPlace({
        placeKey: selectedPlaceKey ?? null,
        attachmentIds: new Set(),
      });
    }
  }, [selectedPlaceKey, viewingImage, revealedForPlace]);

  // タブタイトルへ未読を集約する。ウィンドウが裏にあっても件数が見える。
  // muteしたplaceはsidebar badgeと同じく外す。level=allのchannelは全未読、
  // mentionsはmention未読だけを数え、「呼ばれている数」を表示する。
  useEffect(() => {
    let unread = 0;
    for (const [key, count] of Object.entries(unreadCountByPlace)) {
      const level = notificationLevelByPlace[key] ?? notificationDefaultLevel;
      unread += notificationCountForPlace(
        key,
        level,
        count,
        mentionCountByPlace[key] ?? 0,
      );
    }
    document.title = unread > 0 ? `(${unread}) Sumi` : "Sumi";
  }, [
    unreadCountByPlace,
    mentionCountByPlace,
    notificationLevelByPlace,
    notificationDefaultLevel,
  ]);

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
      if (jump.seq !== undefined) {
        void loadPlaceAround(jump.placeKey, jump.seq);
      }
    },
    [placeNavigate, loadPlaceAround],
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
      <div className="flex h-full items-center justify-center bg-background text-muted-foreground text-sm">
        読み込み中…
      </div>
    );
  }

  const hasSelectablePlace = channels.length > 0 || dms.length > 0;
  const selectedPlaceIsLoaded =
    selectedPlaceKey !== null && activePlaceKey === selectedPlaceKey;
  const selectedChannel = selectedPlaceKey?.startsWith("channel:")
    ? channels.find(
        (channel) => `channel:${channel.channelId}` === selectedPlaceKey,
      )
    : undefined;

  return (
    <div className="flex h-full bg-background text-foreground">
      <Sidebar
        selectedPlaceKey={selectedPlaceKey}
        workspaceId={getMessagingScope()?.workspaceId ?? null}
      />
      {/* ヘッダーはコンテンツ列の全幅に固定し、メンバーパネルはその下で開閉する。
          開閉でヘッダー内のボタンが動かないための構造（ポインタの下でUIを動かさない）。 */}
      <div className="flex min-w-0 flex-1 flex-col">
        <NotificationPermissionBanner />
        <header className="flex h-12 shrink-0 items-center gap-2 border-border/70 border-b px-4 sm:px-5">
          {display?.kind === "channel" ? (
            <Hash className="size-4 shrink-0 text-muted-foreground" />
          ) : null}
          <span className="truncate font-semibold text-[14.5px]">
            {display?.name ?? "メッセージ"}
          </span>
          {/* トピックの編集はサイドバーの place メニューへ移した。ヘッダーは
              いま居る場所を示すだけで、押せるものを置かない。 */}
          {display?.topic ? (
            <>
              <span className="h-4 w-px shrink-0 bg-border" />
              <span className="truncate text-[12px] text-muted-foreground">
                {display.topic}
              </span>
            </>
          ) : null}
          <span className="ml-auto flex items-center gap-1">
            <MessageSearch onJump={requestJump} />
            {selectedPlaceIsLoaded &&
            selectedPlaceKey &&
            (display?.kind !== "channel" || selectedChannel?.voice) ? (
              <CallStartButtons placeKey={selectedPlaceKey} />
            ) : null}
            {canNotify ? <NotificationSettingsMenu /> : null}
            {canReplyLater ? <ReplyLaterMenu onJump={requestJump} /> : null}
            {selectedPlaceIsLoaded ? (
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
            ) : null}
          </span>
        </header>
        <ConnectionBanner />
        {selectedPlaceKey ? (
          <CallFailureNotice placeKey={selectedPlaceKey} />
        ) : null}
        <CallBanner />
        <div className="flex min-h-0 flex-1">
          <main className="flex min-w-0 flex-1 flex-col">
            {selectedPlaceIsLoaded ? (
              <>
                <CallStage />
                <MessageList
                  handleRef={listRef}
                  revealedAttachmentIds={revealedAttachmentIds}
                  onRevealAttachment={revealAttachment}
                  onOpenImage={openImage}
                />
                <TypingIndicator />
                <Composer />
              </>
            ) : (
              <section className="grid min-h-0 flex-1 place-items-center px-6 text-center">
                <div className="max-w-sm">
                  <h2 className="font-medium text-[15px] text-foreground">
                    {hasSelectablePlace
                      ? "場所を選択"
                      : workspaces.length === 0
                        ? "参加中のワークスペースはありません"
                        : "場所はまだありません"}
                  </h2>
                  <p className="mt-1.5 text-[13px] text-muted-foreground leading-5">
                    {hasSelectablePlace
                      ? "サイドバーからチャンネルまたはダイレクトメッセージを選んでください。"
                      : workspaces.length === 0
                        ? "ワークスペースに参加すると、ここから会話を始められます。"
                        : "チャンネルやダイレクトメッセージが作成されると、ここに表示されます。"}
                  </p>
                </div>
              </section>
            )}
          </main>
          {membersOpen && selectedPlaceIsLoaded ? <MemberList /> : null}
        </div>
      </div>
      <IncomingCall />
      {canReplyLater ? <ReplyLaterKnock onJump={requestJump} /> : null}
      {viewingImage?.placeKey === selectedPlaceKey ? (
        <ImageViewer
          attachment={viewingImage.request.attachment}
          href={attachmentURL(viewingImage.request.attachment.attachmentId)}
          authorName={viewingImage.request.authorName}
          createdAt={viewingImage.request.createdAt}
          onClose={() => setViewingImage(null)}
        />
      ) : null}
    </div>
  );
}
