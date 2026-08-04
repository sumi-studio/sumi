import { BellOff, Hash, MoreVertical } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import type { NotificationLevel, PlaceKey, StatusKind } from "../model";
import { participantKey } from "../model";
import { usePlaceNavigate } from "../place-route";
import { notificationLevelFor, useMessaging } from "../store";
import { ParticipantAvatar } from "./participant-avatar";

const STATUS_LABEL: Record<StatusKind, string> = {
  available: "対応可能",
  busy: "取り込み中",
  away: "離席中",
};

export const NOTIFICATION_LEVEL_LABEL: Record<NotificationLevel, string> = {
  all: "すべて通知",
  mentions: "メンションのみ",
  mute: "ミュート",
};

const NOTIFICATION_LEVEL_HINT: Record<NotificationLevel, string> = {
  all: "この場所の発言で呼ばれます",
  mentions: "名前を呼ばれたときだけ",
  mute: "呼ばれません（未読は数えます）",
};

function Badge({
  count,
  urgent,
  muted,
}: {
  count: number;
  urgent: boolean;
  muted: boolean;
}) {
  if (count <= 0) return null;
  return (
    <span
      className={`rounded-full px-1.5 py-px font-semibold text-[10px] tabular-nums ${
        urgent
          ? "bg-rose-500 text-white"
          : "bg-muted-foreground/20 text-foreground"
      } ${muted ? "opacity-40" : ""}`}
    >
      {count > 99 ? "99+" : count}
    </span>
  );
}

/**
 * placeごとの通知レベル。右クリックとホバーの「…」の両方から開く——
 * 右クリックはDiscordを知っている手が最初に試す操作で、ホバーは知らない手が
 * 見つけられる導線。
 */
function PlaceNotificationMenu({
  placeKey: key,
  open,
  onOpenChange,
}: {
  placeKey: PlaceKey;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  const level = useMessaging((state) => notificationLevelFor(state, key));
  const setPlaceNotificationLevel = useMessaging(
    (state) => state.setPlaceNotificationLevel,
  );
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    const closeOnOutsideClick = (event: MouseEvent) => {
      if (!containerRef.current?.contains(event.target as Node)) {
        onOpenChange(false);
      }
    };
    const closeOnEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") onOpenChange(false);
    };
    window.addEventListener("mousedown", closeOnOutsideClick);
    window.addEventListener("keydown", closeOnEscape);
    return () => {
      window.removeEventListener("mousedown", closeOnOutsideClick);
      window.removeEventListener("keydown", closeOnEscape);
    };
  }, [open, onOpenChange]);

  return (
    <div ref={containerRef} className="relative">
      <button
        type="button"
        aria-label="通知設定"
        aria-expanded={open}
        onClick={(event) => {
          event.stopPropagation();
          onOpenChange(!open);
        }}
        className={`flex size-5 shrink-0 items-center justify-center rounded text-muted-foreground transition-opacity hover:bg-accent hover:text-foreground ${
          open ? "opacity-100" : "opacity-0 group-hover:opacity-100"
        }`}
      >
        <MoreVertical className="size-3.5" />
      </button>
      {open ? (
        <div className="absolute top-full right-0 z-30 mt-1 w-56 rounded-lg border border-border bg-background p-1 shadow-md">
          <p className="px-2 pt-1.5 pb-1 font-medium text-[11px] text-muted-foreground">
            通知
          </p>
          {(Object.keys(NOTIFICATION_LEVEL_LABEL) as NotificationLevel[]).map(
            (candidate) => (
              <button
                key={candidate}
                type="button"
                onClick={() => {
                  setPlaceNotificationLevel(key, candidate);
                  onOpenChange(false);
                }}
                className={`block w-full rounded-md px-2 py-1.5 text-left hover:bg-accent ${
                  level === candidate ? "bg-accent/60" : ""
                }`}
              >
                <span
                  className={`block text-[13px] ${
                    level === candidate ? "font-medium" : ""
                  }`}
                >
                  {NOTIFICATION_LEVEL_LABEL[candidate]}
                </span>
                <span className="block text-[11px] text-muted-foreground">
                  {NOTIFICATION_LEVEL_HINT[candidate]}
                </span>
              </button>
            ),
          )}
        </div>
      ) : null}
    </div>
  );
}

function PlaceRow({
  placeKey: key,
  label,
  icon,
  unread,
  mentions,
}: {
  placeKey: PlaceKey;
  label: React.ReactNode;
  icon: React.ReactNode;
  unread: number;
  mentions: number;
}) {
  const activePlaceKey = useMessaging((state) => state.activePlaceKey);
  const canConfigure = useMessaging(
    (state) => state.capabilities.notifications,
  );
  const level = useMessaging((state) => notificationLevelFor(state, key));
  const placeNavigate = usePlaceNavigate();
  const [menuOpen, setMenuOpen] = useState(false);
  const active = activePlaceKey === key;
  const muted = level === "mute";
  return (
    <div
      className={`group flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-[13px] transition-colors ${
        active
          ? "bg-accent text-foreground"
          : unread > 0 && !muted
            ? "font-medium text-foreground hover:bg-accent/60"
            : "text-muted-foreground hover:bg-accent/60 hover:text-foreground"
      }`}
    >
      <button
        type="button"
        onClick={() => placeNavigate(key)}
        onContextMenu={(event) => {
          if (!canConfigure) return;
          event.preventDefault();
          setMenuOpen(true);
        }}
        className="flex min-w-0 flex-1 items-center gap-2 text-left"
      >
        {icon}
        <span className="min-w-0 flex-1 truncate">{label}</span>
      </button>
      {muted ? (
        <BellOff
          aria-label="ミュート中"
          className="size-3 shrink-0 text-muted-foreground/60"
        />
      ) : null}
      <Badge
        count={mentions > 0 ? mentions : unread}
        urgent={mentions > 0}
        muted={muted}
      />
      {canConfigure ? (
        <PlaceNotificationMenu
          placeKey={key}
          open={menuOpen}
          onOpenChange={setMenuOpen}
        />
      ) : null}
    </div>
  );
}

export function Sidebar() {
  const workspaces = useMessaging((state) => state.workspaces);
  const channels = useMessaging((state) => state.channels);
  const dms = useMessaging((state) => state.dms);
  const membersByKey = useMessaging((state) => state.membersByKey);
  const statusByKey = useMessaging((state) => state.statusByKey);
  const unreadCountByPlace = useMessaging((state) => state.unreadCountByPlace);
  const mentionCountByPlace = useMessaging(
    (state) => state.mentionCountByPlace,
  );
  const selfKey = useMessaging((state) => state.selfKey);
  const self = useMessaging((state) => state.self);
  const setStatus = useMessaging((state) => state.setStatus);
  const canSetStatus = useMessaging((state) => state.capabilities.status);
  const [statusMenuOpen, setStatusMenuOpen] = useState(false);

  const selfProfile = self ? membersByKey[selfKey] : undefined;
  const selfStatus = statusByKey[selfKey];

  return (
    <aside className="flex w-60 shrink-0 flex-col border-border/70 border-r bg-muted/20">
      <div className="flex h-12 shrink-0 items-center border-border/70 border-b px-4">
        <span className="truncate font-semibold text-[14px]">
          {workspaces[0]?.name ?? "Sumi"}
        </span>
      </div>
      <nav className="scrollbar-ui min-h-0 flex-1 overflow-y-auto p-2">
        <p className="px-2 pt-2 pb-1 font-medium text-[11px] text-muted-foreground/80">
          チャンネル
        </p>
        {channels.map((channel) => {
          const key = `channel:${channel.channelId}`;
          const unread = unreadCountByPlace[key] ?? 0;
          const mentions = mentionCountByPlace[key] ?? 0;
          return (
            <PlaceRow
              key={key}
              placeKey={key}
              label={channel.name}
              icon={<Hash className="size-3.5 shrink-0 opacity-60" />}
              unread={unread}
              mentions={mentions}
            />
          );
        })}
        <p className="px-2 pt-4 pb-1 font-medium text-[11px] text-muted-foreground/80">
          ダイレクトメッセージ
        </p>
        {dms.map((dm) => {
          const key = `${dm.kind}:${dm.dmId}`;
          const others = dm.participants.filter(
            (ref) => participantKey(ref) !== selfKey,
          );
          const first = others[0];
          const firstKey = first ? participantKey(first) : "";
          const name = others
            .map(
              (ref) => membersByKey[participantKey(ref)]?.displayName ?? "不明",
            )
            .join("、");
          const unread = unreadCountByPlace[key] ?? 0;
          return (
            <PlaceRow
              key={key}
              placeKey={key}
              label={name}
              icon={
                <ParticipantAvatar
                  participantKey={firstKey}
                  name={membersByKey[firstKey]?.displayName ?? "?"}
                  size={18}
                  status={statusByKey[firstKey]?.status}
                />
              }
              unread={unread}
              mentions={unread}
            />
          );
        })}
      </nav>
      <div className="relative shrink-0 border-border/70 border-t p-2">
        {statusMenuOpen && canSetStatus ? (
          <div className="absolute bottom-full left-2 z-10 mb-1 w-52 rounded-lg border border-border bg-background p-1 shadow-md">
            {(Object.keys(STATUS_LABEL) as StatusKind[]).map((kind) => (
              <button
                key={kind}
                type="button"
                onClick={() => {
                  setStatus(kind, kind === "busy" ? "取り込み中" : "");
                  setStatusMenuOpen(false);
                }}
                className={`flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left text-[13px] hover:bg-accent ${
                  selfStatus?.status === kind ? "font-medium" : ""
                }`}
              >
                <span
                  className={`size-2 rounded-full ${
                    kind === "available"
                      ? "bg-emerald-500"
                      : kind === "busy"
                        ? "bg-rose-500"
                        : "bg-amber-400"
                  }`}
                />
                {STATUS_LABEL[kind]}
              </button>
            ))}
            <p className="px-2 pt-1 pb-0.5 text-[10px] text-muted-foreground/70">
              ステータスは自己申告。誰かが勝手に晒すことはありません
            </p>
          </div>
        ) : null}
        <button
          type="button"
          disabled={!canSetStatus}
          onClick={() => {
            if (canSetStatus) setStatusMenuOpen((open) => !open);
          }}
          className="flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left transition-colors enabled:hover:bg-accent/60 disabled:cursor-default"
        >
          <ParticipantAvatar
            participantKey={selfKey}
            name={selfProfile?.displayName ?? "?"}
            size={26}
            status={selfStatus?.status ?? "available"}
          />
          <span className="min-w-0 flex-1">
            <span className="block truncate font-medium text-[13px]">
              {selfProfile?.displayName ?? "…"}
            </span>
            <span className="block truncate text-[11px] text-muted-foreground">
              {selfStatus ? STATUS_LABEL[selfStatus.status] : "対応可能"}
              {selfStatus?.note ? ` — ${selfStatus.note}` : ""}
            </span>
          </span>
        </button>
      </div>
    </aside>
  );
}
