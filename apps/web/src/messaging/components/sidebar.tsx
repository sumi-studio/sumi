import {
  BellOff,
  Check,
  Hash,
  MoreVertical,
  Plus,
  Search,
  X,
} from "lucide-react";
import { useEffect, useMemo, useRef, useState } from "react";
import { isImeComposing } from "../../lib/ime";
import type { NotificationLevel, PlaceKey, StatusKind } from "../model";
import { participantKey } from "../model";
import { usePlaceNavigate } from "../place-route";
import { notificationLevelFor, useMessaging } from "../store";
import { useOverlayPanel } from "./overlay";
import { ParticipantAvatar, STATUS_LABEL } from "./participant-avatar";

/** サイドバーのplace一覧。ここが自前のスクロール領域。 */
const SIDEBAR_PLACES = '[data-slot="sidebar-places"]';

const INPUT_CLASS =
  "w-full rounded-md border border-border bg-background px-2.5 py-1.5 text-[13px] outline-none placeholder:text-muted-foreground/60 focus-visible:border-ring/60 disabled:opacity-50";

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
  // パネルはサイドバーのスクロール領域の内側にあるので、ホイールの転送はいらない。
  const overlay = useOverlayPanel<HTMLButtonElement>({
    open,
    onOpenChange,
    scrollPassthrough: () => null,
  });

  return (
    <div className="relative">
      <button
        type="button"
        aria-label="通知設定"
        aria-haspopup="menu"
        {...overlay.triggerProps}
        onClick={(event) => {
          event.stopPropagation();
          overlay.toggle();
        }}
        className={`flex size-5 shrink-0 items-center justify-center rounded text-muted-foreground transition-opacity hover:bg-accent hover:text-foreground ${
          open ? "opacity-100" : "opacity-0 group-hover:opacity-100"
        }`}
      >
        <MoreVertical className="size-3.5" />
      </button>
      {open ? (
        <div
          {...overlay.panelProps}
          className="absolute top-full right-0 z-30 mt-1 w-56 rounded-lg border border-border bg-background p-1 shadow-md"
        >
          <p
            id={`place-notification-${key}`}
            className="px-2 pt-1.5 pb-1 font-medium text-[11px] text-muted-foreground"
          >
            通知
          </p>
          <div role="radiogroup" aria-labelledby={`place-notification-${key}`}>
            {(Object.keys(NOTIFICATION_LEVEL_LABEL) as NotificationLevel[]).map(
              (candidate) => (
                <label
                  key={candidate}
                  className={`flex w-full cursor-pointer items-start gap-2 rounded-md px-2 py-1.5 text-left transition-colors hover:bg-accent active:bg-accent has-[:focus-visible]:ring-2 has-[:focus-visible]:ring-ring/60 ${
                    level === candidate ? "bg-accent/60" : ""
                  }`}
                >
                  <input
                    type="radio"
                    name={`place-notification-choice-${key}`}
                    checked={level === candidate}
                    onChange={() => {
                      setPlaceNotificationLevel(key, candidate);
                      onOpenChange(false);
                    }}
                    className="sr-only"
                  />
                  {/* 選択中は色だけでなく形（✓）でも示す。 */}
                  <Check
                    aria-hidden
                    className={`mt-0.5 size-3.5 shrink-0 transition-opacity ${
                      level === candidate ? "opacity-100" : "opacity-0"
                    }`}
                  />
                  <span className="min-w-0">
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
                  </span>
                </label>
              ),
            )}
          </div>
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

function DialogShell({
  title,
  onClose,
  children,
}: {
  title: string;
  onClose: () => void;
  children: React.ReactNode;
}) {
  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [onClose]);
  return (
    <div className="fixed inset-0 z-40 flex items-center justify-center bg-black/40 p-4">
      <div
        role="dialog"
        aria-modal="true"
        aria-label={title}
        className="w-80 rounded-xl border border-border bg-background p-4 shadow-lg"
      >
        <div className="flex items-center justify-between">
          <p className="font-semibold text-[14px]">{title}</p>
          <button
            type="button"
            title="閉じる"
            onClick={onClose}
            className="rounded p-1 text-muted-foreground hover:bg-accent hover:text-foreground"
          >
            <X className="size-3.5" />
          </button>
        </div>
        {children}
      </div>
    </div>
  );
}

function CreateChannelDialog({ onClose }: { onClose: () => void }) {
  const createChannel = useMessaging((state) => state.createChannel);
  const placeNavigate = usePlaceNavigate();
  const [name, setName] = useState("");
  const [topic, setTopic] = useState("");
  const [busy, setBusy] = useState(false);
  const [failed, setFailed] = useState(false);
  const nameRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    nameRef.current?.focus();
  }, []);

  const submit = async (event: React.FormEvent) => {
    event.preventDefault();
    const trimmed = name.trim();
    if (!trimmed || busy) return;
    setBusy(true);
    setFailed(false);
    try {
      const key = await createChannel(trimmed, topic.trim());
      placeNavigate(key);
      onClose();
    } catch {
      setFailed(true);
      setBusy(false);
    }
  };

  return (
    <DialogShell title="チャンネルを作成" onClose={onClose}>
      <form
        onSubmit={submit}
        onKeyDown={(event) => {
          // IME変換確定のEnterでフォームを飛ばさない。
          if (event.key === "Enter" && isImeComposing(event)) {
            event.preventDefault();
          }
        }}
        className="mt-3 space-y-3"
      >
        <label className="block">
          <span className="mb-1 block text-[11px] text-muted-foreground">
            名前
          </span>
          <input
            ref={nameRef}
            value={name}
            onChange={(event) => setName(event.target.value)}
            disabled={busy}
            maxLength={80}
            placeholder="例: dev"
            className={INPUT_CLASS}
          />
        </label>
        <label className="block">
          <span className="mb-1 block text-[11px] text-muted-foreground">
            トピック（任意）
          </span>
          <input
            value={topic}
            onChange={(event) => setTopic(event.target.value)}
            disabled={busy}
            maxLength={200}
            placeholder="このチャンネルの話題"
            className={INPUT_CLASS}
          />
        </label>
        {failed ? (
          <p className="text-[11px] text-rose-500">
            チャンネルを作成できませんでした
          </p>
        ) : null}
        <div className="flex justify-end gap-1.5">
          <button
            type="button"
            onClick={onClose}
            className="rounded-md px-2.5 py-1.5 text-[12.5px] text-muted-foreground hover:bg-accent"
          >
            キャンセル
          </button>
          <button
            type="submit"
            disabled={busy || !name.trim()}
            className="rounded-md bg-primary px-2.5 py-1.5 font-medium text-[12.5px] text-primary-foreground hover:opacity-90 disabled:opacity-50"
          >
            作成
          </button>
        </div>
      </form>
    </DialogShell>
  );
}

/** 検索の一致判定。表示名と肩書きの両方を、大小・全角半角を問わず見る。 */
function matchesQuery(haystack: string, needle: string): boolean {
  return haystack
    .normalize("NFKC")
    .toLowerCase()
    .includes(needle.normalize("NFKC").toLowerCase());
}

function StartDMDialog({ onClose }: { onClose: () => void }) {
  const membersByKey = useMessaging((state) => state.membersByKey);
  const statusByKey = useMessaging((state) => state.statusByKey);
  const selfKey = useMessaging((state) => state.selfKey);
  const startDM = useMessaging((state) => state.startDM);
  const placeNavigate = usePlaceNavigate();
  const [selected, setSelected] = useState<Record<string, boolean>>({});
  const [query, setQuery] = useState("");
  const [busy, setBusy] = useState(false);
  const [failed, setFailed] = useState(false);
  const queryRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    queryRef.current?.focus();
  }, []);

  const candidates = useMemo(
    () =>
      Object.values(membersByKey)
        .filter((member) => participantKey(member.participant) !== selfKey)
        .sort((a, b) => a.displayName.localeCompare(b.displayName, "ja")),
    [membersByKey, selfKey],
  );
  const chosen = candidates.filter(
    (member) => selected[participantKey(member.participant)],
  );
  // 絞り込みは表示だけを狭める。選んだ相手は検索語から外れても消えない——
  // 一覧から見えなくなった選択は、黙って人を落とすのと同じことになる。
  const visible = useMemo(() => {
    const trimmed = query.trim();
    if (!trimmed) return candidates;
    return candidates.filter(
      (member) =>
        selected[participantKey(member.participant)] ||
        matchesQuery(member.displayName, trimmed) ||
        matchesQuery(member.tagline, trimmed),
    );
  }, [candidates, query, selected]);

  const submit = async () => {
    if (busy || chosen.length === 0) return;
    setBusy(true);
    setFailed(false);
    try {
      const key = await startDM(chosen.map((member) => member.participant));
      placeNavigate(key);
      onClose();
    } catch {
      setFailed(true);
      setBusy(false);
    }
  };

  return (
    <DialogShell title="ダイレクトメッセージを開始" onClose={onClose}>
      <p className="mt-1 text-[11px] text-muted-foreground/80">
        1人ならDM、複数人ならグループDMになります
      </p>
      <div className="relative mt-2">
        <Search className="pointer-events-none absolute top-1/2 left-2 size-3.5 -translate-y-1/2 text-muted-foreground/60" />
        <input
          ref={queryRef}
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          disabled={busy}
          maxLength={80}
          placeholder="名前で絞り込む"
          aria-label="宛先を検索"
          className={`${INPUT_CLASS} pl-7`}
        />
      </div>
      <div className="scrollbar-ui mt-2 max-h-64 overflow-y-auto rounded-md border border-border/70 p-1">
        {candidates.length === 0 ? (
          <p className="px-2 py-3 text-[12px] text-muted-foreground/70">
            話せる相手がいません
          </p>
        ) : visible.length === 0 ? (
          <p className="px-2 py-3 text-[12px] text-muted-foreground/70">
            「{query.trim()}」に合う相手はいません
          </p>
        ) : (
          visible.map((member) => {
            const key = participantKey(member.participant);
            const checked = selected[key] ?? false;
            return (
              <button
                key={key}
                type="button"
                disabled={busy}
                onClick={() =>
                  setSelected((entry) => ({ ...entry, [key]: !checked }))
                }
                className="flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left hover:bg-accent/60"
              >
                <span
                  className={`flex size-4 shrink-0 items-center justify-center rounded border ${
                    checked
                      ? "border-primary bg-primary text-primary-foreground"
                      : "border-border"
                  }`}
                >
                  {checked ? <Check className="size-3" /> : null}
                </span>
                <ParticipantAvatar
                  participantKey={key}
                  name={member.displayName}
                  size={22}
                  status={statusByKey[key]?.status}
                />
                <span className="min-w-0 flex-1">
                  <span className="block truncate text-[13px]">
                    {member.displayName}
                  </span>
                  {member.tagline ? (
                    <span className="block truncate text-[11px] text-muted-foreground">
                      {member.tagline}
                    </span>
                  ) : null}
                </span>
              </button>
            );
          })
        )}
      </div>
      {chosen.length > 0 ? (
        <p className="mt-2 text-[11px] text-muted-foreground">
          {chosen.map((member) => member.displayName).join("、")} を選択中
        </p>
      ) : null}
      {failed ? (
        <p className="mt-2 text-[11px] text-rose-500">
          会話を開始できませんでした
        </p>
      ) : null}
      {/* 主操作の文言は選択人数で伸び縮みするので、キャンセルは左端に固定して
          ポインタの下から逃げないようにする（両端揃え）。 */}
      <div className="mt-3 flex items-center justify-between gap-2">
        <button
          type="button"
          onClick={onClose}
          className="rounded-md px-2.5 py-1.5 text-[12.5px] text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
        >
          キャンセル
        </button>
        <button
          type="button"
          onClick={() => void submit()}
          disabled={busy || chosen.length === 0}
          className="rounded-md bg-primary px-2.5 py-1.5 font-medium text-[12.5px] text-primary-foreground hover:opacity-90 disabled:opacity-50"
        >
          {chosen.length > 1 ? "グループDMを作成" : "DMを開始"}
        </button>
      </div>
    </DialogShell>
  );
}

function SectionHeader({
  label,
  actionTitle,
  onAction,
}: {
  label: string;
  actionTitle: string;
  onAction: () => void;
}) {
  return (
    <div className="group flex items-center justify-between px-2 pb-1">
      <p className="font-medium text-[11px] text-muted-foreground/80">
        {label}
      </p>
      <button
        type="button"
        title={actionTitle}
        onClick={onAction}
        className="rounded p-0.5 text-muted-foreground/50 transition-colors hover:bg-accent hover:text-foreground"
      >
        <Plus className="size-3.5" />
      </button>
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
  const [openDialog, setOpenDialog] = useState<"channel" | "dm" | null>(null);
  // ステータスメニューはplace一覧の上に浮くので、ホイールは一覧へ渡す。
  const statusOverlay = useOverlayPanel<HTMLButtonElement>({
    open: statusMenuOpen,
    onOpenChange: setStatusMenuOpen,
    scrollPassthrough: () =>
      document.querySelector<HTMLElement>(SIDEBAR_PLACES),
  });

  const selfProfile = self ? membersByKey[selfKey] : undefined;
  const selfStatus = statusByKey[selfKey];

  return (
    <aside className="flex w-60 shrink-0 flex-col border-border/70 border-r bg-muted/20">
      <div className="flex h-12 shrink-0 items-center border-border/70 border-b px-4">
        <span className="truncate font-semibold text-[14px]">
          {workspaces[0]?.name ?? "Sumi"}
        </span>
      </div>
      <nav
        data-slot="sidebar-places"
        className="scrollbar-ui min-h-0 flex-1 overflow-y-auto p-2"
      >
        <div className="pt-2">
          <SectionHeader
            label="チャンネル"
            actionTitle="チャンネルを作成"
            onAction={() => setOpenDialog("channel")}
          />
        </div>
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
        <div className="pt-4">
          <SectionHeader
            label="ダイレクトメッセージ"
            actionTitle="ダイレクトメッセージを開始"
            onAction={() => setOpenDialog("dm")}
          />
        </div>
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
          <div
            {...statusOverlay.panelProps}
            role="dialog"
            aria-label="ステータス"
            className="absolute bottom-full left-2 z-10 mb-1 w-52 rounded-lg border border-border bg-background p-1 shadow-md"
          >
            {(Object.keys(STATUS_LABEL) as StatusKind[]).map((kind) => (
              <label
                key={kind}
                className={`flex w-full cursor-pointer items-center gap-2 rounded-md px-2 py-1.5 text-left text-[13px] transition-colors hover:bg-accent active:bg-accent has-[:focus-visible]:ring-2 has-[:focus-visible]:ring-ring/60 ${
                  selfStatus?.status === kind ? "font-medium" : ""
                }`}
              >
                <input
                  type="radio"
                  name="self-status-choice"
                  checked={selfStatus?.status === kind}
                  onChange={() => {
                    setStatus(kind, kind === "busy" ? "取り込み中" : "");
                    setStatusMenuOpen(false);
                  }}
                  className="sr-only"
                />
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
                <Check
                  aria-hidden
                  className={`ml-auto size-3.5 shrink-0 ${
                    selfStatus?.status === kind ? "opacity-100" : "opacity-0"
                  }`}
                />
              </label>
            ))}
            <p className="px-2 pt-1 pb-0.5 text-[10px] text-muted-foreground/70">
              ステータスは自己申告。誰かが勝手に晒すことはありません
            </p>
          </div>
        ) : null}
        <button
          type="button"
          disabled={!canSetStatus}
          aria-haspopup="menu"
          {...statusOverlay.triggerProps}
          onClick={() => {
            if (canSetStatus) statusOverlay.toggle();
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
      {openDialog === "channel" ? (
        <CreateChannelDialog onClose={() => setOpenDialog(null)} />
      ) : null}
      {openDialog === "dm" ? (
        <StartDMDialog onClose={() => setOpenDialog(null)} />
      ) : null}
    </aside>
  );
}
