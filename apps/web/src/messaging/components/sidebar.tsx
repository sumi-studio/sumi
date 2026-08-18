import { BellOff, Check, Hash, Plus, Search, Volume2, X } from "lucide-react";
import { useEffect, useMemo, useRef, useState } from "react";
import { isImeComposing } from "../../lib/ime";
import { VoiceChannelMembers } from "../call/voice-channel-members";
import { VoiceChannelPanel } from "../call/voice-channel-panel";
import type { PlaceKey } from "../model";
import { parsePlaceKey, participantKey } from "../model";
import { usePlaceNavigate } from "../place-route";
import {
  getMessagingSessionIdentity,
  notificationLevelFor,
  useMessaging,
} from "../store";
import { ownsEvent } from "./overlay";
import { ParticipantAvatar } from "./participant-avatar";
import { ParticipantProfilePopover } from "./participant-profile";
import { PlaceContextMenu } from "./place-context-menu";
import { StatusMenu } from "./status-menu";

const SIDEBAR_PLACES = '[data-slot="sidebar-places"]';

/** サイドバーのオーバーレイが覆う面。 */
const sidebarPlaces = () => document.querySelector<HTMLElement>(SIDEBAR_PLACES);

const INPUT_CLASS =
  "w-full rounded-md border border-border bg-background px-2.5 py-1.5 text-[13px] outline-none placeholder:text-muted-foreground/60 focus-visible:border-ring/60 disabled:opacity-50";

export function Badge({
  count,
  urgent,
  muted,
}: {
  count: number;
  urgent: boolean;
  muted: boolean;
}) {
  if (count <= 0 || muted) return null;
  return (
    <span
      className={`rounded-full px-1.5 py-px font-semibold text-[10px] tabular-nums ${
        urgent
          ? "bg-rose-500 text-white"
          : "bg-muted-foreground/20 text-foreground"
      }`}
    >
      {count > 99 ? "99+" : count}
    </span>
  );
}

/** 検索の突き合わせ用。全角/半角と大小文字の違いで隠れないようにする。 */
export function normalizeForSearch(value: string): string {
  return value.normalize("NFKC").toLocaleLowerCase("ja");
}

function PlaceRow({
  placeKey: key,
  channelId,
  selectedPlaceKey,
  label,
  icon,
  leading,
  unread,
  mentions,
  onEditChannel,
  onDuplicateChannel,
  onCreateChannel,
}: {
  placeKey: PlaceKey;
  /** channelでなければnull。メニューからchannel専用の項目が消える。 */
  channelId: string | null;
  selectedPlaceKey: PlaceKey | null;
  label: React.ReactNode;
  icon: React.ReactNode;
  /**
   * 行頭に置く、place遷移とは別の役割を持つ導線。渡すとiconの代わりに
   * 遷移buttonの外へ出る（buttonの入れ子は作らない）。
   */
  leading?: React.ReactNode;
  unread: number;
  mentions: number;
  onEditChannel: (channelId: string) => void;
  onDuplicateChannel: (channelId: string) => void;
  onCreateChannel: () => void;
}) {
  const canConfigureNotifications = useMessaging(
    (state) => state.capabilities.notifications,
  );
  const level = useMessaging((state) => notificationLevelFor(state, key));
  const placeNavigate = usePlaceNavigate();
  const [menuOpen, setMenuOpen] = useState(false);
  const active = selectedPlaceKey === key;
  const muted = level === "mute";
  // channelなら操作が必ず一つはあるので、通知を設定できない相手でもメニューは出る。
  const hasMenu = channelId !== null || canConfigureNotifications;
  return (
    // 右クリックは行そのものが受ける。leadingへ切り出したアバターの上でも
    // 同じ導線が出る（行の右クリック契約はアバター領域を含む）。行内の
    // buttonへフォーカスしたままのShift+F10もここへ上がってくる。
    // 行が所有するのは行のイベントだけ。行から開いたportal（プロフィール
    // カード）も、行がhostしているだけの通知パネルも「行の中」ではない。
    // biome-ignore lint/a11y/noStaticElementInteractions: 右クリックは補助導線で、正規の入口は同じ行の「…」button。
    <div
      onContextMenu={(event) => {
        if (!hasMenu || !ownsEvent(event)) return;
        event.preventDefault();
        setMenuOpen(true);
      }}
      className={`group flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-[13px] transition-colors ${
        active
          ? "bg-accent text-foreground"
          : unread > 0 && !muted
            ? "font-medium text-foreground hover:bg-accent/60"
            : "text-muted-foreground hover:bg-accent/60 hover:text-foreground"
      }`}
    >
      {leading}
      <button
        type="button"
        aria-current={active ? "page" : undefined}
        onClick={() => placeNavigate(key)}
        className="flex min-w-0 flex-1 items-center gap-2 text-left"
      >
        {leading ? null : icon}
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
      {hasMenu ? (
        <PlaceContextMenu
          placeKey={key}
          channelId={channelId}
          open={menuOpen}
          onOpenChange={setMenuOpen}
          onEditChannel={onEditChannel}
          onDuplicateChannel={onDuplicateChannel}
          onCreateChannel={onCreateChannel}
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

function CreateChannelDialog({
  workspaceId,
  isWorkspaceSelected,
  onClose,
}: {
  workspaceId: string;
  isWorkspaceSelected: () => boolean;
  onClose: () => void;
}) {
  const createChannel = useMessaging((state) => state.createChannel);
  const placeNavigate = usePlaceNavigate();
  const [name, setName] = useState("");
  const [topic, setTopic] = useState("");
  const [voice, setVoice] = useState(false);
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
    const currentIdentity = getMessagingSessionIdentity();
    const expectedSelfKey = useMessaging.getState().selfKey;
    setBusy(true);
    setFailed(false);
    try {
      const key = await createChannel(
        workspaceId,
        trimmed,
        topic.trim(),
        voice,
      );
      const sessionChanged =
        getMessagingSessionIdentity() !== currentIdentity ||
        useMessaging.getState().selfKey !== expectedSelfKey;
      if (sessionChanged || !isWorkspaceSelected()) {
        throw new Error("Messaging session changed before channel navigation");
      }
      placeNavigate(key);
      onClose();
    } catch {
      if (
        getMessagingSessionIdentity() === currentIdentity &&
        useMessaging.getState().selfKey === expectedSelfKey &&
        isWorkspaceSelected()
      ) {
        setFailed(true);
      }
      setBusy(false);
    }
  };

  return (
    <DialogShell title="チャンネルを作成" onClose={onClose}>
      <form
        onSubmit={submit}
        onKeyDown={(event) => {
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
        <label className="flex cursor-pointer items-start gap-2 rounded-md border border-border/70 p-2.5">
          <input
            type="checkbox"
            checked={voice}
            disabled={busy}
            onChange={(event) => setVoice(event.target.checked)}
            className="mt-0.5"
          />
          <span>
            <span className="block text-[12.5px]">ボイスチャンネル</span>
            <span className="block text-[11px] text-muted-foreground">
              テキストを残したまま、いつでも音声で集まれます
            </span>
          </span>
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

/**
 * channelの名前とトピックを一枚で直す。ヘッダーのその場編集をやめてここへ
 * 集めたのは、名前とトピックが同じ一つの「このチャンネルは何か」だから——
 * 別々の場所で別々に直すものではない。
 *
 * 何も変えずに「保存」を押せる状態は作らない。変わった項目だけを送るので、
 * 名前を直したときにトピックが空文字で上書きされることもない。
 */
function EditChannelDialog({
  channelId,
  onClose,
}: {
  channelId: string;
  onClose: () => void;
}) {
  const channel = useMessaging((state) =>
    state.channels.find((entry) => entry.channelId === channelId),
  );
  const updateChannel = useMessaging((state) => state.updateChannel);
  // 開いたときの値が、この編集セッションの比較対象。後から届く更新は
  // channel を新しくしても、本人が触っていない項目を「変更」とはしない。
  const [initial] = useState(() => ({
    name: channel?.name ?? "",
    topic: channel?.topic ?? "",
  }));
  const [name, setName] = useState(initial.name);
  const [topic, setTopic] = useState(initial.topic);
  const [busy, setBusy] = useState(false);
  const [failed, setFailed] = useState(false);
  const nameRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    nameRef.current?.focus();
  }, []);

  // 開いている最中にchannelが消えたら、閉じる。
  useEffect(() => {
    if (!channel) onClose();
  }, [channel, onClose]);
  if (!channel) return null;

  const nextName = name.trim();
  const nextTopic = topic.trim();
  const changed =
    (nextName !== "" && nextName !== initial.name) ||
    nextTopic !== initial.topic;

  const submit = async (event: React.FormEvent) => {
    event.preventDefault();
    if (busy || !changed) return;
    const currentIdentity = getMessagingSessionIdentity();
    setBusy(true);
    setFailed(false);
    try {
      await updateChannel(channelId, {
        ...(nextName !== "" && nextName !== initial.name
          ? { name: nextName }
          : {}),
        ...(nextTopic !== initial.topic ? { topic: nextTopic } : {}),
      });
      onClose();
    } catch {
      if (getMessagingSessionIdentity() === currentIdentity) setFailed(true);
      setBusy(false);
    }
  };

  return (
    <DialogShell title="チャンネルを編集" onClose={onClose}>
      <form
        onSubmit={submit}
        onKeyDown={(event) => {
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
            maxLength={200}
            className={INPUT_CLASS}
          />
        </label>
        <label className="block">
          <span className="mb-1 block text-[11px] text-muted-foreground">
            トピック
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
            チャンネルを更新できませんでした
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
            disabled={busy || !changed}
            className="rounded-md bg-primary px-2.5 py-1.5 font-medium text-[12.5px] text-primary-foreground hover:opacity-90 disabled:opacity-50"
          >
            保存
          </button>
        </div>
      </form>
    </DialogShell>
  );
}

/**
 * DM/グループDMの相手を選ぶ。名前とtaglineの両方を、NFKCで畳んだ上で
 * 突き合わせる——「ｸﾛ」と打った人にKUROが出ないのは、探せないのと同じ。
 *
 * 絞り込んでも選択済みの相手は消えない。検索語を打ち替えるたびに、さっき
 * 選んだ人がリストから居なくなって、選んだ覚えのある名前が画面のどこにも
 * 無い——という状態を作らないため。
 */
function StartDMDialog({ onClose }: { onClose: () => void }) {
  const membersByKey = useMessaging((state) => state.membersByKey);
  const statusByKey = useMessaging((state) => state.statusByKey);
  const selfKey = useMessaging((state) => state.selfKey);
  const startDM = useMessaging((state) => state.startDM);
  const dmPending = useMessaging((state) => state.startingDM !== null);
  const placeNavigate = usePlaceNavigate();
  const [selected, setSelected] = useState<Record<string, boolean>>({});
  const [query, setQuery] = useState("");
  const [busy, setBusy] = useState(false);
  const [failed, setFailed] = useState(false);
  const searchRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    searchRef.current?.focus();
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
  const needle = normalizeForSearch(query.trim());
  const shown = candidates.filter((member) => {
    if (selected[participantKey(member.participant)]) return true;
    if (needle === "") return true;
    return (
      normalizeForSearch(member.displayName).includes(needle) ||
      normalizeForSearch(member.tagline ?? "").includes(needle)
    );
  });

  const submit = async () => {
    if (busy || dmPending || chosen.length === 0) return;
    const currentIdentity = getMessagingSessionIdentity();
    const expectedSelfKey = selfKey;
    setBusy(true);
    setFailed(false);
    try {
      const key = await startDM(chosen.map((member) => member.participant));
      const sessionChanged =
        getMessagingSessionIdentity() !== currentIdentity ||
        useMessaging.getState().selfKey !== expectedSelfKey;
      if (sessionChanged) {
        throw new Error("Messaging session changed before DM navigation");
      }
      placeNavigate(key);
      onClose();
    } catch {
      if (
        getMessagingSessionIdentity() === currentIdentity &&
        useMessaging.getState().selfKey === expectedSelfKey
      ) {
        setFailed(true);
      }
      setBusy(false);
    }
  };

  return (
    <DialogShell title="ダイレクトメッセージを開始" onClose={onClose}>
      <p className="mt-1 text-[11px] text-muted-foreground/80">
        1人ならDM、複数人ならグループDMになります
      </p>
      <label className="relative mt-2 block">
        <Search className="-translate-y-1/2 absolute top-1/2 left-2 size-3.5 text-muted-foreground/60" />
        <input
          ref={searchRef}
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          onKeyDown={(event) => {
            if (event.key !== "Enter" || isImeComposing(event)) return;
            event.preventDefault();
            void submit();
          }}
          disabled={busy}
          placeholder="名前で絞り込む"
          aria-label="名前で絞り込む"
          className={`${INPUT_CLASS} pl-7`}
        />
      </label>
      <div className="scrollbar-ui mt-2 max-h-64 overflow-y-auto rounded-md border border-border/70 p-1">
        {candidates.length === 0 ? (
          <p className="px-2 py-3 text-[12px] text-muted-foreground/70">
            話せる相手がいません
          </p>
        ) : shown.length === 0 ? (
          <p className="px-2 py-3 text-[12px] text-muted-foreground/70">
            「{query.trim()}」に当たる相手がいません
          </p>
        ) : (
          shown.map((member) => {
            const key = participantKey(member.participant);
            const checked = selected[key] ?? false;
            return (
              <button
                key={key}
                type="button"
                disabled={busy}
                aria-pressed={checked}
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
      {/* 「やめる」は左、「進む」は右。押し間違いを取り返せない側を、
          指が流れてくる位置に置かない。 */}
      <div className="mt-3 flex items-center justify-between gap-1.5">
        <button
          type="button"
          onClick={onClose}
          className="rounded-md px-2.5 py-1.5 text-[12.5px] text-muted-foreground hover:bg-accent"
        >
          キャンセル
        </button>
        <button
          type="button"
          onClick={() => void submit()}
          disabled={busy || dmPending || chosen.length === 0}
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
  onAction?: () => void;
}) {
  return (
    <div className="group flex items-center justify-between px-2 pb-1">
      <p className="font-medium text-[11px] text-muted-foreground/80">
        {label}
      </p>
      {onAction ? (
        <button
          type="button"
          title={actionTitle}
          onClick={onAction}
          className="rounded p-0.5 text-muted-foreground/50 transition-colors hover:bg-accent hover:text-foreground"
        >
          <Plus className="size-3.5" />
        </button>
      ) : null}
    </div>
  );
}

export function Sidebar({
  selectedPlaceKey,
  workspaceId,
}: {
  selectedPlaceKey: PlaceKey | null;
  workspaceId: string | null;
}) {
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
  const duplicateChannel = useMessaging((state) => state.duplicateChannel);
  const placeNavigate = usePlaceNavigate();
  const [openDialog, setOpenDialog] = useState<
    | { kind: "channel"; workspaceId: string }
    | { kind: "edit"; channelId: string }
    | { kind: "dm" }
    | null
  >(null);

  const activePlace = selectedPlaceKey ? parsePlaceKey(selectedPlaceKey) : null;
  const activeChannel =
    activePlace?.kind === "channel"
      ? channels.find((channel) => channel.channelId === activePlace.channelId)
      : undefined;
  const channelWorkspace = activeChannel
    ? workspaces.find(
        (workspace) => workspace.workspaceId === activeChannel.workspaceId,
      )
    : undefined;
  const activeWorkspace =
    channelWorkspace?.workspaceId === workspaceId
      ? channelWorkspace
      : selectedPlaceKey === null
        ? workspaces.find((workspace) => workspace.workspaceId === workspaceId)
        : undefined;
  const selectedWorkspaceId = activeWorkspace?.workspaceId ?? null;
  const selectedWorkspaceIdRef = useRef(selectedWorkspaceId);
  selectedWorkspaceIdRef.current = selectedWorkspaceId;

  useEffect(() => {
    if (
      openDialog?.kind === "channel" &&
      openDialog.workspaceId !== selectedWorkspaceId
    ) {
      setOpenDialog(null);
    }
  }, [openDialog, selectedWorkspaceId]);

  const openCreateChannel = () => {
    if (!activeWorkspace) return;
    setOpenDialog({
      kind: "channel",
      workspaceId: activeWorkspace.workspaceId,
    });
  };
  // 複製はダイアログを挟まない。名前はサーバーが決め（「〜 のコピー」）、
  // できたものへそのまま移る——中身は空なので、直したければ開いた先で直せる。
  const runDuplicate = (channelId: string) => {
    const currentIdentity = getMessagingSessionIdentity();
    void duplicateChannel(channelId).then(
      (key) => {
        if (getMessagingSessionIdentity() !== currentIdentity) return;
        placeNavigate(key);
      },
      () => undefined,
    );
  };

  const menuActions = {
    onEditChannel: (channelId: string) =>
      setOpenDialog({ kind: "edit", channelId }),
    onDuplicateChannel: runDuplicate,
    onCreateChannel: openCreateChannel,
  };

  return (
    <aside className="flex w-60 shrink-0 flex-col border-border/70 border-r bg-muted/20">
      <div className="flex h-12 shrink-0 items-center border-border/70 border-b px-4">
        <span className="truncate font-semibold text-[14px]">
          {activeWorkspace?.name ??
            (workspaces.length === 0 ? "ワークスペースなし" : "場所を選択")}
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
            onAction={activeWorkspace ? openCreateChannel : undefined}
          />
        </div>
        {channels.map((channel) => {
          const key = `channel:${channel.channelId}`;
          const unread = unreadCountByPlace[key] ?? 0;
          const mentions = mentionCountByPlace[key] ?? 0;
          return (
            <div key={key}>
              <PlaceRow
                placeKey={key}
                channelId={channel.channelId}
                selectedPlaceKey={selectedPlaceKey}
                label={channel.name}
                icon={
                  channel.voice ? (
                    <Volume2 className="size-3.5 shrink-0 opacity-70" />
                  ) : (
                    <Hash className="size-3.5 shrink-0 opacity-60" />
                  )
                }
                unread={unread}
                mentions={mentions}
                {...menuActions}
              />
              {channel.voice ? (
                <>
                  <VoiceChannelMembers placeKey={key} />
                  <VoiceChannelPanel placeKey={key} />
                </>
              ) : null}
            </div>
          );
        })}
        <div className="pt-4">
          <SectionHeader
            label="ダイレクトメッセージ"
            actionTitle="ダイレクトメッセージを開始"
            onAction={() => setOpenDialog({ kind: "dm" })}
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
          const firstName = membersByKey[firstKey]?.displayName ?? "?";
          const avatar = (
            <ParticipantAvatar
              participantKey={firstKey}
              name={firstName}
              size={18}
              status={statusByKey[firstKey]?.status}
            />
          );
          return (
            <PlaceRow
              key={key}
              placeKey={key}
              channelId={null}
              selectedPlaceKey={selectedPlaceKey}
              label={name}
              icon={avatar}
              // 1対1のDMだけ、アバターがその相手のプロフィールを開く。
              // グループDMのアバターは先頭の1人でしかないので開き口にしない。
              leading={
                others.length === 1 ? (
                  <ParticipantProfilePopover
                    participantKey={firstKey}
                    label={`${firstName}のプロフィール`}
                    side="right"
                    align="start"
                    scrollPassthrough={sidebarPlaces}
                    className="flex shrink-0 rounded-full"
                  >
                    {avatar}
                  </ParticipantProfilePopover>
                ) : undefined
              }
              unread={unread}
              mentions={unread}
              {...menuActions}
            />
          );
        })}
      </nav>
      <div className="shrink-0 border-border/70 border-t p-2">
        <StatusMenu />
      </div>
      {openDialog?.kind === "channel" &&
      openDialog.workspaceId === selectedWorkspaceId ? (
        <CreateChannelDialog
          workspaceId={openDialog.workspaceId}
          isWorkspaceSelected={() =>
            selectedWorkspaceIdRef.current === openDialog.workspaceId
          }
          onClose={() => setOpenDialog(null)}
        />
      ) : null}
      {openDialog?.kind === "edit" ? (
        <EditChannelDialog
          channelId={openDialog.channelId}
          onClose={() => setOpenDialog(null)}
        />
      ) : null}
      {openDialog?.kind === "dm" ? (
        <StartDMDialog onClose={() => setOpenDialog(null)} />
      ) : null}
    </aside>
  );
}
