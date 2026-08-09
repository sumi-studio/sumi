import { Check, ChevronRight, Copy, Pencil, Plus } from "lucide-react";
import { useEffect, useId, useRef, useState } from "react";
import type { NotificationLevel, PlaceKey } from "../model";
import { notificationLevelFor, useMessaging } from "../store";

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

const ITEM_CLASS =
  "flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left text-[13px] hover:bg-accent disabled:opacity-50";

/**
 * placeの操作メニュー。名前の編集・複製・作成といった「その場所をどうするか」を
 * 主メニューに置き、通知設定は横に開くサブメニューへ送る——通知は場所の設定では
 * なく受け手の設定で、粒度が違うものを同じ高さに並べると選び間違える。
 *
 * 開閉は自前で持つ（A1のポップオーバー基盤へは統合時に差し替える）。
 */
export function PlaceContextMenu({
  placeKey: key,
  channelId,
  open,
  onOpenChange,
  onEditChannel,
  onDuplicateChannel,
  onCreateChannel,
}: {
  placeKey: PlaceKey;
  /** channel以外（DM・グループDM）ではnull。channel専用の項目が消える。 */
  channelId: string | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onEditChannel: (channelId: string) => void;
  onDuplicateChannel: (channelId: string) => void;
  onCreateChannel: () => void;
}) {
  const canConfigureNotifications = useMessaging(
    (state) => state.capabilities.notifications,
  );
  const level = useMessaging((state) => notificationLevelFor(state, key));
  const setPlaceNotificationLevel = useMessaging(
    (state) => state.setPlaceNotificationLevel,
  );
  const containerRef = useRef<HTMLDivElement>(null);
  const [submenuOpen, setSubmenuOpen] = useState(false);
  const submenuId = useId();

  useEffect(() => {
    if (!open) {
      setSubmenuOpen(false);
      return;
    }
    const closeOnOutsideClick = (event: MouseEvent) => {
      if (!containerRef.current?.contains(event.target as Node)) {
        onOpenChange(false);
      }
    };
    const closeOnEscape = (event: KeyboardEvent) => {
      if (event.key !== "Escape") return;
      // Escapeは一段ずつ閉じる。サブメニューを開けた手が、主メニューごと
      // 消されて最初からやり直しになるのを避ける。
      setSubmenuOpen((sub) => {
        if (sub) return false;
        onOpenChange(false);
        return false;
      });
    };
    window.addEventListener("mousedown", closeOnOutsideClick);
    window.addEventListener("keydown", closeOnEscape);
    return () => {
      window.removeEventListener("mousedown", closeOnOutsideClick);
      window.removeEventListener("keydown", closeOnEscape);
    };
  }, [open, onOpenChange]);

  if (!open) return null;

  const close = () => {
    setSubmenuOpen(false);
    onOpenChange(false);
  };

  return (
    <div
      ref={containerRef}
      role="menu"
      aria-label="この場所のメニュー"
      className="absolute top-full right-0 z-30 mt-1 w-52 rounded-lg border border-border bg-background p-1 shadow-md"
    >
      {channelId ? (
        <>
          <button
            type="button"
            role="menuitem"
            className={ITEM_CLASS}
            onClick={() => {
              close();
              onEditChannel(channelId);
            }}
          >
            <Pencil className="size-3.5 shrink-0 text-muted-foreground" />
            チャンネルを編集
          </button>
          <button
            type="button"
            role="menuitem"
            className={ITEM_CLASS}
            onClick={() => {
              close();
              onDuplicateChannel(channelId);
            }}
          >
            <Copy className="size-3.5 shrink-0 text-muted-foreground" />
            複製
          </button>
          <button
            type="button"
            role="menuitem"
            className={ITEM_CLASS}
            onClick={() => {
              close();
              onCreateChannel();
            }}
          >
            <Plus className="size-3.5 shrink-0 text-muted-foreground" />
            チャンネルを作成
          </button>
        </>
      ) : null}
      {canConfigureNotifications ? (
        <>
          {channelId ? <div className="my-1 h-px bg-border/70" /> : null}
          {/* 横に開くサブメニュー。ホバーでもクリックでも開く——指す手と
              押す手のどちらにも同じ場所がある。 */}
          <div
            role="none"
            className="relative"
            onMouseEnter={() => setSubmenuOpen(true)}
            onMouseLeave={() => setSubmenuOpen(false)}
          >
            <button
              type="button"
              role="menuitem"
              aria-haspopup="menu"
              aria-expanded={submenuOpen}
              aria-controls={submenuId}
              onClick={() => setSubmenuOpen((value) => !value)}
              className={`${ITEM_CLASS} ${submenuOpen ? "bg-accent" : ""}`}
            >
              通知設定
              <span className="ml-auto text-[11px] text-muted-foreground">
                {NOTIFICATION_LEVEL_LABEL[level]}
              </span>
              <ChevronRight className="size-3.5 shrink-0 text-muted-foreground" />
            </button>
            {submenuOpen ? (
              <div
                id={submenuId}
                role="menu"
                aria-label="通知設定"
                className="absolute top-0 left-full z-40 ml-1 w-56 rounded-lg border border-border bg-background p-1 shadow-md"
              >
                {(
                  Object.keys(NOTIFICATION_LEVEL_LABEL) as NotificationLevel[]
                ).map((candidate) => (
                  <button
                    key={candidate}
                    type="button"
                    role="menuitemradio"
                    aria-checked={level === candidate}
                    onClick={() => {
                      setPlaceNotificationLevel(key, candidate);
                      close();
                    }}
                    className={`flex w-full items-start gap-2 rounded-md px-2 py-1.5 text-left hover:bg-accent ${
                      level === candidate ? "bg-accent/60" : ""
                    }`}
                  >
                    {/* 選択中は色だけでなく形（✓）でも示す。 */}
                    <Check
                      aria-hidden
                      className={`mt-0.5 size-3.5 shrink-0 ${
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
                  </button>
                ))}
              </div>
            ) : null}
          </div>
        </>
      ) : null}
    </div>
  );
}
