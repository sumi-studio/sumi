import {
  Check,
  ChevronRight,
  Copy,
  MoreVertical,
  Pencil,
  Plus,
} from "lucide-react";
import { useEffect, useId, useState } from "react";
import type { NotificationLevel, PlaceKey } from "../model";
import { notificationLevelFor, useMessaging } from "../store";
import { useOverlayPanel } from "./overlay";

export const NOTIFICATION_LEVEL_LABEL: Record<NotificationLevel, string> = {
  all: "すべて通知",
  mentions: "メンションのみ",
  mute: "ミュート",
};

const NOTIFICATION_LEVEL_HINT: Record<NotificationLevel, string> = {
  all: "この場所の発言で呼ばれます",
  mentions: "名前を呼ばれたときだけ",
  mute: "通知も未読バッジも出しません",
};

const ITEM_CLASS =
  "flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left text-[13px] transition-colors hover:bg-accent";

/**
 * placeの操作メニュー。右クリックとホバーの「…」の両方から開く——右クリックは
 * Discordを知っている手が最初に試す操作で、ホバーは知らない手が見つけられる導線。
 *
 * 場所そのものをどうするか（編集・複製・作成）を主メニューに置き、通知は横に開く
 * サブメニューへ送る。通知は場所の設定ではなく受け手の設定で、効く範囲も持ち主も
 * 違うものを同じ高さに並べると選び間違える。
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
  const [submenuOpen, setSubmenuOpen] = useState(false);
  const setPlaceNotificationLevel = useMessaging(
    (state) => state.setPlaceNotificationLevel,
  );
  const submenuId = useId();
  const overlay = useOverlayPanel<HTMLButtonElement>({
    open,
    onOpenChange,
    // このパネルはサイドバーのスクロール領域内にある。
    scrollPassthrough: () => null,
  });

  useEffect(() => {
    if (!open) setSubmenuOpen(false);
  }, [open]);

  const close = () => {
    setSubmenuOpen(false);
    onOpenChange(false);
  };

  return (
    <div className="relative">
      <button
        type="button"
        aria-label="この場所のメニュー"
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
              {/* 横に開くサブメニュー。ホバーでもクリックでも開く——
                  指す手と押す手のどちらにも同じ入口がある。 */}
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
                    className="absolute top-0 right-full z-40 mr-1 w-56 rounded-lg border border-border bg-background p-1 shadow-md"
                  >
                    {(
                      Object.keys(
                        NOTIFICATION_LEVEL_LABEL,
                      ) as NotificationLevel[]
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
                        className={`flex w-full items-start gap-2 rounded-md px-2 py-1.5 text-left transition-colors hover:bg-accent ${
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
      ) : null}
    </div>
  );
}
