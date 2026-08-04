import { Bell, Check, Volume2, VolumeX, X } from "lucide-react";
import { useCallback, useEffect, useRef, useState } from "react";
import { isImeComposing } from "../../lib/ime";
import type { NotificationLevel } from "../model";
import { useMessaging } from "../store";
import { useOverlayPanel } from "./overlay";
import { NOTIFICATION_LEVEL_LABEL } from "./sidebar";

const FEEDBACK_MS = 2_400;

/**
 * 選択中であることを色だけに預けない。形（✓）と太さで示し、実体も radio に
 * するので支援技術と矢印キーがそのまま効く。
 */
function LevelOption({
  level,
  selected,
  onSelect,
}: {
  level: NotificationLevel;
  selected: boolean;
  onSelect: () => void;
}) {
  return (
    <label
      className={`flex w-full cursor-pointer items-center gap-2 rounded-md px-2 py-1.5 text-left text-[13px] transition-colors hover:bg-accent active:bg-accent has-[:focus-visible]:ring-2 has-[:focus-visible]:ring-ring/60 ${
        selected ? "bg-accent/60 font-medium" : ""
      }`}
    >
      <input
        type="radio"
        name="notification-default-level-choice"
        checked={selected}
        onChange={onSelect}
        className="sr-only"
      />
      <Check
        aria-hidden
        className={`size-3.5 shrink-0 transition-opacity ${
          selected ? "opacity-100" : "opacity-0"
        }`}
      />
      {NOTIFICATION_LEVEL_LABEL[level]}
    </label>
  );
}

/** オン・オフをノブの位置で示すトグル。色が見えなくても状態が分かる。 */
function SoundToggle({
  enabled,
  onToggle,
}: {
  enabled: boolean;
  onToggle: () => void;
}) {
  return (
    <button
      type="button"
      role="switch"
      aria-checked={enabled}
      onClick={onToggle}
      className="mt-3 flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left text-[13px] transition-colors hover:bg-accent active:bg-accent"
    >
      {enabled ? (
        <Volume2 aria-hidden className="size-3.5" />
      ) : (
        <VolumeX aria-hidden className="size-3.5 text-muted-foreground" />
      )}
      通知音
      <span className="ml-auto text-[11px] text-muted-foreground">
        この端末だけ
      </span>
      <span
        aria-hidden
        className={`flex h-4 w-7 shrink-0 items-center rounded-full p-0.5 transition-colors ${
          enabled ? "bg-primary" : "bg-muted-foreground/30"
        }`}
      >
        <span
          className={`size-3 rounded-full bg-background shadow-sm transition-transform ${
            enabled ? "translate-x-3" : "translate-x-0"
          }`}
        />
      </span>
    </button>
  );
}

/** 既定のレベル・keyword・音。placeごとの上書きはサイドバー側にある。 */
export function NotificationSettingsMenu() {
  const defaultLevel = useMessaging((state) => state.notificationDefaultLevel);
  const keywords = useMessaging((state) => state.notificationKeywords);
  const soundEnabled = useMessaging((state) => state.notificationSoundEnabled);
  const setDefaultLevel = useMessaging(
    (state) => state.setNotificationDefaultLevel,
  );
  const setKeywords = useMessaging((state) => state.setNotificationKeywords);
  const setSoundEnabled = useMessaging(
    (state) => state.setNotificationSoundEnabled,
  );
  const [open, setOpen] = useState(false);
  const [draft, setDraft] = useState("");
  const [feedback, setFeedback] = useState("");
  const feedbackTimer = useRef<number | null>(null);
  const overlay = useOverlayPanel<HTMLButtonElement>({
    open,
    onOpenChange: setOpen,
  });

  // 変更が届いたことを言葉で返す。押した瞬間の見た目だけだと確信が持てない。
  const flash = useCallback((message: string) => {
    setFeedback(message);
    if (feedbackTimer.current) window.clearTimeout(feedbackTimer.current);
    feedbackTimer.current = window.setTimeout(
      () => setFeedback(""),
      FEEDBACK_MS,
    );
  }, []);

  useEffect(
    () => () => {
      if (feedbackTimer.current) window.clearTimeout(feedbackTimer.current);
    },
    [],
  );

  const addKeyword = () => {
    const value = draft.trim();
    if (!value || keywords.includes(value)) {
      setDraft("");
      return;
    }
    setKeywords([...keywords, value]);
    setDraft("");
    flash(`「${value}」で呼ばれるようにしました`);
  };

  return (
    <div className="relative">
      <button
        type="button"
        title="通知設定"
        aria-haspopup="dialog"
        {...overlay.triggerProps}
        className={`flex size-8 items-center justify-center rounded-md transition-colors hover:bg-accent ${
          open ? "bg-accent text-foreground" : "text-muted-foreground"
        }`}
      >
        <Bell className="size-4" />
      </button>
      {open ? (
        <div
          {...overlay.panelProps}
          role="dialog"
          aria-label="通知設定"
          className="absolute top-full right-0 z-20 mt-1 w-72 rounded-lg border border-border bg-background p-2 shadow-md"
        >
          <p
            id="notification-default-level"
            className="pb-1 font-medium text-[11px] text-muted-foreground"
          >
            既定の通知
          </p>
          <div role="radiogroup" aria-labelledby="notification-default-level">
            {(Object.keys(NOTIFICATION_LEVEL_LABEL) as NotificationLevel[]).map(
              (level) => (
                <LevelOption
                  key={level}
                  level={level}
                  selected={defaultLevel === level}
                  onSelect={() => {
                    setDefaultLevel(level);
                    flash(
                      `既定を「${NOTIFICATION_LEVEL_LABEL[level]}」にしました`,
                    );
                  }}
                />
              ),
            )}
          </div>
          <p className="pt-3 pb-1 font-medium text-[11px] text-muted-foreground">
            キーワード — 名前以外で呼ばれたい言葉
          </p>
          <div className="flex flex-wrap gap-1 pb-1">
            {keywords.map((keyword) => (
              <span
                key={keyword}
                className="flex items-center gap-1 rounded-full bg-muted px-2 py-0.5 text-[12px]"
              >
                {keyword}
                <button
                  type="button"
                  aria-label={`${keyword} を外す`}
                  onClick={() => {
                    setKeywords(keywords.filter((entry) => entry !== keyword));
                    flash(`「${keyword}」を外しました`);
                  }}
                  className="text-muted-foreground hover:text-foreground"
                >
                  <X className="size-3" />
                </button>
              </span>
            ))}
          </div>
          <input
            value={draft}
            onChange={(event) => setDraft(event.target.value)}
            onKeyDown={(event) => {
              // IME変換確定のEnterはタグ追加にしない。
              if (isImeComposing(event)) return;
              if (event.key !== "Enter") return;
              event.preventDefault();
              addKeyword();
            }}
            onBlur={addKeyword}
            placeholder="追加して Enter"
            aria-label="通知キーワードを追加"
            className="w-full rounded-md border border-border bg-transparent px-2 py-1 text-[12.5px] outline-none focus:border-muted-foreground/60"
          />
          <SoundToggle
            enabled={soundEnabled}
            onToggle={() => {
              setSoundEnabled(!soundEnabled);
              flash(soundEnabled ? "通知音を止めました" : "通知音を鳴らします");
            }}
          />
          <p
            role="status"
            aria-live="polite"
            className="min-h-4 px-2 pt-1.5 text-[11px] text-muted-foreground"
          >
            {feedback}
          </p>
        </div>
      ) : null}
    </div>
  );
}
