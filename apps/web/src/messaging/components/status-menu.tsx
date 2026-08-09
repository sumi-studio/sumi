import { ChevronRight } from "lucide-react";
import { useEffect, useId, useRef, useState } from "react";
import type { ParticipantStatus, StatusKind } from "../model";
import { STATUS_DURATIONS } from "../model";
import { useMessaging } from "../store";
import {
  ParticipantAvatar,
  STATUS_DOT,
  STATUS_LABEL,
} from "./participant-avatar";
import { SettingsMenuItem } from "./settings-overlay";

const STATUS_HINT: Record<StatusKind, string> = {
  available: "話しかけて大丈夫です",
  busy: "急ぎでなければ後にしてください",
  away: "いま席にいません",
};

/**
 * 「◯◯まで」の表示。今日のうちなら時刻だけ、日をまたぐなら日付から出す——
 * 「18:00まで」が明後日のことだと分からないのが一番困る。
 */
export function formatUntil(expiresAt: number, now: number): string {
  const target = new Date(expiresAt);
  const sameDay = new Date(now).toDateString() === target.toDateString();
  const time = target.toLocaleTimeString("ja-JP", {
    hour: "2-digit",
    minute: "2-digit",
  });
  if (sameDay) return `${time}まで`;
  const date = target.toLocaleDateString("ja-JP", {
    month: "numeric",
    day: "numeric",
  });
  return `${date} ${time}まで`;
}

/** ステータス行に出す一行。宣言が無いときは何も言っていないと書く。 */
export function statusSummary(
  status: ParticipantStatus | undefined,
  now: number,
): string {
  if (!status) return "ステータス未設定";
  const parts = [STATUS_LABEL[status.status]];
  if (status.note) parts.push(status.note);
  if (status.expiresAt !== null && status.expiresAt > now) {
    parts.push(formatUntil(status.expiresAt, now));
  }
  return parts.join(" — ");
}

/**
 * 左下のアカウント領域から開く自己申告のパネル。状態を選び、必要なら
 * 「◯◯まで」の期間を付ける。期間が切れたらサーバーが直前の宣言へ戻すので、
 * 手で戻しに来る必要はない。
 *
 * 「オンライン状態を隠す」に当たる状態は置いていない。Sumiは在席を観測しない
 * ので隠すべき自動の表示が無く、4つ目の状態を足すと「本当は見えている何かが
 * ある」という誤解だけが残る（ステータスは自己申告）。
 */
export function StatusMenu({
  open,
  onOpenChange,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  const selfKey = useMessaging((state) => state.selfKey);
  const membersByKey = useMessaging((state) => state.membersByKey);
  const statusByKey = useMessaging((state) => state.statusByKey);
  const setStatus = useMessaging((state) => state.setStatus);
  const containerRef = useRef<HTMLDivElement>(null);
  const [submenuFor, setSubmenuFor] = useState<StatusKind | null>(null);
  const [note, setNote] = useState("");
  const submenuId = useId();
  const [now, setNow] = useState(() => Date.now());

  const selfStatus = statusByKey[selfKey];
  const profile = membersByKey[selfKey];
  const selfStatusRef = useRef(selfStatus);
  selfStatusRef.current = selfStatus;

  useEffect(() => {
    if (!open) {
      setSubmenuFor(null);
      return;
    }
    // パネルを開いた瞬間の値だけを下書きへ写す。開いた後の status_updated は
    // 表示には反映しても、入力中のひとことを上書きしてはならない。
    setNote(selfStatusRef.current?.note ?? "");
  }, [open]);

  useEffect(() => {
    if (!open) return;
    // 残り時間の表示だけのための更新。開いている間しか動かさない。
    const timer = window.setInterval(() => setNow(Date.now()), 30_000);
    const closeOnOutsideClick = (event: MouseEvent) => {
      if (!containerRef.current?.contains(event.target as Node)) {
        onOpenChange(false);
      }
    };
    const closeOnEscape = (event: KeyboardEvent) => {
      if (event.key !== "Escape") return;
      setSubmenuFor((current) => {
        if (current) return null;
        onOpenChange(false);
        return null;
      });
    };
    window.addEventListener("mousedown", closeOnOutsideClick);
    window.addEventListener("keydown", closeOnEscape);
    return () => {
      window.clearInterval(timer);
      window.removeEventListener("mousedown", closeOnOutsideClick);
      window.removeEventListener("keydown", closeOnEscape);
    };
  }, [open, onOpenChange]);

  if (!open) return null;

  const declare = (kind: StatusKind, minutes: number | null) => {
    setStatus(
      kind,
      note.trim(),
      minutes === null ? null : Date.now() + minutes * 60_000,
    );
    setSubmenuFor(null);
    onOpenChange(false);
  };

  return (
    <div
      ref={containerRef}
      role="menu"
      aria-label="ステータス"
      className="absolute bottom-full left-2 z-30 mb-1 w-64 rounded-lg border border-border bg-background p-1 shadow-md"
    >
      <div className="flex items-center gap-2 px-2 py-1.5">
        <ParticipantAvatar
          participantKey={selfKey}
          name={profile?.displayName ?? "?"}
          size={28}
          status={selfStatus?.status}
        />
        <span className="min-w-0 flex-1">
          <span className="block truncate font-medium text-[13px]">
            {profile?.displayName ?? "…"}
          </span>
          <span className="block truncate text-[11px] text-muted-foreground">
            {statusSummary(selfStatus, now)}
          </span>
        </span>
      </div>
      {selfStatus?.expiresAt !== null &&
      selfStatus?.expiresAt !== undefined &&
      selfStatus.baseStatus ? (
        <p className="px-2 pb-1 text-[11px] text-muted-foreground/80">
          期限が来たら「{STATUS_LABEL[selfStatus.baseStatus]}」に戻ります
        </p>
      ) : null}
      <div className="my-1 h-px bg-border/70" />
      <label className="block px-2 py-1">
        <span className="mb-1 block text-[11px] text-muted-foreground">
          ひとこと（任意）
        </span>
        <input
          value={note}
          onChange={(event) => setNote(event.target.value)}
          maxLength={200}
          placeholder="例: 会議中"
          aria-label="ステータスのひとこと"
          className="w-full rounded-md border border-border bg-background px-2 py-1 text-[12.5px] outline-none placeholder:text-muted-foreground/60 focus-visible:border-ring/60"
        />
      </label>
      <div className="my-1 h-px bg-border/70" />
      {(Object.keys(STATUS_LABEL) as StatusKind[]).map((kind) => (
        <div
          key={kind}
          role="none"
          className="relative"
          onMouseEnter={() => setSubmenuFor(kind)}
          onMouseLeave={() => setSubmenuFor(null)}
        >
          <button
            type="button"
            role="menuitem"
            aria-haspopup="menu"
            aria-expanded={submenuFor === kind}
            aria-controls={`${submenuId}-${kind}`}
            onClick={() =>
              setSubmenuFor((current) => (current === kind ? null : kind))
            }
            className={`flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left hover:bg-accent ${
              submenuFor === kind ? "bg-accent" : ""
            }`}
          >
            <span
              className={`size-2 shrink-0 rounded-full ${STATUS_DOT[kind]}`}
            />
            <span className="min-w-0 flex-1">
              <span
                className={`block truncate text-[13px] ${
                  selfStatus?.status === kind ? "font-medium" : ""
                }`}
              >
                {STATUS_LABEL[kind]}
              </span>
              <span className="block truncate text-[11px] text-muted-foreground">
                {STATUS_HINT[kind]}
              </span>
            </span>
            <ChevronRight className="size-3.5 shrink-0 text-muted-foreground" />
          </button>
          {submenuFor === kind ? (
            <div
              id={`${submenuId}-${kind}`}
              role="menu"
              aria-label={`${STATUS_LABEL[kind]}の期間`}
              className="absolute bottom-0 left-full z-40 ml-1 w-40 rounded-lg border border-border bg-background p-1 shadow-md"
            >
              <p className="px-2 pt-1 pb-1 text-[11px] text-muted-foreground">
                いつまで
              </p>
              {STATUS_DURATIONS.map((duration) => (
                <button
                  key={duration.label}
                  type="button"
                  role="menuitem"
                  onClick={() => declare(kind, duration.minutes)}
                  className="block w-full rounded-md px-2 py-1.5 text-left text-[13px] hover:bg-accent"
                >
                  {duration.label}
                </button>
              ))}
            </div>
          ) : null}
        </div>
      ))}
      <p className="px-2 pt-1 pb-0.5 text-[10px] text-muted-foreground/70">
        ステータスは自己申告。誰かが勝手に晒すことはありません
      </p>
      <div className="mt-1 border-border/70 border-t pt-1">
        <SettingsMenuItem onOpened={() => onOpenChange(false)} />
      </div>
    </div>
  );
}
