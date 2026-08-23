import { Check, ChevronRight } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { isImeComposing } from "../../lib/ime";
import {
  type ParticipantStatus,
  STATUS_DURATIONS,
  type StatusKind,
} from "../model";
import { useMessaging } from "../store";
import { useOverlayPanel } from "./overlay";
import { ParticipantAvatar } from "./participant-avatar";
import { ParticipantProfilePopover } from "./participant-profile";

const SIDEBAR_PLACES = '[data-slot="sidebar-places"]';

export const STATUS_LABEL: Record<StatusKind, string> = {
  available: "対応可能",
  busy: "取り込み中",
  away: "離席中",
};

const STATUS_DOT: Record<StatusKind, string> = {
  available: "bg-emerald-500",
  busy: "bg-rose-500",
  away: "bg-amber-400",
};

/** 申告を一行で読む形にする。何も言っていない人は既定値で埋めない。 */
export function statusSummary(status: ParticipantStatus | undefined): string {
  if (!status) return "未設定";
  const label = STATUS_LABEL[status.status];
  return status.note ? `${label} — ${status.note}` : label;
}

/**
 * 期限の見せ方。今日のうちなら時刻だけ、日をまたぐなら日付から書く——
 * 「18:00まで」が明日の18時かもしれない、という読み違いを作らない。
 */
export function formatUntil(expiresAt: number, now: number): string {
  const target = new Date(expiresAt);
  const today = new Date(now);
  const hhmm = `${target.getHours()}:${String(target.getMinutes()).padStart(2, "0")}`;
  const sameDay =
    target.getFullYear() === today.getFullYear() &&
    target.getMonth() === today.getMonth() &&
    target.getDate() === today.getDate();
  if (sameDay) return `${hhmm}まで`;
  return `${target.getMonth() + 1}/${target.getDate()} ${hhmm}まで`;
}

/**
 * 期限が来たときに戻る先。いま出ているのが期限付きなら、その下に埋まっている
 * 宣言が戻る先で、期限なしならそれ自体が戻る先になる。サーバーが SetStatus の
 * 同じトランザクションで決めるのと同じ規則をここでも使う——先に見せた予告と
 * 後から返ってくる事実が食い違わないように。
 */
function lastingBeneath(
  current: ParticipantStatus | undefined,
): { status: StatusKind; note: string } | null {
  if (!current) return null;
  if (current.expiresAt === null) {
    return { status: current.status, note: current.note };
  }
  if (current.baseStatus === null) return null;
  return { status: current.baseStatus, note: current.baseNote };
}

/**
 * 期限付きの申告を今から送るとき、サーバーが保存する戻り先をそのまま予告する。
 * kind が次の申告と同じかは関係ない。同じ kind でも一言を戻すための base があれば、
 * 期限後に申告は残る。
 */
function expiryOutcome(
  base: { status: StatusKind; note: string } | null,
): string {
  if (!base) return "期限が来たら申告そのものが解除されます";
  const restored = base.note
    ? `${STATUS_LABEL[base.status]} — ${base.note}`
    : STATUS_LABEL[base.status];
  return `期限が来たら「${restored}」に戻ります`;
}

/**
 * 自分のステータス。三値の申告に「いつまで」と一言を足す。
 *
 * 期間を選ぶところまで一つのメニューに収め、選ぶ前に「期限が来たらどこへ戻るか」
 * を書いておく——「取り込み中を1時間」と言った人が、1時間後に自分が何になるかを
 * 押す前に知っている状態にしたい。
 */
export function StatusMenu() {
  const selfKey = useMessaging((state) => state.selfKey);
  const membersByKey = useMessaging((state) => state.membersByKey);
  const statusByKey = useMessaging((state) => state.statusByKey);
  const setStatus = useMessaging((state) => state.setStatus);
  const canSetStatus = useMessaging((state) => state.capabilities.status);
  const transportGeneration = useMessaging(
    (state) => state.transportGeneration,
  );
  const [open, setOpen] = useState(false);
  const [expanded, setExpanded] = useState<StatusKind | null>(null);
  const [note, setNote] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [failed, setFailed] = useState(false);
  const mutationToken = useRef(0);
  const overlay = useOverlayPanel<HTMLButtonElement>({
    open,
    onOpenChange: setOpen,
    scrollPassthrough: () =>
      document.querySelector<HTMLElement>(SIDEBAR_PLACES),
  });

  const selfProfile = membersByKey[selfKey];
  const selfStatus = statusByKey[selfKey];
  const base = lastingBeneath(selfStatus);

  // 開くたびに、いま出ている一言から書き始める。前回書きかけた文字を
  // 次に開いたときの発言にはしない。
  // biome-ignore lint/correctness/useExhaustiveDependencies: 開いた瞬間の値だけを読む
  useEffect(() => {
    if (open) setNote(selfStatus?.note ?? "");
    else setExpanded(null);
  }, [open]);

  // biome-ignore lint/correctness/useExhaustiveDependencies: transport generation is the authority-replacement signal.
  useEffect(() => {
    mutationToken.current += 1;
    setSubmitting(false);
    setFailed(false);
    setOpen(false);
  }, [transportGeneration]);

  const declare = async (
    kind: StatusKind,
    expiresAt: number | null,
    declarationNote = note.trim(),
  ) => {
    if (submitting) return;
    const token = ++mutationToken.current;
    const generation = transportGeneration;
    setSubmitting(true);
    try {
      await setStatus(kind, declarationNote, expiresAt);
      if (
        mutationToken.current !== token ||
        useMessaging.getState().transportGeneration !== generation
      ) {
        return;
      }
      setSubmitting(false);
      setFailed(false);
      setOpen(false);
    } catch {
      if (
        mutationToken.current !== token ||
        useMessaging.getState().transportGeneration !== generation
      ) {
        return;
      }
      setSubmitting(false);
      setFailed(true);
    }
  };

  return (
    <div className="relative">
      {open && canSetStatus ? (
        <div
          {...overlay.panelProps}
          role="dialog"
          aria-label="ステータス"
          className="absolute bottom-full left-0 z-10 mb-1 w-60 rounded-lg border border-border bg-background p-1 shadow-md"
        >
          <label className="block px-2 pt-1.5 pb-1">
            <span className="mb-1 block font-medium text-[11px] text-muted-foreground">
              ひとこと（任意）
            </span>
            <input
              value={note}
              disabled={submitting}
              onChange={(event) => setNote(event.target.value)}
              onKeyDown={(event) => {
                // 変換確定のEnterでメニューを閉じない。
                if (event.key === "Enter" && !isImeComposing(event)) {
                  event.preventDefault();
                  // ひとことだけ書き替えたい人の期限を、黙って外さない。
                  void declare(
                    selfStatus?.status ?? "available",
                    selfStatus?.expiresAt ?? null,
                  );
                }
              }}
              maxLength={200}
              placeholder="例: 会議中、17時に戻ります"
              className="w-full rounded-md border border-border bg-background px-2 py-1 text-[12.5px] outline-none placeholder:text-muted-foreground/60 focus-visible:border-ring/60"
            />
          </label>
          <div className="my-1 h-px bg-border/70" />
          {(Object.keys(STATUS_LABEL) as StatusKind[]).map((kind) => {
            const chosen = selfStatus?.status === kind;
            const submenuOpen = expanded === kind;
            return (
              <div
                key={kind}
                role="none"
                className="relative"
                onMouseEnter={() => setExpanded(kind)}
                onMouseLeave={() =>
                  setExpanded((value) => (value === kind ? null : value))
                }
              >
                <button
                  type="button"
                  disabled={submitting}
                  aria-haspopup="menu"
                  aria-expanded={submenuOpen}
                  onClick={() =>
                    setExpanded((value) => (value === kind ? null : kind))
                  }
                  className={`flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left text-[13px] transition-colors hover:bg-accent ${
                    submenuOpen ? "bg-accent" : ""
                  } ${chosen ? "font-medium" : ""}`}
                >
                  <span
                    className={`size-2 shrink-0 rounded-full ${STATUS_DOT[kind]}`}
                  />
                  {STATUS_LABEL[kind]}
                  <Check
                    aria-hidden
                    className={`ml-auto size-3.5 shrink-0 ${
                      chosen ? "opacity-100" : "opacity-0"
                    }`}
                  />
                  <ChevronRight className="size-3.5 shrink-0 text-muted-foreground" />
                </button>
                {submenuOpen ? (
                  <div
                    role="menu"
                    aria-label={`${STATUS_LABEL[kind]}の期間`}
                    className="absolute bottom-0 left-full z-20 ml-1 w-44 rounded-lg border border-border bg-background p-1 shadow-md"
                  >
                    {STATUS_DURATIONS.map((duration) => (
                      <button
                        key={duration.label}
                        type="button"
                        role="menuitem"
                        onClick={() =>
                          void declare(
                            kind,
                            duration.minutes === null
                              ? null
                              : Date.now() + duration.minutes * 60_000,
                          )
                        }
                        disabled={submitting}
                        className="flex w-full items-center rounded-md px-2 py-1.5 text-left text-[12.5px] transition-colors hover:bg-accent"
                      >
                        {duration.label}
                      </button>
                    ))}
                    <p className="px-2 pt-1 pb-0.5 text-[10.5px] text-muted-foreground/80">
                      {expiryOutcome(base)}
                    </p>
                  </div>
                ) : null}
              </div>
            );
          })}
          {selfStatus ? (
            <>
              <div className="my-1 h-px bg-border/70" />
              {/* 「解除」とは書かない。サーバーに宣言を取り消す操作は無く、
                  ここで起きるのは「対応可能」と言い直すことだから。 */}
              <button
                type="button"
                disabled={submitting}
                onClick={() => {
                  void declare("available", null, "");
                }}
                className="w-full rounded-md px-2 py-1.5 text-left text-[12.5px] text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
              >
                対応可能に戻す
              </button>
            </>
          ) : null}
          {failed ? (
            <p role="alert" className="px-2 py-1 text-[11px] text-rose-500">
              ステータスを更新できませんでした。もう一度お試しください
            </p>
          ) : null}
          {submitting ? (
            <p
              role="status"
              className="px-2 py-1 text-[11px] text-muted-foreground"
            >
              更新しています…
            </p>
          ) : null}
          <p className="px-2 pt-1 pb-0.5 text-[10px] text-muted-foreground/70">
            ステータスは自己申告。誰かが勝手に晒すことはありません
          </p>
        </div>
      ) : null}
      <div className="flex w-full items-center gap-2 rounded-md px-2 py-1.5">
        <ParticipantProfilePopover
          participantKey={selfKey}
          label={`${selfProfile?.displayName ?? "自分"}のプロフィール`}
          side="top"
          align="start"
          scrollPassthrough={() =>
            document.querySelector<HTMLElement>(SIDEBAR_PLACES)
          }
          className="flex shrink-0 rounded-full"
        >
          <ParticipantAvatar
            participantKey={selfKey}
            name={selfProfile?.displayName ?? "?"}
            size={26}
            status={selfStatus?.status}
          />
        </ParticipantProfilePopover>
        <button
          type="button"
          disabled={!canSetStatus}
          aria-haspopup="dialog"
          {...overlay.triggerProps}
          onClick={() => {
            if (canSetStatus) overlay.toggle();
          }}
          className="min-w-0 flex-1 rounded text-left outline-none focus-visible:ring-2 focus-visible:ring-ring/60 enabled:hover:bg-accent/60 disabled:cursor-default"
        >
          <span className="block truncate font-medium text-[13px]">
            {selfProfile?.displayName ?? "…"}
          </span>
          <span className="block truncate text-[11px] text-muted-foreground">
            {statusSummary(selfStatus)}
            {selfStatus?.expiresAt
              ? `（${formatUntil(selfStatus.expiresAt, Date.now())}）`
              : ""}
          </span>
        </button>
      </div>
    </div>
  );
}
