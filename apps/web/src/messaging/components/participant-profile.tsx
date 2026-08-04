import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@sumi/ui/components/popover";
import { type ReactNode, useState } from "react";
import { usePlaceNavigate } from "../place-route";
import { useMessaging } from "../store";
import {
  ParticipantAvatar,
  STATUS_DOT,
  STATUS_LABEL,
} from "./participant-avatar";

/**
 * 参加者プロフィールカード。人間と人格agentで完全に同じカードを出す
 * （ADR 0008 §1 の同型性: bot badgeのような種別UIは作らない）。
 *
 * 参加者が何者かは、システムが付ける分類（ParticipantRefのkind）ではなく、
 * 本人の側にあるもの——表示名・職務(tagline)・自己申告ステータス——で表す。
 * kindはmap keyとavatarの色にだけ効き、文字としては一切現れない。
 */

function ParticipantProfileCard({
  participantKey: key,
  onDone,
}: {
  participantKey: string;
  onDone: () => void;
}) {
  const member = useMessaging((state) => state.membersByKey[key]);
  const status = useMessaging((state) => state.statusByKey[key]);
  const selfKey = useMessaging((state) => state.selfKey);
  const startDM = useMessaging((state) => state.startDM);
  const placeNavigate = usePlaceNavigate();
  const [busy, setBusy] = useState(false);
  const [failed, setFailed] = useState(false);

  if (!member) {
    return (
      <p className="px-2 py-3 text-[12.5px] text-muted-foreground">
        この参加者の情報がまだありません
      </p>
    );
  }

  const own = key === selfKey;

  const openDM = async () => {
    if (busy) return;
    setBusy(true);
    setFailed(false);
    try {
      const placeKey = await startDM([member.participant]);
      placeNavigate(placeKey);
      onDone();
    } catch {
      setFailed(true);
      setBusy(false);
    }
  };

  return (
    <div className="flex flex-col gap-2.5 p-1.5">
      <div className="flex items-start gap-3">
        <ParticipantAvatar
          participantKey={key}
          name={member.displayName}
          size={48}
          status={status?.status}
        />
        <div className="min-w-0 flex-1 pt-0.5">
          <p className="truncate font-semibold text-[15px]">
            {member.displayName}
            {own ? (
              <span className="ml-1.5 font-normal text-[11px] text-muted-foreground">
                (自分)
              </span>
            ) : null}
          </p>
          {member.tagline ? (
            <p className="mt-0.5 break-words text-[12.5px] text-muted-foreground">
              {member.tagline}
            </p>
          ) : null}
        </div>
      </div>
      {status ? (
        <p className="flex items-start gap-1.5 border-border/70 border-t pt-2 text-[12.5px]">
          <span
            className={`mt-1.5 size-2 shrink-0 rounded-full ${STATUS_DOT[status.status]}`}
          />
          <span className="min-w-0 break-words">
            {STATUS_LABEL[status.status]}
            {status.note ? (
              <span className="text-muted-foreground">
                {" — "}
                {status.note}
              </span>
            ) : null}
          </span>
        </p>
      ) : null}
      {own ? null : (
        <div>
          <button
            type="button"
            onClick={() => void openDM()}
            disabled={busy}
            className="w-full rounded-md bg-primary px-2.5 py-1.5 font-medium text-[12.5px] text-primary-foreground transition-opacity hover:opacity-90 disabled:opacity-50"
          >
            DMを送る
          </button>
          {failed ? (
            <p className="mt-1 text-[11px] text-rose-500">
              DMを開けませんでした
            </p>
          ) : null}
        </div>
      )}
    </div>
  );
}

/**
 * 参加者を指すあらゆる表示（アバター・著者名・メンバーリストの行）を
 * プロフィールカードの開き口にする。Esc・外側クリックで閉じる挙動は
 * @sumi/ui のPopover（Base UI）標準に従う。
 */
export function ParticipantProfilePopover({
  participantKey: key,
  label,
  className,
  side = "bottom",
  align = "start",
  children,
}: {
  participantKey: string;
  /** trigger自体が文字を持たない時（アバターだけ等）のaria-label。 */
  label?: string;
  className?: string;
  side?: "top" | "bottom" | "left" | "right";
  align?: "start" | "center" | "end";
  children: ReactNode;
}) {
  const [open, setOpen] = useState(false);
  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger
        render={
          <button
            type="button"
            aria-label={label}
            className={`text-left outline-none focus-visible:ring-2 focus-visible:ring-ring/60 ${className ?? ""}`}
          />
        }
      >
        {children}
      </PopoverTrigger>
      <PopoverContent side={side} align={align} className="w-64 p-2">
        <ParticipantProfileCard
          participantKey={key}
          onDone={() => setOpen(false)}
        />
      </PopoverContent>
    </Popover>
  );
}
