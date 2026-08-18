import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@sumi/ui/components/popover";
import { type ReactNode, useState } from "react";
import type { ParticipantKey } from "../model";
import { usePlaceNavigate } from "../place-route";
import { getMessagingSessionIdentity, useMessaging } from "../store";
import { useWheelPassthrough } from "./overlay";
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
  participantKey: ParticipantKey;
  onDone: () => void;
}) {
  const member = useMessaging((state) => state.membersByKey[key]);
  const status = useMessaging((state) => state.statusByKey[key]);
  const selfKey = useMessaging((state) => state.selfKey);
  const startDM = useMessaging((state) => state.startDM);
  const dmPending = useMessaging((state) => state.startingDM !== null);
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

  // DM開始の保留はstoreに一つだけある。メンバーリストの行から始まった
  // 保留も同じ一つなので、このカードから2本目が走ることはない。
  //
  // DM遷移はstoreの完了だけでは足りない。待っている間にidentityが
  // 入れ替われば、それは別人のauthorityで開くDMになる（sidebar・member
  // listのDM導線と同じfence）。
  const openDM = async () => {
    if (busy || dmPending) return;
    const currentIdentity = getMessagingSessionIdentity();
    const expectedSelfKey = selfKey;
    setBusy(true);
    setFailed(false);
    try {
      const place = await startDM([member.participant]);
      const sessionChanged =
        getMessagingSessionIdentity() !== currentIdentity ||
        useMessaging.getState().selfKey !== expectedSelfKey;
      if (sessionChanged) {
        throw new Error("Messaging session changed before DM navigation");
      }
      placeNavigate(place);
      onDone();
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
            disabled={busy || dmPending}
            aria-busy={busy}
            className="w-full rounded-md bg-primary px-2.5 py-1.5 font-medium text-[12.5px] text-primary-foreground transition-opacity hover:opacity-90 disabled:opacity-50"
          >
            DMを送る
          </button>
          {failed ? (
            <p
              role="alert"
              aria-live="assertive"
              className="mt-1 text-[11px] text-rose-500"
            >
              DMを開けませんでした
            </p>
          ) : null}
        </div>
      )}
    </div>
  );
}

/** カードが開いた時点の束縛。誰を、誰のauthorityで見ているか。 */
interface ProfileBinding {
  selfKey: ParticipantKey;
  key: ParticipantKey;
}

/** 既定の転送先。開いた側が渡さなければ、カードの上のホイールは何も動かさない。 */
const noPassthrough = () => null;

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
  scrollPassthrough = noPassthrough,
  children,
}: {
  participantKey: ParticipantKey;
  /** trigger自体が文字を持たない時（アバターだけ等）のaria-label。 */
  label?: string;
  className?: string;
  side?: "top" | "bottom" | "left" | "right";
  align?: "start" | "center" | "end";
  /**
   * カードの上のホイールを渡す先。カードはportalで開くので、下に何がある
   * かは開いた側しか知らない。既定は転送しない——会話欄から開いたときだけ
   * 会話欄を渡す。
   */
  scrollPassthrough?: () => HTMLElement | null;
  children: ReactNode;
}) {
  const selfKey = useMessaging((state) => state.selfKey);
  const passthroughRef = useWheelPassthrough<HTMLDivElement>(scrollPassthrough);
  // カードは「誰を、誰のauthorityで見ているか」に束縛する。開いているか
  // どうかは、その束縛が今のprops/storeと一致しているかというrender時の
  // 関数で決まる。effectで後から閉じると、閉じるまでの1コミットで別人の
  // プロフィールが開いた枠に描かれてしまう。
  const [openedFor, setOpenedFor] = useState<ProfileBinding | null>(null);
  const open =
    openedFor !== null &&
    openedFor.selfKey === selfKey &&
    openedFor.key === key;
  if (openedFor !== null && !open) {
    // 一致しなくなった束縛はこのrenderで捨てる（Reactはcommit前に描き直す）。
    setOpenedFor(null);
  }
  const setOpen = (next: boolean) =>
    setOpenedFor(next ? { selfKey, key } : null);

  return (
    // 枠そのものを束縛でkeyする。閉じる時のPopoverは中身を残したまま
    // 消えていくので、keyを持たないと束縛が変わった瞬間に「消えかけの枠に
    // 別人が描き直される」。束縛が変われば枠ごと別物として作り直す。
    <Popover key={`${selfKey}\u001f${key}`} open={open} onOpenChange={setOpen}>
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
      <PopoverContent
        ref={passthroughRef}
        side={side}
        align={align}
        className="w-64 p-2"
      >
        <ParticipantProfileCard
          participantKey={key}
          onDone={() => setOpen(false)}
        />
      </PopoverContent>
    </Popover>
  );
}
