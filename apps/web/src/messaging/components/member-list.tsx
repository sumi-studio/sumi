import { useMemo, useState } from "react";
import { participantKey } from "../model";
import { usePlaceNavigate } from "../place-route";
import { getMessagingSessionIdentity, useMessaging } from "../store";
import { ParticipantAvatar } from "./participant-avatar";
import { ParticipantProfilePopover } from "./participant-profile";

const MEMBER_LIST = '[data-slot="member-list"]';

/** メンバーリストから開いたカードが覆う面。 */
const memberListScroller = () =>
  document.querySelector<HTMLElement>(MEMBER_LIST);

/**
 * メンバーリスト。人間とagentを同じ「参加者」として一つのリストに並べる。
 * bot欄のような区別は作らない。見えるステータスは本人が申告したものだけ。
 */
export function MemberList() {
  const membersByKey = useMessaging((state) => state.membersByKey);
  const statusByKey = useMessaging((state) => state.statusByKey);
  const selfKey = useMessaging((state) => state.selfKey);
  const startDM = useMessaging((state) => state.startDM);
  const startingDM = useMessaging((state) => state.startingDM);
  const placeNavigate = usePlaceNavigate();
  const [failedKey, setFailedKey] = useState<string | null>(null);

  // 保留はstoreに一つだけある。行もカードも同じ一つを見るので、
  // 保留中に別の入口から2本目のstartDMが走ることはない。
  const pendingKey =
    startingDM?.participants.length === 1
      ? participantKey(startingDM.participants[0])
      : null;
  const dmPending = startingDM !== null;

  const members = useMemo(
    () =>
      Object.values(membersByKey).sort((a, b) =>
        a.displayName.localeCompare(b.displayName, "ja"),
      ),
    [membersByKey],
  );

  return (
    <aside className="hidden w-56 shrink-0 flex-col border-border/70 border-l bg-muted/20 lg:flex">
      <p className="shrink-0 px-4 pt-3 pb-1 font-medium text-[12px] text-muted-foreground">
        メンバー — {members.length}
      </p>
      <div
        data-slot="member-list"
        className="scrollbar-ui min-h-0 flex-1 overflow-y-auto p-2"
      >
        {members.map((member) => {
          const key = participantKey(member.participant);
          const status = statusByKey[key];
          // アバターはプロフィールカードの開き口、行の残りは従来どおり
          // DMの開始。役割の違う2つのbuttonを横に並べる（入れ子は作らない）。
          const avatar = (
            <ParticipantProfilePopover
              participantKey={key}
              label={`${member.displayName}のプロフィール`}
              side="left"
              align="start"
              scrollPassthrough={memberListScroller}
              className="flex shrink-0 rounded-full"
            >
              <ParticipantAvatar
                participantKey={key}
                name={member.displayName}
                size={28}
                status={status?.status}
              />
            </ParticipantProfilePopover>
          );
          const content = (
            <span className="min-w-0 flex-1">
              <span className="block truncate font-medium text-[13px]">
                {member.displayName}
                {key === selfKey ? (
                  <span className="ml-1 text-[10px] text-muted-foreground">
                    (自分)
                  </span>
                ) : null}
              </span>
              <span className="block truncate text-[11px] text-muted-foreground">
                {pendingKey === key
                  ? "DMを開始しています…"
                  : status?.note
                    ? status.note
                    : member.tagline}
              </span>
            </span>
          );
          if (key === selfKey) {
            return (
              <div
                key={key}
                className="flex items-center gap-2.5 rounded-md px-2 py-1.5"
              >
                {avatar}
                {content}
              </div>
            );
          }
          return (
            <div key={key}>
              <div className="flex w-full items-center gap-2.5 rounded-md px-2 py-1.5 transition-colors hover:bg-accent/60">
                {avatar}
                <button
                  type="button"
                  title={`${member.displayName}にDMを送る`}
                  aria-label={`${member.displayName}にDMを送る`}
                  aria-busy={pendingKey === key}
                  disabled={dmPending}
                  onClick={async () => {
                    const currentIdentity = getMessagingSessionIdentity();
                    const expectedSelfKey = selfKey;
                    setFailedKey(null);
                    try {
                      const place = await startDM([member.participant]);
                      const sessionChanged =
                        getMessagingSessionIdentity() !== currentIdentity ||
                        useMessaging.getState().selfKey !== expectedSelfKey;
                      if (sessionChanged) {
                        throw new Error(
                          "Messaging session changed before DM navigation",
                        );
                      }
                      placeNavigate(place);
                    } catch {
                      if (
                        getMessagingSessionIdentity() === currentIdentity &&
                        useMessaging.getState().selfKey === expectedSelfKey
                      ) {
                        setFailedKey(key);
                      }
                    }
                  }}
                  className="flex min-w-0 flex-1 items-center gap-2.5 rounded text-left outline-none focus-visible:ring-2 focus-visible:ring-ring/60 disabled:opacity-60"
                >
                  {content}
                </button>
              </div>
              {failedKey === key ? (
                <p
                  role="alert"
                  aria-live="assertive"
                  className="px-2 pb-1 text-[11px] text-rose-500"
                >
                  DMを開始できませんでした。もう一度押してください
                </p>
              ) : null}
            </div>
          );
        })}
      </div>
    </aside>
  );
}
