import { useMemo, useState } from "react";
import { participantKey } from "../model";
import { usePlaceNavigate } from "../place-route";
import { useMessaging } from "../store";
import { ParticipantAvatar } from "./participant-avatar";

/**
 * メンバーリスト。人間とagentを同じ「参加者」として一つのリストに並べる。
 * bot欄のような区別は作らない。見えるステータスは本人が申告したものだけ。
 */
export function MemberList() {
  const membersByKey = useMessaging((state) => state.membersByKey);
  const statusByKey = useMessaging((state) => state.statusByKey);
  const selfKey = useMessaging((state) => state.selfKey);
  const startDM = useMessaging((state) => state.startDM);
  const placeNavigate = usePlaceNavigate();
  const [pendingKey, setPendingKey] = useState<string | null>(null);
  const [failedKey, setFailedKey] = useState<string | null>(null);

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
      <div className="scrollbar-ui min-h-0 flex-1 overflow-y-auto p-2">
        {members.map((member) => {
          const key = participantKey(member.participant);
          const status = statusByKey[key];
          const content = (
            <>
              <ParticipantAvatar
                participantKey={key}
                name={member.displayName}
                size={28}
                status={status?.status ?? "available"}
              />
              <span className="min-w-0 flex-1">
                <span className="block truncate font-medium text-[13px]">
                  {member.displayName}
                  {key === selfKey ? (
                    <span className="ml-1 text-[10px] text-muted-foreground">
                      (自分)
                    </span>
                  ) : null}
                </span>
                {failedKey === key ? (
                  <span
                    role="alert"
                    aria-live="assertive"
                    className="block truncate text-[11px] text-rose-500"
                  >
                    DMを開始できませんでした。もう一度押してください
                  </span>
                ) : (
                  <span className="block truncate text-[11px] text-muted-foreground">
                    {pendingKey === key
                      ? "DMを開始しています…"
                      : status?.note
                        ? status.note
                        : member.tagline}
                  </span>
                )}
              </span>
            </>
          );
          if (key === selfKey) {
            return (
              <div
                key={key}
                className="flex items-center gap-2.5 rounded-md px-2 py-1.5"
              >
                {content}
              </div>
            );
          }
          return (
            <button
              key={key}
              type="button"
              title={`${member.displayName}にDMを送る`}
              aria-label={`${member.displayName}にDMを送る`}
              aria-busy={pendingKey === key}
              disabled={pendingKey !== null}
              onClick={async () => {
                setPendingKey(key);
                setFailedKey(null);
                try {
                  const place = await startDM([member.participant]);
                  placeNavigate(place);
                } catch {
                  setFailedKey(key);
                } finally {
                  setPendingKey(null);
                }
              }}
              className="flex w-full items-center gap-2.5 rounded-md px-2 py-1.5 text-left hover:bg-accent/60 disabled:opacity-60"
            >
              {content}
            </button>
          );
        })}
      </div>
    </aside>
  );
}
