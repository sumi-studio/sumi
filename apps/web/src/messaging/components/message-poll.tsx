import { BarChart3, Check, Users } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import type { Message } from "../model";
import { isPollClosed, participantKey, pollVoterCount } from "../model";
import { useMessaging } from "../store";

const CLOSE_FORMAT = new Intl.DateTimeFormat("ja-JP", {
  month: "numeric",
  day: "numeric",
  hour: "2-digit",
  minute: "2-digit",
});

function deadlineLabel(closesAt: number, now: number): string {
  if (now >= closesAt) return "締め切りました";
  const minutes = Math.max(1, Math.ceil((closesAt - now) / 60_000));
  if (minutes < 60) return `あと${minutes}分`;
  if (minutes < 24 * 60) return `あと${Math.ceil(minutes / 60)}時間`;
  return `${CLOSE_FORMAT.format(closesAt)}まで`;
}

export function MessagePoll({ message }: { message: Message }) {
  const poll = message.poll;
  const selfKey = useMessaging((state) => state.selfKey);
  const membersByKey = useMessaging((state) => state.membersByKey);
  const pollsEnabled = useMessaging((state) =>
    Boolean(state.capabilities.polls),
  );
  const votePoll = useMessaging((state) => state.votePoll);
  const voteState = useMessaging(
    (state) => state.pollVoteByMessage[message.messageId],
  );
  const [now, setNow] = useState(() => Date.now());
  const [disclosedOptionId, setDisclosedOptionId] = useState<string | null>(
    null,
  );
  const closesAt = poll?.closesAt ?? null;

  useEffect(() => {
    if (closesAt === null || now >= closesAt) return;
    const remaining = Math.max(1, closesAt - Date.now());
    const timer = window.setTimeout(
      () => setNow(Date.now()),
      Math.min(remaining, 30_000),
    );
    return () => window.clearTimeout(timer);
  }, [closesAt, now]);

  const canonicalSelection = useMemo(
    () =>
      poll?.options
        .filter((option) =>
          option.voters.some(
            (participant) => participantKey(participant) === selfKey,
          ),
        )
        .map((option) => option.optionId) ?? [],
    [poll, selfKey],
  );

  if (!poll) return null;
  const voterCount = pollVoterCount(poll);
  const closed = isPollClosed(poll, now);
  const outgoing = message.messageId.startsWith("pending:");
  const disabled = closed || outgoing || !pollsEnabled;
  const selected = voteState?.optionIds ?? canonicalSelection;

  const toggle = (optionId: string) => {
    if (disabled) return;
    const alreadySelected = selected.includes(optionId);
    const next = poll.allowMulti
      ? alreadySelected
        ? selected.filter((id) => id !== optionId)
        : [...selected, optionId]
      : alreadySelected
        ? []
        : [optionId];
    // vote_poll is a whole-selection replacement. The latest optimistic
    // selection is the base for a rapid second click, not the stale wire poll.
    void votePoll(message, next).catch(() => undefined);
  };

  return (
    <section
      aria-label={`投票: ${poll.question}`}
      aria-busy={voteState?.pending ?? false}
      data-poll-closed={closed ? "true" : "false"}
      className="mt-1.5 max-w-md rounded-lg border border-border bg-muted/20 p-2.5"
    >
      <p className="flex items-start gap-1.5 font-medium text-[13px]">
        <BarChart3 className="mt-0.5 size-3.5 shrink-0 text-muted-foreground" />
        <span className="min-w-0 flex-1">{poll.question}</span>
      </p>

      <div className="mt-2 space-y-1.5">
        {poll.options.map((option) => {
          const mine = selected.includes(option.optionId);
          const uniqueOptionVoters = new Map(
            option.voters.map((participant) => [
              participantKey(participant),
              participant,
            ]),
          );
          const optionVoterCount = uniqueOptionVoters.size;
          const share =
            voterCount === 0 ? 0 : (optionVoterCount / voterCount) * 100;
          const disclosureOpen = disclosedOptionId === option.optionId;
          const disclosureId = `poll-voters-${message.messageId}-${option.optionId}`;
          const voterNames = [...uniqueOptionVoters.values()].map(
            (participant) =>
              membersByKey[participantKey(participant)]?.displayName ?? "不明",
          );
          return (
            <div key={option.optionId}>
              <button
                type="button"
                disabled={disabled}
                aria-disabled={disabled}
                aria-pressed={mine}
                onClick={() => toggle(option.optionId)}
                className={`relative block w-full overflow-hidden rounded-md border px-2 py-1.5 text-left ${
                  mine
                    ? "border-primary/50 bg-primary/5"
                    : "border-border bg-background"
                } ${
                  disabled
                    ? "cursor-default"
                    : "hover:border-muted-foreground/50"
                }`}
              >
                <span
                  aria-hidden
                  className={`absolute inset-y-0 left-0 ${
                    mine ? "bg-primary/15" : "bg-muted-foreground/10"
                  }`}
                  style={{ width: `${share}%` }}
                />
                <span className="relative flex items-center gap-1.5">
                  <span
                    aria-hidden
                    className={`flex size-3.5 shrink-0 items-center justify-center border ${
                      poll.allowMulti ? "rounded-[3px]" : "rounded-full"
                    } ${
                      mine
                        ? "border-primary bg-primary text-primary-foreground"
                        : "border-muted-foreground/40"
                    }`}
                  >
                    {mine ? <Check className="size-2.5" /> : null}
                  </span>
                  <span className="min-w-0 flex-1 truncate text-[12.5px]">
                    {option.text}
                  </span>
                  <span className="shrink-0 text-[11px] text-muted-foreground tabular-nums">
                    {optionVoterCount}票 · {Math.round(share)}%
                  </span>
                </span>
              </button>
              {optionVoterCount > 0 ? (
                <div className="mt-0.5 flex justify-end">
                  <button
                    type="button"
                    aria-expanded={disclosureOpen}
                    aria-controls={disclosureId}
                    aria-label={`${option.text}の投票者を${
                      disclosureOpen ? "閉じる" : "表示"
                    }`}
                    onClick={() =>
                      setDisclosedOptionId((current) =>
                        current === option.optionId ? null : option.optionId,
                      )
                    }
                    className="flex items-center gap-1 rounded px-1 py-px text-[10.5px] text-muted-foreground hover:bg-accent hover:text-foreground"
                  >
                    <Users className="size-3" />
                    投票者
                  </button>
                </div>
              ) : null}
              {disclosureOpen ? (
                <p
                  id={disclosureId}
                  className="mt-0.5 rounded bg-background/80 px-2 py-1 text-[11px] text-muted-foreground"
                >
                  {voterNames.join("、")}
                </p>
              ) : null}
            </div>
          );
        })}
      </div>

      <p className="mt-1.5 flex flex-wrap items-center gap-x-2 gap-y-0.5 text-[11px] text-muted-foreground">
        <span>{voterCount}人が投票</span>
        <span>{poll.allowMulti ? "複数選べます" : "1つだけ選べます"}</span>
        {poll.closesAt !== null ? (
          <span>{deadlineLabel(poll.closesAt, now)}</span>
        ) : null}
        {!pollsEnabled && !outgoing ? (
          <span>この接続では回答できません</span>
        ) : null}
      </p>

      {voteState?.failed && !closed ? (
        <div
          role="alert"
          className="mt-2 flex items-center gap-2 text-[11px] text-rose-500"
        >
          投票を反映できませんでした
          <button
            type="button"
            onClick={() =>
              void votePoll(message, voteState.optionIds).catch(() => undefined)
            }
            className="rounded border border-rose-500/40 px-1.5 py-px font-medium hover:bg-rose-500/10"
          >
            もう一度
          </button>
        </div>
      ) : null}
    </section>
  );
}
