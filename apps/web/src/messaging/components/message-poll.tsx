import { BarChart3, Check } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import type { Message } from "../model";
import { isPollClosed, participantKey, pollVoteCount } from "../model";
import { useMessaging } from "../store";

const CLOSE_FORMAT = new Intl.DateTimeFormat("ja-JP", {
  month: "numeric",
  day: "numeric",
  hour: "2-digit",
  minute: "2-digit",
});

function deadlineLabel(closesAt: number, now: number): string {
  if (now >= closesAt) return "締め切りました";
  const minutes = Math.round((closesAt - now) / 60_000);
  if (minutes < 60) return `あと${minutes}分`;
  if (minutes < 24 * 60) return `あと${Math.round(minutes / 60)}時間`;
  return `${CLOSE_FORMAT.format(closesAt)}まで`;
}

export function MessagePoll({ message }: { message: Message }) {
  const poll = message.poll;
  const selfKey = useMessaging((state) => state.selfKey);
  const membersByKey = useMessaging((state) => state.membersByKey);
  const votePoll = useMessaging((state) => state.votePoll);
  const [now, setNow] = useState(() => Date.now());
  const [optimisticSelection, setOptimisticSelection] = useState<
    string[] | null
  >(null);
  const pendingSelection = useRef<string[] | null>(null);
  const closesAt = poll?.closesAt ?? null;

  useEffect(() => {
    if (closesAt === null || now >= closesAt) return;
    const timer = window.setInterval(() => setNow(Date.now()), 30_000);
    return () => window.clearInterval(timer);
  }, [closesAt, now]);

  if (!poll) return null;
  const total = pollVoteCount(poll);
  const closed = isPollClosed(poll, now);
  const pending = poll.options.some((option) =>
    option.optionId.startsWith("pending:"),
  );
  const toggle = (optionId: string) => {
    if (closed || pending) return;
    const mine =
      pendingSelection.current ??
      poll.options
        .filter((option) =>
          option.voters.some(
            (participant) => participantKey(participant) === selfKey,
          ),
        )
        .map((option) => option.optionId);
    const selected = mine.includes(optionId);
    const next = poll.allowMulti
      ? selected
        ? mine.filter((id) => id !== optionId)
        : [...mine, optionId]
      : selected
        ? []
        : [optionId];
    // `vote_poll` replaces the whole choice set. Remember the local intent
    // between rapid clicks so the second request contains the first choice.
    pendingSelection.current = next;
    setOptimisticSelection(next);
    void Promise.resolve(votePoll(message, next))
      .finally(() => {
        // Only the last queued replacement owns the displayed intent.
        if (pendingSelection.current === next) {
          pendingSelection.current = null;
          setOptimisticSelection(null);
        }
      })
      .catch(() => undefined);
  };

  return (
    <div className="mt-1.5 max-w-md rounded-lg border border-border bg-muted/20 p-2.5">
      <p className="flex items-start gap-1.5 font-medium text-[13px]">
        <BarChart3 className="mt-0.5 size-3.5 shrink-0 text-muted-foreground" />
        <span className="min-w-0 flex-1">{poll.question}</span>
      </p>
      <div className="mt-2 space-y-1">
        {poll.options.map((option) => {
          const mine =
            optimisticSelection?.includes(option.optionId) ??
            option.voters.some(
              (participant) => participantKey(participant) === selfKey,
            );
          const share = total === 0 ? 0 : (option.voters.length / total) * 100;
          const voters = option.voters
            .map(
              (participant) =>
                membersByKey[participantKey(participant)]?.displayName ??
                "不明",
            )
            .join("、");
          return (
            <button
              key={option.optionId}
              type="button"
              title={voters}
              disabled={closed || pending}
              aria-pressed={mine}
              onClick={() => toggle(option.optionId)}
              className={`relative block w-full overflow-hidden rounded-md border px-2 py-1.5 text-left ${
                mine
                  ? "border-primary/50 bg-primary/5"
                  : "border-border bg-background"
              } ${closed || pending ? "cursor-default" : "hover:border-muted-foreground/50"}`}
            >
              <span
                aria-hidden
                className={`absolute inset-y-0 left-0 ${mine ? "bg-primary/15" : "bg-muted-foreground/10"}`}
                style={{ width: `${share}%` }}
              />
              <span className="relative flex items-center gap-1.5">
                <span
                  className={`flex size-3.5 shrink-0 items-center justify-center border ${
                    poll.allowMulti ? "rounded-[3px]" : "rounded-full"
                  } ${mine ? "border-primary bg-primary text-primary-foreground" : "border-muted-foreground/40"}`}
                >
                  {mine ? <Check className="size-2.5" /> : null}
                </span>
                <span className="min-w-0 flex-1 truncate text-[12.5px]">
                  {option.text}
                </span>
                <span className="shrink-0 text-[11px] text-muted-foreground tabular-nums">
                  {option.voters.length}票 · {Math.round(share)}%
                </span>
              </span>
            </button>
          );
        })}
      </div>
      <p className="mt-1.5 flex items-center gap-2 text-[11px] text-muted-foreground">
        <span>{total}票</span>
        <span>{poll.allowMulti ? "複数選べます" : "1つだけ選べます"}</span>
        {poll.closesAt !== null ? (
          <span>{deadlineLabel(poll.closesAt, now)}</span>
        ) : null}
      </p>
    </div>
  );
}
