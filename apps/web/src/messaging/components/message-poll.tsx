import { BarChart3, Check } from "lucide-react";
import { useEffect, useState } from "react";
import type { Message } from "../model";
import { isPollClosed, participantKey, pollVoteCount } from "../model";
import { useMessaging } from "../store";

/**
 * メッセージの中の投票。票数と割合バー、自分の選択のハイライト、投票と取消。
 * 締切後は結果だけ——押せるものが残っていると嘘になる。
 */

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

  // 締切が近い投票だけ時計を回す。開いているだけで毎秒描き直さない。
  const closesAt = poll?.closesAt ?? null;
  useEffect(() => {
    if (closesAt === null || now >= closesAt) return;
    const timer = window.setInterval(() => setNow(Date.now()), 30_000);
    return () => window.clearInterval(timer);
  }, [closesAt, now]);

  if (!poll) return null;
  const total = pollVoteCount(poll);
  const closed = isPollClosed(poll, now);
  // 楽観的描画中（サーバーが採番する前）は押せる選択肢がまだ無い。
  const pending = poll.options.some((option) =>
    option.optionId.startsWith("pending:"),
  );

  const toggle = (optionId: string) => {
    if (closed || pending) return;
    const mine = poll.options
      .filter((option) =>
        option.voters.some((ref) => participantKey(ref) === selfKey),
      )
      .map((option) => option.optionId);
    const already = mine.includes(optionId);
    if (poll.allowMulti) {
      votePoll(
        message,
        already ? mine.filter((id) => id !== optionId) : [...mine, optionId],
      );
      return;
    }
    // 単一選択では、同じものをもう一度押すのが取り消し。
    votePoll(message, already ? [] : [optionId]);
  };

  return (
    <div className="mt-1.5 max-w-md rounded-lg border border-border bg-muted/20 p-2.5">
      <p className="flex items-start gap-1.5 font-medium text-[13px]">
        <BarChart3 className="mt-0.5 size-3.5 shrink-0 text-muted-foreground" />
        <span className="min-w-0 flex-1">{poll.question}</span>
      </p>
      <div className="mt-2 space-y-1">
        {poll.options.map((option) => {
          const mine = option.voters.some(
            (ref) => participantKey(ref) === selfKey,
          );
          const share = total === 0 ? 0 : (option.voters.length / total) * 100;
          const names = option.voters
            .map(
              (ref) => membersByKey[participantKey(ref)]?.displayName ?? "不明",
            )
            .join("、");
          return (
            <button
              key={option.optionId}
              type="button"
              title={names}
              disabled={closed || pending}
              aria-pressed={mine}
              onClick={() => toggle(option.optionId)}
              className={`relative block w-full overflow-hidden rounded-md border px-2 py-1.5 text-left transition-colors ${
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
                  className={`flex size-3.5 shrink-0 items-center justify-center ${
                    poll.allowMulti ? "rounded-[3px]" : "rounded-full"
                  } border ${
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
                  {option.voters.length}票 · {Math.round(share)}%
                </span>
              </span>
            </button>
          );
        })}
      </div>
      <p className="mt-1.5 flex items-center gap-2 text-[11px] text-muted-foreground">
        <span className="tabular-nums">{total}票</span>
        <span>{poll.allowMulti ? "複数選べます" : "1つだけ選べます"}</span>
        {poll.closesAt !== null ? (
          <span className={closed ? "text-muted-foreground" : ""}>
            {deadlineLabel(poll.closesAt, now)}
          </span>
        ) : null}
      </p>
    </div>
  );
}
