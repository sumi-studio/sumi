import {
  Clock,
  CornerUpLeft,
  Link as LinkIcon,
  Pencil,
  Trash2,
} from "lucide-react";
import { Fragment, memo, type ReactNode } from "react";
import type { MemberProfile, Message, ParticipantKey } from "../model";
import { participantKey } from "../model";
import { ParticipantAvatar } from "./participant-avatar";

const TIME_FORMAT = new Intl.DateTimeFormat("ja-JP", {
  hour: "2-digit",
  minute: "2-digit",
});

const FULL_FORMAT = new Intl.DateTimeFormat("ja-JP", {
  month: "long",
  day: "numeric",
  hour: "2-digit",
  minute: "2-digit",
});

function renderContent(
  content: string,
  members: Record<ParticipantKey, MemberProfile>,
  selfKey: ParticipantKey,
): ReactNode {
  const names = Object.values(members)
    .map((member) => member.displayName)
    .sort((a, b) => b.length - a.length);
  if (names.length === 0) return content;
  const escaped = names.map((name) =>
    name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"),
  );
  const pattern = new RegExp(`@(${escaped.join("|")})`, "g");
  const parts: ReactNode[] = [];
  let cursor = 0;
  let match = pattern.exec(content);
  let index = 0;
  while (match) {
    if (match.index > cursor) {
      parts.push(
        <Fragment key={`t${index}`}>
          {content.slice(cursor, match.index)}
        </Fragment>,
      );
      index += 1;
    }
    const mentioned = Object.values(members).find(
      (member) => member.displayName === match?.[1],
    );
    const isSelf = mentioned
      ? participantKey(mentioned.participant) === selfKey
      : false;
    parts.push(
      <span
        key={`m${index}`}
        className={
          isSelf
            ? "rounded bg-amber-500/15 px-0.5 font-medium text-amber-700 dark:text-amber-400"
            : "rounded bg-primary/10 px-0.5 font-medium text-primary"
        }
      >
        {match[0]}
      </span>,
    );
    index += 1;
    cursor = match.index + match[0].length;
    match = pattern.exec(content);
  }
  if (cursor < content.length) {
    parts.push(<Fragment key={`t${index}`}>{content.slice(cursor)}</Fragment>);
  }
  return parts;
}

function UrgencyChip({ urgency }: { urgency: Message["urgency"] }) {
  if (urgency === "urgent") {
    return (
      <span className="rounded bg-rose-500/12 px-1.5 py-px font-medium text-[11px] text-rose-600 dark:text-rose-400">
        急ぎ
      </span>
    );
  }
  if (urgency === "fyi") {
    return (
      <span className="rounded bg-muted px-1.5 py-px font-medium text-[11px] text-muted-foreground">
        FYI・返信不要
      </span>
    );
  }
  return null;
}

function ToolbarButton({
  label,
  onClick,
  children,
  danger,
}: {
  label: string;
  onClick: () => void;
  children: ReactNode;
  danger?: boolean;
}) {
  return (
    <button
      type="button"
      title={label}
      aria-label={label}
      onClick={onClick}
      className={`flex size-7 items-center justify-center rounded-md transition-colors hover:bg-accent ${
        danger
          ? "text-muted-foreground hover:text-rose-500"
          : "text-muted-foreground hover:text-foreground"
      }`}
    >
      {children}
    </button>
  );
}

export interface MessageItemProps {
  message: Message;
  grouped: boolean;
  pending: boolean;
  selfKey: ParticipantKey;
  membersByKey: Record<ParticipantKey, MemberProfile>;
  findMessage: (messageId: string) => Message | undefined;
  onReply: (message: Message) => void;
  onReplyLater: (message: Message) => void;
  onCopyLink: (message: Message) => void;
  onEdit: (message: Message) => void;
  onDelete: (message: Message) => void;
  onJumpTo: (messageId: string) => void;
}

export const MessageItem = memo(function MessageItem({
  message,
  grouped,
  pending,
  selfKey,
  membersByKey,
  findMessage,
  onReply,
  onReplyLater,
  onCopyLink,
  onEdit,
  onDelete,
  onJumpTo,
}: MessageItemProps) {
  const authorKey = participantKey(message.author);
  const author = membersByKey[authorKey];
  const own = authorKey === selfKey;
  const mentionsSelf = message.mentions.some(
    (ref) => participantKey(ref) === selfKey,
  );
  const replyTarget = message.replyTo
    ? findMessage(message.replyTo)
    : undefined;
  const replyAuthor = replyTarget
    ? membersByKey[participantKey(replyTarget.author)]
    : undefined;

  return (
    <div
      className={`group relative px-4 sm:px-6 ${grouped ? "py-0.5" : "mt-2.5 py-0.5"} ${
        mentionsSelf ? "bg-amber-500/6" : "hover:bg-accent/40"
      } ${pending ? "opacity-55" : ""}`}
    >
      {replyTarget && replyAuthor ? (
        <button
          type="button"
          onClick={() => onJumpTo(replyTarget.messageId)}
          className="mb-0.5 ml-11 flex max-w-full items-center gap-1.5 truncate text-muted-foreground text-xs hover:text-foreground"
        >
          <CornerUpLeft className="size-3 shrink-0" />
          <span className="font-medium">{replyAuthor.displayName}</span>
          <span className="truncate">{replyTarget.content}</span>
        </button>
      ) : null}
      <div className="flex gap-3">
        {grouped ? (
          <span className="w-8 shrink-0 pt-1 text-right text-[10px] text-muted-foreground/0 tabular-nums leading-5 group-hover:text-muted-foreground/70">
            {TIME_FORMAT.format(message.createdAt)}
          </span>
        ) : (
          <span className="pt-0.5">
            <ParticipantAvatar
              participantKey={authorKey}
              name={author?.displayName ?? "?"}
              size={32}
            />
          </span>
        )}
        <div className="min-w-0 flex-1">
          {grouped ? null : (
            <div className="flex items-baseline gap-2">
              <span className="font-semibold text-[13.5px]">
                {author?.displayName ?? "不明な参加者"}
              </span>
              <span
                className="text-[11px] text-muted-foreground tabular-nums"
                title={FULL_FORMAT.format(message.createdAt)}
              >
                {TIME_FORMAT.format(message.createdAt)}
              </span>
              <UrgencyChip urgency={message.urgency} />
            </div>
          )}
          <div className="whitespace-pre-wrap break-words text-[13.5px] leading-6">
            {grouped ? (
              <span className="mr-1.5 align-middle">
                <UrgencyChip urgency={message.urgency} />
              </span>
            ) : null}
            {renderContent(message.content, membersByKey, selfKey)}
            {message.editedAt ? (
              <span
                className="ml-1 text-[10px] text-muted-foreground"
                title={FULL_FORMAT.format(message.editedAt)}
              >
                (編集済み)
              </span>
            ) : null}
          </div>
        </div>
      </div>
      {pending ? null : (
        <div className="absolute top-0 right-4 hidden -translate-y-1/2 items-center gap-0.5 rounded-lg border border-border bg-background p-0.5 shadow-sm group-hover:flex">
          <ToolbarButton label="返信" onClick={() => onReply(message)}>
            <CornerUpLeft className="size-3.5" />
          </ToolbarButton>
          {own ? null : (
            <ToolbarButton
              label="後で返信"
              onClick={() => onReplyLater(message)}
            >
              <Clock className="size-3.5" />
            </ToolbarButton>
          )}
          <ToolbarButton
            label="リンクをコピー"
            onClick={() => onCopyLink(message)}
          >
            <LinkIcon className="size-3.5" />
          </ToolbarButton>
          {own ? (
            <>
              <ToolbarButton label="編集" onClick={() => onEdit(message)}>
                <Pencil className="size-3.5" />
              </ToolbarButton>
              <ToolbarButton
                label="削除"
                danger
                onClick={() => onDelete(message)}
              >
                <Trash2 className="size-3.5" />
              </ToolbarButton>
            </>
          ) : null}
        </div>
      )}
    </div>
  );
});
