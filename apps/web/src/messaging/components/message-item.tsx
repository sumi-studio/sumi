import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@sumi/ui/components/alert-dialog";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@sumi/ui/components/popover";
import {
  Clock,
  CornerUpLeft,
  Link as LinkIcon,
  Pencil,
  SmilePlus,
  Trash2,
} from "lucide-react";
import { Fragment, memo, type ReactNode } from "react";
import { displayMentionPattern } from "../mention";
import type { MemberProfile, Message, ParticipantKey } from "../model";
import { participantKey } from "../model";
import { ParticipantAvatar } from "./participant-avatar";
import { ParticipantProfilePopover } from "./participant-profile";

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

const REACTION_PALETTE = ["👍", "✅", "👀", "🙏", "🎉", "😄", "❤️", "🤔"];

const REPLY_LATER_OPTIONS: { label: string; delayMs: number }[] = [
  { label: "30分後", delayMs: 30 * 60_000 },
  { label: "1時間後", delayMs: 60 * 60_000 },
  { label: "3時間後", delayMs: 3 * 60 * 60_000 },
];

const URL_PATTERN = /https?:\/\/[^\s<>"）」』】]+/g;

/** プレーン文中のURLをリンク化する。開発チームの会話はURLだらけになるので必須の道具。 */
function linkifyText(text: string, keyBase: string): ReactNode[] {
  const parts: ReactNode[] = [];
  let cursor = 0;
  let index = 0;
  for (const match of text.matchAll(URL_PATTERN)) {
    const start = match.index ?? 0;
    if (start > cursor) {
      parts.push(
        <Fragment key={`${keyBase}t${index}`}>
          {text.slice(cursor, start)}
        </Fragment>,
      );
      index += 1;
    }
    parts.push(
      <a
        key={`${keyBase}u${index}`}
        href={match[0]}
        target="_blank"
        rel="noreferrer noopener"
        className="break-all text-primary underline decoration-primary/40 underline-offset-2 hover:decoration-primary"
      >
        {match[0]}
      </a>,
    );
    index += 1;
    cursor = start + match[0].length;
  }
  if (cursor < text.length) {
    parts.push(
      <Fragment key={`${keyBase}t${index}`}>{text.slice(cursor)}</Fragment>,
    );
  }
  return parts;
}

function renderContent(
  content: string,
  members: Record<ParticipantKey, MemberProfile>,
  selfKey: ParticipantKey,
): ReactNode {
  const names = Object.values(members)
    .map((member) => member.displayName)
    .sort((a, b) => b.length - a.length);
  if (names.length === 0) return linkifyText(content, "c");
  const pattern = displayMentionPattern(names);
  const parts: ReactNode[] = [];
  let cursor = 0;
  let match = pattern.exec(content);
  let index = 0;
  while (match) {
    if (match.index > cursor) {
      parts.push(
        ...linkifyText(content.slice(cursor, match.index), `s${index}`),
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
    parts.push(...linkifyText(content.slice(cursor), `s${index}`));
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
  onClick?: () => void;
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

function ReactionChips({
  message,
  selfKey,
  membersByKey,
  onToggleReaction,
}: {
  message: Message;
  selfKey: ParticipantKey;
  membersByKey: Record<ParticipantKey, MemberProfile>;
  onToggleReaction: (message: Message, emoji: string) => void;
}) {
  if (message.reactions.length === 0) return null;
  return (
    <div className="mt-1 flex flex-wrap items-center gap-1">
      {message.reactions.map((reaction) => {
        const mine = reaction.participants.some(
          (ref) => participantKey(ref) === selfKey,
        );
        const names = reaction.participants
          .map(
            (ref) => membersByKey[participantKey(ref)]?.displayName ?? "不明",
          )
          .join("、");
        return (
          <button
            key={reaction.emoji}
            type="button"
            title={names}
            onClick={() => onToggleReaction(message, reaction.emoji)}
            className={`flex items-center gap-1 rounded-full border px-1.5 py-px text-[12px] transition-colors ${
              mine
                ? "border-primary/40 bg-primary/10"
                : "border-border bg-muted/40 hover:border-muted-foreground/40"
            }`}
          >
            <span>{reaction.emoji}</span>
            <span className="text-[11px] text-muted-foreground tabular-nums">
              {reaction.participants.length}
            </span>
          </button>
        );
      })}
    </div>
  );
}

export interface MessageItemProps {
  message: Message;
  grouped: boolean;
  pending: boolean;
  failed: boolean;
  selfKey: ParticipantKey;
  membersByKey: Record<ParticipantKey, MemberProfile>;
  /** このメッセージへ「後で返信します」を置いている相手の表示名。 */
  replyLaterBy: string[];
  allowReactions: boolean;
  allowReplyLater: boolean;
  findMessage: (messageId: string) => Message | undefined;
  onReply: (message: Message) => void;
  onReplyLater: (message: Message, delayMs: number) => void;
  onToggleReaction: (message: Message, emoji: string) => void;
  onCopyLink: (message: Message) => void;
  onEdit: (message: Message) => void;
  onDelete: (message: Message) => void;
  onJumpTo: (messageId: string) => void;
  onRetry: (message: Message) => void;
}

export const MessageItem = memo(function MessageItem({
  message,
  grouped,
  pending,
  failed,
  selfKey,
  membersByKey,
  replyLaterBy,
  allowReactions,
  allowReplyLater,
  findMessage,
  onReply,
  onReplyLater,
  onToggleReaction,
  onCopyLink,
  onEdit,
  onDelete,
  onJumpTo,
  onRetry,
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
      className={`group relative px-4 transition-colors hover:bg-accent/55 sm:px-6 ${grouped ? "py-0.5" : "mt-2.5 py-0.5"} ${
        mentionsSelf ? "bg-amber-500/6" : ""
      } ${pending && !failed ? "opacity-55" : ""}`}
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
          <span className="w-8 shrink-0 pt-0.5">
            <ParticipantProfilePopover
              participantKey={authorKey}
              label={`${author?.displayName ?? "参加者"}のプロフィール`}
              className="rounded-full"
            >
              <ParticipantAvatar
                participantKey={authorKey}
                name={author?.displayName ?? "?"}
                size={32}
              />
            </ParticipantProfilePopover>
          </span>
        )}
        <div className="min-w-0 flex-1">
          {grouped ? null : (
            <div className="flex items-baseline gap-2">
              <ParticipantProfilePopover
                participantKey={authorKey}
                className="rounded font-semibold text-[13.5px] hover:underline"
              >
                {author?.displayName ?? "不明な参加者"}
              </ParticipantProfilePopover>
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
            {grouped && message.urgency !== "normal" ? (
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
          {allowReactions ? (
            <ReactionChips
              message={message}
              selfKey={selfKey}
              membersByKey={membersByKey}
              onToggleReaction={onToggleReaction}
            />
          ) : null}
          {replyLaterBy.length > 0 ? (
            <p className="mt-0.5 flex items-center gap-1 text-[11px] text-muted-foreground">
              <Clock className="size-3" />
              {replyLaterBy.join("、")} が後で返信予定
            </p>
          ) : null}
          {failed ? (
            <div className="mt-0.5 flex items-center gap-2 text-[11px] text-rose-500">
              送信できませんでした
              <button
                type="button"
                onClick={() => onRetry(message)}
                className="rounded border border-rose-500/40 px-1.5 py-px font-medium hover:bg-rose-500/10"
              >
                再送
              </button>
            </div>
          ) : null}
        </div>
      </div>
      {pending ? null : (
        <div className="pointer-events-none absolute top-0 right-4 flex -translate-y-1/2 items-center gap-0.5 rounded-lg border border-border bg-background p-0.5 opacity-0 shadow-sm transition-opacity group-focus-within:pointer-events-auto group-focus-within:opacity-100 group-hover:pointer-events-auto group-hover:opacity-100">
          {allowReactions ? (
            <Popover>
              <PopoverTrigger
                render={
                  <button
                    type="button"
                    title="リアクション"
                    aria-label="リアクション"
                    className="flex size-7 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
                  />
                }
              >
                <SmilePlus className="size-3.5" />
              </PopoverTrigger>
              <PopoverContent side="top" className="flex gap-0.5 p-1">
                {REACTION_PALETTE.map((emoji) => (
                  <button
                    key={emoji}
                    type="button"
                    onClick={() => onToggleReaction(message, emoji)}
                    className="flex size-8 items-center justify-center rounded-md text-[16px] transition-colors hover:bg-accent"
                  >
                    {emoji}
                  </button>
                ))}
              </PopoverContent>
            </Popover>
          ) : null}
          <ToolbarButton label="返信" onClick={() => onReply(message)}>
            <CornerUpLeft className="size-3.5" />
          </ToolbarButton>
          {own || !allowReplyLater ? null : (
            <Popover>
              <PopoverTrigger
                render={
                  <button
                    type="button"
                    title="後で返信"
                    aria-label="後で返信"
                    className="flex size-7 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
                  />
                }
              >
                <Clock className="size-3.5" />
              </PopoverTrigger>
              <PopoverContent side="top" className="w-40 p-1">
                <p className="px-2 pt-1 pb-0.5 text-[11px] text-muted-foreground">
                  後で返信 — いつ知らせる？
                </p>
                {REPLY_LATER_OPTIONS.map((option) => (
                  <button
                    key={option.label}
                    type="button"
                    onClick={() => onReplyLater(message, option.delayMs)}
                    className="w-full rounded-md px-2 py-1 text-left text-[12.5px] transition-colors hover:bg-accent"
                  >
                    {option.label}
                  </button>
                ))}
              </PopoverContent>
            </Popover>
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
              <AlertDialog>
                <AlertDialogTrigger
                  render={
                    <button
                      type="button"
                      title="削除"
                      aria-label="削除"
                      className="flex size-7 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-accent hover:text-rose-500"
                    />
                  }
                >
                  <Trash2 className="size-3.5" />
                </AlertDialogTrigger>
                <AlertDialogContent>
                  <AlertDialogHeader>
                    <AlertDialogTitle>メッセージを削除</AlertDialogTitle>
                    <AlertDialogDescription>
                      削除すると元に戻せません。削除の事実は残ります。
                    </AlertDialogDescription>
                  </AlertDialogHeader>
                  <AlertDialogFooter>
                    <AlertDialogCancel>キャンセル</AlertDialogCancel>
                    <AlertDialogAction onClick={() => onDelete(message)}>
                      削除する
                    </AlertDialogAction>
                  </AlertDialogFooter>
                </AlertDialogContent>
              </AlertDialog>
            </>
          ) : null}
        </div>
      )}
    </div>
  );
});
