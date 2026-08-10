import {
  AtSign,
  ChartBarBig,
  CornerUpLeft,
  MessagesSquare,
  Paperclip,
  SendHorizontal,
  X,
} from "lucide-react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { isImeComposing } from "../../lib/ime";
import { isInsideUnclosedCodeFence } from "../compose-fence";
import type { MemberProfile, Message, Urgency } from "../model";
import { participantKey } from "../model";
import { useMessaging } from "../store";
import { usePlaceDisplay } from "../use-place-name";
import {
  ComposerAttachments,
  useDraftAttachments,
} from "./composer-attachments";
import type { ComposerPlusMenuItem } from "./composer-plus-menu";
import { ComposerPlusMenu } from "./composer-plus-menu";
import { useWheelPassthrough } from "./overlay";
import { ParticipantAvatar } from "./participant-avatar";
import { PollCreateDialog } from "./poll-create-dialog";

const MAX_HEIGHT_PX = 220;
const TYPING_THROTTLE_MS = 2_000;

/** selectorは毎回同じ参照を返す必要がある（新しい[]を作ると無限再レンダー）。 */
const NO_MESSAGES: Message[] = [];

const URGENCIES: { value: Urgency; label: string; hint: string }[] = [
  { value: "normal", label: "普通", hint: "通常の通知" },
  { value: "urgent", label: "急ぎ", hint: "相手へ強めに通知" },
  { value: "fyi", label: "FYI", hint: "返信不要。手すきで見て" },
];

interface MentionQuery {
  query: string;
  start: number;
  end: number;
}

function findMentionQuery(value: string, caret: number): MentionQuery | null {
  const before = value.slice(0, caret);
  const match = /(^|\s)@([^\s@]*)$/.exec(before);
  if (!match) return null;
  const start = match.index + match[1].length;
  return { query: match[2], start, end: caret };
}

export function Composer() {
  const activePlaceKey = useMessaging((state) => state.activePlaceKey);
  const draft = useMessaging((state) =>
    state.activePlaceKey
      ? (state.draftByPlace[state.activePlaceKey] ?? "")
      : "",
  );
  const setDraft = useMessaging((state) => state.setDraft);
  const send = useMessaging((state) => state.send);
  const sendTyping = useMessaging((state) => state.sendTyping);
  const selfKey = useMessaging((state) => state.selfKey);
  const membersByKey = useMessaging((state) => state.membersByKey);
  const messages = useMessaging((state) =>
    state.activePlaceKey
      ? (state.messagesByPlace[state.activePlaceKey] ?? NO_MESSAGES)
      : NO_MESSAGES,
  );
  // 編集はメッセージ本体の位置で行う（message-item のインライン編集）。
  // composerは編集を預からず、↑キーの入り口だけを持つ。
  const editingMessageId = useMessaging((state) => state.editingMessageId);
  const startEdit = useMessaging((state) => state.startEdit);
  const replyTargetId = useMessaging((state) => state.replyTargetId);
  const setReplyTarget = useMessaging((state) => state.setReplyTarget);
  const canPoll = useMessaging((state) => state.capabilities.polls);

  const display = usePlaceDisplay(activePlaceKey);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const [urgency, setUrgency] = useState<Urgency>("normal");
  const [mention, setMention] = useState<MentionQuery | null>(null);
  const [mentionIndex, setMentionIndex] = useState(0);
  const drafts = useDraftAttachments({
    placeKey: activePlaceKey,
  });
  const [dragging, setDragging] = useState(false);
  const [pollOpen, setPollOpen] = useState(false);
  const lastTypingAt = useRef(0);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const replyTarget = replyTargetId
    ? messages.find((entry) => entry.messageId === replyTargetId)
    : undefined;
  const replyAuthor = replyTarget
    ? membersByKey[participantKey(replyTarget.author)]
    : undefined;

  const value = draft;

  // biome-ignore lint/correctness/useExhaustiveDependencies: place切替・返信開始・編集終了をフォーカスのトリガーにする
  useEffect(() => {
    // インライン編集中はキャレットが編集欄にある。奪い返さない。
    if (editingMessageId) return;
    textareaRef.current?.focus();
  }, [activePlaceKey, editingMessageId, replyTargetId]);

  // autogrow: 内容に合わせて高さを伸ばし、上限でスクロールへ切り替える。
  // biome-ignore lint/correctness/useExhaustiveDependencies: 入力値の変化を高さ再計算のトリガーにする
  useEffect(() => {
    const textarea = textareaRef.current;
    if (!textarea) return;
    textarea.style.height = "auto";
    const next = Math.min(textarea.scrollHeight, MAX_HEIGHT_PX);
    textarea.style.height = `${next}px`;
    textarea.style.overflowY =
      textarea.scrollHeight > MAX_HEIGHT_PX ? "auto" : "hidden";
  }, [value]);

  const candidates = useMemo(() => {
    if (!mention) return [];
    const query = mention.query.toLowerCase();
    return Object.values(membersByKey)
      .filter((member) => participantKey(member.participant) !== selfKey)
      .filter((member) => member.displayName.toLowerCase().includes(query))
      .slice(0, 6);
  }, [mention, membersByKey, selfKey]);

  // 入力値の置き場所（draft）だけを面倒みる。
  // メンション候補の開閉は呼び出し側の事情で変わるのでここには含めない。
  const writeValue = useCallback(
    (next: string) => {
      if (!activePlaceKey) return;
      setDraft(activePlaceKey, next);
      const now = Date.now();
      if (next.trim() && now - lastTypingAt.current > TYPING_THROTTLE_MS) {
        lastTypingAt.current = now;
        sendTyping();
      }
    },
    [activePlaceKey, setDraft, sendTyping],
  );

  const updateValue = useCallback(
    (next: string) => {
      writeValue(next);
      const caret = textareaRef.current?.selectionStart ?? next.length;
      setMention(findMentionQuery(next, caret));
      setMentionIndex(0);
    },
    [writeValue],
  );

  /**
   * カーソル位置に @ を差し込み、キーボードで打ったのと同じ候補パネルを開く。
   * 直前が文字ならスペースを補う（findMentionQueryが語頭の @ しか拾わないため）。
   * DOMのcaretはまだ更新前なので、候補の範囲は挿入後の位置から直接組み立てる。
   */
  const insertMentionTrigger = useCallback(() => {
    const textarea = textareaRef.current;
    const caret = textarea?.selectionStart ?? value.length;
    const before = value.slice(0, caret);
    const inserted = before === "" || /\s$/.test(before) ? "@" : " @";
    const next = before + inserted + value.slice(caret);
    const nextCaret = caret + inserted.length;
    writeValue(next);
    setMention({ query: "", start: nextCaret - 1, end: nextCaret });
    setMentionIndex(0);
    window.requestAnimationFrame(() => {
      const current = textareaRef.current;
      if (!current) return;
      current.focus();
      current.setSelectionRange(nextCaret, nextCaret);
    });
  }, [value, writeValue]);

  const applyMention = useCallback(
    (member: MemberProfile) => {
      if (!mention) return;
      const inserted = `@${member.displayName} `;
      const next =
        value.slice(0, mention.start) + inserted + value.slice(mention.end);
      updateValue(next);
      setMention(null);
      window.requestAnimationFrame(() => {
        const textarea = textareaRef.current;
        if (!textarea) return;
        const caret = mention.start + inserted.length;
        textarea.setSelectionRange(caret, caret);
        textarea.focus();
      });
    },
    [mention, value, updateValue],
  );

  // 添付は送信前にアップロードし、送信時にはidを渡すだけにする。
  // 受け皿と表示はcomposer-attachmentsが持つ。
  const addDraftFiles = drafts.addFiles;
  const addFiles = useCallback(
    (files: FileList | File[] | null | undefined) => {
      addDraftFiles(files);
    },
    [addDraftFiles],
  );

  // メンション候補は一覧の上に浮くので、この上でのホイールも一覧へ渡す。
  const mentionPassthroughRef = useWheelPassthrough<HTMLDivElement>();
  const { uploading, ready: readyAttachments, clear: clearDrafts } = drafts;

  const submit = useCallback(() => {
    // place切替直後の古いイベントから、別placeへ添付idを渡さない。
    if (
      !activePlaceKey ||
      useMessaging.getState().activePlaceKey !== activePlaceKey
    ) {
      return;
    }
    // アップロード中に送ると添付を取りこぼす。終わるまで送信しない。
    if (uploading) return;
    if (!value.trim() && readyAttachments.length === 0) return;
    send(value, urgency, readyAttachments);
    clearDrafts();
    setUrgency("normal");
    setMention(null);
  }, [
    activePlaceKey,
    value,
    send,
    urgency,
    uploading,
    readyAttachments,
    clearDrafts,
  ]);

  // 送信できるかどうかの判定は送信ボタンの活殺とEnter送信で同じものを使う。
  const canSend =
    !uploading && (value.trim().length > 0 || readyAttachments.length > 0);

  // ＋メニューの品書き。まだ中身のない導線も準備中として席だけ用意しておく
  // （並行して作られている機能が届いたら、この配列に繋ぎ込むだけで済む）。
  const plusItems = useMemo<ComposerPlusMenuItem[]>(
    () => [
      {
        id: "attach",
        label: "ファイルを添付",
        hint: "画像・書類",
        icon: Paperclip,
        onSelect: () => fileInputRef.current?.click(),
      },
      {
        id: "mention",
        label: "メンション",
        hint: "@ で相手を呼ぶ",
        icon: AtSign,
        onSelect: insertMentionTrigger,
      },
      {
        id: "thread",
        label: "スレッドを作成",
        hint: "準備中",
        icon: MessagesSquare,
        disabled: true,
      },
      {
        id: "poll",
        label: "投票を作成",
        hint: canPoll ? "みんなに聞く" : "準備中",
        icon: ChartBarBig,
        disabled: !canPoll,
        onSelect: () => setPollOpen(true),
      },
    ],
    [insertMentionTrigger, canPoll],
  );

  const onKeyDown = useCallback(
    (event: React.KeyboardEvent<HTMLTextAreaElement>) => {
      if (isImeComposing(event)) return;
      if (mention && candidates.length > 0) {
        if (event.key === "ArrowDown") {
          event.preventDefault();
          setMentionIndex((index) => (index + 1) % candidates.length);
          return;
        }
        if (event.key === "ArrowUp") {
          event.preventDefault();
          setMentionIndex(
            (index) => (index - 1 + candidates.length) % candidates.length,
          );
          return;
        }
        if (event.key === "Enter" || event.key === "Tab") {
          event.preventDefault();
          applyMention(candidates[mentionIndex]);
          return;
        }
        if (event.key === "Escape") {
          event.preventDefault();
          setMention(null);
          return;
        }
      }
      if (event.key === "Escape") {
        if (replyTargetId) {
          event.preventDefault();
          setReplyTarget(null);
        }
        return;
      }
      if (event.key === "Enter" && !event.shiftKey) {
        // 未閉鎖の```コードブロック内では送信せず、デフォルトの改行に任せる。
        const caret = event.currentTarget.selectionStart ?? value.length;
        if (isInsideUnclosedCodeFence(value, caret)) return;
        event.preventDefault();
        submit();
        return;
      }
      // 空欄で↑ = 自分の直前のメッセージをその場で編集し始める。
      if (event.key === "ArrowUp" && !editingMessageId && value === "") {
        const own = [...messages]
          .reverse()
          .find((entry) => participantKey(entry.author) === selfKey);
        if (own) {
          event.preventDefault();
          startEdit(own.messageId);
        }
      }
    },
    [
      mention,
      candidates,
      mentionIndex,
      applyMention,
      editingMessageId,
      replyTargetId,
      setReplyTarget,
      submit,
      value,
      messages,
      selfKey,
      startEdit,
    ],
  );

  if (!activePlaceKey || !display) return null;
  const placeholder =
    display.kind === "channel"
      ? `#${display.name} へメッセージ`
      : `${display.name} へメッセージ`;

  return (
    <div className="relative shrink-0 px-4 pb-4 sm:px-6">
      {mention && candidates.length > 0 ? (
        <div
          ref={mentionPassthroughRef}
          className="absolute bottom-full left-4 z-10 mb-1 w-64 overflow-hidden rounded-lg border border-border bg-background shadow-md sm:left-6"
        >
          {candidates.map((member, index) => {
            const key = participantKey(member.participant);
            return (
              <button
                key={key}
                type="button"
                onMouseDown={(event) => {
                  event.preventDefault();
                  applyMention(member);
                }}
                className={`flex w-full items-center gap-2 px-2.5 py-1.5 text-left text-[13px] ${
                  index === mentionIndex ? "bg-accent" : ""
                }`}
              >
                <ParticipantAvatar
                  participantKey={key}
                  name={member.displayName}
                  size={20}
                  src={member.avatarUrl}
                />
                <span className="shrink-0 font-medium">
                  {member.displayName}
                </span>
                {/* 説明は右端に寄せて、名前だけを目で追えるようにする。 */}
                <span className="ml-auto truncate text-muted-foreground text-xs">
                  {member.tagline}
                </span>
              </button>
            );
          })}
        </div>
      ) : null}
      {replyTarget && replyAuthor ? (
        <div className="mb-1 flex items-center gap-2 text-muted-foreground text-xs">
          <CornerUpLeft className="size-3" />
          <span className="font-medium text-foreground">
            {replyAuthor.displayName}
          </span>
          <span className="truncate">{replyTarget.content}</span>
          <button
            type="button"
            onClick={() => setReplyTarget(null)}
            className="ml-auto rounded p-0.5 hover:bg-accent"
            aria-label="返信をキャンセル"
          >
            <X className="size-3.5" />
          </button>
        </div>
      ) : null}
      {/* biome-ignore lint/a11y/noStaticElementInteractions: ドロップ先は入力欄そのもの。キーボードからはクリップから添付する */}
      <div
        className={`rounded-xl border bg-background shadow-xs transition-colors focus-within:border-ring/60 ${
          dragging ? "border-ring border-dashed bg-accent/40" : "border-border"
        }`}
        onDragOver={(event) => {
          event.preventDefault();
          setDragging(true);
        }}
        onDragLeave={(event) => {
          if (event.currentTarget.contains(event.relatedTarget as Node)) return;
          setDragging(false);
        }}
        onDrop={(event) => {
          event.preventDefault();
          setDragging(false);
          addFiles(event.dataTransfer.files);
        }}
      >
        <ComposerAttachments
          key={activePlaceKey}
          items={drafts.items}
          onRemove={drafts.remove}
          onToggleSpoiler={drafts.toggleSpoiler}
          onEdit={(localId, edit) => {
            void drafts.applyEdit(localId, edit);
          }}
          fileFor={drafts.fileFor}
        />
        <textarea
          ref={textareaRef}
          value={value}
          onChange={(event) => updateValue(event.target.value)}
          onKeyDown={onKeyDown}
          onPaste={(event) => {
            // クリップボードの画像・ファイルはそのまま添付にする。
            if (event.clipboardData.files.length === 0) return;
            event.preventDefault();
            addFiles(event.clipboardData.files);
          }}
          onClick={(event) => {
            const caret = event.currentTarget.selectionStart ?? 0;
            setMention(findMentionQuery(value, caret));
          }}
          rows={1}
          placeholder={placeholder}
          aria-label={placeholder}
          className="block w-full resize-none bg-transparent px-3.5 pt-3 pb-1.5 text-[13.5px] leading-6 outline-none placeholder:text-muted-foreground/70"
        />
        <div className="flex items-center gap-1 px-2.5 pb-2">
          <input
            ref={fileInputRef}
            type="file"
            multiple
            className="hidden"
            onChange={(event) => {
              addFiles(event.target.files);
              // 同じファイルを続けて選べるように選択状態を捨てる。
              event.target.value = "";
            }}
          />
          <ComposerPlusMenu items={plusItems} finalFocusRef={textareaRef} />
          <div className="flex items-center rounded-md bg-muted/60 p-0.5">
            {URGENCIES.map((entry) => (
              <button
                key={entry.value}
                type="button"
                title={entry.hint}
                onClick={() => setUrgency(entry.value)}
                className={`rounded px-2 py-0.5 font-medium text-[11px] transition-colors ${
                  urgency === entry.value
                    ? entry.value === "urgent"
                      ? "bg-background text-rose-500 shadow-xs"
                      : "bg-background text-foreground shadow-xs"
                    : "text-muted-foreground hover:text-foreground"
                }`}
              >
                {entry.label}
              </button>
            ))}
          </div>
          <div className="ml-auto flex min-w-0 items-center gap-2">
            <span className="hidden truncate text-[11px] text-muted-foreground/60 sm:inline">
              {uploading ? "アップロード中…" : "Enterで送信・Shift+Enterで改行"}
            </span>
            {/* キーボードを使わずに送れる口。Enter送信と同じsubmitを呼ぶ。 */}
            <button
              type="button"
              onClick={submit}
              disabled={!canSend}
              title="送信（Enter）"
              aria-label="送信"
              className="flex size-7 shrink-0 items-center justify-center rounded-md bg-primary text-primary-foreground transition-opacity enabled:hover:opacity-90 disabled:bg-muted disabled:text-muted-foreground/60"
            >
              <SendHorizontal className="size-3.5" />
            </button>
          </div>
        </div>
      </div>
      {pollOpen ? (
        <PollCreateDialog onClose={() => setPollOpen(false)} />
      ) : null}
    </div>
  );
}
