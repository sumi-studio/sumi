import {
  AtSign,
  Check,
  CornerUpLeft,
  Paperclip,
  Pencil,
  SendHorizontal,
  X,
} from "lucide-react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { isImeComposing } from "../../lib/ime";
import { isInsideUnclosedCodeFence } from "../compose-fence";
import type { DraftAttachment } from "../draft-attachments";
import type { MemberProfile, Message, Urgency } from "../model";
import { MAX_ATTACHMENTS_PER_MESSAGE, participantKey } from "../model";
import { useMessaging } from "../store";
import { usePlaceDisplay } from "../use-place-name";
import { ComposerAttachments } from "./composer-attachments";
import type { ComposerPlusMenuItem } from "./composer-plus-menu";
import { ComposerPlusMenu } from "./composer-plus-menu";
import { useWheelPassthrough } from "./overlay";
import { ParticipantAvatar } from "./participant-avatar";

const MAX_HEIGHT_PX = 220;
const TYPING_THROTTLE_MS = 2_000;

/** selectorは毎回同じ参照を返す必要がある（新しい[]を作ると無限再レンダー）。 */
const NO_MESSAGES: Message[] = [];
const NO_DRAFT_ATTACHMENTS: DraftAttachment[] = [];

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
  const editingMessageId = useMessaging((state) => state.editingMessageId);
  const cancelEdit = useMessaging((state) => state.cancelEdit);
  const submitEdit = useMessaging((state) => state.submitEdit);
  const startEdit = useMessaging((state) => state.startEdit);
  const replyTargetId = useMessaging((state) => state.replyTargetId);
  const setReplyTarget = useMessaging((state) => state.setReplyTarget);
  const draftAttachments = useMessaging((state) =>
    state.activePlaceKey
      ? (state.draftAttachmentsByPlace[state.activePlaceKey] ??
        NO_DRAFT_ATTACHMENTS)
      : NO_DRAFT_ATTACHMENTS,
  );
  const attachmentOverflow = useMessaging((state) =>
    state.activePlaceKey
      ? (state.draftAttachmentOverflowByPlace[state.activePlaceKey] ?? 0)
      : 0,
  );
  const addDraftAttachments = useMessaging(
    (state) => state.addDraftAttachments,
  );
  const removeDraftAttachment = useMessaging(
    (state) => state.removeDraftAttachment,
  );
  const retryDraftAttachment = useMessaging(
    (state) => state.retryDraftAttachment,
  );
  const editDraftAttachment = useMessaging(
    (state) => state.editDraftAttachment,
  );
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [dragging, setDragging] = useState(false);

  const display = usePlaceDisplay(activePlaceKey);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const [urgency, setUrgency] = useState<Urgency>("normal");
  const [editValue, setEditValue] = useState("");
  const [mention, setMention] = useState<MentionQuery | null>(null);
  const [mentionIndex, setMentionIndex] = useState(0);
  const lastTypingAt = useRef(0);
  // IME変換中か。keydownはeventからisComposingを見られる（lib/ime.ts）が、
  // クリックのイベントには変換の状態が乗らない。ボタンで送るときに変換を
  // 終わらせるべきか判断するために、compositionの生死をここで持つ
  // （message-searchと同じ流儀）。送るか止めるかの判断には使わない。
  const composing = useRef(false);
  const mentionPassthroughRef = useWheelPassthrough<HTMLDivElement>();

  const editingMessage = editingMessageId
    ? messages.find((entry) => entry.messageId === editingMessageId)
    : undefined;
  const replyTarget = replyTargetId
    ? messages.find((entry) => entry.messageId === replyTargetId)
    : undefined;
  const replyAuthor = replyTarget
    ? membersByKey[participantKey(replyTarget.author)]
    : undefined;

  const editing = editingMessage !== undefined;
  const value = editing ? editValue : draft;

  useEffect(() => {
    if (editingMessage) setEditValue(editingMessage.content);
  }, [editingMessage]);

  // biome-ignore lint/correctness/useExhaustiveDependencies: 編集開始・place切替・返信開始をフォーカスのトリガーにする
  useEffect(() => {
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

  // 入力値の置き場所（編集中はeditValue、通常はdraft）だけを面倒みる。
  // メンション候補の開閉は呼び出し側の事情で変わるのでここには含めない。
  const writeValue = useCallback(
    (next: string) => {
      if (editing) {
        setEditValue(next);
        return;
      }
      if (!activePlaceKey) return;
      setDraft(activePlaceKey, next);
      const now = Date.now();
      if (next.trim() && now - lastTypingAt.current > TYPING_THROTTLE_MS) {
        lastTypingAt.current = now;
        sendTyping();
      }
    },
    [editing, activePlaceKey, setDraft, sendTyping],
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
    const caret = textareaRef.current?.selectionStart ?? value.length;
    const before = value.slice(0, caret);
    const inserted = before === "" || /\s$/.test(before) ? "@" : " @";
    const next = before + inserted + value.slice(caret);
    const nextCaret = caret + inserted.length;
    writeValue(next);
    setMention({ query: "", start: nextCaret - 1, end: nextCaret });
    setMentionIndex(0);
    window.requestAnimationFrame(() => {
      const textarea = textareaRef.current;
      if (!textarea) return;
      textarea.focus();
      textarea.setSelectionRange(nextCaret, nextCaret);
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

  const attachmentsSettled = draftAttachments.every(
    (entry) => entry.status === "ready",
  );
  const readyAttachmentCount = draftAttachments.filter(
    (entry) => entry.status === "ready",
  ).length;
  const attachmentsFull =
    draftAttachments.length >= MAX_ATTACHMENTS_PER_MESSAGE;
  // 送れるか（＝編集なら保存できるか）を、対象の文字列から決める。ボタンの活殺は
  // 描画時のReactの値で、submitの関門は送る直前の入力欄の中身で、同じ規則を見る。
  // 編集中の空本文はsubmitEditが黙って捨てるだけなので「保存できない」に倒し、
  // 取り消しはEsc・×の明示の口に一本化する。
  const canSubmitText = useCallback(
    (text: string) =>
      editing
        ? text.trim().length > 0
        : attachmentsSettled &&
          (text.trim().length > 0 || readyAttachmentCount > 0),
    [editing, attachmentsSettled, readyAttachmentCount],
  );
  const canSubmit = canSubmitText(value);

  /**
   * 送信・保存の唯一の関門。Enterもボタンもここを通る。
   *
   * 押されたら「いま入力欄の箱に入っているもの」を確定して送る。変換中かどうかで
   * 仕事を変えない。変換中のEnterを止めるのは、そのEnterがIMEの確定という見える
   * 仕事をするからで、それはEnter経路（onKeyDown）だけの事情。ボタンには対応する
   * 仕事が無いので、ここで同じように止めると、画面が何も変わらず理由も出ないまま
   * enabledで居続ける死んだボタンになる。狭い幅ではこれが唯一の送信口なので、
   * 押されたら必ず「送る」か「送れない理由を出す」のどちらかにする。
   */
  const submit = useCallback(() => {
    // 変換の途中で押されたら、まず終わらせる。blurでcompositionendが来る環境
    // （Chromeデスクトップ）ではそこで確定し、来ない環境（Safari・ソフトキーボード）
    // でも、いま見えている文字を下でそのまま採る。どちらでも押した瞬間に見えている
    // ものが送られ、送ったあとに変換の残りが空の入力欄へ戻ることも無い。
    if (composing.current) {
      composing.current = false;
      textareaRef.current?.blur();
    }
    // 送る値は入力欄の実際の中身。上のblurで確定した直後は、こちらがReactの値より新しい。
    const text = textareaRef.current?.value ?? value;
    if (!canSubmitText(text)) return;
    if (editing) {
      submitEdit(text);
    } else {
      send(text, urgency);
      setUrgency("normal");
      setMention(null);
    }
    // どの入口から送っても、次の一文字は入力欄に入る。ボタンを押して送った直後に
    // ボタンがdisabledになってフォーカスが行き場を失うのを防ぐ意味もある。
    textareaRef.current?.focus();
  }, [canSubmitText, editing, submitEdit, value, send, urgency]);

  const acceptFiles = useCallback(
    (list: FileList | File[] | null | undefined) => {
      if (editing || !list) return;
      const files = Array.from(list).filter((file) => file.size >= 0);
      if (files.length > 0) addDraftAttachments(files);
    },
    [editing, addDraftAttachments],
  );

  // ＋メニューの品書き。入口が増えてもツールバーにボタンを生やさずここへ足す。
  const plusItems = useMemo<ComposerPlusMenuItem[]>(
    () => [
      {
        id: "attach",
        label: "ファイルを添付",
        hint: attachmentsFull
          ? `1通につき${MAX_ATTACHMENTS_PER_MESSAGE}件まで`
          : "貼り付け・ドロップも可",
        icon: Paperclip,
        disabled: attachmentsFull,
        onSelect: () => fileInputRef.current?.click(),
      },
      {
        id: "mention",
        label: "メンション",
        hint: "@ で相手を呼ぶ",
        icon: AtSign,
        onSelect: insertMentionTrigger,
      },
    ],
    [attachmentsFull, insertMentionTrigger],
  );

  const onPaste = useCallback(
    (event: React.ClipboardEvent<HTMLTextAreaElement>) => {
      const files = Array.from(event.clipboardData?.files ?? []);
      if (files.length === 0) return;
      // 画像の貼り付けはファイルとして積む。テキストは通常どおり入力に流す。
      event.preventDefault();
      acceptFiles(files);
    },
    [acceptFiles],
  );

  const onDrop = useCallback(
    (event: React.DragEvent<HTMLDivElement>) => {
      event.preventDefault();
      setDragging(false);
      acceptFiles(event.dataTransfer?.files);
    },
    [acceptFiles],
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
        if (editing) {
          event.preventDefault();
          cancelEdit();
        } else if (replyTargetId) {
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
      // 空欄で↑ = 自分の直前のメッセージを編集（Discordと同じ手癖）。
      if (event.key === "ArrowUp" && !editing && value === "") {
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
      editing,
      cancelEdit,
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

  // ツールバー右端の一言。送れない理由があるときは、それをEnterにもボタンにも
  // 共通の理由として出す（幅が狭くても隠さない）。無ければキーボードの案内に戻す。
  const blockedNotice = editing
    ? canSubmit
      ? null
      : "本文が空だと保存できません（Escで取り消し）"
    : attachmentOverflow > 0
      ? `上限のため${attachmentOverflow}件のファイルを追加できませんでした`
      : draftAttachments.some((entry) => entry.status === "edit_failed")
        ? "添付の保存に失敗しました。鉛筆から内容を直すか、再送してください"
        : draftAttachments.length > 0 && !attachmentsSettled
          ? "添付の準備ができると送信できます"
          : null;

  return (
    <section
      aria-label="メッセージ入力"
      className="relative shrink-0 px-4 pb-4 sm:px-6"
      onDragOver={(event) => {
        if (editing) return;
        event.preventDefault();
        setDragging(true);
      }}
      onDragLeave={() => setDragging(false)}
      onDrop={onDrop}
    >
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
                {/* 説明は右端に寄せて、名前だけを縦に目で追えるようにする。 */}
                <span className="ml-auto truncate text-muted-foreground text-xs">
                  {member.tagline}
                </span>
              </button>
            );
          })}
        </div>
      ) : null}
      {editing ? (
        <div className="mb-1 flex items-center gap-2 text-muted-foreground text-xs">
          <Pencil className="size-3" />
          メッセージを編集中
          <span className="text-muted-foreground/70">
            Enterで保存・Escでキャンセル
          </span>
          <button
            type="button"
            onClick={cancelEdit}
            className="ml-auto rounded p-0.5 hover:bg-accent"
            aria-label="編集をキャンセル"
          >
            <X className="size-3.5" />
          </button>
        </div>
      ) : null}
      {!editing && replyTarget && replyAuthor ? (
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
      <div
        className={`rounded-xl border bg-background shadow-xs transition-colors focus-within:border-ring/60 ${
          dragging ? "border-ring/80 bg-accent/30" : "border-border"
        }`}
      >
        {editing ? null : (
          <ComposerAttachments
            drafts={draftAttachments}
            onRemove={removeDraftAttachment}
            onRetry={retryDraftAttachment}
            onEdit={editDraftAttachment}
          />
        )}
        <textarea
          ref={textareaRef}
          value={value}
          onChange={(event) => updateValue(event.target.value)}
          onKeyDown={onKeyDown}
          onCompositionStart={() => {
            composing.current = true;
          }}
          onCompositionEnd={(event) => {
            composing.current = false;
            // 変換中のonChangeは未変換の読みを見ている。確定した値で組み直す。
            updateValue(event.currentTarget.value);
          }}
          onPaste={onPaste}
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
          {/* 編集中は緊急度セレクタを不可視にするだけで場所は保つ
              （編集開始でツールバー行の高さが変わり入力欄が跳ねないように）。 */}
          <div
            className={`flex items-center rounded-md bg-muted/60 p-0.5 ${
              editing ? "invisible" : ""
            }`}
          >
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
          {!editing ? (
            <>
              <input
                ref={fileInputRef}
                type="file"
                multiple
                className="hidden"
                data-testid="composer-file-input"
                onChange={(event) => {
                  acceptFiles(event.currentTarget.files);
                  event.currentTarget.value = "";
                }}
              />
              <ComposerPlusMenu items={plusItems} finalFocusRef={textareaRef} />
            </>
          ) : null}
          <div className="ml-auto flex min-w-0 items-center gap-2">
            {/* 送れない理由は常に出す。理由が無いときのキーボードの案内だけは、
                編集中は上の編集帯が同じことを言うので出さず、狭い幅では
                送信ボタンに場所を譲る。 */}
            {blockedNotice ? (
              <span className="truncate text-[11px] text-muted-foreground/60">
                {blockedNotice}
              </span>
            ) : editing ? null : (
              <span className="hidden truncate text-[11px] text-muted-foreground/60 sm:inline">
                Enterで送信・Shift+Enterで改行
              </span>
            )}
            {/* キーボードを使わずに送れる口。Enter送信と同じsubmitを呼ぶだけで、
                規律（押されたら確定して送る・送ったら入力欄へ戻す）はsubmitが持つ。
                mousedownの既定は止めない。止めるとblurによる変換確定を潰してしまう。 */}
            <button
              type="button"
              onClick={submit}
              disabled={!canSubmit}
              title={editing ? "編集を保存（Enter）" : "送信（Enter）"}
              aria-label={editing ? "編集を保存" : "送信"}
              className="flex size-7 shrink-0 items-center justify-center rounded-md bg-primary text-primary-foreground transition-opacity enabled:hover:opacity-90 disabled:bg-muted disabled:text-muted-foreground/60"
            >
              {editing ? (
                <Check className="size-3.5" />
              ) : (
                <SendHorizontal className="size-3.5" />
              )}
            </button>
          </div>
        </div>
      </div>
    </section>
  );
}
