import {
  type ChangeEvent,
  type KeyboardEvent,
  type MouseEvent,
  type RefObject,
  useCallback,
  useMemo,
  useRef,
  useState,
} from "react";
import type { MemberProfile, ParticipantKey } from "../model";
import { participantKey } from "../model";
import { useWheelPassthrough } from "./overlay";
import { ParticipantAvatar } from "./participant-avatar";

interface MentionQuery {
  query: string;
  start: number;
  end: number;
}

/** キャレット直前の、まだ確定していない @表示名 を見つける。 */
export function findMentionQuery(
  value: string,
  caret: number,
): MentionQuery | null {
  const before = value.slice(0, caret);
  const match = /(^|\s)@([^\s@]*)$/.exec(before);
  if (!match) return null;
  const start = match.index + match[1].length;
  return { query: match[2], start, end: caret };
}

export interface MentionAutocomplete {
  candidates: MemberProfile[];
  activeIndex: number;
  dismiss(): void;
  insertTrigger(): void;
  onInputChange(event: ChangeEvent<HTMLTextAreaElement>): void;
  onInputClick(event: MouseEvent<HTMLTextAreaElement>): void;
  /** 候補操作を処理したときだけ true。送信・取消などは入力欄自身に委ねる。 */
  onKeyDown(event: KeyboardEvent<HTMLTextAreaElement>): boolean;
  select(member: MemberProfile): void;
}

/**
 * テキスト入力で共用するメンション補完。
 *
 * 入力値の正本は呼び出し側に残す。ここは @ クエリ、候補、選択、キャレット復元だけを
 * 持つため、Composer の下書きとインライン編集のセッションのどちらにも接続できる。
 */
export function useMentionAutocomplete({
  value,
  onValueChange,
  inputRef,
  membersByKey,
  selfKey,
}: {
  value: string;
  onValueChange(value: string): void;
  inputRef: RefObject<HTMLTextAreaElement | null>;
  membersByKey: Record<ParticipantKey, MemberProfile>;
  selfKey: ParticipantKey;
}): MentionAutocomplete {
  const [mention, setMention] = useState<MentionQuery | null>(null);
  const [mentionIndex, setMentionIndex] = useState(0);
  const valueRef = useRef(value);
  valueRef.current = value;

  const candidates = useMemo(() => {
    if (!mention) return [];
    const query = mention.query.toLowerCase();
    return Object.values(membersByKey)
      .filter((member) => participantKey(member.participant) !== selfKey)
      .filter((member) => member.displayName.toLowerCase().includes(query))
      .slice(0, 6);
  }, [mention, membersByKey, selfKey]);
  const activeIndex = Math.min(
    mentionIndex,
    Math.max(candidates.length - 1, 0),
  );

  const updateValue = useCallback(
    (next: string) => {
      valueRef.current = next;
      onValueChange(next);
    },
    [onValueChange],
  );
  const updateMention = useCallback((next: string, caret: number) => {
    setMention(findMentionQuery(next, caret));
    setMentionIndex(0);
  }, []);
  const dismiss = useCallback(() => setMention(null), []);
  const insertTrigger = useCallback(() => {
    const textarea = inputRef.current;
    const current = valueRef.current;
    const caret = textarea?.selectionStart ?? current.length;
    const before = current.slice(0, caret);
    const inserted = before === "" || /\s$/.test(before) ? "@" : " @";
    const next = before + inserted + current.slice(caret);
    const nextCaret = caret + inserted.length;
    updateValue(next);
    setMention(findMentionQuery(next, nextCaret));
    setMentionIndex(0);
    window.requestAnimationFrame(() => {
      const input = inputRef.current;
      if (!input) return;
      input.focus();
      input.setSelectionRange(nextCaret, nextCaret);
    });
  }, [inputRef, updateValue]);

  const select = useCallback(
    (member: MemberProfile) => {
      if (!mention) return;
      const inserted = `@${member.displayName} `;
      const current = valueRef.current;
      const next =
        current.slice(0, mention.start) + inserted + current.slice(mention.end);
      updateValue(next);
      setMention(null);
      window.requestAnimationFrame(() => {
        const textarea = inputRef.current;
        if (!textarea) return;
        const caret = mention.start + inserted.length;
        textarea.setSelectionRange(caret, caret);
        textarea.focus();
      });
    },
    [inputRef, mention, updateValue],
  );

  return {
    candidates,
    activeIndex,
    dismiss,
    insertTrigger,
    onInputChange(event) {
      const next = event.target.value;
      updateValue(next);
      updateMention(next, event.target.selectionStart ?? next.length);
    },
    onInputClick(event) {
      updateMention(
        valueRef.current,
        event.currentTarget.selectionStart ?? valueRef.current.length,
      );
    },
    onKeyDown(event) {
      if (!mention || candidates.length === 0) return false;
      if (event.key === "ArrowDown") {
        event.preventDefault();
        setMentionIndex((index) => (index + 1) % candidates.length);
        return true;
      }
      if (event.key === "ArrowUp") {
        event.preventDefault();
        setMentionIndex(
          (index) => (index - 1 + candidates.length) % candidates.length,
        );
        return true;
      }
      if (event.key === "Enter" || event.key === "Tab") {
        event.preventDefault();
        const candidate = candidates[activeIndex];
        if (candidate) select(candidate);
        return true;
      }
      if (event.key === "Escape") {
        event.preventDefault();
        dismiss();
        return true;
      }
      return false;
    },
    select,
  };
}

/** Composer と編集欄で同じ候補の見せ方・マウス選択を使う。 */
export function MentionSuggestions({
  autocomplete,
  className,
}: {
  autocomplete: MentionAutocomplete;
  className: string;
}) {
  const passthroughRef = useWheelPassthrough<HTMLDivElement>();
  const { candidates, activeIndex, select } = autocomplete;
  if (candidates.length === 0) return null;
  return (
    <div
      ref={passthroughRef}
      className={className}
      data-testid="mention-suggestions"
    >
      {candidates.map((member, index) => {
        const key = participantKey(member.participant);
        return (
          <button
            key={key}
            type="button"
            onMouseDown={(event) => {
              event.preventDefault();
              select(member);
            }}
            className={`flex w-full items-center gap-2 px-2.5 py-1.5 text-left text-[13px] ${
              index === activeIndex ? "bg-accent" : ""
            }`}
          >
            <ParticipantAvatar
              participantKey={key}
              name={member.displayName}
              size={20}
            />
            <span className="font-medium">{member.displayName}</span>
            <span className="truncate text-muted-foreground text-xs">
              {member.tagline}
            </span>
          </button>
        );
      })}
    </div>
  );
}
