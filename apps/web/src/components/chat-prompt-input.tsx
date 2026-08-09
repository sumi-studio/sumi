import {
  PromptInput,
  PromptInputButton,
  PromptInputFooter,
  PromptInputTextarea,
  PromptInputTools,
} from "@sumi/ui/ai-elements/prompt-input";
import { ArrowUpIcon, SquareIcon } from "lucide-react";
import type { KeyboardEvent } from "react";
import { useEffect, useRef } from "react";
import { isImeComposing } from "../lib/ime";

const MIN_HEIGHT = 42;
const MAX_HEIGHT = 186;
const isCoarsePointer =
  typeof window !== "undefined" &&
  typeof window.matchMedia === "function" &&
  window.matchMedia("(pointer: coarse)").matches;

interface ChatPromptInputProps {
  value: string;
  onValueChange: (value: string) => void;
  onSend: () => void;
  onAbort: () => void;
  streaming: boolean;
  disabled?: boolean;
  placeholder?: string;
  className?: string;
}

/** The v1 direct-chat composer. Attachments stay hidden until the wire accepts them. */
export function ChatPromptInput({
  value,
  onValueChange,
  onSend,
  onAbort,
  streaming,
  disabled = false,
  placeholder = "メッセージを入力…",
  className,
}: ChatPromptInputProps) {
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const canSend = !disabled && value.trim().length > 0;

  // biome-ignore lint/correctness/useExhaustiveDependencies: value changes trigger height measurement
  useEffect(() => {
    const textarea = textareaRef.current;
    if (!textarea) return;
    textarea.style.height = "auto";
    textarea.style.height = `${Math.max(
      MIN_HEIGHT,
      Math.min(textarea.scrollHeight, MAX_HEIGHT),
    )}px`;
  }, [value]);

  const handleKeyDown = (event: KeyboardEvent<HTMLTextAreaElement>) => {
    if (event.key === "Enter" && !event.shiftKey && !isImeComposing(event)) {
      event.preventDefault();
      if (canSend) onSend();
    }
  };

  return (
    <PromptInput
      className={className}
      onSubmit={(event) => {
        event.preventDefault();
        if (canSend) onSend();
      }}
    >
      <PromptInputTextarea
        ref={textareaRef}
        rows={1}
        value={value}
        disabled={disabled}
        aria-label="メッセージ"
        placeholder={placeholder}
        enterKeyHint={isCoarsePointer ? "send" : undefined}
        onChange={(event) => onValueChange(event.target.value)}
        onKeyDown={handleKeyDown}
        onBeforeInput={(event) => {
          if (
            isCoarsePointer &&
            (event.nativeEvent as InputEvent).inputType === "insertLineBreak"
          ) {
            event.preventDefault();
            if (canSend) onSend();
          }
        }}
      />
      <PromptInputFooter>
        <div className="flex-1" />
        <PromptInputTools>
          {streaming && (
            <PromptInputButton
              label="停止"
              tooltip="停止"
              variant="default"
              disabled={disabled}
              onClick={onAbort}
            >
              <SquareIcon className="size-3.5 fill-current" />
            </PromptInputButton>
          )}
          {(!streaming || canSend) && (
            <PromptInputButton
              label={streaming ? "割り込んで送信" : "送信"}
              tooltip={streaming ? "割り込んで送信" : "送信"}
              variant="default"
              disabled={!canSend}
              onClick={onSend}
            >
              <ArrowUpIcon className="size-4.5" />
            </PromptInputButton>
          )}
        </PromptInputTools>
      </PromptInputFooter>
    </PromptInput>
  );
}
