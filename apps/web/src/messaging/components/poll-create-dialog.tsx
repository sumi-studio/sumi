import { Plus, X } from "lucide-react";
import { useMemo, useRef, useState } from "react";
import { clampCodePoints, codePointLength } from "../../lib/text-length";
import {
  MAX_POLL_OPTION_CODE_POINTS,
  MAX_POLL_OPTIONS,
  MAX_POLL_QUESTION_CODE_POINTS,
  MIN_POLL_OPTIONS,
  type PollInput,
} from "../model";
import { useMessaging } from "../store";
import { ModalDialog } from "./modal-dialog";

const INPUT_CLASS =
  "w-full rounded-md border border-border bg-background px-2.5 py-1.5 text-[13px] outline-none placeholder:text-muted-foreground/60 focus-visible:border-ring/60";

const DEADLINES = [
  { label: "締切なし", minutes: null },
  { label: "1時間", minutes: 60 },
  { label: "1日", minutes: 24 * 60 },
  { label: "3日", minutes: 3 * 24 * 60 },
] as const;

export function PollCreateDialog({
  onClose,
  onSubmit,
}: {
  onClose: () => void;
  onSubmit: (poll: PollInput) => boolean;
}) {
  const activePlaceKey = useMessaging((state) => state.activePlaceKey);
  const pollsEnabled = useMessaging((state) =>
    Boolean(state.capabilities.polls),
  );
  const hasDraftAttachments = useMessaging((state) =>
    state.activePlaceKey
      ? (state.draftAttachmentsByPlace[state.activePlaceKey]?.length ?? 0) > 0
      : false,
  );
  const [question, setQuestion] = useState("");
  const [options, setOptions] = useState(["", ""]);
  const [allowMulti, setAllowMulti] = useState(false);
  const [deadlineMinutes, setDeadlineMinutes] = useState<number | null>(null);
  const questionRef = useRef<HTMLInputElement>(null);

  const filledOptions = useMemo(
    () => options.map((option) => option.trim()).filter(Boolean),
    [options],
  );
  const distinct = new Set(filledOptions).size === filledOptions.length;
  const normalizedQuestion = question.trim();
  const ready =
    pollsEnabled &&
    activePlaceKey !== null &&
    normalizedQuestion.length > 0 &&
    codePointLength(normalizedQuestion) <= MAX_POLL_QUESTION_CODE_POINTS &&
    filledOptions.length >= MIN_POLL_OPTIONS &&
    filledOptions.length <= MAX_POLL_OPTIONS &&
    filledOptions.every(
      (option) => codePointLength(option) <= MAX_POLL_OPTION_CODE_POINTS,
    ) &&
    distinct;

  return (
    <ModalDialog
      label="投票を作成"
      onClose={onClose}
      initialFocusRef={questionRef}
      className="fixed inset-0 z-40 flex items-center justify-center bg-black/40 p-4"
      onBackdropClick={(event) => {
        if (event.target === event.currentTarget) onClose();
      }}
      testId="poll-create-dialog"
    >
      <form
        className="w-96 max-w-full rounded-xl border border-border bg-background p-4 shadow-lg"
        onSubmit={(event) => {
          event.preventDefault();
          if (!ready || hasDraftAttachments) return;
          const accepted = onSubmit({
            question: normalizedQuestion,
            options: filledOptions,
            allowMulti,
            closesAt:
              deadlineMinutes === null
                ? null
                : Date.now() + deadlineMinutes * 60_000,
          });
          if (accepted) onClose();
        }}
      >
        <div className="flex items-center justify-between gap-3">
          <strong className="text-sm">投票を作成</strong>
          <button
            type="button"
            aria-label="投票作成を閉じる"
            onClick={onClose}
            className="flex size-7 items-center justify-center rounded-md text-muted-foreground hover:bg-accent hover:text-foreground"
          >
            <X className="size-4" />
          </button>
        </div>

        <label className="mt-3 block text-xs">
          質問
          <input
            ref={questionRef}
            value={question}
            onChange={(event) =>
              setQuestion(
                clampCodePoints(
                  event.target.value,
                  MAX_POLL_QUESTION_CODE_POINTS,
                ),
              )
            }
            className={`${INPUT_CLASS} mt-1`}
          />
        </label>

        <fieldset className="mt-3 space-y-1.5">
          <legend className="text-xs">選択肢（2〜10）</legend>
          {options.map((option, index) => (
            <div
              // biome-ignore lint/suspicious/noArrayIndexKey: row order is the draft identity
              key={index}
              className="flex gap-1"
            >
              <input
                aria-label={`選択肢 ${index + 1}`}
                value={option}
                onChange={(event) => {
                  const value = clampCodePoints(
                    event.target.value,
                    MAX_POLL_OPTION_CODE_POINTS,
                  );
                  setOptions((current) =>
                    current.map((entry, at) => (at === index ? value : entry)),
                  );
                }}
                className={INPUT_CLASS}
              />
              {options.length > MIN_POLL_OPTIONS ? (
                <button
                  type="button"
                  aria-label={`選択肢 ${index + 1} を削除`}
                  onClick={() =>
                    setOptions((current) =>
                      current.filter((_entry, at) => at !== index),
                    )
                  }
                  className="flex size-8 shrink-0 items-center justify-center rounded-md text-muted-foreground hover:bg-accent"
                >
                  <X className="size-4" />
                </button>
              ) : null}
            </div>
          ))}
          {options.length < MAX_POLL_OPTIONS ? (
            <button
              type="button"
              className="flex items-center gap-1 rounded px-1 py-0.5 text-xs text-muted-foreground hover:bg-accent hover:text-foreground"
              onClick={() => setOptions((current) => [...current, ""])}
            >
              <Plus className="size-4" />
              選択肢を追加
            </button>
          ) : null}
        </fieldset>

        <button
          type="button"
          aria-pressed={allowMulti}
          className="mt-3 flex items-center gap-2 rounded-md px-1 py-1 text-sm hover:bg-accent"
          onClick={() => setAllowMulti((value) => !value)}
        >
          <span
            aria-hidden
            className={`flex size-4 items-center justify-center rounded-[3px] border ${
              allowMulti
                ? "border-primary bg-primary text-primary-foreground"
                : "border-muted-foreground/40"
            }`}
          >
            {allowMulti ? "✓" : null}
          </span>
          複数選べるようにする
        </button>

        <fieldset className="mt-3">
          <legend className="mb-1.5 text-xs">締切</legend>
          <div className="flex flex-wrap gap-1">
            {DEADLINES.map((choice) => (
              <button
                key={choice.label}
                type="button"
                aria-pressed={deadlineMinutes === choice.minutes}
                className={`rounded-full border px-2 py-0.5 text-xs ${
                  deadlineMinutes === choice.minutes
                    ? "border-primary/40 bg-primary/10 text-foreground"
                    : "border-border text-muted-foreground hover:bg-accent"
                }`}
                onClick={() => setDeadlineMinutes(choice.minutes)}
              >
                {choice.label}
              </button>
            ))}
          </div>
        </fieldset>

        {!distinct ? (
          <p role="alert" className="mt-2 text-xs text-rose-500">
            同じ選択肢は作れません
          </p>
        ) : null}
        {hasDraftAttachments ? (
          <p role="alert" className="mt-2 text-xs text-muted-foreground">
            添付付きの投票は作成できません。添付を外すと送信できます。
          </p>
        ) : null}
        {!pollsEnabled ? (
          <p role="alert" className="mt-2 text-xs text-muted-foreground">
            この接続では投票を送信できません。
          </p>
        ) : null}

        <div className="mt-4 flex justify-end gap-2">
          <button
            type="button"
            onClick={onClose}
            className="rounded-md px-3 py-1.5 text-sm text-muted-foreground hover:bg-accent"
          >
            キャンセル
          </button>
          <button
            type="submit"
            disabled={!ready || hasDraftAttachments}
            className="rounded-md bg-primary px-3 py-1.5 font-medium text-primary-foreground text-sm hover:opacity-90 disabled:opacity-50"
          >
            投票を送信
          </button>
        </div>
      </form>
    </ModalDialog>
  );
}
