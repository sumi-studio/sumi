import { Plus, X } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { MAX_POLL_OPTIONS, MIN_POLL_OPTIONS } from "../model";
import { useMessaging } from "../store";

const INPUT_CLASS =
  "w-full rounded-md border border-border bg-background px-2.5 py-1.5 text-[13px] outline-none placeholder:text-muted-foreground/60 focus-visible:border-ring/60";
const DEADLINES = [
  { label: "締切なし", minutes: null },
  { label: "1時間", minutes: 60 },
  { label: "1日", minutes: 1440 },
  { label: "3日", minutes: 4320 },
] as const;

export function PollCreateDialog({ onClose }: { onClose: () => void }) {
  const send = useMessaging((state) => state.send);
  const draft = useMessaging((state) =>
    state.activePlaceKey
      ? (state.draftByPlace[state.activePlaceKey] ?? "")
      : "",
  );
  const [question, setQuestion] = useState("");
  const [options, setOptions] = useState(["", ""]);
  const [allowMulti, setAllowMulti] = useState(false);
  const [deadline, setDeadline] = useState<number | null>(null);
  const questionRef = useRef<HTMLInputElement>(null);
  useEffect(() => questionRef.current?.focus(), []);
  useEffect(() => {
    const close = (event: KeyboardEvent) => event.key === "Escape" && onClose();
    window.addEventListener("keydown", close);
    return () => window.removeEventListener("keydown", close);
  }, [onClose]);

  const filled = options.map((option) => option.trim()).filter(Boolean);
  const distinct = new Set(filled).size === filled.length;
  const ready =
    question.trim() !== "" && filled.length >= MIN_POLL_OPTIONS && distinct;
  return (
    <div className="fixed inset-0 z-40 flex items-center justify-center bg-black/40 p-4">
      <form
        role="dialog"
        aria-modal="true"
        aria-label="投票を作成"
        className="w-96 max-w-full rounded-xl border border-border bg-background p-4 shadow-lg"
        onSubmit={(event) => {
          event.preventDefault();
          if (!ready) return;
          // A poll is sent as one message with the text already composed for
          // this place. This makes the composer clear only after its draft has
          // become part of the outgoing message, rather than silently losing it.
          send(draft, "normal", {
            question: question.trim(),
            options: filled,
            allowMulti,
            closesAt: deadline === null ? null : Date.now() + deadline * 60_000,
          });
          onClose();
        }}
      >
        <div className="flex items-center justify-between">
          <strong className="text-sm">投票を作成</strong>
          <button type="button" aria-label="閉じる" onClick={onClose}>
            <X className="size-4" />
          </button>
        </div>
        <label className="mt-3 block text-xs">
          質問
          <input
            ref={questionRef}
            value={question}
            maxLength={500}
            onChange={(event) => setQuestion(event.target.value)}
            className={`${INPUT_CLASS} mt-1`}
          />
        </label>
        <div className="mt-3 space-y-1.5">
          <span className="text-xs">選択肢（2〜10）</span>
          {options.map((option, index) => (
            <div
              // biome-ignore lint/suspicious/noArrayIndexKey: order is the row identity
              key={index}
              className="flex gap-1"
            >
              <input
                aria-label={`選択肢 ${index + 1}`}
                value={option}
                maxLength={200}
                onChange={(event) =>
                  setOptions((current) =>
                    current.map((entry, at) =>
                      at === index ? event.target.value : entry,
                    ),
                  )
                }
                className={INPUT_CLASS}
              />
              {options.length > MIN_POLL_OPTIONS ? (
                <button
                  type="button"
                  aria-label={`選択肢 ${index + 1} を削除`}
                  onClick={() =>
                    setOptions((current) =>
                      current.filter((_, at) => at !== index),
                    )
                  }
                >
                  <X className="size-4" />
                </button>
              ) : null}
            </div>
          ))}
          {options.length < MAX_POLL_OPTIONS ? (
            <button
              type="button"
              className="flex items-center gap-1 text-xs text-muted-foreground"
              onClick={() => setOptions((current) => [...current, ""])}
            >
              <Plus className="size-4" />
              選択肢を追加
            </button>
          ) : null}
        </div>
        <button
          type="button"
          aria-pressed={allowMulti}
          className="mt-3 block text-sm"
          onClick={() => setAllowMulti((value) => !value)}
        >
          {allowMulti ? "☑" : "☐"} 複数選べるようにする
        </button>
        <div className="mt-3 flex gap-1">
          {DEADLINES.map((choice) => (
            <button
              key={choice.label}
              type="button"
              className={`rounded-full border px-2 py-0.5 text-xs ${deadline === choice.minutes ? "bg-primary/10" : ""}`}
              onClick={() => setDeadline(choice.minutes)}
            >
              {choice.label}
            </button>
          ))}
        </div>
        {!distinct ? (
          <p className="mt-2 text-xs text-rose-500">同じ選択肢は作れません</p>
        ) : null}
        <div className="mt-4 flex justify-end gap-2">
          <button type="button" onClick={onClose}>
            キャンセル
          </button>
          <button
            type="submit"
            disabled={!ready}
            className="rounded bg-primary px-3 py-1.5 text-primary-foreground disabled:opacity-50"
          >
            投票を送信
          </button>
        </div>
      </form>
    </div>
  );
}
