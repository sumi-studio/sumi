import { Plus, X } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { MAX_POLL_OPTIONS, MIN_POLL_OPTIONS } from "../model";
import { useMessaging } from "../store";

/**
 * 投票の作成。質問・選択肢2〜10・複数可トグル・締切（任意）。
 * 送信は通常のsendに乗る——問いと、それを述べる発言は一つの出来事。
 */

const INPUT_CLASS =
  "w-full rounded-md border border-border bg-background px-2.5 py-1.5 text-[13px] outline-none placeholder:text-muted-foreground/60 focus-visible:border-ring/60 disabled:opacity-50";

const DEADLINE_CHOICES: { label: string; minutes: number | null }[] = [
  { label: "締切なし", minutes: null },
  { label: "1時間", minutes: 60 },
  { label: "1日", minutes: 24 * 60 },
  { label: "3日", minutes: 3 * 24 * 60 },
];

export function PollCreateDialog({ onClose }: { onClose: () => void }) {
  const send = useMessaging((state) => state.send);
  const [question, setQuestion] = useState("");
  const [options, setOptions] = useState(["", ""]);
  const [allowMulti, setAllowMulti] = useState(false);
  const [deadline, setDeadline] = useState<number | null>(null);
  const questionRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    questionRef.current?.focus();
  }, []);

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [onClose]);

  const filled = options.map((option) => option.trim()).filter(Boolean);
  // 同じ文字の選択肢は投票者から見て区別できないので、作らせない。
  const distinct = new Set(filled).size === filled.length;
  const ready =
    question.trim() !== "" && filled.length >= MIN_POLL_OPTIONS && distinct;

  const submit = (event: React.FormEvent) => {
    event.preventDefault();
    if (!ready) return;
    send("", "normal", [], {
      question: question.trim(),
      allowMulti,
      closesAt: deadline === null ? null : Date.now() + deadline * 60_000,
      options: filled,
    });
    onClose();
  };

  return (
    <div className="fixed inset-0 z-40 flex items-center justify-center bg-black/40 p-4">
      <form
        onSubmit={submit}
        role="dialog"
        aria-modal="true"
        aria-label="投票を作成"
        className="w-96 max-w-full rounded-xl border border-border bg-background p-4 shadow-lg"
      >
        <div className="flex items-center justify-between">
          <p className="font-semibold text-[14px]">投票を作成</p>
          <button
            type="button"
            title="閉じる"
            onClick={onClose}
            className="rounded p-1 text-muted-foreground hover:bg-accent hover:text-foreground"
          >
            <X className="size-3.5" />
          </button>
        </div>
        <label className="mt-3 block">
          <span className="mb-1 block text-[11px] text-muted-foreground">
            質問
          </span>
          <input
            ref={questionRef}
            value={question}
            maxLength={500}
            onChange={(event) => setQuestion(event.target.value)}
            placeholder="例: リリースはいつにしますか？"
            className={INPUT_CLASS}
          />
        </label>
        <div className="mt-3">
          <span className="mb-1 block text-[11px] text-muted-foreground">
            選択肢（{MIN_POLL_OPTIONS}〜{MAX_POLL_OPTIONS}）
          </span>
          <div className="space-y-1.5">
            {options.map((option, index) => (
              <div
                // 並びが同一性そのものなので、index を鍵にするのが正しい。
                // biome-ignore lint/suspicious/noArrayIndexKey: 行の同一性は並び順
                key={index}
                className="flex items-center gap-1.5"
              >
                <input
                  value={option}
                  maxLength={200}
                  onChange={(event) =>
                    setOptions((current) =>
                      current.map((entry, at) =>
                        at === index ? event.target.value : entry,
                      ),
                    )
                  }
                  placeholder={`選択肢 ${index + 1}`}
                  aria-label={`選択肢 ${index + 1}`}
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
                    className="shrink-0 rounded p-1 text-muted-foreground hover:bg-accent hover:text-foreground"
                  >
                    <X className="size-3.5" />
                  </button>
                ) : null}
              </div>
            ))}
          </div>
          {options.length < MAX_POLL_OPTIONS ? (
            <button
              type="button"
              onClick={() => setOptions((current) => [...current, ""])}
              className="mt-1.5 flex items-center gap-1 rounded-md px-1.5 py-1 text-[12px] text-muted-foreground hover:bg-accent hover:text-foreground"
            >
              <Plus className="size-3.5" />
              選択肢を追加
            </button>
          ) : null}
        </div>
        <button
          type="button"
          onClick={() => setAllowMulti((value) => !value)}
          aria-pressed={allowMulti}
          className="mt-3 flex w-full items-center gap-2 rounded-md px-1.5 py-1.5 text-left text-[13px] hover:bg-accent"
        >
          <span
            className={`flex size-4 shrink-0 items-center justify-center rounded border ${
              allowMulti
                ? "border-primary bg-primary text-primary-foreground"
                : "border-border"
            }`}
          >
            {allowMulti ? <span className="text-[10px]">✓</span> : null}
          </span>
          複数選べるようにする
        </button>
        <div className="mt-3">
          <span className="mb-1 block text-[11px] text-muted-foreground">
            締切（任意）
          </span>
          <div className="flex flex-wrap gap-1">
            {DEADLINE_CHOICES.map((choice) => (
              <button
                key={choice.label}
                type="button"
                onClick={() => setDeadline(choice.minutes)}
                className={`rounded-full border px-2 py-0.5 text-[12px] transition-colors ${
                  deadline === choice.minutes
                    ? "border-primary/50 bg-primary/10 font-medium"
                    : "border-border hover:border-muted-foreground/40"
                }`}
              >
                {choice.label}
              </button>
            ))}
          </div>
        </div>
        {!distinct ? (
          <p className="mt-2 text-[11px] text-rose-500">
            同じ選択肢は作れません
          </p>
        ) : null}
        <div className="mt-4 flex justify-end gap-1.5">
          <button
            type="button"
            onClick={onClose}
            className="rounded-md px-2.5 py-1.5 text-[12.5px] text-muted-foreground hover:bg-accent"
          >
            キャンセル
          </button>
          <button
            type="submit"
            disabled={!ready}
            className="rounded-md bg-primary px-2.5 py-1.5 font-medium text-[12.5px] text-primary-foreground hover:opacity-90 disabled:opacity-50"
          >
            投票を送信
          </button>
        </div>
      </form>
    </div>
  );
}
