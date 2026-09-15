import { Button } from "@sumi/ui/components/button";
import { LoaderCircle } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import {
  createUsageAPI,
  type UsageAPI,
  type UsageOverview,
  type UsageSource,
} from "../lib/usage";

const defaultClient = createUsageAPI();
const inputClass =
  "mt-1 w-full rounded-lg border border-border bg-background px-3 py-2 text-sm";

const CURRENCIES = ["USD", "JPY"] as const;
type Currency = (typeof CURRENCIES)[number];
const MINOR_DIGITS: Record<Currency, number> = { USD: 2, JPY: 0 };

function minorOf(amount: string, currency: Currency): number | null {
  const parsed = Number(amount);
  if (!Number.isFinite(parsed) || parsed < 0) return null;
  const minor = Math.round(parsed * 10 ** MINOR_DIGITS[currency]);
  return Number.isSafeInteger(minor) ? minor : null;
}

function majorOf(minor: number, currency: string): string {
  const digits = MINOR_DIGITS[currency as Currency] ?? 2;
  return (minor / 10 ** digits).toLocaleString("ja-JP", {
    maximumFractionDigits: digits,
  });
}

export function UsageSettings({
  client = defaultClient,
}: {
  client?: UsageAPI;
}) {
  const [overview, setOverview] = useState<UsageOverview | null>(null);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  const [busy, setBusy] = useState(false);
  const [editing, setEditing] = useState<string | null>(null);
  const lifetime = useRef<AbortController | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    lifetime.current = controller;
    void client
      .overview(controller.signal)
      .then((value) => {
        if (!controller.signal.aborted) setOverview(value);
      })
      .catch(() => {
        if (!controller.signal.aborted)
          setError("利用状況を読み込めませんでした。");
      });
    return () => controller.abort();
  }, [client]);

  async function run(action: (signal: AbortSignal) => Promise<void>) {
    const controller = lifetime.current;
    if (!controller || controller.signal.aborted || busy) return;
    setBusy(true);
    setError("");
    setNotice("");
    try {
      await action(controller.signal);
      const next = await client.overview(controller.signal);
      if (!controller.signal.aborted) setOverview(next);
    } catch (failure) {
      if (!controller.signal.aborted)
        setError(
          failure instanceof Error
            ? failure.message
            : "上限を更新できませんでした。もう一度お試しください。",
        );
    } finally {
      if (!controller.signal.aborted) setBusy(false);
    }
  }

  const waits = overview?.waits ?? [];

  return (
    <section
      className="mt-8 border-t border-border pt-6"
      aria-label="利用量と上限"
    >
      <h2 className="font-medium text-lg">利用量と上限</h2>
      <p className="mt-1 text-muted-foreground text-sm">
        どの接続にどれだけ使ったかと、使いすぎを止める上限を確認できます。金額は設定した単価による概算です。
      </p>
      {waits.length > 0 && (
        <p role="status" className="mt-3 text-sm">
          {waits.length}
          件のリクエストが上限のために待機中です。上限を上げるか外すと自動で再開します。
        </p>
      )}
      {overview && overview.sources.length === 0 && (
        <p className="mt-4 text-sm text-muted-foreground">
          まだ記録された利用はありません。
        </p>
      )}
      {overview?.sources.map((source) => (
        <SourceCard
          key={`${source.kind}:${source.id}`}
          source={source}
          busy={busy}
          editing={editing === `${source.kind}:${source.id}`}
          onEdit={() =>
            setEditing(
              editing === `${source.kind}:${source.id}`
                ? null
                : `${source.kind}:${source.id}`,
            )
          }
          onSave={(input) =>
            void run(async (signal) => {
              await client.setBudget(source.kind, source.id, input, signal);
              if (!signal.aborted) {
                setEditing(null);
                setNotice(
                  "上限を保存しました。待機中のリクエストは収まれば自動で再開します。",
                );
              }
            })
          }
          onClear={() =>
            void run(async (signal) => {
              await client.clearBudget(source.kind, source.id, signal);
              if (!signal.aborted)
                setNotice("上限を外しました。待機中のリクエストは再開します。");
            })
          }
        />
      ))}
      {error && (
        <div className="mt-3 text-sm">
          <p role="alert">{error}</p>
        </div>
      )}
      {notice && (
        <p role="status" className="mt-3 text-sm">
          {notice}
        </p>
      )}
    </section>
  );
}

function SourceCard({
  source,
  busy,
  editing,
  onEdit,
  onSave,
  onClear,
}: {
  source: UsageSource;
  busy: boolean;
  editing: boolean;
  onEdit(): void;
  onSave(input: {
    limitMinor: number;
    currency: string;
    rateInputPerMTok: number;
    rateOutputPerMTok: number;
    rateCachedPerMTok?: number;
    pricingRevision: string;
  }): void;
  onClear(): void;
}) {
  const t = source.totals;
  const b = source.budget;
  const title = source.grant ? "Sumiからの利用枠" : source.name || "API接続";
  return (
    <div className="mt-4 rounded-xl border border-border p-4">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="text-sm font-medium">{title}</p>
          <p className="mt-1 break-all text-xs text-muted-foreground">
            {source.model || "モデル未設定"}
            {source.selected ? " · 現在の接続" : ""}
          </p>
        </div>
        {!source.grant && (
          <Button
            size="sm"
            variant="outline"
            disabled={busy}
            onClick={onEdit}
            aria-label={`${title}の上限を設定`}
          >
            {b ? "上限を変更" : "上限を設定"}
          </Button>
        )}
      </div>
      <dl className="mt-3 grid grid-cols-2 gap-x-4 gap-y-2 text-sm">
        <div>
          <dt className="text-muted-foreground text-xs">呼び出し</dt>
          <dd>{t.calls.toLocaleString("ja-JP")}回</dd>
        </div>
        <div>
          <dt className="text-muted-foreground text-xs">概算コスト</dt>
          <dd>
            {Object.keys(t.costs).length > 0
              ? Object.entries(t.costs)
                  .map(
                    ([currency, minor]) =>
                      `${majorOf(minor, currency)} ${currency}`,
                  )
                  .join(" / ")
              : "—"}
          </dd>
        </div>
        <div>
          <dt className="text-muted-foreground text-xs">トークン</dt>
          <dd>
            入力{t.inputTokens.toLocaleString("ja-JP")} · 出力
            {t.outputTokens.toLocaleString("ja-JP")}
            {t.cachedTokens > 0
              ? ` · キャッシュ${t.cachedTokens.toLocaleString("ja-JP")}`
              : ""}
          </dd>
        </div>
        <div>
          <dt className="text-muted-foreground text-xs">上限</dt>
          <dd>
            {b
              ? `${majorOf(b.limitMinor, b.currency)} ${b.currency}（残り${majorOf(
                  b.remainingMinor,
                  b.currency,
                )}）`
              : "なし"}
          </dd>
        </div>
      </dl>
      {t.unknownCalls > 0 && (
        <p className="mt-2 text-muted-foreground text-xs">
          {t.unknownCalls}
          回の呼び出しは利用量を確認できなかった（または一部しか報告されなかった）ため、事前の見積もりで計上しています。
        </p>
      )}
      {source.grant && (
        <p className="mt-2 text-muted-foreground text-xs">
          この利用枠の上限はSumi側が管理します。
        </p>
      )}
      {editing && (
        <BudgetForm
          source={source}
          busy={busy}
          onSave={onSave}
          onClear={b ? onClear : undefined}
          onCancel={onEdit}
        />
      )}
      {source.recent.length > 0 && (
        <details className="mt-3 text-sm">
          <summary className="cursor-pointer text-muted-foreground text-xs">
            最近の呼び出し（{source.recent.length}件）
          </summary>
          <ul className="mt-2 space-y-1 text-xs">
            {source.recent.map((f) => (
              <li key={f.factId} className="flex justify-between gap-2">
                <span className="text-muted-foreground">
                  {new Date(f.recordedAt).toLocaleString("ja-JP")} ·{" "}
                  {f.phase === "memory" ? "記憶の整理" : "会話"}
                </span>
                <span>
                  {f.status === "unknown"
                    ? f.inputTokens !== undefined ||
                      f.outputTokens !== undefined
                      ? "利用量の一部のみ報告（概算で計上）"
                      : "利用量未確認（概算で計上）"
                    : f.status === "unrecorded"
                      ? "結果の記録なし（概算で計上）"
                      : f.status === "not_sent"
                        ? "未送信"
                        : f.costMinor !== undefined && f.currency
                          ? `${majorOf(f.costMinor, f.currency)} ${f.currency}`
                          : `${(f.inputTokens ?? 0).toLocaleString("ja-JP")}+${(f.outputTokens ?? 0).toLocaleString("ja-JP")}トークン`}
                </span>
              </li>
            ))}
          </ul>
        </details>
      )}
    </div>
  );
}

function BudgetForm({
  source,
  busy,
  onSave,
  onClear,
  onCancel,
}: {
  source: UsageSource;
  busy: boolean;
  onSave(input: {
    limitMinor: number;
    currency: string;
    rateInputPerMTok: number;
    rateOutputPerMTok: number;
    rateCachedPerMTok?: number;
    pricingRevision: string;
  }): void;
  onClear?(): void;
  onCancel(): void;
}) {
  const b = source.budget;
  const [currency, setCurrency] = useState<Currency>(
    (b?.currency as Currency) || "USD",
  );
  const [limit, setLimit] = useState(
    b ? majorOf(b.limitMinor, b.currency) : "",
  );
  const [rateIn, setRateIn] = useState(
    b ? majorOf(b.rateInputPerMTok, b.currency) : "",
  );
  const [rateOut, setRateOut] = useState(
    b ? majorOf(b.rateOutputPerMTok, b.currency) : "",
  );
  const [rateCached, setRateCached] = useState(
    b?.rateCachedPerMTok !== undefined
      ? majorOf(b.rateCachedPerMTok, b.currency)
      : "",
  );
  const [localError, setLocalError] = useState("");
  return (
    <form
      className="mt-4 space-y-3 border-t border-border pt-4"
      onSubmit={(e) => {
        e.preventDefault();
        const limitMinor = minorOf(limit, currency);
        const rateInput = minorOf(rateIn, currency);
        const rateOutput = minorOf(rateOut, currency);
        const rateCachedMinor = rateCached
          ? minorOf(rateCached, currency)
          : undefined;
        if (
          limitMinor === null ||
          rateInput === null ||
          rateOutput === null ||
          rateCachedMinor === null
        ) {
          setLocalError("0以上の数値で入力してください。");
          return;
        }
        setLocalError("");
        onSave({
          limitMinor,
          currency,
          rateInputPerMTok: rateInput,
          rateOutputPerMTok: rateOutput,
          ...(rateCachedMinor === undefined
            ? {}
            : { rateCachedPerMTok: rateCachedMinor }),
          pricingRevision: `settings-${new Date().toISOString().slice(0, 10)}`,
        });
      }}
    >
      <label className="block text-sm">
        上限額
        <span className="mt-1 flex gap-2">
          <input
            className={inputClass}
            inputMode="decimal"
            value={limit}
            required
            disabled={busy}
            onChange={(e) => setLimit(e.target.value)}
            placeholder="10.00"
          />
          <select
            className="rounded-lg border border-border bg-background px-2 text-sm"
            value={currency}
            disabled={busy}
            onChange={(e) => setCurrency(e.target.value as Currency)}
            aria-label="通貨"
          >
            {CURRENCIES.map((c) => (
              <option key={c} value={c}>
                {c}
              </option>
            ))}
          </select>
        </span>
      </label>
      <label className="block text-sm">
        入力の単価（100万トークンあたり）
        <input
          className={inputClass}
          inputMode="decimal"
          value={rateIn}
          required
          disabled={busy}
          onChange={(e) => setRateIn(e.target.value)}
          placeholder="2.50"
        />
      </label>
      <label className="block text-sm">
        出力の単価（100万トークンあたり）
        <input
          className={inputClass}
          inputMode="decimal"
          value={rateOut}
          required
          disabled={busy}
          onChange={(e) => setRateOut(e.target.value)}
          placeholder="10.00"
        />
      </label>
      <label className="block text-sm">
        キャッシュ入力の単価（任意・100万トークンあたり）
        <input
          className={inputClass}
          inputMode="decimal"
          value={rateCached}
          disabled={busy}
          onChange={(e) => setRateCached(e.target.value)}
          placeholder="空欄なら入力と同じ単価"
        />
      </label>
      <p className="text-muted-foreground text-xs leading-relaxed">
        単価はプロバイダーの料金表から自分で転記する概算です。上限はこの概算と事前の見積もりで判定され、達すると新しい呼び出しは止まります。実際の請求額とは異なる場合があります。使用額は通貨ごとに数えるため、通貨を変えると、それまで別の通貨で計上した分は新しい上限に含まれません（履歴には元の通貨のまま残ります）。
      </p>
      {localError && (
        <p role="alert" className="text-sm">
          {localError}
        </p>
      )}
      <div className="flex flex-wrap gap-3">
        <Button type="submit" disabled={busy}>
          {busy ? <LoaderCircle className="size-4 animate-spin" /> : null}
          上限を保存
        </Button>
        <Button
          type="button"
          variant="ghost"
          disabled={busy}
          onClick={onCancel}
        >
          戻る
        </Button>
        {onClear && (
          <Button
            type="button"
            variant="ghost"
            disabled={busy}
            onClick={onClear}
          >
            上限を外す
          </Button>
        )}
      </div>
    </form>
  );
}
