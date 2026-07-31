import { Button } from "@sumi/ui/components/button";
import { LoaderCircle } from "lucide-react";
import { useState } from "react";
import { useResearchConsent } from "./use-research-consent";

/**
 * 研究協力のお願い (ADR 0009 §6). Sumi's default life-log is private — even
 * administrators cannot read it. Research cooperation unseals content logs for
 * research and improvement only, and must be asked honestly rather than buried
 * in settings. The choice is saved to the 戸籍 and can be changed here at any
 * time.
 */
export function ConsentScreen({
  onChangeComplete,
}: {
  onChangeComplete?: () => void;
}) {
  const consent = useResearchConsent(true);
  const [busy, setBusy] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  const choose = async (grant: boolean) => {
    if (busy) return;
    setBusy(true);
    setError(null);
    try {
      await consent.setConsent(grant);
      onChangeComplete?.();
    } catch {
      setError("保存できませんでした。もう一度お試しください。");
    } finally {
      setBusy(false);
    }
  };

  return (
    <main className="grid min-h-dvh place-items-center bg-background px-5 text-foreground">
      <section
        aria-labelledby="consent-title"
        className="w-full max-w-[25rem] rounded-2xl border bg-background px-6 py-8 shadow-sm sm:px-8"
      >
        <div className="mb-6 text-center">
          <div className="mx-auto mb-4 grid size-11 place-items-center rounded-xl bg-foreground font-semibold text-background">
            S
          </div>
          <h1
            id="consent-title"
            className="mb-2 font-semibold text-xl tracking-[-0.025em]"
          >
            研究協力のお願い
          </h1>
        </div>

        <div className="space-y-3 text-foreground text-sm leading-6">
          <p>
            Sumiは、あなたのSecretaryとのやりとりを既定で
            <span className="font-medium">誰も覗けない私人のもの</span>
            として扱います。管理者も例外ではありません。
          </p>
          <p>
            この既定のまま使い続けることもできます。もし、Sumiの改善や研究のために、やりとりの内容を開発チームに見てもよいと思えるなら、研究協力にご協力ください。内容は研究・改善目的に限定して扱います。
          </p>
          <p className="text-muted-foreground">
            どちらを選んでもSumiの利用に違いはありません。あとでいつでも変更できます。
          </p>
        </div>

        {consent.decided && (
          <p
            role="status"
            className="mt-5 rounded-lg bg-neutral-50 px-3 py-2.5 text-muted-foreground text-sm dark:bg-neutral-900/40"
          >
            現在の設定:{" "}
            {consent.granted ? "研究協力に同意済み" : "研究協力を辞退"}
          </p>
        )}

        <div className="mt-6 space-y-3">
          <Button
            type="button"
            onClick={() => void choose(true)}
            disabled={busy || consent.loading}
            className="h-11 w-full justify-center rounded-lg text-sm"
          >
            {busy ? (
              <LoaderCircle className="size-5 animate-spin" />
            ) : (
              "同意する"
            )}
          </Button>
          <Button
            type="button"
            variant="outline"
            onClick={() => void choose(false)}
            disabled={busy || consent.loading}
            className="h-11 w-full justify-center rounded-lg text-sm"
          >
            辞退する
          </Button>
        </div>

        {error && (
          <p
            role="alert"
            className="mt-4 rounded-lg bg-red-50 px-3 py-2.5 text-red-700 text-sm dark:bg-red-950/30 dark:text-red-300"
          >
            {error}
          </p>
        )}
        {consent.error && !error && (
          <p
            role="alert"
            className="mt-4 rounded-lg bg-red-50 px-3 py-2.5 text-red-700 text-sm dark:bg-red-950/30 dark:text-red-300"
          >
            {consent.error}
          </p>
        )}
      </section>
    </main>
  );
}
