import { Button } from "@sumi/ui/components/button";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@sumi/ui/components/sheet";
import { Copy, LoaderCircle, RefreshCw } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import {
  cancelSecretaryReturn,
  clearReturnURL,
  createSecretaryReturn,
  isReturnFeatureDisabled,
  isReturnPolicyUndecided,
  loadReturnURL,
  readSecretaryReturn,
  type SecretaryReturnSession,
  saveReturnURL,
} from "../auth/secretary-return";

const OPEN_STATUSES = new Set(["awaiting_destination", "sealed", "cancelling"]);

// A terminal session the source can accept another return after: nothing
// moved or the move was undone, so the secretary is still active on Cloud.
// `completed` is excluded on purpose — authority already transferred, and
// offering a normal restart would promise a move the source cannot make.
const RETRYABLE_STATUSES = new Set(["aborted", "cancelled", "expired"]);

function statusText(session: SecretaryReturnSession): string {
  switch (session.status) {
    case "awaiting_destination":
      return "ローカルの受け取りを待っています。下のURLをローカルのコマンドに渡してください。";
    case "sealed":
      return "秘書はSumi Cloudで停止し、ローカルへ移行中です。ローカルのコマンドが受け取りを進めています。";
    case "cancelling":
      return "キャンセルを受け付けました。ローカル側の確認が届き次第、秘書はSumi Cloudで動きを再開します。";
    case "completed":
      // The transfer committed — state and answering authority live on the
      // Local side now. It does not claim the secretary already answered
      // there: model setup may still be pending (needs_rebinding below).
      return "秘書の状態と応答権はローカルに移りました。Sumi Cloudでは応答しません。";
    case "aborted":
      return "移行はキャンセルされ、秘書はSumi Cloudで動いています。";
    case "cancelled":
      return "移行は開始前にキャンセルされました。何も移動していません。";
    case "expired":
      return "移行の受付期限が切れました。何も移動していません。";
    default:
      return session.status;
  }
}

export function SecretaryReturn({
  open,
  onOpenChange,
}: {
  open: boolean;
  onOpenChange(open: boolean): void;
}) {
  const [session, setSession] = useState<SecretaryReturnSession | null>(null);
  const [returnURL, setReturnURL] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [busy, setBusy] = useState<string | null>(null);
  // Where the shown error came from decides when it may clear: a read
  // error is stale the moment a read succeeds, but a mutation refusal is
  // the answer to something the user asked for — it stays visible through
  // automatic refreshes until the next meaningful action replaces it.
  const [error, setError] = useState<{
    source: "read" | "mutation";
    message: string;
  } | null>(null);
  const [copied, setCopied] = useState(false);
  const [confirmCancel, setConfirmCancel] = useState(false);
  // The file-handling choice is explicit: nothing is preselected, and the
  // create action refuses to run until the owner picks where the
  // secretary's working files live after the move.
  const [fileMode, setFileMode] = useState<"local" | "cloud" | null>(null);
  const [revision, setRevision] = useState(0);
  const lifetime = useRef<AbortController | null>(null);
  const readVersion = useRef(0);

  useEffect(() => {
    const controller = new AbortController();
    lifetime.current = controller;
    return () => controller.abort();
  }, []);

  // biome-ignore lint/correctness/useExhaustiveDependencies: revision retries a failed read.
  useEffect(() => {
    if (!open || busy !== null) return;
    const controller = new AbortController();
    const version = ++readVersion.current;
    setLoading(true);
    void readSecretaryReturn()
      .then((next) => {
        if (controller.signal.aborted || version !== readVersion.current)
          return;
        setSession(next);
        const saved = next ? loadReturnURL(next.session_id) : null;
        setReturnURL(saved?.returnURL ?? null);
        // Fresh state replaces a read failure; it never erases a mutation
        // refusal — that is the explanation for what the user just tried.
        setError((current) => (current?.source === "read" ? null : current));
      })
      .catch((failure: unknown) => {
        if (controller.signal.aborted) return;
        if (isReturnFeatureDisabled(failure)) {
          setError({
            source: "read",
            message: "この環境では秘書のローカルへの移行は利用できません。",
          });
        } else {
          setError({ source: "read", message: errorMessage(failure) });
        }
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });
    return () => controller.abort();
  }, [open, revision, busy]);

  // Poll while the return is open: the Local command drives it, and this
  // panel only ever reads.
  const openSession = session !== null && OPEN_STATUSES.has(session.status);
  // biome-ignore lint/correctness/useExhaustiveDependencies: session status alone restarts the poll.
  useEffect(() => {
    if (!open || !openSession) return;
    const controller = new AbortController();
    const timer = setInterval(() => {
      void readSecretaryReturn()
        .then((next) => {
          if (controller.signal.aborted) return;
          setSession(next);
          setError((current) => (current?.source === "read" ? null : current));
        })
        .catch(() => {
          // A transient read failure just waits for the next tick.
        });
    }, 4_000);
    return () => {
      controller.abort();
      clearInterval(timer);
    };
  }, [open, openSession, session?.status]);

  async function run(kind: string, action: () => Promise<void>) {
    if (busy) return;
    setBusy(kind);
    setError(null);
    try {
      await action();
    } catch (failure) {
      setError({ source: "mutation", message: errorMessage(failure) });
    } finally {
      setBusy(null);
    }
  }

  function start() {
    if (fileMode === null) return;
    const mode = fileMode;
    void run("create", async () => {
      try {
        const created = await createSecretaryReturn(mode);
        setSession(created.session);
        setReturnURL(created.return_url);
        saveReturnURL(created.session.session_id, created.return_url);
        setCopied(false);
        // The choice is per session: it does not carry into the next
        // issue as a silent default.
        setFileMode(null);
      } catch (failure) {
        // A 409 carries the open session so a lost return-URL answer still
        // recovers its status; the grant itself is never repeated.
        const session = (failure as { session?: SecretaryReturnSession })
          ?.session;
        if (session) {
          setSession(session);
          return;
        }
        if (isReturnPolicyUndecided(failure)) {
          // The shared-file decision is still open: no session was made
          // and nothing moved — say so rather than presenting a URL that
          // would not work.
          setError({
            source: "mutation",
            message:
              "共有ファイルの扱いがまだ決まっていないため、新しい移行は今は開始できません。移行は開始されていません。",
          });
          return;
        }
        throw failure;
      }
    });
  }

  function retry() {
    // Retrying from a terminal session drops the old tab-local grant — it
    // names a session that can never receive a command again.
    if (session) clearReturnURL(session.session_id);
    start();
  }

  function cancel() {
    if (!session) return;
    const id = session.session_id;
    void run("cancel", async () => {
      const next = await cancelSecretaryReturn(id);
      setSession(next);
      setConfirmCancel(false);
      if (!OPEN_STATUSES.has(next.status)) clearReturnURL(id);
    });
  }

  // The file-handling picker — rendered wherever a new session can be
  // issued (first issue and retry from a terminal session alike): a new
  // session always wants a fresh explicit choice, never the old one
  // silently re-applied.
  const fileModePicker = (
    <fieldset className="space-y-2">
      <legend className="text-sm">ファイルの置き場所を選んでください</legend>
      <label className="flex cursor-pointer items-start gap-3 rounded-xl border border-border p-3 has-checked:border-foreground/40 has-checked:bg-muted/50">
        <input
          type="radio"
          name="return-file-mode"
          className="mt-1"
          checked={fileMode === "local"}
          onChange={() => setFileMode("local")}
        />
        <span className="text-sm leading-relaxed">
          <span className="font-medium">ファイルをローカルに持ってくる</span>
          <span className="block text-muted-foreground text-xs">
            Cloudのワークスペースをコピーしてローカルで使います。Cloud側のコピーは読み取り専用で残ります（同期やバックアップではありません）。
          </span>
        </span>
      </label>
      <label className="flex cursor-pointer items-start gap-3 rounded-xl border border-border p-3 has-checked:border-foreground/40 has-checked:bg-muted/50">
        <input
          type="radio"
          name="return-file-mode"
          className="mt-1"
          checked={fileMode === "cloud"}
          onChange={() => setFileMode("cloud")}
        />
        <span className="text-sm leading-relaxed">
          <span className="font-medium">ファイルはCloudに残す</span>
          <span className="block text-muted-foreground text-xs">
            ローカルの秘書とあなたは同じCloudのワークスペースを読み書きします。大きなファイルを移さずに済みますが、ファイル操作には接続が必要です。
          </span>
        </span>
      </label>
    </fieldset>
  );

  const waiting = session?.status === "awaiting_destination";
  const canCancel = session !== null && OPEN_STATUSES.has(session.status);
  const canRetry = session !== null && RETRYABLE_STATUSES.has(session.status);

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="data-[side=right]:w-full sm:data-[side=right]:w-[28rem]">
        <SheetHeader className="border-border border-b px-6 py-5 pr-12">
          <SheetTitle>秘書をローカルに戻す</SheetTitle>
          <SheetDescription>
            同じ秘書が、状態ごとあなたのSumi
            Localに移ります。会話や記憶は引き継がれ、移行中はどちらか片方だけが応答します。
          </SheetDescription>
        </SheetHeader>
        <div className="min-h-0 flex-1 overflow-y-auto px-6 py-6">
          {loading && !session ? (
            <p
              role="status"
              className="flex items-center gap-2 text-muted-foreground text-sm"
            >
              <LoaderCircle className="size-4 animate-spin" />
              状態を確認しています…
            </p>
          ) : null}

          {session ? (
            <>
              <p className="text-sm leading-relaxed">{statusText(session)}</p>
              {session.status === "completed" ? (
                <p className="mt-3 text-muted-foreground text-xs leading-relaxed">
                  ローカル側でAI接続の選択が必要な場合、コマンドが案内します（未選択の間は
                  needs_rebinding として応答を待ちます）。
                </p>
              ) : null}
              {session.status === "completed" && session.file_mode === "local" ? (
                <p className="mt-3 text-muted-foreground text-xs leading-relaxed">
                  ファイルはローカルのワークスペースに移りました。Cloudに残っているコピーは読み取り専用です。同期やバックアップではありません。
                </p>
              ) : null}
              {session.status === "completed" && session.file_mode === "cloud" ? (
                <p className="mt-3 text-muted-foreground text-xs leading-relaxed">
                  ファイルはCloudに残っています。ローカルの秘書とあなたは同じCloudのワークスペースを使います。
                </p>
              ) : null}
              {session.preflight && OPEN_STATUSES.has(session.status) ? (
                <div className="mt-4 rounded-xl border border-border p-4">
                  <p className="text-muted-foreground text-xs">移行前の状態</p>
                  <ul className="mt-2 space-y-1 text-xs leading-relaxed">
                    <li>実行中の作業: {session.preflight.active_jobs}件</li>
                    {session.preflight.pending_approvals > 0 ? (
                      <li>
                        承認待ちの操作が{session.preflight.pending_approvals}
                        件引き継がれます。新しいローカルではあなたへの結び付きがないため、ここで決めるまでローカル側は起動できません（移行を進めるか、決めてからやり直してください）。
                      </li>
                    ) : null}
                    {(session.preflight.pending_file_effects ?? 0) > 0 ? (
                      <li>
                        処理中のファイル操作が{session.preflight.pending_file_effects}
                        件あります。完了が確認できるまで移行は封印されません。
                      </li>
                    ) : null}
                    {session.preflight.model_intent_kind ? (
                      <li>
                        AI接続の指定が引き継がれます。ローカル側で接続を選ぶまで
                        needs_rebinding として待ちます。
                      </li>
                    ) : null}
                    <li>{session.preflight.files}</li>
                  </ul>
                </div>
              ) : null}
            </>
          ) : null}

          {waiting && returnURL ? (
            <section
              className="mt-5 space-y-3 rounded-xl border border-border p-4"
              aria-label="移行URL"
            >
              <p className="text-sm leading-relaxed">
                秘書を受け取るローカルで、次を実行してください:
              </p>
              <code className="block select-all rounded-lg bg-muted px-3 py-2 font-mono text-xs">
                sumi-local-move return
              </code>
              <p className="text-muted-foreground text-xs">
                表示されたら、このURLを貼り付けます。同じマシンなら、秘書を送り出した元のローカルでも新しいローカルでも受け取れます。
              </p>
              <div className="flex items-center justify-between gap-2 rounded-lg bg-muted px-3 py-3">
                <code className="select-all break-all font-mono text-xs">
                  {returnURL}
                </code>
                <Button
                  variant="ghost"
                  size="icon"
                  aria-label="移行URLをコピー"
                  onClick={() => {
                    void (async () => {
                      try {
                        await navigator.clipboard.writeText(returnURL);
                        setCopied(true);
                      } catch {
                        setError({
                          source: "mutation",
                          message: "URLを長押ししてコピーしてください。",
                        });
                      }
                    })();
                  }}
                >
                  <Copy className="size-4" />
                </Button>
              </div>
              {copied ? (
                <p role="status" className="text-muted-foreground text-xs">
                  コピーしました
                </p>
              ) : null}
              <p className="text-muted-foreground text-xs">
                このURLは合言葉です。他人に見せないでください。受付は
                {new Date(session?.admit_until ?? "").toLocaleString("ja-JP")}
                までです。
              </p>
            </section>
          ) : null}
          {waiting && !returnURL ? (
            <p className="mt-4 text-muted-foreground text-xs leading-relaxed">
              移行URLは発行したブラウザのタブにだけ残ります。見つからない場合は、この移行をキャンセルして新しく発行してください。
            </p>
          ) : null}

          {!session ? (
            <div className="mt-5 space-y-3">
              <p className="text-muted-foreground text-sm leading-relaxed">
                移行しても秘書は同じ個体のままです。送り出した元のローカルにも、新しいローカルにも戻せます。
              </p>
              {fileModePicker}
              <Button
                className="min-h-10 w-full"
                disabled={busy !== null || loading || fileMode === null}
                onClick={start}
              >
                {busy === "create" ? (
                  <LoaderCircle className="size-4 animate-spin" />
                ) : null}
                移行URLを発行する
              </Button>
            </div>
          ) : null}

          {canRetry ? (
            <div className="mt-6 border-border border-t pt-4">
              <p className="text-muted-foreground text-sm leading-relaxed">
                この移行は終了しています。もう一度移行する場合は、新しいURLを発行してください。
              </p>
              <div className="mt-3">{fileModePicker}</div>
              <Button
                className="mt-3 min-h-10 w-full"
                disabled={busy !== null || loading || fileMode === null}
                onClick={retry}
              >
                {busy === "create" ? (
                  <LoaderCircle className="size-4 animate-spin" />
                ) : null}
                新しい移行URLを発行する
              </Button>
            </div>
          ) : null}

          {canCancel ? (
            <div className="mt-6 border-border border-t pt-4">
              {confirmCancel ? (
                <div className="space-y-3">
                  <p className="text-sm">
                    {session.status === "awaiting_destination"
                      ? "この移行をキャンセルします。まだ何も移動していません。"
                      : "この移行の停止を求めます。秘書はローカル側の確認が届くまでSumi Cloudで停止したままです（ローカルで有効になっていれば、そちらが優先されます）。"}
                  </p>
                  <div className="flex gap-2">
                    <Button
                      variant="destructive"
                      disabled={busy !== null}
                      onClick={cancel}
                    >
                      移行をキャンセル
                    </Button>
                    <Button
                      variant="ghost"
                      disabled={busy !== null}
                      onClick={() => setConfirmCancel(false)}
                    >
                      戻る
                    </Button>
                  </div>
                </div>
              ) : (
                <Button
                  variant="ghost"
                  disabled={busy !== null}
                  onClick={() => setConfirmCancel(true)}
                >
                  移行をキャンセル
                </Button>
              )}
            </div>
          ) : null}

          {error ? (
            <div className="mt-5 space-y-2">
              <p role="alert" className="text-sm">
                {error.message}
              </p>
              <Button
                variant="outline"
                disabled={loading || busy !== null}
                onClick={() => {
                  setError(null);
                  setRevision((value) => value + 1);
                }}
              >
                <RefreshCw className="size-4" />
                状態を確認
              </Button>
            </div>
          ) : null}
        </div>
      </SheetContent>
    </Sheet>
  );
}

function errorMessage(error: unknown): string {
  return error instanceof Error
    ? error.message
    : "移行の状態を確認できませんでした。もう一度お試しください。";
}
