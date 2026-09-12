import { Button } from "@sumi/ui/components/button";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@sumi/ui/components/sheet";
import {
  Check,
  Copy,
  ExternalLink,
  LoaderCircle,
  RefreshCw,
} from "lucide-react";
import { useEffect, useRef, useState } from "react";
import type { APIConnectionsClient } from "../lib/api-connections";
import {
  CHATGPT_EFFORTS,
  CHATGPT_MODEL,
  type ChatGPTConnection,
  type ChatGPTEffort,
  type ChatGPTLogin,
  createModelConnectionsAPI,
  type ModelConnectionsAPI,
} from "../lib/model-connections";
import { APIConnectionSettings } from "./api-connection-settings";

const defaultAPI = createModelConnectionsAPI();
const EFFORT_LABELS: Record<ChatGPTEffort, string> = {
  low: "Low · 軽め",
  medium: "Medium · 標準",
  high: "High · 深く",
  xhigh: "XHigh · より深く",
  max: "Max · 最大",
};

export function ModelProviderSettings({
  open,
  onOpenChange,
  api = defaultAPI,
  connectionsAPI,
}: {
  open: boolean;
  onOpenChange(open: boolean): void;
  api?: ModelConnectionsAPI;
  connectionsAPI?: APIConnectionsClient;
}) {
  const [connection, setConnection] = useState<ChatGPTConnection | null>(null);
  const [login, setLogin] = useState<ChatGPTLogin | null>(null);
  const [effort, setEffort] = useState<ChatGPTEffort>("medium");
  const [loading, setLoading] = useState(false);
  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [revision, setRevision] = useState(0);
  const [confirmDisconnect, setConfirmDisconnect] = useState(false);
  const [copied, setCopied] = useState(false);
  const lifetime = useRef<AbortController | null>(null);
  const connectionReadVersion = useRef(0);

  useEffect(() => {
    const controller = new AbortController();
    lifetime.current = controller;
    return () => controller.abort();
  }, []);

  useEffect(() => {
    if (!open) setNotice(null);
  }, [open]);

  // biome-ignore lint/correctness/useExhaustiveDependencies: revision triggers the user's status retry.
  useEffect(() => {
    if (!open || busy !== null) return;
    const controller = new AbortController();
    const readVersion = ++connectionReadVersion.current;
    setLoading(true);
    void api
      .status(controller.signal)
      .then((next) => {
        if (
          controller.signal.aborted ||
          readVersion !== connectionReadVersion.current
        )
          return;
        setConnection(next);
        setEffort(next.effort || "medium");
      })
      .catch((failure: unknown) => {
        if (!controller.signal.aborted) setError(errorMessage(failure));
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });
    return () => controller.abort();
  }, [open, api, revision, busy]);

  const pendingLogin = login?.status === "pending" ? login : null;
  const loginID = pendingLogin?.loginId;
  const loginDeadline = pendingLogin?.expiresAt;
  const loginInterval = pendingLogin?.intervalMs;
  // biome-ignore lint/correctness/useExhaustiveDependencies: revision restarts a paused status poll after an explicit retry.
  useEffect(() => {
    if (!open || busy !== null || !loginID || !loginDeadline || !loginInterval)
      return;
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    let failures = 0;
    const poll = async () => {
      try {
        const next = await api.loginStatus(loginID, controller.signal);
        if (controller.signal.aborted) return;
        if (next.loginId !== loginID)
          throw new Error("ログイン状態を確認できませんでした。");
        failures = 0;
        if (next.status === "completed") {
          // The flow receipt describes its completion, not today's connection:
          // another browser may already have disconnected or reconnected it.
          const connected = await api.status(controller.signal);
          if (controller.signal.aborted) return;
          connectionReadVersion.current += 1;
          setConnection(connected);
          setEffort(connected.effort || "medium");
          setLogin(next);
          setError(null);
          setNotice(
            !connected.connected
              ? "現在、ChatGPTは接続されていません。"
              : next.connection?.connectionId &&
                  next.connection.connectionId !== connected.connectionId
                ? "最新の接続状態を表示しています。"
                : "ChatGPTに接続しました。下の「使う接続」でChatGPTを選べます。",
          );
          return;
        }
        if (next.status !== "pending") {
          setLogin(next);
          if (next.status === "failed")
            setError(
              next.error ||
                "ログインを完了できませんでした。もう一度お試しください。",
            );
          return;
        }
        if (Date.now() >= Date.parse(loginDeadline)) {
          setLogin({ ...next, status: "expired" });
          return;
        }
      } catch {
        if (controller.signal.aborted) return;
        failures += 1;
        if (failures >= 3 || Date.now() >= Date.parse(loginDeadline)) {
          setError(
            "ログイン結果を確認できませんでした。「状態を確認」で再確認できます。",
          );
          return;
        }
      }
      timer = setTimeout(() => void poll(), Math.max(1_000, loginInterval));
    };
    void poll();
    return () => {
      controller.abort();
      if (timer) clearTimeout(timer);
    };
  }, [open, loginID, loginDeadline, loginInterval, api, busy, revision]);

  async function run(
    kind: string,
    action: (signal: AbortSignal) => Promise<void>,
  ) {
    const controller = lifetime.current;
    if (!controller || controller.signal.aborted || busy) return;
    setBusy(kind);
    setError(null);
    setNotice(null);
    try {
      await action(controller.signal);
    } catch (failure) {
      if (!controller.signal.aborted) setError(errorMessage(failure));
    } finally {
      if (!controller.signal.aborted) setBusy(null);
    }
  }

  function startLogin() {
    void run("login", async (signal) => {
      const next = await api.startLogin(signal);
      if (signal.aborted) return;
      setLogin(next);
      setCopied(false);
      setConfirmDisconnect(false);
    });
  }

  const connected = connection?.connected === true;
  const dirty =
    connected &&
    (connection.model !== CHATGPT_MODEL || effort !== connection.effort);
  const pending = login?.status === "pending";

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="data-[side=right]:w-full sm:data-[side=right]:w-[28rem]">
        <SheetHeader className="border-border border-b px-6 py-5 pr-12">
          <SheetTitle>AIの接続</SheetTitle>
          <SheetDescription>
            あなたのアカウントやAPIをSumiに接続します。
          </SheetDescription>
        </SheetHeader>
        <div className="min-h-0 flex-1 overflow-y-auto px-6 py-6">
          <div className="flex items-start justify-between gap-4">
            <div>
              <h2 className="font-medium text-lg">ChatGPT</h2>
              <p className="mt-1 text-muted-foreground text-sm">
                接続して、Astraを使う
              </p>
            </div>
            {connected && !connection.reconnectRequired ? (
              <span className="flex items-center gap-1 text-sm">
                <Check className="size-4" />
                接続済み
              </span>
            ) : null}
          </div>
          {loading && !connection ? (
            <p
              role="status"
              className="mt-6 flex items-center gap-2 text-muted-foreground text-sm"
            >
              <LoaderCircle className="size-4 animate-spin" />
              接続を確認しています…
            </p>
          ) : null}
          {connection ? (
            <>
              {connected ? (
                <div className="mt-5 rounded-xl border border-border p-4">
                  <p className="text-muted-foreground text-xs">
                    接続中のアカウント
                  </p>
                  <p className="mt-1 break-all text-sm">
                    {connection.accountId || "ChatGPTアカウント"}
                  </p>
                  {connection.reconnectRequired ? (
                    <p className="mt-3 text-sm">
                      もう一度ログインして接続を更新してください。
                    </p>
                  ) : null}
                </div>
              ) : (
                <p className="mt-5 text-muted-foreground text-sm leading-relaxed">
                  ChatGPTでログインすると、Astra（Medium）の接続を追加できます。使う接続は下で選べます。
                </p>
              )}
              {!pending ? (
                <Button
                  className="mt-4 min-h-10 w-full"
                  disabled={busy !== null}
                  onClick={startLogin}
                >
                  {busy === "login" ? (
                    <LoaderCircle className="size-4 animate-spin" />
                  ) : null}
                  {connected
                    ? "ChatGPTに再接続"
                    : "ChatGPTを接続してAstraを使う"}
                </Button>
              ) : null}
            </>
          ) : null}

          {pending && login ? (
            <section
              className="mt-6 space-y-4 rounded-xl border border-border p-4"
              aria-label="ChatGPTへのログイン"
            >
              <p className="text-sm leading-relaxed">
                ChatGPTのログイン画面で、このコードを入力してください。
              </p>
              <div className="flex items-center justify-between gap-2 rounded-lg bg-muted px-3 py-3">
                <code className="select-all font-mono text-xl tracking-widest">
                  {login.userCode}
                </code>
                <Button
                  variant="ghost"
                  size="icon"
                  aria-label="ログインコードをコピー"
                  onClick={() => {
                    void (async () => {
                      try {
                        await navigator.clipboard.writeText(login.userCode);
                        setCopied(true);
                      } catch {
                        setError("コードを長押ししてコピーしてください。");
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
              <a
                href={login.verificationUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="flex min-h-10 items-center justify-center gap-2 rounded-lg bg-primary px-3 py-2 font-medium text-primary-foreground text-sm"
              >
                ChatGPTでログイン
                <ExternalLink className="size-4" />
              </a>
              <p className="text-muted-foreground text-xs">
                コードは
                {new Date(login.expiresAt).toLocaleTimeString("ja-JP", {
                  hour: "2-digit",
                  minute: "2-digit",
                })}
                まで有効です。
              </p>
              <p className="text-muted-foreground text-xs">
                この画面を閉じても、ログインはコードの有効期限まで続けられます。
              </p>
              <Button
                variant="ghost"
                disabled={busy !== null}
                onClick={() =>
                  void run("cancel", async (signal) => {
                    await api.cancelLogin(login.loginId, signal);
                    if (!signal.aborted)
                      setLogin({ ...login, status: "cancelled" });
                  })
                }
              >
                ログインをキャンセル
              </Button>
            </section>
          ) : null}
          {login?.status === "expired" ? (
            <p role="status" className="mt-4 text-sm">
              コードの有効期限が切れました。接続ボタンから新しいコードを発行できます。
            </p>
          ) : null}

          {connected ? (
            <section
              className="mt-7 border-border border-t pt-6"
              aria-label="モデル設定"
            >
              <h3 className="font-medium text-sm">モデル</h3>
              <p className="mt-2 text-sm">
                Astra <span className="text-muted-foreground">· GPT-6</span>
              </p>
              <label
                htmlFor="chatgpt-effort"
                className="mt-5 mb-2 block text-sm"
              >
                推論の深さ
              </label>
              <select
                id="chatgpt-effort"
                value={effort}
                disabled={busy !== null || pending}
                onChange={(event) =>
                  setEffort(event.target.value as ChatGPTEffort)
                }
                className="min-h-10 w-full rounded-lg border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
              >
                {CHATGPT_EFFORTS.map((value) => (
                  <option key={value} value={value}>
                    {EFFORT_LABELS[value]}
                  </option>
                ))}
              </select>
              <p className="mt-2 text-muted-foreground text-xs leading-relaxed">
                作業中の場合は、一区切りついてから切り替わります。
              </p>
              <Button
                variant="outline"
                className="mt-4 min-h-10"
                disabled={!dirty || busy !== null || pending}
                onClick={() =>
                  void run("save", async (signal) => {
                    const next = await api.selectModel(
                      connection.connectionId ?? "",
                      effort,
                      signal,
                    );
                    if (signal.aborted) return;
                    setConnection(next);
                    setEffort(next.effort || "medium");
                    setNotice(
                      "ChatGPTの設定を保存しました。ChatGPTを使用中なら、作業が一区切りついてから反映されます。",
                    );
                  })
                }
              >
                {busy === "save" ? "保存しています…" : "モデル設定を保存"}
              </Button>
              <div className="mt-6 border-border border-t pt-4">
                {confirmDisconnect ? (
                  <div className="space-y-3">
                    <p className="text-sm">
                      ChatGPTの接続を解除します。ChatGPTを選択中の場合は、別の接続を選んでください。作業中なら一区切りついてから反映されます。
                    </p>
                    <div className="flex gap-2">
                      <Button
                        variant="destructive"
                        disabled={busy !== null}
                        onClick={() =>
                          void run("disconnect", async (signal) => {
                            await api.disconnect(signal);
                            if (signal.aborted) return;
                            setConnection({
                              connected: false,
                              model: "",
                              effort: "",
                              reconnectRequired: false,
                              activation: "next_start",
                            });
                            setLogin(null);
                            setConfirmDisconnect(false);
                            setNotice(
                              "ChatGPTの接続を解除しました。使う接続は下で選べます。",
                            );
                          })
                        }
                      >
                        接続を解除する
                      </Button>
                      <Button
                        variant="ghost"
                        disabled={busy !== null}
                        onClick={() => setConfirmDisconnect(false)}
                      >
                        戻る
                      </Button>
                    </div>
                  </div>
                ) : (
                  <Button
                    variant="ghost"
                    disabled={busy !== null || pending}
                    onClick={() => setConfirmDisconnect(true)}
                  >
                    接続を解除
                  </Button>
                )}
              </div>
            </section>
          ) : null}
          {notice ? (
            <p role="status" className="mt-5 text-sm">
              {notice}
            </p>
          ) : null}
          {error ? (
            <div className="mt-5 space-y-2">
              <p role="alert" className="text-sm">
                {error}
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
          {open && (
            <APIConnectionSettings
              chatgptConnected={connected}
              client={connectionsAPI}
            />
          )}
        </div>
      </SheetContent>
    </Sheet>
  );
}

function errorMessage(error: unknown): string {
  return error instanceof Error
    ? error.message
    : "接続状態を確認できませんでした。もう一度お試しください。";
}
