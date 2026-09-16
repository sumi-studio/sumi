import { Button } from "@sumi/ui/components/button";
import { Check, Copy, LoaderCircle } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import type { PendingAuthConfirmation } from "./auth-confirmation-state";
import { getAuthErrorMessage } from "./auth-errors";
import {
  cancelSecretaryTransfer,
  clearMoveURL,
  createSecretaryTransfer,
  isTransferFeatureDisabled,
  isTransferPendingError,
  isTransferUnavailableError,
  loadMoveURL,
  readSecretaryTransfer,
  type SecretaryTransferSession,
  saveMoveURL,
} from "./secretary-transfer";
import { AuthAPIError } from "./session-client";

const transferPollMs = 4_000;

type Phase = "choice" | "moving" | "arrived";

/** Japanese labels for the documented state-only boundary. Unknown names
 * pass through so a new exclusion still reads as excluded. */
const exclusionLabels: Record<string, string> = {
  files: "ファイル（内容・バージョン）",
  jobs: "バックグラウンドジョブ",
  connections: "モデル接続・認証情報",
  memory_projection: "検索インデックス",
  account_and_workspace: "アカウント・ワークスペース情報",
  usage: "利用量・課金情報",
};

function exclusionLabel(name: string): string {
  return exclusionLabels[name] ?? name;
}

function isOpenSession(
  session: SecretaryTransferSession | null,
): session is SecretaryTransferSession {
  return (
    session !== null &&
    (session.status === "awaiting_bundle" ||
      session.status === "staged" ||
      session.status === "provisioned")
  );
}

function phaseFor(session: SecretaryTransferSession | null): Phase {
  if (session === null) return "choice";
  switch (session.status) {
    case "awaiting_bundle":
      return "moving";
    case "staged":
    case "provisioned":
    case "activated":
      return "arrived";
    default:
      return "choice";
  }
}

/**
 * The secretary choice on a create-account confirmation: start with a new
 * Cloud secretary, or bring the one this credential's Local placement still
 * runs. The panel only ever acts through the live verified flow's authority
 * (flow id + nonce); the subject is never chosen client-side.
 */
export function RegistrationSecretaryPanel({
  confirmation,
  accountLabel,
  busy,
  onConfirm,
  onCancel,
  onError,
}: {
  confirmation: PendingAuthConfirmation;
  accountLabel: string;
  busy: boolean;
  onConfirm: () => Promise<void>;
  onCancel: () => Promise<void>;
  onError: (message: string | null) => void;
}) {
  const flowId = confirmation.flowId;
  const flowNonce = confirmation.nonce;
  const [phase, setPhase] = useState<Phase>("choice");
  const [session, setSession] = useState<SecretaryTransferSession | null>(null);
  const [moveURL, setMoveURL] = useState<string | null>(null);
  const [working, setWorking] = useState(false);
  const [copied, setCopied] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);
  const [authLost, setAuthLost] = useState(false);
  // Pessimistic until the mount read answers: the Local CTA renders only
  // after a mounted-route answer, so an API without transfer routes never
  // flashes a button that would 404.
  const [transferAvailable, setTransferAvailable] = useState(false);
  const mounted = useRef(false);
  useEffect(() => {
    // Setting it inside the effect, not at ref creation, keeps the guard
    // true after StrictMode's mount→cleanup→mount replay.
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);

  const applySession = (next: SecretaryTransferSession | null) => {
    setSession(next);
    setPhase(phaseFor(next));
    if (
      next !== null &&
      (next.status === "cancelled" || next.status === "expired")
    ) {
      setNotice(
        next.status === "expired"
          ? "移動の受付期限が切れました。必要であればもう一度始められます。"
          : "移動は取り消されました。",
      );
    } else {
      setNotice(null);
    }
  };

  const refresh = async (): Promise<SecretaryTransferSession | null> => {
    const next = await readSecretaryTransfer({ flowId, nonce: flowNonce });
    if (mounted.current) applySession(next);
    return next;
  };

  // Restore an in-flight move for this flow: a reload keeps the saved move
  // URL (the grant lives nowhere else) and the session's own status decides
  // where the panel resumes.
  // biome-ignore lint/correctness/useExhaustiveDependencies: this runs once per flow identity; applySession/onError are stable for the effect's lifetime.
  useEffect(() => {
    const saved = loadMoveURL(flowId);
    let cancelled = false;
    void readSecretaryTransfer({ flowId, nonce: flowNonce })
      .then((next) => {
        if (cancelled) return;
        // A real answer — even "no session" — proves the routes are mounted.
        setTransferAvailable(true);
        applySession(next);
        if (
          saved &&
          next !== null &&
          next.session_id === saved.sessionId &&
          isOpenSession(next)
        ) {
          setMoveURL(saved.moveURL);
        }
      })
      .catch((error: unknown) => {
        if (cancelled) return;
        if (isTransferFeatureDisabled(error)) {
          setTransferAvailable(false);
        } else if (error instanceof AuthAPIError && error.status === 401) {
          setAuthLost(true);
        } else {
          onError(getAuthErrorMessage(error));
        }
      });
    return () => {
      cancelled = true;
    };
  }, [flowId, flowNonce]);

  // While waiting for the Local command, watch the session. A proof that the
  // flow can no longer carry (expired, closed, consumed) stops the poll — the
  // move itself continues server-side and a fresh sign-in recovers it.
  // biome-ignore lint/correctness/useExhaustiveDependencies: the poll is scoped to the flow identity and phase; applySession/onError are stable for the effect's lifetime.
  useEffect(() => {
    if (phase !== "moving" || authLost) return;
    const timer = globalThis.setInterval(() => {
      void readSecretaryTransfer({ flowId, nonce: flowNonce })
        .then((next) => {
          if (mounted.current) applySession(next);
        })
        .catch((error: unknown) => {
          if (
            mounted.current &&
            error instanceof AuthAPIError &&
            error.status === 401
          ) {
            setAuthLost(true);
          }
        });
    }, transferPollMs);
    return () => globalThis.clearInterval(timer);
  }, [phase, authLost, flowId, flowNonce]);

  const startMove = async () => {
    setWorking(true);
    onError(null);
    try {
      const created = await createSecretaryTransfer({
        flowId,
        nonce: flowNonce,
      });
      saveMoveURL(
        confirmation.flowId,
        created.session.session_id,
        created.move_url,
      );
      if (!mounted.current) return;
      setMoveURL(created.move_url);
      applySession(created.session);
    } catch (error) {
      if (!mounted.current) return;
      // A 409 answers the credential's open session. The grant is only ever
      // in the first create response, so an awaiting session whose URL was
      // lost is replaced — cancelled with its still-open status pinned —
      // before a fresh one is created. An arrived session needs no URL.
      const open =
        error instanceof AuthAPIError &&
        error.status === 409 &&
        "session" in error &&
        typeof error.session === "object"
          ? (error.session as SecretaryTransferSession)
          : null;
      if (open !== null) {
        try {
          if (open.status === "awaiting_bundle" && !moveURL) {
            await cancelSecretaryTransfer(
              { flowId, nonce: flowNonce },
              open.session_id,
              "awaiting_bundle",
            );
            const created = await createSecretaryTransfer({
              flowId,
              nonce: flowNonce,
            });
            saveMoveURL(flowId, created.session.session_id, created.move_url);
            if (!mounted.current) return;
            setMoveURL(created.move_url);
            applySession(created.session);
            return;
          }
          applySession(open);
          return;
        } catch (recovery) {
          onError(getAuthErrorMessage(recovery));
          return;
        }
      }
      if (error instanceof AuthAPIError && error.status === 401) {
        setAuthLost(true);
        return;
      }
      if (isTransferFeatureDisabled(error)) {
        setTransferAvailable(false);
        return;
      }
      onError(getAuthErrorMessage(error));
    } finally {
      if (mounted.current) setWorking(false);
    }
  };

  const finish = async () => {
    onError(null);
    try {
      await onConfirm();
    } catch (error) {
      if (!mounted.current) return;
      if (isTransferPendingError(error)) {
        // The account transaction refused to mint a second secretary while
        // this one has not arrived. Re-read and wait — never bypass it.
        setNotice(
          "秘書の状態がまだ届いていません。Local側のコマンドを確認してください。",
        );
        await refresh().catch(() => undefined);
        return;
      }
      if (isTransferUnavailableError(error)) {
        setNotice(
          "この移動はもう登録に使えません。状態を確認してやり直してください。",
        );
        await refresh().catch(() => undefined);
        return;
      }
      onError(getAuthErrorMessage(error));
    }
  };

  const cancelMove = async () => {
    if (session === null) {
      setPhase("choice");
      return;
    }
    setWorking(true);
    onError(null);
    try {
      const expected =
        session.status === "awaiting_bundle" || session.status === "staged"
          ? session.status
          : undefined;
      const next = await cancelSecretaryTransfer(
        { flowId, nonce: flowNonce },
        session.session_id,
        expected,
      );
      clearMoveURL(flowId);
      if (!mounted.current) return;
      setMoveURL(null);
      applySession(next);
    } catch (error) {
      if (!mounted.current) return;
      // A pin mismatch means the session moved meanwhile; show where it is.
      if (error instanceof AuthAPIError && error.status === 409) {
        await refresh().catch(() => undefined);
        setNotice("移動の状態が変わりました。表示を確認してください。");
        return;
      }
      onError(getAuthErrorMessage(error));
    } finally {
      if (mounted.current) setWorking(false);
    }
  };

  const cancelFlow = async () => {
    setWorking(true);
    onError(null);
    try {
      // Leaving registration must not strand an open move: the Local source
      // stays sealed until this session closes, so cancel it first.
      if (isOpenSession(session)) {
        try {
          await cancelSecretaryTransfer(
            { flowId, nonce: flowNonce },
            session.session_id,
            session.status === "awaiting_bundle" || session.status === "staged"
              ? session.status
              : undefined,
          );
          clearMoveURL(flowId);
        } catch {
          // The session's own deadlines still close it.
        }
      }
      await onCancel();
    } catch (error) {
      if (mounted.current) onError(getAuthErrorMessage(error));
    } finally {
      if (mounted.current) setWorking(false);
    }
  };

  const copyMoveURL = async () => {
    if (!moveURL) return;
    try {
      await navigator.clipboard.writeText(moveURL);
      setCopied(true);
      globalThis.setTimeout(() => {
        if (mounted.current) setCopied(false);
      }, 2_000);
    } catch {
      onError("コピーできませんでした。URLを選択してコピーしてください。");
    }
  };

  const disabled = busy || working;

  return (
    <div className="space-y-4">
      <p className="rounded-lg bg-muted px-3 py-2.5 text-sm">
        対象アカウント: {accountLabel}
      </p>

      {phase === "choice" && (
        <>
          <p className="text-muted-foreground text-sm leading-6">
            {transferAvailable
              ? "この認証情報に対応するSumiアカウントはまだありません。新しい秘書で登録するか、Sumi Localで使っている秘書を引き継ぐかを選べます。"
              : "この認証情報に対応するSumiアカウントはまだありません。新規登録して続けますか？"}
          </p>
          <Button
            type="button"
            onClick={() => void finish()}
            disabled={disabled}
            className="h-11 w-full rounded-lg"
          >
            {busy && <LoaderCircle className="size-5 animate-spin" />}
            新しい秘書で登録する
          </Button>
          {transferAvailable && (
            <Button
              type="button"
              variant="outline"
              onClick={() => void startMove()}
              disabled={disabled}
              className="h-11 w-full rounded-lg"
            >
              {working && <LoaderCircle className="size-5 animate-spin" />}
              Localの秘書を引き継ぐ
            </Button>
          )}
          <Button
            type="button"
            variant="ghost"
            size="sm"
            onClick={() => void cancelFlow()}
            disabled={disabled}
            className="w-full"
          >
            キャンセル
          </Button>
        </>
      )}

      {phase === "moving" && (
        <>
          {moveURL ? (
            <>
              <p className="text-muted-foreground text-sm leading-6">
                Local側で次のコマンドを実行してください。秘書の状態がCloudへ移動します。
              </p>
              <div className="rounded-lg border bg-muted/50 px-3 py-2.5">
                <code className="block break-all font-mono text-xs leading-5">
                  {`sumi-local-move start '${moveURL}'`}
                </code>
              </div>
              <Button
                type="button"
                variant="outline"
                onClick={() => void copyMoveURL()}
                disabled={disabled}
                className="h-9 w-full gap-2 rounded-lg text-sm"
              >
                {copied ? (
                  <Check className="size-4" />
                ) : (
                  <Copy className="size-4" />
                )}
                {copied ? "コピーしました" : "コマンドをコピー"}
              </Button>
            </>
          ) : (
            <p className="text-muted-foreground text-sm leading-6">
              移動は進行中です。移動URLをお持ちでない場合は、この移動を取り消してやり直してください。
            </p>
          )}
          <p
            role="status"
            className="rounded-lg bg-muted px-3 py-2.5 text-muted-foreground text-sm leading-6"
          >
            {authLost
              ? "ログインの有効期限が切れました。このページを閉じても移動は継続します。もう一度ログインすると続きから確認できます。"
              : session?.status === "awaiting_bundle"
                ? "秘書の到着を待っています…"
                : "状態を確認しています…"}
          </p>
          {session && session.not_included.length > 0 && (
            <p className="text-muted-foreground text-xs leading-5">
              移動するのは秘書の状態のみです。次は引き継がれません:{" "}
              {session.not_included
                .map((entry) => exclusionLabel(entry.name))
                .join("、")}
              。
            </p>
          )}
          <Button
            type="button"
            variant="outline"
            onClick={() => void cancelMove()}
            disabled={disabled}
            className="h-11 w-full rounded-lg"
          >
            {working && <LoaderCircle className="size-5 animate-spin" />}
            移動を取り消して戻る
          </Button>
        </>
      )}

      {phase === "arrived" && session && (
        <>
          <p
            role="status"
            className="rounded-lg bg-emerald-50 px-3 py-2.5 text-emerald-800 text-sm dark:bg-emerald-950/30 dark:text-emerald-200"
          >
            Localの秘書が届きました。この秘書で登録を完了すると、同じ秘書がCloudで有効になり、Local側は終了します。
          </p>
          {session.arrival?.model_connection_required && (
            <p className="text-muted-foreground text-xs leading-5">
              認証情報は移動しません。完了後にモデル接続を選択してください。
            </p>
          )}
          <Button
            type="button"
            onClick={() => void finish()}
            disabled={disabled}
            className="h-11 w-full rounded-lg"
          >
            {busy && <LoaderCircle className="size-5 animate-spin" />}
            この秘書で登録を完了
          </Button>
          {session.status === "staged" && (
            <Button
              type="button"
              variant="ghost"
              size="sm"
              onClick={() => void cancelMove()}
              disabled={disabled}
              className="w-full"
            >
              移動を取り消す
            </Button>
          )}
        </>
      )}

      {notice && (
        <p
          role="status"
          className="rounded-lg bg-amber-50 px-3 py-2.5 text-amber-800 text-sm dark:bg-amber-950/30 dark:text-amber-200"
        >
          {notice}
        </p>
      )}
    </div>
  );
}
