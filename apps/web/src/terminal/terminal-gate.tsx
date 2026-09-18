import { Button } from "@sumi/ui/components/button";
import { RefreshCw, SquareTerminal } from "lucide-react";
import type { ReactNode } from "react";
import { preissuedSessionMode, useAuth } from "../auth/auth-context";
import {
  participantInstallation,
  useParticipantApps,
} from "../participant/app-store";
import { TERMINAL_RENDERER } from "../shell/app-descriptors";
import { TerminalScreen } from "./terminal-screen";

/**
 * Lifecycle gate for the shared terminal. It is Participant-owned like the
 * 直通 surface: the screen only opens for the exact signed-in Human with
 * exactly one enabled `terminal` installation. Authority is re-read from the
 * installation record, so a revoked epoch (or a forbidden WS close) drops
 * back here after the store refresh instead of retrying stale scope.
 */
export function TerminalGate({
  sessionId,
  onSelectSession,
}: {
  sessionId?: string;
  onSelectSession?: (sessionId: string | null) => void;
}) {
  const { authenticated, user } = useAuth();
  const owner = useParticipantApps((state) => state.owner);
  const status = useParticipantApps((state) => state.status);
  const catalog = useParticipantApps((state) => state.catalog);
  const installations = useParticipantApps((state) => state.installations);
  const mutation = useParticipantApps((state) => state.mutation);
  const errorCode = useParticipantApps((state) => state.errorCode);
  const refresh = useParticipantApps((state) => state.refresh);
  const installApp = useParticipantApps((state) => state.installApp);
  const setInstallationState = useParticipantApps(
    (state) => state.setInstallationState,
  );
  const installation = participantInstallation(
    installations,
    TERMINAL_RENDERER.appId,
  );
  const descriptor = catalog.find(
    (app) =>
      app.appId === TERMINAL_RENDERER.appId && app.participantOwnerAllowed,
  );
  const exactHumanOwner =
    authenticated &&
    user !== null &&
    owner?.kind === "participant" &&
    owner.participant.kind === "human" &&
    owner.participant.humanId === user.id;
  const terminalEnabled =
    (preissuedSessionMode && preissuedTerminalBinding !== null) ||
    (exactHumanOwner &&
      installation !== "duplicate" &&
      installation?.state === "enabled");
  const binding = preissuedSessionMode
    ? preissuedTerminalBinding
    : exactHumanOwner &&
        installation !== "duplicate" &&
        installation?.state === "enabled"
      ? {
          installationId: installation.installationId,
          authorityEpoch: installation.authorityEpoch,
        }
      : null;

  if (terminalEnabled && binding) {
    return (
      <TerminalScreen
        {...binding}
        sessionId={sessionId}
        onSelectSession={onSelectSession}
      />
    );
  }

  if (!exactHumanOwner || status === "idle" || status === "loading") {
    return <TerminalLifecycle title="ターミナルを確認しています…" />;
  }

  if (status === "error") {
    return (
      <TerminalLifecycle
        title="ターミナルの状態を確認できません"
        detail="個人用アプリの最新状態を読み込めませんでした。"
        action={
          <Button onClick={() => void refresh()} className="gap-2">
            <RefreshCw className="size-4" />
            再試行
          </Button>
        }
      />
    );
  }

  if (installation === "duplicate") {
    return (
      <TerminalLifecycle
        title="ターミナルの導入状態を修復してください"
        detail="同じ個人用アプリが複数登録されているため、安全に開けません。"
      />
    );
  }

  if (!installation) {
    return (
      <TerminalLifecycle
        title="ターミナルはまだ導入されていません"
        detail={
          descriptor
            ? "ターミナルはあなたと秘書が共有する個人用アプリです。Workspaceとは独立して導入できます。"
            : "この環境ではターミナルを導入できません。"
        }
        error={
          errorCode ? "導入できませんでした。再試行してください。" : undefined
        }
        action={
          descriptor ? (
            <Button
              disabled={mutation !== null}
              onClick={() =>
                void installApp(TERMINAL_RENDERER.appId).catch(() => undefined)
              }
            >
              ターミナルを導入
            </Button>
          ) : undefined
        }
      />
    );
  }

  return (
    <TerminalLifecycle
      title="ターミナルは無効になっています"
      detail="セッションと出力は保持されています。有効にすると、同じターミナルへ戻れます。"
      error={
        errorCode ? "有効にできませんでした。再試行してください。" : undefined
      }
      action={
        <Button
          disabled={mutation !== null}
          onClick={() =>
            void setInstallationState(
              installation.installationId,
              "enabled",
            ).catch(() => undefined)
          }
        >
          有効にする
        </Button>
      }
    />
  );
}

const preissuedTerminalBinding = (() => {
  if (!preissuedSessionMode) return null;
  const env = (
    import.meta as ImportMeta & { env?: Record<string, string | undefined> }
  ).env;
  const installationId = env?.VITE_SUMI_TERMINAL_INSTALLATION_ID?.trim();
  const authorityEpoch = env?.VITE_SUMI_TERMINAL_AUTHORITY_EPOCH?.trim();
  return installationId && authorityEpoch
    ? { installationId, authorityEpoch }
    : null;
})();

function TerminalLifecycle({
  title,
  detail,
  error,
  action,
}: {
  title: string;
  detail?: string;
  error?: string;
  action?: ReactNode;
}) {
  return (
    <div className="flex h-full bg-background text-foreground">
      <main className="grid min-w-0 flex-1 place-items-center px-6">
        <section
          aria-live="polite"
          className="flex max-w-sm flex-col items-center text-center"
        >
          <span className="mb-4 grid size-11 place-items-center rounded-xl border border-border bg-muted/35">
            <SquareTerminal className="size-5 text-muted-foreground" />
          </span>
          <h1 className="font-semibold text-lg tracking-tight">{title}</h1>
          {detail ? (
            <p className="mt-2 text-muted-foreground text-sm leading-6">
              {detail}
            </p>
          ) : null}
          {error ? (
            <p role="alert" className="mt-2 text-red-600 text-xs">
              {error}
            </p>
          ) : null}
          {action ? <div className="mt-5">{action}</div> : null}
        </section>
      </main>
    </div>
  );
}
