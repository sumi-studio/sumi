import { Button } from "@sumi/ui/components/button";
import { DoorOpen, RefreshCw } from "lucide-react";
import { type ReactNode, useEffect, useRef } from "react";
import { useConversation } from "../agent/store";
import { ChatScreen } from "../components/chat-screen";
import {
  participantInstallation,
  useParticipantApps,
} from "../participant/app-store";
import { DIRECT_CHAT_RENDERER } from "../shell/app-descriptors";
import { preissuedSessionMode, useAuth } from "./auth-context";

/**
 * A failed authenticated upgrade is indistinguishable from a transient close
 * at the WebSocket layer. Re-check the HttpOnly session after a live
 * connection attempt closes. The workspace stays mounted during revalidation;
 * confirmed session loss makes AuthGate close it and cancel reconnects.
 */
export function DirectChatGate() {
  const connection = useConversation((state) => state.connection);
  const previousConnection = useRef(connection);
  const { authenticated, refreshSession, user } = useAuth();
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
    DIRECT_CHAT_RENDERER.appId,
  );
  const descriptor = catalog.find(
    (app) =>
      app.appId === DIRECT_CHAT_RENDERER.appId && app.participantOwnerAllowed,
  );
  const exactHumanOwner =
    authenticated &&
    user !== null &&
    owner?.kind === "participant" &&
    owner.participant.kind === "human" &&
    owner.participant.humanId === user.id;
  const directChatEnabled =
    (preissuedSessionMode && preissuedDirectChatBinding !== null) ||
    (exactHumanOwner &&
      installation !== "duplicate" &&
      installation?.state === "enabled");
  const binding = preissuedSessionMode
    ? preissuedDirectChatBinding
    : exactHumanOwner &&
        installation !== "duplicate" &&
        installation?.state === "enabled"
      ? {
          installationId: installation.installationId,
          authorityEpoch: installation.authorityEpoch,
        }
      : null;

  useEffect(() => {
    const previous = previousConnection.current;
    previousConnection.current = connection;
    if (
      directChatEnabled &&
      connection === "closed" &&
      (previous === "connecting" || previous === "connected")
    ) {
      void Promise.allSettled([refreshSession(), refresh()]);
    }
  }, [connection, directChatEnabled, refresh, refreshSession]);

  if (directChatEnabled && binding) {
    return <ChatScreen {...binding} />;
  }

  if (!exactHumanOwner || status === "idle" || status === "loading") {
    return <DirectChatLifecycle title="直通を確認しています…" />;
  }

  if (status === "error") {
    return (
      <DirectChatLifecycle
        title="直通の状態を確認できません"
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
      <DirectChatLifecycle
        title="直通の導入状態を修復してください"
        detail="同じ個人用アプリが複数登録されているため、安全に開けません。"
      />
    );
  }

  if (!installation) {
    return (
      <DirectChatLifecycle
        title="直通はまだ導入されていません"
        detail={
          descriptor
            ? "直通はあなた個人に属するアプリです。Workspaceとは独立して導入できます。"
            : "この環境では直通を導入できません。"
        }
        error={
          errorCode ? "導入できませんでした。再試行してください。" : undefined
        }
        action={
          descriptor ? (
            <Button
              disabled={mutation !== null}
              onClick={() =>
                void installApp(DIRECT_CHAT_RENDERER.appId).catch(
                  () => undefined,
                )
              }
            >
              直通を導入
            </Button>
          ) : undefined
        }
      />
    );
  }

  return (
    <DirectChatLifecycle
      title="直通は無効になっています"
      detail="会話は保持されています。有効にすると、同じ直通へ戻れます。"
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

const preissuedDirectChatBinding = (() => {
  if (!preissuedSessionMode) return null;
  const env = (
    import.meta as ImportMeta & { env?: Record<string, string | undefined> }
  ).env;
  const installationId = env?.VITE_SUMI_DIRECT_CHAT_INSTALLATION_ID?.trim();
  const authorityEpoch = env?.VITE_SUMI_DIRECT_CHAT_AUTHORITY_EPOCH?.trim();
  return installationId && authorityEpoch
    ? { installationId, authorityEpoch }
    : null;
})();

function DirectChatLifecycle({
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
            <DoorOpen className="size-5 text-muted-foreground" />
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
