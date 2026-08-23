import { Button } from "@sumi/ui/components/button";
import { useNavigate } from "@tanstack/react-router";
import { AlertTriangle, MessageCircle, Power } from "lucide-react";
import { type ReactNode, useState } from "react";
import { useAuth } from "../../auth/auth-context";
import { messagingScopeKey, sameMessagingScope } from "../../messaging/scope";
import { getMessagingScope, useMessaging } from "../../messaging/store";
import { messagingScopeForWorkspace } from "../messaging-scope";
import {
  effectiveWorkspacePermissions,
  exactHumanMembership,
  useWorkspaceControl,
} from "../store";

export function MessagingScopeGate({
  workspaceId,
  children,
}: {
  workspaceId: string;
  children: ReactNode;
}) {
  const installations = useWorkspaceControl((state) => state.installations);
  const catalog = useWorkspaceControl((state) => state.catalog);
  const members = useWorkspaceControl((state) => state.members);
  const roles = useWorkspaceControl((state) => state.roles);
  const mutation = useWorkspaceControl((state) => state.mutation);
  const installApp = useWorkspaceControl((state) => state.installApp);
  const setInstallationState = useWorkspaceControl(
    (state) => state.setInstallationState,
  );
  // The shell replaces the transport before this route can render it. Subscribe
  // to that replacement so this gate re-reads the exact scope synchronously.
  useMessaging((state) => state.transportGeneration);
  const { user } = useAuth();
  const [failed, setFailed] = useState("");
  const matching = installations.filter(
    (installation) => installation.appId === "messaging",
  );
  const descriptors = catalog.filter(
    (descriptor) => descriptor.appId === "messaging",
  );
  const descriptor = descriptors.length === 1 ? descriptors[0] : null;
  const installation = matching.length === 1 ? matching[0] : null;
  const ownMembership = exactHumanMembership(members, user?.id);
  const desired = messagingScopeForWorkspace({
    workspaceId,
    humanId: user?.id,
    catalog,
    installations,
    members,
  });
  const current = getMessagingScope();
  const canManageApps = effectiveWorkspacePermissions(ownMembership, roles).has(
    "manage_apps",
  );

  if (!ownMembership) {
    return (
      <MessagingLifecycleState
        workspaceId={workspaceId}
        icon={<AlertTriangle className="size-5" />}
        title="Workspaceへの参加状態を確認できません"
        detail="現在の参加状態が欠けているか重複しています。Workspaceを再読み込みしてください。"
      />
    );
  }

  if (!descriptor) {
    return (
      <MessagingLifecycleState
        workspaceId={workspaceId}
        icon={<AlertTriangle className="size-5" />}
        title="Messagingの提供状態を確認できません"
        detail="アプリカタログのMessagingが欠けているか重複しています。再読み込みしてください。"
      />
    );
  }

  if (matching.length > 1) {
    return (
      <MessagingLifecycleState
        workspaceId={workspaceId}
        icon={<AlertTriangle className="size-5" />}
        title="Messagingの設定を確認できません"
        detail="同じMessagingが複数登録されています。Workspaceのアプリ設定を確認してください。"
      />
    );
  }

  if (!installation) {
    return (
      <MessagingLifecycleState
        workspaceId={workspaceId}
        icon={<MessageCircle className="size-5" />}
        title="Messagingはまだインストールされていません"
        detail="このWorkspaceへ追加すると、チャンネルやDMで会話を始められます。"
        error={failed}
        action={
          canManageApps ? (
            <Button
              disabled={mutation !== null}
              onClick={() => {
                setFailed("");
                void installApp("messaging").catch(() => {
                  setFailed("Messagingをインストールできませんでした。");
                });
              }}
            >
              <MessageCircle className="size-4" />
              {mutation === "install_app"
                ? "インストール中…"
                : "Messagingをインストール"}
            </Button>
          ) : undefined
        }
      />
    );
  }

  if (installation.state === "disabled") {
    return (
      <MessagingLifecycleState
        workspaceId={workspaceId}
        icon={<Power className="size-5" />}
        title="Messagingは無効になっています"
        detail="これまでの会話は残っています。有効に戻すと、再びMessagingを利用できます。"
        error={failed}
        action={
          canManageApps ? (
            <Button
              disabled={mutation !== null}
              onClick={() => {
                setFailed("");
                void setInstallationState(
                  installation.installationId,
                  "enabled",
                ).catch(() => setFailed("Messagingを有効にできませんでした。"));
              }}
            >
              {mutation === "set_installation_enabled"
                ? "有効化中…"
                : "有効にする"}
            </Button>
          ) : undefined
        }
      />
    );
  }

  if (!desired || !sameMessagingScope(current, desired)) {
    return (
      <MessagingLifecycleState
        workspaceId={workspaceId}
        icon={<MessageCircle className="size-5" />}
        title="Messagingへ接続しています…"
      />
    );
  }

  return (
    <div key={messagingScopeKey(desired)} className="contents">
      {children}
    </div>
  );
}

function MessagingLifecycleState({
  workspaceId,
  icon,
  title,
  detail,
  error,
  action,
}: {
  workspaceId: string;
  icon: ReactNode;
  title: string;
  detail?: string;
  error?: string;
  action?: ReactNode;
}) {
  const navigate = useNavigate();
  return (
    <div className="flex h-full bg-background text-foreground">
      <main className="grid min-w-0 flex-1 place-items-center px-8">
        <section
          className="flex max-w-lg flex-col items-center text-center"
          aria-live="polite"
        >
          <span className="mb-5 grid size-12 place-items-center rounded-xl border border-border bg-muted/20">
            {icon}
          </span>
          <h1 className="font-semibold text-xl">{title}</h1>
          {detail ? (
            <p className="mt-2 text-muted-foreground text-sm leading-6">
              {detail}
            </p>
          ) : null}
          <div className="mt-5 flex items-center gap-2">
            {action}
            {detail ? (
              <Button
                variant="secondary"
                onClick={() =>
                  void navigate({
                    to: "/w/$workspaceId",
                    params: { workspaceId },
                  })
                }
              >
                Workspace設定
              </Button>
            ) : null}
          </div>
          {error ? (
            <p role="alert" className="mt-3 text-red-600 text-xs">
              {error}
            </p>
          ) : null}
        </section>
      </main>
    </div>
  );
}
