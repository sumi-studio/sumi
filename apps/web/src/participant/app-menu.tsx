import { Button } from "@sumi/ui/components/button";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@sumi/ui/components/popover";
import { useNavigate } from "@tanstack/react-router";
import {
  AlarmClock,
  AppWindow,
  ArrowUpRight,
  BookOpenText,
  ChevronRight,
  LoaderCircle,
  RefreshCw,
  Trash2,
} from "lucide-react";
import type { ComponentType } from "react";
import { DIRECT_CHAT_RENDERER } from "../shell/app-descriptors";
import type { AppDescriptor, AppInstallation } from "../workspace/model";
import { participantInstallation, useParticipantApps } from "./app-store";

export function ParticipantAppsMenu() {
  const navigate = useNavigate();
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
  const uninstallApp = useParticipantApps((state) => state.uninstallApp);
  const participantApps = catalog.filter(
    (descriptor) => descriptor.participantOwnerAllowed,
  );
  const busy = mutation !== null;

  const run = async (action: () => Promise<unknown>) => {
    try {
      await action();
    } catch {
      // The owner-scoped store retains the stable snapshot and exposes one
      // error state. A failed mutation must not make an enabled rail vanish.
    }
  };

  return (
    <Popover>
      <PopoverTrigger
        openOnHover
        delay={0}
        closeDelay={120}
        render={
          <Button
            variant="ghost"
            className="w-full justify-start gap-2 px-2.5 text-popover-foreground hover:text-popover-foreground"
          />
        }
      >
        <AppWindow className="size-4" />
        <span>個人用アプリ</span>
        {status === "loading" ? (
          <LoaderCircle className="ml-auto size-3.5 animate-spin text-muted-foreground" />
        ) : (
          <ChevronRight className="ml-auto size-4 text-neutral-400" />
        )}
      </PopoverTrigger>
      <PopoverContent
        side="right"
        align="end"
        sideOffset={4}
        className="w-80 p-1.5"
        aria-label="個人用アプリ"
      >
        <header className="flex items-center gap-2 px-2 py-1.5">
          <div className="min-w-0 flex-1">
            <h2 className="font-semibold text-sm">個人用アプリ</h2>
            <p className="mt-0.5 text-muted-foreground text-[11px]">
              Workspaceを切り替えても変わりません
            </p>
          </div>
          {status === "ready" || installations.length > 0 ? (
            <Button
              variant="ghost"
              size="icon"
              aria-label="個人用アプリを更新"
              disabled={busy || status === "loading"}
              onClick={() => void refresh()}
              className="size-7"
            >
              <RefreshCw className="size-3.5" />
            </Button>
          ) : null}
        </header>

        {status === "idle" ||
        (status === "loading" && participantApps.length === 0) ? (
          <p
            role="status"
            className="px-2 py-5 text-center text-muted-foreground text-xs"
          >
            アプリを確認しています…
          </p>
        ) : null}

        {status === "error" && participantApps.length === 0 ? (
          <div className="px-2 py-4 text-center">
            <p role="alert" className="text-muted-foreground text-xs">
              {errorCode
                ? `個人用アプリを読み込めませんでした: ${errorCode}`
                : "個人用アプリを読み込めませんでした"}
            </p>
            <Button
              variant="secondary"
              size="sm"
              onClick={() => void refresh()}
              className="mt-3"
            >
              再試行
            </Button>
          </div>
        ) : null}

        {participantApps.length > 0 ? (
          <div className="space-y-0.5">
            {participantApps.map((descriptor) => (
              <ParticipantAppRow
                key={descriptor.appId}
                descriptor={descriptor}
                installation={participantInstallation(
                  installations,
                  descriptor.appId,
                )}
                busy={busy}
                onInstall={() => run(() => installApp(descriptor.appId))}
                onState={(installationId, state) =>
                  run(() => setInstallationState(installationId, state))
                }
                onUninstall={(installationId) =>
                  run(() => uninstallApp(installationId))
                }
                onOpen={
                  descriptor.appId === DIRECT_CHAT_RENDERER.appId
                    ? () => void navigate({ to: DIRECT_CHAT_RENDERER.route })
                    : undefined
                }
              />
            ))}
          </div>
        ) : null}

        {errorCode && participantApps.length > 0 ? (
          <p role="alert" className="px-2 pt-2 pb-1 text-red-600 text-xs">
            最新の状態を確認できませんでした: {errorCode}
          </p>
        ) : null}
      </PopoverContent>
    </Popover>
  );
}

function ParticipantAppRow({
  descriptor,
  installation,
  busy,
  onInstall,
  onState,
  onUninstall,
  onOpen,
}: {
  descriptor: AppDescriptor;
  installation: AppInstallation | "duplicate" | null;
  busy: boolean;
  onInstall: () => Promise<unknown>;
  onState: (
    installationId: string,
    state: "enabled" | "disabled",
  ) => Promise<unknown>;
  onUninstall: (installationId: string) => Promise<unknown>;
  onOpen?: () => void;
}) {
  const Icon = participantAppIcon(descriptor.appId);
  if (installation === "duplicate") {
    return (
      <div className="rounded-lg px-2 py-2.5">
        <div className="flex items-center gap-2.5">
          <AppIcon icon={Icon} />
          <div className="min-w-0 flex-1">
            <p className="truncate font-medium text-sm">
              {descriptor.displayName}
            </p>
            <p role="alert" className="mt-0.5 text-red-600 text-[11px]">
              重複した導入状態を修復してください
            </p>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="group flex min-h-12 items-center gap-2.5 rounded-lg px-2 py-2 hover:bg-muted/55">
      <AppIcon icon={Icon} />
      <div className="min-w-0 flex-1">
        <p className="truncate font-medium text-sm">{descriptor.displayName}</p>
        <p className="mt-0.5 text-muted-foreground text-[11px]">
          {!installation
            ? "未導入"
            : installation.state === "enabled"
              ? "有効"
              : "無効・データは保持"}
        </p>
      </div>
      <div className="flex shrink-0 items-center gap-1">
        {!installation ? (
          <Button
            size="sm"
            disabled={busy}
            onClick={() => void onInstall()}
            className="h-7 px-2 text-xs"
          >
            導入
          </Button>
        ) : null}
        {installation?.state === "disabled" ? (
          <Button
            size="sm"
            disabled={busy}
            onClick={() => void onState(installation.installationId, "enabled")}
            className="h-7 px-2 text-xs"
          >
            有効化
          </Button>
        ) : null}
        {installation?.state === "enabled" && onOpen ? (
          <Button
            variant="ghost"
            size="icon"
            aria-label={`${descriptor.displayName}を開く`}
            onClick={onOpen}
            className="size-7"
          >
            <ArrowUpRight className="size-3.5" />
          </Button>
        ) : null}
        {installation?.state === "enabled" ? (
          <Button
            variant="secondary"
            size="sm"
            disabled={busy}
            onClick={() =>
              void onState(installation.installationId, "disabled")
            }
            className="h-7 px-2 text-xs"
          >
            無効化
          </Button>
        ) : null}
        {installation ? (
          <Button
            variant="ghost"
            size="icon"
            aria-label={`${descriptor.displayName}をアンインストール`}
            disabled={busy}
            onClick={() => {
              if (
                !window.confirm(
                  `${descriptor.displayName}をアンインストールしますか？`,
                )
              ) {
                return;
              }
              void onUninstall(installation.installationId);
            }}
            className="size-7 text-muted-foreground hover:text-foreground"
          >
            <Trash2 className="size-3.5" />
          </Button>
        ) : null}
      </div>
    </div>
  );
}

function participantAppIcon(
  appId: string,
): ComponentType<{ className?: string }> {
  if (appId === "alarm") return AlarmClock;
  if (appId === "life-log") return BookOpenText;
  if (appId === DIRECT_CHAT_RENDERER.appId) {
    return DIRECT_CHAT_RENDERER.icon;
  }
  return AppWindow;
}

function AppIcon({
  icon: Icon,
}: {
  icon: ComponentType<{ className?: string }>;
}) {
  return (
    <span className="grid size-8 shrink-0 place-items-center rounded-lg bg-muted">
      <Icon className="size-4" />
    </span>
  );
}
