import { Button } from "@sumi/ui/components/button";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@sumi/ui/components/popover";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@sumi/ui/components/tooltip";
import { useNavigate } from "@tanstack/react-router";
import { Building2, Check, ChevronsUpDown, LayoutGrid } from "lucide-react";
import { preissuedSessionMode, useAuth } from "../auth/auth-context";
import { SettingsPopover } from "../components/app-navigation";
import {
  participantInstallation,
  useParticipantApps,
} from "../participant/app-store";
import { useWorkspaceControl } from "../workspace/store";
import {
  DIRECT_CHAT_RENDERER,
  WORKSPACE_APP_RENDERERS,
} from "./app-descriptors";

export function AppRail({
  activeAppId,
  workspaceId,
}: {
  activeAppId: string;
  workspaceId?: string;
}) {
  const navigate = useNavigate();
  const { authenticated, user } = useAuth();
  const workspaces = useWorkspaceControl((state) => state.workspaces);
  const selectedWorkspace = useWorkspaceControl(
    (state) => state.selectedWorkspace,
  );
  const catalog = useWorkspaceControl((state) => state.catalog);
  const installations = useWorkspaceControl((state) => state.installations);
  const participantOwner = useParticipantApps((state) => state.owner);
  const participantInstallations = useParticipantApps(
    (state) => state.installations,
  );
  const enabledApps = catalog.flatMap((descriptor) => {
    if (
      catalog.filter((candidate) => candidate.appId === descriptor.appId)
        .length !== 1
    ) {
      return [];
    }
    const matching = installations.filter(
      (installation) => installation.appId === descriptor.appId,
    );
    const installation = matching.length === 1 ? matching[0] : null;
    const renderer = WORKSPACE_APP_RENDERERS[descriptor.appId];
    return installation?.state === "enabled" && renderer
      ? [{ descriptor, installation, renderer }]
      : [];
  });
  const directInstallation = participantInstallation(
    participantInstallations,
    DIRECT_CHAT_RENDERER.appId,
  );
  const exactHumanOwner =
    authenticated &&
    user !== null &&
    participantOwner?.kind === "participant" &&
    participantOwner.participant.kind === "human" &&
    participantOwner.participant.humanId === user.id;
  const directChatEnabled =
    preissuedSessionMode ||
    (exactHumanOwner &&
      directInstallation !== "duplicate" &&
      directInstallation?.state === "enabled");
  const DirectIcon = DIRECT_CHAT_RENDERER.icon;

  return (
    <aside className="app-sidebar flex h-dvh w-12 shrink-0 flex-col overflow-clip">
      <nav className="flex flex-col gap-1 px-1 py-2" aria-label="Sumi">
        {workspaceId ? (
          <Popover>
            <Tooltip>
              <TooltipTrigger
                render={
                  <PopoverTrigger
                    render={
                      <Button
                        variant="ghost"
                        size="icon"
                        aria-label="Workspaceを切り替える"
                        className="relative size-10"
                      />
                    }
                  />
                }
              >
                <span className="grid size-7 place-items-center rounded-lg bg-foreground font-semibold text-background text-xs">
                  {(selectedWorkspace?.name.trim().at(0) || "W").toUpperCase()}
                </span>
                <ChevronsUpDown className="absolute right-0.5 bottom-0.5 size-3 rounded bg-background text-muted-foreground" />
              </TooltipTrigger>
              <TooltipContent side="right">
                Workspaceを切り替える
              </TooltipContent>
            </Tooltip>
            <PopoverContent
              side="right"
              align="start"
              className="w-64 p-1.5"
              aria-label="Workspaceを切り替える"
            >
              <p className="px-2 py-1 font-medium text-muted-foreground text-xs">
                Workspace
              </p>
              {workspaces.map((workspace) => (
                <Button
                  key={workspace.workspaceId}
                  variant="ghost"
                  className="w-full justify-start gap-2 px-2"
                  onClick={() =>
                    void navigate({
                      to: "/w/$workspaceId",
                      params: { workspaceId: workspace.workspaceId },
                    })
                  }
                >
                  <span className="grid size-6 shrink-0 place-items-center rounded-md bg-muted font-semibold text-[11px]">
                    {(workspace.name.trim().at(0) || "W").toUpperCase()}
                  </span>
                  <span className="min-w-0 flex-1 truncate text-left">
                    {workspace.name}
                  </span>
                  {workspace.workspaceId === workspaceId ? (
                    <Check className="size-3.5 shrink-0" />
                  ) : null}
                </Button>
              ))}
              <div className="my-1 border-border border-t" />
              <Button
                variant="ghost"
                className="w-full justify-start gap-2 px-2"
                onClick={() => void navigate({ to: "/" })}
              >
                <LayoutGrid className="size-4" />
                Workspace一覧
              </Button>
            </PopoverContent>
          </Popover>
        ) : null}

        {workspaceId ? (
          <div className="mx-2 my-1 border-border border-t" />
        ) : null}

        {workspaceId ? (
          <RailButton
            label="Workspace"
            active={activeAppId === "workspace"}
            onClick={() =>
              void navigate({
                to: "/w/$workspaceId",
                params: { workspaceId },
              })
            }
          >
            <Building2 className="size-4" />
          </RailButton>
        ) : null}

        {workspaceId
          ? enabledApps.map(({ descriptor, installation, renderer }) => {
              const Icon = renderer.icon;
              return (
                <RailButton
                  key={installation.installationId}
                  label={descriptor.displayName}
                  active={activeAppId === descriptor.appId}
                  onClick={() =>
                    void navigate({ to: renderer.route(workspaceId) })
                  }
                >
                  <Icon className="size-4" />
                </RailButton>
              );
            })
          : null}

        {directChatEnabled ? (
          <RailButton
            label={DIRECT_CHAT_RENDERER.label}
            active={activeAppId === DIRECT_CHAT_RENDERER.appId}
            onClick={() => void navigate({ to: DIRECT_CHAT_RENDERER.route })}
          >
            <DirectIcon className="size-4" />
          </RailButton>
        ) : null}
      </nav>
      <div className="mt-auto px-2 pb-3">
        <SettingsPopover />
      </div>
    </aside>
  );
}

function RailButton({
  label,
  active,
  onClick,
  children,
}: {
  label: string;
  active: boolean;
  onClick: () => void;
  children: React.ReactNode;
}) {
  return (
    <Tooltip>
      <TooltipTrigger
        render={
          <Button
            variant="ghost"
            size="icon"
            aria-label={label}
            aria-current={active ? "page" : undefined}
            onClick={onClick}
            className={`size-10 ${active ? "bg-interactive-active" : ""}`}
          />
        }
      >
        {children}
      </TooltipTrigger>
      <TooltipContent side="right">{label}</TooltipContent>
    </Tooltip>
  );
}
