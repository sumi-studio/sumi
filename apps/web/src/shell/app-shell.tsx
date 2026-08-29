import { Outlet, useRouterState } from "@tanstack/react-router";
import { MessagingTransport } from "../workspace/components/messaging-transport";
import { AppRail } from "./app-rail";

export function AppShell() {
  const pathname = useRouterState({
    select: (state) => state.location.pathname,
  });
  const workspaceId = workspaceIdFromPath(pathname);
  const activeAppId = /^\/w\/[^/]+\/messaging(?:\/|$)/.test(pathname)
    ? "messaging"
    : pathname === "/direct"
      ? "direct-chat"
      : "workspace";

  return (
    <div className="flex h-dvh bg-background text-foreground">
      <MessagingTransport />
      <AppRail activeAppId={activeAppId} workspaceId={workspaceId} />
      <div className="min-w-0 flex-1">
        <Outlet />
      </div>
    </div>
  );
}

function workspaceIdFromPath(pathname: string): string | undefined {
  const encoded = /^\/w\/([^/]+)(?:\/|$)/.exec(pathname)?.[1];
  if (encoded === undefined) return undefined;
  try {
    return decodeURIComponent(encoded);
  } catch {
    return encoded;
  }
}
