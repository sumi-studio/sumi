import { Outlet, useRouterState } from "@tanstack/react-router";
import { useLayoutEffect, useState } from "react";
import {
  recordFeedbackOrigin,
  resetFeedbackOrigin,
  startFeedbackEvidence,
} from "../feedback/diagnostics";
import { FeedbackMode, openFeedbackMode } from "../feedback/mode";
import { PushSubscriptionBridge } from "../messaging/components/push-bridge";
import { MessagingTransport } from "../workspace/components/messaging-transport";
import { useWorkspaceControl } from "../workspace/store";
import { AppRail } from "./app-rail";

export function AppShell() {
  const pathname = useRouterState({
    select: (state) => state.location.pathname,
  });
  const routeWorkspaceId = workspaceIdFromPath(pathname);
  const scopeKey = useWorkspaceControl((state) => state.sessionScopeKey);
  const selectedWorkspaceId = useWorkspaceControl(
    (state) => state.selectedWorkspaceId,
  );
  const selectionStatus = useWorkspaceControl((state) => state.selectionStatus);
  const [navigation, setNavigation] = useState<{
    scopeKey: string | null;
    workspaceId?: string;
    messagingPath?: string;
  }>({ scopeKey });
  const personalApp = pathname === "/direct" || pathname === "/feedback";
  const workspaceId =
    routeWorkspaceId ??
    (personalApp &&
    navigation.scopeKey === scopeKey &&
    navigation.workspaceId === selectedWorkspaceId &&
    selectionStatus !== "invalid"
      ? navigation.workspaceId
      : undefined);
  const messagingPath =
    navigation.scopeKey === scopeKey && navigation.workspaceId === workspaceId
      ? navigation.messagingPath
      : undefined;

  useLayoutEffect(() => {
    if (routeWorkspaceId) {
      setNavigation((previous) => ({
        scopeKey,
        workspaceId: routeWorkspaceId,
        messagingPath: /^\/w\/[^/]+\/messaging(?:\/|$)/.test(pathname)
          ? pathname
          : previous.scopeKey === scopeKey &&
              previous.workspaceId === routeWorkspaceId
            ? previous.messagingPath
            : undefined,
      }));
    } else if (!personalApp || navigation.scopeKey !== scopeKey) {
      setNavigation({ scopeKey });
    }
  }, [pathname, routeWorkspaceId, personalApp, scopeKey, navigation.scopeKey]);
  // biome-ignore lint/correctness/useExhaustiveDependencies: Reset browser evidence when the authenticated session changes.
  useLayoutEffect(() => {
    resetFeedbackOrigin();
    return startFeedbackEvidence();
  }, [scopeKey]);
  // biome-ignore lint/correctness/useExhaustiveDependencies: Re-seed current route after the session-scoped evidence reset.
  useLayoutEffect(() => {
    recordFeedbackOrigin(pathname, workspaceId);
  }, [pathname, workspaceId, scopeKey]);

  const activeAppId = /^\/w\/[^/]+\/messaging(?:\/|$)/.test(pathname)
    ? "messaging"
    : pathname === "/direct"
      ? "direct-chat"
      : pathname === "/feedback"
        ? "feedback"
        : "workspace";

  return (
    <div className="flex h-dvh bg-background text-foreground">
      <MessagingTransport />
      <PushSubscriptionBridge />
      <AppRail
        activeAppId={activeAppId}
        workspaceId={workspaceId}
        messagingPath={messagingPath}
        onOpenFeedback={() => recordFeedbackOrigin(pathname, workspaceId)}
        onReportFeedback={openFeedbackMode}
      />
      <div className="min-w-0 flex-1">
        <Outlet />
      </div>
      <FeedbackMode pathname={pathname} workspaceId={workspaceId} />
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
