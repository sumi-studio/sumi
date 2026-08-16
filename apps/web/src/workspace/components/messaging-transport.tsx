import { useLayoutEffect } from "react";
import { useAuth } from "../../auth/auth-context";
import { type MessagingScope, sameMessagingScope } from "../../messaging/scope";
import {
  bindMessagingScope,
  getMessagingScope,
  useMessaging,
} from "../../messaging/store";
import { useWorkspaceControl } from "../store";
import { messagingScopeForWorkspace } from "../messaging-scope";

/**
 * Shell-lifetime owner for the Messaging transport. The selected Workspace
 * remains selected while another app is active, so its one socket continues
 * projecting events into the shared store across app-route transitions.
 */
export function MessagingTransport() {
  const { user } = useAuth();
  const selectedWorkspaceId = useWorkspaceControl(
    (state) => state.selectedWorkspaceId,
  );
  const catalog = useWorkspaceControl((state) => state.catalog);
  const installations = useWorkspaceControl((state) => state.installations);
  const members = useWorkspaceControl((state) => state.members);
  const desired: MessagingScope | null = messagingScopeForWorkspace({
    workspaceId: selectedWorkspaceId,
    humanId: user?.id,
    catalog,
    installations,
    members,
  });

  useLayoutEffect(() => {
    if (!sameMessagingScope(getMessagingScope(), desired)) {
      bindMessagingScope(desired);
    }
    if (desired) useMessaging.getState().init();
  }, [
    desired?.authorityEpoch,
    desired?.installationId,
    desired?.workspaceId,
  ]);

  // Deliberately no unmount cleanup: AppShell persists across in-app routes.
  // Session logout resets Messaging through AuthGate; scope changes above close
  // the old backend before installing the next exact authority tuple.
  return null;
}
