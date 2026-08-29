import { useLayoutEffect } from "react";
import { useAuth } from "../../auth/auth-context";
import { type MessagingScope, sameMessagingScope } from "../../messaging/scope";
import {
  bindMessagingScope,
  getMessagingScope,
  useMessaging,
} from "../../messaging/store";
import { messagingScopeForWorkspace } from "../messaging-scope";
import { useWorkspaceControl } from "../store";

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
  const desiredWorkspaceId = desired?.workspaceId ?? null;
  const desiredInstallationId = desired?.installationId ?? null;
  const desiredAuthorityEpoch = desired?.authorityEpoch ?? null;

  useLayoutEffect(() => {
    const next =
      desiredWorkspaceId !== null &&
      desiredInstallationId !== null &&
      desiredAuthorityEpoch !== null
        ? {
            workspaceId: desiredWorkspaceId,
            installationId: desiredInstallationId,
            authorityEpoch: desiredAuthorityEpoch,
          }
        : null;
    if (!sameMessagingScope(getMessagingScope(), next)) {
      bindMessagingScope(next);
    }
    if (next) useMessaging.getState().init();
  }, [desiredAuthorityEpoch, desiredInstallationId, desiredWorkspaceId]);

  // Deliberately no unmount cleanup: AppShell persists across in-app routes.
  // Session logout resets Messaging through AuthGate; scope changes above close
  // the old backend before installing the next exact authority tuple.
  return null;
}
