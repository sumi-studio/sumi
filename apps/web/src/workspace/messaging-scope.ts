import type { AppDescriptor, AppInstallation, WorkspaceMembership } from "./model";
import { exactHumanMembership } from "./store";
import type { MessagingScope } from "../messaging/scope";

/**
 * Resolves the single authority tuple Messaging may use for the selected
 * Workspace. Missing or ambiguous authority deliberately yields no scope.
 */
export function messagingScopeForWorkspace({
  workspaceId,
  humanId,
  catalog,
  installations,
  members,
}: {
  workspaceId: string | null;
  humanId: string | undefined;
  catalog: AppDescriptor[];
  installations: AppInstallation[];
  members: WorkspaceMembership[];
}): MessagingScope | null {
  if (!workspaceId) return null;
  const descriptors = catalog.filter(
    (descriptor) => descriptor.appId === "messaging",
  );
  const matching = installations.filter(
    (installation) => installation.appId === "messaging",
  );
  const descriptor = descriptors.length === 1 ? descriptors[0] : null;
  const installation = matching.length === 1 ? matching[0] : null;
  const membership = exactHumanMembership(members, humanId);
  if (!descriptor || !membership || installation?.state !== "enabled") {
    return null;
  }
  return {
    workspaceId,
    installationId: installation.installationId,
    authorityEpoch: installation.authorityEpoch,
  };
}
