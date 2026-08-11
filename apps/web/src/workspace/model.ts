export type WorkspacePermission =
  | "manage_workspace"
  | "manage_members"
  | "manage_roles"
  | "manage_apps";

export const WORKSPACE_PERMISSIONS = [
  "manage_workspace",
  "manage_members",
  "manage_roles",
  "manage_apps",
] as const satisfies readonly WorkspacePermission[];

// Workspace stores both its own platform permissions and opaque, app-owned
// capability refs on roles. The catalog controls which refs may be newly
// granted; stored refs remain strings so retired capabilities stay visible.
export type WorkspaceRoleCapabilityRef = string;

export interface WorkspaceRoleCapabilityDescriptor {
  ref: string;
  label: string;
}

export type ParticipantRef =
  | { kind: "human"; humanId: string }
  | { kind: "personality_agent"; personalityAgentId: string };

export interface Workspace {
  workspaceId: string;
  name: string;
  ownerWorkspaceMemberId: string;
  createdAt: number;
}

export interface WorkspaceMembership {
  workspaceMemberId: string;
  workspaceId: string;
  participant: ParticipantRef;
  displayName: string;
  owner: boolean;
  roleIds: string[];
  joinedAt: number;
  leftAt: number | null;
}

export interface WorkspaceInvite {
  inviteId: string;
  workspaceId: string;
  code: string;
  expiresAt: number;
  createdAt: number;
}

export type WorkspaceInviteRecord = Omit<WorkspaceInvite, "code">;

export interface WorkspaceInviteSecret {
  inviteId: string;
  code: string;
}

export interface WorkspaceInvitePreview {
  workspaceId: string;
  workspaceName: string;
  expiresAt: number;
}

export interface WorkspaceRoleInput {
  name: string;
  color?: string;
  position?: number;
  permissions: WorkspaceRoleCapabilityRef[];
}

export interface WorkspaceRole extends WorkspaceRoleInput {
  roleId: string;
  workspaceId: string;
  position: number;
  createdAt: number;
}

export interface AppDescriptor {
  appId: string;
  displayName: string;
  workspaceOwnerAllowed: boolean;
  participantOwnerAllowed: boolean;
  workspaceRoleCapabilities: readonly WorkspaceRoleCapabilityDescriptor[];
}

export type AppInstallationState = "enabled" | "disabled";

export type AppOwnerRef =
  | { kind: "workspace"; workspaceId: string }
  | { kind: "participant"; participant: ParticipantRef };

export interface AppInstallation {
  installationId: string;
  owner: AppOwnerRef;
  appId: string;
  state: AppInstallationState;
  installedAt: number;
  updatedAt: number;
}

export function participantKey(participant: ParticipantRef): string {
  return participant.kind === "human"
    ? `human:${participant.humanId}`
    : `personality_agent:${participant.personalityAgentId}`;
}

export function participantID(participant: ParticipantRef): string {
  return participant.kind === "human"
    ? participant.humanId
    : participant.personalityAgentId;
}

export function isWorkspaceInstallation(
  installation: AppInstallation,
  workspaceId: string,
): boolean {
  return (
    installation.owner.kind === "workspace" &&
    installation.owner.workspaceId === workspaceId
  );
}

/**
 * Stable identity of one `AppInstallationOwnerRef`. Owner equality is compared
 * through this key so a Participant owner never collapses into the Workspace
 * that happened to be selected when the installation was read.
 */
export function appOwnerKey(owner: AppOwnerRef): string {
  return owner.kind === "workspace"
    ? `workspace:${owner.workspaceId}`
    : `participant:${participantKey(owner.participant)}`;
}

export function isOwnedBy(
  installation: AppInstallation,
  owner: AppOwnerRef,
): boolean {
  return appOwnerKey(installation.owner) === appOwnerKey(owner);
}
