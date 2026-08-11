import { create } from "zustand";
import {
  WorkspaceAPIError,
  WorkspaceApiClient,
  type WorkspaceControlClient,
} from "./api-client";
import type {
  AppDescriptor,
  AppInstallation,
  AppInstallationState,
  Workspace,
  WorkspaceInvite,
  WorkspaceInvitePreview,
  WorkspaceInviteRecord,
  WorkspaceInviteSecret,
  WorkspaceMembership,
  WorkspaceRole,
  WorkspaceRoleCapabilityRef,
  WorkspaceRoleInput,
} from "./model";
import {
  isWorkspaceInstallation,
  participantKey,
  WORKSPACE_PERMISSIONS,
} from "./model";

export type WorkspaceListStatus = "idle" | "loading" | "ready" | "error";
const INVITE_AUTHORITY_CONTRADICTION =
  "workspace_invites_forbidden_after_authority_refresh";
export type WorkspaceSelectionStatus =
  | "idle"
  | "loading"
  | "ready"
  | "invalid"
  | "error";

export interface WorkspaceControlState {
  sessionIdentity: string | null;
  listStatus: WorkspaceListStatus;
  selectionStatus: WorkspaceSelectionStatus;
  workspaces: Workspace[];
  selectedWorkspaceId: string | null;
  selectedWorkspace: Workspace | null;
  members: WorkspaceMembership[];
  roles: WorkspaceRole[];
  catalog: AppDescriptor[];
  installations: AppInstallation[];
  invites: WorkspaceInviteRecord[];
  createdInviteSecret: WorkspaceInviteSecret | null;
  errorCode: string | null;
  mutation: string | null;

  resetSession(identity: string | null): void;
  init(): Promise<void>;
  refreshWorkspaces(): Promise<void>;
  createWorkspace(name: string): Promise<Workspace>;
  selectWorkspace(workspaceId: string): Promise<void>;
  refreshSelectedWorkspace(): Promise<void>;
  clearSelection(): void;
  updateWorkspace(name: string): Promise<Workspace>;
  transferOwnership(workspaceMemberId: string): Promise<Workspace>;
  leaveWorkspace(): Promise<void>;
  removeMember(workspaceMemberId: string): Promise<void>;
  createInvite(): Promise<WorkspaceInvite>;
  clearCreatedInviteSecret(): void;
  revokeInvite(inviteId: string): Promise<void>;
  previewInvite(code: string): Promise<WorkspaceInvitePreview>;
  redeemInvite(code: string): Promise<WorkspaceMembership>;
  createRole(input: WorkspaceRoleInput): Promise<WorkspaceRole>;
  updateRole(roleId: string, input: WorkspaceRoleInput): Promise<WorkspaceRole>;
  deleteRole(roleId: string): Promise<void>;
  setMemberRoles(workspaceMemberId: string, roleIds: string[]): Promise<void>;
  installApp(appId: string): Promise<AppInstallation>;
  setInstallationState(
    installationId: string,
    state: AppInstallationState,
  ): Promise<AppInstallation>;
  uninstallApp(installationId: string): Promise<void>;
}

interface ScopeToken {
  sessionIdentity: string;
  workspaceId: string;
  generation: number;
}

function emptySelection(): Pick<
  WorkspaceControlState,
  | "selectionStatus"
  | "selectedWorkspaceId"
  | "selectedWorkspace"
  | "members"
  | "roles"
  | "catalog"
  | "installations"
  | "invites"
  | "createdInviteSecret"
> {
  return {
    selectionStatus: "idle",
    selectedWorkspaceId: null,
    selectedWorkspace: null,
    members: [],
    roles: [],
    catalog: [],
    installations: [],
    invites: [],
    createdInviteSecret: null,
  };
}

export function createWorkspaceControlStore(client: WorkspaceControlClient) {
  let sessionGeneration = 0;
  let selectionGeneration = 0;
  let listPromise: Promise<void> | null = null;
  let selectedLoad: { workspaceId: string; promise: Promise<void> } | null =
    null;

  return create<WorkspaceControlState>((set, get) => {
    const currentScope = (): ScopeToken => {
      const state = get();
      if (!state.sessionIdentity || !state.selectedWorkspaceId) {
        throw new Error("Workspace scope is not selected");
      }
      return {
        sessionIdentity: state.sessionIdentity,
        workspaceId: state.selectedWorkspaceId,
        generation: selectionGeneration,
      };
    };

    const isCurrentScope = (token: ScopeToken): boolean => {
      const state = get();
      return (
        state.sessionIdentity === token.sessionIdentity &&
        state.selectedWorkspaceId === token.workspaceId &&
        selectionGeneration === token.generation
      );
    };

    const beginMutation = (name: string): ScopeToken => {
      const token = currentScope();
      if (get().mutation)
        throw new Error("Workspace mutation is already running");
      set({ mutation: name, errorCode: null });
      return token;
    };

    const endMutation = (token: ScopeToken, error?: unknown): void => {
      if (!isCurrentScope(token)) return;
      set({
        mutation: null,
        errorCode: error ? errorCode(error) : null,
      });
    };

    const loadWorkspace = async (
      workspaceId: string,
      clear: boolean,
    ): Promise<void> => {
      const state = get();
      if (!state.sessionIdentity) return;
      const known = state.workspaces.find(
        (workspace) => workspace.workspaceId === workspaceId,
      );
      if (!known) {
        selectionGeneration += 1;
        selectedLoad = null;
        set({
          ...emptySelection(),
          selectedWorkspaceId: workspaceId,
          selectionStatus: "invalid",
          errorCode: "workspace_not_available",
        });
        return;
      }

      if (
        selectedLoad?.workspaceId === workspaceId &&
        get().selectionStatus === "loading"
      ) {
        return selectedLoad.promise;
      }

      const identity = state.sessionIdentity;
      const generation = ++selectionGeneration;
      const token: ScopeToken = {
        sessionIdentity: identity,
        workspaceId,
        generation,
      };
      set({
        ...(clear
          ? {
              selectedWorkspace: null,
              members: [],
              roles: [],
              catalog: [],
              installations: [],
              invites: [],
              createdInviteSecret: null,
            }
          : {}),
        selectedWorkspaceId: workspaceId,
        selectionStatus: "loading",
        errorCode: null,
        mutation: null,
      });

      const promise = Promise.all([
        client.getWorkspace(workspaceId),
        client.listMembers(workspaceId),
        client.listRoles(workspaceId),
        client.listAppCatalog(),
        client.listInstallations(workspaceId),
      ])
        .then(
          async ([
            workspace,
            initialMembers,
            initialRoles,
            catalog,
            installations,
          ]) => {
            if (!isCurrentScope(token)) return;
            let members = initialMembers;
            let roles = initialRoles;
            validateSelectedSnapshot(
              identity,
              workspaceId,
              workspace,
              members,
              roles,
              catalog,
              installations,
            );
            const ownMembership = exactHumanMembership(members, identity);
            const canManageMembers = effectiveWorkspacePermissions(
              ownMembership,
              roles,
            ).has("manage_members");
            let invites: WorkspaceInviteRecord[] = [];
            if (canManageMembers) {
              try {
                invites = await client.listInvites(workspaceId);
                validateInviteRecords(workspaceId, invites);
              } catch (error) {
                if (!isWorkspaceForbidden(error)) throw error;

                // The member/role snapshot and invite authorization are separate
                // HTTP transactions. A 403 can therefore mean authority changed
                // between them. Refresh that authority snapshot exactly once;
                // never turn an invite-subresource denial into Workspace removal.
                [members, roles] = await Promise.all([
                  client.listMembers(workspaceId),
                  client.listRoles(workspaceId),
                ]);
                validateSelectedSnapshot(
                  identity,
                  workspaceId,
                  workspace,
                  members,
                  roles,
                  catalog,
                  installations,
                );
                const refreshedMembership = exactHumanMembership(
                  members,
                  identity,
                );
                const stillCanManageMembers = effectiveWorkspacePermissions(
                  refreshedMembership,
                  roles,
                ).has("manage_members");
                if (stillCanManageMembers) {
                  try {
                    invites = await client.listInvites(workspaceId);
                    validateInviteRecords(workspaceId, invites);
                  } catch (refreshedError) {
                    if (!isWorkspaceForbidden(refreshedError)) {
                      throw refreshedError;
                    }
                    throw new Error(INVITE_AUTHORITY_CONTRADICTION);
                  }
                }
              }
            }
            if (!isCurrentScope(token)) return;
            set({
              selectedWorkspace: workspace,
              members,
              roles,
              catalog,
              installations,
              invites,
              selectionStatus: "ready",
              errorCode: null,
            });
          },
        )
        .catch((error: unknown) => {
          if (!isCurrentScope(token)) return;
          if (
            error instanceof Error &&
            error.message === INVITE_AUTHORITY_CONTRADICTION
          ) {
            set({
              invites: [],
              createdInviteSecret: null,
              selectionStatus: "error",
              errorCode: INVITE_AUTHORITY_CONTRADICTION,
              mutation: null,
            });
            return;
          }
          if (
            error instanceof WorkspaceAPIError &&
            (error.status === 403 || error.status === 404)
          ) {
            set((current) => ({
              ...emptySelection(),
              workspaces: current.workspaces.filter(
                (workspace) => workspace.workspaceId !== workspaceId,
              ),
              selectedWorkspaceId: workspaceId,
              selectionStatus: "invalid",
              errorCode: "workspace_not_available",
            }));
            return;
          }
          set({
            selectionStatus: "error",
            errorCode: errorCode(error),
          });
        })
        .finally(() => {
          if (selectedLoad?.promise === promise) selectedLoad = null;
        });
      selectedLoad = { workspaceId, promise };
      return promise;
    };

    const loadWorkspaces = async (force: boolean): Promise<void> => {
      const state = get();
      if (!state.sessionIdentity) return;
      if (!force && state.listStatus === "ready") return;
      if (!force && listPromise) return listPromise;
      const identity = state.sessionIdentity;
      const generation = sessionGeneration;
      set({ listStatus: "loading", errorCode: null });
      const promise = client
        .listWorkspaces()
        .then((workspaces) => {
          if (
            sessionGeneration !== generation ||
            get().sessionIdentity !== identity
          ) {
            return;
          }
          const selectedWorkspaceId = get().selectedWorkspaceId;
          const selectedStillVisible =
            selectedWorkspaceId === null ||
            workspaces.some(
              (workspace) => workspace.workspaceId === selectedWorkspaceId,
            );
          set({
            workspaces,
            listStatus: "ready",
            errorCode: null,
            ...(selectedStillVisible
              ? {}
              : {
                  ...emptySelection(),
                  selectedWorkspaceId,
                  selectionStatus: "invalid" as const,
                  errorCode: "workspace_not_available",
                }),
          });
        })
        .catch((error: unknown) => {
          if (
            sessionGeneration !== generation ||
            get().sessionIdentity !== identity
          ) {
            return;
          }
          set({ listStatus: "error", errorCode: errorCode(error) });
        })
        .finally(() => {
          if (listPromise === promise) listPromise = null;
        });
      listPromise = promise;
      return promise;
    };

    return {
      sessionIdentity: null,
      listStatus: "idle",
      workspaces: [],
      ...emptySelection(),
      errorCode: null,
      mutation: null,

      resetSession(identity) {
        if (identity === get().sessionIdentity) return;
        sessionGeneration += 1;
        selectionGeneration += 1;
        listPromise = null;
        selectedLoad = null;
        set({
          sessionIdentity: identity,
          listStatus: "idle",
          workspaces: [],
          ...emptySelection(),
          errorCode: null,
          mutation: null,
        });
      },

      init() {
        return loadWorkspaces(false);
      },

      refreshWorkspaces() {
        return loadWorkspaces(true);
      },

      async createWorkspace(name) {
        const trimmed = name.trim();
        const identity = get().sessionIdentity;
        if (!identity || !trimmed)
          throw new Error("Workspace name is required");
        if (get().mutation) {
          throw new Error("Workspace mutation is already running");
        }
        const generation = sessionGeneration;
        set({ mutation: "create_workspace", errorCode: null });
        try {
          const workspace = await client.createWorkspace(trimmed);
          if (
            sessionGeneration !== generation ||
            get().sessionIdentity !== identity
          ) {
            throw new Error("Workspace session changed during creation");
          }
          set((state) => ({
            workspaces: state.workspaces.some(
              (entry) => entry.workspaceId === workspace.workspaceId,
            )
              ? state.workspaces.map((entry) =>
                  entry.workspaceId === workspace.workspaceId
                    ? workspace
                    : entry,
                )
              : [...state.workspaces, workspace],
            listStatus: "ready",
            mutation: null,
          }));
          return workspace;
        } catch (error) {
          if (
            sessionGeneration === generation &&
            get().sessionIdentity === identity
          ) {
            set({ mutation: null, errorCode: errorCode(error) });
          }
          throw error;
        }
      },

      selectWorkspace(workspaceId) {
        return loadWorkspace(workspaceId, true);
      },

      refreshSelectedWorkspace() {
        const workspaceId = get().selectedWorkspaceId;
        return workspaceId
          ? loadWorkspace(workspaceId, false)
          : Promise.resolve();
      },

      clearSelection() {
        selectionGeneration += 1;
        selectedLoad = null;
        set({ ...emptySelection(), mutation: null, errorCode: null });
      },

      async updateWorkspace(name) {
        const token = beginMutation("update_workspace");
        try {
          const workspace = await client.updateWorkspace(
            token.workspaceId,
            name.trim(),
          );
          if (!isCurrentScope(token)) return workspace;
          if (workspace.workspaceId !== token.workspaceId) {
            throw new Error("Workspace update returned a different Workspace");
          }
          set((state) => ({
            selectedWorkspace: workspace,
            workspaces: state.workspaces.map((entry) =>
              entry.workspaceId === token.workspaceId ? workspace : entry,
            ),
          }));
          endMutation(token);
          return workspace;
        } catch (error) {
          endMutation(token, error);
          throw error;
        }
      },

      async transferOwnership(workspaceMemberId) {
        const token = beginMutation("transfer_ownership");
        const target = get().members.find(
          (member) => member.workspaceMemberId === workspaceMemberId,
        );
        if (!target || target.leftAt !== null) {
          const error = new Error("Workspace membership is not active");
          endMutation(token, error);
          throw error;
        }
        try {
          const workspace = await client.transferOwnership(
            token.workspaceId,
            workspaceMemberId,
          );
          if (!isCurrentScope(token)) return workspace;
          if (
            workspace.workspaceId !== token.workspaceId ||
            workspace.ownerWorkspaceMemberId !== workspaceMemberId
          ) {
            throw new Error("Ownership response does not match intent");
          }
          set((state) => ({
            selectedWorkspace: workspace,
            workspaces: state.workspaces.map((entry) =>
              entry.workspaceId === token.workspaceId ? workspace : entry,
            ),
            members: state.members.map((member) => ({
              ...member,
              owner: member.workspaceMemberId === workspaceMemberId,
            })),
          }));
          endMutation(token);
          return workspace;
        } catch (error) {
          endMutation(token, error);
          throw error;
        }
      },

      async leaveWorkspace() {
        const token = beginMutation("leave_workspace");
        try {
          await client.leaveWorkspace(token.workspaceId);
          if (!isCurrentScope(token)) return;
          selectionGeneration += 1;
          set((state) => ({
            workspaces: state.workspaces.filter(
              (workspace) => workspace.workspaceId !== token.workspaceId,
            ),
            ...emptySelection(),
            mutation: null,
            errorCode: null,
          }));
        } catch (error) {
          endMutation(token, error);
          throw error;
        }
      },

      async removeMember(workspaceMemberId) {
        const token = beginMutation("remove_member");
        try {
          await client.removeMember(token.workspaceId, workspaceMemberId);
          if (!isCurrentScope(token)) return;
          set((state) => ({
            members: state.members.filter(
              (member) => member.workspaceMemberId !== workspaceMemberId,
            ),
          }));
          endMutation(token);
        } catch (error) {
          endMutation(token, error);
          throw error;
        }
      },

      async createInvite() {
        const token = beginMutation("create_invite");
        try {
          const invite = await client.createInvite(token.workspaceId);
          if (!isCurrentScope(token)) return invite;
          if (invite.workspaceId !== token.workspaceId) {
            throw new Error("Invite belongs to a different Workspace");
          }
          const { code, ...record } = invite;
          set((state) => ({
            invites: [
              ...state.invites.filter(
                (candidate) => candidate.inviteId !== record.inviteId,
              ),
              record,
            ],
            createdInviteSecret: { inviteId: record.inviteId, code },
          }));
          endMutation(token);
          return invite;
        } catch (error) {
          endMutation(token, error);
          throw error;
        }
      },

      async revokeInvite(inviteId) {
        const token = beginMutation("revoke_invite");
        try {
          await client.revokeInvite(token.workspaceId, inviteId);
          if (!isCurrentScope(token)) return;
          set((state) => ({
            invites: state.invites.filter(
              (candidate) => candidate.inviteId !== inviteId,
            ),
            createdInviteSecret:
              state.createdInviteSecret?.inviteId === inviteId
                ? null
                : state.createdInviteSecret,
          }));
          endMutation(token);
        } catch (error) {
          endMutation(token, error);
          throw error;
        }
      },

      clearCreatedInviteSecret() {
        set({ createdInviteSecret: null });
      },

      previewInvite(code) {
        if (!get().sessionIdentity) {
          return Promise.reject(new Error("Workspace session is not bound"));
        }
        const trimmed = code.trim();
        return trimmed
          ? client.previewInvite(trimmed)
          : Promise.reject(new Error("Invite code is required"));
      },

      async redeemInvite(code) {
        const identity = get().sessionIdentity;
        const generation = sessionGeneration;
        if (!identity) throw new Error("Workspace session is not bound");
        const trimmed = code.trim();
        if (!trimmed) throw new Error("Invite code is required");
        if (get().mutation) {
          throw new Error("Workspace mutation is already running");
        }
        set({ mutation: "redeem_invite", errorCode: null });
        try {
          const membership = await client.redeemInvite(trimmed);
          if (
            sessionGeneration !== generation ||
            get().sessionIdentity !== identity
          ) {
            throw new Error(
              "Workspace session changed during invite redemption",
            );
          }
          await loadWorkspaces(true);
          if (
            sessionGeneration === generation &&
            get().sessionIdentity === identity
          ) {
            set({ mutation: null, errorCode: null });
          }
          return membership;
        } catch (error) {
          if (
            sessionGeneration === generation &&
            get().sessionIdentity === identity
          ) {
            set({ mutation: null, errorCode: errorCode(error) });
          }
          throw error;
        }
      },

      async createRole(input) {
        const token = beginMutation("create_role");
        try {
          const role = await client.createRole(token.workspaceId, input);
          if (!isCurrentScope(token)) return role;
          validateRoleScope(token.workspaceId, role);
          set((state) => ({ roles: [...state.roles, role] }));
          endMutation(token);
          return role;
        } catch (error) {
          endMutation(token, error);
          throw error;
        }
      },

      async updateRole(roleId, input) {
        const token = beginMutation("update_role");
        try {
          const role = await client.updateRole(
            token.workspaceId,
            roleId,
            input,
          );
          if (!isCurrentScope(token)) return role;
          validateRoleScope(token.workspaceId, role);
          if (role.roleId !== roleId) {
            throw new Error("Role update returned a different role");
          }
          set((state) => ({
            roles: state.roles.map((entry) =>
              entry.roleId === roleId ? role : entry,
            ),
          }));
          endMutation(token);
          return role;
        } catch (error) {
          endMutation(token, error);
          throw error;
        }
      },

      async deleteRole(roleId) {
        const token = beginMutation("delete_role");
        try {
          await client.deleteRole(token.workspaceId, roleId);
          if (!isCurrentScope(token)) return;
          set((state) => ({
            roles: state.roles.filter((role) => role.roleId !== roleId),
            members: state.members.map((member) => ({
              ...member,
              roleIds: member.roleIds.filter((id) => id !== roleId),
            })),
          }));
          endMutation(token);
        } catch (error) {
          endMutation(token, error);
          throw error;
        }
      },

      async setMemberRoles(workspaceMemberId, roleIds) {
        const token = beginMutation("set_member_roles");
        const member = get().members.find(
          (entry) => entry.workspaceMemberId === workspaceMemberId,
        );
        if (!member) {
          endMutation(token, new Error("Workspace membership is not active"));
          throw new Error("Workspace membership is not active");
        }
        const knownRoles = new Set(get().roles.map((role) => role.roleId));
        if (roleIds.some((roleId) => !knownRoles.has(roleId))) {
          endMutation(token, new Error("Unknown Workspace role"));
          throw new Error("Unknown Workspace role");
        }
        try {
          const stored = await client.setMemberRoles(
            token.workspaceId,
            workspaceMemberId,
            roleIds,
          );
          if (!isCurrentScope(token)) return;
          set((state) => ({
            members: state.members.map((entry) =>
              entry.workspaceMemberId === workspaceMemberId
                ? { ...entry, roleIds: stored }
                : entry,
            ),
          }));
          endMutation(token);
        } catch (error) {
          endMutation(token, error);
          throw error;
        }
      },

      async installApp(appId) {
        const token = beginMutation("install_app");
        if (
          get().installations.some(
            (installation) => installation.appId === appId,
          )
        ) {
          endMutation(token, new Error("App is already installed"));
          throw new Error("App is already installed");
        }
        try {
          const installation = await client.installApp(
            token.workspaceId,
            appId,
          );
          if (!isCurrentScope(token)) return installation;
          validateInstallation(token.workspaceId, installation, appId);
          set((state) => ({
            installations: [...state.installations, installation],
          }));
          endMutation(token);
          return installation;
        } catch (error) {
          endMutation(token, error);
          throw error;
        }
      },

      async setInstallationState(installationId, state) {
        const token = beginMutation(`set_installation_${state}`);
        const current = get().installations.find(
          (installation) => installation.installationId === installationId,
        );
        if (!current) {
          endMutation(token, new Error("App installation is not active"));
          throw new Error("App installation is not active");
        }
        try {
          const installation = await client.setInstallationState(
            installationId,
            state,
          );
          if (!isCurrentScope(token)) return installation;
          validateInstallation(token.workspaceId, installation, current.appId);
          if (
            installation.installationId !== installationId ||
            installation.state !== state
          ) {
            throw new Error("App lifecycle response does not match intent");
          }
          set((workspaceState) => ({
            installations: workspaceState.installations.map((entry) =>
              entry.installationId === installationId ? installation : entry,
            ),
          }));
          endMutation(token);
          return installation;
        } catch (error) {
          endMutation(token, error);
          throw error;
        }
      },

      async uninstallApp(installationId) {
        const token = beginMutation("uninstall_app");
        if (
          !get().installations.some(
            (installation) => installation.installationId === installationId,
          )
        ) {
          endMutation(token, new Error("App installation is not active"));
          throw new Error("App installation is not active");
        }
        try {
          await client.uninstallApp(installationId);
          if (!isCurrentScope(token)) return;
          set((state) => ({
            installations: state.installations.filter(
              (installation) => installation.installationId !== installationId,
            ),
          }));
          endMutation(token);
        } catch (error) {
          endMutation(token, error);
          throw error;
        }
      },
    };
  });
}

function validateSelectedSnapshot(
  humanId: string,
  workspaceId: string,
  workspace: Workspace,
  members: WorkspaceMembership[],
  roles: WorkspaceRole[],
  catalog: AppDescriptor[],
  installations: AppInstallation[],
): void {
  if (workspace.workspaceId !== workspaceId) {
    throw new Error("Workspace response crossed scope");
  }
  if (
    members.some(
      (membership) =>
        membership.workspaceId !== workspaceId || membership.leftAt !== null,
    )
  ) {
    throw new Error("Workspace membership response crossed scope");
  }
  if (!exactHumanMembership(members, humanId)) {
    throw new Error("Current Human membership is missing or ambiguous");
  }
  if (
    new Set(members.map((membership) => membership.workspaceMemberId)).size !==
      members.length ||
    new Set(members.map((membership) => participantKey(membership.participant)))
      .size !== members.length
  ) {
    throw new Error("Workspace membership response is ambiguous");
  }
  if (
    members.filter(
      (membership) =>
        membership.workspaceMemberId === workspace.ownerWorkspaceMemberId &&
        membership.owner,
    ).length !== 1
  ) {
    throw new Error("Workspace owner membership is missing or ambiguous");
  }
  if (new Set(roles.map((role) => role.roleId)).size !== roles.length) {
    throw new Error("Workspace role response is ambiguous");
  }
  for (const role of roles) validateRoleScope(workspaceId, role);
  const roleIds = new Set(roles.map((role) => role.roleId));
  for (const membership of members) {
    if (
      new Set(membership.roleIds).size !== membership.roleIds.length ||
      membership.roleIds.some((roleId) => !roleIds.has(roleId))
    ) {
      throw new Error("Workspace role assignment response is invalid");
    }
  }
  if (new Set(catalog.map((app) => app.appId)).size !== catalog.length) {
    throw new Error("App catalog response is ambiguous");
  }
  const appIds = new Set(catalog.map((app) => app.appId));
  if (
    new Set(installations.map((installation) => installation.installationId))
      .size !== installations.length ||
    installations.some((installation) => !appIds.has(installation.appId))
  ) {
    throw new Error("App installation response is invalid");
  }
  for (const installation of installations) {
    validateInstallation(workspaceId, installation, installation.appId);
  }
}

function validateRoleScope(workspaceId: string, role: WorkspaceRole): void {
  if (role.workspaceId !== workspaceId) {
    throw new Error("Workspace role response crossed scope");
  }
}

function validateInviteRecords(
  workspaceId: string,
  invites: readonly WorkspaceInviteRecord[],
): void {
  if (
    new Set(invites.map((invite) => invite.inviteId)).size !== invites.length ||
    invites.some((invite) => invite.workspaceId !== workspaceId)
  ) {
    throw new Error("Workspace invite response is invalid");
  }
}

function validateInstallation(
  workspaceId: string,
  installation: AppInstallation,
  appId: string,
): void {
  if (
    installation.appId !== appId ||
    !isWorkspaceInstallation(installation, workspaceId)
  ) {
    throw new Error("App installation response crossed scope");
  }
}

function errorCode(error: unknown): string {
  if (error instanceof WorkspaceAPIError) return error.code;
  return error instanceof Error && error.message
    ? error.message
    : "workspace_request_failed";
}

function isWorkspaceForbidden(error: unknown): boolean {
  return error instanceof WorkspaceAPIError && error.status === 403;
}

export function installationForApp(
  installations: readonly AppInstallation[],
  appId: string,
): AppInstallation | null {
  const matches = installations.filter(
    (installation) => installation.appId === appId,
  );
  return matches.length === 1 ? (matches[0] ?? null) : null;
}

export function exactHumanMembership(
  memberships: readonly WorkspaceMembership[],
  humanId: string | null | undefined,
): WorkspaceMembership | null {
  if (!humanId) return null;
  const matches = memberships.filter(
    (membership) =>
      membership.leftAt === null &&
      membership.participant.kind === "human" &&
      membership.participant.humanId === humanId,
  );
  return matches.length === 1 ? (matches[0] ?? null) : null;
}

export function effectiveWorkspacePermissions(
  membership: WorkspaceMembership | null | undefined,
  roles: readonly WorkspaceRole[],
): ReadonlySet<WorkspaceRoleCapabilityRef> {
  if (!membership) return new Set();
  if (membership.owner) return new Set(WORKSPACE_PERMISSIONS);
  const assigned = new Set(membership.roleIds);
  return new Set(
    roles
      .filter((role) => assigned.has(role.roleId))
      .flatMap((role) => role.permissions),
  );
}

/**
 * UI affordances read authority from the canonical Workspace projection.
 * Server-side commit authorization remains authoritative for every mutation.
 */
export function useCurrentWorkspacePermission(
  permission: WorkspaceRoleCapabilityRef,
): boolean {
  return useWorkspaceControl((state) => {
    const membership = exactHumanMembership(
      state.members,
      state.sessionIdentity,
    );
    return (
      membership?.owner === true ||
      effectiveWorkspacePermissions(membership, state.roles).has(permission)
    );
  });
}

export const useWorkspaceControl = createWorkspaceControlStore(
  new WorkspaceApiClient(),
);

export function bindWorkspaceSessionIdentity(identity: string | null): void {
  useWorkspaceControl.getState().resetSession(identity);
}

export function getWorkspaceSessionIdentity(): string | null {
  return useWorkspaceControl.getState().sessionIdentity;
}
