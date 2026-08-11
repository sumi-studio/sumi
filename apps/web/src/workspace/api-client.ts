import type {
  AppDescriptor,
  AppInstallation,
  AppInstallationState,
  AppOwnerRef,
  ParticipantRef,
  Workspace,
  WorkspaceInvite,
  WorkspaceInvitePreview,
  WorkspaceInviteRecord,
  WorkspaceMembership,
  WorkspaceRole,
  WorkspaceRoleInput,
} from "./model";
import { participantID } from "./model";

const REQUEST_TIMEOUT_MS = 15_000;

export class WorkspaceAPIError extends Error {
  readonly code: string;
  readonly status: number;

  constructor(code: string, status: number) {
    super(code);
    this.name = "WorkspaceAPIError";
    this.code = code;
    this.status = status;
  }
}

export interface WorkspaceControlClient {
  listWorkspaces(): Promise<Workspace[]>;
  createWorkspace(name: string): Promise<Workspace>;
  getWorkspace(workspaceId: string): Promise<Workspace>;
  updateWorkspace(workspaceId: string, name: string): Promise<Workspace>;
  transferOwnership(
    workspaceId: string,
    workspaceMemberId: string,
  ): Promise<Workspace>;
  listMembers(workspaceId: string): Promise<WorkspaceMembership[]>;
  leaveWorkspace(workspaceId: string): Promise<void>;
  removeMember(workspaceId: string, workspaceMemberId: string): Promise<void>;
  createInvite(workspaceId: string): Promise<WorkspaceInvite>;
  listInvites(workspaceId: string): Promise<WorkspaceInviteRecord[]>;
  revokeInvite(workspaceId: string, inviteId: string): Promise<void>;
  previewInvite(code: string): Promise<WorkspaceInvitePreview>;
  redeemInvite(code: string): Promise<WorkspaceMembership>;
  listRoles(workspaceId: string): Promise<WorkspaceRole[]>;
  createRole(
    workspaceId: string,
    input: WorkspaceRoleInput,
  ): Promise<WorkspaceRole>;
  updateRole(
    workspaceId: string,
    roleId: string,
    input: WorkspaceRoleInput,
  ): Promise<WorkspaceRole>;
  deleteRole(workspaceId: string, roleId: string): Promise<void>;
  setMemberRoles(
    workspaceId: string,
    workspaceMemberId: string,
    roleIds: string[],
  ): Promise<string[]>;
  listAppCatalog(): Promise<AppDescriptor[]>;
  // Lifecycle reads and installs carry the canonical AppInstallationOwnerRef.
  // Workspace and Participant owners share one operation; only the owner ref
  // differs, so no scope is ever inferred from the client's current Workspace.
  listInstallations(owner: AppOwnerRef): Promise<AppInstallation[]>;
  installApp(owner: AppOwnerRef, appId: string): Promise<AppInstallation>;
  setInstallationState(
    installationId: string,
    state: AppInstallationState,
  ): Promise<AppInstallation>;
  uninstallApp(installationId: string): Promise<void>;
}

type Fetcher = typeof fetch;

/**
 * Canonical same-origin Workspace/App control-plane adapter. Selection never
 * crosses this boundary: the server receives only exact resource identities.
 */
export class WorkspaceApiClient implements WorkspaceControlClient {
  private readonly fetcher: Fetcher;

  constructor(fetcher: Fetcher = globalThis.fetch.bind(globalThis)) {
    this.fetcher = fetcher;
  }

  async listWorkspaces(): Promise<Workspace[]> {
    const body = asRecord(await this.request("/workspaces"));
    return asArray(body.workspaces).map(parseWorkspace);
  }

  async createWorkspace(name: string): Promise<Workspace> {
    return parseWorkspace(
      await this.request("/workspaces", {
        method: "POST",
        body: { name },
      }),
    );
  }

  async getWorkspace(workspaceId: string): Promise<Workspace> {
    return parseWorkspace(
      await this.request(`/workspaces/${encodeURIComponent(workspaceId)}`),
    );
  }

  async updateWorkspace(workspaceId: string, name: string): Promise<Workspace> {
    return parseWorkspace(
      await this.request(`/workspaces/${encodeURIComponent(workspaceId)}`, {
        method: "PATCH",
        body: { name },
      }),
    );
  }

  async transferOwnership(
    workspaceId: string,
    workspaceMemberId: string,
  ): Promise<Workspace> {
    return parseWorkspace(
      await this.request(
        `/workspaces/${encodeURIComponent(workspaceId)}/owner`,
        {
          method: "PUT",
          body: { workspace_member_id: workspaceMemberId },
        },
      ),
    );
  }

  async listMembers(workspaceId: string): Promise<WorkspaceMembership[]> {
    const body = asRecord(
      await this.request(
        `/workspaces/${encodeURIComponent(workspaceId)}/members`,
      ),
    );
    return asArray(body.members).map(parseMembership);
  }

  async leaveWorkspace(workspaceId: string): Promise<void> {
    await this.request(
      `/workspaces/${encodeURIComponent(workspaceId)}/membership`,
      { method: "DELETE" },
    );
  }

  async removeMember(
    workspaceId: string,
    workspaceMemberId: string,
  ): Promise<void> {
    await this.request(
      `/workspaces/${encodeURIComponent(workspaceId)}/members/${encodeURIComponent(workspaceMemberId)}`,
      { method: "DELETE" },
    );
  }

  async createInvite(workspaceId: string): Promise<WorkspaceInvite> {
    return parseInvite(
      await this.request(
        `/workspaces/${encodeURIComponent(workspaceId)}/invites`,
        { method: "POST", body: {} },
      ),
    );
  }

  async listInvites(workspaceId: string): Promise<WorkspaceInviteRecord[]> {
    const response = asRecord(
      await this.request(
        `/workspaces/${encodeURIComponent(workspaceId)}/invites`,
      ),
    );
    return asArray(response.invites).map(parseInviteRecord);
  }

  async revokeInvite(workspaceId: string, inviteId: string): Promise<void> {
    await this.request(
      `/workspaces/${encodeURIComponent(workspaceId)}/invites/${encodeURIComponent(inviteId)}`,
      { method: "DELETE" },
    );
  }

  async previewInvite(code: string): Promise<WorkspaceInvitePreview> {
    const query = new URLSearchParams({ code });
    return parseInvitePreview(
      await this.request(`/workspace-invites/preview?${query}`),
    );
  }

  async redeemInvite(code: string): Promise<WorkspaceMembership> {
    return parseMembership(
      await this.request("/workspace-invites/redeem", {
        method: "POST",
        body: { code },
      }),
    );
  }

  async listRoles(workspaceId: string): Promise<WorkspaceRole[]> {
    const body = asRecord(
      await this.request(
        `/workspaces/${encodeURIComponent(workspaceId)}/roles`,
      ),
    );
    return asArray(body.roles).map(parseRole);
  }

  async createRole(
    workspaceId: string,
    input: WorkspaceRoleInput,
  ): Promise<WorkspaceRole> {
    return parseRole(
      await this.request(
        `/workspaces/${encodeURIComponent(workspaceId)}/roles`,
        { method: "POST", body: roleToWire(input) },
      ),
    );
  }

  async updateRole(
    workspaceId: string,
    roleId: string,
    input: WorkspaceRoleInput,
  ): Promise<WorkspaceRole> {
    return parseRole(
      await this.request(
        `/workspaces/${encodeURIComponent(workspaceId)}/roles/${encodeURIComponent(roleId)}`,
        { method: "PATCH", body: roleToWire(input) },
      ),
    );
  }

  async deleteRole(workspaceId: string, roleId: string): Promise<void> {
    await this.request(
      `/workspaces/${encodeURIComponent(workspaceId)}/roles/${encodeURIComponent(roleId)}`,
      { method: "DELETE" },
    );
  }

  async setMemberRoles(
    workspaceId: string,
    workspaceMemberId: string,
    roleIds: string[],
  ): Promise<string[]> {
    const body = asRecord(
      await this.request(
        `/workspaces/${encodeURIComponent(workspaceId)}/members/${encodeURIComponent(workspaceMemberId)}/roles`,
        { method: "PUT", body: { role_ids: roleIds } },
      ),
    );
    return asArray(body.role_ids).map(asString);
  }

  async listAppCatalog(): Promise<AppDescriptor[]> {
    const body = asRecord(await this.request("/apps/catalog"));
    return asArray(body.apps).map(parseAppDescriptor);
  }

  async listInstallations(owner: AppOwnerRef): Promise<AppInstallation[]> {
    const body = asRecord(
      await this.request(`/app-installations?${appOwnerQuery(owner)}`),
    );
    return asArray(body.installations).map(parseInstallation);
  }

  async installApp(
    owner: AppOwnerRef,
    appId: string,
  ): Promise<AppInstallation> {
    return parseInstallation(
      await this.request("/app-installations", {
        method: "POST",
        body: { owner: appOwnerToWire(owner), app_id: appId },
      }),
    );
  }

  async setInstallationState(
    installationId: string,
    state: AppInstallationState,
  ): Promise<AppInstallation> {
    return parseInstallation(
      await this.request(
        `/app-installations/${encodeURIComponent(installationId)}/state`,
        { method: "PUT", body: { state } },
      ),
    );
  }

  async uninstallApp(installationId: string): Promise<void> {
    await this.request(
      `/app-installations/${encodeURIComponent(installationId)}`,
      { method: "DELETE" },
    );
  }

  private async request(
    path: string,
    options: { method?: string; body?: unknown } = {},
  ): Promise<unknown> {
    const response = await this.fetcher(path, {
      method: options.method ?? "GET",
      credentials: "include",
      cache: "no-store",
      headers: {
        Accept: "application/json",
        ...(options.body === undefined
          ? {}
          : { "Content-Type": "application/json" }),
      },
      body:
        options.body === undefined ? undefined : JSON.stringify(options.body),
      signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    });
    if (!response.ok) {
      let code = "workspace_request_failed";
      try {
        const failure = asRecord(await response.json());
        if (typeof failure.error === "string") code = failure.error;
      } catch {
        // HTTP status remains the non-sensitive authoritative signal.
      }
      throw new WorkspaceAPIError(code, response.status);
    }
    if (response.status === 204) return null;
    return response.json() as Promise<unknown>;
  }
}

function parseWorkspace(value: unknown): Workspace {
  const wire = asRecord(value);
  return {
    workspaceId: asString(wire.workspace_id),
    name: asString(wire.name),
    ownerWorkspaceMemberId: asString(wire.owner_workspace_member_id),
    createdAt: asTimestamp(wire.created_at),
  };
}

function parseParticipant(value: unknown): ParticipantRef {
  const wire = asRecord(value);
  if (wire.kind === "human") {
    return { kind: "human", humanId: asString(wire.human_id) };
  }
  if (wire.kind === "personality_agent") {
    return {
      kind: "personality_agent",
      personalityAgentId: asString(wire.personality_agent_id),
    };
  }
  throw new Error("invalid participant reference");
}

function parseMembership(value: unknown): WorkspaceMembership {
  const wire = asRecord(value);
  return {
    workspaceMemberId: asString(wire.workspace_member_id),
    workspaceId: asString(wire.workspace_id),
    participant: parseParticipant(wire.participant),
    displayName: asString(wire.display_name),
    owner: asBoolean(wire.owner),
    roleIds: asArray(wire.role_ids).map(asString),
    joinedAt: asTimestamp(wire.joined_at),
    leftAt: wire.left_at == null ? null : asTimestamp(wire.left_at),
  };
}

function parseInvite(value: unknown): WorkspaceInvite {
  const wire = asRecord(value);
  return {
    inviteId: asString(wire.invite_id),
    workspaceId: asString(wire.workspace_id),
    code: asString(wire.code),
    expiresAt: asTimestamp(wire.expires_at),
    createdAt: asTimestamp(wire.created_at),
  };
}

function parseInviteRecord(value: unknown): WorkspaceInviteRecord {
  const wire = asRecord(value);
  return {
    inviteId: asString(wire.invite_id),
    workspaceId: asString(wire.workspace_id),
    expiresAt: asTimestamp(wire.expires_at),
    createdAt: asTimestamp(wire.created_at),
  };
}

function parseInvitePreview(value: unknown): WorkspaceInvitePreview {
  const wire = asRecord(value);
  return {
    workspaceId: asString(wire.workspace_id),
    workspaceName: asString(wire.workspace_name),
    expiresAt: asTimestamp(wire.expires_at),
  };
}

function roleToWire(input: WorkspaceRoleInput): Record<string, unknown> {
  return {
    name: input.name,
    ...(input.color === undefined ? {} : { color: input.color }),
    ...(input.position === undefined ? {} : { position: input.position }),
    permissions: input.permissions,
  };
}

function parseRole(value: unknown): WorkspaceRole {
  const wire = asRecord(value);
  return {
    roleId: asString(wire.role_id),
    workspaceId: asString(wire.workspace_id),
    name: asString(wire.name),
    ...(typeof wire.color === "string" ? { color: wire.color } : {}),
    position: asSafeInteger(wire.position),
    permissions: asArray(wire.permissions).map(asString),
    createdAt: asTimestamp(wire.created_at),
  };
}

function parseAppDescriptor(value: unknown): AppDescriptor {
  const wire = asRecord(value);
  return {
    appId: asString(wire.app_id),
    displayName: asString(wire.display_name),
    workspaceOwnerAllowed: asBoolean(wire.workspace_owner_allowed),
    participantOwnerAllowed: asBoolean(wire.participant_owner_allowed),
    workspaceRoleCapabilities: asArray(wire.workspace_role_capabilities).map(
      (capability) => {
        const capabilityWire = asRecord(capability);
        return {
          ref: asString(capabilityWire.ref),
          label: asString(capabilityWire.label),
        };
      },
    ),
  };
}

/**
 * Owner refs travel as `owner_kind`/`owner_id` (+ `participant_kind` for the
 * Participant variant) on reads and as the nested `AppOwnerRef` sum on install.
 * Both encodings name the owner exactly; neither is derived from UI state.
 */
function appOwnerQuery(owner: AppOwnerRef): URLSearchParams {
  if (owner.kind === "workspace") {
    return new URLSearchParams({
      owner_kind: "workspace",
      owner_id: owner.workspaceId,
    });
  }
  return new URLSearchParams({
    owner_kind: "participant",
    owner_id: participantID(owner.participant),
    participant_kind: owner.participant.kind,
  });
}

function participantToWire(participant: ParticipantRef): Record<string, string> {
  return participant.kind === "human"
    ? { kind: "human", human_id: participant.humanId }
    : {
        kind: "personality_agent",
        personality_agent_id: participant.personalityAgentId,
      };
}

function appOwnerToWire(owner: AppOwnerRef): Record<string, unknown> {
  return owner.kind === "workspace"
    ? { kind: "workspace", workspace_id: owner.workspaceId }
    : { kind: "participant", participant: participantToWire(owner.participant) };
}

function parseAppOwner(value: unknown): AppOwnerRef {
  const wire = asRecord(value);
  if (wire.kind === "workspace") {
    return { kind: "workspace", workspaceId: asString(wire.workspace_id) };
  }
  if (wire.kind === "participant") {
    return {
      kind: "participant",
      participant: parseParticipant(wire.participant),
    };
  }
  throw new Error("invalid app owner");
}

function parseInstallation(value: unknown): AppInstallation {
  const wire = asRecord(value);
  const state = wire.state;
  if (state !== "enabled" && state !== "disabled") {
    throw new Error("invalid app installation state");
  }
  return {
    installationId: asString(wire.installation_id),
    owner: parseAppOwner(wire.owner),
    appId: asString(wire.app_id),
    state,
    installedAt: asTimestamp(wire.installed_at),
    updatedAt: asTimestamp(wire.updated_at),
  };
}

function asRecord(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("invalid Workspace response");
  }
  return value as Record<string, unknown>;
}

function asArray(value: unknown): unknown[] {
  if (!Array.isArray(value)) throw new Error("invalid Workspace response");
  return value;
}

function asString(value: unknown): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error("invalid Workspace response");
  }
  return value;
}

function asBoolean(value: unknown): boolean {
  if (typeof value !== "boolean") throw new Error("invalid Workspace response");
  return value;
}

function asSafeInteger(value: unknown): number {
  if (!Number.isSafeInteger(value))
    throw new Error("invalid Workspace response");
  return value as number;
}

function asTimestamp(value: unknown): number {
  const parsed = Date.parse(asString(value));
  if (!Number.isFinite(parsed)) throw new Error("invalid Workspace timestamp");
  return parsed;
}
