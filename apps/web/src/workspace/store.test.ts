import { describe, expect, it, vi } from "vitest";
import { WorkspaceAPIError, type WorkspaceControlClient } from "./api-client";
import type {
  AppDescriptor,
  AppInstallation,
  Workspace,
  WorkspaceInvitePreview,
  WorkspaceMembership,
  WorkspaceRole,
} from "./model";
import {
  createWorkspaceControlStore,
  effectiveWorkspacePermissions,
  exactHumanMembership,
  installationForApp,
} from "./store";

const WORKSPACE_A_ID = "0198f0f4-9b72-7000-8000-000000000101";
const WORKSPACE_B_ID = "0198f0f4-9b72-7000-8000-000000000102";
const WORKSPACE_C_ID = "0198f0f4-9b72-7000-8000-000000000103";
const MEMBER_A_ID = "0198f0f4-9b72-7000-8000-000000000111";
const MEMBER_B_ID = "0198f0f4-9b72-7000-8000-000000000112";
const MEMBER_C_ID = "0198f0f4-9b72-7000-8000-000000000113";
const HUMAN_ID = "0198f0f4-9b72-7000-8000-000000000121";
const ROLE_A_ID = "0198f0f4-9b72-7000-8000-000000000131";
const ROLE_B_ID = "0198f0f4-9b72-7000-8000-000000000132";
const INSTALLATION_ID = "0198f0f4-9b72-7000-8000-000000000141";
const INSTALLATION_B_ID = "0198f0f4-9b72-7000-8000-000000000142";
const APP_ID = "messaging";
const INVITE_CODE = "r".repeat(43);

const WORKSPACE_A: Workspace = {
  workspaceId: WORKSPACE_A_ID,
  name: "Sumi Atelier",
  ownerWorkspaceMemberId: MEMBER_A_ID,
  createdAt: Date.parse("2026-08-10T06:01:02.345Z"),
};

const WORKSPACE_B: Workspace = {
  workspaceId: WORKSPACE_B_ID,
  name: "Night Studio",
  ownerWorkspaceMemberId: MEMBER_B_ID,
  createdAt: Date.parse("2026-08-10T06:02:03.456Z"),
};

const WORKSPACE_C: Workspace = {
  workspaceId: WORKSPACE_C_ID,
  name: "Moon Archive",
  ownerWorkspaceMemberId: MEMBER_C_ID,
  createdAt: Date.parse("2026-08-10T06:03:04.567Z"),
};

const MEMBER_A: WorkspaceMembership = {
  workspaceMemberId: MEMBER_A_ID,
  workspaceId: WORKSPACE_A_ID,
  displayName: "Yohaku",
  participant: { kind: "human", humanId: HUMAN_ID },
  owner: true,
  roleIds: [],
  joinedAt: Date.parse("2026-08-10T06:01:02.678Z"),
  leftAt: null,
};

const MEMBER_B: WorkspaceMembership = {
  workspaceMemberId: MEMBER_B_ID,
  workspaceId: WORKSPACE_B_ID,
  displayName: "Yohaku",
  participant: { kind: "human", humanId: HUMAN_ID },
  owner: true,
  roleIds: [ROLE_B_ID],
  joinedAt: Date.parse("2026-08-10T06:02:03.789Z"),
  leftAt: null,
};

const ROLE_A: WorkspaceRole = {
  roleId: ROLE_A_ID,
  workspaceId: WORKSPACE_A_ID,
  name: "Curator",
  color: "#4a6670",
  position: 600,
  permissions: ["app.messaging.manage_channels"],
  createdAt: Date.parse("2026-08-10T06:05:06.789Z"),
};

const ROLE_B: WorkspaceRole = {
  roleId: ROLE_B_ID,
  workspaceId: WORKSPACE_B_ID,
  name: "Host",
  color: "#6b5578",
  position: 700,
  permissions: ["manage_members"],
  createdAt: Date.parse("2026-08-10T06:06:07.890Z"),
};

const APP: AppDescriptor = {
  appId: APP_ID,
  displayName: "Messaging",
  workspaceOwnerAllowed: true,
  participantOwnerAllowed: false,
  workspaceRoleCapabilities: [
    { ref: "app.messaging.manage_channels", label: "Manage channels" },
  ],
};

describe("Workspace control store", () => {
  it("represents zero Workspace memberships as a ready unselected state", async () => {
    const listWorkspaces = vi.fn(async () => [] as Workspace[]);
    const store = createWorkspaceControlStore(clientWith({ listWorkspaces }));

    store.getState().resetSession(HUMAN_ID);
    await store.getState().init();

    expect(listWorkspaces).toHaveBeenCalledOnce();
    expect(store.getState()).toMatchObject({
      sessionIdentity: HUMAN_ID,
      listStatus: "ready",
      selectionStatus: "idle",
      workspaces: [],
      selectedWorkspaceId: null,
      selectedWorkspace: null,
      members: [],
      roles: [],
      catalog: [],
      installations: [],
      errorCode: null,
    });
  });

  it("creates and selects Workspaces only through explicit user actions", async () => {
    const getWorkspace = vi.fn(async (workspaceId: string) => {
      if (workspaceId === WORKSPACE_B_ID) return WORKSPACE_B;
      throw new Error(`unexpected Workspace ${workspaceId}`);
    });
    const createWorkspace = vi.fn(async () => WORKSPACE_C);
    const store = createWorkspaceControlStore(
      clientWith({
        listWorkspaces: async () => [WORKSPACE_A, WORKSPACE_B],
        createWorkspace,
        getWorkspace,
        listMembers: async () => [MEMBER_B],
        listRoles: async () => [ROLE_B],
        listAppCatalog: async () => [APP],
        listInstallations: async () => [],
      }),
    );

    store.getState().resetSession(HUMAN_ID);
    await store.getState().init();

    expect(getWorkspace).not.toHaveBeenCalled();
    expect(store.getState().workspaces).toEqual([WORKSPACE_A, WORKSPACE_B]);
    expect(store.getState().selectedWorkspaceId).toBeNull();

    await expect(
      store.getState().createWorkspace("  Moon Archive  "),
    ).resolves.toBe(WORKSPACE_C);
    expect(createWorkspace).toHaveBeenCalledWith("Moon Archive");
    expect(store.getState().workspaces).toEqual([
      WORKSPACE_A,
      WORKSPACE_B,
      WORKSPACE_C,
    ]);
    expect(store.getState().selectedWorkspaceId).toBeNull();
    expect(getWorkspace).not.toHaveBeenCalled();

    await store.getState().selectWorkspace(WORKSPACE_B_ID);

    expect(getWorkspace).toHaveBeenCalledWith(WORKSPACE_B_ID);
    expect(store.getState()).toMatchObject({
      selectionStatus: "ready",
      selectedWorkspaceId: WORKSPACE_B_ID,
      selectedWorkspace: WORKSPACE_B,
      members: [MEMBER_B],
      roles: [ROLE_B],
      catalog: [APP],
      installations: [],
      errorCode: null,
    });
  });

  it.each([
    403, 404,
  ])("marks a stale selection invalid after a %i control-plane response", async (status) => {
    const store = createWorkspaceControlStore(
      clientWith({
        listWorkspaces: async () => [WORKSPACE_A],
        getWorkspace: async () => {
          throw new WorkspaceAPIError(
            status === 403 ? "forbidden" : "not_found",
            status,
          );
        },
        listMembers: async () => [MEMBER_A],
        listRoles: async () => [ROLE_A],
        listAppCatalog: async () => [APP],
        listInstallations: async () => [
          installation(WORKSPACE_A_ID, "enabled", "2026-08-10T06:07:08.901Z"),
        ],
      }),
    );
    store.getState().resetSession(HUMAN_ID);
    await store.getState().init();

    await store.getState().selectWorkspace(WORKSPACE_A_ID);

    expect(store.getState()).toMatchObject({
      listStatus: "ready",
      selectionStatus: "invalid",
      workspaces: [],
      selectedWorkspaceId: WORKSPACE_A_ID,
      selectedWorkspace: null,
      members: [],
      roles: [],
      catalog: [],
      installations: [],
      errorCode: "workspace_not_available",
      mutation: null,
    });
  });

  it.each([
    ["missing", []],
    [
      "ambiguous",
      [
        MEMBER_A,
        {
          ...MEMBER_A,
          workspaceMemberId: "0198f0f4-9b72-7000-8000-000000000114",
        },
      ],
    ],
  ] as const)("fails closed when the current Human membership is %s", async (_condition, memberships) => {
    const store = createWorkspaceControlStore(
      clientWith({
        listWorkspaces: async () => [WORKSPACE_A],
        getWorkspace: async () => WORKSPACE_A,
        listMembers: async () => [...memberships],
        listRoles: async () => [ROLE_A],
        listAppCatalog: async () => [APP],
        listInstallations: async () => [
          installation(WORKSPACE_A_ID, "enabled", "2026-08-10T06:07:08.901Z"),
        ],
      }),
    );
    store.getState().resetSession(HUMAN_ID);
    await store.getState().init();

    await store.getState().selectWorkspace(WORKSPACE_A_ID);

    expect(store.getState()).toMatchObject({
      selectionStatus: "error",
      selectedWorkspaceId: WORKSPACE_A_ID,
      selectedWorkspace: null,
      members: [],
      roles: [],
      catalog: [],
      installations: [],
      errorCode: "Current Human membership is missing or ambiguous",
    });
  });

  it("refreshes authority once and disables invite controls after a raced 403", async () => {
    const ownerMembership: WorkspaceMembership = {
      ...MEMBER_A,
      participant: {
        kind: "human",
        humanId: "0198f0f4-9b72-7000-8000-000000000122",
      },
    };
    const delegatedMembership: WorkspaceMembership = {
      ...MEMBER_A,
      workspaceMemberId: "0198f0f4-9b72-7000-8000-000000000115",
      participant: { kind: "human", humanId: HUMAN_ID },
      owner: false,
      roleIds: [ROLE_A_ID],
    };
    const revokedMembership = { ...delegatedMembership, roleIds: [] };
    const manageMembersRole: WorkspaceRole = {
      ...ROLE_A,
      permissions: ["manage_members"],
    };
    let memberRead = 0;
    const listMembers = vi.fn(async () => {
      memberRead += 1;
      return memberRead === 1
        ? [ownerMembership, delegatedMembership]
        : [ownerMembership, revokedMembership];
    });
    const listRoles = vi.fn(async () => [manageMembersRole]);
    const listInvites = vi.fn(async () => {
      throw new WorkspaceAPIError("forbidden", 403);
    });
    const store = createWorkspaceControlStore(
      clientWith({
        listWorkspaces: async () => [WORKSPACE_A],
        getWorkspace: async () => WORKSPACE_A,
        listMembers,
        listRoles,
        listInvites,
        listAppCatalog: async () => [],
        listInstallations: async () => [],
      }),
    );
    store.getState().resetSession(HUMAN_ID);
    await store.getState().init();

    await store.getState().selectWorkspace(WORKSPACE_A_ID);

    expect(listMembers).toHaveBeenCalledTimes(2);
    expect(listRoles).toHaveBeenCalledTimes(2);
    expect(listInvites).toHaveBeenCalledOnce();
    expect(store.getState()).toMatchObject({
      selectionStatus: "ready",
      selectedWorkspace: WORKSPACE_A,
      members: [ownerMembership, revokedMembership],
      roles: [manageMembersRole],
      invites: [],
      errorCode: null,
    });
    expect(
      effectiveWorkspacePermissions(
        exactHumanMembership(store.getState().members, HUMAN_ID),
        store.getState().roles,
      ).has("manage_members"),
    ).toBe(false);
  });

  it("fails the invite subresource without removing a Workspace after a persistent 403", async () => {
    const ownerMembership: WorkspaceMembership = {
      ...MEMBER_A,
      participant: {
        kind: "human",
        humanId: "0198f0f4-9b72-7000-8000-000000000122",
      },
    };
    const delegatedMembership: WorkspaceMembership = {
      ...MEMBER_A,
      workspaceMemberId: "0198f0f4-9b72-7000-8000-000000000115",
      participant: { kind: "human", humanId: HUMAN_ID },
      owner: false,
      roleIds: [ROLE_A_ID],
    };
    const manageMembersRole: WorkspaceRole = {
      ...ROLE_A,
      permissions: ["manage_members"],
    };
    const listMembers = vi.fn(async () => [
      ownerMembership,
      delegatedMembership,
    ]);
    const listRoles = vi.fn(async () => [manageMembersRole]);
    const listInvites = vi.fn(async () => {
      throw new WorkspaceAPIError("forbidden", 403);
    });
    const store = createWorkspaceControlStore(
      clientWith({
        listWorkspaces: async () => [WORKSPACE_A],
        getWorkspace: async () => WORKSPACE_A,
        listMembers,
        listRoles,
        listInvites,
        listAppCatalog: async () => [],
        listInstallations: async () => [],
      }),
    );
    store.getState().resetSession(HUMAN_ID);
    await store.getState().init();

    await store.getState().selectWorkspace(WORKSPACE_A_ID);

    expect(listMembers).toHaveBeenCalledTimes(2);
    expect(listRoles).toHaveBeenCalledTimes(2);
    expect(listInvites).toHaveBeenCalledTimes(2);
    expect(store.getState()).toMatchObject({
      selectionStatus: "error",
      selectedWorkspaceId: WORKSPACE_A_ID,
      selectedWorkspace: null,
      invites: [],
      errorCode: "workspace_invites_forbidden_after_authority_refresh",
      workspaces: [WORKSPACE_A],
    });
  });

  it("previews and redeems an invite without implicitly selecting its Workspace", async () => {
    const preview: WorkspaceInvitePreview = {
      workspaceId: WORKSPACE_A_ID,
      workspaceName: WORKSPACE_A.name,
      expiresAt: Date.parse("2026-08-11T07:08:09.012Z"),
    };
    let listCall = 0;
    const listWorkspaces = vi.fn(async () => {
      listCall += 1;
      return listCall === 1 ? [] : [WORKSPACE_A];
    });
    const previewInvite = vi.fn(async () => preview);
    const redeemInvite = vi.fn(async () => MEMBER_A);
    const store = createWorkspaceControlStore(
      clientWith({ listWorkspaces, previewInvite, redeemInvite }),
    );
    store.getState().resetSession(HUMAN_ID);
    await store.getState().init();

    await expect(
      store.getState().previewInvite(`  ${INVITE_CODE}  `),
    ).resolves.toBe(preview);
    await expect(
      store.getState().redeemInvite(`  ${INVITE_CODE}  `),
    ).resolves.toBe(MEMBER_A);

    expect(previewInvite).toHaveBeenCalledWith(INVITE_CODE);
    expect(redeemInvite).toHaveBeenCalledWith(INVITE_CODE);
    expect(listWorkspaces).toHaveBeenCalledTimes(2);
    expect(store.getState()).toMatchObject({
      listStatus: "ready",
      workspaces: [WORKSPACE_A],
      selectionStatus: "idle",
      selectedWorkspaceId: null,
      selectedWorkspace: null,
      mutation: null,
      errorCode: null,
    });
  });

  it("keeps invitation identity while clearing the one-time secret", async () => {
    const existing = {
      inviteId: "0198f0f4-9b72-7000-8000-000000000150",
      workspaceId: WORKSPACE_A_ID,
      expiresAt: Date.parse("2026-08-11T05:01:02.345Z"),
      createdAt: Date.parse("2026-08-10T05:01:02.345Z"),
    };
    const invite = {
      inviteId: "0198f0f4-9b72-7000-8000-000000000151",
      workspaceId: WORKSPACE_A_ID,
      code: INVITE_CODE,
      expiresAt: Date.parse("2026-08-11T06:01:02.345Z"),
      createdAt: Date.parse("2026-08-10T06:01:02.345Z"),
    };
    let activeInvites = [existing];
    const store = createWorkspaceControlStore(
      clientWith({
        listWorkspaces: async () => [WORKSPACE_A, WORKSPACE_B],
        getWorkspace: async (workspaceId) =>
          workspaceId === WORKSPACE_A_ID ? WORKSPACE_A : WORKSPACE_B,
        listMembers: async (workspaceId) =>
          workspaceId === WORKSPACE_A_ID ? [MEMBER_A] : [MEMBER_B],
        listRoles: async () => [],
        listAppCatalog: async () => [],
        listInstallations: async () => [],
        listInvites: async (workspaceId) =>
          workspaceId === WORKSPACE_A_ID ? [...activeInvites] : [],
        createInvite: async () => {
          const { code: _code, ...record } = invite;
          activeInvites = [...activeInvites, record];
          return invite;
        },
        revokeInvite: async (_workspaceId, inviteId) => {
          activeInvites = activeInvites.filter(
            (candidate) => candidate.inviteId !== inviteId,
          );
        },
      }),
    );
    store.getState().resetSession(HUMAN_ID);
    await store.getState().init();
    await store.getState().selectWorkspace(WORKSPACE_A_ID);

    expect(store.getState().invites).toEqual([existing]);

    await store.getState().createInvite();
    expect(store.getState().invites).toEqual([existing, activeInvites[1]]);
    expect(store.getState().createdInviteSecret).toEqual({
      inviteId: invite.inviteId,
      code: INVITE_CODE,
    });

    store.getState().clearCreatedInviteSecret();
    expect(store.getState().invites).toHaveLength(2);
    expect(store.getState().createdInviteSecret).toBeNull();

    await store.getState().selectWorkspace(WORKSPACE_B_ID);
    expect(store.getState().invites).toEqual([]);
    expect(store.getState().createdInviteSecret).toBeNull();

    await store.getState().selectWorkspace(WORKSPACE_A_ID);
    expect(store.getState().invites).toEqual(activeInvites);
    expect(store.getState().createdInviteSecret).toBeNull();
    await store.getState().revokeInvite(existing.inviteId);
    expect(store.getState().invites).toEqual([activeInvites[0]]);
  });

  it("mutates the exact membership tenure rather than its participant identity", async () => {
    const setMemberRoles = vi.fn(async () => [ROLE_A_ID]);
    const removeMember = vi.fn(async () => undefined);
    const store = createWorkspaceControlStore(
      clientWith({
        listWorkspaces: async () => [WORKSPACE_A],
        getWorkspace: async () => WORKSPACE_A,
        listMembers: async () => [MEMBER_A],
        listRoles: async () => [ROLE_A],
        setMemberRoles,
        removeMember,
      }),
    );
    store.getState().resetSession(HUMAN_ID);
    await store.getState().init();
    await store.getState().selectWorkspace(WORKSPACE_A_ID);

    await store.getState().setMemberRoles(MEMBER_A_ID, [ROLE_A_ID]);
    expect(setMemberRoles).toHaveBeenCalledWith(WORKSPACE_A_ID, MEMBER_A_ID, [
      ROLE_A_ID,
    ]);
    expect(setMemberRoles).not.toHaveBeenCalledWith(
      WORKSPACE_A_ID,
      HUMAN_ID,
      expect.anything(),
    );
    expect(store.getState().members[0]?.roleIds).toEqual([ROLE_A_ID]);

    await store.getState().removeMember(MEMBER_A_ID);
    expect(removeMember).toHaveBeenCalledWith(WORKSPACE_A_ID, MEMBER_A_ID);
    expect(removeMember).not.toHaveBeenCalledWith(WORKSPACE_A_ID, HUMAN_ID);
    expect(store.getState().members).toEqual([]);
  });

  it("transfers ownership to one exact active membership tenure", async () => {
    const successor: WorkspaceMembership = {
      ...MEMBER_A,
      workspaceMemberId: MEMBER_C_ID,
      participant: {
        kind: "personality_agent",
        personalityAgentId: "0198f0f4-9b72-7000-8000-000000000199",
      },
      displayName: "Kuro",
      owner: false,
    };
    const transferred = {
      ...WORKSPACE_A,
      ownerWorkspaceMemberId: MEMBER_C_ID,
    };
    const transferOwnership = vi.fn(async () => transferred);
    const store = createWorkspaceControlStore(
      clientWith({
        listWorkspaces: async () => [WORKSPACE_A],
        getWorkspace: async () => WORKSPACE_A,
        listMembers: async () => [MEMBER_A, successor],
        listRoles: async () => [],
        transferOwnership,
      }),
    );
    store.getState().resetSession(HUMAN_ID);
    await store.getState().init();
    await store.getState().selectWorkspace(WORKSPACE_A_ID);

    await store.getState().transferOwnership(MEMBER_C_ID);

    expect(transferOwnership).toHaveBeenCalledWith(WORKSPACE_A_ID, MEMBER_C_ID);
    expect(store.getState().selectedWorkspace).toEqual(transferred);
    expect(
      store
        .getState()
        .members.map((member) => [member.workspaceMemberId, member.owner]),
    ).toEqual([
      [MEMBER_A_ID, false],
      [MEMBER_C_ID, true],
    ]);
  });

  it("moves an app from missing to enabled, disabled, enabled, then missing by installation ID", async () => {
    const enabled = installation(
      WORKSPACE_A_ID,
      "enabled",
      "2026-08-10T06:07:08.901Z",
    );
    const disabled = installation(
      WORKSPACE_A_ID,
      "disabled",
      "2026-08-10T06:08:09.012Z",
    );
    const reenabled = installation(
      WORKSPACE_A_ID,
      "enabled",
      "2026-08-10T06:09:10.123Z",
    );
    const installApp = vi.fn(async () => enabled);
    const setInstallationState = vi.fn(
      async (_installationId: string, state: "enabled" | "disabled") =>
        state === "disabled" ? disabled : reenabled,
    );
    const uninstallApp = vi.fn(async () => undefined);
    const store = createWorkspaceControlStore(
      clientWith({
        listWorkspaces: async () => [WORKSPACE_A],
        getWorkspace: async () => WORKSPACE_A,
        listMembers: async () => [MEMBER_A],
        listRoles: async () => [ROLE_A],
        listAppCatalog: async () => [APP],
        listInstallations: async () => [],
        installApp,
        setInstallationState,
        uninstallApp,
      }),
    );
    store.getState().resetSession(HUMAN_ID);
    await store.getState().init();
    await store.getState().selectWorkspace(WORKSPACE_A_ID);

    expect(
      installationForApp(store.getState().installations, APP_ID),
    ).toBeNull();

    await store.getState().installApp(APP_ID);
    expect(installApp).toHaveBeenCalledWith(
      { kind: "workspace", workspaceId: WORKSPACE_A_ID },
      APP_ID,
    );
    expect(installationForApp(store.getState().installations, APP_ID)).toBe(
      enabled,
    );

    await store.getState().setInstallationState(INSTALLATION_ID, "disabled");
    expect(setInstallationState).toHaveBeenLastCalledWith(
      INSTALLATION_ID,
      "disabled",
    );
    expect(installationForApp(store.getState().installations, APP_ID)).toBe(
      disabled,
    );

    await store.getState().setInstallationState(INSTALLATION_ID, "enabled");
    expect(setInstallationState).toHaveBeenLastCalledWith(
      INSTALLATION_ID,
      "enabled",
    );
    expect(installationForApp(store.getState().installations, APP_ID)).toBe(
      reenabled,
    );

    await store.getState().uninstallApp(INSTALLATION_ID);
    expect(uninstallApp).toHaveBeenCalledWith(INSTALLATION_ID);
    expect(uninstallApp).not.toHaveBeenCalledWith(APP_ID);
    expect(
      installationForApp(store.getState().installations, APP_ID),
    ).toBeNull();
  });

  it("never admits a delayed old-Workspace snapshot after switching scope", async () => {
    const oldSnapshotGate = deferred<void>();
    const APP_B: AppDescriptor = {
      appId: "canvas",
      displayName: "Canvas",
      workspaceOwnerAllowed: true,
      participantOwnerAllowed: false,
      workspaceRoleCapabilities: [],
    };
    const installationA = installation(
      WORKSPACE_A_ID,
      "enabled",
      "2026-08-10T06:10:11.234Z",
    );
    const installationB: AppInstallation = {
      installationId: INSTALLATION_B_ID,
      owner: { kind: "workspace", workspaceId: WORKSPACE_B_ID },
      appId: APP_B.appId,
      state: "disabled",
      authorityEpoch: "1",
      installedAt: Date.parse("2026-08-10T06:11:12.345Z"),
      updatedAt: Date.parse("2026-08-10T06:12:13.456Z"),
    };
    const delayedOld = <T>(value: T): Promise<T> =>
      oldSnapshotGate.promise.then(() => value);
    let catalogCall = 0;
    const store = createWorkspaceControlStore(
      clientWith({
        listWorkspaces: async () => [WORKSPACE_A, WORKSPACE_B],
        getWorkspace: (workspaceId) =>
          workspaceId === WORKSPACE_A_ID
            ? delayedOld(WORKSPACE_A)
            : Promise.resolve(WORKSPACE_B),
        listMembers: (workspaceId) =>
          workspaceId === WORKSPACE_A_ID
            ? delayedOld([MEMBER_A])
            : Promise.resolve([MEMBER_B]),
        listRoles: (workspaceId) =>
          workspaceId === WORKSPACE_A_ID
            ? delayedOld([ROLE_A])
            : Promise.resolve([ROLE_B]),
        listAppCatalog: () => {
          catalogCall += 1;
          return catalogCall === 1
            ? delayedOld([APP])
            : Promise.resolve([APP_B]);
        },
        listInstallations: (owner) =>
          owner.kind === "workspace" && owner.workspaceId === WORKSPACE_A_ID
            ? delayedOld([installationA])
            : Promise.resolve([installationB]),
      }),
    );
    store.getState().resetSession(HUMAN_ID);
    await store.getState().init();

    const oldSelection = store.getState().selectWorkspace(WORKSPACE_A_ID);
    expect(store.getState().selectionStatus).toBe("loading");
    const newSelection = store.getState().selectWorkspace(WORKSPACE_B_ID);
    await newSelection;

    expect(store.getState()).toMatchObject({
      selectionStatus: "ready",
      selectedWorkspaceId: WORKSPACE_B_ID,
      selectedWorkspace: WORKSPACE_B,
      members: [MEMBER_B],
      roles: [ROLE_B],
      catalog: [APP_B],
      installations: [installationB],
      errorCode: null,
    });

    oldSnapshotGate.resolve(undefined);
    await oldSelection;

    expect(store.getState()).toMatchObject({
      selectionStatus: "ready",
      selectedWorkspaceId: WORKSPACE_B_ID,
      selectedWorkspace: WORKSPACE_B,
      members: [MEMBER_B],
      roles: [ROLE_B],
      catalog: [APP_B],
      installations: [installationB],
      errorCode: null,
    });
    expect(store.getState().selectedWorkspace).not.toBe(WORKSPACE_A);
    expect(store.getState().members).not.toContain(MEMBER_A);
    expect(store.getState().roles).not.toContain(ROLE_A);
    expect(store.getState().catalog).not.toContain(APP);
    expect(store.getState().installations).not.toContain(installationA);
  });
});

function clientWith(
  overrides: Partial<WorkspaceControlClient> = {},
): WorkspaceControlClient {
  return {
    listWorkspaces: async () => [],
    createWorkspace: () => unexpected("createWorkspace"),
    getWorkspace: () => unexpected("getWorkspace"),
    updateWorkspace: () => unexpected("updateWorkspace"),
    transferOwnership: () => unexpected("transferOwnership"),
    listMembers: async () => [],
    leaveWorkspace: () => unexpected("leaveWorkspace"),
    removeMember: () => unexpected("removeMember"),
    createInvite: () => unexpected("createInvite"),
    listInvites: async () => [],
    revokeInvite: () => unexpected("revokeInvite"),
    previewInvite: () => unexpected("previewInvite"),
    redeemInvite: () => unexpected("redeemInvite"),
    listRoles: async () => [],
    createRole: () => unexpected("createRole"),
    updateRole: () => unexpected("updateRole"),
    deleteRole: () => unexpected("deleteRole"),
    setMemberRoles: () => unexpected("setMemberRoles"),
    listAppCatalog: async () => [],
    listInstallations: async () => [],
    installApp: () => unexpected("installApp"),
    setInstallationState: () => unexpected("setInstallationState"),
    uninstallApp: () => unexpected("uninstallApp"),
    ...overrides,
  };
}

function unexpected(operation: string): Promise<never> {
  return Promise.reject(new Error(`unexpected ${operation}`));
}

function installation(
  workspaceId: string,
  state: "enabled" | "disabled",
  updatedAt: string,
): AppInstallation {
  return {
    installationId: INSTALLATION_ID,
    owner: { kind: "workspace", workspaceId },
    appId: APP_ID,
    state,
    authorityEpoch: "1",
    installedAt: Date.parse("2026-08-10T06:07:08.901Z"),
    updatedAt: Date.parse(updatedAt),
  };
}

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}
