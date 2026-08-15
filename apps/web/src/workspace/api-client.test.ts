import { describe, expect, it, vi } from "vitest";
import {
  type WorkspaceAPIUncertainError,
  WorkspaceApiClient,
} from "./api-client";

const WORKSPACE_A_ID = "0198f0f4-9b72-7000-8000-000000000001";
const WORKSPACE_B_ID = "0198f0f4-9b72-7000-8000-000000000002";
const MEMBER_A_ID = "0198f0f4-9b72-7000-8000-000000000011";
const MEMBER_B_ID = "0198f0f4-9b72-7000-8000-000000000012";
const HUMAN_A_ID = "0198f0f4-9b72-7000-8000-000000000021";
const ROLE_A_ID = "0198f0f4-9b72-7000-8000-000000000031";
const ROLE_B_ID = "0198f0f4-9b72-7000-8000-000000000032";
const INVITE_ID = "0198f0f4-9b72-7000-8000-000000000041";
const TARGETED_INVITE_ID = "0198f0f4-9b72-7000-8000-000000000042";
const INVITE_CODE = "v".repeat(43);
const INSTALLATION_ID = "0198f0f4-9b72-7000-8000-000000000051";
const APP_ID = "messaging";

const WORKSPACE_A_WIRE = {
  workspace_id: WORKSPACE_A_ID,
  name: "Sumi Atelier",
  owner_workspace_member_id: MEMBER_A_ID,
  created_at: "2026-08-10T01:23:45.678Z",
};

const WORKSPACE_B_WIRE = {
  workspace_id: WORKSPACE_B_ID,
  name: "Night Studio",
  owner_workspace_member_id: MEMBER_B_ID,
  created_at: "2026-08-10T02:04:06.789Z",
};

describe("WorkspaceApiClient", () => {
  it("uses the canonical workspace collection and resource wire contract", async () => {
    const renamedWorkspace = { ...WORKSPACE_A_WIRE, name: "Sumi Lab" };
    const transferredWorkspace = {
      ...renamedWorkspace,
      owner_workspace_member_id: MEMBER_B_ID,
    };
    const fetcher = fetchSequence(
      json({ workspaces: [WORKSPACE_A_WIRE] }),
      json(WORKSPACE_B_WIRE, 201),
      json(WORKSPACE_A_WIRE),
      json(renamedWorkspace),
      json(transferredWorkspace),
    );
    const client = new WorkspaceApiClient(fetcher);

    await expect(client.listWorkspaces()).resolves.toEqual([
      {
        workspaceId: WORKSPACE_A_ID,
        name: "Sumi Atelier",
        ownerWorkspaceMemberId: MEMBER_A_ID,
        createdAt: Date.parse("2026-08-10T01:23:45.678Z"),
      },
    ]);
    await expect(client.createWorkspace("Night Studio")).resolves.toEqual({
      workspaceId: WORKSPACE_B_ID,
      name: "Night Studio",
      ownerWorkspaceMemberId: MEMBER_B_ID,
      createdAt: Date.parse("2026-08-10T02:04:06.789Z"),
    });
    await expect(client.getWorkspace(WORKSPACE_A_ID)).resolves.toMatchObject({
      workspaceId: WORKSPACE_A_ID,
      name: "Sumi Atelier",
    });
    await expect(
      client.updateWorkspace(WORKSPACE_A_ID, "Sumi Lab"),
    ).resolves.toMatchObject({ workspaceId: WORKSPACE_A_ID, name: "Sumi Lab" });
    await expect(
      client.transferOwnership(WORKSPACE_A_ID, MEMBER_B_ID),
    ).resolves.toMatchObject({
      workspaceId: WORKSPACE_A_ID,
      ownerWorkspaceMemberId: MEMBER_B_ID,
    });

    expectRequest(fetcher, 0, "/workspaces", "GET");
    expectRequest(fetcher, 1, "/workspaces", "POST", {
      name: "Night Studio",
    });
    expectRequest(fetcher, 2, `/workspaces/${WORKSPACE_A_ID}`, "GET");
    expectRequest(fetcher, 3, `/workspaces/${WORKSPACE_A_ID}`, "PATCH", {
      name: "Sumi Lab",
    });
    expectRequest(fetcher, 4, `/workspaces/${WORKSPACE_A_ID}/owner`, "PUT", {
      workspace_member_id: MEMBER_B_ID,
    });
  });

  it("uses the exact membership tenure ID when leaving or removing members", async () => {
    const fetcher = fetchSequence(
      json({
        members: [
          {
            workspace_member_id: MEMBER_A_ID,
            workspace_id: WORKSPACE_A_ID,
            display_name: "Yohaku",
            participant: { kind: "human", human_id: HUMAN_A_ID },
            owner: true,
            role_ids: [],
            joined_at: "2026-08-10T01:23:45.901Z",
            left_at: null,
          },
        ],
      }),
      new Response(null, { status: 204 }),
      new Response(null, { status: 204 }),
    );
    const client = new WorkspaceApiClient(fetcher);

    await expect(client.listMembers(WORKSPACE_A_ID)).resolves.toEqual([
      {
        workspaceMemberId: MEMBER_A_ID,
        workspaceId: WORKSPACE_A_ID,
        displayName: "Yohaku",
        participant: { kind: "human", humanId: HUMAN_A_ID },
        owner: true,
        roleIds: [],
        joinedAt: Date.parse("2026-08-10T01:23:45.901Z"),
        leftAt: null,
      },
    ]);
    await client.leaveWorkspace(WORKSPACE_A_ID);
    await client.removeMember(WORKSPACE_A_ID, MEMBER_A_ID);

    expectRequest(fetcher, 0, `/workspaces/${WORKSPACE_A_ID}/members`, "GET");
    expectRequest(
      fetcher,
      1,
      `/workspaces/${WORKSPACE_A_ID}/membership`,
      "DELETE",
    );
    expectRequest(
      fetcher,
      2,
      `/workspaces/${WORKSPACE_A_ID}/members/${MEMBER_A_ID}`,
      "DELETE",
    );
    expect(fetcher.mock.calls[2]?.[0]).not.toContain(HUMAN_A_ID);
  });

  it("round-trips role wires and assigns them to the exact membership tenure", async () => {
    const existingRole = {
      role_id: ROLE_A_ID,
      workspace_id: WORKSPACE_A_ID,
      name: "Curator",
      color: "#4a6670",
      position: 600,
      permissions: [
        "app.messaging.manage_channels",
        "app.messaging.retired_capability",
      ],
      created_at: "2026-08-10T03:10:11.012Z",
    };
    const createdRole = {
      role_id: ROLE_B_ID,
      workspace_id: WORKSPACE_A_ID,
      name: "Steward",
      color: "#6b5578",
      position: 700,
      permissions: ["manage_members", "manage_roles"],
      created_at: "2026-08-10T03:12:13.014Z",
    };
    const updatedRole = {
      ...createdRole,
      name: "Host",
      permissions: ["manage_members"],
    };
    const fetcher = fetchSequence(
      json({ roles: [existingRole] }),
      json(createdRole, 201),
      json(updatedRole),
      json({ role_ids: [ROLE_A_ID, ROLE_B_ID] }),
      new Response(null, { status: 204 }),
    );
    const client = new WorkspaceApiClient(fetcher);

    await expect(client.listRoles(WORKSPACE_A_ID)).resolves.toEqual([
      {
        roleId: ROLE_A_ID,
        workspaceId: WORKSPACE_A_ID,
        name: "Curator",
        color: "#4a6670",
        position: 600,
        permissions: [
          "app.messaging.manage_channels",
          "app.messaging.retired_capability",
        ],
        createdAt: Date.parse("2026-08-10T03:10:11.012Z"),
      },
    ]);
    await client.createRole(WORKSPACE_A_ID, {
      name: "Steward",
      color: "#6b5578",
      position: 700,
      permissions: ["manage_members", "manage_roles"],
    });
    await client.updateRole(WORKSPACE_A_ID, ROLE_B_ID, {
      name: "Host",
      permissions: ["manage_members"],
    });
    await expect(
      client.setMemberRoles(WORKSPACE_A_ID, MEMBER_A_ID, [
        ROLE_A_ID,
        ROLE_B_ID,
      ]),
    ).resolves.toEqual([ROLE_A_ID, ROLE_B_ID]);
    await client.deleteRole(WORKSPACE_A_ID, ROLE_A_ID);

    expectRequest(fetcher, 0, `/workspaces/${WORKSPACE_A_ID}/roles`, "GET");
    expectRequest(fetcher, 1, `/workspaces/${WORKSPACE_A_ID}/roles`, "POST", {
      name: "Steward",
      color: "#6b5578",
      position: 700,
      permissions: ["manage_members", "manage_roles"],
    });
    expectRequest(
      fetcher,
      2,
      `/workspaces/${WORKSPACE_A_ID}/roles/${ROLE_B_ID}`,
      "PATCH",
      { name: "Host", permissions: ["manage_members"] },
    );
    expectRequest(
      fetcher,
      3,
      `/workspaces/${WORKSPACE_A_ID}/members/${MEMBER_A_ID}/roles`,
      "PUT",
      { role_ids: [ROLE_A_ID, ROLE_B_ID] },
    );
    expect(fetcher.mock.calls[3]?.[0]).not.toContain(HUMAN_A_ID);
    expectRequest(
      fetcher,
      4,
      `/workspaces/${WORKSPACE_A_ID}/roles/${ROLE_A_ID}`,
      "DELETE",
    );
  });

  it("previews and redeems an opaque invite through the canonical public routes", async () => {
    const inviteWire = {
      invite_id: INVITE_ID,
      workspace_id: WORKSPACE_A_ID,
      code: INVITE_CODE,
      expires_at: "2026-08-11T04:05:06.789Z",
      created_at: "2026-08-10T04:05:06.789Z",
    };
    const previewWire = {
      workspace_id: WORKSPACE_A_ID,
      workspace_name: "Sumi Atelier",
      expires_at: "2026-08-11T04:05:06.789Z",
    };
    const redeemedMembership = {
      workspace_member_id: MEMBER_B_ID,
      workspace_id: WORKSPACE_A_ID,
      participant: { kind: "human", human_id: HUMAN_A_ID },
      display_name: "Yohaku",
      owner: false,
      role_ids: [],
      joined_at: "2026-08-10T04:06:07.890Z",
      left_at: null,
    };
    const fetcher = fetchSequence(
      json(inviteWire, 201),
      json({
        invites: [
          {
            kind: "share_code",
            invite_id: INVITE_ID,
            workspace_id: WORKSPACE_A_ID,
            expires_at: "2026-08-11T04:05:06.789Z",
            created_at: "2026-08-10T04:05:06.789Z",
          },
        ],
      }),
      json(previewWire),
      json(redeemedMembership),
      new Response(null, { status: 204 }),
    );
    const client = new WorkspaceApiClient(fetcher);

    await expect(client.createInvite(WORKSPACE_A_ID)).resolves.toEqual({
      inviteId: INVITE_ID,
      workspaceId: WORKSPACE_A_ID,
      code: INVITE_CODE,
      expiresAt: Date.parse("2026-08-11T04:05:06.789Z"),
      createdAt: Date.parse("2026-08-10T04:05:06.789Z"),
    });
    await expect(client.listInvites(WORKSPACE_A_ID)).resolves.toEqual([
      {
        kind: "share_code",
        inviteId: INVITE_ID,
        workspaceId: WORKSPACE_A_ID,
        expiresAt: Date.parse("2026-08-11T04:05:06.789Z"),
        createdAt: Date.parse("2026-08-10T04:05:06.789Z"),
      },
    ]);
    await expect(client.previewInvite(INVITE_CODE)).resolves.toEqual({
      workspaceId: WORKSPACE_A_ID,
      workspaceName: "Sumi Atelier",
      expiresAt: Date.parse("2026-08-11T04:05:06.789Z"),
    });
    await expect(client.redeemInvite(INVITE_CODE)).resolves.toMatchObject({
      workspaceMemberId: MEMBER_B_ID,
      workspaceId: WORKSPACE_A_ID,
      displayName: "Yohaku",
      roleIds: [],
      leftAt: null,
    });
    await client.revokeInvite(WORKSPACE_A_ID, INVITE_ID);

    expectRequest(
      fetcher,
      0,
      `/workspaces/${WORKSPACE_A_ID}/invites`,
      "POST",
      {},
    );
    expectRequest(fetcher, 1, `/workspaces/${WORKSPACE_A_ID}/invites`, "GET");
    expectRequest(
      fetcher,
      2,
      `/workspace-invites/preview?code=${INVITE_CODE}`,
      "GET",
    );
    expectRequest(fetcher, 3, "/workspace-invites/redeem", "POST", {
      code: INVITE_CODE,
    });
    expectRequest(
      fetcher,
      4,
      `/workspaces/${WORKSPACE_A_ID}/invites/${INVITE_ID}`,
      "DELETE",
    );
  });

  it("keeps targeted PA invitations discriminated and maps the exact current-agent resource", async () => {
    const shareRecord = {
      kind: "share_code",
      invite_id: INVITE_ID,
      workspace_id: WORKSPACE_A_ID,
      expires_at: "2026-08-11T04:05:06.789Z",
      created_at: "2026-08-10T04:05:06.789Z",
    };
    const targetedRecord = {
      kind: "targeted_personality_agent",
      invite_id: TARGETED_INVITE_ID,
      workspace_id: WORKSPACE_A_ID,
      expires_at: "2026-08-11T05:05:06.789Z",
      created_at: "2026-08-10T05:05:06.789Z",
    };
    const fetcher = fetchSequence(
      json({ invites: [shareRecord, targetedRecord] }),
      json(targetedRecord),
      json(targetedRecord, 201),
      json({ error: "not_found" }, 404),
      json({ error: "conflict" }, 409),
    );
    const client = new WorkspaceApiClient(fetcher);

    await expect(client.listInvites(WORKSPACE_A_ID)).resolves.toEqual([
      {
        kind: "share_code",
        inviteId: INVITE_ID,
        workspaceId: WORKSPACE_A_ID,
        expiresAt: Date.parse(shareRecord.expires_at),
        createdAt: Date.parse(shareRecord.created_at),
      },
      {
        kind: "targeted_personality_agent",
        inviteId: TARGETED_INVITE_ID,
        workspaceId: WORKSPACE_A_ID,
        expiresAt: Date.parse(targetedRecord.expires_at),
        createdAt: Date.parse(targetedRecord.created_at),
      },
    ]);
    await expect(client.getCurrentAgentInvite(WORKSPACE_A_ID)).resolves.toEqual(
      {
        status: "pending",
        invite: {
          kind: "targeted_personality_agent",
          inviteId: TARGETED_INVITE_ID,
          workspaceId: WORKSPACE_A_ID,
          expiresAt: Date.parse(targetedRecord.expires_at),
          createdAt: Date.parse(targetedRecord.created_at),
        },
      },
    );
    await expect(
      client.createCurrentAgentInvite(WORKSPACE_A_ID),
    ).resolves.toMatchObject({
      kind: "targeted_personality_agent",
      inviteId: TARGETED_INVITE_ID,
      workspaceId: WORKSPACE_A_ID,
    });
    await expect(client.getCurrentAgentInvite(WORKSPACE_A_ID)).resolves.toEqual(
      {
        status: "none",
      },
    );
    await expect(client.getCurrentAgentInvite(WORKSPACE_A_ID)).resolves.toEqual(
      {
        status: "member",
      },
    );

    expectRequest(fetcher, 0, `/workspaces/${WORKSPACE_A_ID}/invites`, "GET");
    expectRequest(
      fetcher,
      1,
      `/workspaces/${WORKSPACE_A_ID}/invites/current-agent`,
      "GET",
    );
    expectRequest(
      fetcher,
      2,
      `/workspaces/${WORKSPACE_A_ID}/invites/current-agent`,
      "POST",
      {},
    );
  });

  it("rejects missing and unknown Workspace invite discriminators", async () => {
    const common = {
      invite_id: INVITE_ID,
      workspace_id: WORKSPACE_A_ID,
      expires_at: "2026-08-11T04:05:06.789Z",
      created_at: "2026-08-10T04:05:06.789Z",
    };
    const fetcher = fetchSequence(
      json({ invites: [common] }),
      json({ invites: [{ ...common, kind: "current_agent" }] }),
    );
    const client = new WorkspaceApiClient(fetcher);

    await expect(client.listInvites(WORKSPACE_A_ID)).rejects.toThrow(
      "invalid Workspace invite kind",
    );
    await expect(client.listInvites(WORKSPACE_A_ID)).rejects.toThrow(
      "invalid Workspace invite kind",
    );
  });

  it("addresses disable, enable, and uninstall by the exact installation ID", async () => {
    const installedWire = {
      installation_id: INSTALLATION_ID,
      owner: { kind: "workspace", workspace_id: WORKSPACE_A_ID },
      app_id: APP_ID,
      state: "enabled",
      authority_epoch: "1",
      installed_at: "2026-08-10T05:06:07.890Z",
      updated_at: "2026-08-10T05:06:07.890Z",
    };
    const disabledWire = {
      ...installedWire,
      state: "disabled",
      authority_epoch: "2",
      updated_at: "2026-08-10T05:07:08.901Z",
    };
    const reenabledWire = {
      ...installedWire,
      authority_epoch: "2",
      updated_at: "2026-08-10T05:08:09.012Z",
    };
    const fetcher = fetchSequence(
      json({
        apps: [
          {
            app_id: APP_ID,
            display_name: "Messaging",
            workspace_owner_allowed: true,
            participant_owner_allowed: false,
            workspace_role_capabilities: [
              {
                ref: "app.messaging.manage_channels",
                label: "Manage channels",
              },
            ],
          },
        ],
      }),
      json({ installations: [] }),
      json(installedWire, 201),
      json(disabledWire),
      json(reenabledWire),
      new Response(null, { status: 204 }),
    );
    const client = new WorkspaceApiClient(fetcher);

    await expect(client.listAppCatalog()).resolves.toEqual([
      {
        appId: APP_ID,
        displayName: "Messaging",
        workspaceOwnerAllowed: true,
        participantOwnerAllowed: false,
        workspaceRoleCapabilities: [
          { ref: "app.messaging.manage_channels", label: "Manage channels" },
        ],
      },
    ]);
    await expect(
      client.listInstallations({
        kind: "workspace",
        workspaceId: WORKSPACE_A_ID,
      }),
    ).resolves.toEqual([]);
    await expect(
      client.installApp(
        { kind: "workspace", workspaceId: WORKSPACE_A_ID },
        APP_ID,
      ),
    ).resolves.toEqual({
      installationId: INSTALLATION_ID,
      owner: { kind: "workspace", workspaceId: WORKSPACE_A_ID },
      appId: APP_ID,
      state: "enabled",
      authorityEpoch: "1",
      installedAt: Date.parse("2026-08-10T05:06:07.890Z"),
      updatedAt: Date.parse("2026-08-10T05:06:07.890Z"),
    });
    await expect(
      client.setInstallationState(INSTALLATION_ID, "disabled", "1"),
    ).resolves.toMatchObject({
      installationId: INSTALLATION_ID,
      state: "disabled",
      authorityEpoch: "2",
      updatedAt: Date.parse("2026-08-10T05:07:08.901Z"),
    });
    await expect(
      client.setInstallationState(INSTALLATION_ID, "enabled", "2"),
    ).resolves.toMatchObject({
      installationId: INSTALLATION_ID,
      state: "enabled",
      authorityEpoch: "2",
      updatedAt: Date.parse("2026-08-10T05:08:09.012Z"),
    });
    await client.uninstallApp(INSTALLATION_ID);

    expectRequest(fetcher, 0, "/apps/catalog", "GET");
    expectRequest(
      fetcher,
      1,
      `/app-installations?owner_kind=workspace&owner_id=${WORKSPACE_A_ID}`,
      "GET",
    );
    expectRequest(fetcher, 2, "/app-installations", "POST", {
      owner: { kind: "workspace", workspace_id: WORKSPACE_A_ID },
      app_id: APP_ID,
    });
    expectRequest(
      fetcher,
      3,
      `/app-installations/${INSTALLATION_ID}/state`,
      "PUT",
      { state: "disabled", expected_authority_epoch: "1" },
    );
    expectRequest(
      fetcher,
      4,
      `/app-installations/${INSTALLATION_ID}/state`,
      "PUT",
      { state: "enabled", expected_authority_epoch: "2" },
    );
    expectRequest(
      fetcher,
      5,
      `/app-installations/${INSTALLATION_ID}`,
      "DELETE",
    );
    expect(fetcher.mock.calls.slice(3).map(([path]) => path)).not.toContain(
      `/app-installations/${APP_ID}`,
    );
  });

  it("addresses a Participant-owned lifecycle through the same exact owner contract", async () => {
    const participantInstallation = {
      installation_id: INSTALLATION_ID,
      owner: {
        kind: "participant",
        participant: { kind: "human", human_id: HUMAN_A_ID },
      },
      app_id: "direct-chat",
      state: "enabled",
      authority_epoch: "1",
      installed_at: "2026-08-10T05:06:07.890Z",
      updated_at: "2026-08-10T05:06:07.890Z",
    };
    const fetcher = fetchSequence(
      json({ installations: [participantInstallation] }),
      json(participantInstallation, 201),
    );
    const client = new WorkspaceApiClient(fetcher);
    const owner = {
      kind: "participant" as const,
      participant: { kind: "human" as const, humanId: HUMAN_A_ID },
    };
    const operationId = "00000000-0000-4000-8000-000000000202";

    await expect(client.listInstallations(owner)).resolves.toEqual([
      {
        installationId: INSTALLATION_ID,
        owner,
        appId: "direct-chat",
        state: "enabled",
        authorityEpoch: "1",
        installedAt: Date.parse("2026-08-10T05:06:07.890Z"),
        updatedAt: Date.parse("2026-08-10T05:06:07.890Z"),
      },
    ]);
    await expect(
      client.installApp(owner, "direct-chat", operationId),
    ).resolves.toMatchObject({
      installationId: INSTALLATION_ID,
      owner,
      appId: "direct-chat",
    });

    expectRequest(
      fetcher,
      0,
      `/app-installations?owner_kind=participant&owner_id=${HUMAN_A_ID}&participant_kind=human`,
      "GET",
    );
    expectRequest(fetcher, 1, "/app-installations", "POST", {
      owner: {
        kind: "participant",
        participant: { kind: "human", human_id: HUMAN_A_ID },
      },
      app_id: "direct-chat",
      operation_id: operationId,
    });
  });

  it("marks a request without an HTTP response as outcome-uncertain", async () => {
    const transportFailure = new TypeError("response channel closed");
    const fetcher = vi.fn<typeof fetch>().mockRejectedValue(transportFailure);
    const client = new WorkspaceApiClient(fetcher);

    await expect(
      client.setInstallationState(INSTALLATION_ID, "disabled", "1"),
    ).rejects.toMatchObject({
      name: "WorkspaceAPIUncertainError",
      message: "workspace_request_outcome_uncertain",
      cause: transportFailure,
    } satisfies Partial<WorkspaceAPIUncertainError>);
  });

  it("carries an exact durable operation id on Participant install", async () => {
    const fetcher = vi.fn<typeof fetch>().mockResolvedValue(
      json(
        {
          installation_id: INSTALLATION_ID,
          owner: {
            kind: "participant",
            participant: { kind: "human", human_id: HUMAN_A_ID },
          },
          app_id: APP_ID,
          state: "enabled",
          authority_epoch: "1",
          installed_at: "2026-08-10T05:06:07.890Z",
          updated_at: "2026-08-10T05:06:07.890Z",
        },
        201,
      ),
    );
    const client = new WorkspaceApiClient(fetcher);
    const operationId = "00000000-0000-4000-8000-000000000201";

    await client.installApp(
      {
        kind: "participant",
        participant: { kind: "human", humanId: HUMAN_A_ID },
      },
      APP_ID,
      operationId,
    );

    expectRequest(fetcher, 0, "/app-installations", "POST", {
      owner: {
        kind: "participant",
        participant: { kind: "human", human_id: HUMAN_A_ID },
      },
      app_id: APP_ID,
      operation_id: operationId,
    });
  });
});

function fetchSequence(...responses: Response[]) {
  const fetcher = vi.fn<typeof fetch>();
  for (const response of responses) fetcher.mockResolvedValueOnce(response);
  return fetcher;
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function expectRequest(
  fetcher: ReturnType<typeof fetchSequence>,
  index: number,
  path: string,
  method: string,
  body?: unknown,
): void {
  const request = fetcher.mock.calls[index];
  expect(request?.[0]).toBe(path);
  expect(request?.[1]).toEqual({
    method,
    credentials: "include",
    cache: "no-store",
    headers: {
      Accept: "application/json",
      ...(body === undefined ? {} : { "Content-Type": "application/json" }),
    },
    body: body === undefined ? undefined : JSON.stringify(body),
    signal: expect.any(AbortSignal),
  });
}
