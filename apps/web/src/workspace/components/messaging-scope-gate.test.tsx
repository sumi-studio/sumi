// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  bindMessagingSessionIdentity,
  getMessagingScope,
} from "../../messaging/store";
import type { WorkspaceControlState } from "../store";
import { MessagingScopeGate } from "./messaging-scope-gate";

const mocks = vi.hoisted(() => ({
  installApp: vi.fn(),
  navigate: vi.fn(),
  setInstallationState: vi.fn(),
  state: {} as WorkspaceControlState,
}));

vi.mock("@tanstack/react-router", () => ({
  useNavigate: () => mocks.navigate,
}));

vi.mock("../../auth/auth-context", () => ({
  useAuth: () => ({ user: { id: "human-1" } }),
}));

vi.mock("../../shell/app-rail", () => ({
  AppRail: () => <aside data-testid="app-rail" />,
}));

vi.mock("../store", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../store")>();
  return {
    ...actual,
    useWorkspaceControl: (
      selector: (state: WorkspaceControlState) => unknown,
    ) => selector(mocks.state),
  };
});

function workspaceState(
  installations: WorkspaceControlState["installations"],
): WorkspaceControlState {
  return {
    sessionIdentity: "human-1",
    sessionScopeKey: "binding-1",
    listStatus: "ready",
    selectionStatus: "ready",
    workspaces: [],
    selectedWorkspaceId: "workspace-1",
    selectedWorkspace: null,
    members: [
      {
        workspaceMemberId: "member-1",
        workspaceId: "workspace-1",
        displayName: "Yohaku",
        participant: { kind: "human", humanId: "human-1" },
        owner: true,
        roleIds: [],
        joinedAt: 1,
        leftAt: null,
      },
    ],
    roles: [],
    catalog: [
      {
        appId: "messaging",
        displayName: "Messaging",
        workspaceOwnerAllowed: true,
        participantOwnerAllowed: false,
        workspaceRoleCapabilities: [
          { ref: "app.messaging.manage_channels", label: "Manage channels" },
        ],
      },
    ],
    installations,
    invites: [],
    currentAgentInvite: { status: "none" },
    createdInviteSecret: null,
    errorCode: null,
    mutation: null,
    resetSession: vi.fn(),
    init: vi.fn(),
    refreshWorkspaces: vi.fn(),
    createWorkspace: vi.fn(),
    selectWorkspace: vi.fn(),
    refreshSelectedWorkspace: vi.fn(),
    clearSelection: vi.fn(),
    updateWorkspace: vi.fn(),
    transferOwnership: vi.fn(),
    leaveWorkspace: vi.fn(),
    removeMember: vi.fn(),
    createInvite: vi.fn(),
    createCurrentAgentInvite: vi.fn(),
    revokeInvite: vi.fn(),
    clearCreatedInviteSecret: vi.fn(),
    previewInvite: vi.fn(),
    redeemInvite: vi.fn(),
    createRole: vi.fn(),
    updateRole: vi.fn(),
    deleteRole: vi.fn(),
    setMemberRoles: vi.fn(),
    installApp: mocks.installApp,
    setInstallationState: mocks.setInstallationState,
    uninstallApp: vi.fn(),
  };
}

beforeEach(() => {
  bindMessagingSessionIdentity("human-1");
  mocks.installApp.mockResolvedValue(undefined);
  mocks.setInstallationState.mockResolvedValue(undefined);
  mocks.state = workspaceState([]);
});

afterEach(() => {
  cleanup();
  bindMessagingSessionIdentity(null);
  vi.clearAllMocks();
});

describe("MessagingScopeGate", () => {
  it("shows the explicit install journey and never opens an unbound child", () => {
    render(
      <MessagingScopeGate workspaceId="workspace-1">
        <div>Messaging child</div>
      </MessagingScopeGate>,
    );

    expect(
      screen.getByText("Messagingはまだインストールされていません"),
    ).toBeInTheDocument();
    expect(screen.queryByText("Messaging child")).toBeNull();
    expect(getMessagingScope()).toBeNull();
    fireEvent.click(
      screen.getByRole("button", { name: "Messagingをインストール" }),
    );
    expect(mocks.installApp).toHaveBeenCalledWith("messaging");
  });

  it("keeps a disabled installation disconnected and enables its exact ID", () => {
    mocks.state = workspaceState([
      {
        installationId: "installation-1",
        owner: { kind: "workspace", workspaceId: "workspace-1" },
        appId: "messaging",
        state: "disabled",
        authorityEpoch: "1",
        installedAt: 1,
        updatedAt: 1,
      },
    ]);
    render(
      <MessagingScopeGate workspaceId="workspace-1">
        <div>Messaging child</div>
      </MessagingScopeGate>,
    );

    expect(
      screen.getByText("Messagingは無効になっています"),
    ).toBeInTheDocument();
    expect(getMessagingScope()).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "有効にする" }));
    expect(mocks.setInstallationState).toHaveBeenCalledWith(
      "installation-1",
      "enabled",
    );
  });

  it("binds one enabled installation and disposes it on unmount", () => {
    mocks.state = workspaceState([
      {
        installationId: "installation-1",
        owner: { kind: "workspace", workspaceId: "workspace-1" },
        appId: "messaging",
        state: "enabled",
        authorityEpoch: "1",
        installedAt: 1,
        updatedAt: 1,
      },
    ]);
    const view = render(
      <MessagingScopeGate workspaceId="workspace-1">
        <div>Messaging child</div>
      </MessagingScopeGate>,
    );

    expect(screen.getByText("Messaging child")).toBeInTheDocument();
    expect(getMessagingScope()).toEqual({
      workspaceId: "workspace-1",
      installationId: "installation-1",
      authorityEpoch: "1",
    });

    view.unmount();
    expect(getMessagingScope()).toBeNull();
  });

  it("resets the bound transport and subtree when an enabled installation rolls epoch", () => {
    const initialInstallation = {
      installationId: "installation-1",
      owner: { kind: "workspace" as const, workspaceId: "workspace-1" },
      appId: "messaging",
      state: "enabled" as const,
      authorityEpoch: "1",
      installedAt: 1,
      updatedAt: 1,
    };
    mocks.state = workspaceState([initialInstallation]);
    let mounts = 0;
    function Child() {
      mounts += 1;
      return <div>epoch child</div>;
    }
    const view = render(
      <MessagingScopeGate workspaceId="workspace-1">
        <Child />
      </MessagingScopeGate>,
    );
    expect(getMessagingScope()?.authorityEpoch).toBe("1");

    mocks.state = workspaceState([
      {
        ...initialInstallation,
        authorityEpoch: "2",
      },
    ]);
    view.rerender(
      <MessagingScopeGate workspaceId="workspace-1">
        <Child />
      </MessagingScopeGate>,
    );

    expect(getMessagingScope()?.authorityEpoch).toBe("2");
    expect(mounts).toBe(2);
  });

  it("fails closed when duplicate app bindings are returned", () => {
    mocks.state = workspaceState([
      {
        installationId: "installation-1",
        owner: { kind: "workspace", workspaceId: "workspace-1" },
        appId: "messaging",
        state: "enabled",
        authorityEpoch: "1",
        installedAt: 1,
        updatedAt: 1,
      },
      {
        installationId: "installation-2",
        owner: { kind: "workspace", workspaceId: "workspace-1" },
        appId: "messaging",
        state: "enabled",
        authorityEpoch: "1",
        installedAt: 2,
        updatedAt: 2,
      },
    ]);
    render(
      <MessagingScopeGate workspaceId="workspace-1">
        <div>Messaging child</div>
      </MessagingScopeGate>,
    );

    expect(
      screen.getByText("Messagingの設定を確認できません"),
    ).toBeInTheDocument();
    expect(screen.queryByText("Messaging child")).toBeNull();
    expect(getMessagingScope()).toBeNull();
  });

  it.each([
    "missing",
    "ambiguous",
  ] as const)("fails closed when the current Human membership is %s", (condition) => {
    mocks.state = workspaceState([
      {
        installationId: "installation-1",
        owner: { kind: "workspace", workspaceId: "workspace-1" },
        appId: "messaging",
        state: "enabled",
        authorityEpoch: "1",
        installedAt: 1,
        updatedAt: 1,
      },
    ]);
    const [membership] = mocks.state.members;
    if (!membership) throw new Error("test membership fixture is missing");
    mocks.state.members =
      condition === "missing"
        ? []
        : [
            membership,
            {
              ...membership,
              workspaceMemberId: "member-2",
            },
          ];

    render(
      <MessagingScopeGate workspaceId="workspace-1">
        <div>Messaging child</div>
      </MessagingScopeGate>,
    );

    expect(
      screen.getByText("Workspaceへの参加状態を確認できません"),
    ).toBeInTheDocument();
    expect(screen.queryByText("Messaging child")).toBeNull();
    expect(getMessagingScope()).toBeNull();
  });

  it.each([
    "missing",
    "ambiguous",
  ] as const)("fails closed when the Messaging catalog descriptor is %s", (condition) => {
    const state = workspaceState([
      {
        installationId: "installation-1",
        owner: { kind: "workspace", workspaceId: "workspace-1" },
        appId: "messaging",
        state: "enabled",
        authorityEpoch: "1",
        installedAt: 1,
        updatedAt: 1,
      },
    ]);
    const [descriptor] = state.catalog;
    if (!descriptor) throw new Error("test catalog fixture is missing");
    state.catalog =
      condition === "missing" ? [] : [descriptor, { ...descriptor }];
    mocks.state = state;

    render(
      <MessagingScopeGate workspaceId="workspace-1">
        <div>Messaging child</div>
      </MessagingScopeGate>,
    );

    expect(
      screen.getByText("Messagingの提供状態を確認できません"),
    ).toBeInTheDocument();
    expect(screen.queryByText("Messaging child")).toBeNull();
    expect(getMessagingScope()).toBeNull();
  });
});
