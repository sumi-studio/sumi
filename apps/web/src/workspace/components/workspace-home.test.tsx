// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { WorkspaceAPIError } from "../api-client";
import type { WorkspaceControlState } from "../store";
import { WorkspaceHome, workspaceMutationErrorMessage } from "./workspace-home";

const WORKSPACE_ID = "0198f0f4-9b72-7000-8000-000000000101";
const CURRENT_INVITE_ID = "0198f0f4-9b72-7000-8000-000000000162";
const ANONYMOUS_INVITE_ID = "0198f0f4-9b72-7000-8000-000000000161";
const FORBIDDEN_TARGET_ID = "0198f0f4-9b72-7000-8000-000000000199";

const anonymousInvite = {
  kind: "targeted_personality_agent" as const,
  inviteId: ANONYMOUS_INVITE_ID,
  workspaceId: WORKSPACE_ID,
  expiresAt: Date.parse("2026-08-11T08:01:02.345Z"),
  createdAt: Date.parse("2026-08-10T08:01:02.345Z"),
};
const currentInvite = {
  ...anonymousInvite,
  inviteId: CURRENT_INVITE_ID,
  expiresAt: Date.parse("2026-08-11T08:02:03.456Z"),
  createdAt: Date.parse("2026-08-10T08:02:03.456Z"),
};

const mocks = vi.hoisted(() => ({
  createCurrentAgentInvite: vi.fn(),
  navigate: vi.fn(),
  revokeInvite: vi.fn(),
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
  currentAgentInvite: WorkspaceControlState["currentAgentInvite"],
): WorkspaceControlState {
  return {
    sessionIdentity: "human-1",
    sessionScopeKey: "binding-1",
    listStatus: "ready",
    selectionStatus: "ready",
    workspaces: [],
    selectedWorkspaceId: WORKSPACE_ID,
    selectedWorkspace: {
      workspaceId: WORKSPACE_ID,
      name: "Sumi Atelier",
      ownerWorkspaceMemberId: "0198f0f4-9b72-7000-8000-000000000111",
      createdAt: Date.parse("2026-08-10T06:01:02.345Z"),
    },
    members: [
      {
        workspaceMemberId: "0198f0f4-9b72-7000-8000-000000000111",
        workspaceId: WORKSPACE_ID,
        displayName: "Yohaku",
        participant: { kind: "human", humanId: "human-1" },
        owner: true,
        roleIds: [],
        joinedAt: Date.parse("2026-08-10T06:01:02.678Z"),
        leftAt: null,
      },
    ],
    roles: [],
    catalog: [],
    installations: [],
    invites: [anonymousInvite, currentInvite],
    currentAgentInvite,
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
    createCurrentAgentInvite: mocks.createCurrentAgentInvite,
    clearCreatedInviteSecret: vi.fn(),
    revokeInvite: mocks.revokeInvite,
    previewInvite: vi.fn(),
    redeemInvite: vi.fn(),
    createRole: vi.fn(),
    updateRole: vi.fn(),
    deleteRole: vi.fn(),
    setMemberRoles: vi.fn(),
    installApp: vi.fn(),
    setInstallationState: vi.fn(),
    uninstallApp: vi.fn(),
  };
}

function renderMembers() {
  render(<WorkspaceHome workspaceId={WORKSPACE_ID} />);
  fireEvent.click(screen.getByRole("button", { name: "参加者と招待" }));
}

beforeEach(() => {
  mocks.createCurrentAgentInvite.mockResolvedValue(currentInvite);
  mocks.revokeInvite.mockResolvedValue(undefined);
  mocks.state = workspaceState({ status: "none" });
});

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

describe("Workspace current Direct Chat invitation", () => {
  it("separates the exact second record from an anonymous mixed targeted registry", () => {
    mocks.state = workspaceState({
      status: "pending",
      invite: currentInvite,
    });
    renderMembers();

    expect(screen.getByText("招待済み・承諾待ち")).toBeInTheDocument();
    expect(
      screen.getByText("Direct Chatで招待を確認してもらってください。"),
    ).toBeInTheDocument();
    expect(
      screen.getByText("対象を表示しない人格エージェントへの招待"),
    ).toBeInTheDocument();
    expect(screen.getByText(/00000161/)).toBeInTheDocument();
    expect(screen.queryByText(/00000162/)).not.toBeInTheDocument();
    expect(document.body.textContent).not.toContain(FORBIDDEN_TARGET_ID);
    expect(document.body.textContent).not.toContain("personality_agent_id");
    expect(screen.queryByLabelText("招待コード")).not.toBeInTheDocument();
  });

  it("keeps all targeted records anonymous and offers issuance when exact GET is absent", () => {
    renderMembers();

    expect(screen.getByRole("button", { name: "招待する" })).toBeEnabled();
    expect(screen.getByText(/00000161/)).toBeInTheDocument();
    expect(screen.getByText(/00000162/)).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "招待する" }));
    expect(mocks.createCurrentAgentInvite).toHaveBeenCalledOnce();
  });

  it.each([
    ["member", "このWorkspaceに参加済みです"],
    ["unavailable", "現在のDirect Chatとの関係では"],
    ["error", "現在のDirect Chatの招待状態を確認できませんでした"],
  ] as const)("shows %s as an isolated exact-resource state without an issuance CTA", (status, message) => {
    mocks.state = workspaceState({ status });
    renderMembers();

    expect(screen.getByText(new RegExp(message))).toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: "招待する" }),
    ).not.toBeInTheDocument();
    expect(screen.getAllByText("Sumi Atelier")).toHaveLength(2);
    expect(screen.getByText(/00000161/)).toBeInTheDocument();
    expect(screen.getByText(/00000162/)).toBeInTheDocument();
  });

  it("revokes the exact pending invitation by its invitation identity", () => {
    mocks.state = workspaceState({
      status: "pending",
      invite: currentInvite,
    });
    renderMembers();

    fireEvent.click(
      screen.getByRole("button", {
        name: "Direct Chatの相手への招待を取り消す",
      }),
    );
    expect(mocks.revokeInvite).toHaveBeenCalledWith(CURRENT_INVITE_ID);
  });

  it("revokes an anonymous targeted record without treating it as the current PA", () => {
    mocks.state = workspaceState({
      status: "pending",
      invite: currentInvite,
    });
    renderMembers();

    fireEvent.click(
      screen.getByRole("button", {
        name: "人格エージェントへの招待 00000161 を取り消す",
      }),
    );
    expect(mocks.revokeInvite).toHaveBeenCalledWith(ANONYMOUS_INVITE_ID);
  });
});

describe("workspaceMutationErrorMessage", () => {
  it.each([
    ["last_administrator", "最後の管理者"],
    ["forbidden", "管理範囲"],
    ["owner_protected", "Workspace Owner"],
    ["membership_not_active", "参加状態はすでに終了"],
    ["conflict", "競合"],
  ])("maps canonical %s failures without flattening policy", (code, message) => {
    expect(
      workspaceMutationErrorMessage(new WorkspaceAPIError(code, 409)),
    ).toContain(message);
  });

  it("keeps unknown failures generic", () => {
    expect(
      workspaceMutationErrorMessage(new Error("database detail")),
    ).not.toContain("database detail");
  });
});
