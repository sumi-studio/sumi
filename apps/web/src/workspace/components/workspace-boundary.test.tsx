// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { WorkspaceControlState } from "../store";
import { WorkspaceBoundary } from "./workspace-boundary";

const mocks = vi.hoisted(() => ({
  bindMessagingScope: vi.fn(),
  init: vi.fn(),
  navigate: vi.fn(),
  refreshWorkspaces: vi.fn(),
  selectWorkspace: vi.fn(),
  state: {} as WorkspaceControlState,
}));

vi.mock("@tanstack/react-router", () => ({
  useNavigate: () => mocks.navigate,
}));

vi.mock("../../messaging/store", () => ({
  bindMessagingScope: mocks.bindMessagingScope,
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
  selectionStatus: WorkspaceControlState["selectionStatus"],
  selectedWorkspaceId: string | null = "workspace-1",
  listStatus: WorkspaceControlState["listStatus"] = "ready",
): WorkspaceControlState {
  return {
    sessionIdentity: "human-1",
    listStatus,
    selectionStatus,
    workspaces: [],
    selectedWorkspaceId,
    selectedWorkspace: null,
    members: [],
    roles: [],
    catalog: [],
    installations: [],
    invites: [],
    createdInviteSecret: null,
    errorCode: null,
    mutation: null,
    resetSession: vi.fn(),
    init: mocks.init,
    refreshWorkspaces: mocks.refreshWorkspaces,
    createWorkspace: vi.fn(),
    selectWorkspace: mocks.selectWorkspace,
    refreshSelectedWorkspace: vi.fn(),
    clearSelection: vi.fn(),
    updateWorkspace: vi.fn(),
    transferOwnership: vi.fn(),
    leaveWorkspace: vi.fn(),
    removeMember: vi.fn(),
    createInvite: vi.fn(),
    revokeInvite: vi.fn(),
    clearCreatedInviteSecret: vi.fn(),
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

beforeEach(() => {
  mocks.init.mockResolvedValue(undefined);
  mocks.refreshWorkspaces.mockResolvedValue(undefined);
  mocks.selectWorkspace.mockResolvedValue(undefined);
  mocks.state = workspaceState("error");
});

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

describe("WorkspaceBoundary", () => {
  it("retries a failed Workspace list once and selects the route only after recovery", () => {
    mocks.state = workspaceState("idle", null, "loading");
    const view = render(
      <WorkspaceBoundary workspaceId="workspace-1">
        <div>Workspace child</div>
      </WorkspaceBoundary>,
    );

    expect(mocks.init).toHaveBeenCalledOnce();
    expect(mocks.refreshWorkspaces).not.toHaveBeenCalled();
    expect(mocks.selectWorkspace).not.toHaveBeenCalled();

    act(() => {
      mocks.state = workspaceState("idle", null, "error");
      view.rerender(
        <WorkspaceBoundary workspaceId="workspace-1">
          <div>Workspace child</div>
        </WorkspaceBoundary>,
      );
    });
    expect(
      screen.getByRole("heading", { name: "Workspaceを読み込めませんでした" }),
    ).toBeInTheDocument();
    expect(mocks.init).toHaveBeenCalledOnce();
    expect(mocks.refreshWorkspaces).not.toHaveBeenCalled();
    expect(mocks.selectWorkspace).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole("button", { name: "再試行" }));
    expect(mocks.refreshWorkspaces).toHaveBeenCalledOnce();
    expect(mocks.selectWorkspace).not.toHaveBeenCalled();

    act(() => {
      mocks.state = workspaceState("idle", null, "loading");
      view.rerender(
        <WorkspaceBoundary workspaceId="workspace-1">
          <div>Workspace child</div>
        </WorkspaceBoundary>,
      );
    });
    act(() => {
      mocks.state = workspaceState("idle", null, "ready");
      view.rerender(
        <WorkspaceBoundary workspaceId="workspace-1">
          <div>Workspace child</div>
        </WorkspaceBoundary>,
      );
    });
    expect(mocks.refreshWorkspaces).toHaveBeenCalledOnce();
    expect(mocks.selectWorkspace).toHaveBeenCalledOnce();
    expect(mocks.selectWorkspace).toHaveBeenCalledWith("workspace-1");

    act(() => {
      mocks.state = workspaceState("error");
      view.rerender(
        <WorkspaceBoundary workspaceId="workspace-1">
          <div>Workspace child</div>
        </WorkspaceBoundary>,
      );
    });
    expect(mocks.refreshWorkspaces).toHaveBeenCalledOnce();
    expect(mocks.selectWorkspace).toHaveBeenCalledOnce();
  });

  it("keeps an exact error settled until manual retry and does not loop when retry fails", () => {
    const view = render(
      <WorkspaceBoundary workspaceId="workspace-1">
        <div>Workspace child</div>
      </WorkspaceBoundary>,
    );

    expect(
      screen.getByRole("heading", { name: "Workspaceを読み込めませんでした" }),
    ).toBeInTheDocument();
    expect(mocks.selectWorkspace).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole("button", { name: "再試行" }));
    expect(mocks.selectWorkspace).toHaveBeenCalledOnce();
    expect(mocks.selectWorkspace).toHaveBeenLastCalledWith("workspace-1");

    act(() => {
      mocks.state = workspaceState("loading");
      view.rerender(
        <WorkspaceBoundary workspaceId="workspace-1">
          <div>Workspace child</div>
        </WorkspaceBoundary>,
      );
    });
    act(() => {
      mocks.state = workspaceState("error");
      view.rerender(
        <WorkspaceBoundary workspaceId="workspace-1">
          <div>Workspace child</div>
        </WorkspaceBoundary>,
      );
    });

    expect(mocks.selectWorkspace).toHaveBeenCalledOnce();
  });

  it("keeps an exact invalid selection settled", () => {
    mocks.state = workspaceState("invalid");

    render(
      <WorkspaceBoundary workspaceId="workspace-1">
        <div>Workspace child</div>
      </WorkspaceBoundary>,
    );

    expect(
      screen.getByRole("heading", { name: "このWorkspaceを開けません" }),
    ).toBeInTheDocument();
    expect(mocks.selectWorkspace).not.toHaveBeenCalled();
  });

  it.each([
    ["an idle exact selection", "idle", "workspace-1"],
    ["a changed route", "error", "workspace-2"],
  ] as const)("auto-selects %s", (_case, selectionStatus, selectedWorkspaceId) => {
    mocks.state = workspaceState(selectionStatus, selectedWorkspaceId);

    render(
      <WorkspaceBoundary workspaceId="workspace-1">
        <div>Workspace child</div>
      </WorkspaceBoundary>,
    );

    expect(mocks.selectWorkspace).toHaveBeenCalledOnce();
    expect(mocks.selectWorkspace).toHaveBeenCalledWith("workspace-1");
  });
});
