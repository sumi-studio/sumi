// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ParticipantAppsMenu } from "./app-menu";
import { useParticipantApps } from "./app-store";

const HUMAN_ID = "0198f0f4-9b72-7000-8000-000000000021";
const INSTALLATION_ID = "0198f0f4-9b72-7000-8000-000000000051";
const mocks = vi.hoisted(() => ({
  installApp: vi.fn(),
  navigate: vi.fn(),
  setInstallationState: vi.fn(),
  uninstallApp: vi.fn(),
}));

vi.mock("@tanstack/react-router", () => ({
  useNavigate: () => mocks.navigate,
}));

beforeEach(() => {
  vi.clearAllMocks();
  mocks.installApp.mockResolvedValue(undefined);
  mocks.setInstallationState.mockResolvedValue(undefined);
  mocks.uninstallApp.mockResolvedValue(undefined);
  useParticipantApps.setState({
    owner: {
      kind: "participant",
      participant: { kind: "human", humanId: HUMAN_ID },
    },
    status: "ready",
    catalog: [
      {
        appId: "direct-chat",
        displayName: "Direct Chat",
        workspaceOwnerAllowed: false,
        participantOwnerAllowed: true,
        workspaceRoleCapabilities: [],
      },
    ],
    installations: [],
    errorCode: null,
    mutation: null,
    installApp: mocks.installApp,
    setInstallationState: mocks.setInstallationState,
    uninstallApp: mocks.uninstallApp,
  });
});

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("ParticipantAppsMenu", () => {
  it("offers the Participant lifecycle without requiring a Workspace", async () => {
    render(<ParticipantAppsMenu />);
    fireEvent.click(screen.getByRole("button", { name: "個人用アプリ" }));
    fireEvent.click(await screen.findByRole("button", { name: "導入" }));

    await waitFor(() => {
      expect(mocks.installApp).toHaveBeenCalledWith("direct-chat");
    });
    expect(
      screen.getByText("Workspaceを切り替えても変わりません"),
    ).toBeInTheDocument();
  });

  it("keeps disable and uninstall as separate app-owned operations", async () => {
    useParticipantApps.setState({
      installations: [
        {
          installationId: INSTALLATION_ID,
          owner: {
            kind: "participant",
            participant: { kind: "human", humanId: HUMAN_ID },
          },
          appId: "direct-chat",
          state: "enabled",
          installedAt: 1,
          updatedAt: 2,
        },
      ],
    });
    vi.spyOn(window, "confirm").mockReturnValue(true);
    render(<ParticipantAppsMenu />);
    fireEvent.click(screen.getByRole("button", { name: "個人用アプリ" }));

    fireEvent.click(await screen.findByRole("button", { name: "無効化" }));
    fireEvent.click(
      screen.getByRole("button", {
        name: "Direct Chatをアンインストール",
      }),
    );

    await waitFor(() => {
      expect(mocks.setInstallationState).toHaveBeenCalledWith(
        INSTALLATION_ID,
        "disabled",
      );
      expect(mocks.uninstallApp).toHaveBeenCalledWith(INSTALLATION_ID);
    });
  });
});
