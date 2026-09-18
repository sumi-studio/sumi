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
import { useParticipantApps } from "../participant/app-store";
import type { AppInstallation } from "../workspace/model";
import { TerminalGate } from "./terminal-gate";

const HUMAN_ID = "0198f0f4-9b72-7000-8000-000000000021";
const INSTALLATION_ID = "0198f0f4-9b72-7000-8000-000000000061";

const mocks = vi.hoisted(() => ({
  installApp: vi.fn(),
  refresh: vi.fn(),
  setInstallationState: vi.fn(),
}));

vi.mock("./terminal-screen", () => ({
  TerminalScreen: ({
    installationId,
    authorityEpoch,
  }: {
    installationId: string;
    authorityEpoch: string;
  }) => (
    <div
      data-testid="terminal-screen"
      data-installation-id={installationId}
      data-authority-epoch={authorityEpoch}
    >
      terminal screen
    </div>
  ),
}));

vi.mock("../auth/auth-context", () => ({
  preissuedSessionMode: false,
  useAuth: () => ({
    authenticated: true,
    user: { id: HUMAN_ID },
  }),
}));

beforeEach(() => {
  mocks.installApp.mockReset();
  mocks.installApp.mockResolvedValue(undefined);
  mocks.refresh.mockReset();
  mocks.refresh.mockResolvedValue(undefined);
  mocks.setInstallationState.mockReset();
  mocks.setInstallationState.mockResolvedValue(undefined);
  useParticipantApps.setState({
    owner: {
      kind: "participant",
      participant: { kind: "human", humanId: HUMAN_ID },
    },
    status: "ready",
    catalog: [
      {
        appId: "terminal",
        displayName: "Terminal",
        workspaceOwnerAllowed: false,
        participantOwnerAllowed: true,
        workspaceRoleCapabilities: [],
      },
    ],
    installations: [],
    mutation: null,
    errorCode: null,
    refresh: mocks.refresh,
    installApp: mocks.installApp,
    setInstallationState: mocks.setInstallationState,
  });
});

afterEach(cleanup);

describe("TerminalGate", () => {
  it("mounts the terminal only for the exact enabled installation", () => {
    useParticipantApps.setState({ installations: [installation("enabled")] });

    render(<TerminalGate />);

    expect(screen.getByTestId("terminal-screen")).toHaveAttribute(
      "data-installation-id",
      INSTALLATION_ID,
    );
    expect(screen.getByTestId("terminal-screen")).toHaveAttribute(
      "data-authority-epoch",
      "1",
    );
  });

  it("keeps a disabled app out of the terminal and enables the exact installation", async () => {
    useParticipantApps.setState({ installations: [installation("disabled")] });

    render(<TerminalGate />);
    fireEvent.click(screen.getByRole("button", { name: "有効にする" }));

    expect(screen.queryByTestId("terminal-screen")).not.toBeInTheDocument();
    expect(
      screen.getByText("ターミナルは無効になっています"),
    ).toBeInTheDocument();
    await waitFor(() => {
      expect(mocks.setInstallationState).toHaveBeenCalledWith(
        INSTALLATION_ID,
        "enabled",
      );
    });
  });

  it("offers the same Participant-owned install operation when no binding exists", async () => {
    render(<TerminalGate />);
    fireEvent.click(screen.getByRole("button", { name: "ターミナルを導入" }));

    await waitFor(() => {
      expect(mocks.installApp).toHaveBeenCalledWith("terminal");
    });
    expect(screen.queryByTestId("terminal-screen")).not.toBeInTheDocument();
  });

  it("refuses a duplicate installation rather than picking one", () => {
    useParticipantApps.setState({
      installations: [
        installation("enabled"),
        {
          ...installation("enabled"),
          installationId: `${INSTALLATION_ID.slice(0, -1)}f`,
        },
      ],
    });

    render(<TerminalGate />);

    expect(screen.queryByTestId("terminal-screen")).not.toBeInTheDocument();
    expect(
      screen.getByText("ターミナルの導入状態を修復してください"),
    ).toBeInTheDocument();
  });

  it("does not use a previous Human's enabled installation", () => {
    useParticipantApps.setState({
      owner: {
        kind: "participant",
        participant: {
          kind: "human",
          humanId: "0198f0f4-9b72-7000-8000-000000000099",
        },
      },
      installations: [installation("enabled")],
    });

    render(<TerminalGate />);

    expect(screen.queryByTestId("terminal-screen")).not.toBeInTheDocument();
    expect(screen.getByText("ターミナルを確認しています…")).toBeInTheDocument();
  });

  it("shows a retry path when the catalog cannot be read", () => {
    useParticipantApps.setState({ status: "error" });

    render(<TerminalGate />);
    fireEvent.click(screen.getByRole("button", { name: "再試行" }));

    expect(mocks.refresh).toHaveBeenCalledTimes(1);
    expect(screen.queryByTestId("terminal-screen")).not.toBeInTheDocument();
  });
});

function installation(state: "enabled" | "disabled"): AppInstallation {
  return {
    installationId: INSTALLATION_ID,
    owner: {
      kind: "participant",
      participant: { kind: "human", humanId: HUMAN_ID },
    },
    appId: "terminal",
    state,
    authorityEpoch: "1",
    installedAt: 1,
    updatedAt: 2,
  };
}
