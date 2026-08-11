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
import { DirectChatGate } from "./direct-chat-gate";

const HUMAN_ID = "0198f0f4-9b72-7000-8000-000000000021";
const INSTALLATION_ID = "0198f0f4-9b72-7000-8000-000000000051";

const mocks = vi.hoisted(() => ({
  connection: "closed",
  installApp: vi.fn(),
  refresh: vi.fn(),
  refreshSession: vi.fn(),
  setInstallationState: vi.fn(),
}));

vi.mock("../agent/store", () => ({
  useConversation: (selector: (state: { connection: string }) => unknown) =>
    selector({ connection: mocks.connection }),
}));

vi.mock("../components/chat-screen", () => ({
  ChatScreen: ({ installationId }: { installationId: string }) => (
    <div data-testid="direct-chat" data-installation-id={installationId}>
      direct chat
    </div>
  ),
}));

vi.mock("../shell/app-rail", () => ({
  AppRail: () => <aside data-testid="app-rail" />,
}));

vi.mock("./auth-context", () => ({
  preissuedSessionMode: false,
  useAuth: () => ({
    authenticated: true,
    refreshSession: mocks.refreshSession,
    user: { id: HUMAN_ID },
  }),
}));

beforeEach(() => {
  mocks.connection = "closed";
  mocks.installApp.mockReset();
  mocks.installApp.mockResolvedValue(undefined);
  mocks.refresh.mockReset();
  mocks.refresh.mockResolvedValue(undefined);
  mocks.refreshSession.mockReset();
  mocks.refreshSession.mockResolvedValue("authenticated");
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
        appId: "direct-chat",
        displayName: "Direct Chat",
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

describe("DirectChatGate", () => {
  it("mounts the direct-chat connection only for the exact enabled installation", () => {
    useParticipantApps.setState({ installations: [installation("enabled")] });

    render(<DirectChatGate />);

    expect(screen.getByTestId("direct-chat")).toHaveAttribute(
      "data-installation-id",
      INSTALLATION_ID,
    );
    expect(screen.queryByTestId("app-rail")).not.toBeInTheDocument();
  });

  it("keeps a disabled app out of the chat and enables the exact installation", async () => {
    useParticipantApps.setState({ installations: [installation("disabled")] });

    render(<DirectChatGate />);
    fireEvent.click(screen.getByRole("button", { name: "有効にする" }));

    expect(screen.queryByTestId("direct-chat")).not.toBeInTheDocument();
    expect(screen.getByTestId("app-rail")).toBeInTheDocument();
    await waitFor(() => {
      expect(mocks.setInstallationState).toHaveBeenCalledWith(
        INSTALLATION_ID,
        "enabled",
      );
    });
  });

  it("offers the same Participant-owned install operation when no binding exists", async () => {
    render(<DirectChatGate />);
    fireEvent.click(screen.getByRole("button", { name: "直通を導入" }));

    await waitFor(() => {
      expect(mocks.installApp).toHaveBeenCalledWith("direct-chat");
    });
    expect(screen.queryByTestId("direct-chat")).not.toBeInTheDocument();
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

    render(<DirectChatGate />);

    expect(screen.queryByTestId("direct-chat")).not.toBeInTheDocument();
    expect(screen.getByText("直通を確認しています…")).toBeInTheDocument();
  });

  it("refreshes both session and exact app policy after a live close", async () => {
    useParticipantApps.setState({ installations: [installation("enabled")] });
    mocks.connection = "connecting";
    const view = render(<DirectChatGate />);
    mocks.connection = "closed";
    view.rerender(<DirectChatGate />);

    await waitFor(() => {
      expect(mocks.refreshSession).toHaveBeenCalledTimes(1);
      expect(mocks.refresh).toHaveBeenCalledTimes(1);
    });
  });
});

function installation(state: "enabled" | "disabled"): AppInstallation {
  return {
    installationId: INSTALLATION_ID,
    owner: {
      kind: "participant",
      participant: { kind: "human", humanId: HUMAN_ID },
    },
    appId: "direct-chat",
    state,
    installedAt: 1,
    updatedAt: 2,
  };
}
