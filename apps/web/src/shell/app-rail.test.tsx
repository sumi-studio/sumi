// @vitest-environment jsdom

import { TooltipProvider } from "@sumi/ui/components/tooltip";
import "@testing-library/jest-dom/vitest";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SumiProfileUpdateIndeterminateError } from "../auth/session-client";
import { useParticipantApps } from "../participant/app-store";
import { AppRail } from "./app-rail";

const mocks = vi.hoisted(() => ({
  logout: vi.fn(),
  navigate: vi.fn(),
  providerSettings: vi.fn(),
  refreshMessagingMemberProfiles: vi.fn(),
  setTheme: vi.fn(),
  updateDisplayName: vi.fn(),
}));

vi.mock("@tanstack/react-router", () => ({
  useNavigate: () => mocks.navigate,
}));

vi.mock("../auth/auth-context", () => ({
  preissuedSessionMode: false,
  useAuth: () => ({
    authenticated: true,
    logout: mocks.logout,
    updateDisplayName: mocks.updateDisplayName,
    user: {
      id: "01913f5e-7b8a-7abc-8def-0123456789ab",
      displayName: "Yohaku",
      email: "yohaku@example.com",
    },
  }),
}));

vi.mock("../auth/provider-settings", () => ({
  ProviderSettings: ({ humanId }: { humanId: string }) => {
    mocks.providerSettings(humanId);
    return <div data-testid="provider-settings">provider settings</div>;
  },
}));

vi.mock("../messaging/store", () => ({
  refreshMessagingMemberProfiles: mocks.refreshMessagingMemberProfiles,
}));

vi.mock("../theme/theme-provider", () => ({
  useTheme: () => ({ theme: "system", setTheme: mocks.setTheme }),
}));

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
  useParticipantApps.setState({
    owner: null,
    status: "idle",
    catalog: [],
    installations: [],
    errorCode: null,
    mutation: null,
    coordination: "document-only",
  });
});

describe("AppRail settings", () => {
  it("shows a Participant-owned rail only for its exact enabled installation", () => {
    useParticipantApps.setState({
      owner: {
        kind: "participant",
        participant: {
          kind: "human",
          humanId: "01913f5e-7b8a-7abc-8def-0123456789ab",
        },
      },
      coordination: "document-only",
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
      installations: [
        {
          installationId: "0198f0f4-9b72-7000-8000-000000000051",
          owner: {
            kind: "participant",
            participant: {
              kind: "human",
              humanId: "01913f5e-7b8a-7abc-8def-0123456789ab",
            },
          },
          appId: "direct-chat",
          state: "enabled",
          authorityEpoch: "1",
          installedAt: 1,
          updatedAt: 2,
        },
      ],
    });
    const { rerender } = render(
      <TooltipProvider>
        <AppRail activeAppId="workspace" />
      </TooltipProvider>,
    );

    expect(screen.getByRole("button", { name: "直通" })).toBeInTheDocument();

    useParticipantApps.setState({
      installations: useParticipantApps
        .getState()
        .installations.map((installation) => ({
          ...installation,
          state: "disabled",
        })),
    });
    rerender(
      <TooltipProvider>
        <AppRail activeAppId="workspace" />
      </TooltipProvider>,
    );
    expect(
      screen.queryByRole("button", { name: "直通" }),
    ).not.toBeInTheDocument();
  });

  it("uses the direct-chat settings control with provider management", () => {
    render(
      <TooltipProvider>
        <AppRail activeAppId="home" />
      </TooltipProvider>,
    );

    fireEvent.click(screen.getByRole("button", { name: "設定" }));

    expect(screen.getByText("Yohaku")).toBeInTheDocument();
    expect(mocks.providerSettings).toHaveBeenCalledWith(
      "01913f5e-7b8a-7abc-8def-0123456789ab",
    );
    expect(
      screen.queryByText("01913f5e-7b8a-7abc-8def-0123456789ab"),
    ).not.toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "ログアウト" }),
    ).toBeInTheDocument();
  });

  it("edits the canonical Human display name from the shared settings form", async () => {
    mocks.updateDisplayName.mockResolvedValue(undefined);
    mocks.refreshMessagingMemberProfiles.mockResolvedValue(undefined);
    render(
      <TooltipProvider>
        <AppRail activeAppId="home" />
      </TooltipProvider>,
    );
    fireEvent.click(screen.getByRole("button", { name: "設定" }));

    fireEvent.change(screen.getByRole("textbox", { name: "表示名" }), {
      target: { value: " たっけ " },
    });
    fireEvent.click(screen.getByRole("button", { name: "保存" }));

    await waitFor(() => {
      expect(mocks.updateDisplayName).toHaveBeenCalledWith("たっけ");
      expect(mocks.refreshMessagingMemberProfiles).toHaveBeenCalledTimes(1);
    });
  });

  it("keeps a committed rename and explains a delayed messaging refresh", async () => {
    mocks.updateDisplayName.mockResolvedValue(undefined);
    mocks.refreshMessagingMemberProfiles.mockRejectedValue(
      new Error("messaging unavailable"),
    );
    render(
      <TooltipProvider>
        <AppRail activeAppId="home" />
      </TooltipProvider>,
    );
    fireEvent.click(screen.getByRole("button", { name: "設定" }));
    fireEvent.change(screen.getByRole("textbox", { name: "表示名" }), {
      target: { value: "かずい" },
    });
    fireEvent.click(screen.getByRole("button", { name: "保存" }));

    expect(
      await screen.findByText(
        "保存済み。トークの表示は再読み込みで反映されます。",
      ),
    ).toBeInTheDocument();
    expect(
      screen.queryByText("表示名を更新できませんでした。"),
    ).not.toBeInTheDocument();
  });

  it("distinguishes an indeterminate profile result from a rejected update", async () => {
    mocks.updateDisplayName.mockRejectedValue(
      new SumiProfileUpdateIndeterminateError(new TypeError("disconnected")),
    );
    render(
      <TooltipProvider>
        <AppRail activeAppId="home" />
      </TooltipProvider>,
    );
    fireEvent.click(screen.getByRole("button", { name: "設定" }));
    fireEvent.change(screen.getByRole("textbox", { name: "表示名" }), {
      target: { value: "たっけ" },
    });
    fireEvent.click(screen.getByRole("button", { name: "保存" }));

    expect(
      await screen.findByText(
        "更新結果を確認できませんでした。再読み込みしてください。",
      ),
    ).toBeInTheDocument();
  });
});
