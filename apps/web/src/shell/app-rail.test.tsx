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
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  AuthAPIError,
  SumiProfileUpdateIndeterminateError,
} from "../auth/session-client";
import { useParticipantApps } from "../participant/app-store";
import { AppRail } from "./app-rail";

const mocks = vi.hoisted(() => ({
  getSumiProfile: vi.fn(),
  logout: vi.fn(),
  navigate: vi.fn(),
  providerSettings: vi.fn(),
  refreshMessagingMemberProfiles: vi.fn(),
  setTheme: vi.fn(),
  updateProfile: vi.fn(),
}));

vi.mock("@tanstack/react-router", () => ({
  useNavigate: () => mocks.navigate,
}));

vi.mock("../auth/auth-context", () => ({
  preissuedSessionMode: false,
  useAuth: () => ({
    authenticated: true,
    logout: mocks.logout,
    updateProfile: mocks.updateProfile,
    user: {
      id: "01913f5e-7b8a-7abc-8def-0123456789ab",
      displayName: "Yohaku",
      email: "yohaku@example.com",
    },
  }),
}));

vi.mock("../auth/session-client", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../auth/session-client")>()),
  getSumiProfile: mocks.getSumiProfile,
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

beforeEach(() => {
  mocks.getSumiProfile.mockResolvedValue({
    participant: {
      kind: "human",
      humanId: "01913f5e-7b8a-7abc-8def-0123456789ab",
    },
    displayName: "Yohaku",
    tagline: "開発",
  });
  mocks.updateProfile.mockResolvedValue({
    participant: {
      kind: "human",
      humanId: "01913f5e-7b8a-7abc-8def-0123456789ab",
    },
    displayName: "たっけ",
    tagline: "開発",
  });
  mocks.refreshMessagingMemberProfiles.mockResolvedValue(undefined);
});

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
    render(
      <TooltipProvider>
        <AppRail activeAppId="home" />
      </TooltipProvider>,
    );
    fireEvent.click(screen.getByRole("button", { name: "設定" }));

    await screen.findByDisplayValue("Yohaku");

    fireEvent.change(screen.getByRole("textbox", { name: "表示名" }), {
      target: { value: " たっけ " },
    });
    fireEvent.click(screen.getByRole("button", { name: "保存" }));

    await waitFor(() => {
      expect(mocks.updateProfile).toHaveBeenCalledWith({
        displayName: "たっけ",
      });
      expect(mocks.refreshMessagingMemberProfiles).toHaveBeenCalledTimes(1);
    });
    expect(screen.getByText("保存しました。")).toBeInTheDocument();
    expect(screen.getByRole("textbox", { name: "表示名" })).toHaveValue(
      "たっけ",
    );
    expect(screen.getByRole("button", { name: "保存" })).toBeDisabled();
  });

  it("edits the Participant-global tagline without resending the display name", async () => {
    mocks.updateProfile.mockResolvedValue({
      participant: {
        kind: "human",
        humanId: "01913f5e-7b8a-7abc-8def-0123456789ab",
      },
      displayName: "Yohaku",
      tagline: "設計",
    });
    render(
      <TooltipProvider>
        <AppRail activeAppId="home" />
      </TooltipProvider>,
    );
    fireEvent.click(screen.getByRole("button", { name: "設定" }));
    await screen.findByDisplayValue("開発");

    fireEvent.change(screen.getByRole("textbox", { name: "ひとこと" }), {
      target: { value: " 設計 " },
    });
    fireEvent.click(screen.getByRole("button", { name: "保存" }));

    await waitFor(() => {
      expect(mocks.updateProfile).toHaveBeenCalledWith({ tagline: "設計" });
    });
    expect(screen.getByRole("textbox", { name: "表示名" })).toHaveValue(
      "Yohaku",
    );
    expect(screen.getByRole("textbox", { name: "ひとこと" })).toHaveValue(
      "設計",
    );
  });

  it("reports a failed profile read and retries in place", async () => {
    mocks.getSumiProfile.mockRejectedValueOnce(new Error("unavailable"));
    render(
      <TooltipProvider>
        <AppRail activeAppId="home" />
      </TooltipProvider>,
    );
    fireEvent.click(screen.getByRole("button", { name: "設定" }));

    expect(
      await screen.findByText("プロフィールを読み込めませんでした。"),
    ).toBeInTheDocument();
    expect(screen.getByRole("textbox", { name: "表示名" })).toBeDisabled();

    fireEvent.click(screen.getByRole("button", { name: "再試行" }));
    await screen.findByDisplayValue("Yohaku");
    expect(
      screen.queryByText("プロフィールを読み込めませんでした。"),
    ).not.toBeInTheDocument();
  });

  it("refreshes on reopen, follows untouched fields, and preserves a draft", async () => {
    render(
      <TooltipProvider>
        <AppRail activeAppId="home" />
      </TooltipProvider>,
    );
    const settings = screen.getByRole("button", { name: "設定" });
    fireEvent.click(settings);
    await screen.findByDisplayValue("開発");

    fireEvent.change(screen.getByRole("textbox", { name: "ひとこと" }), {
      target: { value: "書きかけ" },
    });
    fireEvent.click(settings);
    mocks.getSumiProfile.mockResolvedValueOnce({
      participant: {
        kind: "human",
        humanId: "01913f5e-7b8a-7abc-8def-0123456789ab",
      },
      displayName: "余白",
      tagline: "別タブの更新",
    });
    fireEvent.click(settings);

    await waitFor(() => {
      expect(mocks.getSumiProfile).toHaveBeenCalledTimes(2);
      expect(screen.getByRole("textbox", { name: "表示名" })).toHaveValue(
        "余白",
      );
      expect(screen.getByRole("textbox", { name: "ひとこと" })).toHaveValue(
        "書きかけ",
      );
    });
  });

  it("validates single-line profile text and clamps Unicode code points", async () => {
    render(
      <TooltipProvider>
        <AppRail activeAppId="home" />
      </TooltipProvider>,
    );
    fireEvent.click(screen.getByRole("button", { name: "設定" }));
    await screen.findByDisplayValue("開発");

    const tagline = screen.getByRole("textbox", { name: "ひとこと" });
    fireEvent.change(tagline, { target: { value: "one\u202etwo" } });
    expect(
      screen.getByText("ひとことは改行や制御文字を含めず入力してください。"),
    ).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "保存" })).toBeDisabled();

    fireEvent.change(tagline, { target: { value: "🌙".repeat(101) } });
    expect(tagline).toHaveValue("🌙".repeat(100));
    expect(screen.getByText(/100 \/ 100/)).toBeInTheDocument();
  });

  it("keeps a committed rename and explains a delayed messaging refresh", async () => {
    mocks.updateProfile.mockResolvedValue({
      participant: {
        kind: "human",
        humanId: "01913f5e-7b8a-7abc-8def-0123456789ab",
      },
      displayName: "かずい",
      tagline: "開発",
    });
    mocks.refreshMessagingMemberProfiles.mockRejectedValue(
      new Error("messaging unavailable"),
    );
    render(
      <TooltipProvider>
        <AppRail activeAppId="home" />
      </TooltipProvider>,
    );
    fireEvent.click(screen.getByRole("button", { name: "設定" }));
    await screen.findByDisplayValue("Yohaku");
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
      screen.queryByText("プロフィールを更新できませんでした。"),
    ).not.toBeInTheDocument();
  });

  it("distinguishes an indeterminate profile result from a rejected update", async () => {
    mocks.updateProfile.mockRejectedValue(
      new SumiProfileUpdateIndeterminateError(new TypeError("disconnected")),
    );
    render(
      <TooltipProvider>
        <AppRail activeAppId="home" />
      </TooltipProvider>,
    );
    fireEvent.click(screen.getByRole("button", { name: "設定" }));
    await screen.findByDisplayValue("Yohaku");
    fireEvent.change(screen.getByRole("textbox", { name: "表示名" }), {
      target: { value: "たっけ" },
    });
    fireEvent.click(screen.getByRole("button", { name: "保存" }));

    expect(
      await screen.findByText(
        "更新結果を確認できませんでした。再読み込みしてください。",
      ),
    ).toBeInTheDocument();
    expect(screen.getByRole("textbox", { name: "表示名" })).toHaveValue(
      "たっけ",
    );
    expect(screen.getByRole("button", { name: "保存" })).toBeEnabled();
    expect(screen.queryByText("保存しました。")).not.toBeInTheDocument();
  });

  it("keeps the draft after an authoritative profile rejection", async () => {
    mocks.updateProfile.mockRejectedValue(
      new AuthAPIError("invalid profile", 422),
    );
    render(
      <TooltipProvider>
        <AppRail activeAppId="home" />
      </TooltipProvider>,
    );
    fireEvent.click(screen.getByRole("button", { name: "設定" }));
    await screen.findByDisplayValue("Yohaku");
    fireEvent.change(screen.getByRole("textbox", { name: "表示名" }), {
      target: { value: "たっけ" },
    });
    fireEvent.click(screen.getByRole("button", { name: "保存" }));

    expect(
      await screen.findByText("プロフィールを更新できませんでした。"),
    ).toBeInTheDocument();
    expect(screen.getByRole("textbox", { name: "表示名" })).toHaveValue(
      "たっけ",
    );
    expect(screen.getByRole("button", { name: "保存" })).toBeEnabled();
    expect(screen.queryByText("保存しました。")).not.toBeInTheDocument();
  });
});
