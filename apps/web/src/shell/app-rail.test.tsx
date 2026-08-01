// @vitest-environment jsdom

import { TooltipProvider } from "@sumi/ui/components/tooltip";
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AppRail } from "./app-rail";

const mocks = vi.hoisted(() => ({
  logout: vi.fn(),
  navigate: vi.fn(),
  setTheme: vi.fn(),
}));

vi.mock("@tanstack/react-router", () => ({
  useNavigate: () => mocks.navigate,
}));

vi.mock("../auth/auth-context", () => ({
  useAuth: () => ({
    authenticated: true,
    logout: mocks.logout,
    user: {
      id: "01913f5e-7b8a-7abc-8def-0123456789ab",
      displayName: "Yohaku",
      email: "yohaku@example.com",
    },
  }),
}));

vi.mock("../auth/provider-settings", () => ({
  ProviderSettings: ({ humanId }: { humanId: string }) => (
    <div data-testid="provider-settings">providers:{humanId}</div>
  ),
}));

vi.mock("../theme/theme-provider", () => ({
  useTheme: () => ({ theme: "system", setTheme: mocks.setTheme }),
}));

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

describe("AppRail settings", () => {
  it("uses the direct-chat settings control with provider management", () => {
    render(
      <TooltipProvider>
        <AppRail activeAppId="home" />
      </TooltipProvider>,
    );

    fireEvent.click(screen.getByRole("button", { name: "設定" }));

    expect(screen.getByText("Yohaku")).toBeInTheDocument();
    expect(screen.getByTestId("provider-settings")).toHaveTextContent(
      "providers:01913f5e-7b8a-7abc-8def-0123456789ab",
    );
    expect(
      screen.getByRole("button", { name: "ログアウト" }),
    ).toBeInTheDocument();
  });
});
