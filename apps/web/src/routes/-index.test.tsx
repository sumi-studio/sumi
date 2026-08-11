// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";

vi.mock("@tanstack/react-router", () => ({
  createFileRoute: () => (options: unknown) => options,
}));

vi.mock("../auth/auth-gate", () => ({
  AuthGate: ({ children }: { children: ReactNode }) => children,
}));

vi.mock("../workspace/components/workspace-landing", () => ({
  WorkspaceLanding: () => <main>Workspaceを選択</main>,
}));

import { HomeRoute } from "./index";

describe("Workspace home route", () => {
  it("renders the explicit Workspace control-plane entry", () => {
    render(<HomeRoute />);

    expect(screen.getByRole("main")).toHaveTextContent("Workspaceを選択");
  });
});
