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

vi.mock("../messaging/components/messaging-screen", () => ({
  MessagingScreen: () => <main>場所を選択</main>,
}));

import { HomeRoute } from "./index";

describe("messaging home route", () => {
  it("renders an unselected home instead of guessing the first channel", () => {
    render(<HomeRoute />);

    expect(screen.getByRole("main")).toHaveTextContent("場所を選択");
  });
});
