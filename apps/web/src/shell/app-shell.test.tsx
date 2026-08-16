// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import {
  createMemoryHistory,
  createRootRoute,
  createRoute,
  createRouter,
  RouterProvider,
} from "@tanstack/react-router";
import { act, cleanup, render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";

const shellMarker = vi.hoisted(() => ({ mounts: 0 }));

vi.mock("../auth/auth-gate", () => ({
  AuthGate: ({ children }: { children: ReactNode }) => children,
}));

vi.mock("./app-rail", async () => {
  const { useEffect } = await import("react");
  return {
    AppRail: () => {
      useEffect(() => {
        shellMarker.mounts += 1;
      }, []);
      return <aside data-testid="shell-marker" />;
    },
  };
});

import { RootLayout } from "../routes/__root";

beforeEach(() => {
  vi.spyOn(window, "scrollTo").mockImplementation(() => undefined);
});

afterEach(() => {
  cleanup();
  shellMarker.mounts = 0;
  vi.restoreAllMocks();
});

it("keeps one authenticated shell across app route transitions", async () => {
  const rootRoute = createRootRoute({ component: RootLayout });
  const directRoute = createRoute({
    getParentRoute: () => rootRoute,
    path: "/direct",
    component: () => <main>direct</main>,
  });
  const messagingRoute = createRoute({
    getParentRoute: () => rootRoute,
    path: "/w/$workspaceId/messaging",
    component: () => <main>messaging</main>,
  });
  const router = createRouter({
    routeTree: rootRoute.addChildren([directRoute, messagingRoute]),
    history: createMemoryHistory({ initialEntries: ["/direct"] }),
  });
  await router.load();

  render(<RouterProvider router={router} />);

  const marker = screen.getByTestId("shell-marker");
  expect(screen.getByText("direct")).toBeInTheDocument();
  expect(shellMarker.mounts).toBe(1);

  await act(async () => {
    await router.navigate({
      to: "/w/$workspaceId/messaging",
      params: { workspaceId: "x" },
    });
  });

  expect(screen.getByText("messaging")).toBeInTheDocument();
  expect(screen.getByTestId("shell-marker")).toBe(marker);
  expect(shellMarker.mounts).toBe(1);

  await act(async () => {
    await router.navigate({ to: "/direct" });
  });

  expect(screen.getByText("direct")).toBeInTheDocument();
  expect(screen.getByTestId("shell-marker")).toBe(marker);
  expect(shellMarker.mounts).toBe(1);
});
