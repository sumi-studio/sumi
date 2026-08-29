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

vi.mock("../auth/auth-context", () => ({
  useAuth: () => ({ user: { id: "human-1" } }),
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

import { ApiMessagingBackend } from "../messaging/api-backend";
import { bindMessagingSessionIdentity, useMessaging } from "../messaging/store";
import { RootLayout } from "../routes/__root";
import { useWorkspaceControl } from "../workspace/store";

beforeEach(() => {
  vi.spyOn(window, "scrollTo").mockImplementation(() => undefined);
  bindMessagingSessionIdentity("human-1");
  useWorkspaceControl.setState({
    selectedWorkspaceId: "workspace-1",
    selectionStatus: "ready",
    catalog: [
      {
        appId: "messaging",
        displayName: "Messaging",
        workspaceOwnerAllowed: true,
        participantOwnerAllowed: false,
        workspaceRoleCapabilities: [],
      },
    ],
    installations: [
      {
        installationId: "installation-1",
        owner: { kind: "workspace", workspaceId: "workspace-1" },
        appId: "messaging",
        state: "enabled",
        authorityEpoch: "1",
        installedAt: 1,
        updatedAt: 1,
      },
    ],
    members: [
      {
        workspaceMemberId: "member-1",
        workspaceId: "workspace-1",
        displayName: "Human",
        participant: { kind: "human", humanId: "human-1" },
        owner: true,
        roleIds: [],
        joinedAt: 1,
        leftAt: null,
      },
    ],
  });
  vi.spyOn(ApiMessagingBackend.prototype, "bootstrap").mockResolvedValue({
    self: { kind: "human", humanId: "human-1" },
    workspaces: [],
    channels: [],
    dms: [],
    members: [],
    statuses: [],
    readMarkers: [],
    unreadSummaries: [],
    replyLaterMarkers: [],
    notificationSetting: {
      owner: { kind: "human", humanId: "human-1" },
      defaults: { level: "all" },
      perPlace: [],
      keywords: [],
    },
    employedAgents: [],
  });
  vi.spyOn(ApiMessagingBackend.prototype, "subscribe").mockReturnValue(
    () => undefined,
  );
  vi.spyOn(
    ApiMessagingBackend.prototype,
    "subscribeConnection",
  ).mockReturnValue(() => undefined);
});

afterEach(() => {
  cleanup();
  shellMarker.mounts = 0;
  bindMessagingSessionIdentity(null);
  useWorkspaceControl.setState({
    selectedWorkspaceId: null,
    selectionStatus: "idle",
    catalog: [],
    installations: [],
    members: [],
  });
  vi.restoreAllMocks();
});

it("keeps one authenticated shell across app route transitions", async () => {
  const bootstrap = vi.mocked(ApiMessagingBackend.prototype.bootstrap);
  const dispose = vi.spyOn(ApiMessagingBackend.prototype, "dispose");
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
    history: createMemoryHistory({
      initialEntries: ["/w/workspace-1/messaging"],
    }),
  });
  await router.load();

  render(<RouterProvider router={router} />);

  const marker = screen.getByTestId("shell-marker");
  expect(screen.getByText("messaging")).toBeInTheDocument();
  expect(shellMarker.mounts).toBe(1);
  expect(bootstrap).toHaveBeenCalledTimes(1);

  await act(async () => {
    await router.navigate({ to: "/direct" });
  });

  expect(screen.getByText("direct")).toBeInTheDocument();
  expect(screen.getByTestId("shell-marker")).toBe(marker);
  expect(shellMarker.mounts).toBe(1);
  expect(bootstrap).toHaveBeenCalledTimes(1);
  expect(dispose).not.toHaveBeenCalled();

  await act(async () => {
    await router.navigate({
      to: "/w/$workspaceId/messaging",
      params: { workspaceId: "workspace-1" },
    });
  });

  expect(screen.getByText("messaging")).toBeInTheDocument();
  expect(screen.getByTestId("shell-marker")).toBe(marker);
  expect(shellMarker.mounts).toBe(1);
  expect(bootstrap).toHaveBeenCalledTimes(1);
  expect(dispose).not.toHaveBeenCalled();
  expect(useMessaging.getState().connection).not.toBe("reconnecting");
});
