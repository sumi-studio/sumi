// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import {
  createMemoryHistory,
  createRootRoute,
  createRoute,
  createRouter,
  Outlet,
  RouterProvider,
} from "@tanstack/react-router";
import { act, cleanup, render, screen } from "@testing-library/react";
import { useEffect } from "react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import {
  authOutcomeNoticeCopy,
  authOutcomeNoticeReadingMilliseconds,
} from "./auth-outcome-notice";
import { AuthOutcomeNoticeHost } from "./auth-outcome-notice-host";
import type { AuthOutcomeNotice as AuthOutcomeNoticeState } from "./auth-outcome-notice-state";

const notice: AuthOutcomeNoticeState = {
  version: 1,
  firebaseUID: "firebase-user",
  humanId: "human-user",
  receiptId: "terminal-receipt",
  createdAt: "2026-08-01T00:00:00.000Z",
  expiresAt: "2026-08-01T00:10:00.000Z",
  outcome: "signed_in",
  intent: "sign_in",
  intentTransition: "none",
};

const authMocks = vi.hoisted(() => ({
  dismissOutcomeNotice: vi.fn(),
  outcomeNotice: null as AuthOutcomeNoticeState | null,
}));

vi.mock("./auth-context", () => ({
  useAuth: () => ({
    canUseDirectChat: true,
    dismissOutcomeNotice: authMocks.dismissOutcomeNotice,
    emailLinkCallbackPending: false,
    outcomeNotice: authMocks.outcomeNotice,
  }),
}));

beforeEach(() => {
  vi.useFakeTimers();
  vi.spyOn(window, "scrollTo").mockImplementation(() => undefined);
  Object.defineProperty(document, "visibilityState", {
    configurable: true,
    value: "visible",
  });
  vi.spyOn(document, "hasFocus").mockReturnValue(true);
  authMocks.outcomeNotice = null;
});

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  vi.useRealTimers();
});

it("keeps one outcome announcement and reading lifetime across a real route transition", async () => {
  const homeUnmounted = vi.fn();
  const rootRoute = createRootRoute({ component: Outlet });
  const indexRoute = createRoute({
    getParentRoute: () => rootRoute,
    path: "/",
    component: () => <RouteMarker label="home" onUnmount={homeUnmounted} />,
  });
  const channelRoute = createRoute({
    getParentRoute: () => rootRoute,
    path: "/w/$workspaceId/messaging/c/$channelId",
    component: () => <RouteMarker label="channel" />,
  });
  const router = createRouter({
    routeTree: rootRoute.addChildren([indexRoute, channelRoute]),
    history: createMemoryHistory({ initialEntries: ["/"] }),
  });
  await router.load();

  const app = () => (
    <>
      <AuthOutcomeNoticeHost />
      <RouterProvider router={router} />
    </>
  );
  const view = render(app());

  expect(screen.getByText("home")).toBeInTheDocument();
  const liveRegion = screen.getByRole("status");
  expect(liveRegion).toBeEmptyDOMElement();

  authMocks.outcomeNotice = notice;
  view.rerender(app());
  const noticeSurface = screen.getByTestId("auth-outcome-notice");
  expect(screen.getByRole("status")).toBe(liveRegion);
  const readingDuration = authOutcomeNoticeReadingMilliseconds(
    authOutcomeNoticeCopy(notice),
  );
  const elapsedBeforeNavigation = Math.floor(readingDuration / 2);
  await advance(elapsedBeforeNavigation);

  const observer = new MutationObserver(() => undefined);
  observer.observe(liveRegion, {
    childList: true,
    characterData: true,
    subtree: true,
  });

  await act(async () => {
    await router.navigate({
      to: "/w/$workspaceId/messaging/c/$channelId",
      params: { channelId: "general", workspaceId: "workspace-1" },
    });
  });

  expect(screen.getByText("channel")).toBeInTheDocument();
  expect(homeUnmounted).toHaveBeenCalledOnce();
  expect(screen.getByRole("status")).toBe(liveRegion);
  expect(screen.getByTestId("auth-outcome-notice")).toBe(noticeSurface);
  expect(observer.takeRecords()).toEqual([]);
  observer.disconnect();

  await advance(readingDuration - elapsedBeforeNavigation - 1);
  expect(noticeSurface).not.toHaveAttribute("data-exiting");
  await advance(1);
  expect(noticeSurface).toHaveAttribute("data-exiting", "true");
});

function RouteMarker({
  label,
  onUnmount,
}: {
  label: string;
  onUnmount?: () => void;
}) {
  useEffect(() => () => onUnmount?.(), [onUnmount]);
  return <div>{label}</div>;
}

async function advance(milliseconds: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(milliseconds);
  });
}
