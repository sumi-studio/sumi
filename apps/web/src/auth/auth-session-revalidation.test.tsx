// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { clearDirectChatAuthority } from "../agent/auth-authority";
import { useConversation } from "../agent/store";
import { Composer } from "../messaging/components/composer";
import { bindMessagingSessionIdentity, useMessaging } from "../messaging/store";
import { useParticipantApps } from "../participant/app-store";
import { bindWorkspaceSessionIdentity } from "../workspace/store";
import { AuthProvider, useAuth } from "./auth-context";
import { AuthGate } from "./auth-gate";
import { DirectChatGate } from "./direct-chat-gate";
import { AuthAPIError, type SumiSessionStatus } from "./session-client";

const mocks = vi.hoisted(() => ({
  getSumiSession: vi.fn(),
  logoutSumiSession: vi.fn(),
  releaseConnection: vi.fn(),
  refreshApps: vi.fn(),
}));

vi.mock("./session-client", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./session-client")>()),
  getSumiSession: mocks.getSumiSession,
  logoutSumiSession: mocks.logoutSumiSession,
}));
vi.mock("./login-screen", () => ({
  LoginScreen: () => <div>Signed out</div>,
}));

const sessionA: SumiSessionStatus = {
  authenticated: true,
  authorityBindingId: "A".repeat(43),
  user: { id: "human-a", displayName: "A" },
};
const sessionB: SumiSessionStatus = {
  authenticated: true,
  authorityBindingId: "B".repeat(43),
  user: { id: "human-b", displayName: "B" },
};
const placeKey = "channel:channel-a";
const resetConversationAuthority = useConversation.getState().resetAuthority;
const attachment = {
  clientNonce: "attachment-a",
  filename: "draft.txt",
  sizeBytes: 5,
  contentType: "text/plain",
  status: "uploading" as const,
};

function SessionControls() {
  const auth = useAuth();
  return (
    <>
      <span data-testid="session-user">{auth.user?.id ?? "none"}</span>
      <button type="button" onClick={() => void auth.refreshSession()}>
        Recheck session
      </button>
      <button type="button" onClick={() => void auth.logout()}>
        Sign out
      </button>
    </>
  );
}

beforeEach(() => {
  vi.clearAllMocks();
  sessionStorage.clear();
  clearDirectChatAuthority();
  bindWorkspaceSessionIdentity(null, null);
  bindMessagingSessionIdentity(null);
  mocks.getSumiSession.mockResolvedValue(sessionA);
  mocks.logoutSumiSession.mockResolvedValue(undefined);
  mocks.refreshApps.mockResolvedValue(undefined);
  // Only the transport is substituted. AuthProvider, both gates, the direct
  // chat textarea, Messaging composer, and their private state are real.
  useConversation.setState({
    acquireConnection: () => mocks.releaseConnection,
  });
  useParticipantApps.setState({
    owner: {
      kind: "participant",
      participant: { kind: "human", humanId: "human-a" },
    },
    status: "ready",
    installations: [
      {
        installationId: "direct-chat-a",
        owner: {
          kind: "participant",
          participant: { kind: "human", humanId: "human-a" },
        },
        appId: "direct-chat",
        state: "enabled",
        authorityEpoch: "1",
        installedAt: 1,
        updatedAt: 1,
      },
    ],
    refresh: mocks.refreshApps,
  });
  vi.stubGlobal(
    "ResizeObserver",
    class {
      observe() {}
      unobserve() {}
      disconnect() {}
    },
  );
});

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  useConversation.setState({ resetAuthority: resetConversationAuthority });
  clearDirectChatAuthority();
  bindWorkspaceSessionIdentity(null, null);
  bindMessagingSessionIdentity(null);
  vi.unstubAllGlobals();
});

async function openWorkspaceWithDrafts() {
  render(
    <AuthProvider>
      <SessionControls />
      <AuthGate>
        <DirectChatGate />
        <Composer />
      </AuthGate>
    </AuthProvider>,
  );
  const directDraft = await screen.findByRole("textbox", {
    name: "メッセージ",
  });
  act(() => {
    useConversation.setState({ connection: "connected", ready: "ready" });
    useMessaging.setState({
      sendTyping: vi.fn(),
      activePlaceKey: placeKey,
      channels: [
        {
          channelId: "channel-a",
          workspaceId: "workspace-a",
          revision: 1,
          name: "general",
          topic: "",
          visibility: "public",
          voice: false,
        },
      ],
      draftAttachmentsByPlace: { [placeKey]: [attachment] },
    });
  });
  fireEvent.change(directDraft, { target: { value: "Private direct draft" } });
  fireEvent.change(
    screen.getByRole("textbox", { name: "#general へメッセージ" }),
    {
      target: { value: "Private workspace draft" },
    },
  );
  return directDraft;
}

function expectDraftsPreserved(directDraft: HTMLElement) {
  expect(screen.getByRole("textbox", { name: "メッセージ" })).toBe(directDraft);
  expect(directDraft).toHaveValue("Private direct draft");
  expect(
    screen.getByRole("textbox", { name: "#general へメッセージ" }),
  ).toHaveValue("Private workspace draft");
  expect(screen.getByText("draft.txt")).toBeInTheDocument();
  expect(useMessaging.getState().draftAttachmentsByPlace[placeKey]?.[0]).toBe(
    attachment,
  );
  expect(mocks.releaseConnection).not.toHaveBeenCalled();
}

describe("authenticated session revalidation", () => {
  it("requires an authenticated session before opening the workspace initially", async () => {
    mocks.getSumiSession.mockRejectedValueOnce(
      new AuthAPIError("unavailable", 503),
    );
    render(
      <AuthProvider>
        <AuthGate>
          <DirectChatGate />
        </AuthGate>
      </AuthProvider>,
    );
    await screen.findByText("Sumiに接続できません");
    expect(
      screen.queryByRole("textbox", { name: "メッセージ" }),
    ).not.toBeInTheDocument();
  });

  it("keeps both composers and an attachment through socket loss, a failed read, and reconnect", async () => {
    const directDraft = await openWorkspaceWithDrafts();
    let rejectRead!: (reason: unknown) => void;
    mocks.getSumiSession.mockReturnValueOnce(
      new Promise<SumiSessionStatus>((_resolve, reject) => {
        rejectRead = reject;
      }),
    );
    act(() => useConversation.setState({ connection: "closed" }));
    await waitFor(() => expect(mocks.getSumiSession).toHaveBeenCalledTimes(2));
    expectDraftsPreserved(directDraft);

    await act(async () =>
      rejectRead(new AuthAPIError("upstream unavailable", 503)),
    );
    expectDraftsPreserved(directDraft);

    fireEvent.click(screen.getByRole("button", { name: "Recheck session" }));
    await waitFor(() => expect(mocks.getSumiSession).toHaveBeenCalledTimes(3));
    act(() =>
      useConversation.setState({ connection: "connected", ready: "ready" }),
    );
    expectDraftsPreserved(directDraft);
  });

  it.each([
    401,
    403,
    "signed-out",
  ])("clears private state when the server confirms %s", async (response) => {
    const directDraft = await openWorkspaceWithDrafts();
    if (typeof response === "number") {
      mocks.getSumiSession.mockRejectedValueOnce(
        new AuthAPIError("revoked", response),
      );
    } else {
      mocks.getSumiSession.mockResolvedValueOnce({ authenticated: false });
    }
    act(() => useConversation.setState({ connection: "closed" }));
    await screen.findByText("Signed out");
    expect(directDraft).not.toBeInTheDocument();
    expect(useMessaging.getState().draftByPlace).toEqual({});
    expect(useMessaging.getState().draftAttachmentsByPlace).toEqual({});
    expect(mocks.releaseConnection).toHaveBeenCalledOnce();
  });

  it("discards previous private drafts when revalidation changes the identity", async () => {
    const directDraft = await openWorkspaceWithDrafts();
    mocks.getSumiSession.mockResolvedValueOnce(sessionB);
    act(() => useConversation.setState({ connection: "closed" }));
    await waitFor(() =>
      expect(screen.getByTestId("session-user")).toHaveTextContent("human-b"),
    );
    expect(directDraft).not.toBeInTheDocument();
    expect(useMessaging.getState().draftByPlace).toEqual({});
    expect(useMessaging.getState().draftAttachmentsByPlace).toEqual({});
    expect(screen.queryByText("draft.txt")).not.toBeInTheDocument();
  });

  it("does not retain the old workspace when a new authority cannot clear private state", async () => {
    const directDraft = await openWorkspaceWithDrafts();
    vi.spyOn(useConversation.getState(), "resetAuthority").mockReturnValue(
      false,
    );
    mocks.getSumiSession.mockResolvedValueOnce(sessionB);
    act(() => useConversation.setState({ connection: "closed" }));
    await screen.findByText("Sumiに接続できません");
    expect(directDraft).not.toBeInTheDocument();
    expect(screen.getByTestId("session-user")).toHaveTextContent("none");
    expect(useMessaging.getState().draftAttachmentsByPlace).toEqual({});
  });

  it("does not let a late successful recheck restore a logged-out workspace", async () => {
    await openWorkspaceWithDrafts();
    let finishRead!: (value: SumiSessionStatus) => void;
    mocks.getSumiSession.mockReturnValueOnce(
      new Promise<SumiSessionStatus>((resolve) => {
        finishRead = resolve;
      }),
    );
    act(() => useConversation.setState({ connection: "closed" }));
    fireEvent.click(screen.getByRole("button", { name: "Sign out" }));
    await screen.findByText("Signed out");
    await act(async () => finishRead(sessionA));
    expect(screen.getByText("Signed out")).toBeInTheDocument();
    expect(useMessaging.getState().draftAttachmentsByPlace).toEqual({});
  });
});
