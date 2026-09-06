// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { TooltipProvider } from "@sumi/ui/components/tooltip";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { useLayoutEffect, useState } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { clearDirectChatAuthority } from "../agent/auth-authority";
import { useConversation } from "../agent/store";
import { SettingsPopover } from "../components/app-navigation";
import { Composer } from "../messaging/components/composer";
import { ConnectionBanner } from "../messaging/components/connection-banner";
import { MessageEditor } from "../messaging/components/message-editor";
import { MessagingScreen } from "../messaging/components/messaging-screen";
import { MockMessagingServer } from "../messaging/mock-server";
import type {
  Message,
  SendMessageInput,
  SendReceipt,
  UploadAttachmentInput,
  UploadAttachmentReceipt,
} from "../messaging/model";
import { MESSAGING_SCOPE } from "../messaging/scope.test-support";
import {
  bindMessagingScope,
  bindMessagingSessionIdentity,
  getMessagingScope,
  installMessagingBackend,
  resumeMessagingTransport,
  useMessaging,
} from "../messaging/store";
import { ParticipantAppBinding } from "../participant/app-binding";
import { useParticipantApps } from "../participant/app-store";
import { ThemeProvider } from "../theme/theme-provider";
import { WorkspaceLanding } from "../workspace/components/workspace-landing";
import {
  bindWorkspaceSessionIdentity,
  useWorkspaceControl,
} from "../workspace/store";
import { AuthProvider, useAuth } from "./auth-context";
import { AuthGate } from "./auth-gate";
import { DirectChatGate } from "./direct-chat-gate";
import { AuthAPIError, type SumiSessionStatus } from "./session-client";

const mocks = vi.hoisted(() => ({
  getSession: vi.fn(),
  getProfile: vi.fn(),
  logout: vi.fn(),
  release: vi.fn(),
}));
vi.mock("./session-client", async (original) => ({
  ...(await original<typeof import("./session-client")>()),
  getSumiSession: mocks.getSession,
  getSumiProfile: mocks.getProfile,
  logoutSumiSession: mocks.logout,
}));
vi.mock("./provider-settings", () => ({ ProviderSettings: () => null }));
vi.mock("../participant/app-menu", () => ({ ParticipantAppsMenu: () => null }));
vi.mock("./login-screen", () => ({ LoginScreen: () => <div>Signed out</div> }));
vi.mock("@tanstack/react-router", async (original) => ({
  ...(await original<typeof import("@tanstack/react-router")>()),
  useNavigate: () => vi.fn(),
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
const placeKey = "channel:ch-general";
const originalAcquire = useConversation.getState().acquireConnection;
let uploads: UploadAttachmentInput[];
let bootstrapFailures = 0;
let uploadResponses: ReturnType<typeof deferred<UploadAttachmentReceipt>>[];
let sends: SendMessageInput[];
let editResponse: ReturnType<typeof deferred<Message>>;

class UploadServer extends MockMessagingServer {
  override async bootstrap() {
    if (bootstrapFailures > 0) {
      bootstrapFailures -= 1;
      throw new Error("bootstrap unavailable");
    }
    const snapshot = await super.bootstrap();
    return {
      ...snapshot,
      self: { kind: "human" as const, humanId: "human-a" },
    };
  }
  override uploadAttachment(
    input: UploadAttachmentInput,
  ): Promise<UploadAttachmentReceipt> {
    uploads.push(input);
    const response = deferred<UploadAttachmentReceipt>();
    uploadResponses.push(response);
    return response.promise;
  }
  override sendMessage(input: SendMessageInput): Promise<SendReceipt> {
    sends.push(input);
    return new Promise(() => {});
  }
  override editMessage(): Promise<Message> {
    return editResponse.promise;
  }
}

// Substitute only the remote Messaging transport. On each effect activation,
// rebind the same installation exactly as the real shell transport does.
function ScopedComposers() {
  const ready = useMessaging((state) => state.ready);
  const activePlaceKey = useMessaging((state) => state.activePlaceKey);
  useLayoutEffect(() => {
    bindMessagingScope(MESSAGING_SCOPE);
    resumeMessagingTransport();
    installMessagingBackend(new UploadServer());
    useMessaging.getState().init();
    useWorkspaceControl.setState({ listStatus: "ready" });
  }, []);
  useLayoutEffect(() => {
    if (ready && activePlaceKey !== placeKey)
      useMessaging.getState().selectPlace(placeKey);
  }, [ready, activePlaceKey]);
  if (!ready) return <MessagingScreen />;
  return (
    <>
      <ConnectionBanner />
      <DirectChatGate />
      <Composer />
      <ActiveEditor />
      <WorkspaceLanding />
      <LogoutControl />
      <SettingsPopover />
    </>
  );
}

function ActiveEditor() {
  const state = useMessaging();
  if (!state.editSession) return null;
  return (
    <MessageEditor
      value={state.editDraft}
      onChange={state.setEditDraft}
      onSubmit={state.submitEdit}
      onCancel={state.cancelEdit}
      conflict={state.editConflict}
      failure={state.editFailure}
      saving={state.editSession.submittedDraft !== null}
      openedToken={state.editSession.openedToken}
      onReloadConflict={state.reloadEditConflict}
      membersByKey={state.membersByKey}
      selfKey={state.selfKey}
    />
  );
}

function LogoutControl() {
  const { logout } = useAuth();
  const [error, setError] = useState("");
  return (
    <>
      <button
        type="button"
        onClick={() => void logout().catch(() => setError("Logout failed"))}
      >
        Sign out
      </button>
      {error && <p role="alert">{error}</p>}
    </>
  );
}
function SessionProbe() {
  const auth = useAuth();
  return (
    <>
      <span data-testid="session-state">{auth.sessionState}</span>
      <span data-testid="session-user">{auth.user?.id ?? "none"}</span>
      <button type="button" onClick={() => void auth.refreshSession()}>
        Recheck
      </button>
    </>
  );
}
function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
}

beforeEach(() => {
  vi.clearAllMocks();
  clearDirectChatAuthority();
  bindWorkspaceSessionIdentity(null, null);
  bindMessagingSessionIdentity(null);
  uploads = [];
  bootstrapFailures = 0;
  uploadResponses = [];
  sends = [];
  editResponse = deferred<Message>();
  mocks.getSession.mockResolvedValue(sessionA);
  mocks.getProfile.mockResolvedValue({
    participant: { kind: "human", humanId: "human-a" },
    displayName: "A",
    tagline: "Original",
  });
  mocks.logout.mockResolvedValue(undefined);
  useConversation.setState({ acquireConnection: () => mocks.release });
  useParticipantApps.setState({
    owner: {
      kind: "participant",
      participant: { kind: "human", humanId: "human-a" },
    },
    status: "ready",
    installations: [
      {
        installationId: "direct-a",
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
  });
  vi.stubGlobal(
    "ResizeObserver",
    class {
      observe() {}
      unobserve() {}
      disconnect() {}
    },
  );
  vi.stubGlobal(
    "matchMedia",
    vi.fn(() => ({
      matches: false,
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
    })),
  );
  URL.createObjectURL = vi.fn(() => "blob:draft-photo");
  URL.revokeObjectURL = vi.fn();
});
afterEach(() => {
  cleanup();
  clearDirectChatAuthority();
  bindWorkspaceSessionIdentity(null, null);
  bindMessagingSessionIdentity(null);
  useConversation.setState({ acquireConnection: originalAcquire });
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

function renderWork() {
  render(
    <ThemeProvider>
      <TooltipProvider>
        <AuthProvider>
          <SessionProbe />
          <AuthGate>
            <ParticipantAppBinding>
              <ScopedComposers />
            </ParticipantAppBinding>
          </AuthGate>
        </AuthProvider>
      </TooltipProvider>
    </ThemeProvider>,
  );
}

async function openWork() {
  renderWork();
  const direct = await screen.findByRole("textbox", { name: "メッセージ" });
  await screen.findByRole("textbox", { name: "#general へメッセージ" });
  act(() =>
    useConversation.setState({ connection: "connected", ready: "ready" }),
  );
  fireEvent.change(direct, { target: { value: "直通の書きかけ" } });
  fireEvent.change(
    screen.getByRole("textbox", { name: "#general へメッセージ" }),
    { target: { value: "日本語の返信途中" } },
  );
  fireEvent.change(
    screen.getByRole("textbox", { name: "新しいWorkspaceの名前" }),
    { target: { value: "展示の準備" } },
  );
  fireEvent.click(screen.getByRole("button", { name: "急ぎ" }));
  const file = new File(["photo bytes"], "draft.png", { type: "image/png" });
  act(() => {
    useMessaging.setState((state) => ({
      draftByPlace: {
        ...state.draftByPlace,
        [placeKey]: {
          ...state.draftByPlace[placeKey],
          replyTarget: {
            messageId: "origin-a",
            authorLabel: "A",
            preview: "返信元",
          },
        },
      },
    }));
    useMessaging.getState().addDraftAttachments([file]);
  });
  expect(uploads).toHaveLength(1);
  return { direct, file, nonce: uploads[0].clientNonce };
}

function expectWork(direct: HTMLElement) {
  expect(screen.getByRole("button", { name: "急ぎ" })).toHaveAttribute(
    "aria-pressed",
    "true",
  );
  expect(screen.getByRole("textbox", { name: "メッセージ" })).toBe(direct);
  expect(direct).toHaveValue("直通の書きかけ");
  expect(
    screen.getByRole("textbox", { name: "#general へメッセージ" }),
  ).toHaveValue("日本語の返信途中");
  expect(
    screen.getByRole("textbox", { name: "新しいWorkspaceの名前" }),
  ).toHaveValue("展示の準備");
  expect(
    useMessaging.getState().draftByPlace[placeKey].replyTarget?.messageId,
  ).toBe("origin-a");
  expect(screen.getByText("draft.png")).toBeInTheDocument();
}

describe("logout preserves work until the server establishes the next session", () => {
  it("stops transport before logout, verifies the same Human, then restores forms, reply and the original File; successful retry discards them", async () => {
    const { direct, file, nonce } = await openWork();
    const logout = deferred<void>();
    const verification = deferred<SumiSessionStatus>();
    mocks.logout.mockImplementationOnce(() => {
      expect(mocks.release).toHaveBeenCalledOnce();
      expect(uploads[0].signal?.aborted).toBe(true);
      expect(getMessagingScope()).toEqual(MESSAGING_SCOPE);
      return logout.promise;
    });
    mocks.getSession.mockReturnValueOnce(verification.promise);
    fireEvent.click(screen.getByRole("button", { name: "Sign out" }));
    await waitFor(() => expect(mocks.logout).toHaveBeenCalledOnce());
    expect(
      screen.queryByRole("textbox", { name: "メッセージ" }),
    ).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Recheck" }));
    expect(mocks.getSession).toHaveBeenCalledTimes(1);
    await act(async () =>
      logout.reject(new AuthAPIError("logout rejected", 503)),
    );
    expect(mocks.getSession).toHaveBeenCalledTimes(2);
    expect(
      screen.queryByRole("button", { name: "Sign out" }),
    ).not.toBeInTheDocument();
    expect(URL.revokeObjectURL).not.toHaveBeenCalled();
    await act(async () => verification.resolve(sessionA));
    expectWork(direct);
    expect(screen.getByRole("alert")).toHaveTextContent("Logout failed");
    expect(uploads[1]).toMatchObject({ body: file, clientNonce: nonce });
    await act(async () =>
      uploadResponses[0].resolve({
        created: true,
        attachment: {
          attachmentId: "stale-receipt",
          filename: "draft.png",
          mime: "image/png",
          sizeBytes: file.size,
          sha256: "",
          position: 0,
          spoiler: false,
          alt: "",
        },
      }),
    );
    expect(
      useMessaging.getState().draftByPlace[placeKey].attachments[0].attachment,
    ).toBeUndefined();

    expect(useParticipantApps.getState().installations[0]?.installationId).toBe(
      "direct-a",
    );
    fireEvent.click(screen.getByRole("button", { name: "Sign out" }));
    await screen.findByText("Signed out");
    expect(direct).not.toBeInTheDocument();
    expect(useMessaging.getState().draftByPlace).toEqual({});
    expect(useWorkspaceControl.getState().sessionIdentity).toBeNull();
    expect(useParticipantApps.getState().owner).toBeNull();
    expect(URL.revokeObjectURL).toHaveBeenCalledWith("blob:draft-photo");
  });

  it("recovers an initial bootstrap failure through the actual loading screen retry", async () => {
    bootstrapFailures = 1;
    renderWork();
    const retry = await screen.findByRole("button", { name: "再試行" });
    expect(useMessaging.getState().ready).toBe(false);
    fireEvent.click(retry);
    await screen.findByRole("textbox", { name: "#general へメッセージ" });
    expect(useMessaging.getState().ready).toBe(true);
    expect(
      screen.queryByRole("button", { name: "再試行" }),
    ).not.toBeInTheDocument();
  });

  it("retries failed resume bootstrap without replacing the held forms or losing the interrupted File", async () => {
    const { direct, file } = await openWork();
    bootstrapFailures = 1;
    mocks.logout.mockRejectedValueOnce(new AuthAPIError("unavailable", 503));
    fireEvent.click(screen.getByRole("button", { name: "Sign out" }));
    await screen.findByText("Logout failed");
    const retry = await screen.findByRole("button", { name: "再試行" });
    expectWork(direct);
    expect(uploads).toHaveLength(1);
    fireEvent.click(retry);
    await waitFor(() => expect(uploads).toHaveLength(2));
    expectWork(direct);
    expect(uploads[1].body).toBe(file);
    expect(
      screen.queryByRole("button", { name: "再試行" }),
    ).not.toBeInTheDocument();
  });

  it("retains an interrupted inline edit and makes its save retryable after verified resume", async () => {
    await openWork();
    const message = useMessaging.getState().messagesByPlace[placeKey][0];
    act(() => useMessaging.getState().startEdit(message.messageId));
    const editor = screen.getByRole("textbox", { name: "メッセージを編集" });
    fireEvent.change(editor, { target: { value: "編集途中の本文" } });
    fireEvent.click(screen.getByRole("button", { name: "保存" }));
    expect(screen.getByRole("button", { name: "保存" })).toBeDisabled();
    mocks.logout.mockRejectedValueOnce(new AuthAPIError("unavailable", 503));
    fireEvent.click(screen.getByRole("button", { name: "Sign out" }));
    await screen.findByText("Logout failed");
    expect(
      screen.getByRole("textbox", { name: "メッセージを編集" }),
    ).toHaveValue("編集途中の本文");
    expect(screen.getByRole("button", { name: "保存" })).toBeEnabled();
    expect(useMessaging.getState().editSession?.messageId).toBe(
      message.messageId,
    );
    await act(async () =>
      editResponse.resolve({
        ...message,
        content: "古い保存応答",
        revision: 2,
      }),
    );
    expect(
      screen.getByRole("textbox", { name: "メッセージを編集" }),
    ).toHaveValue("編集途中の本文");
    expect(
      useMessaging.getState().draftByPlace[placeKey].replyTarget?.messageId,
    ).toBe("origin-a");
  });

  it("keeps a sent pending message separate from newer unsent work and retains its nonce for explicit retry", async () => {
    await openWork();
    act(() => {
      const state = useMessaging.getState();
      state.removeDraftAttachment(uploads[0].clientNonce);
      state.send("送信済み・結果待ち", "urgent");
      state.setDraft(placeKey, "次の未送信文");
    });
    expect(sends).toHaveLength(1);
    mocks.logout.mockRejectedValueOnce(new AuthAPIError("unavailable", 503));
    fireEvent.click(screen.getByRole("button", { name: "Sign out" }));
    await screen.findByText("Logout failed");
    expect(useMessaging.getState().draftByPlace[placeKey].text).toBe(
      "次の未送信文",
    );
    expect(useMessaging.getState().pendingByPlace[placeKey]).toMatchObject([
      {
        content: "送信済み・結果待ち",
        clientNonce: sends[0].clientNonce,
        failed: true,
      },
    ]);
    expect(sends).toHaveLength(1);
    act(() => useMessaging.getState().retrySend(sends[0].clientNonce));
    expect(sends).toHaveLength(2);
    expect(sends[1].clientNonce).toBe(sends[0].clientNonce);
  });

  it("preserves unsaved profile fields and the logout error when Settings effects resume", async () => {
    await openWork();
    fireEvent.click(screen.getByRole("button", { name: "設定" }));
    const name = await screen.findByRole("textbox", { name: "表示名" });
    await waitFor(() => expect(name).toBeEnabled());
    fireEvent.change(name, { target: { value: "編集中の表示名" } });
    fireEvent.change(screen.getByRole("textbox", { name: "ひとこと" }), {
      target: { value: "編集中のひとこと" },
    });
    const request = deferred<void>();
    mocks.logout.mockReturnValueOnce(request.promise);
    fireEvent.click(screen.getByRole("button", { name: "ログアウト" }));
    await waitFor(() => expect(mocks.logout).toHaveBeenCalledOnce());
    expect(
      screen.queryByRole("textbox", { name: "表示名" }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: "ログアウト" }),
    ).not.toBeInTheDocument();
    await act(async () => request.reject(new AuthAPIError("unavailable", 503)));
    await screen.findByText("ログアウトを完了できませんでした。");
    await waitFor(() =>
      expect(screen.getByRole("textbox", { name: "表示名" })).toBeEnabled(),
    );
    expect(screen.getByRole("textbox", { name: "表示名" })).toHaveValue(
      "編集中の表示名",
    );
    expect(screen.getByRole("textbox", { name: "ひとこと" })).toHaveValue(
      "編集中のひとこと",
    );
  });

  it("keeps ambiguous logout hidden until a successful verification retry", async () => {
    const { direct, file } = await openWork();
    mocks.logout.mockRejectedValueOnce(new TypeError("response lost"));
    mocks.getSession.mockRejectedValueOnce(new TypeError("offline"));
    fireEvent.click(screen.getByRole("button", { name: "Sign out" }));
    await screen.findByText("Sumiに接続できません");
    expect(
      screen.queryByRole("textbox", { name: "メッセージ" }),
    ).not.toBeInTheDocument();
    expect(URL.revokeObjectURL).not.toHaveBeenCalled();
    expect(uploads).toHaveLength(1);
    fireEvent.click(screen.getByRole("button", { name: "再試行" }));
    await screen.findByRole("button", { name: "Sign out" });
    expectWork(direct);
    expect(uploads[1].body).toBe(file);
  });

  it.each([
    "cookie-cleared",
    "different-human",
  ])("discards hidden work after ambiguous logout when verification establishes %s", async (result) => {
    const { direct } = await openWork();
    mocks.logout.mockRejectedValueOnce(new TypeError("response lost"));
    mocks.getSession.mockResolvedValueOnce(
      result === "cookie-cleared" ? { authenticated: false } : sessionB,
    );
    fireEvent.click(screen.getByRole("button", { name: "Sign out" }));
    await waitFor(() =>
      expect(URL.revokeObjectURL).toHaveBeenCalledWith("blob:draft-photo"),
    );
    expect(direct).not.toBeInTheDocument();
    expect(useMessaging.getState().draftByPlace[placeKey]?.text ?? "").toBe("");
    expect(screen.queryByDisplayValue("展示の準備")).not.toBeInTheDocument();
    expect(screen.getByTestId("session-user")).toHaveTextContent(
      result === "cookie-cleared" ? "none" : "human-b",
    );
    expect(uploads).toHaveLength(1);
  });
});
