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
import type { ReactNode } from "react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { WorkspaceAPIError } from "../api-client";
import { WorkspaceInvitationPrompt } from "./workspace-invitation-prompt";

const mocks = vi.hoisted(() => ({
  user: { id: "humanA", displayName: "Alice" },
  preview: vi.fn(),
  redeem: vi.fn(),
  proof: vi.fn(),
  navigate: vi.fn(),
  refresh: vi.fn(),
  logout: vi.fn(),
}));
vi.mock("../../auth/auth-context", () => ({
  useAuth: () => ({
    authenticated: true,
    user: mocks.user,
    logout: mocks.logout,
  }),
}));
vi.mock("@tanstack/react-router", () => ({
  useNavigate: () => mocks.navigate,
}));
vi.mock("../store", () => ({
  useWorkspaceControl: {
    getState: () => ({ refreshWorkspaces: mocks.refresh }),
  },
}));
vi.mock("../workspace-invitation-proof", () => ({
  workspaceInvitationIdentityProof: mocks.proof,
}));
vi.mock("../api-client", async (importOriginal) => {
  const original = await importOriginal<typeof import("../api-client")>();
  return {
    ...original,
    WorkspaceApiClient: class {
      previewInvite = mocks.preview;
      redeemInvite = mocks.redeem;
    },
  };
});
vi.mock("@sumi/ui/components/sheet", () => ({
  Sheet: ({ children }: { children: ReactNode }) => children,
  SheetContent: ({ children }: { children: ReactNode }) => (
    <div>{children}</div>
  ),
  SheetHeader: ({ children }: { children: ReactNode }) => (
    <header>{children}</header>
  ),
  SheetTitle: ({ children }: { children: ReactNode }) => <h2>{children}</h2>,
  SheetDescription: ({ children }: { children: ReactNode }) => (
    <p>{children}</p>
  ),
}));
const preview = {
  workspaceId: "workspace1",
  workspaceName: "Atelier",
  expiresAt: 9999999999999,
  requiresEmailVerification: true,
};
beforeEach(() => {
  mocks.user = { id: "humanA", displayName: "Alice" };
  mocks.preview.mockResolvedValue(preview);
  mocks.redeem.mockResolvedValue({ workspaceId: "workspace1" });
  mocks.refresh.mockResolvedValue(undefined);
  mocks.navigate.mockResolvedValue(undefined);
  mocks.proof.mockResolvedValue("existing-id-proof");
});
afterEach(() => {
  cleanup();
  vi.resetAllMocks();
});
it("previews without joining and reserved signup recipient joins only after explicit consent with no redundant proof", async () => {
  const dismiss = vi.fn();
  render(
    <WorkspaceInvitationPrompt code={"w".repeat(43)} onDismiss={dismiss} />,
  );
  await screen.findByText("Atelierに参加しますか？");
  expect(mocks.redeem).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole("button", { name: "参加する" }));
  await waitFor(() => expect(dismiss).toHaveBeenCalledOnce());
  expect(mocks.redeem).toHaveBeenCalledExactlyOnceWith("w".repeat(43));
  expect(mocks.proof).not.toHaveBeenCalled();
  expect(mocks.navigate).toHaveBeenCalledWith({
    to: "/w/$workspaceId",
    params: { workspaceId: "workspace1" },
  });
});
it("obtains existing Firebase proof only after backend requires it, within the explicit join action", async () => {
  mocks.redeem
    .mockRejectedValueOnce(
      new WorkspaceAPIError("invitation_email_verification_required", 403),
    )
    .mockResolvedValueOnce({ workspaceId: "workspace1" });
  render(
    <WorkspaceInvitationPrompt code={"w".repeat(43)} onDismiss={vi.fn()} />,
  );
  await screen.findByText("Atelierに参加しますか？");
  fireEvent.click(screen.getByRole("button", { name: "参加する" }));
  await waitFor(() => expect(mocks.redeem).toHaveBeenCalledTimes(2));
  expect(mocks.redeem.mock.calls[1]).toEqual([
    "w".repeat(43),
    "existing-id-proof",
  ]);
  expect(mocks.proof).toHaveBeenCalledOnce();
});
it("preview network errors are retryable and refusing never redeems", async () => {
  mocks.preview
    .mockRejectedValueOnce(new TypeError("network"))
    .mockResolvedValueOnce(preview);
  const dismiss = vi.fn();
  render(
    <WorkspaceInvitationPrompt code={"w".repeat(43)} onDismiss={dismiss} />,
  );
  fireEvent.click(await screen.findByRole("button", { name: "再試行" }));
  await screen.findByText("Atelierに参加しますか？");
  fireEvent.click(screen.getByRole("button", { name: "今は参加しない" }));
  expect(dismiss).toHaveBeenCalledOnce();
  expect(mocks.redeem).not.toHaveBeenCalled();
});
it("account switch ignores an old join result and presents fresh explicit consent", async () => {
  let finish!: (value: { workspaceId: string }) => void;
  mocks.redeem.mockImplementationOnce(
    () =>
      new Promise((resolve) => {
        finish = resolve;
      }),
  );
  const dismiss = vi.fn();
  const view = render(
    <WorkspaceInvitationPrompt code={"w".repeat(43)} onDismiss={dismiss} />,
  );
  await screen.findByText("Atelierに参加しますか？");
  fireEvent.click(screen.getByRole("button", { name: "参加する" }));
  mocks.user = { id: "humanB", displayName: "Bob" };
  view.rerender(
    <WorkspaceInvitationPrompt code={"w".repeat(43)} onDismiss={dismiss} />,
  );
  await act(async () => finish({ workspaceId: "workspace1" }));
  expect(dismiss).not.toHaveBeenCalled();
  expect(mocks.navigate).not.toHaveBeenCalled();
  expect(screen.getByText(/Bobとして参加/)).toBeInTheDocument();
  expect(mocks.preview).toHaveBeenCalledTimes(2);
});
