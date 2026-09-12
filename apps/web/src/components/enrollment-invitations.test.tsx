// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, expect, it, vi } from "vitest";
import { AuthAPIError } from "../auth/session-client";
import { EnrollmentInvitations } from "./enrollment-invitations";

const api = vi.hoisted(() => ({
  list: vi.fn(),
  create: vi.fn(),
  revoke: vi.fn(),
}));
vi.mock("../auth/enrollment-invitations", () => ({
  listEnrollmentInvitations: api.list,
  createEnrollmentInvitation: api.create,
  revokeEnrollmentInvitation: api.revoke,
}));
vi.mock("@sumi/ui/components/sheet", () => ({
  Sheet: ({ open, children }: { open: boolean; children: ReactNode }) =>
    open ? children : null,
  SheetContent: ({ children }: { children: ReactNode }) => (
    <div>{children}</div>
  ),
  SheetHeader: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  SheetTitle: ({ children }: { children: ReactNode }) => <h2>{children}</h2>,
  SheetDescription: ({ children }: { children: ReactNode }) => (
    <p>{children}</p>
  ),
}));
const invitation = {
  id: "00000000-0000-4000-8000-000000000001",
  issuedBy: "00000000-0000-4000-8000-000000000002",
  createdAt: "2026-09-12T00:00:00Z",
  expiresAt: "2099-09-19T00:00:00Z",
};
afterEach(() => {
  cleanup();
  vi.clearAllMocks();
  vi.unstubAllGlobals();
});
it("copies newly created raw token, revokes via ID and cannot redisplay token after close", async () => {
  const writeText = vi.fn().mockResolvedValue(undefined);
  Object.defineProperty(navigator, "clipboard", {
    configurable: true,
    value: { writeText },
  });
  api.list.mockResolvedValue({ canInvite: true, invitations: [] });
  api.create.mockResolvedValue({ invitation, token: "t".repeat(43) });
  api.revoke.mockResolvedValue(undefined);
  const change = vi.fn();
  const view = render(
    <EnrollmentInvitations
      open
      onOpenChange={vi.fn()}
      onCapabilityChange={change}
    />,
  );
  await waitFor(() =>
    expect(
      screen.getByRole("button", { name: "招待リンクを作る" }),
    ).toBeEnabled(),
  );
  fireEvent.click(screen.getByRole("button", { name: "招待リンクを作る" }));
  await screen.findByLabelText("作成した招待リンク");
  fireEvent.click(screen.getByRole("button", { name: "招待リンクをコピー" }));
  await waitFor(() =>
    expect(writeText).toHaveBeenCalledWith(
      `${location.origin}/#invite=${"t".repeat(43)}`,
    ),
  );
  api.list.mockResolvedValue({
    canInvite: true,
    invitations: [{ ...invitation, revokedAt: "2026-09-12T01:00:00Z" }],
  });
  fireEvent.click(screen.getByRole("button", { name: "取り消す" }));
  await screen.findByText(/取消済み/);
  expect(api.revoke).toHaveBeenCalledWith(invitation.id);
  view.rerender(
    <EnrollmentInvitations
      open={false}
      onOpenChange={vi.fn()}
      onCapabilityChange={change}
    />,
  );
  view.rerender(
    <EnrollmentInvitations
      open
      onOpenChange={vi.fn()}
      onCapabilityChange={change}
    />,
  );
  expect(screen.queryByLabelText("作成した招待リンク")).toBeNull();
});
it("reports unavailable admin capability on403 but keeps network failure retryable", async () => {
  api.list
    .mockRejectedValueOnce(new AuthAPIError("invitation_admin_required", 403))
    .mockRejectedValueOnce(new TypeError("network"))
    .mockResolvedValue({ canInvite: true, invitations: [] });
  const capability = vi.fn();
  const view = render(
    <EnrollmentInvitations
      open
      onOpenChange={vi.fn()}
      onCapabilityChange={capability}
    />,
  );
  await waitFor(() => expect(capability).toHaveBeenCalledWith(false));
  view.unmount();
  render(
    <EnrollmentInvitations
      open
      onOpenChange={vi.fn()}
      onCapabilityChange={capability}
    />,
  );
  await screen.findByText("招待を読み込めませんでした。");
  fireEvent.click(screen.getByRole("button", { name: "一覧を再読み込み" }));
  await waitFor(() => expect(capability).toHaveBeenCalledWith(true));
});

it("does not reveal a late create token after its originating sheet has closed", async () => {
  api.list.mockResolvedValue({ canInvite: true, invitations: [] });
  let finish!: (result: unknown) => void;
  api.create.mockImplementation(
    () =>
      new Promise((resolve) => {
        finish = resolve;
      }),
  );
  const capability = vi.fn(),
    change = vi.fn();
  const view = render(
    <EnrollmentInvitations
      open
      onOpenChange={change}
      onCapabilityChange={capability}
    />,
  );
  await waitFor(() =>
    expect(
      screen.getByRole("button", { name: "招待リンクを作る" }),
    ).toBeEnabled(),
  );
  fireEvent.click(screen.getByRole("button", { name: "招待リンクを作る" }));
  view.rerender(
    <EnrollmentInvitations
      open={false}
      onOpenChange={change}
      onCapabilityChange={capability}
    />,
  );
  finish({ invitation, token: "l".repeat(43) });
  await waitFor(() => expect(api.create).toHaveBeenCalled());
  await new Promise((resolve) => setTimeout(resolve, 0));
  view.rerender(
    <EnrollmentInvitations
      open
      onOpenChange={change}
      onCapabilityChange={capability}
    />,
  );
  expect(screen.queryByLabelText("作成した招待リンク")).toBeNull();
});
