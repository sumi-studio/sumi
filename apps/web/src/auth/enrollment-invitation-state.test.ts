// @vitest-environment jsdom
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  captureEnrollmentInvitation,
  clearEnrollmentInvitation,
  readEnrollmentInvitation,
} from "./enrollment-invitation-state";

afterEach(() => {
  vi.restoreAllMocks();
  clearEnrollmentInvitation();
  history.replaceState(null, "", "/");
});

describe("enrollment invitation navigation", () => {
  it("removes the secret from the address and retains it across login navigation", () => {
    const token = "a".repeat(43);
    history.replaceState({ returnTo: "/" }, "", `/?view=login#invite=${token}`);
    expect(captureEnrollmentInvitation()).toBe(token);
    expect(location.href).not.toContain(token);
    expect(location.search).toBe("?view=login");
    expect(history.state).toEqual({ returnTo: "/" });
    history.replaceState(null, "", "/?auth_callback=1");
    expect(captureEnrollmentInvitation()).toBe(token);
    clearEnrollmentInvitation();
    expect(readEnrollmentInvitation()).toBeNull();
  });

  it("does not reuse an earlier invite when a malformed new link is opened", () => {
    history.replaceState(null, "", `/#invite=${"b".repeat(43)}`);
    captureEnrollmentInvitation();
    history.replaceState(null, "", "/#invite=invalid&section=hello");
    expect(captureEnrollmentInvitation()).toBeNull();
    expect(readEnrollmentInvitation()).toBeNull();
    expect(location.hash).toBe("#section=hello");
  });
});

it("keeps a stripped invitation available for flow startup when session storage is denied", () => {
  vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => {
    throw new Error("denied");
  });
  vi.spyOn(Storage.prototype, "getItem").mockImplementation(() => {
    throw new Error("denied");
  });
  const token = "d".repeat(43);
  history.replaceState(null, "", `/#invite=${token}`);
  expect(captureEnrollmentInvitation()).toBe(token);
  expect(location.hash).toBe("");
  expect(readEnrollmentInvitation()).toBe(token);
  clearEnrollmentInvitation();
  expect(readEnrollmentInvitation()).toBeNull();
});

it("retains both bundle fragments through login while terminal enrollment cleanup preserves explicit Workspace choice", async () => {
  const {
    readWorkspaceInvitation,
    clearWorkspaceInvitation,
    buildWorkspaceInvitationLink,
  } = await import("./enrollment-invitation-state");
  const code = "w".repeat(43),
    token = "e".repeat(43);
  history.replaceState(
    { view: 1 },
    "",
    `/?return=home#section=notes&invite=${token}&workspace_invite=${code}`,
  );
  expect(captureEnrollmentInvitation()).toBe(token);
  expect(readWorkspaceInvitation()).toBe(code);
  expect(location.search).toBe("?return=home");
  expect(location.hash).toBe("#section=notes");
  clearEnrollmentInvitation();
  history.replaceState(null, "", "/?auth_callback=1");
  expect(captureEnrollmentInvitation()).toBeNull();
  expect(readWorkspaceInvitation()).toBe(code);
  expect(buildWorkspaceInvitationLink(code, token)).toBe(
    `${location.origin}/#invite=${token}&workspace_invite=${code}`,
  );
  clearWorkspaceInvitation();
  expect(readWorkspaceInvitation()).toBeNull();
});
