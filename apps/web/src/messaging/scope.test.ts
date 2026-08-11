import { afterEach, describe, expect, it } from "vitest";
import {
  bindMessagingScopeToURL,
  getActiveMessagingScope,
  requireActiveMessagingBoundary,
  requireActiveMessagingScope,
  scopedMessagingPath,
  setActiveMessagingScope,
} from "./scope";

const SCOPE = {
  workspaceId: "workspace-1",
  installationId: "installation-1",
} as const;

afterEach(() => setActiveMessagingScope(null));

describe("MessagingScope", () => {
  it("adds exactly one canonical scope pair while preserving other query input", () => {
    const path = scopedMessagingPath(
      "/messaging/search?q=hello%20world&limit=20",
      SCOPE,
    );
    const url = new URL(path, "https://sumi.test");

    expect(url.pathname).toBe("/messaging/search");
    expect(url.searchParams.get("q")).toBe("hello world");
    expect(url.searchParams.get("limit")).toBe("20");
    expect(url.searchParams.getAll("workspace_id")).toEqual(["workspace-1"]);
    expect(url.searchParams.getAll("installation_id")).toEqual([
      "installation-1",
    ]);
  });

  it("fails closed for an absent, partial, duplicate, or non-Messaging scope", () => {
    expect(() => requireActiveMessagingScope()).toThrow(/not bound/);
    expect(() =>
      scopedMessagingPath("/messaging/bootstrap", {
        workspaceId: "workspace-1",
        installationId: "",
      }),
    ).toThrow(/exact Workspace and installation/);
    expect(() =>
      scopedMessagingPath(
        "/messaging/bootstrap?workspace_id=workspace-shadow",
        SCOPE,
      ),
    ).toThrow(/already present/);
    expect(() =>
      scopedMessagingPath(
        "/messaging/bootstrap?installation_id=installation-shadow",
        SCOPE,
      ),
    ).toThrow(/already present/);
    expect(() => scopedMessagingPath("/workspaces", SCOPE)).toThrow(
      /only bind Messaging routes/,
    );
  });

  it("copies active authority and applies the same pair to a WebSocket URL", () => {
    setActiveMessagingScope(SCOPE);
    const first = getActiveMessagingScope();
    expect(first).toEqual(SCOPE);
    if (!first) throw new Error("scope was not stored");
    first.workspaceId = "mutated";
    expect(requireActiveMessagingScope()).toEqual(SCOPE);

    const url = bindMessagingScopeToURL(
      new URL("wss://sumi.test/messaging/ws?cursor=7"),
      SCOPE,
    );
    expect(url.protocol).toBe("wss:");
    expect(url.searchParams.get("cursor")).toBe("7");
    expect(url.searchParams.getAll("workspace_id")).toEqual(["workspace-1"]);
    expect(url.searchParams.getAll("installation_id")).toEqual([
      "installation-1",
    ]);
  });

  it("aborts the previous scope lifetime before publishing a replacement", () => {
    setActiveMessagingScope(SCOPE);
    const previous = requireActiveMessagingBoundary();

    setActiveMessagingScope({
      workspaceId: "workspace-2",
      installationId: "installation-2",
    });

    expect(previous.signal.aborted).toBe(true);
    const current = requireActiveMessagingBoundary();
    expect(current.signal.aborted).toBe(false);
    expect(current.scope).toEqual({
      workspaceId: "workspace-2",
      installationId: "installation-2",
    });
  });
});
