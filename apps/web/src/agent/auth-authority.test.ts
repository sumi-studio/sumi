// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";
import {
  bindDirectChatAuthority,
  clearDirectChatAuthority,
} from "./auth-authority";

const authorityMocks = vi.hoisted(() => ({
  resetAuthority: vi.fn(() => true),
  resumeMountedConnection: vi.fn(),
}));

vi.mock("./store", () => ({
  useConversation: {
    getState: () => ({
      resetAuthority: authorityMocks.resetAuthority,
      resumeMountedConnection: authorityMocks.resumeMountedConnection,
    }),
  },
}));

const authorityBindingA = "A".repeat(43);
const authorityBindingB = `${"B".repeat(42)}E`;
const authorityBindingStorageKey = "sumi.direct-chat.authority-binding-v1";

afterEach(() => {
  clearDirectChatAuthority();
  globalThis.sessionStorage.clear();
  vi.clearAllMocks();
});

describe("direct-chat authority binding", () => {
  it("keeps the same binding and resets stale state for a different target binding", () => {
    globalThis.sessionStorage.setItem(
      authorityBindingStorageKey,
      authorityBindingA,
    );

    bindDirectChatAuthority(authorityBindingA);
    bindDirectChatAuthority(authorityBindingA);

    expect(authorityMocks.resetAuthority).not.toHaveBeenCalled();
    bindDirectChatAuthority(authorityBindingB);

    expect(authorityMocks.resetAuthority).toHaveBeenCalledTimes(1);
    expect(authorityMocks.resumeMountedConnection).toHaveBeenCalledTimes(1);
    expect(globalThis.sessionStorage.getItem(authorityBindingStorageKey)).toBe(
      authorityBindingB,
    );

    authorityMocks.resetAuthority.mockReturnValueOnce(false);
    expect(() => bindDirectChatAuthority(authorityBindingA)).toThrow(
      "Direct-chat private state could not be cleared for the new authority",
    );
    expect(globalThis.sessionStorage.getItem(authorityBindingStorageKey)).toBe(
      authorityBindingB,
    );
  });

  it("propagates reset failure while storage-key cleanup remains best effort", () => {
    globalThis.sessionStorage.setItem(
      authorityBindingStorageKey,
      authorityBindingA,
    );
    authorityMocks.resetAuthority.mockReturnValueOnce(false);

    expect(clearDirectChatAuthority()).toBe(false);
    expect(
      globalThis.sessionStorage.getItem(authorityBindingStorageKey),
    ).toBeNull();
  });
});
