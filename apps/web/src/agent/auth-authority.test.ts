// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";
import { bindDirectChatAuthority } from "./auth-authority";

const authorityMocks = vi.hoisted(() => ({
  resetAuthority: vi.fn(),
}));

vi.mock("./store", () => ({
  useConversation: {
    getState: () => ({ resetAuthority: authorityMocks.resetAuthority }),
  },
}));

const authorityBindingA = "A".repeat(43);
const authorityBindingB = `${"B".repeat(42)}E`;
const authorityBindingStorageKey = "sumi.direct-chat.authority-binding-v1";
const legacyAuthorityIdentityStorageKey = "sumi.direct-chat.authority-user";

afterEach(() => {
  globalThis.sessionStorage.clear();
  vi.clearAllMocks();
});

describe("direct-chat authority binding", () => {
  it("keeps the same binding and resets stale state for a different target binding", () => {
    globalThis.sessionStorage.setItem(
      authorityBindingStorageKey,
      authorityBindingA,
    );
    globalThis.sessionStorage.setItem(
      legacyAuthorityIdentityStorageKey,
      "same-human-user-id",
    );

    bindDirectChatAuthority(authorityBindingA);
    bindDirectChatAuthority(authorityBindingA);

    expect(authorityMocks.resetAuthority).not.toHaveBeenCalled();
    expect(
      globalThis.sessionStorage.getItem(legacyAuthorityIdentityStorageKey),
    ).toBeNull();

    bindDirectChatAuthority(authorityBindingB);

    expect(authorityMocks.resetAuthority).toHaveBeenCalledTimes(1);
    expect(globalThis.sessionStorage.getItem(authorityBindingStorageKey)).toBe(
      authorityBindingB,
    );
  });
});
