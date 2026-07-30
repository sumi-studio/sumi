import { describe, expect, it } from "vitest";
import { secureRandomUUID } from "./random-uuid";

describe("secureRandomUUID", () => {
  it("uses randomUUID when the secure-context API is available", () => {
    const expected = "00000000-0000-4000-8000-000000000001";
    const source = {
      randomUUID: () => expected,
    } as unknown as Crypto;

    expect(secureRandomUUID(source)).toBe(expected);
  });

  it("builds an RFC 4122 UUIDv4 from getRandomValues on HTTP origins", () => {
    const source = {
      getRandomValues: (target: Uint8Array) => {
        target.set(Array.from({ length: 16 }, (_, index) => index));
        return target;
      },
    } as unknown as Crypto;

    expect(secureRandomUUID(source)).toBe(
      "00010203-0405-4607-8809-0a0b0c0d0e0f",
    );
  });
});
