import { describe, expect, it } from "vitest";
import { classifySessionFailure } from "./auth-context";
import { AuthAPIError } from "./session-client";

describe("auth session failure classification", () => {
  it("stops authenticated transport for rejected sessions", () => {
    expect(classifySessionFailure(new AuthAPIError("unauthorized", 401))).toBe(
      "unauthenticated",
    );
    expect(classifySessionFailure(new AuthAPIError("forbidden", 403))).toBe(
      "unauthenticated",
    );
  });

  it("supports the current pre-issued-cookie fixture when auth routes are absent", () => {
    expect(classifySessionFailure(new AuthAPIError("not found", 404))).toBe(
      "preissued",
    );
  });

  it("distinguishes retryable unavailability from logout", () => {
    expect(classifySessionFailure(new AuthAPIError("unavailable", 503))).toBe(
      "unavailable",
    );
    expect(classifySessionFailure(new TypeError("network failed"))).toBe(
      "unavailable",
    );
  });
});
