import { describe, expect, it } from "vitest";
import { classifySessionFailure, hasAllowedAuthOrigin } from "./auth-context";
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

  it("does not treat a missing production auth route as authorization", () => {
    expect(classifySessionFailure(new AuthAPIError("not found", 404))).toBe(
      "unavailable",
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

describe("browser auth origin boundary", () => {
  it("fails closed for a cross-origin API base outside the fixture mode", () => {
    expect(
      hasAllowedAuthOrigin({
        apiBaseURL: "https://api.sumi.example",
        pageOrigin: "https://app.sumi.example",
      }),
    ).toBe(false);
  });

  it("permits the isolated pre-issued fixture to use its local API server", () => {
    expect(
      hasAllowedAuthOrigin({
        apiBaseURL: "http://127.0.0.1:39001",
        authMode: "preissued",
        pageOrigin: "http://127.0.0.1:4173",
      }),
    ).toBe(true);
  });
});
