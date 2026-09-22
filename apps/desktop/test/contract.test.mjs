import assert from "node:assert/strict";
import test from "node:test";
import {
  LIMITS,
  navigationURL,
  validateAction,
} from "../src/browser/contract.ts";

test("navigation rejects local/privileged protocols and credentials", () => {
  for (const url of [
    "file:///etc/passwd",
    "javascript:alert(1)",
    "sumi://host",
    "data:text/html,hi",
    "https://user:password@example.com",
    "not a URL",
    null,
  ]) {
    assert.throws(() => navigationURL(url), { code: "invalid_request" });
  }
  assert.equal(navigationURL("https://example.com"), "https://example.com/");
});

test("browser action inputs are bounded before dispatch", () => {
  for (const action of [
    null,
    { kind: "evaluate", code: "arbitrary code" },
    { kind: "fill", target: "t0", text: "x".repeat(LIMITS.input + 1) },
    { kind: "scroll", x: Infinity, y: 0 },
    { kind: "click", target: "#selector" },
  ]) {
    assert.throws(() => validateAction(action), { code: "invalid_request" });
  }
  validateAction({ kind: "fill", target: "t0", text: "hello" });
  validateAction({ kind: "scroll", x: 0, y: 200 });
});
