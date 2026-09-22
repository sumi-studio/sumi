import assert from "node:assert/strict";
import test from "node:test";
import { navigationURL } from "../src/browser/contract.ts";
import { filterBrowserRequest } from "../src/browser/request-policy.ts";

test("request filter completes exactly once for malformed and privileged URLs", () => {
  for (const url of [
    "not a URL",
    "http://[",
    "file:///etc/passwd",
    "sumi://host",
  ]) {
    const responses = [];
    assert.doesNotThrow(() =>
      filterBrowserRequest({ url }, (r) => responses.push(r)),
    );
    assert.deepEqual(responses, [{ cancel: true }]);
  }
});

test("shared browser retains normal host HTTP(S) reachability including private sites", () => {
  for (const url of [
    "http://127.0.0.1:8080/",
    "https://192.168.1.1/",
    "http://[::1]/",
    "https://example.com/",
  ]) {
    assert.equal(navigationURL(url), url);
    const responses = [];
    filterBrowserRequest({ url }, (r) => responses.push(r));
    assert.deepEqual(responses, [{ cancel: false }]);
  }
});
