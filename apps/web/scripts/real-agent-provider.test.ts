import assert from "node:assert/strict";
import test from "node:test";
import { LoopbackChatProvider } from "../e2e/support/real-agent-stack";

test("loopback provider counts only requests that pass transport validation", async () => {
  const provider = new LoopbackChatProvider("test-provider-key");
  await provider.start();
  try {
    assert.equal((await fetch(`${provider.url}/probe`)).status, 404);
    assert.equal(
      (
        await fetch(`${provider.url}/chat/completions`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: "{}",
        })
      ).status,
      401,
    );
    assert.equal(
      (
        await fetch(`${provider.url}/chat/completions`, {
          method: "POST",
          headers: {
            Authorization: "Bearer test-provider-key",
            "Content-Type": "text/plain",
          },
          body: "{}",
        })
      ).status,
      415,
    );
    assert.equal(provider.requestCount, 0);

    assert.equal(
      (
        await fetch(`${provider.url}/chat/completions`, {
          method: "POST",
          headers: {
            Authorization: "Bearer test-provider-key",
            "Content-Type": "application/json",
          },
          body: "{}",
        })
      ).status,
      422,
    );
    assert.equal(provider.requestCount, 1);
  } finally {
    await provider.stop();
  }
});
