import assert from "node:assert/strict";
import { createServer, type Server } from "node:http";
import { test } from "node:test";
import { HttpStateClient, StateError } from "../src/state-client.ts";

/**
 * A real local HTTP server that stalls on demand: a path containing
 * "stall-h" never answers (accepted socket, no headers), "stall-b"
 * answers headers plus a partial JSON body and never ends it. Anything
 * else answers normally. Real fetch (undici) + real TCP — this is the
 * transport behavior the DO relies on the binding to reproduce.
 */
function stallingServer(): Promise<{ server: Server; url: string }> {
  const server = createServer((req, res) => {
    const url = req.url ?? "";
    if (url.includes("stall-h")) return; // never answered
    if (url.includes("stall-e")) {
      res.writeHead(500, { "content-type": "application/json" });
      res.write('{"partial"');
      return; // error body never finishes
    }
    if (url.includes("stall-b")) {
      res.writeHead(200, { "content-type": "application/json" });
      res.write('{"partial"');
      return; // body never finishes
    }
    if (url.includes("fast-error")) {
      res.writeHead(500, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: "state unavailable" }));
      return;
    }
    if (url.includes("bad-body")) {
      res.writeHead(200, { "content-type": "application/json" });
      res.end("not JSON");
      return;
    }
    if (url.includes("disconnect")) {
      req.socket.destroy();
      return;
    }
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ persona: { persona_id: "p" } }));
  });
  return new Promise((r) =>
    server.listen(0, "127.0.0.1", () =>
      r({
        server,
        url: `http://127.0.0.1:${(server.address() as { port: number }).port}`,
      }),
    ),
  );
}

const TIMEOUT = 250; // test-scale deadline; production default is 10s

test("a state call that never answers fails at the deadline; the next call succeeds", async () => {
  const { server, url } = await stallingServer();
  try {
    const client = new HttpStateClient(url, "tok", undefined, TIMEOUT);
    const at = Date.now();
    await assert.rejects(client.personaState("stall-h"), (e: unknown) => {
      assert.ok(e instanceof StateError, `want StateError, got ${e}`);
      assert.equal(e.status, 503);
      assert.match(e.message, /timed out/);
      return true;
    });
    const ms = Date.now() - at;
    assert.ok(ms >= TIMEOUT && ms < 5_000, `deadline fired at ${ms}ms`);
    // The wedge was the call, not the client: the same client recovers.
    const ok = await client.personaState("healthy");
    assert.equal(ok.persona?.persona_id, "p");
  } finally {
    server.close();
  }
});

test("headers received but body stalled also fails at the deadline", async () => {
  const { server, url } = await stallingServer();
  try {
    const client = new HttpStateClient(url, "tok", undefined, TIMEOUT);
    const at = Date.now();
    await assert.rejects(client.personaState("stall-b"), (e: unknown) => {
      assert.ok(e instanceof StateError);
      assert.equal(e.status, 503);
      assert.match(e.message, /timed out/);
      return true;
    });
    assert.ok(Date.now() - at < 5_000, "body read bounded by the deadline");
    const ok = await client.personaState("healthy");
    assert.equal(ok.persona?.persona_id, "p");
  } finally {
    server.close();
  }
});

test("an error-status body that stalls is still bounded by the deadline", async () => {
  const { server, url } = await stallingServer();
  try {
    const client = new HttpStateClient(url, "tok", undefined, TIMEOUT);
    const at = Date.now();
    // The status already arrived — the stalled body only delays the error
    // detail read, which the deadline also bounds; the status still
    // classifies the failure (500 → transient).
    await assert.rejects(client.personaState("stall-e"), (e: unknown) => {
      assert.ok(e instanceof StateError);
      assert.equal(e.status, 500);
      return true;
    });
    assert.ok(Date.now() - at < 5_000, "error body read bounded");
  } finally {
    server.close();
  }
});

test("settled calls retain no deadline activity; a pending call keeps its bound", async () => {
  // Deadline bookkeeping: the call's own timer is the one created with the
  // client's timeoutMs; other internals (undici etc.) use different delays
  // and are ignored here. A timer counts as open until it fires or is
  // cleared — this asserts the exchange releases its deadline on every
  // exit while a still-pending call remains bounded.
  const origSet = globalThis.setTimeout;
  const origClear = globalThis.clearTimeout;
  const openDeadlines = new Set<ReturnType<typeof setTimeout>>();
  globalThis.setTimeout = ((
    cb: (...a: unknown[]) => void,
    ms?: number,
    ...a: unknown[]
  ) => {
    if (ms !== TIMEOUT) return origSet(cb, ms, ...a);
    const handle = origSet(() => {
      openDeadlines.delete(handle);
      cb();
    }, ms);
    openDeadlines.add(handle);
    return handle;
  }) as typeof setTimeout;
  globalThis.clearTimeout = ((t: ReturnType<typeof setTimeout> | undefined) => {
    if (t !== undefined) openDeadlines.delete(t);
    return origClear(t);
  }) as typeof clearTimeout;
  try {
    const { server, url } = await stallingServer();
    try {
      const client = new HttpStateClient(url, "tok", undefined, TIMEOUT);
      const ok = await client.personaState("healthy");
      assert.equal(ok.persona?.persona_id, "p");
      // A finished exchange must not keep its deadline armed — a retained
      // timer holds a Durable Object non-hibernateable until it fires.
      await new Promise((r) => origSet(r, 30)); // let microtasks drain
      assert.equal(
        openDeadlines.size,
        0,
        "settled call retained a deadline timer",
      );

      // These fail before the deadline: cleanup must not depend on it firing.
      await assert.rejects(client.personaState("fast-error"), (e: unknown) => {
        assert.ok(e instanceof StateError && e.status === 500);
        assert.equal(e.message, "state unavailable");
        return true;
      });
      assert.equal(
        openDeadlines.size,
        0,
        "HTTP error retained a deadline timer",
      );
      await assert.rejects(client.personaState("bad-body"), (e: unknown) => {
        assert.ok(e instanceof StateError && e.status === 503);
        assert.match(e.message, /unreadable 200 body/);
        return true;
      });
      assert.equal(
        openDeadlines.size,
        0,
        "JSON error retained a deadline timer",
      );
      await assert.rejects(client.personaState("disconnect"));
      assert.equal(
        openDeadlines.size,
        0,
        "fetch failure retained a deadline timer",
      );

      // While a call is still pending its deadline must remain armed.
      const pending = client.personaState("stall-h");
      await new Promise((r) => origSet(r, 30));
      assert.equal(
        openDeadlines.size,
        1,
        "pending call lost its deadline bound",
      );
      await assert.rejects(pending, (e: unknown) => {
        assert.ok(e instanceof StateError && e.status === 503);
        return true;
      });
      // The deadline fired — it no longer counts as retained activity.
      assert.equal(openDeadlines.size, 0, "timed-out call retained a timer");

      // Error-status exit releases its deadline too.
      await assert.rejects(client.personaState("stall-e"), (e: unknown) => {
        assert.ok(e instanceof StateError && e.status === 500);
        return true;
      });
      assert.equal(openDeadlines.size, 0, "error exit retained a timer");
    } finally {
      server.close();
    }
  } finally {
    globalThis.setTimeout = origSet;
    globalThis.clearTimeout = origClear;
  }
});

test("a fetcher that ignores the signal remains pending past the deadline", async () => {
  // Boundary honesty: a FetchLike that never settles AND ignores the
  // signal leaves the promise pending — call() cannot conjure
  // cancellation the transport does not provide. This test pins the
  // contract: the signal must reach the transport, and callers must not
  // mistake a hanging promise for cancellation.
  const client = new HttpStateClient(
    "http://unused.invalid",
    "tok",
    () => new Promise(() => {}), // ignores init.signal entirely
    50,
  );
  const pending = client.personaState("p");
  const raced = await Promise.race([
    pending.then(
      () => "resolved",
      () => "rejected",
    ),
    new Promise<"pending">((r) => setTimeout(() => r("pending"), 200)),
  ]);
  assert.equal(raced, "pending");
});
