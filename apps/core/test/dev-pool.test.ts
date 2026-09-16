import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import type { AddressInfo } from "node:net";
import test from "node:test";

import { DevPool, type PoolChild, serve } from "../src/host/dev-pool.ts";

class FakeChild extends EventEmitter implements PoolChild {
  readonly pid = Math.floor(Math.random() * 100_000);
  killed: (number | NodeJS.Signals)[] = [];
  kill(signal: number | NodeJS.Signals = "SIGTERM") {
    this.killed.push(signal);
    // Emit asynchronously like a real process exit.
    queueMicrotask(() =>
      this.emit("exit", 0, typeof signal === "string" ? signal : null),
    );
  }
}

function makePool(spawned: { env: NodeJS.ProcessEnv; persona: string }[], children: FakeChild[]) {
  return new DevPool({
    stateURL: "http://127.0.0.1:9",
    runtimeToken: "runtime-token-under-test",
    respawnDelayMs: 5,
    log: () => {},
    spawnChild: (personaID, env) => {
      spawned.push({ env, persona: personaID });
      const child = new FakeChild();
      children.push(child);
      return child;
    },
  });
}

const PERSONA = "0198f0f4-9b72-7000-8000-000000000001";

async function listen(pool: DevPool, wakeToken: string) {
  const server = serve(pool, "127.0.0.1:0", wakeToken);
  await new Promise<void>((r) => server.once("listening", r));
  const { port } = server.address() as AddressInfo;
  return { server, base: `http://127.0.0.1:${port}` };
}

test("wake requires the bearer token and a uuidv7 persona", async () => {
  const spawned: { env: NodeJS.ProcessEnv; persona: string }[] = [];
  const pool = makePool(spawned, []);
  const { server, base } = await listen(pool, "wake-token");
  try {
    const noAuth = await fetch(`${base}/personas/${PERSONA}/wake`, {
      method: "POST",
    });
    assert.equal(noAuth.status, 401);
    const badAuth = await fetch(`${base}/personas/${PERSONA}/wake`, {
      method: "POST",
      headers: { Authorization: "Bearer wrong" },
    });
    assert.equal(badAuth.status, 401);
    const badID = await fetch(`${base}/personas/not-a-uuid/wake`, {
      method: "POST",
      headers: { Authorization: "Bearer wake-token" },
    });
    assert.equal(badID.status, 400);
    assert.equal(spawned.length, 0);
  } finally {
    server.close();
    await pool.stop();
  }
});

test("wake starts one local host per persona; repeat wakes are idempotent", async () => {
  const spawned: { env: NodeJS.ProcessEnv; persona: string }[] = [];
  const children: FakeChild[] = [];
  const pool = makePool(spawned, children);
  const { server, base } = await listen(pool, "wake-token");
  try {
    const wake = () =>
      fetch(`${base}/personas/${PERSONA}/wake`, {
        method: "POST",
        headers: { Authorization: "Bearer wake-token" },
      });
    assert.equal((await wake()).status, 200);
    assert.equal((await wake()).status, 200);
    assert.equal(spawned.length, 1);
    const env = spawned[0]!.env;
    assert.equal(env.SUMI_PERSONA_ID, PERSONA);
    assert.equal(env.SUMI_PERSONA_TOKEN, "runtime-token-under-test");
    assert.equal(env.SUMI_STATE_URL, "http://127.0.0.1:9");

    const other = "0198f0f4-9b72-7000-8000-000000000002";
    await fetch(`${base}/personas/${other}/wake`, {
      method: "POST",
      headers: { Authorization: "Bearer wake-token" },
    });
    assert.equal(spawned.length, 2);
  } finally {
    server.close();
    await pool.stop();
  }
});

test("a child that exits on its own is respawned; stop kills children once", async () => {
  const spawned: { env: NodeJS.ProcessEnv; persona: string }[] = [];
  const children: FakeChild[] = [];
  const pool = makePool(spawned, children);
  try {
    pool.ensure(PERSONA);
    assert.equal(children.length, 1);
    children[0]!.emit("exit", 1, null);
    await new Promise((r) => setTimeout(r, 50));
    assert.equal(children.length, 2, "respawned after unexpected exit");

    await pool.stop();
    assert.ok(children[1]!.killed.length >= 1);
    // After stop, no further respawns.
    await new Promise((r) => setTimeout(r, 30));
    assert.equal(children.length, 2);
  } finally {
    await pool.stop();
  }
});

test("the listener refuses a non-loopback bind", async () => {
  const pool = makePool([], []);
  assert.throws(() => serve(pool, "0.0.0.0:8083", "t"), /loopback/);
  await pool.stop();
});
