// The checkpoint page scripts in real headless Chrome (review F3, F5).
// Needs google-chrome (CHROME to override); skipped without it. Loopback
// only: an ephemeral local page server and a temporary Chrome profile under
// $TMPDIR/sumi-cloud-browser-20260927-idb-*, removed afterwards.
import assert from "node:assert/strict";
import { type ChildProcess, spawn } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import http from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, test } from "node:test";
import { COLLECT, type OriginData, RESTORE } from "../src/storage-scripts.ts";

const CHROME = process.env.CHROME ?? ["/usr/bin/google-chrome", "/usr/bin/google-chrome-stable"].find((p) => existsSync(p));
const skip = CHROME ? false : "google-chrome not found";

let chrome: ChildProcess | undefined;
const dirs: string[] = [];
let server: http.Server | undefined;
let socket: WebSocket | undefined;
let nextId = 0;
const pending = new Map<number, { resolve: (v: unknown) => void; reject: (e: Error) => void }>();

before(async () => {
  if (skip) return;
  server = http.createServer((_req, res) => {
    res.setHeader("content-type", "text/html");
    res.end("<!doctype html><title>idb fixture</title>");
  });
  await new Promise<void>((resolve) => server?.listen(0, "127.0.0.1", resolve));
  const origin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  const port = await launchChrome(origin);
  type PageTarget = { type: string; url: string; webSocketDebuggerUrl: string };
  let page: PageTarget | undefined;
  for (let i = 0; i < 50 && !page; i++) {
    const list = (await (await fetch(`http://127.0.0.1:${port}/json/list`)).json()) as PageTarget[];
    page = list.find((t) => t.type === "page" && t.url.startsWith(origin));
    if (!page) await new Promise((r) => setTimeout(r, 100));
  }
  assert.ok(page, "fixture page");
  socket = new WebSocket(page.webSocketDebuggerUrl);
  socket.addEventListener("message", (event) => {
    const message = JSON.parse(String(event.data)) as { id?: number; result?: unknown; error?: { message: string } };
    const waiter = message.id === undefined ? undefined : pending.get(message.id);
    if (!waiter) return;
    pending.delete(message.id as number);
    if (message.error) waiter.reject(new Error(message.error.message));
    else waiter.resolve(message.result);
  });
  await new Promise((resolve, reject) => {
    socket?.addEventListener("open", resolve);
    socket?.addEventListener("error", reject);
  });
  for (let i = 0; i < 50 && (await run<string>("location.origin + document.readyState")) !== `${origin}complete`; i++)
    await new Promise((r) => setTimeout(r, 100));
});

after(async () => {
  socket?.close();
  server?.close();
  if (chrome) await stopChrome(chrome);
  for (const d of dirs) rmSync(d, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
});

/** Stops a launched Chrome and its whole process group (renderer and utility processes too). */
async function stopChrome(proc: ChildProcess): Promise<void> {
  if (proc.exitCode !== null || proc.signalCode !== null) return;
  const exited = new Promise((resolve) => proc.once("exit", resolve));
  try {
    process.kill(-(proc.pid as number), "SIGKILL");
  } catch {
    return; // the group is already gone
  }
  await exited;
}

/**
 * Starts Chrome on a fresh profile and returns its DevTools port. CI has
 * intermittently seen no DevToolsActivePort within the wait while Chrome's
 * output was discarded, so its exit status and stderr tail are reported and
 * one failed launch is retried on a new profile. The scripts under test are
 * unchanged by a relaunch.
 */
async function launchChrome(origin: string): Promise<string> {
  const failures: string[] = [];
  for (let attempt = 1; attempt <= 2; attempt++) {
    const dir = mkdtempSync(join(tmpdir(), "sumi-cloud-browser-20260927-idb-"));
    dirs.push(dir);
    const proc = spawn(CHROME as string, ["--headless=new", "--no-first-run", "--no-default-browser-check", "--remote-debugging-port=0", "--remote-allow-origins=*", `--user-data-dir=${dir}`, origin], { stdio: ["ignore", "ignore", "pipe"], detached: true });
    chrome = proc;
    let stderr = "";
    proc.stderr?.on("data", (chunk: Buffer) => {
      stderr = (stderr + chunk.toString()).slice(-4096);
    });
    let spawnError: Error | undefined;
    proc.once("error", (e) => {
      spawnError = e;
    });
    const portFile = join(dir, "DevToolsActivePort");
    // Chrome writes the port, then the browser target path on a second line.
    const port = () => {
      const lines = existsSync(portFile) ? readFileSync(portFile, "utf8").split("\n") : [];
      return lines.length >= 2 && /^\d+$/.test(lines[0] ?? "") ? lines[0] : undefined;
    };
    const deadline = Date.now() + 30_000;
    while (!port() && !spawnError && proc.exitCode === null && proc.signalCode === null && Date.now() < deadline) await new Promise((r) => setTimeout(r, 100));
    const found = port();
    if (found) return found;
    await stopChrome(proc);
    failures.push(`attempt ${attempt}: ${spawnError ? `spawn error ${spawnError.message}` : proc.exitCode !== null || proc.signalCode !== null ? `Chrome exited (code ${proc.exitCode}, signal ${proc.signalCode})` : "no DevToolsActivePort after 30s"}; stderr tail: ${stderr.trim() || "(empty)"}`);
  }
  throw new Error(`Chrome fixture did not start.\n${failures.join("\n")}`);
}

async function run<T>(expression: string): Promise<T> {
  const id = ++nextId;
  type Evaluated = { result: { value: T }; exceptionDetails?: { exception?: { description?: string }; text: string } };
  const result = await new Promise<Evaluated>((resolve, reject) => {
    pending.set(id, { resolve: (v) => resolve(v as Evaluated), reject });
    socket?.send(JSON.stringify({ id, method: "Runtime.evaluate", params: { expression, awaitPromise: true, returnByValue: true } }));
  });
  if (result.exceptionDetails) throw new Error(result.exceptionDetails.exception?.description ?? result.exceptionDetails.text);
  return result.result.value;
}

const reset = () =>
  run(`(async () => { localStorage.clear(); for (const d of await indexedDB.databases()) await new Promise((r) => { const q = indexedDB.deleteDatabase(d.name); q.onsuccess = q.onerror = q.onblocked = r; }); })()`);

/** Seeds an out-of-line store with a plain record plus typed records and keys. */
const SEED = `(async () => {
  const open = indexedDB.open("app", 1);
  open.onupgradeneeded = () => { open.result.createObjectStore("files"); open.result.createObjectStore("notes", { keyPath: "id" }).createIndex("by_tag", "tag"); };
  const db = await new Promise((res, rej) => { open.onsuccess = () => res(open.result); open.onerror = () => rej(open.error); });
  const tx = db.transaction(["files", "notes"], "readwrite");
  const files = tx.objectStore("files"); const notes = tx.objectStore("notes");
  files.put({ name: "plain", tags: ["a", 1, true, null], nested: { deep: [{ x: 1.5 }] } }, "plain");
  files.put({ name: "avatar.png", bytes: new Uint8Array([137, 80, 78, 71]) }, "typed-array");
  files.put({ blob: new Blob(["hello"]) }, "blob");
  files.put({ savedAt: new Date(0) }, "date-value");
  files.put({ m: new Map([["k", 1]]) }, "map");
  files.put("chunk", new Uint8Array([1, 2, 3]).buffer);
  files.put("dated", new Date(5));
  files.put("compound", ["user", 42]);
  notes.put({ id: 1, tag: "t", text: "hello" });
  notes.put({ id: 2, tag: "t", at: new Date(1) });
  await new Promise((res) => (tx.oncomplete = res));
  db.close();
  localStorage.setItem("theme", "dark");
})()`;

const READ = `(async () => {
  const o = indexedDB.open("app"); const db = await new Promise((res) => (o.onsuccess = () => res(o.result)));
  const all = (name) => new Promise((res) => { const t = db.transaction(name).objectStore(name); const k = t.getAllKeys(); k.onsuccess = () => { const v = t.getAll(); v.onsuccess = () => res(k.result.map((key, i) => [key, v.result[i]])); }; });
  const out = { files: await all("files"), notes: await all("notes"), theme: localStorage.getItem("theme") }; db.close(); return out;
})()`;

type Collected = OriginData & { nonJsonValues: number; sessionStorageKeys: number; tooLarge?: boolean };

test("F3: typed values and keys are counted and skipped, never converted; the rest round-trips", { skip }, async () => {
  await reset();
  await run(SEED);
  const data = await run<Collected>(COLLECT(2 << 20));
  const files = data.indexedDB[0]?.stores.find((s) => s.name === "files");
  const notes = data.indexedDB[0]?.stores.find((s) => s.name === "notes");
  assert.deepEqual(files?.records.map((r) => r.k), ["plain", ["user", 42]]);
  assert.deepEqual(notes?.records.map((r) => (r.v as { id: number }).id), [1]);
  // typed-array, blob, date-value, map values; ArrayBuffer and Date keys; note 2's Date.
  assert.equal(data.nonJsonValues, 7);
  assert.ok(!JSON.stringify(data).includes('"0":137'), "no typed array became an object");

  await reset();
  const result = await run<{ idbRecords: number; failedRecords: number }>(RESTORE({ localStorage: data.localStorage, indexedDB: data.indexedDB }));
  assert.deepEqual(result, { localStorageKeys: 1, idbRecords: 3, failedRecords: 0 });
  const back = await run<{ files: [unknown, unknown][]; notes: [unknown, unknown][]; theme: string }>(READ);
  assert.deepEqual(back.files, [["plain", { name: "plain", tags: ["a", 1, true, null], nested: { deep: [{ x: 1.5 }] } }], [["user", 42], "compound"]]);
  assert.deepEqual(back.notes, [[1, { id: 1, tag: "t", text: "hello" }]]);
  assert.equal(back.theme, "dark");
});

test("F3: a checkpoint written by the old serializer (binary key saved as {}) restores its valid records instead of throwing", { skip }, async () => {
  await reset();
  // Shape recorded by the reviewer from the frozen COLLECT (idb-roundtrip.out).
  const legacy: OriginData = {
    localStorage: { theme: "dark" },
    indexedDB: [{ name: "app", version: 1, stores: [{ name: "files", keyPath: null, autoIncrement: false, indexes: [], records: [{ k: "avatar", v: { name: "avatar.png" } }, { k: {}, v: "chunk" }] }] }],
  };
  const result = await run<{ idbRecords: number; failedRecords: number }>(RESTORE(legacy));
  assert.deepEqual(result, { localStorageKeys: 1, idbRecords: 1, failedRecords: 1 });
  const back = await run<{ files: [unknown, unknown][] }>(READ.replace('notes: await all("notes"), ', ""));
  assert.deepEqual(back.files, [["avatar", { name: "avatar.png" }]]);
});

test("F5: COLLECT stops at its byte cap and says so", { skip }, async () => {
  await reset();
  await run(`localStorage.setItem("cache", "x".repeat(300000)); localStorage.setItem("small", "y")`);
  const capped = await run<Collected>(COLLECT(100_000));
  assert.equal(capped.tooLarge, true);
  assert.equal(capped.localStorage, undefined, "nothing partial is returned");
  const whole = await run<Collected>(COLLECT(2 << 20));
  assert.equal(whole.tooLarge, undefined);
  assert.equal(whole.localStorage.cache?.length, 300_000);
});
