/**
 * In-process filesvc stub implementing the /v1/files/{scope}/{op}
 * protocol the fileaccess client speaks: scoped stat/list/read plus
 * keyed write/mkdir/remove with committed-receipt idempotency. Files
 * live in a Map per scope — this is a test double for the HTTP
 * contract, not for JuiceFS semantics.
 *
 * Fault knobs per scope (or global):
 *  - delayMs: hold the response (the commit still happens on schedule —
 *    a client timeout does NOT un-commit the effect, matching the real
 *    service's semantics).
 *  - dropResponse: commit then destroy the connection — the client sees
 *    a transport failure over a committed effect.
 *  - failStatus: answer a determinate error without committing.
 */

import * as http from "node:http";
import { createHash } from "node:crypto";

interface StoredFile { kind: "file" | "dir"; body: Buffer; version: number }
interface Receipt { hash: string; response: Record<string, unknown>; status: number }

export class StubFileSvc {
  readonly files = new Map<string, Map<string, StoredFile>>();
  readonly receipts = new Map<string, Receipt>();
  faults = { delayMs: 0, dropResponse: false, failStatus: 0 };
  server!: http.Server;
  url = "";
  private versions = new Map<string, number>();

  scopeFiles(scope: string): Map<string, StoredFile> {
    let m = this.files.get(scope);
    if (!m) { m = new Map(); this.files.set(scope, m); }
    return m;
  }

  bump(scope: string): number {
    const v = (this.versions.get(scope) ?? 0) + 1;
    this.versions.set(scope, v);
    return v;
  }

  private async handle(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
    const u = new URL(req.url ?? "", "http://x");
    const m = /^\/v1\/files\/([0-9a-f]{32})\/(stat|list|read|write|mkdir|remove)$/.exec(u.pathname);
    if (!m) { res.writeHead(404).end(`{"error":"no route"}`); return; }
    const scope = m[1]!, op = m[2]!;
    if (req.headers.authorization !== "Bearer svc-token") {
      res.writeHead(401, { "content-type": "application/json" }).end(`{"error":"unauthorized","code":"unauthorized"}`);
      return;
    }
    const path = u.searchParams.get("path") ?? "";
    const body = await new Promise<Buffer>((resolve) => {
      const chunks: Buffer[] = [];
      req.on("data", (c) => chunks.push(c));
      req.on("end", () => resolve(Buffer.concat(chunks)));
    });

    const answer = (): { status: number; json?: Record<string, unknown>; raw?: Buffer; headers?: Record<string, string> } => {
      const fs = this.scopeFiles(scope);
      const f = fs.get(path);
      const version = this.versions.get(scope) ?? 0;
      switch (op) {
        case "stat":
          if (!f) return { status: 404, json: { error: "not found", code: "not_found" } };
          return { status: 200, json: { kind: f.kind, size: f.body.length, mtime_ns: 0, version: f.version, fingerprint: createHash("sha256").update(f.body).digest("hex"), external_change: false } };
        case "list": {
          const prefix = path === "." ? "" : path.replace(/\/?$/, "/");
          const entries = [...fs.entries()]
            .filter(([p]) => p.startsWith(prefix))
            .map(([p, e]) => ({ path: p, kind: e.kind, size: e.body.length, version: e.version }));
          return { status: 200, json: { entries, next_cursor: "" } };
        }
        case "read": {
          if (!f || f.kind !== "file") return { status: 404, json: { error: "not found", code: "not_found" } };
          const off = Number(u.searchParams.get("offset") ?? 0);
          const len = u.searchParams.has("len") ? Number(u.searchParams.get("len")) : -1;
          const slice = len >= 0 ? f.body.subarray(off, off + len) : f.body.subarray(off);
          return { status: 200, raw: slice, headers: { "X-File-Version": String(f.version) } };
        }
        case "write":
        case "mkdir":
        case "remove": {
          const key = req.headers["x-idempotency-key"] as string | undefined;
          const ifv = req.headers["if-version"] as string | undefined;
          const requestHash = createHash("sha256")
            .update(JSON.stringify({ op, path, ifv, body: body.toString("base64") })).digest("hex");
          if (key) {
            const rec = this.receipts.get(`${scope}:${key}`);
            if (rec) {
              if (rec.hash !== requestHash) {
                return { status: 409, json: { error: "same key, different request", code: "idempotency_conflict" } };
              }
              return { status: rec.status, json: { ...rec.response, replayed: true } };
            }
          }
          // if-version check
          if (op === "write" || op === "remove") {
            const cur = fs.get(path);
            if (ifv === "none" && cur) return { status: 412, json: { error: "exists", code: "version_conflict" } };
            if (ifv && ifv !== "any" && ifv !== "none" && Number(ifv) !== (cur?.version ?? -1)) {
              return { status: 412, json: { error: "version mismatch", code: "version_conflict" } };
            }
          }
          if (this.faults.failStatus) {
            return { status: this.faults.failStatus, json: { error: "injected failure", code: "injected" } };
          }
          const v = this.bump(scope);
          if (op === "write") fs.set(path, { kind: "file", body, version: v });
          if (op === "mkdir") fs.set(path, { kind: "dir", body: Buffer.alloc(0), version: v });
          if (op === "remove") fs.delete(path);
          const response: Record<string, unknown> = op === "remove" ? { removed: true } : { version: v };
          if (key) this.receipts.set(`${scope}:${key}`, { hash: requestHash, response, status: 200 });
          // Real filesvc omits `replayed` on a fresh success and includes
          // replayed:true only on a receipt replay — the stub mirrors that
          // optional-field contract, not a stronger one.
          return { status: 200, json: { ...response } };
        }
      }
      return { status: 400, json: { error: "bad op", code: "bad_op" } };
    };

    const commit = answer();
    const send = () => {
      if (this.faults.dropResponse) { req.socket.destroy(); return; }
      if (commit.raw) {
        res.writeHead(commit.status, { "content-type": "application/octet-stream", ...(commit.headers ?? {}) }).end(commit.raw);
      } else {
        res.writeHead(commit.status, { "content-type": "application/json", ...(commit.headers ?? {}) }).end(JSON.stringify(commit.json ?? {}));
      }
    };
    if (this.faults.delayMs > 0) setTimeout(send, this.faults.delayMs);
    else send();
  }

  async start(): Promise<string> {
    this.server = http.createServer((req, res) => { void this.handle(req, res); });
    await new Promise<void>((r) => this.server.listen(0, "127.0.0.1", r));
    const addr = this.server.address() as { port: number };
    this.url = `http://127.0.0.1:${addr.port}`;
    return this.url;
  }

  async stop(): Promise<void> {
    await new Promise((r) => this.server.close(r));
  }
}
