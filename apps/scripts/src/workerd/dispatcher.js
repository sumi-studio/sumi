// sumi script dispatcher — the trusted parent worker of one job's workerd.
// Loads the user's module into a fresh isolate via the Worker Loader with
// globalOutbound: null, and exposes exactly one RPC capability: env.SUMI,
// whose file calls go to the state service's job-scoped file routes under
// the runner's claim. Credentials live only in this parent's env bindings;
// the script sees method stubs, never props, tokens, or the network.

import { WorkerEntrypoint } from "cloudflare:workers";

const b64encode = (bytes) => btoa(String.fromCharCode(...new Uint8Array(bytes)));
const b64decode = (s) => Uint8Array.from(atob(s), (c) => c.charCodeAt(0));

class Budget {
  constructor(limits) {
    this.callsLeft = limits.file_calls;
    this.bytesLeft = limits.file_bytes;
    this.logLeft = limits.log_bytes;
    this.logs = [];
    this.logTruncated = false;
  }
  spendCall(bytes) {
    if (this.callsLeft <= 0) throw new Error("sumi.files call limit reached");
    this.callsLeft -= 1;
    this.bytesLeft -= bytes;
    if (this.bytesLeft < 0) throw new Error("sumi.files byte budget exceeded");
  }
  log(line) {
    if (this.logLeft <= 0) {
      this.logTruncated = true;
      return;
    }
    const cut = line.slice(0, this.logLeft);
    this.logLeft -= cut.length;
    this.logs.push(cut);
  }
}

// Budgets live in module state, keyed by job: ctx.props is a deserialized
// snapshot per RPC call, so mutations there are not guaranteed to persist.
const budgets = new Map();

export class SumiFiles extends WorkerEntrypoint {
  // ctx.props: { token, persona, jobId, runnerId, limits }
  #budget() {
    const key = this.ctx.props.jobId;
    let b = budgets.get(key);
    if (!b) { b = new Budget(this.ctx.props.limits); budgets.set(key, b); }
    return b;
  }

  async #call(op, fields) {
    const p = this.ctx.props;
    // env.SUMI_API is a service binding to the declared ExternalServer —
    // the only network egress in the process. The hostname on the URL is
    // cosmetic; every fetch on this binding goes to the state API.
    const res = await this.env.SUMI_API.fetch(
      `https://state.invalid/internal/core/personas/${p.persona}/jobs/${p.jobId}/files/${op}`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${p.token}`,
        },
        body: JSON.stringify({ runner_id: p.runnerId, ...fields }),
      },
    );
    const payload = await res.json().catch(() => ({}));
    if (!res.ok) {
      const e = new Error(payload.error ?? `file ${op} failed (${res.status})`);
      e.code = payload.code ?? `http_${res.status}`;
      throw e;
    }
    return payload;
  }

  #checkPath(path) {
    if (typeof path !== "string" || path === "" || path.length > 1024 || path.startsWith("/") || path.split("/").includes("..")) {
      throw new Error("path must be a scope-relative path without '..'");
    }
    return path;
  }

  async stat(path) {
    this.#budget().spendCall(0);
    const r = await this.#call("stat", { path: this.#checkPath(path) });
    return r.result;
  }

  async list(path = "", cursor = "") {
    this.#budget().spendCall(0);
    const r = await this.#call("list", { path: this.#checkPath(path || "."), cursor });
    return r.result;
  }

  async read(path, offset = 0, len = 65536) {
    this.#checkPath(path);
    this.#budget().spendCall(Math.min(len, 65536));
    const r = await this.#call("read", { path, offset, len });
    const out = r.result ?? {};
    out.text = out.data_base64 ? new TextDecoder().decode(b64decode(out.data_base64)) : "";
    return out;
  }

  async write(path, data, ifVersion = "any") {
    this.#checkPath(path);
    const bytes = typeof data === "string" ? new TextEncoder().encode(data)
      : data instanceof ArrayBuffer ? new Uint8Array(data)
      : ArrayBuffer.isView(data) ? new Uint8Array(data.buffer, data.byteOffset, data.byteLength)
      : null;
    if (!bytes) throw new Error("write data must be a string or bytes");
    if (bytes.length > 256 * 1024) throw new Error("write data exceeds 256 KiB per call");
    this.#budget().spendCall(bytes.length);
    const r = await this.#call("write", {
      path, if_version: ifVersion, data_base64: b64encode(bytes),
    });
    return r.op ?? r;
  }

  async mkdir(path) {
    this.#checkPath(path);
    this.#budget().spendCall(0);
    const r = await this.#call("mkdir", { path });
    return r.op ?? r;
  }

  async remove(path, ifVersion = "any") {
    this.#checkPath(path);
    this.#budget().spendCall(0);
    const r = await this.#call("remove", { path, if_version: ifVersion });
    return r.op ?? r;
  }

  async log(message) {
    this.#budget().log(String(message).slice(0, 2048));
    return true;
  }

  async _logs() {
    const b = this.#budget();
    return { lines: b.logs, truncated: b.logTruncated, calls_left: b.callsLeft, bytes_left: b.bytesLeft };
  }
}

const SHIM = `
import { run } from "./user.js";
export default {
  async fetch(request, env, ctx) {
    const input = await request.json();
    try {
      if (typeof run !== "function") throw new Error("module must export async function run(input, sumi)");
      // The advertised script contract is a single sumi capability:
      // sumi.files (stat/list/read/write/mkdir/remove over the job's
      // shared files) plus sumi.log. env.SUMI is the flat SumiFiles
      // entrypoint — it is exposed ONLY under .files, so the script sees
      // exactly the documented shape and no ambient flat alias.
      const sumi = { files: env.SUMI, log: (message) => env.SUMI.log(message) };
      const value = await run(input, sumi);
      return Response.json({ ok: true, value });
    } catch (e) {
      return Response.json(
        { ok: false, error: { name: e?.name ?? "Error", message: String(e?.message ?? e) } },
        { status: 200 },
      );
    }
  },
};
`;

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (url.pathname === "/ready") {
      return Response.json({ ready: true });
    }
    if (url.pathname !== "/run" || request.method !== "POST") {
      return Response.json({ error: "POST /run only" }, { status: 404 });
    }
    const { code, input = null } = await request.json();
    if (typeof code !== "string" || code.length === 0 || code.length > 64 * 1024) {
      return Response.json({ error: "code required (<=64KiB)" }, { status: 400 });
    }

    // The only capability: the job-scoped file/log binding. The script
    // cannot reach ctx.props, the persona token, or any other env.
    const sumi = ctx.exports.SumiFiles({
      props: {
        token: env.SUMI_TOKEN, persona: env.SUMI_PERSONA,
        jobId: env.SUMI_JOB, runnerId: env.SUMI_RUNNER,
        limits: {
          file_calls: Number(env.SUMI_MAX_FILE_CALLS), file_bytes: Number(env.SUMI_MAX_FILE_BYTES),
          log_bytes: Number(env.SUMI_MAX_LOG_BYTES),
        },
      },
    });
    const worker = env.LOADER.load({
      compatibilityDate: "2026-08-04",
      mainModule: "shim.js",
      modules: { "shim.js": SHIM, "user.js": code },
      // No raw network for the script: every fetch()/connect() throws.
      globalOutbound: null,
      env: { SUMI: sumi },
      limits: { cpuMs: Number(env.SUMI_CPU_MS), subRequests: 0 },
    });

    const started = Date.now();
    try {
      const res = await worker.getEntrypoint().fetch(
        new Request("https://script.invalid/run", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(input),
        }),
      );
      const payload = await res.json();
      let logs = null;
      try { logs = await sumi._logs(); } catch { logs = null; }
      return Response.json({ ...payload, logs, wall_ms: Date.now() - started });
    } catch (e) {
      let logs = null;
      try { logs = await sumi._logs(); } catch { logs = null; }
      return Response.json({
        ok: false,
        error: { name: e?.name ?? "Error", message: String(e?.message ?? e) },
        logs,
        wall_ms: Date.now() - started,
      });
    }
  },
};
