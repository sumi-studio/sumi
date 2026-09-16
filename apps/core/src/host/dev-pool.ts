/**
 * Dev pool host: the local-development half of the Cloud wake contract.
 *
 * The Go state service's RuntimeWaker POSTs {SUMI_DEV_POOL_LISTEN}
 * /personas/{id}/wake when a persona has work and no live writer — the same
 * route the workerd host serves in front of its Durable Objects. Here each
 * wake ensures one `local.ts` child process runs the persona; the child
 * polls the state service itself, so a wake only ever has to start it.
 *
 *   SUMI_DEV_POOL_LISTEN      127.0.0.1:PORT — loopback only (required)
 *   SUMI_STATE_URL            base URL of the Go API (required)
 *   SUMI_CORE_WAKE_TOKEN      bearer required on /personas/:id/wake (required)
 *   SUMI_CORE_RUNTIME_TOKEN   state credential handed to each child; the same
 *                             runtime credential the Cloud host uses (required)
 *   SUMI_MODEL_*              inherited by every child (provider env contract)
 *   SUMI_DEV_POOL_CHILD       child entrypoint (default: sibling local.ts)
 *
 * Persona state lives in the state service: children hold nothing canonical,
 * are safe to kill at any point, and are respawned here or re-woken by the
 * waker's next sweep. The writer lease in the state service is what makes a
 * replacement child the single operative owner of a persona.
 */

import { spawn } from "node:child_process";
import {
  createServer,
  type IncomingMessage,
  type ServerResponse,
} from "node:http";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const WAKE_ROUTE = /^\/personas\/([^/]+)\/wake$/;
const UUIDV7 =
  /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

function env(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`missing env ${name}`);
  return v;
}

function log(msg: string, fields?: Record<string, unknown>): void {
  console.log(`[dev-pool] ${msg}`, fields ? JSON.stringify(fields) : "");
}

/** Child handle so tests can inject a fake without real node processes. */
export interface PoolChild {
  readonly pid?: number;
  on(event: "exit", cb: (code: number | null, signal: string | null) => void): void;
  kill(signal?: number | NodeJS.Signals): void;
}

export interface DevPoolOptions {
  stateURL: string;
  runtimeToken: string;
  /** Environment inherited by every child (model provider config). */
  childEnv?: NodeJS.ProcessEnv;
  /** Child process factory; default spawns `node <sibling local.ts>`. */
  spawnChild?: (personaID: string, env: NodeJS.ProcessEnv) => PoolChild;
  /** Base delay before respawning a child that exited on its own. */
  respawnDelayMs?: number;
  log?: (msg: string, fields?: Record<string, unknown>) => void;
}

export class DevPool {
  private readonly opts: Required<Omit<DevPoolOptions, "childEnv" | "spawnChild">> & {
    childEnv: NodeJS.ProcessEnv;
    spawnChild: NonNullable<DevPoolOptions["spawnChild"]>;
  };
  private readonly children = new Map<string, PoolChild>();
  private readonly failures = new Map<string, number>();
  private stopping = false;

  constructor(opts: DevPoolOptions) {
    const childEnv = opts.childEnv ?? process.env;
    this.opts = {
      stateURL: opts.stateURL,
      runtimeToken: opts.runtimeToken,
      childEnv,
      spawnChild:
        opts.spawnChild ??
        ((_personaID, childEnvForPersona) =>
          spawn(
            process.execPath,
            [
              join(
                dirname(fileURLToPath(import.meta.url)),
                "local.ts",
              ),
            ],
            {
              env: childEnvForPersona,
              stdio: ["ignore", "inherit", "inherit"],
            },
          )),
      respawnDelayMs: opts.respawnDelayMs ?? 1_000,
      log: opts.log ?? log,
    };
  }

  /** The wake route's job: return once a child for this persona is running. */
  ensure(personaID: string): void {
    if (this.stopping) return;
    const existing = this.children.get(personaID);
    if (existing) return;
    const childEnv: NodeJS.ProcessEnv = {
      ...this.opts.childEnv,
      SUMI_STATE_URL: this.opts.stateURL,
      SUMI_PERSONA_ID: personaID,
      SUMI_PERSONA_TOKEN: this.opts.runtimeToken,
      SUMI_HOLDER_ID: `dev-pool-${process.pid}`,
    };
    const child = this.opts.spawnChild(personaID, childEnv);
    this.children.set(personaID, child);
    this.opts.log("secretary host started", { persona: personaID, pid: child.pid });
    child.on("exit", (code, signal) => {
      this.children.delete(personaID);
      if (this.stopping) return;
      const fast = (this.failures.get(personaID) ?? 0) + 1;
      this.failures.set(personaID, fast);
      // A child that dies early usually means a configuration fault; bound
      // the respawn so a crash loop cannot spin. Pending work also re-wakes
      // through the state service's own sweep, independently of this delay.
      const delay = this.opts.respawnDelayMs * Math.min(2 ** (fast - 1), 60);
      this.opts.log("secretary host exited; respawning", {
        persona: personaID,
        code,
        signal,
        in_ms: delay,
      });
      setTimeout(() => this.ensure(personaID), delay);
    });
  }

  async stop(): Promise<void> {
    this.stopping = true;
    const children = [...this.children.values()];
    for (const child of children) child.kill("SIGTERM");
    const deadline = Date.now() + 3_000;
    while (Date.now() < deadline && this.children.size > 0) {
      await new Promise((r) => setTimeout(r, 50));
    }
    for (const child of this.children.values()) child.kill("SIGKILL");
    this.children.clear();
  }
}

function send(res: ServerResponse, status: number, body: string): void {
  res.writeHead(status, { "content-type": "application/json" });
  res.end(JSON.stringify({ error: body }));
}

export function serve(pool: DevPool, listen: string, wakeToken: string) {
  const m = /^([0-9.]+):([0-9]+)$/.exec(listen);
  if (!m) throw new Error(`SUMI_DEV_POOL_LISTEN must be host:port, got ${listen}`);
  const host = m[1];
  const port = Number(m[2]);
  if (host !== "127.0.0.1" && host !== "::1") {
    // The wake token is a bearer credential; this listener must never answer
    // off-loopback.
    throw new Error(`SUMI_DEV_POOL_LISTEN must be loopback, got ${listen}`);
  }
  const server = createServer((req: IncomingMessage, res: ServerResponse) => {
    if (req.method === "GET" && req.url === "/health") {
      res.writeHead(200, { "content-type": "application/json" });
      res.end("{}");
      return;
    }
    const wake = req.method === "POST" ? WAKE_ROUTE.exec(req.url ?? "") : null;
    if (!wake) {
      send(res, 404, "not found");
      return;
    }
    const header = req.headers.authorization ?? "";
    const presented = header.startsWith("Bearer ") ? header.slice(7) : "";
    if (presented !== wakeToken) {
      send(res, 401, "wake authorization failed");
      return;
    }
    const personaID = wake[1] ?? "";
    if (!UUIDV7.test(personaID)) {
      send(res, 400, "persona id is not a uuidv7");
      return;
    }
    pool.ensure(personaID);
    res.writeHead(200, { "content-type": "application/json" });
    res.end("{}");
  });
  server.listen({ host, port, exclusive: true });
  return server;
}

async function main() {
  const wakeToken = env("SUMI_CORE_WAKE_TOKEN");
  const pool = new DevPool({
    stateURL: env("SUMI_STATE_URL"),
    runtimeToken: env("SUMI_CORE_RUNTIME_TOKEN"),
  });
  const listen = env("SUMI_DEV_POOL_LISTEN");
  const server = serve(pool, listen, wakeToken);
  await new Promise<void>((resolvePromise, reject) => {
    server.once("error", reject);
    server.once("listening", resolvePromise);
  });
  log("dev core pool listening", { listen });

  const ac = new AbortController();
  process.on("SIGINT", () => ac.abort());
  process.on("SIGTERM", () => ac.abort());
  await new Promise<void>((resolvePromise) => {
    ac.signal.addEventListener("abort", () => resolvePromise(), {
      once: true,
    });
  });
  server.close();
  await pool.stop();
}

const isMain =
  process.argv[1] !== undefined &&
  fileURLToPath(import.meta.url) === process.argv[1];
if (isMain) {
  main().catch((e) => {
    console.error("[dev-pool] fatal:", e);
    process.exit(1);
  });
}
