// LOCAL DEVELOPMENT ONLY — not Cloud evidence. wrangler dev has no Browser
// Run acquire(), so with LOCAL_POOL set in .dev.vars the Worker asks this
// process instead: one headless google-chrome per "session", with the
// fixture hosts resolved to the local wrangler dev server.
//   POOL_PORT (default 19401)  FIXTURE_ORIGIN (default 127.0.0.1:19402)
//   CHROME (default google-chrome)
import { spawn } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import http from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";

const PORT = Number(process.env.POOL_PORT ?? 19401);
const FIXTURE = process.env.FIXTURE_ORIGIN ?? "127.0.0.1:19402";
const CHROME = process.env.CHROME ?? "google-chrome";
const sessions = new Map();

async function launch() {
  const dir = mkdtempSync(join(tmpdir(), "sumi-cloud-browser-20260927-chrome-"));
  const proc = spawn(
    CHROME,
    [
      "--headless=new",
      "--no-first-run",
      "--no-default-browser-check",
      "--remote-debugging-port=0",
      "--remote-allow-origins=*",
      "--window-size=1280,800",
      `--user-data-dir=${dir}`,
      `--host-resolver-rules=MAP app.sumi-fixture.test ${FIXTURE}, MAP docs.sumi-fixture.test ${FIXTURE}`,
      "about:blank",
    ],
    { stdio: "ignore" },
  );
  const portFile = join(dir, "DevToolsActivePort");
  for (let i = 0; i < 100 && !existsSync(portFile); i++) await new Promise((r) => setTimeout(r, 100));
  const [port, path] = readFileSync(portFile, "utf8").trim().split("\n");
  const id = crypto.randomUUID();
  sessions.set(id, { proc, dir, ws: `http://127.0.0.1:${port}${path}`, startTime: Date.now() });
  proc.on("exit", () => sessions.delete(id));
  return id;
}

function close(id) {
  const s = sessions.get(id);
  if (!s) return false;
  s.proc.kill("SIGKILL");
  sessions.delete(id);
  setTimeout(() => rmSync(s.dir, { recursive: true, force: true }), 500);
  return true;
}

http
  .createServer(async (req, res) => {
    const url = new URL(req.url, "http://pool.invalid");
    const send = (v, status = 200) => {
      res.writeHead(status, { "content-type": "application/json" });
      res.end(JSON.stringify(v));
    };
    const id = decodeURIComponent(url.pathname.split("/")[2] ?? "");
    if (req.method === "POST" && url.pathname === "/acquire") return send({ sessionId: await launch() });
    if (req.method === "GET" && url.pathname === "/sessions")
      return send([...sessions.entries()].map(([sessionId, s]) => ({ sessionId, startTime: s.startTime })));
    // Test hook: kill a browser without telling its host (simulated loss).
    if (req.method === "POST" && url.pathname.startsWith("/kill/")) return send({ killed: close(id) });
    const s = sessions.get(id);
    if (req.method === "GET" && url.pathname.startsWith("/session/"))
      return send(s ? { sessionId: id, startTime: s.startTime, ws: s.ws } : null);
    if (req.method === "DELETE" && url.pathname.startsWith("/session/")) return send({ status: close(id) ? "closing" : "closed" });
    send({ error: "not found" }, 404);
  })
  .listen(PORT, "127.0.0.1", () => console.log(`local pool on 127.0.0.1:${PORT}`));

const stop = () => {
  for (const id of [...sessions.keys()]) close(id);
  process.exit(0);
};
process.on("SIGTERM", stop);
process.on("SIGINT", stop);
