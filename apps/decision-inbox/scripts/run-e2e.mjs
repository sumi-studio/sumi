import { spawn, spawnSync } from "node:child_process";

const pnpm = process.platform === "win32" ? "pnpm.cmd" : "pnpm";

function run(args) {
  const result = spawnSync(pnpm, args, { stdio: "inherit" });
  if (result.status !== 0) process.exit(result.status ?? 1);
}

async function waitForServer(url, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const response = await fetch(url);
      if (response.ok) return;
    } catch {
      // The Worker is still starting.
    }
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  throw new Error(`Timed out waiting for ${url}`);
}

run(["run", "build:web"]);
const server = spawn(process.execPath, ["scripts/smoke-server.mjs"], {
  stdio: "inherit",
});

try {
  await waitForServer("http://127.0.0.1:8794/api/health", 120_000);
  run(["exec", "playwright", "test"]);
} finally {
  server.kill("SIGTERM");
}
