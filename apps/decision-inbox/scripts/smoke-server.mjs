import { spawn, spawnSync } from "node:child_process";
import { rmSync } from "node:fs";
import { resolve } from "node:path";

const pnpm = process.platform === "win32" ? "pnpm.cmd" : "pnpm";
const persistTo = ".wrangler/smoke-state";
const resolvedPersistTo = resolve(persistTo);
if (!resolvedPersistTo.endsWith("/.wrangler/smoke-state")) {
  throw new Error("Refusing to clear an unexpected smoke-state path");
}
rmSync(resolvedPersistTo, { force: true, recursive: true });
const migration = spawnSync(
  pnpm,
  [
    "exec",
    "wrangler",
    "d1",
    "migrations",
    "apply",
    "sumi-decision-inbox",
    "--local",
    "--persist-to",
    persistTo,
  ],
  { stdio: "inherit" },
);

if (migration.status !== 0) process.exit(migration.status ?? 1);

const worker = spawn(
  pnpm,
  [
    "exec",
    "wrangler",
    "dev",
    "--local",
    "--port",
    "8794",
    "--persist-to",
    persistTo,
    "--env-file",
    ".dev.vars.example",
  ],
  { stdio: "inherit" },
);

for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, () => worker.kill(signal));
}

worker.on("exit", (code) => process.exit(code ?? 0));
