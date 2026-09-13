import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { rmSync } from "node:fs";
import { mkdtemp, readdir, readFile, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

// Shared production build for the edge verification tests. The artifact and
// runtime tests both need a freshly built WebApp with a synthetic release
// identity; building it once per process keeps the assertions on a single
// artifact and removes the duplicate `pnpm run build`. Each process still
// builds from the current source, so a stale artifact can never be reused.

const run = promisify(execFile);
const scriptsDirectory = dirname(fileURLToPath(import.meta.url));

export const webDirectory = resolve(scriptsDirectory, "..");
export const deployableDistDirectory = resolve(webDirectory, "dist");
export const verificationReleaseSha =
  "0123456789abcdef0123456789abcdef01234567";

export interface ProductionArtifact {
  /** Absolute directory holding the isolated `dist` output. */
  directory: string;
  /** Synthetic release SHA the artifact was built with. */
  releaseSha: string;
  /** Fingerprint of `apps/web/dist` taken before the build started. */
  deployableDistFingerprintBeforeBuild: string | null;
}

let artifact: Promise<ProductionArtifact> | undefined;

export function buildProductionArtifactOnce(): Promise<ProductionArtifact> {
  artifact ??= buildProductionArtifact();
  return artifact;
}

async function buildProductionArtifact(): Promise<ProductionArtifact> {
  const temporaryDirectory = await mkdtemp(
    resolve(tmpdir(), "sumi-web-artifact-"),
  );
  process.once("exit", () => {
    rmSync(temporaryDirectory, { force: true, recursive: true });
  });
  const directory = resolve(temporaryDirectory, "dist");
  const deployableDistFingerprintBeforeBuild = await directoryFingerprint(
    deployableDistDirectory,
  );
  await run("pnpm", ["run", "build"], {
    cwd: webDirectory,
    env: {
      ...process.env,
      SUMI_RELEASE_SHA: verificationReleaseSha,
      SUMI_WEB_DIST_DIR: directory,
    },
    maxBuffer: 16 * 1024 * 1024,
  });
  return {
    directory,
    releaseSha: verificationReleaseSha,
    deployableDistFingerprintBeforeBuild,
  };
}

export async function directoryFingerprint(
  root: string,
): Promise<string | null> {
  if (!(await exists(root))) return null;
  const hash = createHash("sha256");
  for (const file of await listFiles(root)) {
    hash.update(file);
    hash.update("\0");
    hash.update(await readFile(resolve(root, file)));
    hash.update("\0");
  }
  return hash.digest("hex");
}

export async function listFiles(root: string): Promise<string[]> {
  const result: string[] = [];
  const visit = async (directory: string): Promise<void> => {
    for (const entry of await readdir(directory, { withFileTypes: true })) {
      const absolute = resolve(directory, entry.name);
      if (entry.isDirectory()) {
        await visit(absolute);
      } else if (entry.isFile()) {
        result.push(relative(root, absolute));
      } else {
        assert.fail(
          `production artifact contains non-regular entry ${absolute}`,
        );
      }
    }
  };
  await visit(root);
  return result.sort();
}

export async function exists(path: string): Promise<boolean> {
  try {
    await stat(path);
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
    throw error;
  }
}
