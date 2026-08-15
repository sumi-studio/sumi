import { execFileSync } from "node:child_process";
import { mkdir, writeFile } from "node:fs/promises";
import { dirname, isAbsolute, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const scriptsDirectory = dirname(fileURLToPath(import.meta.url));
const webDirectory = resolve(scriptsDirectory, "..");
const repositoryRoot = resolve(webDirectory, "../..");
const configuredOutputDirectory = process.env.SUMI_WEB_DIST_DIR;
if (
  configuredOutputDirectory !== undefined &&
  (configuredOutputDirectory.trim() !== configuredOutputDirectory ||
    !isAbsolute(configuredOutputDirectory))
) {
  throw new Error(
    "SUMI_WEB_DIST_DIR must be an exact absolute path when provided",
  );
}
const outputDirectory =
  configuredOutputDirectory === undefined
    ? resolve(webDirectory, "dist")
    : configuredOutputDirectory;
const configuredSha = process.env.SUMI_RELEASE_SHA;
const releaseSha =
  configuredSha === undefined
    ? execFileSync("git", ["rev-parse", "HEAD"], {
        cwd: repositoryRoot,
        encoding: "utf8",
      }).trim()
    : configuredSha;

if (!/^[0-9a-f]{40}$/.test(releaseSha)) {
  throw new Error(
    "SUMI_RELEASE_SHA must be the exact lowercase 40-character Git SHA",
  );
}

const manifest = `${JSON.stringify({ release_sha: releaseSha })}\n`;
const output = resolve(outputDirectory, "release.json");
await mkdir(dirname(output), { recursive: true });
await writeFile(output, manifest, { encoding: "utf8", flag: "w" });
