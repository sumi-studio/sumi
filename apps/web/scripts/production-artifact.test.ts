import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtemp, readdir, readFile, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, relative, resolve } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const run = promisify(execFile);
const scriptsDirectory = dirname(fileURLToPath(import.meta.url));
const webDirectory = resolve(scriptsDirectory, "..");
const deployableDistDirectory = resolve(webDirectory, "dist");
const releaseSha = "0123456789abcdef0123456789abcdef01234567";

test("the isolated production WebApp artifact contains edge policy without mutating deployable dist", {
  timeout: 180_000,
}, async () => {
  const temporaryDirectory = await mkdtemp(
    resolve(tmpdir(), "sumi-web-artifact-"),
  );
  const artifactDirectory = resolve(temporaryDirectory, "dist");
  const deployableArtifactBefore = await directoryFingerprint(
    deployableDistDirectory,
  );
  try {
    await run("pnpm", ["run", "build"], {
      cwd: webDirectory,
      env: {
        ...process.env,
        SUMI_RELEASE_SHA: releaseSha,
        SUMI_WEB_DIST_DIR: artifactDirectory,
      },
      maxBuffer: 16 * 1024 * 1024,
    });

    assert.equal(
      await directoryFingerprint(deployableDistDirectory),
      deployableArtifactBefore,
      "artifact verification must not write its synthetic release identity into apps/web/dist",
    );

    const files = await listFiles(artifactDirectory);
    assert.ok(files.includes("index.html"));
    assert.ok(files.includes("_headers"));
    assert.ok(files.includes("release.json"));
    assert.ok(files.includes("theme-bootstrap.js"));
    assert.ok(
      files.some((file) =>
        /^assets\/.+\.[A-Za-z0-9_-]+\.(?:js|css)$/.test(file),
      ),
    );
    assert.equal(
      files.some((file) => file.includes("mcp-app-sandbox")),
      false,
      `dormant sandbox leaked into dist: ${files.join(", ")}`,
    );
    assert.equal(
      files.some((file) => file.toLowerCase().endsWith(".map")),
      false,
      `source map leaked into dist: ${files.join(", ")}`,
    );

    assert.equal(
      await readFile(resolve(artifactDirectory, "_headers"), "utf8"),
      await readFile(resolve(webDirectory, "public/_headers"), "utf8"),
    );
    assert.deepEqual(
      JSON.parse(
        await readFile(resolve(artifactDirectory, "release.json"), "utf8"),
      ),
      { release_sha: releaseSha },
    );
    assert.doesNotMatch(
      await readFile(resolve(artifactDirectory, "index.html"), "utf8"),
      /<script>(?!\s*<\/script>)/,
    );
    assert.match(
      await readFile(resolve(artifactDirectory, "index.html"), "utf8"),
      /<script src="\/theme-bootstrap\.js"><\/script>/,
    );
    assert.equal(
      await exists(resolve(webDirectory, "public/mcp-app-sandbox.html")),
      false,
    );
    assert.equal(
      await exists(resolve(webDirectory, "e2e/fixtures/mcp-app-sandbox.html")),
      true,
    );

    const sourceServiceWorker = await exists(
      resolve(webDirectory, "public/sw.js"),
    );
    assert.equal(
      await exists(resolve(artifactDirectory, "sw.js")),
      sourceServiceWorker,
      "sw.js must only exist in production when a real source asset exists",
    );
  } finally {
    await rm(temporaryDirectory, { force: true, recursive: true });
  }
});

test("an invalid or padded explicit release identity fails closed", async () => {
  const temporaryDirectory = await mkdtemp(
    resolve(tmpdir(), "sumi-web-invalid-release-"),
  );
  try {
    for (const invalid of [
      "",
      "not-a-git-sha",
      "A".repeat(40),
      ` ${releaseSha}`,
      `${releaseSha} `,
      `${releaseSha}\n`,
    ]) {
      await assert.rejects(
        run("node", ["scripts/write-release-manifest.mjs"], {
          cwd: webDirectory,
          env: {
            ...process.env,
            SUMI_RELEASE_SHA: invalid,
            SUMI_WEB_DIST_DIR: temporaryDirectory,
          },
        }),
        /SUMI_RELEASE_SHA must be the exact lowercase 40-character Git SHA/,
      );
    }
  } finally {
    await rm(temporaryDirectory, { force: true, recursive: true });
  }
});

async function directoryFingerprint(root: string): Promise<string | null> {
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

async function listFiles(root: string): Promise<string[]> {
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

async function exists(path: string): Promise<boolean> {
  try {
    await stat(path);
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
    throw error;
  }
}
