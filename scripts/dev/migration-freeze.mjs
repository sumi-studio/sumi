#!/usr/bin/env node

import { execFile as execFileCallback } from "node:child_process";
import { createHash } from "node:crypto";
import { readdir, readFile, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { promisify } from "node:util";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const migrationDir = path.join(repoRoot, "apps/api/internal/db/migrations");
const manifestPath = path.join(migrationDir, "FROZEN.sha256");
const migrationTreePath = "apps/api/internal/db/migrations";
const execFile = promisify(execFileCallback);

async function manifestForAssets(names, readAsset) {
  const files = names.filter((name) => name.endsWith(".sql")).sort();
  const lines = [];
  for (const name of files) {
    const content = await readAsset(name);
    const digest = createHash("sha256").update(content).digest("hex");
    lines.push(`${digest}  ${name}`);
  }
  return `${lines.join("\n")}\n`;
}

async function manifest() {
  const names = await readdir(migrationDir);
  return manifestForAssets(names, (name) => readFile(path.join(migrationDir, name)));
}

const migrationName = /^(\d{4})_([a-z0-9_]+)\.(up|down)\.sql$/;
const exactCommit = /^[0-9a-f]{40}$/;

function entries(text) {
  if (!text.endsWith("\n") || text.length === 1) {
    throw new Error("migration freeze manifest must be non-empty and end with a newline");
  }
  const parsed = text.slice(0, -1).split("\n").map((line) => {
    const [digest, name, ...extra] = line.split("  ");
    const match = migrationName.exec(name ?? "");
    if (extra.length !== 0 || line !== `${digest}  ${name}` ||
        !/^[0-9a-f]{64}$/.test(digest ?? "") || !match) {
      throw new Error(`invalid migration freeze entry: ${line}`);
    }
    return { line, digest, name, version: Number(match[1]), stem: match[2], direction: match[3] };
  });
  for (let index = 1; index < parsed.length; index++) {
    if (parsed[index - 1].name >= parsed[index].name) {
      throw new Error("migration freeze entries must use canonical filename order");
    }
  }
  return parsed;
}

export function validateSeal(actualText) {
  const grouped = new Map();
  for (const entry of entries(actualText)) {
    const group = grouped.get(entry.version) ?? [];
    group.push(entry);
    grouped.set(entry.version, group);
  }
  for (const [version, pair] of grouped) {
    if (pair.length !== 2 || new Set(pair.map((entry) => entry.direction)).size !== 2 ||
        new Set(pair.map((entry) => entry.stem)).size !== 1) {
      throw new Error(`migration version ${version} must have one matching up/down pair`);
    }
  }
}

export function validateExactSeal(expectedText, actualText) {
  validateSeal(expectedText);
  validateSeal(actualText);
  if (expectedText !== actualText) {
    throw new Error("migration history differs from FROZEN.sha256");
  }
}

export function validateExtension(expectedText, actualText) {
  validateSeal(expectedText);
  validateSeal(actualText);
  const expected = entries(expectedText);
  const actual = entries(actualText);
  for (let index = 0; index < expected.length; index++) {
    if (actual[index]?.line !== expected[index].line) {
      throw new Error(`sealed migration changed, disappeared, or moved: ${expected[index].line}`);
    }
  }
  const added = actual.slice(expected.length);
  const sealedMaximum = Math.max(...expected.map((entry) => entry.version));
  const addedVersions = new Set(added.map((entry) => entry.version));
  if (addedVersions.size !== 1) {
    throw new Error("extend must seal exactly one new migration version");
  }
  const [newVersion] = addedVersions;
  if (newVersion !== sealedMaximum + 1) {
    throw new Error(`new migration version ${newVersion} must immediately follow sealed maximum ${sealedMaximum}`);
  }
  const pair = added.filter((entry) => entry.version === newVersion);
  if (pair.length !== 2 || new Set(pair.map((entry) => entry.direction)).size !== 2 ||
      new Set(pair.map((entry) => entry.stem)).size !== 1) {
    throw new Error(`migration version ${newVersion} must add one matching up/down pair`);
  }
}

// validateCandidateAgainstBase compares two complete snapshots. baseManifest
// is undefined only for the one-time initial seal. Once a base seal exists,
// it is the external append-only anchor; the candidate cannot bless a rewrite
// by changing SQL and its in-tree digest together.
export function validateCandidateAgainstBase({
  baseManifest,
  baseActual,
  candidateManifest,
  candidateActual,
}) {
  validateSeal(baseActual);
  validateExactSeal(candidateManifest, candidateActual);

  if (baseManifest === undefined) {
    if (candidateManifest !== baseActual) {
      throw new Error("initial seal must preserve and exactly seal the base migration SQL assets");
    }
    return "initial seal";
  }

  validateExactSeal(baseManifest, baseActual);
  if (candidateManifest === baseManifest) {
    return "unchanged seal";
  }
  validateExtension(baseManifest, candidateManifest);
  return "one-version extension";
}

async function gitOutput(root, args) {
  try {
    const { stdout } = await execFile("git", args, {
      cwd: root,
      encoding: null,
      maxBuffer: 32 * 1024 * 1024,
    });
    return stdout;
  } catch (error) {
    throw new Error(`cannot read base Git object with git ${args[0]}: ${error.message}`);
  }
}

async function baseSnapshot(root, baseCommit) {
  if (!exactCommit.test(baseCommit ?? "") || /^0{40}$/.test(baseCommit)) {
    throw new Error("base commit must be an exact lowercase 40-hex object ID");
  }
  const objectType = (await gitOutput(root, ["cat-file", "-t", baseCommit])).toString("utf8").trim();
  if (objectType !== "commit") {
    throw new Error(`base object ${baseCommit} is ${objectType || "unknown"}, not a commit`);
  }

  const tree = await gitOutput(root, [
    "ls-tree", "-rz", "--name-only", baseCommit, "--", migrationTreePath,
  ]);
  const prefix = `${migrationTreePath}/`;
  const names = tree.toString("utf8").split("\0").filter(Boolean).map((entryPath) => {
    if (!entryPath.startsWith(prefix)) {
      throw new Error(`unexpected migration path in base tree: ${entryPath}`);
    }
    return entryPath.slice(prefix.length);
  });
  const readBaseAsset = (name) => gitOutput(root, [
    "cat-file", "blob", `${baseCommit}:${migrationTreePath}/${name}`,
  ]);
  const actual = await manifestForAssets(names, readBaseAsset);
  const frozen = names.includes("FROZEN.sha256")
    ? (await readBaseAsset("FROZEN.sha256")).toString("utf8")
    : undefined;
  return { manifest: frozen, actual };
}

export async function verifyAgainstBase(baseCommit, root = repoRoot) {
  const candidateDir = path.join(root, migrationTreePath);
  const candidateActualPromise = readdir(candidateDir).then((names) => manifestForAssets(
    names,
    (name) => readFile(path.join(candidateDir, name)),
  ));
  const [base, candidateManifest, candidateActual] = await Promise.all([
    baseSnapshot(root, baseCommit),
    readFile(path.join(candidateDir, "FROZEN.sha256"), "utf8"),
    candidateActualPromise,
  ]);
  return validateCandidateAgainstBase({
    baseManifest: base.manifest,
    baseActual: base.actual,
    candidateManifest,
    candidateActual,
  });
}

async function main() {
  const mode = process.argv[2];
  if (mode === "seal") {
    const actual = await manifest();
    validateSeal(actual);
    await writeFile(manifestPath, actual, { flag: "wx" });
    console.log(`sealed ${path.relative(repoRoot, manifestPath)}`);
  } else if (mode === "extend") {
    const [expected, actual] = await Promise.all([
      readFile(manifestPath, "utf8"),
      manifest(),
    ]);
    validateSeal(actual);
    validateExtension(expected, actual);
    await writeFile(manifestPath, actual);
    console.log(`extended ${path.relative(repoRoot, manifestPath)}`);
  } else if (mode === "check") {
    const [expected, actual] = await Promise.all([
      readFile(manifestPath, "utf8"),
      manifest(),
    ]);
    validateExactSeal(expected, actual);
    console.log("migration history matches FROZEN.sha256");
  } else if (mode === "verify-base") {
    if (process.argv.length !== 4) {
      throw new Error("verify-base requires exactly one base commit object ID");
    }
    const result = await verifyAgainstBase(process.argv[3]);
    console.log(`migration history is valid against base commit: ${result}`);
  } else {
    console.error("usage: node scripts/dev/migration-freeze.mjs seal|extend|check|verify-base <40-hex-commit>");
    process.exitCode = 2;
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  try {
    await main();
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
