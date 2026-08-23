#!/usr/bin/env node

import { createHash } from "node:crypto";
import { readdir, readFile, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { pathToFileURL } from "node:url";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const migrationDir = path.join(repoRoot, "apps/api/internal/db/migrations");
const manifestPath = path.join(migrationDir, "FROZEN.sha256");

async function manifest() {
  const files = (await readdir(migrationDir))
    .filter((name) => name.endsWith(".sql"))
    .sort();
  const lines = [];
  for (const name of files) {
    const content = await readFile(path.join(migrationDir, name));
    const digest = createHash("sha256").update(content).digest("hex");
    lines.push(`${digest}  ${name}`);
  }
  return `${lines.join("\n")}\n`;
}

const migrationName = /^(\d{4})_([a-z0-9_]+)\.(up|down)\.sql$/;

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
    if (expected !== actual) {
      console.error("migration history differs from FROZEN.sha256");
      process.exitCode = 1;
    } else {
      console.log("migration history matches FROZEN.sha256");
    }
  } else {
    console.error("usage: node scripts/dev/migration-freeze.mjs seal|extend|check");
    process.exitCode = 2;
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  await main();
}
