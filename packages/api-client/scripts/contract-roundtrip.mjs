#!/usr/bin/env node
/**
 * Round-trip the shared `contracts/agent-events-fixtures.json` through
 * JavaScript's JSON parser. This is the TypeScript/Node leg of the T28
 * three-language contract round-trip harness.
 */

import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const fixturePath = join(
  __dirname,
  "../../..",
  "contracts",
  "agent-events-fixtures.json",
);

const raw = readFileSync(fixturePath, "utf8");
const fixtures = JSON.parse(raw);

let passed = 0;
for (const [name, fixture] of Object.entries(fixtures)) {
  const kind = fixture.kind ?? "unknown";
  const wire = fixture.wire;
  if (wire === undefined) {
    throw new Error(`fixture '${name}' is missing 'wire'`);
  }

  const original = normalize(wire);
  const json = JSON.stringify(wire);
  const reparsed = normalize(JSON.parse(json));

  if (JSON.stringify(original) !== JSON.stringify(reparsed)) {
    console.error(`fixture '${name}' (${kind}) round-trip mismatch`);
    console.error("original:", JSON.stringify(original, null, 2));
    console.error("roundtrip:", JSON.stringify(reparsed, null, 2));
    process.exit(1);
  }
  passed += 1;
}

console.log(`contract round-trip: ${passed} fixtures passed`);
process.exit(0);

function normalize(value) {
  if (value === null || typeof value !== "object") {
    return value;
  }
  if (Array.isArray(value)) {
    return value.map(normalize);
  }
  const sorted = Object.keys(value).sort();
  const out = {};
  for (const key of sorted) {
    out[key] = normalize(value[key]);
  }
  return out;
}
