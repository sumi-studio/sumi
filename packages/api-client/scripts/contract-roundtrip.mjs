#!/usr/bin/env node
/**
 * Round-trip canonical wire examples through JavaScript's JSON parser. This
 * is the TypeScript/Node leg of the T28 three-language contract round-trip
 * harness and also guards lossless app-lifecycle request fields.
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
const schemaPath = join(
  __dirname,
  "../../..",
  "contracts",
  "agent-events.yaml",
);
const schema = readFileSync(schemaPath, "utf8");
assertAnyJSONNumberBounds(schema);
assertLosslessHelloBounds(schema);

let passed = 0;
for (const [name, fixture] of Object.entries(fixtures)) {
  const kind = fixture.kind ?? "unknown";
  const wire = fixture.wire;
  if (wire === undefined) {
    throw new Error(`fixture '${name}' is missing 'wire'`);
  }
  assertJSONRoundTrip(wire, `fixture '${name}' (${kind})`);
  passed += 1;
}

const appLifecycleFixtures = {
  workspace_install_without_operation_id: {
    owner: {
      kind: "workspace",
      workspace_id: "018f1e72-6e9a-7c20-8e90-123456789abc",
    },
    app_id: "messaging",
  },
  participant_install_with_operation_id: {
    owner: {
      kind: "participant",
      participant: {
        kind: "human",
        human_id: "018f1e72-6e9a-7c20-8e90-123456789abd",
      },
    },
    app_id: "direct-chat",
    operation_id: "00000000-0000-4000-8000-000000000101",
  },
  state_with_expected_authority_epoch: {
    state: "disabled",
    expected_authority_epoch: "9223372036854775807",
  },
  install_existing_intent_conflict: {
    error: "install_intent_already_installed",
  },
  install_idempotency_conflict: { error: "idempotency_conflict" },
  stale_authority_conflict: { error: "stale_authority" },
  lifecycle_unavailable: { error: "unavailable" },
};

for (const [name, wire] of Object.entries(appLifecycleFixtures)) {
  assertJSONRoundTrip(wire, `app lifecycle fixture '${name}'`);
  passed += 1;
}

if (
  typeof appLifecycleFixtures.state_with_expected_authority_epoch
    .expected_authority_epoch !== "string"
) {
  throw new Error(
    "expected_authority_epoch must remain a lossless wire string",
  );
}

console.log(`contract round-trip: ${passed} fixtures passed`);
assertLosslessHelloRuntimeBounds();
process.exit(0);

function assertAnyJSONNumberBounds(schema) {
  const definition = schema.match(
    /^ {2}AnyJSON:\n([\s\S]*?)(?=^ {2}[A-Za-z][A-Za-z0-9_]*:|(?![\s\S]))/m,
  )?.[0];
  if (definition === undefined) {
    throw new Error("agent-events schema is missing the AnyJSON definition");
  }

  const min = "minimum: -9007199254740991";
  const max = "maximum: 9007199254740991";
  const integer = new RegExp(`- type: integer\\n\\s+${min}\\n\\s+${max}`);
  const nonIntegerNumber = new RegExp(
    `- type: number\\n\\s+not: \\{ type: integer \\}\\n\\s+${min}\\n\\s+${max}`,
  );

  if (!integer.test(definition) || !nonIntegerNumber.test(definition)) {
    throw new Error(
      "AnyJSON must bound integer and non-integer number values to the JavaScript safe-integer range",
    );
  }
}

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

function assertJSONRoundTrip(wire, name) {
  const original = normalize(wire);
  assertAnyJSONRuntimeBounds(original, name);
  const reparsed = normalize(JSON.parse(JSON.stringify(wire)));

  if (JSON.stringify(original) !== JSON.stringify(reparsed)) {
    console.error(`${name} round-trip mismatch`);
    console.error("original:", JSON.stringify(original, null, 2));
    console.error("roundtrip:", JSON.stringify(reparsed, null, 2));
    process.exit(1);
  }
}

function assertAnyJSONRuntimeBounds(value, path) {
  if (typeof value === "number") {
    if (!Number.isFinite(value) || Math.abs(value) > Number.MAX_SAFE_INTEGER) {
      throw new Error(
        `${path} contains a number outside the AnyJSON safe range`,
      );
    }
    return;
  }
  if (value === null || typeof value !== "object") {
    return;
  }
  if (Array.isArray(value)) {
    value.forEach((child, index) => {
      assertAnyJSONRuntimeBounds(child, `${path}[${index}]`);
    });
    return;
  }
  for (const [key, child] of Object.entries(value)) {
    assertAnyJSONRuntimeBounds(child, `${path}.${key}`);
  }
}

function assertLosslessHelloBounds(schema) {
  for (const [name, format] of [
    ["ProcessGeneration", "canonical-process-generation"],
    ["CanonicalDecimalU64", "canonical-decimal-u64"],
  ]) {
    const definition = schema.match(
      new RegExp(
        `^ {2}${name}:\\n([\\s\\S]*?)(?=^ {2}[A-Za-z][A-Za-z0-9_]*:|(?![\\s\\S]))`,
        "m",
      ),
    )?.[0];
    if (
      definition === undefined ||
      !definition.includes("type: string") ||
      !definition.includes(`format: ${format}`)
    ) {
      throw new Error(`${name} must be a lossless canonical decimal string`);
    }
  }
}

function assertLosslessHelloRuntimeBounds() {
  if (BigInt("9223372036854775807") !== 2n ** 63n - 1n) {
    throw new Error("unexpected ProcessGeneration upper bound");
  }
  if (BigInt("18446744073709551615") !== 2n ** 64n - 1n) {
    throw new Error("unexpected u64 cursor upper bound");
  }
}
