import { execSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

function interfaceHasIndexSignature(text, name) {
  const match = text.match(
    new RegExp(`export interface ${name} \\{([\\s\\S]*?)\\n\\}`, "m"),
  );
  if (!match) return true;
  return /\[k: string\]: unknown/.test(match[1]);
}

const __dirname = dirname(fileURLToPath(import.meta.url));
const repoRoot = join(__dirname, "..", "..", "..");
const generatedDir = join(__dirname, "..", "src", "generated");

const tmp = mkdtempSync(join(tmpdir(), "sumi-drift-"));

try {
  const expected = {
    "agent-events.d.ts": join(generatedDir, "agent-events.d.ts"),
    "schema.d.ts": join(generatedDir, "schema.d.ts"),
  };

  execSync(
    `npx openapi-typescript ${join(repoRoot, "contracts", "openapi.yaml")} -o ${join(tmp, "schema.d.ts")}`,
    { stdio: "inherit", cwd: join(__dirname, "..") },
  );
  execSync(
    `npx json2ts --unreachableDefinitions ${join(repoRoot, "contracts", "agent-events.yaml")} -o ${join(tmp, "agent-events.d.ts")}`,
    { stdio: "inherit", cwd: join(__dirname, "..") },
  );

  let failed = false;
  for (const [name, expectedPath] of Object.entries(expected)) {
    const generated = readFileSync(join(tmp, name), "utf8");
    const committed = readFileSync(expectedPath, "utf8");
    if (hash(generated) !== hash(committed)) {
      failed = true;
      console.error(
        `Generated ${name} differs from committed file. Run 'pnpm --filter @sumi/api-client generate' and commit the result.`,
      );
    }
  }

  const agentEvents = readFileSync(join(tmp, "agent-events.d.ts"), "utf8");
  const envelopeMatch = agentEvents.match(/export type Envelope = ([^;]+);/s);
  if (!envelopeMatch || envelopeMatch[1].includes("[k: string]")) {
    failed = true;
    console.error(
      "Generated Envelope type is permissive (allows extra properties). The schema must keep durable and volatile envelopes strict.",
    );
  }
  for (const name of ["DurableEnvelope", "VolatileEnvelope"]) {
    if (interfaceHasIndexSignature(agentEvents, name)) {
      failed = true;
      console.error(
        `Generated ${name} allows extra properties via an index signature. additionalProperties: false must be preserved.`,
      );
    }
  }

  if (failed) {
    process.exit(1);
  }

  console.log("Generated type files match the canonical YAML contracts.");
} finally {
  rmSync(tmp, { recursive: true, force: true });
}

function hash(s) {
  return createHash("sha256").update(s).digest("hex");
}
