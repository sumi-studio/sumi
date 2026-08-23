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
  const schemaTypes = readFileSync(join(tmp, "schema.d.ts"), "utf8");
  const workspaceHTTP = readFileSync(
    join(repoRoot, "apps", "api", "internal", "workspace", "http.go"),
    "utf8",
  );
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

  const lifecycleTypeAssertions = [
    {
      component: "WorkspaceAppInstallRequest",
      pattern: /operation_id: components\["schemas"\]\["UUIDv4"\];/,
      message:
        "Workspace install operation_id must remain a required canonical UUIDv4 in generated types.",
    },
    {
      component: "ParticipantAppInstallRequest",
      pattern: /operation_id: components\["schemas"\]\["UUIDv4"\];/,
      message:
        "Participant install operation_id must remain a required canonical UUIDv4 in generated types.",
    },
    {
      component: "AppInstallRequest",
      pattern: /WorkspaceAppInstallRequest[^\n]+ParticipantAppInstallRequest/,
      message:
        "Generated install request must remain an owner-discriminated Workspace/Participant union.",
    },
    {
      component: "AppInstallationStateRequest",
      pattern:
        /expected_authority_epoch\?: components\["schemas"\]\["AppInstallationAuthorityEpoch"\];/,
      message:
        "State mutation expected_authority_epoch must remain optional and lossless in generated types.",
    },
    {
      component: "APIError",
      pattern:
        /install_intent_already_installed[^\n]+idempotency_conflict[^\n]+stale_authority[^\n]+unavailable/,
      message:
        "Generated shared APIError must include every runtime app-lifecycle code.",
    },
    {
      component: "AppInstallConflictError",
      pattern:
        /error: "conflict" \| "install_intent_already_installed" \| "idempotency_conflict";/,
      message:
        "Generated install 409 response must expose only the exact install conflict taxonomy.",
    },
    {
      component: "StaleAppAuthorityError",
      pattern: /error: "stale_authority";/,
      message:
        "Generated state 409 response must expose the exact stale_authority code.",
    },
    {
      component: "AppLifecycleUnavailableError",
      pattern: /error: "unavailable";/,
      message:
        "Generated lifecycle 503 response must expose the exact unavailable code.",
    },
    {
      component: "WorkspaceCurrentAgentInviteRequest",
      pattern: /Record<string, never>/,
      message:
        "Current-agent Workspace invitation issuance must remain an exact empty-object request.",
    },
    {
      component: "WorkspaceInviteRecord",
      pattern:
        /WorkspaceShareCodeInviteRecord[^\n]+WorkspaceTargetedPersonalityAgentInviteRecord/,
      message:
        "Workspace invite records must remain the explicit share-code/targeted-PA union.",
    },
    {
      component: "WorkspaceShareCodeInviteRecord",
      pattern: /kind: "share_code";/,
      message:
        "Share-code Workspace invite records must keep their exact wire discriminator.",
    },
    {
      component: "WorkspaceTargetedPersonalityAgentInviteRecord",
      pattern: /kind: "targeted_personality_agent";/,
      message:
        "Targeted PA Workspace invite records must keep their durable wire discriminator.",
    },
  ];
  for (const { component, pattern, message } of lifecycleTypeAssertions) {
    const definition = generatedSchemaComponent(schemaTypes, component);
    if (definition === undefined || !pattern.test(definition)) {
      failed = true;
      console.error(message);
    }
  }

  for (const [operation, requestComponent] of [
    ["installApp", "AppInstallRequest"],
    ["setAppInstallationState", "AppInstallationStateRequest"],
  ]) {
    const definition = generatedOperation(schemaTypes, operation);
    if (
      definition === undefined ||
      !definition.includes(`components["schemas"]["${requestComponent}"]`)
    ) {
      failed = true;
      console.error(
        `Generated ${operation} operation must use ${requestComponent} as its canonical request body.`,
      );
    }
  }

  for (const { operation, status, response } of [
    {
      operation: "installApp",
      status: 409,
      response: "AppInstallConflict",
    },
    {
      operation: "installApp",
      status: 503,
      response: "AppLifecycleUnavailable",
    },
    {
      operation: "setAppInstallationState",
      status: 409,
      response: "StaleAppAuthority",
    },
    {
      operation: "setAppInstallationState",
      status: 503,
      response: "AppLifecycleUnavailable",
    },
    {
      operation: "uninstallApp",
      status: 503,
      response: "AppLifecycleUnavailable",
    },
  ]) {
    const definition = generatedOperation(schemaTypes, operation);
    const expected = `${status}: components["responses"]["${response}"];`;
    if (definition === undefined || !definition.includes(expected)) {
      failed = true;
      console.error(
        `Generated ${operation} ${status} response must use ${response}.`,
      );
    }
  }

  const writeDomainError = goFunctionBody(workspaceHTTP, "writeDomainError");
  for (const { domainError, status, code } of [
    // Only active Go domain errors belong here. The legacy
    // `ErrAlreadyInstalled` symbol no longer exists; its name is also a prefix
    // of `ErrInstallIntentAlreadyInstalled`, so retaining it produced a false
    // substring match instead of checking a real public mapping.
    {
      domainError: "applicationapps.ErrInstallIntentAlreadyInstalled",
      status: "http.StatusConflict",
      code: "install_intent_already_installed",
    },
    {
      domainError: "applicationapps.ErrInstallIntentMismatch",
      status: "http.StatusConflict",
      code: "idempotency_conflict",
    },
    {
      domainError: "applicationapps.ErrAuthorityEpochStale",
      status: "http.StatusConflict",
      code: "stale_authority",
    },
    {
      domainError: "applicationapps.ErrInstallIntentIncomplete",
      status: "http.StatusServiceUnavailable",
      code: "unavailable",
    },
    {
      domainError: "directchat.ErrLifecycleFenceUnavailable",
      status: "http.StatusServiceUnavailable",
      code: "unavailable",
    },
  ]) {
    if (!goSwitchCaseMaps(writeDomainError, domainError, status, code)) {
      failed = true;
      console.error(
        `OpenAPI lifecycle taxonomy drifted from writeDomainError mapping ${domainError} -> ${status} ${code}.`,
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

function generatedSchemaComponent(text, name) {
  return generatedBlock(text, `        ${name}:`, "        ");
}

function generatedOperation(text, name) {
  return generatedBlock(text, `    ${name}:`, "    ");
}

function generatedBlock(text, marker, indentation) {
  const start = text.indexOf(marker);
  if (start === -1) return undefined;
  const remainder = text.slice(start + marker.length);
  const nextLine = remainder.match(new RegExp(`^${indentation}\\S`, "m"));
  const end =
    nextLine?.index === undefined
      ? text.length
      : start + marker.length + nextLine.index;
  return text.slice(start, end);
}

function goFunctionBody(source, name) {
  const start = source.indexOf(`func ${name}(`);
  if (start === -1) return undefined;
  const nextFunction = source.indexOf("\nfunc ", start + 1);
  return source.slice(
    start,
    nextFunction === -1 ? source.length : nextFunction,
  );
}

function goSwitchCaseMaps(functionBody, domainError, status, code) {
  if (functionBody === undefined) return false;
  const errorMarker = `errors.Is(err, ${domainError})`;
  const errorIndex = functionBody.indexOf(errorMarker);
  if (errorIndex === -1) return false;
  const nextCase = functionBody.indexOf(
    "\n\tcase ",
    errorIndex + errorMarker.length,
  );
  const switchCase = functionBody.slice(
    errorIndex,
    nextCase === -1 ? functionBody.length : nextCase,
  );
  return switchCase.includes(`writeAPIError(w, ${status}, "${code}")`);
}
