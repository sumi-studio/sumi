import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { parse } from "@apidevtools/json-schema-ref-parser";
import Ajv2020 from "@redocly/ajv/dist/2020.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const repoRoot = join(__dirname, "..", "..", "..");
const contractPath = join(repoRoot, "contracts", "agent-events.yaml");
const openApiPath = join(repoRoot, "contracts", "openapi.yaml");
const fixturesPath = join(repoRoot, "contracts", "agent-events-fixtures.json");

const [schema, openApi] = await Promise.all([
  parse(contractPath),
  parse(openApiPath),
]);

const ajv = new Ajv2020({ strict: false, allErrors: true, logger: false });
ajv.addFormat(
  "uuid",
  /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/,
);
ajv.addFormat("date-time", (s) => {
  return /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/.test(
    s,
  );
});
ajv.addFormat("canonical-decimal-u64", (s) => {
  return (
    typeof s === "string" &&
    /^(0|[1-9][0-9]*)$/.test(s) &&
    BigInt(s) <= 18446744073709551615n
  );
});
ajv.addFormat("canonical-process-generation", (s) => {
  return (
    typeof s === "string" &&
    /^(0|[1-9][0-9]*)$/.test(s) &&
    BigInt(s) <= 9223372036854775807n
  );
});

const kindToDef = {
  outbound_frame: "OutboundFrame",
  command_envelope: "CommandEnvelope",
  agent_hello: "AgentHello",
  api_hello: "ApiHello",
  agent_event: "AgentEvent",
  public_message: "PublicMessage",
  browser_hello: "BrowserHello",
  browser_command_frame: "BrowserCommandFrame",
  browser_event_frame: "BrowserEventFrame",
  browser_command_accepted: "BrowserCommandAcceptedFrame",
  browser_command_rejected: "BrowserCommandRejectedFrame",
  browser_direct_chat_status: "DirectChatStatusFrame",
};

const validators = new Map();
function getValidator(def) {
  if (!validators.has(def)) {
    const validate = ajv.compile({
      $schema: schema.$schema,
      $ref: `#/$defs/${def}`,
      $defs: schema.$defs,
    });
    validators.set(def, validate);
  }
  return validators.get(def);
}

function describeErrors(errors) {
  return errors.map((e) => `${e.instancePath || "/"}: ${e.message}`).join("; ");
}

const fixtures = JSON.parse(readFileSync(fixturesPath, "utf8"));
let failed = false;

for (const route of ["/direct-chat/commands", "/direct-chat/ws"]) {
  if (openApi.paths?.[route] === undefined) {
    console.error(`OpenAPI is missing required direct-chat route ${route}`);
    failed = true;
  }
}
for (const legacyRoute of [
  "/conversations/{conversation_id}/commands",
  "/conversations/{conversation_id}/ws",
]) {
  if (openApi.paths?.[legacyRoute] !== undefined) {
    console.error(`OpenAPI still exposes legacy route ${legacyRoute}`);
    failed = true;
  }
}

const directChatPost = openApi.paths?.["/direct-chat/commands"]?.post;
const idempotencyParameter = directChatPost?.parameters?.find(
  (parameter) =>
    parameter.in === "header" && parameter.name === "Idempotency-Key",
);
if (
  !idempotencyParameter?.required ||
  idempotencyParameter.schema?.minLength !== 1 ||
  idempotencyParameter.schema?.maxLength !== 1024 ||
  directChatPost?.responses?.["403"] === undefined ||
  directChatPost?.responses?.["409"] === undefined
) {
  console.error(
    "Direct-chat HTTP admission must require Idempotency-Key (1..1024) and expose Origin rejection (403) and idempotency conflict (409).",
  );
  failed = true;
}

const directChatRequest =
  openApi.components?.schemas?.DirectChatUserMessageCommand;
if (
  directChatRequest?.type !== "object" ||
  directChatRequest?.additionalProperties !== false ||
  directChatRequest?.properties?.type?.const !== "user_message" ||
  directChatRequest?.properties?.text?.type !== "string" ||
  directChatRequest?.properties?.attachments?.maxItems !== 0
) {
  console.error(
    "Direct-chat HTTP admission must preserve the strict structured user-message command body.",
  );
  failed = true;
}

const boundedDecimalCases = [
  {
    name: "ProcessGeneration",
    valid: [
      ["zero", "0"],
      ["exact maximum", "9223372036854775807"],
    ],
    invalid: [
      ["maximum plus one", "9223372036854775808"],
      ["same-length near overflow", "9223372036854775900"],
      ["same-length high overflow", "9999999999999999999"],
      ["leading zero", "01"],
      ["zero with a leading zero", "00"],
      ["negative", "-1"],
      ["negative zero", "-0"],
    ],
  },
  {
    name: "CanonicalDecimalU64",
    valid: [
      ["zero", "0"],
      ["exact maximum", "18446744073709551615"],
    ],
    invalid: [
      ["maximum plus one", "18446744073709551616"],
      ["same-length near overflow", "18446744073709551999"],
      ["same-length high overflow", "99999999999999999999"],
      ["leading zero", "01"],
      ["zero with a leading zero", "00"],
      ["negative", "-1"],
      ["negative zero", "-0"],
    ],
  },
];

const schemaOnlyAjv = new Ajv2020({
  strict: false,
  allErrors: true,
  logger: false,
  validateFormats: false,
});
schemaOnlyAjv.addSchema(schema, "agent-events.yaml");

function getOpenApiSchemaValidator(definition) {
  return schemaOnlyAjv.compile({
    $schema: "https://json-schema.org/draft/2020-12/schema",
    $ref: `#/components/schemas/${definition}`,
    components: openApi.components,
  });
}

const workspaceId = "018f1e72-6e9a-7c20-8e90-123456789abc";
const humanId = "018f1e72-6e9a-7c20-8e90-123456789abd";
const operationId = "00000000-0000-4000-8000-000000000101";

const appLifecycleRequestCases = [
  {
    name: "app install request",
    definition: "AppInstallRequest",
    valid: [
      {
        owner: { kind: "workspace", workspace_id: workspaceId },
        app_id: "messaging",
      },
      {
        owner: { kind: "workspace", workspace_id: workspaceId },
        app_id: "messaging",
        operation_id: operationId,
      },
      {
        owner: {
          kind: "participant",
          participant: { kind: "human", human_id: humanId },
        },
        app_id: "direct-chat",
        operation_id: operationId,
      },
    ],
    invalid: [
      {
        name: "Participant operation identity is omitted",
        value: {
          owner: {
            kind: "participant",
            participant: { kind: "human", human_id: humanId },
          },
          app_id: "direct-chat",
        },
      },
      {
        name: "Participant operation identity is null",
        value: {
          owner: {
            kind: "participant",
            participant: { kind: "human", human_id: humanId },
          },
          app_id: "direct-chat",
          operation_id: null,
        },
      },
      {
        name: "Participant operation identity is empty",
        value: {
          owner: {
            kind: "participant",
            participant: { kind: "human", human_id: humanId },
          },
          app_id: "direct-chat",
          operation_id: "",
        },
      },
      {
        name: "Participant operation identity is UUIDv7",
        value: {
          owner: {
            kind: "participant",
            participant: { kind: "human", human_id: humanId },
          },
          app_id: "direct-chat",
          operation_id: workspaceId,
        },
      },
      {
        name: "Participant operation identity is noncanonical uppercase",
        value: {
          owner: {
            kind: "participant",
            participant: { kind: "human", human_id: humanId },
          },
          app_id: "direct-chat",
          operation_id: "00000000-0000-4000-8000-00000000010A",
        },
      },
      {
        name: "Workspace operation identity is invalid when present",
        value: {
          owner: { kind: "workspace", workspace_id: workspaceId },
          app_id: "messaging",
          operation_id: "",
        },
      },
      {
        name: "Owner discriminants cannot be combined",
        value: {
          owner: {
            kind: "workspace",
            workspace_id: workspaceId,
            participant: { kind: "human", human_id: humanId },
          },
          app_id: "messaging",
        },
      },
      {
        name: "Unknown body properties are rejected",
        value: {
          owner: { kind: "workspace", workspace_id: workspaceId },
          app_id: "messaging",
          operation: operationId,
        },
      },
    ],
  },
  {
    name: "app installation state request",
    definition: "AppInstallationStateRequest",
    valid: [
      { state: "disabled" },
      { state: "enabled", expected_authority_epoch: "1" },
      {
        state: "disabled",
        expected_authority_epoch: "9223372036854775807",
      },
    ],
    invalid: [
      {
        name: "Expected authority epoch is null",
        value: { state: "disabled", expected_authority_epoch: null },
      },
      {
        name: "Expected authority epoch is empty",
        value: { state: "disabled", expected_authority_epoch: "" },
      },
      {
        name: "Expected authority epoch is zero",
        value: { state: "disabled", expected_authority_epoch: "0" },
      },
      {
        name: "Expected authority epoch has a leading zero",
        value: { state: "disabled", expected_authority_epoch: "01" },
      },
      {
        name: "Expected authority epoch overflows signed 64-bit",
        value: {
          state: "disabled",
          expected_authority_epoch: "9223372036854775808",
        },
      },
      {
        name: "Unknown state properties are rejected",
        value: { state: "disabled", authority_epoch: "1" },
      },
    ],
  },
];

for (const { name, definition, valid, invalid } of appLifecycleRequestCases) {
  const validate = getOpenApiSchemaValidator(definition);
  for (const value of valid) {
    if (!validate(value)) {
      console.error(
        `${name} rejected valid wire value ${JSON.stringify(value)}: ${describeErrors(validate.errors)}`,
      );
      failed = true;
    }
  }
  for (const { name: caseName, value } of invalid) {
    if (validate(value)) {
      console.error(
        `${name} accepted invalid ${caseName}: ${JSON.stringify(value)}`,
      );
      failed = true;
    }
  }
}

const workspaceInviteCommon = {
  invite_id: "018f1e72-6e9a-7c20-8e90-123456789abe",
  workspace_id: workspaceId,
  expires_at: "2026-08-11T04:05:06.789Z",
  created_at: "2026-08-10T04:05:06.789Z",
};
const workspaceInviteSchemaCases = [
  {
    name: "current-agent Workspace invite request",
    definition: "WorkspaceCurrentAgentInviteRequest",
    valid: [{}],
    invalid: [
      null,
      [],
      { personality_agent_id: humanId },
      { target_id: humanId },
      { workspace_id: workspaceId },
    ],
  },
  {
    name: "Workspace invite strict sum",
    definition: "WorkspaceInviteRecord",
    valid: [
      { ...workspaceInviteCommon, kind: "share_code" },
      {
        ...workspaceInviteCommon,
        kind: "targeted_personality_agent",
      },
    ],
    invalid: [
      { ...workspaceInviteCommon },
      { ...workspaceInviteCommon, kind: "current_agent" },
      {
        ...workspaceInviteCommon,
        kind: "share_code",
        code: "one-time-secret",
      },
      {
        ...workspaceInviteCommon,
        kind: "share_code",
        target_id: humanId,
      },
      {
        ...workspaceInviteCommon,
        kind: "targeted_personality_agent",
        personality_agent_id: humanId,
      },
      {
        ...workspaceInviteCommon,
        kind: "targeted_personality_agent",
        target_kind: "personality_agent",
      },
      {
        ...workspaceInviteCommon,
        kind: "targeted_personality_agent",
        code_hash: "not-public",
      },
    ],
  },
];

for (const { name, definition, valid, invalid } of workspaceInviteSchemaCases) {
  const validate = getOpenApiSchemaValidator(definition);
  for (const value of valid) {
    if (!validate(value)) {
      console.error(
        `${name} rejected valid wire value ${JSON.stringify(value)}: ${describeErrors(validate.errors)}`,
      );
      failed = true;
    }
  }
  for (const value of invalid) {
    if (validate(value)) {
      console.error(
        `${name} accepted privacy/strict-sum counterexample ${JSON.stringify(value)}`,
      );
      failed = true;
    }
  }
}

const lifecycleErrorSchemaCases = [
  {
    name: "app install conflict response",
    definition: "AppInstallConflictError",
    valid: [
      { error: "conflict" },
      { error: "install_intent_already_installed" },
      { error: "idempotency_conflict" },
    ],
    invalid: [
      { error: "stale_authority" },
      { error: "unavailable" },
      { error: "idempotency_conflict", detail: "different app" },
      {},
    ],
  },
  {
    name: "stale app authority response",
    definition: "StaleAppAuthorityError",
    valid: [{ error: "stale_authority" }],
    invalid: [
      { error: "conflict" },
      { error: "unavailable" },
      { error: "stale_authority", authority_epoch: "2" },
      {},
    ],
  },
  {
    name: "app lifecycle unavailable response",
    definition: "AppLifecycleUnavailableError",
    valid: [{ error: "unavailable" }],
    invalid: [
      { error: "stale_authority" },
      { error: "internal_error" },
      { error: "unavailable", retry_after: 1 },
      {},
    ],
  },
];

for (const { name, definition, valid, invalid } of lifecycleErrorSchemaCases) {
  const validate = getOpenApiSchemaValidator(definition);
  for (const value of valid) {
    if (!validate(value)) {
      console.error(
        `${name} rejected valid wire value ${JSON.stringify(value)}: ${describeErrors(validate.errors)}`,
      );
      failed = true;
    }
  }
  for (const value of invalid) {
    if (validate(value)) {
      console.error(
        `${name} accepted invalid wire value: ${JSON.stringify(value)}`,
      );
      failed = true;
    }
  }
}

for (const code of [
  "install_intent_already_installed",
  "idempotency_conflict",
  "stale_authority",
  "unavailable",
]) {
  if (
    !openApi.components?.schemas?.APIError?.properties?.error?.enum?.includes(
      code,
    )
  ) {
    console.error(`Shared APIError omits runtime lifecycle code ${code}`);
    failed = true;
  }
}

for (const { path, method, status, response } of [
  {
    path: "/app-installations",
    method: "post",
    status: "409",
    response: "AppInstallConflict",
  },
  {
    path: "/app-installations",
    method: "post",
    status: "503",
    response: "AppLifecycleUnavailable",
  },
  {
    path: "/app-installations/{installation_id}/state",
    method: "put",
    status: "409",
    response: "StaleAppAuthority",
  },
  {
    path: "/app-installations/{installation_id}/state",
    method: "put",
    status: "503",
    response: "AppLifecycleUnavailable",
  },
  {
    path: "/app-installations/{installation_id}",
    method: "delete",
    status: "503",
    response: "AppLifecycleUnavailable",
  },
]) {
  const actual = openApi.paths?.[path]?.[method]?.responses?.[status]?.$ref;
  const expected = `#/components/responses/${response}`;
  if (actual !== expected) {
    console.error(
      `${method.toUpperCase()} ${path} ${status} must reference ${response}; got ${actual ?? "nothing"}`,
    );
    failed = true;
  }
}

const httpRejectionCases = [
  {
    name: "HTTP pre-sequence rejection",
    definition: "DirectChatCommandRejectedResponse",
    valid: [
      { error: "invalid_command", reject_reason: "schema_violation" },
      {
        error: "invalid_command",
        idempotency_key: "valid-key",
        reject_reason: "oversized",
      },
    ],
    invalid: [
      {
        error: "invalid_command",
        idempotency_key: "",
        reject_reason: "schema_violation",
      },
      { error: "invalid_command", reject_reason: "idempotency_conflict" },
    ],
  },
  {
    name: "HTTP idempotency conflict",
    definition: "DirectChatCommandIdempotencyConflictResponse",
    valid: [
      {
        error: "idempotency_conflict",
        idempotency_key: "valid-key",
        reject_reason: "idempotency_conflict",
      },
    ],
    invalid: [
      { error: "idempotency_conflict", reject_reason: "idempotency_conflict" },
      {
        error: "idempotency_conflict",
        idempotency_key: "",
        reject_reason: "idempotency_conflict",
      },
    ],
  },
];

for (const { name, definition, valid, invalid } of httpRejectionCases) {
  const validate = schemaOnlyAjv.compile(
    openApi.components.schemas[definition],
  );
  for (const value of valid) {
    if (!validate(value)) {
      console.error(
        `${name} rejected valid response: ${describeErrors(validate.errors)}`,
      );
      failed = true;
    }
  }
  for (const value of invalid) {
    if (validate(value)) {
      console.error(
        `${name} accepted invalid response: ${JSON.stringify(value)}`,
      );
      failed = true;
    }
  }
}

for (const { name, valid, invalid } of boundedDecimalCases) {
  const agentEventsDefinition = schema.$defs[name];
  const openApiDefinition = openApi.components.schemas[name];

  if (
    JSON.stringify(agentEventsDefinition) !== JSON.stringify(openApiDefinition)
  ) {
    console.error(`${name} differs between agent-events.yaml and openapi.yaml`);
    failed = true;
  }

  for (const [source, definition] of [
    ["agent-events.yaml", agentEventsDefinition],
    ["openapi.yaml", openApiDefinition],
  ]) {
    const validate = schemaOnlyAjv.compile(definition);
    for (const [caseName, value] of valid) {
      if (!validate(value)) {
        console.error(
          `${source} ${name} rejected ${caseName} '${value}' with formats ignored: ${describeErrors(validate.errors)}`,
        );
        failed = true;
      }
    }
    for (const [caseName, value] of invalid) {
      if (validate(value)) {
        console.error(
          `${source} ${name} accepted ${caseName} '${value}' with formats ignored`,
        );
        failed = true;
      }
    }
  }
}

for (const [name, fixture] of Object.entries(fixtures)) {
  const def = kindToDef[fixture.kind];
  if (!def) {
    console.error(`Fixture ${name} has unknown kind ${fixture.kind}`);
    failed = true;
    continue;
  }
  const validate = getValidator(def);
  if (!validate(fixture.wire)) {
    console.error(
      `Fixture ${name} (${fixture.kind}) failed schema validation: ${describeErrors(validate.errors)}`,
    );
    failed = true;
  }
}

const counterexamples = [
  {
    name: "applied command disposition rejects reject_reason",
    def: "CommandDispositionEvent",
    value: {
      type: "command_disposition",
      command_id: "00000000-0000-4000-8000-000000000001",
      command_seq: 1,
      status: "applied",
      reject_reason: "schema_violation",
    },
  },
  {
    name: "rejected command disposition requires reject_reason",
    def: "CommandDispositionEvent",
    value: {
      type: "command_disposition",
      command_id: "00000000-0000-4000-8000-000000000001",
      command_seq: 1,
      status: "rejected",
    },
  },
  {
    name: "superseded command disposition rejects reject_reason",
    def: "CommandDispositionEvent",
    value: {
      type: "command_disposition",
      command_id: "00000000-0000-4000-8000-000000000001",
      command_seq: 1,
      status: "superseded",
      reject_reason: "schema_violation",
    },
  },
  {
    name: "command disposition requires a lower-case UUID",
    def: "CommandDispositionEvent",
    value: {
      type: "command_disposition",
      command_id: "00000000-0000-4000-8000-00000000000A",
      command_seq: 1,
      status: "applied",
    },
  },
  {
    name: "command disposition rejects non-JSON-safe command sequence",
    def: "CommandDispositionEvent",
    value: {
      type: "command_disposition",
      command_id: "00000000-0000-4000-8000-000000000001",
      command_seq: 9007199254740992,
      status: "applied",
    },
  },
  {
    name: "volatile envelope with disallowed seq",
    def: "Envelope",
    value: {
      personality_agent_id: "018f1e72-6e9a-7c20-8e90-123456789abc",
      event: { type: "error", message: "x" },
      seq: 1,
    },
  },
  {
    name: "volatile envelope requires personality agent ID",
    def: "Envelope",
    value: { event: { type: "error", message: "x" } },
  },
  {
    name: "hello rejects noncanonical decimal",
    def: "AgentHello",
    value: {
      personality_agent_id: "018f1e72-6e9a-7c20-8e90-123456789abc",
      generation: "07",
      last_sent_event_seq: "0",
      last_received_command_seq: "0",
      last_applied_command_seq: "0",
    },
  },
  {
    name: "hello rejects overflowing cursor",
    def: "ApiHello",
    value: {
      personality_agent_id: "018f1e72-6e9a-7c20-8e90-123456789abc",
      accepted_generation: "7",
      last_received_event_seq: "18446744073709551616",
      next_command_seq: "1",
    },
  },
  {
    name: "hello rejects overflowing generation",
    def: "ApiHello",
    value: {
      personality_agent_id: "018f1e72-6e9a-7c20-8e90-123456789abc",
      accepted_generation: "9223372036854775808",
      last_received_event_seq: "0",
      next_command_seq: "1",
    },
  },
  {
    name: "durable envelope missing seq",
    def: "Envelope",
    value: {
      personality_agent_id: "018f1e72-6e9a-7c20-8e90-123456789abc",
      event: { type: "agent_start" },
    },
  },
  {
    name: "envelope with extra property",
    def: "Envelope",
    value: {
      personality_agent_id: "018f1e72-6e9a-7c20-8e90-123456789abc",
      event: { type: "agent_start" },
      seq: 1,
      extra: "bad",
    },
  },
  {
    name: "event with extra property",
    def: "AgentEvent",
    value: { type: "agent_start", extra: "bad" },
  },
  ...[
    "018f1e72-6e9a-1c20-8e90-123456789abc",
    "018f1e72-6e9a-4c20-8e90-123456789abc",
    "018f1e72-6e9a-6c20-8e90-123456789abc",
    "018f1e72-6e9a-7c20-7e90-123456789abc",
    "018F1E72-6E9A-7C20-8E90-123456789ABC",
    "{018f1e72-6e9a-7c20-8e90-123456789abc}",
    "urn:uuid:018f1e72-6e9a-7c20-8e90-123456789abc",
    "018f1e726e9a7c208e90123456789abc",
    " 018f1e72-6e9a-7c20-8e90-123456789abc ",
  ].map((value) => ({
    name: `personality agent ID rejects '${value}'`,
    def: "PersonalityAgentId",
    value,
  })),
  {
    name: "provenance rejects a missing actor",
    def: "DirectChatProvenanceV1",
    value: {
      version: 1,
      tenant_id: "tenant-1",
      personality_agent_id: "018f1e72-6e9a-7c20-8e90-123456789abc",
      source: { surface: "direct_chat" },
    },
  },
  {
    name: "provenance rejects an unknown field",
    def: "DirectChatProvenanceV1",
    value: {
      version: 1,
      tenant_id: "tenant-1",
      personality_agent_id: "018f1e72-6e9a-7c20-8e90-123456789abc",
      actor: { kind: "human", principal_id: "alice" },
      source: { surface: "direct_chat" },
      extra: true,
    },
  },
  {
    name: "command envelope rejects legacy agent_id",
    def: "CommandEnvelope",
    value: {
      seq: 1,
      command_id: "00000000-0000-4000-8000-000000000001",
      agent_id: "agent-1",
      command: { type: "abort" },
    },
  },
  {
    name: "command ack rejects missing personality agent ID",
    def: "CommandAck",
    value: {
      seq: 1,
      command_id: "00000000-0000-4000-8000-000000000001",
      status: "received",
    },
  },
  {
    name: "command ack rejects malformed personality agent ID",
    def: "CommandAck",
    value: {
      seq: 1,
      command_id: "00000000-0000-4000-8000-000000000001",
      personality_agent_id: "018f1e72-6e9a-7c20-7e90-123456789abc",
      status: "received",
    },
  },
  {
    name: "API hello rejects missing personality agent ID",
    def: "ApiHello",
    value: {
      accepted_generation: "7",
      last_received_event_seq: "0",
      next_command_seq: "1",
    },
  },
  {
    name: "browser command rejects an empty idempotency key",
    def: "BrowserCommandFrame",
    value: { type: "command", idempotency_key: "", command: { type: "abort" } },
  },
  {
    name: "browser command rejects a content-only payload",
    def: "BrowserCommandFrame",
    value: {
      type: "command",
      idempotency_key: "key-1",
      content: "hello",
    },
  },
  {
    name: "browser event rejects an internal target",
    def: "BrowserEventFrame",
    value: {
      type: "event",
      envelope: {
        seq: 1,
        personality_agent_id: "018f1e72-6e9a-7c20-8e90-123456789abc",
        event: { type: "agent_start" },
      },
    },
  },
  {
    name: "browser acceptance rejects an internal command envelope",
    def: "BrowserCommandAcceptedFrame",
    value: {
      type: "command_accepted",
      envelope: { seq: 1, command_id: "00000000-0000-4000-8000-000000000001" },
    },
  },
  {
    name: "browser acceptance rejects a nonterminal disposition",
    def: "BrowserCommandAcceptedFrame",
    value: {
      type: "command_accepted",
      idempotency_key: "key-1",
      command_id: "00000000-0000-4000-8000-000000000001",
      seq: 1,
      disposition: {
        type: "command_disposition",
        command_id: "00000000-0000-4000-8000-000000000001",
        command_seq: 1,
        status: "received",
      },
    },
  },
  {
    name: "browser acceptance rejects rejected disposition without reason",
    def: "BrowserCommandAcceptedFrame",
    value: {
      type: "command_accepted",
      idempotency_key: "key-1",
      command_id: "00000000-0000-4000-8000-000000000001",
      seq: 1,
      disposition: {
        type: "command_disposition",
        command_id: "00000000-0000-4000-8000-000000000001",
        command_seq: 1,
        status: "rejected",
      },
    },
  },
  {
    name: "browser rejection requires its idempotency key",
    def: "BrowserCommandRejectedFrame",
    value: { type: "command_rejected", reject_reason: "idempotency_conflict" },
  },
];

for (const { name, def, value } of counterexamples) {
  const validate = getValidator(def);
  if (validate(value)) {
    console.error(`Counterexample ${name} was incorrectly accepted by ${def}`);
    failed = true;
  }
}

if (failed) {
  process.exit(1);
}

console.log(
  "All contract fixtures, app-lifecycle and Workspace-invite schemas, bounded-decimal cases, and extra-property counterexamples passed schema validation.",
);
