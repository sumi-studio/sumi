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
  (parameter) => parameter.in === "header" && parameter.name === "Idempotency-Key",
);
if (
  !idempotencyParameter?.required ||
  idempotencyParameter.schema?.minLength !== 1 ||
  idempotencyParameter.schema?.maxLength !== 1024 ||
  directChatPost?.responses?.["409"] === undefined
) {
  console.error("Direct-chat HTTP admission must require Idempotency-Key (1..1024) and expose 409.");
  failed = true;
}

const directChatRequest = openApi.components?.schemas?.DirectChatUserMessageCommand;
if (
  directChatRequest?.type !== "object" ||
  directChatRequest?.additionalProperties !== false ||
  directChatRequest?.properties?.type?.const !== "user_message" ||
  directChatRequest?.properties?.text?.type !== "string" ||
  directChatRequest?.properties?.attachments?.maxItems !== 0
) {
  console.error("Direct-chat HTTP admission must preserve the strict structured user-message command body.");
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
  "All contract fixtures, bounded-decimal cases, and extra-property counterexamples passed schema validation.",
);
