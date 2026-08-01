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
      conversation_id: "conversation-1",
      event: { type: "error", message: "x" },
      seq: 1,
    },
  },
  {
    name: "hello rejects noncanonical decimal",
    def: "AgentHello",
    value: {
      agent_id: "agent-1",
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
      accepted_generation: "7",
      last_received_event_seq: "18446744073709551616",
      next_command_seq: "1",
    },
  },
  {
    name: "hello rejects overflowing generation",
    def: "ApiHello",
    value: {
      accepted_generation: "9223372036854775808",
      last_received_event_seq: "0",
      next_command_seq: "1",
    },
  },
  {
    name: "durable envelope missing seq",
    def: "Envelope",
    value: {
      conversation_id: "conversation-1",
      event: { type: "agent_start" },
    },
  },
  {
    name: "envelope with extra property",
    def: "Envelope",
    value: {
      conversation_id: "conversation-1",
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
];

for (const { name, def, value } of counterexamples) {
  const validate = getValidator(def);
  if (validate(value)) {
    console.error(`Counterexample ${name} was incorrectly accepted by ${def}`);
    failed = true;
  }
}

const declaredErrorCodes = new Set(openApi.components.schemas.ErrorCode.enum);
for (const [name, response] of Object.entries(openApi.components.responses)) {
  const code = response?.content?.["application/json"]?.example?.error?.code;
  if (code && !declaredErrorCodes.has(code)) {
    console.error(
      `OpenAPI response ${name} uses undeclared ErrorCode '${code}'`,
    );
    failed = true;
  }
}

if (failed) {
  process.exit(1);
}

console.log(
  "All contract fixtures, bounded-decimal cases, Todo error codes, and extra-property counterexamples passed schema validation.",
);
