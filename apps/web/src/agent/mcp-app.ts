import {
  type McpUiResourceCsp,
  McpUiResourceCspSchema,
  McpUiResourcePermissionsSchema,
  RESOURCE_MIME_TYPE,
} from "@modelcontextprotocol/ext-apps/app-bridge";
import {
  type CallToolResult,
  CallToolResultSchema,
} from "@modelcontextprotocol/sdk/types.js";

export const MAX_MCP_APP_HTML_BYTES = 512 * 1024;
export const MAX_MCP_APP_DATA_BYTES = 256 * 1024;
export const MAX_MCP_APP_MESSAGE_BYTES = 256 * 1024;
export const MAX_MCP_APP_CSP_ORIGINS = 16;

const MAX_IDENTIFIER_LENGTH = 256;
const MAX_RESOURCE_URI_LENGTH = 2_048;
const SHA256_PATTERN = /^sha256:([a-f0-9]{64})$/;
const BASE64_PATTERN =
  /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;

export interface TrustedMcpAppProjection {
  /**
   * Backend-only projection produced after resolving `_meta.ui.resourceUri`
   * and reading that `ui://` resource on the same MCP server connection.
   * Generic tool-result content must never be promoted into this shape.
   */
  kind: "trusted_mcp_app_projection";
  provenance: {
    serverId: string;
    toolName: string;
    resourceUri: string;
    resourceSha256: string;
  };
  resource: {
    uri: string;
    mimeType: typeof RESOURCE_MIME_TYPE;
    text?: string;
    blob?: string;
    csp?: McpUiResourceCsp;
    prefersBorder?: boolean;
  };
  toolInput: Record<string, unknown>;
  toolResult: CallToolResult;
}

export interface VerifiedMcpAppProjection extends TrustedMcpAppProjection {
  html: string;
}

export interface McpAppSandboxConfig {
  url: string;
  origin: string;
  deploymentId: string;
}

export function parseTrustedMcpAppProjection(
  value: unknown,
): TrustedMcpAppProjection | null {
  if (
    !isRecord(value) ||
    !hasOnlyKeys(value, [
      "kind",
      "provenance",
      "resource",
      "toolInput",
      "toolResult",
    ]) ||
    value.kind !== "trusted_mcp_app_projection" ||
    !isRecord(value.provenance) ||
    !hasOnlyKeys(value.provenance, [
      "serverId",
      "toolName",
      "resourceUri",
      "resourceSha256",
    ]) ||
    !isBoundedIdentifier(value.provenance.serverId) ||
    !isBoundedIdentifier(value.provenance.toolName) ||
    !isResourceUri(value.provenance.resourceUri) ||
    typeof value.provenance.resourceSha256 !== "string" ||
    !SHA256_PATTERN.test(value.provenance.resourceSha256) ||
    !isRecord(value.resource) ||
    !hasOnlyKeys(value.resource, [
      "uri",
      "mimeType",
      "text",
      "blob",
      "csp",
      "permissions",
      "domain",
      "prefersBorder",
    ]) ||
    !isResourceUri(value.resource.uri) ||
    value.resource.uri !== value.provenance.resourceUri ||
    value.resource.mimeType !== RESOURCE_MIME_TYPE ||
    !hasExactlyOneContentRepresentation(value.resource) ||
    !isRecord(value.toolInput) ||
    !isBoundedJson(value.toolInput, MAX_MCP_APP_DATA_BYTES) ||
    !isBoundedJson(value.toolResult, MAX_MCP_APP_DATA_BYTES)
  ) {
    return null;
  }

  const toolResult = CallToolResultSchema.safeParse(value.toolResult);
  if (!toolResult.success) {
    return null;
  }

  const csp = parseCsp(value.resource.csp);
  if (value.resource.csp !== undefined && !csp) {
    return null;
  }
  if (
    value.resource.permissions !== undefined &&
    (!isRecord(value.resource.permissions) ||
      !hasOnlyKeys(value.resource.permissions, [
        "camera",
        "microphone",
        "geolocation",
        "clipboardWrite",
      ]) ||
      !McpUiResourcePermissionsSchema.safeParse(value.resource.permissions)
        .success)
  ) {
    return null;
  }
  if (
    value.resource.domain !== undefined &&
    (typeof value.resource.domain !== "string" ||
      value.resource.domain.length > MAX_IDENTIFIER_LENGTH)
  ) {
    return null;
  }

  const resource = value.resource;
  if (
    resource.prefersBorder !== undefined &&
    typeof resource.prefersBorder !== "boolean"
  ) {
    return null;
  }

  if (resource.text !== undefined) {
    if (
      typeof resource.text !== "string" ||
      utf8Length(resource.text) > MAX_MCP_APP_HTML_BYTES
    ) {
      return null;
    }
  } else if (
    typeof resource.blob !== "string" ||
    resource.blob.length > maxBase64Length(MAX_MCP_APP_HTML_BYTES) ||
    !BASE64_PATTERN.test(resource.blob)
  ) {
    return null;
  }

  return {
    kind: "trusted_mcp_app_projection",
    provenance: {
      serverId: value.provenance.serverId,
      toolName: value.provenance.toolName,
      resourceUri: value.provenance.resourceUri,
      resourceSha256: value.provenance.resourceSha256,
    },
    resource: {
      uri: resource.uri as string,
      mimeType: RESOURCE_MIME_TYPE,
      ...(resource.text === undefined
        ? { blob: resource.blob as string }
        : { text: resource.text }),
      ...(csp ? { csp } : {}),
      ...(resource.prefersBorder === undefined
        ? {}
        : { prefersBorder: resource.prefersBorder }),
    },
    toolInput: value.toolInput,
    toolResult: toolResult.data,
  };
}

export async function verifyTrustedMcpAppProjection(
  projection: TrustedMcpAppProjection,
): Promise<VerifiedMcpAppProjection | null> {
  const htmlBytes = readHtmlBytes(projection.resource);
  if (!htmlBytes || htmlBytes.byteLength > MAX_MCP_APP_HTML_BYTES) {
    return null;
  }

  const expectedHash = SHA256_PATTERN.exec(
    projection.provenance.resourceSha256,
  )?.[1];
  if (!expectedHash || !globalThis.crypto?.subtle) {
    return null;
  }

  const digestInput = Uint8Array.from(htmlBytes);
  const digest = new Uint8Array(
    await globalThis.crypto.subtle.digest("SHA-256", digestInput.buffer),
  );
  if (!constantTimeEqual(toHex(digest), expectedHash)) {
    return null;
  }

  return {
    ...projection,
    html: new TextDecoder("utf-8", { fatal: true }).decode(htmlBytes),
  };
}

export function resolveMcpAppSandboxConfig(
  rawUrl: string | undefined,
  deploymentId: string | undefined,
  hostUrl: string,
): McpAppSandboxConfig | null {
  if (!rawUrl || !deploymentId || !/^[a-f0-9]{64}$/.test(deploymentId)) {
    return null;
  }

  let host: URL;
  let sandbox: URL;
  try {
    host = new URL(hostUrl);
    sandbox = new URL(rawUrl);
  } catch {
    return null;
  }

  if (
    host.protocol !== "https:" ||
    sandbox.protocol !== "https:" ||
    sandbox.origin === host.origin ||
    sandbox.username !== "" ||
    sandbox.password !== "" ||
    sandbox.hash !== ""
  ) {
    return null;
  }

  if (
    sandbox.searchParams.has("hostOrigin") ||
    sandbox.searchParams.has("deploymentId")
  ) {
    return null;
  }

  sandbox.searchParams.set("hostOrigin", host.origin);
  sandbox.searchParams.set("deploymentId", deploymentId);
  return {
    url: sandbox.href,
    origin: sandbox.origin,
    deploymentId,
  };
}

export function boundedJsonByteLength(value: unknown): number | null {
  try {
    const serialized = JSON.stringify(value);
    return serialized === undefined ? null : utf8Length(serialized);
  } catch {
    return null;
  }
}

function parseCsp(value: unknown): McpUiResourceCsp | undefined | null {
  if (value === undefined) {
    return undefined;
  }
  if (
    !isRecord(value) ||
    !hasOnlyKeys(value, [
      "connectDomains",
      "resourceDomains",
      "frameDomains",
      "baseUriDomains",
    ])
  ) {
    return null;
  }
  const parsed = McpUiResourceCspSchema.safeParse(value);
  if (!parsed.success) {
    return null;
  }
  for (const [field, origins] of Object.entries(parsed.data)) {
    if (
      origins &&
      (origins.length > MAX_MCP_APP_CSP_ORIGINS ||
        !origins.every((origin) =>
          isAllowedCspSource(
            origin,
            field === "connectDomains",
            field === "resourceDomains",
          ),
        ))
    ) {
      return null;
    }
  }
  return parsed.data;
}

function isAllowedCspSource(
  value: string,
  allowSecureWebSocket: boolean,
  allowWildcard: boolean,
): boolean {
  try {
    const url = new URL(value);
    const wildcard = url.hostname.startsWith("*.");
    return (
      (url.protocol === "https:" ||
        (allowSecureWebSocket && url.protocol === "wss:")) &&
      url.username === "" &&
      url.password === "" &&
      url.pathname === "/" &&
      url.search === "" &&
      url.hash === "" &&
      (!wildcard ||
        (allowWildcard &&
          url.hostname.length > 3 &&
          !url.hostname.slice(2).includes("*"))) &&
      value === url.origin
    );
  } catch {
    return false;
  }
}

function readHtmlBytes(
  resource: TrustedMcpAppProjection["resource"],
): Uint8Array | null {
  if (resource.text !== undefined) {
    return new TextEncoder().encode(resource.text);
  }
  if (!resource.blob || !BASE64_PATTERN.test(resource.blob)) {
    return null;
  }
  try {
    const decoded = atob(resource.blob);
    return Uint8Array.from(decoded, (character) => character.charCodeAt(0));
  } catch {
    return null;
  }
}

function hasExactlyOneContentRepresentation(
  resource: Record<string, unknown>,
): boolean {
  return (resource.text === undefined) !== (resource.blob === undefined);
}

function isResourceUri(value: unknown): value is string {
  if (
    typeof value !== "string" ||
    value.length > MAX_RESOURCE_URI_LENGTH ||
    !value.startsWith("ui://")
  ) {
    return false;
  }
  try {
    const uri = new URL(value);
    return (
      uri.protocol === "ui:" &&
      uri.hostname.length > 0 &&
      uri.username === "" &&
      uri.password === ""
    );
  } catch {
    return false;
  }
}

function isBoundedIdentifier(value: unknown): value is string {
  return (
    typeof value === "string" &&
    value.length > 0 &&
    value.length <= MAX_IDENTIFIER_LENGTH
  );
}

function isBoundedJson(value: unknown, maxBytes: number): boolean {
  const length = boundedJsonByteLength(value);
  return length !== null && length <= maxBytes;
}

function hasOnlyKeys(
  value: Record<string, unknown>,
  allowed: readonly string[],
): boolean {
  const allowedKeys = new Set(allowed);
  return Object.keys(value).every((key) => allowedKeys.has(key));
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function utf8Length(value: string): number {
  return new TextEncoder().encode(value).byteLength;
}

function maxBase64Length(bytes: number): number {
  return Math.ceil(bytes / 3) * 4;
}

function toHex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join(
    "",
  );
}

function constantTimeEqual(left: string, right: string): boolean {
  if (left.length !== right.length) {
    return false;
  }
  let difference = 0;
  for (let index = 0; index < left.length; index += 1) {
    difference |= left.charCodeAt(index) ^ right.charCodeAt(index);
  }
  return difference === 0;
}
