/**
 * ChatGPT subscription ("Sign in with ChatGPT") transport details for the
 * Responses adapter: the Codex backend at chatgpt.com/backend-api/codex.
 *
 * Only the model call goes there. Sumi's own context, tools, memory and
 * agent loop are unchanged — this is not a Codex agent or app-server.
 * Protocol reference: openai/codex 44fe510c (codex-rs/core/src/client.rs
 * build_responses_request, tools/src/tool_spec.rs, codex-api/src/
 * api_bridge.rs, models-manager/models.json).
 */

import { ModelError } from "../provider.ts";

/** Access material for one call; refreshed by the state service. */
export interface ChatGPTAccess {
  accessToken: string;
  accountId: string;
}

export interface ChatGPTDialect {
  /** ChatGPT-Account-ID for the initial access token. */
  accountId: string;
  /** Requested reasoning effort; omitted when unset. */
  reasoningEffort?: string;
  /**
   * Called at most once per call, after a 401 and before any output is
   * read. Returns the replacement credential or throws a ModelError.
   */
  refresh(rejectedAccessToken: string): Promise<ChatGPTAccess>;
}

/** Header the Codex client sets for "responses lite" models. */
export const LITE_HEADER = "x-openai-internal-codex-responses-lite";
/** Namespace the lite dialect groups function tools under. */
export const FUNCTION_NAMESPACE = "functions";

/**
 * Whether the Codex backend serves `model` with the "responses lite"
 * request shape. Mirrors the `use_responses_lite` flag of the Codex model
 * catalog at the reference commit (gpt-6-*, gpt-5.6-*, gpt-daybreak-*,
 * codex-auto-review: lite; gpt-5.5 and earlier: standard). The live
 * catalog is served by the backend; a model added later with another
 * flag needs this table updated.
 */
export function usesResponsesLite(model: string): boolean {
  return /^(gpt-6(\.\d+)?-|gpt-5\.6-|gpt-daybreak-|codex-auto-review$)/.test(
    model,
  );
}

const hex = (b: Uint8Array) =>
  Array.from(b, (x) => x.toString(16).padStart(2, "0")).join("");

function uuidBytes(uuid: string): Uint8Array {
  const h = uuid.replace(/-/g, "");
  const out = new Uint8Array(16);
  for (let i = 0; i < 16; i++) out[i] = parseInt(h.slice(i * 2, i * 2 + 2), 16);
  return out;
}

/** RFC 4122 version-5 UUID (SHA-1), via Web Crypto (Node and workerd). */
export async function uuidV5(namespace: string, name: string): Promise<string> {
  const ns = uuidBytes(namespace);
  const nameBytes = new TextEncoder().encode(name);
  const data = new Uint8Array(ns.length + nameBytes.length);
  data.set(ns);
  data.set(nameBytes, ns.length);
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-1", data));
  const b = digest.slice(0, 16);
  b[6] = ((b[6] ?? 0) & 0x0f) | 0x50;
  b[8] = ((b[8] ?? 0) & 0x3f) | 0x80;
  const s = hex(b);
  return `${s.slice(0, 8)}-${s.slice(8, 12)}-${s.slice(12, 16)}-${s.slice(16, 20)}-${s.slice(20)}`;
}

const NAMESPACE_OID = "6ba7b812-9dad-11d1-80b4-00c04fd430c8";

/** SHA-256 hex of a token — how a rejected token is named to the state service. */
export async function tokenDigest(token: string): Promise<string> {
  const d = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(token),
  );
  return hex(new Uint8Array(d));
}

/**
 * The lite dialect's prompt prefix: the tool set as an `additional_tools`
 * developer item (function tools grouped under the `functions`
 * namespace), then the instructions as a developer message. Their ids are
 * derived from the persona and the visible payload — never a random
 * request id — so identical prefixes keep the same identity across calls
 * and retries (the Codex client does the same per thread).
 */
export async function litePrefix(
  personaId: string,
  instructions: string | undefined,
  functions: Record<string, unknown>[],
): Promise<Record<string, unknown>[]> {
  const ns = await uuidV5(NAMESPACE_OID, personaId);
  const tools = functions.length
    ? [
        {
          type: "namespace",
          name: FUNCTION_NAMESPACE,
          description: "",
          tools: functions,
        },
      ]
    : [];
  const prefix: Record<string, unknown>[] = [
    {
      type: "additional_tools",
      id: `at_${await uuidV5(ns, JSON.stringify(tools))}`,
      role: "developer",
      tools,
    },
  ];
  if (instructions) {
    prefix.push({
      type: "message",
      id: `msg_${await uuidV5(ns, instructions)}`,
      role: "developer",
      content: [{ type: "input_text", text: instructions }],
    });
  }
  return prefix;
}

/**
 * Classify a Codex-backend 429. `usage_limit_reached` (the plan's window
 * is exhausted) and `usage_not_included` (the plan does not include this
 * use) are not transient on any retry horizon the core keeps, so they fail
 * visibly with the reset time instead of silently spending retries.
 * Returns undefined for an ordinary rate limit.
 */
export function usageLimitError(
  status: number,
  body: string,
): ModelError | undefined {
  if (status !== 429) return undefined;
  let err: { type?: unknown; resets_at?: unknown; plan_type?: unknown };
  try {
    err = (JSON.parse(body) as { error?: typeof err }).error ?? {};
  } catch {
    return undefined;
  }
  if (err.type === "usage_not_included") {
    return new ModelError(
      "ChatGPT subscription usage is not included in this account's plan",
      { retryable: false, cause: "model_usage_limit" },
    );
  }
  if (err.type !== "usage_limit_reached") return undefined;
  const resets =
    typeof err.resets_at === "number" && Number.isFinite(err.resets_at)
      ? new Date(err.resets_at * 1000).toISOString()
      : undefined;
  return new ModelError(
    `ChatGPT subscription usage limit reached${resets ? `; resets at ${resets}` : ""}`,
    { retryable: false, cause: "model_usage_limit" },
  );
}
