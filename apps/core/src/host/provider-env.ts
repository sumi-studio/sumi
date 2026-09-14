/**
 * Shared model-provider wiring for both hosts: the same resolution runs
 * under the Node local host and the workerd Durable Object alike, so a
 * persona's secretary uses the same model in either runtime.
 *
 * The persona's human's model selection is authoritative and is re-read
 * from the state service for every model call, so a changed selection or a
 * rotated key applies from the next consultation without a restart:
 *
 *   api      → exactly that connection's base_url / model / api_key /
 *              extra_headers, on the wire its preset declares (chat
 *              completions, OpenAI Responses, or Anthropic Messages).
 *              Any other preset fails the request.
 *   none     → the user chose "接続しない" (do not switch to another
 *              account): the request fails; no operator model is used.
 *   chatgpt  → not implemented by this core: the request fails.
 *   unset    → no selection exists (dev personas without a human, or a
 *              human who never chose): the operator env default below.
 *
 * A selection that cannot be honored is a non-retryable model failure the
 * user can see, never a silent fallback to a different model.
 *
 *   SUMI_MODEL_PROVIDER=mock|openai   (default mock)
 *   SUMI_MODEL_BASE_URL / _API_KEY / _MODEL   (openai)
 *   SUMI_MODEL_HEADERS_JSON           (openai; static extra request headers
 *                                      as a JSON object, e.g. a provider
 *                                      routing/session header)
 *   SUMI_MODEL_EXTRA_JSON             (openai; extra request fields as a
 *                                      JSON object, e.g. max_tokens)
 *   SUMI_MODEL_TIMEOUT_MS             (openai and selected connections;
 *                                      per-request wall timeout, default
 *                                      120000)
 */

import {
  ModelError,
  type ModelEvent,
  type ModelProvider,
  type ModelRequest,
} from "../provider.ts";
import { AnthropicProvider } from "../providers/anthropic.ts";
import { MockProvider } from "../providers/mock.ts";
import { OpenAIProvider } from "../providers/openai.ts";
import { OpenAIResponsesProvider } from "../providers/openai-responses.ts";
import { type StateClient, StateError } from "../state-client.ts";
import type { ModelBinding } from "../types.ts";

/**
 * Connection presets whose wire protocol is OpenAI chat completions — the
 * same set the Rust agent maps to ApiProtocol::OpenAiChatCompletions.
 */
export const CHAT_COMPLETIONS_PRESETS: ReadonlySet<string> = new Set([
  "openai-chat",
  "kimi-k3",
  "glm-5.2",
  "umans",
  "umans-kimi-k2.7",
  "opencode-go",
  "opencode-zen-go",
]);

/** Presets on the OpenAI Responses wire (POST {base}/responses). */
export const RESPONSES_PRESETS: ReadonlySet<string> = new Set([
  "openai-responses",
]);

/** Presets on the Anthropic Messages wire (POST {base}/v1/messages). */
export const ANTHROPIC_PRESETS: ReadonlySet<string> = new Set(["anthropic"]);

/** Non-secret identity of the connection a consultation actually used. */
export type BindingIdentity =
  | { selection: "unset"; provider: string }
  | {
      selection: "api";
      connection_id: string;
      preset: string;
      model: string;
      version: string;
    };

/**
 * Build the persona's provider. Construction validates the env default
 * eagerly (a misconfigured host fails at boot, as before); the selection
 * itself is resolved per model call.
 */
export function providerForPersona(
  state: StateClient,
  persona: string,
  get: (name: string) => string | undefined,
  log?: (msg: string, fields?: Record<string, unknown>) => void,
): ModelProvider {
  return new SelectedModelProvider({
    state,
    persona,
    fallback: providerFromEnv(get),
    timeoutMs: numEnv(get, "SUMI_MODEL_TIMEOUT_MS", 120_000),
    log,
  });
}

export class SelectedModelProvider implements ModelProvider {
  readonly name = "selected";
  private readonly opts: {
    state: StateClient;
    persona: string;
    fallback: ModelProvider;
    timeoutMs: number;
    log?: (msg: string, fields?: Record<string, unknown>) => void;
  };

  constructor(opts: SelectedModelProvider["opts"]) {
    this.opts = opts;
  }

  async *stream(request: ModelRequest): AsyncIterable<ModelEvent> {
    const { provider, identity } = await this.resolve();
    for await (const ev of provider.stream(request)) {
      // The recorded plan's usage names the connection that produced the
      // decision, so "which model answered" is durable evidence.
      yield ev.type === "done"
        ? { type: "done", usage: { ...ev.usage, model_binding: identity } }
        : ev;
    }
  }

  private async resolve(): Promise<{
    provider: ModelProvider;
    identity: BindingIdentity;
  }> {
    const { state, persona } = this.opts;
    let binding: ModelBinding;
    try {
      binding = await state.modelBinding(persona);
    } catch (e) {
      // Not knowing the selection is not a reason to guess it: an env
      // fallback could run a model the user did not choose. A transient
      // lookup failure retries like any provider outage.
      const msg = e instanceof Error ? e.message : String(e);
      const definite =
        e instanceof StateError &&
        e.status >= 400 &&
        e.status < 500 &&
        e.status !== 429;
      throw new ModelError(`model selection lookup failed: ${msg}`, {
        retryable: !definite,
      });
    }
    switch (binding.selection) {
      case "unset":
        return {
          provider: this.opts.fallback,
          identity: { selection: "unset", provider: this.opts.fallback.name },
        };
      case "needs_rebinding":
        // A transferred secretary's carried selection intent is not yet
        // satisfied here. Running the environment default now would be a
        // silent model substitution — fail closed until the destination
        // binds a matching connection (or clears the intent).
        throw unusable(
          binding.reason ??
            `the secretary's model selection intent (${binding.intent?.kind ?? "unknown"}) needs a destination connection binding`,
        );
      case "none":
        throw unusable(
          "the selected model connection is 接続しない (none); choose a connection to let the secretary answer",
        );
      case "chatgpt":
        throw unusable(
          "the selected ChatGPT connection is not supported by this core yet; choose an API connection",
        );
      case "api":
        break;
      default:
        throw unusable(
          `unknown model selection ${String((binding as { selection: unknown }).selection)}`,
        );
    }
    const c = binding.connection;
    if (!c) {
      throw unusable(
        binding.reason ?? "the selected API connection no longer exists",
      );
    }
    if (
      !CHAT_COMPLETIONS_PRESETS.has(c.preset) &&
      !RESPONSES_PRESETS.has(c.preset) &&
      !ANTHROPIC_PRESETS.has(c.preset)
    ) {
      throw unusable(
        `the selected connection ${c.name} uses preset ${c.preset}, whose protocol this core does not implement`,
      );
    }
    if (!binding.credential_available || !binding.api_key) {
      throw unusable(
        `the selected connection ${c.name} has no usable credential (${binding.reason ?? "unavailable"}); re-enter its API key`,
      );
    }
    // Per-connection extra headers travel with the binding (sealed with
    // the credential on the server) and reach only this connection's
    // endpoint.
    const headers = c.extra_headers;
    const shared = {
      baseUrl: c.base_url,
      apiKey: binding.api_key,
      model: c.model,
      headers,
      timeoutMs: this.opts.timeoutMs,
    };
    const provider: ModelProvider = RESPONSES_PRESETS.has(c.preset)
      ? new OpenAIResponsesProvider(shared)
      : ANTHROPIC_PRESETS.has(c.preset)
        ? new AnthropicProvider(shared)
        : new OpenAIProvider(shared);
    const identity: BindingIdentity = {
      selection: "api",
      connection_id: c.id,
      preset: c.preset,
      model: c.model,
      version: c.version,
    };
    this.opts.log?.("model bound to selected connection", identity);
    return { provider, identity };
  }
}

function unusable(message: string): ModelError {
  return new ModelError(message, { retryable: false });
}

export function providerFromEnv(
  get: (name: string) => string | undefined,
): ModelProvider {
  const kind = get("SUMI_MODEL_PROVIDER") ?? "mock";
  if (kind === "openai") {
    return new OpenAIProvider({
      baseUrl: required(get, "SUMI_MODEL_BASE_URL"),
      apiKey: required(get, "SUMI_MODEL_API_KEY"),
      model: required(get, "SUMI_MODEL_MODEL"),
      headers: jsonObj(get, "SUMI_MODEL_HEADERS_JSON") as
        | Record<string, string>
        | undefined,
      extra: jsonObj(get, "SUMI_MODEL_EXTRA_JSON"),
      timeoutMs: numEnv(get, "SUMI_MODEL_TIMEOUT_MS", 120_000),
    });
  }
  if (kind !== "mock") throw new Error(`unknown SUMI_MODEL_PROVIDER ${kind}`);
  return new MockProvider();
}

function required(
  get: (name: string) => string | undefined,
  name: string,
): string {
  const v = get(name);
  if (!v) throw new Error(`missing env ${name}`);
  return v;
}

function jsonObj(
  get: (name: string) => string | undefined,
  name: string,
): Record<string, unknown> | undefined {
  const raw = get(name);
  if (!raw) return undefined;
  const parsed = JSON.parse(raw) as unknown;
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new Error(`${name} must be a JSON object`);
  }
  return parsed as Record<string, unknown>;
}

function numEnv(
  get: (name: string) => string | undefined,
  name: string,
  fallback: number,
): number {
  const raw = get(name);
  if (!raw) return fallback;
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) {
    throw new Error(`${name} must be a positive number`);
  }
  return n;
}
