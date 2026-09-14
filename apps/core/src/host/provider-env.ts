/**
 * Shared model-provider wiring for both hosts: the same resolution runs
 * under the Node local host and the workerd Durable Object alike, so a
 * persona's secretary uses the same model in either runtime.
 *
 * The persona's human's model selection is authoritative and is re-read
 * from the state service for every model call, so a changed selection or a
 * rotated key applies from the next consultation without a restart:
 *
 *   api      → exactly that connection's base_url / model / api_key, when
 *              its preset speaks chat completions (the wire this core
 *              implements). Anything else fails the request.
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
import { StateError, type StateClient } from "../state-client.ts";
import type { FundingRef, ModelBinding } from "../types.ts";
import {
  BudgetWaitError,
  newFactId,
  reportedTokens,
  requestEstimate,
} from "../usage.ts";
import { MockProvider } from "../providers/mock.ts";
import { OpenAIProvider } from "../providers/openai.ts";

/**
 * Connection presets whose wire protocol is OpenAI chat completions — the
 * same set the Rust agent maps to ApiProtocol::OpenAiChatCompletions.
 * openai-responses and anthropic use other wires this core does not speak.
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

  /**
   * Preflight for callers that hold durable work before consulting the
   * model (memory preparation claims a chunk): resolves the binding
   * without sending a request so an unusable selection is a pause, not a
   * spent attempt. The stream still re-resolves — the binding can change
   * between this check and the call.
   */
  async probe(): Promise<void> {
    await this.resolve();
  }

  /**
   * The metered call path: resolve the selected funding, admit the
   * priced estimate under the writer generation BEFORE any provider
   * bytes leave, stream, then record one usage fact. A denial throws
   * BudgetWaitError — no request was sent. The fact id is unique to this
   * invocation: a retried or resent call is genuinely additional spend
   * and records its own fact; only the record's own redelivery is
   * idempotent.
   */
  async *stream(request: ModelRequest): AsyncIterable<ModelEvent> {
    const { provider, identity } = await this.resolve();
    const { state, persona } = this.opts;
    if (request.generation === undefined) {
      throw new Error("a metered call requires the writer generation");
    }
    const factId = newFactId(request);
    const funding = fundingRef(identity);
    const admission = await state.admitUsage(persona, request.generation, {
      factId,
      kind: "model_call",
      phase: request.phase ?? "turn",
      turnId: request.turnId,
      round: request.round,
      funding,
      estimate: requestEstimate(request),
    });
    if (!admission.admitted) {
      throw new BudgetWaitError(
        admission.wait ?? {
          funding,
          needed_minor: 0,
          limit_minor: 0,
          spent_minor: 0,
          held_minor: 0,
          remaining_minor: 0,
          currency: "",
          pricing_revision: "",
          bounded: false,
        },
      );
    }
    let usage: Record<string, unknown> | null = null;
    let streamError: unknown = null;
    try {
      for await (const ev of provider.stream(request)) {
        if (ev.type !== "done") {
          yield ev;
          continue;
        }
        usage = ev.usage;
        // The recorded plan's usage names the connection that produced the
        // decision, so "which model answered" is durable evidence.
        yield { type: "done", usage: { ...ev.usage, model_binding: identity } };
      }
    } catch (e) {
      streamError = e;
    }
    await this.record(factId, request, funding, usage);
    if (streamError !== null) throw streamError;
  }

  /**
   * Persist the call's usage fact. Recording is not writer-fenced — the
   * spend already happened — so this still lands after a fence loss or an
   * aborted stream. A call whose usage never resolved records 'unknown',
   * never silently zero. A recording failure must not fail the turn:
   * retrying the turn would spend again, so after bounded retries the gap
   * is logged and the held reservation reconciles at turn commit or
   * generation recovery instead.
   */
  private async record(
    factId: string,
    request: ModelRequest,
    funding: FundingRef,
    usage: Record<string, unknown> | null,
  ): Promise<void> {
    const { state, persona, log } = this.opts;
    const tokens = usage === null
      ? { input: null, output: null, cached: null }
      : reportedTokens(usage);
    // 'reported' only when the provider's report carried at least one
    // token category; a done event without recognizable usage — or a call
    // that ended before its report — is 'unknown', not a zero bill.
    const reported =
      tokens.input !== null || tokens.output !== null || tokens.cached !== null;
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        await state.recordUsage(persona, {
          factId,
          kind: "model_call",
          phase: request.phase ?? "turn",
          turnId: request.turnId,
          inputId: request.inputId,
          round: request.round,
          funding,
          status: reported ? "reported" : "unknown",
          inputTokens: tokens.input,
          outputTokens: tokens.output,
          cachedTokens: tokens.cached,
          quantities: usage ?? {},
        });
        return;
      } catch (e) {
        if (attempt === 2) {
          log?.("usage fact could not be recorded", {
            fact_id: factId,
            turn_id: request.turnId,
            error: String(e),
          });
          return;
        }
        await new Promise((r) => setTimeout(r, 250 * (attempt + 1)));
      }
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
        e instanceof StateError && e.status >= 400 && e.status < 500 &&
        e.status !== 429;
      throw new ModelError(`model selection lookup failed: ${msg}`, {
        retryable: !definite,
        unavailable: true,
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
    if (!CHAT_COMPLETIONS_PRESETS.has(c.preset)) {
      throw unusable(
        `the selected connection ${c.name} uses preset ${c.preset}, whose protocol this core does not implement`,
      );
    }
    if (!binding.credential_available || !binding.api_key) {
      throw unusable(
        `the selected connection ${c.name} has no usable credential (${binding.reason ?? "unavailable"}); re-enter its API key`,
      );
    }
    const identity: BindingIdentity = {
      selection: "api",
      connection_id: c.id,
      preset: c.preset,
      model: c.model,
      version: c.version,
    };
    this.opts.log?.("model bound to selected connection", identity);
    return {
      provider: new OpenAIProvider({
        baseUrl: c.base_url,
        apiKey: binding.api_key,
        model: c.model,
        timeoutMs: this.opts.timeoutMs,
      }),
      identity,
    };
  }
}

function unusable(message: string): ModelError {
  return new ModelError(message, { retryable: false, unavailable: true });
}

/**
 * The funding principal for the resolved binding — the identity recorded
 * on the usage fact and charged at admission. 'unset' (no selection)
 * spends the operator's environment default as kind 'operator'/'env'; a
 * selected API connection is kind 'connection' under its own id, with the
 * version/model/preset snapshot preserved at call time.
 */
function fundingRef(identity: BindingIdentity): FundingRef {
  if (identity.selection === "unset") {
    return { kind: "operator", id: "env", provider: identity.provider };
  }
  return {
    kind: "connection",
    id: identity.connection_id,
    version: identity.version,
    model: identity.model,
    provider: identity.preset,
  };
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
