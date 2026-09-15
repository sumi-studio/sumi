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
 *   SUMI_MODEL_PROVIDER=mock|openai|none   (default mock; none = an
 *                                      unselected persona has no model and
 *                                      its requests fail visibly — the Cloud
 *                                      setting, where no operator model may
 *                                      answer for a user)
 *   SUMI_MODEL_BASE_URL / _API_KEY / _MODEL   (openai)
 *   SUMI_MODEL_HEADERS_JSON           (openai; static extra request headers
 *                                      as a JSON object, e.g. a provider
 *                                      routing/session header)
 *   SUMI_MODEL_EXTRA_JSON             (openai; extra request fields as a
 *                                      JSON object, e.g. max_tokens)
 *   SUMI_MODEL_TIMEOUT_MS             (openai and selected connections;
 *                                      per-request wall timeout, default
 *                                      120000)
 *   SUMI_MODEL_PROVIDER=fixture       (scripted deterministic model for
 *                                      integration tests)
 *   SUMI_MODEL_FIXTURE_JSON           (fixture; the script as inline JSON —
 *                                      portable, works under workerd)
 *   SUMI_MODEL_FIXTURE                (fixture; a script file path — only on
 *                                      hosts that pass a file loader, i.e.
 *                                      the Node local host)
 */

import {
  ModelError,
  type ModelEvent,
  type ModelProvider,
  type ModelRequest,
} from "../provider.ts";
import { AnthropicProvider } from "../providers/anthropic.ts";
import { FixtureProvider } from "../providers/fixture.ts";
import { MockProvider } from "../providers/mock.ts";
import { OpenAIProvider } from "../providers/openai.ts";
import { OpenAIResponsesProvider } from "../providers/openai-responses.ts";
import { type StateClient, StateError } from "../state-client.ts";
import type { FundingRef, ModelBinding, UsageAdmitResult } from "../types.ts";
import {
  BudgetWaitError,
  newFactId,
  reportedTokens,
  requestEstimate,
} from "../usage.ts";

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

/**
 * Presets served by the OpenCode Go endpoint — the legacy agent sent
 * `x-opencode-session` with the PA's stable session id on this wire.
 */
export const OPENCODE_PRESETS: ReadonlySet<string> = new Set([
  "opencode-go",
  "opencode-zen-go",
]);

/**
 * Whether the connection should send `x-opencode-session` with the
 * persona's stable identity: the opencode-* presets always do, and any
 * protocol preset pointed at an opencode.ai host needs it too — the Go
 * gateway requires the header on chat, responses AND messages alike.
 * Hostname match, not URL-prefix guessing; the base URL is already
 * shape-validated upstream.
 */
export function opencodeSessionHeader(
  preset: string,
  baseUrl: string,
): string | undefined {
  if (OPENCODE_PRESETS.has(preset)) return "x-opencode-session";
  try {
    const host = new URL(baseUrl).hostname.toLowerCase();
    if (host === "opencode.ai" || host.endsWith(".opencode.ai")) {
      return "x-opencode-session";
    }
  } catch {
    /* unparseable base URL fails at the store/binding boundary */
  }
  return undefined;
}

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
  loadFixtureScript?: (path: string) => string,
): ModelProvider {
  return new SelectedModelProvider({
    state,
    persona,
    fallback: providerFromEnv(get, loadFixtureScript),
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
   * Binding preflight: resolves the selection exactly as the next call
   * would, without sending a request. Throws the same `ModelError` the
   * call would raise — callers that gate work on a usable model (memory
   * preparation) can pause instead of spending it. The stream still
   * re-resolves — the binding can change between this check and the call.
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
    if (request.generation === undefined) {
      throw new Error("a metered call requires the writer generation");
    }
    const factId = newFactId(request);
    const funding = fundingRef(identity);
    // The estimate prices the provider's real wire bound — an output cap
    // the adapter does not send cannot count as a bound on the bill.
    const estimate = requestEstimate(request, provider.outputBound?.());
    const admission = await this.admit(factId, request, funding, estimate);
    if (!admission.admitted) {
      throw new BudgetWaitError(
        admission.wait ?? {
          estimate,
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
    // The provider asserts no request was produced only via an
    // 'unavailable' error — every other outcome may have sent bytes.
    let notSent = false;
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
      notSent = e instanceof ModelError && e.unavailable === true;
    } finally {
      // finally, not a post-loop statement: a consumer early-return or a
      // caller abort also lands the fact — recording cannot silently
      // disappear with the held reservation.
      await this.record(factId, request, funding, usage, notSent);
    }
    if (streamError !== null) throw streamError;
  }

  /**
   * Admit one call, replaying the SAME fact id across transient retries —
   * the state service replays a held reservation, so a lost admit
   * response never double-reserves. A definitive denial is a verdict, not
   * a failure; a contract violation or a dead-writer fence is not
   * retryable.
   */
  private async admit(
    factId: string,
    request: ModelRequest,
    funding: FundingRef,
    estimate: ReturnType<typeof requestEstimate>,
  ): Promise<UsageAdmitResult> {
    const { state, persona } = this.opts;
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        return await state.admitUsage(persona, request.generation ?? 0, {
          factId,
          kind: "model_call",
          phase: request.phase ?? "turn",
          turnId: request.turnId,
          inputId: request.inputId,
          round: request.round,
          funding,
          estimate,
        });
      } catch (e) {
        const transient =
          !(e instanceof StateError) || e.status === 429 || e.status >= 500;
        if (!transient || attempt === 2) {
          throw new ModelError(`usage admission failed: ${e}`, {
            retryable: transient,
          });
        }
        await new Promise((r) => setTimeout(r, 150 * (attempt + 1)));
      }
    }
    throw new ModelError("usage admission failed", { retryable: true });
  }

  /**
   * Persist the call's usage fact. Recording is not writer-fenced — the
   * spend already happened — so this still lands after a fence loss or an
   * aborted stream. A call whose usage never resolved records 'unknown'
   * (the admission estimate stays spent), never silently zero; a call the
   * provider asserts was never produced records 'not_sent' and releases
   * its hold. A recording failure must not fail the turn: retrying the
   * turn would spend again, so after bounded retries the gap is logged
   * and the held reservation reconciles into an inspectable 'unrecorded'
   * fact at turn commit or generation recovery — the durable record of
   * the call survives even when this report does not.
   */
  private async record(
    factId: string,
    request: ModelRequest,
    funding: FundingRef,
    usage: Record<string, unknown> | null,
    notSent: boolean,
  ): Promise<void> {
    const { state, persona, log } = this.opts;
    const tokens =
      usage === null
        ? { input: null, output: null, cached: null }
        : reportedTokens(usage);
    // 'reported' only when the report is complete enough to price: input
    // and output both present (cached is an optional subset). A partial
    // report — or none at all — is 'unknown': the categories it did carry
    // are kept, the admission estimate stays spent, and a missing category
    // is never priced as zero. The state service refuses a partial
    // 'reported' too. A call that produced usage was sent, whatever error
    // followed.
    const reported = tokens.input !== null && tokens.output !== null;
    const status =
      notSent && usage === null
        ? "not_sent"
        : reported
          ? "reported"
          : "unknown";
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
          status,
          inputTokens: tokens.input,
          outputTokens: tokens.output,
          cachedTokens: tokens.cached,
          quantities: usage ?? {},
        });
        return;
      } catch (e) {
        // A contract conflict (409) is authoritative — the fact is
        // recorded or genuinely different; retrying cannot help either way.
        if (e instanceof StateError && e.status === 409) {
          log?.("usage fact conflicted with a recorded fact", {
            fact_id: factId,
            turn_id: request.turnId,
            error: String(e),
          });
          return;
        }
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
        e instanceof StateError &&
        e.status >= 400 &&
        e.status < 500 &&
        e.status !== 429;
      throw new ModelError(`model selection lookup failed: ${msg}`, {
        retryable: !definite,
        // The model was never consulted — this is an availability gap,
        // not an evaluated-model failure.
        unavailable: true,
      });
    }
    switch (binding.selection) {
      case "unset":
        if (this.opts.fallback instanceof NoSelectionProvider) {
          throw unusable(NO_SELECTION_MESSAGE);
        }
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
    // OpenCode Go requires x-opencode-session on every protocol endpoint
    // (chat, responses and messages all answer 400 MissingSessionID
    // without it — verified live). The header carries the persona's
    // stable identity per request; it is sent whenever the connection
    // targets OpenCode, whichever protocol preset was selected.
    const sessionHeader = opencodeSessionHeader(c.preset, c.base_url);
    const shared = {
      baseUrl: c.base_url,
      apiKey: binding.api_key,
      model: c.model,
      headers,
      timeoutMs: this.opts.timeoutMs,
      maxOutputTokens: c.max_output_tokens,
      sessionHeader,
    };
    const provider: ModelProvider = RESPONSES_PRESETS.has(c.preset)
      ? new OpenAIResponsesProvider(shared)
      : ANTHROPIC_PRESETS.has(c.preset)
        ? new AnthropicProvider({ ...shared, maxTokens: c.max_output_tokens })
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
  return new ModelError(message, { retryable: false, unavailable: true });
}

const NO_SELECTION_MESSAGE =
  "no model connection is selected for this secretary; choose a connection to let the secretary answer";

/** SUMI_MODEL_PROVIDER=none: the env default is "no model". */
export class NoSelectionProvider implements ModelProvider {
  readonly name = "none";

  stream(_request: ModelRequest): AsyncIterable<ModelEvent> {
    return {
      [Symbol.asyncIterator]: () => ({
        next: () => Promise.reject(unusable(NO_SELECTION_MESSAGE)),
      }),
    };
  }
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
  loadFixtureScript?: (path: string) => string,
): ModelProvider {
  const kind = get("SUMI_MODEL_PROVIDER") ?? "mock";
  if (kind === "openai") {
    const baseUrl = required(get, "SUMI_MODEL_BASE_URL");
    // The connection path's URL is validated by the Go store; the env
    // path has no such boundary, so an unparseable base URL must fail
    // here at boot — not as a per-request fetch defect.
    try {
      new URL(baseUrl);
    } catch {
      throw new Error(`SUMI_MODEL_BASE_URL is not a URL: ${baseUrl}`);
    }
    return new OpenAIProvider({
      baseUrl,
      apiKey: required(get, "SUMI_MODEL_API_KEY"),
      model: required(get, "SUMI_MODEL_MODEL"),
      headers: jsonObj(get, "SUMI_MODEL_HEADERS_JSON") as
        | Record<string, string>
        | undefined,
      extra: jsonObj(get, "SUMI_MODEL_EXTRA_JSON"),
      timeoutMs: numEnv(get, "SUMI_MODEL_TIMEOUT_MS", 120_000),
    });
  }
  if (kind === "fixture") {
    // Scripted deterministic model for integration tests; see fixture.ts.
    // The script is data: any runtime may pass it inline, while a path needs
    // a host that can read files — this module is bundled into workerd.
    const inline = get("SUMI_MODEL_FIXTURE_JSON");
    if (inline) return new FixtureProvider(inline);
    const scriptPath = required(get, "SUMI_MODEL_FIXTURE");
    if (!loadFixtureScript) {
      throw new Error(
        "SUMI_MODEL_FIXTURE is a file path; this host cannot read files — pass the script as SUMI_MODEL_FIXTURE_JSON",
      );
    }
    return new FixtureProvider(loadFixtureScript(scriptPath));
  }
  if (kind === "none") return new NoSelectionProvider();
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
