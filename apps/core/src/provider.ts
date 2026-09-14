/**
 * Model provider port. The core assembles messages from the journal tail and
 * the current input, streams a response, and collects text deltas plus tool
 * calls. Providers are responsible only for the model call — persistence,
 * fencing, and tool execution all live in the core/state boundary.
 */

export interface ChatMessage {
  role: "system" | "user" | "assistant" | "tool";
  content: string;
  /** Present on tool-result messages. */
  toolCallId?: string;
  name?: string;
  /**
   * Present on assistant messages that carry this round's decided tool
   * calls — used to feed committed tool results back to the model.
   */
  toolCalls?: ToolCall[];
}

export interface ToolSpec {
  name: string;
  description: string;
  parameters: Record<string, unknown>;
}

export interface ToolCall {
  id: string;
  name: string;
  /**
   * The invocation route the model chose for this call (ADR 0013 §1).
   * Providers must supply it from the wire envelope — a call without a
   * route is malformed, never silently normal.
   */
  route: "normal" | "elevated";
  arguments: Record<string, unknown>;
}

export type ModelEvent =
  | { type: "text"; delta: string }
  | { type: "tool_call"; call: ToolCall }
  | { type: "done"; usage: Record<string, unknown> };

export interface ModelRequest {
  personaId: string;
  turnId: string;
  /**
   * Which model consultation this is within the turn: 0 is the initial
   * decision; each round whose tool calls have been durably executed is
   * fed back as messages and consulted as the next round.
   */
  round: number;
  messages: ChatMessage[];
  tools: ToolSpec[];
  signal?: AbortSignal;
}

export interface ModelProvider {
  readonly name: string;
  /** Streaming contract: text deltas, tool calls, then exactly one done. */
  stream(request: ModelRequest): AsyncIterable<ModelEvent>;
}

/**
 * A provider failure carrying its own retry disposition. `retryable`
 * distinguishes transient conditions (5xx/429, network, timeout, an
 * incomplete stream) from rejections no retry can fix (auth, bad
 * request). `retryAfterMs` carries provider-supplied pacing (Retry-After)
 * so the durable retry honors it instead of guessing.
 *
 * `refusal` marks a deterministic rejection of the request's content —
 * "context_length" = the provider refused because the request was too
 * large. Such a refusal is never transient: retrying the identical request
 * can never succeed, so it stays non-retryable and does not spend the
 * transient-retry budget — but the same turn may continue with a smaller
 * temporary working view. It is classified from the provider's own
 * status/code/message, never from a configured context window.
 */
export class ModelError extends Error {
  readonly retryable: boolean;
  readonly retryAfterMs?: number;
  readonly refusal?: "context_length";
  constructor(
    message: string,
    opts: {
      retryable: boolean;
      retryAfterMs?: number;
      refusal?: "context_length";
    },
  ) {
    super(message);
    this.name = "ModelError";
    this.retryable = opts.retryable;
    this.retryAfterMs = opts.retryAfterMs;
    this.refusal = opts.refusal;
  }
}
