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
