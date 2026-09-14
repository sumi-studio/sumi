import {
  type ChatMessage,
  ModelError,
  type ModelEvent,
  type ModelProvider,
  type ModelRequest,
  type ToolCall,
} from "../provider.ts";
import {
  assertExtraHeaders,
  httpError,
  isContextLengthRefusal,
  networkError,
  parseCallEnvelope,
  requestDeadline,
  sseEvents,
  wireTools,
} from "./shared.ts";

export interface AnthropicConfig {
  baseUrl: string;
  apiKey: string;
  model: string;
  /** Extra request fields (temperature, top_p, ...). */
  extra?: Record<string, unknown>;
  /**
   * Per-connection extra request headers (e.g. a gateway routing header).
   * Validated and sealed with the credential on the connection; sent only
   * to this connection's endpoint.
   */
  headers?: Record<string, string>;
  /**
   * Per-request wall-clock timeout covering the whole streamed response.
   * Default 120_000ms.
   */
  timeoutMs?: number;
  /**
   * max_tokens is a required field on the Messages API. Default 16_384 —
   * the legacy agent's default output budget for this protocol.
   */
  maxTokens?: number;
}

const API_VERSION = "2023-06-01";
const DEFAULT_MAX_TOKENS = 16_384;

/**
 * Anthropic Messages API provider (POST {base}/v1/messages, streaming
 * SSE). The conversation is rebuilt from the durable journal on every
 * call: assistant turns replay their decided tool calls as `tool_use`
 * blocks, and tool results feed back as `tool_result` blocks inside a
 * user message.
 */
export class AnthropicProvider implements ModelProvider {
  readonly name = "anthropic";
  private readonly cfg: AnthropicConfig;
  private readonly fetchImpl: typeof fetch;

  constructor(
    cfg: AnthropicConfig,
    fetchImpl: typeof fetch = (input, init) => fetch(input, init),
  ) {
    this.cfg = cfg;
    this.fetchImpl = fetchImpl;
  }

  async *stream(request: ModelRequest): AsyncIterable<ModelEvent> {
    assertExtraHeaders(this.cfg.headers);
    const tools = wireTools(request.tools);
    const toWire = new Map(tools.map((t) => [t.spec.name, t.wire]));
    const fromWire = new Map(tools.map((t) => [t.wire, t.spec.name]));

    const deadline = requestDeadline(
      request.signal,
      this.cfg.timeoutMs ?? 120_000,
    );
    let events: AsyncGenerator<{ event: string; data: string }> | null = null;
    try {
      let res: Response;
      try {
        res = await this.fetchImpl(messagesUrl(this.cfg.baseUrl), {
          method: "POST",
          headers: {
            "x-api-key": this.cfg.apiKey,
            "anthropic-version": API_VERSION,
            "Content-Type": "application/json",
            ...this.cfg.headers,
          },
          signal: deadline.signal,
          body: JSON.stringify({
            model: this.cfg.model,
            stream: true,
            max_tokens: this.cfg.maxTokens ?? DEFAULT_MAX_TOKENS,
            ...toMessages(request.messages, toWire),
            ...(tools.length
              ? {
                  tools: tools.map((t) => ({
                    name: t.wire,
                    description: t.spec.description,
                    input_schema: t.parameters,
                  })),
                }
              : {}),
            ...this.cfg.extra,
          }),
        });
      } catch (e) {
        throw networkError(e, request.signal);
      }
      if (!res.ok || !res.body) {
        throw await httpError(res);
      }

      // Content blocks in flight, keyed by their stream index. Text blocks
      // stream deltas straight out; tool_use blocks accumulate
      // input_json_delta partial JSON until content_block_stop.
      const blocks = new Map<
        number,
        { kind: "tool_use"; id: string; name: string; args: string }
      >();
      let usage: Record<string, unknown> = {};
      let stopReason: string | null = null;
      let sawStop = false;
      events = sseEvents(res.body);

      for await (const { data } of events) {
        let json: {
          type?: string;
          index?: number;
          content_block?: {
            type?: string;
            id?: string;
            name?: string;
          };
          delta?: {
            type?: string;
            text?: string;
            partial_json?: string;
            stop_reason?: string;
          };
          usage?: { input_tokens?: number; output_tokens?: number };
          message?: { usage?: Record<string, unknown> };
          error?: { type?: string; message?: string };
        };
        try {
          json = JSON.parse(data);
        } catch {
          throw new ModelError("malformed SSE data from provider", {
            retryable: true,
          });
        }
        switch (json.type) {
          case "message_start":
            if (json.message?.usage) usage = { ...json.message.usage };
            break;
          case "content_block_start": {
            const block = json.content_block;
            if (block?.type === "tool_use") {
              blocks.set(json.index ?? blocks.size, {
                kind: "tool_use",
                id: block.id ?? "",
                name: block.name ?? "",
                args: "",
              });
            }
            break;
          }
          case "content_block_delta": {
            const delta = json.delta;
            if (delta?.type === "text_delta" && delta.text) {
              yield { type: "text", delta: delta.text };
            } else if (delta?.type === "input_json_delta") {
              const cur = blocks.get(json.index ?? -1);
              if (cur && delta.partial_json) cur.args += delta.partial_json;
            }
            // thinking_delta / signature_delta are not surfaced — the port
            // carries text + tool calls only.
            break;
          }
          case "content_block_stop":
            break;
          case "message_delta":
            if (json.delta?.stop_reason) stopReason = json.delta.stop_reason;
            if (json.usage) usage = { ...usage, ...json.usage };
            break;
          case "message_stop":
            sawStop = true;
            break;
          case "ping":
            break;
          case "error": {
            const err = json.error;
            const message = err?.message ?? "stream error";
            const code = err?.type ?? "";
            // Overload / transient server errors retry — api_error is the
            // generic 500-class condition the docs recommend retrying with
            // backoff; auth and request rejections never do.
            const transient =
              /overloaded|rate_limit|timeout|internal|api_error/i.test(code);
            throw new ModelError(`provider stream error: ${message}`, {
              retryable: transient,
              refusal:
                code === "request_too_large" ||
                isContextLengthRefusal(null, code, message)
                  ? "context_length"
                  : undefined,
            });
          }
          default:
            break;
        }
        if (sawStop) break;
      }

      // A stream that closes without message_stop was cut off
      // mid-response — the partial text is not a reply.
      if (!sawStop) {
        throw new ModelError(
          "provider stream ended before message_stop — incomplete response",
          { retryable: true },
        );
      }
      // A response that stopped at the output cap is a completed but
      // truncated answer: record the reason in usage so the stored
      // decision shows it, like the chat adapter's finish_reason.
      if (stopReason !== null) {
        usage = { ...usage, finish_reason: stopReason };
      }

      const sorted = [...blocks.entries()].sort(([a], [b]) => a - b);
      for (const [, b] of sorted) {
        let args: Record<string, unknown> = {};
        try {
          args = JSON.parse(b.args || "{}") as Record<string, unknown>;
        } catch {
          throw new ModelError(
            `model emitted unparseable tool arguments for ${b.name}: ${b.args.slice(0, 1024)}`,
            { retryable: true },
          );
        }
        const envelope = parseCallEnvelope(b.name, args);
        const call: ToolCall = {
          id: b.id || `call-${b.name}`,
          // Unknown wire names (model hallucination) pass through
          // unchanged; the state service records a definite tool error.
          name: fromWire.get(b.name) ?? b.name,
          route: envelope.route,
          arguments: envelope.input,
        };
        yield { type: "tool_call", call };
      }
      yield { type: "done", usage };
    } finally {
      deadline.done();
      await events?.return(undefined).catch(() => {});
    }
  }
}

/**
 * The Messages endpoint for a configured base URL. `https://x` (no path)
 * and `https://x/v1` both resolve to `https://x/v1/messages`; any other
 * path prefix (a gateway mount) is kept and `/v1/messages` appended.
 */
function messagesUrl(baseUrl: string): string {
  const base = baseUrl.replace(/\/+$/, "");
  return base.endsWith("/v1") ? `${base}/messages` : `${base}/v1/messages`;
}

/**
 * Convert the journal's message list into Anthropic `system` +
 * `messages`. Tool results are `tool_result` blocks inside a user
 * message — consecutive tool messages coalesce into one user turn — and
 * consecutive same-role messages merge, since the API requires strict
 * user/assistant alternation.
 */
function toMessages(
  messages: ChatMessage[],
  toWire: Map<string, string>,
): {
  system?: string;
  messages: { role: "user" | "assistant"; content: unknown }[];
} {
  const system: string[] = [];
  const out: { role: "user" | "assistant"; content: unknown[] }[] = [];
  const push = (role: "user" | "assistant", block: Record<string, unknown>) => {
    const last = out.at(-1);
    if (last && last.role === role) last.content.push(block);
    else out.push({ role, content: [block] });
  };
  for (const m of messages) {
    switch (m.role) {
      case "system":
        system.push(m.content);
        break;
      case "user":
        // Anthropic rejects empty text blocks — an empty user message
        // contributes no block (any adjacent tool_result blocks still
        // coalesce into a user turn).
        if (m.content) push("user", { type: "text", text: m.content });
        break;
      case "assistant":
        if (m.content) push("assistant", { type: "text", text: m.content });
        for (const c of m.toolCalls ?? []) {
          // The recorded name is canonical; the model emitted the
          // wire-safe name, so replay must translate back.
          push("assistant", {
            type: "tool_use",
            id: c.id,
            name: toWire.get(c.name) ?? c.name,
            input: { route: c.route, input: c.arguments },
          });
        }
        break;
      case "tool":
        push("user", {
          type: "tool_result",
          tool_use_id: m.toolCallId,
          content: m.content,
        });
        break;
    }
  }
  return {
    ...(system.length ? { system: system.join("\n\n") } : {}),
    messages: out,
  };
}
