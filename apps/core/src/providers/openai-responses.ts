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
  encodeCallArguments,
  httpError,
  isContextLengthRefusal,
  networkError,
  parseCallEnvelope,
  redirectRefusal,
  requestDeadline,
  requestHeaders,
  SUMI_USER_AGENT,
  sanitizeToolName,
  sseEvents,
  wireTools,
} from "./shared.ts";

export interface ResponsesConfig {
  baseUrl: string;
  apiKey: string;
  model: string;
  /** Extra request fields (reasoning effort, text format, ...). */
  extra?: Record<string, unknown>;
  /**
   * Per-connection extra request headers (routing requirements such as a
   * gateway session header). Validated and sealed with the credential on
   * the connection; sent only to this connection's endpoint.
   */
  headers?: Record<string, string>;
  /**
   * Per-request wall-clock timeout covering the whole streamed response.
   * Default 120_000ms.
   */
  timeoutMs?: number;
  /**
   * Bound on generated tokens (max_output_tokens), per connection. When
   * unset the field is omitted entirely — the parameter is optional in
   * the API and the model's own output cap applies, so no fixed default
   * can make a lower-cap model fail every request.
   */
  maxOutputTokens?: number;
  /**
   * When set, the request carries this header with the persona's stable
   * identity (request.personaId) — e.g. x-opencode-session, which
   * OpenCode Go requires on every endpoint for routing. Applied after
   * `headers` so the live identity always wins over a static value.
   */
  sessionHeader?: string;
}

/**
 * OpenAI Responses API provider (POST {base}/responses, streaming SSE).
 *
 * The provider's server-side state (`store`/`previous_response_id`) is
 * not used: the durable journal is the conversation, so every call sends
 * the rendered context and `store:false` asks the provider to keep no
 * record of it.
 */
export class OpenAIResponsesProvider implements ModelProvider {
  readonly name = "openai-responses";
  private readonly cfg: ResponsesConfig;
  private readonly fetchImpl: typeof fetch;

  constructor(
    cfg: ResponsesConfig,
    fetchImpl: typeof fetch = (input, init) => fetch(input, init),
  ) {
    this.cfg = cfg;
    this.fetchImpl = fetchImpl;
  }

  /** The output bound the next request will actually send, if any. */
  outputBound(): number | undefined {
    const extra = this.cfg.extra;
    const overridden =
      extra !== undefined ? numOr(extra.max_output_tokens) : undefined;
    return overridden ?? this.cfg.maxOutputTokens;
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
        res = await this.fetchImpl(
          `${this.cfg.baseUrl.replace(/\/$/, "")}/responses`,
          {
            method: "POST",
            headers: requestHeaders(
              {
                Authorization: `Bearer ${this.cfg.apiKey}`,
                "Content-Type": "application/json",
                "User-Agent": SUMI_USER_AGENT,
              },
              this.cfg.headers,
              this.cfg.sessionHeader
                ? { header: this.cfg.sessionHeader, value: request.personaId }
                : undefined,
            ),
            signal: deadline.signal,
            // Never follow a redirect: this request carries credentials
            // and fetch forwards x-api-key/extra headers cross-origin.
            redirect: "manual",
            body: JSON.stringify({
              model: this.cfg.model,
              stream: true,
              store: false,
              // Stable per-persona identity for provider prefix-cache
              // routing — the same identity the legacy agent supplied as
              // `session_id` (session IDs are the documented common value).
              prompt_cache_key: request.personaId,
              ...(this.cfg.maxOutputTokens
                ? { max_output_tokens: this.cfg.maxOutputTokens }
                : {}),
              ...toInput(request.messages, toWire),
              ...(tools.length
                ? {
                    tools: tools.map((t) => ({
                      type: "function",
                      name: t.wire,
                      description: t.spec.description,
                      parameters: t.parameters,
                    })),
                  }
                : {}),
              ...this.cfg.extra,
            }),
          },
        );
      } catch (e) {
        throw networkError(e, request.signal);
      }
      const refused = redirectRefusal(res);
      if (refused) throw refused;
      if (!res.ok || !res.body) {
        throw await httpError(res);
      }

      // Function calls in flight, keyed by output_index; `items` maps a
      // call item's `item_id` (used by argument delta events) back to it.
      const calls = new Map<
        number,
        { callId: string; name: string; args: string }
      >();
      const items = new Map<string, number>();
      let usage: Record<string, unknown> = {};
      let finished = false;
      events = sseEvents(res.body);

      for await (const { data } of events) {
        let json: {
          type?: string;
          output_index?: number;
          item_id?: string;
          delta?: string;
          arguments?: string;
          item?: {
            type?: string;
            id?: string;
            call_id?: string;
            name?: string;
            arguments?: string;
          };
          response?: {
            status?: string;
            error?: { code?: unknown; message?: string } | null;
            incomplete_details?: { reason?: string } | null;
            usage?: Record<string, unknown> | null;
            output?: {
              type?: string;
              call_id?: string;
              name?: string;
              arguments?: string;
            }[];
          };
          code?: unknown;
          message?: string;
        };
        try {
          json = JSON.parse(data);
        } catch {
          throw new ModelError("malformed SSE data from provider", {
            retryable: true,
          });
        }
        switch (json.type) {
          case "response.output_text.delta":
            if (json.delta) yield { type: "text", delta: json.delta };
            break;
          case "response.output_item.added": {
            const item = json.item;
            if (item?.type === "function_call") {
              const idx = json.output_index ?? calls.size;
              if (item.id) items.set(item.id, idx);
              calls.set(idx, {
                callId: item.call_id ?? item.id ?? "",
                name: item.name ?? "",
                args: "",
              });
            }
            break;
          }
          case "response.function_call_arguments.delta": {
            const idx =
              (json.item_id ? items.get(json.item_id) : undefined) ??
              json.output_index;
            const cur = idx === undefined ? undefined : calls.get(idx);
            if (cur && json.delta) cur.args += json.delta;
            break;
          }
          case "response.function_call_arguments.done": {
            const idx =
              (json.item_id ? items.get(json.item_id) : undefined) ??
              json.output_index;
            const cur = idx === undefined ? undefined : calls.get(idx);
            if (cur && typeof json.arguments === "string") {
              cur.args = json.arguments;
            }
            break;
          }
          case "response.output_item.done": {
            const item = json.item;
            if (item?.type === "function_call") {
              const idx = json.output_index ?? calls.size;
              const cur = calls.get(idx) ?? { callId: "", name: "", args: "" };
              if (item.call_id) cur.callId = item.call_id;
              if (item.name) cur.name = item.name;
              if (typeof item.arguments === "string" && item.arguments) {
                cur.args = item.arguments;
              }
              calls.set(idx, cur);
            }
            break;
          }
          case "response.completed": {
            const r = json.response;
            if (r?.usage) usage = r.usage;
            // Provider did not emit item events (some compatible servers):
            // recover calls from the terminal response object itself.
            for (const [i, item] of (r?.output ?? []).entries()) {
              if (item?.type === "function_call" && !calls.has(i)) {
                calls.set(i, {
                  callId: item.call_id ?? "",
                  name: item.name ?? "",
                  args: item.arguments ?? "",
                });
              }
            }
            finished = true;
            break;
          }
          case "response.incomplete": {
            const reason =
              json.response?.incomplete_details?.reason ?? "unknown";
            if (json.response?.usage) usage = json.response.usage;
            // Recover any calls the terminal object carries, as with
            // response.completed — a truncated stream may still contain
            // finished function_call items.
            for (const [i, item] of (json.response?.output ?? []).entries()) {
              if (item?.type === "function_call" && !calls.has(i)) {
                calls.set(i, {
                  callId: item.call_id ?? "",
                  name: item.name ?? "",
                  args: item.arguments ?? "",
                });
              }
            }
            if (reason === "max_output_tokens") {
              // The output cap is not an input-context refusal: like
              // finish_reason "length"/"max_tokens" on the other wires,
              // the truncated text is the completed answer, recorded so
              // the stored decision shows it was cut off.
              usage = { ...usage, finish_reason: reason };
              finished = true;
              break;
            }
            if (reason === "content_filter") {
              // A deterministic refusal — the identical request can never
              // pass the filter, so it must not spend the retry budget.
              throw new ModelError(`provider stream incomplete: ${reason}`, {
                retryable: false,
              });
            }
            throw new ModelError(`provider stream incomplete: ${reason}`, {
              retryable: true,
            });
          }
          case "response.failed": {
            const err = json.response?.error;
            const message = err?.message ?? "response failed";
            const code = typeof err?.code === "string" ? err.code : undefined;
            throw new ModelError(`provider stream failed: ${message}`, {
              // A server-side failure is transient unless it reports a
              // definite permanent code.
              retryable:
                code !== undefined
                  ? !/invalid|authentication|permission/i.test(code)
                  : true,
              refusal: isContextLengthRefusal(null, code, message)
                ? "context_length"
                : undefined,
            });
          }
          case "error": {
            const message = json.message ?? "stream error";
            const code = typeof json.code === "string" ? json.code : "";
            throw new ModelError(`provider stream error: ${message}`, {
              retryable: !/invalid|authentication|permission/i.test(code),
              refusal: isContextLengthRefusal(null, code, message)
                ? "context_length"
                : undefined,
            });
          }
          default:
            // response.created / .in_progress / content_part.* / reasoning /
            // ping-style keepalives carry nothing the port surfaces.
            break;
        }
        if (finished) break;
      }

      // A stream that ends without a terminal event was cut off — the
      // partial text is not a reply.
      if (!finished) {
        throw new ModelError(
          "provider stream ended before response.completed — incomplete response",
          { retryable: true },
        );
      }

      const sorted = [...calls.entries()].sort(([a], [b]) => a - b);
      for (const [, c] of sorted) {
        let args: Record<string, unknown> = {};
        try {
          args = JSON.parse(c.args || "{}") as Record<string, unknown>;
        } catch {
          throw new ModelError(
            `model emitted unparseable tool arguments for ${c.name}: ${c.args.slice(0, 1024)}`,
            { retryable: true },
          );
        }
        const envelope = parseCallEnvelope(c.name, args);
        const call: ToolCall = {
          id: c.callId || `call-${c.name}`,
          // Unknown wire names (model hallucination) pass through unchanged;
          // the state service records a definite tool error for them.
          name: fromWire.get(c.name) ?? c.name,
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
 * Convert the journal's message list into Responses `instructions` +
 * `input` items. Assistant tool calls replay as `function_call` items
 * carrying the recorded call_id verbatim; tool results feed back as
 * `function_call_output` keyed by that same call_id.
 */
function toInput(
  messages: ChatMessage[],
  toWire: Map<string, string>,
): {
  instructions?: string;
  input: Record<string, unknown>[];
} {
  const system: string[] = [];
  const input: Record<string, unknown>[] = [];
  for (const m of messages) {
    switch (m.role) {
      case "system":
        system.push(m.content);
        break;
      case "user":
        input.push({
          type: "message",
          role: "user",
          content: [{ type: "input_text", text: m.content }],
        });
        break;
      case "assistant": {
        if (m.content) {
          // The easy input-message form: role "assistant" is documented as
          // "presumed to have been generated by the model in previous
          // interactions" and needs no item id — unlike the output-message
          // form, where id is a required field the journal never stored.
          input.push({
            type: "message",
            role: "assistant",
            content: m.content,
          });
        }
        for (const c of m.toolCalls ?? []) {
          // call_id links the result back; the item `id` is optional on a
          // function_call input item and is not persisted in the journal.
          input.push({
            type: "function_call",
            call_id: c.id,
            // The recorded name is canonical; replay must still produce a
            // valid wire name when the tool is no longer advertised in
            // this request, so the deterministic transform is the
            // fallback.
            name: toWire.get(c.name) ?? sanitizeToolName(c.name),
            arguments: encodeCallArguments(c),
            status: "completed",
          });
        }
        break;
      }
      case "tool":
        input.push({
          type: "function_call_output",
          call_id: m.toolCallId,
          output: m.content,
        });
        break;
    }
  }
  return {
    ...(system.length ? { instructions: system.join("\n\n") } : {}),
    input,
  };
}

function numOr(v: unknown): number | undefined {
  return typeof v === "number" && Number.isFinite(v) && v > 0 ? v : undefined;
}
