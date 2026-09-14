import {
  ModelError,
  type ModelEvent,
  type ModelProvider,
  type ModelRequest,
  type ToolCall,
} from "../provider.ts";
import {
  encodeCallArguments,
  httpError,
  isContextLengthRefusal,
  networkError,
  parseCallEnvelope,
  requestDeadline,
  routeEnvelope,
  toolNameMaps,
} from "./shared.ts";

export interface OpenAIConfig {
  baseUrl: string;
  apiKey: string;
  model: string;
  /** Extra request fields (temperature, reasoning effort, ...). */
  extra?: Record<string, unknown>;
  /**
   * Static extra request headers — provider routing requirements such as
   * OpenCode Go's x-opencode-session belong here, not in the request body.
   */
  headers?: Record<string, string>;
  /**
   * Per-request wall-clock timeout covering the whole streamed response —
   * connect through final event. A stalled provider must not hold a turn
   * open forever (CR3-N1). Default 120_000ms.
   */
  timeoutMs?: number;
}

/**
 * OpenAI-compatible chat-completions provider (streaming SSE). Real network
 * path — NOT exercised in slice tests; requires SUMI_MODEL_* env config.
 * Tool calls are exposed to the model only when the request supplies specs.
 */
export class OpenAIProvider implements ModelProvider {
  readonly name = "openai";
  private readonly cfg: OpenAIConfig;
  private readonly fetchImpl: typeof fetch;

  constructor(
    cfg: OpenAIConfig,
    fetchImpl: typeof fetch = (input, init) => fetch(input, init),
  ) {
    this.cfg = cfg;
    this.fetchImpl = fetchImpl;
  }

  async *stream(request: ModelRequest): AsyncIterable<ModelEvent> {
    // Chat-completions function names must match ^[a-zA-Z][a-zA-Z0-9_-]*$ —
    // canonical tool names like "journal.note" are rejected outright by some
    // providers (OpenCode Go returns 400). Translate per request: send the
    // sanitized wire name, map the model's calls back to canonical names.
    const { toWire, fromWire } = toolNameMaps(request.tools);

    // Per-request wall timeout composed with the caller's abort signal —
    // covers connect and every stalled read until the stream ends.
    const deadline = requestDeadline(
      request.signal,
      this.cfg.timeoutMs ?? 120_000,
    );
    let readerRef: ReadableStreamDefaultReader<Uint8Array> | null = null;
    try {
      let res: Response;
      try {
        res = await this.fetchImpl(
          `${this.cfg.baseUrl.replace(/\/$/, "")}/chat/completions`,
          {
            method: "POST",
            headers: {
              Authorization: `Bearer ${this.cfg.apiKey}`,
              "Content-Type": "application/json",
              ...this.cfg.headers,
            },
            signal: deadline.signal,
            body: JSON.stringify({
              model: this.cfg.model,
              stream: true,
              stream_options: { include_usage: true },
              messages: request.messages.map((m) => ({
                role: m.role,
                content: m.content,
                ...(m.toolCallId ? { tool_call_id: m.toolCallId } : {}),
                ...(m.name ? { name: m.name } : {}),
                ...(m.toolCalls?.length
                  ? {
                      tool_calls: m.toolCalls.map((c) => ({
                        id: c.id,
                        type: "function",
                        function: {
                          name: toWire.get(c.name) ?? c.name,
                          // The wire envelope is {route, input} — replay the
                          // decided call in the same shape the model emitted.
                          arguments: encodeCallArguments(c),
                        },
                      })),
                    }
                  : {}),
              })),
              ...(request.tools.length
                ? {
                    tools: request.tools.map((t) => ({
                      type: "function",
                      function: {
                        name: toWire.get(t.name) ?? t.name,
                        description: t.description,
                        // Invocation-route envelope (ADR 0013 §1): the model
                        // must declare the call's route explicitly. A missing
                        // or unknown route is rejected at parse — never
                        // silently treated as "normal".
                        parameters: routeEnvelope(t.parameters),
                      },
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
      if (!res.ok || !res.body) {
        throw await httpError(res);
      }

      const calls = new Map<
        number,
        { id: string; name: string; args: string }
      >();
      let usage: Record<string, unknown> = {};
      let sawDone = false;
      let finishReason: string | null = null;
      const decoder = new TextDecoder();
      const reader = res.body.getReader();
      readerRef = reader;
      let buf = "";

      for (;;) {
        const { done, value } = await reader.read();
        if (done) {
          // Flush a trailing line that arrived without a final newline.
          buf += decoder.decode();
          const tail = buf.trim();
          if (tail) buf = tail + "\n";
        } else {
          buf += decoder.decode(value, { stream: true });
        }
        for (;;) {
          const nl = buf.indexOf("\n");
          if (nl < 0) break;
          const line = buf.slice(0, nl).trim();
          buf = buf.slice(nl + 1);
          if (!line.startsWith("data:")) continue;
          const data = line.slice(5).trim();
          if (data === "[DONE]") {
            sawDone = true;
            continue;
          }
          let json: {
            choices?: {
              delta?: {
                content?: string | null;
                tool_calls?: {
                  index?: number;
                  id?: string;
                  function?: { name?: string; arguments?: string };
                }[];
              };
              finish_reason?: string | null;
            }[];
            usage?: Record<string, unknown>;
            error?:
              | { message?: string; code?: unknown; type?: unknown }
              | string;
          };
          try {
            json = JSON.parse(data);
          } catch {
            // A router/proxy mid-stream failure can emit non-JSON lines —
            // an unterminated response is a transient failure, not a reply.
            throw new ModelError("malformed SSE data from provider", {
              retryable: true,
            });
          }
          // Some OpenAI-compatible routers signal failure as an in-band
          // error chunk on a 200 stream — it is a failed call, not text.
          // `!= null`: routers (LiteLLM et al.) also serialize
          // "error": null on ordinary chunks — that is no error at all.
          if (json.error != null) {
            const err = json.error;
            const em =
              typeof err === "string"
                ? err
                : (err.message ?? JSON.stringify(err));
            // Routers embed a status/code/type: a permanent-looking
            // rejection (auth, invalid request) must not retry forever.
            const code =
              typeof err === "object" && err !== null
                ? Number(err.code ?? 0)
                : 0;
            const etype =
              typeof err === "object" && err !== null
                ? String(err.type ?? "")
                : "";
            const permanent =
              (code >= 400 && code < 500 && code !== 408 && code !== 429) ||
              /authentication|invalid|permission|not_found/i.test(etype);
            throw new ModelError(
              `provider stream error: ${em.slice(0, 1024)}`,
              {
                retryable: !permanent,
                refusal: isContextLengthRefusal(
                  Number.isFinite(code) && code > 0 ? code : null,
                  typeof err === "object" && err !== null
                    ? (err.code ?? etype)
                    : undefined,
                  em,
                )
                  ? "context_length"
                  : undefined,
              },
            );
          }
          if (json.usage) usage = json.usage;
          const choice = json.choices?.[0];
          if (choice?.finish_reason) finishReason = choice.finish_reason;
          const delta = choice?.delta;
          if (!delta) continue;
          // reasoning_content deltas are deliberately not surfaced — the
          // provider port carries text + tool calls only.
          if (delta.content) yield { type: "text", delta: delta.content };
          for (const tc of delta.tool_calls ?? []) {
            const idx = tc.index ?? 0;
            const cur = calls.get(idx) ?? { id: "", name: "", args: "" };
            if (tc.id) cur.id = tc.id;
            if (tc.function?.name) cur.name += tc.function.name;
            if (tc.function?.arguments) cur.args += tc.function.arguments;
            calls.set(idx, cur);
          }
        }
        if (done) break;
      }

      // A stream that closes without [DONE] or any finish_reason was cut
      // off mid-response — the partial text is not a reply (F2). Record
      // the finish reason (e.g. "length") so a truncated-by-limit answer
      // is distinguishable in the stored usage rather than invisible.
      if (!sawDone && finishReason === null) {
        throw new ModelError(
          "provider stream ended before [DONE]/finish_reason — incomplete response",
          { retryable: true },
        );
      }
      if (finishReason !== null)
        usage = { ...usage, finish_reason: finishReason };

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
          id: c.id || `call-${c.name}`,
          // Unknown wire names (model hallucination) pass through unchanged;
          // the state service then records a definite tool error for them.
          name: fromWire.get(c.name) ?? c.name,
          route: envelope.route,
          arguments: envelope.input,
        };
        yield { type: "tool_call", call };
      }
      yield { type: "done", usage };
    } finally {
      deadline.done();
      await readerRef?.cancel().catch(() => {});
    }
  }
}
