import type {
  ModelEvent,
  ModelProvider,
  ModelRequest,
  ToolCall,
} from "../provider.ts";

interface OpenAIConfig {
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
    const timeoutMs = this.cfg.timeoutMs ?? 120_000;
    const ac = new AbortController();
    const onAbort = () => ac.abort(request.signal?.reason);
    if (request.signal?.aborted) ac.abort(request.signal.reason);
    else request.signal?.addEventListener("abort", onAbort, { once: true });
    const timer = setTimeout(
      () => ac.abort(new Error(`model request timed out after ${timeoutMs}ms`)),
      timeoutMs,
    );
    (timer as { unref?: () => void }).unref?.();
    try {
      const res = await this.fetchImpl(
        `${this.cfg.baseUrl.replace(/\/$/, "")}/chat/completions`,
        {
          method: "POST",
          headers: {
            Authorization: `Bearer ${this.cfg.apiKey}`,
            "Content-Type": "application/json",
            ...this.cfg.headers,
          },
          signal: ac.signal,
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
                        arguments: JSON.stringify(c.arguments),
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
                      parameters: t.parameters,
                    },
                  })),
                }
              : {}),
            ...this.cfg.extra,
          }),
        },
      );
      if (!res.ok || !res.body) {
        // Untrusted bytes bounded: a multi-MB or NUL-laden error body is
        // diagnostic text, and it flows into a commit's error field.
        const body = (await res.text()).slice(0, 4096);
        throw new Error(`model request failed: ${res.status} ${body}`);
      }

      const calls = new Map<number, { id: string; name: string; args: string }>();
      let usage: Record<string, unknown> = {};
      const decoder = new TextDecoder();
      const reader = res.body.getReader();
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
          if (data === "[DONE]") continue;
          const json = JSON.parse(data) as {
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
          };
          if (json.usage) usage = json.usage;
          const delta = json.choices?.[0]?.delta;
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

      const sorted = [...calls.entries()].sort(([a], [b]) => a - b);
      for (const [, c] of sorted) {
        let args: Record<string, unknown> = {};
        try {
          args = JSON.parse(c.args || "{}") as Record<string, unknown>;
        } catch {
          throw new Error(
            `model emitted unparseable tool arguments for ${c.name}: ${c.args.slice(0, 1024)}`,
          );
        }
        const call: ToolCall = {
          id: c.id || `call-${c.name}`,
          // Unknown wire names (model hallucination) pass through unchanged;
          // the state service then records a definite tool error for them.
          name: fromWire.get(c.name) ?? c.name,
          arguments: args,
        };
        yield { type: "tool_call", call };
      }
      yield { type: "done", usage };
    } finally {
      clearTimeout(timer);
      request.signal?.removeEventListener("abort", onAbort);
    }
  }
}

/**
 * Per-request translation between canonical tool names and the names a
 * chat-completions wire accepts (^[a-zA-Z][a-zA-Z0-9_-]*$). Sanitization is
 * lossy, so a collision between two tools in one request fails loudly at
 * request-build time rather than misrouting a call.
 */
function toolNameMaps(tools: { name: string }[]): {
  toWire: Map<string, string>;
  fromWire: Map<string, string>;
} {
  const toWire = new Map<string, string>();
  const fromWire = new Map<string, string>();
  for (const t of tools) {
    let wire = t.name.replace(/[^a-zA-Z0-9_-]/g, "_");
    if (!/^[a-zA-Z]/.test(wire)) wire = `t_${wire}`;
    const taken = fromWire.get(wire);
    if (taken !== undefined && taken !== t.name) {
      throw new Error(
        `tool names collide on the wire: ${taken} and ${t.name} → ${wire}`,
      );
    }
    toWire.set(t.name, wire);
    fromWire.set(wire, t.name);
  }
  return { toWire, fromWire };
}
