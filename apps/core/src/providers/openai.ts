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
    const res = await this.fetchImpl(
      `${this.cfg.baseUrl.replace(/\/$/, "")}/chat/completions`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${this.cfg.apiKey}`,
          "Content-Type": "application/json",
        },
        signal: request.signal ?? null,
        body: JSON.stringify({
          model: this.cfg.model,
          stream: true,
          stream_options: { include_usage: true },
          messages: request.messages.map((m) => ({
            role: m.role,
            content: m.content,
            ...(m.toolCallId ? { tool_call_id: m.toolCallId } : {}),
            ...(m.name ? { name: m.name } : {}),
          })),
          ...(request.tools.length
            ? {
                tools: request.tools.map((t) => ({
                  type: "function",
                  function: {
                    name: t.name,
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
      throw new Error(
        `model request failed: ${res.status} ${await res.text()}`,
      );
    }

    const calls = new Map<number, { id: string; name: string; args: string }>();
    let usage: Record<string, unknown> = {};
    const decoder = new TextDecoder();
    const reader = res.body.getReader();
    let buf = "";

    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      let nl = buf.indexOf("\n");
      while (nl >= 0) {
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
          }[];
          usage?: Record<string, unknown>;
        };
        if (json.usage) usage = json.usage;
        const delta = json.choices?.[0]?.delta;
        if (!delta) continue;
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
      nl = buf.indexOf("\n");
    }

    const sorted = [...calls.entries()].sort(([a], [b]) => a - b);
    for (const [, c] of sorted) {
      let args: Record<string, unknown> = {};
      try {
        args = JSON.parse(c.args || "{}") as Record<string, unknown>;
      } catch {
        throw new Error(
          `model emitted unparseable tool arguments for ${c.name}: ${c.args}`,
        );
      }
      const call: ToolCall = {
        id: c.id || `call-${c.name}`,
        name: c.name,
        arguments: args,
      };
      yield { type: "tool_call", call };
    }
    yield { type: "done", usage };
  }
}
