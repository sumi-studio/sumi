import {
  ModelError,
  type ModelEvent,
  type ModelProvider,
  type ModelRequest,
  type ToolCall,
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
      } catch (e) {
        // Caller cancellation propagates untouched; our wall timeout and
        // network failures are transient provider errors worth retrying.
        if (request.signal?.aborted) throw e;
        const reason = e instanceof Error ? e.message : String(e);
        throw new ModelError(`model request failed: ${reason}`, {
          retryable: true,
        });
      }
      if (!res.ok || !res.body) {
        // Untrusted bytes bounded: a multi-MB or NUL-laden error body is
        // diagnostic text, and it flows into a commit's error field.
        const body = (await res.text()).slice(0, 4096);
        const errFields = errorBodyFields(body);
        throw new ModelError(`model request failed: ${res.status} ${body}`, {
          retryable:
            res.status === 408 || res.status === 429 || res.status >= 500,
          retryAfterMs: parseRetryAfter(res.headers.get("retry-after")),
          refusal: isContextLengthRefusal(
            res.status,
            errFields?.code ?? errFields?.type,
            errFields?.message ?? body,
          )
            ? "context_length"
            : undefined,
        });
      }

      const calls = new Map<number, { id: string; name: string; args: string }>();
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
      if (finishReason !== null) usage = { ...usage, finish_reason: finishReason };

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
      await readerRef?.cancel().catch(() => {});
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
      throw new ModelError(
        `tool names collide on the wire: ${taken} and ${t.name} → ${wire}`,
        { retryable: false },
      );
    }
    toWire.set(t.name, wire);
    fromWire.set(wire, t.name);
  }
  return { toWire, fromWire };
}

/**
 * Machine-readable codes the OpenAI-compatible ecosystem uses for a
 * context-capacity refusal. Authoritative even when the display message
 * contains broad words such as "tokens".
 */
const CONTEXT_LENGTH_CODES = new Set([
  "model_context_window_exceeded",
  "context_length_exceeded",
  "request_too_large",
  "413",
  "http_413",
]);

/**
 * Codes authoritative in the other direction: a rate limit, a server or
 * transport failure, or a content/auth refusal is never a capacity signal,
 * even when the display text mentions tokens.
 */
const NON_OVERFLOW_CODES = new Set([
  "network_error",
  "request_error",
  "transport_error",
  "overloaded_error",
  "server_error",
  "unexpected_sse_eof",
  "idle_timeout",
  "response_header_timeout",
  "sensitive",
  "content_filter",
  "cancelled",
  "invalid_provider_stream",
  "rate_limit",
  "rate_limit_exceeded",
  "throttling",
  "too_many_requests",
  "insufficient_quota",
  "invalid_api_key",
  "authentication",
  "permission_denied",
  "408",
  "429",
  "500",
  "502",
  "503",
  "504",
  "524",
  "http_408",
  "http_429",
  "http_500",
  "http_502",
  "http_503",
  "http_504",
  "http_524",
]);

/** Display text that means rate limiting, not capacity. */
const NON_OVERFLOW_PATTERNS = [
  /^(Throttling error|Service unavailable):/i,
  /rate limit/i,
  /too many requests/i,
];

/** Display text providers use for context-length rejection. */
const CONTEXT_LENGTH_PATTERNS = [
  /prompt is too long/i,
  /request_too_large/i,
  /input is too long for requested model/i,
  /exceeds the context window/i,
  /exceeds (the )?(model'?s )?maximum context length/i,
  /input token count.*exceeds the maximum/i,
  /maximum prompt length is \d+/i,
  /reduce the length of the messages/i,
  /maximum context length is \d+ tokens/i,
  /exceeds (the )?maximum allowed input length/i,
  /is longer than the model'?s context length/i,
  /exceeds the limit of \d+/i,
  /exceeds the available context size/i,
  /greater than the context length/i,
  /context window exceeds limit/i,
  /exceeded model token limit/i,
  /too large for model with \d+ maximum context length/i,
  /prompt has [\d,]+ tokens?.*configured context size/i,
  /model_context_window_exceeded/i,
  /prompt too long; exceeded (max )?context length/i,
  /context[_ ]length[_ ]exceeded/i,
  /too many tokens/i,
  /token limit exceeded/i,
];

/**
 * Whether an error response is a deterministic context-capacity refusal:
 * HTTP 413 or a recognized provider code is authoritative; otherwise the
 * message text decides, unless a known non-capacity code or a rate-limit
 * phrasing explains it better. The provider's own response is the only
 * signal — there is no configured context window to compare against.
 */
function isContextLengthRefusal(
  status: number | null,
  code: unknown,
  text: string,
): boolean {
  if (status === 413) return true;
  const c =
    typeof code === "number"
      ? String(code)
      : typeof code === "string"
        ? code
        : "";
  if (c) {
    if (CONTEXT_LENGTH_CODES.has(c)) return true;
    if (NON_OVERFLOW_CODES.has(c)) return false;
  }
  if (NON_OVERFLOW_PATTERNS.some((p) => p.test(text))) return false;
  return CONTEXT_LENGTH_PATTERNS.some((p) => p.test(text));
}

/** Best-effort extraction of a provider error body's structured fields. */
function errorBodyFields(
  body: string,
): { code?: unknown; type?: unknown; message?: string } | null {
  try {
    const err = (JSON.parse(body) as { error?: unknown })?.error;
    if (typeof err === "string") return { message: err };
    if (err && typeof err === "object") {
      const e = err as { code?: unknown; type?: unknown; message?: string };
      return { code: e.code, type: e.type, message: e.message };
    }
  } catch {
    // Not JSON — the caller matches on the raw body text.
  }
  return null;
}

/** Parse a Retry-After header (delay-seconds or HTTP-date) into ms. */
function parseRetryAfter(v: string | null): number | undefined {
  if (!v) return undefined;
  const secs = Number(v);
  if (Number.isFinite(secs)) return Math.max(0, secs * 1000);
  const at = Date.parse(v);
  return Number.isNaN(at) ? undefined : Math.max(0, at - Date.now());
}
