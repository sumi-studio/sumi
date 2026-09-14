/**
 * Shared plumbing for the wire-protocol providers (chat completions,
 * OpenAI Responses, Anthropic Messages): abort/timeout composition, an
 * SSE reader, the invocation-route envelope every adapter speaks, tool
 * name translation, and error classification. Protocol-specific request
 * and event shapes stay inside each adapter — nothing here knows a wire.
 */

import { ModelError, type ToolCall, type ToolSpec } from "../provider.ts";

/**
 * Compose the caller's abort signal with a per-request wall-clock
 * deadline covering connect through the final streamed event — a stalled
 * provider must not hold a turn open forever (CR3-N1). `done` releases
 * the timer and listener.
 */
export function requestDeadline(
  signal: AbortSignal | undefined,
  timeoutMs: number,
): { signal: AbortSignal; done(): void } {
  const ac = new AbortController();
  const onAbort = () => ac.abort(signal?.reason);
  if (signal?.aborted) ac.abort(signal.reason);
  else signal?.addEventListener("abort", onAbort, { once: true });
  const timer = setTimeout(
    () => ac.abort(new Error(`model request timed out after ${timeoutMs}ms`)),
    timeoutMs,
  );
  (timer as { unref?: () => void }).unref?.();
  return {
    signal: ac.signal,
    done() {
      clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
    },
  };
}

/**
 * Async-iterable SSE parser: yields one {event, data} per event block
 * (blank-line delimited). Comment lines and bare `event:`/`data:` fields
 * follow the SSE spec; multi-line data is joined with "\n". Cancelling
 * the iteration cancels the underlying reader.
 */
export async function* sseEvents(
  body: ReadableStream<Uint8Array>,
): AsyncGenerator<{ event: string; data: string }> {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buf = "";
  let event = "";
  let data = "";
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) {
        // A trailing line may arrive without a final newline — close it.
        buf += `${decoder.decode()}\n`;
      } else {
        buf += decoder.decode(value, { stream: true });
      }
      for (;;) {
        const nl = buf.indexOf("\n");
        if (nl < 0) break;
        let line = buf.slice(0, nl);
        buf = buf.slice(nl + 1);
        if (line.endsWith("\r")) line = line.slice(0, -1);
        if (line === "") {
          if (data !== "") yield { event, data };
          event = "";
          data = "";
          continue;
        }
        if (line.startsWith(":")) continue;
        if (line.startsWith("event:")) {
          event = line.slice(6).trim();
          continue;
        }
        if (line.startsWith("data:")) {
          const part = line.slice(5).replace(/^ /, "");
          data = data === "" ? part : `${data}\n${part}`;
        }
      }
      if (done) {
        // Flush a trailing event that arrived without a final blank line.
        if (data !== "") yield { event, data };
        return;
      }
    }
  } finally {
    await reader.cancel().catch(() => {});
  }
}

/**
 * The invocation-route envelope (ADR 0013 §1) every tool spec is wrapped
 * in on the wire: the model must declare the call's route explicitly. A
 * missing or unknown route is rejected at parse — never silently treated
 * as "normal".
 */
export function routeEnvelope(
  parameters: Record<string, unknown>,
): Record<string, unknown> {
  return {
    type: "object",
    additionalProperties: false,
    required: ["route", "input"],
    properties: {
      route: {
        type: "string",
        enum: ["normal", "elevated"],
        description:
          "Invocation route: 'normal' runs under your own authority; 'elevated' asks the human for a one-shot approval before the effect runs. A tool that needs consent can only run elevated — on 'normal' it is blocked without asking anyone.",
      },
      input: parameters,
    },
  };
}

/** Serialize a decided call for replay in the envelope shape. */
export function encodeCallArguments(call: ToolCall): string {
  return JSON.stringify({ route: call.route, input: call.arguments });
}

/**
 * Strictly validate the invocation-route envelope (ADR 0013 §1): exactly
 * {route, input}, route ∈ {normal, elevated}, input an object. A missing
 * route, unknown route, unknown field, or non-object input is a malformed
 * call — rejected, never defaulted to "normal".
 */
export function parseCallEnvelope(
  name: string,
  args: Record<string, unknown>,
): { route: "normal" | "elevated"; input: Record<string, unknown> } {
  const keys = Object.keys(args);
  const route = args.route;
  const input = args.input;
  if (
    keys.every((k) => k === "route" || k === "input") &&
    (route === "normal" || route === "elevated") &&
    input !== null &&
    typeof input === "object" &&
    !Array.isArray(input)
  ) {
    return { route, input: input as Record<string, unknown> };
  }
  throw new ModelError(
    `model emitted a malformed call envelope for ${name} — expected {route, input}`,
    { retryable: true },
  );
}

/**
 * Per-request translation between canonical tool names and the names a
 * function-calling wire accepts (OpenAI requires
 * ^[a-zA-Z][a-zA-Z0-9_-]*$, Anthropic ^[a-zA-Z0-9_-]{1,128}$ — the
 * stricter shape satisfies both). Sanitization is lossy, so a collision
 * between two tools in one request fails loudly at request-build time
 * rather than misrouting a call.
 */
export function toolNameMaps(tools: { name: string }[]): {
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

/** Tool specs wrapped in the route envelope, with wire-safe names. */
export function wireTools(
  tools: ToolSpec[],
): { spec: ToolSpec; wire: string; parameters: Record<string, unknown> }[] {
  const { toWire } = toolNameMaps(tools);
  return tools.map((t) => ({
    spec: t,
    wire: toWire.get(t.name) ?? t.name,
    parameters: routeEnvelope(t.parameters),
  }));
}

/**
 * Build the ModelError for a non-2xx provider response: bounded body
 * text, transient-vs-permanent classification by status, Retry-After
 * pacing, and context-length refusal detection. Header names and bodies
 * are the provider's — custom request headers never appear here.
 */
export async function httpError(res: Response): Promise<ModelError> {
  // Untrusted bytes bounded: a multi-MB or NUL-laden error body is
  // diagnostic text, and it flows into a commit's error field.
  const body = (await res.text().catch(() => "")).slice(0, 4096);
  const errFields = errorBodyFields(body);
  return new ModelError(`model request failed: ${res.status} ${body}`, {
    retryable: res.status === 408 || res.status === 429 || res.status >= 500,
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

/**
 * Classify a fetch() rejection: caller cancellation propagates
 * untouched; our wall deadline and network failures are transient
 * provider errors worth retrying.
 */
export function networkError(e: unknown, signal?: AbortSignal): ModelError {
  if (signal?.aborted) throw e;
  const reason = e instanceof Error ? e.message : String(e);
  return new ModelError(`model request failed: ${reason}`, {
    retryable: true,
  });
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
 * transport failure, or a content/auth refusal is never a capacity
 * signal, even when the display text mentions tokens.
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
 * message text decides, unless a known non-capacity code, a rate-limit
 * phrasing, or a retryable transport status explains it better. The
 * provider's own response is the only signal — there is no configured
 * context window to compare against.
 */
export function isContextLengthRefusal(
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
  // A retryable transport status is authoritative in the non-capacity
  // direction: a codeless 429/5xx whose display text happens to mention
  // tokens is throttling or a server error, not a size refusal — the
  // identical request can succeed once the condition clears. Message
  // patterns classify only status-less (in-band) errors and 4xx rejects.
  if (status !== null && (status === 408 || status === 429 || status >= 500)) {
    return false;
  }
  return CONTEXT_LENGTH_PATTERNS.some((p) => p.test(text));
}

/** Best-effort extraction of a provider error body's structured fields. */
export function errorBodyFields(
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
export function parseRetryAfter(v: string | null): number | undefined {
  if (!v) return undefined;
  const secs = Number(v);
  if (Number.isFinite(secs)) return Math.max(0, secs * 1000);
  const at = Date.parse(v);
  return Number.isNaN(at) ? undefined : Math.max(0, at - Date.now());
}
