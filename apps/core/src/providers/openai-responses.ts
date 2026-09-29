import {
  type ChatMessage,
  type ModelBindingSnapshot,
  ModelError,
  type ModelEvent,
  type ModelProvider,
  type ModelRequest,
  type ProviderContinuation,
  type ToolCall,
} from "../provider.ts";
import { assertBindingSnapshot, snapshotFor } from "./binding-snapshot.ts";
import {
  CHATGPT_MAX_REQUEST_BYTES,
  CHATGPT_REJECTED_HEADER,
  type ChatGPTDialect,
  continuationEntry,
  continuationScope,
  FUNCTION_NAMESPACE,
  litePrefix,
  MAX_CONTINUATION_BYTES,
  safeDiagnosticCode,
  usageLimitError,
  usesResponsesLite,
  utf8Length,
} from "./chatgpt-codex.ts";
import {
  assertExtraHeaders,
  disambiguateCallIds,
  encodeCallArguments,
  errorBodyFields,
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
  /**
   * ChatGPT request dialect with an authenticated API transport. Core
   * never receives its OAuth token. An explicit 401 permits one resend
   * with the rejected digest; Go owns serialized grant refresh.
   */
  chatgpt?: ChatGPTDialect;
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
  readonly name: string;
  private readonly cfg: ResponsesConfig;
  private readonly fetchImpl: typeof fetch;

  constructor(
    cfg: ResponsesConfig,
    fetchImpl: typeof fetch = (input, init) => fetch(input, init),
  ) {
    this.cfg = cfg;
    this.fetchImpl = fetchImpl;
    this.name = cfg.chatgpt ? "chatgpt-codex" : "openai-responses";
  }

  /** The output bound the next request will actually send, if any. */
  outputBound(): number | undefined {
    // The Codex backend takes no output bound; none is sent.
    if (this.cfg.chatgpt) return undefined;
    const extra = this.cfg.extra;
    const overridden =
      extra !== undefined ? numOr(extra.max_output_tokens) : undefined;
    return overridden ?? this.cfg.maxOutputTokens;
  }

  async snapshotBinding(): Promise<ModelBindingSnapshot> {
    const { chatgpt, ...settings } = this.cfg;
    const effort =
      chatgpt?.reasoningEffort ??
      (this.cfg.extra?.reasoning as { effort?: string } | undefined)?.effort;
    return snapshotFor(
      this.name,
      {
        ...settings,
        ...(chatgpt
          ? {
              accountId: chatgpt.accountId,
              reasoningEffort: chatgpt.reasoningEffort,
            }
          : {}),
      },
      this.cfg.model,
      effort,
    );
  }

  async *stream(request: ModelRequest): AsyncIterable<ModelEvent> {
    const preservePrefix =
      request.phase === "memory" || !!request.bindingSnapshot;
    if (request.bindingSnapshot) {
      assertBindingSnapshot(
        request.bindingSnapshot,
        await this.snapshotBinding(),
      );
    }
    if (request.reasoningEffort !== undefined) {
      if (
        request.phase !== "memory" ||
        this.cfg.model !== "gpt-6-astra" ||
        request.reasoningEffort !== "medium" ||
        typeof request.reasoningEffortAfter !== "number" ||
        !Number.isSafeInteger(request.reasoningEffortAfter) ||
        request.reasoningEffortAfter < 0 ||
        request.reasoningEffortAfter > request.messages.length
      ) {
        throw new ModelError("invalid memory reasoning configuration update", {
          retryable: false,
          unavailable: true,
        });
      }
    }
    assertExtraHeaders(this.cfg.headers);
    const tools = wireTools(request.tools);
    const toWire = new Map(tools.map((t) => [t.spec.name, t.wire]));
    const fromWire = new Map(tools.map((t) => [t.wire, t.spec.name]));

    const timeoutMs = this.cfg.timeoutMs ?? 120_000;
    const deadlineAt = Date.now() + timeoutMs;
    const deadline = requestDeadline(request.signal, timeoutMs);
    let events: AsyncGenerator<{ event: string; data: string }> | null = null;
    try {
      const chatgpt = this.cfg.chatgpt;
      // Continuation (encrypted reasoning) is exchanged only on the
      // ChatGPT dialect, scoped to this account and model.
      const scope = chatgpt
        ? await continuationScope(chatgpt.accountId, this.cfg.model)
        : undefined;
      const built = chatgpt
        ? await chatGPTBody(
            this.cfg.model,
            chatgpt,
            request,
            tools,
            toWire,
            scope,
          )
        : undefined;
      let replaying = built?.replaying ?? false;
      let body = built
        ? built.body
        : JSON.stringify({
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
            ...standardInput(request.messages, toWire, request),
            ...(tools.length
              ? {
                  // `strict: false` is explicit: when it is omitted,
                  // Responses normalizes each schema into strict mode
                  // where it can, which marks every property required.
                  // Sumi's tools use optional fields as alternatives
                  // (conversation_history's seq/chunk_seq/from_seq, a
                  // search-only query), so the normalized schema admits
                  // no valid read at all. The canonical schema, as
                  // written, is the contract on every provider;
                  // receivers validate the call.
                  tools: tools.map((t) => functionTool(t)),
                }
              : {}),
            ...this.cfg.extra,
          });
      if (chatgpt) {
        // A saved branch must keep its exact inherited prefix. Only an
        // unpinned ordinary turn may omit optional continuation. Requests
        // still above the transport cap are refused before any send.
        if (
          !preservePrefix &&
          replaying &&
          utf8Length(body) > CHATGPT_MAX_REQUEST_BYTES
        ) {
          body = (
            await chatGPTBody(
              this.cfg.model,
              chatgpt,
              request,
              tools,
              toWire,
              scope,
              true,
            )
          ).body;
          replaying = false;
        }
        const bytes = utf8Length(body);
        if (bytes > CHATGPT_MAX_REQUEST_BYTES) {
          throw new ModelError(
            `ChatGPT request is ${bytes} bytes, above the ${CHATGPT_MAX_REQUEST_BYTES}-byte transport limit; it was not sent`,
            { retryable: false, refusal: "context_length", unavailable: true },
          );
        }
      }
      let rejectedDigest: string | undefined;
      let refreshed = false;
      let res: Response;
      for (;;) {
        try {
          res = chatgpt
            ? await chatgpt.send(
                body,
                deadline.signal,
                rejectedDigest,
                Math.max(1, deadlineAt - Date.now()),
              )
            : await this.fetchImpl(
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
                      ? {
                          header: this.cfg.sessionHeader,
                          value: request.personaId,
                        }
                      : undefined,
                  ),
                  signal: deadline.signal,
                  // Never follow a redirect: this request carries credentials
                  // and fetch forwards x-api-key/extra headers cross-origin.
                  redirect: "manual",
                  body,
                },
              );
        } catch (e) {
          if (chatgpt) {
            if (e instanceof ModelError || request.signal?.aborted) throw e;
            throw new ModelError(
              "ChatGPT transport ended without a response; acceptance is unknown",
              { retryable: false },
            );
          }
          throw networkError(e, request.signal);
        }
        // A subscription access token rejected before any output: refresh
        // once through the state service (which serializes rotation) and
        // resend the identical body. A second 401 is not retried.
        if (res.status === 401 && chatgpt && !refreshed) {
          await res.body?.cancel().catch(() => {});
          const digest = res.headers.get(CHATGPT_REJECTED_HEADER);
          if (!digest || !/^[a-f0-9]{64}$/.test(digest)) {
            throw new ModelError("ChatGPT transport authorization failed", {
              retryable: false,
              unavailable: true,
            });
          }
          rejectedDigest = digest;
          refreshed = true;
          continue;
        }
        // Ordinary unpinned turns can retry a rejected optional continuation.
        // For pinned/memory calls, return the refusal without changing the
        // inherited context; the durable branch owns recovery.
        if (res.status === 400 && chatgpt && replaying && !preservePrefix) {
          await res.body?.cancel().catch(() => {});
          body = (
            await chatGPTBody(
              this.cfg.model,
              chatgpt,
              request,
              tools,
              toWire,
              scope,
              true,
            )
          ).body;
          replaying = false;
          rejectedDigest = undefined; // The earlier refresh already persisted.
          continue;
        }
        break;
      }
      if (res.status === 401 && chatgpt) {
        // The refresh succeeded — a reconnect would only repeat it — and
        // the backend still refuses the fresh token. Why is ChatGPT's to
        // say; the body's error code is kept only when it is shaped like
        // an error identifier, never the body itself.
        const text = (await res.text().catch(() => "")).slice(0, 4096);
        const code = safeDiagnosticCode(text);
        throw new ModelError(
          `ChatGPT rejected this connection's sign-in again right after it was refreshed (HTTP 401${code ? `, code ${code}` : ""})`,
          // No output was produced and nothing was billed: the model was
          // not consulted.
          {
            retryable: false,
            cause: "model_auth_rejected",
            unavailable: true,
          },
        );
      }
      const refused = redirectRefusal(res);
      if (refused) throw refused;
      if (chatgpt && res.status === 429) {
        const text = (await res.text().catch(() => "")).slice(0, 4096);
        throw (
          usageLimitError(429, text) ??
          (await httpError(new Response(text, res)))
        );
      }
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
      // The round's output order with its encrypted reasoning, kept for
      // continuation (ChatGPT dialect only).
      const kept = new Map<number, Record<string, unknown>>();
      const messageText = new Map<number, { text: string; bytes: number }>();
      const encoder = new TextEncoder();
      let pendingMessageBytes = 0;
      let continuationBytes = 0;
      let continuationTooLarge = false;
      let reasoningItems = 0;
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
            retryable: !chatgpt,
          });
        }
        switch (json.type) {
          case "response.output_text.delta":
            if (json.delta) {
              if (chatgpt && !continuationTooLarge) {
                const idx = json.output_index ?? 0;
                const prior = messageText.get(idx);
                const deltaBytes = encoder.encode(json.delta).length;
                pendingMessageBytes += deltaBytes;
                if (
                  continuationBytes + pendingMessageBytes >
                  MAX_CONTINUATION_BYTES
                ) {
                  continuationTooLarge = true;
                  messageText.clear();
                  kept.clear();
                } else
                  messageText.set(idx, {
                    text: (prior?.text ?? "") + json.delta,
                    bytes: (prior?.bytes ?? 0) + deltaBytes,
                  });
              }
              yield { type: "text", delta: json.delta };
            }
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
            if (chatgpt && !continuationTooLarge) {
              const entry = continuationEntry(item);
              if (entry) {
                const idx = json.output_index ?? kept.size;
                if (entry.type === "message" && entry.text === undefined) {
                  entry.text = messageText.get(idx)?.text ?? "";
                }
                pendingMessageBytes -= messageText.get(idx)?.bytes ?? 0;
                messageText.delete(idx);
                continuationBytes += encoder.encode(
                  JSON.stringify(entry),
                ).length;
                if (
                  continuationBytes + pendingMessageBytes >
                  MAX_CONTINUATION_BYTES
                ) {
                  continuationTooLarge = true;
                  kept.clear();
                  messageText.clear();
                } else kept.set(idx, entry);
                if (entry.type === "reasoning") {
                  reasoningItems += 1;
                }
              }
            }
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
            throw new ModelError(
              chatgpt
                ? "ChatGPT response incomplete"
                : `provider stream incomplete: ${reason}`,
              {
                retryable: !chatgpt,
              },
            );
          }
          case "response.failed": {
            const err = json.response?.error;
            const message = chatgpt
              ? "ChatGPT response failed"
              : (err?.message ?? "response failed");
            const code = chatgpt
              ? safeDiagnosticCode(JSON.stringify({ error: err }))
              : typeof err?.code === "string"
                ? err.code
                : undefined;
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
            const err = errorBodyFields(data) ?? {
              code: json.code,
              message: json.message,
            };
            const message = chatgpt
              ? "ChatGPT stream error"
              : streamErrorMessage(err.message, this.cfg.apiKey);
            const code =
              safeDiagnosticCode(JSON.stringify({ error: err })) ?? "";
            throw new ModelError(
              `provider stream error${code ? ` (${code})` : ""}: ${message}`,
              {
                retryable:
                  code !== "project_spend_limit_exceeded" &&
                  !/invalid|authentication|permission/i.test(code),
                refusal: isContextLengthRefusal(null, code, message)
                  ? "context_length"
                  : undefined,
              },
            );
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
          { retryable: !chatgpt },
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
      const continuation: ProviderContinuation | undefined =
        scope && reasoningItems > 0 && !continuationTooLarge
          ? {
              scope,
              output: [...kept.entries()]
                .sort(([a], [b]) => a - b)
                .map(([, entry]) => entry),
            }
          : undefined;
      yield { type: "done", usage, ...(continuation ? { continuation } : {}) };
    } catch (e) {
      if (
        this.cfg.chatgpt &&
        !(e instanceof ModelError) &&
        !request.signal?.aborted
      ) {
        throw new ModelError(
          "ChatGPT stream interrupted; acceptance is unknown",
          { retryable: false },
        );
      }
      throw e;
    } finally {
      deadline.done();
      await events?.return(undefined).catch(() => {});
    }
  }
}

/** Provider diagnostics can contain account links or echoed credentials. */
function streamErrorMessage(message: unknown, apiKey: string): string {
  if (typeof message !== "string" || !message) return "stream error";
  return (apiKey ? message.replaceAll(apiKey, "[redacted key]") : message)
    .replace(/https?:\/\/\S+/gi, "[redacted URL]")
    .replace(/\bsk-[\w-]+/g, "[redacted key]")
    .replace(/\bBearer\s+\S+/gi, "Bearer [redacted]")
    .slice(0, 1024);
}

/**
 * Convert the journal's message list into Responses `instructions` +
 * `input` items. Assistant tool calls replay as `function_call` items
 * carrying the recorded call_id; tool results feed back as
 * `function_call_output` keyed by that same call_id. A call_id only has
 * to be non-empty and unique within the input: a repeated or empty one is
 * renamed (disambiguateCallIds), except the ids a replayed continuation
 * references, which stay verbatim — as do the continuation's own item ids.
 */
function toInput(
  messages: ChatMessage[],
  toWire: Map<string, string>,
  dialect: "standard" | "chatgpt" | "chatgpt-lite" = "standard",
  /** Continuation scope to replay; unset = replay none. */
  replayScope?: string,
  omitReasoning = false,
  configuration?: Pick<
    ModelRequest,
    "reasoningEffort" | "reasoningEffortAfter"
  >,
): {
  instructions?: string;
  input: Record<string, unknown>[];
  /** Whether any recorded continuation was placed in `input`. */
  replaying: boolean;
} {
  let replaying = false;
  const system: string[] = [];
  const input: Record<string, unknown>[] = [];
  const appendConfiguration = (index: number) => {
    if (
      configuration?.reasoningEffort &&
      configuration.reasoningEffortAfter === index
    ) {
      input.push({
        type: "configuration_update",
        reasoning: { effort: configuration.reasoningEffort },
      });
    }
  };
  // A round's recorded continuation is replayed when its scope matches;
  // only then do its opaque items reference its call ids.
  const replays = (m: ChatMessage) =>
    dialect !== "standard" &&
    replayScope !== undefined &&
    m.continuation?.scope === replayScope;
  for (const [index, m] of disambiguateCallIds(
    messages,
    (id) => id !== "",
    (m) => !omitReasoning && replays(m),
  ).entries()) {
    appendConfiguration(index);
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
        let textDone = !m.content;
        const pushText = (part?: string) => {
          if (part === undefined && textDone) return;
          textDone = true;
          const text = part ?? m.content;
          if (!text) return;
          // The easy input-message form: role "assistant" is documented as
          // "presumed to have been generated by the model in previous
          // interactions" and needs no item id — unlike the output-message
          // form, where id is a required field the journal never stored.
          // The Codex backend receives the content-part form its own
          // client replays.
          input.push({
            type: "message",
            role: "assistant",
            content:
              dialect === "standard" ? text : [{ type: "output_text", text }],
          });
        };
        const pending = new Map((m.toolCalls ?? []).map((c) => [c.id, c]));
        const pushCall = (c: ToolCall, id?: string) => {
          pending.delete(c.id);
          // call_id links the result back. The item `id` is optional on a
          // function_call input item; it is sent only when the round's
          // continuation recorded it.
          input.push({
            type: "function_call",
            ...(id ? { id } : {}),
            call_id: c.id,
            // The recorded name is canonical; replay must still produce a
            // valid wire name when the tool is no longer advertised in
            // this request, so the deterministic transform is the
            // fallback.
            name: toWire.get(c.name) ?? sanitizeToolName(c.name),
            ...(dialect === "chatgpt-lite"
              ? { namespace: FUNCTION_NAMESPACE }
              : {}),
            arguments: encodeCallArguments(c),
            ...(dialect === "standard" ? { status: "completed" } : {}),
          });
        };
        // A round's recorded continuation replays in the round's own
        // output order: its encrypted reasoning items byte-for-byte, the
        // text and calls at the positions the provider emitted them.
        const cont = replays(m) ? m.continuation!.output : [];
        for (const o of cont) {
          const entry = continuationEntry(o);
          if (entry?.type === "reasoning") {
            if (!omitReasoning) {
              input.push(entry);
              replaying = true;
            }
          } else if (entry?.type === "message") {
            pushText(typeof entry.text === "string" ? entry.text : undefined);
          } else if (entry?.type === "function_call") {
            const c = pending.get(String(entry.call_id));
            if (c)
              pushCall(
                c,
                omitReasoning ? undefined : (entry.id as string | undefined),
              );
          }
        }
        pushText();
        for (const c of pending.values()) pushCall(c);
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
  appendConfiguration(messages.length);
  return {
    ...(system.length ? { instructions: system.join("\n\n") } : {}),
    input,
    replaying,
  };
}

/** The standard wire's instructions + input (no continuation exists there). */
function standardInput(
  messages: ChatMessage[],
  toWire: Map<string, string>,
  configuration?: Pick<
    ModelRequest,
    "reasoningEffort" | "reasoningEffortAfter"
  >,
): { instructions?: string; input: Record<string, unknown>[] } {
  const { replaying: _, ...rest } = toInput(
    messages,
    toWire,
    "standard",
    undefined,
    false,
    configuration,
  );
  return rest;
}

function numOr(v: unknown): number | undefined {
  return typeof v === "number" && Number.isFinite(v) && v > 0 ? v : undefined;
}

type WireTool = ReturnType<typeof wireTools>[number];

function functionTool(t: WireTool): Record<string, unknown> {
  return {
    type: "function",
    name: t.wire,
    description: t.spec.description,
    parameters: t.parameters,
    strict: false,
  };
}

/**
 * The Codex-backend request body, in the shape the Codex client sends:
 * store:false + stream:true, tool_choice auto, no output bound, the
 * persona as prompt_cache_key. "Lite" models carry tools and instructions
 * as ordered input items (see litePrefix) instead of `tools` /
 * `instructions`, and take no parallel tool calls.
 */
async function chatGPTBody(
  model: string,
  dialect: ChatGPTDialect,
  request: ModelRequest,
  tools: WireTool[],
  toWire: Map<string, string>,
  replayScope?: string,
  omitReasoning = false,
): Promise<{ body: string; replaying: boolean }> {
  const lite = usesResponsesLite(model);
  const { instructions, input, replaying } = toInput(
    request.messages,
    toWire,
    lite ? "chatgpt-lite" : "chatgpt",
    replayScope,
    omitReasoning,
    request,
  );
  const functions = tools.map((t) => functionTool(t));
  // The Codex client's reasoning parameters: the requested effort, and on
  // lite models `context: "all_turns"` so reasoning items in the input are
  // used rather than only the current turn's.
  const reasoning = {
    ...(dialect.reasoningEffort ? { effort: dialect.reasoningEffort } : {}),
    ...(lite ? { context: "all_turns" } : {}),
  };
  const body = JSON.stringify({
    model,
    ...(lite ? {} : instructions ? { instructions } : {}),
    input: lite
      ? [
          ...(await litePrefix(request.personaId, instructions, functions)),
          ...input,
        ]
      : input,
    ...(!lite && functions.length ? { tools: functions } : {}),
    tool_choice: "auto",
    parallel_tool_calls: !lite,
    ...(Object.keys(reasoning).length ? { reasoning } : {}),
    store: false,
    stream: true,
    // With store:false the provider keeps nothing, so reasoning can only
    // continue into the next round when its encrypted form is returned
    // and resent (see ProviderContinuation).
    include: ["reasoning.encrypted_content"],
    prompt_cache_key: request.personaId,
  });
  return { body, replaying };
}
