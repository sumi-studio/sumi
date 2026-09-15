import { readFileSync } from "node:fs";
import type { ModelEvent, ModelProvider, ModelRequest } from "../provider.ts";

/**
 * FIXTURE PROVIDER — scripted model for integration tests, no network.
 * Selected by SUMI_MODEL_PROVIDER=fixture; the script comes from
 * SUMI_MODEL_FIXTURE=<path>, a JSON file:
 *
 *   {"rules": [
 *     {"when": {"kind": "call_started"},
 *      "do": [{"tool": "call.join", "args": {"place_id": "$place_id"}}]},
 *     {"when": {"kind": "call_utterance", "text_regex": "leave"},
 *      "do": [{"tool": "call.leave", "args": {"session_id": "$session_id"}}]},
 *     {"when": {"kind": "call_utterance"},
 *      "do": [{"tool": "call.say",
 *              "args": {"session_id": "$session_id",
 *                       "text": "fixture reply"}}]}
 *   ]}
 *
 * - `when.kind` matches the current input's `payload.kind` (falling back to
 *   `payload.event`); `when.text_regex` optionally matches `payload.text`.
 * - `do` emits tool calls on the normal route; an arg value of "$field"
 *   resolves to that field of the input payload. An empty `do` means the
 *   scripted secretary chooses silence — a legitimate participation outcome.
 * - First matching rule wins; no rule matches → plain echo reply.
 * - Round-aware like a real model: rules are evaluated on round 0 only.
 *   Later rounds (after tool results) emit a plain echo reply — the
 *   committed reply post-dates the tool effects.
 *
 * The script is data supplied by the test; no fixture phrase lives in call
 * logic. This provider is not a stand-in for a real model acceptance run.
 */

interface FixtureRule {
  when: { kind?: string; text_regex?: string };
  do?: { tool: string; args?: Record<string, unknown> }[];
}

export class FixtureProvider implements ModelProvider {
  readonly name = "fixture";
  private readonly rules: { kind?: string; textRe?: RegExp; calls: { tool: string; args: Record<string, unknown> }[] }[];

  constructor(scriptPath: string) {
    const parsed = JSON.parse(readFileSync(scriptPath, "utf8")) as {
      rules?: FixtureRule[];
    };
    this.rules = (parsed.rules ?? []).map((r) => ({
      kind: r.when.kind,
      textRe: r.when.text_regex ? new RegExp(r.when.text_regex, "i") : undefined,
      calls: (r.do ?? []).map((d) => ({ tool: d.tool, args: d.args ?? {} })),
    }));
  }

  async *stream(request: ModelRequest): AsyncIterable<ModelEvent> {
    const text = lastUserText(request).replace(/^\[[^\]]*\]\s*/, "");
    let reply = `echo: ${text.slice(0, 120)}`;
    const toolCalls: { name: string; args: Record<string, unknown> }[] = [];

    if (request.round === 0) {
      const payload = tryParsePayload(text);
      const rule = this.rules.find((r) => {
        const kind = payload?.kind ?? payload?.event;
        if (r.kind && kind !== r.kind) return false;
        if (r.textRe && !r.textRe.test(String(payload?.text ?? text))) {
          return false;
        }
        return true;
      });
      if (rule && payload) {
        for (const call of rule.calls) {
          const args: Record<string, unknown> = {};
          for (const [k, v] of Object.entries(call.args)) {
            args[k] =
              typeof v === "string" && v.startsWith("$")
                ? payload[v.slice(1)]
                : v;
          }
          toolCalls.push({ name: call.tool, args });
        }
        reply =
          rule.calls.length === 0
            ? "echo: (silent)"
            : `tool:${rule.calls.map((c) => c.tool).join(",")}`;
      }
    }

    for (const ch of reply) {
      if (request.signal?.aborted) return;
      yield { type: "text", delta: ch };
    }
    for (const call of toolCalls) {
      yield {
        type: "tool_call",
        call: {
          id: `call-${call.name}-0`,
          name: call.name,
          route: "normal",
          arguments: call.args,
        },
      };
    }
    yield { type: "done", usage: { fixture: true, input_chars: text.length } };
  }
}

function lastUserText(request: ModelRequest): string {
  for (let i = request.messages.length - 1; i >= 0; i--) {
    const m = request.messages[i];
    if (m && m.role === "user") return m.content;
  }
  return "";
}

function tryParsePayload(text: string): Record<string, unknown> | null {
  const trimmed = text.trim();
  if (!trimmed.startsWith("{")) return null;
  try {
    const parsed = JSON.parse(trimmed) as unknown;
    return typeof parsed === "object" && parsed !== null
      ? (parsed as Record<string, unknown>)
      : null;
  } catch {
    return null;
  }
}
