import type { ModelEvent, ModelProvider, ModelRequest } from "../provider.ts";

/**
 * MOCK PROVIDER — deterministic, no network. It exists for test and dev
 * coverage only; it is not a stand-in for a real model acceptance run.
 *
 * Directives parsed from the input text (`payload.text` of the latest input):
 *   "!<tool> <json-args>"  → one tool call, e.g.
 *     !journal.note {"text":"remember x"}
 *     !schedule.set {"wake_at":"+500", "payload":{"note":"ping"}}
 *       ("+N" means N milliseconds from now)
 *   "!slow <ms> <text>"    → delay before answering (used to kill mid-turn)
 *   anything else          → echo reply text
 */
export class MockProvider implements ModelProvider {
  readonly name = "mock";
  private readonly now: () => Date;

  constructor(now: () => Date = () => new Date()) {
    this.now = now;
  }

  async *stream(request: ModelRequest): AsyncIterable<ModelEvent> {
    // Context assembly prefixes "[actor]" markers; directives come first after
    // the marker so `!tool` still parses.
    const text = lastUserText(request).replace(/^\[[^\]]*\]\s*/, "");
    let reply = `echo: ${text}`;
    const toolCalls: { name: string; args: Record<string, unknown> }[] = [];

    const slow = /^!slow\s+(\d+)\s*(.*)$/s.exec(text);
    if (slow) {
      const delay = Number(slow[1]);
      await waitWithSignal(delay, request.signal);
      reply = `echo: ${slow[2] ?? ""}`;
    } else if (text.startsWith("!")) {
      const m = /^!(\S+)\s+(.+)$/s.exec(text);
      if (m) {
        const name = m[1] ?? "";
        const raw = m[2];
        try {
          const args = JSON.parse(raw ?? "{}") as Record<string, unknown>;
          if (
            name === "schedule.set" &&
            typeof args.wake_at === "string" &&
            args.wake_at.startsWith("+")
          ) {
            args.wake_at = new Date(
              this.now().getTime() + Number(args.wake_at.slice(1)),
            ).toISOString();
          }
          if (!("schedule_id" in args) && name === "schedule.set") {
            args.schedule_id = `sched-${request.turnId}`;
          }
          toolCalls.push({ name, args });
          reply = `tool:${name}`;
        } catch {
          reply = `echo: unparseable tool args`;
        }
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
          arguments: call.args,
        },
      };
    }
    yield { type: "done", usage: { mock: true, input_chars: text.length } };
  }
}

function lastUserText(request: ModelRequest): string {
  for (let i = request.messages.length - 1; i >= 0; i--) {
    const m = request.messages[i];
    if (m && m.role === "user") return m.content;
  }
  return "";
}

function waitWithSignal(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => resolve(), ms);
    signal?.addEventListener("abort", () => {
      clearTimeout(t);
      reject(new Error("aborted"));
    });
  });
}
