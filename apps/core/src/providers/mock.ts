import type { ModelEvent, ModelProvider, ModelRequest } from "../provider.ts";

/**
 * MOCK PROVIDER — deterministic, no network. It exists for test and dev
 * coverage only; it is not a stand-in for a real model acceptance run.
 *
 * Directives parsed from the input text (`payload.text` of the latest input):
 *   "!<tool> <json-args>"  → one normal-route tool call, e.g.
 *     !journal.note {"text":"remember x"}
 *     !schedule.set {"wake_at":"+500", "payload":{"note":"ping"}}
 *       ("+N" means N milliseconds from now)
 *   "!elevated <tool> <json-args>" → the same call on the elevated route
 *     (the model asking the human for a one-shot approval, ADR 0013 §1)
 *   "!slow <ms> <text>"    → delay before answering (used to kill mid-turn)
 *   "!pad <bytes> <text>"  → pad the reply to <bytes> bytes (used to cross
 *     the durable plan-request size boundary in tests). A re-plan consult
 *     after a decision-size notice sees the notice, not the directive, so
 *     !pad exercises the recovery path.
 *   "!alwayspad <bytes>"   → like !pad but scans every visible user message,
 *     so a re-plan still emits the oversized reply — the deterministic
 *     terminal-failure path.
 *   anything else          → echo reply text
 *
 * Round-aware like a real model: directives are parsed on round 0 only.
 * Later rounds (after tool results were fed back) emit a plain echo reply
 * — the committed reply post-dates the tool effects, matching the real
 * conversational contract.
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
    const toolCalls: {
      name: string;
      route: "normal" | "elevated";
      args: Record<string, unknown>;
    }[] = [];

    const slow =
      request.round === 0 ? /^!slow\s+(\d+)\s*(.*)$/s.exec(text) : null;
    const pad =
      request.round === 0 ? /^!pad\s+(\d+)\s*(.*)$/s.exec(text) : null;
    // !alwayspad looks past the latest user message: a re-plan consult after
    // a decision-size notice still finds the directive and re-emits the
    // oversized reply, so the turn reaches its terminal-failure bound.
    let alwayspad: RegExpExecArray | null = null;
    if (request.round === 0) {
      for (const m of request.messages) {
        if (m.role !== "user") continue;
        const t = m.content.replace(/^\[[^\]]*\]\s*/, "");
        alwayspad = /^!alwayspad\s+(\d+)\s*(.*)$/s.exec(t);
        if (alwayspad) break;
      }
    }
    if (alwayspad) {
      const size = Number(alwayspad[1]);
      reply = `echo: ${alwayspad[2] ?? ""}`;
      if (reply.length < size) reply = reply.padEnd(size, "x");
    } else if (pad) {
      const size = Number(pad[1]);
      reply = `echo: ${pad[2] ?? ""}`;
      if (reply.length < size) reply = reply.padEnd(size, "x");
    } else if (slow) {
      const delay = Number(slow[1]);
      await waitWithSignal(delay, request.signal);
      reply = `echo: ${slow[2] ?? ""}`;
    } else if (request.round === 0 && text.startsWith("!")) {
      const m = /^!(elevated\s+)?(\S+)\s+(.+)$/s.exec(text);
      if (m) {
        const route = m[1] ? "elevated" : "normal";
        const name = m[2] ?? "";
        const raw = m[3];
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
          toolCalls.push({ name, route, args });
          reply = `tool:${route}:${name}`;
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
          route: call.route,
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
