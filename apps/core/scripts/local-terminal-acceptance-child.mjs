// Invoked by Go's Local terminal integration test against its real HTTP state service.
// The provider is deterministic; all Core/state/terminal/PTY operations are real.
import { Secretary } from '../src/secretary.ts';
import { HttpStateClient } from '../src/state-client.ts';
const call = JSON.parse(process.env.TERMINAL_TEST_CALL ?? 'null');
let issued = false;
const provider = {
  name: 'terminal-acceptance',
  async *stream(req) {
    console.log(JSON.stringify({ tools: req.tools, messages: req.messages }));
    if (call && !issued) {
      issued = true;
      if (!req.tools.some((t) => t.name === call.tool)) throw new Error('tool not advertised');
      yield { type: 'tool_call', call: { id: 'terminal-call', name: call.tool, route: 'normal', arguments: call.request } };
    } else yield { type: 'text', delta: 'Recorded.' };
    yield { type: 'done', usage: {} };
  },
};
const secretary = new Secretary({
  personaId: process.env.TERMINAL_TEST_PERSONA, holderId: 'terminal-acceptance',
  state: new HttpStateClient(process.env.TERMINAL_TEST_URL, process.env.TERMINAL_TEST_TOKEN), provider,
  leaseTtlMs: 30000, renewEveryMs: 5000, contextLimit: 100,
  pollIntervalMs: 50, scheduleEveryMs: 1000000, idgen: () => crypto.randomUUID(),
});
await secretary.start();
try { for (let i = 0; i < 12; i++) { if (await secretary.step() !== 'turn') break; } }
finally { await secretary.stop(); }
