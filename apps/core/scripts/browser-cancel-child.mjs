// One real Secretary tool call against the Go cancellation fixture.
import { Secretary } from "../src/secretary.ts";
import { HttpStateClient } from "../src/state-client.ts";

let issued = false;
const secretary = new Secretary({
  personaId: process.env.BROWSER_TEST_PERSONA,
  holderId: `browser-cancel-${process.pid}`,
  state: new HttpStateClient(
    process.env.BROWSER_TEST_URL,
    process.env.BROWSER_TEST_TOKEN,
  ),
  provider: {
    name: "browser-cancel-acceptance",
    async *stream(req) {
      for (const message of req.messages.filter((m) => m.role === "tool")) {
        console.log(
          JSON.stringify({
            tool: message.name,
            result: JSON.parse(message.content),
          }),
        );
      }
      if (!issued) {
        issued = true;
        yield {
          type: "tool_call",
          call: {
            id: "cancel-browser-job",
            name: "job.cancel",
            route: "normal",
            arguments: { job_id: process.env.BROWSER_TEST_JOB },
          },
        };
      } else yield { type: "text", delta: "Cancellation outcome recorded." };
      yield { type: "done", usage: {} };
    },
  },
  leaseTtlMs: 30_000,
  renewEveryMs: 5_000,
  contextLimit: 100,
  pollIntervalMs: 50,
  scheduleEveryMs: 1_000_000,
  idgen: () => crypto.randomUUID(),
});
await secretary.start();
try {
  for (let i = 0; i < 8; i++) {
    if ((await secretary.step()) !== "turn") break;
  }
} finally {
  await secretary.stop();
}
