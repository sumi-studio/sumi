#!/usr/bin/env node
/**
 * Minimal OpenAI-compatible chat-completions stub for the local-host
 * fixture. It implements just enough of POST {base}/chat/completions with
 * stream:true to exercise the real OpenAIProvider network path — SSE
 * deltas, finish_reason, usage, [DONE] — without any external call.
 *
 * Replies "stub(<model>): <last user text>". Any Authorization bearer is
 * accepted and logged only as a presence flag, never printed.
 *
 *   STUB_PORT=9551 node stub-model.mjs
 */
import { createServer } from "node:http";

const port = Number(process.env.STUB_PORT ?? 9551);
const host = process.env.STUB_HOST ?? "127.0.0.1";

function sse(res, obj) {
  res.write(`data: ${JSON.stringify(obj)}\n\n`);
}

const server = createServer((req, res) => {
  if (req.method === "POST" && req.url === "/v1/chat/completions") {
    let body = "";
    req.on("data", (d) => (body += d));
    req.on("end", () => {
      const parsed = JSON.parse(body);
      const lastUser = [...(parsed.messages ?? [])]
        .reverse()
        .find((m) => m.role === "user");
      const text = `stub(${parsed.model ?? "?"}): ${lastUser?.content ?? ""}`;
      res.writeHead(200, {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        Connection: "keep-alive",
      });
      sse(res, {
        choices: [{ delta: { role: "assistant", content: "" }, index: 0 }],
      });
      sse(res, { choices: [{ delta: { content: text }, index: 0 }] });
      sse(res, {
        choices: [{ delta: {}, finish_reason: "stop", index: 0 }],
        usage: { prompt_tokens: 10, completion_tokens: 4, total_tokens: 14 },
      });
      res.write("data: [DONE]\n\n");
      res.end();
    });
    return;
  }
  res.writeHead(404).end("not found");
});

server.listen(port, host, () => {
  console.log(`stub-model listening on http://${host}:${port}/v1`);
});
