// Loopback test double of TypeSafe's documented `POST /v1/systemone`.
// It checks the request against the documented contract (typed questions,
// choice criteria of 2–255 options, Bearer auth) and answers through a
// test-supplied policy. It is NOT Jev: acceptance with it proves Sumi's
// adapter, loop and safety behavior, not Jev's judgment quality.
import assert from "node:assert/strict";
import { createServer } from "node:http";

export const FIXTURE_KEY = "sumi-jev-20260927-fixture-key";

function validate(body) {
  assert.deepEqual(Object.keys(body).sort(), ["model", "questions", "state"]);
  assert.equal(typeof body.model, "string");
  assert.ok(body.state !== undefined);
  for (const [id, q] of Object.entries(body.questions)) {
    assert.ok(["choice", "noul", "score"].includes(q.type), id);
    assert.ok(["string", "object"].includes(typeof q.instructions), id);
    if (q.type === "choice") {
      const n = Object.keys(q.criteria).length;
      assert.ok(n >= 2 && n <= 255, `${id} has ${n} options`);
    }
  }
}

/** A well-formed choice answer: the chosen option carries the largest
 * probability (at least `confidence`), the rest share the remainder. */
export function choice(options, chosen, confidence = 0.9) {
  const rest = options.filter((o) => o !== chosen);
  const top = Math.max(confidence, 1 / options.length);
  const probabilities = Object.fromEntries(
    options.map((o) => [
      o,
      o === chosen ? top : (1 - top) / Math.max(rest.length, 1),
    ]),
  );
  return { type: "choice", choice: chosen, probabilities, confidence };
}

/** Answers every question a policy left out: the first option of a choice,
 * and for the completion noul, high only when the operation is DONE. */
export function fillDefaults(questions, answers) {
  for (const [id, q] of Object.entries(questions)) {
    if (answers[id]) continue;
    if (q.type === "noul")
      answers[id] = {
        type: "noul",
        noul: answers.operation?.choice === "DONE" ? 0.95 : 0.05,
      };
    else {
      const options = Object.keys(q.criteria);
      answers[id] = choice(options, options[0], 0.5);
    }
  }
  return answers;
}

/** Rule-based stand-in used by the Electron journeys: fill the first field
 * whose label names an input, then press a Save/Search/Submit control, then
 * report DONE when `doneText` is visible. */
export function formPolicy({ doneText }) {
  return (body) => {
    const q = body.questions;
    const answers = {};
    const ops = Object.keys(q.operation.criteria);
    let op = "BLOCKED";
    const text = body.state.page.text;
    const pairs = q.fill_pair ? Object.keys(q.fill_pair.criteria) : [];
    const pairFor = (key) => {
      const [, input] = key.split("=");
      return q.fill_pair
        ? q.fill_pair.criteria[key]
            .toLowerCase()
            .includes(`"${input.split("_")[0]}`)
        : false;
    };
    const clickNames = q.click_target
      ? Object.entries(q.click_target.criteria)
      : [];
    const button = clickNames.find(([, d]) => /save|search|submit/i.test(d));
    if (text.includes(doneText)) op = "DONE";
    else if (pairs.some(pairFor) && ops.includes("FILL")) {
      op = "FILL";
      answers.fill_pair = choice(pairs, pairs.find(pairFor));
    } else if (button && ops.includes("CLICK")) {
      op = "CLICK";
      answers.click_target = choice(
        clickNames.map(([k]) => k),
        button[0],
      );
    } else if (ops.includes("CLICK") && !q.click_target) op = "CLICK";
    answers.operation = choice(ops, op);
    return fillDefaults(q, answers);
  };
}

/** Starts the double. `fault()` may return {status, headers, body} to fail
 * a request (401/422/429/529…); otherwise `policy(body)` supplies answers. */
export async function startJevFixture({
  port = 0,
  policy,
  fault = () => undefined,
  key = FIXTURE_KEY,
}) {
  const requests = [];
  const server = createServer(async (req, res) => {
    let raw = "";
    for await (const chunk of req) raw += chunk;
    const reply = (status, body, headers = {}) => {
      res.writeHead(status, { "Content-Type": "application/json", ...headers });
      res.end(JSON.stringify(body));
    };
    try {
      assert.equal(req.method, "POST");
      assert.equal(req.url, "/v1/systemone");
      assert.equal(req.headers["content-type"], "application/json");
      if (req.headers.authorization !== `Bearer ${key}`)
        return reply(401, { error: "invalid api key" });
      const body = JSON.parse(raw);
      validate(body);
      requests.push({ body, raw });
      const failure = fault(body, requests.length);
      if (failure)
        return reply(failure.status, failure.body ?? {}, failure.headers);
      reply(200, {
        model: "jev-fixture-double",
        answers: fillDefaults(
          body.questions,
          await policy(body, requests.length),
        ),
        usage: { input_tokens: raw.length >> 2, output_tokens: 8 },
      });
    } catch (error) {
      reply(422, { detail: String(error.message).slice(0, 200) });
    }
  });
  await new Promise((resolve) => server.listen(port, "127.0.0.1", resolve));
  return {
    endpoint: `http://127.0.0.1:${server.address().port}`,
    requests,
    close: () => {
      server.closeAllConnections();
      server.close();
    },
  };
}
