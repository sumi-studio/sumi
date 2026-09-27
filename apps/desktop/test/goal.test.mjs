import assert from "node:assert/strict";
import test from "node:test";
import {
  buildDecision,
  parseGoalRequest,
  privateRedactor,
  runGoal,
} from "../dist/browser/goal.js";
import { JevClient, JevError } from "../dist/browser/jev.js";
import {
  choice,
  FIXTURE_KEY,
  formPolicy,
  startJevFixture,
} from "./jev-fixture.mjs";

const tab = { runtimeId: "r", profileId: "p", tabId: "t" };

/** In-memory page with an Email field, a Password field and a Save button. */
class FakePage {
  constructor() {
    this.email = "";
    this.password = "";
    this.saved = "";
    this.revision = 0;
    this.observations = 0;
    this.acts = [];
    this.refuseNext = [];
  }
  async observe() {
    this.observations++;
    return {
      tab,
      binding: {
        revision: this.revision,
        observationId: `o${this.observations}`,
        url: "http://fixture.test/",
      },
      title: "Fixture form",
      text: this.saved ? `Saved ${this.saved}` : "Please sign up",
      truncated: false,
      targets: [
        {
          id: "t0",
          tag: "input",
          type: "email",
          role: "",
          name: "Email",
          value: this.email,
          bounds: {},
        },
        {
          id: "t1",
          tag: "input",
          type: "password",
          role: "",
          name: "Password",
          bounds: {},
        },
        { id: "t2", tag: "button", role: "", name: "Save", bounds: {} },
      ],
    };
  }
  async act(_tab, binding, action, options) {
    assert.equal(options?.guard, true, "goal actions are guarded");
    assert.equal(binding.observationId, `o${this.observations}`);
    const refusal = this.refuseNext.shift();
    if (refusal) throw Object.assign(new Error(refusal), { code: refusal });
    this.acts.push(action);
    if (action.kind === "fill" && action.target === "t0")
      this.email = action.text;
    if (action.kind === "fill" && action.target === "t1")
      this.password = action.text;
    if (action.kind === "click" && action.target === "t2" && this.email)
      this.saved = this.email;
    return { tab, status: "dispatched", revision: this.revision };
  }
}

const request = parseGoalRequest({
  goal: "Sign up with my email and password, then save.",
  inputs: { email: "ada@example.test", password: "s3cret-never-sent" },
  private_inputs: ["password"],
});

function session(answers = []) {
  const reports = [];
  return {
    reports,
    signal: new AbortController().signal,
    report: async (progress) => {
      reports.push(progress);
      return answers.shift() ?? "continue";
    },
  };
}

// Deterministic stand-in: fill email, fill password, click Save, then DONE.
function signupPolicy(body) {
  const q = body.questions;
  const ops = Object.keys(q.operation.criteria);
  const text = body.state.page.text;
  const pairs = q.fill_pair ? Object.keys(q.fill_pair.criteria) : [];
  const next =
    pairs.find((p) => p === "t0=email") ??
    pairs.find((p) => p === "t1=password");
  const answers = {};
  if (text.startsWith("Saved")) answers.operation = choice(ops, "DONE");
  else if (next) {
    answers.operation = choice(ops, "FILL");
    answers.fill_pair = choice(pairs, next);
  } else answers.operation = choice(ops, "CLICK");
  return answers;
}

async function jevFor(t, options) {
  const fixture = await startJevFixture(options);
  t.after(fixture.close);
  return {
    fixture,
    jev: new JevClient({
      apiKey: FIXTURE_KEY,
      endpoint: fixture.endpoint,
      maxRetries: 2,
    }),
  };
}

test("goal reaches DONE through Jev choices and supplied inputs only", async (t) => {
  const page = new FakePage();
  const { fixture, jev } = await jevFor(t, { policy: signupPolicy });
  let passwordFilled = false;
  const s = session();
  const receipt = await runGoal({
    browser: page,
    tab,
    jev,
    request,
    session: s,
  });
  assert.equal(receipt.status, "done", JSON.stringify(receipt));
  const value = receipt.result.value;
  assert.equal(value.operation_layer, "jev");
  assert.equal(value.goal_outcome, "jev_reported_done");
  assert.equal(value.actions_dispatched, 3);
  assert.equal(value.jev.calls, 4);
  assert.equal(value.jev.model, "jev-fixture-double");
  assert.equal(receipt.result.dispatched, true);
  assert.equal(receipt.result.outcome, "returned");
  passwordFilled = page.password === "s3cret-never-sent";
  assert.ok(passwordFilled);
  assert.equal(page.saved, "ada@example.test");
  // One admission per action, before it.
  assert.equal(s.reports.filter((r) => r.phase === "acting").length, 3);
  // The private value and the key never reach Jev's state/questions or results.
  for (const { raw } of fixture.requests) {
    assert.ok(!raw.includes("s3cret-never-sent"));
    assert.ok(raw.includes("ada@example.test"));
  }
  assert.ok(!JSON.stringify(receipt).includes(FIXTURE_KEY));
  assert.ok(!JSON.stringify(receipt).includes("s3cret-never-sent"));
  assert.ok(!JSON.stringify(jev).includes(FIXTURE_KEY));
});

test("a page changed by the person after an observation is re-observed, not acted on", async (t) => {
  const page = new FakePage();
  page.refuseNext = ["page_changed"];
  const { jev } = await jevFor(t, { policy: signupPolicy });
  const receipt = await runGoal({
    browser: page,
    tab,
    jev,
    request,
    session: session(),
  });
  assert.equal(receipt.result.value.goal_outcome, "jev_reported_done");
  assert.equal(receipt.result.value.steps[0].result, "refused:page_changed");
  assert.equal(page.acts.length, 3);
});

test("repeated page changes stop the goal for the person/secretary to take over", async (t) => {
  const page = new FakePage();
  page.refuseNext = ["page_changed", "target_unavailable", "page_changed"];
  const { jev } = await jevFor(t, { policy: signupPolicy });
  const receipt = await runGoal({
    browser: page,
    tab,
    jev,
    request,
    session: session(),
  });
  assert.equal(receipt.result.value.goal_outcome, "page_changed_repeatedly");
  assert.equal(receipt.result.dispatched, false);
  assert.equal(page.acts.length, 0);
});

test("cancellation, revocation and a lost claim stop before the next action", async (t) => {
  for (const [admission, status, outcome, code] of [
    ["cancel", "cancelled", "cancelled", undefined],
    ["revoked", "failed", "revoked", "grant_revoked"],
    ["lost", "failed", "claim_lost", "claim_lost"],
  ]) {
    const page = new FakePage();
    const { jev } = await jevFor(t, { policy: signupPolicy });
    const receipt = await runGoal({
      browser: page,
      tab,
      jev,
      request,
      session: session(["continue", admission]),
    });
    assert.equal(receipt.status, status);
    assert.equal(receipt.result.value.goal_outcome, outcome);
    assert.equal(receipt.result.code, code);
    assert.equal(
      page.acts.length,
      1,
      `${admission}: only the admitted action ran`,
    );
    // No observation after cancel/revoke/loss.
    assert.equal(page.observations, 2);
  }
});

test("Jev API errors are reported with codes, never replaced by another model", async (t) => {
  const cases = [
    [{ status: 401 }, "jev_auth_failed"],
    [{ status: 422, body: { detail: "bad question" } }, "jev_request_rejected"],
    [{ status: 529 }, "jev_overloaded"],
    [{ status: 429, headers: { "retry-after": "0" } }, "jev_rate_limited"],
  ];
  for (const [failure, code] of cases) {
    const page = new FakePage();
    const { fixture, jev } = await jevFor(t, {
      policy: signupPolicy,
      fault: () => failure,
    });
    const receipt = await runGoal({
      browser: page,
      tab,
      jev,
      request,
      session: session(),
    });
    assert.equal(receipt.status, "failed");
    assert.equal(receipt.result.code, code);
    assert.equal(receipt.result.value.goal_outcome, "jev_error");
    assert.equal(receipt.result.value.jev.calls, 0);
    assert.equal(receipt.result.dispatched, false);
    assert.equal(receipt.result.outcome, "not_dispatched");
    assert.equal(page.acts.length, 0);
    // 408/429/5xx are retried twice like the official SDKs; 401/422 are not.
    const retried = failure.status >= 429;
    assert.equal(fixture.requests.length, retried ? 3 : 1, code);
  }
});

test("a transient 429 is retried and the goal continues", async (t) => {
  const page = new FakePage();
  const { jev } = await jevFor(t, {
    policy: signupPolicy,
    fault: (_b, n) =>
      n === 1 ? { status: 429, headers: { "retry-after": "0" } } : undefined,
  });
  const receipt = await runGoal({
    browser: page,
    tab,
    jev,
    request,
    session: session(),
  });
  assert.equal(receipt.result.value.goal_outcome, "jev_reported_done");
});

test("an unoffered choice or low confidence executes nothing", async (t) => {
  const page = new FakePage();
  const { jev } = await jevFor(t, {
    policy: (body) => ({
      operation: {
        ...choice(Object.keys(body.questions.operation.criteria), "CLICK"),
        choice: "DELETE_ACCOUNT",
      },
    }),
  });
  const bad = await runGoal({
    browser: page,
    tab,
    jev,
    request,
    session: session(),
  });
  assert.equal(bad.result.code, "jev_invalid_response");
  assert.equal(page.acts.length, 0);

  const { jev: unsure } = await jevFor(t, {
    policy: (body) => {
      const answers = signupPolicy(body);
      answers.operation.confidence = 0.1;
      return answers;
    },
  });
  const low = await runGoal({
    browser: page,
    tab,
    jev: unsure,
    request,
    session: session(),
  });
  assert.equal(low.status, "done");
  assert.equal(low.result.value.goal_outcome, "uncertain");
  assert.equal(page.acts.length, 0);
});

test("without Jev the goal is refused as not configured and nothing runs", async () => {
  const page = new FakePage();
  const receipt = await runGoal({
    browser: page,
    tab,
    jev: undefined,
    request,
    session: session(),
  });
  assert.equal(receipt.result.code, "jev_not_configured");
  assert.equal(receipt.result.value.jev.calls, 0);
  assert.equal(page.observations, 0);
});

test("an action with unknown effect is never repeated", async (t) => {
  const page = new FakePage();
  page.act = async () => {
    throw Object.assign(new Error("timeout"), { code: "operation_timed_out" });
  };
  const { jev } = await jevFor(t, { policy: signupPolicy });
  const receipt = await runGoal({
    browser: page,
    tab,
    jev,
    request,
    session: session(),
  });
  assert.equal(receipt.status, "failed");
  assert.equal(receipt.result.outcome, "unknown");
  assert.equal(receipt.result.code, "operation_timed_out");
  assert.equal(receipt.result.value.steps.length, 1);
});

test("clicks that change nothing end as no_progress within budget", async (t) => {
  const page = new FakePage();
  page.email = "ada@example.test";
  page.password = "set";
  page.act = async (_t, _b, action) => {
    page.acts.push(action);
    return { tab, status: "dispatched", revision: 0 };
  };
  const { jev } = await jevFor(t, {
    policy: formPolicy({ doneText: "never" }),
  });
  const receipt = await runGoal({
    browser: page,
    tab,
    jev,
    request: { ...request, private_inputs: [] },
    session: session(),
  });
  assert.equal(receipt.result.value.goal_outcome, "no_progress");
  assert.ok(page.acts.length <= 4);
});

test("decision space: private values hidden, filled pairs removed, options bounded", async () => {
  const page = await new FakePage().observe();
  page.targets[0].value = "ada@example.test";
  const space = buildDecision(page, request, []);
  assert.equal(space.state.inputs.password, "(private value, not shown)");
  assert.ok(!("t0=email" in space.fill), "already filled field is not offered");
  assert.ok("t1=password" in space.fill);
  assert.match(space.state.controls, /already contains input `email`/);
  // Single candidates are resolved in code; the API requires two options.
  for (const q of Object.values(space.questions))
    if (q.type === "choice") assert.ok(Object.keys(q.criteria).length >= 2);
  assert.throws(
    () => parseGoalRequest({ goal: "x", inputs: { "Bad Name": "v" } }),
    {
      code: "invalid_request",
    },
  );
  assert.throws(() => parseGoalRequest({ goal: "x", max_steps: 41 }));
  assert.throws(
    () => new JevClient({ apiKey: "k", endpoint: "http://evil.example" }),
  );
  assert.ok(new JevError("jev_auth_failed", "m") instanceof Error);
});

/** Page that echoes whatever was typed: into its text, title, URL, a
 * read-only summary control and a button label (like many sites do). */
class EchoPage extends FakePage {
  async observe() {
    const o = await super.observe();
    const pw = this.password;
    if (!pw) return o;
    o.text = `Your password ${pw} was accepted. Upper: ${pw.toUpperCase()}`;
    o.title = `Welcome ${pw}`;
    o.binding.url = `http://fixture.test/?pw=${encodeURIComponent(pw)}&f=${new URLSearchParams({ v: pw }).toString().slice(2)}`;
    o.targets.push(
      {
        id: "t3",
        tag: "input",
        type: "text",
        role: "",
        name: "Summary",
        value: `pw=${pw}`,
        bounds: {},
      },
      {
        id: "t4",
        tag: "button",
        role: "",
        name: `Log in as ${pw}`,
        bounds: {},
      },
    );
    return o;
  }
}

test("private inputs echoed by the page never reach Jev, progress or results", async (t) => {
  const secret = "S3cret pass/word!";
  const req = parseGoalRequest({
    goal: "Sign up with my email and password, then save.",
    inputs: { email: "ada@example.test", password: secret, pin: "42" },
    private_inputs: ["password", "pin"],
  });
  const page = new EchoPage();
  let echoedClicked = false;
  const { fixture, jev } = await jevFor(t, {
    policy: (body) => {
      const answers = signupPolicy(body);
      const targets = Object.keys(body.questions.click_target?.criteria ?? {});
      // Once, click the button whose label echoes the private value, so the
      // admission progress names it as the next target.
      if (answers.operation.choice === "CLICK" && targets.includes("t4")) {
        answers.click_target = choice(targets, echoedClicked ? "t2" : "t4");
        echoedClicked = true;
      }
      return answers;
    },
  });
  const s = session();
  const receipt = await runGoal({
    browser: page,
    tab,
    jev,
    request: req,
    session: s,
  });
  assert.equal(page.password, secret, "the value itself was typed");
  assert.ok(
    s.reports.some((r) => r.next?.target === "t4 Log in as [private:password]"),
    "the echoed target was reported, scrubbed",
  );
  const forms = [
    secret,
    secret.toUpperCase(),
    encodeURIComponent(secret),
    new URLSearchParams({ v: secret }).toString().slice(2),
  ];
  const sent = fixture.requests.map((r) => r.raw).join("\n");
  const kept = JSON.stringify({ receipt, reports: s.reports });
  for (const form of forms) {
    assert.ok(!sent.includes(form), `Jev request contains ${form}`);
    assert.ok(!kept.includes(form), `record contains ${form}`);
  }
  assert.match(sent, /\[private:password\]/, "echo replaced, page text kept");
  assert.match(sent, /was accepted/);
  assert.ok(
    receipt.result.value.final_page.title.includes("[private:password]"),
  );
});

// Synthetic private values of the kinds the independent review found leaking:
// multi-line, quoted/backslashed, longer than a shown value, and a value
// straddling a truncation bound.
const PRIVATE = {
  address: "1 Quinzel Lane\nZephyrton 40417",
  passphrase: 'correct "horse" \\ zygomatic-battery',
  token: `tok_${"Q7x9Kp2Lm4".repeat(60)}`,
  member: "MEMBER-4411-2233-9988",
};
const FRAGMENTS = [
  "quinzel",
  "zephyrton",
  "horse",
  "zygomatic",
  "q7x9kp2lm4",
  "member-4",
];

/** A page echoing every private value at a place where the head at review
 * formatted or cut it before redaction, or where the page itself cut it. */
function echoPage({ truncated }) {
  const { address, passphrase, token, member } = PRIVATE;
  let text = "Saved. ";
  text += `${"q".repeat(1990 - text.length)} ${member} `; // straddles 2000
  text += `${address.replace("\n", " ")} ${JSON.stringify(passphrase)} `;
  text += `${"p".repeat(5991 - text.length)}${member} `; // straddles 6000
  const observed = {
    title: `Saved ${"T".repeat(500)} ${token}`.slice(0, 512),
    // The page's own 16 000-character cut ends inside the token.
    text: truncated ? `Saved. Reference ${token.slice(0, 40)}` : text,
    truncated,
    targets: [
      {
        id: "t0",
        tag: "textarea",
        role: "",
        name: "Address",
        value: address,
        bounds: {},
      },
      {
        id: "t1",
        tag: "input",
        type: "text",
        role: "",
        name: "Passphrase",
        value: passphrase,
        bounds: {},
      },
      // Page-capped at 512 characters: only the beginning of the token.
      {
        id: "t2",
        tag: "input",
        type: "text",
        role: "",
        name: "Token",
        value: token.slice(0, 512),
        bounds: {},
      },
      {
        id: "t3",
        tag: "input",
        type: "text",
        role: "",
        name: "Reference",
        value: `ref ${member}`,
        bounds: {},
      },
      // "t4 " + name is cut at 160 in steps/progress: inside the member.
      {
        id: "t4",
        tag: "button",
        role: "",
        name: `${"x".repeat(147)} ${member}`,
        bounds: {},
      },
      // A name the page cut at 256 inside the passphrase.
      {
        id: "t5",
        tag: "a",
        role: "",
        name: `${"y".repeat(245)} ${passphrase}`.slice(0, 256),
        bounds: {},
      },
      {
        id: "t6",
        tag: "select",
        role: "",
        name: "Deliver to",
        value: "home",
        options: [
          { value: "home", label: `Home: ${address}` },
          { value: "work", label: "Work" },
        ],
        bounds: {},
      },
    ],
  };
  let n = 0;
  return {
    async observe() {
      n++;
      return {
        tab,
        binding: {
          revision: 0,
          observationId: `o${n}`,
          url: `http://fixture.test/?${new URLSearchParams({ a: address, p: passphrase })}`,
        },
        ...structuredClone(observed),
      };
    },
    async act() {
      return { tab, status: "dispatched", revision: 0 };
    },
  };
}

test("private values are protected before escaping, shortening and at every truncation bound", async (t) => {
  for (const truncated of [false, true]) {
    const req = parseGoalRequest({
      goal: "Check my delivery details are saved.",
      inputs: PRIVATE,
      private_inputs: Object.keys(PRIVATE),
    });
    let call = 0;
    const { fixture, jev } = await jevFor(t, {
      policy: (body) => {
        const ops = Object.keys(body.questions.operation.criteria);
        const targets = Object.keys(body.questions.click_target.criteria);
        return ++call === 1
          ? {
              operation: choice(ops, "CLICK"),
              click_target: choice(targets, "t4"),
            }
          : {
              operation: choice(ops, "DONE"),
              goal_complete: { type: "noul", noul: 0.95 },
            };
      },
    });
    const s = session();
    const receipt = await runGoal({
      browser: echoPage({ truncated }),
      tab,
      jev,
      request: req,
      session: s,
    });
    assert.equal(receipt.result.value.goal_outcome, "jev_reported_done");
    const sent = fixture.requests.map((r) => r.raw);
    assert.equal(sent.length, 2);
    const kept = JSON.stringify({ receipt, reports: s.reports });
    const label = truncated ? "page cut by the host" : "full page";
    for (const [where, text] of [
      ...sent.map((r, i) => [`Jev request ${i + 1}`, r]),
      ["progress and result", kept],
    ]) {
      for (const [name, value] of Object.entries(PRIVATE))
        for (const form of [
          value,
          JSON.stringify(value).slice(1, -1),
          encodeURIComponent(value),
          new URLSearchParams({ v: value }).toString().slice(2),
        ])
          assert.ok(!text.includes(form), `${label}: ${where} has ${name}`);
      for (const fragment of FRAGMENTS)
        assert.ok(
          !text.toLowerCase().includes(fragment),
          `${label}: ${where} has fragment ${fragment}`,
        );
    }
    // What is not private stays visible to Jev.
    const state = JSON.parse(sent[0]).state;
    assert.match(state.page.text, /^Saved\. /);
    assert.match(
      state.controls,
      /\[t0\] textarea "Address" · value "\[private:address\]" · already contains input `address`/,
    );
    assert.match(
      state.controls,
      /\[t2\] input\(text\) "Token" · value "\[private:token\]"/,
    );
    assert.match(
      state.controls,
      /\[t3\] input\(text\) "Reference" · value "\[private:member\]"/,
    );
    assert.match(state.controls, /selected "Home: \[private:address\]"/);
    assert.ok(
      s.reports.some((r) =>
        r.next?.target?.startsWith(`t4 ${"x".repeat(147)} [priv`),
      ),
      "the next target is reported with the value protected",
    );
  }
});

test("capped private echoes with a public prefix stay out of decisions and saved observations", async (t) => {
  const token = `tok_${"Q7w8E9r0T1".repeat(60)}`;
  const req = parseGoalRequest({
    goal: "Check my token is saved.",
    inputs: { token },
    private_inputs: ["token"],
  });
  for (const [name, echo] of [
    ["control value", { value: `ref ${token}`.slice(0, 512) }],
    ["role attribute", { role: `ref ${token}`.slice(0, 100) }],
  ]) {
    await t.test(name, () => {
      const page = {
        tab,
        binding: {
          revision: 0,
          observationId: "o",
          url: "http://fixture.test/",
        },
        title: "Token",
        text: "Saved",
        truncated: false,
        targets: [
          {
            id: "t0",
            tag: "input",
            type: "text",
            role: "",
            name: "Reference",
            bounds: {},
            ...echo,
          },
          { id: "t1", tag: "button", role: "", name: "Save", bounds: {} },
          { id: "t2", tag: "button", role: "", name: "Cancel", bounds: {} },
        ],
      };
      const decision = buildDecision(page, req, []);
      const wire = JSON.stringify({
        state: decision.state,
        questions: decision.questions,
      });
      const saved = JSON.stringify(privateRedactor(req).page(page));
      for (const output of [wire, saved]) {
        assert.ok(
          !output.includes(token.slice(0, 40)),
          "no private prefix leaves the host",
        );
        assert.ok(output.includes("[private:token]"));
      }
      assert.equal(
        req.inputs.token,
        token,
        "the value available for filling stays intact",
      );
    });
  }
});

test("the redactor keeps short values exact-only and leaves unrelated text alone", () => {
  const scrub = privateRedactor(
    parseGoalRequest({
      goal: "g",
      inputs: { pin: "42", code: "Save the whales" },
      private_inputs: ["pin", "code"],
    }),
  );
  assert.equal(scrub.field("42"), "[private:pin]");
  assert.equal(scrub.text("Order 42 shipped"), "Order 42 shipped");
  assert.equal(scrub.field("Save the"), "[private:code]", "begins the value");
  assert.equal(scrub.text("Save"), "Save", "a short word is not a prefix leak");
  assert.equal(
    scrub.text("Please SAVE  THE\nwhales today"),
    "Please [private:code] today",
  );
  assert.equal(scrub.text("abc Save the wh", 200, true), "abc [private:code]");
  assert.equal(scrub.text("abc Save the wh", 200, false), "abc Save the wh");
});

test("a click whose effect lands asynchronously is not decided on (or repeated) too early", async (t) => {
  let saves = 0;
  let saved = false;
  let n = 0;
  const page = {
    async observe() {
      n++;
      return {
        tab,
        binding: {
          revision: 0,
          observationId: `o${n}`,
          url: "http://fixture.test/",
        },
        title: "Form",
        text: saved ? "Saved." : "Not saved yet",
        truncated: false,
        targets: [
          { id: "t0", tag: "button", role: "", name: "Save", bounds: {} },
          { id: "t1", tag: "button", role: "", name: "Cancel", bounds: {} },
        ],
      };
    },
    async act() {
      // No spinner, no disabled button: the save lands 600 ms later.
      saves++;
      setTimeout(() => {
        saved = true;
      }, 600);
      return { tab, status: "dispatched", revision: 0 };
    },
  };
  // Like the review's double: press Save whenever the page is not saved.
  const { fixture, jev } = await jevFor(t, {
    policy: (body) => {
      const ops = Object.keys(body.questions.operation.criteria);
      return body.state.page.text === "Saved."
        ? {
            operation: choice(ops, "DONE"),
            goal_complete: { type: "noul", noul: 0.95 },
          }
        : {
            operation: choice(ops, "CLICK"),
            click_target: choice(["t0", "t1"], "t0"),
          };
    },
  });
  const receipt = await runGoal({
    browser: page,
    tab,
    jev,
    request: parseGoalRequest({ goal: "Save the form." }),
    session: session(),
  });
  assert.equal(receipt.result.value.goal_outcome, "jev_reported_done");
  assert.equal(saves, 1, "Save pressed once");
  assert.equal(fixture.requests.length, 2, "no decision on the unsettled page");
});

test("each recent step names the page it was taken on, so Next on the following page is not a repeat", async (t) => {
  let p = 1;
  let n = 0;
  const clicks = [];
  const pager = {
    async observe() {
      n++;
      return {
        tab,
        binding: {
          revision: p,
          observationId: `o${n}`,
          url: `http://fixture.test/pager?p=${p}`,
        },
        title: `Pager – Page ${p}`,
        text: `Page ${p}\nNext\nHome`,
        truncated: false,
        targets: [
          { id: "t0", tag: "a", role: "button", name: "Next", bounds: {} },
          { id: "t1", tag: "a", role: "link", name: "Home", bounds: {} },
        ],
      };
    },
    async act(_t, _b, action) {
      clicks.push(action.target);
      if (action.target === "t0") p++;
      return { tab, status: "dispatched", revision: p };
    },
  };
  // Like real Jev (Cloud acceptance 2026-09-28): a succeeded click is not
  // taken again unless recent_steps shows it was taken on another page; a
  // step without a page reads as taken on this one.
  const { fixture, jev } = await jevFor(t, {
    policy: (body) => {
      const ops = Object.keys(body.questions.operation.criteria);
      const { page, recent_steps } = body.state;
      if (page.text.startsWith("Page 3"))
        return {
          operation: choice(ops, "DONE"),
          goal_complete: { type: "noul", noul: 0.95 },
        };
      const repeat = recent_steps.some(
        (s) => s.result === "dispatched" && (s.page?.url ?? page.url) === page.url,
      );
      return repeat
        ? { operation: choice(ops, "WAIT") }
        : {
            operation: choice(ops, "CLICK"),
            click_target: choice(["t0", "t1"], "t0"),
          };
    },
  });
  const receipt = await runGoal({
    browser: pager,
    tab,
    jev,
    request: parseGoalRequest({
      goal: "Press Next until the page shows Page 3.",
      max_steps: 4,
    }),
    session: session(),
  });
  const v = receipt.result.value;
  assert.equal(v.goal_outcome, "jev_reported_done");
  assert.equal(v.final_page.title, "Pager – Page 3");
  assert.deepEqual(clicks, ["t0", "t0"], "Next once per page, nothing else");
  const page = (q) => ({
    url: `http://fixture.test/pager?p=${q}`,
    title: `Pager – Page ${q}`,
  });
  const sent = fixture.requests.map((r) => r.body.state.recent_steps);
  assert.deepEqual(sent[0], []);
  assert.deepEqual(
    sent[1].map((s) => [s.operation, s.target, s.result, s.page]),
    [["CLICK", "t0 Next", "dispatched", page(1)]],
  );
  assert.deepEqual(
    sent[2].map((s) => s.page),
    [page(1), page(2)],
  );
  assert.match(
    fixture.requests[1].body.questions.operation.instructions,
    /page \(url and title\) it was taken on/,
  );
  assert.deepEqual(
    v.steps.map((s) => s.page.title),
    ["Pager – Page 1", "Pager – Page 2", "Pager – Page 3"],
  );
});

test("a malformed or unbalanced Jev answer executes nothing", async (t) => {
  const page = new FakePage();
  const { jev } = await jevFor(t, {
    policy: (body) => {
      const ops = Object.keys(body.questions.operation.criteria);
      const a = choice(ops, "CLICK");
      // Chosen option is not the most probable one.
      a.probabilities[ops[0]] = 0.95;
      a.probabilities.CLICK = 0.01;
      return { operation: a };
    },
  });
  const receipt = await runGoal({
    browser: page,
    tab,
    jev,
    request,
    session: session(),
  });
  assert.equal(receipt.result.code, "jev_invalid_response");
  assert.equal(page.acts.length, 0);
});

test("a Retry-After longer than a prompt stop allows is reported, not slept", async (t) => {
  const page = new FakePage();
  const { fixture, jev } = await jevFor(t, {
    policy: signupPolicy,
    fault: () => ({ status: 429, headers: { "retry-after": "30" } }),
  });
  const started = Date.now();
  const receipt = await runGoal({
    browser: page,
    tab,
    jev,
    request,
    session: session(),
  });
  assert.equal(receipt.result.code, "jev_rate_limited");
  assert.equal(fixture.requests.length, 1);
  assert.ok(Date.now() - started < 2000);
});

test("the person's stop aborts a pending Jev decision without acting", async (t) => {
  const page = new FakePage();
  const stop = new AbortController();
  const { jev } = await jevFor(t, {
    policy: async (body) => {
      stop.abort();
      await new Promise((r) => setTimeout(r, 1500));
      return signupPolicy(body);
    },
  });
  const started = Date.now();
  const receipt = await runGoal({
    browser: page,
    tab,
    jev,
    request,
    session: { ...session(), stop: stop.signal },
  });
  assert.equal(receipt.status, "cancelled");
  assert.equal(receipt.result.value.goal_outcome, "stopped_by_person");
  assert.equal(page.acts.length, 0);
  assert.ok(Date.now() - started < 1000, "did not wait for the late answer");
});

test("a decision that arrives too late is discarded, never applied", async (t) => {
  const page = new FakePage();
  let clock = 1_000_000;
  const { jev } = await jevFor(t, {
    policy: (body) => {
      clock += 9_000; // Jev (with retries) took longer than decisionMs.
      return signupPolicy(body);
    },
  });
  const receipt = await runGoal({
    browser: page,
    tab,
    jev,
    request,
    now: () => clock,
    session: session(),
  });
  assert.equal(receipt.result.code, "jev_too_slow");
  assert.equal(page.acts.length, 0);
  assert.deepEqual(
    receipt.result.value.steps.map((s) => s.result),
    Array(3).fill("discarded:late_decision"),
  );
});

test("DONE without Jev's completion judgment is uncertain, not done", async (t) => {
  const page = new FakePage();
  const { jev } = await jevFor(t, {
    policy: (body) => ({
      operation: choice(Object.keys(body.questions.operation.criteria), "DONE"),
      goal_complete: { type: "noul", noul: 0.2 },
    }),
  });
  const receipt = await runGoal({
    browser: page,
    tab,
    jev,
    request,
    session: session(),
  });
  const v = receipt.result.value;
  assert.equal(v.goal_outcome, "uncertain");
  assert.equal(v.jev_claimed_complete, false);
  assert.equal(v.verified, false);
});

test("native select options are offered as code-owned choices", async (t) => {
  const page = new FakePage();
  const base = page.observe.bind(page);
  page.country = "";
  page.observe = async () => {
    const o = await base();
    o.targets.push({
      id: "t3",
      tag: "select",
      role: "",
      name: "Country",
      value: page.country,
      options: [
        { value: "", label: "Choose…" },
        { value: "jp", label: "Japan" },
        { value: "fr", label: "France" },
        { value: "xx", label: "Closed", disabled: true },
      ],
      bounds: {},
    });
    o.text = page.country ? `Country ${page.country}` : o.text;
    return o;
  };
  const act = page.act.bind(page);
  page.act = async (t2, b, action, opts) => {
    if (action.kind === "select") page.country = action.value;
    return act(t2, b, action, opts);
  };
  const { fixture, jev } = await jevFor(t, {
    policy: (body) => {
      const q = body.questions;
      if (body.state.page.text.startsWith("Country"))
        return { operation: choice(Object.keys(q.operation.criteria), "DONE") };
      const options = Object.keys(q.select_option.criteria);
      assert.deepEqual(
        options,
        ["t3:1", "t3:2"],
        "no current or disabled option",
      );
      return {
        operation: choice(Object.keys(q.operation.criteria), "SELECT"),
        select_option: choice(options, "t3:2"),
      };
    },
  });
  const receipt = await runGoal({
    browser: page,
    tab,
    jev,
    request: parseGoalRequest({ goal: "Set my country to France" }),
    session: session(),
  });
  assert.equal(receipt.result.value.goal_outcome, "jev_reported_done");
  assert.deepEqual(page.acts, [{ kind: "select", target: "t3", value: "fr" }]);
  assert.match(fixture.requests[0].raw, /Choose \\"France\\"/);
});
