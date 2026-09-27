// Electron child for the full-stack Jev goal acceptance (driven by the Go test
// TestSecretaryJevGoalRealBrowser): real attached tab + real host bridge with a
// configured Jev client pointing at the contract-checking test double.
import assert from "node:assert/strict";
import { mkdirSync, writeFileSync } from "node:fs";
import { createServer } from "node:http";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { app, BaseWindow } from "electron";
import { attachBrowserTab, BrowserHostAgent } from "../dist/browser/host.js";
import { JevClient } from "../dist/browser/jev.js";
import { SharedBrowserRuntime } from "../dist/browser/runtime.js";
import {
  choice,
  FIXTURE_KEY,
  formPolicy,
  startJevFixture,
} from "./jev-fixture.mjs";

const dir = process.env.BROWSER_TEST_ARTIFACTS;
assert.ok(dir);
mkdirSync(join(dir, "profile"), { recursive: true });
app.setPath("userData", join(dir, "profile"));
const SITE_PORT = Number(process.env.SUMI_JEV_SITE_PORT ?? 19274);
const JEV_PORT = Number(process.env.SUMI_JEV_FIXTURE_PORT ?? 19275);

const signup = `<!doctype html><title>Sign-up fixture</title>
<h1>Newsletter sign-up</h1>
<label>Name <input id="name" aria-label="Name"></label>
<label>Email <input id="email" type="email" aria-label="Email"></label>
<label>Note <textarea id="note" aria-label="Note"></textarea></label>
<button id="save" onclick="saves++;document.querySelector('#result').textContent='Saved '+document.querySelector('#name').value+' <'+document.querySelector('#email').value+'> x'+saves">Save</button>
<p id="result">Not saved</p><script>let saves=0</script>`;
const pager = `<!doctype html><title>Pager fixture</title>
<h1 id="page">Page 0</h1>
<button id="next" onclick="n++;document.querySelector('#page').textContent='Page '+n">Next</button>
<a href="#help">Help</a><script>let n=0</script>`;

app
  .whenReady()
  .then(async () => {
    const site = createServer((req, res) => {
      res.setHeader("Content-Type", "text/html");
      res.end(req.url.startsWith("/pager") ? pager : signup);
    });
    await new Promise((r) => site.listen(SITE_PORT, "127.0.0.1", r));
    const origin = `http://127.0.0.1:${SITE_PORT}`;
    const runtime = new SharedBrowserRuntime({
      profileId: "sumi-jev-20260927-e2e",
    });
    const window = new BaseWindow({ width: 900, height: 650 });
    const tab = await runtime.openTab(window, `${origin}/signup`);
    const contents = window.contentView.children[0].webContents;
    const js = (code) => contents.executeJavaScript(code);

    let interrupted = false;
    const form = formPolicy({ doneText: "Saved" });
    const jevFixture = await startJevFixture({
      port: JEV_PORT,
      // The outage goal sees an auth failure, as with a revoked/invalid key.
      fault: (body) =>
        body.state.goal.includes("outage") ? { status: 401 } : undefined,
      policy: async (body) => {
        const q = body.questions;
        const ops = Object.keys(q.operation.criteria);
        if (body.state.goal.includes("Next")) {
          await delay(400);
          if (!body.state.page.url.includes("/pager"))
            return { ...fill(q), operation: choice(ops, "NAVIGATE") };
          return {
            ...fill(q),
            operation: choice(ops, "CLICK"),
            click_target: choice(Object.keys(q.click_target.criteria), "t0"),
          };
        }
        if (!interrupted) {
          // The person types a note while Jev decides the first step.
          interrupted = true;
          const point = await js(
            `(()=>{const r=document.querySelector('#note').getBoundingClientRect();return {x:Math.round(r.x+8),y:Math.round(r.y+8)}})()`,
          );
          window.focus();
          contents.focus();
          contents.sendInputEvent({
            type: "mouseDown",
            button: "left",
            clickCount: 1,
            ...point,
          });
          contents.sendInputEvent({
            type: "mouseUp",
            button: "left",
            clickCount: 1,
            ...point,
          });
          await delay(20);
          for (const keyCode of "person note")
            contents.sendInputEvent({ type: "char", keyCode });
          await delay(30);
        }
        return form(body);
      },
    });
    function fill(q) {
      return Object.fromEntries(
        Object.entries(q).map(([id, v]) => [
          id,
          choice(Object.keys(v.criteria), Object.keys(v.criteria)[0]),
        ]),
      );
    }
    const credential = await attachBrowserTab(
      process.env.BROWSER_TEST_URL,
      { "Test-Human": process.env.BROWSER_TEST_HUMAN },
      {
        persona_id: process.env.BROWSER_TEST_PERSONA,
        name: "Jev acceptance tab",
        tab,
        allow_actions: true,
      },
    );
    const bridge = new BrowserHostAgent({
      apiOrigin: process.env.BROWSER_TEST_URL,
      credential,
      browser: runtime,
      tab,
      jev: new JevClient({
        apiKey: FIXTURE_KEY,
        endpoint: jevFixture.endpoint,
      }),
    });
    await bridge.tick();
    writeFileSync(
      join(dir, "ready.json"),
      JSON.stringify({ attachment: credential.attachment }),
    );
    const abort = new AbortController();
    const errors = [];
    const running = bridge.run(abort.signal, (e) => errors.push(e.message));
    let saved = null;
    const deadline = Date.now() + 90_000;
    while (Date.now() < deadline) {
      const state = await js(
        `({url: location.href, result: document.querySelector('#result')?.textContent ?? null, note: document.querySelector('#note')?.value ?? null, page: document.querySelector('#page')?.textContent ?? null})`,
      ).catch(() => null);
      if (state) {
        if (state.result?.startsWith("Saved") && !saved) {
          saved = state;
          writeFileSync(
            join(dir, "signup-saved.png"),
            (await contents.capturePage()).toPNG(),
          );
        }
        writeFileSync(
          join(dir, "state.json"),
          JSON.stringify({
            ...state,
            saved,
            errors,
            jevRequests: jevFixture.requests.map((r) => r.raw),
          }),
        );
      }
      await delay(100);
    }
    abort.abort();
    await running;
    runtime.dispose();
    window.destroy();
    jevFixture.close();
    site.close();
    app.exit(0);
  })
  .catch((error) => {
    console.error(error);
    app.exit(1);
  });
