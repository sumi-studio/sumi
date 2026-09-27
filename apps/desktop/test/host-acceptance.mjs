import assert from "node:assert/strict";
import { mkdirSync, writeFileSync } from "node:fs";
import { createServer } from "node:http";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { app, BaseWindow, webContents } from "electron";
import { attachBrowserTab, BrowserHostAgent } from "../dist/browser/host.js";
import { SharedBrowserRuntime } from "../dist/browser/runtime.js";

const dir = process.env.BROWSER_TEST_ARTIFACTS;
assert.ok(dir);
mkdirSync(join(dir, "profile"), { recursive: true });
app.setPath("userData", join(dir, "profile"));
app
  .whenReady()
  .then(async () => {
    const server = createServer((_req, res) => {
      res.setHeader("Content-Type", "text/html");
      res.end(
        `<!doctype html><title>Secretary browser attachment</title><h1>Shared form</h1><input id="input" aria-label="Message"><button id="save" onclick="count++;document.querySelector('#result').textContent=document.querySelector('#input').value+' / clicks '+count">Save</button><p id="result">No save</p><script>let count=0;window.inputTrusted=false;document.querySelector('#input').addEventListener('input',e=>window.inputTrusted=e.isTrusted)</script>`,
      );
    });
    await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
    const runtime = new SharedBrowserRuntime({ profileId: "core-acceptance" });
    const window = new BaseWindow({ width: 900, height: 650 });
    const tab = await runtime.openTab(
      window,
      `http://127.0.0.1:${server.address().port}`,
    );
    const contents = window.contentView.children[0].webContents;
    window.focus();
    contents.focus();
    const point = await contents.executeJavaScript(
      `(()=>{const r=document.querySelector('#input').getBoundingClientRect();return {x:Math.round(r.x+10),y:Math.round(r.y+10)}})()`,
    );
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
    await delay(30);
    for (const keyCode of "human through real input")
      contents.sendInputEvent({ type: "char", keyCode });
    await delay(50);
    assert.equal(await contents.executeJavaScript("window.inputTrusted"), true);
    const credential = await attachBrowserTab(
      process.env.BROWSER_TEST_URL,
      { "Test-Human": process.env.BROWSER_TEST_HUMAN },
      {
        persona_id: process.env.BROWSER_TEST_PERSONA,
        name: "Acceptance tab",
        tab,
        allow_actions: true,
      },
    );
    const bridge = new BrowserHostAgent({
      apiOrigin: process.env.BROWSER_TEST_URL,
      credential,
      browser: runtime,
      tab,
    });
    await bridge.tick(); // first heartbeat makes this tab discoverably available
    writeFileSync(
      join(dir, "ready.json"),
      JSON.stringify({
        attachment: credential.attachment,
        contentsId: contents.id,
      }),
    );
    const abort = new AbortController();
    const errors = [];
    const running = bridge.run(abort.signal, (e) => errors.push(e.message));
    const deadline = Date.now() + 45000;
    while (Date.now() < deadline) {
      const text = await contents.executeJavaScript(
        "document.querySelector('#result').textContent",
      );
      if (text === "secretary through authorized Core / clicks 1") {
        assert.equal(webContents.getAllWebContents().length, 1);
        writeFileSync(
          join(dir, "same-tab.png"),
          (await contents.capturePage()).toPNG(),
        );
        writeFileSync(
          join(dir, "visible.json"),
          JSON.stringify({ text, contentsId: contents.id, tab, errors }),
        );
      }
      await delay(100);
    }
    abort.abort();
    await running;
    runtime.dispose();
    window.destroy();
    server.close();
    app.exit(0);
  })
  .catch((error) => {
    console.error(error);
    app.exit(1);
  });
