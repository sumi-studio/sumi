import assert from "node:assert/strict";
import { once } from "node:events";
import { mkdirSync } from "node:fs";
import { mkdir, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { app, BaseWindow, session, webContents } from "electron";
import { SharedBrowserRuntime } from "../dist/browser/runtime.js";

assert.ok(
  process.env.SUMI_BROWSER_TEST_HOME,
  "Set an isolated SUMI_BROWSER_TEST_HOME for the test profile.",
);
mkdirSync(process.env.SUMI_BROWSER_TEST_HOME, { recursive: true });
app.setPath("userData", process.env.SUMI_BROWSER_TEST_HOME);
app
  .whenReady()
  .then(async () => {
    const results = [];
    const windows = [];
    const runtimes = [];
    let port;
    const html =
      () => `<!doctype html><html><head><title>Shared tab acceptance</title></head><body>
<h1>One shared document</h1>
<label>Message <input id="message" aria-label="Message"></label>
<button id="save" onclick="document.querySelector('#result').textContent = document.querySelector('#message').value">Save</button>
<p id="result">No message yet</p>
<a id="next" href="/second">Next page</a>
<a id="download" href="/download">Download</a>
<input type="password" value="password-not-observed" aria-label="Password">
<div hidden>hidden-not-observed</div>
<div style="position:absolute;top:5000px">offscreen-not-observed</div>
<iframe id="foreign" src="http://localhost:${port}/foreign" style="height:20px"></iframe>
<script>
document.querySelector('#message').addEventListener('input', e => { window.lastInputTrusted = e.isTrusted; });
localStorage.setItem('visited', 'persistent-profile');
window.pageAttack = {node: typeof process, require: typeof require, electron: typeof electron, ipc: typeof ipcRenderer};
</script></body></html>`;
    const server = createServer((request, response) => {
      if (request.url === "/foreign") {
        response.setHeader("Content-Type", "text/html");
        response.end(
          "<h1>Different origin</h1><script>document.cookie='foreign=isolated; SameSite=Lax'</script>",
        );
      } else if (request.url === "/download") {
        response.setHeader(
          "Content-Disposition",
          "attachment; filename=blocked.txt",
        );
        response.end("not saved");
      } else if (request.url === "/redirect-file") {
        response.writeHead(302, { Location: "file:///etc/passwd" });
        response.end();
      } else if (request.url === "/slow") {
        // Closed by cleanup; intentionally no response, to test navigation timeout.
      } else {
        response.setHeader("Content-Type", "text/html");
        response.setHeader("Set-Cookie", "shared=session; SameSite=Lax");
        response.end(html());
      }
    });
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    port = server.address().port;
    const origin = `http://127.0.0.1:${port}`;

    async function until(check, message) {
      const deadline = Date.now() + 4000;
      while (Date.now() < deadline) {
        if (await check()) return;
        await delay(20);
      }
      throw new Error(`Timed out: ${message}`);
    }
    async function failure(operation, code) {
      await assert.rejects(operation, (error) => error.code === code, code);
    }
    function newWindow() {
      const window = new BaseWindow({ width: 900, height: 700, show: true });
      windows.push(window);
      return window;
    }
    function newRuntime(profileId) {
      const runtime = new SharedBrowserRuntime({ profileId });
      runtimes.push(runtime);
      return runtime;
    }
    function visibleContents(window) {
      assert.equal(window.contentView.children.length, 1);
      const view = window.contentView.children[0];
      assert.ok(view.getBounds().width > 0);
      assert.ok(window.isVisible());
      return view.webContents;
    }
    async function humanClick(window, contents, selector) {
      const rect = await contents.executeJavaScript(`(() => {
    const r = document.querySelector(${JSON.stringify(selector)}).getBoundingClientRect();
    return {x: Math.round(r.x+r.width/2), y: Math.round(r.y+r.height/2)};
  })()`);
      window.focus();
      contents.focus();
      contents.sendInputEvent({
        type: "mouseDown",
        button: "left",
        clickCount: 1,
        ...rect,
      });
      contents.sendInputEvent({
        type: "mouseUp",
        button: "left",
        clickCount: 1,
        ...rect,
      });
      await delay(30);
    }
    const target = (observation, name) => {
      const result = observation.targets.find(
        (candidate) => candidate.name === name,
      );
      assert.ok(result, `Visible target: ${name}`);
      return result.id;
    };
    const passed = (name) => {
      results.push(name);
      console.log(`PASS ${name}`);
    };

    try {
      const runtime = newRuntime("acceptance-shared");
      const window = newWindow();
      const tab = await runtime.openTab(window, origin);
      const contents = visibleContents(window);
      const identity = contents.id;
      assert.equal(
        webContents.getAllWebContents().length,
        1,
        "No second/hidden automation browser",
      );
      assert.equal(runtime.listTabs()[0].tab.tabId, tab.tabId);

      // Human-input path: native Chromium mouse/keyboard input on the attached view,
      // independent of the secretary's DOM action API. This is simulated human input.
      await humanClick(window, contents, "#message");
      for (const keyCode of "human typed")
        contents.sendInputEvent({ type: "char", keyCode });
      await until(
        () =>
          contents.executeJavaScript(
            "document.querySelector('#message').value === 'human typed'",
          ),
        "human keyboard input",
      );
      assert.equal(
        await contents.executeJavaScript("window.lastInputTrusted"),
        true,
      );
      let observation = await runtime.observe(tab);
      assert.equal(
        observation.targets.find((item) => item.name === "Message").value,
        "human typed",
      );
      assert.ok(!JSON.stringify(observation).includes("password-not-observed"));
      assert.ok(!observation.text.includes("hidden-not-observed"));
      assert.ok(!observation.text.includes("offscreen-not-observed"));
      passed(
        "human native input is observed by secretary in the attached real tab",
      );

      await runtime.act(tab, observation.binding, {
        kind: "fill",
        target: target(observation, "Message"),
        text: "secretary changed this same input",
      });
      await failure(
        () =>
          runtime.act(tab, observation.binding, {
            kind: "click",
            target: "t1",
          }),
        "stale_observation",
      );
      observation = await runtime.observe(tab);
      await runtime.act(tab, observation.binding, {
        kind: "click",
        target: target(observation, "Save"),
      });
      assert.equal(
        await contents.executeJavaScript(
          "document.querySelector('#result').textContent",
        ),
        "secretary changed this same input",
      );
      observation = await runtime.observe(tab);
      assert.ok(observation.text.includes("secretary changed this same input"));
      assert.equal(contents.id, identity);
      assert.equal(webContents.getAllWebContents().length, 1);
      if (process.env.SUMI_BROWSER_TEST_ARTIFACTS) {
        await mkdir(process.env.SUMI_BROWSER_TEST_ARTIFACTS, {
          recursive: true,
        });
        await writeFile(
          join(process.env.SUMI_BROWSER_TEST_ARTIFACTS, "shared-tab.png"),
          (await contents.capturePage()).toPNG(),
        );
      }
      passed(
        "secretary fill/click changes the human-visible document; single-use binding enforced",
      );

      const stale = observation;
      await humanClick(window, contents, "#next");
      await until(
        () =>
          contents.getURL() === `${origin}/second` &&
          !contents.isLoadingMainFrame(),
        "human navigation",
      );
      await failure(
        () => runtime.act(tab, stale.binding, { kind: "click", target: "t1" }),
        "stale_observation",
      );
      observation = await runtime.observe(tab);
      assert.ok(observation.binding.revision > stale.binding.revision);
      assert.equal(contents.id, identity);
      await contents.executeJavaScript("history.pushState({}, '', '#changed')");
      await failure(
        () =>
          runtime.act(tab, observation.binding, {
            kind: "scroll",
            x: 0,
            y: 10,
          }),
        "stale_observation",
      );
      observation = await runtime.observe(tab);
      await runtime.act(tab, observation.binding, {
        kind: "navigate",
        url: `${origin}/secretary-navigation`,
      });
      await until(
        () => !contents.isLoadingMainFrame(),
        "secretary navigation settled",
      );
      assert.equal(contents.getURL(), `${origin}/secretary-navigation`);
      passed(
        "human and secretary navigation share the tab and invalidate document bindings, including history changes",
      );

      observation = await runtime.observe(tab);
      const old = observation;
      await runtime.observe(tab);
      await failure(
        () => runtime.act(tab, old.binding, { kind: "scroll", x: 0, y: 1 }),
        "stale_observation",
      );
      const first = runtime.observe(tab);
      await failure(() => runtime.observe(tab), "tab_busy");
      await first;
      observation = await runtime.observe(tab);
      await contents.executeJavaScript(
        "document.querySelector('#save').remove()",
      );
      await failure(
        () =>
          runtime.act(tab, observation.binding, {
            kind: "click",
            target: target(observation, "Save"),
          }),
        "target_unavailable",
      );
      passed(
        "replaced observations, concurrent calls, and detached DOM targets fail explicitly",
      );

      assert.deepEqual(await contents.executeJavaScript("window.pageAttack"), {
        node: "undefined",
        require: "undefined",
        electron: "undefined",
        ipc: "undefined",
      });
      const preferences = contents.getLastWebPreferences();
      assert.equal(preferences.sandbox, true);
      assert.equal(preferences.contextIsolation, true);
      assert.equal(preferences.nodeIntegration, false);
      assert.equal(preferences.webSecurity, true);
      assert.ok(!preferences.preload);
      assert.equal(
        await contents.executeJavaScript(
          "typeof globalThis.sumiBrowserSnapshot",
        ),
        "undefined",
      );
      await contents.executeJavaScript(
        "globalThis.sumiBrowserSnapshot = {id:'forged',targets:new Map()}; window.open('https://example.com')",
      );
      await delay(50);
      assert.equal(webContents.getAllWebContents().length, 1);
      assert.equal(
        await contents.executeJavaScript("Notification.requestPermission()"),
        "denied",
      );
      const geo = await contents.executeJavaScript(
        "new Promise(r => navigator.geolocation.getCurrentPosition(() => r('allowed'), e => r(e.code)))",
      );
      assert.equal(geo, 1);
      await until(
        () =>
          contents.executeJavaScript(
            "document.querySelector('#foreign').contentWindow.length === 0",
          ),
        "foreign frame",
      );
      assert.equal(
        await contents.executeJavaScript(
          "(() => { try { return document.querySelector('#foreign').contentWindow.document.body.textContent } catch(e) { return e.name } })()",
        ),
        "SecurityError",
      );
      const navigationBefore = contents.getURL();
      for (const url of [
        "file:///etc/passwd",
        "sumi://privileged",
        "javascript:document.body.textContent='bad'",
      ]) {
        observation = await runtime.observe(tab);
        await failure(
          () =>
            runtime.act(tab, observation.binding, { kind: "navigate", url }),
          "invalid_request",
        );
      }
      await contents.executeJavaScript("location.href='file:///etc/passwd'");
      await delay(100);
      assert.equal(contents.getURL(), navigationBefore);
      await contents.executeJavaScript("location.href='sumi://privileged'");
      await delay(100);
      assert.equal(contents.getURL(), navigationBefore);
      const downloadBlocked = new Promise((resolve) =>
        contents.session.once("will-download", (event) =>
          resolve(event.defaultPrevented),
        ),
      );
      await humanClick(window, contents, "#download");
      assert.equal(await downloadBlocked, true);
      observation = await runtime.observe(tab);
      await failure(
        () =>
          runtime.act(tab, observation.binding, {
            kind: "navigate",
            url: `${origin}/redirect-file`,
          }),
        "navigation_failed",
      );
      assert.ok(!contents.getURL().startsWith("file:"));
      // Chromium presents its navigation error document; recover in the same tab.
      await contents.loadURL(origin);
      await until(
        () => !contents.isLoadingMainFrame(),
        "recover from blocked redirect",
      );
      passed(
        "hostile page has no Node/preload/IPC; isolation, permissions, origin, popup, protocol and download boundaries hold",
      );

      const anotherWindow = newWindow();
      const anotherTab = await runtime.openTab(anotherWindow, origin);
      const anotherContents = visibleContents(anotherWindow);
      assert.equal(anotherContents.session, contents.session);
      assert.equal(
        await anotherContents.executeJavaScript(
          "localStorage.getItem('visited')",
        ),
        "persistent-profile",
      );
      assert.ok(
        (await anotherContents.executeJavaScript("document.cookie")).includes(
          "shared=session",
        ),
      );
      assert.equal(session.defaultSession === contents.session, false);
      const otherRuntime = newRuntime("acceptance-separate");
      const separateWindow = newWindow();
      const separateTab = await otherRuntime.openTab(separateWindow, origin);
      assert.notEqual(
        visibleContents(separateWindow).session,
        contents.session,
      );
      await contents.executeJavaScript(
        "localStorage.setItem('onlyShared', 'private-to-profile')",
      );
      assert.equal(
        await visibleContents(separateWindow).executeJavaScript(
          "localStorage.getItem('onlyShared')",
        ),
        null,
      );
      await failure(() => otherRuntime.observe(tab), "wrong_runtime");
      otherRuntime.closeTab(separateTab);
      runtime.closeTab(anotherTab);
      passed(
        "same-profile tabs share session; another profile and shell default session remain separate",
      );

      runtime.closeTab(tab);
      const replacement = await runtime.openTab(window, origin);
      assert.notEqual(replacement.tabId, tab.tabId);
      await failure(() => runtime.observe(tab), "tab_closed");
      await failure(
        () =>
          runtime.act(tab, observation.binding, { kind: "scroll", x: 0, y: 1 }),
        "tab_closed",
      );
      const replacementContents = visibleContents(window);
      replacementContents.forcefullyCrashRenderer();
      await until(
        () => runtime.listTabs()[0].status === "unavailable",
        "renderer crash",
      );
      await failure(() => runtime.observe(replacement), "tab_unavailable");
      runtime.dispose();
      await failure(() => runtime.observe(replacement), "runtime_unavailable");
      const restarted = newRuntime("acceptance-shared");
      await failure(() => restarted.observe(replacement), "wrong_runtime");
      const restartedTab = await restarted.openTab(window, origin);
      assert.equal(
        await visibleContents(window).executeJavaScript(
          "localStorage.getItem('onlyShared')",
        ),
        "private-to-profile",
      );
      assert.notEqual(restartedTab.runtimeId, replacement.runtimeId);
      passed(
        "closed/replaced tabs, renderer crash and runtime disposal never rebind stale references; profile survives runtime recreation",
      );

      const slowWindow = newWindow();
      await failure(
        () => restarted.openTab(slowWindow, `${origin}/slow`),
        "operation_timed_out",
      );
      assert.equal(slowWindow.contentView.children.length, 0);
      passed(
        "stalled navigation has a bounded timeout and its failed tab is removed",
      );

      window.close();
      await failure(() => restarted.observe(restartedTab), "tab_closed");
      passed(
        "human window close destroys its WebContents and invalidates the tab",
      );
      const evidence = {
        versions: process.versions,
        sandboxDisabled: app.commandLine.hasSwitch("no-sandbox"),
        results,
      };
      assert.equal(
        evidence.sandboxDisabled,
        false,
        "Run acceptance with Chromium sandbox enabled",
      );
      if (process.env.SUMI_BROWSER_TEST_ARTIFACTS)
        await writeFile(
          join(process.env.SUMI_BROWSER_TEST_ARTIFACTS, "acceptance.json"),
          JSON.stringify(evidence, null, 2),
        );
      console.log(`PASS all ${results.length} real Electron acceptance groups`);
    } catch (error) {
      console.error(error);
      process.exitCode = 1;
    } finally {
      for (const runtime of runtimes) runtime.dispose();
      for (const window of windows) if (!window.isDestroyed()) window.destroy();
      server.closeAllConnections();
      server.close();
      app.exit(process.exitCode ?? 0);
    }
  })
  .catch((error) => {
    console.error(error);
    app.exit(1);
  });
