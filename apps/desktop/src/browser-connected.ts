import { readFileSync, statSync } from "node:fs";
import { app, BaseWindow } from "electron";
import { attachBrowserTab, BrowserHostAgent } from "./browser/host.js";
import { SharedBrowserRuntime } from "./browser/runtime.js";

// Engineering entry, not a new authentication UI. The main-process configuration
// supplies the current Sumi session + Origin/CSRF headers for explicit attachment.
// No configuration, token, or preload is handed to the arbitrary website.
const path = process.env.SUMI_BROWSER_HOST_CONFIG;
if (!path)
  throw new Error(
    "Set SUMI_BROWSER_HOST_CONFIG to an owned JSON configuration file.",
  );
const stat = statSync(path);
if (
  !stat.isFile() ||
  stat.size > 32_768 ||
  (process.platform !== "win32" &&
    ((stat.mode & 0o077) !== 0 || stat.uid !== process.getuid?.()))
)
  throw new Error(
    "Browser host configuration must be an owner-only regular file of at most 32 KiB.",
  );
const config = JSON.parse(readFileSync(path, "utf8")) as {
  apiOrigin: string;
  humanHeaders: Record<string, string>;
  personaId: string;
  profileId: string;
  name: string;
  url: string;
  allowActions: boolean;
};
app
  .whenReady()
  .then(async () => {
    const runtime = new SharedBrowserRuntime({ profileId: config.profileId });
    const window = new BaseWindow({
      width: 1100,
      height: 800,
      title: config.name,
    });
    const abort = new AbortController();
    app.on("before-quit", () => {
      abort.abort();
      runtime.dispose();
    });
    app.on("window-all-closed", () => app.quit());
    const tab = await runtime.openTab(window, config.url);
    const credential = await attachBrowserTab(
      config.apiOrigin,
      config.humanHeaders,
      {
        persona_id: config.personaId,
        name: config.name,
        tab,
        allow_actions: config.allowActions === true,
      },
    );
    // Do not retain the login header object in the running host bridge.
    config.humanHeaders = {};
    const host = new BrowserHostAgent({
      apiOrigin: config.apiOrigin,
      credential,
      browser: runtime,
      tab,
    });
    console.log(
      JSON.stringify({
        event: "browser-attached",
        attachment: credential.attachment,
      }),
    );
    await host.run(abort.signal, (error) =>
      console.error(
        error instanceof Error
          ? error.message
          : "Browser host connection failed",
      ),
    );
  })
  .catch((error) => {
    console.error(
      error instanceof Error ? error.message : "Browser host failed",
    );
    app.exit(1);
  });
