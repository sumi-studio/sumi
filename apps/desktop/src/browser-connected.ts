import { readFileSync, statSync } from "node:fs";
import { app, BaseWindow } from "electron";
import { attachBrowserTab, BrowserHostAgent } from "./browser/host.js";
import { JevClient } from "./browser/jev.js";
import { SharedBrowserRuntime } from "./browser/runtime.js";

// Engineering entry, not a new authentication UI. The main-process configuration
// supplies the current Sumi session + Origin/CSRF headers for explicit attachment.
// No configuration, token, or preload is handed to the arbitrary website.
const path = process.env.SUMI_BROWSER_HOST_CONFIG;
if (!path)
  throw new Error(
    "Set SUMI_BROWSER_HOST_CONFIG to an owned JSON configuration file.",
  );
function readOwnerOnly(file: string, max: number, what: string): string {
  const stat = statSync(file);
  if (
    !stat.isFile() ||
    stat.size > max ||
    (process.platform !== "win32" &&
      ((stat.mode & 0o077) !== 0 || stat.uid !== process.getuid?.()))
  )
    throw new Error(
      `${what} must be an owner-only regular file of at most ${max} bytes.`,
    );
  return readFileSync(file, "utf8");
}
const config = JSON.parse(
  readOwnerOnly(path, 32_768, "Browser host configuration"),
) as {
  apiOrigin: string;
  humanHeaders: Record<string, string>;
  personaId: string;
  profileId: string;
  name: string;
  url: string;
  allowActions: boolean;
  /** Optional Jev operation layer. The key is read from its own owner-only
   * file into the main process only; it is never placed in the environment. */
  jev?: {
    apiKeyFile: string;
    model?: string;
    endpoint?: string;
    minConfidence?: number;
  };
};
let jev: JevClient | undefined;
if (config.jev) {
  try {
    jev = new JevClient({
      apiKey: readOwnerOnly(config.jev.apiKeyFile, 4096, "Jev API key file"),
      model: config.jev.model,
      endpoint: config.jev.endpoint,
    });
  } catch (error) {
    // Direct browser use stays available; browser.tabs reports Jev as not
    // configured instead of pretending the operation layer exists.
    console.error(
      `Jev operation layer disabled: ${error instanceof Error ? error.message : "invalid configuration"}`,
    );
  }
}
const minConfidence = config.jev?.minConfidence;
if (
  minConfidence !== undefined &&
  !(
    typeof minConfidence === "number" &&
    minConfidence >= 0 &&
    minConfidence <= 1
  )
)
  throw new Error("jev.minConfidence must be a number from 0 to 1.");
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
      jev,
      minConfidence,
    });
    console.log(
      JSON.stringify({
        event: "browser-attached",
        attachment: credential.attachment,
        jev: jev ? { model: jev.model, endpoint: jev.endpoint } : null,
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
