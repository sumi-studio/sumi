import { app, BaseWindow } from "electron";
import { SharedBrowserRuntime } from "./browser/runtime.js";

// Minimal engineering surface; this does not replace apps/web or define desktop
// product chrome. The host-only port is imported by a future authorized adapter.
app
  .whenReady()
  .then(async () => {
    const runtime = new SharedBrowserRuntime({ profileId: "development" });
    const window = new BaseWindow({
      width: 1100,
      height: 800,
      title: "Sumi shared browser runtime",
    });
    app.on("before-quit", () => runtime.dispose());
    app.on("window-all-closed", () => app.quit());
    try {
      const tab = await runtime.openTab(
        window,
        process.env.SUMI_BROWSER_URL ?? "https://example.com",
      );
      console.log(JSON.stringify({ event: "browser-tab-opened", tab }));
    } catch (error) {
      console.error(error);
      app.exit(1);
    }
  })
  .catch((error) => {
    console.error(error);
    app.exit(1);
  });
