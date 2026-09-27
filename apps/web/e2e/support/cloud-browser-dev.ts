// Manual debugging aid: start the Cloud browser journey stack (local mode)
// and keep it running until SIGINT. Prints the web URL and the session
// cookie path (the cookie itself goes to a 0600 file, never stdout).
import { writeFileSync } from "node:fs";
import { buildWorkspaceBrowserStack, removeWorkspaceBrowserBuild } from "./real-agent-stack";
import { startCloudBrowserStack } from "./cloud-browser-stack";

const databaseURL = process.env.CLOUD_BROWSER_E2E_DB_URL ?? "";
const out = process.env.CLOUD_BROWSER_DEV_OUT ?? "";
const build = await buildWorkspaceBrowserStack();
const stack = await startCloudBrowserStack(build, { mode: "local", databaseURL, artifacts: out });
writeFileSync(`${out}/session`, stack.sessionCookie, { mode: 0o600 });
writeFileSync(`${out}/stack.json`, JSON.stringify({ web: stack.webURL, api: stack.apiURL, worker: stack.workerURL, persona: stack.personaID }), { mode: 0o600 });
console.log(`ready ${stack.webURL}`);
setInterval(() => writeFileSync(`${out}/stack.log`, stack.logs()), 2000);
process.on("SIGINT", async () => {
  await stack.stop();
  await removeWorkspaceBrowserBuild(build);
  process.exit(0);
});
