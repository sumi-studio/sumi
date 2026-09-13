// Single-process entry for the edge tests that need a built WebApp.
//
// `node --test` runs every file argument in its own process, so listing
// production-artifact.test.ts and cloudflare-runtime.test.ts separately would
// build the artifact twice. Importing both here registers their tests in one
// process, where production-artifact-build.ts builds exactly once. Each file
// still works on its own (`pnpm run test:edge:artifact` builds by itself).
import "./production-artifact.test.ts";
import "./cloudflare-runtime.test.ts";
