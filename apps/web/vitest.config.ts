import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["src/**/*.test.{ts,tsx}"],
    exclude: [
      // These node:test suites run through test:runtime.
      "src/agent/reducer.test.ts",
      "src/agent/store.test.ts",
      "src/agent/history.test.ts",
      "e2e/**",
      "scripts/**",
    ],
  },
});
