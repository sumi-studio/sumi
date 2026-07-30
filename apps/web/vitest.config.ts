import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["src/**/*.test.{ts,tsx}"],
    exclude: [
      "src/agent/reducer.test.ts",
      "src/agent/store.test.ts",
      "e2e/**",
      "scripts/**",
    ],
  },
});
