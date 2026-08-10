import {
  cloudflareTest,
  readD1Migrations,
} from "@cloudflare/vitest-pool-workers";
import { defineConfig } from "vitest/config";

const migrations = await readD1Migrations("migrations");

export default defineConfig({
  plugins: [
    cloudflareTest({
      wrangler: { configPath: "./wrangler.jsonc" },
      miniflare: {
        bindings: {
          PUBLISHER_TOKEN: "test-publisher-token-which-is-long",
          PUBLISHER_ID: "test-publisher",
          HUMAN_BOOTSTRAP_SECRET: "test-human-bootstrap-token-which-is-long",
          SESSION_SIGNING_SECRET:
            "test-session-signing-secret-at-least-32-bytes",
          CALLBACK_SIGNING_SECRET:
            "test-callback-signing-secret-at-least-32-bytes",
          VAPID_PUBLIC_KEY:
            "BES0_AyhYwO2hhcb0_4LeFvUQfZr-IVANqMZbIQ0aNls3vEhJGUUoKAVNYQQVvdx4gXF4K0wdym7G8UsJC-7t0s",
          VAPID_PRIVATE_KEY: "hJ680txcQOzb6A2ymLhaLsMYODeJkjV_doERndhvq_4",
          VAPID_SUBJECT: "mailto:test@example.invalid",
          COOKIE_SECURE: "true",
          SESSION_MAX_AGE_SECONDS: "2592000",
          TEST_MIGRATIONS: migrations,
        },
      },
    }),
  ],
  test: {
    include: ["test/**/*.test.ts"],
    setupFiles: ["./test/setup.ts"],
  },
});
