import assert from "node:assert/strict";
import { test } from "node:test";
import { containerEnv, idlePolicy } from "../src/config.ts";

test("forwards only Sumi/Google string configuration into the container", () => {
  const env = containerEnv(
    {
      SUMI_API: { idFromName() {} },
      SUMI_DB_URL: "postgres://example/sumi",
      SUMI_CORE_STATE_TOKEN: "state-token",
      GOOGLE_CLOUD_PROJECT: "project",
      SUMI_API_IDLE_POLICY: "keep",
      SUMI_COMMAND_LOG_DIR: "/elsewhere",
      SUMI_API_JOURNAL_MIRROR: "",
      SUMI_RUNTIME_PROVISIONER_SOCKET: "/run/socket",
      SUMI_MESSAGING_ATTACHMENT_STORE: "disk",
      SUMI_MESSAGING_ATTACHMENT_TOTAL_QUOTA_BYTES: "41943040",
      UNRELATED: "x",
    },
    "cloudflare-container abc",
  );
  assert.deepEqual(env, {
    SUMI_DB_URL: "postgres://example/sumi",
    SUMI_CORE_STATE_TOKEN: "state-token",
    GOOGLE_CLOUD_PROJECT: "project",
    SUMI_MESSAGING_ATTACHMENT_TOTAL_QUOTA_BYTES: "41943040",
    SUMI_API_INSTANCE: "cloudflare-container abc",
  });
});

test("idle policy keeps the API running unless sleep is chosen", () => {
  assert.equal(idlePolicy({}), "keep");
  assert.equal(idlePolicy({ SUMI_API_IDLE_POLICY: " keep " }), "keep");
  assert.equal(idlePolicy({ SUMI_API_IDLE_POLICY: "sleep" }), "sleep");
  assert.throws(() => idlePolicy({ SUMI_API_IDLE_POLICY: "never" }));
});
