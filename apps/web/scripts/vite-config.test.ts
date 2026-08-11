import assert from "node:assert/strict";
import test from "node:test";
import {
  createDevServerConfig,
  SUMI_COMPOSE_API_ORIGIN,
  SUMI_DEV_API_ORIGIN,
  SUMI_DEV_HOST,
  SUMI_DEV_ORIGIN,
  SUMI_DEV_PORT,
} from "../vite.config.ts";

test("the supported dev origin proxies every same-origin app API surface", () => {
  const server = createDevServerConfig(SUMI_DEV_API_ORIGIN, SUMI_DEV_HOST);
  assert.equal(SUMI_DEV_ORIGIN, "http://127.0.0.1:5173");
  assert.equal(SUMI_DEV_HOST, "127.0.0.1");
  assert.equal(SUMI_DEV_PORT, 5173);
  assert.equal(server.strictPort, true);

  const auth = server.proxy?.["/auth"];
  const directChat = server.proxy?.["/direct-chat"];
  const messaging = server.proxy?.["/messaging"];
  const workspace = server.proxy?.["/workspaces"];
  const workspaceInvites = server.proxy?.["/workspace-invites"];
  const apps = server.proxy?.["/apps"];
  const appInstallations = server.proxy?.["/app-installations"];
  assert.equal(typeof auth, "object");
  assert.equal(typeof directChat, "object");
  assert.equal(typeof messaging, "object");
  assert.equal(typeof workspace, "object");
  assert.equal(typeof workspaceInvites, "object");
  assert.equal(typeof apps, "object");
  assert.equal(typeof appInstallations, "object");
  if (
    typeof auth !== "object" ||
    typeof directChat !== "object" ||
    typeof messaging !== "object" ||
    typeof workspace !== "object" ||
    typeof workspaceInvites !== "object" ||
    typeof apps !== "object" ||
    typeof appInstallations !== "object"
  )
    return;
  assert.deepEqual(auth, {
    target: SUMI_DEV_API_ORIGIN,
    changeOrigin: false,
  });
  assert.deepEqual(directChat, {
    target: SUMI_DEV_API_ORIGIN,
    changeOrigin: false,
    ws: true,
  });
  assert.deepEqual(messaging, {
    target: SUMI_DEV_API_ORIGIN,
    changeOrigin: false,
    ws: true,
  });
  for (const proxy of [workspace, workspaceInvites, apps, appInstallations]) {
    assert.deepEqual(proxy, {
      target: SUMI_DEV_API_ORIGIN,
      changeOrigin: false,
    });
  }
});

test("the proxy target accepts only literal IPv4 or the exact Compose service", () => {
  const compose = createDevServerConfig(SUMI_COMPOSE_API_ORIGIN);
  const composeAuth = compose.proxy?.["/auth"];
  assert.equal(typeof composeAuth, "object");
  if (typeof composeAuth === "object") {
    assert.equal(composeAuth.target, SUMI_COMPOSE_API_ORIGIN);
  }
  for (const value of [
    "https://127.0.0.1:8080",
    "http://api.example.test:8080",
    "http://127.0.0.1:8080/prefix",
    "http://0.0.0.0:8080",
  ]) {
    assert.throws(() => createDevServerConfig(value), /exact Compose service/);
  }
  assert.throws(
    () => createDevServerConfig(SUMI_DEV_API_ORIGIN, "0.0.0.0"),
    /exact Compose service/,
  );
});

test("an explicit Tailnet IPv4 binds Vite and its API proxy without widening", () => {
  const server = createDevServerConfig(
    "http://100.64.0.42:8080",
    "100.64.0.42",
  );
  assert.equal(server.host, "100.64.0.42");
  assert.equal(server.port, 5173);
  const auth = server.proxy?.["/auth"];
  const directChat = server.proxy?.["/direct-chat"];
  const messaging = server.proxy?.["/messaging"];
  assert.equal(typeof auth, "object");
  assert.equal(typeof directChat, "object");
  assert.equal(typeof messaging, "object");
  if (
    typeof auth !== "object" ||
    typeof directChat !== "object" ||
    typeof messaging !== "object"
  )
    return;
  assert.equal(auth.target, "http://100.64.0.42:8080");
  assert.equal(directChat.target, "http://100.64.0.42:8080");
  assert.equal(directChat.ws, true);
  assert.equal(messaging.target, "http://100.64.0.42:8080");
  assert.equal(messaging.ws, true);
});
