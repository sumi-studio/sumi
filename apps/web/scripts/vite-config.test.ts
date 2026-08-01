import assert from "node:assert/strict";
import test from "node:test";
import {
  createDevServerConfig,
  SUMI_DEV_API_ORIGIN,
  SUMI_DEV_HOST,
  SUMI_DEV_ORIGIN,
  SUMI_DEV_PORT,
} from "../vite.config.ts";

test("the supported dev origin proxies auth, direct-chat, and domain APIs", () => {
  const server = createDevServerConfig(SUMI_DEV_API_ORIGIN, SUMI_DEV_HOST);
  assert.equal(SUMI_DEV_ORIGIN, "http://127.0.0.1:5173");
  assert.equal(SUMI_DEV_HOST, "127.0.0.1");
  assert.equal(SUMI_DEV_PORT, 5173);
  assert.equal(server.strictPort, true);

  const auth = server.proxy?.["/auth"];
  const directChat = server.proxy?.["/direct-chat"];
  const domainAPI = server.proxy?.["/v1"];
  assert.equal(typeof auth, "object");
  assert.equal(typeof directChat, "object");
  assert.equal(typeof domainAPI, "object");
  if (
    typeof auth !== "object" ||
    typeof directChat !== "object" ||
    typeof domainAPI !== "object"
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
  assert.deepEqual(domainAPI, {
    target: SUMI_DEV_API_ORIGIN,
    changeOrigin: false,
  });
});

test("the proxy target cannot silently become a hostname, wildcard, or path", () => {
  for (const value of [
    "https://127.0.0.1:8080",
    "http://api.example.test:8080",
    "http://127.0.0.1:8080/prefix",
    "http://0.0.0.0:8080",
  ]) {
    assert.throws(
      () => createDevServerConfig(value),
      /explicit literal IPv4 addresses/,
    );
  }
  assert.throws(
    () => createDevServerConfig(SUMI_DEV_API_ORIGIN, "0.0.0.0"),
    /explicit literal IPv4 addresses/,
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
  const domainAPI = server.proxy?.["/v1"];
  assert.equal(typeof auth, "object");
  assert.equal(typeof directChat, "object");
  assert.equal(typeof domainAPI, "object");
  if (
    typeof auth !== "object" ||
    typeof directChat !== "object" ||
    typeof domainAPI !== "object"
  )
    return;
  assert.equal(auth.target, "http://100.64.0.42:8080");
  assert.equal(directChat.target, "http://100.64.0.42:8080");
  assert.equal(directChat.ws, true);
  assert.equal(domainAPI.target, "http://100.64.0.42:8080");
});
