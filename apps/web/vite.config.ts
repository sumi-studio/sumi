import { isIP } from "node:net";
import { isAbsolute } from "node:path";
import tailwindcss from "@tailwindcss/vite";
import { tanstackRouter } from "@tanstack/router-plugin/vite";
import react from "@vitejs/plugin-react";
import { defineConfig, type ProxyOptions, type ServerOptions } from "vite";

export const SUMI_DEV_HOST = "127.0.0.1";
export const SUMI_DEV_PORT = 5173;
export const SUMI_DEV_ORIGIN = `http://${SUMI_DEV_HOST}:${SUMI_DEV_PORT}`;
export const SUMI_DEV_API_ORIGIN = "http://127.0.0.1:8080";
export const SUMI_COMPOSE_API_ORIGIN = "http://api:8080";

function productionOutputDirectory(): string {
  const configured = process.env.SUMI_WEB_DIST_DIR;
  if (configured === undefined) return "dist";
  if (configured.trim() !== configured || !isAbsolute(configured)) {
    throw new Error(
      "SUMI_WEB_DIST_DIR must be an exact absolute path when provided",
    );
  }
  return configured;
}

function apiProxy(target: string, websocket = false): ProxyOptions {
  return {
    target,
    changeOrigin: false,
    ...(websocket ? { ws: true } : {}),
  };
}

/**
 * Extra browser hosts (for example a Tailscale HTTPS name that fronts the dev
 * server) that Vite must accept in the Host header. Empty means Vite's default
 * host check, which admits only localhost and the literal listen host.
 */
export function parseDevAllowedHosts(raw: string | undefined): string[] {
  return (raw ?? "")
    .split(",")
    .map((value) => value.trim())
    .filter((value) => value !== "");
}

export function createDevServerConfig(
  apiOrigin = SUMI_DEV_API_ORIGIN,
  host = SUMI_DEV_HOST,
  allowedHosts: readonly string[] = [],
): ServerOptions {
  const target = new URL(apiOrigin);
  if (
    target.origin !== apiOrigin ||
    target.protocol !== "http:" ||
    ((isIP(target.hostname) !== 4 || target.hostname === "0.0.0.0") &&
      target.origin !== SUMI_COMPOSE_API_ORIGIN) ||
    isIP(host) !== 4 ||
    host === "0.0.0.0"
  ) {
    throw new Error(
      "Sumi dev host must be an explicit literal IPv4 address and API origin must be literal IPv4 or the exact Compose service",
    );
  }
  for (const allowed of allowedHosts) {
    if (!/^\.?[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?$/.test(allowed)) {
      throw new Error(
        `Sumi dev allowed host is not a plain hostname: ${allowed}`,
      );
    }
  }
  return {
    host,
    port: SUMI_DEV_PORT,
    strictPort: true,
    ...(allowedHosts.length > 0 ? { allowedHosts: [...allowedHosts] } : {}),
    proxy: {
      "/auth": apiProxy(target.origin),
      "/direct-chat": apiProxy(target.origin, true),
      "/messaging": apiProxy(target.origin, true),
      "/workspaces": apiProxy(target.origin),
      "/workspace-invites": apiProxy(target.origin),
      "/apps": apiProxy(target.origin),
      "/app-installations": apiProxy(target.origin),
    },
  };
}

export default defineConfig({
  build: { outDir: productionOutputDirectory() },
  plugins: [
    tanstackRouter({ target: "react", autoCodeSplitting: true }),
    react(),
    tailwindcss(),
  ],
  server: createDevServerConfig(
    process.env.SUMI_DEV_API_ORIGIN?.trim() || SUMI_DEV_API_ORIGIN,
    process.env.SUMI_DEV_HOST?.trim() || SUMI_DEV_HOST,
    parseDevAllowedHosts(process.env.SUMI_DEV_ALLOWED_HOSTS),
  ),
});
