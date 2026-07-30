import { isIP } from "node:net";
import tailwindcss from "@tailwindcss/vite";
import { tanstackRouter } from "@tanstack/router-plugin/vite";
import react from "@vitejs/plugin-react";
import { defineConfig, type ProxyOptions, type ServerOptions } from "vite";

export const SUMI_DEV_HOST = "127.0.0.1";
export const SUMI_DEV_PORT = 5173;
export const SUMI_DEV_ORIGIN = `http://${SUMI_DEV_HOST}:${SUMI_DEV_PORT}`;
export const SUMI_DEV_API_ORIGIN = "http://127.0.0.1:8080";

function apiProxy(target: string, websocket = false): ProxyOptions {
  return {
    target,
    changeOrigin: false,
    ...(websocket ? { ws: true } : {}),
  };
}

export function createDevServerConfig(
  apiOrigin = SUMI_DEV_API_ORIGIN,
  host = SUMI_DEV_HOST,
): ServerOptions {
  const target = new URL(apiOrigin);
  if (
    target.origin !== apiOrigin ||
    target.protocol !== "http:" ||
    isIP(target.hostname) !== 4 ||
    target.hostname === "0.0.0.0" ||
    isIP(host) !== 4 ||
    host === "0.0.0.0"
  ) {
    throw new Error(
      "Sumi dev host and API origin must use explicit literal IPv4 addresses",
    );
  }
  return {
    host,
    port: SUMI_DEV_PORT,
    strictPort: true,
    proxy: {
      "/auth": apiProxy(target.origin),
      "/direct-chat": apiProxy(target.origin, true),
    },
  };
}

export default defineConfig({
  plugins: [
    tanstackRouter({ target: "react", autoCodeSplitting: true }),
    react(),
    tailwindcss(),
  ],
  server: createDevServerConfig(
    process.env.SUMI_DEV_API_ORIGIN?.trim() || SUMI_DEV_API_ORIGIN,
    process.env.SUMI_DEV_HOST?.trim() || SUMI_DEV_HOST,
  ),
});
