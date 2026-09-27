import { resolve } from "node:path";
import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

const outDir = process.env.SUMI_LOCAL_WEB_OUT;
if (!outDir || !outDir.startsWith("/")) {
  throw new Error("SUMI_LOCAL_WEB_OUT must name an absolute build output directory");
}
export default defineConfig({
  root: resolve(import.meta.dirname, "local-terminal"),
  base: "/local-terminal/",
  // A Local archive must never inherit Cloud API or preissued-auth settings.
  envPrefix: [],
  define: {
    "import.meta.env.VITE_API_BASE_URL": JSON.stringify(""),
    "import.meta.env.VITE_SUMI_AUTH_MODE": JSON.stringify("session"),
  },
  plugins: [react(), tailwindcss()],
  build: { outDir, emptyOutDir: true },
});
