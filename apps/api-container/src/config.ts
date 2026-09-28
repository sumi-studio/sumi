/**
 * Pure configuration helpers for the API container Worker, kept apart from
 * the Container class so they can be tested under Node.
 */

/**
 * Environment names the Worker never forwards: Worker-only settings, and the
 * container-local paths/mode the image entrypoint sets itself.
 */
const RESERVED = new Set([
  "SUMI_API",
  "SUMI_API_IDLE_POLICY",
  "SUMI_API_JOURNAL_MIRROR",
  "SUMI_COMMAND_LOG_DIR",
  "SUMI_BROWSER_EVENT_DIR",
  "SUMI_RUNTIME_PROVISIONER_SOCKET",
  "SUMI_MESSAGING_ATTACHMENT_ROOT",
  "SUMI_MESSAGING_ATTACHMENT_STORE",
  "PORT",
]);

const FORWARDED_PREFIXES = ["SUMI_", "GOOGLE_CLOUD_PROJECT"];

/**
 * Selects the API configuration from Worker vars and secrets. Only string
 * values with a Sumi/Google prefix are forwarded; bindings never are.
 */
export function containerEnv(
  env: Record<string, unknown>,
  instanceLabel: string,
): Record<string, string> {
  const forwarded: Record<string, string> = {};
  for (const [name, value] of Object.entries(env)) {
    if (typeof value !== "string" || RESERVED.has(name)) continue;
    if (!FORWARDED_PREFIXES.some((prefix) => name.startsWith(prefix))) continue;
    forwarded[name] = value;
  }
  forwarded.SUMI_API_INSTANCE = instanceLabel;
  return forwarded;
}

export function idlePolicy(env: { SUMI_API_IDLE_POLICY?: string }): "keep" | "sleep" {
  const value = (env.SUMI_API_IDLE_POLICY ?? "").trim();
  if (value === "" || value === "keep") return "keep";
  if (value === "sleep") return "sleep";
  throw new Error('SUMI_API_IDLE_POLICY must be "keep" or "sleep"');
}
