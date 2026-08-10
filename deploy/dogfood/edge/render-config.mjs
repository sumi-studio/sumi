import { readFile, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const directory = dirname(fileURLToPath(import.meta.url));
const output = resolve(directory, "wrangler.generated.json");
const dnsName =
  /^(?=.{1,253}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/;

const canonicalHost = requireDNSName("SUMI_CANONICAL_HOST");
const zoneName = requireDNSName("SUMI_CLOUDFLARE_ZONE");
const appSHA = environmentValue("SUMI_APP_SHA");
if (!/^(?:[0-9a-f]{40}|[0-9a-f]{64})$/.test(appSHA)) {
  throw new Error(
    "SUMI_APP_SHA must be an exact 40- or 64-character lowercase Git object ID",
  );
}
if (canonicalHost !== zoneName && !canonicalHost.endsWith(`.${zoneName}`)) {
  throw new Error("SUMI_CANONICAL_HOST must be inside SUMI_CLOUDFLARE_ZONE");
}

const template = await readFile(
  resolve(directory, "wrangler.template.json"),
  "utf8",
);
const rendered = template
  .replaceAll("__SUMI_CANONICAL_HOST__", canonicalHost)
  .replaceAll("__SUMI_ZONE_NAME__", zoneName)
  .replaceAll("__SUMI_APP_SHA__", appSHA);
const parsed = JSON.parse(rendered);
await writeFile(output, `${JSON.stringify(parsed, null, 2)}\n`, {
  mode: 0o600,
});
process.stdout.write(`${output}\n`);

function requireDNSName(name) {
  const value = environmentValue(name);
  if (value !== value.toLowerCase() || !dnsName.test(value))
    throw new Error(`${name} must be a lowercase DNS name`);
  return value;
}

function environmentValue(name) {
  return process.env[name]?.trim() ?? "";
}
