import { lstat, readFile } from "node:fs/promises";
import { basename, isAbsolute, normalize, resolve } from "node:path";

const digestImage = /^[a-z0-9./:_-]+@sha256:[0-9a-f]{64}$/;
const gitObject = /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/;
const dnsName =
  /^(?=.{1,253}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/;

for (const name of [
  "SUMI_API_IMAGE",
  "SUMI_PROVISIONER_IMAGE",
  "SUMI_POSTGRES_IMAGE",
  "SUMI_CLOUDFLARED_IMAGE",
]) {
  const value = required(name);
  if (!digestImage.test(value))
    throw new Error(`${name} must be an image@sha256 digest`);
}
const appSHA = required("SUMI_APP_SHA");
if (!gitObject.test(appSHA))
  throw new Error("SUMI_APP_SHA must be an exact lowercase Git object ID");
const host = required("SUMI_CANONICAL_HOST");
const zone = required("SUMI_CLOUDFLARE_ZONE");
if (!dnsName.test(host) || !dnsName.test(zone))
  throw new Error("canonical host and zone must be lowercase DNS names");
if (host !== zone && !host.endsWith(`.${zone}`))
  throw new Error("canonical host must be inside the configured zone");
if (!/^[A-Za-z0-9_.-]+$/.test(required("SUMI_DOGFOOD_DOCKER_CONTEXT"))) {
  throw new Error("SUMI_DOGFOOD_DOCKER_CONTEXT is not a context name");
}

const stateRoot = required("SUMI_DOGFOOD_STATE_ROOT");
if (
  !isAbsolute(stateRoot) ||
  normalize(stateRoot) !== stateRoot ||
  stateRoot === "/"
) {
  throw new Error(
    "SUMI_DOGFOOD_STATE_ROOT must be a clean absolute non-root path",
  );
}
const stateInfo = await lstat(stateRoot);
if (
  !stateInfo.isDirectory() ||
  stateInfo.isSymbolicLink() ||
  (stateInfo.mode & 0o777) !== 0o711
) {
  throw new Error(
    "SUMI_DOGFOOD_STATE_ROOT must be a real directory with mode 0711",
  );
}
const operationLock = required("SUMI_DOGFOOD_OPERATION_LOCK");
if (
  !isAbsolute(operationLock) ||
  normalize(operationLock) !== operationLock ||
  operationLock === "/" ||
  operationLock !== resolve(stateRoot, ".operations.lock")
) {
  throw new Error(
    "SUMI_DOGFOOD_OPERATION_LOCK must be the state root .operations.lock",
  );
}
const operationLockInfo = await lstat(operationLock);
if (
  !operationLockInfo.isFile() ||
  operationLockInfo.isSymbolicLink() ||
  (operationLockInfo.mode & 0o077) !== 0
) {
  throw new Error(
    "SUMI_DOGFOOD_OPERATION_LOCK must be a protected regular non-symlink",
  );
}
const protectedFiles = [
  "SUMI_POSTGRES_PASSWORD_FILE",
  "SUMI_DOCKER_CONFIG_FILE",
  "SUMI_FIREBASE_ADC_FILE",
  "SUMI_CLOUDFLARE_TUNNEL_TOKEN_FILE",
];
for (const name of protectedFiles) {
  const value = required(name);
  if (!isAbsolute(value) || normalize(value) !== value || value === "/") {
    throw new Error(`${name} must be a clean absolute file path`);
  }
  const info = await lstat(value);
  if (!info.isFile() || info.isSymbolicLink() || (info.mode & 0o077) !== 0) {
    throw new Error(
      `${name} must be a regular non-symlink file with no group/other permissions`,
    );
  }
}
const databaseURL = postgresURL(required("SUMI_DB_URL"));
const passwordFile = required("SUMI_POSTGRES_PASSWORD_FILE");
let databasePassword = await readFile(passwordFile, "utf8");
databasePassword = databasePassword.replace(/\r?\n$/, "");
if (!databasePassword || /[\r\n\0]/.test(databasePassword)) {
  throw new Error("SUMI_POSTGRES_PASSWORD_FILE must contain one password");
}
if (decodeURIComponent(databaseURL.password) !== databasePassword) {
  throw new Error("SUMI_DB_URL password differs from its protected file");
}

const dockerConfig = JSON.parse(
  await readFile(required("SUMI_DOCKER_CONFIG_FILE"), "utf8"),
);
if (basename(required("SUMI_DOCKER_CONFIG_FILE")) !== "config.json") {
  throw new Error("SUMI_DOCKER_CONFIG_FILE must be named config.json");
}
const registryAuth =
  dockerConfig?.auths?.["ghcr.io"]?.auth ??
  dockerConfig?.auths?.["https://ghcr.io"]?.auth;
if (
  typeof registryAuth !== "string" ||
  !canonicalBase64(registryAuth) ||
  !Buffer.from(registryAuth, "base64").toString("utf8").includes(":")
) {
  throw new Error(
    "SUMI_DOCKER_CONFIG_FILE must contain inline ghcr.io credentials",
  );
}

for (const name of ["SUMI_AGENT_TOKEN_SECRET", "SUMI_BROWSER_SESSION_SECRET"]) {
  const value = required(name);
  if (!canonicalBase64(value) || Buffer.from(value, "base64").length < 32) {
    throw new Error(
      `${name} must be canonical base64 containing at least 32 bytes`,
    );
  }
}
for (const name of ["SUMI_LOCAL_CONTROL_TENANT_ID", "SUMI_PROVIDER_API_KEY"])
  required(name);
if (!/^[0-9a-f]{64}$/.test(required("SUMI_APPROVAL_SECRET_DIGEST_KEY"))) {
  throw new Error(
    "SUMI_APPROVAL_SECRET_DIGEST_KEY must be 64 lowercase hex characters",
  );
}

function required(name) {
  const value = process.env[name]?.trim() ?? "";
  if (!value || value.includes("<") || value.includes(">"))
    throw new Error(`${name} is required and cannot be a placeholder`);
  return value;
}

function canonicalBase64(value) {
  if (
    !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(
      value,
    )
  ) {
    return false;
  }
  return Buffer.from(value, "base64").toString("base64") === value;
}

function postgresURL(value) {
  let parsed;
  try {
    parsed = new URL(value);
  } catch {
    throw new Error("SUMI_DB_URL must be a valid Postgres URL");
  }
  if (
    !["postgres:", "postgresql:"].includes(parsed.protocol) ||
    decodeURIComponent(parsed.username) !== "sumi" ||
    parsed.hostname !== "postgres" ||
    parsed.port !== "5432" ||
    parsed.pathname !== "/sumi" ||
    parsed.searchParams.get("sslmode") !== "disable"
  ) {
    throw new Error(
      "SUMI_DB_URL must address the internal postgres:5432/sumi service",
    );
  }
  return parsed;
}
