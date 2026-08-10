import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { lstat, readdir, readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";

const [
  mode,
  directoryArgument,
  snapshotID,
  appSHA,
  apiImage,
  provisionerImage,
  postgresImage,
] = process.argv.slice(2);
const imageDigest = /^[a-z0-9./:_-]+@sha256:[0-9a-f]{64}$/;
const requiredArtifacts = [
  "database.dump",
  "host-state.tar",
  "host-state.manifest.json",
  "attachment-rows.tsv",
  "attachments.manifest.json",
  "agent-volumes.tar",
  "agent-volume-set.json",
  "migration-manifest.json",
];
if (!mode || !directoryArgument) {
  throw new Error(
    "usage: snapshot-manifest.mjs create|verify SNAPSHOT_DIR [SNAPSHOT_ID APP_SHA API_IMAGE PROVISIONER_IMAGE POSTGRES_IMAGE]",
  );
}
const directory = resolve(directoryArgument);
const directoryInfo = await lstat(directory);
if (!directoryInfo.isDirectory() || directoryInfo.isSymbolicLink()) {
  throw new Error("snapshot directory must be a real directory");
}

if (mode === "create") {
  if (!/^[0-9]{8}T[0-9]{6}Z-[0-9a-f]{12}$/.test(snapshotID ?? ""))
    throw new Error("snapshot ID is not canonical");
  if (!/^(?:[0-9a-f]{40}|[0-9a-f]{64})$/.test(appSHA ?? ""))
    throw new Error("app SHA is not exact");
  if (
    !imageDigest.test(apiImage ?? "") ||
    !imageDigest.test(provisionerImage ?? "") ||
    !imageDigest.test(postgresImage ?? "")
  )
    throw new Error("snapshot images are not exact digests");
  const migration = await readMigrationManifest();
  const artifacts = [];
  for (const name of requiredArtifacts) artifacts.push(await describe(name));
  await writeFile(
    resolve(directory, "snapshot.json"),
    `${JSON.stringify(
      {
        version: 1,
        snapshot_id: snapshotID,
        app_sha: appSHA,
        api_image: apiImage,
        provisioner_image: provisionerImage,
        postgres_image: postgresImage,
        migration_manifest_sha256: migration.manifest_sha256,
        created_at: new Date().toISOString(),
        artifacts,
      },
      null,
      2,
    )}\n`,
    { flag: "wx", mode: 0o600 },
  );
  process.stdout.write(`${snapshotID}\n`);
} else if (mode === "verify") {
  const manifest = JSON.parse(
    await readFile(resolve(directory, "snapshot.json"), "utf8"),
  );
  if (
    manifest.version !== 1 ||
    !/^[0-9]{8}T[0-9]{6}Z-[0-9a-f]{12}$/.test(manifest.snapshot_id ?? "")
  ) {
    throw new Error("snapshot manifest identity is invalid");
  }
  if (!/^(?:[0-9a-f]{40}|[0-9a-f]{64})$/.test(manifest.app_sha ?? "")) {
    throw new Error("snapshot app SHA is invalid");
  }
  if (
    !imageDigest.test(manifest.api_image ?? "") ||
    !imageDigest.test(manifest.provisioner_image ?? "") ||
    !imageDigest.test(manifest.postgres_image ?? "")
  ) {
    throw new Error("snapshot image identity is invalid");
  }
  if (!Number.isFinite(Date.parse(manifest.created_at ?? ""))) {
    throw new Error("snapshot creation time is invalid");
  }
  const migration = await readMigrationManifest();
  if (manifest.migration_manifest_sha256 !== migration.manifest_sha256) {
    throw new Error("snapshot migration digest disagrees with its artifact");
  }
  if (
    !Array.isArray(manifest.artifacts) ||
    manifest.artifacts.length !== requiredArtifacts.length
  ) {
    throw new Error("snapshot artifact set is incomplete");
  }
  for (let index = 0; index < requiredArtifacts.length; index++) {
    const actual = await describe(requiredArtifacts[index]);
    const recorded = manifest.artifacts[index];
    if (JSON.stringify(actual) !== JSON.stringify(recorded))
      throw new Error(
        `snapshot artifact mismatch: ${requiredArtifacts[index]}`,
      );
  }
  const entries = await readdir(directory);
  const expectedEntries = [...requiredArtifacts, "snapshot.json"].sort();
  if (JSON.stringify(entries.sort()) !== JSON.stringify(expectedEntries)) {
    throw new Error("snapshot bundle contains an unexpected artifact");
  }
  process.stdout.write(`${manifest.snapshot_id}\n`);
} else {
  throw new Error(`unknown mode ${mode}`);
}

async function readMigrationManifest() {
  const migration = JSON.parse(
    await readFile(resolve(directory, "migration-manifest.json"), "utf8"),
  );
  if (
    !/^[0-9a-f]{64}$/.test(migration.manifest_sha256 ?? "") ||
    !Array.isArray(migration.migrations)
  ) {
    throw new Error(
      "migration manifest has no canonical digest and entry list",
    );
  }
  return migration;
}

async function describe(name) {
  const path = resolve(directory, name);
  const info = await lstat(path);
  if (!info.isFile() || info.isSymbolicLink())
    throw new Error(`${name} is not a regular file`);
  return { name, size: info.size, sha256: await sha256(path) };
}

function sha256(path) {
  return new Promise((resolveHash, reject) => {
    const hash = createHash("sha256");
    const input = createReadStream(path);
    input.on("error", reject);
    input.on("data", (chunk) => hash.update(chunk));
    input.on("end", () => resolveHash(hash.digest("hex")));
  });
}
