import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { lstat, readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";

const [mode, encryptedArgument, manifestArgument, snapshotArgument] =
  process.argv.slice(2);
const imageDigest = /^[a-z0-9./:_-]+@sha256:[0-9a-f]{64}$/;
if (!mode || !encryptedArgument || !manifestArgument) {
  throw new Error(
    "usage: handoff-manifest.mjs create|verify ENCRYPTED HANDOFF_JSON [SNAPSHOT_JSON]",
  );
}
const encrypted = resolve(encryptedArgument);
const handoff = resolve(manifestArgument);
const encryptedInfo = await lstat(encrypted);
if (!encryptedInfo.isFile() || encryptedInfo.isSymbolicLink())
  throw new Error("encrypted bundle must be a regular file");
const encryptedSHA256 = await sha256(encrypted);

if (mode === "create") {
  if (!snapshotArgument) throw new Error("snapshot manifest is required");
  const snapshotPath = resolve(snapshotArgument);
  const snapshot = JSON.parse(await readFile(snapshotPath, "utf8"));
  validateSnapshotIdentity(snapshot);
  await writeFile(
    handoff,
    `${JSON.stringify(
      {
        version: 1,
        snapshot_id: snapshot.snapshot_id,
        app_sha: snapshot.app_sha,
        api_image: snapshot.api_image,
        provisioner_image: snapshot.provisioner_image,
        postgres_image: snapshot.postgres_image,
        encrypted_size: encryptedInfo.size,
        encrypted_sha256: encryptedSHA256,
        snapshot_manifest_sha256: await sha256(snapshotPath),
      },
      null,
      2,
    )}\n`,
    { flag: "wx", mode: 0o600 },
  );
  process.stdout.write(`${snapshot.snapshot_id}\n`);
} else if (mode === "verify") {
  const handoffInfo = await lstat(handoff);
  if (!handoffInfo.isFile() || handoffInfo.isSymbolicLink())
    throw new Error("handoff manifest must be a regular file");
  const recorded = JSON.parse(await readFile(handoff, "utf8"));
  if (
    recorded.version !== 1 ||
    !/^[0-9]{8}T[0-9]{6}Z-[0-9a-f]{12}$/.test(recorded.snapshot_id ?? "") ||
    !/^(?:[0-9a-f]{40}|[0-9a-f]{64})$/.test(recorded.app_sha ?? "") ||
    !imageDigest.test(recorded.api_image ?? "") ||
    !imageDigest.test(recorded.provisioner_image ?? "") ||
    !imageDigest.test(recorded.postgres_image ?? "") ||
    !Number.isSafeInteger(recorded.encrypted_size) ||
    recorded.encrypted_size < 0 ||
    !/^[0-9a-f]{64}$/.test(recorded.encrypted_sha256 ?? "") ||
    !/^[0-9a-f]{64}$/.test(recorded.snapshot_manifest_sha256 ?? "") ||
    recorded.encrypted_size !== encryptedInfo.size ||
    recorded.encrypted_sha256 !== encryptedSHA256
  ) {
    throw new Error("encrypted handoff does not match its manifest");
  }
  if (snapshotArgument) {
    const snapshotPath = resolve(snapshotArgument);
    const snapshot = JSON.parse(await readFile(snapshotPath, "utf8"));
    validateSnapshotIdentity(snapshot);
    if (
      snapshot.snapshot_id !== recorded.snapshot_id ||
      snapshot.app_sha !== recorded.app_sha ||
      snapshot.api_image !== recorded.api_image ||
      snapshot.provisioner_image !== recorded.provisioner_image ||
      snapshot.postgres_image !== recorded.postgres_image ||
      (await sha256(snapshotPath)) !== recorded.snapshot_manifest_sha256
    ) {
      throw new Error(
        "decrypted snapshot manifest is not the handed-off manifest",
      );
    }
  }
  process.stdout.write(`${recorded.snapshot_id}\n`);
} else {
  throw new Error(`unknown mode ${mode}`);
}

function validateSnapshotIdentity(snapshot) {
  if (
    snapshot?.version !== 1 ||
    !/^[0-9]{8}T[0-9]{6}Z-[0-9a-f]{12}$/.test(snapshot.snapshot_id ?? "") ||
    !/^(?:[0-9a-f]{40}|[0-9a-f]{64})$/.test(snapshot.app_sha ?? "") ||
    !imageDigest.test(snapshot.api_image ?? "") ||
    !imageDigest.test(snapshot.provisioner_image ?? "") ||
    !imageDigest.test(snapshot.postgres_image ?? "")
  ) {
    throw new Error("snapshot manifest identity is invalid");
  }
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
