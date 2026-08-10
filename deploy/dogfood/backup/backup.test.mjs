import assert from "node:assert/strict";
import { execFile, spawn } from "node:child_process";
import {
  chmod,
  mkdir,
  mkdtemp,
  readFile,
  rm,
  stat,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const run = promisify(execFile);
const directory = dirname(fileURLToPath(import.meta.url));
const appSHA = "a".repeat(40);
const attachmentID = "0190abcd-1234-7abc-8def-0123456789ab";
const migrationDigest = "b".repeat(64);
const apiImage = `ghcr.io/sumi-studio/sumi-api@sha256:${"c".repeat(64)}`;
const postgresImage = `postgres:17-alpine@sha256:${"d".repeat(64)}`;

test("attachment inventory binds every database row to one canonical blob", async (t) => {
  const root = await temporary(t, "sumi-attachment-manifest-");
  const blobs = join(root, "blobs");
  const rows = join(root, "rows.tsv");
  const output = join(root, "manifest.json");
  await mkdir(join(blobs, "01", "90"), { recursive: true });
  await writeFile(join(blobs, "01", "90", `${attachmentID}.bin`), "hello");
  await writeFile(rows, `${attachmentID}\t5\n`);

  await run(process.execPath, [
    resolve(directory, "verify-attachments.mjs"),
    blobs,
    rows,
    output,
  ]);
  const manifest = JSON.parse(await readFile(output, "utf8"));
  assert.equal(manifest.version, 1);
  assert.deepEqual(
    manifest.files.map(({ path, size }) => ({ path, size })),
    [{ path: `01/90/${attachmentID}.bin`, size: 5 }],
  );
  assert.match(manifest.files[0].sha256, /^[0-9a-f]{64}$/);

  await writeFile(rows, `${attachmentID}\t4\n`);
  await assert.rejects(
    run(process.execPath, [
      resolve(directory, "verify-attachments.mjs"),
      blobs,
      rows,
      join(root, "bad.json"),
    ]),
    /database says 4/,
  );

  await writeFile(rows, `${attachmentID}\t9007199254740993\n`);
  await assert.rejects(
    run(process.execPath, [
      resolve(directory, "verify-attachments.mjs"),
      blobs,
      rows,
      join(root, "unsafe-size.json"),
    ]),
    /unsafe size/,
  );
});

test("snapshot and handoff manifests detect mutation and bind to each other", async (t) => {
  const root = await temporary(t, "sumi-snapshot-manifest-");
  const snapshotID = "20260810T120000Z-aaaaaaaaaaaa";
  await writeSnapshotInputs(root);
  await run(process.execPath, [
    resolve(directory, "snapshot-manifest.mjs"),
    "create",
    root,
    snapshotID,
    appSHA,
    apiImage,
    postgresImage,
  ]);
  const verified = await run(process.execPath, [
    resolve(directory, "snapshot-manifest.mjs"),
    "verify",
    root,
  ]);
  assert.equal(verified.stdout.trim(), snapshotID);

  const encrypted = join(root, "snapshot.encrypted");
  const handoff = join(root, "snapshot.handoff.json");
  await writeFile(encrypted, "authenticated encrypted bytes");
  await run(process.execPath, [
    resolve(directory, "handoff-manifest.mjs"),
    "create",
    encrypted,
    handoff,
    join(root, "snapshot.json"),
  ]);
  await run(process.execPath, [
    resolve(directory, "handoff-manifest.mjs"),
    "verify",
    encrypted,
    handoff,
    join(root, "snapshot.json"),
  ]);

  const snapshot = JSON.parse(
    await readFile(join(root, "snapshot.json"), "utf8"),
  );
  snapshot.created_at = "2026-08-10T12:00:01.000Z";
  await writeFile(
    join(root, "snapshot.json"),
    `${JSON.stringify(snapshot, null, 2)}\n`,
  );
  await assert.rejects(
    run(process.execPath, [
      resolve(directory, "handoff-manifest.mjs"),
      "verify",
      encrypted,
      handoff,
      join(root, "snapshot.json"),
    ]),
    /not the handed-off manifest/,
  );

  await writeFile(join(root, "database.dump"), "tampered database");
  await assert.rejects(
    run(process.execPath, [
      resolve(directory, "snapshot-manifest.mjs"),
      "verify",
      root,
    ]),
    /artifact mismatch/,
  );
});

test("archive path validation permits tar root entries but rejects traversal", async () => {
  const script = resolve(directory, "safe-archive-paths.mjs");
  assert.equal(
    (await runWithInput(script, "./\n./01/\n./01/blob.bin\n")).code,
    0,
  );
  for (const unsafe of [
    "../secret\n",
    "./ok/../../secret\n",
    "/etc/passwd\n",
    "ok//secret\n",
    "ok\\secret\n",
  ]) {
    const result = await runWithInput(script, unsafe);
    assert.notEqual(result.code, 0, unsafe);
    assert.match(result.stderr, /unsafe archive path/);
  }
});

test("coordinated snapshot resumes only after a successful quiesce", async (t) => {
  const root = await temporary(t, "sumi-backup-create-");
  const work = join(root, "work");
  const blobs = join(root, "blobs");
  const helpers = join(root, "helpers");
  const dockerConfigDirectory = join(root, "docker");
  const log = join(root, "operations.log");
  const operationLock = join(root, "operations.lock");
  await mkdir(work);
  await mkdir(join(blobs, "01", "90"), { recursive: true });
  await mkdir(helpers);
  await mkdir(dockerConfigDirectory);
  const dockerConfig = join(dockerConfigDirectory, "config.json");
  await writeFile(dockerConfig, "{}\n", { mode: 0o600 });
  await writeFile(operationLock, "", { mode: 0o600 });
  await writeFile(join(blobs, "01", "90", `${attachmentID}.bin`), "hello");

  const paths = await fakeHelpers(helpers);
  const baseEnvironment = {
    ...process.env,
    SUMI_DB_URL: "postgres://backup.invalid/sumi",
    SUMI_APP_SHA: appSHA,
    SUMI_API_IMAGE: apiImage,
    SUMI_POSTGRES_IMAGE: postgresImage,
    SUMI_BACKUP_WORK_ROOT: work,
    SUMI_ATTACHMENT_ROOT: blobs,
    SUMI_MIGRATE_BIN: paths.migrate,
    SUMI_PSQL_BIN: paths.psql,
    SUMI_PG_DUMP_BIN: paths.pgDump,
    SUMI_TAR_BIN: "/usr/bin/tar",
    SUMI_QUIESCE_HELPER: paths.quiesce,
    SUMI_RESUME_HELPER: paths.resume,
    SUMI_ENCRYPT_HELPER: paths.encrypt,
    SUMI_HANDOFF_HELPER: paths.handoff,
    SUMI_TEST_LOG: log,
    SUMI_TEST_ATTACHMENT_ID: attachmentID,
    SUMI_DOGFOOD_OPERATION_LOCK: operationLock,
    SUMI_DOCKER_CONFIG_FILE: dockerConfig,
  };

  const snapshotID = "20260810T120001Z-aaaaaaaaaaaa";
  const success = await run("bash", [resolve(directory, "create.sh")], {
    env: { ...baseEnvironment, SUMI_SNAPSHOT_ID_OVERRIDE: snapshotID },
  });
  assert.match(
    success.stdout,
    new RegExp(`snapshot ${snapshotID} encrypted and handed off`),
  );
  assert.equal(await readFile(log, "utf8"), "quiesce\nresume\nhandoff\n");
  await assert.rejects(
    stat(join(work, snapshotID, `${snapshotID}.bundle.tar`)),
  );
  await run(process.execPath, [
    resolve(directory, "handoff-manifest.mjs"),
    "verify",
    join(work, snapshotID, `${snapshotID}.bundle.encrypted`),
    join(work, snapshotID, `${snapshotID}.handoff.json`),
    join(work, snapshotID, "snapshot.json"),
  ]);

  await writeFile(log, "");
  await assert.rejects(
    run("bash", [resolve(directory, "create.sh")], {
      env: {
        ...baseEnvironment,
        SUMI_SNAPSHOT_ID_OVERRIDE: "20260810T120002Z-aaaaaaaaaaaa",
        SUMI_TEST_FAIL_MIGRATE: "1",
      },
    }),
  );
  assert.equal(await readFile(log, "utf8"), "quiesce\nresume\n");

  await writeFile(log, "");
  await assert.rejects(
    run("bash", [resolve(directory, "create.sh")], {
      env: {
        ...baseEnvironment,
        SUMI_SNAPSHOT_ID_OVERRIDE: "20260810T120003Z-aaaaaaaaaaaa",
        SUMI_TEST_FAIL_QUIESCE: "1",
      },
    }),
  );
  assert.equal(await readFile(log, "utf8"), "quiesce\n");
});

test("scratch restore reconstructs and verifies the coordinated attachment set", async (t) => {
  const root = await temporary(t, "sumi-backup-restore-");
  const snapshot = join(root, "snapshot");
  const sourceBlobs = join(root, "source-blobs");
  const restoreBlobs = join(root, "restore-blobs");
  const helpers = join(root, "helpers");
  const snapshotID = "20260810T120004Z-aaaaaaaaaaaa";
  await mkdir(snapshot);
  await mkdir(join(sourceBlobs, "01", "90"), { recursive: true });
  await mkdir(restoreBlobs);
  await mkdir(helpers);
  await writeFile(
    join(sourceBlobs, "01", "90", `${attachmentID}.bin`),
    "hello",
  );
  await writeFile(
    join(snapshot, "attachment-rows.tsv"),
    `${attachmentID}\t5\n`,
  );
  await run(process.execPath, [
    resolve(directory, "verify-attachments.mjs"),
    sourceBlobs,
    join(snapshot, "attachment-rows.tsv"),
    join(snapshot, "attachments.manifest.json"),
  ]);
  await run("/usr/bin/tar", [
    "--create",
    `--file=${join(snapshot, "attachments.tar")}`,
    `--directory=${sourceBlobs}`,
    ".",
  ]);
  await writeFile(join(snapshot, "database.dump"), "database");
  await writeFile(
    join(snapshot, "migration-manifest.json"),
    `${JSON.stringify({
      manifest_sha256: migrationDigest,
      migrations: [],
    })}\n`,
  );
  await run(process.execPath, [
    resolve(directory, "snapshot-manifest.mjs"),
    "create",
    snapshot,
    snapshotID,
    appSHA,
    apiImage,
    postgresImage,
  ]);

  const bundle = join(root, `${snapshotID}.bundle.tar`);
  const encrypted = join(root, `${snapshotID}.encrypted`);
  const handoff = join(root, `${snapshotID}.handoff.json`);
  await run("/usr/bin/tar", [
    "--create",
    `--file=${bundle}`,
    `--directory=${snapshot}`,
    "database.dump",
    "attachments.tar",
    "attachment-rows.tsv",
    "attachments.manifest.json",
    "migration-manifest.json",
    "snapshot.json",
  ]);
  await writeFile(encrypted, await readFile(bundle));
  await run(process.execPath, [
    resolve(directory, "handoff-manifest.mjs"),
    "create",
    encrypted,
    handoff,
    join(snapshot, "snapshot.json"),
  ]);

  const migrate = join(helpers, "migrate");
  const psql = join(helpers, "psql");
  const pgRestore = join(helpers, "pg-restore");
  const decrypt = join(helpers, "decrypt");
  await writeExecutable(
    migrate,
    `#!/usr/bin/env bash
set -Eeuo pipefail
if [[ "\${1:-}" == manifest ]]; then
  printf '%s\\n' '{"manifest_sha256":"${migrationDigest}","migrations":[]}'
fi
`,
  );
  await writeExecutable(
    psql,
    `#!/usr/bin/env bash
set -Eeuo pipefail
arguments="$*"
if [[ "\${arguments}" == *pg_class* ]]; then
  printf '0\\n'
else
  printf '%s\\t5\\n' "\${SUMI_TEST_ATTACHMENT_ID}"
fi
`,
  );
  await writeExecutable(pgRestore, "#!/usr/bin/env bash\nset -Eeuo pipefail\n");
  await writeExecutable(
    decrypt,
    '#!/usr/bin/env bash\nset -Eeuo pipefail\ncp -- "$1" "$2"\n',
  );

  const restored = await run(
    "bash",
    [
      resolve(directory, "restore-scratch.sh"),
      encrypted,
      handoff,
      restoreBlobs,
    ],
    {
      env: {
        ...process.env,
        SUMI_RESTORE_DB_URL: "postgres://restore.invalid/scratch",
        SUMI_RESTORE_CONFIRM_SCRATCH: snapshotID,
        SUMI_RESTORE_WORK_ROOT: root,
        SUMI_DECRYPT_HELPER: decrypt,
        SUMI_PSQL_BIN: psql,
        SUMI_PG_RESTORE_BIN: pgRestore,
        SUMI_MIGRATE_BIN: migrate,
        SUMI_TAR_BIN: "/usr/bin/tar",
        SUMI_TEST_ATTACHMENT_ID: attachmentID,
      },
    },
  );
  assert.match(
    restored.stdout,
    new RegExp(`scratch restore ${snapshotID} verified`),
  );
  assert.equal(
    await readFile(
      join(restoreBlobs, "01", "90", `${attachmentID}.bin`),
      "utf8",
    ),
    "hello",
  );
});

async function temporary(t, prefix) {
  const path = await mkdtemp(join(tmpdir(), prefix));
  t.after(() => rm(path, { recursive: true, force: true }));
  return path;
}

async function writeSnapshotInputs(root) {
  await Promise.all([
    writeFile(join(root, "database.dump"), "database"),
    writeFile(join(root, "attachments.tar"), "attachments"),
    writeFile(join(root, "attachment-rows.tsv"), ""),
    writeFile(
      join(root, "attachments.manifest.json"),
      '{"version":1,"files":[]}\n',
    ),
    writeFile(
      join(root, "migration-manifest.json"),
      `${JSON.stringify({
        manifest_sha256: migrationDigest,
        migrations: [],
      })}\n`,
    ),
  ]);
}

async function fakeHelpers(root) {
  const paths = {
    migrate: join(root, "migrate"),
    psql: join(root, "psql"),
    pgDump: join(root, "pg-dump"),
    quiesce: join(root, "quiesce"),
    resume: join(root, "resume"),
    encrypt: join(root, "encrypt"),
    handoff: join(root, "handoff"),
  };
  await writeExecutable(
    paths.migrate,
    `#!/usr/bin/env bash
set -Eeuo pipefail
if [[ "\${1:-}" == manifest ]]; then
  printf '%s\\n' '{"manifest_sha256":"${migrationDigest}","migrations":[]}'
elif [[ "\${SUMI_TEST_FAIL_MIGRATE:-0}" == 1 ]]; then
  exit 9
fi
`,
  );
  await writeExecutable(
    paths.psql,
    `#!/usr/bin/env bash
set -Eeuo pipefail
printf '%s\\t5\\n' "\${SUMI_TEST_ATTACHMENT_ID}"
`,
  );
  await writeExecutable(
    paths.pgDump,
    `#!/usr/bin/env bash
set -Eeuo pipefail
for argument in "$@"; do
  case "\${argument}" in --file=*) output="\${argument#--file=}";; esac
done
printf database > "\${output:?}"
`,
  );
  await writeExecutable(
    paths.quiesce,
    `#!/usr/bin/env bash
set -Eeuo pipefail
printf 'quiesce\\n' >> "\${SUMI_TEST_LOG}"
[[ "\${SUMI_TEST_FAIL_QUIESCE:-0}" != 1 ]]
`,
  );
  await writeExecutable(
    paths.resume,
    `#!/usr/bin/env bash
set -Eeuo pipefail
printf 'resume\\n' >> "\${SUMI_TEST_LOG}"
`,
  );
  await writeExecutable(
    paths.encrypt,
    `#!/usr/bin/env bash
set -Eeuo pipefail
cp -- "$1" "$2"
`,
  );
  await writeExecutable(
    paths.handoff,
    `#!/usr/bin/env bash
set -Eeuo pipefail
[[ -s "$1" && -s "$2" ]]
printf 'handoff\\n' >> "\${SUMI_TEST_LOG}"
`,
  );
  return paths;
}

async function writeExecutable(path, body) {
  await writeFile(path, body);
  await chmod(path, 0o700);
}

function runWithInput(script, input) {
  return new Promise((resolveRun, reject) => {
    const child = spawn(process.execPath, [script], {
      stdio: ["pipe", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk) => {
      stdout += chunk;
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk;
    });
    child.on("error", reject);
    child.on("close", (code) => resolveRun({ code, stdout, stderr }));
    child.stdin.end(input);
  });
}
