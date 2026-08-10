import assert from "node:assert/strict";
import { execFile, spawn } from "node:child_process";
import {
  chmod,
  link,
  mkdir,
  mkdtemp,
  readFile,
  rm,
  stat,
  symlink,
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
const provisionerImage = `ghcr.io/sumi-studio/sumi-runtime-provisioner@sha256:${"e".repeat(64)}`;
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
    provisionerImage,
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

test("agent volume set binds the exact canonical volume and artifact set", async (t) => {
  const root = await temporary(t, "sumi-agent-volume-set-");
  const artifacts = join(root, "artifacts");
  const rows = join(root, "rows.tsv");
  const output = join(root, "agent-volume-set.json");
  const personalityAgentID = "0190abcd-1234-7abc-8def-0123456789ab";
  const project = `sumi-${personalityAgentID.replaceAll("-", "")}`;
  const logicalVolumes = JSON.parse(emptyAgentVolumeSet()).logical_volumes;
  await mkdir(join(artifacts, personalityAgentID), { recursive: true });

  const rowLines = [];
  for (const logical of logicalVolumes) {
    const archive = `${personalityAgentID}/${logical}.tar`;
    const manifest = `${personalityAgentID}/${logical}.manifest`;
    await writeFile(join(artifacts, archive), `archive:${logical}`);
    await writeFile(join(artifacts, manifest), `manifest:${logical}`);
    rowLines.push(
      [
        "V",
        personalityAgentID,
        project,
        logical,
        `${project}_${logical}`,
        archive,
        manifest,
      ].join("\t"),
    );
  }
  await writeFile(rows, `${rowLines.join("\n")}\n`);
  await run(process.execPath, [
    resolve(directory, "agent-volume-set.mjs"),
    "create",
    artifacts,
    rows,
    output,
  ]);

  await writeFile(join(artifacts, "unexpected"), "not in the set");
  await assert.rejects(
    run(process.execPath, [
      resolve(directory, "agent-volume-set.mjs"),
      "verify",
      artifacts,
      output,
    ]),
    /artifact root has an unexpected entry/,
  );
  await rm(join(artifacts, "unexpected"));
  await run(process.execPath, [
    resolve(directory, "agent-volume-set.mjs"),
    "verify",
    artifacts,
    output,
  ]);
  const listed = await run(process.execPath, [
    resolve(directory, "agent-volume-set.mjs"),
    "list",
    artifacts,
    output,
  ]);
  assert.equal(listed.stdout.trim().split("\n").length, 10);

  await writeFile(
    join(artifacts, personalityAgentID, "workspace.tar"),
    "mutated",
  );
  await assert.rejects(
    run(process.execPath, [
      resolve(directory, "agent-volume-set.mjs"),
      "verify",
      artifacts,
      output,
    ]),
    /agent volume archive mismatch/,
  );

  const malformed = JSON.parse(await readFile(output, "utf8"));
  malformed.agents[0].volumes[0].archive.name = `${personalityAgentID}/workspace.tar`;
  await writeFile(
    join(root, "malformed.json"),
    `${JSON.stringify(malformed)}\n`,
  );
  await assert.rejects(
    run(process.execPath, [
      resolve(directory, "agent-volume-set.mjs"),
      "verify",
      artifacts,
      join(root, "malformed.json"),
    ]),
    /noncanonical agent volume artifact binding/,
  );
});

test("agent volume inspection requires canonical Compose ownership labels", async () => {
  const project = "sumi-0190abcd12347abc8def0123456789ab";
  const name = `${project}_workspace`;
  const valid = JSON.stringify([
    {
      Name: name,
      Driver: "local",
      Scope: "local",
      Labels: {
        "com.docker.compose.project": project,
        "com.docker.compose.volume": "workspace",
      },
    },
  ]);
  const script = resolve(directory, "validate-agent-volume.mjs");
  assert.equal(
    (await runWithInput(script, valid, [project, "workspace", name])).code,
    0,
  );
  const wrongOwnerDocument = JSON.parse(valid);
  wrongOwnerDocument[0].Labels["com.docker.compose.project"] =
    "sumi-ffffffffffffffffffffffffffffffff";
  const wrongOwner = JSON.stringify(wrongOwnerDocument);
  const rejected = await runWithInput(script, wrongOwner, [
    project,
    "workspace",
    name,
  ]);
  assert.notEqual(rejected.code, 0);
  assert.match(rejected.stderr, /not the canonical local Compose volume/);
});

test("volume tree manifest is NUL-safe and rejects links", async (t) => {
  const root = await temporary(t, "sumi-volume-tree-");
  const nested = join(root, "nested");
  const unusual = join(nested, "line\nbreak\t.bin");
  await mkdir(nested);
  await writeFile(unusual, "content");
  await chmod(unusual, 0o640);
  const script = resolve(directory, "volume-tree-manifest.sh");
  const manifest = await run("bash", [script, root]);
  const rootRow = manifest.stdout
    .trimEnd()
    .split("\n")
    .find((line) => line.startsWith("d\t") && line.endsWith("\tLg=="));
  assert.ok(rootRow, "volume root metadata is absent from the manifest");
  const fileRow = manifest.stdout
    .trimEnd()
    .split("\n")
    .find((line) => line.startsWith("f\t"));
  assert.ok(fileRow);
  const fields = fileRow.split("\t");
  assert.equal(fields[3], "640");
  assert.equal(fields[4], "7");
  assert.equal(
    Buffer.from(fields[6], "base64").toString("utf8"),
    "nested/line\nbreak\t.bin",
  );

  await symlink("nested", join(root, "linked"));
  await assert.rejects(run("bash", [script, root]), /symlink is forbidden/);
  await rm(join(root, "linked"));
  await link(unusual, join(nested, "second-link"));
  await assert.rejects(run("bash", [script, root]), /hard-linked/);
  await rm(join(nested, "second-link"));
  await chmod(nested, 0o000);
  await assert.rejects(run("bash", [script, root]));
  await chmod(nested, 0o700);
});

test("agent volume restore uses isolated scratch volumes and verifies every tree", async (t) => {
  const root = await temporary(t, "sumi-agent-volume-restore-");
  const artifacts = join(root, "artifacts");
  const helpers = join(root, "helpers");
  const dockerConfigDirectory = join(root, "docker-config");
  const dockerConfig = join(dockerConfigDirectory, "config.json");
  const docker = join(helpers, "docker");
  const log = join(root, "docker.log");
  const rows = join(root, "rows.tsv");
  const volumeSet = join(root, "agent-volume-set.json");
  const snapshotID = "20260810T120010Z-aaaaaaaaaaaa";
  const personalityAgentID = "0190abcd-1234-7abc-8def-0123456789ab";
  const project = `sumi-${personalityAgentID.replaceAll("-", "")}`;
  const logicalVolumes = JSON.parse(emptyAgentVolumeSet()).logical_volumes;
  await mkdir(join(artifacts, personalityAgentID), { recursive: true });
  await mkdir(helpers);
  await mkdir(dockerConfigDirectory);
  await writeFile(dockerConfig, "{}\n", { mode: 0o600 });
  await writeFile(log, "");

  const rowLines = [];
  for (const logical of logicalVolumes) {
    const archive = `${personalityAgentID}/${logical}.tar`;
    const manifest = `${personalityAgentID}/${logical}.manifest`;
    await run("/usr/bin/tar", [
      "--create",
      `--file=${join(artifacts, archive)}`,
      "--files-from=/dev/null",
    ]);
    await writeFile(join(artifacts, manifest), `manifest:${logical}`);
    rowLines.push(
      [
        "V",
        personalityAgentID,
        project,
        logical,
        `${project}_${logical}`,
        archive,
        manifest,
      ].join("\t"),
    );
  }
  await writeFile(rows, `${rowLines.join("\n")}\n`);
  await run(process.execPath, [
    resolve(directory, "agent-volume-set.mjs"),
    "create",
    artifacts,
    rows,
    volumeSet,
  ]);

  await writeExecutable(
    docker,
    `#!/usr/bin/env bash
set -Eeuo pipefail
printf '%s\\n' "$*" >> "\${SUMI_TEST_DOCKER_LOG}"
arguments="$*"
if [[ "\${arguments}" == *"context inspect"* ]]; then
  printf 'unix:///var/run/docker.sock\\n'
elif [[ "\${arguments}" == *"volume inspect"* ]]; then
  exit 1
elif [[ "\${arguments}" == *"volume create"* ]]; then
  printf '%s\\n' "\${!#}"
elif [[ "\${arguments}" == *"/usr/local/bin/sumi-volume-manifest /source"* ]]; then
  for logical in ${logicalVolumes.join(" ")}; do
    if [[ "\${arguments}" == *"-\${logical},dst=/source"* ]]; then
      printf 'manifest:%s' "\${logical}"
      exit 0
    fi
  done
  exit 2
elif [[ "\${arguments}" == *"run --rm"* ]]; then
  exit 0
else
  exit 2
fi
`,
  );

  const restored = await run(
    "bash",
    [
      resolve(directory, "restore-agent-volumes.sh"),
      artifacts,
      volumeSet,
      snapshotID,
    ],
    {
      env: {
        ...process.env,
        SUMI_DOCKER_BIN: docker,
        SUMI_DOGFOOD_DOCKER_CONTEXT: "dogfood-test",
        SUMI_DOCKER_CONFIG_FILE: dockerConfig,
        SUMI_PROVISIONER_IMAGE: provisionerImage,
        SUMI_TAR_BIN: "/usr/bin/tar",
        SUMI_TEST_DOCKER_LOG: log,
      },
    },
  );
  assert.match(restored.stdout, /scratch volumes are recorded/);
  const restoredMap = await readFile(
    join(artifacts, "restored-agent-volumes.tsv"),
    "utf8",
  );
  if (restoredMap.trim().split("\n").length !== 10) {
    throw new Error(`unexpected restored map: ${JSON.stringify(restoredMap)}`);
  }
  const operations = await readFile(log, "utf8");
  assert.match(
    operations,
    /sumi\.backup\.snapshot=20260810T120010Z-aaaaaaaaaaaa/,
  );
  assert.match(
    operations,
    /--extract --preserve-permissions --same-owner --numeric-owner --acls --xattrs/,
  );
});

test("coordinated snapshot resumes only after a successful quiesce", async (t) => {
  const root = await temporary(t, "sumi-backup-create-");
  const work = join(root, "work");
  const state = join(root, "state");
  const helpers = join(root, "helpers");
  const dockerConfigDirectory = join(root, "docker");
  const log = join(root, "operations.log");
  const operationLock = join(state, ".operations.lock");
  await mkdir(work);
  await mkdir(state);
  await mkdir(join(state, "command-log"));
  await mkdir(join(state, "runtime-state"));
  await mkdir(join(state, "attachments", "01", "90"), { recursive: true });
  await mkdir(helpers);
  await mkdir(dockerConfigDirectory);
  const dockerConfig = join(dockerConfigDirectory, "config.json");
  await writeFile(dockerConfig, "{}\n", { mode: 0o600 });
  await writeFile(operationLock, "", { mode: 0o600 });
  await writeFile(
    join(state, "attachments", "01", "90", `${attachmentID}.bin`),
    "hello",
  );

  const paths = await fakeHelpers(helpers);
  const baseEnvironment = {
    ...process.env,
    SUMI_DB_URL: "postgres://backup.invalid/sumi",
    SUMI_APP_SHA: appSHA,
    SUMI_API_IMAGE: apiImage,
    SUMI_PROVISIONER_IMAGE: provisionerImage,
    SUMI_POSTGRES_IMAGE: postgresImage,
    SUMI_BACKUP_WORK_ROOT: work,
    SUMI_DOGFOOD_STATE_ROOT: state,
    SUMI_ATTACHMENT_ROOT: join(state, "attachments"),
    SUMI_MIGRATE_BIN: paths.migrate,
    SUMI_DATABASE_HELPER: paths.database,
    SUMI_AGENT_VOLUME_HELPER: paths.agentVolume,
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

test("scratch restore reconstructs and verifies the coordinated host state", async (t) => {
  const root = await temporary(t, "sumi-backup-restore-");
  const snapshot = join(root, "snapshot");
  const sourceState = join(root, "source-state");
  const restoreState = join(root, "restore-state");
  const helpers = join(root, "helpers");
  const snapshotID = "20260810T120004Z-aaaaaaaaaaaa";
  await mkdir(snapshot);
  await mkdir(sourceState);
  await mkdir(join(sourceState, "command-log"));
  await mkdir(join(sourceState, "runtime-state"));
  await mkdir(join(sourceState, "attachments", "01", "90"), {
    recursive: true,
  });
  await writeFile(join(sourceState, ".operations.lock"), "", { mode: 0o600 });
  await mkdir(restoreState);
  await mkdir(helpers);
  await writeFile(
    join(sourceState, "attachments", "01", "90", `${attachmentID}.bin`),
    "hello",
  );
  await writeFile(
    join(snapshot, "attachment-rows.tsv"),
    `${attachmentID}\t5\n`,
  );
  await run(process.execPath, [
    resolve(directory, "verify-attachments.mjs"),
    join(sourceState, "attachments"),
    join(snapshot, "attachment-rows.tsv"),
    join(snapshot, "attachments.manifest.json"),
  ]);
  await run(process.execPath, [
    resolve(directory, "host-state-manifest.mjs"),
    "create",
    sourceState,
    join(snapshot, "host-state.manifest.json"),
  ]);
  await run("/usr/bin/tar", [
    "--create",
    `--file=${join(snapshot, "host-state.tar")}`,
    `--directory=${sourceState}`,
    "command-log",
    "runtime-state",
    "attachments",
  ]);
  await writeFile(
    join(snapshot, "agent-volume-set.json"),
    emptyAgentVolumeSet(),
  );
  await run("/usr/bin/tar", [
    "--create",
    `--file=${join(snapshot, "agent-volumes.tar")}`,
    "--files-from=/dev/null",
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
    provisionerImage,
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
    "host-state.tar",
    "host-state.manifest.json",
    "attachment-rows.tsv",
    "attachments.manifest.json",
    "agent-volumes.tar",
    "agent-volume-set.json",
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
  const database = join(helpers, "database");
  const agentRestore = join(helpers, "agent-restore");
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
    database,
    `#!/usr/bin/env bash
set -Eeuo pipefail
case "\${1:-}" in
  scratch-object-count) printf '0\\n';;
  scratch-restore) cat >/dev/null;;
  scratch-attachment-rows) printf '%s\\t5\\n' "\${SUMI_TEST_ATTACHMENT_ID}";;
  *) exit 2;;
esac
`,
  );
  await writeExecutable(
    agentRestore,
    "#!/usr/bin/env bash\nset -Eeuo pipefail\n",
  );
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
      restoreState,
    ],
    {
      env: {
        ...process.env,
        SUMI_RESTORE_DB_URL: "postgres://restore.invalid/scratch",
        SUMI_RESTORE_CONFIRM_SCRATCH: snapshotID,
        SUMI_RESTORE_WORK_ROOT: root,
        SUMI_DECRYPT_HELPER: decrypt,
        SUMI_DATABASE_HELPER: database,
        SUMI_AGENT_RESTORE_HELPER: agentRestore,
        SUMI_MIGRATE_BIN: migrate,
        SUMI_TAR_BIN: "/usr/bin/tar",
        SUMI_TEST_ATTACHMENT_ID: attachmentID,
        SUMI_RESTORE_ALLOW_NONROOT_FOR_TESTS: "1",
      },
    },
  );
  assert.match(
    restored.stdout,
    new RegExp(`scratch restore ${snapshotID} verified`),
  );
  assert.equal(
    await readFile(
      join(restoreState, "attachments", "01", "90", `${attachmentID}.bin`),
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
    writeFile(join(root, "host-state.tar"), "host-state"),
    writeFile(join(root, "host-state.manifest.json"), '{"version":1}\n'),
    writeFile(join(root, "attachment-rows.tsv"), ""),
    writeFile(
      join(root, "attachments.manifest.json"),
      '{"version":1,"files":[]}\n',
    ),
    writeFile(join(root, "agent-volumes.tar"), "agent-volumes"),
    writeFile(join(root, "agent-volume-set.json"), emptyAgentVolumeSet()),
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
    database: join(root, "database"),
    agentVolume: join(root, "agent-volume"),
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
    paths.database,
    `#!/usr/bin/env bash
set -Eeuo pipefail
case "\${1:-}" in
  attachment-rows) printf '%s\\t5\\n' "\${SUMI_TEST_ATTACHMENT_ID}";;
  dump) printf database;;
  *) exit 2;;
esac
`,
  );
  await writeExecutable(
    paths.agentVolume,
    `#!/usr/bin/env bash
set -Eeuo pipefail
printf agent-volumes > "$1/agent-volumes.tar"
printf '%s' '${emptyAgentVolumeSet().trim()}' > "$1/agent-volume-set.json"
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

function emptyAgentVolumeSet() {
  return `${JSON.stringify({
    version: 1,
    logical_volumes: [
      "allocator-root",
      "allocator-state",
      "artifacts",
      "broker-identity",
      "broker-ipc",
      "executor-identity",
      "executor-ipc",
      "runtime-identity",
      "state",
      "workspace",
    ],
    agents: [],
  })}\n`;
}

async function writeExecutable(path, body) {
  await writeFile(path, body);
  await chmod(path, 0o700);
}

function runWithInput(script, input, arguments_ = []) {
  return new Promise((resolveRun, reject) => {
    const child = spawn(process.execPath, [script, ...arguments_], {
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
