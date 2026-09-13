import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const sourceRoot = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const ledgerBin = join(sourceRoot, "scripts/operations/task-ledger");

// A stub `gh` keeps the suite hermetic: no tracker reads or writes.
// $GH_STUB_ISSUES controls `issue list` output; `pr list` returns [].
// $GH_STUB_FAIL=1 makes every call fail (for exit-4 coverage).
const GH_STUB = `#!/usr/bin/env bash
set -Eeuo pipefail
if [[ "\${GH_STUB_FAIL:-0}" == 1 ]]; then
  echo "stub gh failure" >&2
  exit 1
fi
if [[ "\${1:-}" == "issue" && "\${2:-}" == "list" ]]; then
  cat "\${GH_STUB_ISSUES}"
elif [[ "\${1:-}" == "pr" && "\${2:-}" == "list" ]]; then
  echo "[]"
else
  echo "stub gh: unexpected args: $*" >&2
  exit 1
fi
`;

async function fixture(issues = []) {
  const dir = await mkdtemp(join(tmpdir(), "task-ledger-test-"));
  const binDir = join(dir, "bin");
  const ledgerDir = join(dir, "ledger");
  const issuesFile = join(dir, "issues.json");
  await execFileAsync("mkdir", ["-p", binDir]);
  await writeFile(join(binDir, "gh"), GH_STUB, { mode: 0o755 });
  await writeFile(issuesFile, JSON.stringify(issues));
  const env = {
    ...process.env,
    PATH: `${binDir}:${process.env.PATH}`,
    SUMI_TASK_LEDGER_DIR: ledgerDir,
    GH_STUB_ISSUES: issuesFile,
  };
  return { dir, ledgerDir, env };
}

async function run(env, args) {
  try {
    const { stdout, stderr } = await execFileAsync(ledgerBin, args, { env });
    return { code: 0, stdout, stderr };
  } catch (error) {
    return {
      code: error.code ?? -1,
      stdout: error.stdout ?? "",
      stderr: error.stderr ?? "",
    };
  }
}

test("claim records owner, worktree, caller pid and lease", async () => {
  const { env, ledgerDir } = await fixture();
  const result = await run(env, [
    "claim",
    "42",
    "--owner",
    "swe-2/session-a",
    "--worktree",
    "/wt/a",
    "--lease-minutes",
    "60",
    "--pid",
    "424242",
  ]);
  assert.equal(result.code, 0, result.stderr);
  const record = JSON.parse(
    await readFile(join(ledgerDir, "issue-42.json"), "utf8"),
  );
  assert.equal(record.state, "claimed");
  assert.equal(record.owner, "swe-2/session-a");
  assert.equal(record.worktree, "/wt/a");
  assert.equal(record.pid, 424242);
  assert.ok(Date.parse(record.leaseExpiresAt) > Date.now());
});

test("concurrent claims on one issue: exactly one wins", async () => {
  const { env } = await fixture();
  const workers = Array.from({ length: 12 }, (_, i) => `worker-${i}`);
  const results = await Promise.all(
    workers.map((owner) =>
      run(env, ["claim", "7", "--owner", owner, "--lease-minutes", "60"]),
    ),
  );
  const winners = results.filter((r) => r.code === 0);
  const losers = results.filter((r) => r.code === 3);
  assert.equal(winners.length, 1);
  assert.equal(losers.length, workers.length - 1);
});

test("second claim on a live lease is refused, even by the holder", async () => {
  const { env } = await fixture();
  await run(env, ["claim", "9", "--owner", "a", "--lease-minutes", "60"]);
  const other = await run(env, [
    "claim",
    "9",
    "--owner",
    "b",
    "--lease-minutes",
    "60",
  ]);
  assert.equal(other.code, 3);
  const same = await run(env, [
    "claim",
    "9",
    "--owner",
    "a",
    "--lease-minutes",
    "60",
  ]);
  assert.equal(same.code, 3);
});

test("expired lease is not handed out automatically; reclaim needs evidence", async () => {
  const { env } = await fixture();
  await run(env, [
    "claim",
    "11",
    "--owner",
    "gone-worker",
    "--lease-minutes",
    "0.02",
    "--pid",
    "999999",
  ]);
  await new Promise((r) => setTimeout(r, 1500));

  const steal = await run(env, ["claim", "11", "--owner", "new-worker"]);
  assert.equal(steal.code, 3);
  assert.match(steal.stderr, /expired/);
  assert.match(steal.stderr, /reclaim/);

  const noEvidence = await run(env, ["reclaim", "11", "--owner", "new-worker"]);
  assert.equal(noEvidence.code, 1);

  const reclaim = await run(env, [
    "reclaim",
    "11",
    "--owner",
    "new-worker",
    "--evidence",
    "pid 999999 dead; worktree absent; no open PR",
  ]);
  assert.equal(reclaim.code, 0, reclaim.stderr);
  assert.match(reclaim.stderr, /pid=999999\(dead\)/);
  const record = JSON.parse(
    await readFile(join(env.SUMI_TASK_LEDGER_DIR, "issue-11.json"), "utf8"),
  );
  assert.equal(record.owner, "new-worker");
  assert.ok(record.history.some((e) => e.event === "reclaim"));
});

test("reclaim refuses a live lease", async () => {
  const { env } = await fixture();
  await run(env, ["claim", "13", "--owner", "alive", "--lease-minutes", "60"]);
  const reclaim = await run(env, [
    "reclaim",
    "13",
    "--owner",
    "thief",
    "--evidence",
    "want it",
  ]);
  assert.equal(reclaim.code, 3);
  assert.match(reclaim.stderr, /lease live/);
});

test("release reasons are validated; foreign release is refused", async () => {
  const { env } = await fixture();
  await run(env, ["claim", "17", "--owner", "a", "--lease-minutes", "60"]);
  const badReason = await run(env, [
    "release",
    "17",
    "--owner",
    "a",
    "--reason",
    "bogus",
  ]);
  assert.equal(badReason.code, 1);
  const foreign = await run(env, [
    "release",
    "17",
    "--owner",
    "b",
    "--reason",
    "ready",
  ]);
  assert.equal(foreign.code, 3);
  const ok = await run(env, [
    "release",
    "17",
    "--owner",
    "a",
    "--reason",
    "review",
    "--note",
    "pr #55",
  ]);
  assert.equal(ok.code, 0, ok.stderr);
});

test("released issue can be claimed again — iterative reopen", async () => {
  const { env, ledgerDir } = await fixture();
  await run(env, ["claim", "19", "--owner", "a", "--lease-minutes", "60"]);
  await run(env, ["release", "19", "--owner", "a", "--reason", "ready"]);
  const reclaim = await run(env, [
    "claim",
    "19",
    "--owner",
    "b",
    "--lease-minutes",
    "60",
  ]);
  assert.equal(reclaim.code, 0, reclaim.stderr);
  const record = JSON.parse(
    await readFile(join(ledgerDir, "issue-19.json"), "utf8"),
  );
  assert.deepEqual(
    record.history.map((e) => e.event),
    ["claim", "release:ready", "claim"],
  );
});

test("renew extends lease and requires the claim owner", async () => {
  const { env } = await fixture();
  await run(env, ["claim", "23", "--owner", "a", "--lease-minutes", "60"]);
  const foreign = await run(env, ["renew", "23", "--owner", "b"]);
  assert.equal(foreign.code, 3);
  const renewed = await run(env, [
    "renew",
    "23",
    "--owner",
    "a",
    "--lease-minutes",
    "240",
  ]);
  assert.equal(renewed.code, 0, renewed.stderr);
  const record = JSON.parse(
    await readFile(join(env.SUMI_TASK_LEDGER_DIR, "issue-23.json"), "utf8"),
  );
  const leaseMs = Date.parse(record.leaseExpiresAt) - Date.now();
  assert.ok(leaseMs > 200 * 60_000);
});

test("list hides released records unless --all", async () => {
  const { env } = await fixture();
  await run(env, ["claim", "29", "--owner", "a"]);
  await run(env, ["claim", "31", "--owner", "b"]);
  await run(env, ["release", "31", "--owner", "b", "--reason", "done"]);
  const active = await run(env, ["list"]);
  assert.match(active.stdout, /#29/);
  assert.doesNotMatch(active.stdout, /#31/);
  const all = await run(env, ["list", "--all"]);
  assert.match(all.stdout, /#31\treleased\tdone/);
});

test("next lists ready issues minus claimed; empty means quiet idle", async () => {
  const issues = [
    { number: 37, title: "first", updatedAt: "2026-09-13T00:00:00Z" },
    { number: 41, title: "second", updatedAt: "2026-09-13T00:00:00Z" },
  ];
  const { env } = await fixture(issues);
  let result = await run(env, ["next", "--label", "state:ready"]);
  assert.equal(result.code, 0, result.stderr);
  assert.match(result.stdout, /#37\tfirst/);
  assert.match(result.stdout, /#41\tsecond/);

  await run(env, ["claim", "37", "--owner", "a"]);
  result = await run(env, ["next", "--label", "state:ready"]);
  assert.doesNotMatch(result.stdout, /#37/);
  assert.match(result.stdout, /#41/);

  await run(env, ["claim", "41", "--owner", "b"]);
  result = await run(env, ["next", "--label", "state:ready"]);
  assert.equal(result.code, 0);
  assert.equal(result.stdout.trim(), "");
});

test("next exits 4 when the tracker cannot be read", async () => {
  const { env } = await fixture();
  const result = await run({ ...env, GH_STUB_FAIL: "1" }, ["next"]);
  assert.equal(result.code, 4);
});

test("a torn record fails closed instead of looking unclaimed", async () => {
  const { env, ledgerDir } = await fixture();
  await execFileAsync("mkdir", ["-p", ledgerDir]);
  await writeFile(join(ledgerDir, "issue-43.json"), "{torn");
  const result = await run(env, ["claim", "43", "--owner", "a"]);
  assert.equal(result.code, 2);
});
