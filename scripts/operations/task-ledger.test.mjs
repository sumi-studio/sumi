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
// Every invocation's argv is appended to $GH_LOG for sync assertions.
// $GH_STUB_FAIL=1 makes every call fail (for exit-4 coverage).
const GH_STUB = `#!/usr/bin/env bash
set -Eeuo pipefail
echo "$*" >> "$GH_LOG"
if [[ "\${GH_STUB_FAIL:-0}" == 1 ]]; then
  echo "stub gh failure" >&2
  exit 1
fi
if [[ "\${1:-}" == "issue" && "\${2:-}" == "list" ]]; then
  cat "\${GH_STUB_ISSUES}"
elif [[ "\${1:-}" == "pr" && "\${2:-}" == "list" ]]; then
  echo "[]"
elif [[ "\${1:-}" == "issue" && "\${2:-}" == "edit" && $# -le 3 ]]; then
  echo "gh: a flag is required to edit" >&2
  exit 1
fi
exit 0
`;

async function fixture(issues = []) {
  const dir = await mkdtemp(join(tmpdir(), "task-ledger-test-"));
  const binDir = join(dir, "bin");
  const ledgerDir = join(dir, "ledger");
  const issuesFile = join(dir, "issues.json");
  const ghLog = join(dir, "gh.log");
  await execFileAsync("mkdir", ["-p", binDir]);
  await writeFile(join(binDir, "gh"), GH_STUB, { mode: 0o755 });
  await writeFile(issuesFile, JSON.stringify(issues));
  await writeFile(ghLog, "");
  const env = {
    ...process.env,
    PATH: `${binDir}:${process.env.PATH}`,
    SUMI_TASK_LEDGER_DIR: ledgerDir,
    SUMI_TASK_LEDGER_REPO: "test-org/test-repo",
    GH_STUB_ISSUES: issuesFile,
    GH_LOG: ghLog,
  };
  const ghCalls = async () =>
    (await readFile(ghLog, "utf8")).trim().split("\n").filter(Boolean);
  return { dir, ledgerDir, env, ghCalls };
}

async function run(env, args, cwd) {
  try {
    const { stdout, stderr } = await execFileAsync(ledgerBin, args, {
      env,
      ...(cwd ? { cwd } : {}),
    });
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

test("gh-sync claim removes every other state label, then adds in-progress", async () => {
  const { env, ghCalls } = await fixture();
  const result = await run(env, ["claim", "51", "--owner", "a", "--gh-sync"]);
  assert.equal(result.code, 0, result.stderr);
  assert.deepEqual(await ghCalls(), [
    "issue edit 51 --remove-label state:ready " +
      "--remove-label state:review --remove-label state:blocked " +
      "--add-label state:in-progress -R test-org/test-repo",
  ]);
});

test("gh-sync release done strips all state labels but never closes", async () => {
  const { env, ghCalls } = await fixture();
  await run(env, ["claim", "53", "--owner", "a"]);
  const result = await run(env, [
    "release",
    "53",
    "--owner",
    "a",
    "--reason",
    "done",
    "--gh-sync",
  ]);
  assert.equal(result.code, 0, result.stderr);
  const calls = await ghCalls();
  assert.deepEqual(calls, [
    "issue edit 53 --remove-label state:ready " +
      "--remove-label state:in-progress --remove-label state:review " +
      "--remove-label state:blocked -R test-org/test-repo",
  ]);
  assert.match(result.stderr, /acceptor closes the issue/);
  assert.match(result.stderr, /gh issue close 53 -R test-org\/test-repo/);
});

test("gh-sync release abandoned returns the issue to state:ready", async () => {
  const { env, ghCalls } = await fixture();
  await run(env, ["claim", "57", "--owner", "a"]);
  const result = await run(env, [
    "release",
    "57",
    "--owner",
    "a",
    "--reason",
    "abandoned",
    "--gh-sync",
  ]);
  assert.equal(result.code, 0, result.stderr);
  assert.deepEqual(await ghCalls(), [
    "issue edit 57 --remove-label state:in-progress " +
      "--remove-label state:review --remove-label state:blocked " +
      "--add-label state:ready -R test-org/test-repo",
  ]);
});

test("without --gh-sync no tracker call is made; equivalent commands print", async () => {
  const { env, ghCalls } = await fixture();
  const claim = await run(env, ["claim", "59", "--owner", "a"]);
  assert.equal(claim.code, 0, claim.stderr);
  assert.equal((await ghCalls()).length, 0);
  assert.match(claim.stderr, /equivalent: gh issue edit 59 /);
  assert.match(claim.stderr, /--add-label state:in-progress/);

  const released = await run(env, [
    "release",
    "59",
    "--owner",
    "a",
    "--reason",
    "done",
  ]);
  assert.equal(released.code, 0, released.stderr);
  assert.equal((await ghCalls()).length, 0);
  assert.match(released.stderr, /equivalent: gh issue edit 59 /);
  assert.doesNotMatch(released.stderr, /--add-label/);
  assert.match(released.stderr, /gh issue close 59 -R test-org\/test-repo/);
});

test("next warns on stderr when ready issues are held by expired claims", async () => {
  const issues = [
    { number: 61, title: "held", updatedAt: "2026-09-13T00:00:00Z" },
    { number: 63, title: "free", updatedAt: "2026-09-13T00:00:00Z" },
  ];
  const { env } = await fixture(issues);
  await run(env, ["claim", "61", "--owner", "gone", "--lease-minutes", "0.02"]);
  await new Promise((r) => setTimeout(r, 1500));
  const result = await run(env, ["next"]);
  assert.equal(result.code, 0, result.stderr);
  assert.doesNotMatch(result.stdout, /#61/);
  assert.match(result.stdout, /#63\tfree/);
  assert.match(result.stderr, /1 ready issue\(s\) held by expired claims/);
});

test("ledger binds to the first resolving repo and refuses conflicts", async () => {
  const { env, dir, ghCalls } = await fixture([
    { number: 71, title: "x", updatedAt: "2026-09-13T00:00:00Z" },
  ]);
  const unbound = { ...env };
  delete unbound.SUMI_TASK_LEDGER_REPO;

  // A foreign checkout (e.g. the ChatGPT-Coding worktrees) supplies the
  // repo only when the ledger has no binding yet — first resolution wins.
  const foreign = join(dir, "foreign-checkout");
  await execFileAsync("git", ["init", foreign]);
  await execFileAsync("git", [
    "-C",
    foreign,
    "remote",
    "add",
    "origin",
    "git@github.com:other-org/other-repo.git",
  ]);
  const claim = await run(
    unbound,
    ["claim", "71", "--owner", "a", "--gh-sync"],
    foreign,
  );
  assert.equal(claim.code, 0, claim.stderr);
  assert.match((await ghCalls()).join("\n"), /-R other-org\/other-repo/);
  const stored = await readFile(join(env.SUMI_TASK_LEDGER_DIR, "repo"), "utf8");
  assert.equal(stored.trim(), "other-org/other-repo");

  // A later explicit setting (flag or env) conflicting with the stored
  // binding is refused instead of silently retargeting the namespace.
  const conflict = await run(
    env,
    ["claim", "72", "--owner", "b", "--repo", "test-org/test-repo"],
    foreign,
  );
  assert.equal(conflict.code, 2);
  assert.match(conflict.stderr, /bound to other-org\/other-repo/);

  // An inherited GH_REPO pointing elsewhere cannot divert gh: the bound
  // -R flag wins over gh's GH_REPO/cwd resolution.
  const diverted = { ...unbound, GH_REPO: "evil-org/evil-repo" };
  const next = await run(diverted, ["next"], foreign);
  assert.equal(next.code, 0, next.stderr);
  const calls = await ghCalls();
  assert.match(calls.at(-1), /issue list .*-R other-org\/other-repo/);
});

test("next refuses to guess a repo when none is resolvable", async () => {
  const { env, dir } = await fixture();
  const unbound = { ...env };
  delete unbound.SUMI_TASK_LEDGER_REPO;
  const bare = join(dir, "plain-dir");
  await execFileAsync("mkdir", ["-p", bare]);
  const result = await run(unbound, ["next"], bare);
  assert.equal(result.code, 4);
  assert.match(result.stderr, /cannot determine the repository/);
});

test("unbound claim works offline; unbound --gh-sync refuses to write", async () => {
  const { env, dir, ghCalls } = await fixture();
  const unbound = { ...env };
  delete unbound.SUMI_TASK_LEDGER_REPO;
  const bare = join(dir, "nowhere");
  await execFileAsync("mkdir", ["-p", bare]);

  const claim = await run(unbound, ["claim", "73", "--owner", "a"], bare);
  assert.equal(claim.code, 0, claim.stderr);
  assert.match(claim.stderr, /repository unresolved/);
  assert.doesNotMatch(claim.stderr, /gh issue edit 73 [^\n]*-R /);

  const release = await run(
    unbound,
    ["release", "73", "--owner", "a", "--reason", "ready", "--gh-sync"],
    bare,
  );
  assert.equal(release.code, 0, release.stderr);
  assert.match(release.stderr, /needs a repository binding/);
  assert.equal((await ghCalls()).length, 0);
});

test("unbound done suggestion is not an executable unscoped command", async () => {
  const { env, dir } = await fixture();
  const unbound = { ...env };
  delete unbound.SUMI_TASK_LEDGER_REPO;
  const bare = join(dir, "plain2");
  await execFileAsync("mkdir", ["-p", bare]);
  await run(unbound, ["claim", "75", "--owner", "a"], bare);
  const released = await run(
    unbound,
    ["release", "75", "--owner", "a", "--reason", "done"],
    bare,
  );
  assert.equal(released.code, 0, released.stderr);
  assert.match(released.stderr, /repository unresolved/);
  // No bare `gh issue close 75` that cwd/GH_REPO could retarget.
  assert.doesNotMatch(released.stderr, /gh issue close 75\b/);
});

test("reclaim skips the PR evidence lookup when no repo is bound", async () => {
  const { env, dir, ghCalls } = await fixture();
  const unbound = { ...env };
  delete unbound.SUMI_TASK_LEDGER_REPO;
  const bare = join(dir, "plain3");
  await execFileAsync("mkdir", ["-p", bare]);
  await run(
    unbound,
    ["claim", "77", "--owner", "gone", "--lease-minutes", "0.02"],
    bare,
  );
  await new Promise((r) => setTimeout(r, 1500));
  const reclaim = await run(
    unbound,
    ["reclaim", "77", "--owner", "b", "--evidence", "holder gone"],
    bare,
  );
  assert.equal(reclaim.code, 0, reclaim.stderr);
  assert.match(reclaim.stderr, /open-PR lookup skipped/);
  // gh was never invoked: no unscoped `pr list` against the caller's cwd.
  assert.equal((await ghCalls()).length, 0);
});
