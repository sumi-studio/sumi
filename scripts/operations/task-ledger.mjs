import { execFileSync } from "node:child_process";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { readdir, readFile, rename, writeFile } from "node:fs/promises";
import { hostname } from "node:os";
import { join } from "node:path";

// Durable task-claim ledger for concurrent workers sharing one host.
// Serialized by the ./task-ledger wrapper via flock(1). Records are
// per-issue JSON files written atomically. See docs/agents/workflow.md.
//
// Exit codes: 0 ok · 1 usage · 2 ledger/IO error · 3 claim conflict ·
// 4 GitHub lookup failed.

const DEFAULT_LEASE_MINUTES = 180;
const RELEASE_REASONS = new Set([
  "ready",
  "review",
  "done",
  "blocked",
  "abandoned",
]);
const MAX_HISTORY = 50;

function usage() {
  console.error(`usage: task-ledger <command> [options]

  claim    <issue> --owner ID [--session ID] [--worktree PATH]
           [--lease-minutes N] [--pid N] [--note TEXT] [--gh-sync]
  renew    <issue> --owner ID [--lease-minutes N]
  release  <issue> --owner ID --reason ready|review|done|blocked|abandoned
           [--note TEXT] [--force] [--gh-sync]
  reclaim  <issue> --owner ID --evidence TEXT [--session ID]
           [--worktree PATH] [--lease-minutes N] [--note TEXT] [--gh-sync]
  show     <issue> [--json]
  list     [--all] [--json]
  next     [--label NAME] [--limit N] [--json]

Common options: --repo OWNER/NAME pins the tracker repository;
SUMI_TASK_LEDGER_REPO does the same. A ledger directory binds to one
repository namespace — the first resolved repo is persisted to
$LEDGER_DIR/repo and wins over the caller's checkout from then on.

--owner identifies the worker, e.g. swe-2/devin-cli/<session> or
opus5/<session>. Claims carry a lease; an expired lease is NOT
transferred automatically — inspect the holder (pid, worktree, open
PRs) and then reclaim with --evidence describing what was checked.`);
  process.exit(1);
}

function parseArgs(argv) {
  const args = { _: [] };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (
      arg === "--gh-sync" ||
      arg === "--force" ||
      arg === "--all" ||
      arg === "--json"
    ) {
      args[arg.slice(2)] = true;
    } else if (arg.startsWith("--")) {
      const key = arg.slice(2);
      if (i + 1 >= argv.length || argv[i + 1].startsWith("--")) {
        throw new Error(`option --${key} requires a value`);
      }
      args[key] = argv[i + 1];
      i += 1;
    } else {
      args._.push(arg);
    }
  }
  return args;
}

function parseIssue(value) {
  if (!/^\d+$/.test(value ?? "")) usage();
  return Number(value);
}

function now() {
  return new Date();
}

function iso(date) {
  return date.toISOString();
}

function expired(record, at) {
  return Date.parse(record.leaseExpiresAt) <= at.getTime();
}

function recordPath(dir, issue) {
  return join(dir, `issue-${issue}.json`);
}

async function readRecord(dir, issue) {
  try {
    return JSON.parse(await readFile(recordPath(dir, issue), "utf8"));
  } catch (error) {
    if (error.code === "ENOENT") return null;
    throw error;
  }
}

async function writeRecord(dir, issue, record) {
  const tmp = `${recordPath(dir, issue)}.${process.pid}.tmp`;
  await writeFile(tmp, `${JSON.stringify(record, null, 2)}\n`);
  await rename(tmp, recordPath(dir, issue));
}

function history(record, event, owner, note) {
  record.history = (record.history ?? []).concat({
    at: iso(now()),
    event,
    owner,
    ...(note ? { note } : {}),
  });
  if (record.history.length > MAX_HISTORY) {
    record.history = record.history.slice(-MAX_HISTORY);
  }
}

function pidAlive(pid) {
  if (!pid) return "unknown";
  try {
    process.kill(pid, 0);
    return "alive";
  } catch (error) {
    return error.code === "ESRCH" ? "dead" : "unknown";
  }
}

function findOpenPrs(issue, repo) {
  try {
    const argv = [
      "pr",
      "list",
      "--state",
      "open",
      "--limit",
      "100",
      "--json",
      "number,title,headRefName,body",
    ];
    if (repo) argv.push("-R", repo);
    const out = execFileSync("gh", argv, {
      timeout: 15_000,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
    });
    const needle = new RegExp(`#${issue}\\b`);
    return JSON.parse(out)
      .filter((pr) => needle.test(pr.title) || needle.test(pr.body ?? ""))
      .map((pr) => `#${pr.number} ${pr.title}`);
  } catch {
    return null;
  }
}

function leaseMinutes(args) {
  const minutes = Number(args["lease-minutes"] ?? DEFAULT_LEASE_MINUTES);
  if (!Number.isFinite(minutes) || minutes <= 0) usage();
  return minutes;
}

function callerPid(args) {
  // biome-ignore lint/suspicious/noUndeclaredEnvVars: set by the task-ledger wrapper, not a cached Turbo task
  const pid = Number(args.pid ?? process.env.SUMI_TASK_LEDGER_CALLER_PID);
  return Number.isInteger(pid) && pid > 0 ? pid : process.ppid;
}

function newClaim(args, issue, at) {
  const minutes = leaseMinutes(args);
  return {
    issue,
    state: "claimed",
    owner: args.owner,
    session: args.session ?? null,
    worktree: args.worktree ?? process.cwd(),
    host: hostname(),
    pid: callerPid(args),
    claimedAt: iso(at),
    leaseExpiresAt: iso(new Date(at.getTime() + minutes * 60_000)),
    lastReason: null,
    history: [],
  };
}

// Tracker label semantics (see docs/agents/workflow.md):
//   claim/reclaim        -> state:in-progress (any other state:* removed)
//   release ready        -> state:ready
//   release review       -> state:review
//   release blocked      -> state:blocked
//   release abandoned    -> state:ready (the work returns to the pool)
//   release done         -> all state:* removed; Done means closed, and
//                           closing is the acceptor's action, so the
//                           ledger never closes the issue itself.
const STATE_LABELS = [
  "state:ready",
  "state:in-progress",
  "state:review",
  "state:blocked",
];
const TARGET_LABEL = {
  claim: "state:in-progress",
  ready: "state:ready",
  review: "state:review",
  blocked: "state:blocked",
  abandoned: "state:ready",
  done: null,
};

function ghCommands(issue, transition, repo) {
  if (!(transition in TARGET_LABEL)) return [];
  const add = TARGET_LABEL[transition];
  const argv = ["issue", "edit", String(issue)];
  for (const label of STATE_LABELS) {
    if (label !== add) argv.push("--remove-label", label);
  }
  if (add) argv.push("--add-label", add);
  if (repo) argv.push("-R", repo);
  return [argv];
}

function ghSync(issue, transition, repo) {
  for (const argv of ghCommands(issue, transition, repo)) {
    try {
      execFileSync("gh", argv, {
        timeout: 15_000,
        stdio: ["ignore", "ignore", "pipe"],
      });
    } catch {
      console.error(
        `task-ledger: warning: gh label sync failed; run manually: gh ${argv.join(" ")}`,
      );
    }
  }
  if (transition === "done") printDoneNote(issue, repo);
}

// Done = closed, and closing belongs to the acceptor. The suggestion is
// only executable when the repo is bound — never print an unscoped
// `gh issue close` that cwd/GH_REPO could retarget.
function printDoneNote(issue, repo) {
  const close = repo
    ? `gh issue close ${issue} -R ${repo}`
    : "gh issue close <N> -R OWNER/REPO (repository unresolved)";
  console.error(
    `task-ledger: released as done; the acceptor closes the issue once verified: ${close}`,
  );
}

function printGhPlan(issue, transition, repo) {
  if (!repo) {
    console.error(
      "task-ledger: repository unresolved; add `-R OWNER/REPO` to the " +
        "commands below or set --repo/SUMI_TASK_LEDGER_REPO",
    );
  }
  for (const argv of ghCommands(issue, transition, repo)) {
    console.error(
      `task-ledger: tracker not synced; equivalent: gh ${argv.join(" ")}`,
    );
  }
  if (transition === "done") printDoneNote(issue, repo);
}

function syncOrPrint(args, issue, transition, repo) {
  if (args["gh-sync"] && !repo) {
    console.error(
      "task-ledger: --gh-sync needs a repository binding; " +
        "set --repo or SUMI_TASK_LEDGER_REPO — not syncing",
    );
    printGhPlan(issue, transition, repo);
    return;
  }
  if (args["gh-sync"]) ghSync(issue, transition, repo);
  else printGhPlan(issue, transition, repo);
}

// One ledger directory = one repository namespace. Resolution order:
// --repo flag > SUMI_TASK_LEDGER_REPO > the binding persisted at
// $LEDGER_DIR/repo > `git remote get-url origin` in the caller's cwd.
// The first successful resolution is persisted, so a later invocation
// from a different checkout (e.g. the ChatGPT-Coding worktrees) still
// addresses the bound repo instead of its same-numbered issues.
function repoFromGitRemote() {
  try {
    const url = execFileSync("git", ["remote", "get-url", "origin"], {
      timeout: 5_000,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
    }).trim();
    const match = url.match(/github\.com[:/]([^/\s]+\/[^/\s]+?)(?:\.git)?$/);
    return match ? match[1] : null;
  } catch {
    return null;
  }
}

function resolveRepo(args, dir) {
  // biome-ignore lint/suspicious/noUndeclaredEnvVars: caller-set binding, not a cached Turbo task
  const explicit = args.repo ?? process.env.SUMI_TASK_LEDGER_REPO ?? null;
  let stored = null;
  try {
    stored = readFileSync(join(dir, "repo"), "utf8").trim() || null;
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
  }
  if (stored) {
    if (explicit && explicit !== stored) {
      throw new Error(
        `ledger ${dir} is bound to ${stored}; --repo ${explicit} ` +
          `conflicts — use a different SUMI_TASK_LEDGER_DIR or remove ${dir}/repo`,
      );
    }
    return stored;
  }
  const resolved = explicit ?? repoFromGitRemote();
  if (resolved) writeFileSync(join(dir, "repo"), `${resolved}\n`);
  return resolved;
}

function describeHolder(record, at) {
  const lease = expired(record, at) ? "expired" : "live";
  return `issue #${record.issue} is claimed by ${record.owner} (lease ${lease} until ${record.leaseExpiresAt}, worktree ${record.worktree ?? "?"})`;
}

async function cmdClaim(args, dir, repo) {
  const issue = parseIssue(args._[1]);
  if (!args.owner) usage();
  const at = now();
  const record = await readRecord(dir, issue);
  if (record?.state === "claimed") {
    console.error(`task-ledger: ${describeHolder(record, at)}`);
    if (expired(record, at)) {
      console.error(
        "task-ledger: lease expired; inspect the holder, then use `reclaim --evidence …`",
      );
    }
    process.exit(3);
  }
  const claim = newClaim(args, issue, at);
  if (record) claim.history = record.history ?? [];
  history(claim, "claim", args.owner, args.note);
  await writeRecord(dir, issue, claim);
  syncOrPrint(args, issue, "claim", repo);
  console.log(
    `claimed #${issue} owner=${claim.owner} lease-until=${claim.leaseExpiresAt}`,
  );
}

async function cmdReclaim(args, dir, repo) {
  const issue = parseIssue(args._[1]);
  if (!args.owner || !args.evidence) usage();
  const at = now();
  const record = await readRecord(dir, issue);
  if (record?.state === "claimed" && !expired(record, at)) {
    console.error(
      `task-ledger: refusing reclaim: ${describeHolder(record, at)}`,
    );
    process.exit(3);
  }
  if (record?.state === "claimed") {
    const sameHost = record.host === hostname();
    const pid = sameHost ? pidAlive(record.pid) : "unknown";
    const worktree = record.worktree
      ? existsSync(record.worktree)
        ? "present"
        : "missing"
      : "unknown";
    // No binding: never let the lookup fall back to the caller's cwd
    // repo — an unrelated repository's PRs are not evidence here.
    const prs = repo ? findOpenPrs(issue, repo) : "skipped";
    console.error(
      `task-ledger: evidence — prior owner=${record.owner} ` +
        `pid=${record.pid}(${pid}) worktree=${record.worktree}(${worktree})`,
    );
    if (prs === "skipped")
      console.error(
        "task-ledger: evidence — open-PR lookup skipped (no repo binding)",
      );
    else if (prs === null)
      console.error("task-ledger: evidence — open-PR lookup failed (gh)");
    else if (prs.length)
      console.error(`task-ledger: evidence — open PRs: ${prs.join("; ")}`);
    else
      console.error("task-ledger: evidence — no open PR references this issue");
  }
  const claim = newClaim(args, issue, at);
  if (record) claim.history = record.history ?? [];
  history(
    claim,
    "reclaim",
    args.owner,
    `evidence: ${args.evidence}${args.note ? ` — ${args.note}` : ""}`,
  );
  await writeRecord(dir, issue, claim);
  syncOrPrint(args, issue, "claim", repo);
  console.log(
    `reclaimed #${issue} owner=${claim.owner} lease-until=${claim.leaseExpiresAt}`,
  );
}

async function cmdRenew(args, dir) {
  const issue = parseIssue(args._[1]);
  if (!args.owner) usage();
  const record = await readRecord(dir, issue);
  if (record?.state !== "claimed" || record.owner !== args.owner) {
    console.error(
      `task-ledger: no matching claim on #${issue} for owner ${args.owner}`,
    );
    process.exit(3);
  }
  record.leaseExpiresAt = iso(
    new Date(now().getTime() + leaseMinutes(args) * 60_000),
  );
  history(record, "renew", args.owner);
  await writeRecord(dir, issue, record);
  console.log(`renewed #${issue} lease-until=${record.leaseExpiresAt}`);
}

async function cmdRelease(args, dir, repo) {
  const issue = parseIssue(args._[1]);
  if (!args.owner || !RELEASE_REASONS.has(args.reason)) usage();
  const record = await readRecord(dir, issue);
  if (record?.state !== "claimed") {
    console.error(`task-ledger: no active claim on #${issue}`);
    process.exit(3);
  }
  if (record.owner !== args.owner && !args.force) {
    console.error(
      `task-ledger: #${issue} is claimed by ${record.owner}; ` +
        "use --force only to clear an abandoned claim after inspection",
    );
    process.exit(3);
  }
  record.state = "released";
  record.lastReason = args.reason;
  record.releasedAt = iso(now());
  history(record, `release:${args.reason}`, args.owner, args.note);
  await writeRecord(dir, issue, record);
  syncOrPrint(args, issue, args.reason, repo);
  console.log(`released #${issue} reason=${args.reason}`);
}

async function cmdShow(args, dir) {
  const issue = parseIssue(args._[1]);
  const record = await readRecord(dir, issue);
  if (!record) {
    console.error(`task-ledger: no record for #${issue}`);
    process.exit(0);
  }
  if (args.json) {
    console.log(JSON.stringify(record, null, 2));
    return;
  }
  console.log(
    `#${record.issue} ${record.state} owner=${record.owner} ` +
      `lease-until=${record.leaseExpiresAt}` +
      (record.lastReason ? ` last-reason=${record.lastReason}` : ""),
  );
  for (const entry of record.history ?? []) {
    console.log(
      `  ${entry.at} ${entry.event} ${entry.owner}${entry.note ? ` — ${entry.note}` : ""}`,
    );
  }
}

async function cmdList(args, dir) {
  const files = (await readdir(dir)).filter((name) =>
    /^issue-\d+\.json$/.test(name),
  );
  const records = [];
  for (const file of files) {
    const record = JSON.parse(await readFile(join(dir, file), "utf8"));
    if (args.all || record.state === "claimed") records.push(record);
  }
  records.sort((a, b) => a.issue - b.issue);
  if (args.json) {
    console.log(JSON.stringify(records, null, 2));
    return;
  }
  const at = now();
  for (const record of records) {
    const lease =
      record.state === "claimed"
        ? expired(record, at)
          ? "expired"
          : "live"
        : record.lastReason;
    console.log(
      `#${record.issue}\t${record.state}\t${lease}\t${record.owner}\t${record.worktree ?? "-"}`,
    );
  }
}

async function cmdNext(args, dir, repo) {
  if (!repo) {
    console.error(
      "task-ledger: cannot determine the repository for `next` — " +
        "set --repo or SUMI_TASK_LEDGER_REPO, or run inside the checkout",
    );
    process.exit(4);
  }
  const label = args.label ?? "state:ready";
  const limit = args.limit ?? "100";
  let issues;
  try {
    const out = execFileSync(
      "gh",
      [
        "issue",
        "list",
        "--state",
        "open",
        "--label",
        label,
        "--limit",
        limit,
        "--json",
        "number,title,updatedAt",
        "-R",
        repo,
      ],
      { timeout: 20_000, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] },
    );
    issues = JSON.parse(out);
  } catch (error) {
    console.error(
      `task-ledger: gh issue list failed: ${error.stderr?.toString().trim() || error.message}`,
    );
    process.exit(4);
  }
  const claimed = new Map();
  for (const file of await readdir(dir)) {
    if (!/^issue-\d+\.json$/.test(file)) continue;
    const record = JSON.parse(await readFile(join(dir, file), "utf8"));
    if (record.state === "claimed") {
      claimed.set(record.issue, expired(record, now()));
    }
  }
  let expiredHeld = 0;
  const available = issues.filter((issue) => {
    const isClaimed = claimed.get(issue.number);
    if (isClaimed === undefined) return true;
    if (isClaimed) expiredHeld += 1;
    return false;
  });
  if (expiredHeld > 0) {
    console.error(
      `task-ledger: ${expiredHeld} ready issue(s) held by expired claims — ` +
        "inspect with `list`/`show`, then `reclaim --evidence …` if the holder is gone",
    );
  }
  if (args.json) {
    console.log(JSON.stringify(available, null, 2));
    return;
  }
  for (const issue of available) {
    console.log(`#${issue.number}\t${issue.title}`);
  }
}

async function main() {
  const argv = process.argv.slice(2);
  let dir = null;
  while (argv[0] === "--ledger-dir") {
    dir = argv[1];
    argv.splice(0, 2);
  }
  if (!dir || argv.length === 0) usage();
  const args = parseArgs(argv);
  const command = args._[0];
  const commands = {
    claim: cmdClaim,
    reclaim: cmdReclaim,
    renew: cmdRenew,
    release: cmdRelease,
    show: cmdShow,
    list: cmdList,
    next: cmdNext,
  };
  const handler = commands[command];
  if (!handler) usage();
  // Only tracker-facing commands resolve (and on first use, persist) the
  // repository binding; local bookkeeping stays free of cwd side effects.
  const repo = ["claim", "reclaim", "release", "next"].includes(command)
    ? resolveRepo(args, dir)
    : null;
  await handler(args, dir, repo);
}

main().catch((error) => {
  console.error(`task-ledger: ${error.message}`);
  process.exit(2);
});
