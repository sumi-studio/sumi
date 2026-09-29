/**
 * Shared pieces of the memory e2e scripts (e2e-memory, e2e-workerd-memory,
 * e2e-portable-memory, e2e-usage).
 *
 * memoryAgentRound is a scripted model for the private memory branch
 * (src/memory-branch.ts). It decides every round only from what the branch
 * request carries — the frozen parent prefix, the branch instruction and the
 * branch's own durable transcript — so a restarted or relocated worker
 * continues exactly where the saved transcript stands:
 *
 *   read_source  page memory/<chunk>/source.json to its end
 *   write        write a draft built from the source records it read
 *   reread       read that version of the candidate and the source again
 *   review       {"action":"review", version, sha256} of the written version
 *   confirm      {"action":"confirm", …, token, checks} for the opened modal
 *
 * It proves the runtime lifecycle and durability of a branch. It does not
 * show that a real model writes a faithful replacement.
 *
 * startFileService runs the real workspace file service (apps/files): the
 * state service only advertises file.read/file.write — which the frozen
 * parent tools must contain for a branch to run — when one is configured.
 */
import { spawn, spawnSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { existsSync, mkdirSync, openSync, readdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

export const MEMORY_INSTRUCTION_PREFIX = "ここからは非同期の記憶整理の分岐";
const RESUMED_NOTE = "[Memory execution resumed after a model/connection change]";

/** Stable short digest of one provider message, for prefix comparisons. */
export function messageDigest(m) {
  return createHash("sha256")
    .update(
      JSON.stringify({
        role: m.role,
        content: m.content,
        toolCalls: m.toolCalls ?? null,
        toolCallId: m.toolCallId ?? null,
      }),
    )
    .digest("hex")
    .slice(0, 16);
}

/**
 * The branch parts of a request: the frozen prefix before the instruction,
 * the instruction's chunk and journal range, and the branch's own messages
 * after it. Null for an ordinary turn.
 */
export function memoryBranchView(messages) {
  const start = messages.findIndex(
    (m) =>
      m.role === "user" &&
      typeof m.content === "string" &&
      m.content.startsWith(MEMORY_INSTRUCTION_PREFIX),
  );
  if (start < 0) return null;
  const instruction = messages[start].content;
  const chunk = Number(/memory\/(\d+)\/source\.json/.exec(instruction)?.[1]);
  const seq = /journal seq (\d+)〜(\d+)/.exec(instruction);
  return {
    chunk,
    first_seq: Number(seq?.[1]),
    last_seq: Number(seq?.[2]),
    prefix: messages.slice(0, start),
    own: messages.slice(start + 1),
  };
}

const parse = (text) => {
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
};

/** Everything the branch transcript says about its files and controls. */
export function branchProgress(view) {
  const paths = {
    source: `memory/${view.chunk}/source.json`,
    candidate: `memory/${view.chunk}/candidate.md`,
  };
  const results = [];
  const writes = [];
  const controlErrors = [];
  let resetAt = -1;
  let modal = null;
  view.own.forEach((m, i) => {
    if (m.role === "tool") {
      const r = parse(m.content);
      if (!r) return;
      results.push({ i, r });
      if (
        r.path === paths.candidate &&
        typeof r.version === "number" &&
        r.content_text === undefined
      )
        writes.push({ i, r });
    } else if (m.role === "user") {
      const c = parse(m.content);
      if (c?.memory_confirmation) modal = { i, ...c.memory_confirmation };
      if (c?.memory_control_error) controlErrors.push(c.memory_control_error);
      if (String(m.content).startsWith(RESUMED_NOTE)) resetAt = i;
    }
  });
  const draftCalls = view.own.flatMap((m) =>
    (m.toolCalls ?? []).filter(
      (c) => c.name === "file.write" && c.arguments?.path === paths.candidate,
    ),
  );
  return {
    paths,
    results,
    writes,
    draftCalls,
    controlErrors,
    resetAt,
    modal,
    last: view.own.at(-1) ?? null,
  };
}

const collapsePads = (s) =>
  s.replace(/(.)\1{63,}/g, (run, c) => `[${run.length}×${c}]`);

/** A draft that names its writer, built from the source records read. */
function draftFrom(view, sourceText, label) {
  const source = JSON.parse(sourceText);
  const lines = source.records.map(({ message: m }) => {
    const body = collapsePads(
      String(m.content ?? "").replace(/^\[Received [^\]]*\]\n/, ""),
    );
    const calls = (m.toolCalls ?? []).map((c) => ` →${c.name}`).join("");
    return `- ${m.role}${m.name ? ` ${m.name}` : ""}: ${body.slice(0, 200)}${calls}`;
  });
  return [
    `L1 chunk ${view.chunk} (journal seq ${source.first_seq}–${source.last_seq}; draft by ${label})`,
    ...lines,
  ].join("\n");
}

/**
 * One scripted round of the memory branch. Returns the stage it acted in
 * and the decision: text (a control JSON) or file tool calls, canonically
 * named, all on the normal route.
 */
export function memoryAgentRound(view, { label, pageBytes = 32 * 1024 }) {
  const p = branchProgress(view);
  if (p.modal && p.last?.role === "user" && p.modal.i === view.own.length - 1)
    return {
      stage: "confirm",
      text: JSON.stringify({
        action: "confirm",
        version: p.modal.version,
        sha256: p.modal.sha256,
        token: p.modal.token,
        checks: {
          source_and_speakers: true,
          sequence_and_changes: true,
          uncertainty_and_relationship: true,
          no_new_conclusions: true,
          satisfied_with_this_version: true,
        },
      }),
      calls: [],
    };
  const read = (path, offset) => ({
    id: `mem-${randomUUID()}`,
    name: "file.read",
    arguments: { path, offset, len: pageBytes },
  });
  const pages = (path, after) =>
    p.results.filter(
      (x) =>
        x.i > after &&
        x.r.path === path &&
        typeof x.r.content_text === "string",
    );
  const nextOffset = (path, after) => {
    const got = pages(path, after).at(-1)?.r;
    if (!got) return 0;
    return got.has_more ? got.next_offset : null;
  };
  const lastWrite = p.writes.at(-1);
  if (!lastWrite) {
    const next = nextOffset(p.paths.source, -1);
    if (next !== null)
      return {
        stage: "read_source",
        text: "",
        calls: [read(p.paths.source, next)],
      };
    const sourceText = pages(p.paths.source, -1)
      .map((x) => x.r.content_text)
      .join("");
    return {
      stage: "write",
      text: "",
      calls: [
        {
          id: `mem-${randomUUID()}`,
          name: "file.write",
          arguments: {
            path: p.paths.candidate,
            content_text: draftFrom(view, sourceText, label),
            expect_version: "none",
          },
        },
      ],
    };
  }
  const after = Math.max(lastWrite.i, p.resetAt);
  const calls = [p.paths.source, p.paths.candidate].flatMap((path) => {
    const next = nextOffset(path, after);
    return next === null ? [] : [read(path, next)];
  });
  if (calls.length) return { stage: "reread", text: "", calls };
  return {
    stage: "review",
    text: JSON.stringify({
      action: "review",
      version: lastWrite.r.version,
      sha256: lastWrite.r.sha256,
    }),
    calls: [],
  };
}

// ------------------------------------------------ chat-completions wire ---

/** Chat-completions wire messages as provider ChatMessages (file tools
 * carry their canonical names; other names stay as sent). */
export function fromChatCompletions(messages) {
  const canonical = (n) =>
    n === "file_read" ? "file.read" : n === "file_write" ? "file.write" : n;
  return messages.map((m) => ({
    role: m.role,
    content: typeof m.content === "string" ? m.content : "",
    ...(m.tool_calls?.length
      ? {
          toolCalls: m.tool_calls.map((c) => {
            const args = parse(c.function?.arguments ?? "") ?? {};
            return {
              id: c.id,
              name: canonical(c.function?.name),
              route: args.route,
              arguments: args.input ?? {},
            };
          }),
        }
      : {}),
    ...(m.tool_call_id ? { toolCallId: m.tool_call_id } : {}),
  }));
}

/** One chat-completions SSE body for a scripted decision. */
export function chatCompletionsSSE(decision, usage = null) {
  const wire = (n) => n.replace(/[^a-zA-Z0-9_-]/g, "_");
  const chunks = [];
  if (decision.text)
    chunks.push({ choices: [{ delta: { content: decision.text } }] });
  decision.calls.forEach((c, index) =>
    chunks.push({
      choices: [
        {
          delta: {
            tool_calls: [
              {
                index,
                id: c.id,
                type: "function",
                function: {
                  name: wire(c.name),
                  arguments: JSON.stringify({
                    route: "normal",
                    input: c.arguments,
                  }),
                },
              },
            ],
          },
        },
      ],
    }),
  );
  chunks.push({
    choices: [
      {
        delta: {},
        finish_reason: decision.calls.length ? "tool_calls" : "stop",
      },
    ],
  });
  if (usage) chunks.push({ usage });
  return `${chunks.map((c) => `data: ${JSON.stringify(c)}\n\n`).join("")}data: [DONE]\n\n`;
}

// ------------------------------------------------------ file service ---

/**
 * Build and start the real workspace file service on `port`, sharing the
 * state database as a local install does. Its canonical root is stable per
 * database (the service binds one root to one database), under the OS temp
 * directory. Returns the env the state service needs to advertise file.*;
 * pass `token` when that service was started first.
 */
export async function startFileService({
  dbUrl,
  port,
  outDir,
  children,
  token = `e2e-files-${randomUUID()}`,
}) {
  const filesDir = resolve(import.meta.dirname, "../../files");
  const bin = join(outDir, "filesvc");
  const build = spawnSync(
    "go",
    ["build", "-buildvcs=false", "-o", bin, "./cmd/filesvc"],
    { cwd: filesDir, stdio: "inherit" },
  );
  if (build.status !== 0) throw new Error("filesvc build failed");
  const root = join(
    tmpdir(),
    `sumi-e2e-filesvc-${createHash("sha256").update(dbUrl).digest("hex").slice(0, 12)}`,
  );
  mkdirSync(root, { recursive: true });
  const url = `http://127.0.0.1:${port}`;
  const out = openSync(join(outDir, "filesvc.log"), "a");
  const proc = spawn(bin, [], {
    env: {
      ...process.env,
      FILESV_LISTEN: `127.0.0.1:${port}`,
      FILESV_ROOT: root,
      FILESV_DB_URL: dbUrl,
      FILESV_TOKENS: `${token}:*`,
    },
    stdio: ["ignore", out, out],
  });
  children?.push(proc);
  for (const deadline = Date.now() + 30_000; ; ) {
    try {
      if ((await fetch(`${url}/healthz`)).ok) break;
    } catch {
      /* not up */
    }
    if (proc.exitCode !== null || Date.now() > deadline)
      throw new Error(`filesvc did not become healthy (see ${outDir}/filesvc.log)`);
    await new Promise((r) => setTimeout(r, 200));
  }
  return {
    proc,
    root,
    env: { SUMI_FILESVC_URL: url, SUMI_FILESVC_TOKEN: token },
  };
}

/** Workspace entries under the service root named like branch files. */
export function memoryPathsInWorkspace(root) {
  const found = [];
  const walk = (dir, depth) => {
    if (depth > 6 || !existsSync(dir)) return;
    for (const e of readdirSync(dir, { withFileTypes: true })) {
      const p = join(dir, e.name);
      if (/candidate\.md$|source\.json$/.test(e.name)) found.push(p);
      if (e.isDirectory()) walk(p, depth + 1);
    }
  };
  walk(root, 0);
  return found;
}
