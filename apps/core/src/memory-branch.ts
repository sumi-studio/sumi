import { estTextTokens } from "./memory.ts";
import { L0_TO_L1_INSTRUCTION } from "./memory-instructions/l0-to-l1.ts";
import { L1_TO_L2_INSTRUCTION } from "./memory-instructions/l1-to-l2.ts";
import { L2_TO_L2_INSTRUCTION } from "./memory-instructions/l2-to-l2.ts";
import { memoryTaskHeader } from "./memory-instructions/task.ts";
import { memoryWorkspaceInstruction } from "./memory-instructions/workspace.ts";
/** Durable, private workspace of one asynchronous memory branch. No ordinary
 * tool executor is reachable from this module. Every effect is a checkpoint. */
import type {
  ChatMessage,
  ModelBindingSnapshot,
  ProviderContinuation,
  ToolCall,
  ToolSpec,
} from "./provider.ts";
import type { MemoryChunk } from "./types.ts";

export interface MemorySourceRange {
  first_seq: number;
  last_seq: number;
  /** Index into the exact frozen messages; absent for non-rendered bookkeeping. */
  message_index?: number;
  layer?: number;
  chunk_seq?: number;
}
export interface MemorySnapshot {
  messages: ChatMessage[];
  tools: ToolSpec[];
  ranges: MemorySourceRange[];
  binding?: ModelBindingSnapshot;
}
export interface MemoryPolicy {
  maxRounds: number;
  /** Total reported input + output tokens; zero disables this optional cap. */
  maxTokens: number;
  maxConsecutiveFailures: number;
}
export const DEFAULT_MEMORY_POLICY: MemoryPolicy = {
  maxRounds: 32,
  maxTokens: 0,
  maxConsecutiveFailures: 3,
};
export interface MemoryCandidate {
  text: string;
  version: number;
  sha256: string;
}
export interface MemoryReview {
  kind: "replace" | "keep";
  version: number;
  sha256: string;
  token: string;
  opened_round: number;
}
export interface MemoryBranchState {
  messages: ChatMessage[];
  candidate: MemoryCandidate | null;
  candidate_read: [number, number][];
  source_read: [number, number][];
  review: MemoryReview | null;
  final: MemoryReview | null;
  rounds: number;
  /** Durable admissions include failed/interrupted requests, bounding retries. */
  model_calls?: number;
  budget_extension?: { rounds: number; tokens: number };
  rebranch_requested?: boolean;
  restart_budget?: { rounds: number; tokens: number };
  execution_budget?: { rounds: number; tokens: number };
  tokens: number;
  failures: number;
  status: "running" | "paused" | "prepared" | "kept";
  pause_reason: string | null;
  retry_at: string | null;
  issue: { code: string; message: string } | null;
  policy: MemoryPolicy;
  effective_binding?: ModelBindingSnapshot;
  binding_changes?: {
    from: ModelBindingSnapshot;
    to: ModelBindingSnapshot;
    at_round: number;
  }[];
  in_flight?: { round: number; started_at: string } | null;
  interruptions?: number;
}
export interface MemoryBranch {
  chunk: MemoryChunk;
  snapshot: MemorySnapshot;
  state: MemoryBranchState | null;
  revision: number;
  previous_attempt?: MemoryBranchState;
}
export interface MemoryDecision {
  text: string;
  calls: ToolCall[];
  usage: Record<string, unknown>;
  continuation?: ProviderContinuation;
}
const encoder = new TextEncoder();
export const MEMORY_CHECKS = [
  "source_and_speakers",
  "sequence_and_changes",
  "uncertainty_and_relationship",
  "no_new_conclusions",
  "satisfied_with_this_version",
] as const;
const CHECK_EXPLANATIONS = {
  source_and_speakers:
    "今回の対象と照らし、誰の言葉・行為・観測・理解なのかを見直した",
  sequence_and_changes: "出来事の経過と、途中で変わった理解を見直した",
  uncertainty_and_relationship:
    "未確定のことや、人とのやり取りの具体がどう残るかを見直した",
  no_new_conclusions: "対象の経験と、この整理中に考えたことを混同していない",
  satisfied_with_this_version:
    "確認対象の版を読み直し、今回の整理の目的に照らして全体に納得できる",
};
export function memoryPaths(chunk: number) {
  return {
    candidate: `memory/${chunk}/candidate.md`,
    source: `memory/${chunk}/source.json`,
  };
}
export async function sha256(text: string): Promise<string> {
  return [
    ...new Uint8Array(
      await crypto.subtle.digest("SHA-256", encoder.encode(text)),
    ),
  ]
    .map((x) => x.toString(16).padStart(2, "0"))
    .join("");
}
/** The only source file: copies of messages already in the frozen prefix.
 * It never consults the journal, history, network, or another file. */
export function memorySource(branch: MemoryBranch): string {
  const ranges = branch.snapshot.ranges.filter(
    (r) =>
      r.first_seq >= branch.chunk.first_seq &&
      r.last_seq <= branch.chunk.last_seq,
  );
  const byMessage = new Map<number, MemorySourceRange[]>();
  for (const r of ranges)
    if (r.message_index !== undefined) {
      const group = byMessage.get(r.message_index) ?? [];
      group.push(r);
      byMessage.set(r.message_index, group);
    }
  return JSON.stringify({
    first_seq: branch.chunk.first_seq,
    last_seq: branch.chunk.last_seq,
    records: [...byMessage.entries()]
      .sort(([a], [b]) => a - b)
      .map(([index, group]) => ({
        first_seq: Math.min(...group.map((r) => r.first_seq)),
        last_seq: Math.max(...group.map((r) => r.last_seq)),
        journal_ranges: group.map((r) => ({
          first_seq: r.first_seq,
          last_seq: r.last_seq,
        })),
        message: branch.snapshot.messages[index],
      })),
  });
}
export function memoryInstruction(branch: MemoryBranch): string {
  const { candidate, source } = memoryPaths(branch.chunk.chunk_seq);
  const sourceLayer = branch.snapshot.ranges.find(
    (r) => r.first_seq === branch.chunk.first_seq,
  )?.layer;
  const transition =
    branch.chunk.layer === 1
      ? "l0_to_l1"
      : sourceLayer === 2
        ? "l2_to_l2"
        : "l1_to_l2";
  const purpose = {
    l0_to_l1: L0_TO_L1_INSTRUCTION,
    l1_to_l2: L1_TO_L2_INSTRUCTION,
    l2_to_l2: L2_TO_L2_INSTRUCTION,
  }[transition];
  const task = memoryTaskHeader({
    transition,
    chunk_seq: branch.chunk.chunk_seq,
    first_seq: branch.chunk.first_seq,
    last_seq: branch.chunk.last_seq,
  });
  return `${task}\n\n${purpose}\n\n${memoryWorkspaceInstruction({
    firstSeq: branch.chunk.first_seq,
    lastSeq: branch.chunk.last_seq,
    source,
    candidate,
    checks: MEMORY_CHECKS,
  })}`;
}
export function initialMemoryState(
  branch: MemoryBranch,
  policy: MemoryPolicy,
): MemoryBranchState {
  for (const [k, value] of Object.entries(policy)) {
    if (!Number.isSafeInteger(value) || value < (k === "maxTokens" ? 0 : 1))
      throw new Error(`invalid memory policy ${k}`);
  }
  return {
    messages: [
      {
        role: "user",
        content:
          memoryInstruction(branch) +
          (branch.previous_attempt
            ? "\nThis is a new attempt from a newly observed parent context, not continuation of the old cache prefix. The previous attempt remains archived. Its candidate, if any, is an unreviewed draft; read the current source and candidate before reviewing."
            : ""),
      },
    ],
    candidate: branch.previous_attempt?.candidate ?? null,
    candidate_read: [],
    source_read: [],
    review: null,
    final: null,
    rounds: 0,
    model_calls: 0,
    tokens: 0,
    failures: 0,
    status: "running",
    pause_reason: null,
    retry_at: null,
    issue: branch.previous_attempt?.issue ?? null,
    policy,
    execution_budget: branch.previous_attempt?.restart_budget,
    effective_binding: branch.snapshot.binding,
    binding_changes: [],
    in_flight: null,
    interruptions: 0,
  };
}
function completeRead(ranges: [number, number][], length: number): boolean {
  let end = 0;
  for (const [a, b] of [...ranges].sort((x, y) => x[0] - y[0])) {
    if (a > end) return false;
    end = Math.max(end, b);
  }
  return end >= length;
}
function tokenCount(usage: Record<string, unknown>): number {
  const total = usage.total_tokens;
  if (typeof total === "number" && Number.isFinite(total) && total >= 0)
    return total;
  return [
    usage.input_tokens ?? usage.prompt_tokens,
    usage.output_tokens ?? usage.completion_tokens,
  ].reduce<number>(
    (n, v) =>
      n + (typeof v === "number" && Number.isFinite(v) && v >= 0 ? v : 0),
    0,
  );
}
function strictKeys(value: Record<string, unknown>, keys: string[]) {
  return Object.keys(value).every((k) => keys.includes(k));
}
async function privateFile(
  branch: MemoryBranch,
  state: MemoryBranchState,
  call: ToolCall,
): Promise<Record<string, unknown>> {
  const paths = memoryPaths(branch.chunk.chunk_seq);
  if (
    call.route !== "normal" ||
    !["file.read", "file.write"].includes(call.name)
  )
    return { error: "memory_branch_permission_denied" };
  const p = call.arguments;
  if (p.path !== paths.candidate && p.path !== paths.source)
    return { error: "memory_branch_path_denied" };
  if (call.name === "file.write") {
    if (p.path !== paths.candidate)
      return { error: "memory_source_is_read_only" };
    if (
      !strictKeys(p, [
        "path",
        "content_text",
        "content_base64",
        "expect_version",
      ])
    )
      return { error: "invalid_file_arguments" };
    let text: string;
    try {
      if (
        (typeof p.content_text === "string") ===
        (typeof p.content_base64 === "string")
      )
        return { error: "provide_one_content_encoding" };
      text =
        typeof p.content_text === "string"
          ? p.content_text
          : new TextDecoder("utf-8", { fatal: true }).decode(
              Uint8Array.from(atob(p.content_base64 as string), (c) =>
                c.charCodeAt(0),
              ),
            );
    } catch {
      return { error: "candidate_must_be_utf8_text" };
    }
    if (encoder.encode(text).length > 2 * 1024 * 1024 || text.includes("\0"))
      return { error: "candidate_too_large_or_contains_nul" };
    const expected = p.expect_version ?? "none";
    if (expected !== (state.candidate?.version ?? "none"))
      return {
        error: "version_conflict",
        version: state.candidate?.version ?? null,
      };
    state.candidate = {
      text,
      version: (state.candidate?.version ?? 0) + 1,
      sha256: await sha256(text),
    };
    state.candidate_read = [];
    state.source_read = [];
    state.review = null;
    return {
      path: paths.candidate,
      version: state.candidate.version,
      sha256: state.candidate.sha256,
      size_bytes: encoder.encode(text).length,
    };
  }
  if (!strictKeys(p, ["path", "offset", "len"]))
    return { error: "invalid_file_arguments" };
  const source = p.path === paths.source;
  const text = source ? memorySource(branch) : state.candidate?.text;
  if (text === undefined) return { error: "file_not_found" };
  const bytes = encoder.encode(text);
  const offset = p.offset ?? 0,
    len = p.len ?? 64 * 1024;
  if (
    !Number.isSafeInteger(offset) ||
    !Number.isSafeInteger(len) ||
    (offset as number) < 0 ||
    (len as number) < 1 ||
    (len as number) > 1048576 ||
    (offset as number) > bytes.length
  )
    return { error: "invalid_file_range" };
  const start = offset as number;
  let end = Math.min(bytes.length, start + (len as number));
  // Do not split UTF-8 code points across pages, and reject mid-codepoint offsets.
  if (start < bytes.length && (bytes[start]! & 0xc0) === 0x80)
    return { error: "offset_not_utf8_boundary" };
  while (end > start && end < bytes.length && (bytes[end]! & 0xc0) === 0x80)
    end--;
  if (end === start && start < bytes.length)
    return { error: "len_too_small_for_utf8_character" };
  (source ? state.source_read : state.candidate_read).push([start, end]);
  return {
    path: p.path,
    content_text: new TextDecoder().decode(bytes.slice(start, end)),
    offset: start,
    bytes_returned: end - start,
    next_offset: end,
    has_more: end < bytes.length,
    version: source ? 1 : state.candidate!.version,
    sha256: source ? await sha256(text) : state.candidate!.sha256,
  };
}
export async function advanceMemoryBranch(
  branch: MemoryBranch,
  decision: MemoryDecision,
): Promise<MemoryBranchState> {
  if (!branch.state || branch.state.status !== "running")
    throw new Error("memory branch is not running");
  const state = structuredClone(branch.state);
  state.rounds++;
  state.tokens += tokenCount(decision.usage);
  state.messages.push({
    role: "assistant",
    content: decision.text,
    ...(decision.calls.length ? { toolCalls: decision.calls } : {}),
    ...(decision.continuation ? { continuation: decision.continuation } : {}),
  });
  if (decision.calls.length) {
    for (const call of decision.calls)
      state.messages.push({
        role: "tool",
        toolCallId: call.id,
        name: call.name,
        content: JSON.stringify(await privateFile(branch, state, call)),
      });
    state.failures = 0;
    return state;
  }
  let error = "";
  try {
    const control = JSON.parse(decision.text) as Record<string, unknown>;
    if (!control || Array.isArray(control) || typeof control !== "object")
      throw new Error("expected JSON object");
    const source = memorySource(branch);
    if (control.action === "review" || control.action === "review_keep") {
      const keep = control.action === "review_keep";
      if (
        !strictKeys(
          control,
          keep ? ["action"] : ["action", "version", "sha256"],
        )
      )
        throw new Error("unexpected review fields");
      if (!completeRead(state.source_read, encoder.encode(source).length))
        throw new Error(
          "read the complete source after the latest candidate edit",
        );
      if (
        !keep &&
        (!state.candidate ||
          control.version !== state.candidate.version ||
          control.sha256 !== state.candidate.sha256 ||
          !completeRead(
            state.candidate_read,
            encoder.encode(state.candidate.text).length,
          ))
      )
        throw new Error(
          "read the complete current candidate and use its exact version and sha256",
        );
      if (!keep && !state.candidate!.text.trim())
        throw new Error("empty candidate cannot replace memory");
      if (
        !keep &&
        estTextTokens(state.candidate!.text) >= branch.chunk.est_tokens
      )
        throw new Error(
          "The draft is not smaller than the target. Revise and review again if you can retain what matters in less space, or use review_keep. No target content has been removed.",
        );
      state.review = {
        kind: keep ? "keep" : "replace",
        version: keep ? 0 : state.candidate!.version,
        sha256: keep ? await sha256(source) : state.candidate!.sha256,
        token: crypto.randomUUID(),
        opened_round: state.rounds,
      };
      state.messages.push({
        role: "user",
        content: JSON.stringify({
          memory_confirmation: state.review,
          checks: CHECK_EXPLANATIONS,
          instruction:
            "Confirm this exact version in a separate response, or revise it and read/review again. This is a branch control message, not a message from your person.",
        }),
      });
    } else if (control.action === "confirm") {
      if (
        !strictKeys(control, ["action", "version", "sha256", "token", "checks"])
      )
        throw new Error("unexpected confirmation fields");
      const r = state.review;
      const checks = control.checks as Record<string, unknown> | undefined;
      if (
        !r ||
        r.opened_round >= state.rounds ||
        control.version !== r.version ||
        control.sha256 !== r.sha256 ||
        control.token !== r.token ||
        !checks ||
        !strictKeys(checks, [...MEMORY_CHECKS]) ||
        !MEMORY_CHECKS.every((k) => checks[k] === true)
      )
        throw new Error(
          "confirmation does not match the open version and complete checklist",
        );
      if (
        r.kind === "replace" &&
        (state.candidate?.version !== r.version ||
          state.candidate.sha256 !== r.sha256)
      )
        throw new Error("candidate changed after review");
      state.final = structuredClone(r);
      state.status = r.kind === "keep" ? "kept" : "prepared";
    } else throw new Error("use file tools, review, review_keep, or confirm");
    state.failures = 0;
  } catch (e) {
    error = e instanceof Error ? e.message : String(e);
  }
  if (error) {
    const normalDraftFeedback = error.startsWith("The draft is not smaller");
    state.failures = normalDraftFeedback ? 0 : state.failures + 1;
    state.messages.push({
      role: "user",
      content: JSON.stringify({
        memory_control_error: error,
        candidate: state.candidate
          ? { version: state.candidate.version, sha256: state.candidate.sha256 }
          : null,
      }),
    });
    if (state.failures >= state.policy.maxConsecutiveFailures) {
      state.status = "paused";
      state.pause_reason = "control_protocol";
      state.retry_at = new Date(Date.now() + 15 * 60_000).toISOString();
      state.issue = {
        code: "memory_control_protocol",
        message:
          "Memory preparation could not complete its review/confirmation protocol. Draft and branch context remain saved.",
      };
    }
  }
  return state;
}
