import { estTextTokens } from "./memory.ts";
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
  source_and_speakers: "原文と文脈に照らし、誰の発言・行為・観測かを保った",
  sequence_and_changes: "順序、途中で変わった理解や訂正を保った",
  uncertainty_and_relationship:
    "未確定のことや共に過ごした具体を、確定事項や仕事の一覧に変えていない",
  no_new_conclusions:
    "対象外の出来事・教訓・後からの解釈や真偽の評価を当時の事実として足していない",
  satisfied_with_this_version:
    "この版を読み直し、全体として納得できる。必要な修正後にも再度見直した",
};
export const COMPACTION_PURPOSES = {
  l1: "対象はまだ整理していない出来事。まず重複する外枠や同じ本文の再掲を減らす。同じ言葉や行為が再び起きた事実は消さない。原文のまま残す部分があってよい。",
  l2: "対象は以前整理したL1断片。断片間の重複やつながりを整理する。抽象化そのものを目的にせず、その期間のやり取りを辿れる形にする。",
  reintegrate:
    "対象はL2断片の再整理。まとまり方を変えてよいが、矛盾や理解の変化を消して一貫した人物像や物語へ揃えない。",
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
  return JSON.stringify({
    first_seq: branch.chunk.first_seq,
    last_seq: branch.chunk.last_seq,
    records: ranges.flatMap((r) =>
      r.message_index === undefined
        ? []
        : [
            {
              first_seq: r.first_seq,
              last_seq: r.last_seq,
              message: branch.snapshot.messages[r.message_index],
            },
          ],
    ),
  });
}
export function memoryInstruction(branch: MemoryBranch): string {
  const { candidate, source } = memoryPaths(branch.chunk.chunk_seq);
  const sourceLayer = branch.snapshot.ranges.find(
    (r) => r.first_seq === branch.chunk.first_seq,
  )?.layer;
  const purpose =
    branch.chunk.layer === 1
      ? COMPACTION_PURPOSES.l1
      : sourceLayer === 2
        ? COMPACTION_PURPOSES.reintegrate
        : COMPACTION_PURPOSES.l2;
  return `ここからは非同期の記憶整理の分岐。上のmessagesとtool definitionsは発火時点のまま固定されている。本体はその後も別のやり取りを続け得る。この分岐の新しい経験を本体の過去として足さない。\n\n${purpose}\n対象はjournal seq ${branch.chunk.first_seq}〜${branch.chunk.last_seq}。前後は読み解くために使い、置換する出来事とそのときの理解は対象内に保つ。発言者、言葉や数値、順序、観測と解釈、仮の考えと未確認、理解が変わった過程を区別する。今の仕事への有用性だけで選ばず、人と共に過ごした具体を一律に課題一覧・固定属性・教訓へ変えない。必要な表現は長く残してよい。一定の圧縮率や見出しはない。元の対話に訂正や否定がない限り、発言者の違いから誤り・創作・誤解を新しく判定しない。他の人が同じ内容を言っていないこと、沈黙や非復唱を否定の証拠にしない。誰が何を言ったかを残すことと、後から真偽を査定することは異なる。既存の人名・道具名・IDは勝手に改称せず同じ表記を使う。\n\nこの分岐で実行できるのはnormal routeのfile.readとfile.writeだけ。読めるのは ${source} と ${candidate}、書けるのは ${candidate} のみ。sourceは上の固定文脈に含まれる対象のコピーで、更新されない。別のpath、terminal、Messaging、conversation_history、その他の探索や外部操作は実行できない。tool definitionsは元のままだが実行権限はこの範囲に限定される。\n\n初稿をcandidateへ書く。次にsourceとその版のcandidateを最後まで読み、上の自分の文脈と照合し、全体を見直す。直したら、修正版とsourceをもう一度読んで見直す。短くしたことだけで終わりにしない。file.readはoffset/lenでページを続けられる。file.writeの最初はexpect_version:"none"、更新は直前のversionを指定する。\n\n納得できたら、ツールなしの本文で厳密なJSON {"action":"review","version":候補のversion,"sha256":"候補のsha256"} を返す。確認画面が返るので、その対象版とchecklistを見て、別の応答で {"action":"confirm","version":同じversion,"sha256":"同じhash","token":"確認画面のtoken","checks":{各checkの名前:true}} と確定する。修正すれば以前の確認は無効になる。チェックの自己申告は内容品質を証明しないので、実際に読んで判断する。\n\n今回は置換しないと判断した場合は、sourceを最後まで読み、{"action":"review_keep"} でその判断の確認を開く。保持も同じconfirm形式で確定する。検討や草稿はこの分岐内に留まり、本体や人間へ逐一報告しない。仕組みの異常は実行側が別に記録する。`;
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
    messages: [{ role: "user", content: memoryInstruction(branch) }],
    candidate: null,
    candidate_read: [],
    source_read: [],
    review: null,
    final: null,
    rounds: 0,
    tokens: 0,
    failures: 0,
    status: "running",
    pause_reason: null,
    retry_at: null,
    issue: null,
    policy,
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
      state.issue = {
        code: "memory_control_protocol",
        message:
          "Memory preparation could not complete its review/confirmation protocol. Draft and branch context remain saved.",
      };
    }
  }
  return state;
}
