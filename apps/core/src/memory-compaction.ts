import { ModelError, type ModelEvent, type ModelProvider } from "./provider.ts";
import { BudgetWaitError } from "./usage.ts";
import { FencedError, StateError, type StateClient } from "./state-client.ts";
import {
  advanceMemoryBranch,
  DEFAULT_MEMORY_POLICY,
  initialMemoryState,
  type MemoryBranch,
  type MemoryBranchState,
  type MemoryDecision,
  type MemoryPolicy,
  type MemorySnapshot,
} from "./memory-branch.ts";

export const DEFAULT_MEMORY_PREPARATION_TIMEOUT_MS = 10 * 60_000;
export type MemoryPreparationResult = "idle" | "worked" | "unavailable";
export interface MemoryPreparationDeps {
  personaId: string;
  generation: number;
  state: StateClient;
  provider: ModelProvider;
  snapshot?: MemorySnapshot;
  policy?: Partial<MemoryPolicy>;
  signal?: AbortSignal;
  timeoutMs?: number;
  log?: (message: string, fields?: Record<string, unknown>) => void;
}
function wait(ms: number, signal: AbortSignal) {
  return new Promise<void>((resolve, reject) => {
    if (signal.aborted) {
      reject(signal.reason);
      return;
    }
    const abort = () => {
      clearTimeout(timer);
      reject(signal.reason);
    };
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", abort);
      resolve();
    }, ms);
    signal.addEventListener("abort", abort, { once: true });
  });
}
async function collect(
  stream: AsyncIterable<ModelEvent>,
  signal: AbortSignal,
): Promise<MemoryDecision> {
  const it = stream[Symbol.asyncIterator]();
  let onAbort = () => {};
  const stop = new Promise<never>((_, reject) => {
    onAbort = () => reject(signal.reason ?? new Error("aborted"));
    if (signal.aborted) onAbort();
    else signal.addEventListener("abort", onAbort, { once: true });
  });
  stop.catch(() => {});
  const decision: MemoryDecision = { text: "", calls: [], usage: {} };
  let done = false;
  try {
    for (;;) {
      const next = await Promise.race([it.next(), stop]);
      if (next.done) break;
      const ev = next.value;
      if (ev.type === "text") decision.text += ev.delta;
      else if (ev.type === "tool_call") decision.calls.push(ev.call);
      else {
        decision.usage = ev.usage;
        decision.continuation = ev.continuation;
        done = true;
      }
    }
    if (
      !done ||
      (typeof decision.usage.finish_reason === "string" &&
        !["stop", "tool_calls"].includes(decision.usage.finish_reason))
    )
      throw new Error("memory model stream did not finish normally");
    return decision;
  } finally {
    signal.removeEventListener("abort", onAbort);
    if (signal.aborted) void Promise.resolve(it.return?.()).catch(() => {});
  }
}
/** The same pending checkpoint is retried byte-for-byte. Its private effects
 * have not escaped this branch, and the store deduplicates the revision. */
async function checkpoint(
  deps: MemoryPreparationDeps,
  branch: MemoryBranch,
  state: MemoryBranchState,
  signal: AbortSignal,
): Promise<MemoryBranch> {
  for (let failures = 0; ; failures++) {
    if (signal.aborted) throw signal.reason;
    try {
      return await deps.state.saveMemoryBranch(
        deps.personaId,
        deps.generation,
        branch.chunk.chunk_seq,
        branch.revision,
        state,
      );
    } catch (e) {
      if (
        e instanceof FencedError ||
        (e instanceof StateError && e.status < 500 && e.status !== 429)
      )
        throw e;
      deps.log?.(
        "memory checkpoint unavailable; saved branch remains authoritative",
        { chunk_seq: branch.chunk.chunk_seq },
      );
      await wait(Math.min(250 * 2 ** Math.min(failures, 5), 8_000), signal);
    }
  }
}
export async function runMemoryPreparation(
  deps: MemoryPreparationDeps,
): Promise<MemoryPreparationResult> {
  if (deps.signal?.aborted) return "idle";
  const controller = new AbortController();
  const onAbort = () =>
    controller.abort(deps.signal?.reason ?? new Error("host stopped"));
  deps.signal?.addEventListener("abort", onAbort, { once: true });
  const timeout = setTimeout(
    () => controller.abort(new Error("memory execution slice ended")),
    deps.timeoutMs ?? DEFAULT_MEMORY_PREPARATION_TIMEOUT_MS,
  );
  const signal = controller.signal;
  const policy = { ...DEFAULT_MEMORY_POLICY, ...deps.policy };
  let branch: MemoryBranch | null = null;
  let bindingSwitches = 0;
  try {
    branch = await deps.state.claimMemoryBranch(
      deps.personaId,
      deps.generation,
      deps.snapshot,
      policy,
    );
    if (!branch) return "idle";
    if (!branch.state)
      branch = await checkpoint(
        deps,
        branch,
        initialMemoryState(branch, policy),
        signal,
      );
    let state = branch.state!;
    if (state.in_flight) {
      const resumed = structuredClone(state);
      resumed.in_flight = null;
      resumed.interruptions = (resumed.interruptions ?? 0) + 1;
      if (resumed.interruptions >= resumed.policy.maxConsecutiveFailures) {
        resumed.status = "paused";
        resumed.pause_reason = "interrupted_round";
        resumed.retry_at = null;
        resumed.issue = {
          code: "memory_round_interrupted",
          message:
            "Memory preparation repeatedly stopped before a model round or its checkpoint completed. The last confirmed checkpoint and draft remain saved.",
        };
      }
      branch = await checkpoint(deps, branch, resumed, signal);
      state = branch.state!;
    }
    if (state.status === "prepared" || state.status === "kept") return "idle";
    if (
      !["file.read", "file.write"].every((name) =>
        branch!.snapshot.tools.some((tool) => tool.name === name),
      )
    ) {
      const paused = structuredClone(state);
      paused.status = "paused";
      paused.pause_reason = "private_tools_missing";
      paused.retry_at = null;
      paused.issue = {
        code: "memory_private_tools_missing",
        message:
          "The frozen parent tool definitions do not include both private file tools. The original context remains intact; this branch cannot execute its review workflow.",
      };
      await checkpoint(deps, branch, paused, signal);
      return "worked";
    }

    if (state.status === "paused") {
      const changedPolicy =
        JSON.stringify(state.policy) !== JSON.stringify(policy);
      const oldBinding = state.effective_binding ?? branch.snapshot.binding;
      const currentBinding = deps.snapshot?.binding;
      const changedBinding = !!(
        currentBinding &&
        oldBinding &&
        currentBinding.fingerprint !== oldBinding.fingerprint
      );
      if (
        state.retry_at === null &&
        !(changedPolicy && state.pause_reason === "configured_budget") &&
        !changedBinding
      )
        return "idle";
      if (
        state.retry_at !== null &&
        Date.parse(state.retry_at) > Date.now() &&
        !changedPolicy &&
        !changedBinding
      )
        return "idle";
      state = structuredClone(state);
      state.status = "running";
      state.pause_reason = null;
      state.retry_at = null;
      state.policy = policy;
      if (changedBinding) {
        state.effective_binding = currentBinding;
        state.failures = 0;
        (state.binding_changes ??= []).push({
          from: oldBinding!,
          to: currentBinding!,
          at_round: state.rounds,
        });
        state.review = null;
        state.final = null;
        state.source_read = [];
        state.candidate_read = [];
        state.messages.push({
          role: "user",
          content:
            "[Memory execution resumed after a model/connection change] The source and draft remain saved. Read both again and open a fresh confirmation before completing.",
        });
      }
      branch = await checkpoint(deps, branch, state, signal);
    }
    for (;;) {
      state = branch.state!;
      if (signal.aborted) return "idle";
      if (
        state.rounds >= state.policy.maxRounds ||
        (state.policy.maxTokens > 0 && state.tokens >= state.policy.maxTokens)
      ) {
        const paused = structuredClone(state);
        paused.status = "paused";
        paused.pause_reason = "configured_budget";
        paused.retry_at = null;
        paused.issue = {
          code: "memory_budget_exhausted",
          message:
            "Memory preparation reached its configured execution budget. Draft and progress remain saved; a larger budget can resume this exact branch.",
        };
        await checkpoint(deps, branch, paused, signal);
        return "worked";
      }
      // Persist admission before model execution. Repeated host/commit failures
      // remain observable after a restart, even when the failed save never landed.
      const admitted = structuredClone(state);
      admitted.in_flight = {
        round: state.rounds,
        started_at: new Date().toISOString(),
      };
      branch = await checkpoint(deps, branch, admitted, signal);
      state = branch.state!;
      let decision: MemoryDecision;
      try {
        const binding = state.effective_binding ?? branch.snapshot.binding;
        const model = binding?.model;
        decision = await collect(
          deps.provider.stream({
            personaId: deps.personaId,
            generation: deps.generation,
            turnId: `memory-l${branch.chunk.layer}-${branch.chunk.chunk_seq}`,
            phase: "memory",
            round: state.rounds,
            messages: [...branch.snapshot.messages, ...state.messages],
            tools: branch.snapshot.tools,
            ...(binding ? { bindingSnapshot: binding } : {}),
            ...(model === "gpt-6-astra" &&
            binding?.provider === "openai-responses"
              ? {
                  reasoningEffort: "medium" as const,
                  reasoningEffortAfter: branch.snapshot.messages.length,
                }
              : {}),
            signal,
          }),
          signal,
        );
      } catch (e) {
        if (signal.aborted || e instanceof FencedError) {
          if (e instanceof FencedError) throw e;
          return "idle";
        }
        const paused = structuredClone(state);
        paused.in_flight = null;
        paused.failures++;
        paused.status = "paused";
        if (e instanceof ModelError && e.cause === "model_binding_changed") {
          const binding = await deps.provider.snapshotBinding?.();
          const prior = state.effective_binding ?? branch.snapshot.binding;
          if (
            binding &&
            prior &&
            binding.fingerprint !== prior.fingerprint &&
            bindingSwitches++ < paused.policy.maxConsecutiveFailures
          ) {
            paused.effective_binding = binding;
            paused.failures = 0;
            (paused.binding_changes ??= []).push({
              from: prior,
              to: binding,
              at_round: state.rounds,
            });
            paused.review = null;
            paused.final = null;
            paused.source_read = [];
            paused.candidate_read = [];
            paused.status = "running";
            paused.pause_reason = null;
            paused.retry_at = null;
            paused.messages.push({
              role: "user",
              content:
                "[Memory branch execution setting changed] The selected model or connection changed. The original source, draft and branch transcript are retained. Read the source and current candidate again before opening a new confirmation; the previous confirmation is invalid. This is operational information, not a new experience for the replacement.",
            });
            branch = await checkpoint(deps, branch, paused, signal);
            continue;
          }
          paused.pause_reason = "model_binding_changed";
          paused.retry_at = new Date(Date.now() + 60_000).toISOString();
          paused.issue = {
            code: "memory_model_binding_changed",
            message:
              "Memory preparation could not resume under the changed model connection. Original context, draft and previous execution bindings remain saved.",
          };
          await checkpoint(deps, branch, paused, signal);
          return "unavailable";
        }
        if (e instanceof ModelError && e.refusal === "context_length") {
          paused.pause_reason = "context_capacity";
          paused.retry_at = null;
          paused.issue = {
            code: "memory_context_capacity",
            message:
              "The memory branch was refused for context capacity. Its exact context and draft are preserved; the same oversized request will not be resent automatically.",
          };
          await checkpoint(deps, branch, paused, signal);
          return "worked";
        }
        const unavailable =
          e instanceof BudgetWaitError ||
          (e instanceof ModelError && e.unavailable);
        paused.pause_reason = unavailable
          ? "model_unavailable"
          : "provider_error";
        paused.retry_at = new Date(
          Date.now() +
            Math.min(
              30_000 * 2 ** Math.min(paused.failures - 1, 5),
              15 * 60_000,
            ),
        ).toISOString();
        if (
          !unavailable &&
          paused.failures >= paused.policy.maxConsecutiveFailures
        ) {
          paused.retry_at = null;
          paused.issue = {
            code: "memory_provider_failure",
            message:
              "The memory branch repeatedly failed to obtain a complete model response. Its frozen context and draft remain saved.",
          };
        }
        branch = await checkpoint(deps, branch, paused, signal);
        return unavailable ? "unavailable" : "worked";
      }
      const next = await advanceMemoryBranch(branch, decision);
      next.in_flight = null;
      next.interruptions = 0;
      if (next.failures === 0 && next.status !== "paused") next.issue = null;
      try {
        branch = await checkpoint(deps, branch, next, signal);
      } catch (e) {
        if (e instanceof FencedError || signal.aborted) throw e;
        // A deterministic completion refusal must not silently discard the draft
        // or keep trying a stale confirmation. Record a durable mechanism fault.
        if (e instanceof StateError && e.status < 500) {
          const failed = structuredClone(branch.state!);
          failed.status = "paused";
          failed.pause_reason = "checkpoint_rejected";
          failed.retry_at = null;
          failed.issue = {
            code: "memory_checkpoint_rejected",
            message:
              "The memory mechanism rejected a checkpoint or completion. The last durable candidate and context remain saved; no replacement was applied.",
          };
          await checkpoint(deps, branch, failed, signal);
          return "worked";
        }
        throw e;
      }
      if (branch.state!.status !== "running") return "worked";
    }
  } catch (e) {
    if (e instanceof FencedError) throw e;
    if (signal.aborted) return "idle";
    deps.log?.("memory branch could not advance", {
      error: String(e),
      chunk_seq: branch?.chunk.chunk_seq,
    });
    return "unavailable";
  } finally {
    clearTimeout(timeout);
    deps.signal?.removeEventListener("abort", onAbort);
  }
}
