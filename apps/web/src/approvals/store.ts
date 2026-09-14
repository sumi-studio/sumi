import { create } from "zustand";
import {
  ApprovalsAPIError,
  decideCoreApproval,
  listCoreApprovals,
} from "./api";
import type { ApprovalDecision, CoreApproval } from "./model";

/**
 * The session human's durable approval inbox. The server row is the record:
 * this store is a projection that refreshes on the live `core_approval_changed`
 * nudge, on focus/visibility return, and on a slow poll — so a lost socket
 * event or a decision committed by another tab always converges.
 *
 * decide() latches one in-flight decision per approval and pins a
 * decision_id to the approval until it resolves: a retried send after a lost
 * response replays the recorded decision on the server instead of minting a
 * conflicting command.
 */
export type ApprovalsStatus = "idle" | "loading" | "ready" | "error";

interface ApprovalsState {
  status: ApprovalsStatus;
  pending: CoreApproval[];
  resolved: CoreApproval[];
  /** approval_id -> decision currently in flight. */
  deciding: Record<string, ApprovalDecision>;
  /** approval_id -> user-facing decision error code. */
  decisionErrors: Record<string, string>;
  refresh: () => Promise<void>;
  decide: (approval: CoreApproval, decision: ApprovalDecision) => Promise<void>;
  reset: () => void;
}

// decisionIDs pins one idempotency id per approval for the life of an
// unanswered decision: a lost response's retry must carry the identical id.
const decisionIDs = new Map<string, string>();

function sortResolved(approvals: CoreApproval[]): CoreApproval[] {
  return [...approvals].sort((a, b) =>
    (b.decided_at ?? "").localeCompare(a.decided_at ?? ""),
  );
}

export const useCoreApprovals = create<ApprovalsState>((set, get) => ({
  status: "idle",
  pending: [],
  resolved: [],
  deciding: {},
  decisionErrors: {},

  async refresh() {
    let approvals: CoreApproval[];
    try {
      approvals = await listCoreApprovals();
    } catch (error) {
      // A dead session is not a transient blip — clear the inbox so a stale
      // pending card is never left looking decidable.
      if (error instanceof ApprovalsAPIError && error.status === 401) {
        set({ status: "error", pending: [], resolved: [] });
        return;
      }
      set((state) => ({
        status: state.status === "idle" ? "error" : state.status,
      }));
      return;
    }
    const pending = approvals.filter((a) => a.status === "pending");
    const live = new Set(pending.map((a) => a.approval_id));
    for (const id of [...decisionIDs.keys()]) {
      if (!live.has(id)) decisionIDs.delete(id);
    }
    set({
      status: "ready",
      pending,
      resolved: sortResolved(approvals.filter((a) => a.status !== "pending")),
      deciding: Object.fromEntries(
        Object.entries(get().deciding).filter(([id]) => live.has(id)),
      ),
    });
  },

  async decide(approval, decision) {
    const state = get();
    if (state.deciding[approval.approval_id]) return;
    if (approval.status !== "pending") return;
    let decisionId = decisionIDs.get(approval.approval_id);
    if (!decisionId) {
      decisionId = crypto.randomUUID();
      decisionIDs.set(approval.approval_id, decisionId);
    }
    set((s) => ({
      deciding: { ...s.deciding, [approval.approval_id]: decision },
      decisionErrors: omitKey(s.decisionErrors, approval.approval_id),
    }));
    try {
      const resolved = await decideCoreApproval(
        approval.approval_id,
        decision,
        decisionId,
      );
      decisionIDs.delete(approval.approval_id);
      set((s) => ({
        pending: s.pending.filter(
          (a) => a.approval_id !== approval.approval_id,
        ),
        resolved: sortResolved([
          resolved,
          ...s.resolved.filter(
            (a) => a.approval_id !== approval.approval_id,
          ),
        ]),
        deciding: omitKey(s.deciding, approval.approval_id),
      }));
    } catch (error) {
      const code =
        error instanceof ApprovalsAPIError ? error.code : "network_error";
      const status = error instanceof ApprovalsAPIError ? error.status : 0;
      set((s) => ({
        deciding: omitKey(s.deciding, approval.approval_id),
        decisionErrors: {
          ...s.decisionErrors,
          [approval.approval_id]: code,
        },
      }));
      if (status === 401 || status === 403 || status === 404 || status === 409) {
        // The durable record moved without this tab — converge on it.
        await get().refresh();
      }
    }
  },

  reset() {
    decisionIDs.clear();
    set({
      status: "idle",
      pending: [],
      resolved: [],
      deciding: {},
      decisionErrors: {},
    });
  },
}));

function omitKey<T>(record: Record<string, T>, key: string): Record<string, T> {
  const next = { ...record };
  delete next[key];
  return next;
}

export function pendingApprovalCount(): number {
  return useCoreApprovals.getState().pending.length;
}
