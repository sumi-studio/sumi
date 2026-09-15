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
 *
 * Three fences keep asynchronous continuations honest:
 * - inboxEpoch bounds every continuation to the account/session lifetime
 *   that issued it. reset() — run when the inbox unmounts on logout or the
 *   signed-in human changes — invalidates in-flight refreshes and decisions,
 *   so a late response can never write another person's rows or errors.
 * - refreshSeq keeps the newest issued refresh authoritative: when several
 *   are in flight (poll + focus + live nudge) only the last issued may
 *   commit, whichever response arrives first.
 * - commitVersion blocks a snapshot issued before an intervening decision
 *   commit, so a pre-decision response cannot resurrect a resolved card.
 *
 * `owner` tags the loaded data with the session human the server said it
 * belongs to. Components render rows only while owner matches their
 * accountID, so an account switch can never commit another person's
 * approvals into the DOM — even for the single render before the sync
 * effect's cleanup runs.
 */
export type ApprovalsStatus = "idle" | "loading" | "ready" | "error";

interface ApprovalsState {
  status: ApprovalsStatus;
  /** Session human the loaded rows describe; null until a refresh lands. */
  owner: string | null;
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

let inboxEpoch = 0;
let commitVersion = 0;
let refreshSeq = 0;

function sortResolved(approvals: CoreApproval[]): CoreApproval[] {
  return [...approvals].sort((a, b) =>
    (b.decided_at ?? "").localeCompare(a.decided_at ?? ""),
  );
}

export const useCoreApprovals = create<ApprovalsState>((set, get) => ({
  status: "idle",
  owner: null,
  pending: [],
  resolved: [],
  deciding: {},
  decisionErrors: {},

  async refresh() {
    const epoch = inboxEpoch;
    const version = commitVersion;
    const seq = ++refreshSeq;
    const current = () =>
      epoch === inboxEpoch && seq === refreshSeq && version === commitVersion;
    // A first (or post-error) load is honestly "loading", never "empty".
    if (get().status !== "ready") set({ status: "loading" });
    let body: Awaited<ReturnType<typeof listCoreApprovals>>;
    try {
      body = await listCoreApprovals();
    } catch (error) {
      if (!current()) return;
      // A dead session is not a transient blip — clear the inbox so a stale
      // pending card is never left looking decidable.
      if (error instanceof ApprovalsAPIError && error.status === 401) {
        commitVersion++;
        set({ status: "error", owner: null, pending: [], resolved: [] });
        return;
      }
      // A transient failure keeps last-known-good data; only a load with
      // nothing honest to show reports an error instead of a fake "empty".
      commitVersion++;
      set((state) => ({
        status: state.status === "ready" ? "ready" : "error",
      }));
      return;
    }
    if (!current()) return;
    commitVersion++;
    const approvals = body.approvals;
    const pending = approvals.filter((a) => a.status === "pending");
    const live = new Set(pending.map((a) => a.approval_id));
    for (const id of [...decisionIDs.keys()]) {
      if (!live.has(id)) decisionIDs.delete(id);
    }
    set({
      status: "ready",
      // The server told us whose inbox this is; renders that no longer
      // belong to that human must never see these rows.
      owner: body.human ?? null,
      pending,
      resolved: sortResolved(approvals.filter((a) => a.status !== "pending")),
      deciding: Object.fromEntries(
        Object.entries(get().deciding).filter(([id]) => live.has(id)),
      ),
    });
  },

  async decide(approval, decision) {
    const epoch = inboxEpoch;
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
      if (epoch !== inboxEpoch) return;
      commitVersion++;
      decisionIDs.delete(approval.approval_id);
      set((s) => ({
        pending: s.pending.filter(
          (a) => a.approval_id !== approval.approval_id,
        ),
        resolved: sortResolved([
          resolved,
          ...s.resolved.filter((a) => a.approval_id !== approval.approval_id),
        ]),
        deciding: omitKey(s.deciding, approval.approval_id),
      }));
    } catch (error) {
      if (epoch !== inboxEpoch) return;
      commitVersion++;
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
      if (
        status === 401 ||
        status === 403 ||
        status === 404 ||
        status === 409
      ) {
        // The durable record moved without this tab — converge on it.
        await get().refresh();
      }
    }
  },

  reset() {
    inboxEpoch++;
    commitVersion++;
    decisionIDs.clear();
    set({
      status: "idle",
      owner: null,
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
