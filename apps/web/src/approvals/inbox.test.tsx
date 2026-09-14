// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { TooltipProvider } from "@sumi/ui/components/tooltip";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { CoreApprovalsInbox } from "./inbox";
import type { CoreApproval } from "./model";
import { useCoreApprovals } from "./store";

/**
 * Mounted lifecycle coverage for the approval inbox's account boundary. The
 * harness reproduces the production wiring — app-rail mounts the component
 * only while a session user exists and passes that user's id — so logout is
 * the real unmount and account replacement the real prop change, not a test
 * reaching inside the store. fetch is the only stubbed boundary.
 */

function approval(over: Partial<CoreApproval> = {}): CoreApproval {
  return {
    approval_id: "a-1",
    persona_id: "p-1",
    input_id: "in-1",
    call_index: 0,
    operation_id: "op-1",
    turn_id: "t-1",
    tool: "message.send",
    route: "elevated",
    required_by: "route",
    request: { text: "account A private approval text" },
    action_digest: "digest-1",
    status: "pending",
    decision: null,
    decided_at: null,
    consumed_at: null,
    created_at: "2026-09-15T00:00:00Z",
    secretary_name: "A-secretary",
    ...over,
  };
}

function jsonResponse(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function deferred<T>() {
  let resolve!: (v: T) => void;
  let reject!: (e: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

/** The production mount condition: rendered only for a session human. */
function Rail({ user }: { user: { id: string } | null }) {
  return (
    <TooltipProvider>
      {user ? <CoreApprovalsInbox accountID={user.id} /> : null}
    </TooltipProvider>
  );
}

async function flush() {
  await act(async () => {});
}

describe("approval inbox across logout/login", () => {
  beforeEach(() => {
    useCoreApprovals.getState().reset();
  });
  afterEach(() => {
    cleanup();
    useCoreApprovals.getState().reset();
    vi.unstubAllGlobals();
  });

  it("clears A's inbox on logout and fences A's late refresh under B", async () => {
    // Account A's mount refresh stays in flight across the switch.
    const aRefresh = deferred<Response>();
    const fetchMock = vi.fn().mockImplementationOnce(() => aRefresh.promise);
    vi.stubGlobal("fetch", fetchMock);

    const view = render(<Rail user={{ id: "human-a" }} />);
    await flush();

    // In-place logout -> login B: unmount resets, B's mount refreshes.
    const bRefresh = deferred<Response>();
    fetchMock.mockImplementationOnce(() => bRefresh.promise);
    view.rerender(<Rail user={null} />);
    view.rerender(<Rail user={{ id: "human-b" }} />);
    await flush();

    // B must see neither A's rows nor a claim that nothing is pending yet.
    const during = useCoreApprovals.getState();
    expect(during.pending).toHaveLength(0);
    expect(during.status).toBe("loading");

    await act(async () => {
      bRefresh.resolve(jsonResponse(200, { approvals: [] }));
    });
    expect(useCoreApprovals.getState().status).toBe("ready");

    // A's slow response lands last — fenced to A's ended lifetime.
    await act(async () => {
      aRefresh.resolve(jsonResponse(200, { approvals: [approval()] }));
    });
    const s = useCoreApprovals.getState();
    expect(s.pending).toHaveLength(0);
    expect(s.status).toBe("ready");
  });

  it("failed first refresh under B surfaces an error, not A's rows or a fake empty", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse(200, { approvals: [approval()] }))
      .mockResolvedValueOnce(jsonResponse(503, { error: "unavailable" }));
    vi.stubGlobal("fetch", fetchMock);

    const view = render(<Rail user={{ id: "human-a" }} />);
    await flush();
    expect(useCoreApprovals.getState().pending).toHaveLength(1);

    view.rerender(<Rail user={null} />);
    view.rerender(<Rail user={{ id: "human-b" }} />);
    await flush();

    const s = useCoreApprovals.getState();
    expect(s.status).toBe("error");
    expect(s.pending).toHaveLength(0);

    // The popover tells B the inbox failed to load — it cannot announce
    // "no pending approvals" for a state that is unknown.
    fireEvent.click(screen.getByRole("button", { name: "承認" }));
    expect(
      await screen.findByText("承認の一覧を読み込めませんでした。"),
    ).toBeInTheDocument();
    expect(screen.queryByText("承認待ちはありません")).not.toBeInTheDocument();
    expect(screen.queryByText(/A-secretary/)).not.toBeInTheDocument();
  });

  it("fences a stale in-flight decision callback across the account change", async () => {
    const decideResp = deferred<Response>();
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse(200, { approvals: [approval()] }))
      .mockImplementationOnce(() => decideResp.promise) // POST /decision
      .mockResolvedValueOnce(jsonResponse(200, { approvals: [] })); // B's inbox
    vi.stubGlobal("fetch", fetchMock);

    const view = render(<Rail user={{ id: "human-a" }} />);
    await flush();

    // A sends a decision; the response has not arrived when the account ends.
    const pending = useCoreApprovals.getState().pending[0];
    let deciding!: Promise<void>;
    await act(async () => {
      deciding = useCoreApprovals.getState().decide(pending, "approve_once");
    });
    expect(useCoreApprovals.getState().deciding["a-1"]).toBe("approve_once");

    view.rerender(<Rail user={null} />);
    view.rerender(<Rail user={{ id: "human-b" }} />);
    await flush();
    expect(useCoreApprovals.getState().pending).toHaveLength(0);

    // A's decision resolves late: nothing may write into B's inbox.
    await act(async () => {
      decideResp.resolve(
        jsonResponse(200, {
          approval: approval({
            status: "approved",
            decision: "approve_once",
            decided_at: "2026-09-15T01:00:00Z",
          }),
        }),
      );
    });
    await deciding;
    const s = useCoreApprovals.getState();
    expect(s.pending).toHaveLength(0);
    expect(s.resolved).toHaveLength(0);
    expect(s.decisionErrors["a-1"]).toBeUndefined();
    expect(fetchMock).toHaveBeenCalledTimes(3); // no converge-refresh spawned
  });

  it("account replacement while mounted drops the prior account's state", async () => {
    const aRefresh = deferred<Response>();
    const fetchMock = vi
      .fn()
      .mockImplementationOnce(() => aRefresh.promise)
      .mockResolvedValueOnce(
        jsonResponse(200, {
          approvals: [
            approval({ approval_id: "b-1", secretary_name: "B-secretary" }),
          ],
        }),
      );
    vi.stubGlobal("fetch", fetchMock);

    const view = render(<Rail user={{ id: "human-a" }} />);
    await flush();

    // The session's human changes without an unauthenticated interstitial.
    view.rerender(<Rail user={{ id: "human-b" }} />);
    await flush();
    const s = useCoreApprovals.getState();
    expect(s.pending.map((a) => a.approval_id)).toEqual(["b-1"]);

    await act(async () => {
      aRefresh.resolve(jsonResponse(200, { approvals: [approval()] }));
    });
    expect(
      useCoreApprovals.getState().pending.map((a) => a.approval_id),
    ).toEqual(["b-1"]);
  });
});
