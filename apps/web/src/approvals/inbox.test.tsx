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
import { useLayoutEffect } from "react";
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
      bRefresh.resolve(jsonResponse(200, { human: "human-b", approvals: [] }));
    });
    expect(useCoreApprovals.getState().status).toBe("ready");

    // A's slow response lands last — fenced to A's ended lifetime.
    await act(async () => {
      aRefresh.resolve(
        jsonResponse(200, { human: "human-a", approvals: [approval()] }),
      );
    });
    const s = useCoreApprovals.getState();
    expect(s.pending).toHaveLength(0);
    expect(s.status).toBe("ready");
  });

  it("failed first refresh under B surfaces an error, not A's rows or a fake empty", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse(200, { human: "human-a", approvals: [approval()] }),
      )
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
      .mockResolvedValueOnce(
        jsonResponse(200, { human: "human-a", approvals: [approval()] }),
      )
      .mockImplementationOnce(() => decideResp.promise) // POST /decision
      .mockResolvedValueOnce(
        jsonResponse(200, { human: "human-b", approvals: [] }),
      ); // B's inbox
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

  it("commits none of A's DOM under B — not even the first commit before cleanup", async () => {
    // A's inbox is fully loaded and its popover is open when the session's
    // human changes in place. A layout-effect probe records every committed
    // DOM — including the commit React publishes before the sync effect's
    // passive cleanup can run — so this witnesses what B could actually see.
    const bRefresh = deferred<Response>();
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse(200, { human: "human-a", approvals: [approval()] }),
      )
      .mockImplementationOnce(() => bRefresh.promise);
    vi.stubGlobal("fetch", fetchMock);

    const commits: { user: string | null; dom: string }[] = [];
    function Observed({ user }: { user: { id: string } | null }) {
      useLayoutEffect(() => {
        commits.push({
          user: user?.id ?? null,
          dom: document.body.textContent ?? "",
        });
      });
      return <Rail user={user} />;
    }

    const view = render(<Observed user={{ id: "human-a" }} />);
    await flush();
    fireEvent.click(screen.getByRole("button", { name: "承認待ち 1 件" }));
    expect(await screen.findByText(/A-secretary/)).toBeInTheDocument();

    commits.length = 0;
    await act(async () => {
      view.rerender(<Observed user={{ id: "human-b" }} />);
    });

    const bCommits = commits.filter((c) => c.user === "human-b");
    expect(bCommits.length).toBeGreaterThan(0);
    for (const commit of bCommits) {
      expect(commit.dom).not.toContain("A-secretary");
      expect(commit.dom).not.toContain("account A private approval text");
    }

    // B's own refresh then commits honestly: empty inbox, owned by B.
    await act(async () => {
      bRefresh.resolve(jsonResponse(200, { human: "human-b", approvals: [] }));
    });
    expect(useCoreApprovals.getState().owner).toBe("human-b");
    expect(await screen.findByText("承認待ちはありません")).toBeInTheDocument();
    expect(screen.queryByText(/A-secretary/)).not.toBeInTheDocument();
  });

  it("account replacement while mounted drops the prior account's state", async () => {
    const aRefresh = deferred<Response>();
    const fetchMock = vi
      .fn()
      .mockImplementationOnce(() => aRefresh.promise)
      .mockResolvedValueOnce(
        jsonResponse(200, {
          human: "human-b",
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
      aRefresh.resolve(
        jsonResponse(200, { human: "human-a", approvals: [approval()] }),
      );
    });
    expect(
      useCoreApprovals.getState().pending.map((a) => a.approval_id),
    ).toEqual(["b-1"]);
  });
});

describe("settled explanations and malformed inbox reads", () => {
  beforeEach(() => {
    useCoreApprovals.getState().reset();
  });
  afterEach(() => {
    cleanup();
    useCoreApprovals.getState().reset();
    vi.unstubAllGlobals();
  });

  /** A's inbox open, approve clicked on a card whose secretary was sealed. */
  async function sealedClick() {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse(200, { human: "human-a", approvals: [approval()] }),
      )
      .mockResolvedValueOnce(jsonResponse(409, { error: "persona_inactive" }))
      .mockResolvedValueOnce(
        jsonResponse(200, { human: "human-a", approvals: [] }),
      );
    vi.stubGlobal("fetch", fetchMock);
    return fetchMock;
  }

  it("keeps a legible, non-committal explanation after the card disappears", async () => {
    await sealedClick();
    render(<Rail user={{ id: "human-a" }} />);
    await flush();
    fireEvent.click(screen.getByRole("button", { name: "承認待ち 1 件" }));
    fireEvent.click(
      await screen.findByRole("button", { name: "今回のみ許可" }),
    );
    await flush();
    await flush();

    // Settled: the card is gone, but the person is told what happened to it.
    const notice = await screen.findByRole("status");
    expect(notice).toHaveTextContent("A-secretary: message.send");
    expect(notice).toHaveTextContent(
      "今回の操作はここでは受け付けられませんでした",
    );
    expect(notice).toHaveTextContent("移る手続きに入っている");
    // It must not claim the decision took effect or that the move finished.
    expect(notice).not.toHaveTextContent("許可しました");
    expect(notice).not.toHaveTextContent("完了");
    expect(notice).not.toHaveTextContent("移行先");
    expect(
      screen.queryByRole("button", { name: "今回のみ許可" }),
    ).not.toBeInTheDocument();
    expect(screen.getByText("承認待ちはありません")).toBeInTheDocument();

    // Closing the inbox acknowledges it; reopening shows no stale notice.
    fireEvent.click(
      screen.getByRole("button", { name: "承認待ちはありません" }),
    );
    await flush();
    expect(useCoreApprovals.getState().notices).toEqual([]);
    fireEvent.click(
      screen.getByRole("button", { name: "承認待ちはありません" }),
    );
    expect(await screen.findByText("承認待ちはありません")).toBeInTheDocument();
    expect(screen.queryByRole("status")).not.toBeInTheDocument();
  });

  it("a notice never reaches another account's DOM, including the first commit", async () => {
    await sealedClick();
    const commits: { user: string | null; html: string }[] = [];
    function Observed({ user }: { user: { id: string } | null }) {
      useLayoutEffect(() => {
        commits.push({ user: user?.id ?? null, html: document.body.innerHTML });
      });
      return <Rail user={user} />;
    }
    const view = render(<Observed user={{ id: "human-a" }} />);
    await flush();
    fireEvent.click(screen.getByRole("button", { name: "承認待ち 1 件" }));
    fireEvent.click(
      await screen.findByRole("button", { name: "今回のみ許可" }),
    );
    await flush();
    await flush();
    expect(await screen.findByRole("status")).toHaveTextContent("A-secretary");

    const bRefresh = deferred<Response>();
    vi.stubGlobal(
      "fetch",
      vi.fn().mockImplementationOnce(() => bRefresh.promise),
    );
    commits.length = 0;
    // Synchronous: the first commit under B precedes the reset cleanup.
    view.rerender(<Observed user={{ id: "human-b" }} />);
    expect(useCoreApprovals.getState().notices).toHaveLength(0);
    await flush();
    await act(async () => {
      bRefresh.resolve(jsonResponse(200, { human: "human-b", approvals: [] }));
    });
    const bCommits = commits.filter((c) => c.user === "human-b");
    expect(bCommits.length).toBeGreaterThan(0);
    for (const commit of bCommits) {
      expect(commit.html).not.toContain("A-secretary");
      expect(commit.html).not.toContain("account A private approval text");
      expect(commit.html).not.toContain("受け付けられませんでした");
    }
    expect(screen.queryByRole("status")).not.toBeInTheDocument();
  });

  it("an account switch before the store resets hides A's notice in B's first commit", async () => {
    await sealedClick();
    let firstB: string | null = null;
    let noticesInStore = -1;
    function Probe({ user }: { user: { id: string } }) {
      useLayoutEffect(() => {
        if (user.id === "human-b" && firstB === null) {
          firstB = document.body.innerHTML;
          noticesInStore = useCoreApprovals.getState().notices.length;
        }
      });
      return <Rail user={user} />;
    }
    const view = render(<Probe user={{ id: "human-a" }} />);
    await flush();
    fireEvent.click(screen.getByRole("button", { name: "承認待ち 1 件" }));
    fireEvent.click(
      await screen.findByRole("button", { name: "今回のみ許可" }),
    );
    await flush();
    await flush();
    expect(useCoreApprovals.getState().notices).toHaveLength(1);
    expect(document.body.innerHTML).toContain("受け付けられませんでした");

    vi.stubGlobal(
      "fetch",
      vi.fn().mockImplementation(() => new Promise(() => {})),
    );
    view.rerender(<Probe user={{ id: "human-b" }} />);
    // The notice is still in the store at that commit; the render gate hides it.
    expect(noticesInStore).toBe(1);
    expect(firstB).not.toBeNull();
    expect(firstB).not.toContain("A-secretary");
    expect(firstB).not.toContain("受け付けられませんでした");
  });

  it("a late decision failure from the previous account creates no notice", async () => {
    const decideResp = deferred<Response>();
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse(200, { human: "human-a", approvals: [approval()] }),
      )
      .mockImplementationOnce(() => decideResp.promise)
      .mockResolvedValueOnce(
        jsonResponse(200, { human: "human-b", approvals: [] }),
      );
    vi.stubGlobal("fetch", fetchMock);
    const view = render(<Rail user={{ id: "human-a" }} />);
    await flush();
    const pending = useCoreApprovals.getState().pending[0];
    let deciding!: Promise<void>;
    await act(async () => {
      deciding = useCoreApprovals.getState().decide(pending, "approve_once");
    });
    view.rerender(<Rail user={null} />);
    view.rerender(<Rail user={{ id: "human-b" }} />);
    await flush();

    await act(async () => {
      decideResp.resolve(jsonResponse(409, { error: "persona_inactive" }));
    });
    await deciding;
    await flush();
    expect(useCoreApprovals.getState().notices).toEqual([]);
    expect(fetchMock).toHaveBeenCalledTimes(3);
    fireEvent.click(
      screen.getByRole("button", { name: "承認待ちはありません" }),
    );
    expect(await screen.findByText("承認待ちはありません")).toBeInTheDocument();
    expect(screen.queryByRole("status")).not.toBeInTheDocument();
  });

  it("a 200 list without its human reads as a load failure, never as empty", async () => {
    vi.stubGlobal(
      "fetch",
      vi
        .fn()
        .mockResolvedValueOnce(jsonResponse(200, { approvals: [approval()] })),
    );
    render(<Rail user={{ id: "human-a" }} />);
    await flush();

    const button = screen.getByRole("button", { name: "承認" });
    expect(button).toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: "承認待ちはありません" }),
    ).not.toBeInTheDocument();
    fireEvent.click(button);
    expect(
      await screen.findByText("承認の一覧を読み込めませんでした。"),
    ).toBeInTheDocument();
    expect(screen.queryByText("承認待ちはありません")).not.toBeInTheDocument();
    expect(screen.queryByText(/A-secretary/)).not.toBeInTheDocument();
  });
});
