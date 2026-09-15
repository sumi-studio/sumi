import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { CoreApproval } from "./model";
import { useCoreApprovals } from "./store";

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
    request: { text: "会議は15時です" },
    action_digest: "digest-1",
    status: "pending",
    decision: null,
    decided_at: null,
    consumed_at: null,
    created_at: "2026-09-15T00:00:00Z",
    secretary_name: "Kuro",
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

describe("core approvals store", () => {
  beforeEach(() => {
    useCoreApprovals.getState().reset();
    vi.restoreAllMocks();
  });
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("partitions the durable inbox into pending and resolved", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse(200, {
        human: "h-1",
        approvals: [
          approval(),
          approval({
            approval_id: "a-2",
            status: "approved",
            decision: "approve_once",
            decided_at: "2026-09-15T01:00:00Z",
          }),
        ],
      }),
    );
    vi.stubGlobal("fetch", fetchMock);

    await useCoreApprovals.getState().refresh();
    const state = useCoreApprovals.getState();
    expect(state.status).toBe("ready");
    expect(state.owner).toBe("h-1");
    expect(state.pending.map((a) => a.approval_id)).toEqual(["a-1"]);
    expect(state.resolved.map((a) => a.approval_id)).toEqual(["a-2"]);
    expect(fetchMock).toHaveBeenCalledWith(
      "/me/approvals",
      expect.objectContaining({ credentials: "include" }),
    );
  });

  it("clears the inbox on 401 rather than leaving stale cards", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse(200, { human: "h-1", approvals: [approval()] }),
      )
      .mockResolvedValueOnce(jsonResponse(401, { error: "invalid_session" }));
    vi.stubGlobal("fetch", fetchMock);

    await useCoreApprovals.getState().refresh();
    expect(useCoreApprovals.getState().pending).toHaveLength(1);
    await useCoreApprovals.getState().refresh();
    const state = useCoreApprovals.getState();
    expect(state.status).toBe("error");
    expect(state.pending).toHaveLength(0);
  });

  it("decides once with a session-bound body that carries no actor", async () => {
    const resolved = approval({
      status: "approved",
      decision: "approve_once",
      decided_at: "2026-09-15T02:00:00Z",
    });
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse(200, { human: "h-1", approvals: [approval()] }),
      )
      .mockResolvedValueOnce(jsonResponse(200, { approval: resolved }));
    vi.stubGlobal("fetch", fetchMock);

    await useCoreApprovals.getState().refresh();
    const pending = useCoreApprovals.getState().pending[0];
    await useCoreApprovals.getState().decide(pending, "approve_once");

    const [, call] = fetchMock.mock.calls;
    const body = JSON.parse(String(call[1]?.body));
    expect(call[0]).toBe("/me/approvals/a-1/decision");
    expect(body.decision).toBe("approve_once");
    expect(typeof body.decision_id).toBe("string");
    expect(body).not.toHaveProperty("actor_id");
    expect(body).not.toHaveProperty("decided_by_id");

    const state = useCoreApprovals.getState();
    expect(state.pending).toHaveLength(0);
    expect(state.resolved[0].status).toBe("approved");
    expect(state.deciding["a-1"]).toBeUndefined();
  });

  it("retries a lost response with the same decision_id", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse(200, { human: "h-1", approvals: [approval()] }),
      )
      .mockRejectedValueOnce(new TypeError("network down"))
      .mockResolvedValueOnce(
        jsonResponse(200, {
          approval: approval({
            status: "denied",
            decision: "deny_once",
            decided_at: "2026-09-15T03:00:00Z",
          }),
        }),
      );
    vi.stubGlobal("fetch", fetchMock);

    await useCoreApprovals.getState().refresh();
    const pending = useCoreApprovals.getState().pending[0];
    await useCoreApprovals.getState().decide(pending, "deny_once");
    expect(useCoreApprovals.getState().decisionErrors["a-1"]).toBe(
      "network_error",
    );

    await useCoreApprovals.getState().decide(pending, "deny_once");
    const first = JSON.parse(String(fetchMock.mock.calls[1][1]?.body));
    const retry = JSON.parse(String(fetchMock.mock.calls[2][1]?.body));
    expect(retry.decision_id).toBe(first.decision_id);
    expect(retry.decision).toBe("deny_once");
    expect(useCoreApprovals.getState().resolved[0].status).toBe("denied");
  });

  it("latches concurrent sends to one in-flight decision", async () => {
    let release!: (value: Response) => void;
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse(200, { human: "h-1", approvals: [approval()] }),
      )
      .mockImplementationOnce(
        () => new Promise<Response>((resolve) => (release = resolve)),
      );
    vi.stubGlobal("fetch", fetchMock);

    await useCoreApprovals.getState().refresh();
    const pending = useCoreApprovals.getState().pending[0];
    const first = useCoreApprovals.getState().decide(pending, "approve_once");
    await useCoreApprovals.getState().decide(pending, "deny_once");
    expect(fetchMock).toHaveBeenCalledTimes(2);

    release(jsonResponse(200, { approval: approval({ status: "approved" }) }));
    await first;
  });

  it("does not resurrect a resolved card from a predecision refresh", async () => {
    const staleList = deferred<Response>();
    const decideResp = deferred<Response>();
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse(200, { human: "h-1", approvals: [approval()] }),
      )
      .mockImplementationOnce(() => decideResp.promise)
      .mockImplementationOnce(() => staleList.promise);
    vi.stubGlobal("fetch", fetchMock);

    await useCoreApprovals.getState().refresh();
    const pending = useCoreApprovals.getState().pending[0];

    const deciding = useCoreApprovals
      .getState()
      .decide(pending, "approve_once");
    // Issued while the decision is in flight; its snapshot predates it.
    const lateRefresh = useCoreApprovals.getState().refresh();

    decideResp.resolve(
      jsonResponse(200, {
        approval: approval({
          status: "approved",
          decision: "approve_once",
          decided_at: "2026-09-15T01:00:00Z",
        }),
      }),
    );
    await deciding;
    expect(useCoreApprovals.getState().resolved[0].status).toBe("approved");

    // The older snapshot must not regress the committed decision.
    staleList.resolve(
      jsonResponse(200, { human: "h-1", approvals: [approval()] }),
    );
    await lateRefresh;
    const s = useCoreApprovals.getState();
    expect(s.pending).toHaveLength(0);
    expect(s.resolved[0].status).toBe("approved");
    expect(s.deciding["a-1"]).toBeUndefined();
  });

  it("discards an older refresh that lands after a newer one", async () => {
    const first = deferred<Response>();
    const fetchMock = vi
      .fn()
      .mockImplementationOnce(() => first.promise)
      .mockResolvedValueOnce(
        jsonResponse(200, { human: "h-1", approvals: [approval()] }),
      );
    vi.stubGlobal("fetch", fetchMock);

    const inflightOld = useCoreApprovals.getState().refresh();
    await useCoreApprovals.getState().refresh();
    expect(useCoreApprovals.getState().pending).toHaveLength(1);

    // The earlier-issued response arrives last; the newer inbox stands.
    first.resolve(jsonResponse(200, { human: "h-1", approvals: [] }));
    await inflightOld;
    expect(useCoreApprovals.getState().pending).toHaveLength(1);
    expect(useCoreApprovals.getState().status).toBe("ready");
  });

  it("prefers the later-issued refresh when completions invert", async () => {
    // R1 issues, then a nudge issues R2 — but the older R1 response lands
    // first. R2's snapshot is newer and must still win.
    const old = deferred<Response>();
    const fresh = deferred<Response>();
    const fetchMock = vi
      .fn()
      .mockImplementationOnce(() => old.promise)
      .mockImplementationOnce(() => fresh.promise);
    vi.stubGlobal("fetch", fetchMock);

    const r1 = useCoreApprovals.getState().refresh();
    const r2 = useCoreApprovals.getState().refresh();

    old.resolve(jsonResponse(200, { human: "h-1", approvals: [approval()] }));
    await r1;
    expect(useCoreApprovals.getState().pending).toHaveLength(0);

    fresh.resolve(jsonResponse(200, { human: "h-1", approvals: [] }));
    await r2;
    const s = useCoreApprovals.getState();
    expect(s.status).toBe("ready");
    expect(s.owner).toBe("h-1");
    expect(s.pending).toHaveLength(0);
  });

  it("a reset drops the owner tag so another account inherits nothing", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse(200, { human: "h-1", approvals: [approval()] }),
      );
    vi.stubGlobal("fetch", fetchMock);
    await useCoreApprovals.getState().refresh();
    expect(useCoreApprovals.getState().owner).toBe("h-1");

    useCoreApprovals.getState().reset();
    const s = useCoreApprovals.getState();
    expect(s.owner).toBeNull();
    expect(s.status).toBe("idle");
  });

  it("reports a failed first load as error, not as a truthful empty", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(jsonResponse(503, { error: "unavailable" }));
    vi.stubGlobal("fetch", fetchMock);

    await useCoreApprovals.getState().refresh();
    const s = useCoreApprovals.getState();
    expect(s.status).toBe("error");
    expect(s.pending).toHaveLength(0);
    expect(s.resolved).toHaveLength(0);
  });

  it("converges on the server record after a conflict", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse(200, { human: "h-1", approvals: [approval()] }),
      )
      .mockResolvedValueOnce(jsonResponse(409, { error: "approval_conflict" }))
      .mockResolvedValueOnce(
        jsonResponse(200, {
          human: "h-1",
          approvals: [
            approval({
              status: "denied",
              decision: "deny_once",
              decided_at: "2026-09-15T04:00:00Z",
            }),
          ],
        }),
      );
    vi.stubGlobal("fetch", fetchMock);

    await useCoreApprovals.getState().refresh();
    const pending = useCoreApprovals.getState().pending[0];
    await useCoreApprovals.getState().decide(pending, "approve_once");

    const state = useCoreApprovals.getState();
    // Another tab's denial wins; the durable record is the truth this tab shows.
    expect(state.pending).toHaveLength(0);
    expect(state.resolved[0].status).toBe("denied");
    // The card is still listed (as resolved), so it carries its own error —
    // no separate notice.
    expect(state.decisionErrors["a-1"]).toBe("approval_conflict");
    expect(state.notices).toEqual([]);
  });
});

describe("malformed inbox responses", () => {
  beforeEach(() => {
    useCoreApprovals.getState().reset();
  });
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it.each([
    ["no human", { approvals: [approval()] }],
    ["empty human", { human: "", approvals: [approval()] }],
    ["non-string human", { human: 7, approvals: [] }],
    ["no approvals list", { human: "h-1" }],
    ["non-array approvals", { human: "h-1", approvals: {} }],
  ])("a 200 with %s is a load error, not an empty inbox", async (_, body) => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(jsonResponse(200, body)));

    await useCoreApprovals.getState().refresh();
    const s = useCoreApprovals.getState();
    expect(s.status).toBe("error");
    expect(s.owner).toBeNull();
    expect(s.pending).toHaveLength(0);
  });

  it("a malformed refresh while ready keeps the owned last-known-good inbox", async () => {
    vi.stubGlobal(
      "fetch",
      vi
        .fn()
        .mockResolvedValueOnce(
          jsonResponse(200, { human: "h-1", approvals: [approval()] }),
        )
        .mockResolvedValueOnce(jsonResponse(200, { approvals: [] })),
    );

    await useCoreApprovals.getState().refresh();
    await useCoreApprovals.getState().refresh();
    const s = useCoreApprovals.getState();
    expect(s.status).toBe("ready");
    expect(s.owner).toBe("h-1");
    expect(s.pending.map((a) => a.approval_id)).toEqual(["a-1"]);
  });
});

describe("notices for failed decisions whose card left the inbox", () => {
  beforeEach(() => {
    useCoreApprovals.getState().reset();
  });
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  async function loadAndFail(code: string, status: number, after: unknown) {
    vi.stubGlobal(
      "fetch",
      vi
        .fn()
        .mockResolvedValueOnce(
          jsonResponse(200, { human: "h-1", approvals: [approval()] }),
        )
        .mockResolvedValueOnce(jsonResponse(status, { error: code }))
        .mockResolvedValueOnce(jsonResponse(200, after)),
    );
    await useCoreApprovals.getState().refresh();
    const pending = useCoreApprovals.getState().pending[0];
    await useCoreApprovals.getState().decide(pending, "approve_once");
  }

  it("records an owner-tagged notice when the converge refresh removes the card", async () => {
    await loadAndFail("persona_inactive", 409, { human: "h-1", approvals: [] });
    const s = useCoreApprovals.getState();
    expect(s.pending).toHaveLength(0);
    expect(s.decisionErrors).toEqual({});
    expect(s.notices).toHaveLength(1);
    expect(s.notices[0]).toMatchObject({
      code: "persona_inactive",
      owner: "h-1",
      approval: { approval_id: "a-1", secretary_name: "Kuro" },
    });

    useCoreApprovals.getState().dismissNotices();
    expect(useCoreApprovals.getState().notices).toEqual([]);
  });

  it("a snapshot for a different human creates no notice", async () => {
    await loadAndFail("approval_not_found", 404, {
      human: "h-2",
      approvals: [],
    });
    expect(useCoreApprovals.getState().notices).toEqual([]);
  });

  it("drops the notice once the approval is actionable again", async () => {
    await loadAndFail("persona_inactive", 409, { human: "h-1", approvals: [] });
    expect(useCoreApprovals.getState().notices).toHaveLength(1);

    vi.stubGlobal(
      "fetch",
      vi
        .fn()
        .mockResolvedValue(
          jsonResponse(200, { human: "h-1", approvals: [approval()] }),
        ),
    );
    await useCoreApprovals.getState().refresh();
    const s = useCoreApprovals.getState();
    expect(s.notices).toEqual([]);
    expect(s.pending.map((a) => a.approval_id)).toEqual(["a-1"]);
  });

  it("still explains a failure whose card an earlier refresh already removed", async () => {
    const decision = deferred<Response>();
    vi.stubGlobal(
      "fetch",
      vi
        .fn()
        .mockResolvedValueOnce(
          jsonResponse(200, { human: "h-1", approvals: [approval()] }),
        )
        .mockImplementationOnce(() => decision.promise)
        // A poll lands while the decision is in flight: the card is gone.
        .mockResolvedValueOnce(
          jsonResponse(200, { human: "h-1", approvals: [] }),
        )
        // The decision's converge refresh.
        .mockResolvedValueOnce(
          jsonResponse(200, { human: "h-1", approvals: [] }),
        ),
    );
    await useCoreApprovals.getState().refresh();
    const pending = useCoreApprovals.getState().pending[0];
    const deciding = useCoreApprovals.getState().decide(pending, "deny_once");
    await useCoreApprovals.getState().refresh();
    expect(useCoreApprovals.getState().pending).toHaveLength(0);

    decision.resolve(jsonResponse(409, { error: "persona_inactive" }));
    await deciding;
    const s = useCoreApprovals.getState();
    expect(s.notices.map((n) => [n.approval.approval_id, n.code])).toEqual([
      ["a-1", "persona_inactive"],
    ]);
  });

  it("a successful retry leaves no notice behind", async () => {
    vi.stubGlobal(
      "fetch",
      vi
        .fn()
        .mockResolvedValueOnce(
          jsonResponse(200, { human: "h-1", approvals: [approval()] }),
        )
        .mockRejectedValueOnce(new TypeError("network down"))
        .mockResolvedValueOnce(
          jsonResponse(200, {
            approval: approval({
              status: "approved",
              decision: "approve_once",
              decided_at: "2026-09-15T05:00:00Z",
            }),
          }),
        )
        .mockResolvedValueOnce(
          jsonResponse(200, { human: "h-1", approvals: [] }),
        ),
    );
    await useCoreApprovals.getState().refresh();
    const pending = useCoreApprovals.getState().pending[0];
    await useCoreApprovals.getState().decide(pending, "approve_once");
    await useCoreApprovals.getState().decide(pending, "approve_once");
    // The approved row later ages out of the listed history entirely.
    await useCoreApprovals.getState().refresh();
    expect(useCoreApprovals.getState().notices).toEqual([]);
  });

  it("a dead session and a reset both drop notices", async () => {
    await loadAndFail("persona_inactive", 409, { human: "h-1", approvals: [] });
    vi.stubGlobal(
      "fetch",
      vi
        .fn()
        .mockResolvedValue(jsonResponse(401, { error: "invalid_session" })),
    );
    await useCoreApprovals.getState().refresh();
    expect(useCoreApprovals.getState().notices).toEqual([]);

    useCoreApprovals.getState().reset();
    await loadAndFail("persona_inactive", 409, { human: "h-1", approvals: [] });
    expect(useCoreApprovals.getState().notices).toHaveLength(1);
    useCoreApprovals.getState().reset();
    expect(useCoreApprovals.getState().notices).toEqual([]);
  });
});
