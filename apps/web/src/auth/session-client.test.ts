import { afterEach, describe, expect, it, vi } from "vitest";
import {
  AuthAPIError,
  authRequestTimeoutMilliseconds,
  getSumiProfile,
  getSumiSession,
  logoutSumiSession,
  postAuthJSON,
  SumiSessionCompensatedError,
  SumiSessionCompensationFailedError,
  updateSumiProfile,
  verifyCommittedSumiSession,
} from "./session-client";

const authorityBindingA = "A".repeat(43);

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("Sumi browser session client", () => {
  it("posts auth JSON only after obtaining CSRF", async () => {
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ csrf_token: "c".repeat(43) }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
      )
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ outcome: "proof_required" }), {
          status: 201,
        }),
      );
    vi.stubGlobal("fetch", fetchMock);

    await postAuthJSON("/auth/flows", { intent: "sign_in" });

    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(fetchMock.mock.calls[0]).toEqual([
      "/auth/csrf",
      expect.objectContaining({ credentials: "include", cache: "no-store" }),
    ]);
    expect(fetchMock.mock.calls[1]).toEqual([
      "/auth/flows",
      expect.objectContaining({
        method: "POST",
        credentials: "include",
        cache: "no-store",
        body: JSON.stringify({ intent: "sign_in" }),
        headers: expect.objectContaining({
          "Content-Type": "application/json",
          "X-CSRF-Token": "c".repeat(43),
        }),
      }),
    ]);
  });

  it("uses cookie-backed session status as authorization truth", async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValueOnce(
      new Response(
        JSON.stringify({
          authenticated: true,
          authority_binding_id: authorityBindingA,
          user: { id: "user-1", display_name: "薄明色の忘れ路" },
        }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      ),
    );
    vi.stubGlobal("fetch", fetchMock);

    await expect(getSumiSession()).resolves.toEqual({
      authenticated: true,
      authorityBindingId: authorityBindingA,
      user: { id: "user-1", displayName: "薄明色の忘れ路" },
    });
    expect(fetchMock).toHaveBeenCalledWith(
      "/auth/session",
      expect.objectContaining({ credentials: "include", cache: "no-store" }),
    );
  });

  it.each([
    ["missing", undefined],
    ["null", null],
    ["empty", ""],
  ])("maps a %s legacy display name to a neutral profile", async (_name, displayName) => {
    const user: Record<string, unknown> = { id: "legacy-user" };
    if (displayName !== undefined) user.display_name = displayName;
    vi.stubGlobal(
      "fetch",
      vi.fn<typeof fetch>().mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            authenticated: true,
            authority_binding_id: authorityBindingA,
            user,
          }),
          { status: 200 },
        ),
      ),
    );

    await expect(getSumiSession()).resolves.toMatchObject({
      authenticated: true,
      user: { id: "legacy-user", displayName: null },
    });
  });

  it.each([
    ["missing", undefined],
    ["short", "A".repeat(42)],
    ["padded", `${"A".repeat(43)}=`],
    ["non-canonical trailing bits", "B".repeat(43)],
    ["invalid alphabet", `${"A".repeat(42)}!`],
  ])("rejects a %s authority binding ID", async (_name, authorityBindingID) => {
    const body: Record<string, unknown> = {
      authenticated: true,
      user: { id: "user-1", display_name: "薄明色の忘れ路" },
    };
    if (authorityBindingID !== undefined) {
      body.authority_binding_id = authorityBindingID;
    }
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(
        new Response(JSON.stringify(body), { status: 200 }),
      );
    vi.stubGlobal("fetch", fetchMock);

    await expect(getSumiSession()).rejects.toBeInstanceOf(AuthAPIError);
  });

  it("reads the durable Human profile without a Workspace scope", async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValueOnce(
      new Response(
        JSON.stringify({
          user: { id: "user-1", display_name: "かずい" },
          profile: {
            participant: { kind: "human", human_id: "user-1" },
            display_name: "かずい",
            tagline: "開発",
          },
        }),
        { status: 200 },
      ),
    );
    vi.stubGlobal("fetch", fetchMock);

    await expect(getSumiProfile()).resolves.toEqual({
      participant: { kind: "human", humanId: "user-1" },
      displayName: "かずい",
      tagline: "開発",
    });
    expect(fetchMock).toHaveBeenCalledWith(
      "/auth/profile",
      expect.objectContaining({ credentials: "include", cache: "no-store" }),
    );
  });

  it("updates the canonical Human profile through one authenticated request", async () => {
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ csrf_token: "p".repeat(43) }), {
          status: 200,
        }),
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            user: { id: "user-1", display_name: "かずい" },
            profile: {
              participant: { kind: "human", human_id: "user-1" },
              display_name: "かずい",
              tagline: "開発",
            },
          }),
          { status: 200 },
        ),
      );
    vi.stubGlobal("fetch", fetchMock);

    await expect(
      updateSumiProfile({ displayName: "かずい", tagline: " 開発 " }),
    ).resolves.toEqual({
      id: "user-1",
      displayName: "かずい",
      profile: {
        participant: { kind: "human", humanId: "user-1" },
        displayName: "かずい",
        tagline: "開発",
      },
    });
    expect(fetchMock.mock.calls[1]).toEqual([
      "/auth/profile",
      expect.objectContaining({
        method: "POST",
        credentials: "include",
        body: JSON.stringify({ display_name: "かずい", tagline: "開発" }),
      }),
    ]);
  });

  it("rejects more than 80 Unicode code points before sending a profile request", async () => {
    const fetchMock = vi.fn<typeof fetch>();
    vi.stubGlobal("fetch", fetchMock);

    await expect(
      updateSumiProfile({ displayName: "🌙".repeat(81) }),
    ).rejects.toMatchObject({ status: 400 });
    await expect(
      updateSumiProfile({ tagline: "🌙".repeat(101) }),
    ).rejects.toMatchObject({ status: 400 });
    await expect(
      updateSumiProfile({ tagline: "one\ntwo" }),
    ).rejects.toMatchObject({ status: 400 });
    await expect(updateSumiProfile({})).rejects.toMatchObject({ status: 400 });
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("compensates a committed terminal flow before surfacing status failure", async () => {
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ error: "status unavailable" }), {
          status: 503,
        }),
      )
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ csrf_token: "f".repeat(43) }), {
          status: 200,
        }),
      )
      .mockResolvedValueOnce(new Response(null, { status: 204 }));
    vi.stubGlobal("fetch", fetchMock);

    const failure = await verifyCommittedSumiSession().catch(
      (error: unknown) => error,
    );
    expect(failure).toBeInstanceOf(SumiSessionCompensatedError);
    expect((failure as SumiSessionCompensatedError).cause).toMatchObject({
      status: 503,
    });

    expect(fetchMock).toHaveBeenCalledTimes(3);
    expect(fetchMock.mock.calls.map(([url]) => url)).toEqual([
      "/auth/session",
      "/auth/csrf",
      "/auth/logout",
    ]);
  });

  it("surfaces an explicit fail-closed error when compensation cannot complete", async () => {
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(new Response("invalid status", { status: 200 }))
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ csrf_token: "h".repeat(43) }), {
          status: 200,
        }),
      )
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ error: "logout unavailable" }), {
          status: 503,
        }),
      );
    vi.stubGlobal("fetch", fetchMock);

    await expect(verifyCommittedSumiSession()).rejects.toBeInstanceOf(
      SumiSessionCompensationFailedError,
    );
    expect(fetchMock.mock.calls.map(([url]) => url)).toEqual([
      "/auth/session",
      "/auth/csrf",
      "/auth/logout",
    ]);
  });

  it("protects logout with a fresh CSRF token", async () => {
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ csrf_token: "d".repeat(43) }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
      )
      .mockResolvedValueOnce(new Response(null, { status: 204 }));
    vi.stubGlobal("fetch", fetchMock);

    await logoutSumiSession();

    expect(fetchMock.mock.calls[1]).toEqual([
      "/auth/logout",
      expect.objectContaining({
        method: "POST",
        credentials: "include",
        headers: expect.objectContaining({
          "X-CSRF-Token": "d".repeat(43),
        }),
      }),
    ]);
  });

  it("bounds every authentication request with a fresh timeout signal", async () => {
    const signals = Array.from(
      { length: 5 },
      () => new AbortController().signal,
    );
    const timeout = vi
      .spyOn(AbortSignal, "timeout")
      .mockImplementation(
        () => signals.shift() ?? new AbortController().signal,
      );
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ csrf_token: "i".repeat(43) }), {
          status: 200,
        }),
      )
      .mockResolvedValueOnce(new Response("{}", { status: 200 }))
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ authenticated: false }), { status: 200 }),
      )
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ csrf_token: "j".repeat(43) }), {
          status: 200,
        }),
      )
      .mockResolvedValueOnce(new Response(null, { status: 204 }));
    vi.stubGlobal("fetch", fetchMock);

    await postAuthJSON("/auth/flows", { intent: "sign_in" });
    await getSumiSession();
    await logoutSumiSession();

    expect(timeout).toHaveBeenCalledTimes(5);
    expect(timeout).toHaveBeenCalledWith(authRequestTimeoutMilliseconds);
    expect(
      fetchMock.mock.calls.every(
        ([, init]) => init?.signal instanceof AbortSignal,
      ),
    ).toBe(true);
  });

  it("rejects a session response with an oversized declared body", async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValueOnce(
      new Response("{}", {
        status: 200,
        headers: { "Content-Length": "4097" },
      }),
    );
    vi.stubGlobal("fetch", fetchMock);

    await expect(getSumiSession()).rejects.toBeInstanceOf(AuthAPIError);
  });

  it("rejects a session response that exceeds the limit while streaming", async () => {
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new Uint8Array(4_097));
        controller.close();
      },
    });
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(new Response(body, { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);

    await expect(getSumiSession()).rejects.toBeInstanceOf(AuthAPIError);
  });
});
