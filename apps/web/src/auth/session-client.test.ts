import { afterEach, describe, expect, it, vi } from "vitest";
import {
  AuthAPIError,
  exchangeFirebaseIDToken,
  getSumiSession,
  logoutSumiSession,
} from "./session-client";

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("Sumi browser session client", () => {
  it("exchanges a Firebase ID token only after obtaining CSRF", async () => {
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ csrf_token: "c".repeat(43) }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
      )
      .mockResolvedValueOnce(new Response(null, { status: 204 }));
    vi.stubGlobal("fetch", fetchMock);

    await exchangeFirebaseIDToken("firebase-id-token");

    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(fetchMock.mock.calls[0]).toEqual([
      "/auth/csrf",
      expect.objectContaining({ credentials: "include", cache: "no-store" }),
    ]);
    expect(fetchMock.mock.calls[1]).toEqual([
      "/auth/session",
      expect.objectContaining({
        method: "POST",
        credentials: "include",
        cache: "no-store",
        body: JSON.stringify({ id_token: "firebase-id-token" }),
        headers: expect.objectContaining({
          "Content-Type": "application/json",
          "X-CSRF-Token": "c".repeat(43),
        }),
      }),
    ]);
  });

  it("uses cookie-backed session status as authorization truth", async () => {
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({ authenticated: true, user: { id: "user-1" } }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        ),
      );
    vi.stubGlobal("fetch", fetchMock);

    await expect(getSumiSession()).resolves.toEqual({
      authenticated: true,
      user: { id: "user-1" },
    });
    expect(fetchMock).toHaveBeenCalledWith(
      "/auth/session",
      expect.objectContaining({ credentials: "include", cache: "no-store" }),
    );
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
