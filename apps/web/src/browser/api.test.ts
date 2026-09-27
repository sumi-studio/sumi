import { describe, expect, it } from "vitest";
import { CloudBrowserAPIError, createCloudBrowserAPI, parseOverview } from "./api";

const CSRF = "c".repeat(43);

function stub(handler: (url: string, init: RequestInit) => Response) {
  const calls: { url: string; init: RequestInit }[] = [];
  const fetcher = (async (input: RequestInfo | URL, init: RequestInit = {}) => {
    const url = String(input);
    calls.push({ url, init });
    if (url === "/auth/csrf") return Response.json({ csrf_token: CSRF });
    return handler(url, init);
  }) as typeof fetch;
  return { fetcher, calls };
}

describe("Cloud browser API client", () => {
  it("parses an unconfigured deployment without failing", () => {
    expect(parseOverview({ configured: false })).toEqual({ configured: false });
  });

  it("reads personas, profiles with grants, and Jev status", async () => {
    const { fetcher, calls } = stub(() =>
      Response.json({
        configured: true,
        personas: [{ persona_id: "p1", name: "すみ" }],
        profiles: [
          {
            profile_id: "b1",
            persona_id: "p1",
            persona_name: "すみ",
            state: "live",
            incarnation: 2,
            tab_ids: ["t1"],
            checkpoint_at: "2026-09-27T10:00:00Z",
            checkpoint_bytes: 1200,
            grants: [{ attachment_id: "a1", tab_id: "t1", name: "Fixture", allow_actions: true }],
          },
        ],
        jev: { configured: true, rejected: false },
        snapshot_limit_bytes: 2097152,
      }),
    );
    const overview = await createCloudBrowserAPI(fetcher).overview();
    expect(calls[0]?.init.method).toBe("GET");
    expect(overview).toMatchObject({
      configured: true,
      profiles: [{ profileId: "b1", state: "live", grants: [{ attachmentId: "a1", allowActions: true }] }],
      jev: { configured: true },
    });
  });

  it("sends mutations with the CSRF token and maps API errors", async () => {
    const { fetcher, calls } = stub((url) =>
      url.endsWith("/grants") ? Response.json({ error: "not_authorized" }, { status: 403 }) : Response.json({ ticket: "sbt1.a.b", expires_at: "x", path: "/browser-cloud/viewer" }),
    );
    const api = createCloudBrowserAPI(fetcher);
    const ticket = await api.viewerTicket("b1");
    expect(ticket.ticket).toBe("sbt1.a.b");
    const post = calls.find((c) => c.url.endsWith("/viewer-ticket"));
    expect(post?.init.method).toBe("POST");
    expect((post?.init.headers as Record<string, string> | undefined)?.["X-CSRF-Token"]).toBe(CSRF);
    await expect(api.grant("b1", "t1", "Fixture", true)).rejects.toMatchObject({ code: "not_authorized" });
  });

  it("reports a network failure distinctly", async () => {
    const fetcher = (async () => {
      throw new TypeError("offline");
    }) as typeof fetch;
    await expect(createCloudBrowserAPI(fetcher).overview()).rejects.toBeInstanceOf(CloudBrowserAPIError);
  });
});
