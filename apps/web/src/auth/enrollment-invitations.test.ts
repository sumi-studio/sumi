import { afterEach, describe, expect, it, vi } from "vitest";
import {
  createEnrollmentInvitation,
  inspectEnrollmentInvitation,
  isEnrollmentInvitationUnavailable,
  listEnrollmentInvitations,
  revokeEnrollmentInvitation,
} from "./enrollment-invitations";
import { AuthAPIError } from "./session-client";

const uuid = "00000000-0000-4000-8000-000000000001";
const metadata = {
  id: uuid,
  issued_by: uuid,
  created_at: "2026-09-12T00:00:00Z",
  expires_at: "2026-09-19T00:00:00Z",
};
afterEach(() => vi.unstubAllGlobals());
describe("invitation transport", () => {
  it("accepts bounded100 metadata list larger than small auth response budget without exposing raw token fields", async () => {
    const fetcher = vi.fn().mockResolvedValue(
      Response.json({
        can_invite: true,
        invitations: Array.from({ length: 100 }, () => ({
          ...metadata,
          email: "tester@example.com",
          token: "must-not-expose",
        })),
      }),
    );
    vi.stubGlobal("fetch", fetcher);
    const result = await listEnrollmentInvitations();
    expect(result.invitations).toHaveLength(100);
    expect(result.invitations[0]).not.toHaveProperty("token");
    expect(fetcher.mock.calls[0][1]).toMatchObject({
      credentials: "include",
      cache: "no-store",
    });
  });
  it("rejects excessive streamed list and too many rows", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(new Response(" ".repeat(256 * 1024 + 1))),
    );
    await expect(listEnrollmentInvitations()).rejects.toBeInstanceOf(
      AuthAPIError,
    );
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        Response.json({
          can_invite: true,
          invitations: Array.from({ length: 101 }, () => metadata),
        }),
      ),
    );
    await expect(listEnrollmentInvitations()).rejects.toBeInstanceOf(
      AuthAPIError,
    );
  });
  it("creates with numeric lifetime and CSRF then revokes with explicit empty body", async () => {
    const fetcher = vi
      .fn()
      .mockResolvedValueOnce(Response.json({ csrf_token: "c".repeat(43) }))
      .mockResolvedValueOnce(
        Response.json(
          { invitation: metadata, token: "t".repeat(43) },
          { status: 201 },
        ),
      )
      .mockResolvedValueOnce(Response.json({ csrf_token: "c".repeat(43) }))
      .mockResolvedValueOnce(new Response(null, { status: 204 }));
    vi.stubGlobal("fetch", fetcher);
    expect(
      (
        await createEnrollmentInvitation({
          email: "test@example.com",
          expiresInSeconds: 3600,
        })
      ).token,
    ).toHaveLength(43);
    expect(JSON.parse(fetcher.mock.calls[1][1].body)).toEqual({
      email: "test@example.com",
      expires_in_seconds: 3600,
    });
    expect(fetcher.mock.calls[1][1].headers["X-CSRF-Token"]).toBe(
      "c".repeat(43),
    );
    await revokeEnrollmentInvitation(uuid);
    expect(fetcher.mock.calls[3][0]).toBe(`/auth/invitations/${uuid}/revoke`);
    expect(fetcher.mock.calls[3][1].body).toBe("{}");
  });
  it("distinguishes confirmed unavailable invites from connectivity and throttling failures", async () => {
    expect(
      isEnrollmentInvitationUnavailable(
        new AuthAPIError("invitation_required", 403),
      ),
    ).toBe(true);
    for (const error of [
      new TypeError("network"),
      new AuthAPIError("rate_limited", 429),
      new AuthAPIError("failure", 500),
      new AuthAPIError("invitation_admin_required", 403),
    ])
      expect(isEnrollmentInvitationUnavailable(error)).toBe(false);
    const fetcher = vi
      .fn()
      .mockResolvedValueOnce(Response.json({ csrf_token: "c".repeat(43) }))
      .mockResolvedValueOnce(
        Response.json({ error: "invitation_required" }, { status: 403 }),
      );
    vi.stubGlobal("fetch", fetcher);
    await expect(
      inspectEnrollmentInvitation("t".repeat(43)),
    ).rejects.toMatchObject({ status: 403, message: "invitation_required" });
  });
});
