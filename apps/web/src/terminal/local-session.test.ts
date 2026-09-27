import { describe, expect, it, vi } from "vitest";
import { localTerminalSession } from "../../local-terminal/session";

const binding = { installation_id: "persona", authority_epoch: 1, expires_in: 3600, terminal_base: "/terminal" };
const credentials = () => ({ persona: "persona", token: "fm-private" });
describe("Local terminal authentication", () => {
  it("exchanges only the current same-origin capability and deduplicates renewals", async () => {
    const fetcher = vi.fn(async () => Response.json(binding));
    const session = localTerminalSession(credentials, fetcher);
    const [a, b] = await Promise.all([session.renew(), session.renew()]);
    expect(a).toEqual(b);
    expect(a.installationId).toBe("persona");
    expect(fetcher).toHaveBeenCalledTimes(1);
    expect(fetcher).toHaveBeenCalledWith("/fm/persona/terminal-session", expect.objectContaining({
      method: "POST", credentials: "same-origin", headers: { Authorization: "Bearer fm-private", Accept: "application/json" },
    }));
  });
  it("renews after definite 401 and repeats the refused request once without adding a bearer", async () => {
    const fetcher = vi.fn().mockResolvedValueOnce(new Response("missing session", { status: 401 }))
      .mockResolvedValueOnce(Response.json(binding)).mockResolvedValueOnce(Response.json({ ok: true }));
    const session = localTerminalSession(credentials, fetcher);
    const options = { method: "POST", body: "input", credentials: "include" as const };
    expect((await session.fetch("/terminal/input", options)).ok).toBe(true);
    expect(fetcher.mock.calls[0]).toEqual(["/terminal/input", options]);
    expect(fetcher.mock.calls[2]).toEqual(["/terminal/input", options]);
  });
  it("does not retry ambiguous mutations or forbidden calls", async () => {
    const fetcher = vi.fn().mockRejectedValueOnce(new Error("network"))
      .mockResolvedValueOnce(new Response("not authorized", { status: 403 }));
    const session = localTerminalSession(credentials, fetcher);
    await expect(session.fetch("/terminal/input", { method: "POST" })).rejects.toThrow("network");
    expect((await session.fetch("/terminal/input", { method: "POST" })).status).toBe(403);
    expect(fetcher).toHaveBeenCalledTimes(2);
  });
  it("refuses foreign bindings and explains unavailable Local storage", async () => {
    const fetcher = vi.fn().mockResolvedValueOnce(Response.json({ ...binding, installation_id: "foreign" }))
      .mockResolvedValueOnce(Response.json({ message: "Cloud working storage is not mounted" }, { status: 503 }));
    const session = localTerminalSession(credentials, fetcher);
    await expect(session.renew()).rejects.toThrow("接続情報");
    await expect(session.renew()).rejects.toThrow("Cloud working storage");
  });
});
