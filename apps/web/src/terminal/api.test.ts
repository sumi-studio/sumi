import { describe, expect, it, vi } from "vitest";
import {
  TerminalAPIError,
  TerminalAPIUncertainError,
  TerminalApiClient,
} from "./api";

const SCOPE = {
  installationId: "0198f0f4-9b72-7000-8000-000000000051",
  authorityEpoch: "3",
};
const SESSION_ID = "0198f0f4-9b72-7000-8000-0000000000aa";

const sessionWire = {
  session_id: SESSION_ID,
  name: "main",
  mode: "interactive",
  backend: "container",
  status: "active",
  requested_by: "human",
  output_bytes: 0,
  output_base: 0,
  control_holder: "",
  created_at: "2026-09-19T00:00:00Z",
  updated_at: "2026-09-19T00:00:00Z",
};

function stubFetch(
  handler: (url: string, init: RequestInit) => Response,
): typeof fetch {
  return vi.fn((input: RequestInfo | URL, init?: RequestInit) =>
    Promise.resolve(handler(String(input), init ?? {})),
  ) as unknown as typeof fetch;
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

describe("TerminalApiClient", () => {
  it("scopes every request with the exact installation identity", async () => {
    let seen = "";
    const client = new TerminalApiClient(
      SCOPE,
      stubFetch((url) => {
        seen = url;
        return json({ sessions: [] });
      }),
    );
    await client.listSessions();
    const url = new URL(seen, "https://sumi.test");
    expect(url.pathname).toBe("/terminal/list");
    expect(url.searchParams.get("installation_id")).toBe(SCOPE.installationId);
    expect(url.searchParams.get("authority_epoch")).toBe("3");
  });

  it("posts the session name to open", async () => {
    let body: unknown;
    const client = new TerminalApiClient(
      SCOPE,
      stubFetch((_url, init) => {
        body = JSON.parse(String(init.body));
        return json({ session: sessionWire });
      }),
    );
    const session = await client.openSession(" build ");
    expect(session.sessionId).toBe(SESSION_ID);
    expect(body).toEqual({ name: " build " });
  });

  it("posts durable input payloads without inventing fields", async () => {
    let body: unknown;
    const client = new TerminalApiClient(
      SCOPE,
      stubFetch((_url, init) => {
        body = JSON.parse(String(init.body));
        return json({
          input: { input_id: "in-1", seq: 7, status: "intended" },
        });
      }),
    );
    const receipt = await client.submitInput(SESSION_ID, {
      kind: "resize",
      cols: 120,
      rows: 40,
    });
    expect(receipt.status).toBe("intended");
    expect(body).toEqual({
      session_id: SESSION_ID,
      kind: "resize",
      cols: 120,
      rows: 40,
    });
  });

  it("reads output with an absolute cursor", async () => {
    let seen = "";
    const client = new TerminalApiClient(
      SCOPE,
      stubFetch((url) => {
        seen = url;
        return json({ session: sessionWire, chunks: [], cursor: 512 });
      }),
    );
    const read = await client.readOutput(SESSION_ID, 512);
    expect(read.nextCursor).toBe(512);
    const url = new URL(seen, "https://sumi.test");
    expect(url.pathname).toBe("/terminal/read");
    expect(url.searchParams.get("cursor")).toBe("512");
  });

  it("maps the API's plain-text error bodies to stable codes", async () => {
    const cases: Array<[number, string, string]> = [
      [404, "terminal session not found", "not_found"],
      [409, "terminal session has ended", "ended"],
      [409, "terminal session is not live", "not_live"],
      [409, "terminal control is held", "control_held"],
      [429, "too many live terminal sessions", "capacity"],
      [503, "terminal backend unavailable", "unavailable"],
      [503, "terminal unavailable", "unavailable"],
      [401, "invalid session", "auth"],
      [403, "not authorized", "forbidden"],
      [400, "invalid_scope", "invalid_scope"],
    ];
    for (const [status, body, code] of cases) {
      const client = new TerminalApiClient(
        SCOPE,
        stubFetch(() => new Response(body, { status })),
      );
      try {
        await client.listSessions();
        expect.unreachable(`status ${status} should throw`);
      } catch (error) {
        expect(error).toBeInstanceOf(TerminalAPIError);
        expect((error as TerminalAPIError).code).toBe(code);
      }
    }
  });

  it("reports transport failure as uncertain, never as a clean rejection", async () => {
    const client = new TerminalApiClient(
      SCOPE,
      vi.fn(() => Promise.reject(new TypeError("offline"))) as typeof fetch,
    );
    await expect(client.closeSession(SESSION_ID)).rejects.toBeInstanceOf(
      TerminalAPIUncertainError,
    );
  });
});
