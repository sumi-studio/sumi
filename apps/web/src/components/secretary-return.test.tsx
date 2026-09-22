// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { SecretaryReturnSession } from "../auth/secretary-return";
import { SecretaryReturn } from "./secretary-return";

/**
 * Mounted coverage for the return panel's error and retry model. fetch is
 * the only stubbed boundary — the component runs its real read/mutation
 * effects against canned route answers.
 */

function returnSession(
  status: SecretaryReturnSession["status"],
  over: Partial<SecretaryReturnSession> = {},
): SecretaryReturnSession {
  return {
    session_id: "sess-1",
    transfer_id: "sess-1",
    persona_id: "persona-1",
    status,
    admit_until: "2030-01-01T00:00:00Z",
    source_placement_id: "placement-1",
    state_only: true,
    not_included: [],
    ...over,
  };
}

function jsonResponse(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

const csrfBody = { csrf_token: "a".repeat(43) };

/**
 * Routes fetch by method+path like the real mux would. Handlers return a
 * Response or a promise of one; unmatched calls fail loudly.
 */
function stubRoutes(
  routes: Record<string, (() => Response | Promise<Response>)[]>,
) {
  const used: Record<string, number> = {};
  const mock = vi.fn().mockImplementation((input: RequestInfo | URL) => {
    const url = typeof input === "string" ? input : input.toString();
    const path = new URL(url, "http://localhost").pathname;
    const handlers = routes[path];
    if (!handlers) return Promise.reject(new Error(`unstubbed ${path}`));
    const index = used[path] ?? 0;
    used[path] = index + 1;
    const handler = handlers[Math.min(index, handlers.length - 1)];
    return Promise.resolve(handler());
  });
  vi.stubGlobal("fetch", mock);
  return mock;
}

const GET_SESSION = "/api/secretary-return/session";
const POST_SESSIONS = "/api/secretary-return/sessions";
const CSRF = "/auth/csrf";

/** Picks the explicit "carry files into Local" choice — issue/retry stay
 * disabled until the owner makes one. */
async function chooseLocalFiles() {
  fireEvent.click(
    await screen.findByRole("radio", {
      name: /ファイルをローカルに持ってくる/,
    }),
  );
}

describe("SecretaryReturn", () => {
  beforeEach(() => {
    vi.stubGlobal(
      "ResizeObserver",
      class {
        observe() {}
        unobserve() {}
        disconnect() {}
      },
    );
  });
  afterEach(() => {
    cleanup();
    vi.unstubAllGlobals();
    sessionStorage.clear();
  });

  it("keeps a mutation refusal visible through the automatic re-read", async () => {
    // The open panel reads a null session; clicking issue is refused by the
    // undecided file policy; the post-mutation re-read then succeeds — the
    // refusal must still be on screen, not erased by that refresh.
    stubRoutes({
      [GET_SESSION]: [
        () => jsonResponse(404, { error: "return session not found" }),
        () => jsonResponse(404, { error: "return session not found" }),
        () => jsonResponse(404, { error: "return session not found" }),
      ],
      [CSRF]: [() => jsonResponse(200, csrfBody)],
      [POST_SESSIONS]: [
        () =>
          jsonResponse(409, {
            error: "return admission refused",
            code: "file_policy_undecided",
          }),
      ],
    });
    render(<SecretaryReturn open onOpenChange={() => {}} />);

    const issue = await screen.findByRole("button", {
      name: "移行URLを発行する",
    });
    // No selection, no issue: the button stays disabled until the owner
    // explicitly picks where the working files live.
    expect(issue).toBeDisabled();
    await chooseLocalFiles();
    await waitFor(() => expect(issue).toBeEnabled());
    fireEvent.click(issue);
    const refusal = await screen.findByText(
      /共有ファイルの扱いがまだ決まっていないため/,
    );
    expect(refusal).toBeVisible();

    // The busy→null transition runs the read effect again; wait for it to
    // settle, then assert the refusal survived the successful read.
    await waitFor(() =>
      expect(
        screen.getByText(/共有ファイルの扱いがまだ決まっていないため/),
      ).toBeVisible(),
    );
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(
      screen.getByText(/共有ファイルの扱いがまだ決まっていないため/),
    ).toBeVisible();
  });

  it("clears a read error once a status check succeeds", async () => {
    stubRoutes({
      [GET_SESSION]: [
        () => jsonResponse(500, { error: "boom" }),
        () => jsonResponse(200, returnSession("cancelled")),
      ],
    });
    render(<SecretaryReturn open onOpenChange={() => {}} />);

    await screen.findByText("boom");
    const reread = screen.getByRole("button", { name: "状態を確認" });
    await waitFor(() => expect(reread).toBeEnabled());
    fireEvent.click(reread);
    await screen.findByText(/移行は開始前にキャンセルされました/);
    expect(screen.queryByText("boom")).not.toBeInTheDocument();
  });

  it("offers a new URL after a cancelled return and issues it", async () => {
    const created = returnSession("awaiting_destination", {
      session_id: "sess-2",
      transfer_id: "sess-2",
    });
    const fetchMock = stubRoutes({
      // First read shows the terminal session; the automatic re-read after
      // the mutation settles sees the newly admitted one, like the real
      // backend would.
      [GET_SESSION]: [
        () => jsonResponse(200, returnSession("cancelled")),
        () => jsonResponse(200, created),
      ],
      [CSRF]: [() => jsonResponse(200, csrfBody)],
      [POST_SESSIONS]: [
        () =>
          jsonResponse(201, {
            session: created,
            return_url:
              "https://api.example/api/secretary-return/sessions/sess-2#grant=opaque",
          }),
      ],
    });
    render(<SecretaryReturn open onOpenChange={() => {}} />);

    await screen.findByText(/移行は開始前にキャンセルされました/);
    // The cancelled view explains nothing moved and still offers a way
    // forward — the gap F395 reported.
    const retry = screen.getByRole("button", {
      name: "新しい移行URLを発行する",
    });
    // The retry path asks for the file choice again — a new session never
    // inherits the last one's mode.
    expect(retry).toBeDisabled();
    await chooseLocalFiles();
    await waitFor(() => expect(retry).toBeEnabled());
    fireEvent.click(retry);
    await screen.findByText(/ローカルの受け取りを待っています/);
    expect(
      screen.getByText(/sess-2#grant=opaque/, { selector: "code" }),
    ).toBeInTheDocument();
    expect(fetchMock).toHaveBeenCalledWith(
      expect.stringContaining(POST_SESSIONS),
      expect.objectContaining({ method: "POST" }),
    );
    // The create body carries the owner's explicit choice.
    const createCall = fetchMock.mock.calls.find(([u]) =>
      String(u).includes(POST_SESSIONS),
    );
    expect(JSON.parse(String(createCall?.[1]?.body))).toEqual({
      file_mode: "local",
    });
  });

  it("shows the honest refusal again when retry meets the undecided gate", async () => {
    stubRoutes({
      [GET_SESSION]: [() => jsonResponse(200, returnSession("expired"))],
      [CSRF]: [() => jsonResponse(200, csrfBody)],
      [POST_SESSIONS]: [
        () =>
          jsonResponse(409, {
            error: "return admission refused",
            code: "file_policy_undecided",
          }),
      ],
    });
    render(<SecretaryReturn open onOpenChange={() => {}} />);

    await screen.findByText(/移行の受付期限が切れました/);
    const retry = screen.getByRole("button", {
      name: "新しい移行URLを発行する",
    });
    await chooseLocalFiles();
    await waitFor(() => expect(retry).toBeEnabled());
    fireEvent.click(retry);
    await screen.findByText(/移行は開始されていません/);
  });

  it("never offers a normal restart after a completed return", async () => {
    stubRoutes({
      [GET_SESSION]: [() => jsonResponse(200, returnSession("completed"))],
    });
    render(<SecretaryReturn open onOpenChange={() => {}} />);

    await screen.findByText(/状態と応答権はローカルに移りました/);
    expect(
      screen.queryByRole("button", { name: /移行URLを発行する/ }),
    ).not.toBeInTheDocument();
  });
});
