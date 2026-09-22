import type { TerminalScope } from "../src/terminal/api";

export interface LocalTerminalBinding extends TerminalScope {
  expiresIn: number;
}

/** The existing same-origin fm capability is sent only to its bootstrap route. */
export function localTerminalSession(
  credentials: () => { persona: string; token: string },
  fetcher: typeof fetch = globalThis.fetch.bind(globalThis),
) {
  let pending: Promise<LocalTerminalBinding> | null = null;
  const renew = (): Promise<LocalTerminalBinding> => {
    if (pending) return pending;
    pending = (async () => {
      const { persona, token } = credentials();
      if (!persona || !token) {
        throw new Error("Localのページを sumi-local url で開いてから、ターミナルを開いてください。");
      }
      const response = await fetcher(`/fm/${encodeURIComponent(persona)}/terminal-session`, {
        method: "POST", credentials: "same-origin", cache: "no-store",
        headers: { Authorization: `Bearer ${token}`, Accept: "application/json" },
        signal: AbortSignal.timeout(15_000),
      });
      if (!response.ok) {
        const detail = await response.json().catch(() => null);
        throw new Error(typeof detail?.message === "string" ? detail.message :
          response.status === 401 || response.status === 403
            ? "Localの認証を更新できません。sumi-local url のURLから開き直してください。"
            : "ターミナルに接続できません。Localホストの状態を確認して再試行してください。");
      }
      const body = await response.json();
      if (body.installation_id !== persona || body.terminal_base !== "/terminal" ||
          !Number.isSafeInteger(body.authority_epoch) || body.authority_epoch < 1 ||
          !Number.isFinite(body.expires_in) || body.expires_in <= 0) {
        throw new Error("Localターミナルの接続情報が正しくありません。");
      }
      return { installationId: body.installation_id, authorityEpoch: String(body.authority_epoch), expiresIn: body.expires_in };
    })().finally(() => { pending = null; });
    return pending;
  };
  const authorizedFetch: typeof fetch = async (input, init) => {
    const response = await fetcher(input, init);
    // Only a definite pre-dispatch auth refusal can be retried. Network
    // failures and ambiguous mutation outcomes remain the client's concern.
    if (response.status !== 401) return response;
    await renew();
    return fetcher(input, init);
  };
  return { renew, fetch: authorizedFetch };
}
