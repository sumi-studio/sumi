import { z } from "zod";
import { fetchCSRFToken } from "../auth/session-client";

const connectionSchema = z.object({
  id: z.string(),
  name: z.string(),
  preset: z.string(),
  baseUrl: z.string(),
  model: z.string(),
  maxOutputTokens: z.number().int().positive().optional(),
});
const stateSchema = z.object({
  available: z.boolean(),
  unavailableReason: z.string().optional(),
  connections: z.array(connectionSchema),
  selection: z
    .union([
      z.object({ kind: z.enum(["none", "chatgpt"]) }),
      z.object({ kind: z.literal("api"), connectionId: z.string() }),
    ])
    .nullable(),
  activation: z.literal("next_start"),
});

function decode<T>(schema: z.ZodType<T>, value: unknown): T {
  const result = schema.safeParse(value);
  if (!result.success) throw new Error("unexpected model-connections response");
  return result.data;
}

// The API's input-validation reasons are short sentences naming a field or
// header the user supplied; anything longer or with control characters is
// not that contract and is dropped.
const VALIDATION_DETAIL_MAX = 300;

/**
 * A failed model-connections response. Its message is safe to display: it
 * is fixed text, plus the API's input-validation reason only for a 400
 * (e.g. which extra header is reserved). Other failures — transport errors,
 * timeouts, unexpected bodies — are not this type and must not be shown.
 */
export class APIConnectionError extends Error {
  readonly status: number;
  constructor(status: number, validationDetail?: unknown) {
    super(failureMessage(status, validationDetail));
    this.name = "APIConnectionError";
    this.status = status;
  }
}
function failureMessage(status: number, detail: unknown): string {
  if (status === 404) return "接続が見つかりません。状態を更新してください。";
  if (status !== 400)
    return "接続を変更できませんでした。接続状態を確認して、もう一度お試しください。";
  const reason =
    typeof detail === "string" &&
    detail.length <= VALIDATION_DETAIL_MAX &&
    !/\p{Cc}/u.test(detail)
      ? detail.trim()
      : "";
  return `接続を変更できませんでした。入力内容を確認してください。${reason ? ` ${reason}` : ""}`;
}

export interface APIConnection {
  id: string;
  name: string;
  preset: string;
  baseUrl: string;
  model: string;
  /**
   * Requested bound on generated tokens for this connection. Set it when
   * the model's output cap is below the core default (a bound above the
   * cap makes every request fail with a provider 400). Undefined means
   * the protocol default.
   */
  maxOutputTokens?: number;
}
export type ConnectionSelection =
  | { kind: "none" | "chatgpt" }
  | { kind: "api"; connectionId: string };
export interface ConnectionsState {
  available: boolean;
  unavailableReason?: string;
  connections: APIConnection[];
  selection: ConnectionSelection | null;
  activation: "next_start";
}
export type ConnectionInput = Omit<APIConnection, "id"> & {
  apiKey?: string;
  /**
   * Extra per-connection request headers (e.g. a gateway routing header).
   * Sealed with the API key and write-only: setting or clearing them
   * requires resubmitting the key; omit to keep the stored headers.
   */
  extraHeaders?: Record<string, string>;
};
export interface APIConnectionsClient {
  list(signal: AbortSignal): Promise<ConnectionsState>;
  save(
    input: ConnectionInput,
    id: string | undefined,
    signal: AbortSignal,
  ): Promise<APIConnection>;
  remove(id: string, signal: AbortSignal): Promise<void>;
  select(selection: ConnectionSelection, signal: AbortSignal): Promise<void>;
}
export function createAPIConnectionsClient(
  fetcher: typeof fetch = globalThis.fetch.bind(globalThis),
): APIConnectionsClient {
  async function request(
    path: string,
    signal: AbortSignal,
    method = "GET",
    body?: unknown,
  ) {
    const csrf =
      method === "GET" ? undefined : await fetchCSRFToken({ fetcher, signal });
    const response = await fetcher(`/api/model-connections${path}`, {
      method,
      signal: AbortSignal.any([signal, AbortSignal.timeout(15000)]),
      credentials: "include",
      cache: "no-store",
      headers: {
        Accept: "application/json",
        ...(csrf ? { "X-CSRF-Token": csrf } : {}),
        ...(body ? { "Content-Type": "application/json" } : {}),
      },
      ...(body ? { body: JSON.stringify(body) } : {}),
    });
    if (!response.ok) {
      // Only an input-invalid 400 carries {error:{detail}} the user can act
      // on; no other failure body is read.
      const detail =
        response.status === 400
          ? await response
              .json()
              .then((body) => body?.error?.detail)
              .catch(() => undefined)
          : undefined;
      throw new APIConnectionError(response.status, detail);
    }
    return response.status === 204 ? undefined : response.json();
  }
  return {
    list: async (signal) => decode(stateSchema, await request("", signal)),
    save: async (body, id, signal) =>
      decode(
        connectionSchema,
        await request(
          id ? `/api/${encodeURIComponent(id)}` : "/api",
          signal,
          id ? "PUT" : "POST",
          body,
        ),
      ),
    remove: async (id, signal) => {
      await request(`/api/${encodeURIComponent(id)}`, signal, "DELETE");
    },
    select: async (body, signal) => {
      await request("/selection", signal, "PUT", body);
    },
  };
}
