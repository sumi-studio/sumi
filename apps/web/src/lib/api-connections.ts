import { z } from "zod";
import { fetchCSRFToken } from "../auth/session-client";

const connectionSchema = z.object({
  id: z.string(),
  name: z.string(),
  preset: z.string(),
  baseUrl: z.string(),
  model: z.string(),
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
  if (!result.success)
    throw new Error("接続情報を読み込めませんでした。もう一度お試しください。");
  return result.data;
}

export interface APIConnection {
  id: string;
  name: string;
  preset: string;
  baseUrl: string;
  model: string;
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
export type ConnectionInput = Omit<APIConnection, "id"> & { apiKey?: string };
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
    if (!response.ok)
      throw new Error(
        response.status === 404
          ? "接続が見つかりません。状態を更新してください。"
          : "接続を変更できませんでした。入力と接続状態を確認してください。",
      );
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
