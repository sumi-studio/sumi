import type { components } from "@sumi/api-client";

export type Todo = components["schemas"]["Todo"];
export type TodoList = components["schemas"]["TodoList"];
export type TodoPriority = components["schemas"]["TodoPriority"];
export type TodoStatus = components["schemas"]["TodoStatus"];
export type TodoDueInput = components["schemas"]["TodoDueInput"];
export type CreateTodoRequest = components["schemas"]["CreateTodoRequest"];
export type UpdateTodoRequest = components["schemas"]["UpdateTodoRequest"];

export type TodoListFilters = {
  status?: TodoStatus;
  overdue?: boolean;
  q?: string;
  sort?: "updated_at" | "due";
  limit?: number;
  offset?: number;
};

type ErrorBody = components["schemas"]["ErrorResponse"];

export class TodoAPIError extends Error {
  readonly status: number;
  readonly code?: string;
  readonly currentVersion?: number;

  constructor(status: number, body?: ErrorBody) {
    super(body?.error.message ?? `Todo API request failed (${status})`);
    this.name = "TodoAPIError";
    this.status = status;
    this.code = body?.error.code;
    this.currentVersion = body?.error.current_version;
  }
}

const apiBaseURL = (import.meta.env?.VITE_API_BASE_URL ?? "").replace(
  /\/$/,
  "",
);

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${apiBaseURL}${path}`, {
    ...init,
    credentials: "include",
    headers: {
      Accept: "application/json",
      ...(init?.body ? { "Content-Type": "application/json" } : {}),
      ...init?.headers,
    },
  });
  if (!response.ok) {
    let body: ErrorBody | undefined;
    try {
      body = (await response.json()) as ErrorBody;
    } catch {
      // Non-JSON failures (for example a proxy error) still retain HTTP status.
    }
    throw new TodoAPIError(response.status, body);
  }
  if (response.status === 204) return undefined as T;
  return (await response.json()) as T;
}

export function listTodos(filters: TodoListFilters = {}): Promise<TodoList> {
  const query = new URLSearchParams();
  if (filters.status) query.set("status", filters.status);
  if (filters.overdue !== undefined)
    query.set("overdue", String(filters.overdue));
  if (filters.q) query.set("q", filters.q);
  if (filters.sort) query.set("sort", filters.sort);
  if (filters.limit !== undefined) query.set("limit", String(filters.limit));
  if (filters.offset !== undefined) query.set("offset", String(filters.offset));
  const suffix = query.size > 0 ? `?${query}` : "";
  return request<TodoList>(`/v1/todos${suffix}`);
}

export function createTodo(input: CreateTodoRequest): Promise<Todo> {
  return request<Todo>("/v1/todos", {
    method: "POST",
    body: JSON.stringify(input),
  });
}

export function updateTodo(
  id: string,
  input: UpdateTodoRequest,
): Promise<Todo> {
  return request<Todo>(`/v1/todos/${encodeURIComponent(id)}`, {
    method: "PATCH",
    body: JSON.stringify(input),
  });
}

export function deleteTodo(id: string, expectedVersion: number): Promise<void> {
  return request<void>(
    `/v1/todos/${encodeURIComponent(id)}?expected_version=${expectedVersion}`,
    { method: "DELETE" },
  );
}

// This endpoint exists only when the Compose development environment enables
// it. Production authentication remains outside the Todo API.
export function startDevelopmentSession(): Promise<void> {
  return request<void>("/__dev__/session", { method: "POST" });
}
