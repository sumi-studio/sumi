import type { ApiError, DecisionRequest } from "./contracts";

export class ApiClientError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
  ) {
    super(message);
  }
}

export interface HumanSessionPayload {
  authenticated: true;
  csrfToken: string;
  expiresAt: string;
  vapidPublicKey: string;
  pushSubscriptionCount: number;
}

async function decode<T>(response: Response): Promise<T> {
  const body = (await response.json()) as T | ApiError;
  if (!response.ok) {
    const apiError = body as ApiError;
    throw new ApiClientError(
      response.status,
      apiError.error?.code ?? "request_failed",
      apiError.error?.message ?? "The request failed",
    );
  }
  return body as T;
}

export class DecisionApi {
  private csrfToken = "";

  setCsrfToken(value: string): void {
    this.csrfToken = value;
  }

  async bootstrap(token: string): Promise<HumanSessionPayload> {
    const response = await fetch("/api/auth/bootstrap", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "same-origin",
      body: JSON.stringify({ token }),
    });
    const payload = await decode<HumanSessionPayload>(response);
    this.csrfToken = payload.csrfToken;
    return payload;
  }

  async session(): Promise<HumanSessionPayload> {
    const response = await fetch("/api/human/session", {
      credentials: "same-origin",
    });
    const payload = await decode<HumanSessionPayload>(response);
    this.csrfToken = payload.csrfToken;
    return payload;
  }

  async list(view: "pending" | "history"): Promise<DecisionRequest[]> {
    const response = await fetch(`/api/human/requests?view=${view}`, {
      credentials: "same-origin",
    });
    return (await decode<{ requests: DecisionRequest[] }>(response)).requests;
  }

  async get(id: string): Promise<DecisionRequest> {
    const response = await fetch(
      `/api/human/requests/${encodeURIComponent(id)}`,
      {
        credentials: "same-origin",
      },
    );
    return (await decode<{ request: DecisionRequest }>(response)).request;
  }

  async respond(
    id: string,
    input: { choiceId?: string; reply?: string; idempotencyKey: string },
  ): Promise<DecisionRequest> {
    const response = await fetch(
      `/api/human/requests/${encodeURIComponent(id)}/respond`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-CSRF-Token": this.csrfToken,
        },
        credentials: "same-origin",
        body: JSON.stringify(input),
      },
    );
    return (await decode<{ request: DecisionRequest }>(response)).request;
  }

  async subscribe(subscription: PushSubscriptionJSON): Promise<void> {
    const response = await fetch("/api/human/push-subscriptions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-CSRF-Token": this.csrfToken,
      },
      credentials: "same-origin",
      body: JSON.stringify(subscription),
    });
    await decode(response);
  }

  async unsubscribe(endpoint: string): Promise<void> {
    const response = await fetch("/api/human/push-subscriptions", {
      method: "DELETE",
      headers: {
        "Content-Type": "application/json",
        "X-CSRF-Token": this.csrfToken,
      },
      credentials: "same-origin",
      body: JSON.stringify({ endpoint }),
    });
    await decode(response);
  }

  async logout(): Promise<void> {
    const response = await fetch("/api/human/logout", {
      method: "POST",
      headers: { "X-CSRF-Token": this.csrfToken },
      credentials: "same-origin",
    });
    await decode(response);
    this.csrfToken = "";
  }
}
