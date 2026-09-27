/** Minimal TypeSafe Jev client for the privileged desktop host.
 * Contract: https://docs.typesafe.ai/api — `POST /v1/systemone` with typed
 * `noul`/`choice`/`score` questions. Jev returns typed choices and probabilities,
 * never generated text. The API key lives only in this object; it is never put
 * in `process.env`, a web page, a log line, an error message or a job result. */

export type JevErrorCode =
  | "jev_auth_failed"
  | "jev_rate_limited"
  | "jev_overloaded"
  | "jev_request_rejected"
  | "jev_unreachable"
  | "jev_invalid_response"
  | "jev_cancelled";

export class JevError extends Error {
  constructor(
    readonly code: JevErrorCode,
    message: string,
    readonly status?: number,
  ) {
    super(message);
    this.name = "JevError";
  }
}

export interface ChoiceQuestion {
  type: "choice";
  instructions: string | Record<string, unknown>;
  criteria: Record<string, string | null>;
}
export interface NoulQuestion {
  type: "noul";
  instructions: string | Record<string, unknown>;
  criteria?: { true?: string; false?: string };
}
export type JevQuestion = ChoiceQuestion | NoulQuestion;

export interface ChoiceAnswer {
  type: "choice";
  choice: string;
  probabilities: Record<string, number>;
  confidence: number;
}
export interface NoulAnswer {
  type: "noul";
  noul: number;
}
export interface JevResponse {
  model: string;
  answers: Record<string, ChoiceAnswer | NoulAnswer>;
  usage: { input_tokens: number; output_tokens: number };
}

export interface JevOptions {
  apiKey: string;
  /** Alias or pinned id; see https://docs.typesafe.ai/models. */
  model?: string;
  /** Origin only. HTTPS, or HTTP on loopback for owned test fixtures. */
  endpoint?: string;
  timeoutMs?: number;
  /** Retries after the first attempt for 408/429/5xx/connection failures
   * (the official SDKs' default policy is 2). */
  maxRetries?: number;
  fetch?: typeof fetch;
}

export const JEV_DEFAULT_ENDPOINT = "https://api.typesafe.ai";
const MAX_RESPONSE = 256_000;

export function jevEndpoint(value: string = JEV_DEFAULT_ENDPOINT): string {
  const url = new URL(value);
  const loopback = ["127.0.0.1", "[::1]", "localhost"].includes(url.hostname);
  if (
    url.username ||
    url.password ||
    url.search ||
    url.hash ||
    url.pathname !== "/" ||
    !(url.protocol === "https:" || (url.protocol === "http:" && loopback))
  )
    throw new Error(
      "Jev endpoint must be an HTTPS origin (HTTP is allowed only on loopback).",
    );
  return new URL("/v1/systemone", url).href;
}

export class JevClient {
  readonly model: string;
  readonly endpoint: string;
  readonly #key: string;
  readonly #fetch: typeof fetch;
  readonly #timeoutMs: number;
  readonly #retries: number;

  constructor(options: JevOptions) {
    const key = options.apiKey?.trim() ?? "";
    if (!key || key.length > 512 || /\s/.test(key))
      throw new Error("Jev API key is missing or malformed.");
    this.#key = key;
    this.model = options.model?.trim() || "jev-latest";
    if (!/^[A-Za-z0-9._-]{1,64}$/.test(this.model))
      throw new Error("Invalid Jev model name.");
    this.endpoint = jevEndpoint(options.endpoint);
    this.#fetch = options.fetch ?? fetch;
    this.#timeoutMs = options.timeoutMs ?? 10_000;
    this.#retries = Math.max(0, Math.min(options.maxRetries ?? 2, 4));
  }

  /** Never exposes the key through JSON/inspection of the host object. */
  toJSON() {
    return { model: this.model, endpoint: this.endpoint };
  }

  async evaluate(
    state: unknown,
    questions: Record<string, JevQuestion>,
    signal?: AbortSignal,
  ): Promise<JevResponse> {
    const body = JSON.stringify({ state, model: this.model, questions });
    for (let attempt = 0; ; attempt++) {
      if (signal?.aborted) throw new JevError("jev_cancelled", "Cancelled.");
      let response: Response;
      try {
        response = await this.#fetch(this.endpoint, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${this.#key}`,
            "Content-Type": "application/json",
          },
          body,
          redirect: "error",
          signal: signal
            ? AbortSignal.any([signal, AbortSignal.timeout(this.#timeoutMs)])
            : AbortSignal.timeout(this.#timeoutMs),
        });
      } catch {
        if (signal?.aborted) throw new JevError("jev_cancelled", "Cancelled.");
        if (attempt < this.#retries) {
          await backoff(retryDelay(attempt) as number, signal);
          continue;
        }
        throw new JevError(
          "jev_unreachable",
          "Jev API could not be reached or timed out.",
        );
      }
      if (response.ok)
        return this.#parse(await readBounded(response), questions);
      const detail = this.#detail(await readBounded(response).catch(() => ""));
      const status = response.status;
      if (
        (status === 408 || status === 429 || status >= 500) &&
        attempt < this.#retries
      ) {
        const wait = retryDelay(attempt, response.headers);
        // A server asking for a longer pause than a person should wait for a
        // stop to take effect is reported, not slept through.
        if (wait !== undefined) {
          await backoff(wait, signal);
          continue;
        }
      }
      if (status === 401 || status === 403)
        throw new JevError(
          "jev_auth_failed",
          `Jev API rejected the configured key (HTTP ${status}).`,
          status,
        );
      if (status === 429)
        throw new JevError(
          "jev_rate_limited",
          "Jev API rate limit exceeded (HTTP 429).",
          status,
        );
      if (status === 408 || status >= 500)
        throw new JevError(
          "jev_overloaded",
          `Jev API is unavailable or overloaded (HTTP ${status}).`,
          status,
        );
      throw new JevError(
        "jev_request_rejected",
        `Jev API rejected the request (HTTP ${status})${detail ? `: ${detail}` : ""}.`,
        status,
      );
    }
  }

  #detail(text: string): string {
    // Only a bounded validation hint; the key is scrubbed defensively.
    return text
      .replaceAll(this.#key, "[redacted]")
      .replace(/\s+/g, " ")
      .slice(0, 300);
  }

  #parse(text: string, questions: Record<string, JevQuestion>): JevResponse {
    let value: unknown;
    try {
      value = JSON.parse(text);
    } catch {
      throw new JevError("jev_invalid_response", "Jev response was not JSON.");
    }
    const v = value as Partial<JevResponse> | null;
    if (
      !v ||
      typeof v !== "object" ||
      !v.answers ||
      typeof v.answers !== "object"
    )
      throw new JevError(
        "jev_invalid_response",
        "Jev response has no answers.",
      );
    const answers: JevResponse["answers"] = {};
    for (const [id, question] of Object.entries(questions)) {
      const answer = (v.answers as Record<string, unknown>)[id] as
        | Record<string, unknown>
        | undefined;
      if (!answer || answer.type !== question.type)
        throw new JevError(
          "jev_invalid_response",
          `Jev response is missing a ${question.type} answer for ${id}.`,
        );
      if (question.type === "noul") {
        const noul = answer.noul;
        if (!probability(noul))
          throw new JevError("jev_invalid_response", `Invalid noul for ${id}.`);
        answers[id] = { type: "noul", noul };
        continue;
      }
      const options = Object.keys(question.criteria);
      const raw = answer.probabilities;
      const choice = answer.choice;
      if (typeof choice !== "string" || !options.includes(choice))
        throw new JevError(
          "jev_invalid_response",
          `Jev chose an option that was not offered for ${id}.`,
        );
      // Same shape checks as browser-use/jev-ultrafast's validate_choice
      // (MIT): exactly the offered options, a distribution, and the choice
      // at its maximum. A malformed answer executes nothing.
      const probabilities: Record<string, number> = {};
      if (raw && typeof raw === "object" && !Array.isArray(raw))
        for (const [option, p] of Object.entries(raw))
          if (options.includes(option) && probability(p))
            probabilities[option] = p;
      const values = Object.values(probabilities);
      if (
        values.length !== options.length ||
        Object.keys(raw as object).length !== options.length ||
        Math.abs(values.reduce((a, b) => a + b, 0) - 1) > 0.02 ||
        (probabilities[choice] as number) < Math.max(...values) - 1e-6 ||
        !probability(answer.confidence)
      )
        throw new JevError(
          "jev_invalid_response",
          `Jev returned a malformed ${id} answer; nothing was executed.`,
        );
      answers[id] = {
        type: "choice",
        choice,
        probabilities,
        confidence: answer.confidence,
      };
    }
    const usage = v.usage ?? { input_tokens: 0, output_tokens: 0 };
    return {
      model: typeof v.model === "string" ? v.model.slice(0, 64) : this.model,
      answers,
      usage: {
        input_tokens: Number.isFinite(usage.input_tokens)
          ? usage.input_tokens
          : 0,
        output_tokens: Number.isFinite(usage.output_tokens)
          ? usage.output_tokens
          : 0,
      },
    };
  }
}

function probability(value: unknown): value is number {
  return typeof value === "number" && value >= 0 && value <= 1;
}

async function readBounded(response: Response): Promise<string> {
  const reader = response.body?.getReader();
  if (!reader) return "";
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    for (;;) {
      const chunk = await reader.read();
      if (chunk.done) break;
      size += chunk.value.length;
      if (size > MAX_RESPONSE)
        throw new JevError("jev_invalid_response", "Jev response too large.");
      chunks.push(chunk.value);
    }
  } finally {
    await reader.cancel().catch(() => {});
  }
  return Buffer.concat(chunks).toString("utf8");
}

const MAX_RETRY_WAIT = 5_000;

/** Delay before retry `attempt`, or undefined when the server's Retry-After
 * exceeds what the goal loop may sleep while staying promptly stoppable. */
export function retryDelay(
  attempt: number,
  headers?: Headers,
): number | undefined {
  const ms = Number(headers?.get("retry-after-ms") ?? Number.NaN);
  const seconds = Number(headers?.get("retry-after") ?? Number.NaN);
  const asked =
    Number.isFinite(ms) && ms >= 0
      ? ms
      : Number.isFinite(seconds) && seconds >= 0
        ? seconds * 1000
        : undefined;
  if (asked !== undefined) return asked <= MAX_RETRY_WAIT ? asked : undefined;
  return (
    Math.min(500 * 2 ** attempt, MAX_RETRY_WAIT) * (0.75 + Math.random() * 0.5)
  );
}

async function backoff(ms: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted) throw new JevError("jev_cancelled", "Cancelled.");
  await new Promise<void>((resolve, reject) => {
    const onAbort = () => {
      clearTimeout(timer);
      reject(new JevError("jev_cancelled", "Cancelled."));
    };
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}
