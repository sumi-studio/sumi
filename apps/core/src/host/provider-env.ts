/**
 * Shared model-provider wiring for both hosts: the same SUMI_MODEL_* env
 * contract selects the provider under the Node local host and the workerd
 * Durable Object alike, so a persona's secretary behaves identically in
 * either runtime.
 *
 *   SUMI_MODEL_PROVIDER=mock|openai   (default mock)
 *   SUMI_MODEL_BASE_URL / _API_KEY / _MODEL   (openai)
 *   SUMI_MODEL_HEADERS_JSON           (openai; static extra request headers
 *                                      as a JSON object, e.g. a provider
 *                                      routing/session header)
 *   SUMI_MODEL_EXTRA_JSON             (openai; extra request fields as a
 *                                      JSON object, e.g. max_tokens)
 *   SUMI_MODEL_TIMEOUT_MS             (openai; per-request wall timeout,
 *                                      default 120000)
 */

import type { ModelProvider } from "../provider.ts";
import { MockProvider } from "../providers/mock.ts";
import { OpenAIProvider } from "../providers/openai.ts";

export function providerFromEnv(
  get: (name: string) => string | undefined,
): ModelProvider {
  const kind = get("SUMI_MODEL_PROVIDER") ?? "mock";
  if (kind === "openai") {
    return new OpenAIProvider({
      baseUrl: required(get, "SUMI_MODEL_BASE_URL"),
      apiKey: required(get, "SUMI_MODEL_API_KEY"),
      model: required(get, "SUMI_MODEL_MODEL"),
      headers: jsonObj(get, "SUMI_MODEL_HEADERS_JSON") as
        | Record<string, string>
        | undefined,
      extra: jsonObj(get, "SUMI_MODEL_EXTRA_JSON"),
      timeoutMs: numEnv(get, "SUMI_MODEL_TIMEOUT_MS", 120_000),
    });
  }
  if (kind !== "mock") throw new Error(`unknown SUMI_MODEL_PROVIDER ${kind}`);
  return new MockProvider();
}

function required(
  get: (name: string) => string | undefined,
  name: string,
): string {
  const v = get(name);
  if (!v) throw new Error(`missing env ${name}`);
  return v;
}

function jsonObj(
  get: (name: string) => string | undefined,
  name: string,
): Record<string, unknown> | undefined {
  const raw = get(name);
  if (!raw) return undefined;
  const parsed = JSON.parse(raw) as unknown;
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new Error(`${name} must be a JSON object`);
  }
  return parsed as Record<string, unknown>;
}

function numEnv(
  get: (name: string) => string | undefined,
  name: string,
  fallback: number,
): number {
  const raw = get(name);
  if (!raw) return fallback;
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) {
    throw new Error(`${name} must be a positive number`);
  }
  return n;
}
