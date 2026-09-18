/**
 * Script job spec normalization — the runner's mirror of the API's
 * admission bounds (agentstate validateJobRequest kind 'script'). The API
 * validates at persistence; the runner re-derives an executable spec here
 * so a malformed stored request fails deterministically instead of
 * reaching workerd.
 *
 * Honesty notes baked into the spec:
 *  - cpu_seconds is whole-second RLIMIT_CPU granularity — never a
 *    millisecond-exact bound.
 *  - memory_mib is enforced by the cgroup when the launcher supports it,
 *    and reported as a configured (not enforced) bound otherwise.
 */

export interface ScriptLimits {
  cpu_seconds: number;
  wall_ms: number;
  memory_mib: number;
  output_bytes: number;
  log_bytes: number;
  file_calls: number;
  file_bytes: number;
}

export interface ScriptSpec {
  code: string;
  input: unknown;
  limits: ScriptLimits;
}

export const LIMIT_BOUNDS: Record<keyof ScriptLimits, [number, number]> = {
  cpu_seconds: [1, 600],
  wall_ms: [100, 3_600_000],
  memory_mib: [32, 1024],
  output_bytes: [1, 64 << 10],
  log_bytes: [0, 64 << 10],
  file_calls: [0, 256],
  file_bytes: [0, 16 << 20],
};

export const DEFAULT_LIMITS: ScriptLimits = {
  cpu_seconds: 5,
  wall_ms: 30_000,
  memory_mib: 256,
  output_bytes: 64 << 10,
  log_bytes: 16 << 10,
  file_calls: 32,
  file_bytes: 1 << 20,
};

export const CODE_MAX_BYTES = 64 << 10;
export const INPUT_MAX_BYTES = 32 << 10;

export class SpecError extends Error {}

function intIn(v: unknown, name: string, [lo, hi]: [number, number]): number {
  if (typeof v !== "number" || !Number.isInteger(v) || v < lo || v > hi) {
    throw new SpecError(`script limit ${name} must be an integer in [${lo}, ${hi}]`);
  }
  return v;
}

export function normalizeSpec(request: unknown): ScriptSpec {
  if (typeof request !== "object" || request === null) {
    throw new SpecError("script job request must be an object");
  }
  const req = request as Record<string, unknown>;
  const code = req.code;
  if (typeof code !== "string" || code === "") {
    throw new SpecError("script job requires non-empty code");
  }
  if (Buffer.byteLength(code, "utf8") > CODE_MAX_BYTES) {
    throw new SpecError(`script code exceeds ${CODE_MAX_BYTES} bytes`);
  }
  let input: unknown = req.input ?? null;
  if (req.input !== undefined) {
    const raw = JSON.stringify(input);
    if (raw === undefined || Buffer.byteLength(raw, "utf8") > INPUT_MAX_BYTES) {
      throw new SpecError(`script input must be JSON-serializable within ${INPUT_MAX_BYTES} bytes`);
    }
  }
  const limits: ScriptLimits = { ...DEFAULT_LIMITS };
  if (req.limits !== undefined) {
    if (typeof req.limits !== "object" || req.limits === null || Array.isArray(req.limits)) {
      throw new SpecError("script limits must be an object");
    }
    for (const [k, v] of Object.entries(req.limits as Record<string, unknown>)) {
      const key = k as keyof ScriptLimits;
      const bounds = LIMIT_BOUNDS[key];
      if (!bounds) throw new SpecError(`unknown script limit ${k}`);
      limits[key] = intIn(v, k, bounds);
    }
  }
  return { code, input, limits };
}
