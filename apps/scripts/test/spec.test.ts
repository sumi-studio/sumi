import { test } from "node:test";
import * as assert from "node:assert/strict";
import { normalizeSpec, SpecError, DEFAULT_LIMITS } from "../src/spec.ts";

test("valid minimal spec gets defaults", () => {
  const s = normalizeSpec({ code: "export async function run(){return 1}" });
  assert.equal(s.input, null);
  assert.deepEqual(s.limits, DEFAULT_LIMITS);
});

test("limits are integer-bounded", () => {
  assert.throws(() => normalizeSpec({ code: "x", limits: { cpu_seconds: 0 } }), SpecError);
  assert.throws(() => normalizeSpec({ code: "x", limits: { cpu_seconds: 601 } }), SpecError);
  assert.throws(() => normalizeSpec({ code: "x", limits: { cpu_seconds: 1.5 } }), SpecError);
  assert.throws(() => normalizeSpec({ code: "x", limits: { wall_ms: 99 } }), SpecError);
  assert.throws(() => normalizeSpec({ code: "x", limits: { bogus: 1 } }), SpecError);
});

test("code and input bounds", () => {
  assert.throws(() => normalizeSpec({}), SpecError);
  assert.throws(() => normalizeSpec({ code: "" }), SpecError);
  assert.throws(() => normalizeSpec({ code: "x".repeat(70 << 10) }), SpecError);
  assert.throws(() => normalizeSpec({ code: "x", input: "y".repeat(40 << 10) }), SpecError);
  const ok = normalizeSpec({ code: "x", input: { a: 1 } });
  assert.deepEqual(ok.input, { a: 1 });
});

test("non-serializable input refused", () => {
  assert.throws(() => normalizeSpec({ code: "x", input: () => {} }), SpecError);
  assert.throws(() => normalizeSpec({ code: "x", input: Symbol("s") }), SpecError);
  const s = normalizeSpec({ code: "x", limits: { file_calls: 0, file_bytes: 0 } });
  assert.equal(s.limits.file_calls, 0);
});
