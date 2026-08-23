import assert from "node:assert/strict";
import test from "node:test";

import {
  validateCandidateAgainstBase,
  validateExactSeal,
  validateExtension,
  validateSeal,
  verifyAgainstBase,
} from "./migration-freeze.mjs";

const line = (version, stem, direction, digest) =>
  `${digest.repeat(64)}  ${String(version).padStart(4, "0")}_${stem}.${direction}.sql`;

const sealed = [
  line(15, "notifications", "down", "b"),
  line(15, "notifications", "up", "a"),
  line(16, "identity", "down", "d"),
  line(16, "identity", "up", "c"),
].join("\n") + "\n";

const nextPair = line(17, "workspace", "down", "f") + "\n" +
  line(17, "workspace", "up", "e") + "\n";

test("accepts one matching pair at the next version", () => {
  const actual = sealed + nextPair;
  assert.doesNotThrow(() => validateExtension(sealed, actual));
});

test("initial seal rejects a version without a matching pair", () => {
  assert.doesNotThrow(() => validateSeal(sealed));
  assert.throws(
    () => validateSeal(sealed + line(17, "workspace", "up", "e") + "\n"),
    /matching up\/down pair/,
  );
});

test("rejects a migration inserted into a sealed numeric gap", () => {
  const actual = line(12, "retroactive", "down", "f") + "\n" +
    line(12, "retroactive", "up", "e") + "\n" + sealed;
  assert.throws(() => validateExtension(sealed, actual), /changed, disappeared, or moved/);
});

test("rejects skipping the next forward version", () => {
  const actual = sealed + line(18, "workspace", "down", "f") + "\n" +
    line(18, "workspace", "up", "e") + "\n";
  assert.throws(() => validateExtension(sealed, actual), /immediately follow sealed maximum/);
});

test("rejects an incomplete or mismatched pair", () => {
  assert.throws(
    () => validateExtension(sealed, sealed + line(17, "workspace", "up", "e") + "\n"),
    /matching up\/down pair/,
  );
  const mismatched = sealed + line(17, "other", "down", "f") + "\n" +
    line(17, "workspace", "up", "e") + "\n";
  assert.throws(() => validateExtension(sealed, mismatched), /matching up\/down pair/);
});

test("rejects changing a sealed migration while extending", () => {
  const changed = sealed.replace("a".repeat(64), "f".repeat(64));
  const actual = changed + line(17, "workspace", "down", "f") + "\n" +
    line(17, "workspace", "up", "e") + "\n";
  assert.throws(() => validateExtension(sealed, actual), /changed, disappeared, or moved/);
});

test("rejects reordering sealed entries", () => {
  const reordered = sealed.split("\n");
  [reordered[0], reordered[1]] = [reordered[1], reordered[0]];
  assert.throws(() => validateExtension(sealed, reordered.join("\n")), /canonical filename order/);
});

test("rejects non-canonical formatting", () => {
  assert.throws(() => validateSeal(sealed.trimEnd()), /end with a newline/);
  assert.throws(() => validateSeal(sealed.replace("  0015", " 0015")), /invalid migration freeze entry/);
});

test("exact check validates malformed matching inputs before comparing bytes", () => {
  const malformed = sealed.trimEnd();
  assert.throws(() => validateExactSeal(malformed, malformed), /end with a newline/);
});

test("base comparison allows an unchanged valid seal", () => {
  assert.equal(validateCandidateAgainstBase({
    baseManifest: sealed,
    baseActual: sealed,
    candidateManifest: sealed,
    candidateActual: sealed,
  }), "unchanged seal");
});

test("base comparison allows one exact next migration pair", () => {
  const extended = sealed + nextPair;
  assert.equal(validateCandidateAgainstBase({
    baseManifest: sealed,
    baseActual: sealed,
    candidateManifest: extended,
    candidateActual: extended,
  }), "one-version extension");
});

test("base comparison rejects a synchronized sealed SQL and digest rewrite", () => {
  const rewritten = sealed.replace("a".repeat(64), "9".repeat(64));
  assert.throws(() => validateCandidateAgainstBase({
    baseManifest: sealed,
    baseActual: sealed,
    candidateManifest: rewritten,
    candidateActual: rewritten,
  }), /changed, disappeared, or moved/);
});

test("base comparison rejects malformed candidate and base history", () => {
  const malformed = sealed.trimEnd();
  assert.throws(() => validateCandidateAgainstBase({
    baseManifest: sealed,
    baseActual: sealed,
    candidateManifest: malformed,
    candidateActual: malformed,
  }), /end with a newline/);
  assert.throws(() => validateCandidateAgainstBase({
    baseManifest: malformed,
    baseActual: sealed,
    candidateManifest: sealed,
    candidateActual: sealed,
  }), /end with a newline/);
  const inconsistentBase = sealed.replace("a".repeat(64), "9".repeat(64));
  assert.throws(() => validateCandidateAgainstBase({
    baseManifest: sealed,
    baseActual: inconsistentBase,
    candidateManifest: sealed,
    candidateActual: sealed,
  }), /differs from FROZEN/);
});

test("base comparison rejects two versions and a forward gap", () => {
  const twoVersions = sealed + nextPair +
    line(18, "profiles", "down", "8") + "\n" +
    line(18, "profiles", "up", "7") + "\n";
  assert.throws(() => validateCandidateAgainstBase({
    baseManifest: sealed,
    baseActual: sealed,
    candidateManifest: twoVersions,
    candidateActual: twoVersions,
  }), /exactly one new migration version/);

  const gap = sealed +
    line(18, "profiles", "down", "8") + "\n" +
    line(18, "profiles", "up", "7") + "\n";
  assert.throws(() => validateCandidateAgainstBase({
    baseManifest: sealed,
    baseActual: sealed,
    candidateManifest: gap,
    candidateActual: gap,
  }), /immediately follow sealed maximum/);
});

test("initial seal requires candidate SQL to exactly match the unsealed base", () => {
  assert.equal(validateCandidateAgainstBase({
    baseManifest: undefined,
    baseActual: sealed,
    candidateManifest: sealed,
    candidateActual: sealed,
  }), "initial seal");

  const changedCandidates = [
    sealed.replace("a".repeat(64), "9".repeat(64)),
    sealed + nextPair,
    sealed.split("\n").filter((entry) => !entry.includes("0016_")).join("\n"),
    sealed.replaceAll("identity", "profile"),
  ];
  for (const changed of changedCandidates) {
    assert.throws(() => validateCandidateAgainstBase({
      baseManifest: undefined,
      baseActual: sealed,
      candidateManifest: changed,
      candidateActual: changed,
    }), /preserve and exactly seal the base migration SQL assets/);
  }
});

test("Git base verification rejects ambiguous or injectable refs before lookup", async () => {
  await assert.rejects(() => verifyAgainstBase("main; touch /tmp/not-allowed"), /exact lowercase 40-hex/);
});
