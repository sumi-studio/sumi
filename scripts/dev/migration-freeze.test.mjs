import assert from "node:assert/strict";
import test from "node:test";

import { validateExtension, validateSeal } from "./migration-freeze.mjs";

const line = (version, stem, direction, digest) =>
  `${digest.repeat(64)}  ${String(version).padStart(4, "0")}_${stem}.${direction}.sql`;

const sealed = [
  line(15, "notifications", "down", "b"),
  line(15, "notifications", "up", "a"),
  line(16, "identity", "down", "d"),
  line(16, "identity", "up", "c"),
].join("\n") + "\n";

test("accepts one matching pair at the next version", () => {
  const actual = sealed + line(17, "workspace", "down", "f") + "\n" +
    line(17, "workspace", "up", "e") + "\n";
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
