import assert from "node:assert/strict";
import { createHmac, randomUUID } from "node:crypto";
import test from "node:test";
import { verifyTicket } from "../src/ticket.ts";

const token = "t".repeat(40);
const b64 = (b: Buffer | string) => Buffer.from(b).toString("base64url");

/** Mirrors apps/api/internal/cloudbrowser IssueTicket. */
function issue(claims: Record<string, unknown>, secret = token): string {
  const key = createHmac("sha256", secret).update("sumi.cloud-browser.viewer-ticket.v1").digest();
  const payload = b64(JSON.stringify(claims));
  return `sbt1.${payload}.${b64(createHmac("sha256", key).update(payload).digest())}`;
}

const claims = () => ({ v: 1, h: randomUUID(), p: randomUUID(), b: randomUUID(), s: 1, e: Date.now() + 60_000, n: randomUUID() });

test("a ticket binds owner, persona, profile and serializer version", async () => {
  const c = claims();
  assert.deepEqual(await verifyTicket(issue(c), token), c);
});

test("tampered, foreign, expired or wrong-version tickets are refused", async () => {
  const c = claims();
  const good = issue(c);
  const [, payload, sig] = good.split(".");
  const swapped = b64(JSON.stringify({ ...c, b: randomUUID() }));
  assert.equal(await verifyTicket(`sbt1.${swapped}.${sig}`, token), null);
  assert.equal(await verifyTicket(issue(c, "o".repeat(40)), token), null);
  assert.equal(await verifyTicket(issue({ ...c, e: Date.now() - 1 }), token), null);
  assert.equal(await verifyTicket(issue({ ...c, s: 2 }), token), null);
  assert.equal(await verifyTicket(issue({ ...c, h: "not-a-uuid" }), token), null);
  assert.equal(await verifyTicket(`sbt0.${payload}.${sig}`, token), null);
  assert.equal(await verifyTicket("x".repeat(2000), token), null);
});
