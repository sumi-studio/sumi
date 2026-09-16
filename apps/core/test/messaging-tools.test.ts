import assert from "node:assert/strict";
import { test } from "node:test";
import { eventMessage, inputBodyText } from "../src/memory.ts";
import { assemble, summarizeToolResults } from "../src/secretary.ts";
import { toolSpecs } from "../src/tools.ts";
import type { Event } from "../src/types.ts";

const MESSAGING_TOOLS = [
  "messaging.send",
  "messaging.overview",
  "messaging.open",
  "messaging.search",
  "messaging.start_dm",
  "messaging.create_channel",
  "messaging.update_channel",
  "messaging.duplicate_channel",
  "messaging.create_thread",
  "messaging.edit_message",
  "messaging.delete_message",
  "messaging.notification_settings",
  "messaging.upload_attachment",
  "messaging.open_attachment",
];

test("delegated messaging tools are withheld until claimable", () => {
  // A bare state service cannot execute delegated effects — none are
  // advertised without a claimable set.
  const bare = toolSpecs().map((s) => s.name);
  for (const name of MESSAGING_TOOLS) {
    assert.ok(!bare.includes(name), `${name} advertised without a host`);
  }
  // Once the host registers them (Go GET .../tools lists claimable), the
  // model sees the full ordinary Messaging surface.
  const offered = toolSpecs(new Set(MESSAGING_TOOLS)).map((s) => s.name);
  for (const name of MESSAGING_TOOLS) {
    assert.ok(offered.includes(name), `${name} missing when claimable`);
  }
});

test("messaging.send advertises urgency and attachment binding", () => {
  const send = toolSpecs(new Set(MESSAGING_TOOLS)).find(
    (s) => s.name === "messaging.send",
  );
  assert.ok(send);
  const props = (send.parameters as Record<string, any>).properties;
  assert.ok(props.urgency, "urgency parameter missing");
  assert.ok(props.attachments, "attachments parameter missing");
  assert.ok(props.place_id && props.content);
});

test("messaging.search requires the workspace; open_attachment pages", () => {
  const specs = toolSpecs(new Set(MESSAGING_TOOLS));
  const search = specs.find((s) => s.name === "messaging.search");
  assert.ok(search);
  assert.deepEqual(
    (search.parameters as Record<string, any>).required,
    ["query", "workspace_id"],
  );
  const open = specs.find((s) => s.name === "messaging.open_attachment");
  assert.ok(open);
  const props = (open.parameters as Record<string, any>).properties;
  assert.ok(props.offset && props.max_bytes, "paging parameters missing");
});

test("oversized tool results leave the turn summary bounded", () => {
  const big = "x".repeat(256 * 1024);
  const [small, large] = summarizeToolResults([
    { call_id: "c1", tool: "messaging.send", result: { ok: true }, replayed: false },
    { call_id: "c2", tool: "messaging.open_attachment", result: { content_base64: big }, replayed: false },
  ]);
  assert.deepEqual(small!.result, { ok: true });
  assert.equal(large!.result, undefined);
  assert.equal(typeof large!.result_bytes, "number");
});

test("tool result budgeting measures UTF-8 bytes, not string length", () => {
  // 40k three-byte characters serialize to ~120 KB — under the 64 KiB
  // verbatim budget in code units but far over it on the wire. A
  // .length-based measure would let that ride the commit body.
  const jp = "あ".repeat(40_000);
  const [entry] = summarizeToolResults([
    {
      call_id: "c1",
      tool: "messaging.open",
      result: { content_text: jp },
      replayed: false,
    },
  ]);
  assert.equal(entry!.result, undefined);
  assert.equal(
    entry!.result_bytes,
    new TextEncoder().encode(JSON.stringify({ content_text: jp })).length,
  );
});

const ATTACHMENT = {
  attachment_id: "01h2v3attachment0000000000000a",
  filename: "図.png",
  mime: "image/png",
  size_bytes: 4096,
  sha256: "ab".repeat(32),
  position: 0,
  spoiler: false,
  alt: "外観図",
};

test("attachment-only input produces a meaningful body", () => {
  const payload: Record<string, unknown> = {
    attachments: [ATTACHMENT],
  };
  const body = inputBodyText(payload);
  assert.match(body, /attachment/);
  assert.match(body, /図\.png/);
  assert.match(body, /image\/png/);
  assert.match(body, /attachment_id=01h2v3/);
  // The metadata names the read affordance — not a path, never byte access.
  assert.match(body, /messaging\.open_attachment/);
});

test("text plus attachments keeps both", () => {
  const body = inputBodyText({ text: "見て", attachments: [ATTACHMENT] });
  assert.match(body, /^見て/);
  assert.match(body, /図\.png/);
});

test("journal replay of an attachment input keeps its metadata", () => {
  const ev: Event = {
    seq: 7,
    persona_id: "persona",
    turn_id: "turn-1",
    kind: "input_received",
    created_at: "2026-09-16T00:00:00Z",
    payload: {
      input_id: "messaging:ev-1",
      kind: "message",
      text: null,
      actor_kind: "human",
      actor_id: "01human0000000000000000000000a1",
      actor_display: "Yohaku",
      source_surface: "messaging",
      thread_id: "01place000000000000000000000aa",
      place_name: "general",
      place_kind: "channel",
      attention: "reply",
      occurred_at: "2026-09-16T00:00:00Z",
      event_id: "ev-1",
      message_id: "01msg00000000000000000000000aa",
      message_seq: 3,
      reason: "mention",
      message_change: null,
      attachments: [ATTACHMENT],
      attempt: 0,
    },
  };
  const m = eventMessage(ev);
  assert.ok(m);
  assert.match(m!.content as string, /\[Yohaku \(human\) in general/);
  assert.match(m!.content as string, /place_id=01place/);
  assert.match(m!.content as string, /message_id=01msg/);
  assert.match(m!.content as string, /図\.png \(image\/png\), 4\.0 KiB/);
});

test("assemble renders an attachment-only wake without raw JSON", () => {
  const input = {
    persona_id: "persona",
    input_id: "messaging:ev-2",
    kind: "message",
    payload: {
      event_id: "ev-2",
      actor: { kind: "human", id: "h1", display_name: "Yohaku" },
      place: { id: "p1", kind: "channel", name: "general" },
      message_id: "m1",
      attachments: [ATTACHMENT],
    },
    actor_kind: "human",
    actor_id: "h1",
    source_surface: "messaging",
    thread_id: "p1",
    occurred_at: "2026-09-16T00:00:00Z",
    attention: "reply",
    status: "queued",
    created_at: "2026-09-16T00:00:00Z",
  } as any;
  const messages = assemble([], input);
  const last = messages[messages.length - 1]!;
  assert.match(last.content as string, /図\.png/);
  assert.match(last.content as string, /attachment_id=/);
  assert.doesNotMatch(last.content as string, /sha256/);
});
