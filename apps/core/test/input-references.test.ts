import assert from "node:assert/strict";
import { test } from "node:test";
import { FakeState } from "../src/fake-state.ts";
import { eventMessage } from "../src/memory.ts";
import type {
  ModelEvent,
  ModelProvider,
  ModelRequest,
} from "../src/provider.ts";
import { assemble, Secretary } from "../src/secretary.ts";
import type { Input } from "../src/types.ts";

const PERSONA = "01930e00-0000-7000-8000-000000000001";

class ObservingProvider implements ModelProvider {
  readonly name = "input-reference-observer";
  readonly requests: ModelRequest[] = [];

  async *stream(request: ModelRequest): AsyncIterable<ModelEvent> {
    this.requests.push(request);
    yield { type: "text", delta: "Received." };
    yield { type: "done", usage: {} };
  }
}

function secretary(state: FakeState, provider: ModelProvider): Secretary {
  return new Secretary({
    personaId: PERSONA,
    holderId: crypto.randomUUID(),
    state,
    provider,
    leaseTtlMs: 30_000,
    renewEveryMs: 1_000,
    contextLimit: 20,
    pollIntervalMs: 1,
    scheduleEveryMs: 0,
    idgen: () => crypto.randomUUID(),
  });
}

function addInput(state: FakeState, id: string, input: Partial<Input>): Input {
  state.addInput(PERSONA, id, "");
  const stored = state.inputs.find((value) => value.input_id === id)!;
  Object.assign(stored, input);
  return stored;
}

test("same-named terminal notifications keep their session and outcome through a secretary restart", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  const provider = new ObservingProvider();
  const life = secretary(state, provider);
  const notifications = [
    {
      session_id: "terminal-one",
      status: "closed",
      end_reason: "shell_exit",
      exit_code: 0,
    },
    {
      session_id: "terminal-two",
      status: "lost",
      end_reason: "runner_lost",
      exit_signal: "SIGKILL",
    },
  ];
  await life.start();
  try {
    for (const fields of notifications) {
      addInput(state, `terminal:${fields.session_id}`, {
        kind: "terminal_ended",
        actor_kind: "terminal",
        actor_id: fields.session_id,
        source_surface: "core_terminal_sessions",
        payload: { text: 'Terminal session "work" ended.', ...fields },
      });
      assert.equal(await life.step(), "turn");
    }
  } finally {
    await life.stop();
  }
  const received = (await state.events(PERSONA, 0)).filter(
    (event) => event.kind === "input_received",
  );
  const originalMessages = provider.requests.map(
    (request) => request.messages.at(-1)!,
  );
  for (const [index, fields] of notifications.entries()) {
    const message = originalMessages[index]!;
    for (const [key, value] of Object.entries(fields)) {
      assert.ok(
        message.content.includes(`${key}=${value}`),
        `current input carries ${key}=${value}`,
      );
      assert.equal(
        received[index]!.payload[key],
        value,
        `journal stores ${key}`,
      );
    }
    assert.deepEqual(
      eventMessage(received[index]!),
      message,
      "journal and delivered view agree",
    );
  }
  assert.ok(
    !originalMessages[1]!.content.includes("exit_code="),
    "unknown exit code is not success",
  );

  const restarted = secretary(state, provider);
  await restarted.start();
  try {
    state.addInput(PERSONA, "after-restart", "What happened to the terminals?");
    assert.equal(await restarted.step(), "turn");
    const context = provider.requests.at(-1)!.messages;
    for (const message of originalMessages) {
      assert.ok(
        context.some(
          (value) =>
            value.role === message.role && value.content === message.content,
        ),
      );
    }
  } finally {
    await restarted.stop();
  }
});

test("message edits preserve their workspace and distinct revisions in current and remembered context", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  const provider = new ObservingProvider();
  const life = secretary(state, provider);
  await life.start();
  try {
    for (const revision of [1, 2]) {
      addInput(state, `message-event-${revision}`, {
        actor_kind: "human",
        actor_id: "human-one",
        source_surface: "messaging",
        thread_id: "place-one",
        payload: {
          text: revision === 1 ? "Original message" : "Edited message",
          actor: { display_name: "Haru" },
          place: { name: "general", kind: "channel" },
          workspace_id: "workspace-one",
          message_id: "message-one",
          message_revision: revision,
          ...(revision === 2 ? { message_change: "edited" } : {}),
        },
      });
      assert.equal(await life.step(), "turn");
    }
  } finally {
    await life.stop();
  }
  const events = (await state.events(PERSONA, 0)).filter(
    (event) => event.kind === "input_received",
  );
  for (const [index, event] of events.entries()) {
    const message = provider.requests[index]!.messages.at(-1)!;
    assert.match(message.content, /workspace_id=workspace-one/);
    assert.match(message.content, /place_id=place-one message_id=message-one/);
    assert.ok(message.content.includes(`message_revision=${index + 1}`));
    assert.equal(event.payload.workspace_id, "workspace-one");
    assert.equal(event.payload.message_revision, index + 1);
    assert.deepEqual(eventMessage(event), message);
  }
  assert.ok(
    provider.requests[1]!.messages.some(
      (message) =>
        message.content.includes("message_revision=1") &&
        message.content.endsWith("Original message"),
    ),
  );
});

test("reference metadata is scoped to its actual input surface", () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  const input = addInput(state, "plain-message", {
    actor_kind: "human",
    source_surface: "test",
    payload: {
      text: "Hello",
      workspace_id: "not-a-messaging-event",
      session_id: "not-a-terminal-event",
      status: "closed",
      exit_code: 0,
    },
  });
  assert.equal(assemble([], input).at(-1)!.content, "[human] Hello");
});
