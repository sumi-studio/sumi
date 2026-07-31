import assert from "node:assert/strict";
import test from "node:test";
import {
  createTodo,
  listTodos,
  TodoAPIError,
  updateTodo,
} from "../src/lib/todo-api.ts";

const sample = {
  id: "019c0000-0000-7000-8000-000000000010",
  title: "請求書を送る",
  description: "",
  status: "open",
  priority: "high",
  due: null,
  version: 1,
  via_agent: false,
  completed_at: null,
  created_at: "2026-07-31T00:00:00Z",
  updated_at: "2026-07-31T00:00:00Z",
};

test("listTodos encodes filters and sends browser credentials", async (t) => {
  const originalFetch = globalThis.fetch;
  t.after(() => {
    globalThis.fetch = originalFetch;
  });
  globalThis.fetch = async (input, init) => {
    assert.equal(
      String(input),
      "/v1/todos?status=open&overdue=true&q=100%25_ready&sort=due&limit=100",
    );
    assert.equal(init.credentials, "include");
    return Response.json({ items: [sample], total: 1 });
  };

  const result = await listTodos({
    status: "open",
    overdue: true,
    q: "100%_ready",
    sort: "due",
    limit: 100,
  });
  assert.equal(result.items[0].title, sample.title);
});

test("createTodo serializes the OpenAPI request body", async (t) => {
  const originalFetch = globalThis.fetch;
  t.after(() => {
    globalThis.fetch = originalFetch;
  });
  globalThis.fetch = async (input, init) => {
    assert.equal(String(input), "/v1/todos");
    assert.equal(init.method, "POST");
    assert.deepEqual(JSON.parse(String(init.body)), {
      title: sample.title,
      priority: "high",
    });
    return Response.json(sample, { status: 201 });
  };

  const result = await createTodo({ title: sample.title, priority: "high" });
  assert.equal(result.version, 1);
});

test("updateTodo exposes version conflict details", async (t) => {
  const originalFetch = globalThis.fetch;
  t.after(() => {
    globalThis.fetch = originalFetch;
  });
  globalThis.fetch = async () =>
    Response.json(
      {
        error: {
          code: "version_conflict",
          message: "todo was updated by another request",
          current_version: 4,
        },
      },
      { status: 409 },
    );

  await assert.rejects(
    updateTodo(sample.id, { expected_version: 3, status: "done" }),
    (error) => {
      assert.ok(error instanceof TodoAPIError);
      assert.equal(error.status, 409);
      assert.equal(error.code, "version_conflict");
      assert.equal(error.currentVersion, 4);
      return true;
    },
  );
});
