import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { test } from "node:test";
import {
  argumentsForRun,
  exactCorrectionApproval,
  workspaceProbe,
} from "./artifact-delivery.mjs";

const runner = new URL("./artifact-delivery.mjs", import.meta.url);
const args = [
  "--run",
  "--sha",
  "a".repeat(40),
  "--manifest",
  "/tmp/images.json",
  "--origin",
  "http://127.0.0.1:5173",
  "--issuer",
  "/tmp/issuer",
  "--provider",
  "opencode-go",
  "--model",
  "kimi-k2.7-code",
  "--browser",
  "/usr/bin/google-chrome",
];
test("no arguments skips without needing Docker or browser", () => {
  const result = spawnSync(process.execPath, [runner.pathname], {
    encoding: "utf8",
    env: { PATH: "" },
  });
  assert.ifError(result.error);
  assert.equal(result.status, 0);
  assert.equal(JSON.parse(result.stdout).status, "SKIPPED");
});
test("live execution requires opt-in and an exact credential-free origin", () => {
  assert.equal(argumentsForRun(args).model, "kimi-k2.7-code");
  assert.throws(() => argumentsForRun(args.slice(1)), /EXPLICIT_RUN_REQUIRED/);
  for (const origin of [
    "https://user:secret@example.com",
    "https://example.com/path",
    "https://example.com/?secret=x",
    "file:///tmp",
  ]) {
    const altered = [...args];
    altered[altered.indexOf("--origin") + 1] = origin;
    assert.throws(() => argumentsForRun(altered), /EXACT_ORIGIN_REQUIRED/);
  }
});
test("public claims cannot substitute for successful exact-byte tool results", () => {
  const probe = workspaceProbe("012345abcdef");
  assert.throws(() => probe.verify(), /FILE_SEQUENCE_INCOMPLETE/);
  probe.start(
    {
      tool_name: "read_file",
      tool_call_id: "read",
      args: { path: probe.path },
    },
    1,
  );
  assert.throws(
    () =>
      probe.end(
        {
          tool_call_id: "read",
          is_error: false,
          result: {
            tool_call_id: "read",
            tool_name: "read_file",
            is_error: false,
            details: { content: probe.edited, truncated: false },
          },
        },
        2,
      ),
    /CSV_READ_MISMATCH/,
  );
});
test("failed or mismatched tool completion is never accepted", () => {
  for (const variant of ["failed", "mismatched"]) {
    const probe = workspaceProbe("012345abcdef");
    probe.start(
      {
        tool_name: "write_file",
        tool_call_id: "write",
        args: { path: probe.path, content: probe.original },
      },
      1,
    );
    assert.throws(
      () =>
        probe.end(
          {
            tool_call_id: variant === "mismatched" ? "other" : "write",
            is_error: true,
            result: {},
          },
          2,
        ),
      /UNMATCHED_TOOL_RESULT|TOOL_EXECUTION_FAILED/,
    );
  }
});
test("correction approval rejects extra arguments and unrelated resource scopes", () => {
  const path = "report-012345abcdef.csv";
  const request = {
    id: "01a07fed-414f-77e2-8eb5-618c210ee453",
    tool_call_id: "edit",
    tool_name: "edit_file",
    action: {
      reviewable: {
        operation: "edit_file",
        capability: "mutate",
        resource_scopes: [
          {
            type: "resource",
            namespace: "sumi.foundation.workspace",
            kind: "path",
            id: path,
          },
        ],
      },
    },
    args_summary: {
      operation: "edit_file",
      path,
      old_string: "apples,2",
      new_string: "apples,3",
    },
  };
  assert.equal(
    exactCorrectionApproval(request, path, "MODEL_CORRECT_CSV"),
    true,
  );
  assert.equal(
    exactCorrectionApproval(
      {
        ...request,
        args_summary: { ...request.args_summary, replace_all: true },
      },
      path,
      "MODEL_CORRECT_CSV",
    ),
    false,
  );
  request.action.reviewable.resource_scopes.push({
    type: "resource",
    namespace: "sumi.foundation.workspace",
    kind: "path",
    id: "another.csv",
  });
  assert.equal(
    exactCorrectionApproval(request, path, "MODEL_CORRECT_CSV"),
    false,
  );
});
