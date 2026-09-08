#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
// Opt-in acceptance for the local Docker-managed sumi-dev deployment. Always uses a fresh identity.
// --self-check is pure: no Docker, browser, network, or database operations.
import fs from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath } from "node:url";

const SHA = /^[0-9a-f]{40}$/,
  IID = /^sha256:[0-9a-f]{64}$/;
const UUID =
  /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const repositoryRoot = fileURLToPath(new URL("../../", import.meta.url));
const roles = ["api", "agent", "provisioner", "web"];
const source = "https://github.com/sumi-studio/sumi";
class CheckFailure extends Error {
  constructor(code) {
    super(code);
    this.code = code;
  }
}
function check(ok, code) {
  if (!ok) throw new CheckFailure(code);
}
function safeFailure(error) {
  return error instanceof CheckFailure ? error.code : "STEP_FAILED";
}
function uuidv7() {
  const b = randomBytes(16);
  b.writeUIntBE(Date.now(), 0, 6);
  b[6] = (b[6] & 15) | 112;
  b[8] = (b[8] & 63) | 128;
  const h = b.toString("hex");
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}
function ownerMatches(value, human) {
  return (
    value?.app_id === "direct-chat" &&
    value?.owner?.kind === "participant" &&
    value.owner.participant?.kind === "human" &&
    value.owner.participant.human_id === human
  );
}
function seedSQL(human, agent, marker, key) {
  check(
    UUID.test(human) &&
      UUID.test(agent) &&
      human !== agent &&
      /^[a-f0-9]{12}$/.test(marker) &&
      /^[a-f0-9]{64}$/.test(key),
    "INVALID_FIXTURE",
  );
  return `BEGIN;
INSERT INTO humans (human_id,display_name) VALUES ('${human}','Sumi deployment verification ${marker}');
INSERT INTO agents (personality_agent_id,human_id,display_name,warmth) VALUES ('${agent}','${human}','Verification ${marker}','cold');
INSERT INTO employments (agent_id,employer_type,employer_id) VALUES ('${agent}','human','${human}');
INSERT INTO agent_secrets (personality_agent_id,wrapping_key_id,wrapping_key) VALUES ('${agent}','dogfood-verification/v1/${marker}','${key}');
COMMIT;`;
}
function run(code, command, args, options = {}) {
  const r = spawnSync(command, args, {
    encoding: "utf8",
    stdio: ["pipe", "pipe", "pipe"],
    timeout: 30000,
    maxBuffer: 4 * 1024 * 1024,
    ...options,
  });
  check(!r.error && r.status === 0, code);
  return r.stdout;
}
function docker(args) {
  return run("DOCKER_READ_FAILED", "docker", ["--context", "default", ...args]);
}
function inspect(args) {
  return JSON.parse(docker(args));
}
function readPrivateJSON(file) {
  const s = fs.lstatSync(file);
  check(
    s.isFile() &&
      s.uid === process.getuid() &&
      !(s.mode & 63) &&
      s.size <= 1048576,
    "MANIFEST_NOT_PRIVATE",
  );
  return JSON.parse(fs.readFileSync(file, "utf8"));
}
export function argumentsForRun(args) {
  check(args[0] === "--run", "EXPLICIT_RUN_REQUIRED");
  args = args.slice(1);
  check(
    args.length === 14,
    "USAGE_SHA_MANIFEST_ORIGIN_ISSUER_PROVIDER_MODEL_BROWSER_REQUIRED",
  );
  const out = {};
  for (let i = 0; i < args.length; i += 2) {
    check(
      [
        "--sha",
        "--manifest",
        "--origin",
        "--issuer",
        "--provider",
        "--model",
        "--browser",
      ].includes(args[i]) && !out[args[i]],
      "INVALID_ARGUMENT",
    );
    out[args[i]] = args[i + 1];
  }
  check(
    SHA.test(out["--sha"]) &&
      path.isAbsolute(out["--manifest"]) &&
      path.isAbsolute(out["--issuer"]),
    "INVALID_INPUT",
  );
  const u = new URL(out["--origin"]);
  check(
    ["http:", "https:"].includes(u.protocol) &&
      !u.username &&
      !u.password &&
      u.pathname === "/" &&
      !u.search &&
      !u.hash,
    "EXACT_ORIGIN_REQUIRED",
  );
  check(path.isAbsolute(out["--browser"]), "ABSOLUTE_BROWSER_REQUIRED");
  check(
    [out["--provider"], out["--model"]].every(
      (v) =>
        typeof v === "string" && /^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$/.test(v),
    ),
    "INVALID_EXPECTED_MODEL",
  );
  return {
    sha: out["--sha"],
    manifest: out["--manifest"],
    origin: u.origin,
    issuer: out["--issuer"],
    provider: out["--provider"],
    model: out["--model"],
    browser: out["--browser"],
  };
}

export function exactCorrectionApproval(request, csvPath, phase) {
  const a = request?.action?.reviewable,
    x = request?.args_summary,
    scopes = a?.resource_scopes;
  return (
    phase === "MODEL_CORRECT_CSV" &&
    UUID.test(request?.id) &&
    typeof request?.tool_call_id === "string" &&
    request.tool_call_id.length > 0 &&
    request.tool_name === "edit_file" &&
    a?.operation === "edit_file" &&
    a.capability === "mutate" &&
    Array.isArray(scopes) &&
    scopes.length === 1 &&
    scopes[0].type === "resource" &&
    scopes[0].namespace === "sumi.foundation.workspace" &&
    scopes[0].kind === "path" &&
    scopes[0].id === csvPath &&
    x?.operation === "edit_file" &&
    x.path === csvPath &&
    x.old_string === "apples,2" &&
    x.new_string === "apples,3" &&
    Object.keys(x).sort().join(",") === "new_string,old_string,operation,path"
  );
}

export function workspaceProbe(marker) {
  const path = `report-${marker}.csv`,
    original = "item,count\napples,2\n",
    edited = "item,count\napples,3\n";
  let stage = 1,
    active = null,
    destination = null;
  const completed = [];
  return {
    path,
    original,
    edited,
    completed,
    setStage(value) {
      stage = value;
    },
    setDestination(value) {
      destination = value;
    },
    start(event, seq) {
      check(!active, "PARALLEL_TOOL_NOT_EXPECTED");
      const name = event.tool_name,
        a = event.args;
      check(a && typeof a === "object", "TOOL_ARGUMENTS_MISSING");
      if (stage === 1)
        check(
          (name === "write_file" &&
            a.path === path &&
            a.content === original) ||
            (name === "read_file" && a.path === path),
          "CREATE_TOOL_SCOPE_MISMATCH",
        );
      else if (stage === 2)
        check(
          (name === "edit_file" &&
            a.path === path &&
            a.old_string === "apples,2" &&
            a.new_string === "apples,3") ||
            (name === "read_file" && a.path === path),
          "CORRECTION_TOOL_SCOPE_MISMATCH",
        );
      else if (name === "workspace_invitation_list")
        check(Object.keys(a).length === 0, "INVITATION_LIST_SCOPE");
      else if (name === "workspace_invitation_accept")
        check(
          a.invitation_id === destination.invite,
          "INVITATION_TARGET_MISMATCH",
        );
      else if (name === "messaging") {
        check(
          a.workspace_id === destination.workspace,
          "MESSAGING_WORKSPACE_MISMATCH",
        );
        check(
          ["overview", "open", "write"].includes(a.action),
          "UNEXPECTED_MESSAGING_MUTATION",
        );
        if (a.action === "open")
          check(a.place_id === destination.place, "MESSAGING_PLACE_MISMATCH");
        if (a.action === "write")
          check(
            a.content === `CSV delivery ${marker}` &&
              JSON.stringify(a.attachments) === JSON.stringify([path]) &&
              completed.filter(
                (x) => x.name === "messaging" && x.action === "write",
              ).length === 0,
            "DELIVERY_CONTENT_MISMATCH",
          );
      } else check(false, "UNEXPECTED_DELIVERY_TOOL");
      active = { id: event.tool_call_id, name, args: a, start_seq: seq, stage };
    },
    end(event, seq) {
      check(
        active && event.tool_call_id === active.id,
        "UNMATCHED_TOOL_RESULT",
      );
      const r = event.result;
      check(
        event.is_error === false &&
          r?.is_error === false &&
          r.tool_call_id === active.id &&
          r.tool_name === active.name,
        "TOOL_EXECUTION_FAILED",
      );
      if (active.name === "read_file")
        check(
          r.details?.content === (stage === 1 ? original : edited) &&
            r.details.truncated === false,
          "CSV_READ_MISMATCH",
        );
      if (active.name === "write_file")
        check(r.details?.written === true, "CSV_WRITE_UNCONFIRMED");
      if (active.name === "edit_file")
        check(r.details?.edited === true, "CSV_EDIT_UNCONFIRMED");
      completed.push({
        stage: active.stage,
        name: active.name,
        action: active.args.action,
        call_id: active.id,
        start_seq: active.start_seq,
        end_seq: seq,
      });
      active = null;
    },
    verify() {
      check(!active, "TOOL_STILL_ACTIVE");
      const own = completed.filter((x) => x.stage === stage);
      if (stage < 3)
        check(
          own.length === 2 &&
            own[0].name === (stage === 1 ? "write_file" : "edit_file") &&
            own[1].name === "read_file",
          "FILE_SEQUENCE_INCOMPLETE",
        );
      else
        check(
          own.some((x) => x.name === "workspace_invitation_accept") &&
            own.some((x) => x.name === "messaging" && x.action === "open") &&
            own.filter((x) => x.name === "messaging" && x.action === "write")
              .length === 1,
          "DELIVERY_TOOL_SEQUENCE_INCOMPLETE",
        );
    },
  };
}

let fixtureMayExist = false;

async function main() {
  // biome-ignore lint/suspicious/noUndeclaredEnvVars: Disable inherited browser debug logging of authenticated traffic.
  delete process.env.DEBUG;
  // biome-ignore lint/suspicious/noUndeclaredEnvVars: An opt-in headless run must not wait on an inherited inspector.
  delete process.env.PWDEBUG;
  const input = argumentsForRun(process.argv.slice(2));
  const build = readPrivateJSON(input.manifest);
  check(
    build.schema_version === 2 &&
      build.status === "COMPLETE" &&
      build.source?.revision === input.sha &&
      build.source?.repository === source &&
      build.build?.requested_tag === input.sha,
    "BUILD_MANIFEST_MISMATCH",
  );
  check(
    Array.isArray(build.images) && build.images.length === 4,
    "FOUR_IMAGES_REQUIRED",
  );
  const expected = Object.fromEntries(build.images.map((x) => [x.role, x.id]));
  check(
    roles.every((r) => IID.test(expected[r])) &&
      new Set(Object.values(expected)).size === 4,
    "IMAGE_IDS_INVALID",
  );
  check(
    build.images.every(
      (x) =>
        x.requested_reference ===
          `ghcr.io/sumi-studio/sumi-${x.role}:${input.sha}` &&
        x.labels?.["org.opencontainers.image.revision"] === input.sha,
    ),
    "IMAGE_LABELS_INVALID",
  );
  const issuerStat = fs.lstatSync(input.issuer);
  check(
    issuerStat.isFile() &&
      issuerStat.uid === process.getuid() &&
      !(issuerStat.mode & 18) &&
      issuerStat.mode & 64,
    "ISSUER_NOT_OWNED_EXECUTABLE",
  );
  const directory = fs.mkdtempSync("/tmp/sumi-artifact-delivery-acceptance-");
  fs.chmodSync(directory, 448);
  const evidenceFile = path.join(directory, "evidence.json");
  const human = uuidv7(),
    agent = uuidv7(),
    marker = randomBytes(6).toString("hex");
  const evidence = {
    schemaVersion: 1,
    expectedModel: { provider: input.provider, model: input.model },
    status: "PENDING",
    sha: input.sha,
    origin: input.origin,
    human,
    agent,
    marker,
    phase: "PRECHECK",
    fixture: "not-created",
    cleanup: {},
    retainedState: true,
  };
  const save = () =>
    fs.writeFileSync(evidenceFile, `${JSON.stringify(evidence, null, 2)}\n`, {
      mode: 384,
    });
  const phase = (name) => {
    evidence.phase = name;
    save();
  };
  save();
  let browser, context, page, installation, cookie, failure;
  try {
    const controls = inspect([
      "inspect",
      "sumi-dev-api-1",
      "sumi-dev-runtime-provisioner-1",
      "sumi-dev-web-1",
    ]);
    for (const [i, role] of ["api", "provisioner", "web"].entries())
      check(
        controls[i].State.Running &&
          controls[i].Image === expected[role] &&
          controls[i].Config.Labels["com.docker.compose.project"] ===
            "sumi-dev",
        "DEPLOYED_IMAGES_MISMATCH",
      );
    const apiEnv = Object.fromEntries(
      controls[0].Config.Env.map((s) => s.split(/=(.*)/s).slice(0, 2)),
    );
    const provisionerEnv = Object.fromEntries(
      controls[1].Config.Env.map((s) => s.split(/=(.*)/s).slice(0, 2)),
    );
    check(
      provisionerEnv.SUMI_AGENT_IMAGE_TAG === input.sha &&
        provisionerEnv.SUMI_AGENT_IMAGE_PULL_POLICY === "never",
      "AGENT_SELECTION_MISMATCH",
    );
    check(
      inspect([
        "image",
        "inspect",
        `ghcr.io/sumi-studio/sumi-agent:${input.sha}`,
      ])[0].Id === expected.agent,
      "AGENT_IMAGE_MISMATCH",
    );
    check(
      (apiEnv.SUMI_BROWSER_WS_ALLOWED_ORIGINS || "")
        .split(",")
        .map((s) => s.trim())
        .includes(input.origin),
      "ORIGIN_NOT_ALLOWED",
    );
    check(
      apiEnv.SUMI_BROWSER_SESSION_SECRET &&
        apiEnv.SUMI_AUTH_TENANT_ID &&
        apiEnv.SUMI_PROVIDER_API_KEY &&
        apiEnv.SUMI_PROVIDER_API_KEY !== "dummy",
      "RUNTIME_CONFIGURATION_MISSING",
    );
    evidence.images = expected;
    // Resolve browser dependencies before creating any durable fixture.
    const { chromium, expect } = createRequire(
      path.join(repositoryRoot, "apps/web/package.json"),
    )("@playwright/test");
    check(fs.statSync(input.browser).isFile(), "BROWSER_EXECUTABLE_MISSING");
    browser = await chromium.launch({
      executablePath: input.browser,
      headless: true,
      timeout: 30000,
    });
    phase("SEED_FIXTURE");
    fixtureMayExist = true;
    evidence.fixture = "commit-unknown";
    save();
    run(
      "FIXTURE_COMMIT_UNCONFIRMED",
      "docker",
      [
        "--context",
        "default",
        "exec",
        "-i",
        "sumi-dev-postgres-1",
        "sh",
        "-c",
        'exec psql -X -q -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"',
      ],
      { input: seedSQL(human, agent, marker, randomBytes(32).toString("hex")) },
    );
    evidence.fixture = "committed";
    save();
    const ownership = run(
      "FIXTURE_OWNERSHIP_READ_FAILED",
      "docker",
      [
        "--context",
        "default",
        "exec",
        "-i",
        "sumi-dev-postgres-1",
        "sh",
        "-c",
        'exec psql -X -q -t -A -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"',
      ],
      {
        input: `SELECT count(*) FROM agents a JOIN humans h ON h.human_id=a.human_id JOIN employments e ON e.agent_id=a.personality_agent_id JOIN agent_secrets s ON s.personality_agent_id=a.personality_agent_id WHERE a.personality_agent_id='${agent}' AND h.human_id='${human}' AND e.employer_type='human' AND e.employer_id='${human}' AND e.ended_at IS NULL AND s.wrapping_key_id='dogfood-verification/v1/${marker}';`,
      },
    ).trim();
    check(ownership === "1", "FIXTURE_OWNERSHIP_MISMATCH");
    phase("ISSUE_SESSION");
    cookie = run("SESSION_ISSUER_FAILED", input.issuer, [], {
      env: {
        PATH: process.env.PATH,
        LANG: "C",
        SUMI_BROWSER_SESSION_SECRET: apiEnv.SUMI_BROWSER_SESSION_SECRET,
        SUMI_BROWSER_SESSION_AUDIENCE:
          apiEnv.SUMI_BROWSER_SESSION_AUDIENCE || "",
        SUMI_E2E_SESSION_TENANT_ID: apiEnv.SUMI_AUTH_TENANT_ID,
        SUMI_E2E_SESSION_USER_ID: human,
        SUMI_E2E_SESSION_PERSONALITY_AGENT_ID: agent,
      },
    }).trim();
    check(
      cookie.length > 0 && cookie.length <= 4096 && !/\s/.test(cookie),
      "INVALID_SESSION_OUTPUT",
    );
    evidence.sessionExpiresBy = new Date(Date.now() + 15 * 60000).toISOString();
    phase("BROWSER_SESSION");
    context = await browser.newContext({
      viewport: { width: 1280, height: 900 },
    });
    context.setDefaultTimeout(10000);
    context.setDefaultNavigationTimeout(30000);
    page = await context.newPage();
    await context.addCookies([
      {
        name: "sumi_session",
        value: cookie,
        domain: new URL(input.origin).hostname,
        path: "/",
        httpOnly: true,
        secure: new URL(input.origin).protocol === "https:",
        sameSite: "Lax",
      },
    ]);
    let epoch = 0,
      ended = 0,
      observationFailure = null,
      sentCommands = 0,
      invalidSocketScope = false,
      pendingCorrectionApproval = null,
      approvalCommands = 0;
    evidence.approvals = [];
    const probe = workspaceProbe(marker),
      observedSeqs = new Set();
    evidence.toolOperations = probe.completed;
    const replies = [];
    page.on("websocket", (socket) => {
      const socketURL = new URL(socket.url());
      if (socketURL.pathname !== "/direct-chat/ws") return;
      if (
        !installation ||
        socketURL.host !== new URL(input.origin).host ||
        socketURL.searchParams.get("installation_id") !== installation.id ||
        socketURL.searchParams.get("authority_epoch") !== installation.epoch
      ) {
        invalidSocketScope = true;
        return;
      }
      const openedEpoch = epoch;
      socket.on("framesent", ({ payload }) => {
        try {
          const frame = JSON.parse(String(payload));
          if (frame.type === "command") {
            if (frame.command?.type === "approval_decision") {
              const approved = evidence.approvals.find(
                (a) => a.id === frame.command.request_id && a.uiClicked,
              );
              check(
                approved &&
                  frame.command.decision?.type === "approve_once" &&
                  approvalCommands === 0,
                "UNEXPECTED_APPROVAL_COMMAND",
              );
              approvalCommands++;
              evidence.approvalCommands = approvalCommands;
              save();
            } else sentCommands++;
          }
        } catch (error) {
          observationFailure =
            safeFailure(error); /* No raw frame persistence. */
        }
      });
      socket.on("framereceived", ({ payload }) => {
        try {
          const f = JSON.parse(String(payload));
          const e = f.type === "event" ? f.envelope?.event : null;
          if (openedEpoch === 0 && e && Number.isSafeInteger(f.envelope.seq)) {
            const seq = f.envelope.seq;
            if (observedSeqs.has(seq)) return;
            observedSeqs.add(seq);
            if (e.type === "approval_requested") {
              if (
                exactCorrectionApproval(
                  e.request,
                  probe.path,
                  evidence.phase,
                ) &&
                evidence.approvals.length === 0 &&
                !pendingCorrectionApproval
              ) {
                pendingCorrectionApproval = e.request;
                evidence.approvals.push({
                  id: e.request.id,
                  tool_call_id: e.request.tool_call_id,
                  tool_name: e.request.tool_name,
                  args_summary: e.request.args_summary,
                  reason: e.request.reason,
                  uiClicked: false,
                });
                save();
              } else {
                evidence.blocker = {
                  kind: "approval_requested",
                  request_id: e.request?.id || null,
                };
                save();
                observationFailure = "HUMAN_APPROVAL_REQUIRED";
              }
            }
            if (e.type === "tool_execution_start") {
              probe.start(e, seq);
              save();
            }
            if (e.type === "tool_execution_end") {
              probe.end(e, seq);
              save();
            }
          }
          if (e?.type === "agent_end" && openedEpoch === 0)
            ended = Number(f.envelope.seq);
          if (e?.type !== "message_end" || e.message?.role !== "assistant")
            return;
          const m = e.message,
            text = (m.content || [])
              .filter((x) => x.type === "text")
              .map((x) => x.text)
              .join("");
          if (
            m.stop_reason === "stop" &&
            m.error_message === null &&
            !m.interrupted &&
            UUID.test(e.message_id) &&
            Number.isSafeInteger(f.envelope.seq) &&
            f.envelope.seq > 0 &&
            text.length <= 4096
          )
            replies.push({
              epoch: openedEpoch,
              id: e.message_id,
              seq: f.envelope.seq,
              text,
              model: String(m.model).slice(0, 128),
              provider: String(m.provider).slice(0, 128),
            });
        } catch (error) {
          observationFailure =
            safeFailure(error); /* No raw frames or reasoning persisted. */
        }
      });
    });
    const sessionIsOwned = async () => {
      const value = await page.evaluate(async () => {
        const r = await fetch("/auth/session", {
          credentials: "include",
          cache: "no-store",
          signal: AbortSignal.timeout(8000),
        });
        return { status: r.status, body: await r.json() };
      });
      check(
        value.status === 200 &&
          value.body.authenticated === true &&
          value.body.user?.id === human,
        "SESSION_OWNER_MISMATCH",
      );
    };
    await page.goto(input.origin);
    await sessionIsOwned();
    phase("CREATE_OWNED_SHARED_PLACE");
    const humanPage = await context.newPage();
    await humanPage.goto(input.origin);
    await humanPage
      .getByRole("textbox", { name: "新しいWorkspaceの名前" })
      .fill(`Delivery ${marker}`);
    const createdWorkspace = humanPage
      .waitForResponse(
        (r) =>
          r.request().method() === "POST" &&
          new URL(r.url()).pathname === "/workspaces",
      )
      .catch(() => null);
    await humanPage
      .getByRole("button", { name: "作成して開く", exact: true })
      .click();
    const workspaceResponse = await createdWorkspace;
    check(workspaceResponse?.status() === 201, "WORKSPACE_CREATE_FAILED");
    const workspace = (await workspaceResponse.json()).workspace_id;
    check(UUID.test(workspace), "WORKSPACE_ID_INVALID");
    evidence.delivery = { workspace };
    save();
    await humanPage.getByRole("button", { name: "参加者と招待" }).click();
    const invited = humanPage
      .waitForResponse(
        (r) =>
          r.request().method() === "POST" &&
          new URL(r.url()).pathname ===
            `/workspaces/${workspace}/invites/current-agent`,
      )
      .catch(() => null);
    await humanPage
      .getByRole("button", { name: "招待する", exact: true })
      .click();
    const inviteResponse = await invited;
    check(inviteResponse?.status() === 201, "INVITE_FAILED");
    const inviteBody = await inviteResponse.json();
    check(
      inviteBody.kind === "targeted_personality_agent" &&
        inviteBody.workspace_id === workspace &&
        UUID.test(inviteBody.invite_id),
      "INVITE_SCOPE_INVALID",
    );
    evidence.delivery.invite = inviteBody.invite_id;
    save();
    await humanPage
      .getByRole("button", { name: "アプリ", exact: true })
      .click();
    const installedMessaging = humanPage
      .waitForResponse(
        (r) =>
          r.request().method() === "POST" &&
          new URL(r.url()).pathname === "/app-installations",
      )
      .catch(() => null);
    await humanPage
      .getByRole("button", { name: "インストール", exact: true })
      .click();
    const messagingResponse = await installedMessaging;
    check(messagingResponse?.status() === 201, "MESSAGING_INSTALL_FAILED");
    const messaging = await messagingResponse.json();
    check(
      UUID.test(messaging.installation_id) &&
        messaging.authority_epoch === "1" &&
        messaging.state === "enabled" &&
        messaging.app_id === "messaging" &&
        messaging.owner?.kind === "workspace" &&
        messaging.owner.workspace_id === workspace,
      "MESSAGING_OWNER_MISMATCH",
    );
    evidence.delivery.messaging = {
      id: messaging.installation_id,
      epoch: messaging.authority_epoch,
    };
    save();
    await humanPage.getByRole("button", { name: "開く", exact: true }).click();
    await humanPage.getByTitle("チャンネルを作成").click();
    const channel = humanPage.getByRole("dialog", { name: "チャンネルを作成" });
    await channel
      .getByRole("textbox", { name: "名前", exact: true })
      .fill(`delivery-${marker}`);
    const createdPlace = humanPage
      .waitForResponse(
        (r) =>
          r.request().method() === "POST" &&
          new URL(r.url()).pathname === "/messaging/channels",
      )
      .catch(() => null);
    await channel.getByRole("button", { name: "作成", exact: true }).click();
    const placeResponse = await createdPlace;
    check(placeResponse?.status() === 201, "PLACE_CREATE_FAILED");
    const place = (await placeResponse.json()).channel_id;
    check(UUID.test(place), "PLACE_ID_INVALID");
    evidence.delivery.place = place;
    probe.setDestination({ workspace, place, invite: inviteBody.invite_id });
    save();
    phase("INSTALL_PERSONAL_APP");
    await page.getByRole("button", { name: "設定", exact: true }).click();
    await page
      .getByRole("dialog", { name: "設定" })
      .getByRole("button", { name: "個人用アプリ", exact: true })
      .click();
    const row = page
      .getByRole("dialog", { name: "個人用アプリ" })
      .getByText("Direct Chat", { exact: true })
      .locator("xpath=../..");
    const responsePromise = page
      .waitForResponse(
        (r) =>
          r.request().method() === "POST" &&
          new URL(r.url()).pathname === "/app-installations",
      )
      .catch(() => null);
    evidence.installationAttempted = true;
    save();
    await row.getByRole("button", { name: "導入", exact: true }).click();
    const response = await responsePromise;
    check(response?.status() === 201, "INSTALL_NOT_CONFIRMED");
    const installed = await response.json();
    check(
      ownerMatches(installed, human) &&
        UUID.test(installed.installation_id) &&
        installed.state === "enabled" &&
        installed.authority_epoch === "1",
      "INSTALL_OWNER_MISMATCH",
    );
    installation = {
      id: installed.installation_id,
      epoch: installed.authority_epoch,
    };
    evidence.installation = installation;
    save();
    await page.keyboard.press("Escape");
    await page.keyboard.press("Escape");
    phase("LAZY_AGENT_START");
    await page.getByRole("button", { name: "直通", exact: true }).click();
    await expect(
      page.getByText("エージェント利用可能", { exact: true }),
    ).toBeVisible({ timeout: 60000 });
    const project = `sumi-${agent.replaceAll("-", "")}`;
    const agentIDs = docker([
      "ps",
      "-q",
      "--filter",
      `label=com.docker.compose.project=${project}`,
    ])
      .trim()
      .split(/\s+/)
      .filter(Boolean);
    check(agentIDs.length > 0, "OWNED_RUNTIME_ABSENT");
    const runtime = inspect(["inspect", ...agentIDs]);
    for (const service of ["runtime", "executor", "broker"])
      check(
        runtime.some(
          (c) =>
            c.State.Running &&
            c.Image === expected.agent &&
            c.Config.Labels["com.docker.compose.service"] === service,
        ),
        "OWNED_RUNTIME_IMAGE_MISMATCH",
      );
    evidence.runtime = runtime.map((c) => ({
      id: c.Id,
      image: c.Image,
      service: c.Config.Labels["com.docker.compose.service"],
      project,
    }));
    const waitFor = async (condition, milliseconds) => {
      const deadline = Date.now() + milliseconds;
      while (!condition()) {
        check(!invalidSocketScope, "SOCKET_SCOPE_MISMATCH");
        check(!observationFailure, observationFailure || "OBSERVATION_FAILED");
        check(Date.now() < deadline, "COMPLETION_TIMEOUT");
        if (pendingCorrectionApproval) {
          const request = pendingCorrectionApproval;
          pendingCorrectionApproval = null;
          await sessionIsOwned();
          check(
            exactCorrectionApproval(request, probe.path, evidence.phase),
            "APPROVAL_SCOPE_CHANGED",
          );
          const summary =
            typeof request.action.reviewable === "string"
              ? request.action.reviewable
              : JSON.stringify(request.action.reviewable);
          const card = page
            .getByRole("alert")
            .filter({ has: page.getByText(summary, { exact: true }) });
          const button = card.getByRole("button", {
            name: "今回のみ許可",
            exact: true,
          });
          await expect(card).toHaveCount(1);
          await expect(button).toHaveCount(1);
          check(
            !observationFailure &&
              !invalidSocketScope &&
              !pendingCorrectionApproval,
            "APPROVAL_OBSERVATION_CHANGED",
          );
          check(
            evidence.approvals.length === 1 &&
              evidence.approvals[0].id === request.id &&
              exactCorrectionApproval(request, probe.path, evidence.phase),
            "APPROVAL_IDENTITY_CHANGED",
          );
          evidence.approvals[0].uiClicked = true;
          save();
          await button.click();
        }
        await new Promise((r) => setTimeout(r, 200));
      }
      check(!observationFailure, observationFailure || "OBSERVATION_FAILED");
    };
    const turns = [
      `Create ${probe.path} using write_file with exact UTF-8 content ${JSON.stringify(probe.original)}, then read_file to verify it. Do not send it yet. Use only these two tools once each. Reply CREATE ${marker} after confirmed success.`,
      `Correction: change apples,2 to apples,3 in ${probe.path} with edit_file, then read_file to verify. Do not send it yet. Use only these two tools once each. Reply CORRECTED ${marker} after confirmed success.`,
      `Deliver the corrected CSV ${probe.path} to the shared Workspace ${workspace}, place ${place}. List your invitations and accept only invitation ${inviteBody.invite_id} for this Workspace. Use messaging to open exactly that place, then messaging action write with content ${JSON.stringify(`CSV delivery ${marker}`)} and attachments ${JSON.stringify([probe.path])}. This attachments field is the workspace-file path; let the existing tool upload it. Do not write a substitute message or claim delivery on error. Reply DELIVERED ${marker} after confirmed success.`,
    ];
    let reply;
    for (let turn = 0; turn < turns.length; turn++) {
      phase(
        ["MODEL_CREATE_CSV", "MODEL_CORRECT_CSV", "MODEL_DELIVER_CSV"][turn],
      );
      probe.setStage(turn + 1);
      const after = ended;
      await page
        .getByRole("textbox", { name: "メッセージ", exact: true })
        .fill(turns[turn]);
      evidence.commandAttempted = true;
      save();
      await page.getByRole("button", { name: "送信", exact: true }).click();
      await waitFor(() => ended > after, 240000);
      reply = replies.findLast(
        (r) => r.epoch === 0 && r.seq > after && ended > r.seq,
      );
      evidence.lastReply = reply || null;
      save();
      check(reply, "RUN_ENDED_WITHOUT_PUBLIC_REPLY");
      check(
        reply.provider === input.provider && reply.model === input.model,
        "ACTUAL_MODEL_MISMATCH",
      );
      probe.verify();
      check(reply.text.includes(marker), "CONFIRMATION_MARKER_MISSING");
      if (turn === 1 && evidence.approvals.length) {
        check(
          approvalCommands === 1 &&
            probe.completed.some(
              (t) =>
                t.stage === 2 &&
                t.name === "edit_file" &&
                t.call_id === evidence.approvals[0].tool_call_id,
            ),
          "APPROVED_CALL_NOT_EXECUTED",
        );
        evidence.approvals[0].executionConfirmed = true;
        save();
      }
      check(sentCommands === turn + 1, "COMMAND_COUNT_MISMATCH");
    }
    phase("HUMAN_DOWNLOAD_EXACT_CSV");
    // Download from the real Human-facing message card, not a tool receipt or
    // a synthetic anchor. This proves the uploaded message is actually visible.
    const card = humanPage.getByRole("link", {
      name: probe.path,
      exact: false,
    });
    await expect(card).toHaveCount(1, { timeout: 30000 });
    const history = await humanPage.evaluate(
      async ({ workspace, place, messaging }) => {
        const scope = new URLSearchParams({
          workspace_id: workspace,
          installation_id: messaging.installation_id,
          authority_epoch: messaging.authority_epoch,
        });
        const r = await fetch(`/messaging/places/${place}/messages?${scope}`, {
          credentials: "include",
          cache: "no-store",
          signal: AbortSignal.timeout(8000),
        });
        return { status: r.status, body: await r.json() };
      },
      { workspace, place, messaging },
    );
    check(
      history.status === 200 && history.body.messages.length === 1,
      "DELIVERED_MESSAGE_COUNT_MISMATCH",
    );
    const delivered = history.body.messages[0];
    check(
      delivered.author?.kind === "personality_agent" &&
        delivered.author.personality_agent_id === agent &&
        delivered.content === `CSV delivery ${marker}` &&
        delivered.attachments?.length === 1,
      "DELIVERED_AUTHOR_OR_CONTENT_MISMATCH",
    );
    evidence.delivery.message = {
      id: delivered.message_id,
      seq: delivered.seq,
      author: delivered.author,
    };
    const href = await card.getAttribute("href");
    const url = new URL(href, input.origin);
    check(
      url.origin === input.origin &&
        url.pathname.startsWith("/messaging/attachments/"),
      "DOWNLOAD_SCOPE_MISMATCH",
    );
    check(
      url.searchParams.get("workspace_id") === workspace &&
        url.searchParams.get("installation_id") === messaging.installation_id &&
        url.searchParams.get("authority_epoch") === messaging.authority_epoch,
      "DOWNLOAD_AUTHORITY_MISMATCH",
    );
    const downloadPromise = humanPage
      .waitForEvent("download", { timeout: 30000 })
      .catch(() => null);
    await card.click();
    const download = await downloadPromise;
    check(download, "DOWNLOAD_NOT_CONFIRMED");
    check(
      (await download.failure()) === null &&
        download.suggestedFilename() === probe.path,
      "DOWNLOAD_FAILED",
    );
    const downloaded = await download.path();
    check(downloaded, "DOWNLOAD_PATH_MISSING");
    const bytes = fs.readFileSync(downloaded);
    check(
      bytes.equals(Buffer.from(probe.edited, "utf8")),
      "DELIVERED_CSV_BYTES_MISMATCH",
    );
    evidence.delivery.download = {
      filename: probe.path,
      bytes: bytes.length,
      sha256: createHash("sha256").update(bytes).digest("hex"),
      content: probe.edited,
    };
    evidence.reply = reply;
    evidence.sentCommandsBeforeReload = sentCommands;
    evidence.approvalCommandsBeforeReload = approvalCommands;
    save();
    phase("RELOAD_DURABILITY");
    epoch = 1;
    await page.reload();
    await sessionIsOwned();
    await waitFor(
      () =>
        replies.some(
          (r) =>
            r.epoch === 1 &&
            r.id === reply.id &&
            r.seq === reply.seq &&
            r.text === reply.text,
        ),
      30000,
    );
    await expect(page.getByText(reply.text, { exact: true })).toBeVisible();
    check(
      sentCommands === evidence.sentCommandsBeforeReload &&
        approvalCommands === evidence.approvalCommandsBeforeReload,
      "RELOAD_SENT_ANOTHER_COMMAND",
    );
    const finalControls = inspect([
      "inspect",
      "sumi-dev-api-1",
      "sumi-dev-runtime-provisioner-1",
      "sumi-dev-web-1",
    ]);
    for (const [i, role] of ["api", "provisioner", "web"].entries())
      check(
        finalControls[i].State.Running &&
          finalControls[i].Image === expected[role],
        "DEPLOYMENT_CHANGED_DURING_ACCEPTANCE",
      );
    const finalRuntime = inspect(["inspect", ...runtime.map((c) => c.Id)]);
    for (const service of ["runtime", "executor", "broker"]) {
      const original = runtime.find(
        (c) => c.Config.Labels["com.docker.compose.service"] === service,
      );
      check(
        finalRuntime.some(
          (c) =>
            c.Id === original.Id &&
            c.State.Running &&
            c.Image === expected.agent &&
            c.Config.Labels["com.docker.compose.project"] === project &&
            c.Config.Labels["com.docker.compose.service"] === service,
        ),
        "OWNED_RUNTIME_CHANGED_DURING_ACCEPTANCE",
      );
    }
    evidence.runtimeRechecked = true;
    evidence.reloadConfirmed = true;
    evidence.sentCommandsAfterReload = sentCommands;
    evidence.status = "PASSED";
  } catch (error) {
    failure = safeFailure(error);
    evidence.status =
      failure === "HUMAN_APPROVAL_REQUIRED" ? "BLOCKED" : "FAILED";
    evidence.failure = failure;
  } finally {
    evidence.lastWorkPhase = evidence.phase;
    evidence.phase = "CLEANUP";
    try {
      save();
    } catch {
      evidence.cleanup.evidenceWriteFailed = true;
    }
    if (
      page &&
      !page.isClosed() &&
      new URL(page.url()).origin === input.origin
    ) {
      try {
        const cleanup = await page.evaluate(
          async ({
            installation,
            installationAttempted,
            delivery,
            human,
            uuidPattern,
          }) => {
            const result = {};
            const request = (url, options = {}) =>
              fetch(url, {
                credentials: "include",
                cache: "no-store",
                ...options,
                signal: AbortSignal.timeout(8000),
              });
            const session = await request("/auth/session").then((r) =>
              r.json(),
            );
            if (session.authenticated !== true || session.user?.id !== human)
              return { sessionOwned: false };
            try {
              // Recover only this fresh Human's installation if the POST committed but its reply was lost.
              if (!installation && installationAttempted) {
                const r = await request(
                  `/app-installations?owner_kind=participant&participant_kind=human&owner_id=${human}`,
                );
                if (r.ok) {
                  const owned = (await r.json()).installations.filter(
                    (v) =>
                      v.app_id === "direct-chat" &&
                      v.owner?.kind === "participant" &&
                      v.owner.participant?.kind === "human" &&
                      v.owner.participant.human_id === human,
                  );
                  if (
                    owned.length === 1 &&
                    new RegExp(uuidPattern).test(owned[0].installation_id) &&
                    /^[1-9][0-9]*$/.test(owned[0].authority_epoch)
                  )
                    installation = {
                      id: owned[0].installation_id,
                      epoch: owned[0].authority_epoch,
                    };
                  result.installationAbsent = owned.length === 0;
                }
              }
              if (installation) {
                result.installation = installation;
                const r = await request(
                  `/app-installations/${installation.id}/state`,
                  {
                    method: "PUT",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({
                      state: "disabled",
                      expected_authority_epoch: installation.epoch,
                    }),
                  },
                );
                result.disableStatus = r.status;
                if (r.ok) {
                  const v = await r.json();
                  result.installationDisabled =
                    v.installation_id === installation.id &&
                    v.state === "disabled" &&
                    v.owner?.participant?.human_id === human;
                }
              }
            } catch {
              result.disableConfirmationFailed = true;
            }
            try {
              if (delivery?.workspace) {
                // Discover only the fresh Workspace's Messaging app, including a lost POST reply.
                const listed = await request(
                  `/app-installations?owner_kind=workspace&owner_id=${delivery.workspace}`,
                );
                if (listed.ok) {
                  const owned = (await listed.json()).installations.filter(
                    (v) =>
                      v.app_id === "messaging" &&
                      v.owner?.kind === "workspace" &&
                      v.owner.workspace_id === delivery.workspace,
                  );
                  result.messagingAbsent = owned.length === 0;
                  if (
                    owned.length === 1 &&
                    new RegExp(uuidPattern).test(owned[0].installation_id)
                  ) {
                    const app = owned[0];
                    const r = await request(
                      `/app-installations/${app.installation_id}/state`,
                      {
                        method: "PUT",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify({
                          state: "disabled",
                          expected_authority_epoch: app.authority_epoch,
                        }),
                      },
                    );
                    result.messagingDisableStatus = r.status;
                    if (r.ok) {
                      const v = await r.json();
                      result.messagingDisabled =
                        v.installation_id === app.installation_id &&
                        v.state === "disabled" &&
                        v.owner?.kind === "workspace" &&
                        v.owner.workspace_id === delivery.workspace;
                    }
                  }
                }
              }
            } catch {
              result.messagingDisableConfirmationFailed = true;
            }
            try {
              const csrf = await request("/auth/csrf");
              result.csrfStatus = csrf.status;
              if (csrf.ok) {
                const { csrf_token } = await csrf.json();
                const r = await request("/auth/logout", {
                  method: "POST",
                  headers: { "X-CSRF-Token": csrf_token },
                });
                result.logoutStatus = r.status;
                const status = await request("/auth/session").then((r) =>
                  r.json(),
                );
                result.sessionRevoked = r.ok && status.authenticated === false;
              }
            } catch {
              result.logoutConfirmationFailed = true;
            }
            return result;
          },
          {
            installation,
            installationAttempted: evidence.installationAttempted,
            delivery: evidence.delivery,
            human,
            uuidPattern: UUID.source,
          },
        );
        Object.assign(evidence.cleanup, cleanup);
      } catch {
        evidence.cleanup = { ...evidence.cleanup, confirmationFailed: true };
      }
    } else evidence.cleanup.notAttempted = true;
    try {
      await context?.close();
      evidence.cleanup.contextClosed = true;
    } catch {
      evidence.cleanup.contextClosed = false;
    }
    try {
      await browser?.close();
      evidence.cleanup.browserClosed = true;
    } catch {
      evidence.cleanup.browserClosed = false;
    }
    cookie = undefined;
    if (
      evidence.status === "PASSED" &&
      (!evidence.cleanup.sessionRevoked ||
        !evidence.cleanup.installationDisabled ||
        !evidence.cleanup.messagingDisabled ||
        !evidence.cleanup.contextClosed ||
        !evidence.cleanup.browserClosed)
    )
      evidence.status = "PASSED_CLEANUP_INCOMPLETE";
    evidence.completedAt = new Date().toISOString();
    evidence.phase = "FINISHED";
    save();
    process.stdout.write(
      `${JSON.stringify({
        status: evidence.status,
        stage: evidence.lastWorkPhase,
        failure,
        evidence: evidenceFile,
        retainedState: true,
      })}\n`,
    );
    process.exitCode = evidence.status === "PASSED" ? 0 : 1;
  }
}

export function selfCheck() {
  const p = workspaceProbe("012345abcdef");
  let seq = 1;
  const complete = (name, args, details = {}) => {
    const id = `call-${seq}`;
    p.start({ tool_name: name, args, tool_call_id: id }, seq++);
    p.end(
      {
        tool_call_id: id,
        is_error: false,
        result: { tool_name: name, tool_call_id: id, is_error: false, details },
      },
      seq++,
    );
  };
  complete(
    "write_file",
    { path: p.path, content: p.original },
    { written: true },
  );
  complete(
    "read_file",
    { path: p.path },
    { content: p.original, truncated: false },
  );
  p.verify();
  p.setStage(2);
  complete(
    "edit_file",
    { path: p.path, old_string: "apples,2", new_string: "apples,3" },
    { edited: true },
  );
  complete(
    "read_file",
    { path: p.path },
    { content: p.edited, truncated: false },
  );
  p.verify();
  p.setDestination({
    workspace: "own-workspace",
    place: "own-place",
    invite: "own-invite",
  });
  p.setStage(3);
  complete("workspace_invitation_list", {});
  complete("workspace_invitation_accept", { invitation_id: "own-invite" });
  complete("messaging", {
    workspace_id: "own-workspace",
    action: "open",
    place_id: "own-place",
  });
  complete("messaging", {
    workspace_id: "own-workspace",
    action: "write",
    content: "CSV delivery 012345abcdef",
    attachments: [p.path],
  });
  p.verify();
  const request = {
    id: "01a07fed-414f-77e2-8eb5-618c210ee453",
    tool_call_id: "edit-test",
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
            id: p.path,
          },
        ],
      },
    },
    args_summary: {
      operation: "edit_file",
      path: p.path,
      old_string: "apples,2",
      new_string: "apples,3",
    },
  };
  check(
    exactCorrectionApproval(request, p.path, "MODEL_CORRECT_CSV"),
    "EXACT_APPROVAL_REJECTED",
  );
  check(
    !exactCorrectionApproval(request, p.path, "MODEL_DELIVER_CSV"),
    "WRONG_STAGE_APPROVAL_ACCEPTED",
  );
  check(
    !exactCorrectionApproval(
      {
        ...request,
        args_summary: { ...request.args_summary, path: "other.csv" },
      },
      p.path,
      "MODEL_CORRECT_CSV",
    ),
    "WRONG_PATH_APPROVAL_ACCEPTED",
  );
  check(
    !exactCorrectionApproval(
      {
        ...request,
        args_summary: { ...request.args_summary, new_string: "apples,999" },
      },
      p.path,
      "MODEL_CORRECT_CSV",
    ),
    "WRONG_EDIT_APPROVAL_ACCEPTED",
  );
  let refused = false;
  try {
    workspaceProbe("012345abcdef").verify();
  } catch {
    refused = true;
  }
  check(refused, "CLAIM_WITHOUT_TOOL_ACCEPTED");
  refused = false;
  try {
    p.start(
      {
        tool_call_id: "wrong",
        tool_name: "messaging",
        args: {
          workspace_id: "someone-else",
          action: "open",
          place_id: "own-place",
        },
      },
      seq++,
    );
  } catch {
    refused = true;
  }
  check(refused, "CROSS_WORKSPACE_CALL_ACCEPTED");
  process.stdout.write(
    "Pure CSV tool-sequence and scope checks passed; no external operations.\n",
  );
}

if (
  process.argv[1] &&
  path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)
) {
  if (process.argv[2] === "--self-check") selfCheck();
  else if (process.argv.length === 2 || process.argv[2] === "--help") {
    process.stdout.write(
      `${JSON.stringify({
        status: "SKIPPED",
        reason: "OPT_IN_REQUIRED",
        usage:
          "See scripts/agent-acceptance/README.md; --self-check performs no external operations.",
      })}\n`,
    );
  } else
    main().catch((error) => {
      process.stderr.write(
        `${JSON.stringify({
          status: "FAILED",
          phase: fixtureMayExist ? "UNHANDLED_AFTER_SEED" : "PRECHECK",
          failure: safeFailure(error),
          fixture: fixtureMayExist ? "creation-attempted" : "not-created",
        })}\n`,
      );
      process.exitCode = 1;
    });
}
