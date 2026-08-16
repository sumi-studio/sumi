import { type ChildProcess, spawn } from "node:child_process";
import { generateKeyPairSync, randomBytes } from "node:crypto";
import { once } from "node:events";
import { chmod, lstat, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import {
  createServer,
  type IncomingMessage,
  type ServerResponse,
} from "node:http";
import { createServer as createNetServer } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import type { BrowserContext } from "@playwright/test";

const supportDirectory = dirname(fileURLToPath(import.meta.url));
const repositoryRoot = resolve(supportDirectory, "../../../..");
const apiDirectory = resolve(repositoryRoot, "apps/api");
const agentDirectory = resolve(repositoryRoot, "apps/agent");
const webDirectory = resolve(repositoryRoot, "apps/web");
const maxChildLogCharacters = 64 * 1024;
const processStopTimeoutMilliseconds = 8_000;
const browserSessionAudience = "sumi:web";

export const firstUserMessage = "real browser turn one";
export const secondUserMessage = "real browser turn two";
export const firstProviderResponse = "real-agent-turn-one";
export const secondProviderResponse = "real-agent-turn-two-context-ok";
export const executorAuthorityProbeFile = "executor-authority-probe.txt";
export const humanMessagingAttachmentFile = "human-messaging-e2e.txt";
export const humanMessagingAttachmentContents =
  "exact Human Messaging attachment bytes\n";

const personalityAgentID = "0198f0f4-9b72-7000-8000-000000000001";
const realAgentHumanID = "0198f0f4-9b72-7000-8000-000000000002";
const workspaceBrowserHumanID = "0198f0f4-9b72-7000-8000-00000000e2e0";
const generation = "7";
const firebaseProjectID = "sumi-studio";
const firebaseWebAPIKey = "sumi-direct-chat-e2e-public-key";
const firebaseWebAppID = "1:000000000000:web:0000000000000000000000";
const invitationListToolCallID = "call-real-agent-invitation-list";
const invitationAcceptToolCallID = "call-real-agent-invitation-accept";
const workspaceListToolCallID = "call-real-agent-workspace-list";
const executorToolCallID = "call-real-agent-list-dir";
const messagingOverviewToolCallID = "call-real-agent-messaging-overview";
const messagingOpenHumanToolCallID = "call-real-agent-messaging-open-human";
const messagingOpenHumanAttachmentToolCallID =
  "call-real-agent-messaging-open-human-attachment";
const messagingWriteToolCallID = "call-real-agent-messaging-write";
const messagingOpenAgentToolCallID = "call-real-agent-messaging-open-agent";
const messagingOpenAgentAttachmentToolCallID =
  "call-real-agent-messaging-open-agent-attachment";
const executorAuthorityProbeContents = "exact executor authority probe\n";

export interface RealAgentBuild {
  directory: string;
  apiServer: string;
  sessionIssuer: string;
  agent: string;
}

export interface WorkspaceBrowserBuild {
  directory: string;
  apiServer: string;
  sessionIssuer: string;
}

export interface ProviderRequest {
  messages: unknown[];
  raw: Record<string, unknown>;
}

export class LoopbackChatProvider {
  readonly requests: ProviderRequest[] = [];
  requestCount = 0;
  contextVerified = false;
  executorToolVerified = false;
  invitationListVerified = false;
  invitationAcceptVerified = false;
  workspaceMembershipVerified = false;
  invitationID: string | undefined;
  workspaceID: string | undefined;
  workspaceName: string | undefined;
  messagingVerified = false;
  private messagingChannelID: string | undefined;
  private humanAttachmentID: string | undefined;
  private humanAttachmentDigest: string | undefined;
  private agentMessageID: string | undefined;
  private agentAttachmentID: string | undefined;
  private agentAttachmentDigest: string | undefined;
  url = "";

  private readonly apiKey: string;
  private readonly server = createServer((request, response) => {
    void this.handle(request, response);
  });

  constructor(apiKey: string) {
    this.apiKey = apiKey;
  }

  async start(): Promise<void> {
    this.server.listen(0, "127.0.0.1");
    await once(this.server, "listening");
    const address = this.server.address();
    if (!address || typeof address === "string") {
      throw new Error("loopback provider did not expose a TCP address");
    }
    this.url = `http://127.0.0.1:${address.port}`;
  }

  get reviewerForbiddenValues(): readonly string[] {
    return [
      this.workspaceID,
      this.workspaceName,
      this.messagingChannelID,
      this.humanAttachmentID,
      this.humanAttachmentDigest,
      this.agentMessageID,
      this.agentAttachmentID,
      this.agentAttachmentDigest,
      humanMessagingAttachmentFile,
      humanMessagingAttachmentContents,
      executorAuthorityProbeFile,
      executorAuthorityProbeContents,
    ].filter((value): value is string => typeof value === "string");
  }

  async stop(): Promise<void> {
    if (!this.server.listening) return;
    const closed = once(this.server, "close");
    this.server.close();
    this.server.closeAllConnections();
    await closed;
  }

  private async handle(
    request: IncomingMessage,
    response: ServerResponse,
  ): Promise<void> {
    try {
      if (request.method !== "POST" || request.url !== "/chat/completions") {
        respondJSON(response, 404, { error: "not_found" });
        return;
      }
      const authorizationHeaders = request.rawHeaders.filter(
        (_value, index) =>
          index % 2 === 0 &&
          request.rawHeaders[index].toLowerCase() === "authorization",
      );
      if (
        authorizationHeaders.length !== 1 ||
        request.headers.authorization !== `Bearer ${this.apiKey}`
      ) {
        respondJSON(response, 401, { error: "invalid_authorization" });
        return;
      }
      if (!request.headers["content-type"]?.startsWith("application/json")) {
        respondJSON(response, 415, { error: "application_json_required" });
        return;
      }
      this.requestCount++;
      const raw = await readBoundedJSON(request);
      const messages = Array.isArray(raw.messages) ? raw.messages : undefined;
      if (!messages || raw.stream !== true || raw.model !== "kimi-k2.7-code") {
        respondJSON(response, 422, { error: "invalid_chat_request" });
        return;
      }
      this.requests.push({ messages, raw });
      const turn = this.requests.length;
      if (turn === 1) {
        if (
          !hasExactConversation(messages, [
            { role: "user", text: firstUserMessage },
          ]) ||
          !hasProviderTool(raw.tools, "workspace_invitation_list") ||
          !hasProviderTool(raw.tools, "workspace_invitation_accept") ||
          !hasProviderTool(raw.tools, "workspace_list") ||
          !hasProviderTool(raw.tools, "list_dir")
        ) {
          respondJSON(response, 422, {
            error: "turn_one_user_or_workspace_tools_missing",
          });
          return;
        }
        respondToolCallSSE(
          response,
          turn,
          invitationListToolCallID,
          "workspace_invitation_list",
          {},
        );
        return;
      }
      if (turn === 2) {
        const listResult = exactToolResultJSON(
          messages,
          invitationListToolCallID,
          "workspace_invitation_list",
          {},
        );
        const invitation = exactSingleInvitation(listResult);
        if (
          !hasExactConversation(messages, [
            { role: "user", text: firstUserMessage },
          ]) ||
          !invitation
        ) {
          respondJSON(response, 422, {
            error: "exact_targeted_invitation_list_result_missing",
          });
          return;
        }
        this.invitationID = invitation.invitationID;
        this.workspaceID = invitation.workspaceID;
        this.workspaceName = invitation.workspaceName;
        this.invitationListVerified = true;
        respondToolCallSSE(
          response,
          turn,
          invitationAcceptToolCallID,
          "workspace_invitation_accept",
          { invitation_id: invitation.invitationID },
        );
        return;
      }
      if (turn === 3) {
        const acceptResult = exactToolResultJSON(
          messages,
          invitationAcceptToolCallID,
          "workspace_invitation_accept",
          { invitation_id: this.invitationID },
        );
        if (
          !this.workspaceID ||
          !exactAcceptedMembership(acceptResult, this.workspaceID)
        ) {
          respondJSON(response, 422, {
            error: "exact_targeted_invitation_accept_result_missing",
          });
          return;
        }
        this.invitationAcceptVerified = true;
        respondToolCallSSE(
          response,
          turn,
          workspaceListToolCallID,
          "workspace_list",
          {},
        );
        return;
      }
      if (turn === 4) {
        const workspaceListResult = exactToolResultJSON(
          messages,
          workspaceListToolCallID,
          "workspace_list",
          {},
        );
        if (
          !this.workspaceID ||
          !this.workspaceName ||
          !exactWorkspaceMembershipList(
            workspaceListResult,
            this.workspaceID,
            this.workspaceName,
          )
        ) {
          respondJSON(response, 422, {
            error: "exact_workspace_membership_list_result_missing",
          });
          return;
        }
        this.workspaceMembershipVerified = true;
        respondToolCallSSE(response, turn, executorToolCallID, "list_dir", {
          path: ".",
        });
        return;
      }
      if (turn === 5) {
        const executorResult = exactToolResultContent(
          messages,
          executorToolCallID,
          "list_dir",
          { path: "." },
        );
        if (executorResult !== executorAuthorityProbeFile) {
          respondJSON(response, 422, {
            error: "exact_executor_result_missing",
          });
          return;
        }
        this.executorToolVerified = true;
        respondSSE(response, turn, firstProviderResponse);
        return;
      }
      if (turn === 6) {
        if (
          !hasExactConversation(messages, [
            { role: "user", text: firstUserMessage },
            { role: "assistant", text: firstProviderResponse },
            { role: "user", text: secondUserMessage },
          ])
        ) {
          respondJSON(response, 422, { error: "shared_context_missing" });
          return;
        }
        this.contextVerified = true;
        // The focused provider/config contract deliberately exercises the
        // pre-Messaging conversation surface. The real stack proves the v3
        // attachment sequence below whenever the registered tool is present.
        if (!hasProviderTool(raw.tools, "messaging")) {
          respondSSE(response, turn, secondProviderResponse);
          return;
        }
        if (!this.workspaceID) {
          respondJSON(response, 422, {
            error: "workspace_missing_for_messaging",
          });
          return;
        }
        respondToolCallSSE(
          response,
          turn,
          messagingOverviewToolCallID,
          "messaging",
          {
            workspace_id: this.workspaceID,
            action: "overview",
          },
        );
        return;
      }
      if (turn === 7) {
        const overview = exactToolResultJSON(
          messages,
          messagingOverviewToolCallID,
          "messaging",
          { workspace_id: this.workspaceID, action: "overview" },
        );
        const channel = exactSingleMessagingChannel(overview, this.workspaceID);
        if (!channel || !this.workspaceID) {
          respondJSON(response, 422, {
            error: "exact_messaging_overview_result_missing",
          });
          return;
        }
        this.messagingChannelID = channel.channelID;
        respondToolCallSSE(
          response,
          turn,
          messagingOpenHumanToolCallID,
          "messaging",
          {
            workspace_id: this.workspaceID,
            action: "open",
            place_id: channel.channelID,
          },
        );
        return;
      }
      if (turn === 8) {
        const open = exactToolResultJSON(
          messages,
          messagingOpenHumanToolCallID,
          "messaging",
          {
            workspace_id: this.workspaceID,
            action: "open",
            place_id: this.messagingChannelID,
          },
        );
        const attachment = exactHumanMessagingAttachment(open);
        if (!attachment || !this.workspaceID || !this.messagingChannelID) {
          respondJSON(response, 422, {
            error: "exact_human_attachment_metadata_missing",
          });
          return;
        }
        this.humanAttachmentID = attachment.attachmentID;
        this.humanAttachmentDigest = attachment.sha256;
        respondToolCallSSE(
          response,
          turn,
          messagingOpenHumanAttachmentToolCallID,
          "messaging",
          {
            workspace_id: this.workspaceID,
            action: "open_attachment",
            attachment_id: attachment.attachmentID,
          },
        );
        return;
      }
      if (turn === 9) {
        const bytes = exactToolResultContent(
          messages,
          messagingOpenHumanAttachmentToolCallID,
          "messaging",
          {
            workspace_id: this.workspaceID,
            action: "open_attachment",
            attachment_id: this.humanAttachmentID,
          },
        );
        if (
          !this.workspaceID ||
          !this.messagingChannelID ||
          !this.humanAttachmentID ||
          bytes !== humanMessagingAttachmentContents
        ) {
          respondJSON(response, 422, {
            error: "exact_human_attachment_bytes_missing",
          });
          return;
        }
        respondToolCallSSE(
          response,
          turn,
          messagingWriteToolCallID,
          "messaging",
          {
            workspace_id: this.workspaceID,
            action: "write",
            content: "",
            attachments: [executorAuthorityProbeFile],
          },
        );
        return;
      }
      if (turn === 10) {
        const writeContent = exactToolResultContent(
          messages,
          messagingWriteToolCallID,
          "messaging",
          {
            workspace_id: this.workspaceID,
            action: "write",
            content: "",
            attachments: [executorAuthorityProbeFile],
          },
        );
        const receipt = exactMessagingWriteReceipt(
          parseJSONOrUndefined(writeContent),
        );
        if (!receipt || !this.workspaceID || !this.messagingChannelID) {
          respondJSON(response, 422, {
            error: "exact_messaging_write_receipt_missing",
          });
          return;
        }
        this.agentMessageID = receipt.messageID;
        respondToolCallSSE(
          response,
          turn,
          messagingOpenAgentToolCallID,
          "messaging",
          {
            workspace_id: this.workspaceID,
            action: "open",
            place_id: this.messagingChannelID,
          },
        );
        return;
      }
      if (turn === 11) {
        const open = exactToolResultJSON(
          messages,
          messagingOpenAgentToolCallID,
          "messaging",
          {
            workspace_id: this.workspaceID,
            action: "open",
            place_id: this.messagingChannelID,
          },
        );
        const attachment = this.agentMessageID
          ? exactAgentMessagingAttachment(open, this.agentMessageID)
          : undefined;
        if (!attachment || !this.workspaceID || !this.agentMessageID) {
          respondJSON(response, 422, {
            error: "exact_agent_attachment_metadata_missing",
          });
          return;
        }
        this.agentAttachmentID = attachment.attachmentID;
        this.agentAttachmentDigest = attachment.sha256;
        respondToolCallSSE(
          response,
          turn,
          messagingOpenAgentAttachmentToolCallID,
          "messaging",
          {
            workspace_id: this.workspaceID,
            action: "open_attachment",
            attachment_id: attachment.attachmentID,
          },
        );
        return;
      }
      if (turn === 12) {
        const bytes = exactToolResultContent(
          messages,
          messagingOpenAgentAttachmentToolCallID,
          "messaging",
          {
            workspace_id: this.workspaceID,
            action: "open_attachment",
            attachment_id: this.agentAttachmentID,
          },
        );
        if (
          !this.agentAttachmentID ||
          bytes !== executorAuthorityProbeContents
        ) {
          respondJSON(response, 422, {
            error: "exact_agent_attachment_bytes_missing",
          });
          return;
        }
        this.messagingVerified = true;
        respondSSE(response, turn, secondProviderResponse);
        return;
      }
      respondJSON(response, 429, { error: "unexpected_provider_request" });
    } catch {
      if (!response.headersSent) {
        respondJSON(response, 400, { error: "invalid_request" });
      } else {
        response.destroy();
      }
    }
  }
}

class LoopbackReviewerProvider {
  readonly requests: Record<string, unknown>[] = [];
  requestCount = 0;
  url = "";

  private readonly server = createServer((request, response) => {
    void this.handle(request, response);
  });

  constructor(
    private readonly apiKey: string,
    private readonly model: string,
    private readonly responseText: string,
    private readonly expectedResponseFormat: "json_schema" | "json_object",
  ) {}

  async start(): Promise<void> {
    this.server.listen(0, "127.0.0.1");
    await once(this.server, "listening");
    const address = this.server.address();
    if (!address || typeof address === "string") {
      throw new Error("loopback reviewer did not expose a TCP address");
    }
    this.url = `http://127.0.0.1:${address.port}`;
  }

  async stop(): Promise<void> {
    if (!this.server.listening) return;
    const closed = once(this.server, "close");
    this.server.close();
    this.server.closeAllConnections();
    await closed;
  }

  private async handle(
    request: IncomingMessage,
    response: ServerResponse,
  ): Promise<void> {
    try {
      if (request.method !== "POST" || request.url !== "/chat/completions") {
        respondJSON(response, 404, { error: "not_found" });
        return;
      }
      if (request.headers.authorization !== `Bearer ${this.apiKey}`) {
        respondJSON(response, 401, { error: "invalid_authorization" });
        return;
      }
      const raw = await readBoundedJSON(request);
      const responseFormat = raw.response_format;
      if (
        raw.stream !== true ||
        raw.model !== this.model ||
        !Array.isArray(raw.messages) ||
        !isRecord(responseFormat) ||
        responseFormat.type !== this.expectedResponseFormat
      ) {
        respondJSON(response, 422, { error: "invalid_reviewer_request" });
        return;
      }
      this.requests.push(raw);
      this.requestCount++;
      respondSSE(response, this.requestCount, this.responseText, this.model);
    } catch {
      if (!response.headersSent) {
        respondJSON(response, 400, { error: "invalid_request" });
      } else {
        response.destroy();
      }
    }
  }
}

export class RealAgentStack {
  readonly apiURL: string;
  readonly webURL: string;
  readonly provider: LoopbackChatProvider;

  private readonly runtimeDirectory: string;
  private readonly sessionCookie: string;
  private readonly children: ManagedProcess[];
  private readonly executionReviewerProvider: LoopbackReviewerProvider;
  private readonly escalationReviewerProvider: LoopbackReviewerProvider;
  private readonly reviewerProviders: LoopbackReviewerProvider[];
  private stopped = false;

  constructor({
    apiURL,
    webURL,
    provider,
    runtimeDirectory,
    sessionCookie,
    children,
    executionReviewerProvider,
    escalationReviewerProvider,
    reviewerProviders,
  }: {
    apiURL: string;
    webURL: string;
    provider: LoopbackChatProvider;
    runtimeDirectory: string;
    sessionCookie: string;
    children: ManagedProcess[];
    executionReviewerProvider: LoopbackReviewerProvider;
    escalationReviewerProvider: LoopbackReviewerProvider;
    reviewerProviders: LoopbackReviewerProvider[];
  }) {
    this.apiURL = apiURL;
    this.webURL = webURL;
    this.provider = provider;
    this.runtimeDirectory = runtimeDirectory;
    this.sessionCookie = sessionCookie;
    this.children = children;
    this.executionReviewerProvider = executionReviewerProvider;
    this.escalationReviewerProvider = escalationReviewerProvider;
    this.reviewerProviders = reviewerProviders;
  }

  get executionReviewCount(): number {
    return this.executionReviewerProvider.requestCount;
  }

  get escalationReviewCount(): number {
    return this.escalationReviewerProvider.requestCount;
  }

  get executionReviewRequests(): readonly Record<string, unknown>[] {
    return this.executionReviewerProvider.requests;
  }

  async installSession(context: BrowserContext): Promise<void> {
    await context.addCookies([
      {
        name: "sumi_session",
        value: this.sessionCookie,
        domain: "127.0.0.1",
        path: "/",
        httpOnly: true,
        secure: false,
        sameSite: "Lax",
      },
    ]);
  }

  diagnostics(): string {
    const sections = this.children
      .map((child) => child.diagnosticSection())
      .filter((section) => section.length > 0);
    sections.push(
      [
        "Loopback provider:",
        `request_count=${this.provider.requestCount}`,
        `executor_tool_verified=${this.provider.executorToolVerified}`,
        `execution_review_count=${this.executionReviewCount}`,
        `escalation_review_count=${this.escalationReviewCount}`,
        `context_verified=${this.provider.contextVerified}`,
      ].join("\n"),
    );
    return sections.join("\n\n");
  }

  async stop(): Promise<void> {
    if (this.stopped) return;
    this.stopped = true;
    const errors: Error[] = [];
    for (const child of [...this.children].reverse()) {
      try {
        await child.stop();
      } catch (error) {
        errors.push(toError(error));
      }
    }
    for (const provider of this.reviewerProviders) {
      try {
        await provider.stop();
      } catch (error) {
        errors.push(toError(error));
      }
    }
    try {
      await this.provider.stop();
    } catch (error) {
      errors.push(toError(error));
    }
    try {
      await rm(this.runtimeDirectory, { recursive: true, force: true });
    } catch (error) {
      errors.push(toError(error));
    }
    if (errors.length > 0) {
      throw new Error(errors.map((error) => error.message).join("; "));
    }
  }
}

/**
 * Production API + Postgres + Vite boundary for Human Workspace journeys.
 *
 * This deliberately does not start a PersonalityAgent. Workspace and
 * Messaging Human operations do not require one, and keeping this fixture
 * smaller makes failures in the app-owned control plane observable without
 * being hidden behind an unrelated runtime bootstrap failure.
 */
export class WorkspaceBrowserStack {
  readonly apiURL: string;
  readonly webURL: string;

  private readonly runtimeDirectory: string;
  private readonly sessionCookie: string;
  private readonly children: ManagedProcess[];
  private stopped = false;

  constructor({
    apiURL,
    webURL,
    runtimeDirectory,
    sessionCookie,
    children,
  }: {
    apiURL: string;
    webURL: string;
    runtimeDirectory: string;
    sessionCookie: string;
    children: ManagedProcess[];
  }) {
    this.apiURL = apiURL;
    this.webURL = webURL;
    this.runtimeDirectory = runtimeDirectory;
    this.sessionCookie = sessionCookie;
    this.children = children;
  }

  async installSession(context: BrowserContext): Promise<void> {
    await context.addCookies([
      {
        name: "sumi_session",
        value: this.sessionCookie,
        domain: "127.0.0.1",
        path: "/",
        httpOnly: true,
        secure: false,
        sameSite: "Lax",
      },
    ]);
  }

  diagnostics(): string {
    return this.children
      .map((child) => child.diagnosticSection())
      .filter((section) => section.length > 0)
      .join("\n\n");
  }

  async stop(): Promise<void> {
    if (this.stopped) return;
    this.stopped = true;
    const errors: Error[] = [];
    for (const child of [...this.children].reverse()) {
      try {
        await child.stop();
      } catch (error) {
        errors.push(toError(error));
      }
    }
    try {
      await rm(this.runtimeDirectory, { recursive: true, force: true });
    } catch (error) {
      errors.push(toError(error));
    }
    if (errors.length > 0) {
      throw new Error(errors.map((error) => error.message).join("; "));
    }
  }
}

export async function buildWorkspaceBrowserStack(): Promise<WorkspaceBrowserBuild> {
  const directory = await secureTempDirectory("sumi-workspace-browser-build-");
  const apiServer = join(directory, "sumi-api-server");
  const sessionIssuer = join(directory, "sumi-e2e-session-cookie");
  try {
    await Promise.all([
      runCommand(
        "build Go API server",
        "go",
        ["build", "-buildvcs=false", "-o", apiServer, "./cmd/server"],
        { cwd: apiDirectory, timeoutMilliseconds: 180_000 },
      ),
      runCommand(
        "build Go session issuer",
        "go",
        [
          "build",
          "-buildvcs=false",
          "-o",
          sessionIssuer,
          "./cmd/e2e-session-cookie",
        ],
        { cwd: apiDirectory, timeoutMilliseconds: 180_000 },
      ),
    ]);
    return { directory, apiServer, sessionIssuer };
  } catch (error) {
    await rm(directory, { recursive: true, force: true });
    throw error;
  }
}

export async function removeWorkspaceBrowserBuild(
  build: WorkspaceBrowserBuild,
): Promise<void> {
  await rm(build.directory, { recursive: true, force: true });
}

export async function startWorkspaceBrowserStack(
  build: WorkspaceBrowserBuild,
  databaseURL: string,
): Promise<WorkspaceBrowserStack> {
  if (!databaseURL.trim()) {
    throw new Error("a disposable empty Postgres database URL is required");
  }
  const runtimeDirectory = await secureTempDirectory(
    "sumi-workspace-browser-runtime-",
  );
  const children: ManagedProcess[] = [];
  const redactions = [databaseURL];
  try {
    const commandLog = join(runtimeDirectory, "command-log");
    const gatewayState = join(runtimeDirectory, "gateway-state");
    await Promise.all(
      [commandLog, gatewayState].map((path) =>
        mkdir(path, { recursive: true, mode: 0o700 }),
      ),
    );
    await Promise.all(
      [commandLog, gatewayState].map((path) => chmod(path, 0o700)),
    );

    const [publicPort, webPort] = await Promise.all([
      ephemeralPort(),
      ephemeralPort(),
    ]);
    const apiURL = `http://127.0.0.1:${publicPort}`;
    const webURL = `http://127.0.0.1:${webPort}`;
    const browserSessionSecret = randomBytes(48).toString("base64");
    const tenantID = `workspace-browser-e2e-${randomIdentifier()}`;
    const userID = workspaceBrowserHumanID;
    redactions.push(browserSessionSecret);

    const baseEnvironment = environmentWithoutSumiConfiguration();
    const api = ManagedProcess.start(
      "Go production Workspace API",
      build.apiServer,
      [],
      {
        cwd: apiDirectory,
        env: {
          ...baseEnvironment,
          PORT: String(publicPort),
          SUMI_PUBLIC_LOOPBACK_LISTEN: `127.0.0.1:${publicPort}`,
          SUMI_COMMAND_LOG_DIR: commandLog,
          SUMI_AGENT_RUNTIME_STATE_DIR: gatewayState,
          SUMI_BROWSER_SESSION_SECRET: browserSessionSecret,
          SUMI_BROWSER_SESSION_AUDIENCE: browserSessionAudience,
          SUMI_BROWSER_WS_ALLOWED_ORIGINS: webURL,
          SUMI_DB_URL: databaseURL,
        },
        redactions,
      },
    );
    children.push(api);
    await waitForHTTP(`${apiURL}/health`, api, 30_000);
    api.assertRunning();

    const sessionCookie = (
      await runCommand(
        "provision Human and issue production Workspace browser session",
        build.sessionIssuer,
        [],
        {
          cwd: apiDirectory,
          env: {
            ...baseEnvironment,
            SUMI_BROWSER_SESSION_SECRET: browserSessionSecret,
            SUMI_BROWSER_SESSION_AUDIENCE: browserSessionAudience,
            SUMI_E2E_SESSION_TENANT_ID: tenantID,
            SUMI_E2E_SESSION_USER_ID: userID,
            SUMI_E2E_SESSION_PERSONALITY_AGENT_ID: personalityAgentID,
            SUMI_E2E_SESSION_PROVISION_SECRETARY: "1",
            SUMI_E2E_SESSION_DATABASE_URL: databaseURL,
            SUMI_E2E_SESSION_DISPLAY_NAME: "Workspace E2E Human",
          },
          redactions,
          timeoutMilliseconds: 15_000,
        },
      )
    ).trim();
    if (
      sessionCookie.length === 0 ||
      sessionCookie.length > 4_096 ||
      /\s/.test(sessionCookie)
    ) {
      throw new Error("session issuer returned an invalid opaque cookie");
    }
    redactions.push(sessionCookie);

    const vite = ManagedProcess.start(
      "Vite Workspace browser server",
      process.execPath,
      [
        resolve(webDirectory, "node_modules/vite/bin/vite.js"),
        "--host",
        "127.0.0.1",
        "--port",
        String(webPort),
        "--strictPort",
      ],
      {
        cwd: webDirectory,
        env: {
          ...baseEnvironment,
          VITE_SUMI_AUTH_MODE: "preissued",
          VITE_SUMI_PREISSUED_USER_ID: userID,
          SUMI_DEV_API_ORIGIN: apiURL,
        },
        redactions,
      },
    );
    children.push(vite);
    await waitForHTTP(`${webURL}/`, vite, 20_000);
    vite.assertRunning();

    return new WorkspaceBrowserStack({
      apiURL,
      webURL,
      runtimeDirectory,
      sessionCookie,
      children,
    });
  } catch (error) {
    const cleanupErrors: Error[] = [];
    for (const child of [...children].reverse()) {
      try {
        await child.stop();
      } catch (cleanupError) {
        cleanupErrors.push(toError(cleanupError));
      }
    }
    try {
      await rm(runtimeDirectory, { recursive: true, force: true });
    } catch (cleanupError) {
      cleanupErrors.push(toError(cleanupError));
    }
    if (cleanupErrors.length > 0) {
      throw new StartupCleanupError(toError(error), cleanupErrors);
    }
    throw error;
  }
}

export async function buildRealAgentStack(): Promise<RealAgentBuild> {
  const directory = await secureTempDirectory("sumi-real-agent-build-");
  const apiServer = join(directory, "sumi-api-server");
  const sessionIssuer = join(directory, "sumi-e2e-session-cookie");
  const agent = resolve(agentDirectory, "target/debug/sumi-agent");
  try {
    await Promise.all([
      runCommand(
        "build Go API server",
        "go",
        ["build", "-buildvcs=false", "-o", apiServer, "./cmd/server"],
        { cwd: apiDirectory, timeoutMilliseconds: 180_000 },
      ),
      runCommand(
        "build Go session issuer",
        "go",
        [
          "build",
          "-buildvcs=false",
          "-o",
          sessionIssuer,
          "./cmd/e2e-session-cookie",
        ],
        { cwd: apiDirectory, timeoutMilliseconds: 180_000 },
      ),
      runCommand(
        "build Rust agent",
        "cargo",
        ["build", "--bin", "sumi-agent"],
        { cwd: agentDirectory, timeoutMilliseconds: 360_000 },
      ),
    ]);
    return { directory, apiServer, sessionIssuer, agent };
  } catch (error) {
    await rm(directory, { recursive: true, force: true });
    throw error;
  }
}

export async function removeRealAgentBuild(
  build: RealAgentBuild,
): Promise<void> {
  await rm(build.directory, { recursive: true, force: true });
}

export async function startRealAgentStack(
  build: RealAgentBuild,
  databaseURL: string,
): Promise<RealAgentStack> {
  if (!databaseURL.trim()) {
    throw new Error("a disposable empty Postgres database URL is required");
  }
  const maxAddressBindAttempts = 3;
  for (let attempt = 1; attempt <= maxAddressBindAttempts; attempt++) {
    try {
      return await startRealAgentStackOnce(build, databaseURL);
    } catch (error) {
      if (
        error instanceof StartupCleanupError ||
        attempt === maxAddressBindAttempts ||
        !isAddressInUse(error)
      ) {
        throw error;
      }
    }
  }
  throw new Error("unreachable address-bind retry exhaustion");
}

async function startRealAgentStackOnce(
  build: RealAgentBuild,
  databaseURL: string,
): Promise<RealAgentStack> {
  const runtimeDirectory = await secureTempDirectory(
    "sumi-real-agent-runtime-",
  );
  const children: ManagedProcess[] = [];
  const providerApiKey = randomToken();
  const provider = new LoopbackChatProvider(providerApiKey);
  const executionReviewerApiKey = randomToken();
  const executionReviewerProvider = new LoopbackReviewerProvider(
    executionReviewerApiKey,
    "e2e-execution-reviewer",
    JSON.stringify({
      outcome: "allow",
      risk: "low",
      rationale: "bounded read-only workspace directory listing",
    }),
    "json_schema",
  );
  const escalationReviewerApiKey = randomToken();
  const escalationReviewerProvider = new LoopbackReviewerProvider(
    escalationReviewerApiKey,
    "e2e-escalation-reviewer",
    JSON.stringify({
      outcome: "block",
      risk: "low",
      misunderstanding: null,
      rationale: "unexpected elevated review",
    }),
    "json_object",
  );
  const reviewerProviders = [
    executionReviewerProvider,
    escalationReviewerProvider,
  ];
  try {
    const paths = {
      commandLog: join(runtimeDirectory, "command-log"),
      gatewayState: join(runtimeDirectory, "gateway-state"),
      agentState: join(runtimeDirectory, "agent-state"),
      workspace: join(runtimeDirectory, "workspace"),
      messagingAttachmentRoot: join(runtimeDirectory, "messaging-attachments"),
      ipc: join(runtimeDirectory, "ipc"),
      localControl: join(runtimeDirectory, "local-control"),
    };
    await Promise.all(
      Object.values(paths).map((path) =>
        mkdir(path, { recursive: true, mode: 0o700 }),
      ),
    );
    await Promise.all(
      Object.entries(paths).map(([name, path]) =>
        chmod(path, name === "localControl" ? 0o750 : 0o700),
      ),
    );
    await writeFile(
      join(paths.workspace, executorAuthorityProbeFile),
      executorAuthorityProbeContents,
      { encoding: "utf8", mode: 0o600 },
    );

    const executorServerUID = process.getuid?.();
    const localControlSocketGID = process.getgid?.();
    if (
      executorServerUID === undefined ||
      executorServerUID === 0 ||
      localControlSocketGID === undefined
    ) {
      throw new Error(
        "the real-agent fixture requires a non-root Unix identity",
      );
    }

    const [publicPort, webPort] = await Promise.all([
      ephemeralPort(),
      ephemeralPort(),
    ]);
    const apiURL = `http://127.0.0.1:${publicPort}`;
    const webURL = `http://127.0.0.1:${webPort}`;
    const agentTokenSecret = randomBytes(48).toString("base64");
    const browserSessionSecret = randomBytes(48).toString("base64");
    const localControlBearer = randomToken();
    const tenantID = `real-agent-e2e-${randomIdentifier()}`;
    const userID = realAgentHumanID;
    const rpcNonce = `rpc-${randomIdentifier()}`;
    const leaseID = `lease-${randomIdentifier()}`;
    const fenceID = `fence-${randomIdentifier()}`;
    const wrappingKey = randomBytes(32).toString("hex");
    const wrappingKeyID = `e2e-${randomIdentifier()}`;
    const executorSocket = join(paths.ipc, "executor.sock");
    const localControlSocket = join(paths.localControl, "control.sock");
    const executorAuthorityKeyPair = generateExecutorAuthorityKeyPair();
    const redactions = [
      agentTokenSecret,
      browserSessionSecret,
      localControlBearer,
      providerApiKey,
      executionReviewerApiKey,
      escalationReviewerApiKey,
      wrappingKey,
      executorAuthorityKeyPair.privateKeyHex,
      databaseURL,
    ];
    const baseEnvironment = environmentWithoutSumiConfiguration();
    const commonIdentityEnvironment = {
      SUMI_PERSONALITY_AGENT_ID: personalityAgentID,
      SUMI_RPC_GENERATION: generation,
      SUMI_RPC_NONCE: rpcNonce,
    };
    const firebaseAuthEmulator = requiredFirebaseAuthEmulator();

    await Promise.all([
      provider.start(),
      executionReviewerProvider.start(),
      escalationReviewerProvider.start(),
    ]);
    await assertFirebaseAuthEmulator(firebaseAuthEmulator);

    const api = ManagedProcess.start("Go production API", build.apiServer, [], {
      cwd: apiDirectory,
      env: {
        ...baseEnvironment,
        PORT: String(publicPort),
        SUMI_PUBLIC_LOOPBACK_LISTEN: `127.0.0.1:${publicPort}`,
        SUMI_COMMAND_LOG_DIR: paths.commandLog,
        SUMI_AGENT_RUNTIME_STATE_DIR: paths.gatewayState,
        SUMI_MESSAGING_ATTACHMENT_ROOT: paths.messagingAttachmentRoot,
        SUMI_MESSAGING_ATTACHMENT_WORKSPACE_QUOTA_BYTES: "20971520",
        SUMI_MESSAGING_ATTACHMENT_WORKSPACE_QUOTA_OBJECTS: "10",
        SUMI_MESSAGING_ATTACHMENT_TOTAL_QUOTA_BYTES: "41943040",
        SUMI_MESSAGING_ATTACHMENT_TOTAL_QUOTA_OBJECTS: "100",
        SUMI_AGENT_TOKEN_SECRET: agentTokenSecret,
        SUMI_BROWSER_SESSION_SECRET: browserSessionSecret,
        SUMI_BROWSER_SESSION_AUDIENCE: browserSessionAudience,
        SUMI_BROWSER_WS_ALLOWED_ORIGINS: webURL,
        FIREBASE_AUTH_EMULATOR_HOST: firebaseAuthEmulator.host,
        SUMI_AUTH_FIREBASE_PROJECT_ID: firebaseProjectID,
        SUMI_AUTH_TENANT_ID: tenantID,
        SUMI_AUTH_ALLOW_INSECURE_COOKIES: "true",
        SUMI_AGENT_WRAPPING_KEY_ID: wrappingKeyID,
        SUMI_DB_URL: databaseURL,
        SUMI_LOCAL_CONTROL_ENABLED: "1",
        SUMI_LOCAL_CONTROL_BEARER: localControlBearer,
        SUMI_LOCAL_CONTROL_TENANT_ID: tenantID,
        SUMI_LOCAL_CONTROL_PERSONALITY_AGENT_ID: personalityAgentID,
        SUMI_LOCAL_CONTROL_GENERATION: generation,
        SUMI_LOCAL_CONTROL_RPC_BOOT_NONCE: rpcNonce,
        SUMI_LOCAL_CONTROL_AUDIENCE: "sumi:agent:events",
        SUMI_LOCAL_CONTROL_DELIVERY_AUTHORIZATION: "raw",
        SUMI_LOCAL_CONTROL_UNIX_SOCKET: localControlSocket,
        SUMI_LOCAL_CONTROL_SOCKET_GID: String(localControlSocketGID),
      },
      redactions,
    });
    children.push(api);
    await waitForHTTP(`${apiURL}/health`, api, 20_000);
    await waitForSocket(
      localControlSocket,
      api,
      20_000,
      "local-control Unix socket",
    );
    await delay(100);
    api.assertRunning();

    const sessionCookie = (
      await runCommand(
        "provision Human and Secretary and issue production browser session",
        build.sessionIssuer,
        [],
        {
          cwd: apiDirectory,
          env: {
            ...baseEnvironment,
            SUMI_BROWSER_SESSION_SECRET: browserSessionSecret,
            SUMI_BROWSER_SESSION_AUDIENCE: browserSessionAudience,
            SUMI_E2E_SESSION_TENANT_ID: tenantID,
            SUMI_E2E_SESSION_USER_ID: userID,
            SUMI_E2E_SESSION_PERSONALITY_AGENT_ID: personalityAgentID,
            SUMI_E2E_SESSION_DATABASE_URL: databaseURL,
            SUMI_E2E_SESSION_DISPLAY_NAME: "Direct Chat E2E Human",
            SUMI_E2E_SESSION_PROVISION_SECRETARY: "1",
          },
          redactions,
          timeoutMilliseconds: 15_000,
        },
      )
    ).trim();
    if (
      sessionCookie.length === 0 ||
      sessionCookie.length > 4_096 ||
      /\s/.test(sessionCookie)
    ) {
      throw new Error("session issuer returned an invalid opaque cookie");
    }
    redactions.push(sessionCookie);

    const executor = ManagedProcess.start(
      "Rust tool executor",
      build.agent,
      ["--tool-executor-socket"],
      {
        cwd: agentDirectory,
        env: {
          ...baseEnvironment,
          ...commonIdentityEnvironment,
          SUMI_EXECUTOR_CALL_AUTHORITY_PUBLIC_KEY:
            executorAuthorityKeyPair.publicKeyHex,
          SUMI_WORKSPACE: paths.workspace,
          SUMI_EXECUTOR_SOCKET: executorSocket,
          SUMI_LOG: "sumi_agent=info",
        },
        redactions,
      },
    );
    children.push(executor);
    await waitForSocket(executorSocket, executor, 20_000, "executor socket");

    const agent = ManagedProcess.start(
      "Rust production personality agent",
      build.agent,
      [],
      {
        cwd: agentDirectory,
        env: {
          ...baseEnvironment,
          ...commonIdentityEnvironment,
          SUMI_EXECUTOR_CALL_AUTHORITY_PRIVATE_KEY:
            executorAuthorityKeyPair.privateKeyHex,
          SUMI_PROCESS_GENERATION_LEASE_ID: leaseID,
          SUMI_GENERATION_RECOVERY_FENCE_ID: fenceID,
          SUMI_STATE_DIR: paths.agentState,
          SUMI_WORKSPACE: paths.workspace,
          SUMI_EXECUTOR_SOCKET: executorSocket,
          SUMI_EXECUTOR_SERVER_UID: String(executorServerUID),
          SUMI_GATEWAY_URL: `ws://127.0.0.1:${publicPort}/agent/ws`,
          SUMI_ALLOW_INSECURE_LOOPBACK_GATEWAY: "true",
          SUMI_LOCAL_CONTROL_UNIX_SOCKET: localControlSocket,
          SUMI_LOCAL_CONTROL_SERVER_UID: String(executorServerUID),
          SUMI_LOCAL_CONTROL_SOCKET_GID: String(localControlSocketGID),
          SUMI_LOCAL_CONTROL_BEARER: localControlBearer,
          SUMI_LOCAL_CONTROL_BEARER_EXPIRES_AT_UNIX: String(
            Math.floor(Date.now() / 1_000) + 30 * 60,
          ),
          SUMI_AGENT_WRAPPING_KEY_ID: wrappingKeyID,
          SUMI_AGENT_WRAPPING_KEY: wrappingKey,
          SUMI_MODEL_PRESET: "opencode-go",
          SUMI_MODEL_BASE_URL: provider.url,
          SUMI_MODEL_API_KEY_ENV: "SUMI_E2E_PROVIDER_API_KEY",
          SUMI_EXECUTION_REVIEWER_MODEL_PRESET: "kimi-k3",
          SUMI_EXECUTION_REVIEWER_MODEL_ID: "e2e-execution-reviewer",
          SUMI_EXECUTION_REVIEWER_MODEL_BASE_URL: executionReviewerProvider.url,
          SUMI_EXECUTION_REVIEWER_MODEL_ACCOUNT_SCOPE: "e2e-execution-reviewer",
          SUMI_EXECUTION_REVIEWER_MODEL_API_KEY_ENV:
            "SUMI_E2E_EXECUTION_REVIEWER_API_KEY",
          SUMI_ESCALATION_REVIEWER_MODEL_PRESET: "glm-5.2",
          SUMI_ESCALATION_REVIEWER_MODEL_ID: "e2e-escalation-reviewer",
          SUMI_ESCALATION_REVIEWER_MODEL_BASE_URL:
            escalationReviewerProvider.url,
          SUMI_ESCALATION_REVIEWER_MODEL_ACCOUNT_SCOPE:
            "e2e-escalation-reviewer",
          SUMI_ESCALATION_REVIEWER_MODEL_API_KEY_ENV:
            "SUMI_E2E_ESCALATION_REVIEWER_API_KEY",
          SUMI_E2E_PROVIDER_API_KEY: providerApiKey,
          SUMI_E2E_EXECUTION_REVIEWER_API_KEY: executionReviewerApiKey,
          SUMI_E2E_ESCALATION_REVIEWER_API_KEY: escalationReviewerApiKey,
          SUMI_SYSTEM_PROMPT:
            "Answer each user message with the deterministic provider response.",
          SUMI_LOG: "sumi_agent=info",
        },
        redactions,
      },
    );
    children.push(agent);

    const vite = ManagedProcess.start(
      "Vite same-origin production-session server",
      process.execPath,
      [
        resolve(webDirectory, "node_modules/vite/bin/vite.js"),
        "--host",
        "127.0.0.1",
        "--port",
        String(webPort),
        "--strictPort",
      ],
      {
        cwd: webDirectory,
        env: {
          ...baseEnvironment,
          SUMI_DEV_API_ORIGIN: apiURL,
          VITE_FIREBASE_API_KEY: firebaseWebAPIKey,
          VITE_FIREBASE_AUTH_DOMAIN: `${firebaseProjectID}.firebaseapp.com`,
          VITE_FIREBASE_PROJECT_ID: firebaseProjectID,
          VITE_FIREBASE_APP_ID: firebaseWebAppID,
          VITE_FIREBASE_AUTH_EMULATOR_URL: firebaseAuthEmulator.url,
        },
        redactions,
      },
    );
    children.push(vite);
    await waitForHTTP(`${webURL}/`, vite, 20_000);
    await delay(100);
    vite.assertRunning();

    return new RealAgentStack({
      apiURL,
      webURL,
      provider,
      runtimeDirectory,
      sessionCookie,
      children,
      executionReviewerProvider,
      escalationReviewerProvider,
      reviewerProviders,
    });
  } catch (error) {
    const cleanupErrors: Error[] = [];
    for (const child of [...children].reverse()) {
      try {
        await child.stop();
      } catch (cleanupError) {
        cleanupErrors.push(toError(cleanupError));
      }
    }
    try {
      await provider.stop();
    } catch (cleanupError) {
      cleanupErrors.push(toError(cleanupError));
    }
    for (const reviewerProvider of reviewerProviders) {
      try {
        await reviewerProvider.stop();
      } catch (cleanupError) {
        cleanupErrors.push(toError(cleanupError));
      }
    }
    try {
      await rm(runtimeDirectory, { recursive: true, force: true });
    } catch (cleanupError) {
      cleanupErrors.push(toError(cleanupError));
    }
    if (cleanupErrors.length > 0) {
      throw new StartupCleanupError(toError(error), cleanupErrors);
    }
    throw error;
  }
}

class StartupCleanupError extends AggregateError {
  constructor(startupError: Error, cleanupErrors: Error[]) {
    super(
      [startupError, ...cleanupErrors],
      `${startupError.message}; startup cleanup failed: ${cleanupErrors
        .map((error) => error.message)
        .join("; ")}`,
      { cause: startupError },
    );
    this.name = "StartupCleanupError";
  }
}

class ManagedProcess {
  readonly child: ChildProcess;
  private readonly label: string;
  private readonly log: BoundedLog;
  private spawnError: Error | undefined;

  private constructor(
    label: string,
    child: ChildProcess,
    redactions: string[],
  ) {
    this.label = label;
    this.child = child;
    this.log = new BoundedLog(redactions);
    child.stdout?.on("data", (chunk: Buffer) => this.log.append(chunk));
    child.stderr?.on("data", (chunk: Buffer) => this.log.append(chunk));
    child.on("error", (error) => {
      this.spawnError = error;
      this.log.append(Buffer.from(error.message));
    });
  }

  static start(
    label: string,
    command: string,
    arguments_: string[],
    {
      cwd,
      env,
      redactions = [],
    }: {
      cwd: string;
      env: NodeJS.ProcessEnv;
      redactions?: string[];
    },
  ): ManagedProcess {
    const child = spawn(command, arguments_, {
      cwd,
      env,
      stdio: ["ignore", "pipe", "pipe"],
    });
    return new ManagedProcess(label, child, redactions);
  }

  assertRunning(): void {
    if (this.spawnError) {
      throw new Error(`${this.label} failed to start${this.diagnostics()}`);
    }
    if (this.child.exitCode === null && this.child.signalCode === null) return;
    throw new Error(
      `${this.label} exited early (${this.child.exitCode ?? this.child.signalCode})${this.diagnostics()}`,
    );
  }

  diagnostics(): string {
    const value = this.log.value().trim();
    return value ? `:\n${value}` : "";
  }

  diagnosticSection(): string {
    const status =
      this.child.exitCode === null && this.child.signalCode === null
        ? "running"
        : `exited (${this.child.exitCode ?? this.child.signalCode})`;
    const value = this.log.value().trim();
    return value
      ? `${this.label} [${status}]:\n${value}`
      : `${this.label} [${status}]: no captured output`;
  }

  capturedOutput(): string {
    return this.log.value();
  }

  async stop(): Promise<void> {
    if (this.spawnError) return;
    if (this.child.exitCode !== null || this.child.signalCode !== null) return;
    this.child.kill("SIGTERM");
    const graceful = await waitForChildExit(
      this.child,
      processStopTimeoutMilliseconds,
    );
    if (
      graceful ||
      this.child.exitCode !== null ||
      this.child.signalCode !== null
    )
      return;
    this.child.kill("SIGKILL");
    if (!(await waitForChildExit(this.child, processStopTimeoutMilliseconds))) {
      throw new Error(`${this.label} did not exit after SIGKILL`);
    }
  }
}

class BoundedLog {
  private value_ = "";
  private readonly redactions: string[];

  constructor(redactions: string[]) {
    this.redactions = redactions;
  }

  append(chunk: Buffer): void {
    this.value_ += chunk.toString("utf8");
    if (this.value_.length > maxChildLogCharacters) {
      this.value_ = this.value_.slice(-maxChildLogCharacters);
    }
  }

  value(): string {
    let output = this.value_;
    for (const secret of this.redactions) {
      if (secret) output = output.replaceAll(secret, "[REDACTED]");
    }
    return output;
  }
}

async function runCommand(
  label: string,
  command: string,
  arguments_: string[],
  {
    cwd,
    env = process.env,
    redactions = [],
    timeoutMilliseconds,
  }: {
    cwd: string;
    env?: NodeJS.ProcessEnv;
    redactions?: string[];
    timeoutMilliseconds: number;
  },
): Promise<string> {
  const process_ = ManagedProcess.start(label, command, arguments_, {
    cwd,
    env,
    redactions,
  });
  const result = await waitForCommand(process_.child, timeoutMilliseconds);
  if (result === "timeout") {
    await process_.stop();
    throw new Error(`${label} timed out${process_.diagnostics()}`);
  }
  if ("error" in result) {
    throw new Error(`${label} failed to start${process_.diagnostics()}`);
  }
  if (result.code !== 0) {
    throw new Error(
      `${label} failed (${result.code ?? result.signal})${process_.diagnostics()}`,
    );
  }
  return process_.capturedOutput();
}

function waitForCommand(
  child: ChildProcess,
  timeoutMilliseconds: number,
): Promise<
  | "timeout"
  | { code: number | null; signal: NodeJS.Signals | null }
  | { error: Error }
> {
  return new Promise((resolveResult) => {
    const settle = (
      result:
        | "timeout"
        | { code: number | null; signal: NodeJS.Signals | null }
        | { error: Error },
    ) => {
      clearTimeout(timer);
      child.off("exit", onExit);
      child.off("error", onError);
      resolveResult(result);
    };
    const onExit = (code: number | null, signal: NodeJS.Signals | null) =>
      settle({ code, signal });
    const onError = (error: Error) => settle({ error });
    const timer = setTimeout(() => settle("timeout"), timeoutMilliseconds);
    child.once("exit", onExit);
    child.once("error", onError);
  });
}

function waitForChildExit(
  child: ChildProcess,
  timeoutMilliseconds: number,
): Promise<boolean> {
  if (child.exitCode !== null || child.signalCode !== null) {
    return Promise.resolve(true);
  }
  return new Promise((resolveExit) => {
    const onExit = () => {
      clearTimeout(timer);
      resolveExit(true);
    };
    const timer = setTimeout(() => {
      child.off("exit", onExit);
      resolveExit(false);
    }, timeoutMilliseconds);
    child.once("exit", onExit);
  });
}

async function secureTempDirectory(prefix: string): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), prefix));
  await chmod(directory, 0o700);
  return directory;
}

async function ephemeralPort(): Promise<number> {
  const server = createNetServer();
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  if (!address || typeof address === "string") {
    server.close();
    throw new Error("ephemeral port reservation did not expose a TCP address");
  }
  const port = address.port;
  const closed = once(server, "close");
  server.close();
  await closed;
  return port;
}

async function waitForHTTP(
  url: string,
  process_: ManagedProcess,
  timeoutMilliseconds: number,
): Promise<void> {
  const deadline = Date.now() + timeoutMilliseconds;
  while (Date.now() < deadline) {
    process_.assertRunning();
    try {
      const response = await fetch(url, { signal: AbortSignal.timeout(1_000) });
      if (response.ok) return;
    } catch {
      // The bounded retry owns startup races.
    }
    await delay(100);
  }
  process_.assertRunning();
  throw new Error(`timed out waiting for ${url}${process_.diagnostics()}`);
}

async function waitForSocket(
  socket: string,
  process_: ManagedProcess,
  timeoutMilliseconds: number,
  label: string,
): Promise<void> {
  const deadline = Date.now() + timeoutMilliseconds;
  while (Date.now() < deadline) {
    process_.assertRunning();
    try {
      if ((await lstat(socket)).isSocket()) return;
    } catch {
      // The bounded retry owns startup races.
    }
    await delay(100);
  }
  process_.assertRunning();
  throw new Error(`timed out waiting for ${label}${process_.diagnostics()}`);
}

async function readBoundedJSON(
  request: IncomingMessage,
): Promise<Record<string, unknown>> {
  const chunks: Buffer[] = [];
  let bytes = 0;
  for await (const chunk of request) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    bytes += buffer.length;
    if (bytes > 2 * 1024 * 1024) {
      throw new Error("provider request exceeds bound");
    }
    chunks.push(buffer);
  }
  const parsed: unknown = JSON.parse(Buffer.concat(chunks).toString("utf8"));
  if (!isRecord(parsed)) throw new Error("provider request must be an object");
  return parsed;
}

function hasExactConversation(
  messages: unknown[],
  expected: ReadonlyArray<{ role: "user" | "assistant"; text: string }>,
): boolean {
  const conversation = messages.flatMap((message) => {
    if (
      !isRecord(message) ||
      (message.role !== "user" && message.role !== "assistant")
    ) {
      return [];
    }
    const text = messageText(message.content);
    if (
      message.role === "assistant" &&
      Array.isArray(message.tool_calls) &&
      !text
    ) {
      return [];
    }
    return [{ role: message.role, text }];
  });
  if (conversation.length !== expected.length) return false;
  return expected.every(
    (entry, index) =>
      conversation[index]?.role === entry.role &&
      conversation[index]?.text === entry.text,
  );
}

function hasProviderTool(tools: unknown, name: string): boolean {
  if (!Array.isArray(tools)) return false;
  return tools.some((tool) => {
    if (
      !isRecord(tool) ||
      tool.type !== "function" ||
      !isRecord(tool.function)
    ) {
      return false;
    }
    const parameters = tool.function.parameters;
    if (tool.function.name !== name || !isRecord(parameters)) return false;
    const properties = parameters.properties;
    if (!isRecord(properties)) return false;
    const route = properties.route;
    const input = properties.input;
    return (
      isRecord(route) &&
      Array.isArray(route.enum) &&
      route.enum.includes("normal") &&
      isRecord(input)
    );
  });
}

function exactToolResultContent(
  messages: unknown[],
  callID: string,
  toolName: string,
  expectedInput: Record<string, unknown>,
): string | undefined {
  const applicationMessages = messages.filter(
    (message): message is Record<string, unknown> =>
      isRecord(message) &&
      (message.role === "user" ||
        message.role === "assistant" ||
        message.role === "tool"),
  );
  const matchingIndices = applicationMessages.flatMap((message, index) => {
    if (message.role !== "assistant" || !Array.isArray(message.tool_calls)) {
      return [];
    }
    return message.tool_calls.some(
      (toolCall) => isRecord(toolCall) && toolCall.id === callID,
    )
      ? [index]
      : [];
  });
  if (matchingIndices.length !== 1) return undefined;
  const assistantIndex = matchingIndices[0];
  const assistant = applicationMessages[assistantIndex];
  const toolResult = applicationMessages[assistantIndex + 1];
  if (
    assistant?.role !== "assistant" ||
    toolResult?.role !== "tool" ||
    toolResult.tool_call_id !== callID ||
    typeof toolResult.content !== "string"
  ) {
    return undefined;
  }
  if (
    !Array.isArray(assistant.tool_calls) ||
    assistant.tool_calls.length !== 1
  ) {
    return undefined;
  }
  const toolCall = assistant.tool_calls[0];
  if (
    !isRecord(toolCall) ||
    toolCall.id !== callID ||
    toolCall.type !== "function" ||
    !isRecord(toolCall.function) ||
    toolCall.function.name !== toolName ||
    typeof toolCall.function.arguments !== "string"
  ) {
    return undefined;
  }
  try {
    const args: unknown = JSON.parse(toolCall.function.arguments);
    if (
      !isRecord(args) ||
      args.route !== "normal" ||
      !isRecord(args.input) ||
      Object.keys(args).sort().join("\0") !== "input\0route" ||
      !exactShallowObject(args.input, expectedInput)
    ) {
      return undefined;
    }
    return toolResult.content;
  } catch {
    return undefined;
  }
}

function exactToolResultJSON(
  messages: unknown[],
  callID: string,
  toolName: string,
  expectedInput: Record<string, unknown>,
): unknown {
  const content = exactToolResultContent(
    messages,
    callID,
    toolName,
    expectedInput,
  );
  if (content === undefined) return undefined;
  try {
    return JSON.parse(content) as unknown;
  } catch {
    return undefined;
  }
}

function parseJSONOrUndefined(content: string | undefined): unknown {
  if (content === undefined) return undefined;
  try {
    return JSON.parse(content) as unknown;
  } catch {
    return undefined;
  }
}

function exactShallowObject(
  actual: Record<string, unknown>,
  expected: Record<string, unknown>,
): boolean {
  const keys = Object.keys(actual).sort();
  const expectedKeys = Object.keys(expected).sort();
  return (
    keys.length === expectedKeys.length &&
    keys.every(
      (key, index) =>
        key === expectedKeys[index] &&
        exactJSONValue(actual[key], expected[key]),
    )
  );
}

function exactJSONValue(actual: unknown, expected: unknown): boolean {
  if (Array.isArray(actual) || Array.isArray(expected)) {
    return (
      Array.isArray(actual) &&
      Array.isArray(expected) &&
      actual.length === expected.length &&
      actual.every((entry, index) => exactJSONValue(entry, expected[index]))
    );
  }
  return actual === expected;
}

function exactSingleInvitation(value: unknown):
  | {
      invitationID: string;
      workspaceID: string;
      workspaceName: string;
    }
  | undefined {
  if (
    !isRecord(value) ||
    Object.keys(value).join("\0") !== "invitations" ||
    !Array.isArray(value.invitations) ||
    value.invitations.length !== 1
  ) {
    return undefined;
  }
  const invitation = value.invitations[0];
  if (
    !isRecord(invitation) ||
    Object.keys(invitation).sort().join("\0") !==
      "created_at\0expires_at\0invitation_id\0workspace_id\0workspace_name" ||
    !isCanonicalUUIDv7(invitation.invitation_id) ||
    !isCanonicalUUIDv7(invitation.workspace_id) ||
    typeof invitation.workspace_name !== "string" ||
    invitation.workspace_name.length === 0 ||
    !isRFC3339Timestamp(invitation.created_at) ||
    !isRFC3339Timestamp(invitation.expires_at)
  ) {
    return undefined;
  }
  return {
    invitationID: invitation.invitation_id,
    workspaceID: invitation.workspace_id,
    workspaceName: invitation.workspace_name,
  };
}

function exactAcceptedMembership(value: unknown, workspaceID: string): boolean {
  if (!isRecord(value)) return false;
  return (
    Object.keys(value).sort().join("\0") ===
      "display_name\0joined_at\0left_at\0owner\0role_ids\0workspace_id\0workspace_member_id" &&
    isCanonicalUUIDv7(value.workspace_member_id) &&
    value.workspace_id === workspaceID &&
    typeof value.display_name === "string" &&
    value.display_name.length > 0 &&
    value.owner === false &&
    Array.isArray(value.role_ids) &&
    value.role_ids.length === 0 &&
    isRFC3339Timestamp(value.joined_at) &&
    value.left_at === null
  );
}

function exactWorkspaceMembershipList(
  value: unknown,
  workspaceID: string,
  workspaceName: string,
): boolean {
  if (
    !isRecord(value) ||
    Object.keys(value).join("\0") !== "workspaces" ||
    !Array.isArray(value.workspaces) ||
    value.workspaces.length !== 1
  ) {
    return false;
  }
  const workspace = value.workspaces[0];
  return (
    isRecord(workspace) &&
    Object.keys(workspace).sort().join("\0") === "name\0workspace_id" &&
    workspace.workspace_id === workspaceID &&
    workspace.name === workspaceName
  );
}

function exactSingleMessagingChannel(
  value: unknown,
  workspaceID: string | undefined,
): { channelID: string } | undefined {
  if (!workspaceID || !isRecord(value)) return undefined;
  if (
    Object.keys(value).sort().join("\0") !==
      "channels\0dms\0members\0read_markers\0reply_later_markers\0self\0unread_summaries\0workspaces" ||
    !Array.isArray(value.workspaces) ||
    value.workspaces.length !== 1 ||
    !Array.isArray(value.channels) ||
    value.channels.length !== 1 ||
    !Array.isArray(value.dms) ||
    value.dms.length !== 0
  ) {
    return undefined;
  }
  const workspace = value.workspaces[0];
  const channel = value.channels[0];
  if (
    !isRecord(workspace) ||
    Object.keys(workspace).sort().join("\0") !== "name\0workspace_id" ||
    workspace.workspace_id !== workspaceID ||
    typeof workspace.name !== "string" ||
    workspace.name.length === 0 ||
    !isRecord(channel) ||
    Object.keys(channel).sort().join("\0") !==
      "channel_id\0name\0topic\0visibility\0workspace_id" ||
    !isCanonicalUUIDv7(channel.channel_id) ||
    channel.workspace_id !== workspaceID ||
    typeof channel.name !== "string" ||
    channel.name.length === 0 ||
    typeof channel.topic !== "string" ||
    typeof channel.visibility !== "string"
  ) {
    return undefined;
  }
  return { channelID: channel.channel_id };
}

function exactHumanMessagingAttachment(
  value: unknown,
): { attachmentID: string; sha256: string } | undefined {
  const matches = exactOpenMessages(value).flatMap((message) =>
    message.author.kind === "human" &&
    message.author.id === realAgentHumanID &&
    message.attachments.length === 1
      ? [{ message, attachment: message.attachments[0] }]
      : [],
  );
  if (matches.length !== 1) return undefined;
  const { message, attachment } = matches[0];
  if (
    message.content !== "" ||
    attachment.filename !== humanMessagingAttachmentFile ||
    attachment.mime !== "text/plain" ||
    attachment.sizeBytes !==
      Buffer.byteLength(humanMessagingAttachmentContents) ||
    attachment.position !== 0
  ) {
    return undefined;
  }
  return { attachmentID: attachment.attachmentID, sha256: attachment.sha256 };
}

function exactAgentMessagingAttachment(
  value: unknown,
  messageID: string,
): { attachmentID: string; sha256: string } | undefined {
  const matches = exactOpenMessages(value).flatMap((message) =>
    message.messageID === messageID &&
    message.author.kind === "personality_agent" &&
    message.attachments.length === 1
      ? [{ message, attachment: message.attachments[0] }]
      : [],
  );
  if (matches.length !== 1) return undefined;
  const { message, attachment } = matches[0];
  if (
    message.content !== "" ||
    attachment.filename !== executorAuthorityProbeFile ||
    attachment.mime !== "text/plain" ||
    attachment.sizeBytes !==
      Buffer.byteLength(executorAuthorityProbeContents) ||
    attachment.position !== 0
  ) {
    return undefined;
  }
  return { attachmentID: attachment.attachmentID, sha256: attachment.sha256 };
}

function exactMessagingWriteReceipt(
  value: unknown,
): { messageID: string } | undefined {
  if (
    !isRecord(value) ||
    Object.keys(value).sort().join("\0") !==
      "client_nonce\0created\0message_id\0seq" ||
    typeof value.client_nonce !== "string" ||
    value.client_nonce.length === 0 ||
    value.created !== true ||
    !isCanonicalUUIDv7(value.message_id) ||
    !Number.isSafeInteger(value.seq) ||
    value.seq <= 0
  ) {
    return undefined;
  }
  return { messageID: value.message_id };
}

function exactOpenMessages(value: unknown): Array<{
  messageID: string;
  content: string;
  author: { kind: string; id: string };
  attachments: Array<{
    attachmentID: string;
    filename: string;
    mime: string;
    sizeBytes: number;
    position: number;
    sha256: string;
  }>;
}> {
  if (
    !isRecord(value) ||
    Object.keys(value).sort().join("\0") !==
      "last_read_seq\0latest_seq\0members\0messages\0place" ||
    !Array.isArray(value.messages)
  ) {
    return [];
  }
  const messages = value.messages.flatMap((entry) => {
    if (
      !isRecord(entry) ||
      Object.keys(entry).sort().join("\0") !==
        "attachments\0author\0client_nonce\0content\0created_at\0deleted\0edited_at\0mentions\0message_id\0place\0reactions\0reply_to\0seq\0urgency" ||
      !isCanonicalUUIDv7(entry.message_id) ||
      typeof entry.content !== "string" ||
      !isRecord(entry.author) ||
      !Array.isArray(entry.attachments)
    ) {
      return [];
    }
    const author = entry.author;
    const authorID =
      author.kind === "human" ? author.human_id : author.personality_agent_id;
    if (
      (author.kind !== "human" && author.kind !== "personality_agent") ||
      typeof authorID !== "string" ||
      authorID.length === 0
    ) {
      return [];
    }
    const attachments = entry.attachments.flatMap((attachment) => {
      if (
        !isRecord(attachment) ||
        Object.keys(attachment).sort().join("\0") !==
          "attachment_id\0filename\0mime\0position\0sha256\0size_bytes" ||
        !isCanonicalUUIDv7(attachment.attachment_id) ||
        typeof attachment.filename !== "string" ||
        typeof attachment.mime !== "string" ||
        !Number.isSafeInteger(attachment.size_bytes) ||
        attachment.size_bytes < 1 ||
        !Number.isSafeInteger(attachment.position) ||
        attachment.position < 0 ||
        typeof attachment.sha256 !== "string" ||
        !/^[a-f0-9]{64}$/.test(attachment.sha256)
      ) {
        return [];
      }
      return [
        {
          attachmentID: attachment.attachment_id,
          filename: attachment.filename,
          mime: attachment.mime,
          sizeBytes: attachment.size_bytes,
          position: attachment.position,
          sha256: attachment.sha256,
        },
      ];
    });
    if (attachments.length !== entry.attachments.length) return [];
    return [
      {
        messageID: entry.message_id,
        content: entry.content,
        author: { kind: author.kind, id: authorID },
        attachments,
      },
    ];
  });
  return messages.length === value.messages.length ? messages : [];
}

function isCanonicalUUIDv7(value: unknown): value is string {
  return (
    typeof value === "string" &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(
      value,
    )
  );
}

function isRFC3339Timestamp(value: unknown): value is string {
  return (
    typeof value === "string" &&
    /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$/.test(value) &&
    Number.isFinite(Date.parse(value))
  );
}

function messageText(content: unknown): string {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";
  return content
    .map((part) => {
      if (!isRecord(part)) return "";
      return typeof part.text === "string"
        ? part.text
        : typeof part.content === "string"
          ? part.content
          : "";
    })
    .join("");
}

function respondSSE(
  response: ServerResponse,
  turn: number,
  text: string,
  model = "kimi-k2.7-code",
): void {
  response.writeHead(200, {
    "cache-control": "no-store",
    "content-type": "text/event-stream",
  });
  const id = `real-agent-e2e-${turn}`;
  response.write(
    `data: ${JSON.stringify({
      id,
      model,
      choices: [
        {
          index: 0,
          delta: { role: "assistant", content: text },
          finish_reason: null,
        },
      ],
    })}\n\n`,
  );
  response.write(
    `data: ${JSON.stringify({
      id,
      model,
      choices: [{ index: 0, delta: {}, finish_reason: "stop" }],
      usage: {
        prompt_tokens: turn * 10,
        completion_tokens: 4,
        total_tokens: turn * 10 + 4,
      },
    })}\n\n`,
  );
  response.end("data: [DONE]\n\n");
}

function respondToolCallSSE(
  response: ServerResponse,
  turn: number,
  callID: string,
  toolName: string,
  input: Record<string, unknown>,
): void {
  response.writeHead(200, {
    "cache-control": "no-store",
    "content-type": "text/event-stream",
  });
  const id = `real-agent-e2e-${turn}`;
  response.write(
    `data: ${JSON.stringify({
      id,
      model: "kimi-k2.7-code",
      choices: [
        {
          index: 0,
          delta: {
            role: "assistant",
            tool_calls: [
              {
                index: 0,
                id: callID,
                type: "function",
                function: {
                  name: toolName,
                  arguments: JSON.stringify({
                    route: "normal",
                    input,
                  }),
                },
              },
            ],
          },
          finish_reason: null,
        },
      ],
    })}\n\n`,
  );
  response.write(
    `data: ${JSON.stringify({
      id,
      model: "kimi-k2.7-code",
      choices: [{ index: 0, delta: {}, finish_reason: "tool_calls" }],
      usage: {
        prompt_tokens: 10,
        completion_tokens: 4,
        total_tokens: 14,
      },
    })}\n\n`,
  );
  response.end("data: [DONE]\n\n");
}

function respondJSON(
  response: ServerResponse,
  status: number,
  body: Record<string, unknown>,
): void {
  response.writeHead(status, {
    "cache-control": "no-store",
    "content-type": "application/json",
  });
  response.end(JSON.stringify(body));
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function generateExecutorAuthorityKeyPair(): {
  privateKeyHex: string;
  publicKeyHex: string;
} {
  const { privateKey, publicKey } = generateKeyPairSync("ed25519");
  const privateJwk = privateKey.export({ format: "jwk" });
  const publicJwk = publicKey.export({ format: "jwk" });
  if (
    privateJwk.kty !== "OKP" ||
    privateJwk.crv !== "Ed25519" ||
    typeof privateJwk.d !== "string" ||
    typeof privateJwk.x !== "string" ||
    publicJwk.kty !== "OKP" ||
    publicJwk.crv !== "Ed25519" ||
    typeof publicJwk.x !== "string"
  ) {
    throw new Error("Node returned an invalid Ed25519 JWK pair");
  }
  const privateSeed = Buffer.from(privateJwk.d, "base64url");
  const embeddedPublic = Buffer.from(privateJwk.x, "base64url");
  const publicBytes = Buffer.from(publicJwk.x, "base64url");
  try {
    if (
      privateSeed.length !== 32 ||
      embeddedPublic.length !== 32 ||
      publicBytes.length !== 32 ||
      !embeddedPublic.equals(publicBytes)
    ) {
      throw new Error("Node returned a non-corresponding Ed25519 JWK pair");
    }
    return {
      privateKeyHex: privateSeed.toString("hex"),
      publicKeyHex: publicBytes.toString("hex"),
    };
  } finally {
    privateSeed.fill(0);
    embeddedPublic.fill(0);
    publicBytes.fill(0);
  }
}

function randomToken(): string {
  return randomBytes(48).toString("base64url");
}

function randomIdentifier(): string {
  return randomBytes(18).toString("hex");
}

function requiredFirebaseAuthEmulator(): { host: string; url: string } {
  const rawHost = process.env.FIREBASE_AUTH_EMULATOR_HOST?.trim();
  if (!rawHost) {
    throw new Error(
      "FIREBASE_AUTH_EMULATOR_HOST must name the local Firebase Auth emulator",
    );
  }
  let url: URL;
  try {
    url = new URL(`http://${rawHost}`);
  } catch {
    throw new Error(
      "FIREBASE_AUTH_EMULATOR_HOST must be host:port without a scheme",
    );
  }
  if (
    url.protocol !== "http:" ||
    url.username !== "" ||
    url.password !== "" ||
    url.pathname !== "/" ||
    url.search !== "" ||
    url.hash !== "" ||
    !url.hostname ||
    !url.port ||
    rawHost.includes("/")
  ) {
    throw new Error(
      "FIREBASE_AUTH_EMULATOR_HOST must be host:port without a scheme",
    );
  }
  return { host: url.host, url: url.origin };
}

async function assertFirebaseAuthEmulator({
  url,
}: {
  url: string;
}): Promise<void> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 5_000);
  try {
    const response = await fetch(
      `${url}/emulator/v1/projects/${firebaseProjectID}/config`,
      { signal: controller.signal },
    );
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
  } catch (error) {
    throw new Error(
      `Firebase Auth emulator is unavailable at ${url}: ${toError(error).message}`,
    );
  } finally {
    clearTimeout(timeout);
  }
}

function environmentWithoutSumiConfiguration(): NodeJS.ProcessEnv {
  return Object.fromEntries(
    Object.entries(process.env).filter(
      ([name]) =>
        !name.startsWith("SUMI_") &&
        !name.startsWith("VITE_") &&
        name !== "FIREBASE_AUTH_EMULATOR_HOST" &&
        name !== "GOOGLE_APPLICATION_CREDENTIALS" &&
        name !== "GOOGLE_CLOUD_PROJECT",
    ),
  );
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolveDelay) => setTimeout(resolveDelay, milliseconds));
}

function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

function isAddressInUse(error: unknown): boolean {
  return /address already in use|port \d+ is already in use/i.test(
    toError(error).message,
  );
}
