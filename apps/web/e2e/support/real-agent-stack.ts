import { type ChildProcess, spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { once } from "node:events";
import { chmod, lstat, mkdir, mkdtemp, rm } from "node:fs/promises";
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

const personalityAgentID = "0198f0f4-9b72-7000-8000-000000000001";
const realAgentHumanID = "0198f0f4-9b72-7000-8000-000000000002";
const workspaceBrowserHumanID = "0198f0f4-9b72-7000-8000-00000000e2e0";
const generation = "7";
const firebaseProjectID = "sumi-studio";
const firebaseWebAPIKey = "sumi-direct-chat-e2e-public-key";
const firebaseWebAppID = "1:000000000000:web:0000000000000000000000";

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
          ])
        ) {
          respondJSON(response, 422, { error: "turn_one_user_missing" });
          return;
        }
        respondSSE(response, turn, firstProviderResponse);
        return;
      }
      if (turn === 2) {
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

export class RealAgentStack {
  readonly apiURL: string;
  readonly webURL: string;
  readonly provider: LoopbackChatProvider;

  private readonly runtimeDirectory: string;
  private readonly sessionCookie: string;
  private readonly children: ManagedProcess[];
  private readonly reviewerProviders: LoopbackChatProvider[];
  private stopped = false;

  constructor({
    apiURL,
    webURL,
    provider,
    runtimeDirectory,
    sessionCookie,
    children,
    reviewerProviders,
  }: {
    apiURL: string;
    webURL: string;
    provider: LoopbackChatProvider;
    runtimeDirectory: string;
    sessionCookie: string;
    children: ManagedProcess[];
    reviewerProviders: LoopbackChatProvider[];
  }) {
    this.apiURL = apiURL;
    this.webURL = webURL;
    this.provider = provider;
    this.runtimeDirectory = runtimeDirectory;
    this.sessionCookie = sessionCookie;
    this.children = children;
    this.reviewerProviders = reviewerProviders;
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
  const executionReviewerProvider = new LoopbackChatProvider(
    executionReviewerApiKey,
  );
  const escalationReviewerApiKey = randomToken();
  const escalationReviewerProvider = new LoopbackChatProvider(
    escalationReviewerApiKey,
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
      ipc: join(runtimeDirectory, "ipc"),
    };
    await Promise.all(
      Object.values(paths).map((path) =>
        mkdir(path, { recursive: true, mode: 0o700 }),
      ),
    );
    await Promise.all(Object.values(paths).map((path) => chmod(path, 0o700)));

    const executorServerUID = process.getuid?.();
    if (executorServerUID === undefined || executorServerUID === 0) {
      throw new Error(
        "the real-agent fixture requires a non-root Unix UID for its executor",
      );
    }

    const [publicPort, localControlPort, webPort] = await Promise.all([
      ephemeralPort(),
      ephemeralPort(),
      ephemeralPort(),
    ]);
    const apiURL = `http://127.0.0.1:${publicPort}`;
    const localControlURL = `http://127.0.0.1:${localControlPort}`;
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
    const redactions = [
      agentTokenSecret,
      browserSessionSecret,
      localControlBearer,
      providerApiKey,
      executionReviewerApiKey,
      escalationReviewerApiKey,
      wrappingKey,
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
        SUMI_LOCAL_CONTROL_LOOPBACK_LISTEN: `127.0.0.1:${localControlPort}`,
      },
      redactions,
    });
    children.push(api);
    await waitForHTTP(`${apiURL}/health`, api, 20_000);
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
          SUMI_WORKSPACE: paths.workspace,
          SUMI_EXECUTOR_SOCKET: executorSocket,
          SUMI_LOG: "sumi_agent=info",
        },
        redactions,
      },
    );
    children.push(executor);
    await waitForSocket(executorSocket, executor, 20_000);

    const agent = ManagedProcess.start(
      "Rust production personality agent",
      build.agent,
      [],
      {
        cwd: agentDirectory,
        env: {
          ...baseEnvironment,
          ...commonIdentityEnvironment,
          SUMI_PROCESS_GENERATION_LEASE_ID: leaseID,
          SUMI_GENERATION_RECOVERY_FENCE_ID: fenceID,
          SUMI_STATE_DIR: paths.agentState,
          SUMI_WORKSPACE: paths.workspace,
          SUMI_EXECUTOR_SOCKET: executorSocket,
          SUMI_EXECUTOR_SERVER_UID: String(executorServerUID),
          SUMI_GATEWAY_URL: `ws://127.0.0.1:${publicPort}/agent/ws`,
          SUMI_ALLOW_INSECURE_LOOPBACK_GATEWAY: "true",
          SUMI_LOCAL_CONTROL_URL: localControlURL,
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
  throw new Error(
    `timed out waiting for executor socket${process_.diagnostics()}`,
  );
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
    return [{ role: message.role, text: messageText(message.content) }];
  });
  if (conversation.length !== expected.length) return false;
  return expected.every(
    (entry, index) =>
      conversation[index]?.role === entry.role &&
      conversation[index]?.text === entry.text,
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
          delta: { role: "assistant", content: text },
          finish_reason: null,
        },
      ],
    })}\n\n`,
  );
  response.write(
    `data: ${JSON.stringify({
      id,
      model: "kimi-k2.7-code",
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
