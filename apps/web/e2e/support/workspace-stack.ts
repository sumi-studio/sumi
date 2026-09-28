import { type ChildProcess, spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { once } from "node:events";
import { chmod, mkdir, mkdtemp, rm } from "node:fs/promises";
import { createServer as createNetServer } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import type { BrowserContext } from "@playwright/test";

const supportDirectory = dirname(fileURLToPath(import.meta.url));
const repositoryRoot = resolve(supportDirectory, "../../../..");
const apiDirectory = resolve(repositoryRoot, "apps/api");
const webDirectory = resolve(repositoryRoot, "apps/web");
const maxChildLogCharacters = 64 * 1024;
const processStopTimeoutMilliseconds = 8_000;
const browserSessionAudience = "sumi:web";

const personalityAgentID = "0198f0f4-9b72-7000-8000-000000000001";
const workspaceBrowserHumanID = "0198f0f4-9b72-7000-8000-00000000e2e0";
const firebaseProjectID = "sumi-studio";
export interface WorkspaceBrowserBuild { directory:string; apiServer:string; sessionIssuer:string }
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
    const messagingAttachmentRoot = join(
      runtimeDirectory,
      "messaging-attachments",
    );
    await Promise.all(
      [commandLog, gatewayState, messagingAttachmentRoot].map((path) =>
        mkdir(path, { recursive: true, mode: 0o700 }),
      ),
    );
    await Promise.all(
      [commandLog, gatewayState, messagingAttachmentRoot].map((path) =>
        chmod(path, 0o700),
      ),
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
    const firebaseAuthEmulator = requiredFirebaseAuthEmulator();
    await assertFirebaseAuthEmulator(firebaseAuthEmulator);
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
          SUMI_BROWSER_EVENT_DIR: gatewayState,
          SUMI_BROWSER_SESSION_SECRET: browserSessionSecret,
          SUMI_BROWSER_SESSION_AUDIENCE: browserSessionAudience,
          SUMI_BROWSER_WS_ALLOWED_ORIGINS: webURL,
          FIREBASE_AUTH_EMULATOR_HOST: firebaseAuthEmulator.host,
          SUMI_AUTH_FIREBASE_PROJECT_ID: firebaseProjectID,
          SUMI_AUTH_TENANT_ID: tenantID,
          SUMI_AUTH_ALLOW_INSECURE_COOKIES: "true",
          SUMI_CORE_STATE_TOKEN: randomBytes(32).toString("hex"),
          SUMI_DB_URL: databaseURL,
          SUMI_MESSAGING_ATTACHMENT_ROOT: messagingAttachmentRoot,
          SUMI_MESSAGING_ATTACHMENT_WORKSPACE_QUOTA_BYTES: "20971520",
          SUMI_MESSAGING_ATTACHMENT_WORKSPACE_QUOTA_OBJECTS: "10",
          SUMI_MESSAGING_ATTACHMENT_TOTAL_QUOTA_BYTES: "41943040",
          SUMI_MESSAGING_ATTACHMENT_TOTAL_QUOTA_OBJECTS: "100",
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
