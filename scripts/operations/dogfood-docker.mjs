#!/usr/bin/env node

import { spawn } from "node:child_process";
import { chmod, mkdtemp, rm } from "node:fs/promises";
import process from "node:process";
import { pathToFileURL } from "node:url";

const DOCKER_INSPECT_TIMEOUT_MS = 5_000;
const DOCKER_TERM_GRACE_MS = 250;
const DOCKER_REAP_TIMEOUT_MS = 1_000;
const DOCKER_STDOUT_LIMIT = 1024 * 1024;
const DOCKER_STDERR_LIMIT = 64 * 1024;
const PRIVATE_CONFIG_PREFIX = "/tmp/sumi-dogfood-docker.";

class ExternalCancellationError extends Error {
  constructor(signal) {
    super(`Docker operation cancelled by ${signal}`);
    this.exitCode = signal === "SIGINT" ? 130 : 143;
  }
}

function linuxOnly() {
  if (process.platform !== "linux") {
    throw new Error("dogfood Docker operations require Linux process groups");
  }
}

function signalGroup(child, signal) {
  if (!child.pid) return;
  try {
    process.kill(-child.pid, signal);
  } catch (error) {
    if (error?.code !== "ESRCH") throw error;
  }
}

function processGroupExists(child) {
  if (!child.pid) return false;
  try {
    process.kill(-child.pid, 0);
    return true;
  } catch (error) {
    if (error?.code === "ESRCH") return false;
    throw error;
  }
}

function delay(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

export async function terminateProcessGroup(child, signal = "SIGTERM") {
  signalGroup(child, signal);
  await delay(DOCKER_TERM_GRACE_MS);
  signalGroup(child, "SIGKILL");
  const reapDeadline = Date.now() + DOCKER_REAP_TIMEOUT_MS;
  while (processGroupExists(child)) {
    if (Date.now() >= reapDeadline) {
      throw new Error("Docker process group did not exit after SIGKILL");
    }
    await delay(10);
  }
}

function appendBounded(chunks, chunk, state, limit) {
  const remaining = limit - state.bytes;
  if (remaining > 0) {
    const accepted = chunk.subarray(0, remaining);
    chunks.push(accepted);
    state.bytes += accepted.length;
  }
  return chunk.length > remaining;
}

export async function runBoundedProcess(
  command,
  args,
  environment,
  cancellationSignal,
) {
  linuxOnly();
  return new Promise((resolve) => {
    const child = spawn(command, args, {
      detached: true,
      env: environment,
      stdio: ["ignore", "pipe", "pipe"],
    });
    const stdout = [];
    const stderr = [];
    const stdoutState = { bytes: 0 };
    const stderrState = { bytes: 0 };
    let terminationReason;
    let terminationError;
    let terminationPromise;
    let spawnError;

    const terminate = (reason, signal = "SIGTERM") => {
      if (terminationReason) return;
      terminationReason = reason;
      terminationPromise = terminateProcessGroup(child, signal).catch(
        (error) => {
          terminationError = error;
        },
      );
    };
    const cancel = () => {
      const signal = cancellationSignal?.reason;
      terminate(
        `external-${signal === "SIGINT" ? "SIGINT" : "SIGTERM"}`,
        signal === "SIGINT" ? "SIGINT" : "SIGTERM",
      );
    };
    if (cancellationSignal?.aborted) {
      cancel();
    } else {
      cancellationSignal?.addEventListener("abort", cancel, { once: true });
    }

    const deadline = setTimeout(() => {
      terminate("timeout");
    }, DOCKER_INSPECT_TIMEOUT_MS);

    child.stdout.on("data", (chunk) => {
      if (appendBounded(stdout, chunk, stdoutState, DOCKER_STDOUT_LIMIT)) {
        terminate("stdout-limit");
      }
    });
    child.stderr.on("data", (chunk) => {
      if (appendBounded(stderr, chunk, stderrState, DOCKER_STDERR_LIMIT)) {
        terminate("stderr-limit");
      }
    });
    child.once("error", (error) => {
      spawnError = error;
    });
    child.once("close", async (code, signal) => {
      clearTimeout(deadline);
      if (terminationPromise) await terminationPromise;
      cancellationSignal?.removeEventListener("abort", cancel);
      resolve({
        code,
        signal,
        terminationReason,
        spawnError: spawnError ?? terminationError,
        stdout: Buffer.concat(stdout),
        stderr: Buffer.concat(stderr),
      });
    });
  });
}

function dockerEnvironment(configDirectory) {
  const path = process.env.PATH;
  if (!path) throw new Error("PATH is required to locate the Docker CLI");
  return {
    PATH: path,
    HOME: configDirectory,
    DOCKER_CONFIG: configDirectory,
    LANG: "C",
    LC_ALL: "C",
  };
}

async function withPrivateDockerConfig(operation) {
  linuxOnly();
  const controller = new AbortController();
  let receivedSignal;
  const handlers = new Map(
    ["SIGINT", "SIGTERM"].map((signal) => [
      signal,
      () => {
        receivedSignal ??= signal;
        if (!controller.signal.aborted) controller.abort(signal);
      },
    ]),
  );
  for (const [signal, handler] of handlers) process.on(signal, handler);

  let configDirectory;
  let operationResult;
  let operationError;
  let cleanupError;
  try {
    configDirectory = await mkdtemp(PRIVATE_CONFIG_PREFIX);
    await chmod(configDirectory, 0o700);
    if (!controller.signal.aborted) {
      operationResult = await operation(configDirectory, controller.signal);
    }
  } catch (error) {
    operationError = error;
  } finally {
    if (configDirectory) {
      try {
        await rm(configDirectory, { recursive: true, force: true });
      } catch (error) {
        cleanupError = error;
      }
    }
    for (const [signal, handler] of handlers) {
      process.removeListener(signal, handler);
    }
  }
  if (cleanupError) throw cleanupError;
  if (receivedSignal) throw new ExternalCancellationError(receivedSignal);
  if (operationError) throw operationError;
  return operationResult;
}

function dockerArguments(configDirectory, args) {
  return ["--config", configDirectory, "--context", "default", ...args];
}

function isVerifiedImageAbsence(result, subject) {
  const expectedError = Buffer.from(
    `Error response from daemon: No such image: ${subject}\n`,
  );
  const expectedErrorWithLeadingLf = Buffer.concat([
    Buffer.from("\n"),
    expectedError,
  ]);
  return (
    result.code === 1 &&
    result.signal === null &&
    result.stdout.length === 0 &&
    (result.stderr.equals(expectedError) ||
      result.stderr.equals(expectedErrorWithLeadingLf))
  );
}

export async function inspectDockerObject(subject) {
  try {
    return await withPrivateDockerConfig(
      async (configDirectory, cancellationSignal) => {
        const result = await runBoundedProcess(
          "docker",
          dockerArguments(configDirectory, [
            "image",
            "inspect",
            "--format",
            "{{json .}}",
            subject,
          ]),
          dockerEnvironment(configDirectory),
          cancellationSignal,
        );
        if (result.spawnError) return { ok: false, reason: "unavailable" };
        if (result.terminationReason === "timeout") {
          return { ok: false, reason: "timeout" };
        }
        if (result.terminationReason?.startsWith("external-")) {
          return { ok: false, reason: "cancelled" };
        }
        if (result.terminationReason) {
          return { ok: false, reason: "output-limit" };
        }
        if (result.code !== 0) {
          return {
            ok: false,
            reason: isVerifiedImageAbsence(result, subject)
              ? "absent"
              : "inspect-error",
          };
        }
        let value;
        try {
          value = JSON.parse(result.stdout.toString("utf8"));
        } catch {
          return { ok: false, reason: "invalid-json" };
        }
        if (!value || Array.isArray(value) || typeof value !== "object") {
          return { ok: false, reason: "invalid-json" };
        }
        return { ok: true, value };
      },
    );
  } catch (error) {
    if (error instanceof ExternalCancellationError) {
      return { ok: false, reason: "cancelled", exitCode: error.exitCode };
    }
    throw error;
  }
}

async function runDockerBuild(args) {
  return withPrivateDockerConfig(
    async (configDirectory, cancellationSignal) => {
      const child = spawn(
        "docker",
        dockerArguments(configDirectory, ["build", ...args]),
        {
          detached: true,
          env: dockerEnvironment(configDirectory),
          stdio: "inherit",
        },
      );
      let cancellation;
      let spawnError;
      const cancel = () => {
        const signal =
          cancellationSignal.reason === "SIGINT" ? "SIGINT" : "SIGTERM";
        cancellation ??= terminateProcessGroup(child, signal);
      };
      if (cancellationSignal.aborted) {
        cancel();
      } else {
        cancellationSignal.addEventListener("abort", cancel, { once: true });
      }
      const { code, signal } = await new Promise((resolve) => {
        child.once("error", (error) => {
          spawnError = error;
        });
        child.once("close", (code, signal) => resolve({ code, signal }));
      });
      cancellationSignal.removeEventListener("abort", cancel);
      if (cancellation) await cancellation;
      if (spawnError) throw spawnError;
      if (code === 0) return;
      const error = new Error(
        `Docker build failed (${signal ? `signal ${signal}` : `exit ${code}`})`,
      );
      if (Number.isInteger(code) && code > 0 && code < 256) {
        error.exitCode = code;
      }
      throw error;
    },
  );
}

function safeInspectFailure(reason) {
  switch (reason) {
    case "timeout":
      return "Docker inspection exceeded its hard deadline";
    case "output-limit":
      return "Docker inspection exceeded its output limit";
    case "invalid-json":
      return "Docker inspection returned invalid JSON";
    case "unavailable":
      return "Docker inspection could not start";
    case "cancelled":
      return "Docker inspection was cancelled";
    case "inspect-error":
      return "Docker inspection failed without verified absence";
    default:
      return "Docker image is absent";
  }
}

async function main() {
  const [operation, ...args] = process.argv.slice(2);
  if (operation === "reference-status" && args.length === 1) {
    const result = await inspectDockerObject(args[0]);
    if (result.ok) process.exit(0);
    if (result.reason === "absent") process.exit(1);
    process.stderr.write(`${safeInspectFailure(result.reason)}\n`);
    process.exit(2);
  }
  if (operation === "verify-iid" && args.length === 3) {
    const [expectedId, revision, source] = args;
    const result = await inspectDockerObject(expectedId);
    if (!result.ok) {
      process.stderr.write(`${safeInspectFailure(result.reason)}\n`);
      process.exit(1);
    }
    if (result.value.Id !== expectedId) {
      process.stderr.write("inspection subject does not match iidfile\n");
      process.exit(1);
    }
    const labels = result.value.Config?.Labels;
    if (!labels || labels["org.opencontainers.image.revision"] !== revision) {
      process.stderr.write("wrong revision label\n");
      process.exit(1);
    }
    if (labels["org.opencontainers.image.source"] !== source) {
      process.stderr.write("wrong source label\n");
      process.exit(1);
    }
    return;
  }
  if (operation === "build" && args.length > 0) {
    await runDockerBuild(args);
    return;
  }
  throw new Error(
    "usage: dogfood-docker.mjs reference-status <reference> | verify-iid <iid> <revision> <source> | build <args...>",
  );
}

const invokedPath = process.argv[1]
  ? pathToFileURL(process.argv[1]).href
  : undefined;
if (invokedPath === import.meta.url) {
  main().catch((error) => {
    process.stderr.write(`[sumi-dogfood-docker] ERROR: ${error.message}\n`);
    process.exit(error.exitCode ?? 1);
  });
}
