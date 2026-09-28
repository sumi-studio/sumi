import { Container, getContainer } from "@cloudflare/containers";
import { containerEnv, idlePolicy } from "./config.ts";

/**
 * Hosts the Sumi Go API in one Cloudflare Container.
 *
 * The Web Worker (`SUMI_ORIGIN`) and the secretary Core Worker (`SUMI_STATE`)
 * reach this Worker through service bindings; it has no public route. Every
 * request goes to the same named instance: the API owns its journals through
 * a single-writer PostgreSQL mirror, and a second instance would fence the
 * first.
 *
 * The API also runs background work that no request triggers (waking
 * secretaries with queued input, attention delivery, email delivery, transfer
 * expiry). By default the instance therefore keeps running when requests stop,
 * and a cron trigger starts it again after a crash or rollout. Setting
 * SUMI_API_IDLE_POLICY=sleep lets it stop after `sleepAfter` of inactivity;
 * that saves cost only if nothing depends on that background work.
 */
const API_INSTANCE = "origin";

export interface Env {
  SUMI_API: DurableObjectNamespaceLike;
  /** "keep" (default) or "sleep". */
  SUMI_API_IDLE_POLICY?: string;
  [name: string]: unknown;
}

interface DurableObjectNamespaceLike {
  idFromName(name: string): unknown;
  get(id: unknown): { fetch(request: Request): Promise<Response> };
}

export class SumiApiContainer extends Container<Env> {
  defaultPort = 8080;
  sleepAfter = "15m";
  enableInternet = true;
  // The API's own readiness route; the container is running once it answers.
  pingEndpoint = "container/health";
  private readonly policy: "keep" | "sleep";

  constructor(ctx: ConstructorParameters<typeof Container<Env>>[0], env: Env) {
    super(ctx, env);
    this.policy = idlePolicy(env);
    this.envVars = containerEnv(env, `cloudflare-container ${String(ctx.id)}`);
  }

  override async onActivityExpired(): Promise<void> {
    if (this.policy === "sleep") {
      await this.stop();
    }
    // "keep": returning without stopping renews the activity timer.
  }

  override onStart(): void {
    console.log("sumi api container started");
  }

  override onStop(params: unknown): void {
    console.log("sumi api container stopped", JSON.stringify(params));
  }

  override onError(error: unknown): unknown {
    console.error("sumi api container error", error);
    throw error;
  }
}

function apiInstance(env: Env) {
  return getContainer(
    env.SUMI_API as unknown as Parameters<typeof getContainer>[0],
    API_INSTANCE,
  );
}

export default {
  fetch(request: Request, env: Env): Promise<Response> {
    return apiInstance(env).fetch(request);
  },

  /** Restarts the instance after a crash or rollout under the "keep" policy. */
  async scheduled(_controller: unknown, env: Env): Promise<void> {
    if (idlePolicy(env) !== "keep") return;
    const response = await apiInstance(env).fetch(
      new Request("http://sumi-api-container/health"),
    );
    await response.body?.cancel();
    if (!response.ok) {
      console.error(`sumi api keep-alive health answered ${response.status}`);
    }
  },
};
