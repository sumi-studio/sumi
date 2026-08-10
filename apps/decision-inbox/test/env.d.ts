import type { D1Migration } from "@cloudflare/vitest-pool-workers";
import type { Env as DecisionInboxEnv } from "../src/worker";

declare global {
  namespace Cloudflare {
    interface Env extends DecisionInboxEnv {
      TEST_MIGRATIONS: D1Migration[];
    }
  }
}
