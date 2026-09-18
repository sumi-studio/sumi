/**
 * Per-job workerd config generation. The trusted parent worker's module
 * (dispatcher.js) and the socket path go into a capnp written to a
 * per-job file. Job-scoped values (token, persona, limits) travel as
 * workerd `text` bindings — config syntax, never code interpolation:
 * a hostile string cannot escape a quoted binding value because every
 * control character is JSON-escaped. The dispatcher parses numeric
 * bindings itself, so capnp `json` typing quirks never matter.
 */

import { writeFileSync, readFileSync } from "node:fs";
import { join } from "node:path";

export interface WorkerdConfigInput {
  socketPath: string;
  dispatcherPath: string; // absolute path to dispatcher.js
  api: string;            // state API origin, e.g. http://127.0.0.1:3001
  token: string;          // runner bearer token (internal claim credential)
  personaID: string;
  jobID: string;
  runnerID: string;
  limits: {
    cpu_ms: number;
    file_calls: number;
    file_bytes: number;
    log_bytes: number;
  };
}

const q = (s: string): string => JSON.stringify(s);

export function renderCapnp(c: WorkerdConfigInput): string {
  const dispatcher = readFileSync(c.dispatcherPath, "utf8");
  // The API is a DECLARED external service — the only network egress in
  // the whole process — reached through a service binding, never raw
  // connect()/fetch (which restrictPeers refuses anyway). The loaded
  // script isolate gets globalOutbound: null regardless.
  return `using Workerd = import "/workerd/workerd.capnp";

const config :Workerd.Config = (
  services = [
    ( name = "api", external = ( address = ${q(c.api.replace(/^https?:\/\//, "").replace(/\/$/, ""))}, http = () ) ),
    (
      name = "sumi-script",
      worker = (
        modules = [ ( name = "dispatcher.js", esModule = ${q(dispatcher)} ) ],
        compatibilityDate = "2026-08-04",
        compatibilityFlags = ["nodejs_compat"],
        bindings = [
          ( name = "LOADER", workerLoader = ( id = "sumi-script-loader" ) ),
          ( name = "SUMI_API", service = "api" ),
          ( name = "SUMI_TOKEN", text = ${q(c.token)} ),
          ( name = "SUMI_PERSONA", text = ${q(c.personaID)} ),
          ( name = "SUMI_JOB", text = ${q(c.jobID)} ),
          ( name = "SUMI_RUNNER", text = ${q(c.runnerID)} ),
          ( name = "SUMI_CPU_MS", text = ${q(String(c.limits.cpu_ms))} ),
          ( name = "SUMI_MAX_FILE_CALLS", text = ${q(String(c.limits.file_calls))} ),
          ( name = "SUMI_MAX_FILE_BYTES", text = ${q(String(c.limits.file_bytes))} ),
          ( name = "SUMI_MAX_LOG_BYTES", text = ${q(String(c.limits.log_bytes))} ),
        ]
      )
    )
  ],
  sockets = [
    ( name = "job", address = ${q(`unix:${c.socketPath}`)}, http = (), service = "sumi-script" )
  ]
);
`;
}

export function writeConfig(dir: string, c: WorkerdConfigInput): string {
  const p = join(dir, "config.capnp");
  writeFileSync(p, renderCapnp(c));
  return p;
}
