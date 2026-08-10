import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { lstat, readdir, readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";

const [mode, rootArgument, inputArgument, outputArgument] =
  process.argv.slice(2);
const uuidV7 =
  /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const projectName = /^sumi-[0-9a-f]{32}$/;
const logicalVolumes = [
  "allocator-root",
  "allocator-state",
  "artifacts",
  "broker-identity",
  "broker-ipc",
  "executor-identity",
  "executor-ipc",
  "runtime-identity",
  "state",
  "workspace",
];
if (!mode || !rootArgument || !inputArgument) {
  throw new Error(
    "usage: agent-volume-set.mjs create ROOT ROWS OUTPUT | verify ROOT SET | list ROOT SET",
  );
}
const root = resolve(rootArgument);
const rootInfo = await lstat(root);
if (!rootInfo.isDirectory() || rootInfo.isSymbolicLink()) {
  throw new Error("agent volume artifact root must be a real directory");
}

if (mode === "create") {
  if (!outputArgument) throw new Error("create requires an output path");
  const rows = (await readFile(resolve(inputArgument), "utf8"))
    .split("\n")
    .filter(Boolean)
    .map((line) => line.split("\t"));
  const agents = [];
  let current;
  for (const fields of rows) {
    if (fields[0] === "A" && fields.length === 4) {
      const [, personalityAgentID, project, state] = fields;
      validateIdentity(personalityAgentID, project);
      if (state !== "unprovisioned") throw new Error("invalid agent state row");
      current = {
        personality_agent_id: personalityAgentID,
        project,
        state,
        volumes: [],
      };
      agents.push(current);
      continue;
    }
    if (fields[0] === "V" && fields.length === 7) {
      const [
        ,
        personalityAgentID,
        project,
        logical,
        volume,
        archive,
        manifest,
      ] = fields;
      validateIdentity(personalityAgentID, project);
      if (!current || current.personality_agent_id !== personalityAgentID) {
        current = {
          personality_agent_id: personalityAgentID,
          project,
          state: "provisioned",
          volumes: [],
        };
        agents.push(current);
      }
      if (current.project !== project || current.state !== "provisioned") {
        throw new Error("agent volume rows are not contiguous and coherent");
      }
      if (
        !logicalVolumes.includes(logical) ||
        volume !== `${project}_${logical}`
      ) {
        throw new Error(`noncanonical agent volume ${volume}`);
      }
      if (
        archive !== `${personalityAgentID}/${logical}.tar` ||
        manifest !== `${personalityAgentID}/${logical}.manifest`
      ) {
        throw new Error(`noncanonical agent volume artifact for ${logical}`);
      }
      current.volumes.push({
        logical_name: logical,
        source_volume: volume,
        archive: await describe(archive),
        content_manifest: await describe(manifest),
      });
      continue;
    }
    throw new Error("invalid agent volume row");
  }
  const seen = new Set();
  for (const agent of agents) {
    if (seen.has(agent.personality_agent_id))
      throw new Error("duplicate agent in volume set");
    seen.add(agent.personality_agent_id);
    if (agent.state === "provisioned") {
      const names = agent.volumes.map((entry) => entry.logical_name);
      if (JSON.stringify(names) !== JSON.stringify(logicalVolumes)) {
        throw new Error(
          `agent ${agent.personality_agent_id} has a partial volume set`,
        );
      }
    }
  }
  const sorted = [...agents].sort((a, b) =>
    a.personality_agent_id.localeCompare(b.personality_agent_id),
  );
  if (JSON.stringify(sorted) !== JSON.stringify(agents))
    throw new Error("agent rows are not sorted");
  await writeFile(
    resolve(outputArgument),
    `${JSON.stringify({ version: 1, logical_volumes: logicalVolumes, agents }, null, 2)}\n`,
    { flag: "wx", mode: 0o600 },
  );
} else if (mode === "verify" || mode === "list") {
  const set = JSON.parse(await readFile(resolve(inputArgument), "utf8"));
  validateSet(set);
  await verifyExactArtifactTree(set);
  if (mode === "verify") {
    for (const agent of set.agents) {
      for (const volume of agent.volumes) {
        if (
          JSON.stringify(await describe(volume.archive.name)) !==
          JSON.stringify(volume.archive)
        ) {
          throw new Error(
            `agent volume archive mismatch: ${volume.archive.name}`,
          );
        }
        if (
          JSON.stringify(await describe(volume.content_manifest.name)) !==
          JSON.stringify(volume.content_manifest)
        ) {
          throw new Error(
            `agent volume manifest mismatch: ${volume.content_manifest.name}`,
          );
        }
      }
    }
  } else {
    for (const agent of set.agents) {
      for (const volume of agent.volumes) {
        process.stdout.write(
          `${[
            agent.personality_agent_id,
            agent.project,
            volume.logical_name,
            volume.source_volume,
            volume.archive.name,
            volume.content_manifest.name,
          ].join("\t")}\n`,
        );
      }
    }
  }
} else {
  throw new Error(`unknown mode ${mode}`);
}

async function verifyExactArtifactTree(set) {
  const provisioned = set.agents.filter(
    (agent) => agent.state === "provisioned",
  );
  const expectedDirectories = provisioned.map(
    (agent) => agent.personality_agent_id,
  );
  const actualRootEntries = await readdir(root, { withFileTypes: true });
  if (
    actualRootEntries.some((entry) => !entry.isDirectory()) ||
    JSON.stringify(actualRootEntries.map((entry) => entry.name).sort()) !==
      JSON.stringify([...expectedDirectories].sort())
  ) {
    throw new Error("agent volume artifact root has an unexpected entry");
  }
  for (const agent of provisioned) {
    const expectedFiles = agent.volumes
      .flatMap((volume) => [
        `${volume.logical_name}.tar`,
        `${volume.logical_name}.manifest`,
      ])
      .sort();
    const actualFiles = await readdir(
      resolve(root, agent.personality_agent_id),
      { withFileTypes: true },
    );
    if (
      actualFiles.some((entry) => !entry.isFile()) ||
      JSON.stringify(actualFiles.map((entry) => entry.name).sort()) !==
        JSON.stringify(expectedFiles)
    ) {
      throw new Error(
        `agent ${agent.personality_agent_id} artifact set is not exact`,
      );
    }
  }
}

function validateIdentity(personalityAgentID, project) {
  if (!uuidV7.test(personalityAgentID) || !projectName.test(project))
    throw new Error("invalid agent volume identity");
  if (project !== `sumi-${personalityAgentID.replaceAll("-", "")}`)
    throw new Error("project does not derive from PAID");
}

function validateSet(set) {
  if (
    set.version !== 1 ||
    JSON.stringify(set.logical_volumes) !== JSON.stringify(logicalVolumes) ||
    !Array.isArray(set.agents)
  ) {
    throw new Error("invalid agent volume set");
  }
  let previous = "";
  for (const agent of set.agents) {
    validateIdentity(agent.personality_agent_id, agent.project);
    if (agent.personality_agent_id <= previous)
      throw new Error("agent volume set is not unique and sorted");
    previous = agent.personality_agent_id;
    if (
      !Array.isArray(agent.volumes) ||
      !["provisioned", "unprovisioned"].includes(agent.state)
    )
      throw new Error("invalid agent volume state");
    if (agent.state === "unprovisioned" && agent.volumes.length !== 0)
      throw new Error("unprovisioned agent has volumes");
    if (agent.state === "provisioned") {
      if (
        JSON.stringify(agent.volumes.map((entry) => entry.logical_name)) !==
        JSON.stringify(logicalVolumes)
      )
        throw new Error("partial agent volume set");
      for (const volume of agent.volumes) {
        if (volume.source_volume !== `${agent.project}_${volume.logical_name}`)
          throw new Error("noncanonical source volume");
        if (
          volume.archive?.name !==
            `${agent.personality_agent_id}/${volume.logical_name}.tar` ||
          volume.content_manifest?.name !==
            `${agent.personality_agent_id}/${volume.logical_name}.manifest`
        ) {
          throw new Error("noncanonical agent volume artifact binding");
        }
        validateArtifact(volume.archive);
        validateArtifact(volume.content_manifest);
      }
    }
  }
}

function validateArtifact(artifact) {
  if (
    !artifact ||
    !/^[0-9a-f]{64}$/.test(artifact.sha256 ?? "") ||
    !Number.isSafeInteger(artifact.size) ||
    artifact.size < 0 ||
    !/^[0-9a-f-]{36}\/(?:[a-z-]+)\.(?:tar|manifest)$/.test(artifact.name ?? "")
  ) {
    throw new Error("invalid agent volume artifact");
  }
}

async function describe(name) {
  const path = resolve(root, name);
  if (!path.startsWith(`${root}/`))
    throw new Error("agent volume artifact escaped its root");
  const info = await lstat(path);
  if (!info.isFile() || info.isSymbolicLink())
    throw new Error(`${name} is not a regular file`);
  return { name, size: info.size, sha256: await sha256(path) };
}

function sha256(path) {
  return new Promise((resolveHash, reject) => {
    const hash = createHash("sha256");
    const input = createReadStream(path);
    input.on("error", reject);
    input.on("data", (chunk) => hash.update(chunk));
    input.on("end", () => resolveHash(hash.digest("hex")));
  });
}
