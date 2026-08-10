import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { lstat, readdir, readFile, writeFile } from "node:fs/promises";
import { relative, resolve, sep } from "node:path";

const [mode, rootArgument, manifestArgument] = process.argv.slice(2);
const topLevel = ["attachments", "command-log", "runtime-state"];
if (!mode || !rootArgument || !manifestArgument) {
  throw new Error(
    "usage: host-state-manifest.mjs create|verify STATE_ROOT MANIFEST",
  );
}
const root = resolve(rootArgument);
const manifestPath = resolve(manifestArgument);
const rootInfo = await lstat(root);
if (!rootInfo.isDirectory() || rootInfo.isSymbolicLink()) {
  throw new Error("host state root must be a real directory");
}

const actual = await describeTree();
if (mode === "create") {
  const rootEntries = await readdir(root);
  const allowed = new Set([...topLevel, ".operations.lock"]);
  for (const name of rootEntries) {
    if (!allowed.has(name))
      throw new Error(`unexpected host state entry ${name}`);
  }
  if (!rootEntries.includes(".operations.lock")) {
    throw new Error("host state operation lock is missing");
  }
  const lock = await lstat(resolve(root, ".operations.lock"));
  if (
    !lock.isFile() ||
    lock.isSymbolicLink() ||
    (lock.mode & 0o777) !== 0o600
  ) {
    throw new Error(
      "host state operation lock is not a protected regular file",
    );
  }
  await writeFile(
    manifestPath,
    `${JSON.stringify({ version: 1, roots: topLevel, entries: actual }, null, 2)}\n`,
    { flag: "wx", mode: 0o600 },
  );
} else if (mode === "verify") {
  const rootEntries = (await readdir(root)).sort(byteOrder);
  if (
    JSON.stringify(rootEntries) !==
    JSON.stringify([...topLevel].sort(byteOrder))
  ) {
    throw new Error(
      "restored host state has a missing or unexpected root entry",
    );
  }
  const expected = JSON.parse(await readFile(manifestPath, "utf8"));
  if (
    expected.version !== 1 ||
    JSON.stringify(expected.roots) !== JSON.stringify(topLevel) ||
    JSON.stringify(expected.entries) !== JSON.stringify(actual)
  ) {
    throw new Error(
      "restored host state metadata or content differs from snapshot",
    );
  }
} else {
  throw new Error(`unknown mode ${mode}`);
}

async function describeTree() {
  const entries = [];
  for (const name of topLevel) {
    const path = resolve(root, name);
    const info = await lstat(path);
    if (!info.isDirectory() || info.isSymbolicLink()) {
      throw new Error(`${name} must be a real directory`);
    }
    await walk(path);
  }
  entries.sort((left, right) => byteOrder(left.path, right.path));
  return entries;

  async function walk(path) {
    const info = await lstat(path);
    const lexical = relative(root, path);
    if (!lexical || lexical === ".." || lexical.startsWith(`..${sep}`)) {
      throw new Error("host state traversal escaped its root");
    }
    const common = {
      path: lexical.split(sep).join("/"),
      mode: info.mode & 0o7777,
      uid: info.uid,
      gid: info.gid,
    };
    if (info.isDirectory()) {
      entries.push({ ...common, type: "directory" });
      const children = await readdir(path);
      children.sort(byteOrder);
      for (const child of children) await walk(resolve(path, child));
      return;
    }
    if (info.isSymbolicLink())
      throw new Error(`symlink is forbidden: ${lexical}`);
    if (!info.isFile())
      throw new Error(`special host state entry is forbidden: ${lexical}`);
    if (info.nlink !== 1)
      throw new Error(`hard-linked host state file is forbidden: ${lexical}`);
    entries.push({
      ...common,
      type: "file",
      size: info.size,
      sha256: await sha256(path),
    });
  }
}

function byteOrder(left, right) {
  return Buffer.compare(Buffer.from(left), Buffer.from(right));
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
