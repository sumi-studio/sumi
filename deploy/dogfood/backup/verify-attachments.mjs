import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { lstat, readdir, readFile, writeFile } from "node:fs/promises";
import { relative, resolve, sep } from "node:path";

const [rootArgument, rowsPath, outputPath] = process.argv.slice(2);
if (!rootArgument || !rowsPath || !outputPath) {
  throw new Error(
    "usage: verify-attachments.mjs ATTACHMENT_ROOT ROWS_TSV OUTPUT_JSON",
  );
}
const root = resolve(rootArgument);
const rootInfo = await lstat(root);
if (!rootInfo.isDirectory() || rootInfo.isSymbolicLink())
  throw new Error("attachment root must be a real directory");

const uuidV7 =
  /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const expected = new Map();
for (const line of (await readFile(rowsPath, "utf8")).split("\n")) {
  if (!line) continue;
  const fields = line.split("\t");
  if (
    fields.length !== 2 ||
    !uuidV7.test(fields[0]) ||
    !/^(?:0|[1-9][0-9]*)$/.test(fields[1])
  ) {
    throw new Error("attachment row export is not canonical id<TAB>size data");
  }
  if (expected.has(fields[0]))
    throw new Error(`duplicate attachment row ${fields[0]}`);
  const size = Number(fields[1]);
  if (!Number.isSafeInteger(size))
    throw new Error(`attachment row ${fields[0]} has an unsafe size`);
  expected.set(fields[0], size);
}

const files = [];
await walk(root);
files.sort((left, right) => left.path.localeCompare(right.path));
const seen = new Set();
for (const file of files) {
  const match = /^([0-9a-f]{2})\/([0-9a-f]{2})\/([0-9a-f-]{36})\.bin$/.exec(
    file.path,
  );
  if (
    !match ||
    !uuidV7.test(match[3]) ||
    match[1] !== match[3].slice(0, 2) ||
    match[2] !== match[3].slice(2, 4)
  ) {
    throw new Error(`noncanonical attachment blob path ${file.path}`);
  }
  const expectedSize = expected.get(match[3]);
  if (expectedSize === undefined)
    throw new Error(`orphan attachment blob ${match[3]}`);
  if (expectedSize !== file.size)
    throw new Error(
      `attachment ${match[3]} size ${file.size}, database says ${expectedSize}`,
    );
  seen.add(match[3]);
}
for (const id of expected.keys()) {
  if (!seen.has(id)) throw new Error(`database attachment ${id} has no blob`);
}
await writeFile(
  outputPath,
  `${JSON.stringify({ version: 1, files }, null, 2)}\n`,
  { flag: "wx", mode: 0o600 },
);

async function walk(directory) {
  const entries = await readdir(directory, { withFileTypes: true });
  entries.sort((left, right) => left.name.localeCompare(right.name));
  for (const entry of entries) {
    const absolute = resolve(directory, entry.name);
    const lexical = relative(root, absolute);
    if (
      !lexical ||
      lexical.startsWith(`..${sep}`) ||
      lexical === ".." ||
      lexical.includes("\\")
    ) {
      throw new Error("attachment traversal escaped its root");
    }
    const info = await lstat(absolute);
    if (info.isSymbolicLink())
      throw new Error(`symlink is forbidden in attachment tree: ${lexical}`);
    if (info.isDirectory()) {
      await walk(absolute);
      continue;
    }
    if (!info.isFile())
      throw new Error(`non-file attachment artifact: ${lexical}`);
    files.push({
      path: lexical.split(sep).join("/"),
      size: info.size,
      sha256: await sha256(absolute),
    });
  }
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
