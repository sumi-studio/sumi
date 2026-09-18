/**
 * Host-local filesystem privacy for runner-owned state. The per-job
 * config.capnp carries the cross-persona runtime token — a file or
 * directory created under a permissive umask, or a pre-existing
 * permissive path, is a real read window that deleting the file later
 * cannot close (F392).
 *
 * Rules enforced here:
 *  - directories are 0700 and files 0600 FROM CREATION — an explicit
 *    creation mode is the ceiling under any umask, so there is no
 *    initial readable window;
 *  - a pre-existing path is tightened only when it is a regular
 *    dir/file owned by THIS uid — never a symlink followed, never an
 *    arbitrary/shared or foreign-owned path chmod'd (an unowned path
 *    is an honest startup/job error, not a takeover);
 *  - for an existing permissive file the chmod happens BEFORE the new
 *    contents are written, so the credential never sits in a
 *    group/other-readable file at any instant.
 */

import { chmodSync, lstatSync, mkdirSync, writeFileSync } from "node:fs";

function ownedByUs(st: { uid: number }, p: string): void {
  const uid = process.getuid?.();
  if (uid != null && st.uid !== uid) {
    throw new Error(`${p} is owned by uid ${st.uid}, not this runner (uid ${uid}) — refusing to claim or tighten a foreign path`);
  }
}

/** Create `dir` 0700 or tighten a pre-existing owned directory. */
export function ensurePrivateDir(dir: string): void {
  try {
    const st = lstatSync(dir); // lstat: a symlink is not silently followed
    if (!st.isDirectory()) throw new Error(`${dir} exists but is not a directory`);
    ownedByUs(st, dir);
    if ((st.mode & 0o777) !== 0o700) chmodSync(dir, 0o700);
  } catch (e) {
    if ((e as NodeJS.ErrnoException).code === "ENOENT") {
      mkdirSync(dir, { mode: 0o700 }); // creation mode is the umask ceiling
      return;
    }
    throw e;
  }
}

/** Write `p` 0600, tightening an existing owned file before rewriting. */
export function writePrivateFile(p: string, data: string): void {
  try {
    const st = lstatSync(p);
    if (!st.isFile()) throw new Error(`${p} exists but is not a regular file`);
    ownedByUs(st, p);
    if ((st.mode & 0o777) !== 0o600) chmodSync(p, 0o600);
  } catch (e) {
    if ((e as NodeJS.ErrnoException).code !== "ENOENT") throw e;
  }
  writeFileSync(p, data, { mode: 0o600 });
}
