/** Page scripts for the semantic checkpoint. They run in the page's main
 * world because site storage belongs to it.
 *
 * Supported: localStorage strings, and IndexedDB records whose key and value
 * are plain JSON data (null, booleans, finite numbers, strings, arrays and
 * plain objects; keys: strings, finite numbers and arrays of them). Records
 * holding anything else — Blob, File, Date, ArrayBuffer and typed arrays,
 * Map/Set, class instances, binary or Date keys — are counted and skipped,
 * never converted. Not saved: sessionStorage, page memory, form drafts,
 * history, HTTP cache, service workers, Cache Storage, OPFS. */

export interface OriginData {
  localStorage: Record<string, string>;
  indexedDB: {
    name: string;
    version: number;
    stores: {
      name: string;
      keyPath: string | string[] | null;
      autoIncrement: boolean;
      indexes: { name: string; keyPath: string | string[]; unique: boolean; multiEntry: boolean }[];
      records: { k: unknown; v: unknown }[];
    }[];
  }[];
}

export type Collected =
  | (OriginData & { nonJsonValues: number; sessionStorageKeys: number; bytes: number; tooLarge?: undefined })
  | { tooLarge: true; bytes: number };

/** Reads one origin's storage, giving up (tooLarge) as soon as its JSON
 * would exceed `maxBytes`, so one heavy site never reaches the host whole. */
export const COLLECT = (maxBytes: number) => `(async () => {
  const MAX = ${Math.max(0, Math.floor(maxBytes))};
  const enc = new TextEncoder();
  let bytes = 0;
  const TOO_LARGE = {};
  const add = (value) => { bytes += enc.encode(JSON.stringify(value)).length + 1; if (bytes > MAX) throw TOO_LARGE; };
  const req = (r) => new Promise((res, rej) => { r.onsuccess = () => res(r.result); r.onerror = () => rej(r.error); });
  const plain = (v, depth) => {
    if (depth > 100) return false;
    if (v === null || typeof v === "string" || typeof v === "boolean") return true;
    if (typeof v === "number") return Number.isFinite(v);
    if (typeof v !== "object") return false;
    if (Array.isArray(v)) {
      if (Object.getPrototypeOf(v) !== Array.prototype) return false;
      for (let i = 0; i < v.length; i++) if (!(i in v) || !plain(v[i], depth + 1)) return false;
      return true;
    }
    const proto = Object.getPrototypeOf(v);
    if (proto !== Object.prototype && proto !== null) return false;
    for (const k of Object.keys(v)) if (!plain(v[k], depth + 1)) return false;
    return true;
  };
  const key = (k) => typeof k === "string" || (typeof k === "number" && Number.isFinite(k)) || (Array.isArray(k) && k.every(key));
  try {
    const ls = {};
    for (let i = 0; i < localStorage.length; i++) { const k = localStorage.key(i); const v = localStorage.getItem(k); add([k, v]); ls[k] = v; }
    const dbs = []; let skipped = 0;
    for (const info of (indexedDB.databases ? await indexedDB.databases() : [])) {
      if (!info.name) continue;
      const db = await req(indexedDB.open(info.name));
      try {
        const stores = [];
        for (const name of db.objectStoreNames) {
          const st = db.transaction(name).objectStore(name);
          const keys = await req(st.getAllKeys()); const values = await req(st.getAll());
          const records = [];
          for (let i = 0; i < keys.length; i++) {
            const k = keys[i]; const v = values[i];
            if (!key(k) || !plain(v, 0)) { skipped++; continue; }
            add({ k, v }); records.push({ k, v });
          }
          stores.push({ name, keyPath: st.keyPath, autoIncrement: st.autoIncrement,
            indexes: [...st.indexNames].map((n) => { const ix = st.index(n); return { name: n, keyPath: ix.keyPath, unique: ix.unique, multiEntry: ix.multiEntry }; }),
            records });
        }
        dbs.push({ name: info.name, version: db.version, stores });
      } finally { db.close(); }
    }
    return { localStorage: ls, indexedDB: dbs, nonJsonValues: skipped, sessionStorageKeys: sessionStorage.length, bytes };
  } catch (error) {
    if (error === TOO_LARGE) return { tooLarge: true, bytes };
    throw error;
  }
})()`;

/** Seeds one origin's storage. A record the browser refuses (for example a
 * key saved by an older serializer) is counted and skipped; it never stops
 * the rest of the origin, or the profile, from being restored. */
export const RESTORE = (data: OriginData) => `(async () => {
  const data = ${JSON.stringify(data)};
  const req = (r) => new Promise((res, rej) => { r.onsuccess = () => res(r.result); r.onerror = () => rej(r.error); r.onblocked = () => rej(new Error('blocked')); });
  let failed = 0;
  localStorage.clear();
  for (const [k, v] of Object.entries(data.localStorage || {})) { try { localStorage.setItem(k, v); } catch { failed++; } }
  let records = 0;
  for (const d of data.indexedDB || []) {
    await req(indexedDB.deleteDatabase(d.name));
    const open = indexedDB.open(d.name, d.version);
    open.onupgradeneeded = () => { const db = open.result;
      for (const s of d.stores) { const st = db.createObjectStore(s.name, { keyPath: s.keyPath ?? undefined, autoIncrement: s.autoIncrement });
        for (const ix of s.indexes) st.createIndex(ix.name, ix.keyPath, { unique: ix.unique, multiEntry: ix.multiEntry }); } };
    const db = await req(open);
    for (const s of d.stores) { if (!s.records.length) continue;
      const tx = db.transaction(s.name, 'readwrite'); const st = tx.objectStore(s.name);
      for (const r of s.records) {
        try {
          const q = s.keyPath == null ? st.put(r.v, r.k) : st.put(r.v);
          q.onsuccess = () => { records++; };
          q.onerror = (event) => { event.preventDefault(); failed++; };
        } catch { failed++; }
      }
      await new Promise((res, rej) => { tx.oncomplete = res; tx.onerror = (event) => event.preventDefault(); tx.onabort = () => rej(tx.error); }); }
    db.close();
  }
  return { localStorageKeys: Object.keys(data.localStorage || {}).length, idbRecords: records, failedRecords: failed };
})()`;
