/** Page scripts for the semantic checkpoint. They run in the page's main
 * world because site storage belongs to it.
 *
 * Supported: localStorage strings, and IndexedDB databases whose object
 * stores, indexes and records survive a JSON round trip (records that do not
 * — Blob, Date, typed arrays, cyclic values — are counted and skipped).
 * Not saved: sessionStorage, page memory, form drafts, history, HTTP cache,
 * service workers, Cache Storage, OPFS. */

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

export const COLLECT = `(async () => {
  const req = (r) => new Promise((res, rej) => { r.onsuccess = () => res(r.result); r.onerror = () => rej(r.error); });
  const same = (v) => { try { const s = JSON.stringify(v); return s !== undefined && JSON.stringify(JSON.parse(s)) === s; } catch { return false; } };
  const ls = {}; for (let i = 0; i < localStorage.length; i++) { const k = localStorage.key(i); ls[k] = localStorage.getItem(k); }
  const dbs = []; let nonJson = 0;
  for (const info of (indexedDB.databases ? await indexedDB.databases() : [])) {
    if (!info.name) continue;
    const db = await req(indexedDB.open(info.name));
    const stores = [];
    for (const name of db.objectStoreNames) {
      const st = db.transaction(name).objectStore(name);
      const keys = await req(st.getAllKeys()); const values = await req(st.getAll());
      const records = [];
      keys.forEach((k, i) => { const v = values[i]; if (same(k) && same(v)) records.push({ k, v }); else nonJson++; });
      stores.push({ name, keyPath: st.keyPath, autoIncrement: st.autoIncrement,
        indexes: [...st.indexNames].map((n) => { const ix = st.index(n); return { name: n, keyPath: ix.keyPath, unique: ix.unique, multiEntry: ix.multiEntry }; }),
        records });
    }
    dbs.push({ name: info.name, version: db.version, stores }); db.close();
  }
  return { localStorage: ls, indexedDB: dbs, nonJsonValues: nonJson, sessionStorageKeys: sessionStorage.length };
})()`;

export const RESTORE = (data: OriginData) => `(async () => {
  const data = ${JSON.stringify(data)};
  const req = (r) => new Promise((res, rej) => { r.onsuccess = () => res(r.result); r.onerror = () => rej(r.error); r.onblocked = () => rej(new Error('blocked')); });
  localStorage.clear(); for (const [k, v] of Object.entries(data.localStorage || {})) localStorage.setItem(k, v);
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
      for (const r of s.records) { s.keyPath == null ? st.put(r.v, r.k) : st.put(r.v); records++; }
      await new Promise((res, rej) => { tx.oncomplete = res; tx.onerror = () => rej(tx.error); }); }
    db.close();
  }
  return { localStorageKeys: Object.keys(data.localStorage || {}).length, idbRecords: records };
})()`;
