/**
 * sumi-fabric-obj — a minimal S3-compatible object-store facade backed by a
 * Durable Object (SQLite storage). It exists so the JuiceFS canonical-volume
 * prototype can store its object bytes in real Cloudflare storage while R2 is
 * not enabled on this account. When R2 is enabled this shim is retired and
 * JuiceFS points at R2 directly; nothing else in the design depends on it.
 *
 * Scope and honest limits:
 * - Single-part PUT/GET/HEAD/DELETE, ListObjects v1+v2, CopyObject, and
 *   DeleteObjects are implemented — the surface JuiceFS's S3 backend uses.
 *   Multipart upload is NOT implemented (JuiceFS writes 4 MiB blocks as single
 *   PUTs; add it only if a real client requires it).
 * - Auth: requests must carry an AWS4-HMAC-SHA256 Authorization header whose
 *   Credential access-key id equals the S3_ACCESS_KEY secret. The signature
 *   itself is not verified — the access key id functions as a bearer secret.
 *   This is a validation-plane simplification, not a production auth design.
 * - Each bucket maps to one DO via idFromName(bucket); all objects for that
 *   bucket are serialized through it. Fine for validation; a real store shards.
 */

interface DOStorageSql {
  exec(
    query: string,
    ...bindings: unknown[]
  ): {
    toArray(): Record<string, unknown>[];
    one(): Record<string, unknown>;
  };
}

interface DOStorage {
  sql: DOStorageSql;
  transactionSync<T>(fn: () => T): T;
}

interface DOState {
  storage: DOStorage;
}

interface NamespaceBinding {
  idFromName(name: string): unknown;
  get(id: unknown): { fetch(req: Request): Promise<Response> };
}

interface EnvLike {
  BUCKET: NamespaceBinding;
  S3_ACCESS_KEY?: string;
  [key: string]: unknown;
}

const CHUNK = 512 * 1024; // bytes per SQLite row; stays under any value-size cap

function xmlEscape(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function s3Error(code: string, message: string, status: number): Response {
  const body =
    `<?xml version="1.0" encoding="UTF-8"?>` +
    `<Error><Code>${code}</Code><Message>${xmlEscape(message)}</Message></Error>`;
  return new Response(body, {
    status,
    headers: { "content-type": "application/xml" },
  });
}

function authorized(req: Request, env: EnvLike): boolean {
  const want = env.S3_ACCESS_KEY;
  if (!want) return false; // fail closed when the secret is not configured
  const auth = req.headers.get("authorization") ?? "";
  const m = /Credential=([^,/]+)/.exec(auth);
  return m?.[1] === want;
}

export default {
  async fetch(req: Request, env: EnvLike): Promise<Response> {
    const url = new URL(req.url);
    if (url.pathname === "/healthz") return new Response("ok");
    if (!authorized(req, env))
      return s3Error("AccessDenied", "unknown access key", 403);

    // Strip a leading slash, then bucket[/key...]. Keys may contain any bytes;
    // keep the raw tail and let URL decoding handle escapes once.
    const path = url.pathname.replace(/^\/+/, "");
    const slash = path.indexOf("/");
    const bucket = slash === -1 ? path : path.slice(0, slash);
    const key = slash === -1 ? "" : decodeURIComponent(path.slice(slash + 1));
    if (!bucket) {
      // GET / — ListBuckets. The shim cannot enumerate DO names; return the
      // fixed bucket this deployment serves.
      const body =
        `<?xml version="1.0" encoding="UTF-8"?>` +
        `<ListAllMyBucketsResult><Buckets/></ListAllMyBucketsResult>`;
      return new Response(body, {
        headers: { "content-type": "application/xml" },
      });
    }
    const id = env.BUCKET.idFromName(bucket);
    const stub = env.BUCKET.get(id);
    const inner = new URL(req.url);
    inner.searchParams.set("__bucket", bucket);
    inner.searchParams.set("__key", key);
    return stub.fetch(new Request(inner.toString(), req));
  },
};

export class BucketObject {
  private state: DOState;
  private ready = false;

  constructor(state: DOState) {
    this.state = state;
  }

  private ensure() {
    if (this.ready) return;
    this.state.storage.sql.exec(
      `CREATE TABLE IF NOT EXISTS objects (
         key TEXT PRIMARY KEY, size INTEGER NOT NULL,
         etag TEXT NOT NULL, mtime TEXT NOT NULL)`,
    );
    this.state.storage.sql.exec(
      `CREATE TABLE IF NOT EXISTS chunks (
         key TEXT NOT NULL, seq INTEGER NOT NULL, data BLOB NOT NULL,
         PRIMARY KEY (key, seq))`,
    );
    this.state.storage.sql.exec(
      `CREATE INDEX IF NOT EXISTS chunks_key ON chunks(key)`,
    );
    this.ready = true;
  }

  async fetch(req: Request): Promise<Response> {
    this.ensure();
    const url = new URL(req.url);
    const key = url.searchParams.get("__key") ?? "";
    const method = req.method;

    try {
      if (!key) return this.bucketOp(method, url, req);
      switch (method) {
        case "PUT": {
          const copySource = req.headers.get("x-amz-copy-source");
          if (copySource) return this.copyObject(copySource, key);
          return this.putObject(key, req);
        }
        case "GET":
          return this.getObject(key, req.headers.get("range"));
        case "HEAD":
          return this.headObject(key);
        case "DELETE":
          this.state.storage.transactionSync(() => {
            this.state.storage.sql.exec(
              "DELETE FROM objects WHERE key = ?",
              key,
            );
            this.state.storage.sql.exec(
              "DELETE FROM chunks WHERE key = ?",
              key,
            );
          });
          return new Response(null, { status: 204 });
        case "POST":
          if (url.searchParams.has("delete")) return this.deleteObjects(req);
          return s3Error("NotImplemented", "unsupported POST", 501);
        default:
          return s3Error("NotImplemented", `unsupported ${method}`, 501);
      }
    } catch (err) {
      return s3Error(
        "InternalError",
        err instanceof Error ? err.message : String(err),
        500,
      );
    }
  }

  private bucketOp(method: string, url: URL, req: Request): Promise<Response> | Response {
    if (method === "PUT") return new Response(null, { status: 200 }); // create
    if (method === "HEAD") return new Response(null, { status: 200 });
    if (method === "GET") {
      const isV2 = url.searchParams.get("list-type") === "2";
      return this.list(url, isV2);
    }
    if (method === "POST" && url.searchParams.has("delete"))
      return this.deleteObjects(req);
    return s3Error("NotImplemented", `unsupported bucket ${method}`, 501);
  }

  private async putObject(key: string, req: Request): Promise<Response> {
    const body = new Uint8Array(await req.arrayBuffer());
    const etag = `"${body.length.toString(16)}-${Date.now().toString(16)}"`;
    const mtime = new Date().toISOString();
    this.state.storage.transactionSync(() => {
      this.state.storage.sql.exec("DELETE FROM chunks WHERE key = ?", key);
      for (let off = 0, seq = 0; off < body.length || seq === 0; seq++) {
        const part = body.subarray(off, off + CHUNK);
        this.state.storage.sql.exec(
          "INSERT INTO chunks (key, seq, data) VALUES (?, ?, ?)",
          key,
          seq,
          part,
        );
        off += CHUNK;
        if (off >= body.length) break;
      }
      this.state.storage.sql.exec(
        `INSERT INTO objects (key, size, etag, mtime) VALUES (?, ?, ?, ?)
         ON CONFLICT(key) DO UPDATE SET size=excluded.size,
           etag=excluded.etag, mtime=excluded.mtime`,
        key,
        body.length,
        etag,
        mtime,
      );
    });
    return new Response(null, { status: 200, headers: { etag } });
  }

  private getObject(key: string, range: string | null): Response {
    const meta = this.state.storage.sql
      .exec("SELECT size, etag, mtime FROM objects WHERE key = ?", key)
      .toArray()[0];
    if (!meta) return s3Error("NoSuchKey", key, 404);
    const size = Number(meta.size);
    let start = 0;
    let end = size - 1;
    if (range) {
      const m = /^bytes=(\d*)-(\d*)$/.exec(range.trim());
      if (m) {
        if (m[1] === "" && m[2] !== "") {
          start = Math.max(0, size - Number(m[2]));
        } else {
          start = Number(m[1] || 0);
          if (m[2] !== "") end = Math.min(end, Number(m[2]));
        }
      }
      if (start > end) return s3Error("InvalidRange", range, 416);
    }
    const firstSeq = Math.floor(start / CHUNK);
    const lastSeq = Math.floor(end / CHUNK);
    const rows = this.state.storage.sql
      .exec(
        "SELECT seq, data FROM chunks WHERE key = ? AND seq BETWEEN ? AND ? ORDER BY seq",
        key,
        firstSeq,
        lastSeq,
      )
      .toArray();
    const parts: Uint8Array[] = [];
    for (const row of rows) parts.push(new Uint8Array(row.data as ArrayBuffer));
    const total = parts.reduce((n, p) => n + p.length, 0);
    const buf = new Uint8Array(total);
    let at = 0;
    for (const p of parts) {
      buf.set(p, at);
      at += p.length;
    }
    const slice = buf.subarray(start - firstSeq * CHUNK, end - firstSeq * CHUNK + 1);
    const headers: Record<string, string> = {
      etag: String(meta.etag),
      // HTTP-date (IMF-fixdate) — AWS SDKs reject ISO 8601 in this header.
      "last-modified": new Date(String(meta.mtime)).toUTCString(),
      "content-length": String(slice.length),
      "accept-ranges": "bytes",
    };
    if (range) {
      headers["content-range"] = `bytes ${start}-${end}/${size}`;
      return new Response(slice, { status: 206, headers });
    }
    return new Response(slice, { status: 200, headers });
  }

  private headObject(key: string): Response {
    const meta = this.state.storage.sql
      .exec("SELECT size, etag, mtime FROM objects WHERE key = ?", key)
      .toArray()[0];
    if (!meta) return s3Error("NoSuchKey", key, 404);
    return new Response(null, {
      status: 200,
      headers: {
        etag: String(meta.etag),
        "last-modified": new Date(String(meta.mtime)).toUTCString(),
        "content-length": String(meta.size),
        "accept-ranges": "bytes",
      },
    });
  }

  private list(url: URL, v2: boolean): Response {
    const prefix = url.searchParams.get("prefix") ?? "";
    const delimiter = url.searchParams.get("delimiter") ?? "";
    const maxKeys = Math.min(
      Number(url.searchParams.get("max-keys") || 1000),
      1000,
    );
    const marker = v2
      ? (url.searchParams.get("continuation-token") ?? "")
      : (url.searchParams.get("marker") ?? "");
    // Over-fetch then fold by delimiter so CommonPrefixes count correctly.
    const rows = this.state.storage.sql
      .exec(
        `SELECT key, size, etag, mtime FROM objects
         WHERE key > ? AND substr(key, 1, ?) = ?
         ORDER BY key LIMIT ?`,
        marker,
        prefix.length,
        prefix,
        maxKeys * 4 + 100,
      )
      .toArray();
    const contents: string[] = [];
    const common: string[] = [];
    let count = 0;
    let truncated = false;
    let lastKey = "";
    for (const row of rows) {
      const k = String(row.key);
      let commonPrefix: string | null = null;
      if (delimiter) {
        const rest = k.slice(prefix.length);
        const d = rest.indexOf(delimiter);
        if (d !== -1) commonPrefix = prefix + rest.slice(0, d) + delimiter;
      }
      if (commonPrefix !== null) {
        if (common[common.length - 1] !== commonPrefix) {
          if (count >= maxKeys) {
            truncated = true;
            break;
          }
          common.push(commonPrefix);
          count++;
        }
        continue;
      }
      if (count >= maxKeys) {
        truncated = true;
        break;
      }
      contents.push(
        `<Contents><Key>${xmlEscape(k)}</Key>` +
          `<LastModified>${xmlEscape(String(row.mtime))}</LastModified>` +
          `<ETag>${xmlEscape(String(row.etag))}</ETag>` +
          `<Size>${Number(row.size)}</Size></Contents>`,
      );
      lastKey = k;
      count++;
    }
    truncated = truncated || rows.length > 0 && false;
    const name = xmlEscape(url.searchParams.get("__bucket") ?? "");
    const cp = common
      .map((p) => `<CommonPrefixes><Prefix>${xmlEscape(p)}</Prefix></CommonPrefixes>`)
      .join("");
    const body = v2
      ? `<?xml version="1.0" encoding="UTF-8"?>` +
        `<ListBucketResult><Name>${name}</Name><Prefix>${xmlEscape(prefix)}</Prefix>` +
        `<KeyCount>${count}</KeyCount><MaxKeys>${maxKeys}</MaxKeys>` +
        `<IsTruncated>${truncated}</IsTruncated>${contents.join("")}${cp}` +
        (truncated
          ? `<NextContinuationToken>${xmlEscape(lastKey)}</NextContinuationToken>`
          : "") +
        `</ListBucketResult>`
      : `<?xml version="1.0" encoding="UTF-8"?>` +
        `<ListBucketResult><Name>${name}</Name><Prefix>${xmlEscape(prefix)}</Prefix>` +
        `<Marker>${xmlEscape(marker)}</Marker><MaxKeys>${maxKeys}</MaxKeys>` +
        `<IsTruncated>${truncated}</IsTruncated>${contents.join("")}${cp}` +
        `</ListBucketResult>`;
    return new Response(body, {
      headers: { "content-type": "application/xml" },
    });
  }

  private copyObject(copySource: string, destKey: string): Response {
    // x-amz-copy-source: /bucket/key or bucket/key, URL-encoded possibly.
    let src = decodeURIComponent(copySource).replace(/^\/+/, "");
    const slash = src.indexOf("/");
    if (slash !== -1) src = src.slice(slash + 1); // same-bucket copy assumed
    const meta = this.state.storage.sql
      .exec("SELECT size, etag FROM objects WHERE key = ?", src)
      .toArray()[0];
    if (!meta) return s3Error("NoSuchKey", src, 404);
    const mtime = new Date().toISOString();
    this.state.storage.transactionSync(() => {
      this.state.storage.sql.exec("DELETE FROM chunks WHERE key = ?", destKey);
      this.state.storage.sql.exec(
        `INSERT INTO chunks (key, seq, data)
         SELECT ?, seq, data FROM chunks WHERE key = ?`,
        destKey,
        src,
      );
      this.state.storage.sql.exec(
        `INSERT INTO objects (key, size, etag, mtime) VALUES (?, ?, ?, ?)
         ON CONFLICT(key) DO UPDATE SET size=excluded.size,
           etag=excluded.etag, mtime=excluded.mtime`,
        destKey,
        meta.size,
        `"copy-${Date.now().toString(16)}"`,
        mtime,
      );
    });
    const body =
      `<?xml version="1.0" encoding="UTF-8"?>` +
      `<CopyObjectResult><LastModified>${mtime}</LastModified>` +
      `<ETag>${xmlEscape(String(meta.etag))}</ETag></CopyObjectResult>`;
    return new Response(body, {
      headers: { "content-type": "application/xml" },
    });
  }

  private async deleteObjects(req: Request): Promise<Response> {
    const text = await req.text();
    const keys = [...text.matchAll(/<Key>([^<]*)<\/Key>/g)].map((m) =>
      (m[1] ?? "").replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&amp;/g, "&"),
    );
    this.state.storage.transactionSync(() => {
      for (const key of keys) {
        this.state.storage.sql.exec("DELETE FROM objects WHERE key = ?", key);
        this.state.storage.sql.exec("DELETE FROM chunks WHERE key = ?", key);
      }
    });
    const deleted = keys.map((k) => `<Deleted><Key>${xmlEscape(k)}</Key></Deleted>`);
    return new Response(
      `<?xml version="1.0" encoding="UTF-8"?><DeleteResult>${deleted.join("")}</DeleteResult>`,
      { headers: { "content-type": "application/xml" } },
    );
  }
}
