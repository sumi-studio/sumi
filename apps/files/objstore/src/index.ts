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
 * - Auth: every request except /healthz must carry a valid AWS Signature V4
 *   Authorization header (header form only; presigned URLs are refused) for
 *   the S3_ACCESS_KEY / S3_SECRET_KEY secret pair, dated within 15 minutes.
 *   A hex x-amz-content-sha256 is checked against the body; UNSIGNED-PAYLOAD
 *   relies on TLS for body integrity. The access key id alone grants nothing
 *   (JuiceFS prints it in `juicefs status`), and the secret never travels.
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
  S3_SECRET_KEY?: string;
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

const encoder = new TextEncoder();
const MAX_CLOCK_SKEW_MS = 15 * 60 * 1000;

function toHex(buf: ArrayBuffer): string {
  return [...new Uint8Array(buf)]
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

async function sha256Hex(data: ArrayBuffer | Uint8Array): Promise<string> {
  return toHex(await crypto.subtle.digest("SHA-256", data));
}

async function hmac(
  key: ArrayBuffer | Uint8Array,
  data: string,
): Promise<ArrayBuffer> {
  const k = await crypto.subtle.importKey(
    "raw",
    key,
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  return crypto.subtle.sign("HMAC", k, encoder.encode(data));
}

// RFC 3986 encoding as the AWS SigV4 signer applies it: everything except
// unreserved characters is percent-encoded (slashes kept in paths).
function awsEncode(s: string, keepSlash: boolean): string {
  let out = "";
  for (const byte of encoder.encode(s)) {
    const c = String.fromCharCode(byte);
    if (/[A-Za-z0-9\-._~]/.test(c) || (keepSlash && c === "/")) out += c;
    else out += `%${byte.toString(16).toUpperCase().padStart(2, "0")}`;
  }
  return out;
}

function timingSafeEqual(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return diff === 0;
}

type AuthResult =
  | { ok: true; body: ArrayBuffer | null }
  | { ok: false; reason: string };

// Verifies an AWS Signature V4 header-signed request. Returns the buffered
// body (when the method carries one) so the caller can forward it.
async function verifySigV4(req: Request, env: EnvLike): Promise<AuthResult> {
  const accessKey = env.S3_ACCESS_KEY;
  const secretKey = env.S3_SECRET_KEY;
  if (!accessKey || !secretKey) return { ok: false, reason: "not configured" }; // fail closed
  const auth = req.headers.get("authorization") ?? "";
  const m =
    /^AWS4-HMAC-SHA256\s+Credential=([^/,\s]+)\/(\d{8})\/([^/,\s]+)\/s3\/aws4_request,\s*SignedHeaders=([a-z0-9;-]+),\s*Signature=([0-9a-f]{64})$/.exec(
      auth.trim(),
    );
  if (!m) return { ok: false, reason: "malformed authorization" };
  const [, keyId, scopeDate, region, signedHeaderList, signature] =
    m as unknown as string[];
  if (!timingSafeEqual(keyId ?? "", accessKey))
    return { ok: false, reason: "unknown access key" };

  const amzDate = req.headers.get("x-amz-date") ?? "";
  const dm = /^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z$/.exec(amzDate);
  if (!dm || amzDate.slice(0, 8) !== scopeDate)
    return { ok: false, reason: "bad x-amz-date" };
  const [, year = 0, month = 1, day = 0, hour = 0, minute = 0, second = 0] =
    dm.map(Number);
  const when = Date.UTC(year, month - 1, day, hour, minute, second);
  if (Math.abs(Date.now() - when) > MAX_CLOCK_SKEW_MS)
    return { ok: false, reason: "request time too skewed" };

  const signedHeaders = (signedHeaderList ?? "").split(";");
  if (!signedHeaders.includes("host") || !signedHeaders.includes("x-amz-date"))
    return { ok: false, reason: "host and x-amz-date must be signed" };

  const payloadHash = req.headers.get("x-amz-content-sha256") ?? "";
  let body: ArrayBuffer | null = null;
  if (req.method === "PUT" || req.method === "POST")
    body = await req.arrayBuffer();
  if (/^[0-9a-f]{64}$/.test(payloadHash)) {
    if ((await sha256Hex(body ?? new Uint8Array(0))) !== payloadHash)
      return { ok: false, reason: "payload hash mismatch" };
  } else if (payloadHash !== "UNSIGNED-PAYLOAD") {
    return { ok: false, reason: "unsupported x-amz-content-sha256" };
  }

  const url = new URL(req.url);
  let canonicalPath: string;
  try {
    canonicalPath = awsEncode(
      url.pathname
        .split("/")
        .map((seg) => decodeURIComponent(seg))
        .join("/"),
      true,
    );
  } catch {
    return { ok: false, reason: "malformed path" };
  }
  const query = [...url.searchParams.entries()]
    .map(([k, v]) => [awsEncode(k, false), awsEncode(v, false)] as const)
    .sort((a, b) =>
      a[0] === b[0] ? (a[1] < b[1] ? -1 : 1) : a[0] < b[0] ? -1 : 1,
    )
    .map(([k, v]) => `${k}=${v}`)
    .join("&");
  let key = await hmac(encoder.encode(`AWS4${secretKey}`), scopeDate ?? "");
  key = await hmac(key, region ?? "");
  key = await hmac(key, "s3");
  key = await hmac(key, "aws4_request");
  const credentialScope = `${scopeDate}/${region}/s3/aws4_request`;

  const signatureFor = async (
    overrides: Record<string, string>,
  ): Promise<string> => {
    const headerLines = signedHeaders.map((h) => {
      const v = overrides[h] ?? req.headers.get(h) ?? "";
      return `${h}:${v.trim().replace(/\s+/g, " ")}\n`;
    });
    const canonicalRequest = [
      req.method,
      canonicalPath,
      query,
      headerLines.join(""),
      signedHeaderList,
      payloadHash,
    ].join("\n");
    const stringToSign = [
      "AWS4-HMAC-SHA256",
      amzDate,
      credentialScope,
      await sha256Hex(encoder.encode(canonicalRequest)),
    ].join("\n");
    return toHex(await hmac(key, stringToSign));
  };

  if (timingSafeEqual(await signatureFor({}), signature ?? ""))
    return { ok: true, body };
  // The Workers runtime rewrites Accept-Encoding before the handler sees it
  // (observed: a client's signed "identity" arrives as "br, gzip"). AWS SDKs
  // send "identity" for S3; accept a signature over that value. The header
  // carries no authority, and the signature still proves the secret.
  if (
    signedHeaders.includes("accept-encoding") &&
    timingSafeEqual(
      await signatureFor({ "accept-encoding": "identity" }),
      signature ?? "",
    )
  )
    return { ok: true, body };
  return { ok: false, reason: "signature mismatch" };
}

export default {
  async fetch(req: Request, env: EnvLike): Promise<Response> {
    const url = new URL(req.url);
    if (url.pathname === "/healthz") return new Response("ok");
    const auth = await verifySigV4(req, env);
    if (!auth.ok) return s3Error("AccessDenied", auth.reason, 403);

    // Strip a leading slash, then bucket[/key...]. Keys may contain any bytes;
    // keep the raw tail and let URL decoding handle escapes once.
    const path = url.pathname.replace(/^\/+/, "");
    const slash = path.indexOf("/");
    const bucket = slash === -1 ? path : path.slice(0, slash);
    let key = "";
    if (slash !== -1) {
      try {
        key = decodeURIComponent(path.slice(slash + 1));
      } catch {
        return s3Error(
          "InvalidArgument",
          "malformed percent-encoding in key",
          400,
        );
      }
    }
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
    // The verified body was buffered; forward exactly those bytes.
    return stub.fetch(
      new Request(inner.toString(), {
        method: req.method,
        headers: req.headers,
        body: auth.body,
      }),
    );
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

  private bucketOp(
    method: string,
    url: URL,
    req: Request,
  ): Promise<Response> | Response {
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
    const slice = buf.subarray(
      start - firstSeq * CHUNK,
      end - firstSeq * CHUNK + 1,
    );
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
      ? (url.searchParams.get("continuation-token") ??
        url.searchParams.get("start-after") ??
        "")
      : (url.searchParams.get("marker") ?? "");
    // Over-fetch then fold by delimiter so CommonPrefixes count correctly.
    const fetchLimit = maxKeys * 4 + 100;
    const rows = this.state.storage.sql
      .exec(
        `SELECT key, size, etag, mtime FROM objects
         WHERE key > ? AND substr(key, 1, ?) = ?
         ORDER BY key LIMIT ?`,
        marker,
        prefix.length,
        prefix,
        fetchLimit,
      )
      .toArray();
    const contents: string[] = [];
    const common: string[] = [];
    let count = 0;
    let truncated = false;
    let lastKey = "";
    // If the resume marker sits inside a folded prefix group, that prefix
    // was already emitted on the previous page — seed `lastPrefix` so the
    // group's remaining rows fold without re-emitting a duplicate
    // CommonPrefix (operation-review B F8). The seed is only a dedup
    // marker: it must NOT appear in this page's CommonPrefixes.
    let lastPrefix = "";
    if (delimiter && marker.startsWith(prefix)) {
      const rest = marker.slice(prefix.length);
      const d = rest.indexOf(delimiter);
      if (d !== -1) lastPrefix = prefix + rest.slice(0, d) + delimiter;
    }
    // lastKey is the resume token: the last *consumed* key. Folded rows
    // count as consumed (their prefix is already emitted), so resuming
    // after them neither re-emits a straddling prefix nor skips rows.
    for (const row of rows) {
      const k = String(row.key);
      let commonPrefix: string | null = null;
      if (delimiter) {
        const rest = k.slice(prefix.length);
        const d = rest.indexOf(delimiter);
        if (d !== -1) commonPrefix = prefix + rest.slice(0, d) + delimiter;
      }
      if (commonPrefix !== null) {
        if (commonPrefix !== lastPrefix) {
          if (count >= maxKeys) {
            truncated = true;
            break;
          }
          common.push(commonPrefix);
          lastPrefix = commonPrefix;
          count++;
        }
        lastKey = k;
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
    // Hit the over-fetch limit without reaching maxKeys: there may be more.
    if (!truncated && rows.length >= fetchLimit) truncated = true;
    const name = xmlEscape(url.searchParams.get("__bucket") ?? "");
    const cp = common
      .map(
        (p) =>
          `<CommonPrefixes><Prefix>${xmlEscape(p)}</Prefix></CommonPrefixes>`,
      )
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
        (truncated ? `<NextMarker>${xmlEscape(lastKey)}</NextMarker>` : "") +
        `</ListBucketResult>`;
    return new Response(body, {
      headers: { "content-type": "application/xml" },
    });
  }

  private copyObject(copySource: string, destKey: string): Response {
    // x-amz-copy-source: /bucket/key or bucket/key, URL-encoded possibly.
    let src: string;
    try {
      src = decodeURIComponent(copySource).replace(/^\/+/, "");
    } catch {
      return s3Error(
        "InvalidArgument",
        "malformed percent-encoding in copy source",
        400,
      );
    }
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
      (m[1] ?? "")
        .replace(/&lt;/g, "<")
        .replace(/&gt;/g, ">")
        .replace(/&amp;/g, "&"),
    );
    this.state.storage.transactionSync(() => {
      for (const key of keys) {
        this.state.storage.sql.exec("DELETE FROM objects WHERE key = ?", key);
        this.state.storage.sql.exec("DELETE FROM chunks WHERE key = ?", key);
      }
    });
    const deleted = keys.map(
      (k) => `<Deleted><Key>${xmlEscape(k)}</Key></Deleted>`,
    );
    return new Response(
      `<?xml version="1.0" encoding="UTF-8"?><DeleteResult>${deleted.join("")}</DeleteResult>`,
      { headers: { "content-type": "application/xml" } },
    );
  }
}
