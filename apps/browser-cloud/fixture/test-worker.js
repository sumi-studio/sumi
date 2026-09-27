// TEST CONFIGURATION ONLY (wrangler.test.jsonc). The product Worker plus a
// synthetic fixture website that the remote browser reaches only through
// Browser Run's outboundByHost (Cloud) or Chrome host-resolver-rules pointed
// at wrangler dev (local, LOCAL_POOL set). No real site, no real account.
//   http://app.sumi-fixture.test  sign-in cookie, localStorage, IndexedDB,
//                                 memo form (Japanese input), pager (goals),
//                                 slow order (uncertain effect)
//   http://docs.sumi-fixture.test second origin: localStorage, long page
import { DurableObject, WorkerEntrypoint } from "cloudflare:workers";
import worker from "../src/worker.ts";

export { ProfileBrowser } from "../src/worker.ts";

const APP = "app.sumi-fixture.test";
const DOCS = "docs.sumi-fixture.test";

export class FixtureState extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS sessions (sid TEXT PRIMARY KEY, name TEXT, created INTEGER);
      CREATE TABLE IF NOT EXISTS orders (token TEXT PRIMARY KEY, item TEXT, created INTEGER);
      CREATE TABLE IF NOT EXISTS order_attempts (n INTEGER PRIMARY KEY AUTOINCREMENT, token TEXT, accepted INTEGER, at INTEGER);
      CREATE TABLE IF NOT EXISTS memos (n INTEGER PRIMARY KEY AUTOINCREMENT, sid TEXT, text TEXT, at INTEGER)`);
  }
  login(name) {
    const sid = crypto.randomUUID();
    this.ctx.storage.sql.exec("INSERT INTO sessions VALUES (?,?,?)", sid, name, Date.now());
    return sid;
  }
  who(sid) {
    if (!sid) return null;
    return this.ctx.storage.sql.exec("SELECT name FROM sessions WHERE sid=?", sid).toArray()[0]?.name ?? null;
  }
  revoke(sid) {
    this.ctx.storage.sql.exec("DELETE FROM sessions WHERE sid=?", sid);
  }
  placeOrder(token, item) {
    const dup = this.ctx.storage.sql.exec("SELECT 1 FROM orders WHERE token=?", token).toArray().length > 0;
    this.ctx.storage.sql.exec("INSERT INTO order_attempts (token, accepted, at) VALUES (?,?,?)", token, dup ? 0 : 1, Date.now());
    if (!dup) this.ctx.storage.sql.exec("INSERT INTO orders VALUES (?,?,?)", token, item, Date.now());
    return !dup;
  }
  saveMemo(sid, text) {
    this.ctx.storage.sql.exec("INSERT INTO memos (sid, text, at) VALUES (?,?,?)", sid, text, Date.now());
  }
  lastMemo(sid) {
    return this.ctx.storage.sql.exec("SELECT text FROM memos WHERE sid=? ORDER BY n DESC LIMIT 1", sid).toArray()[0]?.text ?? null;
  }
  report() {
    return {
      orders: this.ctx.storage.sql.exec("SELECT token, item FROM orders ORDER BY created").toArray(),
      attempts: this.ctx.storage.sql.exec("SELECT token, accepted FROM order_attempts ORDER BY n").toArray(),
      memos: this.ctx.storage.sql.exec("SELECT text FROM memos ORDER BY n").toArray().map((r) => r.text),
    };
  }
}

const escape = (s) => String(s).replace(/[&<>"']/g, (c) => `&#${c.charCodeAt(0)};`);
const cookie = (req, name) =>
  (req.headers.get("cookie") || "")
    .split(/;\s*/)
    .map((p) => p.split("="))
    .find(([k]) => k === name)?.[1];

const html = (title, body, headers = {}) =>
  new Response(
    `<!doctype html><html lang="ja"><head><meta charset="utf-8"><title>${title}</title>
<style>body{font:16px system-ui,sans-serif;margin:24px;background:#f7f7f5;color:#222}
button,input,textarea{font:inherit;padding:6px 10px;margin:4px 0}button{cursor:pointer}
.box{background:#fff;border:1px solid #ccc;border-radius:6px;padding:12px 16px;margin:12px 0;max-width:720px}
#mem{font-weight:bold}.tag{display:inline-block;background:#dde;padding:2px 6px;border-radius:4px}</style></head>
<body>${body}</body></html>`,
    { headers: { "content-type": "text/html; charset=utf-8", "cache-control": "no-store", ...headers } },
  );

function appPage(name) {
  if (!name)
    return html(
      "Fixture app – sign in",
      `<h1>Fixture app</h1>
<div class="box"><p id="auth">Not signed in</p>
<form method="post" action="/login"><label>Name <input name="name" id="name" value="Synthetic Ada"></label><br>
<label>Password <input name="password" id="password" type="password"></label><br>
<button id="signin" type="submit">Sign in</button></form></div>`,
    );
  return html(
    "Fixture app – dashboard",
    `<h1>Fixture app</h1>
<div class="box"><p id="auth">Signed in as <b id="user">${escape(name)}</b></p>
<a id="memo-link" href="/memo">Memo</a> · <a id="pager-link" href="/pager?p=1">Pager</a> · <a id="new-order-link" href="/order">New order</a> ·
<form method="post" action="/logout" style="display:inline"><button id="signout">Sign out</button></form></div>
<div class="box"><h2>Saved preferences</h2>
<p>localStorage theme: <span class="tag" id="ls-theme">(none)</span></p>
<p>IndexedDB note: <span class="tag" id="idb-note">(none)</span></p>
<button id="theme-dark">Use dark theme</button> <button id="theme-light">Use light theme</button>
<button id="save-note">Save note to IndexedDB</button></div>
<div class="box"><h2>Page memory</h2>
<p>In-memory counter (not saved anywhere): <span id="mem">0</span></p>
<button id="count">Count +1</button>
<p>Clicks seen by page: <span id="clicks">0</span>; last target: <span id="last-click">-</span></p></div>
<script>
window.__mem = 0;
document.getElementById('count').onclick = () => { window.__mem++; document.getElementById('mem').textContent = window.__mem; };
let clicks = 0;
document.addEventListener('click', (e) => { clicks++; document.getElementById('clicks').textContent = clicks;
  document.getElementById('last-click').textContent = (e.target.id || e.target.tagName) + (e.isTrusted ? ' (trusted)' : ' (synthetic)'); }, true);
const show = () => { document.getElementById('ls-theme').textContent = localStorage.getItem('fx_theme') || '(none)';
  document.body.style.background = localStorage.getItem('fx_theme') === 'dark' ? '#333' : '#f7f7f5';
  document.body.style.color = localStorage.getItem('fx_theme') === 'dark' ? '#eee' : '#222'; };
show();
document.getElementById('theme-dark').onclick = () => { localStorage.setItem('fx_theme', 'dark'); show(); };
document.getElementById('theme-light').onclick = () => { localStorage.setItem('fx_theme', 'light'); show(); };
function db() { return new Promise((res, rej) => { const r = indexedDB.open('fx-db', 1);
  r.onupgradeneeded = () => r.result.createObjectStore('notes', { keyPath: 'id' }); r.onsuccess = () => res(r.result); r.onerror = () => rej(r.error); }); }
async function loadNote() { const d = await db(); const tx = d.transaction('notes'); const g = tx.objectStore('notes').get('n1');
  g.onsuccess = () => { document.getElementById('idb-note').textContent = g.result ? g.result.text : '(none)'; }; }
document.getElementById('save-note').onclick = async () => { const d = await db(); const tx = d.transaction('notes', 'readwrite');
  tx.objectStore('notes').put({ id: 'n1', text: 'fixture note v1' }); tx.oncomplete = loadNote; };
loadNote();
</script>`,
  );
}

function memoPage(last) {
  return html(
    "Fixture app – memo",
    `<h1>Memo</h1>
<div class="box"><p>Saved memo: <span class="tag" id="saved">${last ? escape(last) : "(none)"}</span></p>
<form method="post" action="/memo"><label for="memo">Memo</label><br>
<textarea id="memo" name="memo" rows="3" cols="40"></textarea><br>
<button id="save-memo" type="submit">Save memo</button></form>
<p>Composition events seen: <span id="compositions">0</span></p></div>
<script>let n = 0; document.getElementById('memo').addEventListener('compositionend', () => { n++; document.getElementById('compositions').textContent = n; });</script>`,
  );
}

function pagerPage(p) {
  return html(
    `Pager – Page ${p}`,
    `<h1 id="page">Page ${p}</h1>
<div class="box"><p>Press Next to go to the following page.</p>
<a id="next" href="/pager?p=${p + 1}" role="button">Next</a> · <a id="home" href="/">Home</a></div>`,
  );
}

function orderPage(token) {
  return html(
    "Fixture app – new order",
    `<h1>New order</h1>
<div class="box"><p>Synthetic order form. Submitting records one order on the fixture server.</p>
<form method="post" action="/order"><input type="hidden" name="token" value="${token}">
<label>Item <input id="item" name="item" value="Synthetic notebook"></label><br>
<button id="place-order" type="submit">Place order</button></form></div>`,
  );
}

function docsPage() {
  const sections = Array.from(
    { length: 40 },
    (_, i) => `<div class="box" id="s${i + 1}"><h2>Section ${i + 1}</h2><p>Synthetic documentation paragraph ${i + 1}.</p></div>`,
  ).join("");
  return html(
    "Fixture docs",
    `<h1>Fixture docs</h1>
<p>localStorage docs_last: <span class="tag" id="ls-last">(none)</span></p>
<button id="mark">Mark section read</button>
<script>
document.getElementById('ls-last').textContent = localStorage.getItem('docs_last') || '(none)';
document.getElementById('mark').onclick = () => { localStorage.setItem('docs_last', 'read v1');
  document.getElementById('ls-last').textContent = localStorage.getItem('docs_last'); };
</script>${sections}`,
  );
}

export class FixtureSite extends WorkerEntrypoint {
  async fetch(request) {
    const url = new URL(request.url);
    const state = this.env.FIXTURE_STATE.get(this.env.FIXTURE_STATE.idFromName("site"));
    if (url.hostname === DOCS) return url.pathname === "/" ? docsPage() : new Response("not found", { status: 404 });
    if (url.hostname !== APP) return new Response("unknown fixture host", { status: 404 });
    const sid = cookie(request, "fx_session");
    const name = await state.who(sid);
    if (request.method === "POST" && url.pathname === "/login") {
      const form = await request.formData();
      const newSid = await state.login(String(form.get("name") || "Synthetic Ada").slice(0, 40));
      return new Response(null, {
        status: 303,
        headers: { location: "/", "set-cookie": `fx_session=${newSid}; Path=/; HttpOnly; SameSite=Lax; Max-Age=3600` },
      });
    }
    if (request.method === "POST" && url.pathname === "/logout") {
      if (sid) await state.revoke(sid);
      return new Response(null, { status: 303, headers: { location: "/", "set-cookie": "fx_session=; Path=/; Max-Age=0" } });
    }
    if (url.pathname === "/") return appPage(name);
    if (url.pathname === "/pager") return pagerPage(Math.max(1, Math.min(999, Number(url.searchParams.get("p")) || 1)));
    if (!name) return new Response(null, { status: 303, headers: { location: "/" } });
    if (url.pathname === "/memo" && request.method === "GET") return memoPage(await state.lastMemo(sid));
    if (url.pathname === "/memo" && request.method === "POST") {
      const form = await request.formData();
      await state.saveMemo(sid, String(form.get("memo") ?? "").slice(0, 400));
      return new Response(null, { status: 303, headers: { location: "/memo" } });
    }
    if (url.pathname === "/order" && request.method === "GET") return orderPage(crypto.randomUUID());
    // The same order as a link (a navigation the secretary can perform).
    if (url.pathname === "/order/place" && request.method === "GET") {
      const accepted = await state.placeOrder(String(url.searchParams.get("token")).slice(0, 80), String(url.searchParams.get("item") ?? "").slice(0, 60));
      await new Promise((r) => setTimeout(r, 6000));
      return html("Order result", `<h1 id="result">${accepted ? "Order placed" : "Duplicate order rejected"}</h1><a href="/">Home</a>`);
    }
    if (url.pathname === "/order" && request.method === "POST") {
      const form = await request.formData();
      const accepted = await state.placeOrder(String(form.get("token")), String(form.get("item")).slice(0, 60));
      // Commit first, answer slowly: a browser lost in this window leaves
      // the client unsure whether the order happened.
      await new Promise((r) => setTimeout(r, 6000));
      return html("Order result", `<h1 id="result">${accepted ? "Order placed" : "Duplicate order rejected"}</h1><a href="/">Home</a>`);
    }
    return new Response("not found", { status: 404 });
  }
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    // Local stand-in only: Chrome's host-resolver-rules send fixture hosts here.
    if (env.LOCAL_POOL && /\.sumi-fixture\.test$/.test(url.hostname)) return env.FIXTURE.fetch(request);
    // Test harness only, bearer-protected: the fixture site's server-side
    // record (orders, memos) to check effects without driving the browser.
    if (url.pathname === "/__fixture/report") {
      const token = env.SUMI_BROWSER_CLOUD_TOKEN ?? "";
      if (!token || request.headers.get("authorization") !== `Bearer ${token}`)
        return new Response("unauthorized", { status: 401 });
      const state = env.FIXTURE_STATE.get(env.FIXTURE_STATE.idFromName("site"));
      return Response.json(await state.report());
    }
    return worker.fetch(request, env, ctx);
  },
};
