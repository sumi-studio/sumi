// Command first-model is the first real-model development server: it mounts
// the persona-scoped agentstate contract for a local Node/workerd core and a
// scoped browser surface for one persona's conversation. It exists so the
// real-model slice is usable end to end at a local URL — it is engineering
// test UX, not approved product UX.
//
// Two credential scopes, deliberately disjoint:
//
//   - SUMI_CORE_STATE_TOKEN (admin) and derived core_<hmac> persona tokens
//     authorize the /internal/core/* routes the secretary core uses;
//   - fm_<hmac> browser tokens authorize ONLY the /fm/{persona}/* surface —
//     submit a human message, read the outbox/journal/state. A browser token
//     cannot acquire a writer lease, save plans, or claim operations, and
//     the page never sees the persona's core_ token.
//
// fm_<token> = "fm_" + hex(HMAC-SHA256(adminSecret, "fm:"+personaID)) — one
// persona, browser surface only.
//
// This binary binds loopback by default. The /internal/core/* routes and the
// fm surface are a development surface; do not expose them on a public
// interface — production browser auth lands with the auth-flow milestone.
//
// Env:
//
//	SUMI_DB_URL            postgres connection string (required)
//	SUMI_CORE_STATE_TOKEN  admin/service bearer (required, >=16 chars)
//	SUMI_FM_LISTEN         listen address (default 127.0.0.1:8470)
//	SUMI_FM_PERSONA_ID     reuse this persona (uuidv7) across restarts;
//	                       unset → a fresh persona is created each boot
//	SUMI_FM_PERSONA_NAME   display name when creating (default "first model")
//	SUMI_WORKSPACE_ROOT   Local filesvc workspace root (with persona subdirectory)
//	SUMI_LOCAL_TERMINAL_ROOT durable Local PTY identity/output journal directory
//	SUMI_LOCAL_WORKING_STORE cloud disables Local PTYs even if roots are present
package main

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/fileaccess"
)

var uuidv7Re = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

type fmServer struct {
	store  *agentstate.Store
	secret []byte
	// files is the install's configured file service client (local
	// filesvc, or a returned secretary's scoped Cloud storage proxy —
	// fileaccess.FromEnv makes the distinction invisible here). nil
	// leaves the file routes unmounted.
	files *fileaccess.Client
}

// fmToken derives the browser-scoped capability for one persona.
func (s *fmServer) fmToken(personaID string) string {
	mac := hmac.New(sha256.New, s.secret)
	mac.Write([]byte("fm:" + personaID))
	return "fm_" + hex.EncodeToString(mac.Sum(nil))
}

// fmAuthorized accepts the fm_ token for this persona or the admin secret —
// never a core_ persona token, so holding a browser token grants no
// internal-route access and vice versa.
func (s *fmServer) fmAuthorized(r *http.Request, personaID string) bool {
	const prefix = "Bearer "
	h := r.Header.Get("Authorization")
	if len(h) <= len(prefix) || h[:len(prefix)] != prefix {
		return false
	}
	tok := h[len(prefix):]
	if subtle.ConstantTimeCompare([]byte(tok), s.secret) == 1 {
		return true
	}
	return subtle.ConstantTimeCompare([]byte(tok), []byte(s.fmToken(personaID))) == 1
}

func (s *fmServer) scope(w http.ResponseWriter, r *http.Request) (string, bool) {
	personaID := r.PathValue("persona")
	if !uuidv7Re.MatchString(personaID) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "persona must be a uuidv7"})
		return "", false
	}
	if !s.fmAuthorized(r, personaID) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "unauthorized"})
		return "", false
	}
	return personaID, true
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// submitMessage records a human message input — the only write the browser
// surface can perform.
func (s *fmServer) submitMessage(w http.ResponseWriter, r *http.Request) {
	personaID, ok := s.scope(w, r)
	if !ok {
		return
	}
	var body struct {
		Text    string `json:"text"`
		InputID string `json:"input_id"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "bad json"})
		return
	}
	if body.Text == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "text is required"})
		return
	}
	if body.InputID == "" {
		body.InputID = "fm-" + randHex(12)
	}
	in, _, err := s.store.SubmitInput(r.Context(), &agentstate.Input{
		PersonaID:     personaID,
		InputID:       body.InputID,
		Kind:          "message",
		Payload:       map[string]any{"text": body.Text},
		ActorKind:     "human",
		ActorID:       "fm-user",
		SourceSurface: "first-model",
	})
	if err != nil {
		writeJSON(w, submitErrorStatus(err), map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"input": in})
}

// submitErrorStatus maps SubmitInput failures: caller problems stay 4xx —
// a malformed request is 400, a replayed input_id carrying a different
// request is a 409 conflict — while genuine internal failures stay 500.
func submitErrorStatus(err error) int {
	switch {
	case errors.Is(err, agentstate.ErrBadRequest):
		return http.StatusBadRequest
	case errors.Is(err, agentstate.ErrTurnConflict):
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}

func (s *fmServer) outbox(w http.ResponseWriter, r *http.Request) {
	personaID, ok := s.scope(w, r)
	if !ok {
		return
	}
	entries, err := s.store.Outbox(r.Context(), personaID, afterSeq(r), 500)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"outbox": entries})
}

func (s *fmServer) events(w http.ResponseWriter, r *http.Request) {
	personaID, ok := s.scope(w, r)
	if !ok {
		return
	}
	evs, err := s.store.Events(r.Context(), personaID, afterSeq(r), 500)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": evs})
}

func (s *fmServer) state(w http.ResponseWriter, r *http.Request) {
	personaID, ok := s.scope(w, r)
	if !ok {
		return
	}
	st, err := s.store.PersonaState(r.Context(), personaID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"state": st})
}

func afterSeq(r *http.Request) int64 {
	n, _ := strconv.ParseInt(r.URL.Query().Get("after_seq"), 10, 64)
	return n
}

// fmFileOps is the person-facing file surface's op set — the same
// delegated operations every file surface uses. The scope is derived
// from the authorized persona, never from a request parameter.
var fmFileOps = map[string]string{
	"list":   http.MethodGet,
	"stat":   http.MethodGet,
	"read":   http.MethodGet,
	"write":  http.MethodPut,
	"mkdir":  http.MethodPost,
	"remove": http.MethodDelete,
}

// fmFileParams is the query vocabulary the file surface forwards — path
// selection, paging and ranges only; nothing caller-supplied ever names
// a scope or an upstream credential.
var fmFileParams = map[string]bool{
	"path": true, "limit": true, "cursor": true,
	"offset": true, "len": true,
}

// files serves /fm/{persona}/files/{op}: the person's access to the same
// working file set the secretary uses — the local file store, or the
// Cloud store a cloud-mode return kept. The fm token's persona binding
// is what scopes the operation; the upstream answer streams back with
// its own verdicts (version, external-change) intact.
func (s *fmServer) serveFiles(w http.ResponseWriter, r *http.Request) {
	personaID, ok := s.scope(w, r)
	if !ok {
		return
	}
	op := r.PathValue("op")
	want, known := fmFileOps[op]
	if !known || r.Method != want {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "unknown file op"})
		return
	}
	scope, err := fileaccess.ScopeForPersona(personaID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	q := url.Values{}
	for k := range fmFileParams {
		if v := r.URL.Query().Get(k); v != "" {
			q.Set(k, v)
		}
	}
	headers := http.Header{}
	for _, h := range []string{"If-Version", "X-Idempotency-Key", "Content-Type"} {
		if v := r.Header.Get(h); v != "" {
			headers.Set(h, v)
		}
	}
	var body io.Reader
	if r.Method == http.MethodPut {
		// A person-surface write is bounded independently of the store's
		// own ceiling — the body streams, it is never assembled here.
		body = http.MaxBytesReader(w, r.Body, 32<<20)
	}
	resp, err := s.files.ProxyOp(r.Context(), scope, op, r.Method, q, headers, body)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{
			"error": "file service unreachable; safe to retry",
		})
		return
	}
	defer resp.Body.Close()
	for _, h := range []string{"Content-Type", "Content-Length", "X-File-Version", "X-External-Change"} {
		if v := resp.Header.Get(h); v != "" {
			w.Header().Set(h, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

func main() {
	databaseURL := os.Getenv("SUMI_DB_URL")
	if databaseURL == "" {
		log.Fatal("SUMI_DB_URL is required")
	}
	token := os.Getenv("SUMI_CORE_STATE_TOKEN")
	if len(token) < 16 {
		log.Fatal("SUMI_CORE_STATE_TOKEN is required (>=16 chars)")
	}
	listen := os.Getenv("SUMI_FM_LISTEN")
	if listen == "" {
		listen = "127.0.0.1:8470"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := db.Open(ctx, databaseURL)
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	defer pool.Close()
	if err := db.Migrate(ctx, pool.Pool); err != nil {
		log.Fatalf("migrate: %v", err)
	}
	store := agentstate.NewStore(pool.Pool)

	// One development persona per boot unless SUMI_FM_PERSONA_ID pins one —
	// pinning is what lets an ordinary restart continue the same recorded
	// conversation.
	personaID := os.Getenv("SUMI_FM_PERSONA_ID")
	if personaID == "" {
		id, err := uuid.NewV7()
		if err != nil {
			log.Fatalf("persona id: %v", err)
		}
		personaID = id.String()
	}
	name := os.Getenv("SUMI_FM_PERSONA_NAME")
	if name == "" {
		name = "first model"
	}
	if _, _, err := store.EnsurePersona(ctx, personaID, nil, name); err != nil {
		log.Fatalf("ensure persona: %v", err)
	}

	core := agentstate.NewServer(pool.Pool, token)

	// The install's file service: a local filesvc when this install owns
	// the working store, or the scoped Cloud storage proxy a cloud-mode
	// return configured — fileaccess.FromEnv makes the two identical
	// here. The secretary's file.* effects and the person's /fm files
	// surface share the one client, so both faces reach the same store.
	filesClient, err := fileaccess.FromEnv(os.Getenv)
	if err != nil {
		log.Fatalf("file service config: %v", err)
	}
	fm := &fmServer{store: store, secret: []byte(token), files: filesClient}
	if filesClient != nil {
		for tool, effect := range fileaccess.FileEffects(filesClient) {
			if err := core.RegisterToolEffect(tool, effect); err != nil {
				log.Fatalf("register file effect %s: %v", tool, err)
			}
		}
		core.SetJobFileService(fileaccess.JobFileService(filesClient))
		log.Print("file service configured (file.* effects and /fm files surface armed)")
	}

	mux := http.NewServeMux()
	core.RegisterRoutes(mux)
	stopTerminal, err := wireLocalTerminal(core, fm, mux, personaID, "http://"+listen)
	if err != nil {
		log.Fatalf("Local terminal: %v", err)
	}
	defer stopTerminal()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
	})
	// Scoped browser surface — fm_ token only.
	mux.HandleFunc("POST /fm/{persona}/inputs", fm.submitMessage)
	mux.HandleFunc("GET /fm/{persona}/outbox", fm.outbox)
	mux.HandleFunc("GET /fm/{persona}/events", fm.events)
	mux.HandleFunc("GET /fm/{persona}/state", fm.state)
	if fm.files != nil {
		mux.HandleFunc("GET /fm/{persona}/files/{op}", fm.serveFiles)
		mux.HandleFunc("PUT /fm/{persona}/files/{op}", fm.serveFiles)
		mux.HandleFunc("POST /fm/{persona}/files/{op}", fm.serveFiles)
		mux.HandleFunc("DELETE /fm/{persona}/files/{op}", fm.serveFiles)
	}
	mux.HandleFunc("GET /", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(uiHTML))
	})

	browserToken := fm.fmToken(personaID)
	coreToken := core.PersonaToken(personaID)
	log.Printf("first-model listening on http://%s", listen)
	log.Printf("persona: %s", personaID)
	log.Printf("browser UI: http://%s/?persona=%s&fm=%s", listen, personaID, browserToken)
	log.Printf("core env:   SUMI_STATE_URL=http://%s SUMI_PERSONA_ID=%s SUMI_PERSONA_TOKEN=%s", listen, personaID, coreToken)
	log.Fatal(http.ListenAndServe(listen, mux))
}

const uiHTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Sumi — first model (engineering surface)</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  :root { color-scheme: light dark; }
  body { font: 15px/1.5 system-ui, sans-serif; max-width: 720px; margin: 2rem auto; padding: 0 1rem; }
  h1 { font-size: 1.2rem; }
  #log { border: 1px solid #8884; border-radius: 8px; padding: 1rem; min-height: 12rem; }
  .msg { margin: .4rem 0; }
  .you { color: #58a; }
  .sumi { color: #4a4; }
  .fail { color: #c55; }
  .act { color: #999; font-size: .85em; }
  form { display: flex; gap: .5rem; margin-top: 1rem; }
  input[type=text] { flex: 1; padding: .5rem; font: inherit; }
  button { padding: .5rem 1rem; font: inherit; }
  #status { color: #999; font-size: .85em; margin-top: .5rem; }
</style>
</head>
<body>
<h1>Sumi — first model <span class="act">(engineering surface, not product UX)</span></h1>
<div id="log"></div>
<form id="f"><input id="t" type="text" autocomplete="off" placeholder="Say something to your secretary…"><button>Send</button></form>
<div id="status"></div>
<section id="files" style="margin-top:1.5rem; display:none">
  <h2 style="font-size:1rem">Files <span class="act">(same store the secretary uses)</span></h2>
  <div style="display:flex;gap:.5rem;margin:.4rem 0">
    <input id="fpath" type="text" placeholder="path (blank lists the workspace root)" style="flex:1">
    <button id="flist" type="button">List</button>
    <button id="fread" type="button">Read</button>
  </div>
  <pre id="fout" style="border:1px solid #8884;border-radius:8px;padding:.75rem;min-height:4rem;max-height:16rem;overflow:auto;white-space:pre-wrap"></pre>
  <div style="display:flex;gap:.5rem">
    <input id="fwpath" type="text" placeholder="path to write" style="flex:1">
    <input id="fwtext" type="text" placeholder="text content" style="flex:2">
    <button id="fwrite" type="button">Write</button>
  </div>
</section>
<script>
const q = new URLSearchParams(location.search);
const persona = q.get("persona") || localStorage.fmPersona;
const fm = q.get("fm") || localStorage.fmToken;
if (persona && fm) { localStorage.fmPersona = persona; localStorage.fmToken = fm; }
const log = document.getElementById("log"), status = document.getElementById("status");
function line(cls, text) {
  const d = document.createElement("div");
  d.className = "msg " + cls;
  d.textContent = text;
  log.appendChild(d);
  log.scrollTop = log.scrollHeight;
}
if (!persona || !fm) {
  line("fail", "Missing persona/fm token — open the URL printed by the first-model server.");
} else {
  const headers = { Authorization: "Bearer " + fm, "Content-Type": "application/json" };
  let outboxSeq = 0, eventSeq = 0;
  const seenInputs = new Set();
  async function poll() {
    try {
      const [ob, ev, st] = await Promise.all([
        fetch("/fm/" + persona + "/outbox?after_seq=" + outboxSeq, { headers }).then(r => r.json()),
        fetch("/fm/" + persona + "/events?after_seq=" + eventSeq, { headers }).then(r => r.json()),
        fetch("/fm/" + persona + "/state", { headers }).then(r => r.json()),
      ]);
      for (const e of ev.events || []) {
        eventSeq = Math.max(eventSeq, e.seq);
        if (e.kind === "input_received" && e.payload.actor_kind === "human") {
          if (!seenInputs.has(e.payload.input_id)) {
            seenInputs.add(e.payload.input_id);
            line("you", "you: " + (e.payload.text ?? ""));
          }
        } else if (e.kind === "tool_call") {
          line("act", "→ " + e.payload.tool + " " + JSON.stringify(e.payload.request ?? {}));
        } else if (e.kind === "tool_result") {
          line("act", "← " + e.payload.tool + " " + (e.payload.error ? "error: " + e.payload.error : JSON.stringify(e.payload.response ?? {})));
        } else if (e.kind === "note") {
          line("act", "note: " + (e.payload.text ?? ""));
        }
      }
      for (const o of ob.outbox || []) {
        outboxSeq = Math.max(outboxSeq, o.seq);
        if (o.kind === "turn_completed") {
          const t = (o.payload.output && o.payload.output.text) || "";
          line("sumi", "sumi: " + (t || "(empty reply)"));
        } else if (o.kind === "turn_failed") {
          line("fail", "request failed: " + (o.payload.error ?? "unknown"));
        }
      }
      const s = st.state || {};
      status.textContent = "queued:" + (s.queued_inputs ?? "?") +
        " running:" + (s.running_turn ? "yes" : "no") +
        " pending schedules:" + (s.pending_schedules ?? "?");
    } catch (e) { status.textContent = "poll error: " + e; }
    setTimeout(poll, 800);
  }
  document.getElementById("f").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    const t = document.getElementById("t");
    if (!t.value.trim()) return;
    const r = await fetch("/fm/" + persona + "/inputs", {
      method: "POST", headers, body: JSON.stringify({ text: t.value }),
    });
    if (!r.ok) line("fail", "send failed: " + r.status);
    t.value = "";
  });
  // Files pane: the person's window on the same store the secretary's
  // file.* tools use. Hidden unless the surface answers — an install
  // without a configured file service shows no broken pane.
  const fsec = document.getElementById("files"), fout = document.getElementById("fout");
  fetch("/fm/" + persona + "/files/list?limit=1", { headers }).then(r => {
    if (!r.ok) return;
    fsec.style.display = "block";
  }).catch(() => {});
  document.getElementById("flist").addEventListener("click", async () => {
    const p = document.getElementById("fpath").value.trim();
    const r = await fetch("/fm/" + persona + "/files/list?path=" + encodeURIComponent(p) + "&limit=1000", { headers });
    const j = await r.json().catch(() => null);
    fout.textContent = r.ok
      ? (j.entries || []).map(e => e.kind.padEnd(7) + " " + e.name).join("\n") || "(empty)"
      : "list failed: " + (j && j.error ? j.error : r.status);
  });
  document.getElementById("fread").addEventListener("click", async () => {
    const p = document.getElementById("fpath").value.trim();
    const r = await fetch("/fm/" + persona + "/files/read?path=" + encodeURIComponent(p), { headers });
    fout.textContent = r.ok ? await r.text() : "read failed: " + r.status;
  });
  document.getElementById("fwrite").addEventListener("click", async () => {
    const p = document.getElementById("fwpath").value.trim();
    if (!p) return;
    const r = await fetch("/fm/" + persona + "/files/write?path=" + encodeURIComponent(p), {
      method: "PUT", headers, body: document.getElementById("fwtext").value,
    });
    fout.textContent = r.ok ? "wrote " + p : "write failed: " + r.status;
  });
  poll();
}
</script>
</body>
</html>
`
