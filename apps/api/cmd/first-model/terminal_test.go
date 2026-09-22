//go:build linux

package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

const localPersona = "0198f0f4-9b72-7000-8000-000000000341"
const foreignPersona = "0198f0f4-9b72-7000-8000-000000000342"

type terminalFixture struct {
	t         *testing.T
	server    *httptest.Server
	core      *agentstate.Server
	fm        *fmServer
	cookie    *http.Cookie
	workspace string
}

func newTerminalFixture(t *testing.T) *terminalFixture {
	t.Helper()
	pool := testdb.Create(t)
	ctx := context.Background()
	if e := db.Migrate(ctx, pool); e != nil {
		t.Fatal(e)
	}
	core := agentstate.NewServer(pool, "local-terminal-test-admin-secret-32-bytes")
	for _, id := range []string{localPersona, foreignPersona} {
		if _, _, e := core.Store().EnsurePersona(ctx, id, nil, "Local"); e != nil {
			t.Fatal(e)
		}
	}
	fm := &fmServer{store: core.Store(), secret: []byte("local-terminal-test-admin-secret-32-bytes")}
	mux := http.NewServeMux()
	core.RegisterRoutes(mux)
	mux.HandleFunc("POST /fm/{persona}/inputs", fm.submitMessage)
	server := httptest.NewUnstartedServer(mux)
	root := t.TempDir()
	workspace := filepath.Join(root, "workspace")
	t.Setenv("SUMI_WORKSPACE_ROOT", workspace)
	t.Setenv("SUMI_LOCAL_TERMINAL_ROOT", filepath.Join(root, "terminals"))
	stop, e := wireLocalTerminal(core, fm, mux, localPersona, "http://"+server.Listener.Addr().String())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(stop)
	server.Start()
	t.Cleanup(server.Close)
	f := &terminalFixture{t: t, server: server, core: core, fm: fm, workspace: workspace}
	resp := f.request("POST", "/fm/"+localPersona+"/terminal-session", nil, fm.fmToken(localPersona))
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("bootstrap %d %s", resp.StatusCode, b)
	}
	for _, c := range resp.Cookies() {
		if c.Name == agentevents.BrowserSessionCookie {
			f.cookie = c
		}
	}
	if f.cookie == nil || !f.cookie.HttpOnly || f.cookie.Path != "/terminal" || f.cookie.SameSite != http.SameSiteStrictMode || f.cookie.MaxAge != 3600 {
		t.Fatal("terminal cookie missing or wrong scope")
	}
	return f
}
func (f *terminalFixture) request(method, path string, body any, token string) *http.Response {
	f.t.Helper()
	raw, _ := json.Marshal(body)
	req, e := http.NewRequest(method, f.server.URL+path, bytes.NewReader(raw))
	if e != nil {
		f.t.Fatal(e)
	}
	req.Header.Set("Origin", f.server.URL)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if f.cookie != nil {
		req.AddCookie(f.cookie)
	}
	resp, e := http.DefaultClient.Do(req)
	if e != nil {
		f.t.Fatal(e)
	}
	return resp
}
func (f *terminalFixture) path(route string) string {
	return "/terminal/" + route + "?installation_id=" + localPersona + "&authority_epoch=1"
}
func (f *terminalFixture) open() agentstate.TerminalSession {
	f.t.Helper()
	resp := f.request("POST", f.path("open"), map[string]any{"name": "Shared Local shell"}, "")
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		f.t.Fatalf("open %d %s", resp.StatusCode, b)
	}
	var out struct {
		Session agentstate.TerminalSession `json:"session"`
	}
	if e := json.NewDecoder(resp.Body).Decode(&out); e != nil {
		f.t.Fatal(e)
	}
	return f.waitStatus(out.Session.SessionID, "active")
}
func (f *terminalFixture) waitStatus(id, status string) agentstate.TerminalSession {
	f.t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	var got agentstate.TerminalSession
	for time.Now().Before(deadline) {
		var e error
		got, e = f.core.Store().GetTerminalSession(context.Background(), localPersona, id)
		if e != nil {
			f.t.Fatal(e)
		}
		if got.Status == status {
			return got
		}
		time.Sleep(20 * time.Millisecond)
	}
	f.t.Fatalf("wanted %s: %+v", status, got)
	return got
}
func (f *terminalFixture) coreCall(tool string, request map[string]any) string {
	f.t.Helper()
	resp := f.request("POST", "/fm/"+localPersona+"/inputs", map[string]any{"text": "Local terminal acceptance: " + tool}, f.fm.fmToken(localPersona))
	resp.Body.Close()
	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		f.t.Fatal("input", resp.StatusCode)
	}
	raw, _ := json.Marshal(map[string]any{"tool": tool, "request": request})
	script, e := filepath.Abs("../../../core/scripts/local-terminal-acceptance-child.mjs")
	if e != nil {
		f.t.Fatal(e)
	}
	cmd := exec.Command("node", script)
	cmd.Env = append(os.Environ(), "TERMINAL_TEST_URL="+f.server.URL, "TERMINAL_TEST_PERSONA="+localPersona, "TERMINAL_TEST_TOKEN="+f.core.PersonaToken(localPersona), "TERMINAL_TEST_CALL="+string(raw))
	out, e := cmd.CombinedOutput()
	if e != nil {
		f.t.Fatalf("Core child %v\n%s", e, out)
	}
	return string(out)
}

type attach struct {
	conn   *websocket.Conn
	mu     sync.Mutex
	text   string
	frames []map[string]any
	done   chan struct{}
}

func (f *terminalFixture) attach(id string, cursor int64) *attach {
	f.t.Helper()
	header := http.Header{"Origin": {f.server.URL}, "Cookie": {f.cookie.String()}}
	url := "ws" + strings.TrimPrefix(f.server.URL, "http") + f.path("ws") + "&session_id=" + id + "&cursor=" + strconv.FormatInt(cursor, 10)
	conn, resp, e := websocket.DefaultDialer.Dial(url, header)
	if e != nil {
		if resp != nil {
			f.t.Fatalf("attach HTTP %d: %v", resp.StatusCode, e)
		}
		f.t.Fatal(e)
	}
	a := &attach{conn: conn, done: make(chan struct{})}
	f.t.Cleanup(func() { conn.Close(); <-a.done })
	go func() {
		defer close(a.done)
		for {
			var frame map[string]any
			if conn.ReadJSON(&frame) != nil {
				return
			}
			a.mu.Lock()
			a.frames = append(a.frames, frame)
			if frame["type"] == "output" {
				b, _ := base64.StdEncoding.DecodeString(frame["data"].(string))
				a.text += string(b)
			}
			a.mu.Unlock()
		}
	}()
	return a
}
func (a *attach) send(t *testing.T, v map[string]any) {
	t.Helper()
	if e := a.conn.WriteJSON(v); e != nil {
		t.Fatal(e)
	}
}
func (a *attach) stdin(t *testing.T, data string) {
	a.send(t, map[string]any{"type": "stdin", "data": base64.StdEncoding.EncodeToString([]byte(data))})
}
func (a *attach) wait(t *testing.T, want string) {
	t.Helper()
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		a.mu.Lock()
		text := a.text
		a.mu.Unlock()
		if strings.Contains(text, want) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	t.Fatalf("attach missing %q: %s", want, a.text)
}
func TestLocalHostCoreAndHumanShareActualPTY(t *testing.T) {
	f := newTerminalFixture(t)
	resp := f.request("POST", "/fm/"+localPersona+"/terminal-session", nil, f.core.PersonaToken(localPersona))
	resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatal("core token granted browser authority", resp.StatusCode)
	}
	resp = f.request("POST", "/fm/"+foreignPersona+"/terminal-session", nil, f.fm.fmToken(foreignPersona))
	resp.Body.Close()
	if resp.StatusCode != 403 {
		t.Fatal("foreign persona bootstrap", resp.StatusCode)
	}
	badHeaders := http.Header{"Origin": {"https://foreign.example"}, "Cookie": {f.cookie.String()}}
	_, rejected, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(f.server.URL, "http")+f.path("ws"), badHeaders)
	if err == nil || rejected == nil || rejected.StatusCode != 403 {
		t.Fatal("foreign origin accepted", rejected, err)
	}
	rejected.Body.Close()
	session := f.open()
	socket := f.attach(session.SessionID, 0)
	socket.stdin(t, "stty -echo; LOCAL_SHARED=still-here; printf 'human-file' > shared.txt; printf '\\nhuman-ran\\n'\n")
	socket.wait(t, "\r\nhuman-ran\r\n")
	f.coreCall("terminal.write", map[string]any{"session_id": session.SessionID, "data": "printf '+secretary' >> shared.txt; printf '\\nsecretary-ran\\n'\n"})
	socket.wait(t, "\r\nsecretary-ran\r\n")
	if b, e := os.ReadFile(filepath.Join(f.workspace, localPersona, "shared.txt")); e != nil || string(b) != "human-file+secretary" {
		t.Fatal("shared workspace", string(b), e)
	}
	inputs, e := f.core.Store().ListTerminalInputs(context.Background(), localPersona, session.SessionID, 0, 100)
	if e != nil {
		t.Fatal(e)
	}
	sources := map[string]bool{}
	for _, in := range inputs {
		if in.Status == "written" {
			sources[in.Source] = true
		}
	}
	if !sources["human"] || !sources["agent"] {
		t.Fatal("shared delivery ledger", inputs)
	}
	f.coreCall("terminal.write", map[string]any{"session_id": session.SessionID, "data": "false; printf '\nfailed-command-status=%s\n' \"$?\"; cat shared.txt; printf '\n'\n"})
	socket.wait(t, "failed-command-status=1\r\n")
	socket.stdin(t, "printf '\nafter-failure:%s\n' \"$LOCAL_SHARED\"; cat shared.txt; printf '\n'\n")
	socket.wait(t, "after-failure:still-here\r\nhuman-file+secretary\r\n")
	// A browser detach does not own the shell lifetime or mint another session.
	socket.conn.Close()
	<-socket.done
	again := f.attach(session.SessionID, 0)
	again.stdin(t, "printf '\\n%s\\n' \"$LOCAL_SHARED\"\n")
	again.wait(t, "\r\nstill-here\r\n")
	again.send(t, map[string]any{"type": "resize", "cols": 101, "rows": 31})
	again.stdin(t, "stty size\n")
	again.wait(t, "31 101\r\n")
	again.stdin(t, "sleep 30\n")
	time.Sleep(300 * time.Millisecond)
	again.send(t, map[string]any{"type": "signal", "signal": "INT"})
	again.stdin(t, "printf '\\nsignal-returned\\n'\n")
	again.wait(t, "\r\nsignal-returned\r\n")
	foreign, e := f.core.Store().CreateTerminalSession(context.Background(), foreignPersona, "foreign", "human", "test")
	if e != nil {
		t.Fatal(e)
	}
	resp = f.request("GET", f.path("session")+"&session_id="+foreign.SessionID, nil, "")
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatal("foreign session exposed", resp.StatusCode)
	}
	f.coreCall("terminal.write", map[string]any{"session_id": session.SessionID, "data": "exit 7\n"})
	ended := f.waitStatus(session.SessionID, "ended")
	if ended.ExitCode == nil || *ended.ExitCode != 7 {
		t.Fatal("exit outcome", ended)
	}
	// A secretary-created later shell sees the same files, not a new scratch area.
	f.coreCall("terminal.open", map[string]any{"name": "Second Local shell"})
	sessions, e := f.core.Store().ListTerminalSessions(context.Background(), localPersona)
	if e != nil || len(sessions) != 2 {
		t.Fatal("sessions", sessions, e)
	}
	var next agentstate.TerminalSession
	for _, s := range sessions {
		if s.SessionID != session.SessionID {
			next = f.waitStatus(s.SessionID, "active")
		}
	}
	other := f.attach(next.SessionID, 0)
	other.stdin(t, "stty -echo; printf '\\n'; cat shared.txt; printf '\\n'\n")
	other.wait(t, "\r\nhuman-file+secretary\r\n")
	resp = f.request("POST", f.path("close"), map[string]any{"session_id": next.SessionID}, "")
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatal("close", resp.StatusCode)
	}
	f.waitStatus(next.SessionID, "ended")
	t.Log("actual first-model wiring + Local fm capability bootstrap + existing HTTP/WS attach + actual TypeScript Core + PostgreSQL input/output ledger + shared Linux PTY verified")
}

func TestLocalTerminalCookieExpiry(t *testing.T) {
	a := &localTerminalAuthority{persona: localPersona, sessions: map[string]terminalSession{}}
	cookie, e := a.issue()
	if e != nil {
		t.Fatal(e)
	}
	claims, e := a.VerifySession(context.Background(), cookie)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = a.VerifySession(context.Background(), "invented-cookie"); e == nil {
		t.Fatal("unissued cookie accepted")
	}
	a.mu.Lock()
	session := a.sessions[claims.UserID]
	session.expires = time.Now().Add(-time.Second)
	a.sessions[claims.UserID] = session
	a.mu.Unlock()
	if _, e = a.VerifySession(context.Background(), cookie); e == nil {
		t.Fatal("expired cookie accepted")
	}
	called := false
	if e = a.AuthorizeSession(context.Background(), claims, func() error { called = true; return nil }); e == nil || called {
		t.Fatal("expired claim executed")
	}
	restarted := &localTerminalAuthority{persona: localPersona, sessions: map[string]terminalSession{}}
	if _, e = restarted.VerifySession(context.Background(), cookie); e == nil {
		t.Fatal("cookie survived host restart")
	}
}

func TestLocalTerminalCloudStorageUnavailable(t *testing.T) {
	t.Setenv("SUMI_WORKSPACE_ROOT", "")
	t.Setenv("SUMI_LOCAL_TERMINAL_ROOT", "")
	t.Setenv("SUMI_LOCAL_WORKING_STORE", "cloud")
	fm := &fmServer{secret: []byte("local-terminal-test-admin-secret-32-bytes")}
	mux := http.NewServeMux()
	stop, e := wireLocalTerminal(nil, fm, mux, localPersona, "http://localhost")
	if e != nil {
		t.Fatal(e)
	}
	defer stop()
	request := httptest.NewRequest("POST", "/fm/"+localPersona+"/terminal-session", nil)
	request.Header.Set("Authorization", "Bearer "+fm.fmToken(localPersona))
	result := httptest.NewRecorder()
	mux.ServeHTTP(result, request)
	if result.Code != 503 || !strings.Contains(result.Body.String(), `"working_store":"cloud"`) || !strings.Contains(result.Body.String(), "local_terminal_unavailable") {
		t.Fatal(result.Code, result.Body.String())
	}
}
