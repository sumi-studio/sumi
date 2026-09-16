package main

// F235: the bundle upload must not hang forever on a silent connection, and
// must not give up on a slow one that is still moving. Both bounds are
// exercised against an owned endpoint with a short test configuration.

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
)

func answerBound(m *mover, d time.Duration) {
	m.client.Transport.(*http.Transport).ResponseHeaderTimeout = d
	m.answer = d
}

// A Cloud that swallows the whole bundle and then says nothing ends the
// attempt within the answer bound. The seal is untouched, the export ledger
// still holds exactly one sealed transfer, and a later resume finishes the
// same move — no re-seal, no second export row.
func TestUploadStopsOnASilentCloudAndStaysResumable(t *testing.T) {
	c := setupMove(t)
	uid := "fixture-silent-" + c.pid[24:]
	sid, moveURL := c.newSession(uid)

	quiet := make(chan struct{})
	t.Cleanup(func() { close(quiet) })
	attempts := make(chan struct{}, 16)
	c.setIntercept(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method != http.MethodPut {
			return false
		}
		attempts <- struct{}{}
		_, _ = io.Copy(io.Discard, r.Body)
		<-quiet
		return true
	})

	m, out := c.mover()
	answerBound(m, 500*time.Millisecond)
	started := time.Now()
	code := m.Start(c.ctx, moveURL)
	took := time.Since(started)
	if code != exitPending {
		t.Fatalf("start against a silent Cloud: %d\n%s", code, out)
	}
	if !strings.Contains(out.String(), "stays sealed") {
		t.Fatalf("output does not say where the secretary stands:\n%s", out)
	}
	if took > 30*time.Second {
		t.Fatalf("a silent Cloud held the command for %s", took)
	}
	if len(attempts) == 0 {
		t.Fatal("the upload never reached the endpoint")
	}
	if a := authority(t, c.local, c.pid); a != "sealed" {
		t.Fatalf("authority after the silent upload: %s", a)
	}
	if n := exportRows(t, c.local, c.pid); n != 1 {
		t.Fatalf("export rows after the silent upload: %d", n)
	}
	if s := c.sessionStatus(sid); s != "awaiting_bundle" {
		t.Fatalf("session after the silent upload: %s", s)
	}
	out.Reset()

	// Cloud answers again: the same sealed bundle goes through, with no
	// second seal.
	c.setIntercept(nil)
	answerBound(m, answerTimeout)
	expect(t, m.Resume(c.ctx), exitPending, out, "waiting for the registration")
	if strings.Contains(out.String(), "Sealing") {
		t.Fatalf("the resume re-sealed the secretary:\n%s", out)
	}
	if n := exportRows(t, c.local, c.pid); n != 1 {
		t.Fatalf("export rows after the resume: %d", n)
	}
	c.provision(uid, sid)
	expect(t, m.Resume(c.ctx), exitDone, out, "Choose a model connection")
	if l, d := authority(t, c.local, c.pid), authority(t, c.dest, c.pid); l != "transferred" || d != "active" {
		t.Fatalf("after completion: local %s, cloud %s", l, d)
	}
}

// A Cloud that stops reading part way through a bundle ends the attempt on
// the silence bound rather than blocking on the socket for good. Whether the
// client's writes stop being accepted must not depend on the host's socket
// autotuning — a kernel that can hold the whole bundle makes the attempt a
// legitimate answer wait instead of a stall — so this case runs the traffic
// through sockets whose buffers are deliberately tiny at both ends.
func TestUploadStopsWhenTheConnectionStopsAcceptingBytes(t *testing.T) {
	c := setupMove(t)
	uid := "fixture-stall-" + c.pid[24:]
	sid, moveURL := c.newSession(uid)
	fillSecretary(t, c, 400, 4096)

	// The same dispatch as c.srv, on a listener whose accepted sockets can
	// hold only a few KB: a handler that stops reading is then a real stall,
	// not the receive window quietly absorbing the bundle.
	tight := httptest.NewUnstartedServer(c.srv.Config.Handler)
	tight.Config.ConnContext = func(ctx context.Context, conn net.Conn) context.Context {
		if tcp, ok := conn.(*net.TCPConn); ok {
			_ = tcp.SetReadBuffer(4096)
		}
		return ctx
	}
	tight.Start()
	t.Cleanup(tight.Close)
	moveURL = strings.Replace(moveURL, c.srv.URL, tight.URL, 1)

	quiet := make(chan struct{})
	t.Cleanup(func() { close(quiet) })
	c.setIntercept(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method != http.MethodPut {
			return false
		}
		// Take a little and then stop reading, the way a connection that
		// silently goes away does. A cancelled attempt releases the handler
		// with its request instead of parking until the test ends.
		_, _ = io.CopyN(io.Discard, r.Body, 1024)
		select {
		case <-quiet:
		case <-r.Context().Done():
		}
		return true
	})

	m, out := c.mover()
	m.stall = 300 * time.Millisecond
	// Keep the client's own send queue small too, or it can hold the whole
	// bundle locally while the peer's window is shut. The short answer bound
	// is the loud fallback: a platform whose buffers still swallow the bundle
	// ends each attempt on the answer bound and fails the stall assertion
	// below fast, rather than retrying the production answer wait for ten
	// minutes.
	answerBound(m, 300*time.Millisecond)
	tr := m.client.Transport.(*http.Transport)
	dial := tr.DialContext
	tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := dial(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		if tcp, ok := conn.(*net.TCPConn); ok {
			_ = tcp.SetWriteBuffer(4096)
		}
		return conn, nil
	}
	started := time.Now()
	code := m.Start(c.ctx, moveURL)
	took := time.Since(started)
	if code != exitPending {
		t.Fatalf("start against a connection that stops reading: %d\n%s", code, out)
	}
	if !strings.Contains(out.String(), "stopped moving data") || !strings.Contains(out.String(), "stays sealed") {
		t.Fatalf("output does not explain the stall:\n%s", out)
	}
	if took > 30*time.Second {
		t.Fatalf("a stalled connection held the command for %s", took)
	}
	if a := authority(t, c.local, c.pid); a != "sealed" {
		t.Fatalf("authority after the stalled upload: %s", a)
	}
	if n := exportRows(t, c.local, c.pid); n != 1 {
		t.Fatalf("export rows after the stalled upload: %d", n)
	}
	if s := c.sessionStatus(sid); s != "awaiting_bundle" {
		t.Fatalf("session after the stalled upload: %s", s)
	}
	out.Reset()

	c.setIntercept(nil)
	// Recovery runs over a healthy transport: a fresh command with the
	// production socket buffers and silence bounds, continuing the same
	// recorded move. The stall conditions above exist to inject the fault,
	// not to measure a working upload — under a loaded CI machine a
	// few-KB window can keep a healthy peer's byte gaps above a 300ms
	// test bound.
	m2, out2 := c.mover()
	expect(t, m2.Resume(c.ctx), exitPending, out2, "waiting for the registration")
	if n := exportRows(t, c.local, c.pid); n != 1 {
		t.Fatalf("export rows after the resume: %d", n)
	}
	c.provision(uid, sid)
	expect(t, m2.Resume(c.ctx), exitDone, out2, "Choose a model connection")
}

// A Cloud that swallows the bundle, answers the response headers and then
// goes silent is the same failure as one that never answers: the wait is
// bounded, the seal untouched, and a later resume finishes the same move.
func TestUploadStopsWhenTheAnswerStallsAfterHeaders(t *testing.T) {
	c := setupMove(t)
	uid := "fixture-ansilent-" + c.pid[24:]
	sid, moveURL := c.newSession(uid)

	quiet := make(chan struct{})
	t.Cleanup(func() { close(quiet) })
	attempts := make(chan struct{}, 16)
	c.setIntercept(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method != http.MethodPut {
			return false
		}
		attempts <- struct{}{}
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-quiet
		return true
	})

	m, out := c.mover()
	answerBound(m, 300*time.Millisecond)
	started := time.Now()
	code := m.Start(c.ctx, moveURL)
	took := time.Since(started)
	if code != exitPending {
		t.Fatalf("start against a Cloud that stalls after the headers: %d\n%s", code, out)
	}
	if !strings.Contains(out.String(), "stays sealed") {
		t.Fatalf("output does not say where the secretary stands:\n%s", out)
	}
	if took > 30*time.Second {
		t.Fatalf("a half-silent Cloud held the command for %s", took)
	}
	if len(attempts) == 0 {
		t.Fatal("the upload never reached the endpoint")
	}
	if a := authority(t, c.local, c.pid); a != "sealed" {
		t.Fatalf("authority after the half-silent upload: %s", a)
	}
	if n := exportRows(t, c.local, c.pid); n != 1 {
		t.Fatalf("export rows after the half-silent upload: %d", n)
	}
	if s := c.sessionStatus(sid); s != "awaiting_bundle" {
		t.Fatalf("session after the half-silent upload: %s", s)
	}
	out.Reset()

	c.setIntercept(nil)
	answerBound(m, answerTimeout)
	expect(t, m.Resume(c.ctx), exitPending, out, "waiting for the registration")
	if n := exportRows(t, c.local, c.pid); n != 1 {
		t.Fatalf("export rows after the resume: %d", n)
	}
	c.provision(uid, sid)
	expect(t, m.Resume(c.ctx), exitDone, out, "Choose a model connection")
}

// A slow connection that keeps moving is not a stall: the upload finishes
// even though it takes many silence windows' worth of time in total.
func TestSlowButProgressingUploadIsNotCancelled(t *testing.T) {
	c := setupMove(t)
	uid := "fixture-slow-" + c.pid[24:]
	sid, moveURL := c.newSession(uid)
	fillSecretary(t, c, 60, 4096)

	c.setIntercept(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method != http.MethodPut {
			return false
		}
		// Trickle: read the bundle in small pieces with pauses shorter than
		// the silence bound, for much longer in total than that bound.
		r.Body = &slowReader{r: r.Body, chunk: 4096, pause: 30 * time.Millisecond}
		return false
	})

	m, out := c.mover()
	m.stall = 300 * time.Millisecond
	started := time.Now()
	code := m.Start(c.ctx, moveURL)
	took := time.Since(started)
	if code != exitPending {
		t.Fatalf("start over a slow link: %d\n%s", code, out)
	}
	if strings.Contains(out.String(), "stopped moving data") {
		t.Fatalf("a progressing upload was treated as a stall:\n%s", out)
	}
	if took < 3*m.stall {
		t.Fatalf("the slow upload finished in %s, which does not exercise the bound", took)
	}
	if s := c.sessionStatus(sid); s != "staged" {
		t.Fatalf("session after the slow upload: %s", s)
	}
	c.provision(uid, sid)
	expect(t, m.Resume(c.ctx), exitDone, out, "Choose a model connection")
}

type slowReader struct {
	r     io.Reader
	chunk int
	pause time.Duration
}

func (s *slowReader) Read(b []byte) (int, error) {
	if len(b) > s.chunk {
		b = b[:s.chunk]
	}
	n, err := s.r.Read(b)
	if n > 0 {
		time.Sleep(s.pause)
	}
	return n, err
}

func (s *slowReader) Close() error {
	if c, ok := s.r.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// fillSecretary gives the Local secretary enough carried state that its
// bundle cannot fit in a connection's buffers.
func fillSecretary(t *testing.T, c *cloud, inputs, size int) {
	t.Helper()
	text := strings.Repeat("あ", size/3)
	for i := 0; i < inputs; i++ {
		if _, _, err := c.local.state.SubmitInput(c.ctx, &agentstate.Input{
			PersonaID: c.pid, InputID: "in-bulk-" + strconv.Itoa(i), Kind: "message",
			Payload: map[string]any{"text": text}, ActorKind: "human", ActorID: "owner", SourceSurface: "test",
		}); err != nil {
			t.Fatal(err)
		}
	}
}
