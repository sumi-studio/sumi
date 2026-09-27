package main

// Residual composition proof for the loss-event contract: the producer
// is the real runtimeprovision.Service tailing a real docker-json
// journal file on disk (pumpJournal → recordGap → gapsAtOrAfter), the
// transport is the real termexec.Driver, the ledger is real Postgres,
// and the consumers are the real REST /terminal/read and WS
// /terminal/ws handlers. The only scripted part is the backend below
// the provisioner: the container itself is represented by the journal
// file the supervisor actually tails — no DB row or fake consumer
// stands in for the producer seam.
//
// OpenProcessInput cannot be implemented outside runtimeprovision
// (processSink is package-private), so it is promoted from the
// embedded interface and never called: this test drives the output
// path only. Input delivery is proven by the termexec interleaving
// tests against fakeProc.

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/runtimeprovision"
	"github.com/sumi-studio/sumi/apps/api/internal/termexec"
)

// journalBackend answers the provisioner backend surface with a real
// journal file the test writes and rotates.
type journalBackend struct {
	runtimeprovision.InteractiveProcessBackend // embedded: OpenProcessInput only; never called on this output-only path
	mu                                         sync.Mutex
	journal                                    string
	launched                                   int
	stopped                                    bool
}

func (b *journalBackend) Prepare(context.Context, runtimeprovision.PrepareRequest) (runtimeprovision.PreparedEpoch, error) {
	return runtimeprovision.PreparedEpoch{}, nil
}
func (b *journalBackend) Activate(context.Context, runtimeprovision.ActivateRequest) error {
	return nil
}
func (b *journalBackend) Abort(context.Context, runtimeprovision.PreparedEpoch) (runtimeprovision.Inspection, error) {
	return runtimeprovision.Inspection{}, nil
}
func (b *journalBackend) Inspect(context.Context, string) (runtimeprovision.Inspection, error) {
	return runtimeprovision.Inspection{}, nil
}
func (b *journalBackend) Stop(context.Context, runtimeprovision.PreparedEpoch) (runtimeprovision.Inspection, error) {
	return runtimeprovision.Inspection{}, nil
}
func (b *journalBackend) Reconcile(context.Context, runtimeprovision.ReconcileRequest) (runtimeprovision.Inspection, error) {
	return runtimeprovision.Inspection{}, nil
}
func (b *journalBackend) LaunchProcess(context.Context, runtimeprovision.ProcessOperation) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.launched++
	return nil
}
func (b *journalBackend) InspectProcess(_ context.Context, o runtimeprovision.ProcessOperation) (runtimeprovision.ProcessObservation, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.launched == 0 {
		return runtimeprovision.ProcessObservation{}, nil
	}
	return runtimeprovision.ProcessObservation{
		Exists: true, Running: !b.stopped, StartedAt: time.Now().UTC(),
	}, nil
}
func (b *journalBackend) StopProcess(context.Context, runtimeprovision.ProcessOperation) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.stopped = true
	return nil
}
func (b *journalBackend) ProcessJournalPath(context.Context, runtimeprovision.ProcessOperation) (string, error) {
	return b.journal, nil
}
func (b *journalBackend) SignalProcess(context.Context, runtimeprovision.ProcessOperation, string) error {
	return nil
}
func (b *journalBackend) ResizeProcess(context.Context, runtimeprovision.ProcessOperation, int, int) error {
	return nil
}

func appendJournalLine(t *testing.T, path, s string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	fmt.Fprintf(f, `{"log":%q,"stream":"stdout","time":"2026-09-19T00:00:01Z"}`+"\n", s)
}

func readTerminalREST(t *testing.T, w *composedTerminalWorld, scope, sessionID, extra string) map[string]any {
	t.Helper()
	status, read := w.request(t, "GET",
		"/terminal/read?"+scope+"&session_id="+sessionID+extra, nil, w.cookie, nil)
	if status != http.StatusOK {
		t.Fatalf("terminal read = %d: %v", status, read)
	}
	return read
}

func hasZeroWidthGap(chunks []any, at float64) (seq float64, ok bool) {
	for _, c := range chunks {
		m := c.(map[string]any)
		if m["kind"] == "gap" && m["base"] == at {
			to, _ := m["gap_to"].(float64)
			if to == at {
				return m["seq"].(float64), true
			}
		}
	}
	return 0, false
}

func collectWS(t *testing.T, frames chan map[string]any, want func(map[string]any) bool, timeout time.Duration) map[string]any {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case f, ok := <-frames:
			if !ok {
				t.Fatalf("ws closed waiting for frame")
			}
			if want(f) {
				return f
			}
		case <-deadline:
			t.Fatalf("timed out waiting for ws frame")
		}
	}
}

// startComposedJournalWorld wires the full producer→consumer seam:
// real runtimeprovision.Service tailing a real journal file, real
// termexec driver, real agentstate/PG, real REST/WS routes. Returns
// the open session already claimed and active.
func startComposedJournalWorld(t *testing.T) (w *composedTerminalWorld, journal string, scope, sessionID string, waitFor func(string, func() bool)) {
	w = newComposedTerminalWorld(t)
	w.serve()
	ctx := context.Background()

	// Real provisioner service: durable state dir, real process store,
	// real superviseInteractive tailer over a real journal file.
	dir := t.TempDir()
	journal = filepath.Join(dir, "journal.json.log")
	appendJournalLine(t, journal, "pre-loss\r\n")
	backend := &journalBackend{journal: journal}
	// The driver always requests the persona's canonical files scope;
	// the provisioner verifies it by executing CheckPath (production:
	// sumi-files-check). The stub answers the same contract: the
	// kernel-resolved scope path under the mountpoint.
	filesRoot := filepath.Join(dir, "files")
	checkPath := filepath.Join(dir, "sumi-files-check-stub.sh")
	if err := os.WriteFile(checkPath,
		[]byte("#!/bin/sh\n# args: mountpoint volumeUUID scopeName (no --wait: CheckWaitSeconds=0)\nmkdir -p \"$1/$3\" && echo \"$1/$3\"\n"), 0755); err != nil {
		t.Fatal(err)
	}
	svc, err := runtimeprovision.NewService(backend, runtimeprovision.ServiceConfig{
		StateDirectory: dir + "/state",
		Files: runtimeprovision.FilesEnvironment{
			Mountpoint: filesRoot, VolumeUUID: "composed-test-volume",
			CheckPath: checkPath,
		},
	})
	if err != nil {
		t.Fatalf("service: %v", err)
	}
	obsCtx, obsStop := context.WithCancel(ctx)
	t.Cleanup(obsStop)
	go svc.RunProcessObserver(obsCtx)

	// Real driver on the real store, claiming the real session.
	var logs sync.Map
	drv := termexec.New(w.state, svc, nil, termexec.Config{
		RunnerID: "runner-composed", Backend: "cloud",
		Lease: 5 * time.Second, Interval: 60 * time.Millisecond,
		PollInterval: 50 * time.Millisecond, HeartbeatEvery: 2,
		UnknownWait: 3 * time.Second, CallTimeout: 2 * time.Second,
		Logf: func(format string, args ...any) {
			logs.Store(time.Now().String(), fmt.Sprintf(format, args...))
		},
	})
	dctx, dstop := context.WithCancel(ctx)
	ddone := make(chan struct{})
	go func() { drv.Run(dctx); close(ddone) }()
	t.Cleanup(func() { dstop(); <-ddone })

	installationID, epoch := w.installTerminal(t, nil)
	scope = terminalScope(installationID, epoch)
	status, opened := w.request(t, "POST", "/terminal/open?"+scope,
		map[string]any{"name": "loss"}, w.cookie, nil)
	if status != http.StatusOK {
		t.Fatalf("terminal open = %d: %v", status, opened)
	}
	sessionID = opened["session"].(map[string]any)["session_id"].(string)

	// The real driver claims → StartProcess → observer launches →
	// superviseInteractive tails the real journal → REST shows active.
	waitFor = func(what string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			if cond() {
				return
			}
			time.Sleep(30 * time.Millisecond)
		}
		logs.Range(func(k, v any) bool {
			t.Logf("driver %v: %v", k, v)
			return true
		})
		t.Fatalf("timed out waiting for %s", what)
	}
	waitFor("session active", func() bool {
		st, list := w.request(t, "GET", "/terminal/list?"+scope, nil, w.cookie, nil)
		if st != http.StatusOK {
			return false
		}
		for _, s := range list["sessions"].([]any) {
			m := s.(map[string]any)
			if m["session_id"] == sessionID {
				return m["status"] == "active"
			}
		}
		return false
	})
	return w, journal, scope, sessionID, waitFor
}

func TestTerminalComposedRealJournalLossReachesConsumers(t *testing.T) {
	w, journal, scope, sessionID, waitFor := startComposedJournalWorld(t)
	ctx := context.Background()
	_ = ctx

	// Real journal bytes reach REST at byte cursor 0.
	waitFor("pre-loss bytes on REST", func() bool {
		read := readTerminalREST(t, w, scope, sessionID, "&cursor=0")
		chunks := read["chunks"].([]any)
		for _, c := range chunks {
			if c.(map[string]any)["kind"] == "data" {
				return true
			}
		}
		return false
	})
	read := readTerminalREST(t, w, scope, sessionID, "&cursor=0")
	preEnd := int64(read["next_cursor"].(float64))
	if preEnd <= 0 {
		t.Fatalf("pre-loss next_cursor = %v", read["next_cursor"])
	}

	// Rotate the journal under the live tailer — the real pumpJournal
	// sees the inode change and journals an explicit gap, then resumes
	// on the fresh file.
	rotated := journal + ".1"
	if err := os.Rename(journal, rotated); err != nil {
		t.Fatal(err)
	}
	appendJournalLine(t, journal, "post-loss\r\n")

	// The zero-width loss marker reaches a reader already caught up at
	// the loss position (cursor=preEnd) — the exact caught-up case the
	// byte cursor alone could not serve.
	waitFor("loss marker on REST at caught-up cursor", func() bool {
		read := readTerminalREST(t, w, scope, sessionID,
			fmt.Sprintf("&cursor=%d&event_cursor=0", preEnd))
		_, ok := hasZeroWidthGap(read["chunks"].([]any), float64(preEnd))
		return ok
	})
	read = readTerminalREST(t, w, scope, sessionID,
		fmt.Sprintf("&cursor=%d&event_cursor=0", preEnd))
	chunks := read["chunks"].([]any)
	markerSeq, ok := hasZeroWidthGap(chunks, float64(preEnd))
	if !ok {
		t.Fatalf("no zero-width marker at %d: %v", preEnd, chunks)
	}
	eventCursor := int64(read["event_cursor"].(float64))
	if eventCursor < int64(markerSeq) {
		t.Fatalf("event_cursor %d did not cover marker seq %v", eventCursor, markerSeq)
	}
	postEnd := int64(read["next_cursor"].(float64))
	if postEnd <= preEnd {
		t.Fatalf("post-loss bytes did not advance next_cursor: %v", read)
	}

	// Repeated poll echoing the returned event_cursor is quiet — the
	// marker is consumed exactly once.
	read = readTerminalREST(t, w, scope, sessionID,
		fmt.Sprintf("&cursor=%d&event_cursor=%d", preEnd, eventCursor))
	for _, c := range read["chunks"].([]any) {
		if c.(map[string]any)["kind"] == "gap" {
			t.Fatalf("loss marker re-served after consumption: %v", c)
		}
	}

	// WS attach at byte cursor 0 receives the data frames AND the loss
	// frame with its durable event_seq.
	ws := w.dialTerminalWS(t, scope+"&cursor=0", sessionID)
	frames := wsFrames(ws)
	var gapFrame map[string]any
	deadline := time.After(10 * time.Second)
	for gapFrame == nil {
		select {
		case f, ok := <-frames:
			if !ok {
				t.Fatal("ws closed before loss frame")
			}
			if f["type"] == "gap" && f["base"] == float64(preEnd) {
				gapFrame = f
			}
		case <-deadline:
			t.Fatal("no loss frame on ws")
		}
	}
	wsSeq, ok := gapFrame["event_seq"].(float64)
	if !ok || int64(wsSeq) != int64(markerSeq) {
		t.Fatalf("ws loss frame event_seq = %v, want %v", gapFrame["event_seq"], markerSeq)
	}
	ws.Close()

	// Reconnect with the consumed event_cursor: the marker is not
	// re-sent, but post-loss data at the byte cursor is.
	ws = w.dialTerminalWS(t, scope+
		fmt.Sprintf("&cursor=%d&event_cursor=%d", preEnd, eventCursor), sessionID)
	frames = wsFrames(ws)
	got := collectWS(t, frames, func(f map[string]any) bool {
		return f["type"] == "output" || f["type"] == "gap"
	}, 8*time.Second)
	if got["type"] == "gap" && got["base"] == float64(preEnd) && got["event_seq"] == wsSeq {
		t.Fatalf("ws re-sent consumed loss marker: %v", got)
	}

	// Later output keeps advancing next_cursor past the boundary.
	appendJournalLine(t, journal, "later\r\n")
	waitFor("later output advances next_cursor", func() bool {
		read := readTerminalREST(t, w, scope, sessionID,
			fmt.Sprintf("&cursor=%d&event_cursor=%d", postEnd, eventCursor))
		return int64(read["next_cursor"].(float64)) > postEnd
	})
	ws.Close()
}

// TREV2-08 downstream proof: an in-place truncation of the SAME
// journal inode (copytruncate-class) followed by regrowth past the
// committed offset before the next poll must reach consumers as an
// explicit loss marker plus the intact new-era bytes — the real
// pumpJournal continuity witness, real driver, real PG, real REST/WS.
func TestTerminalComposedInPlaceTruncationReachesConsumers(t *testing.T) {
	w, journal, scope, sessionID, waitFor := startComposedJournalWorld(t)

	// Initial bytes reach REST at cursor 0.
	waitFor("pre-truncate bytes on REST", func() bool {
		read := readTerminalREST(t, w, scope, sessionID, "&cursor=0")
		for _, c := range read["chunks"].([]any) {
			if c.(map[string]any)["kind"] == "data" {
				return true
			}
		}
		return false
	})
	read := readTerminalREST(t, w, scope, sessionID, "&cursor=0")
	preEnd := int64(read["next_cursor"].(float64))
	if preEnd <= 0 {
		t.Fatalf("pre-truncate next_cursor = %v", read["next_cursor"])
	}

	// In-place truncation: same inode, then regrow PAST the committed
	// offset before the next poll — size < offset is never observed.
	if err := os.Truncate(journal, 0); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		appendJournalLine(t, journal, "post-truncate-era\r\n")
	}

	// The caught-up reader at the loss position receives the zero-width
	// marker AND the intact new-era bytes — no silent skip, no tear.
	var markerSeq float64
	waitFor("truncation marker + new-era bytes on REST", func() bool {
		read = readTerminalREST(t, w, scope, sessionID,
			fmt.Sprintf("&cursor=%d&event_cursor=0", preEnd))
		chunks := read["chunks"].([]any)
		seq, gap := hasZeroWidthGap(chunks, float64(preEnd))
		if !gap {
			return false
		}
		markerSeq = seq
		var post string
		for _, c := range chunks {
			m := c.(map[string]any)
			if m["kind"] == "data" {
				if d, _ := m["data"].(string); d != "" {
					raw, derr := base64.StdEncoding.DecodeString(d)
					if derr == nil {
						post += string(raw)
					}
				}
			}
		}
		return strings.Count(post, "post-truncate-era\r\n") == 8
	})
	eventCursor := int64(read["event_cursor"].(float64))
	if eventCursor < int64(markerSeq) {
		t.Fatalf("event_cursor %d did not cover marker seq %v", eventCursor, markerSeq)
	}

	// Repeated poll with the consumed cursor stays quiet.
	read = readTerminalREST(t, w, scope, sessionID,
		fmt.Sprintf("&cursor=%d&event_cursor=%d", preEnd, eventCursor))
	for _, c := range read["chunks"].([]any) {
		if c.(map[string]any)["kind"] == "gap" {
			t.Fatalf("loss marker re-served after consumption: %v", c)
		}
	}

	// WS attach sees the durable loss frame with event_seq.
	ws := w.dialTerminalWS(t, scope+"&cursor=0", sessionID)
	defer ws.Close()
	got := collectWS(t, wsFrames(ws), func(f map[string]any) bool {
		return f["type"] == "gap" && f["base"] == float64(preEnd)
	}, 10*time.Second)
	if got["event_seq"].(float64) != markerSeq {
		t.Fatalf("ws loss frame event_seq = %v, want %v", got["event_seq"], markerSeq)
	}
}
