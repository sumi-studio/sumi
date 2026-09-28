package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/journalmirror"
)

// startupGate is the public handler. The API listens before it builds the
// application, because with the journal mirror a start may first wait for
// the mirror lease and restore the journals. Until the application is
// ready, and again once the process stops after losing the lease, every
// request gets a retryable 503 that names the phase instead of a refused or
// hanging connection. A GET or HEAD request first waits up to hold for the
// application (see startupHold).
type startupGate struct {
	mu       sync.Mutex
	handler  http.Handler
	phase    string
	progress *journalmirror.AttachProgress
	stopped  error
	stopHook func()
	settled  chan struct{} // closed once the gate opens or stops
	settle   sync.Once
	hold     time.Duration
}

// startupHold is how long a GET or HEAD request that arrives while the API
// starts (a cold start restoring the journals, usually) waits for it before
// the gate answers 503. The request has not reached the application, so
// passing it on once the application opens is its first delivery, not a
// retry. Requests with other methods are not held: their body's read timeout
// would run meanwhile, and they get the 503 at once as before. /health is not
// held either; the container platform's readiness ping and monitors read the
// phase from it.
const startupHold = 10 * time.Second

func newStartupGate() *startupGate {
	return &startupGate{phase: "starting", settled: make(chan struct{}), hold: startupHold}
}

// setPhase names the current startup step. The name is public; details
// belong in the log.
func (g *startupGate) setPhase(phase string) {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.phase, g.progress = phase, nil
	g.mu.Unlock()
}

func (g *startupGate) restoreProgress(p journalmirror.AttachProgress) {
	log.Printf("journal mirror %s: restored %d of %d files, %d of %d bytes", p.Dir, p.FilesDone, p.Files, p.BytesDone, p.Bytes)
	if g == nil {
		return
	}
	g.mu.Lock()
	g.phase, g.progress = "journal_restore", &p
	g.mu.Unlock()
}

// open starts serving handler.
func (g *startupGate) open(handler http.Handler) {
	g.mu.Lock()
	g.handler = handler
	g.mu.Unlock()
	g.settle.Do(func() { close(g.settled) })
}

// stop closes the gate for good and runs the stop hook once.
func (g *startupGate) stop(reason error) {
	if g == nil {
		return
	}
	g.mu.Lock()
	if g.stopped != nil {
		g.mu.Unlock()
		return
	}
	g.stopped = reason
	hook := g.stopHook
	g.mu.Unlock()
	g.settle.Do(func() { close(g.settled) })
	if hook != nil {
		hook()
	}
}

// onStop registers what stop runs. If the gate already stopped, hook runs now.
func (g *startupGate) onStop(hook func()) {
	g.mu.Lock()
	stopped := g.stopped != nil
	if !stopped {
		g.stopHook = hook
	}
	g.mu.Unlock()
	if stopped {
		hook()
	}
}

type startupGateStatus struct {
	Error    string                        `json:"error"`
	Phase    string                        `json:"phase,omitempty"`
	Progress *journalmirror.AttachProgress `json:"progress,omitempty"`
}

func (g *startupGate) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if g.holds(r) {
		timer := time.NewTimer(g.hold)
		select {
		case <-g.settled:
		case <-timer.C:
		case <-r.Context().Done():
		}
		timer.Stop()
	}
	g.mu.Lock()
	handler, stopped := g.handler, g.stopped
	status := startupGateStatus{Error: "api_starting", Phase: g.phase, Progress: g.progress}
	g.mu.Unlock()
	if stopped == nil && handler != nil {
		handler.ServeHTTP(w, r)
		return
	}
	if stopped != nil {
		status = startupGateStatus{Error: "api_stopping"}
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Retry-After", "5")
	w.WriteHeader(http.StatusServiceUnavailable)
	_ = json.NewEncoder(w).Encode(status)
}

// holds reports whether r waits for the application; see startupHold.
func (g *startupGate) holds(r *http.Request) bool {
	if g.hold <= 0 || (r.Method != http.MethodGet && r.Method != http.MethodHead) || r.URL.Path == "/health" {
		return false
	}
	select {
	case <-g.settled:
		return false
	default:
		return true
	}
}
