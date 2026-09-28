package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/journalmirror"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

func gateStatus(t *testing.T, gate http.Handler) (int, startupGateStatus, http.Header) {
	t.Helper()
	rec := httptest.NewRecorder()
	gate.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	var status startupGateStatus
	if rec.Code == http.StatusServiceUnavailable {
		if err := json.Unmarshal(rec.Body.Bytes(), &status); err != nil {
			t.Fatalf("gate body %q: %v", rec.Body.String(), err)
		}
	}
	return rec.Code, status, rec.Header()
}

// F6: while the application starts (waiting for the journal lease,
// restoring), the port answers a retryable 503 that names the phase and the
// restore progress; after a lease loss it answers 503 again and runs the
// stop hook once.
func TestStartupGateAnswersRetryablyUntilReadyAndAfterStop(t *testing.T) {
	gate := newStartupGate()
	gate.setPhase("journal_lease")
	code, status, header := gateStatus(t, gate)
	if code != http.StatusServiceUnavailable || status.Error != "api_starting" || status.Phase != "journal_lease" || header.Get("Retry-After") == "" {
		t.Fatalf("starting gate = %d %+v retry-after %q", code, status, header.Get("Retry-After"))
	}
	gate.restoreProgress(journalmirror.AttachProgress{Dir: "commands", FilesDone: 1, Files: 2, BytesDone: 10, Bytes: 20})
	if _, status, _ := gateStatus(t, gate); status.Phase != "journal_restore" || status.Progress == nil || status.Progress.BytesDone != 10 {
		t.Fatalf("restoring gate = %+v", status)
	}

	gate.open(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	if code, _, _ := gateStatus(t, gate); code != http.StatusNoContent {
		t.Fatalf("open gate = %d", code)
	}

	stops := 0
	gate.onStop(func() { stops++ })
	gate.stop(journalmirror.ErrLeaseLost)
	gate.stop(journalmirror.ErrFenced)
	if code, status, _ := gateStatus(t, gate); code != http.StatusServiceUnavailable || status.Error != "api_stopping" {
		t.Fatalf("stopped gate = %d %+v", code, status)
	}
	if stops != 1 {
		t.Fatalf("stop hook ran %d times", stops)
	}
	late := 0
	gate.onStop(func() { late++ })
	if late != 1 {
		t.Fatal("a stop hook registered after the stop did not run")
	}
}

// journalMirrorDatabaseEnv points SUMI_DB_URL at a fresh test database and
// returns a pool on it.
func journalMirrorDatabaseEnv(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool := testdb.Create(t)
	t.Setenv("SUMI_DB_URL", pool.Config().ConnString())
	return pool
}

func startJournalMirror(t *testing.T, mode, cmdDir, runtimeDir string, gate *startupGate) (*journalmirror.Mirror, error) {
	t.Helper()
	t.Setenv(journalMirrorEnv, mode)
	database, mirror, err := journalMirrorFromEnv(context.Background(), cmdDir, runtimeDir, gate)
	if err != nil {
		return nil, err
	}
	t.Cleanup(database.Close)
	t.Cleanup(func() { _ = mirror.Close() })
	return mirror, nil
}

// F1: the Container's restore-only mode refuses a database whose mirror was
// never initialized and changes nothing; the current host's adopt mode
// initializes it from its journals; a replacement host with empty disks then
// restores them.
func TestJournalMirrorStartModes(t *testing.T) {
	journalMirrorDatabaseEnv(t)
	t.Setenv(journalMirrorEnv, "postgres-sync")
	if _, _, err := journalMirrorFromEnv(context.Background(), t.TempDir(), t.TempDir(), nil); err == nil {
		t.Fatal("unknown mirror mode accepted")
	}
	t.Setenv(journalMirrorRestoreTimeoutEnv, "soon")
	if _, _, err := journalMirrorFromEnv(context.Background(), t.TempDir(), t.TempDir(), nil); err == nil {
		t.Fatal("invalid restore timeout accepted")
	}
	t.Setenv(journalMirrorRestoreTimeoutEnv, "")

	files := map[string]string{
		"commands/commands-018f47a2-9b3c-7def-8abc-0123456789ab.jsonl": `{"seq":1}` + "\n",
		"events/events-018f47a2-9b3c-7def-8abc-0123456789ab.jsonl":     `{"seq":1}` + "\n",
	}
	current := t.TempDir()
	for name, content := range files {
		path := filepath.Join(current, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	cmdDir, runtimeDir := filepath.Join(current, "commands"), filepath.Join(current, "events")

	if _, err := startJournalMirror(t, "postgres", cmdDir, runtimeDir, nil); !errors.Is(err, journalmirror.ErrNotInitialized) {
		t.Fatalf("restore-only start on an uninitialized database = %v, want ErrNotInitialized", err)
	}
	for name, content := range files {
		if got, err := os.ReadFile(filepath.Join(current, name)); err != nil || string(got) != content {
			t.Fatalf("refused start changed %s: %q %v", name, got, err)
		}
	}

	adopted, err := startJournalMirror(t, "postgres-adopt", cmdDir, runtimeDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := adopted.Close(); err != nil {
		t.Fatal(err)
	}

	gate := newStartupGate()
	freshCommands, freshEvents := t.TempDir(), t.TempDir()
	if _, err := startJournalMirror(t, "postgres", freshCommands, freshEvents, gate); err != nil {
		t.Fatal(err)
	}
	for name, content := range files {
		dir := freshCommands
		if filepath.Dir(name) == "events" {
			dir = freshEvents
		}
		if got, err := os.ReadFile(filepath.Join(dir, filepath.Base(name))); err != nil || string(got) != content {
			t.Fatalf("restored %s = %q %v", name, got, err)
		}
	}
	if _, status, _ := gateStatus(t, gate); status.Phase != "journal_restore" || status.Progress == nil {
		t.Fatalf("gate after restore = %+v", status)
	}
}

// F4: when the lease session ends, the API stops answering, runs its stop
// hook (background work and browser connections) and ends the process.
func TestLostJournalMirrorLeaseStopsTheAPI(t *testing.T) {
	pool := journalMirrorDatabaseEnv(t)
	stopped := make(chan struct{}, 1)
	previous := stopProcess
	stopProcess = func() { stopped <- struct{}{} }
	t.Cleanup(func() { stopProcess = previous })

	current := t.TempDir()
	cmdDir, runtimeDir := filepath.Join(current, "commands"), filepath.Join(current, "events")
	for _, path := range []string{filepath.Join(cmdDir, "commands-a.jsonl"), filepath.Join(runtimeDir, "events-a.jsonl")} {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	gate := newStartupGate()
	mirror, err := startJournalMirror(t, "postgres-adopt", cmdDir, runtimeDir, gate)
	if err != nil {
		t.Fatal(err)
	}
	gate.open(http.NotFoundHandler())
	hooked := make(chan struct{})
	gate.onStop(func() { close(hooked) })

	var n int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity
		WHERE datname = current_database() AND application_name = 'sumi-journal-mirror-lease'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("terminate lease session: %d sessions, %v", n, err)
	}
	select {
	case <-hooked:
	case <-time.After(5 * time.Second):
		t.Fatal("stop hook did not run after the lease was lost")
	}
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("process was not stopped after the lease was lost")
	}
	if code, status, _ := gateStatus(t, gate); code != http.StatusServiceUnavailable || status.Error != "api_stopping" {
		t.Fatalf("gate after lease loss = %d %+v", code, status)
	}
	if !mirror.Fenced() {
		t.Fatal("mirror still reports ownership")
	}
}
