package agentevents

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/journalmirror"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

// mirrorTestPool is a migrated database whose journal mirrors were
// initialized empty (a new installation).
func mirrorTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool := testdb.Create(t)
	ctx := context.Background()
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	for _, logical := range []string{"commands", "browser-events"} {
		if _, _, err := journalmirror.InitializeEmpty(ctx, pool, logical, "test"); err != nil {
			t.Fatal(err)
		}
	}
	return pool
}

type mirroredHost struct {
	mirror     *journalmirror.Mirror
	faults     *faultyMirror
	commands   *CommandStore
	journal    *BrowserJournal
	commandDir string
	journalDir string
}

// startMirroredHost acquires the mirror, restores both journal directories
// from it and opens the journals on them. Fresh directories are a replaced
// container's empty disk; a previous host's directories are a restart in
// place.
func startMirroredHost(t *testing.T, pool *pgxpool.Pool, holder, commandDir, journalDir string) mirroredHost {
	t.Helper()
	ctx := context.Background()
	mirror, err := journalmirror.Acquire(ctx, pool, journalmirror.Options{Holder: holder, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mirror.Close() })
	if _, err := mirror.Attach(ctx, "commands", commandDir, journalmirror.AttachOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := mirror.Attach(ctx, "browser-events", journalDir, journalmirror.AttachOptions{}); err != nil {
		t.Fatal(err)
	}
	faults := &faultyMirror{Mirror: mirror}
	commands, err := OpenMirroredCommandStore(commandDir, faults)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = commands.Close() })
	journal, err := OpenMirroredBrowserJournal(journalDir, commands, faults)
	if err != nil {
		t.Fatal(err)
	}
	journal.PollInterval = 5 * time.Millisecond
	return mirroredHost{mirror: mirror, faults: faults, commands: commands, journal: journal, commandDir: commandDir, journalDir: journalDir}
}

// crashLease ends the lease session of the running host without closing it,
// as a host that vanished does once its connection is gone.
func crashLease(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	var n int
	err := pool.QueryRow(context.Background(), `
		SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity
		WHERE datname = current_database() AND application_name = 'sumi-journal-mirror-lease'`).Scan(&n)
	if err != nil || n != 1 {
		t.Fatalf("terminate lease session: %d sessions, %v", n, err)
	}
}

// databaseOutage makes every mirror commit fail until the returned function
// runs: the database answers, but rejects the writes (as during a failover
// or a full disk). The local journals are unaffected.
func databaseOutage(t *testing.T, pool *pgxpool.Pool) (end func()) {
	t.Helper()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `ALTER TABLE api_journal_mirror_files ADD CONSTRAINT injected_outage CHECK (false) NOT VALID`); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	end = func() {
		once.Do(func() {
			if _, err := pool.Exec(ctx, `ALTER TABLE api_journal_mirror_files DROP CONSTRAINT injected_outage`); err != nil {
				t.Fatal(err)
			}
		})
	}
	t.Cleanup(end)
	return end
}

// faultyMirror passes the journals through a real mirror. When armed, the
// next mirrored Sync of a file in the armed directory commits to PostgreSQL
// and then reports a replication failure, as when the connection breaks after
// COMMIT: the ambiguous outcome. The armed function runs between the commit
// and the error.
type faultyMirror struct {
	*journalmirror.Mirror

	mu       sync.Mutex
	armedDir string
	armed    func()
}

func (f *faultyMirror) arm(dir string, afterCommit func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.armedDir, f.armed = dir, afterCommit
}

func (f *faultyMirror) take(dir string) func() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.armed == nil || dir != f.armedDir {
		return nil
	}
	armed := f.armed
	f.armed = nil
	return armed
}

func (f *faultyMirror) Wrap(path string, flag int, file JournalFile) (JournalFile, error) {
	wrapped, err := f.Mirror.Wrap(path, flag, file)
	if err != nil || wrapped == file {
		return wrapped, err
	}
	return &faultyFile{JournalFile: wrapped, mirror: f, dir: filepath.Dir(path)}, nil
}

type faultyFile struct {
	JournalFile
	mirror *faultyMirror
	dir    string
}

func (f *faultyFile) Sync() error {
	if err := f.JournalFile.Sync(); err != nil {
		return err
	}
	if afterCommit := f.mirror.take(f.dir); afterCommit != nil {
		afterCommit()
		return &journalmirror.ReplicationError{Err: errors.New("injected: connection lost after COMMIT")}
	}
	return nil
}

var mirrorTestCommand = json.RawMessage(`{"type":"user_message","text":"remember the dentist on Friday","attachments":[]}`)

func appendCommand(t *testing.T, s *CommandStore, key string) CommandEnvelope {
	t.Helper()
	env, err := s.Append(context.Background(), testDirectChatProvenance(historyPA), key, mirrorTestCommand)
	if err != nil {
		t.Fatalf("append %s: %v", key, err)
	}
	return env
}

func requireNextSeq(t *testing.T, s *CommandStore, want uint64) {
	t.Helper()
	if seq, err := s.NextCommandSeq(context.Background(), historyPA); err != nil || seq != want {
		t.Fatalf("next command seq = %d, %v; want %d", seq, err, want)
	}
}

func requireMirrorEqual(t *testing.T, pool *pgxpool.Pool, logical, dir string) {
	t.Helper()
	report, err := journalmirror.Verify(context.Background(), pool, logical, dir)
	if err != nil || !report.Equal() {
		t.Fatalf("mirror %s differs from %s: %+v %v", logical, dir, report, err)
	}
}

// Everything the API acknowledged on one host — an admitted Direct Chat
// command and its idempotency key, the visible conversation events, and a
// logged-out browser session — is present on a replacement host that starts
// with empty disks after the first host vanished. The vanished host can no
// longer acknowledge writes.
func TestMirroredJournalsSurviveHostReplacement(t *testing.T) {
	pool := mirrorTestPool(t)
	ctx := context.Background()
	a := startMirroredHost(t, pool, "host-a", t.TempDir(), privateRuntimeDir(t))

	admitted := appendCommand(t, a.commands, "idem-1")
	seedProjectedRun(t, a.journal, historyPA, 1, 5)
	eventsA, err := a.journal.EventCatchUp(ctx, historyPA, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(eventsA) == 0 {
		t.Fatal("host A has no events")
	}
	rawSession := make([]byte, browserSessionIDBytes)
	if _, err := rand.Read(rawSession); err != nil {
		t.Fatal(err)
	}
	session := base64.RawURLEncoding.EncodeToString(rawSession)
	now := time.Now()
	expires := now.Add(time.Hour)
	if err := a.journal.RevokeBrowserSession(ctx, session, expires, now); err != nil {
		t.Fatal(err)
	}

	crashLease(t, pool)
	b := startMirroredHost(t, pool, "host-b", t.TempDir(), privateRuntimeDir(t))

	replay := appendCommand(t, b.commands, "idem-1")
	if replay.Seq != admitted.Seq || replay.CommandID != admitted.CommandID {
		t.Fatalf("idempotent replay on host B = seq %d id %s, want seq %d id %s", replay.Seq, replay.CommandID, admitted.Seq, admitted.CommandID)
	}
	next := appendCommand(t, b.commands, "idem-2")
	if next.Seq != admitted.Seq+1 {
		t.Fatalf("next seq on host B = %d, want %d", next.Seq, admitted.Seq+1)
	}
	eventsB, err := b.journal.EventCatchUp(ctx, historyPA, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(eventsA, eventsB) {
		t.Fatalf("host B events differ: %d vs %d", len(eventsB), len(eventsA))
	}
	seedProjectedRun(t, b.journal, historyPA, 6, 2)
	if err := b.journal.CheckBrowserSession(ctx, session, expires, time.Now()); !errors.Is(err, errBrowserSessionRevoked) {
		t.Fatalf("logged-out session on host B = %v, want revoked", err)
	}

	if _, err := a.commands.Append(ctx, testDirectChatProvenance(historyPA), "idem-3", mirrorTestCommand); err == nil {
		t.Fatal("replaced host A still acknowledged a command")
	}
	if !a.mirror.Fenced() {
		t.Fatal("host A was not fenced")
	}

	if err := b.mirror.Close(); err != nil {
		t.Fatal(err)
	}
	c := startMirroredHost(t, pool, "host-c", t.TempDir(), privateRuntimeDir(t))
	eventsC, err := c.journal.EventCatchUp(ctx, historyPA, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(eventsC) <= len(eventsA) {
		t.Fatalf("host C sees %d events; host B's later run is missing", len(eventsC))
	}
	requireNextSeq(t, c.commands, next.Seq+1)
}

// F3 (the review's reproduction): a database outage during an append and
// its rollback fails that append, but does not disable Direct Chat. Once the
// database answers again, the same idempotency key is admitted once, at the
// next seq, and a replacement host sees exactly the acknowledged commands.
func TestDatabaseOutageDoesNotPoisonDirectChat(t *testing.T) {
	pool := mirrorTestPool(t)
	ctx := context.Background()
	a := startMirroredHost(t, pool, "host-a", t.TempDir(), privateRuntimeDir(t))
	first := appendCommand(t, a.commands, "idem-1")

	end := databaseOutage(t, pool)
	_, err := a.commands.Append(ctx, testDirectChatProvenance(historyPA), "idem-2", mirrorTestCommand)
	if err == nil {
		t.Fatal("append acknowledged during the outage")
	}
	if strings.Contains(err.Error(), "poisoned") || strings.Contains(err.Error(), "rollback could not be confirmed") {
		t.Fatalf("outage poisoned the command log: %v", err)
	}
	requireNextSeq(t, a.commands, first.Seq+1)
	// Still failing, still not poisoned, while the outage lasts.
	if _, err := a.commands.Append(ctx, testDirectChatProvenance(historyPA), "idem-2", mirrorTestCommand); err == nil || strings.Contains(err.Error(), "poisoned") {
		t.Fatalf("second append during the outage = %v", err)
	}
	end()

	retried := appendCommand(t, a.commands, "idem-2")
	if retried.Seq != first.Seq+1 {
		t.Fatalf("retry after the outage = seq %d, want %d", retried.Seq, first.Seq+1)
	}
	third := appendCommand(t, a.commands, "idem-3")
	requireMirrorEqual(t, pool, "commands", a.commandDir)

	crashLease(t, pool)
	b := startMirroredHost(t, pool, "host-b", t.TempDir(), privateRuntimeDir(t))
	for _, acked := range []struct {
		key  string
		want CommandEnvelope
	}{{"idem-1", first}, {"idem-2", retried}, {"idem-3", third}} {
		got := appendCommand(t, b.commands, acked.key)
		if got.Seq != acked.want.Seq || got.CommandID != acked.want.CommandID {
			t.Fatalf("replay of %s on host B = seq %d id %s, want seq %d id %s", acked.key, got.Seq, got.CommandID, acked.want.Seq, acked.want.CommandID)
		}
	}
	requireNextSeq(t, b.commands, third.Seq+1)
}

// F3: an append whose commit reached PostgreSQL but was reported as failed is
// rolled back in both copies when the rollback's own commit succeeds. The
// retry is admitted once and a replacement host has one record per key.
func TestAmbiguousCommandCommitIsRolledBackEverywhere(t *testing.T) {
	pool := mirrorTestPool(t)
	ctx := context.Background()
	a := startMirroredHost(t, pool, "host-a", t.TempDir(), privateRuntimeDir(t))
	first := appendCommand(t, a.commands, "idem-1")

	a.faults.arm(a.commandDir, func() {})
	if _, err := a.commands.Append(ctx, testDirectChatProvenance(historyPA), "idem-2", mirrorTestCommand); err == nil {
		t.Fatal("ambiguous commit acknowledged")
	}
	requireMirrorEqual(t, pool, "commands", a.commandDir)
	retried := appendCommand(t, a.commands, "idem-2")
	if retried.Seq != first.Seq+1 {
		t.Fatalf("retry = seq %d, want %d", retried.Seq, first.Seq+1)
	}

	crashLease(t, pool)
	b := startMirroredHost(t, pool, "host-b", t.TempDir(), privateRuntimeDir(t))
	if got := appendCommand(t, b.commands, "idem-2"); got.Seq != retried.Seq || got.CommandID != retried.CommandID {
		t.Fatalf("replay on host B = seq %d id %s, want seq %d id %s", got.Seq, got.CommandID, retried.Seq, retried.CommandID)
	}
	requireNextSeq(t, b.commands, retried.Seq+1)
}

// F3: an append whose commit reached PostgreSQL but was reported as failed,
// followed by an outage that also fails the rollback's commit. PostgreSQL
// then holds a record the client was told failed, and the host's disk does
// not. Whichever copy survives, the client's retry with the same key never
// creates a second command:
//   - the host keeps running: its next append replaces PostgreSQL's tail,
//     so the retry is admitted once, at the same seq;
//   - the host restarts in place, or is replaced: PostgreSQL's copy is
//     restored (the host's rolled-back file is kept in quarantine), and the
//     retry replays the committed record.
func TestAmbiguousCommandCommitWithFailedRollbackConvergesToOneHistory(t *testing.T) {
	for _, tc := range []struct {
		name  string
		after func(t *testing.T, pool *pgxpool.Pool, a mirroredHost) mirroredHost
	}{
		{"host keeps running", func(t *testing.T, pool *pgxpool.Pool, a mirroredHost) mirroredHost { return a }},
		{"host restarts in place", func(t *testing.T, pool *pgxpool.Pool, a mirroredHost) mirroredHost {
			crashLease(t, pool)
			_ = a.commands.Close()
			return startMirroredHost(t, pool, "host-a-restarted", a.commandDir, a.journalDir)
		}},
		{"host is replaced", func(t *testing.T, pool *pgxpool.Pool, a mirroredHost) mirroredHost {
			crashLease(t, pool)
			return startMirroredHost(t, pool, "host-b", t.TempDir(), privateRuntimeDir(t))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := mirrorTestPool(t)
			ctx := context.Background()
			a := startMirroredHost(t, pool, "host-a", t.TempDir(), privateRuntimeDir(t))
			first := appendCommand(t, a.commands, "idem-1")

			var end func()
			a.faults.arm(a.commandDir, func() { end = databaseOutage(t, pool) })
			_, err := a.commands.Append(ctx, testDirectChatProvenance(historyPA), "idem-2", mirrorTestCommand)
			if err == nil {
				t.Fatal("ambiguous commit acknowledged")
			}
			if strings.Contains(err.Error(), "rollback could not be confirmed") {
				t.Fatalf("failed rollback commit poisoned the command log: %v", err)
			}
			if end == nil {
				t.Fatal("the ambiguous commit was not injected")
			}
			end()

			host := tc.after(t, pool, a)
			retried := appendCommand(t, host.commands, "idem-2")
			if retried.Seq != first.Seq+1 {
				t.Fatalf("retry = seq %d, want %d", retried.Seq, first.Seq+1)
			}
			if again := appendCommand(t, host.commands, "idem-2"); again.CommandID != retried.CommandID {
				t.Fatalf("second retry = id %s, want %s", again.CommandID, retried.CommandID)
			}
			third := appendCommand(t, host.commands, "idem-3")
			if third.Seq != first.Seq+2 {
				t.Fatalf("next command = seq %d, want %d", third.Seq, first.Seq+2)
			}
			requireMirrorEqual(t, pool, "commands", host.commandDir)
			if host.commandDir == a.commandDir && host.mirror != a.mirror {
				entries, err := os.ReadDir(filepath.Join(a.commandDir, ".journal-mirror-quarantine"))
				if err != nil || len(entries) != 1 {
					t.Fatalf("restart in place kept %d quarantine entries: %v", len(entries), err)
				}
			}
		})
	}
}

// F3, browser journal: a database outage fails a projected-event append and
// nothing else. After the outage the same batch commits once, and a
// replacement host sees exactly the events the first host acknowledged.
func TestDatabaseOutageDoesNotBreakBrowserEvents(t *testing.T) {
	pool := mirrorTestPool(t)
	ctx := context.Background()
	a := startMirroredHost(t, pool, "host-a", t.TempDir(), privateRuntimeDir(t))
	seedProjectedRun(t, a.journal, historyPA, 1, 5)
	before, err := a.journal.EventCatchUp(ctx, historyPA, 0)
	if err != nil {
		t.Fatal(err)
	}

	end := databaseOutage(t, pool)
	batch := []ProjectedEvent{{RunMarker: RunMarkerStart}}
	for i := 6; i < 9; i++ {
		raw, err := json.Marshal(historyMessage(i))
		if err != nil {
			t.Fatal(err)
		}
		batch = append(batch, ProjectedEvent{Event: raw})
	}
	batch = append(batch, ProjectedEvent{RunMarker: RunMarkerEnd})
	for attempt := 0; attempt < 2; attempt++ {
		if err := a.journal.AppendProjectedEvents(ctx, historyPA, batch); err == nil {
			t.Fatal("projected events acknowledged during the outage")
		}
	}
	if during, err := a.journal.EventCatchUp(ctx, historyPA, 0); err != nil || !reflect.DeepEqual(during, before) {
		t.Fatalf("events during the outage = %d, %v; want the %d acknowledged", len(during), err, len(before))
	}
	end()

	if err := a.journal.AppendProjectedEvents(ctx, historyPA, batch); err != nil {
		t.Fatal(err)
	}
	after, err := a.journal.EventCatchUp(ctx, historyPA, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after[:len(before)], before) {
		t.Fatal("events acknowledged before the outage changed")
	}
	for i := 6; i < 9; i++ {
		id := fmt.Sprintf("01992000-0000-7000-8000-%012x", i)
		count := 0
		for _, event := range after {
			if strings.Contains(string(event.Event), id) {
				count++
			}
		}
		if count != 1 {
			t.Fatalf("message %d committed %d times after the outage, want once", i, count)
		}
	}
	requireMirrorEqual(t, pool, "browser-events", a.journalDir)

	crashLease(t, pool)
	b := startMirroredHost(t, pool, "host-b", t.TempDir(), privateRuntimeDir(t))
	replaced, err := b.journal.EventCatchUp(ctx, historyPA, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(replaced, after) {
		t.Fatalf("replacement host has %d events, want the %d acknowledged", len(replaced), len(after))
	}
}
