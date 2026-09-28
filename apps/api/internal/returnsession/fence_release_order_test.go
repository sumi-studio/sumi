package returnsession_test

import (
	"context"
	"crypto/rand"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sumi-studio/sumi/apps/api/internal/fileaccess"
	"github.com/sumi-studio/sumi/apps/api/internal/returnsession"
)

// barrierFiles models filesvc's persisted barrier the way
// apps/files/internal/filesvc/store.go SetScopeFrozen decides it: a freeze
// may only advance the recorded (epoch, owner), a release applies when the
// barrier's (epoch, owner) is <= the caller's. Two attempts of the SAME
// session share (owner, epoch), so the barrier cannot tell a late release
// from a current one — only the order of arrival decides.
type barrierFiles struct {
	mu        sync.Mutex
	up        bool
	owner     string
	epoch     int64
	onFreeze  func(owner string) error // after the freeze lands; an error = unconfirmed
	onRelease func(owner string)       // before the release applies
}

func (f *barrierFiles) SetScopeFrozen(_ context.Context, _, owner string, epoch int64, _ string, frozen bool) error {
	f.mu.Lock()
	hookF, hookR := f.onFreeze, f.onRelease
	f.mu.Unlock()
	if !frozen {
		if hookR != nil {
			hookR(owner)
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.up && (f.epoch < epoch || (f.epoch == epoch && f.owner <= owner)) {
			f.up = false
		}
		return nil
	}
	f.mu.Lock()
	if f.up && (f.epoch > epoch || (f.epoch == epoch && f.owner > owner)) {
		f.mu.Unlock()
		return errors.New("scope barrier is held by a newer lineage")
	}
	f.up, f.owner, f.epoch = true, owner, epoch
	f.mu.Unlock()
	if hookF != nil {
		return hookF(owner)
	}
	return nil
}

func (f *barrierFiles) List(context.Context, string, string, string, int) (fileaccess.ListResult, error) {
	return fileaccess.ListResult{}, nil
}

func (f *barrierFiles) frozen() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.up
}

// sessionRowBusy reports whether another transaction holds the session
// row lock right now.
func sessionRowBusy(t *testing.T, pool *pgxpool.Pool, sid string) bool {
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Errorf("begin: %v", err)
		return false
	}
	defer tx.Rollback(context.Background())
	_, err = tx.Exec(context.Background(), `SELECT 1 FROM return_sessions WHERE session_id = $1 FOR UPDATE NOWAIT`, sid)
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "55P03"
}

// waitLockWaiter waits until some backend waits on a lock in a statement
// matching like.
func waitLockWaiter(t *testing.T, pool *pgxpool.Pool, like string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database() AND wait_event_type = 'Lock' AND query LIKE $1`, like).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no backend waiting on a lock for %q", like)
}

func exported(t *testing.T, h *harness, sid string) bool {
	t.Helper()
	var ok bool
	if err := h.cloud.pool.QueryRow(h.ctx, `SELECT EXISTS(SELECT 1 FROM core_transfers
		WHERE direction = 'export' AND transfer_id = $1)`, sid).Scan(&ok); err != nil {
		t.Fatal(err)
	}
	return ok
}

// A bind whose request is cancelled while its seal transaction waits in
// PostgreSQL (here: on the persona row) loses its connection, and with it
// the session row lock, before its fence release runs. The release must
// not act on the old lock: it is issued only under a lock it takes again,
// so the mover's retry — same session, same barrier lineage — cannot seal
// in between and then have its fence cleared by the stale release.
//
// Found by independent review 02 (F-A): the release ran with the row
// already free; a retry that sealed before it landed was left without a
// barrier over its copy window.
func TestCancelledSealAttemptCannotClearRetryFence(t *testing.T) {
	h := setup(t, returnsession.Config{})
	bf := &barrierFiles{}
	h.sessions.SetFileStore(bf)
	sid, _, grant := h.createMode("local")
	d := destMode(t, h.local, h.persona, "absent", "local")

	blocker, err := h.cloud.pool.Begin(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(context.Background())
	if _, err := blocker.Exec(h.ctx, `SELECT 1 FROM core_personas WHERE persona_id = $1 FOR UPDATE`, h.persona); err != nil {
		t.Fatal(err)
	}

	var (
		armed     atomic.Bool
		mu        sync.Mutex
		unlocked  int // releases for sid issued with the session row free
		first     atomic.Bool
		bDone     = make(chan struct{})
		bView     returnsession.View
		bErr      error
		retryOnce sync.Once
	)
	retry := func() {
		retryOnce.Do(func() {
			go func() {
				defer close(bDone)
				bView, bErr = h.sessions.BindDestination(context.Background(), sid, grant, d)
			}()
		})
	}
	bf.onRelease = func(owner string) {
		if owner != sid || !armed.Load() {
			return
		}
		busy := sessionRowBusy(t, h.cloud.pool, sid)
		mu.Lock()
		if !busy {
			unlocked++
		}
		mu.Unlock()
		if !first.CompareAndSwap(false, true) {
			return // the retry's own reconcile, not the stale release
		}
		// The stale release is on its way. Let the parked backend go and
		// give the retry every chance to seal before it lands.
		_ = blocker.Rollback(context.Background())
		retry()
		select {
		case <-bDone:
		case <-time.After(2 * time.Second):
		}
	}

	ctxA, cancelA := context.WithCancel(h.ctx)
	aDone := make(chan error, 1)
	go func() {
		_, err := h.sessions.BindDestination(ctxA, sid, grant, d)
		aDone <- err
	}()
	waitLockWaiter(t, h.cloud.pool, "%FOR NO KEY UPDATE%")
	if !bf.frozen() {
		t.Fatal("the attempt did not raise its fence before sealing")
	}
	armed.Store(true)
	cancelA()
	select {
	case err := <-aDone:
		if err == nil {
			t.Fatal("the cancelled attempt reported success")
		}
	case <-time.After(60 * time.Second):
		t.Fatal("the cancelled attempt did not return")
	}
	// If the attempt found the lock still busy it released nothing; the
	// mover's retry runs now.
	_ = blocker.Rollback(context.Background())
	retry()
	select {
	case <-bDone:
	case <-time.After(60 * time.Second):
		t.Fatal("the retry did not return")
	}
	if bErr != nil || bView.Status != returnsession.StatusSealed {
		t.Fatalf("retry: %s %v", bView.Status, bErr)
	}
	mu.Lock()
	n := unlocked
	mu.Unlock()
	if n > 0 {
		t.Errorf("%d fence release(s) were issued while nothing held the session row", n)
	}
	if !exported(t, h, sid) || !bf.frozen() {
		t.Fatalf("sealed copy window without its barrier (export %v, frozen %v)", exported(t, h, sid), bf.frozen())
	}
	if _, err := h.sessions.Sweep(h.ctx); err != nil {
		t.Fatal(err)
	}
	if !bf.frozen() {
		t.Fatal("a sweep cleared the sealed session's barrier")
	}
}

// A refused seal releases its fence without asking the pool for a second
// connection while it still holds the session row: with every other
// connection held by requests waiting on that row, the release would wait
// on itself. Here the pool has two connections and an owner cancel waits
// on the row.
//
// Found by independent review 02 (F-B): the bind stalled 30 s, gave up
// the release and left the fence up; the cancel waited the whole time.
func TestRefusedSealReleaseNeedsNoSecondConnection(t *testing.T) {
	h := setup(t, returnsession.Config{})
	cfg, err := pgxpool.ParseConfig(h.cloud.pool.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 2
	small, err := pgxpool.NewWithConfig(h.ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer small.Close()
	svc := returnsession.New(small, returnsession.Config{FilePolicy: returnsession.FilePolicyFixture})
	bf := &barrierFiles{}
	svc.SetFileStore(bf)
	sid, _, grant := h.createMode("local")
	d := destMode(t, h.local, h.persona, "absent", "local")

	cancelDone := make(chan error, 1)
	var once sync.Once
	bf.onFreeze = func(owner string) error {
		if owner != sid {
			return nil
		}
		var hook error
		once.Do(func() {
			go func() {
				_, err := svc.CancelByGrant(context.Background(), sid, grant)
				cancelDone <- err
			}()
			waitLockWaiter(t, h.cloud.pool, "%FOR UPDATE%")
			hook = errors.New("file effects still settling")
		})
		return hook
	}
	start := time.Now()
	if _, err := svc.BindDestination(h.ctx, sid, grant, d); err == nil {
		t.Fatal("a bind whose fence was not confirmed sealed")
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("the refused bind held the session row for %s", elapsed.Round(time.Second))
	}
	select {
	case err := <-cancelDone:
		if err != nil {
			t.Fatalf("cancel: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the cancel never returned")
	}
	if got := h.sessionStatus(sid); got != returnsession.StatusCancelled {
		t.Fatalf("session %s", got)
	}
	if bf.frozen() {
		t.Fatal("the refused, cancelled move left Cloud's workspace frozen")
	}
	if got := authority(t, h.cloud, h.persona); got != "active" {
		t.Fatalf("authority %s", got)
	}
}

// insertWaitingBinds adds n bound local-mode sessions that never sealed,
// one per synthetic secretary — the population a busy deployment can
// accumulate, since a bound session never expires.
func insertWaitingBinds(t *testing.T, h *harness, n int) map[string]bool {
	t.Helper()
	ids := make(map[string]bool, n)
	for i := 0; i < n; i++ {
		sid := newID(t)
		grant := make([]byte, 32)
		if _, err := rand.Read(grant); err != nil {
			t.Fatal(err)
		}
		mustExec(t, h.cloud.pool, `INSERT INTO return_sessions (session_id, human_id, persona_id, grant_hash,
			status, destination_placement_id, destination_persona_id, destination_slot_state,
			destination_bound_at, admit_until, file_mode)
			VALUES ($1, $2, $3, $4, 'awaiting_destination', $5, $3, 'absent', now(), now() - interval '1 hour', 'local')`,
			sid, h.human, newID(t), grant, newID(t))
		ids[sid] = true
	}
	return ids
}

// insertDueExpiries adds n unbound sessions whose admission deadline has
// passed — lifecycle work a sweep owes now.
func insertDueExpiries(t *testing.T, h *harness, n int) []string {
	t.Helper()
	var ids []string
	for i := 0; i < n; i++ {
		sid := newID(t)
		grant := make([]byte, 32)
		if _, err := rand.Read(grant); err != nil {
			t.Fatal(err)
		}
		mustExec(t, h.cloud.pool, `INSERT INTO return_sessions (session_id, human_id, persona_id, grant_hash,
			status, admit_until, file_mode)
			VALUES ($1, $2, $3, $4, 'awaiting_destination', now() - interval '1 minute', 'local')`,
			sid, h.human, newID(t), grant)
		ids = append(ids, sid)
	}
	return ids
}

// releasedAmong is the set of owners in ids the fake saw released.
func releasedAmong(f *fakeFiles, ids map[string]bool) map[string]bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]bool{}
	for _, c := range f.frozenCalls {
		owner, _, _ := strings.Cut(c, "|")
		if ids[owner] && strings.HasSuffix(c, "=false") {
			out[owner] = true
		}
	}
	return out
}

// Bound sessions waiting to seal never leave the sweep's candidates. However
// many there are, a sweep still does every due lifecycle step, and it
// visits the waiting ones only in a bounded batch.
//
// Found by independent review 02 (F-D): they shared one unordered
// LIMIT 200 with the lifecycle work.
func TestSweepDueLifecycleNotDisplacedByWaitingBinds(t *testing.T) {
	h := setup(t, returnsession.Config{})
	ff := &fakeFiles{}
	h.sessions.SetFileStore(ff)
	waiting := insertWaitingBinds(t, h, 400)
	due := insertDueExpiries(t, h, 5)

	if _, err := h.sessions.Sweep(h.ctx); err != nil {
		t.Fatal(err)
	}
	for _, sid := range due {
		if got := h.sessionStatus(sid); got != returnsession.StatusExpired {
			t.Errorf("due session %s is %s after one sweep", sid, got)
		}
	}
	if n := len(releasedAmong(ff, waiting)); n == 0 || n > 50 {
		t.Fatalf("one sweep visited %d waiting binds (want 1..50)", n)
	}
	for sid := range waiting {
		if got := h.sessionStatus(sid); got != returnsession.StatusAwaitingDestination {
			t.Fatalf("waiting bind %s became %s", sid, got)
		}
	}
}

// The waiting binds are visited in turn: successive sweeps continue where
// the last stopped and wrap, so a large set is covered rather than the
// same subset every time — by batch, and by time budget when the file
// service is slow.
func TestSweepStrandedPassCoversWaitingBindsInTurn(t *testing.T) {
	t.Run("batch", func(t *testing.T) {
		h := setup(t, returnsession.Config{})
		ff := &fakeFiles{}
		h.sessions.SetFileStore(ff)
		h.sessions.SetFenceSweepForTest(7, time.Hour)
		waiting := insertWaitingBinds(t, h, 20)
		seen := map[string]bool{}
		for i := 1; i <= 3; i++ {
			ff.reset()
			if _, err := h.sessions.Sweep(h.ctx); err != nil {
				t.Fatal(err)
			}
			got := releasedAmong(ff, waiting)
			if len(got) != 7 {
				t.Fatalf("sweep %d visited %d (want the batch of 7)", i, len(got))
			}
			for sid := range got {
				seen[sid] = true
			}
		}
		if len(seen) != 20 {
			t.Fatalf("three sweeps of 7 reached %d of 20 waiting binds", len(seen))
		}
	})
	t.Run("slow file service", func(t *testing.T) {
		h := setup(t, returnsession.Config{})
		ff := &fakeFiles{}
		h.sessions.SetFileStore(ff)
		h.sessions.SetFenceSweepForTest(50, 300*time.Millisecond)
		waiting := insertWaitingBinds(t, h, 12)
		ff.onRelease = func(owner string) {
			if waiting[owner] {
				time.Sleep(150 * time.Millisecond)
			}
		}
		due := insertDueExpiries(t, h, 1)
		all := map[string]bool{}
		for sid := range waiting {
			all[sid] = true
		}
		var sweeps int
		for len(releasedAmong(ff, all)) < len(all) {
			sweeps++
			if sweeps > 12 {
				t.Fatalf("12 sweeps reached %d of %d waiting binds", len(releasedAmong(ff, all)), len(all))
			}
			before := len(releasedAmong(ff, all))
			start := time.Now()
			if _, err := h.sessions.Sweep(h.ctx); err != nil {
				t.Fatal(err)
			}
			if took := time.Since(start); took > 2*time.Second {
				t.Fatalf("sweep %d took %s against a slow file service", sweeps, took)
			}
			if sweeps == 1 {
				if got := h.sessionStatus(due[0]); got != returnsession.StatusExpired {
					t.Fatalf("due session %s after the first sweep", got)
				}
				if n := len(releasedAmong(ff, all)); n >= len(all) {
					t.Fatalf("the time budget did not stop the first sweep (%d visited)", n)
				}
			}
			if len(releasedAmong(ff, all)) == before {
				t.Fatalf("sweep %d made no progress", sweeps)
			}
		}
		t.Logf("%d waiting binds reached in %d budgeted sweeps", len(all), sweeps)
	})
}
