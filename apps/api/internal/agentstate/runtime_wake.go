package agentstate

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// A Cloud placement runs the secretary core as a Durable Object that sleeps
// between drains: nothing in that runtime polls this database, and an object
// that was never woken has no alarm at all. RuntimeWaker is the state
// service's half of that contract. It finds personas with work that no live
// writer holds — a queued input (a Messaging message, a job notification, an
// input requeued by an approval decision), an input left claimed by a writer
// whose lease expired, or a due schedule — and POSTs the core host's
// authenticated wake route for each.
//
// The database is the only record: a lost wake (the core host unreachable, a
// dropped response) is simply found again on the next sweep, so admission
// never depends on a wake landing. A persona whose pending work does not
// change between wakes is re-woken with a doubling gap, so a runtime that
// cannot make progress is not hammered.
//
// Memory preparation is the runtime's own schedule — it arms its wake from
// memory maintenance and honours its in-process "model unavailable" shelf —
// so the sweep does not chase it. It only rescues memory work left behind
// by a missed signal: a chunk overdue by MemoryRescueGrace (longer than any
// alarm drain lives) with no live writer — e.g. a persona reactivated after
// a transfer while its runtime slept, or a planned wake the platform gave
// up on during an outage. A rescue wake only makes the runtime re-plan;
// preparation itself still starts from the runtime's own alarm, subject to
// its shelf, and a memory-only persona is re-woken no sooner than
// MemoryRescueGap.

// AwaitingRuntime is one persona with work and no live writer lease.
type AwaitingRuntime struct {
	PersonaID string
	// Progress changes when the persona's pending work moves forward; an
	// unchanged value across sweeps means the previous wake achieved nothing.
	Progress string
	// MemoryOnly: the only work is an overdue memory rescue.
	MemoryOnly bool
}

// MemoryRescueGrace is how long memory work may sit overdue before the
// sweep treats its wake as lost: longer than an alarm drain's lifetime, so
// a runtime working on schedule is never rescued.
const MemoryRescueGrace = 15 * time.Minute

// A rescue can resume a durable branch. Sealed ranges without an actual
// parent snapshot wait for the next main consultation; an alarm cannot invent
// that snapshot. Indefinitely paused branches are visible but do not spin.
const memoryRescueSQL = `EXISTS (SELECT 1 FROM core_memory_branches mb
 JOIN core_memory_chunks mc USING(persona_id,chunk_seq)
 WHERE mb.persona_id=p.persona_id AND mc.status='preparing'
 AND ((mb.status='running' AND mb.updated_at<=now()-$3::interval)
 OR (mb.status='paused' AND mb.retry_at<=now()-$3::interval)))`

// PersonasAwaitingRuntime lists active personas whose work needs a runtime
// and whose writer lease is absent or expired: due inputs and schedules, and
// memory work overdue past MemoryRescueGrace.
//
// after is a rotation cursor: rows with persona_id > after sort first, then
// the result wraps to the smallest ids. The caller advances the cursor to
// the last returned id ("" restarts from the beginning), so a bounded
// result walks the whole awaiting set over consecutive calls instead of
// always returning the same oldest prefix — application-level backoff can
// then never starve later personas, however long a batch takes.
func (s *Store) PersonasAwaitingRuntime(ctx context.Context, limit int, after string) ([]AwaitingRuntime, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT p.persona_id::text,
			COALESCE((SELECT min(i.admission_seq) FROM core_inputs i
				WHERE i.persona_id = p.persona_id AND i.status IN ('queued','claimed')), 0),
			(SELECT count(*) FROM core_inputs i
				WHERE i.persona_id = p.persona_id AND i.status IN ('queued','claimed')),
			(SELECT count(*) FROM core_schedules sc
				WHERE sc.persona_id = p.persona_id AND sc.status = 'pending' AND sc.wake_at <= now()),
			w.work,
			(SELECT count(*) FILTER (WHERE mc.status = 'sealed') || ':' || count(*) FILTER (WHERE mc.status = 'preparing')
				FROM core_memory_chunks mc WHERE mc.persona_id = p.persona_id)
		FROM core_personas p
		CROSS JOIN LATERAL (SELECT
			EXISTS (SELECT 1 FROM core_inputs i
				WHERE i.persona_id = p.persona_id
				  AND ((i.status = 'queued' AND (i.not_before IS NULL OR i.not_before <= now()))
				       OR i.status = 'claimed'))
			OR EXISTS (SELECT 1 FROM core_schedules sc
				WHERE sc.persona_id = p.persona_id AND sc.status = 'pending' AND sc.wake_at <= now()) AS work) w
		WHERE p.authority = 'active'
		  AND (w.work OR `+memoryRescueSQL+`)
		  AND NOT EXISTS (SELECT 1 FROM core_writer_leases l
				WHERE l.persona_id = p.persona_id AND l.expires_at > now())
		ORDER BY (p.persona_id > $2::text) DESC, p.persona_id
		LIMIT $1`, limit, after, MemoryRescueGrace.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AwaitingRuntime
	for rows.Next() {
		var id, memory string
		var oldest, pending, due int64
		var work bool
		if err := rows.Scan(&id, &oldest, &pending, &due, &work, &memory); err != nil {
			return nil, err
		}
		out = append(out, AwaitingRuntime{PersonaID: id,
			Progress:   fmt.Sprintf("%d/%d/%d/%s", oldest, pending, due, memory),
			MemoryOnly: !work})
	}
	return out, rows.Err()
}

// NextWork is when a persona next needs a runtime for work recorded here.
// NextWorkAt is nil when nothing waits; otherwise it is never before Now,
// the database clock, so a host can arm its own clock relative to Now.
type NextWork struct {
	NextWorkAt *time.Time `json:"next_work_at"`
	Now        time.Time  `json:"now"`
}

// NextWorkFor is the runtime's own view of the work PersonasAwaitingRuntime
// sweeps for, so a core host can sleep until it is needed instead of
// polling: the earliest of a queued input's not_before (now when unset), a
// pending schedule's wake_at, and — for an input left claimed by an
// interrupted turn — the expiry of the live writer lease (now when none,
// where the next start's recovery requeues it). With no writer lease live,
// NextWorkAt <= Now exactly when the sweep would select the persona.
//
// Waiting inputs are excluded: an approval decision requeues its input,
// which the sweep then finds. Memory preparation is excluded too — its
// timing comes from memory maintenance, and only the runtime knows whether
// its model layer is currently usable. An inactive persona has no work
// here; a missing one is ErrPersonaNotFound.
func (s *Store) NextWorkFor(ctx context.Context, personaID string) (NextWork, error) {
	var out NextWork
	var authority string
	var next *time.Time
	err := s.pool.QueryRow(ctx, `
		SELECT p.authority, now(), LEAST(
			(SELECT min(COALESCE(i.not_before, now())) FROM core_inputs i
				WHERE i.persona_id = p.persona_id AND i.status = 'queued'),
			(SELECT CASE WHEN EXISTS (SELECT 1 FROM core_inputs i
					WHERE i.persona_id = p.persona_id AND i.status = 'claimed')
				THEN COALESCE((SELECT l.expires_at FROM core_writer_leases l
					WHERE l.persona_id = p.persona_id AND l.expires_at > now()), now())
				END),
			(SELECT min(sc.wake_at) FROM core_schedules sc
				WHERE sc.persona_id = p.persona_id AND sc.status = 'pending'))
		FROM core_personas p WHERE p.persona_id = $1`, personaID).
		Scan(&authority, &out.Now, &next)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, ErrPersonaNotFound
	}
	if err != nil {
		return out, err
	}
	if authority != "active" || next == nil {
		return out, nil
	}
	if next.Before(out.Now) {
		next = &out.Now
	}
	out.NextWorkAt = next
	return out, nil
}

// RuntimeWaker wakes a remote core host for personas awaiting a runtime.
type RuntimeWaker struct {
	store  *Store
	base   string
	token  string
	client *http.Client

	// Interval between sweeps; MinGap/MaxGap bound re-waking a persona whose
	// pending work did not move. MemoryGap is the least gap before re-waking
	// a persona whose only work is a memory rescue: its runtime may be
	// deliberately resting on a shelf the database cannot see.
	Interval  time.Duration
	MinGap    time.Duration
	MaxGap    time.Duration
	MemoryGap time.Duration

	mu    sync.Mutex
	marks map[string]wakeMark
	// cursor is the rotation position handed to PersonasAwaitingRuntime;
	// only Sweep touches it, so it needs no lock.
	cursor string
}

type wakeMark struct {
	at       time.Time
	progress string
	gap      time.Duration
	failed   bool
}

const (
	// SUMI_CORE_WAKE_URL is the core host's base URL; POST
	// {url}/personas/{id}/wake is the wake route.
	RuntimeWakeURLEnv = "SUMI_CORE_WAKE_URL"
	// SUMI_CORE_WAKE_TOKEN is the bearer the core host requires on wake.
	RuntimeWakeTokenEnv = "SUMI_CORE_WAKE_TOKEN"
	// SUMI_CORE_RUNTIME_TOKEN is the bearer a core host presents here for
	// every persona's persona-scoped routes (see Server.SetRuntimeToken).
	RuntimeTokenEnv = "SUMI_CORE_RUNTIME_TOKEN"

	minRuntimeSecretLen = 32
	wakeConcurrency     = 8
)

// RuntimeWakerFromEnv returns nil when no wake URL is configured (a Local
// placement, where the Node host polls). The URL and token must be set
// together.
func RuntimeWakerFromEnv(store *Store, getenv func(string) string) (*RuntimeWaker, error) {
	raw := strings.TrimSpace(getenv(RuntimeWakeURLEnv))
	token := strings.TrimSpace(getenv(RuntimeWakeTokenEnv))
	if raw == "" && token == "" {
		return nil, nil
	}
	if raw == "" || token == "" {
		return nil, fmt.Errorf("%s and %s must be set together", RuntimeWakeURLEnv, RuntimeWakeTokenEnv)
	}
	return NewRuntimeWaker(store, raw, token)
}

func NewRuntimeWaker(store *Store, baseURL, token string) (*RuntimeWaker, error) {
	u, err := url.Parse(baseURL)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		// The message must not echo the URL: a userinfo password in it is
		// exactly the secret this check exists to keep out of logs.
		return nil, fmt.Errorf("%s must be an absolute http(s) URL without query, fragment or userinfo", RuntimeWakeURLEnv)
	}
	if len(token) < minRuntimeSecretLen {
		return nil, fmt.Errorf("%s must be at least %d characters", RuntimeWakeTokenEnv, minRuntimeSecretLen)
	}
	return &RuntimeWaker{
		store: store,
		base:  strings.TrimRight(u.String(), "/"),
		token: token,
		client: &http.Client{
			Timeout: 10 * time.Second,
			// The wake route answers directly; a redirect would carry the
			// bearer somewhere this configuration did not name.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		Interval:  time.Second,
		MinGap:    5 * time.Second,
		MaxGap:    5 * time.Minute,
		MemoryGap: 30 * time.Minute,
		marks:     map[string]wakeMark{},
	}, nil
}

// Target is the configured core host base URL (for startup logs).
func (w *RuntimeWaker) Target() string { return w.base }

// Run sweeps until ctx ends.
func (w *RuntimeWaker) Run(ctx context.Context) {
	for {
		w.Sweep(ctx)
		timer := time.NewTimer(w.Interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// Sweep runs one pass and returns how many wakes it sent successfully.
func (w *RuntimeWaker) Sweep(ctx context.Context) int {
	now := time.Now()
	const limit = 200
	awaiting, err := w.store.PersonasAwaitingRuntime(ctx, limit, w.cursor)
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("core wake: pending-work query failed: %v", err)
		}
		return 0
	}
	// Advance the rotation cursor. A short page or a last row at or below
	// the old cursor means the result wrapped past the end — the whole
	// awaiting set was covered this rotation, so restart at the beginning.
	// Otherwise the next sweep continues after the last id returned.
	// Skipped (in-gap) rows still advance the cursor: fairness lives in
	// selection, so a stalled prefix cannot hold the head of the queue.
	if n := len(awaiting); n == 0 || n < limit || awaiting[n-1].PersonaID <= w.cursor {
		w.cursor = ""
	} else {
		w.cursor = awaiting[n-1].PersonaID
	}
	w.mu.Lock()
	seen := make(map[string]bool, len(awaiting))
	var due []AwaitingRuntime
	for _, a := range awaiting {
		seen[a.PersonaID] = true
		m, ok := w.marks[a.PersonaID]
		if ok && m.progress == a.Progress && now.Sub(m.at) < m.gap {
			continue
		}
		gap := w.MinGap
		if ok && m.progress == a.Progress {
			gap = min(m.gap*2, w.MaxGap)
		}
		if a.MemoryOnly {
			gap = max(gap, w.MemoryGap)
		}
		w.marks[a.PersonaID] = wakeMark{at: now, progress: a.Progress, gap: gap, failed: m.failed}
		due = append(due, a)
	}
	// A persona that left the page is either out of the rotation window or
	// done working — the bounded result cannot tell which. Its mark is kept
	// while its gap runs (still enforcing backoff when the cursor returns)
	// and dropped once the gap has passed, where the persona was due anyway;
	// a fresh mark just restarts the doubling.
	for id, m := range w.marks {
		if !seen[id] && now.Sub(m.at) >= m.gap {
			delete(w.marks, id)
		}
	}
	w.mu.Unlock()

	var sent int
	var wg sync.WaitGroup
	var countMu sync.Mutex
	sem := make(chan struct{}, wakeConcurrency)
	for _, a := range due {
		wg.Add(1)
		sem <- struct{}{}
		go func(personaID string) {
			defer wg.Done()
			defer func() { <-sem }()
			err := w.wake(ctx, personaID)
			w.mu.Lock()
			m, ok := w.marks[personaID]
			wasFailed := ok && m.failed
			if ok {
				m.failed = err != nil
				w.marks[personaID] = m
			}
			w.mu.Unlock()
			switch {
			case err == nil:
				countMu.Lock()
				sent++
				countMu.Unlock()
				if wasFailed {
					log.Printf("core wake: persona %s woken again after failures", personaID)
				}
			case ctx.Err() == nil:
				log.Printf("core wake: persona %s not woken (will retry): %v", personaID, err)
			}
		}(a.PersonaID)
	}
	wg.Wait()
	return sent
}

func (w *RuntimeWaker) wake(ctx context.Context, personaID string) error {
	if !uuidv7Re.MatchString(personaID) {
		return errors.New("persona id is not a uuidv7")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.base+"/personas/"+personaID+"/wake", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+w.token)
	res, err := w.client.Do(req)
	if err != nil {
		// url.Error carries only the URL, never the header.
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		// The body names why the host refused (e.g. its runtime credential
		// was rejected); bound it so a hostile or buggy host cannot flood
		// the log. The bearer is never in it — it travels in the header.
		snippet, _ := io.ReadAll(io.LimitReader(res.Body, 512))
		if detail := strings.TrimSpace(string(snippet)); detail != "" {
			return fmt.Errorf("core host answered %d: %s", res.StatusCode, detail)
		}
		return fmt.Errorf("core host answered %d", res.StatusCode)
	}
	return nil
}
