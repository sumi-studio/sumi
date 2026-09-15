package agentstate

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
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

// AwaitingRuntime is one persona with work and no live writer lease.
type AwaitingRuntime struct {
	PersonaID string
	// Progress changes when the persona's pending work moves forward; an
	// unchanged value across sweeps means the previous wake achieved nothing.
	Progress string
}

// PersonasAwaitingRuntime lists active personas whose work needs a runtime
// and whose writer lease is absent or expired.
func (s *Store) PersonasAwaitingRuntime(ctx context.Context, limit int) ([]AwaitingRuntime, error) {
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
				WHERE sc.persona_id = p.persona_id AND sc.status = 'pending' AND sc.wake_at <= now())
		FROM core_personas p
		WHERE p.authority = 'active'
		  AND (EXISTS (SELECT 1 FROM core_inputs i
				WHERE i.persona_id = p.persona_id
				  AND ((i.status = 'queued' AND (i.not_before IS NULL OR i.not_before <= now()))
				       OR i.status = 'claimed'))
		    OR EXISTS (SELECT 1 FROM core_schedules sc
				WHERE sc.persona_id = p.persona_id AND sc.status = 'pending' AND sc.wake_at <= now()))
		  AND NOT EXISTS (SELECT 1 FROM core_writer_leases l
				WHERE l.persona_id = p.persona_id AND l.expires_at > now())
		ORDER BY p.persona_id
		LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AwaitingRuntime
	for rows.Next() {
		var id string
		var oldest, pending, due int64
		if err := rows.Scan(&id, &oldest, &pending, &due); err != nil {
			return nil, err
		}
		out = append(out, AwaitingRuntime{PersonaID: id, Progress: fmt.Sprintf("%d/%d/%d", oldest, pending, due)})
	}
	return out, rows.Err()
}

// RuntimeWaker wakes a remote core host for personas awaiting a runtime.
type RuntimeWaker struct {
	store  *Store
	base   string
	token  string
	client *http.Client

	// Interval between sweeps; MinGap/MaxGap bound re-waking a persona whose
	// pending work did not move.
	Interval time.Duration
	MinGap   time.Duration
	MaxGap   time.Duration

	mu    sync.Mutex
	marks map[string]wakeMark
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
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("%s must be an absolute http(s) URL without query", RuntimeWakeURLEnv)
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
		Interval: time.Second,
		MinGap:   5 * time.Second,
		MaxGap:   5 * time.Minute,
		marks:    map[string]wakeMark{},
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
	awaiting, err := w.store.PersonasAwaitingRuntime(ctx, 200)
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("core wake: pending-work query failed: %v", err)
		}
		return 0
	}
	now := time.Now()
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
		w.marks[a.PersonaID] = wakeMark{at: now, progress: a.Progress, gap: gap, failed: m.failed}
		due = append(due, a)
	}
	// A persona that left the list got a live writer or finished its work:
	// its next pending work is woken immediately.
	for id := range w.marks {
		if !seen[id] {
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
		return fmt.Errorf("core host answered %d", res.StatusCode)
	}
	return nil
}
