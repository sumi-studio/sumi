package agentstate

// Durable interactive terminal sessions — the session/input/output
// half of the shared terminal. Same claim-epoch fencing as
// core_jobs/call_sessions:
//
//   - A session is durable before it is executed: terminal.open or the
//     human attach creates a 'requested' row; a backend runner claims
//     it under (claimed_by, claim_expires_at, epoch).
//   - Claim expiry sweeps the session to 'interrupted' — reclaimable,
//     never silently dead. The container belongs to the provisioner
//     and may still be running; the next claim re-attaches to it.
//   - Accepted input is durable before delivery (core_terminal_inputs,
//     seq-ordered). On a claim bump, rows still 'intended' re-stamp to
//     the new epoch (they provably never left the DB — deliverable);
//     rows 'dequeued' mark 'unknown' (maybe delivered — never resent).
//   - Output is append-only chunks at absolute provisioner offsets
//     (core_terminal_output). Replayed drains dedupe on
//     (session_id, base, kind); the runner inserts 'gap' rows where
//     retained output skipped ahead.
//   - Input queues on every recoverable status (requested, claimed,
//     active, interrupted) — the ledger is durable, epoch fencing
//     restamps still-'intended' rows to the next claimant. Only a
//     closing or ended session refuses new input.
//   - 'ending' survives a claim sweep as 'ending' — the close intent
//     is durable; the next claim's runner finishes the physical stop
//     instead of resurrecting a session the user already closed.
//   - A held human control lease refuses agent-originated input at
//     admission ('control_held') rather than silently dropping it.

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrTerminalNotFound   = errors.New("terminal session not found")
	ErrTerminalNotClaimed = errors.New("terminal session is not claimed by this runner or epoch")
	ErrTerminalNotLive    = errors.New("terminal session has no live claim")
	ErrTerminalEnded      = errors.New("terminal session has ended")
	ErrTerminalControl    = errors.New("terminal session is under exclusive human control")
	ErrTerminalCapacity   = errors.New("terminal session capacity exhausted")
	ErrTerminalBackend    = errors.New("terminal backend is not running in this deployment")
)

// Terminal bounds: sessions per persona, serving scrollback, input
// payload, control lease. The scrollback cap is the product surface —
// the provisioner's own ring is deliberately larger (crash buffer).
const (
	terminalMaxSessions    = 4
	terminalScrollbackCap  = 4 << 20
	terminalScrollbackKeep = 3 << 20
	terminalMaxInputBytes  = 64 << 10
	// Matches the documented exclusive-control lease the person routes
	// request (terminalHumanControlLease = 5m); the clamp bounds callers,
	// it must not silently shorten the documented hold.
	terminalMaxControlSecs = 300
	terminalMaxName        = 80
	// ListTerminalInputs page bound — the ledger is read-back, not a
	// delivery queue, so one page is generous for humans and tools.
	terminalInputListMax = 200
)

var terminalSignalAllowlist = map[string]bool{
	"INT": true, "TERM": true, "HUP": true, "QUIT": true,
	"KILL": true, "TSTP": true, "USR1": true, "USR2": true,
}

type TerminalSession struct {
	SessionID      string     `json:"session_id"`
	PersonaID      string     `json:"persona_id"`
	Name           string     `json:"name"`
	Mode           string     `json:"mode"`
	Backend        string     `json:"backend"`
	OperationID    string     `json:"operation_id,omitempty"`
	Status         string     `json:"status"`
	Epoch          int64      `json:"epoch"`
	ClaimedBy      string     `json:"claimed_by,omitempty"`
	ClaimExpiresAt *time.Time `json:"claim_expires_at,omitempty"`
	ControlHolder  string     `json:"control_holder,omitempty"`
	ControlUntil   *time.Time `json:"control_until,omitempty"`
	RequestedBy    string     `json:"requested_by"`
	CreatedBy      string     `json:"created_by"`
	ExitCode       *int       `json:"exit_code,omitempty"`
	ExitSignal     string     `json:"exit_signal,omitempty"`
	EndReason      string     `json:"end_reason,omitempty"`
	OutputBytes    int64      `json:"output_bytes"`
	OutputBase     int64      `json:"output_base"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	EndedAt        *time.Time `json:"ended_at,omitempty"`
}

const terminalSessionCols = `session_id, persona_id, name, mode, backend,
	COALESCE(operation_id, ''), status, epoch, COALESCE(claimed_by, ''),
	claim_expires_at, COALESCE(control_holder, ''), control_until,
	requested_by, created_by, exit_code, COALESCE(exit_signal, ''),
	COALESCE(end_reason, ''), output_bytes, output_base,
	created_at, updated_at, ended_at`

func scanTerminalSession(row pgx.Row) (TerminalSession, error) {
	var t TerminalSession
	err := row.Scan(&t.SessionID, &t.PersonaID, &t.Name, &t.Mode, &t.Backend,
		&t.OperationID, &t.Status, &t.Epoch, &t.ClaimedBy, &t.ClaimExpiresAt,
		&t.ControlHolder, &t.ControlUntil, &t.RequestedBy, &t.CreatedBy,
		&t.ExitCode, &t.ExitSignal, &t.EndReason, &t.OutputBytes, &t.OutputBase,
		&t.CreatedAt, &t.UpdatedAt, &t.EndedAt)
	return t, err
}

type TerminalInput struct {
	InputID      string         `json:"input_id"`
	SessionID    string         `json:"session_id"`
	SessionEpoch int64          `json:"session_epoch"`
	Seq          int64          `json:"seq"`
	Kind         string         `json:"kind"`
	Payload      map[string]any `json:"payload"`
	Source       string         `json:"source"`
	Status       string         `json:"status"`
	Detail       map[string]any `json:"detail,omitempty"`
	CreatedAt    time.Time      `json:"created_at"`
	UpdatedAt    time.Time      `json:"updated_at"`
}

type TerminalOutputChunk struct {
	SessionID string    `json:"session_id"`
	Seq       int64     `json:"seq"`
	Kind      string    `json:"kind"` // "data" | "gap"
	Base      int64     `json:"base"`
	GapTo     *int64    `json:"gap_to,omitempty"`
	Data      []byte    `json:"data,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

func terminalLive(status string) bool {
	return status == "requested" || status == "claimed" || status == "active" || status == "ending" || status == "interrupted"
}

func terminalClaimed(status string) bool {
	return status == "claimed" || status == "active" || status == "ending"
}

// createTerminalSessionTx inserts a 'requested' session under the
// persona authority share-lock — the same fence SubmitJob uses, so a
// transfer seal can never let a session slip between the authority
// check and the commit.
func (s *Store) createTerminalSessionTx(ctx context.Context, tx pgx.Tx, personaID, name, requestedBy, createdBy string) (TerminalSession, error) {
	if len(name) > terminalMaxName {
		return TerminalSession{}, fmt.Errorf("%w: terminal name too long", ErrBadRequest)
	}
	backend := s.defaultTerminalBackend
	if backend == "" {
		backend = "local"
	}
	// A session on a backend with no verified runner would queue
	// forever — refuse honestly at admission, the same gate jobs use.
	if !s.terminalBackendAvailable[backend] {
		return TerminalSession{}, fmt.Errorf("%w: %q", ErrTerminalBackend, backend)
	}
	var authority string
	err := tx.QueryRow(ctx,
		`SELECT authority FROM core_personas WHERE persona_id = $1 FOR SHARE`,
		personaID).Scan(&authority)
	if errors.Is(err, pgx.ErrNoRows) {
		return TerminalSession{}, ErrPersonaNotFound
	}
	if err != nil {
		return TerminalSession{}, dataErr(err)
	}
	if authority != "active" {
		return TerminalSession{}, inactiveAuthorityError(authority)
	}
	var live int
	if err := tx.QueryRow(ctx,
		`SELECT count(*) FROM core_terminal_sessions
		 WHERE persona_id = $1 AND status IN
		 ('requested','claimed','active','ending','interrupted')`,
		personaID).Scan(&live); err != nil {
		return TerminalSession{}, dataErr(err)
	}
	if live >= terminalMaxSessions {
		return TerminalSession{}, ErrTerminalCapacity
	}
	sessionID := uuid.Must(uuid.NewV7()).String()
	return scanTerminalSession(tx.QueryRow(ctx, `
		INSERT INTO core_terminal_sessions
			(session_id, persona_id, name, mode, backend, status,
			 requested_by, created_by)
		VALUES ($1::uuidv7, $2::uuidv7, $3, 'pty', $4, 'requested', $5, $6)
		RETURNING `+terminalSessionCols,
		sessionID, personaID, name, backend, requestedBy, createdBy))
}

// CreateTerminalSession is the human-facing session create (the
// terminal app's "new session"); the secretary path goes through the
// terminal.open tool effect instead.
func (s *Store) CreateTerminalSession(ctx context.Context, personaID, name, requestedBy, createdBy string) (TerminalSession, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return TerminalSession{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	t, err := s.createTerminalSessionTx(ctx, tx, personaID, name, requestedBy, createdBy)
	if err != nil {
		return TerminalSession{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return TerminalSession{}, err
	}
	return t, nil
}

func (s *Store) GetTerminalSession(ctx context.Context, personaID, sessionID string) (TerminalSession, error) {
	t, err := scanTerminalSession(s.pool.QueryRow(ctx,
		`SELECT `+terminalSessionCols+` FROM core_terminal_sessions
		 WHERE persona_id = $1::uuidv7 AND session_id = $2::uuidv7`,
		personaID, sessionID))
	if errors.Is(err, pgx.ErrNoRows) {
		return TerminalSession{}, ErrTerminalNotFound
	}
	return t, err
}

func (s *Store) ListTerminalSessions(ctx context.Context, personaID string) ([]TerminalSession, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+terminalSessionCols+` FROM core_terminal_sessions
		 WHERE persona_id = $1::uuidv7 ORDER BY created_at`, personaID)
	if err != nil {
		return nil, dataErr(err)
	}
	defer rows.Close()
	var out []TerminalSession
	for rows.Next() {
		t, err := scanTerminalSession(rows)
		if err != nil {
			return nil, dataErr(err)
		}
		out = append(out, t)
	}
	return out, dataErr(rows.Err())
}

// lockTerminalSessionTx loads a session FOR UPDATE inside a mutation tx.
func lockTerminalSessionTx(ctx context.Context, tx pgx.Tx, personaID, sessionID string) (TerminalSession, error) {
	t, err := scanTerminalSession(tx.QueryRow(ctx,
		`SELECT `+terminalSessionCols+` FROM core_terminal_sessions
		 WHERE persona_id = $1::uuidv7 AND session_id = $2::uuidv7 FOR UPDATE`,
		personaID, sessionID))
	if errors.Is(err, pgx.ErrNoRows) {
		return TerminalSession{}, ErrTerminalNotFound
	}
	if err != nil {
		return TerminalSession{}, dataErr(err)
	}
	return t, nil
}

// submitTerminalInputTx commits one input row. source is 'agent' or
// 'human'; a held human control lease refuses agent input at
// admission. Input queues on every recoverable status — 'requested',
// 'claimed', 'active', and 'interrupted' — because the ledger is
// durable and a (re)claim delivers it; the epoch fencing in
// claimTerminalSessionsTx restamps still-'intended' rows to the new
// claimant. Only a session that is closing or already ended refuses.
func (s *Store) submitTerminalInputTx(ctx context.Context, tx pgx.Tx, personaID, sessionID, source, kind string, payload map[string]any) (TerminalInput, error) {
	t, err := lockTerminalSessionTx(ctx, tx, personaID, sessionID)
	if err != nil {
		return TerminalInput{}, err
	}
	switch t.Status {
	case "ended", "lost":
		return TerminalInput{}, ErrTerminalEnded
	case "requested", "claimed", "active", "interrupted":
		// queueable: a live or recoverable claim delivers it
	default:
		return TerminalInput{}, ErrTerminalNotLive
	}
	if source == "agent" && t.ControlHolder == "human" && t.ControlUntil != nil && t.ControlUntil.After(time.Now()) {
		return TerminalInput{}, ErrTerminalControl
	}
	if err := validateTerminalInput(kind, payload); err != nil {
		return TerminalInput{}, err
	}
	inputID := uuid.Must(uuid.NewV7()).String()
	var in TerminalInput
	err = tx.QueryRow(ctx, `
		INSERT INTO core_terminal_inputs
			(input_id, session_id, session_epoch, seq, kind, payload, source, status)
		SELECT $1::uuidv7, $2::uuidv7, $3,
			COALESCE(MAX(seq), 0) + 1, $4, $5, $6, 'intended'
		FROM core_terminal_inputs WHERE session_id = $2::uuidv7
		RETURNING input_id, session_id, session_epoch, seq, kind, payload, source, status, created_at, updated_at`,
		inputID, sessionID, t.Epoch, kind, payload, source).
		Scan(&in.InputID, &in.SessionID, &in.SessionEpoch, &in.Seq, &in.Kind, &in.Payload, &in.Source, &in.Status, &in.CreatedAt, &in.UpdatedAt)
	if err != nil {
		return TerminalInput{}, dataErr(err)
	}
	return in, nil
}

func validateTerminalInput(kind string, payload map[string]any) error {
	switch kind {
	case "stdin":
		data, _ := payload["data"].(string)
		if data == "" {
			return fmt.Errorf("%w: stdin input requires data", ErrBadRequest)
		}
		if len(data) > terminalMaxInputBytes {
			return fmt.Errorf("%w: stdin input exceeds %d bytes", ErrBadRequest, terminalMaxInputBytes)
		}
	case "resize":
		cols, cok := payload["cols"].(float64)
		rows, rok := payload["rows"].(float64)
		if !cok || !rok || cols < 2 || cols > 1000 || rows < 2 || rows > 500 {
			return fmt.Errorf("%w: resize requires cols 2..1000 and rows 2..500", ErrBadRequest)
		}
	case "signal":
		sig, _ := payload["signal"].(string)
		if !terminalSignalAllowlist[sig] {
			return fmt.Errorf("%w: signal not permitted", ErrBadRequest)
		}
	case "eof":
	default:
		return fmt.Errorf("%w: unknown input kind", ErrBadRequest)
	}
	return nil
}

// SubmitTerminalInput is the human-side input path; the agent path is
// the terminal.write/resize/signal tool effects, which funnel here.
func (s *Store) SubmitTerminalInput(ctx context.Context, personaID, sessionID, source, kind string, payload map[string]any) (TerminalInput, error) {
	if source != "agent" && source != "human" {
		return TerminalInput{}, fmt.Errorf("%w: invalid input source", ErrBadRequest)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return TerminalInput{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	in, err := s.submitTerminalInputTx(ctx, tx, personaID, sessionID, source, kind, payload)
	if err != nil {
		return TerminalInput{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return TerminalInput{}, err
	}
	return in, nil
}

// SetTerminalControl takes or releases the exclusive human control
// lease. While held, agent-originated input is refused at admission
// ('control_held') — the human explicitly owns the keyboard.
func (s *Store) SetTerminalControl(ctx context.Context, personaID, sessionID string, hold bool, lease time.Duration) (TerminalSession, error) {
	if lease <= 0 || lease > terminalMaxControlSecs*time.Second {
		lease = terminalMaxControlSecs * time.Second
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return TerminalSession{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	t, err := lockTerminalSessionTx(ctx, tx, personaID, sessionID)
	if err != nil {
		return TerminalSession{}, err
	}
	if t.Status == "ended" || t.Status == "lost" {
		return TerminalSession{}, ErrTerminalEnded
	}
	if hold {
		_, err = tx.Exec(ctx, `
			UPDATE core_terminal_sessions
			SET control_holder = 'human', control_until = now() + $3::interval,
				updated_at = now()
			WHERE persona_id = $1 AND session_id = $2`,
			personaID, sessionID, fmt.Sprintf("%d seconds", int(lease.Seconds())))
	} else {
		_, err = tx.Exec(ctx, `
			UPDATE core_terminal_sessions
			SET control_holder = NULL, control_until = NULL, updated_at = now()
			WHERE persona_id = $1 AND session_id = $2`,
			personaID, sessionID)
	}
	if err != nil {
		return TerminalSession{}, dataErr(err)
	}
	out, err := lockTerminalSessionTx(ctx, tx, personaID, sessionID)
	if err != nil {
		return TerminalSession{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return TerminalSession{}, err
	}
	return out, nil
}

// CloseTerminalSession requests session end: the owning runner kills
// the container and reports the terminal outcome. A session with no
// live claim ends immediately — nothing is left to stop, which is a
// definite fact, not a guess.
func (s *Store) CloseTerminalSession(ctx context.Context, personaID, sessionID, reason string) (TerminalSession, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return TerminalSession{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	t, err := lockTerminalSessionTx(ctx, tx, personaID, sessionID)
	if err != nil {
		return TerminalSession{}, err
	}
	if reason == "" {
		reason = "closed"
	}
	if t.Status == "ended" || t.Status == "lost" {
		if err := tx.Commit(ctx); err != nil {
			return TerminalSession{}, err
		}
		return t, nil
	}
	liveClaim := terminalClaimed(t.Status) && t.ClaimExpiresAt != nil && t.ClaimExpiresAt.After(time.Now())
	if liveClaim {
		_, err = tx.Exec(ctx, `
			UPDATE core_terminal_sessions
			SET status = 'ending', updated_at = now()
			WHERE persona_id = $1 AND session_id = $2`,
			personaID, sessionID)
		if err != nil {
			return TerminalSession{}, dataErr(err)
		}
		t.Status = "ending"
		if err := tx.Commit(ctx); err != nil {
			return TerminalSession{}, err
		}
		return t, nil
	}
	// No live claim. 'requested' never had a runner — nothing exists to
	// stop, so ending it is a definite fact. But 'interrupted' and
	// dead-lease 'claimed'/'active' rows may still have a live
	// provisioner op (claim lapse does not stop containers). Stamping
	// 'ended' would certify a stop that never ran AND remove the
	// session from every claimable state — the physical stop intent
	// would be dropped for good. Instead stamp 'ending' with the claim
	// cleared: 'ending AND claimed_by IS NULL' is claimable, so the
	// next claim's runner re-attaches and performs the real stop.
	if t.Status == "requested" {
		if err := s.endTerminalSessionTx(ctx, tx, &t, "ended", reason, nil, ""); err != nil {
			return TerminalSession{}, err
		}
	} else {
		if _, err := tx.Exec(ctx, `
			UPDATE core_terminal_sessions
			SET status = 'ending', claimed_by = NULL, claim_expires_at = NULL, updated_at = now()
			WHERE persona_id = $1 AND session_id = $2`,
			personaID, sessionID); err != nil {
			return TerminalSession{}, dataErr(err)
		}
		t.Status = "ending"
		t.ClaimedBy = ""
		t.ClaimExpiresAt = nil
	}
	if err := tx.Commit(ctx); err != nil {
		return TerminalSession{}, err
	}
	return t, nil
}

// endTerminalSessionTx performs the terminal transition and its
// exactly-once notification in one transaction.
func (s *Store) endTerminalSessionTx(ctx context.Context, tx pgx.Tx, t *TerminalSession, status, reason string, exitCode *int, exitSignal string) error {
	now := time.Now()
	_, err := tx.Exec(ctx, `
		UPDATE core_terminal_sessions
		SET status = $3, end_reason = $4, exit_code = $5, exit_signal = $6,
			ended_at = $7, updated_at = $7, claimed_by = NULL, claim_expires_at = NULL
		WHERE persona_id = $1 AND session_id = $2`,
		t.PersonaID, t.SessionID, status, reason, exitCode, exitSignal, now)
	if err != nil {
		return dataErr(err)
	}
	// 'intended' rows provably never left the ledger — 'expired' is a
	// definite fact. 'dequeued' rows are indeterminate by definition:
	// the runner may already have written the bytes before it died —
	// the same 'unknown' the claim-bump path records, never resent.
	if _, err := tx.Exec(ctx, `
		UPDATE core_terminal_inputs
		SET status = 'expired', updated_at = now()
		WHERE session_id = $1 AND status = 'intended'`,
		t.SessionID); err != nil {
		return dataErr(err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE core_terminal_inputs
		SET status = 'unknown', updated_at = now()
		WHERE session_id = $1 AND status = 'dequeued'`,
		t.SessionID); err != nil {
		return dataErr(err)
	}
	t.Status = status
	t.EndReason = reason
	t.ExitCode = exitCode
	t.ExitSignal = exitSignal
	t.EndedAt = &now
	t.ClaimedBy = ""
	t.ClaimExpiresAt = nil
	return s.notifyTerminalEndedTx(ctx, tx, t)
}

// notifyTerminalEndedTx enqueues the session's terminal notification
// input exactly once, so a secretary that was not watching still
// learns its terminal ended (and why).
func (s *Store) notifyTerminalEndedTx(ctx context.Context, tx pgx.Tx, t *TerminalSession) error {
	payload := map[string]any{
		"session_id": t.SessionID,
		"status":     t.Status,
		"end_reason": t.EndReason,
		"text": fmt.Sprintf("Terminal session %q ended (%s).",
			t.Name, t.EndReason),
	}
	if t.ExitCode != nil {
		payload["exit_code"] = *t.ExitCode
	}
	if t.ExitSignal != "" {
		payload["exit_signal"] = t.ExitSignal
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO core_inputs (persona_id, input_id, kind, payload, actor_kind, actor_id, source_surface, attention, status)
		VALUES ($1::uuidv7, $2, 'terminal_ended', $3, 'terminal', $4, 'core_terminal_sessions', 'reply', 'queued')
		ON CONFLICT (persona_id, input_id) DO NOTHING`,
		t.PersonaID, "terminal:"+t.SessionID, payload, t.SessionID)
	if err != nil {
		return fmt.Errorf("notify terminal: %w", dataErr(err))
	}
	return nil
}

// ReadTerminalOutput serves scrollback to humans and the secretary.
// The cursor is an absolute emitted offset: below output_base the
// caller observes an explicit gap, never silent truncation.
//
// Loss-event markers are zero-width gap rows (kind='gap' with
// gap_to = base) naming a journal-loss boundary whose lost byte
// count is unknown. They carry no byte range, so byte progress
// alone cannot express whether a reader has already consumed one:
// end > cursor hides them at a caught-up cursor while end >= cursor
// would replay them forever. The reader therefore tracks a second
// progress dimension — eventCursor, a chunk-seq high-water mark.
// Markers are served exactly when seq > eventCursor; the response's
// EventCursor is echoed back on the next read to consume each
// notification once. An omitted/zero eventCursor means a fresh
// reader: every marker is served once, then suppressed.
type TerminalOutputRead struct {
	Session     TerminalSession       `json:"session"`
	Base        int64                 `json:"base"`
	Cursor      int64                 `json:"cursor"`
	NextCursor  int64                 `json:"next_cursor"`
	EventCursor int64                 `json:"event_cursor"`
	Chunks      []TerminalOutputChunk `json:"chunks"`
	Gap         bool                  `json:"gap"`
	EOF         bool                  `json:"eof"`
}

func (s *Store) ReadTerminalOutput(ctx context.Context, personaID, sessionID string, cursor, eventCursor int64, limit int) (TerminalOutputRead, error) {
	if cursor < 0 {
		cursor = 0
	}
	if eventCursor < 0 {
		eventCursor = 0
	}
	if limit <= 0 || limit > 64 {
		limit = 32
	}
	t, err := s.GetTerminalSession(ctx, personaID, sessionID)
	if err != nil {
		return TerminalOutputRead{}, err
	}
	if cursor == 0 {
		// A fresh attach starts at the retained base, not at stream
		// start — replaying megabytes of scrollback on every
		// reconnect would bury the live edge.
		cursor = t.OutputBase
	}
	rows, err := s.pool.Query(ctx, `
		SELECT session_id, seq, kind, base, gap_to, data, created_at
		FROM core_terminal_output
		WHERE session_id = $1::uuidv7 AND (
			GREATEST(base + octet_length(data), COALESCE(gap_to, 0)) > $2
			OR (kind = 'gap' AND gap_to = base AND seq > $3)
		)
		ORDER BY seq LIMIT $4`,
		sessionID, cursor, eventCursor, limit+1)
	if err != nil {
		return TerminalOutputRead{}, dataErr(err)
	}
	defer rows.Close()
	var chunks []TerminalOutputChunk
	for rows.Next() {
		var c TerminalOutputChunk
		if err := rows.Scan(&c.SessionID, &c.Seq, &c.Kind, &c.Base, &c.GapTo, &c.Data, &c.CreatedAt); err != nil {
			return TerminalOutputRead{}, dataErr(err)
		}
		chunks = append(chunks, c)
	}
	if err := rows.Err(); err != nil {
		return TerminalOutputRead{}, dataErr(err)
	}
	gap := cursor < t.OutputBase
	next := cursor
	nextEvent := eventCursor
	for _, c := range chunks {
		if c.Seq > nextEvent {
			nextEvent = c.Seq
		}
		end := c.Base
		if c.Kind == "gap" && c.GapTo != nil {
			end = *c.GapTo
		} else {
			end += int64(len(c.Data))
		}
		if end > next {
			next = end
		}
	}
	eof := !terminalLive(t.Status) && next >= t.OutputBytes
	return TerminalOutputRead{
		Session: t, Base: t.OutputBase, Cursor: cursor,
		NextCursor: next, EventCursor: nextEvent, Chunks: chunks, Gap: gap, EOF: eof,
	}, nil
}

// RunnableTerminalPersonas is the driver's discovery feed: personas
// holding either claimable sessions ('requested'/'interrupted') or
// sessions still claimed by this runner (a driver restart resumes
// them under the existing epoch rather than waiting out the lease).
func (s *Store) RunnableTerminalPersonas(ctx context.Context, runnerID, backend string, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 64
	}
	// The runner identity is a logical name; each lock acquisition
	// claims under a distinct incarnation ("runner#inc"). Discovery
	// matches the logical runner so sessions owned by any incarnation
	// — current or stale — keep this persona runnable.
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT persona_id FROM core_terminal_sessions
		WHERE backend = $2
		  AND (status IN ('requested', 'interrupted')
		       OR (status = 'ending' AND claimed_by IS NULL)
		       OR ((claimed_by = $1 OR claimed_by LIKE $1 || '#%')
		           AND status IN ('claimed', 'active', 'ending')))
		ORDER BY persona_id LIMIT $3`,
		runnerID, backend, limit)
	if err != nil {
		return nil, dataErr(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, dataErr(err)
		}
		out = append(out, id)
	}
	return out, dataErr(rows.Err())
}

// OwnedTerminalSessions lists the sessions still claimed by this
// runner — the resume set after a driver restart. The heartbeat fence
// decides whether the lease is still live.
func (s *Store) OwnedTerminalSessions(ctx context.Context, personaID, runnerID string) ([]TerminalSession, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+terminalSessionCols+` FROM core_terminal_sessions
		 WHERE persona_id = $1::uuidv7 AND claimed_by = $2
		   AND status IN ('claimed', 'active', 'ending')
		 ORDER BY created_at`, personaID, runnerID)
	if err != nil {
		return nil, dataErr(err)
	}
	defer rows.Close()
	var out []TerminalSession
	for rows.Next() {
		t, err := scanTerminalSession(rows)
		if err != nil {
			return nil, dataErr(err)
		}
		out = append(out, t)
	}
	return out, dataErr(rows.Err())
}

// claimLogicalRunner extracts the stable runner identity from a
// claim id. Claims are stamped "runner#incarnation" — a fresh
// incarnation per advisory-lock acquisition — so two processes that
// legitimately share a RunnerID across a lock handoff cannot both
// hold delivery authority: the successor's claim bumps the epoch and
// re-stamps 'intended' rows, which fences every stale-incarnation
// disposition, append, heartbeat, and report.
func claimLogicalRunner(runnerID string) string {
	if i := strings.IndexByte(runnerID, '#'); i >= 0 {
		return runnerID[:i]
	}
	return runnerID
}

// ClaimTerminalSessions is the runner's periodic call: it sweeps
// expired claims to 'interrupted' (reclaimable — the container may
// still be running, only our claim lapsed) and claims up to limit
// requested/interrupted sessions, bumping each epoch so stale-epoch
// input dispositions, output appends, and status reports all fence.
//
// A session still claimed by a *stale incarnation of the same
// logical runner* is claimable by steal: the advisory lock guarantees
// the old process lost ownership before this claimer could acquire
// it, but its lease may not have lapsed and its pump may still be
// draining a cached input. Stealing re-stamps every 'intended' row
// to the new epoch and marks 'dequeued' rows 'unknown' — atomically,
// in the same transaction — so a stale pump's cached row can never
// gain fresh delivery authority and an indeterminate row is never
// re-served.
func (s *Store) ClaimTerminalSessions(ctx context.Context, personaID, runnerID, backend string, lease time.Duration, limit int) (claimed []TerminalSession, interrupted []TerminalSession, err error) {
	logical := claimLogicalRunner(runnerID)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	interrupted, err = s.sweepTerminalExpiredTx(ctx, tx, personaID)
	if err != nil {
		return nil, nil, err
	}
	if limit <= 0 {
		limit = 1
	}
	rows, err := tx.Query(ctx, `
		SELECT `+terminalSessionCols+` FROM core_terminal_sessions
		WHERE persona_id = $1 AND backend = $2
		  AND (status IN ('requested', 'interrupted')
		       OR (status = 'ending' AND claimed_by IS NULL)
		       OR (claimed_by <> $4
		           AND status IN ('claimed', 'active', 'ending')
		           AND (claimed_by = $5 OR claimed_by LIKE $5 || '#%')))
		ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT $3`,
		personaID, backend, limit, runnerID, logical)
	if err != nil {
		return nil, nil, dataErr(err)
	}
	var candidates []TerminalSession
	for rows.Next() {
		t, err := scanTerminalSession(rows)
		if err != nil {
			rows.Close()
			return nil, nil, dataErr(err)
		}
		candidates = append(candidates, t)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, nil, dataErr(err)
	}
	for _, t := range candidates {
		epoch := t.Epoch + 1
		// An 'ending' claim keeps the 'ending' status — the runner
		// that takes it goes straight to the physical stop; a
		// 'claimed' stamp would resurrect a session already closing.
		res, err := tx.Exec(ctx, `
			UPDATE core_terminal_sessions
			SET status = CASE WHEN status = 'ending' THEN 'ending' ELSE 'claimed' END,
				epoch = $3, claimed_by = $4,
				claim_expires_at = now() + $5::interval, updated_at = now()
			WHERE persona_id = $1 AND session_id = $2
			  AND (status IN ('requested', 'interrupted')
			       OR (status = 'ending' AND claimed_by IS NULL)
			       OR (claimed_by <> $4
			           AND status IN ('claimed', 'active', 'ending')
			           AND (claimed_by = $6 OR claimed_by LIKE $6 || '#%')))`,
			personaID, t.SessionID, epoch, runnerID,
			fmt.Sprintf("%d milliseconds", lease.Milliseconds()), logical)
		if err != nil {
			return nil, nil, dataErr(err)
		}
		if res.RowsAffected() == 0 {
			continue
		}
		// Epoch bump fences input delivery: rows still 'intended' were
		// provably never dequeued, so they re-stamp to the new epoch
		// and stay deliverable; rows 'dequeued' may have reached the
		// terminal already — 'unknown', never resent.
		if _, err := tx.Exec(ctx, `
			UPDATE core_terminal_inputs SET status = 'unknown', updated_at = now()
			WHERE session_id = $1 AND session_epoch < $2 AND status = 'dequeued'`,
			t.SessionID, epoch); err != nil {
			return nil, nil, dataErr(err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE core_terminal_inputs SET session_epoch = $2, updated_at = now()
			WHERE session_id = $1 AND session_epoch < $2 AND status = 'intended'`,
			t.SessionID, epoch); err != nil {
			return nil, nil, dataErr(err)
		}
		fresh, err := lockTerminalSessionTx(ctx, tx, personaID, t.SessionID)
		if err != nil {
			return nil, nil, err
		}
		claimed = append(claimed, fresh)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, nil, err
	}
	return claimed, interrupted, nil
}

// sweepTerminalExpiredTx reclaims expired claims. 'claimed'/'active'
// become 'interrupted' — a reclaimable state, NOT an end verdict;
// the container may still be running under the provisioner. 'ending'
// keeps its status and only loses the claim: the close intent is
// durable, and the next claim's runner finishes the physical stop
// instead of resurrecting a session the user already closed.
func (s *Store) sweepTerminalExpiredTx(ctx context.Context, tx pgx.Tx, personaID string) ([]TerminalSession, error) {
	rows, err := tx.Query(ctx, `
		UPDATE core_terminal_sessions
		SET status = 'interrupted', claimed_by = NULL, claim_expires_at = NULL,
			updated_at = now()
		WHERE persona_id = $1
		  AND status IN ('claimed', 'active')
		  AND claim_expires_at IS NOT NULL AND claim_expires_at <= now()
		RETURNING `+terminalSessionCols, personaID)
	if err != nil {
		return nil, dataErr(err)
	}
	var out []TerminalSession
	for rows.Next() {
		t, err := scanTerminalSession(rows)
		if err != nil {
			rows.Close()
			return nil, dataErr(err)
		}
		out = append(out, t)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, dataErr(err)
	}
	rows2, err := tx.Query(ctx, `
		UPDATE core_terminal_sessions
		SET claimed_by = NULL, claim_expires_at = NULL, updated_at = now()
		WHERE persona_id = $1
		  AND status = 'ending'
		  AND claim_expires_at IS NOT NULL AND claim_expires_at <= now()
		RETURNING `+terminalSessionCols, personaID)
	if err != nil {
		return nil, dataErr(err)
	}
	defer rows2.Close()
	for rows2.Next() {
		t, err := scanTerminalSession(rows2)
		if err != nil {
			return nil, dataErr(err)
		}
		out = append(out, t)
	}
	return out, dataErr(rows2.Err())
}

// SweepExpiredTerminalClaims is the standalone recovery route — same
// reasoning as jobs/sweep-expired: a quiet persona's lapsed session
// should not wait for new work to notice it is reclaimable.
func (s *Store) SweepExpiredTerminalClaims(ctx context.Context, personaID string) ([]TerminalSession, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	out, err := s.sweepTerminalExpiredTx(ctx, tx, personaID)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

// HeartbeatTerminalSession renews a live claim and returns the
// session so the runner observes 'ending' / a control hold. A lost
// claim is ErrTerminalNotClaimed.
func (s *Store) HeartbeatTerminalSession(ctx context.Context, personaID, sessionID, runnerID string, epoch int64, lease time.Duration) (TerminalSession, error) {
	var out TerminalSession
	err := s.terminalClaimTx(ctx, personaID, sessionID, runnerID, epoch, func(ctx context.Context, tx pgx.Tx, t *TerminalSession) error {
		res, err := tx.Exec(ctx, `
			UPDATE core_terminal_sessions
			SET claim_expires_at = now() + $4::interval, updated_at = now()
			WHERE persona_id = $1 AND session_id = $2 AND claimed_by = $3`,
			personaID, sessionID, runnerID,
			fmt.Sprintf("%d milliseconds", lease.Milliseconds()))
		if err != nil {
			return dataErr(err)
		}
		if res.RowsAffected() == 0 {
			return ErrTerminalNotClaimed
		}
		return nil
	}, &out)
	return out, err
}

// terminalClaimTx is the shared (persona, session, runner, epoch)
// fence: the closure runs only while the row is locked and the claim
// is verified live, so every runner mutation lands under one
// authority. `t` is the freshly reloaded row after the closure.
func (s *Store) terminalClaimTx(ctx context.Context, personaID, sessionID, runnerID string, epoch int64, fn func(context.Context, pgx.Tx, *TerminalSession) error, out *TerminalSession) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	t, err := lockTerminalSessionTx(ctx, tx, personaID, sessionID)
	if err != nil {
		return err
	}
	if t.Epoch != epoch || t.ClaimedBy != runnerID || !terminalClaimed(t.Status) ||
		t.ClaimExpiresAt == nil || !t.ClaimExpiresAt.After(time.Now()) {
		if out != nil {
			*out = t
		}
		return ErrTerminalNotClaimed
	}
	if err := fn(ctx, tx, &t); err != nil {
		if out != nil {
			*out = t
		}
		return err
	}
	fresh, err := lockTerminalSessionTx(ctx, tx, personaID, sessionID)
	if err != nil {
		return err
	}
	if out != nil {
		*out = fresh
	}
	return tx.Commit(ctx)
}

// ListTerminalInputs is the persona-facing read of the durable input
// ledger — every status, in seq order. The acceptance ack is 'intended';
// delivery outcome is only knowable here ('written', 'failed', 'unknown',
// 'interrupted', 'expired'). Reading changes nothing: 'unknown' rows are
// never resent, and there is no replay on this path.
func (s *Store) ListTerminalInputs(ctx context.Context, personaID, sessionID string, afterSeq int64, limit int) ([]TerminalInput, error) {
	// Session ownership gates the ledger: cross-persona reads are 404,
	// same as every other session surface.
	if _, err := s.GetTerminalSession(ctx, personaID, sessionID); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > terminalInputListMax {
		limit = terminalInputListMax
	}
	rows, err := s.pool.Query(ctx, `
		SELECT input_id, session_id, session_epoch, seq, kind, payload, source, status, detail, created_at, updated_at
		FROM core_terminal_inputs
		WHERE session_id = $1::uuidv7 AND seq > $2
		ORDER BY seq LIMIT $3`, sessionID, afterSeq, limit)
	if err != nil {
		return nil, dataErr(err)
	}
	defer rows.Close()
	var out []TerminalInput
	for rows.Next() {
		var in TerminalInput
		if err := rows.Scan(&in.InputID, &in.SessionID, &in.SessionEpoch, &in.Seq, &in.Kind, &in.Payload, &in.Source, &in.Status, &in.Detail, &in.CreatedAt, &in.UpdatedAt); err != nil {
			return nil, dataErr(err)
		}
		out = append(out, in)
	}
	return out, dataErr(rows.Err())
}

// PendingTerminalInputs lists this epoch's deliverable inputs in seq
// order under the live claim. 'dequeued' rows re-appear here until
// dispositioned — dequeue is reported separately so a crash between
// fetch and disposition is an honest 'unknown', not a silent loss.
func (s *Store) PendingTerminalInputs(ctx context.Context, personaID, sessionID, runnerID string, epoch int64) ([]TerminalInput, error) {
	var out []TerminalInput
	err := s.terminalClaimTx(ctx, personaID, sessionID, runnerID, epoch, func(ctx context.Context, tx pgx.Tx, t *TerminalSession) error {
		rows, err := tx.Query(ctx, `
			SELECT input_id, session_id, session_epoch, seq, kind, payload, source, status, detail, created_at, updated_at
			FROM core_terminal_inputs
			WHERE session_id = $1 AND session_epoch = $2 AND status = 'intended'
			ORDER BY seq`, sessionID, epoch)
		if err != nil {
			return dataErr(err)
		}
		defer rows.Close()
		for rows.Next() {
			var in TerminalInput
			if err := rows.Scan(&in.InputID, &in.SessionID, &in.SessionEpoch, &in.Seq, &in.Kind, &in.Payload, &in.Source, &in.Status, &in.Detail, &in.CreatedAt, &in.UpdatedAt); err != nil {
				return dataErr(err)
			}
			out = append(out, in)
		}
		return dataErr(rows.Err())
	}, nil)
	return out, err
}

// ResolveDequeuedTerminalInputs marks this epoch's orphan 'dequeued'
// rows 'unknown' under the live claim. A runner calls it once when it
// adopts a session it already owns (same-epoch resume): rows the
// previous pump left 'dequeued' are indeterminate — maybe delivered —
// and PendingTerminalInputs correctly never re-serves them, but
// without this sweep they would sit unresolved until the next epoch
// bump or session end. It runs inside terminalClaimTx so it can only
// fire while this process holds the claim; combined with the runner
// advisory lock (one live driver per RunnerID) it cannot race a
// still-live pump — and even if it somehow did, a later 'written'
// disposition is refused on a row that is already 'unknown', which
// stays honest (maybe-delivered, never resent) rather than claiming
// non-delivery.
func (s *Store) ResolveDequeuedTerminalInputs(ctx context.Context, personaID, sessionID, runnerID string, epoch int64) (int64, error) {
	var resolved int64
	err := s.terminalClaimTx(ctx, personaID, sessionID, runnerID, epoch, func(ctx context.Context, tx pgx.Tx, t *TerminalSession) error {
		res, err := tx.Exec(ctx, `
			UPDATE core_terminal_inputs
			SET status = 'unknown', updated_at = now()
			WHERE session_id = $1 AND session_epoch = $2 AND status = 'dequeued'`,
			sessionID, epoch)
		if err != nil {
			return dataErr(err)
		}
		resolved = res.RowsAffected()
		return nil
	}, nil)
	return resolved, err
}

// terminalRunnerLockKey derives the advisory-lock key for a runner
// identity: one Postgres session may hold it at a time, fencing
// driver *processes*, not just claim epochs.
func terminalRunnerLockKey(runnerID string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte("sumi-terminal-runner:" + runnerID))
	return int64(h.Sum64())
}

// TryAcquireTerminalRunnerLock attempts the session-scoped advisory
// lock for this runner identity on a dedicated pooled connection.
// The lock dies with the connection, so a crashed process releases
// automatically and a restarting driver safely takes over. The
// returned handle MUST be kept: callers should heartbeat it (a dead
// lock connection means another process may already hold the lock —
// continuing to pump would be the duplicate-delivery hazard this
// fence exists to prevent) and release it on shutdown.
func (s *Store) TryAcquireTerminalRunnerLock(ctx context.Context, runnerID string) (*TerminalRunnerLock, error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	var held bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, terminalRunnerLockKey(runnerID)).Scan(&held); err != nil {
		conn.Release()
		return nil, dataErr(err)
	}
	if !held {
		conn.Release()
		return nil, nil
	}
	var pid int
	_ = conn.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid)
	return &TerminalRunnerLock{conn: conn, pid: pid}, nil
}

// TerminalRunnerLock is the held advisory lock: Ping verifies the
// connection (and therefore the lock) is still alive; Release frees
// both. The mutex serializes Ping and Release so a shutdown never
// releases the pool connection out from under a watchdog probe.
type TerminalRunnerLock struct {
	mu   sync.Mutex
	conn *pgxpool.Conn
	pid  int
}

// PID is the Postgres backend pid holding the lock connection,
// recorded at acquisition — ops and tests identify the exact
// connection to terminate without guessing from activity tables.
func (l *TerminalRunnerLock) PID() int {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.pid
}

// Ping reports whether the lock connection is still alive. False
// means the lock may already be held by another process — the caller
// must stop driving immediately.
func (l *TerminalRunnerLock) Ping(ctx context.Context) bool {
	if l == nil {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.conn == nil {
		return false
	}
	return l.conn.Ping(ctx) == nil
}

// Release frees the advisory lock and returns the connection.
func (l *TerminalRunnerLock) Release(ctx context.Context) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.conn == nil {
		return
	}
	_, _ = l.conn.Exec(ctx, `SELECT pg_advisory_unlock_all()`)
	l.conn.Release()
	l.conn = nil
}

// ReportTerminalInputDisposition records one input hop under the
// claim: intended → dequeued → written | failed | unknown. A
// disposition for a stale epoch or foreign runner is refused.
func (s *Store) ReportTerminalInputDisposition(ctx context.Context, personaID, sessionID, inputID, runnerID string, epoch int64, status string, detail map[string]any) (TerminalInput, error) {
	var allowed = map[string]bool{"dequeued": true, "written": true, "failed": true, "unknown": true}
	if !allowed[status] {
		return TerminalInput{}, fmt.Errorf("%w: invalid input disposition", ErrBadRequest)
	}
	var out TerminalInput
	err := s.terminalClaimTx(ctx, personaID, sessionID, runnerID, epoch, func(ctx context.Context, tx pgx.Tx, t *TerminalSession) error {
		err := tx.QueryRow(ctx, `
			UPDATE core_terminal_inputs
			SET status = $4, detail = $5, updated_at = now()
			WHERE input_id = $1 AND session_id = $2 AND session_epoch = $3
			  AND status IN ('intended', 'dequeued')
			RETURNING input_id, session_id, session_epoch, seq, kind, payload, source, status, detail, created_at, updated_at`,
			inputID, sessionID, epoch, status, detail).
			Scan(&out.InputID, &out.SessionID, &out.SessionEpoch, &out.Seq, &out.Kind, &out.Payload, &out.Source, &out.Status, &out.Detail, &out.CreatedAt, &out.UpdatedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: input not pending in this epoch", ErrTerminalNotClaimed)
		}
		return err
	}, nil)
	return out, err
}

// AppendTerminalOutput appends runner-drained output chunks under the
// claim. Chunk bases are the provisioner's absolute offsets;
// (session_id, base, kind) dedupe makes a replayed drain idempotent.
// After the append the serving cap prunes the oldest chunks and
// advances output_base — readers below it see an explicit gap.
func (s *Store) AppendTerminalOutput(ctx context.Context, personaID, sessionID, runnerID string, epoch int64, chunks []TerminalOutputChunk) (TerminalSession, error) {
	var out TerminalSession
	err := s.terminalClaimTx(ctx, personaID, sessionID, runnerID, epoch, func(ctx context.Context, tx pgx.Tx, t *TerminalSession) error {
		var seq int64
		if err := tx.QueryRow(ctx,
			`SELECT COALESCE(MAX(seq), 0) FROM core_terminal_output WHERE session_id = $1`,
			sessionID).Scan(&seq); err != nil {
			return dataErr(err)
		}
		high := t.OutputBytes
		for _, c := range chunks {
			seq++
			if c.Kind == "gap" {
				gapTo := c.Base
				if c.GapTo != nil {
					gapTo = *c.GapTo
				}
				if _, err := tx.Exec(ctx, `
					INSERT INTO core_terminal_output (session_id, seq, kind, base, gap_to)
					VALUES ($1, $2, 'gap', $3, $4) ON CONFLICT (session_id, base, kind) DO NOTHING`,
					sessionID, seq, c.Base, gapTo); err != nil {
					return dataErr(err)
				}
				if gapTo > high {
					high = gapTo
				}
				continue
			}
			if len(c.Data) == 0 {
				continue
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO core_terminal_output (session_id, seq, kind, base, data)
				VALUES ($1, $2, 'data', $3, $4) ON CONFLICT (session_id, base, kind) DO NOTHING`,
				sessionID, seq, c.Base, c.Data); err != nil {
				return dataErr(err)
			}
			if end := c.Base + int64(len(c.Data)); end > high {
				high = end
			}
		}
		if _, err := tx.Exec(ctx, `
			UPDATE core_terminal_sessions
			SET output_bytes = GREATEST(output_bytes, $3), updated_at = now()
			WHERE persona_id = $1 AND session_id = $2`,
			personaID, sessionID, high); err != nil {
			return dataErr(err)
		}
		return s.pruneTerminalOutputTx(ctx, tx, t)
	}, &out)
	return out, err
}

// pruneTerminalOutputTx enforces the serving cap: whole chunks below
// the target are deleted; a straddling chunk is trimmed so retained
// data stays contiguous. output_base advances to the new floor.
func (s *Store) pruneTerminalOutputTx(ctx context.Context, tx pgx.Tx, t *TerminalSession) error {
	var retained int64
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(SUM(octet_length(data)), 0) FROM core_terminal_output
		WHERE session_id = $1 AND kind = 'data'`, t.SessionID).Scan(&retained); err != nil {
		return dataErr(err)
	}
	if retained <= terminalScrollbackCap {
		return nil
	}
	target := retained - terminalScrollbackKeep
	rows, err := tx.Query(ctx, `
		SELECT seq, base, octet_length(data) FROM core_terminal_output
		WHERE session_id = $1 AND kind = 'data' ORDER BY base`, t.SessionID)
	if err != nil {
		return dataErr(err)
	}
	type chunkRow struct {
		seq, base, size int64
	}
	var ordered []chunkRow
	for rows.Next() {
		var c chunkRow
		if err := rows.Scan(&c.seq, &c.base, &c.size); err != nil {
			rows.Close()
			return dataErr(err)
		}
		ordered = append(ordered, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return dataErr(err)
	}
	var removed int64
	newBase := t.OutputBase
	for _, c := range ordered {
		if removed >= target {
			break
		}
		if removed+c.size <= target {
			if _, err := tx.Exec(ctx, `DELETE FROM core_terminal_output WHERE session_id = $1 AND seq = $2`, t.SessionID, c.seq); err != nil {
				return dataErr(err)
			}
			removed += c.size
			newBase = c.base + c.size
			continue
		}
		// Straddling chunk: trim the head so the retained window stays
		// contiguous and base keeps absolute meaning.
		trim := target - removed
		if _, err := tx.Exec(ctx, `
			UPDATE core_terminal_output SET base = base + $3, data = substring(data from $4)
			WHERE session_id = $1 AND seq = $2`,
			t.SessionID, c.seq, trim, trim+1); err != nil {
			return dataErr(err)
		}
		newBase = c.base + trim
		removed += trim
	}
	if _, err := tx.Exec(ctx, `
		UPDATE core_terminal_sessions SET output_base = GREATEST(output_base, $3)
		WHERE persona_id = $1 AND session_id = $2`,
		t.PersonaID, t.SessionID, newBase); err != nil {
		return dataErr(err)
	}
	t.OutputBase = newBase
	return nil
}

// ReportTerminalStatus moves the session lifecycle under the claim.
// The runner reports 'active' once its runtime op is live (carrying
// the provisioner operation id), 'ended' with the observed exit, or
// 'lost' when the outcome is genuinely indeterminate — never a
// re-launch verdict. Identical resend replays the stored row.
func (s *Store) ReportTerminalStatus(ctx context.Context, personaID, sessionID, runnerID string, epoch int64, status, reason string, exitCode *int, exitSignal, operationID string) (TerminalSession, error) {
	var allowed = map[string]bool{"active": true, "ended": true, "lost": true}
	if !allowed[status] {
		return TerminalSession{}, fmt.Errorf("%w: invalid session status report", ErrBadRequest)
	}
	var out TerminalSession
	err := s.terminalClaimTx(ctx, personaID, sessionID, runnerID, epoch, func(ctx context.Context, tx pgx.Tx, t *TerminalSession) error {
		if status == "active" {
			// The report records the live operation id unconditionally,
			// but a durable 'ending' can never be resurrected: a close
			// that landed between claim and this report keeps its
			// intent — the runner's next heartbeat observes 'ending'
			// and terminates the op it just launched.
			if _, err := tx.Exec(ctx, `
				UPDATE core_terminal_sessions
				SET status = CASE WHEN status = 'ending' THEN 'ending' ELSE 'active' END,
					operation_id = $3, updated_at = now()
				WHERE persona_id = $1 AND session_id = $2`,
				personaID, sessionID, operationID); err != nil {
				return dataErr(err)
			}
			return nil
		}
		return s.endTerminalSessionTx(ctx, tx, t, status, reason, exitCode, exitSignal)
	}, &out)
	return out, err
}

// internalTerminalTool is the secretary's tool surface: open/list/
// read/write/resize/signal/close. Effects run inside the operation
// claim transaction like other internal tools — the recorded tool
// result and the session mutation commit or roll back together.
func (s *Store) internalTerminalTool(ctx context.Context, tx pgx.Tx, personaID, tool string, request map[string]any) (map[string]any, error) {
	switch tool {
	case "terminal.open":
		name, _ := request["name"].(string)
		t, err := s.createTerminalSessionTx(ctx, tx, personaID, name, "agent", "tool:terminal.open")
		if err != nil {
			return nil, err
		}
		return map[string]any{"session": t}, nil
	case "terminal.list":
		rows, err := tx.Query(ctx,
			`SELECT `+terminalSessionCols+` FROM core_terminal_sessions
			 WHERE persona_id = $1::uuidv7 ORDER BY created_at`, personaID)
		if err != nil {
			return nil, dataErr(err)
		}
		defer rows.Close()
		var sessions []TerminalSession
		for rows.Next() {
			t, err := scanTerminalSession(rows)
			if err != nil {
				return nil, dataErr(err)
			}
			sessions = append(sessions, t)
		}
		if err := rows.Err(); err != nil {
			return nil, dataErr(err)
		}
		return map[string]any{"sessions": sessions}, nil
	case "terminal.read":
		sessionID, _ := request["session_id"].(string)
		if sessionID == "" {
			return nil, fmt.Errorf("%w: terminal.read requires session_id", ErrBadRequest)
		}
		var cursor int64
		if c, ok := request["cursor"].(float64); ok {
			cursor = int64(c)
		}
		// event_cursor tracks consumed zero-width loss markers by
		// chunk seq — the byte cursor alone cannot express whether a
		// boundary event at the current position was already seen.
		var eventCursor int64
		if e, ok := request["event_cursor"].(float64); ok {
			eventCursor = int64(e)
		}
		limit := 32
		if l, ok := request["limit"].(float64); ok {
			limit = int(l)
		}
		t, err := lockTerminalSessionTx(ctx, tx, personaID, sessionID)
		if err != nil {
			return nil, terminalToolErr(err)
		}
		if cursor == 0 {
			cursor = t.OutputBase
			if tail, _ := request["tail"].(bool); tail {
				// A tail read starts near the end of the retained
				// scrollback — the model asks for "what's on screen
				// now", not the whole history.
				if c := t.OutputBytes - 64*1024; c > cursor {
					cursor = c
				}
			}
		}
		if limit <= 0 || limit > 64 {
			limit = 32
		}
		rows, err := tx.Query(ctx, `
			SELECT session_id, seq, kind, base, gap_to, data, created_at
			FROM core_terminal_output
			WHERE session_id = $1::uuidv7 AND (
				GREATEST(base + octet_length(data), COALESCE(gap_to, 0)) > $2
				OR (kind = 'gap' AND gap_to = base AND seq > $3)
			)
			ORDER BY seq LIMIT $4`, sessionID, cursor, eventCursor, limit+1)
		if err != nil {
			return nil, dataErr(err)
		}
		defer rows.Close()
		var chunks []TerminalOutputChunk
		for rows.Next() {
			var c TerminalOutputChunk
			if err := rows.Scan(&c.SessionID, &c.Seq, &c.Kind, &c.Base, &c.GapTo, &c.Data, &c.CreatedAt); err != nil {
				return nil, dataErr(err)
			}
			chunks = append(chunks, c)
		}
		if err := rows.Err(); err != nil {
			return nil, dataErr(err)
		}
		var text string
		next := cursor
		nextEvent := eventCursor
		for _, c := range chunks {
			if c.Seq > nextEvent {
				nextEvent = c.Seq
			}
			if c.Kind == "gap" {
				to := c.Base
				if c.GapTo != nil {
					to = *c.GapTo
				}
				if to > next {
					next = to
				}
				if to == c.Base {
					// A zero-width boundary is a journaled loss event
					// (journal rotation/vanish, uncertified resume): the
					// lost window's size is unknown, so the marker names
					// the boundary, never an invented byte range.
					text += fmt.Sprintf("\n[output may be missing at byte %d — journal loss boundary]\n", c.Base)
				} else {
					text += fmt.Sprintf("\n[output lost: bytes %d–%d were compacted away]\n", c.Base, to)
				}
				continue
			}
			end := c.Base + int64(len(c.Data))
			if end > next {
				next = end
			}
			text += string(c.Data)
		}
		return map[string]any{
			"session":      t,
			"base":         t.OutputBase,
			"cursor":       cursor,
			"next_cursor":  next,
			"event_cursor": nextEvent,
			"gap":          cursor < t.OutputBase,
			"eof":          !terminalLive(t.Status) && next >= t.OutputBytes,
			"content":      text,
			"content_b64":  base64.StdEncoding.EncodeToString([]byte(text)),
		}, nil
	case "terminal.inputs":
		// The secretary's view of the same durable ledger the person
		// reads at GET /terminal/inputs — every status, seq order.
		// Acceptance is not delivery; this read is the honest outcome
		// channel. It changes nothing: 'unknown' rows are not resent.
		sessionID, _ := request["session_id"].(string)
		if sessionID == "" {
			return nil, fmt.Errorf("%w: terminal.inputs requires session_id", ErrBadRequest)
		}
		var afterSeq int64
		if c, ok := request["after_seq"].(float64); ok {
			afterSeq = int64(c)
		}
		t, err := lockTerminalSessionTx(ctx, tx, personaID, sessionID)
		if err != nil {
			return nil, terminalToolErr(err)
		}
		rows, err := tx.Query(ctx, `
			SELECT input_id, session_id, session_epoch, seq, kind, payload, source, status, detail, created_at, updated_at
			FROM core_terminal_inputs
			WHERE session_id = $1::uuidv7 AND seq > $2
			ORDER BY seq LIMIT $3`, sessionID, afterSeq, terminalInputListMax)
		if err != nil {
			return nil, dataErr(err)
		}
		defer rows.Close()
		var inputs []TerminalInput
		for rows.Next() {
			var in TerminalInput
			if err := rows.Scan(&in.InputID, &in.SessionID, &in.SessionEpoch, &in.Seq, &in.Kind, &in.Payload, &in.Source, &in.Status, &in.Detail, &in.CreatedAt, &in.UpdatedAt); err != nil {
				return nil, dataErr(err)
			}
			inputs = append(inputs, in)
		}
		if err := rows.Err(); err != nil {
			return nil, dataErr(err)
		}
		return map[string]any{"session": t, "inputs": inputs}, nil
	case "terminal.write", "terminal.resize", "terminal.signal", "terminal.eof":
		sessionID, _ := request["session_id"].(string)
		if sessionID == "" {
			return nil, fmt.Errorf("%w: %s requires session_id", ErrBadRequest, tool)
		}
		var kind string
		payload := map[string]any{}
		switch tool {
		case "terminal.write":
			kind = "stdin"
			data, _ := request["data"].(string)
			payload["data"] = data
			if eof, _ := request["eof"].(bool); eof {
				kind = "eof"
				payload = map[string]any{}
			}
		case "terminal.resize":
			kind = "resize"
			payload["cols"] = request["cols"]
			payload["rows"] = request["rows"]
		case "terminal.signal":
			kind = "signal"
			sig, _ := request["signal"].(string)
			payload["signal"] = sig
		case "terminal.eof":
			kind = "eof"
		}
		in, err := s.submitTerminalInputTx(ctx, tx, personaID, sessionID, "agent", kind, payload)
		if err != nil {
			return nil, terminalToolErr(err)
		}
		return map[string]any{"input": in}, nil
	case "terminal.close":
		sessionID, _ := request["session_id"].(string)
		if sessionID == "" {
			return nil, fmt.Errorf("%w: terminal.close requires session_id", ErrBadRequest)
		}
		t, err := lockTerminalSessionTx(ctx, tx, personaID, sessionID)
		if err != nil {
			return nil, terminalToolErr(err)
		}
		if t.Status == "ended" || t.Status == "lost" {
			return map[string]any{"session": t}, nil
		}
		liveClaim := terminalClaimed(t.Status) && t.ClaimExpiresAt != nil && t.ClaimExpiresAt.After(time.Now())
		if liveClaim {
			if _, err := tx.Exec(ctx, `
				UPDATE core_terminal_sessions SET status = 'ending', updated_at = now()
				WHERE persona_id = $1 AND session_id = $2`, personaID, sessionID); err != nil {
				return nil, dataErr(err)
			}
			t.Status = "ending"
			return map[string]any{"session": t}, nil
		}
		// Same rule as CloseTerminalSession: only 'requested' ends
		// outright. An 'interrupted' or dead-lease row may still have a
		// live op — stamp 'ending' + clear the claim so a reclaiming
		// runner performs the physical stop instead of certifying one
		// that never ran.
		if t.Status == "requested" {
			if err := s.endTerminalSessionTx(ctx, tx, &t, "ended", "closed", nil, ""); err != nil {
				return nil, err
			}
		} else {
			if _, err := tx.Exec(ctx, `
				UPDATE core_terminal_sessions
				SET status = 'ending', claimed_by = NULL, claim_expires_at = NULL, updated_at = now()
				WHERE persona_id = $1 AND session_id = $2`, personaID, sessionID); err != nil {
				return nil, dataErr(err)
			}
			t.Status = "ending"
			t.ClaimedBy = ""
			t.ClaimExpiresAt = nil
		}
		return map[string]any{"session": t}, nil
	}
	return nil, nil
}

// terminalToolErr keeps definite rejections (ended session, control
// held, capacity, bad request) as recorded tool results while letting
// transient store failures propagate so the claim retries.
func terminalToolErr(err error) error {
	switch {
	case errors.Is(err, ErrTerminalNotFound), errors.Is(err, ErrTerminalEnded),
		errors.Is(err, ErrTerminalNotLive), errors.Is(err, ErrTerminalControl),
		errors.Is(err, ErrTerminalCapacity), errors.Is(err, ErrBadRequest):
		return fmt.Errorf("%w: %s", ErrBadRequest, err.Error())
	}
	return err
}
