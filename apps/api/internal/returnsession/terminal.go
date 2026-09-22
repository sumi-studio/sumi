package returnsession

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/sumi-studio/sumi/apps/api/internal/runtimeprovision"
)

// terminal.go is the physical-writer half of the file cut. The filesvc
// freeze and the seal's job drain only see effects that declare through
// the API; an interactive terminal is one Docker container with the
// persona's files scope bind-mounted, writing bytes the file service
// never observes. Before the seal commits, every terminal session of
// the persona must prove Quiesced — terminal AND no physical writer
// remains — or the cut refuses honestly.
//
// Ordering: the caller holds the persona row lock for the whole gate,
// so terminal-session admission (createTerminalSessionTx takes FOR
// SHARE on the same row, and ClaimTerminalSessions re-checks authority
// under it) can neither slip a new session between the scan and the
// seal nor launch a queued one afterwards. The provisioner calls below
// need no API-DB locks — the container reconcile is the runtime's own
// journal + daemon work — so waiting on quiescence inside the
// transaction cannot deadlock the writers it is waiting on.
//
// Semantics per session — the ROW is classified before any runtime
// call, because an admitted runSession can be anywhere between its
// committed claim and its StartProcess RPC (scope ensure, scheduling,
// a runner restart): 'operation absent' is never evidence that a
// delayed start cannot arrive.
//   - claimed/active under a live claim      → deliberate interactive
//     work: the bind is refused and the sessions are named, before any
//     provisioner call is made. The person closes them (or cancels the
//     return); nothing is killed to make the cut pass.
//   - anything else                          → CancelProcess with
//     TombstoneIfAbsent: a live op is stopped+removed; an absent op is
//     durably fenced so a delayed start replays the cancellation and
//     never registers. Then the gate polls until Quiesced or the
//     deadline — never a certified live writer. Never-launched
//     tombstones stay recoverable: after a cancelled move the driver's
//     launch fence releases them (see termexec startFenced), so a
//     reclaim starts the session normally.

// TerminalProcesses is the narrow provisioner surface the gate needs.
// *runtimeprovision.Client satisfies it; tests substitute a fake.
type TerminalProcesses interface {
	ProcessStatus(context.Context, runtimeprovision.ProcessLookupRequest) (runtimeprovision.ProcessOperation, error)
	CancelProcess(context.Context, runtimeprovision.ProcessLookupRequest) (runtimeprovision.ProcessOperation, error)
}

// SetTerminalProcesses wires the runtime provisioner's process surface.
// Without it a persona with terminal session rows cannot be proven
// quiesced and a seal for that persona refuses — a persona with no
// terminal rows seals normally either way.
func (s *Service) SetTerminalProcesses(p TerminalProcesses) { s.termProcs = p }

// terminalQuiesceBounds cap the wait inside the seal transaction. The
// persona lock is held meanwhile, so the bound stays short: a runtime
// that cannot reach its daemon answers pending, not a hung seal.
const (
	terminalQuiesceDeadline = 20 * time.Second
	terminalQuiescePoll     = 250 * time.Millisecond
)

// ErrTerminalSessionsOpen refuses the seal while live-claimed
// interactive sessions exist — the person closes them or cancels the
// return. It wraps ErrConflict so existing callers map it the same way.
var ErrTerminalSessionsOpen = errors.New("terminal sessions still open")

// ErrTerminalQuiescePending means the runtime was asked to stop writers
// but could not prove quiescence inside the deadline — retryable; the
// seal did not commit.
var ErrTerminalQuiescePending = errors.New("terminal writers are still quiescing")

type terminalSessionRow struct {
	id        string
	name      string
	status    string
	claimedBy string
	liveClaim bool
}

// quiesceTerminalWriters runs inside the seal transaction with the
// persona row already locked. It returns nil when no terminal writer
// can still reach the workspace.
func (s *Service) quiesceTerminalWriters(ctx context.Context, tx pgx.Tx, personaID string) error {
	rows, err := tx.Query(ctx, `
		SELECT session_id, name, status, COALESCE(claimed_by, ''),
		       claim_expires_at IS NOT NULL AND claim_expires_at > now()
		FROM core_terminal_sessions WHERE persona_id = $1::uuidv7`, personaID)
	if err != nil {
		return err
	}
	var sessions []terminalSessionRow
	for rows.Next() {
		var t terminalSessionRow
		if err := rows.Scan(&t.id, &t.name, &t.status, &t.claimedBy, &t.liveClaim); err != nil {
			rows.Close()
			return err
		}
		sessions = append(sessions, t)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if len(sessions) == 0 {
		return nil
	}
	if s.termProcs == nil {
		return fmt.Errorf("%w: %d terminal session(s) exist and the runtime process service is not configured — their writers cannot be proven stopped",
			ErrTerminalQuiescePending, len(sessions))
	}

	// First pass is read-only AND runtime-free: a live claim is
	// deliberate work whether or not its operation has reached the
	// provisioner yet. A refusal must leave nothing cancelled — the
	// person may still choose to abort the move instead.
	var blockers, pending []terminalSessionRow
	for _, t := range sessions {
		if (t.status == "claimed" || t.status == "active") && t.liveClaim {
			blockers = append(blockers, t)
			continue
		}
		pending = append(pending, t)
	}
	if len(blockers) > 0 {
		names := make([]string, 0, len(blockers))
		for _, t := range blockers {
			names = append(names, fmt.Sprintf("%s (%s)", terminalDisplay(t), t.status))
		}
		sort.Strings(names)
		return fmt.Errorf("%w: %s — close the terminal session(s) or cancel the return; nothing was stopped",
			ErrTerminalSessionsOpen, strings.Join(names, ", "))
	}

	// Second pass: stop every unquiesced writer that is not deliberate
	// interactive work, then wait for physical proof.
	deadline := time.Now().Add(s.termWait)
	for _, t := range pending {
		opID := terminalOpID(personaID, t.id)
		if _, cerr := s.termProcs.CancelProcess(ctx, runtimeprovision.ProcessLookupRequest{
			PersonalityAgentID: personaID, OperationID: opID, TombstoneIfAbsent: true}); cerr != nil &&
			!errors.Is(cerr, runtimeprovision.ErrProcessNotFound) {
			return fmt.Errorf("%w: terminal op cancel: %v", ErrTerminalQuiescePending, cerr)
		}
	}
	for len(pending) > 0 {
		var still []terminalSessionRow
		for _, t := range pending {
			op, perr := s.termProcs.ProcessStatus(ctx, runtimeprovision.ProcessLookupRequest{
				PersonalityAgentID: personaID, OperationID: terminalOpID(personaID, t.id)})
			if perr == nil && op.Quiesced {
				continue
			}
			// Anything else — error, NotFound, or not yet quiesced —
			// is not proof of stop; keep waiting for the deadline.
			still = append(still, t)
		}
		if len(still) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			names := make([]string, 0, len(still))
			for _, t := range still {
				names = append(names, fmt.Sprintf("%s (%s)", terminalDisplay(t), t.status))
			}
			return fmt.Errorf("%w: %s — the runtime could not prove them stopped within %s; retry or cancel the return",
				ErrTerminalQuiescePending, strings.Join(names, ", "), s.termWait)
		}
		pending = still
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.termPoll):
		}
	}
	return nil
}

// terminalOpID is the deterministic runtime operation id a terminal
// session launches under — the same derivation the termexec driver
// uses, so the gate sees the same record.
func terminalOpID(personaID, sessionID string) string {
	return runtimeprovision.ProcessOperationID(personaID, "term:"+sessionID)
}

func terminalDisplay(t terminalSessionRow) string {
	if t.name != "" {
		return t.name
	}
	return t.id
}

// openTerminalSessions lists the sessions a seal would still have to
// quiesce — the preflight's honest answer to "why is the move waiting".
func (s *Service) openTerminalSessions(ctx context.Context, personaID string) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT COALESCE(NULLIF(name, ''), session_id::text), status
		FROM core_terminal_sessions
		WHERE persona_id = $1::uuidv7
		  AND status IN ('requested','claimed','active','ending','interrupted','lost')
		ORDER BY created_at`, personaID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name, status string
		if err := rows.Scan(&name, &status); err != nil {
			return nil, err
		}
		out = append(out, name+" ("+status+")")
	}
	return out, rows.Err()
}
