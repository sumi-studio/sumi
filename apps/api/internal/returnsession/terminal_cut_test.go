package returnsession_test

// Terminal-writer cut proofs: a sealed return must wait for physical
// proof that every admitted PTY writer stopped (runtime Quiesced), live
// deliberate sessions refuse rather than being killed, and the cut is
// honestly pending while the runtime cannot prove stop. Claim-side
// fences are covered in agentstate; these tests drive the seal path.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/returnsession"
	"github.com/sumi-studio/sumi/apps/api/internal/runtimeprovision"
)

// fakeProcs is the runtime-provisioner ProcessAPI stand-in. Each op is
// keyed by operation id; cancel marks quiesced (the reconcile loop would
// flip the real record once SIGKILL lands) unless neverQuiesce is set,
// and every call is recorded so a test can prove nothing was killed.
type fakeProcs struct {
	mu           sync.Mutex
	ops          map[string]*runtimeprovision.ProcessOperation
	cancels      []string
	cancelReqs   []runtimeprovision.ProcessLookupRequest
	neverQuiesce bool
	statusErr    error
}

func newFakeProcs() *fakeProcs {
	return &fakeProcs{ops: map[string]*runtimeprovision.ProcessOperation{}}
}

func termOp(personaID, sessionID string) string {
	return runtimeprovision.ProcessOperationID(personaID, "term:"+sessionID)
}

func (f *fakeProcs) addOp(personaID, sessionID string, quiesced bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ops[termOp(personaID, sessionID)] = &runtimeprovision.ProcessOperation{
		OperationID: termOp(personaID, sessionID), PersonalityAgentID: personaID,
		State: "running", Quiesced: quiesced,
	}
}

func (f *fakeProcs) ProcessStatus(_ context.Context, req runtimeprovision.ProcessLookupRequest) (runtimeprovision.ProcessOperation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.statusErr != nil {
		return runtimeprovision.ProcessOperation{}, f.statusErr
	}
	op, ok := f.ops[req.OperationID]
	if !ok || op.PersonalityAgentID != req.PersonalityAgentID {
		return runtimeprovision.ProcessOperation{}, runtimeprovision.ErrProcessNotFound
	}
	return *op, nil
}

func (f *fakeProcs) CancelProcess(_ context.Context, req runtimeprovision.ProcessLookupRequest) (runtimeprovision.ProcessOperation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancels = append(f.cancels, req.OperationID)
	f.cancelReqs = append(f.cancelReqs, req)
	op, ok := f.ops[req.OperationID]
	if !ok {
		if !req.TombstoneIfAbsent {
			return runtimeprovision.ProcessOperation{}, runtimeprovision.ErrProcessNotFound
		}
		op = &runtimeprovision.ProcessOperation{OperationID: req.OperationID, PersonalityAgentID: req.PersonalityAgentID}
		f.ops[req.OperationID] = op
		op.Tombstone = true
	}
	op.State = "cancelled"
	if !f.neverQuiesce {
		op.Quiesced = true
	}
	return *op, nil
}

func (f *fakeProcs) cancelCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.cancels)
}

// termRow inserts a core_terminal_sessions row in a chosen status so the
// test controls exactly what the seal scan sees (live claim vs limbo).
func termRow(t *testing.T, h *harness, sessionID, status string) {
	t.Helper()
	var claimedBy *string
	var expires any
	if status == "active" || status == "claimed" {
		runner := "runner#1"
		claimedBy = &runner
		expires = time.Now().Add(5 * time.Minute)
	}
	if _, err := h.cloud.pool.Exec(h.ctx, `INSERT INTO core_terminal_sessions
		(session_id, persona_id, mode, backend, status, requested_by, created_by, claimed_by, claim_expires_at)
		VALUES ($1,$2,'pty','cloud',$3,'human','test',$4,$5)`,
		sessionID, h.persona, status, claimedBy, expires); err != nil {
		t.Fatalf("term row: %v", err)
	}
}

func bindDest(t *testing.T, h *harness, sid, grant string) (int, []byte) {
	t.Helper()
	return h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/destination", sid),
		grant, jsonBody(destMode(t, h.local, h.persona, "absent", "local")))
}

// A live, deliberately-attached session must block the seal rather than
// being killed: 409 terminal_sessions_open, zero runtime cancels, and
// the return stays open so the person can close the session and retry.
func TestReturnSealRefusesLiveTerminalSession(t *testing.T) {
	h := setup(t, returnsession.Config{})
	h.sessions.SetFileStore(&fakeFiles{})
	procs := newFakeProcs()
	h.sessions.SetTerminalProcesses(procs)

	term := newID(t)
	termRow(t, h, term, "active")
	procs.addOp(h.persona, term, false)

	sid, _, grant := h.createMode("local")
	code, raw := bindDest(t, h, sid, grant)
	if code != http.StatusConflict {
		t.Fatalf("live terminal seal: %d %s", code, raw)
	}
	var body struct {
		Code string `json:"code"`
	}
	unmarshal(t, raw, &body)
	if body.Code != "terminal_sessions_open" {
		t.Fatalf("code = %q: %s", body.Code, raw)
	}
	if procs.cancelCount() != 0 {
		t.Fatalf("live deliberate session was cancelled: %v", procs.cancels)
	}
	if a := authority(t, h.cloud, h.persona); a != "active" {
		t.Fatalf("persona authority after refused seal = %s", a)
	}

	// The person closes the terminal; retrying the seal then succeeds.
	mustExec(t, h.cloud.pool,
		`UPDATE core_terminal_sessions SET status = 'ended', claimed_by = NULL, claim_expires_at = NULL WHERE session_id = $1`, term)
	code, raw = bindDest(t, h, sid, grant)
	if code != http.StatusOK {
		t.Fatalf("seal after close: %d %s", code, raw)
	}
	if v := asView(t, raw); v.Status != returnsession.StatusSealed {
		t.Fatalf("status = %s", v.Status)
	}
}

// RWC-01: a session CLAIMED before the seal whose admitted runSession
// has not yet reached the provisioner (no operation journaled — scope
// ensure, scheduling, restart delay) must still refuse the cut. 'Absent'
// is not evidence the admitted start cannot arrive; the live claim is
// classified before any runtime call, so the refusal makes zero
// provisioner calls in both file modes.
func TestReturnSealFencesClaimedSessionBeforeProcessJournals(t *testing.T) {
	for _, mode := range []string{"local", "cloud"} {
		t.Run(mode, func(t *testing.T) {
			h := setup(t, returnsession.Config{})
			h.sessions.SetFileStore(&fakeFiles{})
			procs := newFakeProcs()
			h.sessions.SetTerminalProcesses(procs)

			term := newID(t)
			termRow(t, h, term, "claimed") // live claim, no op record

			sid, _, grant := h.createMode(mode)
			code, raw := h.grantReq(http.MethodPost,
				fmt.Sprintf("/api/secretary-return/sessions/%s/destination", sid),
				grant, jsonBody(destMode(t, h.local, h.persona, "absent", mode)))
			if code != http.StatusConflict {
				t.Fatalf("seal accepted an admitted-but-unlaunched terminal: %d %s", code, raw)
			}
			var body struct {
				Code string `json:"code"`
			}
			unmarshal(t, raw, &body)
			if body.Code != "terminal_sessions_open" {
				t.Fatalf("code = %q: %s", body.Code, raw)
			}
			if procs.cancelCount() != 0 {
				t.Fatalf("a refused seal must not cancel anything: %v", procs.cancels)
			}
			if a := authority(t, h.cloud, h.persona); a != "active" {
				t.Fatalf("authority after refused seal = %s", a)
			}
		})
	}
}

// The complementary leg: a NON-live row (interrupted/lost/expired claim)
// whose operation never journaled is not deliberate work — the seal
// proceeds, but plants a durable cancel tombstone so an admitted start
// still in flight replays 'cancelled' instead of registering late.
func TestReturnSealTombstonesUnlaunchedLimboOperation(t *testing.T) {
	h := setup(t, returnsession.Config{})
	h.sessions.SetFileStore(&fakeFiles{})
	procs := newFakeProcs()
	h.sessions.SetTerminalProcesses(procs)

	term := newID(t)
	termRow(t, h, term, "interrupted") // no live claim, no op record

	sid, _, grant := h.createMode("local")
	code, raw := bindDest(t, h, sid, grant)
	if code != http.StatusOK {
		t.Fatalf("seal with unlaunched limbo session: %d %s", code, raw)
	}
	if a := authority(t, h.cloud, h.persona); a != "sealed" {
		t.Fatalf("authority = %s", a)
	}
	procs.mu.Lock()
	defer procs.mu.Unlock()
	if len(procs.cancelReqs) != 1 || !procs.cancelReqs[0].TombstoneIfAbsent {
		t.Fatalf("absent op was not fenced: %+v", procs.cancelReqs)
	}
	op := procs.ops[termOp(h.persona, term)]
	if op == nil || !op.Tombstone || !op.Quiesced {
		t.Fatalf("no tombstone fence planted: %+v", op)
	}
}

// An expired claim is not deliberate work either: the admitted runner
// may still be mid-start, so the row is quiesced (tombstone fence), not
// treated as a blocker that could stall the move forever.
func TestReturnSealQuiescesExpiredClaim(t *testing.T) {
	h := setup(t, returnsession.Config{})
	h.sessions.SetFileStore(&fakeFiles{})
	procs := newFakeProcs()
	h.sessions.SetTerminalProcesses(procs)

	term := newID(t)
	runner := "runner#gone"
	mustExec(t, h.cloud.pool, `INSERT INTO core_terminal_sessions
		(session_id, persona_id, mode, backend, status, requested_by, created_by, claimed_by, claim_expires_at)
		VALUES ($1,$2,'pty','cloud','claimed','human','test',$3, now() - interval '1 second')`,
		term, h.persona, runner)

	sid, _, grant := h.createMode("local")
	code, raw := bindDest(t, h, sid, grant)
	if code != http.StatusOK {
		t.Fatalf("seal with expired-claim session: %d %s", code, raw)
	}
	procs.mu.Lock()
	defer procs.mu.Unlock()
	if len(procs.cancelReqs) != 1 || !procs.cancelReqs[0].TombstoneIfAbsent {
		t.Fatalf("expired-claim absent op not fenced: %+v", procs.cancelReqs)
	}
}

// A limbo session (lost/ending — no deliberate work to preserve) with a
// live runtime op must be cancelled through the provisioner and the seal
// proceeds only once the op reports Quiesced.
func TestReturnSealQuiescesLimboWriter(t *testing.T) {
	h := setup(t, returnsession.Config{})
	h.sessions.SetFileStore(&fakeFiles{})
	procs := newFakeProcs()
	h.sessions.SetTerminalProcesses(procs)

	term := newID(t)
	termRow(t, h, term, "lost")
	procs.addOp(h.persona, term, false)

	sid, _, grant := h.createMode("local")
	code, raw := bindDest(t, h, sid, grant)
	if code != http.StatusOK {
		t.Fatalf("seal with limbo writer: %d %s", code, raw)
	}
	if procs.cancelCount() != 1 {
		t.Fatalf("cancels = %d", procs.cancelCount())
	}
	if a := authority(t, h.cloud, h.persona); a != "sealed" {
		t.Fatalf("authority = %s", a)
	}
}

// When the runtime cannot prove the writer stopped — status errors or a
// never-quiescing op — the cut stays pending instead of proceeding on
// hope. The session remains open so a later retry can succeed.
func TestReturnSealPendingWhenQuiesceNeverProven(t *testing.T) {
	h := setup(t, returnsession.Config{
		TerminalQuiesceTimeout: 300 * time.Millisecond,
		TerminalQuiescePoll:    20 * time.Millisecond,
	})
	h.sessions.SetFileStore(&fakeFiles{})
	procs := newFakeProcs()
	procs.neverQuiesce = true
	h.sessions.SetTerminalProcesses(procs)

	term := newID(t)
	termRow(t, h, term, "interrupted")
	procs.addOp(h.persona, term, false)

	sid, _, grant := h.createMode("local")
	code, raw := bindDest(t, h, sid, grant)
	if code != http.StatusConflict {
		t.Fatalf("unproven quiesce: %d %s", code, raw)
	}
	var body struct {
		Code string `json:"code"`
	}
	unmarshal(t, raw, &body)
	if body.Code != "terminal_quiesce_pending" {
		t.Fatalf("code = %q: %s", body.Code, raw)
	}
	if a := authority(t, h.cloud, h.persona); a != "active" {
		t.Fatalf("authority = %s", a)
	}

	// The same holds when the provisioner itself errors on status.
	procs.mu.Lock()
	procs.statusErr = errors.New("provisioner unreachable")
	procs.mu.Unlock()
	code, raw = bindDest(t, h, sid, grant)
	if code != http.StatusConflict {
		t.Fatalf("status error: %d %s", code, raw)
	}
	unmarshal(t, raw, &body)
	if body.Code != "terminal_quiesce_pending" {
		t.Fatalf("code = %q: %s", body.Code, raw)
	}
}

// Terminal sessions on the persona but no wired process surface must
// refuse the cut — absent evidence is never proof of stop.
func TestReturnSealRefusesWhenProcessesUnwired(t *testing.T) {
	h := setup(t, returnsession.Config{})
	h.sessions.SetFileStore(&fakeFiles{})
	termRow(t, h, newID(t), "interrupted")

	sid, _, grant := h.createMode("local")
	code, raw := bindDest(t, h, sid, grant)
	if code != http.StatusConflict {
		t.Fatalf("unwired processes: %d %s", code, raw)
	}
}

// Preflight surfaces open writers before the person picks a destination.
func TestReturnPreflightListsPendingTerminalSessions(t *testing.T) {
	h := setup(t, returnsession.Config{})
	h.sessions.SetFileStore(&fakeFiles{})
	h.sessions.SetTerminalProcesses(newFakeProcs())
	term := newID(t)
	termRow(t, h, term, "active")

	sid, _, grant := h.createMode("local")
	code, raw := h.grantReq(http.MethodGet,
		fmt.Sprintf("/api/secretary-return/sessions/%s", sid), grant, nil)
	if code != http.StatusOK {
		t.Fatalf("view: %d %s", code, raw)
	}
	v := asView(t, raw)
	if v.Preflight == nil {
		t.Fatalf("view has no preflight: %s", raw)
	}
	found := false
	for _, name := range v.Preflight.PendingTerminalSessions {
		if strings.Contains(name, term) {
			found = true
		}
	}
	if !found {
		t.Fatalf("pending_terminal_sessions = %v, want %s", v.Preflight.PendingTerminalSessions, term)
	}
}

// A requested-but-never-claimed session must not launch after the cut:
// sealing proceeds (there is no writer to wait for), then the claim path
// must refuse, and cancelling the return must make it claimable again.
func TestTerminalClaimFencesSealedPersona(t *testing.T) {
	h := setup(t, returnsession.Config{})
	h.sessions.SetFileStore(&fakeFiles{})
	h.sessions.SetTerminalProcesses(newFakeProcs())

	term := newID(t)
	termRow(t, h, term, "requested")

	// RunnableTerminalPersonas exposes the persona while active.
	ids, err := h.cloud.state.RunnableTerminalPersonas(h.ctx, "runner", "cloud", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != h.persona {
		t.Fatalf("runnable personas = %v", ids)
	}

	sid, _, grant := h.createMode("local")
	code, raw := bindDest(t, h, sid, grant)
	if code != http.StatusOK {
		t.Fatalf("seal: %d %s", code, raw)
	}
	if a := authority(t, h.cloud, h.persona); a != "sealed" {
		t.Fatalf("authority = %s", a)
	}

	// Post-cut: discovery and claim both see nothing claimable.
	ids, err = h.cloud.state.RunnableTerminalPersonas(h.ctx, "runner", "cloud", 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if id == h.persona {
			t.Fatalf("sealed persona %s is still runnable", id)
		}
	}
	claimed, _, err := h.cloud.state.ClaimTerminalSessions(h.ctx, h.persona, "runner#late", "cloud", time.Minute, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 0 {
		t.Fatalf("claimed %d sessions on a sealed persona", len(claimed))
	}

	// Cancel the return through its full proof path: cancel marks the
	// session cancelling, the destination tombstones the never-arrived
	// transfer, and the retired report unseals the source. Only then is
	// the persona active and the queued session claimable again.
	code, raw = h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/cancel", sid), grant, nil)
	if code != http.StatusOK {
		t.Fatalf("cancel: %d %s", code, raw)
	}
	if v := asView(t, raw); v.Status != returnsession.StatusCancelling {
		t.Fatalf("status %s", v.Status)
	}
	v := asView(t, raw)
	rec, err := h.local.svc.Retire(h.ctx, h.persona, sid, mustPlacement(t, h.local), v.TransferKey)
	if err != nil {
		t.Fatalf("tombstone: %v", err)
	}
	code, raw = h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/retired", sid),
		grant, jsonBody(map[string]string{"retire_proof": rec.RetireProof}))
	if code != http.StatusOK {
		t.Fatalf("retired: %d %s", code, raw)
	}
	if a := authority(t, h.cloud, h.persona); a != "active" {
		t.Fatalf("authority after abort = %s", a)
	}
	claimed, _, err = h.cloud.state.ClaimTerminalSessions(h.ctx, h.persona, "runner#1", "cloud", time.Minute, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].SessionID != term {
		t.Fatalf("post-cancel claim = %+v", claimed)
	}
}

// The serialization itself: a claim that arrives while the seal
// transaction holds the persona FOR NO KEY UPDATE must wait on its
// FOR SHARE, then observe the committed sealed authority and claim
// nothing — never a stale 'active' read from before the cut.
func TestTerminalClaimWaitsBehindSealAndClaimsNothing(t *testing.T) {
	h := setup(t, returnsession.Config{})
	h.sessions.SetFileStore(&fakeFiles{})
	h.sessions.SetTerminalProcesses(newFakeProcs())
	term := newID(t)
	termRow(t, h, term, "requested")

	// Stand in for the seal transaction: persona row FOR NO KEY UPDATE.
	lockTx, err := h.cloud.pool.Begin(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lockTx.Rollback(h.ctx)
	if _, err := lockTx.Exec(h.ctx,
		`SELECT 1 FROM core_personas WHERE persona_id = $1 FOR NO KEY UPDATE`, h.persona); err != nil {
		t.Fatalf("persona lock: %v", err)
	}

	type result struct {
		n   int
		err error
	}
	done := make(chan result, 1)
	go func() {
		cl, _, err := h.cloud.state.ClaimTerminalSessions(h.ctx, h.persona, "runner#mid", "cloud", time.Minute, 4)
		done <- result{len(cl), err}
	}()

	// The claim must be blocked on the persona lock, not racing ahead.
	select {
	case r := <-done:
		t.Fatalf("claim completed while the seal lock was held: %+v", r)
	case <-time.After(300 * time.Millisecond):
	}

	// Commit the authority transition while the claim still waits.
	if _, err := lockTx.Exec(h.ctx,
		`UPDATE core_personas SET authority = 'sealed' WHERE persona_id = $1`, h.persona); err != nil {
		t.Fatalf("seal update: %v", err)
	}
	if err := lockTx.Commit(h.ctx); err != nil {
		t.Fatalf("seal commit: %v", err)
	}

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("claim after seal: %v", r.err)
		}
		if r.n != 0 {
			t.Fatalf("claim that waited behind the seal still claimed %d sessions", r.n)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("claim never unblocked after the seal committed")
	}
}
