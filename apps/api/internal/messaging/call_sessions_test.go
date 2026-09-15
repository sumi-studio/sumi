package messaging

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/portable"
)

// recordingRoomService fakes the RoomService boundary: the session layer's
// claims are about the durable record and about which removal calls the API
// makes — actual LiveKit behavior is exercised by the live-stack e2e.
type recordingRoomService struct {
	participants []liveKitParticipant
	removed      []string
}

func (s *recordingRoomService) ListRooms(context.Context) ([]liveKitRoom, error) {
	return nil, nil
}
func (s *recordingRoomService) ListParticipants(_ context.Context, room string) ([]liveKitParticipant, error) {
	return s.participants, nil
}
func (s *recordingRoomService) RemoveParticipant(_ context.Context, room, identity string) error {
	s.removed = append(s.removed, identity)
	return nil
}

// newCallSessionWorld builds the shared world, a DM between the human and
// the secretary, a live call in the registry, and a CallService with the
// durable session store wired.
func newCallSessionWorld(t *testing.T, ctx context.Context) (world, *CallService, *agentstate.Store, Place) {
	t.Helper()
	w := newSharedIntakeWorld(t, ctx)
	w.workspaceWithChannel(t, ctx)
	dm, _, err := w.store.EnsureDM(ctx, w.humanA, w.agent)
	if err != nil {
		t.Fatalf("ensure dm: %v", err)
	}
	coreStore := agentstate.NewStore(w.store.core.pool)
	// The secretary's core persona exists and holds 'active' placement
	// authority — the bridge gates every call mutation on it.
	if _, _, err := coreStore.EnsurePersona(ctx, w.agent.ID, nil, "Test Secretary"); err != nil {
		t.Fatalf("ensure persona: %v", err)
	}
	calls := &CallService{
		Server:   &Server{Store: w.store.core},
		LiveKit:  testLiveKit(),
		Registry: NewCallRegistry(),
		Hooks:    &CallHooks{Core: coreStore},
	}
	for tool, effect := range map[string]agentstate.ToolEffect{
		CallJoinTool:  calls.CallJoinEffect(),
		CallLeaveTool: calls.CallLeaveEffect(),
		CallSayTool:   calls.CallSayEffect(),
		CallStateTool: calls.CallStateEffect(),
	} {
		if err := coreStore.RegisterEffect(tool, effect); err != nil {
			t.Fatalf("register %s: %v", tool, err)
		}
	}
	calls.applyWebhook(ctx, livekitWebhookEvent{Event: "room_started", Room: struct {
		Name string `json:"name"`
		SID  string `json:"sid"`
	}{Name: dm.PlaceID, SID: testRoomSID}})
	return w, calls, coreStore, dm
}

// applyCallEffect invokes a delegated effect inside a transaction exactly
// the way the operation-claim path would.
func applyCallEffect(t *testing.T, ctx context.Context, pool *pgxpool.Pool, effect agentstate.ToolEffect, personaID string, request map[string]any) map[string]any {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	result, err := effect.Apply(ctx, tx, personaID, "idem-"+personaID, request)
	if err != nil {
		t.Fatalf("effect: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	return result
}

// applyCallEffectErr runs the effect and returns its error for tests that
// expect refusal; a successful apply is committed and reported as a nil
// error.
func applyCallEffectErr(ctx context.Context, pool *pgxpool.Pool, effect agentstate.ToolEffect, personaID string, request map[string]any) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := effect.Apply(ctx, tx, personaID, "idem-"+personaID, request); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func callSessionRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sessionID string) CallSession {
	t.Helper()
	var s CallSession
	if err := pool.QueryRow(ctx, `SELECT `+callSessionCols+` FROM call_sessions WHERE session_id=$1`, sessionID).
		Scan(callSessionScan(&s)...); err != nil {
		t.Fatalf("load session: %v", err)
	}
	return s
}

func callUtteranceRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, utteranceID string) CallUtterance {
	t.Helper()
	var u CallUtterance
	if err := pool.QueryRow(ctx, `SELECT `+callUtteranceCols+` FROM call_utterances WHERE utterance_id=$1`, utteranceID).
		Scan(callUtteranceScan(&u)...); err != nil {
		t.Fatalf("load utterance: %v", err)
	}
	return u
}

func joinCallSession(t *testing.T, ctx context.Context, calls *CallService, pool *pgxpool.Pool, personaID, placeID string) CallSession {
	t.Helper()
	result := applyCallEffect(t, ctx, pool, calls.CallJoinEffect(), personaID, map[string]any{"place_id": placeID})
	session, _ := result["session"].(CallSession)
	if session.SessionID == "" {
		t.Fatalf("join result = %v", result)
	}
	return session
}

func sayCallUtterance(t *testing.T, ctx context.Context, calls *CallService, pool *pgxpool.Pool, personaID, sessionID, text string) CallUtterance {
	t.Helper()
	result := applyCallEffect(t, ctx, pool, calls.CallSayEffect(), personaID,
		map[string]any{"session_id": sessionID, "text": text})
	u, _ := result["utterance"].(CallUtterance)
	if u.UtteranceID == "" {
		t.Fatalf("say result = %v", result)
	}
	return u
}

func TestCallSessionJoinClaimLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, calls, _, dm := newCallSessionWorld(t, ctx)
	pool := w.store.core.pool

	session := joinCallSession(t, ctx, calls, pool, w.agent.ID, dm.PlaceID)
	if session.Status != CallSessionRequested {
		t.Fatalf("session status = %s", session.Status)
	}
	// A second join while the first is live returns the same session — no
	// second presence fork.
	again := joinCallSession(t, ctx, calls, pool, w.agent.ID, dm.PlaceID)
	if again.SessionID != session.SessionID {
		t.Fatalf("rejoin forked session %s vs %s", again.SessionID, session.SessionID)
	}

	claimed, err := calls.ClaimCallSessions(ctx, w.agent.ID, "runner-a", 30*time.Second, 4)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 1 || claimed[0].Epoch != 1 || claimed[0].Status != CallSessionClaimed {
		t.Fatalf("claimed = %+v", claimed)
	}
	// A second runner cannot claim the held session.
	other, err := calls.ClaimCallSessions(ctx, w.agent.ID, "runner-b", 30*time.Second, 4)
	if err != nil {
		t.Fatalf("second claim pass: %v", err)
	}
	if len(other) != 0 {
		t.Fatalf("runner-b claimed %+v while runner-a holds it", other)
	}

	// The ticket binds the epoch-tagged identity and the room name.
	ticket, err := calls.CallSessionTicket(ctx, w.agent.ID, session.SessionID, "runner-a", 1)
	if err != nil {
		t.Fatalf("ticket: %v", err)
	}
	if ticket.Identity != w.agent.Key()+"#e1" || ticket.Room != dm.PlaceID {
		t.Fatalf("ticket = %+v", ticket)
	}

	if _, err := calls.ReportCallSessionStatus(ctx, w.agent.ID, session.SessionID, "runner-a", 1, CallSessionActive, "connected"); err != nil {
		t.Fatalf("mark active: %v", err)
	}
	hb, err := calls.HeartbeatCallSession(ctx, w.agent.ID, session.SessionID, "runner-a", 1, 30*time.Second)
	if err != nil || hb.Status != CallSessionActive {
		t.Fatalf("heartbeat: %v %+v", err, hb)
	}
	// A stale runner/epoch cannot heartbeat, mint, or move status.
	if _, err := calls.HeartbeatCallSession(ctx, w.agent.ID, session.SessionID, "runner-b", 1, 30*time.Second); !errors.Is(err, ErrCallClaimLost) {
		t.Fatalf("stale heartbeat = %v", err)
	}
	if _, err := calls.CallSessionTicket(ctx, w.agent.ID, session.SessionID, "runner-b", 1); !errors.Is(err, ErrCallClaimLost) {
		t.Fatalf("stale ticket = %v", err)
	}

	// call.leave moves a claim-held session to ending; the claim holder
	// reports ended and the record becomes terminal.
	leave := applyCallEffect(t, ctx, pool, calls.CallLeaveEffect(), w.agent.ID,
		map[string]any{"session_id": session.SessionID})
	if ended, _ := leave["session"].(CallSession); ended.Status != CallSessionEnding {
		t.Fatalf("leave session = %+v", ended)
	}
	final, err := calls.ReportCallSessionStatus(ctx, w.agent.ID, session.SessionID, "runner-a", 1, CallSessionEnded, "call.leave")
	if err != nil || final.Status != CallSessionEnded {
		t.Fatalf("end report: %v %+v", err, final)
	}
	row := callSessionRow(t, ctx, pool, session.SessionID)
	if row.EndedAt == nil || row.ClaimedBy != "" {
		t.Fatalf("terminal row = %+v", row)
	}
}

func TestCallSessionClaimLapseReclaimsWithoutReplay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, calls, _, dm := newCallSessionWorld(t, ctx)
	pool := w.store.core.pool

	session := joinCallSession(t, ctx, calls, pool, w.agent.ID, dm.PlaceID)
	claimed, err := calls.ClaimCallSessions(ctx, w.agent.ID, "runner-a", 30*time.Second, 4)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v %+v", err, claimed)
	}
	utterance := sayCallUtterance(t, ctx, calls, pool, w.agent.ID, session.SessionID, "first answer")
	pending, err := calls.PendingCallUtterances(ctx, w.agent.ID, session.SessionID, "runner-a", 1)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending: %v %+v", err, pending)
	}
	// Runner dequeues and begins emitting — then dies mid-flight.
	if _, err := calls.ReportUtteranceDisposition(ctx, w.agent.ID, session.SessionID, utterance.UtteranceID, "runner-a", 1, CallUtteranceDequeued, nil); err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if _, err := calls.ReportUtteranceDisposition(ctx, w.agent.ID, session.SessionID, utterance.UtteranceID, "runner-a", 1, CallUtteranceEmitting, nil); err != nil {
		t.Fatalf("emitting: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`UPDATE call_sessions SET claim_expires_at = now() - interval '1 second' WHERE session_id=$1`,
		session.SessionID); err != nil {
		t.Fatalf("expire claim: %v", err)
	}

	// The dead runner's stale epoch loses all authority immediately.
	if _, err := calls.PendingCallUtterances(ctx, w.agent.ID, session.SessionID, "runner-a", 1); !errors.Is(err, ErrCallClaimLost) {
		t.Fatalf("stale pending read = %v", err)
	}
	if _, err := calls.ReportUtteranceDisposition(ctx, w.agent.ID, session.SessionID, utterance.UtteranceID, "runner-a", 1, CallUtteranceEmitted, nil); !errors.Is(err, ErrCallClaimLost) {
		t.Fatalf("stale disposition = %v", err)
	}

	// A successor claim reclaims the session at a new epoch and the
	// mid-flight utterance is recorded unknown — never returned for replay.
	reclaimed, err := calls.ClaimCallSessions(ctx, w.agent.ID, "runner-b", 30*time.Second, 4)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if len(reclaimed) != 1 || reclaimed[0].Epoch != 2 || reclaimed[0].Status != CallSessionClaimed {
		t.Fatalf("reclaimed = %+v", reclaimed)
	}
	u := callUtteranceRow(t, ctx, pool, utterance.UtteranceID)
	if u.Status != CallUtteranceUnknown {
		t.Fatalf("mid-flight utterance status = %s, want unknown", u.Status)
	}
	pending, err = calls.PendingCallUtterances(ctx, w.agent.ID, session.SessionID, "runner-b", 2)
	if err != nil {
		t.Fatalf("successor pending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("successor was offered %d stale utterances", len(pending))
	}
	// The superseded-epoch utterance is unreportable even under the new claim.
	if _, err := calls.ReportUtteranceDisposition(ctx, w.agent.ID, session.SessionID, utterance.UtteranceID, "runner-b", 2, CallUtteranceEmitted, nil); !errors.Is(err, ErrUtteranceTerminal) {
		t.Fatalf("superseded utterance disposition = %v", err)
	}
}

func TestCallSessionEndSweepsPendingSpeech(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, calls, _, dm := newCallSessionWorld(t, ctx)
	pool := w.store.core.pool

	session := joinCallSession(t, ctx, calls, pool, w.agent.ID, dm.PlaceID)
	if _, err := calls.ClaimCallSessions(ctx, w.agent.ID, "runner-a", 30*time.Second, 4); err != nil {
		t.Fatalf("claim: %v", err)
	}
	queued := sayCallUtterance(t, ctx, calls, pool, w.agent.ID, session.SessionID, "never picked up")
	// call.leave on a claimed-but-not-active session marks ending; the holder
	// reports ended, and pending speech expires — it cannot be heard later.
	applyCallEffect(t, ctx, pool, calls.CallLeaveEffect(), w.agent.ID,
		map[string]any{"session_id": session.SessionID})
	if _, err := calls.ReportCallSessionStatus(ctx, w.agent.ID, session.SessionID, "runner-a", 1, CallSessionEnded, "call.leave"); err != nil {
		t.Fatalf("end: %v", err)
	}
	u := callUtteranceRow(t, ctx, pool, queued.UtteranceID)
	if u.Status != CallUtteranceExpired {
		t.Fatalf("queued utterance status = %s, want expired", u.Status)
	}
	// The session is terminal: no further claim and no more speech.
	if _, err := calls.ClaimCallSessions(ctx, w.agent.ID, "runner-c", 30*time.Second, 4); err != nil {
		t.Fatalf("claim after end: %v", err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	_, sayErr := calls.CallSayEffect().Apply(ctx, tx, w.agent.ID, "idem-x",
		map[string]any{"session_id": session.SessionID, "text": "too late"})
	_ = tx.Rollback(context.Background())
	if !errors.Is(sayErr, agentstate.ErrBadRequest) {
		t.Fatalf("say on ended session = %v", sayErr)
	}
}

func TestCallJoinNeedsActiveCallAndMembership(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, calls, _, dm := newCallSessionWorld(t, ctx)
	pool := w.store.core.pool

	// No call in a fresh registry snapshot → join fails honestly.
	calls.Registry = NewCallRegistry()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	_, joinErr := calls.CallJoinEffect().Apply(ctx, tx, w.agent.ID, "idem-1",
		map[string]any{"place_id": dm.PlaceID})
	_ = tx.Rollback(context.Background())
	if joinErr == nil || !strings.Contains(joinErr.Error(), ErrNoActiveCall.Error()) {
		t.Fatalf("join without call = %v", joinErr)
	}

	// A stranger persona is not a place member and cannot join.
	calls.applyWebhook(ctx, livekitWebhookEvent{Event: "room_started", Room: struct {
		Name string `json:"name"`
		SID  string `json:"sid"`
	}{Name: dm.PlaceID, SID: testRoomSID}})
	stranger := w.mintHuman(t, ctx, "Stranger")
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	_, joinErr = calls.CallJoinEffect().Apply(ctx, tx, stranger.ID, "idem-2",
		map[string]any{"place_id": dm.PlaceID})
	_ = tx.Rollback(context.Background())
	if !errors.Is(joinErr, agentstate.ErrBadRequest) {
		t.Fatalf("non-member join = %v", joinErr)
	}
}

func TestCallStartedInputIsIdempotentPerRoom(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, calls, _, dm := newCallSessionWorld(t, ctx)
	pool := w.store.core.pool

	calls.notifyCallStarted(dm.PlaceID, testRoomSID)
	calls.notifyCallStarted(dm.PlaceID, testRoomSID)
	var count int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM core_inputs WHERE persona_id=$1 AND kind='call_started'`,
		w.agent.ID).Scan(&count); err != nil {
		t.Fatalf("count inputs: %v", err)
	}
	if count != 1 {
		t.Fatalf("call_started inputs = %d, want exactly one per room", count)
	}
	var attention, threadID string
	if err := pool.QueryRow(ctx,
		`SELECT attention, thread_id::text FROM core_inputs WHERE persona_id=$1 AND kind='call_started'`,
		w.agent.ID).Scan(&attention, &threadID); err != nil {
		t.Fatalf("read input: %v", err)
	}
	if attention != "reply" || threadID != dm.PlaceID {
		t.Fatalf("call_started attention/thread = %s/%s", attention, threadID)
	}
}

func TestRoomFinishEndsSessionAndNotifies(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, calls, _, dm := newCallSessionWorld(t, ctx)
	pool := w.store.core.pool

	session := joinCallSession(t, ctx, calls, pool, w.agent.ID, dm.PlaceID)
	if _, err := calls.ClaimCallSessions(ctx, w.agent.ID, "runner-a", 30*time.Second, 4); err != nil {
		t.Fatalf("claim: %v", err)
	}
	calls.endCallSessionsForRoom(dm.PlaceID, testRoomSID)
	row := callSessionRow(t, ctx, pool, session.SessionID)
	if row.Status != CallSessionEnded || row.EndReason != "room_finished" || row.ClaimedBy != "" {
		t.Fatalf("room-finished session = %+v", row)
	}
	var inputs int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM core_inputs WHERE persona_id=$1 AND kind='call_event'`,
		w.agent.ID).Scan(&inputs); err != nil {
		t.Fatalf("count call_event inputs: %v", err)
	}
	if inputs != 1 {
		t.Fatalf("call_event inputs = %d, want 1", inputs)
	}
}

func TestStaleEpochParticipantIsRemovedPrecisely(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, calls, _, dm := newCallSessionWorld(t, ctx)
	pool := w.store.core.pool
	// The stub fakes the removal boundary only: a registry rebuild from its
	// empty room list would erase the webhook-built call the join needs.
	rooms := &recordingRoomService{}
	calls.RoomService = rooms
	calls.rebuiltOnce = true

	session := joinCallSession(t, ctx, calls, pool, w.agent.ID, dm.PlaceID)
	if _, err := calls.ClaimCallSessions(ctx, w.agent.ID, "runner-a", 30*time.Second, 4); err != nil {
		t.Fatalf("claim: %v", err)
	}
	// Registry-observed joins are checked against the durable epoch.
	stale := fmt.Sprintf("%s#e9", w.agent.Key())
	currentRef, _ := participantFromIdentity(w.agent.Key() + "#e1")
	calls.removeStaleEpochParticipant(ctx, dm.PlaceID, stale, ParticipantRef{Kind: KindPersonalityAgent, ID: w.agent.ID})
	if len(rooms.removed) != 1 || rooms.removed[0] != stale {
		t.Fatalf("stale removal = %v", rooms.removed)
	}
	// The current epoch's identity is left alone.
	calls.removeStaleEpochParticipant(ctx, dm.PlaceID, w.agent.Key()+"#e1", currentRef)
	if len(rooms.removed) != 1 {
		t.Fatalf("current-epoch identity was removed: %v", rooms.removed)
	}
	// A fresh claim sweep removes every connected actor of this persona —
	// whichever generation it belongs to — and never touches the human.
	rooms.participants = []liveKitParticipant{
		{Identity: w.agent.Key() + "#e1"},
		{Identity: w.humanA.Key()},
	}
	if _, err := pool.Exec(ctx,
		`UPDATE call_sessions SET claim_expires_at = now() - interval '1 second' WHERE session_id=$1`,
		session.SessionID); err != nil {
		t.Fatalf("expire claim: %v", err)
	}
	reclaimed, err := calls.ClaimCallSessions(ctx, w.agent.ID, "runner-b", 30*time.Second, 4)
	if err != nil || len(reclaimed) != 1 {
		t.Fatalf("successor claim: %v %+v", err, reclaimed)
	}
	foundStaleSweep := false
	for _, removed := range rooms.removed {
		if removed == w.humanA.Key() {
			t.Fatalf("claim sweep removed the human participant")
		}
		if removed == w.agent.Key()+"#e1" {
			foundStaleSweep = true
		}
	}
	if !foundStaleSweep {
		t.Fatalf("claim sweep did not remove prior actor: %v", rooms.removed)
	}
}

// A call.say committed between call.join and the runner's claim is speech
// no generation ever had a chance to play: the first claim adopts it into
// its epoch and the runner delivers it once — it is not expired as stale,
// and the disposition machine accepts it end to end.
func TestCallSayBeforeClaimIsAdoptedByFirstClaim(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, calls, _, dm := newCallSessionWorld(t, ctx)
	pool := w.store.core.pool

	session := joinCallSession(t, ctx, calls, pool, w.agent.ID, dm.PlaceID)
	say := applyCallEffect(t, ctx, pool, calls.CallSayEffect(), w.agent.ID,
		map[string]any{"session_id": session.SessionID, "text": "hello before claim"})
	utterance, _ := say["utterance"].(CallUtterance)
	if utterance.UtteranceID == "" || utterance.SessionEpoch != 0 {
		t.Fatalf("preclaim utterance = %+v", utterance)
	}
	if say["awaiting_claim"] != true {
		t.Fatalf("preclaim say result lacks awaiting_claim: %v", say)
	}

	claimed, err := calls.ClaimCallSessions(ctx, w.agent.ID, "runner-a", 30*time.Second, 4)
	if err != nil || len(claimed) != 1 || claimed[0].Epoch != 1 {
		t.Fatalf("claim: %v %+v", err, claimed)
	}
	u := callUtteranceRow(t, ctx, pool, utterance.UtteranceID)
	if u.Status != CallUtteranceIntended || u.SessionEpoch != 1 {
		t.Fatalf("adopted utterance = %+v", u)
	}
	pending, err := calls.PendingCallUtterances(ctx, w.agent.ID, session.SessionID, "runner-a", 1)
	if err != nil || len(pending) != 1 || pending[0].UtteranceID != utterance.UtteranceID {
		t.Fatalf("pending after adoption: %v %+v", err, pending)
	}
	// The full disposition machine accepts the adopted utterance.
	for _, status := range []string{CallUtteranceDequeued, CallUtteranceEmitting, CallUtteranceEmitted} {
		if _, err := calls.ReportUtteranceDisposition(ctx, w.agent.ID, session.SessionID,
			utterance.UtteranceID, "runner-a", 1, status, nil); err != nil {
			t.Fatalf("disposition %s: %v", status, err)
		}
	}
	u = callUtteranceRow(t, ctx, pool, utterance.UtteranceID)
	if u.Status != CallUtteranceEmitted {
		t.Fatalf("final utterance = %+v", u)
	}
}

// Speech committed while a session's epoch is already superseded can never
// be delivered — the next claim bumps the epoch and expires the row. The say
// is refused honestly rather than queued as 'awaiting_claim', and the model
// can act on the failure: after the successor claim lands, a fresh say
// commits under the live epoch. No speech from a dead claim is replayed.
func TestCallSayOnInterruptedExpiresAtReclaim(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, calls, _, dm := newCallSessionWorld(t, ctx)
	pool := w.store.core.pool

	session := joinCallSession(t, ctx, calls, pool, w.agent.ID, dm.PlaceID)
	if _, err := calls.ClaimCallSessions(ctx, w.agent.ID, "runner-a", 30*time.Second, 4); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`UPDATE call_sessions SET claim_expires_at = now() - interval '1 second' WHERE session_id=$1`,
		session.SessionID); err != nil {
		t.Fatalf("expire claim: %v", err)
	}
	// Variant A: the session still reads claimed-but-lapsed. The say must be
	// refused, not committed as a doomed 'awaiting_claim' intent.
	err := applyCallEffectErr(ctx, pool, calls.CallSayEffect(), w.agent.ID,
		map[string]any{"session_id": session.SessionID, "text": "said into a dead claim"})
	if !errors.Is(err, agentstate.ErrBadRequest) {
		t.Fatalf("say on lapsed claim = %v, want ErrBadRequest", err)
	}

	// Variant B: the lapse is swept first, leaving 'interrupted'.
	reclaimed, err := calls.ClaimCallSessions(ctx, w.agent.ID, "runner-b", 30*time.Second, 4)
	if err != nil || len(reclaimed) != 1 || reclaimed[0].Epoch != 2 {
		t.Fatalf("reclaim: %v %+v", err, reclaimed)
	}
	if _, err := pool.Exec(ctx,
		`UPDATE call_sessions SET status='interrupted', claimed_by=NULL, claim_expires_at=NULL WHERE session_id=$1`,
		session.SessionID); err != nil {
		t.Fatalf("interrupt: %v", err)
	}
	err = applyCallEffectErr(ctx, pool, calls.CallSayEffect(), w.agent.ID,
		map[string]any{"session_id": session.SessionID, "text": "said while interrupted"})
	if !errors.Is(err, agentstate.ErrBadRequest) {
		t.Fatalf("say on interrupted = %v, want ErrBadRequest", err)
	}

	// Nothing was committed on either doomed attempt.
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM call_utterances WHERE session_id=$1`,
		session.SessionID).Scan(&n); err != nil || n != 0 {
		t.Fatalf("doomed say committed rows: n=%d err=%v", n, err)
	}
	// Actionable outcome: once a claim again holds the epoch, a fresh say
	// commits and is deliverable.
	reclaimed, err = calls.ClaimCallSessions(ctx, w.agent.ID, "runner-c", 30*time.Second, 4)
	if err != nil || len(reclaimed) != 1 || reclaimed[0].Epoch != 3 {
		t.Fatalf("second reclaim: %v %+v", err, reclaimed)
	}
	say := applyCallEffect(t, ctx, pool, calls.CallSayEffect(), w.agent.ID,
		map[string]any{"session_id": session.SessionID, "text": "said after reclaim"})
	if say["awaiting_claim"] == true {
		t.Fatalf("post-reclaim say still claims awaiting_claim: %v", say)
	}
	utterance, _ := say["utterance"].(CallUtterance)
	u := callUtteranceRow(t, ctx, pool, utterance.UtteranceID)
	if u.Status != CallUtteranceIntended || u.SessionEpoch != 3 {
		t.Fatalf("post-reclaim utterance = %+v", u)
	}
	pending, err := calls.PendingCallUtterances(ctx, w.agent.ID, session.SessionID, "runner-c", 3)
	if err != nil || len(pending) != 1 || pending[0].UtteranceID != utterance.UtteranceID {
		t.Fatalf("successor pending: %v %+v", err, pending)
	}
}

// A committed call.leave must not be undone by a runner crash: when the
// claim lapses while 'ending', the session ends durably — the next claim
// pass does not rejoin a call the secretary deliberately left.
func TestEndingSessionEndsOnClaimLapse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, calls, _, dm := newCallSessionWorld(t, ctx)
	pool := w.store.core.pool

	session := joinCallSession(t, ctx, calls, pool, w.agent.ID, dm.PlaceID)
	if _, err := calls.ClaimCallSessions(ctx, w.agent.ID, "runner-a", 30*time.Second, 4); err != nil {
		t.Fatalf("claim: %v", err)
	}
	applyCallEffect(t, ctx, pool, calls.CallLeaveEffect(), w.agent.ID,
		map[string]any{"session_id": session.SessionID})
	// The runner dies before observing 'ending'.
	if _, err := pool.Exec(ctx,
		`UPDATE call_sessions SET claim_expires_at = now() - interval '1 second' WHERE session_id=$1`,
		session.SessionID); err != nil {
		t.Fatalf("expire claim: %v", err)
	}
	claimed, err := calls.ClaimCallSessions(ctx, w.agent.ID, "runner-b", 30*time.Second, 4)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if len(claimed) != 0 {
		t.Fatalf("ended-by-leave session was reclaimed: %+v", claimed)
	}
	row := callSessionRow(t, ctx, pool, session.SessionID)
	if row.Status != CallSessionEnded || row.EndReason != "claim_lapsed_after_leave" {
		t.Fatalf("lapsed ending session = %+v", row)
	}
	var inputs int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM core_inputs WHERE persona_id=$1 AND kind='call_event'`,
		w.agent.ID).Scan(&inputs); err != nil {
		t.Fatalf("count call_event inputs: %v", err)
	}
	if inputs != 1 {
		t.Fatalf("call_event inputs = %d, want 1", inputs)
	}
}

// Omitted disposition detail stores a JSON object, and a later sweep merge
// keeps the row an object — never the [null, {...}] array corruption.
func TestUtteranceDetailStaysCoherentAcrossSweeps(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, calls, _, dm := newCallSessionWorld(t, ctx)
	pool := w.store.core.pool

	session := joinCallSession(t, ctx, calls, pool, w.agent.ID, dm.PlaceID)
	if _, err := calls.ClaimCallSessions(ctx, w.agent.ID, "runner-a", 30*time.Second, 4); err != nil {
		t.Fatalf("claim: %v", err)
	}
	utterance := sayCallUtterance(t, ctx, calls, pool, w.agent.ID, session.SessionID, "detail-free")
	if _, err := calls.ReportUtteranceDisposition(ctx, w.agent.ID, session.SessionID,
		utterance.UtteranceID, "runner-a", 1, CallUtteranceDequeued, nil); err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	var raw string
	if err := pool.QueryRow(ctx,
		`SELECT detail::text FROM call_utterances WHERE utterance_id=$1`,
		utterance.UtteranceID).Scan(&raw); err != nil {
		t.Fatalf("read detail: %v", err)
	}
	if raw != "{}" {
		t.Fatalf("nil detail stored as %s, want {}", raw)
	}
	// Lapse sweeps the mid-flight utterance; the merge must keep detail an
	// object and the row scannable.
	if _, err := pool.Exec(ctx,
		`UPDATE call_sessions SET claim_expires_at = now() - interval '1 second' WHERE session_id=$1`,
		session.SessionID); err != nil {
		t.Fatalf("expire claim: %v", err)
	}
	if _, err := calls.ClaimCallSessions(ctx, w.agent.ID, "runner-b", 30*time.Second, 4); err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	u := callUtteranceRow(t, ctx, pool, utterance.UtteranceID)
	if u.Status != CallUtteranceUnknown || u.Detail["reason"] == nil {
		t.Fatalf("swept utterance = %+v", u)
	}
}

// A delayed room_finished for a dead room generation must not end sessions
// stamped with the current generation's SID — the durable room binding is
// what protects them after a restart wipes the volatile registry.
func TestStaleRoomFinishSparesCurrentGeneration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, calls, _, dm := newCallSessionWorld(t, ctx)
	pool := w.store.core.pool

	session := joinCallSession(t, ctx, calls, pool, w.agent.ID, dm.PlaceID)
	if session.RoomSID != testRoomSID {
		t.Fatalf("join stamped room_sid %q, want %q", session.RoomSID, testRoomSID)
	}
	// A finish event for a room SID this session never belonged to leaves
	// it live.
	calls.endCallSessionsForRoom(dm.PlaceID, "RM_dead_generation")
	row := callSessionRow(t, ctx, pool, session.SessionID)
	if row.Status != CallSessionRequested {
		t.Fatalf("session ended by foreign room_finished: %+v", row)
	}
	// The matching generation's finish still ends it.
	calls.endCallSessionsForRoom(dm.PlaceID, testRoomSID)
	row = callSessionRow(t, ctx, pool, session.SessionID)
	if row.Status != CallSessionEnded || row.EndReason != "room_finished" {
		t.Fatalf("session after own room_finished: %+v", row)
	}
}

// A claim that has lapsed carries no write authority — even in the window
// before any successor reclaims the session. Heartbeat, status, ticket, and
// utterance reports all fail with ErrCallClaimLost.
func TestExpiredClaimHasNoWriteAuthority(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, calls, _, dm := newCallSessionWorld(t, ctx)
	pool := w.store.core.pool

	session := joinCallSession(t, ctx, calls, pool, w.agent.ID, dm.PlaceID)
	if _, err := calls.ClaimCallSessions(ctx, w.agent.ID, "runner-a", 30*time.Second, 4); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if _, err := calls.ReportCallSessionStatus(ctx, w.agent.ID, session.SessionID, "runner-a", 1, CallSessionActive, "connected"); err != nil {
		t.Fatalf("activate: %v", err)
	}
	utterance := sayCallUtterance(t, ctx, calls, pool, w.agent.ID, session.SessionID, "hello")
	if _, err := pool.Exec(ctx,
		`UPDATE call_sessions SET claim_expires_at = now() - interval '1 second' WHERE session_id=$1`,
		session.SessionID); err != nil {
		t.Fatalf("expire claim: %v", err)
	}
	if _, err := calls.HeartbeatCallSession(ctx, w.agent.ID, session.SessionID, "runner-a", 1, 30*time.Second); !errors.Is(err, ErrCallClaimLost) {
		t.Fatalf("heartbeat on expired claim = %v", err)
	}
	if _, err := calls.CallSessionTicket(ctx, w.agent.ID, session.SessionID, "runner-a", 1); !errors.Is(err, ErrCallClaimLost) {
		t.Fatalf("ticket on expired claim = %v", err)
	}
	if _, err := calls.ReportCallSessionStatus(ctx, w.agent.ID, session.SessionID, "runner-a", 1, CallSessionEnded, "bye"); !errors.Is(err, ErrCallClaimLost) {
		t.Fatalf("status on expired claim = %v", err)
	}
	if _, err := calls.ReportUtteranceDisposition(ctx, w.agent.ID, session.SessionID, utterance.UtteranceID, "runner-a", 1, CallUtteranceEmitted, nil); !errors.Is(err, ErrCallClaimLost) {
		t.Fatalf("disposition on expired claim = %v", err)
	}
}

// Human removal revokes the durable claim before the media kick lands, so
// the evicted runner's racing report hits a dead claim: 'removed_by_member'
// is the recorded cause, not the runner's 'evicted' reason.
func TestCallRemovalOutracesRunnerEvictedReport(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, calls, _, dm := newCallSessionWorld(t, ctx)
	pool := w.store.core.pool

	session := joinCallSession(t, ctx, calls, pool, w.agent.ID, dm.PlaceID)
	if _, err := calls.ClaimCallSessions(ctx, w.agent.ID, "runner-a", 30*time.Second, 4); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if _, err := calls.ReportCallSessionStatus(ctx, w.agent.ID, session.SessionID, "runner-a", 1, CallSessionActive, "connected"); err != nil {
		t.Fatalf("activate: %v", err)
	}
	// The remove route revokes first; the runner's evicted report arrives
	// after its claim is already gone.
	if err := calls.RevokePlaceCallSessions(ctx, w.agent.ID, dm.PlaceID, "removed_by_member"); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if _, err := calls.ReportCallSessionStatus(ctx, w.agent.ID, session.SessionID, "runner-a", 1, CallSessionFailed, "evicted:participant_removed"); !errors.Is(err, ErrCallClaimLost) {
		t.Fatalf("evicted report on revoked claim = %v", err)
	}
	row := callSessionRow(t, ctx, pool, session.SessionID)
	if row.Status != CallSessionRevoked || row.EndReason != "removed_by_member" {
		t.Fatalf("removed session = %+v", row)
	}
	// Removal ends current participation only — it is not a ban: the same
	// secretary may join a later call in the place.
	rejoined := joinCallSession(t, ctx, calls, pool, w.agent.ID, dm.PlaceID)
	if rejoined.SessionID == "" || rejoined.SessionID == session.SessionID {
		t.Fatalf("rejoin after removal = %+v", rejoined)
	}
}

// Seal is the placement-authority cut: live call participation is revoked
// in the same transaction that retires the persona, mid-flight speech is
// recorded honestly, and every call mutation is refused afterwards.
// Portable/direct-service proof — no HTTP or live-audio destination claim.
func TestSealRetiresLiveCallParticipation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, calls, _, dm := newCallSessionWorld(t, ctx)
	pool := w.store.core.pool

	session := joinCallSession(t, ctx, calls, pool, w.agent.ID, dm.PlaceID)
	if _, err := calls.ClaimCallSessions(ctx, w.agent.ID, "runner-a", 30*time.Second, 4); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if _, err := calls.ReportCallSessionStatus(ctx, w.agent.ID, session.SessionID, "runner-a", 1, CallSessionActive, "connected"); err != nil {
		t.Fatalf("activate: %v", err)
	}
	pending := sayCallUtterance(t, ctx, calls, pool, w.agent.ID, session.SessionID, "never dequeued")
	inflight := sayCallUtterance(t, ctx, calls, pool, w.agent.ID, session.SessionID, "mid-emit")
	for _, status := range []string{CallUtteranceDequeued, CallUtteranceEmitting} {
		if _, err := calls.ReportUtteranceDisposition(ctx, w.agent.ID, session.SessionID, inflight.UtteranceID, "runner-a", 1, status, nil); err != nil {
			t.Fatalf("%s: %v", status, err)
		}
	}

	svc := portable.NewService(pool)
	if _, err := svc.Seal(ctx, w.agent.ID, newUUIDv7(), newUUIDv7()); err != nil {
		t.Fatalf("seal: %v", err)
	}
	row := callSessionRow(t, ctx, pool, session.SessionID)
	if row.Status != CallSessionRevoked || row.EndReason != "transfer_sealed" || row.ClaimedBy != "" {
		t.Fatalf("sealed session = %+v", row)
	}
	if u := callUtteranceRow(t, ctx, pool, pending.UtteranceID); u.Status != CallUtteranceExpired || u.Detail["reason"] != "transfer_sealed" {
		t.Fatalf("sealed pending utterance = %+v", u)
	}
	if u := callUtteranceRow(t, ctx, pool, inflight.UtteranceID); u.Status != CallUtteranceUnknown || u.Detail["reason"] != "transfer_sealed" {
		t.Fatalf("sealed mid-flight utterance = %+v", u)
	}
	// The retired persona holds no call authority anywhere.
	if _, err := calls.HeartbeatCallSession(ctx, w.agent.ID, session.SessionID, "runner-a", 1, 30*time.Second); !errors.Is(err, ErrCallClaimLost) {
		t.Fatalf("heartbeat after seal = %v", err)
	}
	if _, err := calls.CallSessionTicket(ctx, w.agent.ID, session.SessionID, "runner-a", 1); !errors.Is(err, ErrCallClaimLost) {
		t.Fatalf("ticket after seal = %v", err)
	}
	if _, err := calls.ReportCallSessionStatus(ctx, w.agent.ID, session.SessionID, "runner-a", 1, CallSessionEnded, "bye"); !errors.Is(err, ErrCallClaimLost) {
		t.Fatalf("status after seal = %v", err)
	}
	// A successor claim pass is refused outright: the sealed persona holds
	// no authority under which a new runner could adopt the session.
	if _, err := calls.ClaimCallSessions(ctx, w.agent.ID, "runner-b", 30*time.Second, 4); err == nil || !strings.Contains(err.Error(), "authority is sealed") {
		t.Fatalf("post-seal claim pass = %v", err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	_, joinErr := calls.applyCallJoin(ctx, tx, w.agent.ID, "idem-sealed",
		map[string]any{"place_id": dm.PlaceID})
	_ = tx.Rollback(context.Background())
	if !errors.Is(joinErr, agentstate.ErrBadRequest) || !strings.Contains(joinErr.Error(), "authority") {
		t.Fatalf("join on sealed persona = %v", joinErr)
	}
}

// The seal and a call join serialize on the persona row: a join that opens
// while the seal holds FOR NO KEY UPDATE blocks, then observes the retired
// authority and is refused — no session can land on a retired placement.
func TestSealSerializesInFlightCallJoin(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, calls, _, dm := newCallSessionWorld(t, ctx)
	pool := w.store.core.pool

	lockTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin lock tx: %v", err)
	}
	if _, err := lockTx.Exec(ctx,
		`SELECT authority FROM core_personas WHERE persona_id=$1 FOR NO KEY UPDATE`,
		w.agent.ID); err != nil {
		t.Fatalf("seal-side lock: %v", err)
	}
	type joinResult struct{ err error }
	joinCh := make(chan joinResult, 1)
	go func() {
		jtx, err := pool.Begin(context.Background())
		if err != nil {
			joinCh <- joinResult{err}
			return
		}
		defer func() { _ = jtx.Rollback(context.Background()) }()
		_, err = calls.applyCallJoin(context.Background(), jtx, w.agent.ID, "idem-seal-race",
			map[string]any{"place_id": dm.PlaceID})
		joinCh <- joinResult{err}
	}()
	// The join must be blocked on the persona row lock, not sail through.
	select {
	case res := <-joinCh:
		t.Fatalf("join did not serialize with the seal lock: %v", res.err)
	case <-time.After(300 * time.Millisecond):
	}
	// Commit the authority cut the way Seal does.
	if _, err := lockTx.Exec(ctx,
		`UPDATE core_personas SET authority='sealed' WHERE persona_id=$1`,
		w.agent.ID); err != nil {
		t.Fatalf("seal-side update: %v", err)
	}
	if err := lockTx.Commit(ctx); err != nil {
		t.Fatalf("seal-side commit: %v", err)
	}
	select {
	case res := <-joinCh:
		if !errors.Is(res.err, agentstate.ErrBadRequest) || !strings.Contains(res.err.Error(), "authority") {
			t.Fatalf("racing join result = %v", res.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("join stayed blocked after the seal committed")
	}
	var sessions int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM call_sessions WHERE personality_agent_id=$1`,
		w.agent.ID).Scan(&sessions); err != nil {
		t.Fatalf("count sessions: %v", err)
	}
	if sessions != 0 {
		t.Fatalf("racing join left %d sessions", sessions)
	}
}

// Ticket issuance linearizes with the seal: a ticket request that reaches
// its claim check while the seal holds the persona lock waits, then sees
// retired authority and is refused — no fresh 60-second credential can be
// minted on a retired placement. A ticket that commits before the seal's
// lock lands is legitimately pre-seal issuance (accepted TTL window).
func TestTicketCannotMintAcrossSealCommit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, calls, _, dm := newCallSessionWorld(t, ctx)
	pool := w.store.core.pool

	session := joinCallSession(t, ctx, calls, pool, w.agent.ID, dm.PlaceID)
	if _, err := calls.ClaimCallSessions(ctx, w.agent.ID, "runner-a", 30*time.Second, 4); err != nil {
		t.Fatalf("claim: %v", err)
	}
	// A ticket before any retirement still mints — sanity.
	if _, err := calls.CallSessionTicket(ctx, w.agent.ID, session.SessionID, "runner-a", 1); err != nil {
		t.Fatalf("pre-seal ticket: %v", err)
	}

	lockTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin lock tx: %v", err)
	}
	if _, err := lockTx.Exec(ctx,
		`SELECT authority FROM core_personas WHERE persona_id=$1 FOR NO KEY UPDATE`,
		w.agent.ID); err != nil {
		t.Fatalf("seal-side lock: %v", err)
	}
	type ticketResult struct {
		ticket agentstate.CallTicket
		err    error
	}
	ticketCh := make(chan ticketResult, 1)
	go func() {
		tk, err := calls.CallSessionTicket(context.Background(), w.agent.ID,
			session.SessionID, "runner-a", 1)
		ticketCh <- ticketResult{tk, err}
	}()
	// The request must be blocked at the persona row — it cannot read the
	// pre-seal authority while the seal's lock is held.
	select {
	case res := <-ticketCh:
		t.Fatalf("ticket did not serialize with the seal lock: %+v / %v", res.ticket, res.err)
	case <-time.After(300 * time.Millisecond):
	}
	// Complete the retirement exactly as Seal commits it.
	if _, err := lockTx.Exec(ctx,
		`UPDATE core_personas SET authority='sealed' WHERE persona_id=$1`,
		w.agent.ID); err != nil {
		t.Fatalf("seal-side update: %v", err)
	}
	if _, err := lockTx.Exec(ctx,
		`UPDATE call_sessions SET status='revoked', ended_at=now(), end_reason='transfer_sealed',
		    claimed_by=NULL, claim_expires_at=NULL, updated_at=now()
		 WHERE personality_agent_id=$1 AND status IN ('requested','claimed','active','ending','interrupted')`,
		w.agent.ID); err != nil {
		t.Fatalf("seal-side session revoke: %v", err)
	}
	if err := lockTx.Commit(ctx); err != nil {
		t.Fatalf("seal-side commit: %v", err)
	}
	select {
	case res := <-ticketCh:
		if !errors.Is(res.err, ErrCallClaimLost) || res.ticket.Token != "" {
			t.Fatalf("post-seal ticket = %+v / %v", res.ticket, res.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("ticket request stayed blocked after the seal committed")
	}
}

// The same boundary holds for ordinary revocation, not just retirement: a
// session row lock held by the remove/seal sweep serializes with issuance,
// so a ticket cannot be minted from a claim that has already died.
func TestTicketCannotMintAcrossRevocation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, calls, _, dm := newCallSessionWorld(t, ctx)
	pool := w.store.core.pool

	session := joinCallSession(t, ctx, calls, pool, w.agent.ID, dm.PlaceID)
	if _, err := calls.ClaimCallSessions(ctx, w.agent.ID, "runner-a", 30*time.Second, 4); err != nil {
		t.Fatalf("claim: %v", err)
	}
	// Hold the session row the way a revoke/update would.
	lockTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin lock tx: %v", err)
	}
	if _, err := lockTx.Exec(ctx,
		`SELECT session_id FROM call_sessions WHERE session_id=$1 FOR NO KEY UPDATE`,
		session.SessionID); err != nil {
		t.Fatalf("revoke-side lock: %v", err)
	}
	type ticketResult struct {
		ticket agentstate.CallTicket
		err    error
	}
	ticketCh := make(chan ticketResult, 1)
	go func() {
		tk, err := calls.CallSessionTicket(context.Background(), w.agent.ID,
			session.SessionID, "runner-a", 1)
		ticketCh <- ticketResult{tk, err}
	}()
	select {
	case res := <-ticketCh:
		t.Fatalf("ticket did not serialize with the session row lock: %+v / %v", res.ticket, res.err)
	case <-time.After(300 * time.Millisecond):
	}
	// The revoke commits first; the blocked ticket then observes a dead claim.
	if _, err := lockTx.Exec(ctx,
		`UPDATE call_sessions SET status='revoked', ended_at=now(), end_reason='removed_by_member',
		    claimed_by=NULL, claim_expires_at=NULL, updated_at=now() WHERE session_id=$1`,
		session.SessionID); err != nil {
		t.Fatalf("revoke-side update: %v", err)
	}
	if err := lockTx.Commit(ctx); err != nil {
		t.Fatalf("revoke-side commit: %v", err)
	}
	select {
	case res := <-ticketCh:
		if !errors.Is(res.err, ErrCallClaimLost) || res.ticket.Token != "" {
			t.Fatalf("post-revocation ticket = %+v / %v", res.ticket, res.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("ticket request stayed blocked after the revoke committed")
	}
}

// Authenticated HTTP boundary for the retirement cut: the real mounted
// agentstate bridge routes (persona capability token) and the real mounted
// portable seal route (admin secret) over httptest. Before the seal the
// runner token claims and mints; after a correct admin seal the same token
// cannot renew, mint, or report — the credential still authenticates, it
// just carries no call authority.
func TestSealCutsCallBridgeAuthorityOverHTTP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, calls, _, dm := newCallSessionWorld(t, ctx)
	pool := w.store.core.pool
	const adminSecret = "test-admin-secret-for-call-seal"

	mux := http.NewServeMux()
	core := agentstate.NewServer(pool, adminSecret)
	core.SetCallBridge(calls)
	core.RegisterRoutes(mux)
	portable.NewServer(pool, adminSecret).RegisterRoutes(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	runnerToken := core.PersonaToken(w.agent.ID)
	post := func(path, token, body string) *http.Response {
		req, err := http.NewRequest(http.MethodPost, srv.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatalf("request %s: %v", path, err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("post %s: %v", path, err)
		}
		return res
	}

	session := joinCallSession(t, ctx, calls, pool, w.agent.ID, dm.PlaceID)
	res := post("/internal/core/personas/"+w.agent.ID+"/calls/claim", runnerToken,
		`{"runner_id":"runner-a","lease_ms":30000,"limit":4}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("HTTP claim = %d", res.StatusCode)
	}
	_ = res.Body.Close()
	res = post("/internal/core/personas/"+w.agent.ID+"/calls/sessions/"+session.SessionID+"/ticket",
		runnerToken, `{"runner_id":"runner-a","epoch":1}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("HTTP ticket = %d", res.StatusCode)
	}
	_ = res.Body.Close()

	// The admin seal route runs the real Seal service: sessions revoked.
	transferID := newUUIDv7()
	res = post("/internal/core/personas/"+w.agent.ID+"/transfers/"+transferID+"/seal",
		adminSecret, `{"destination_id":"`+newUUIDv7()+`"}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("HTTP seal = %d", res.StatusCode)
	}
	_ = res.Body.Close()
	row := callSessionRow(t, ctx, pool, session.SessionID)
	if row.Status != CallSessionRevoked || row.EndReason != "transfer_sealed" {
		t.Fatalf("sealed session = %+v", row)
	}

	// The persona credential still authenticates but carries no call
	// authority: heartbeat, ticket, and status are all 409 claim_lost.
	for _, tc := range []struct{ path, body string }{
		{"/internal/core/personas/" + w.agent.ID + "/calls/sessions/" + session.SessionID + "/heartbeat",
			`{"runner_id":"runner-a","epoch":1,"lease_ms":30000}`},
		{"/internal/core/personas/" + w.agent.ID + "/calls/sessions/" + session.SessionID + "/ticket",
			`{"runner_id":"runner-a","epoch":1}`},
		{"/internal/core/personas/" + w.agent.ID + "/calls/sessions/" + session.SessionID + "/status",
			`{"runner_id":"runner-a","epoch":1,"status":"ended","reason":"bye"}`},
	} {
		res := post(tc.path, runnerToken, tc.body)
		if res.StatusCode != http.StatusConflict {
			t.Fatalf("post-seal %s = %d, want 409", tc.path, res.StatusCode)
		}
		_ = res.Body.Close()
	}
	// A fresh claim pass is refused too.
	res = post("/internal/core/personas/"+w.agent.ID+"/calls/claim", runnerToken,
		`{"runner_id":"runner-b","lease_ms":30000,"limit":4}`)
	if res.StatusCode != http.StatusConflict {
		t.Fatalf("post-seal HTTP claim = %d, want 409", res.StatusCode)
	}
	_ = res.Body.Close()
}

// Revocation and its speech sweep are one transaction: an injected failure
// at the sweep boundary leaves NO acknowledged revocation behind — the
// session row rolls back with the utterances, so nothing is left revoked
// with permanently non-terminal speech. Once the fault clears, the same
// call completes both halves atomically.
func TestRevokeKeepsSpeechAndSessionAtomic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, calls, _, dm := newCallSessionWorld(t, ctx)
	pool := w.store.core.pool

	session := joinCallSession(t, ctx, calls, pool, w.agent.ID, dm.PlaceID)
	if _, err := calls.ClaimCallSessions(ctx, w.agent.ID, "runner-a", 30*time.Second, 4); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if _, err := calls.ReportCallSessionStatus(ctx, w.agent.ID, session.SessionID, "runner-a", 1, CallSessionActive, "connected"); err != nil {
		t.Fatalf("activate: %v", err)
	}
	// One still-intended utterance and one mid-flight (dequeued) one, so
	// both terminal outcomes are witnessed.
	intended := sayCallUtterance(t, ctx, calls, pool, w.agent.ID, session.SessionID, "never picked up")
	dequeued := sayCallUtterance(t, ctx, calls, pool, w.agent.ID, session.SessionID, "mid flight")
	if _, err := calls.ReportUtteranceDisposition(ctx, w.agent.ID, session.SessionID,
		dequeued.UtteranceID, "runner-a", 1, CallUtteranceDequeued, nil); err != nil {
		t.Fatalf("dequeue: %v", err)
	}

	// Inject a fault inside the sweep half of the revocation transaction.
	if _, err := pool.Exec(ctx, `
		CREATE OR REPLACE FUNCTION fail_utterance_sweep() RETURNS trigger AS $$
		BEGIN RAISE EXCEPTION 'injected sweep failure'; END; $$ LANGUAGE plpgsql;
		CREATE TRIGGER fail_utterance_sweep BEFORE UPDATE ON call_utterances
			FOR EACH ROW EXECUTE FUNCTION fail_utterance_sweep()`); err != nil {
		t.Fatalf("install trigger: %v", err)
	}
	err := calls.RevokePlaceCallSessions(ctx, w.agent.ID, dm.PlaceID, "removed_by_member")
	if err == nil {
		t.Fatal("revoke under injected sweep fault succeeded")
	}
	// Atomicity witness: no acknowledged revocation exists — the session is
	// still live and both utterances remain in their pre-revoke states.
	row := callSessionRow(t, ctx, pool, session.SessionID)
	if row.Status != CallSessionActive || row.ClaimedBy != "runner-a" {
		t.Fatalf("session after failed revoke = %+v, want still claimed", row)
	}
	if u := callUtteranceRow(t, ctx, pool, intended.UtteranceID); u.Status != CallUtteranceIntended {
		t.Fatalf("intended utterance after failed revoke = %+v", u)
	}
	if u := callUtteranceRow(t, ctx, pool, dequeued.UtteranceID); u.Status != CallUtteranceDequeued {
		t.Fatalf("dequeued utterance after failed revoke = %+v", u)
	}

	// Fault cleared: revocation now completes both halves in one commit.
	if _, err := pool.Exec(ctx, `DROP TRIGGER fail_utterance_sweep ON call_utterances`); err != nil {
		t.Fatalf("drop trigger: %v", err)
	}
	if err := calls.RevokePlaceCallSessions(ctx, w.agent.ID, dm.PlaceID, "removed_by_member"); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	row = callSessionRow(t, ctx, pool, session.SessionID)
	if row.Status != CallSessionRevoked || row.EndReason != "removed_by_member" {
		t.Fatalf("session after revoke = %+v", row)
	}
	// 'intended' was never handed to media → expired; 'dequeued' may have
	// been partially emitted → unknown. Neither claims a listener heard it.
	if u := callUtteranceRow(t, ctx, pool, intended.UtteranceID); u.Status != CallUtteranceExpired {
		t.Fatalf("intended utterance after revoke = %+v", u)
	}
	if u := callUtteranceRow(t, ctx, pool, dequeued.UtteranceID); u.Status != CallUtteranceUnknown {
		t.Fatalf("dequeued utterance after revoke = %+v", u)
	}
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM call_utterances
		 WHERE session_id=$1 AND status IN ('intended','dequeued','emitting')`,
		session.SessionID).Scan(&n); err != nil || n != 0 {
		t.Fatalf("non-terminal speech on revoked session: n=%d err=%v", n, err)
	}
}
