package messaging

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
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
	rooms := &recordingRoomService{}
	calls.RoomService = rooms

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
