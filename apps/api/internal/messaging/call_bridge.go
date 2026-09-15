package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/canonicalid"
)

// The bridge-facing half of call sessions. A per-placement media runner
// authenticates with its persona token and drives sessions through claim →
// ticket → status → utterance dispositions. Every write re-checks the claim
// (runner id + epoch + unexpired lease) so a runner that lost authority can
// neither mint media credentials nor move durable state.
//
// What this does NOT do: it cannot reach into LiveKit to stop an already
// connected participant. LiveKit fencing is the join-side eviction of the
// prior epoch's identity plus explicit RemoveParticipant sweeps; a runner
// that keeps a stale socket alive after losing its claim is outside the
// API's enforcement boundary and is handled by the runner's own stop rule.

// Compile-time binding to the persona-scoped surface in agentstate.
var _ agentstate.CallBridge = (*CallService)(nil)

func clampCallLease(lease time.Duration) time.Duration {
	if lease <= 0 {
		return 30 * time.Second
	}
	if lease > CallBridgeMaxLease {
		return CallBridgeMaxLease
	}
	return lease
}

// ClaimCallSessions sweeps lapsed claims to 'interrupted' (and their pending
// speech to 'unknown'), then claims up to limit requested/interrupted
// sessions for this persona under a fresh epoch each.
func (c *CallService) ClaimCallSessions(ctx context.Context, personaID, runnerID string, lease time.Duration, limit int) ([]agentstate.CallSession, error) {
	if runnerID == "" {
		return nil, fmt.Errorf("%w: runner_id is required", agentstate.ErrBadRequest)
	}
	if limit <= 0 {
		limit = 4
	}
	lease = clampCallLease(lease)
	tx, err := c.Server.Store.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	// Placement authority first: a persona whose authority was sealed or
	// transferred cannot gain or renew call presence. The share lock also
	// serializes this claim against an in-flight seal of the same persona.
	authority, found, err := c.callPersonaAuthorityInTx(ctx, tx, personaID)
	if err != nil {
		return nil, err
	}
	if !found || authority != "active" {
		return nil, fmt.Errorf("%w: persona authority is %s", ErrCallClaimLost, authority)
	}

	// A claim whose lease lapsed no longer holds authority. A session the
	// secretary already committed to leaving ('ending') ends for good —
	// durable departure intent survives the crash window and the next claim
	// must not resurrect presence. Sessions still wanted live become
	// reclaimable 'interrupted' and their unfinished speech is recorded —
	// never auto-replayed by a later claim.
	lapsed, err := tx.Query(ctx, `
		UPDATE call_sessions
		SET status = CASE WHEN status = 'ending' THEN 'ended' ELSE 'interrupted' END,
		    ended_at = CASE WHEN status = 'ending' THEN now() ELSE ended_at END,
		    end_reason = CASE WHEN status = 'ending' THEN 'claim_lapsed_after_leave' ELSE end_reason END,
		    claimed_by = NULL, claim_expires_at = NULL, updated_at = now()
		WHERE personality_agent_id = $1
		  AND status IN ('claimed','active','ending')
		  AND claim_expires_at < now()
		RETURNING `+callSessionCols, personaID)
	if err != nil {
		return nil, err
	}
	lapsedSessions := []CallSession{}
	for lapsed.Next() {
		var session CallSession
		if err := lapsed.Scan(callSessionScan(&session)...); err != nil {
			lapsed.Close()
			return nil, err
		}
		lapsedSessions = append(lapsedSessions, session)
	}
	lapsed.Close()
	if err := lapsed.Err(); err != nil {
		return nil, err
	}
	endedOnLapse := []CallSession{}
	for _, session := range lapsedSessions {
		if session.Status == CallSessionEnded {
			endedOnLapse = append(endedOnLapse, session)
		}
		if err := sweepCallUtterancesInTx(ctx, tx, session.SessionID, "runner_claim_lapsed"); err != nil {
			return nil, err
		}
	}

	rows, err := tx.Query(ctx, `
		WITH picked AS (
			SELECT session_id FROM call_sessions
			WHERE personality_agent_id = $1 AND status IN ('requested','interrupted')
			ORDER BY created_at
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		UPDATE call_sessions s
		SET status='claimed', epoch = s.epoch + 1, claimed_by=$3,
		    claim_expires_at = now() + $4::interval, updated_at=now()
		FROM picked WHERE s.session_id = picked.session_id
		RETURNING s.session_id, s.workspace_id, s.place_id, s.personality_agent_id,
			COALESCE(s.room_sid, ''), s.status, s.epoch, COALESCE(s.claimed_by, ''),
			s.claim_expires_at, s.requested_by, s.created_at, s.updated_at,
			s.ended_at, COALESCE(s.end_reason, '')`,
		personaID, limit, runnerID, fmt.Sprintf("%f seconds", lease.Seconds()))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	claimed := []agentstate.CallSession{}
	claimedSessions := []CallSession{}
	for rows.Next() {
		var session CallSession
		if err := rows.Scan(callSessionScan(&session)...); err != nil {
			return nil, err
		}
		claimedSessions = append(claimedSessions, session)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, session := range claimedSessions {
		if session.RoomSID == "" {
			// Bind the session's room generation durably so a delayed
			// room_finished for a dead generation cannot end it.
			if sid := c.currentRoomSID(session.PlaceID); sid != "" {
				if _, err := tx.Exec(ctx, `
					UPDATE call_sessions SET room_sid = $2
					WHERE session_id = $1 AND (room_sid IS NULL OR room_sid = '')`,
					session.SessionID, sid); err != nil {
					return nil, err
				}
			}
		}
		if err := c.adoptOrSweepUtterancesInTx(ctx, tx, session); err != nil {
			return nil, err
		}
		claimed = append(claimed, callSessionWire(session))
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit call session claim: %w", err)
	}
	for _, session := range endedOnLapse {
		c.notifyCallEnded(ctx, session)
	}
	// A fresh claim supersedes every connected actor of this persona in that
	// room — any still-connected identity belongs to a dead or stale claim
	// generation. Sweep best-effort; the webhook observer catches stragglers.
	if c.RoomService != nil {
		for _, session := range claimed {
			c.sweepPersonaCallParticipants(ctx, session.PlaceID, personaID)
		}
	}
	return claimed, nil
}

// adoptOrSweepUtterancesInTx resolves every non-terminal utterance when a
// claim takes a session. Speech committed before any claim existed
// (session_epoch 0) was never exposed to a media generation, so it is
// adopted into the new epoch and delivered once — the model was told
// 'queued', not 'spoken'. Speech tied to a superseded generation may
// already have been heard; it is never replayed: 'intended' expires and
// anything mid-flight is recorded unknown.
func (c *CallService) adoptOrSweepUtterancesInTx(ctx context.Context, tx pgx.Tx, session CallSession) error {
	if _, err := tx.Exec(ctx, `
		UPDATE call_utterances SET session_epoch = $2, updated_at = now()
		WHERE session_id = $1 AND session_epoch = 0 AND status = 'intended'`,
		session.SessionID, session.Epoch); err != nil {
		return err
	}
	detail, err := json.Marshal(map[string]any{"reason": "epoch_superseded"})
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		UPDATE call_utterances
		SET status = CASE WHEN status = 'intended' THEN 'expired' ELSE 'unknown' END,
		    detail = CASE WHEN jsonb_typeof(detail) = 'object'
		                  THEN detail || $3::jsonb ELSE $3::jsonb END,
		    updated_at = now()
		WHERE session_id = $1 AND session_epoch <> $2
		  AND status IN ('intended','dequeued','emitting')`,
		session.SessionID, session.Epoch, detail)
	return err
}

// sweepPersonaCallParticipants removes every connected participant whose ref
// is this persona in the place's room. Runs right after a claim epoch bump,
// when any connected actor is by definition from a superseded generation.
func (c *CallService) sweepPersonaCallParticipants(ctx context.Context, placeID, personaID string) {
	participants, err := c.RoomService.ListParticipants(ctx, placeID)
	if err != nil {
		return
	}
	target := PersonalityAgent(personaID)
	for _, entry := range participants {
		if ref, err := participantFromIdentity(entry.Identity); err == nil && ref == target {
			c.RemoveStaleCallParticipant(ctx, placeID, entry.Identity)
		}
	}
}

// HeartbeatCallSession renews the claim and returns the session row so the
// runner observes 'ending' promptly. A lost claim is a hard error — the
// runner must tear down media and stop without retrying.
func (c *CallService) HeartbeatCallSession(ctx context.Context, personaID, sessionID, runnerID string, epoch int64, lease time.Duration) (agentstate.CallSession, error) {
	lease = clampCallLease(lease)
	var session CallSession
	err := withCallTx(ctx, c.Server.Store.pool, func(tx pgx.Tx) error {
		authority, found, err := c.callPersonaAuthorityInTx(ctx, tx, personaID)
		if err != nil {
			return err
		}
		if !found || authority != "active" {
			return fmt.Errorf("%w: persona authority is %s", ErrCallClaimLost, authority)
		}
		return tx.QueryRow(ctx, `
			UPDATE call_sessions
			SET claim_expires_at = now() + $4::interval, updated_at = now()
			WHERE session_id = $1 AND personality_agent_id = $2
			  AND claimed_by = $3 AND epoch = $5
			  AND claim_expires_at > now()
			  AND status IN ('claimed','active','ending')
			RETURNING `+callSessionCols,
			sessionID, personaID, runnerID, fmt.Sprintf("%f seconds", lease.Seconds()), epoch,
		).Scan(callSessionScan(&session)...)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return agentstate.CallSession{}, ErrCallClaimLost
	}
	if err != nil {
		return agentstate.CallSession{}, err
	}
	return callSessionWire(session), nil
}

// CallSessionTicket mints a short room credential for the session's current
// epoch-tagged identity. Minting is gated on the live claim: a runner whose
// claim lapsed cannot refresh a ticket, and a ticket's remaining life is
// bounded by CallBridgeTicketTTL regardless.
func (c *CallService) CallSessionTicket(ctx context.Context, personaID, sessionID, runnerID string, epoch int64) (agentstate.CallTicket, error) {
	if !c.LiveKit.configured() {
		return agentstate.CallTicket{}, errors.New("LiveKit is not configured")
	}
	session, err := c.requireCallClaim(ctx, personaID, sessionID, runnerID, epoch)
	if err != nil {
		return agentstate.CallTicket{}, err
	}
	if session.Status != CallSessionClaimed && session.Status != CallSessionActive {
		return agentstate.CallTicket{}, fmt.Errorf("%w: status %s cannot mint a ticket", ErrCallSessionNotLive, session.Status)
	}
	if session.RoomSID == "" {
		// The session predates the registry learning the room SID — stamp it
		// now so a stale room_finished for a dead generation can't end it.
		if sid := c.currentRoomSID(session.PlaceID); sid != "" {
			if _, err := c.Server.Store.pool.Exec(ctx, `
				UPDATE call_sessions SET room_sid = $2
				WHERE session_id = $1 AND (room_sid IS NULL OR room_sid = '')`,
				sessionID, sid); err == nil {
				session.RoomSID = sid
			}
		}
	}
	identity := callSessionIdentity(&session)
	token, err := c.LiveKit.accessToken(session.PlaceID, identity, "", c.now(), CallBridgeTicketTTL)
	if err != nil {
		return agentstate.CallTicket{}, err
	}
	return agentstate.CallTicket{
		URL:      c.LiveKit.URL,
		Token:    token,
		Room:     session.PlaceID,
		Identity: identity,
	}, nil
}

// ReportCallSessionStatus moves a session along its lifecycle under the
// caller's live claim: claimed→active, any live→ending, and any claim-held
// status→ended/failed. Terminal transitions clear the claim, stamp
// end_reason, sweep pending speech, and record a call-ended core input.
func (c *CallService) ReportCallSessionStatus(ctx context.Context, personaID, sessionID, runnerID string, epoch int64, status, reason string) (agentstate.CallSession, error) {
	var session CallSession
	var notify bool
	err := withCallTx(ctx, c.Server.Store.pool, func(tx pgx.Tx) error {
		authority, found, err := c.callPersonaAuthorityInTx(ctx, tx, personaID)
		if err != nil {
			return err
		}
		if !found || authority != "active" {
			return fmt.Errorf("%w: persona authority is %s", ErrCallClaimLost, authority)
		}
		var current CallSession
		err = tx.QueryRow(ctx, `
			SELECT `+callSessionCols+` FROM call_sessions
			WHERE session_id = $1 AND personality_agent_id = $2 FOR UPDATE`,
			sessionID, personaID).Scan(callSessionScan(&current)...)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrCallSessionNotFound
		}
		if err != nil {
			return err
		}
		if current.ClaimedBy != runnerID || current.Epoch != epoch ||
			!callSessionLive(current.Status) || current.Status == CallSessionRequested ||
			current.ClaimExpiresAt == nil || !current.ClaimExpiresAt.After(c.now()) {
			return ErrCallClaimLost
		}
		switch status {
		case CallSessionActive:
			if current.Status != CallSessionClaimed {
				return fmt.Errorf("%w: cannot mark %s active", ErrCallSessionNotLive, current.Status)
			}
		case CallSessionEnding:
		case CallSessionEnded, CallSessionFailed:
		default:
			return fmt.Errorf("%w: status %s", agentstate.ErrBadRequest, status)
		}
		if status == CallSessionEnded || status == CallSessionFailed {
			err = tx.QueryRow(ctx, `
				UPDATE call_sessions
				SET status=$2, ended_at=now(), end_reason=$3, updated_at=now(),
				    claimed_by=NULL, claim_expires_at=NULL
				WHERE session_id=$1 RETURNING `+callSessionCols,
				sessionID, status, reason).Scan(callSessionScan(&session)...)
			if err != nil {
				return err
			}
			if err := sweepCallUtterancesInTx(ctx, tx, sessionID, "session_"+status); err != nil {
				return err
			}
			notify = true
			return nil
		}
		err = tx.QueryRow(ctx, `
			UPDATE call_sessions SET status=$2, updated_at=now()
			WHERE session_id=$1 RETURNING `+callSessionCols,
			sessionID, status).Scan(callSessionScan(&session)...)
		return err
	})
	if err != nil {
		return agentstate.CallSession{}, err
	}
	if notify {
		c.notifyCallEnded(ctx, session)
	}
	return callSessionWire(session), nil
}

// PendingCallUtterances returns the session's 'intended' speech under a live
// claim. Dequeue is a separate disposition report so a bridge crash between
// read and play cannot double-emit.
func (c *CallService) PendingCallUtterances(ctx context.Context, personaID, sessionID, runnerID string, epoch int64) ([]agentstate.CallUtterance, error) {
	session, err := c.requireCallClaim(ctx, personaID, sessionID, runnerID, epoch)
	if err != nil {
		return nil, err
	}
	// Only utterances bound to the claiming epoch are deliverable; stale-
	// epoch rows are already swept, but the filter keeps a read between
	// claim and sweep from ever exposing one to a runner.
	rows, err := c.Server.Store.pool.Query(ctx, `
		SELECT `+callUtteranceCols+` FROM call_utterances
		WHERE session_id = $1 AND status = 'intended' AND session_epoch = $2
		ORDER BY seq LIMIT 50`,
		sessionID, session.Epoch)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []agentstate.CallUtterance{}
	for rows.Next() {
		var u CallUtterance
		if err := rows.Scan(callUtteranceScan(&u)...); err != nil {
			return nil, err
		}
		out = append(out, callUtteranceWire(u))
	}
	return out, rows.Err()
}

// callUtteranceTransitions is the bridge-driven disposition machine. Every
// hop is recorded, so the journal can distinguish intended / dequeued /
// emitting / emitted / interrupted / expired / failed / unknown.
var callUtteranceTransitions = map[string]map[string]bool{
	CallUtteranceIntended: {
		CallUtteranceDequeued: true, CallUtteranceInterrupted: true,
		CallUtteranceExpired: true, CallUtteranceFailed: true,
		CallUtteranceUnknown: true,
	},
	CallUtteranceDequeued: {
		CallUtteranceEmitting: true, CallUtteranceInterrupted: true,
		CallUtteranceFailed: true, CallUtteranceUnknown: true,
	},
	CallUtteranceEmitting: {
		CallUtteranceEmitted: true, CallUtteranceInterrupted: true,
		CallUtteranceFailed: true, CallUtteranceUnknown: true,
	},
}

// ReportUtteranceDisposition moves one utterance under the session's live
// claim. Invalid or terminal-state transitions are rejected; the bridge must
// report each hop it actually performed rather than skipping to the end.
func (c *CallService) ReportUtteranceDisposition(ctx context.Context, personaID, sessionID, utteranceID, runnerID string, epoch int64, status string, detail map[string]any) (agentstate.CallUtterance, error) {
	if !canonicalid.IsUUIDv7(utteranceID) {
		return agentstate.CallUtterance{}, fmt.Errorf("%w: utterance id must be a uuidv7", agentstate.ErrBadRequest)
	}
	var utterance CallUtterance
	err := withCallTx(ctx, c.Server.Store.pool, func(tx pgx.Tx) error {
		session, err := c.requireCallClaimInTx(ctx, tx, personaID, sessionID, runnerID, epoch)
		if err != nil {
			return err
		}
		if session.Epoch != epoch {
			return ErrCallClaimLost
		}
		var current CallUtterance
		err = tx.QueryRow(ctx, `
			SELECT `+callUtteranceCols+` FROM call_utterances
			WHERE utterance_id = $1 AND session_id = $2 FOR UPDATE`,
			utteranceID, sessionID).Scan(callUtteranceScan(&current)...)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrUtteranceNotFound
		}
		if err != nil {
			return err
		}
		if current.SessionEpoch != session.Epoch {
			// Speech committed under a superseded claim generation is never
			// replayable; only its recorded disposition may be observed.
			return fmt.Errorf("%w: utterance epoch %d is superseded", ErrUtteranceTerminal, current.SessionEpoch)
		}
		if !callUtteranceTransitions[current.Status][status] {
			if _, terminal := map[string]bool{
				CallUtteranceEmitted: true, CallUtteranceInterrupted: true,
				CallUtteranceExpired: true, CallUtteranceFailed: true,
				CallUtteranceUnknown: true,
			}[current.Status]; terminal {
				return fmt.Errorf("%w: %s is already %s", ErrUtteranceTerminal, utteranceID, current.Status)
			}
			return fmt.Errorf("%w: %s -> %s", agentstate.ErrBadRequest, current.Status, status)
		}
		if detail == nil {
			// The API shape is an object; storing a bare null would make a
			// later detail merge produce an array and corrupt the row.
			detail = map[string]any{}
		}
		encoded, err := json.Marshal(detail)
		if err != nil {
			return fmt.Errorf("%w: detail is not json", agentstate.ErrBadRequest)
		}
		// Merge rather than replace detail: each hop's report (dequeue
		// timestamp, interruption fraction, failure) stays readable, later
		// keys win.
		return tx.QueryRow(ctx, `
			UPDATE call_utterances
			SET status=$3,
			    detail = CASE WHEN jsonb_typeof(detail) = 'object'
			                  THEN detail ELSE '{}'::jsonb END || $4::jsonb,
			    updated_at=now()
			WHERE utterance_id=$1 AND session_id=$2
			RETURNING `+callUtteranceCols,
			utteranceID, sessionID, status, encoded,
		).Scan(callUtteranceScan(&utterance)...)
	})
	if err != nil {
		return agentstate.CallUtterance{}, err
	}
	return callUtteranceWire(utterance), nil
}

// RevokePlaceCallSessions terminally revokes the persona's live sessions for
// a place: called when membership/admission is withdrawn (kick or workspace
// membership close). Media removal is done separately by the caller through
// RoomService.
func (c *CallService) RevokePlaceCallSessions(ctx context.Context, personaID, placeID, reason string) error {
	var revoked []CallSession
	err := withCallTx(ctx, c.Server.Store.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			UPDATE call_sessions
			SET status='revoked', ended_at=now(), end_reason=$3, updated_at=now(),
			    claimed_by=NULL, claim_expires_at=NULL
			WHERE personality_agent_id=$1 AND place_id=$2
			  AND status IN ('requested','claimed','active','ending','interrupted')
			RETURNING `+callSessionCols,
			personaID, placeID, reason)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var session CallSession
			if err := rows.Scan(callSessionScan(&session)...); err != nil {
				return err
			}
			revoked = append(revoked, session)
		}
		return rows.Err()
	})
	if err != nil {
		return err
	}
	for _, session := range revoked {
		tx, err := c.Server.Store.pool.Begin(ctx)
		if err != nil {
			continue
		}
		if err := sweepCallUtterancesInTx(ctx, tx, session.SessionID, "session_revoked"); err != nil {
			_ = tx.Rollback(context.Background())
			continue
		}
		if err := tx.Commit(ctx); err != nil {
			continue
		}
		c.notifyCallEnded(ctx, session)
	}
	return nil
}

// callPersonaAuthorityInTx share-locks the persona row so call authority
// serializes with a placement seal: the seal holds FOR NO KEY UPDATE, so a
// call mutation either commits before it (and is then revoked by the seal's
// own session sweep) or observes the retired authority and is refused.
// This is placement authority only — an ordinary writer-generation change
// never disturbs an independently live media claim.
func (c *CallService) callPersonaAuthorityInTx(ctx context.Context, tx pgx.Tx, personaID string) (string, bool, error) {
	var authority string
	err := tx.QueryRow(ctx,
		`SELECT authority FROM core_personas WHERE persona_id = $1 FOR SHARE`,
		personaID).Scan(&authority)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	return authority, true, err
}

// requireCallClaim loads the session and enforces the caller's live claim.
func (c *CallService) requireCallClaim(ctx context.Context, personaID, sessionID, runnerID string, epoch int64) (CallSession, error) {
	var session CallSession
	err := withCallTx(ctx, c.Server.Store.pool, func(tx pgx.Tx) error {
		s, err := c.requireCallClaimInTx(ctx, tx, personaID, sessionID, runnerID, epoch)
		session = s
		return err
	})
	return session, err
}

func (c *CallService) requireCallClaimInTx(ctx context.Context, tx pgx.Tx, personaID, sessionID, runnerID string, epoch int64) (CallSession, error) {
	if !canonicalid.IsUUIDv7(sessionID) {
		return CallSession{}, fmt.Errorf("%w: session id must be a uuidv7", agentstate.ErrBadRequest)
	}
	authority, found, err := c.callPersonaAuthorityInTx(ctx, tx, personaID)
	if err != nil {
		return CallSession{}, err
	}
	if !found || authority != "active" {
		return CallSession{}, fmt.Errorf("%w: persona authority is %s", ErrCallClaimLost, authority)
	}
	var session CallSession
	err = tx.QueryRow(ctx, `
		SELECT `+callSessionCols+` FROM call_sessions
		WHERE session_id = $1 AND personality_agent_id = $2`,
		sessionID, personaID).Scan(callSessionScan(&session)...)
	if errors.Is(err, pgx.ErrNoRows) {
		return CallSession{}, ErrCallSessionNotFound
	}
	if err != nil {
		return CallSession{}, err
	}
	if session.ClaimedBy != runnerID || session.Epoch != epoch {
		return CallSession{}, ErrCallClaimLost
	}
	if session.ClaimExpiresAt == nil || session.ClaimExpiresAt.Before(c.now()) {
		return CallSession{}, ErrCallClaimLost
	}
	return session, nil
}

// RemoveStaleCallParticipant drops an epoch-tagged PA participant whose tag
// does not match the session's current epoch — invoked by the webhook
// observer and by the claim sweep. Removal is precise because the identity
// carries the epoch, so this cannot evict the current generation.
func (c *CallService) RemoveStaleCallParticipant(ctx context.Context, placeID, identity string) {
	if c.RoomService == nil {
		return
	}
	if err := c.RoomService.RemoveParticipant(ctx, placeID, identity); err != nil {
		log.Printf("call: remove stale participant %s in %s: %v", identity, placeID, err)
	}
}

// ensureCallPersona lazily provisions the core persona for a place member
// before its input is submitted — the same ensure CoreAttentionDelivery
// performs on message admission. It never resurrects a sealed or
// transferred persona: EnsurePersona only creates a missing row, and
// SubmitInput still refuses non-active authority.
func (c *CallService) ensureCallPersona(ctx context.Context, paID string) error {
	core := c.coreStore()
	if core == nil {
		return nil
	}
	var humanID *string
	var displayName string
	var hid string
	err := c.Server.Store.pool.QueryRow(ctx,
		"SELECT human_id::text, display_name FROM agents WHERE personality_agent_id = $1",
		paID).Scan(&hid, &displayName)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// No agent record to provision from; SubmitInput will report the
		// missing persona and the caller logs it.
	case err != nil:
		return fmt.Errorf("resolve agent for call persona: %w", err)
	default:
		humanID = &hid
	}
	_, _, err = core.EnsurePersona(ctx, paID, humanID, displayName)
	return err
}

// notifyCallStarted delivers a durable 'call_started' input to each PA member
// of the place. Admission dedup is (persona, input_id) inside the core — the
// input id binds place + room sid so one call instance rings once.
func (c *CallService) notifyCallStarted(placeID, roomSID string) {
	core := c.coreStore()
	if core == nil || c == nil || c.Server == nil || c.Server.Store == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	rows, err := c.Server.Store.pool.Query(ctx, `
		SELECT pm.member_id::text, p.kind, p.workspace_id::text
		FROM place_members pm JOIN places p
		  ON p.workspace_id = pm.workspace_id AND p.place_id = pm.place_id
		WHERE pm.place_id = $1 AND pm.member_kind = 'personality_agent'
		  AND pm.left_at IS NULL`, placeID)
	if err != nil {
		log.Printf("call: list PA members for call_started %s: %v", placeID, err)
		return
	}
	defer rows.Close()
	type member struct {
		paID        string
		kind        string
		workspaceID string
	}
	members := []member{}
	for rows.Next() {
		var m member
		if err := rows.Scan(&m.paID, &m.kind, &m.workspaceID); err != nil {
			return
		}
		members = append(members, m)
	}
	for _, m := range members {
		attention := "observe"
		if m.kind == PlaceDM || m.kind == PlaceGroupDM {
			// A call in a direct place is addressed to the secretary; a
			// channel call is ambient unless the secretary judges otherwise.
			attention = "reply"
		}
		if err := c.ensureCallPersona(ctx, m.paID); err != nil {
			log.Printf("call: ensure persona %s for call_started: %v", m.paID, err)
			continue
		}
		_, _, err := core.SubmitInput(ctx, &agentstate.Input{
			PersonaID:     m.paID,
			InputID:       "call_started:" + placeID + ":" + roomSID,
			Kind:          "call_started",
			ActorKind:     "human",
			ActorID:       "",
			SourceSurface: "call",
			ThreadID:      placeID,
			Attention:     attention,
			Payload: map[string]any{
				"kind":         "call_started",
				"place_id":     placeID,
				"workspace_id": m.workspaceID,
				"room_sid":     roomSID,
				"place_kind":   m.kind,
			},
		})
		if err != nil && !errors.Is(err, agentstate.ErrPersonaInactive) {
			log.Printf("call: call_started input for %s in %s: %v", m.paID, placeID, err)
		}
	}
}

// notifyCallEnded records a durable 'call_event' input on terminal session
// transitions so the secretary's journal reflects that the call ended under
// this reason — including endings it did not initiate.
func (c *CallService) notifyCallEnded(ctx context.Context, session CallSession) {
	core := c.coreStore()
	if core == nil {
		return
	}
	submitCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := c.ensureCallPersona(submitCtx, session.PersonalityAgentID); err != nil {
		log.Printf("call: ensure persona %s for call_ended: %v", session.PersonalityAgentID, err)
		return
	}
	_, _, err := core.SubmitInput(submitCtx, &agentstate.Input{
		PersonaID:     session.PersonalityAgentID,
		InputID:       "call_ended:" + session.SessionID + ":" + session.Status,
		Kind:          "call_event",
		ActorKind:     "personality_agent",
		ActorID:       session.PersonalityAgentID,
		SourceSurface: "call",
		ThreadID:      session.PlaceID,
		Attention:     "observe",
		Payload: map[string]any{
			"event":      "call_ended",
			"session_id": session.SessionID,
			"place_id":   session.PlaceID,
			"status":     session.Status,
			"reason":     session.EndReason,
		},
	})
	if err != nil && !errors.Is(err, agentstate.ErrPersonaInactive) {
		log.Printf("call: call_ended input for session %s: %v", session.SessionID, err)
	}
}

func withCallTx(ctx context.Context, pool callPool, fn func(pgx.Tx) error) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// callPool is satisfied by *pgxpool.Pool.
type callPool interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

func callSessionWire(s CallSession) agentstate.CallSession {
	return agentstate.CallSession{
		SessionID:          s.SessionID,
		WorkspaceID:        s.WorkspaceID,
		PlaceID:            s.PlaceID,
		PersonalityAgentID: s.PersonalityAgentID,
		RoomSID:            s.RoomSID,
		Status:             s.Status,
		Epoch:              s.Epoch,
		ClaimedBy:          s.ClaimedBy,
		ClaimExpiresAt:     s.ClaimExpiresAt,
		RequestedBy:        s.RequestedBy,
		CreatedAt:          s.CreatedAt,
		UpdatedAt:          s.UpdatedAt,
		EndedAt:            s.EndedAt,
		EndReason:          s.EndReason,
	}
}

func callUtteranceWire(u CallUtterance) agentstate.CallUtterance {
	return agentstate.CallUtterance{
		UtteranceID:  u.UtteranceID,
		SessionID:    u.SessionID,
		SessionEpoch: u.SessionEpoch,
		Seq:          u.Seq,
		Text:         u.Text,
		Status:       u.Status,
		Detail:       u.Detail,
		CreatedAt:    u.CreatedAt,
		UpdatedAt:    u.UpdatedAt,
	}
}
