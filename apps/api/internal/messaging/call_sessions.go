package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	applicationapps "github.com/sumi-studio/sumi/apps/api/internal/apps"
	"github.com/sumi-studio/sumi/apps/api/internal/canonicalid"
)

// Call sessions are the durable authority record for a secretary's presence
// in a place's LiveKit room. The TypeScript core commits them through the
// delegated call.join effect; a per-placement media bridge claims, joins, and
// reports back through the persona-scoped bridge routes. The record owns the
// intended lifecycle — LiveKit owns actual connectivity, and the two are
// reconciled by heartbeats, webhooks, and explicit removal rather than by any
// claim of instantaneous fencing.
//
// Epoch: every (re)claim bumps epoch and the bridge joins LiveKit under an
// epoch-tagged identity (personality_agent:<pa>#e<epoch>). A stale-epoch
// participant is distinguishable from the current one, so removal can target
// exactly the dead generation instead of racing it.

const (
	CallSessionRequested   = "requested"
	CallSessionClaimed     = "claimed"
	CallSessionActive      = "active"
	CallSessionEnding      = "ending"
	CallSessionEnded       = "ended"
	CallSessionInterrupted = "interrupted"
	CallSessionRevoked     = "revoked"
	CallSessionFailed      = "failed"
)

const (
	CallUtteranceIntended    = "intended"
	CallUtteranceDequeued    = "dequeued"
	CallUtteranceEmitting    = "emitting"
	CallUtteranceEmitted     = "emitted"
	CallUtteranceInterrupted = "interrupted"
	CallUtteranceExpired     = "expired"
	CallUtteranceFailed      = "failed"
	CallUtteranceUnknown     = "unknown"
)

// Bridge room tickets are deliberately shorter than a human's call token:
// they exist to connect a currently-claimed session, not to keep a stale
// credential useful. A ticket's useful life ends with the claim anyway —
// minting is gated on a live claim and re-minting requires a heartbeat.
const CallBridgeTicketTTL = 60 * time.Second

// CallBridgeClaimLease bounds how long a runner may hold a session without a
// heartbeat. The value a runner passes is clamped to this ceiling.
const CallBridgeMaxLease = 90 * time.Second

var (
	ErrCallSessionNotFound = errors.New("call session not found")
	ErrCallSessionNotLive  = errors.New("call session is not live")
	ErrCallClaimLost       = errors.New("call session claim is held by another runner or epoch")
	ErrNoActiveCall        = errors.New("no active call in this place")
	ErrUtteranceNotFound   = errors.New("call utterance not found")
	ErrUtteranceTerminal   = errors.New("call utterance disposition is terminal")
)

// CallSession is one durable authority record for a secretary in a room.
type CallSession struct {
	SessionID          string     `json:"session_id"`
	WorkspaceID        string     `json:"workspace_id"`
	PlaceID            string     `json:"place_id"`
	PersonalityAgentID string     `json:"personality_agent_id"`
	RoomSID            string     `json:"room_sid,omitempty"`
	Status             string     `json:"status"`
	Epoch              int64      `json:"epoch"`
	ClaimedBy          string     `json:"claimed_by,omitempty"`
	ClaimExpiresAt     *time.Time `json:"claim_expires_at,omitempty"`
	RequestedBy        string     `json:"requested_by"`
	CreatedAt          time.Time  `json:"created_at"`
	UpdatedAt          time.Time  `json:"updated_at"`
	EndedAt            *time.Time `json:"ended_at,omitempty"`
	EndReason          string     `json:"end_reason,omitempty"`
}

// CallUtterance is the secretary's committed speech intent for one session.
// Status records what is known about playback; it never records what a
// listener heard.
type CallUtterance struct {
	UtteranceID  string         `json:"utterance_id"`
	SessionID    string         `json:"session_id"`
	SessionEpoch int64          `json:"session_epoch"`
	Seq          int64          `json:"seq"`
	Text         string         `json:"text"`
	Status       string         `json:"status"`
	Detail       map[string]any `json:"detail,omitempty"`
	CreatedAt    time.Time      `json:"created_at"`
	UpdatedAt    time.Time      `json:"updated_at"`
}

func callSessionLive(status string) bool {
	switch status {
	case CallSessionRequested, CallSessionClaimed, CallSessionActive,
		CallSessionEnding, CallSessionInterrupted:
		return true
	}
	return false
}

// callSessionIdentity is the LiveKit participant identity the bridge joins
// under for one claim generation. The epoch tag lets the API distinguish and
// remove a stale-generation participant without touching the current one.
func callSessionIdentity(session *CallSession) string {
	return fmt.Sprintf("%s#e%d", PersonalityAgent(session.PersonalityAgentID).Key(), session.Epoch)
}

// CallCoreTools are the state-internal tools delegated to Messaging.
const (
	CallJoinTool  = "call.join"
	CallLeaveTool = "call.leave"
	CallSayTool   = "call.say"
	CallStateTool = "call.state"
)

// CallHooks let the call surface reach the durable core without agentstate
// knowing Messaging: wired in main when a core state store exists.
type CallHooks struct {
	Core *agentstate.Store
}

func (c *CallService) coreStore() *agentstate.Store {
	if c == nil || c.Hooks == nil {
		return nil
	}
	return c.Hooks.Core
}

// ---------------------------------------------------------------------------
// Delegated effects (run inside the state service's operation-claim tx)
// ---------------------------------------------------------------------------

// CallJoinEffect commits a call_sessions row when the secretary's own place
// membership authorizes it and the place has a live call to join.
func (c *CallService) CallJoinEffect() agentstate.ToolEffect {
	return agentstate.ToolEffect{Apply: c.applyCallJoin}
}

func (c *CallService) applyCallJoin(ctx context.Context, tx pgx.Tx, personaID, _ string, request map[string]any) (map[string]any, error) {
	placeID, _ := request["place_id"].(string)
	scoped, place, err := c.scopeForCallEffect(ctx, tx, personaID, placeID)
	if err != nil {
		return nil, err
	}
	if place.Kind == PlaceThread {
		return nil, fmt.Errorf("%w: calls are not supported in threads", agentstate.ErrBadRequest)
	}
	if place.Kind == PlaceChannel && !place.Voice {
		return nil, fmt.Errorf("%w: channel is not voice-enabled", agentstate.ErrBadRequest)
	}
	// A session is only useful while a call is actually running in the
	// place. The registry is volatile; rebuild once if it has never been
	// populated (e.g. fresh API start before the first webhook).
	c.rebuildRegistryOnce(ctx)
	if snap := c.snapshotCallsFor(place.PlaceID); snap == nil || !snap.Active {
		return nil, fmt.Errorf("%w: %v", agentstate.ErrBadRequest, ErrNoActiveCall)
	}
	var session CallSession
	err = tx.QueryRow(ctx, `
		INSERT INTO call_sessions
			(session_id, workspace_id, place_id, personality_agent_id,
			 status, requested_by)
		VALUES ($1, $2, $3, $4, 'requested', 'call.join')
		RETURNING `+callSessionCols,
		newUUIDv7(), scoped.Scope.WorkspaceID, place.PlaceID, personaID,
	).Scan(callSessionScan(&session)...)
	if err != nil {
		if isUniqueViolation(err) {
			// A live session for this secretary in this place already
			// exists — return it rather than forking a second presence.
			session, err = c.liveCallSessionInTx(ctx, tx, personaID, place.PlaceID)
			if err != nil {
				return nil, callEffectFailure(err)
			}
			return map[string]any{"session": session, "joined": true, "existing": true}, nil
		}
		return nil, fmt.Errorf("insert call session: %w", err)
	}
	return map[string]any{"session": session, "joined": true}, nil
}

func (c *CallService) liveCallSessionInTx(ctx context.Context, tx pgx.Tx, personaID, placeID string) (CallSession, error) {
	var session CallSession
	err := tx.QueryRow(ctx, `
		SELECT `+callSessionCols+`
		FROM call_sessions
		WHERE personality_agent_id = $1 AND place_id = $2
		  AND status IN ('requested','claimed','active','ending','interrupted')`,
		personaID, placeID).Scan(callSessionScan(&session)...)
	if err != nil {
		return CallSession{}, err
	}
	return session, nil
}

// CallLeaveEffect marks the session ending. The claim holder performs the
// actual disconnect on its next heartbeat/status read and reports ended; a
// session with no live claim ends immediately.
func (c *CallService) CallLeaveEffect() agentstate.ToolEffect {
	return agentstate.ToolEffect{Apply: c.applyCallLeave}
}

func (c *CallService) applyCallLeave(ctx context.Context, tx pgx.Tx, personaID, _ string, request map[string]any) (map[string]any, error) {
	sessionID, _ := request["session_id"].(string)
	if !canonicalid.IsUUIDv7(sessionID) {
		return nil, fmt.Errorf("%w: session_id must be a uuidv7", agentstate.ErrBadRequest)
	}
	var session CallSession
	err := tx.QueryRow(ctx, `
		SELECT `+callSessionCols+` FROM call_sessions
		WHERE session_id = $1 AND personality_agent_id = $2 FOR UPDATE`,
		sessionID, personaID).Scan(callSessionScan(&session)...)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: %v", agentstate.ErrBadRequest, ErrCallSessionNotFound)
	}
	if err != nil {
		return nil, err
	}
	if !callSessionLive(session.Status) {
		return map[string]any{"session": session, "left": true, "already_terminal": true}, nil
	}
	if session.Status == CallSessionRequested || session.Status == CallSessionInterrupted {
		// No live claim holds media authority — nothing to disconnect.
		return c.endCallSessionInTx(ctx, tx, session, "left")
	}
	var ended CallSession
	err = tx.QueryRow(ctx, `
		UPDATE call_sessions SET status='ending', updated_at=now()
		WHERE session_id = $1 RETURNING `+callSessionCols,
		sessionID).Scan(callSessionScan(&ended)...)
	if err != nil {
		return nil, err
	}
	return map[string]any{"session": ended, "left": true}, nil
}

// CallSayEffect commits the secretary's speech intent. Audibility is decided
// by the claim-holding bridge's disposition reports, not by this commit.
func (c *CallService) CallSayEffect() agentstate.ToolEffect {
	return agentstate.ToolEffect{Apply: c.applyCallSay}
}

func (c *CallService) applyCallSay(ctx context.Context, tx pgx.Tx, personaID, _ string, request map[string]any) (map[string]any, error) {
	sessionID, _ := request["session_id"].(string)
	text, _ := request["text"].(string)
	if !canonicalid.IsUUIDv7(sessionID) {
		return nil, fmt.Errorf("%w: session_id must be a uuidv7", agentstate.ErrBadRequest)
	}
	if strings.TrimSpace(text) == "" {
		return nil, fmt.Errorf("%w: text is required", agentstate.ErrBadRequest)
	}
	var session CallSession
	err := tx.QueryRow(ctx, `
		SELECT `+callSessionCols+` FROM call_sessions
		WHERE session_id = $1 AND personality_agent_id = $2 FOR UPDATE`,
		sessionID, personaID).Scan(callSessionScan(&session)...)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: %v", agentstate.ErrBadRequest, ErrCallSessionNotFound)
	}
	if err != nil {
		return nil, err
	}
	if !callSessionLive(session.Status) {
		return nil, fmt.Errorf("%w: %v", agentstate.ErrBadRequest, ErrCallSessionNotLive)
	}
	var utterance CallUtterance
	err = tx.QueryRow(ctx, `
		INSERT INTO call_utterances (utterance_id, session_id, session_epoch, seq, text, status)
		SELECT $1, $2::uuidv7, $3, COALESCE(MAX(seq), 0) + 1, $4, 'intended'
		FROM call_utterances WHERE session_id = $2
		RETURNING `+callUtteranceCols,
		newUUIDv7(), sessionID, session.Epoch, text,
	).Scan(callUtteranceScan(&utterance)...)
	if err != nil {
		return nil, fmt.Errorf("insert call utterance: %w", err)
	}
	return map[string]any{"utterance": utterance, "queued": true}, nil
}

// CallStateEffect is a read: the place's call state plus this persona's own
// live sessions, so the secretary can answer "am I in this call" truthfully.
func (c *CallService) CallStateEffect() agentstate.ToolEffect {
	return agentstate.ToolEffect{Apply: c.applyCallState}
}

func (c *CallService) applyCallState(ctx context.Context, tx pgx.Tx, personaID, _ string, request map[string]any) (map[string]any, error) {
	placeID, _ := request["place_id"].(string)
	response := map[string]any{}
	c.rebuildRegistryOnce(ctx)
	if placeID != "" {
		scoped, place, err := c.scopeForCallEffect(ctx, tx, personaID, placeID)
		if err != nil {
			return nil, err
		}
		_ = scoped
		state := c.snapshotCallsFor(place.PlaceID)
		response["call"] = state
	}
	rows, err := tx.Query(ctx, `
		SELECT `+callSessionCols+` FROM call_sessions
		WHERE personality_agent_id = $1
		  AND status IN ('requested','claimed','active','ending','interrupted')
		ORDER BY created_at`, personaID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	sessions := make([]CallSession, 0)
	for rows.Next() {
		var session CallSession
		if err := rows.Scan(callSessionScan(&session)...); err != nil {
			return nil, err
		}
		sessions = append(sessions, session)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	response["sessions"] = sessions
	return response, nil
}

// scopeForCallEffect authorizes the persona's place access inside the claim
// transaction under the installation's live authority epoch — the same rule
// the human call-token route applies to the browser.
func (c *CallService) scopeForCallEffect(ctx context.Context, tx pgx.Tx, personaID, placeID string) (*ScopedStore, Place, error) {
	if !canonicalid.IsUUIDv7(placeID) {
		return nil, Place{}, fmt.Errorf("%w: %v", agentstate.ErrBadRequest, ErrPlaceNotFound)
	}
	var workspaceID string
	err := tx.QueryRow(ctx,
		"SELECT workspace_id::text FROM places WHERE place_id = $1", placeID).Scan(&workspaceID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, Place{}, fmt.Errorf("%w: %v", agentstate.ErrBadRequest, ErrPlaceNotFound)
	}
	if err != nil {
		return nil, Place{}, err
	}
	var installationID string
	var epoch int64
	err = tx.QueryRow(ctx, `
		SELECT installation_id::text, authority_epoch
		FROM app_installations
		WHERE owner_kind = 'workspace' AND owner_id = $1
		  AND app_id = $2 AND enabled
		ORDER BY installed_at, installation_id LIMIT 1`,
		workspaceID, MessagingAppID).Scan(&installationID, &epoch)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, Place{}, fmt.Errorf("%w: %v", agentstate.ErrBadRequest, applicationapps.ErrInstallationNotFound)
	}
	if err != nil {
		return nil, Place{}, err
	}
	scoped, err := c.Server.Store.Scoped(Scope{
		WorkspaceID: workspaceID, InstallationID: installationID,
		AuthorityEpoch: epoch, Actor: PersonalityAgent(personaID),
	})
	if err != nil {
		return nil, Place{}, err
	}
	place, err := scoped.lockScopedPlace(ctx, tx, placeID)
	if err != nil {
		return nil, Place{}, callEffectFailure(err)
	}
	if _, err := scoped.placeAccessAfterAuthorization(ctx, tx, place, scoped.Scope.Actor); err != nil {
		return nil, Place{}, callEffectFailure(err)
	}
	return scoped, place, nil
}

// callEffectFailure maps messaging rejections onto deterministic tool errors
// so a denied call effect lands as a tool result, not a retrying turn.
func callEffectFailure(err error) error {
	switch {
	case errors.Is(err, ErrPlaceNotFound), errors.Is(err, ErrForbidden),
		errors.Is(err, ErrNotAMember), errors.Is(err, ErrNotReachable),
		errors.Is(err, ErrInvalidScope), errors.Is(err, ErrParticipantNotFound),
		errors.Is(err, applicationapps.ErrInstallationNotFound),
		errors.Is(err, applicationapps.ErrAppDisabled),
		errors.Is(err, applicationapps.ErrAuthorityEpochStale):
		return fmt.Errorf("%w: %v", agentstate.ErrBadRequest, err)
	}
	return err
}

// endCallSessionInTx terminally ends a session inside a tx: stamps the row,
// sweeps pending speech, and returns the response the caller reports.
func (c *CallService) endCallSessionInTx(ctx context.Context, tx pgx.Tx, session CallSession, reason string) (map[string]any, error) {
	var ended CallSession
	err := tx.QueryRow(ctx, `
		UPDATE call_sessions
		SET status='ended', ended_at=now(), end_reason=$2, updated_at=now(),
		    claimed_by=NULL, claim_expires_at=NULL
		WHERE session_id = $1 RETURNING `+callSessionCols,
		session.SessionID, reason).Scan(callSessionScan(&ended)...)
	if err != nil {
		return nil, err
	}
	if err := sweepCallUtterancesInTx(ctx, tx, session.SessionID, "session_ended"); err != nil {
		return nil, err
	}
	return map[string]any{"session": ended, "left": true}, nil
}

// sweepCallUtterancesInTx closes every non-terminal utterance of a session:
// 'intended' was never picked up so it 'expired'; anything mid-flight was
// possibly partially emitted so the honest record is 'unknown'.
func sweepCallUtterancesInTx(ctx context.Context, tx pgx.Tx, sessionID, reason string) error {
	detail, err := json.Marshal(map[string]any{"reason": reason})
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		UPDATE call_utterances
		SET status = CASE WHEN status = 'intended' THEN 'expired' ELSE 'unknown' END,
		    detail = COALESCE(detail, '{}'::jsonb) || $2::jsonb, updated_at = now()
		WHERE session_id = $1 AND status IN ('intended','dequeued','emitting')`,
		sessionID, detail)
	return err
}

const callSessionCols = `session_id, workspace_id, place_id, personality_agent_id,
	COALESCE(room_sid, ''), status, epoch, COALESCE(claimed_by, ''),
	claim_expires_at, requested_by, created_at, updated_at, ended_at,
	COALESCE(end_reason, '')`

func callSessionScan(s *CallSession) []any {
	return []any{&s.SessionID, &s.WorkspaceID, &s.PlaceID, &s.PersonalityAgentID,
		&s.RoomSID, &s.Status, &s.Epoch, &s.ClaimedBy, &s.ClaimExpiresAt,
		&s.RequestedBy, &s.CreatedAt, &s.UpdatedAt, &s.EndedAt, &s.EndReason}
}

const callUtteranceCols = `utterance_id, session_id, session_epoch, seq, text,
	status, detail, created_at, updated_at`

func callUtteranceScan(u *CallUtterance) []any {
	return []any{&u.UtteranceID, &u.SessionID, &u.SessionEpoch, &u.Seq, &u.Text,
		&u.Status, &u.Detail, &u.CreatedAt, &u.UpdatedAt}
}
