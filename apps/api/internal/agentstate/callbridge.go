package agentstate

import (
	"context"
	"time"
)

// CallBridge is the persona-scoped backend a per-placement call media runner
// talks to. It is wired by the host (main) to the Messaging call-session
// service, which owns durable session authority, admission, and LiveKit
// ticket minting. The state service only authenticates the persona and
// validates the envelope; every authority decision lives behind this
// interface.
//
// Claimed sessions are independent lifecycles: a bridge's claim — not the
// writer lease — authorizes media work, so call participation survives a
// writer restart or a persona transfer drain.
type CallBridge interface {
	// ClaimCallSessions sweeps lapsed claims to interrupted and claims up to
	// limit requested/interrupted sessions, bumping each session's epoch.
	ClaimCallSessions(ctx context.Context, personaID, runnerID string, lease time.Duration, limit int) ([]CallSession, error)
	// HeartbeatCallSession renews a live claim and returns the session so the
	// runner observes 'ending'. A lost claim is an error.
	HeartbeatCallSession(ctx context.Context, personaID, sessionID, runnerID string, epoch int64, lease time.Duration) (CallSession, error)
	// CallSessionTicket mints a short LiveKit credential for the session's
	// epoch-tagged identity, gated on the live claim.
	CallSessionTicket(ctx context.Context, personaID, sessionID, runnerID string, epoch int64) (CallTicket, error)
	// ReportCallSessionStatus moves the session lifecycle under the claim:
	// active / ending / ended / failed.
	ReportCallSessionStatus(ctx context.Context, personaID, sessionID, runnerID string, epoch int64, status, reason string) (CallSession, error)
	// PendingCallUtterances lists the session's 'intended' speech under the
	// claim. Dequeue is a separate disposition report.
	PendingCallUtterances(ctx context.Context, personaID, sessionID, runnerID string, epoch int64) ([]CallUtterance, error)
	// ReportUtteranceDisposition records one utterance hop under the claim:
	// intended→dequeued→emitting→emitted|interrupted|failed|expired|unknown.
	ReportUtteranceDisposition(ctx context.Context, personaID, sessionID, utteranceID, runnerID string, epoch int64, status string, detail map[string]any) (CallUtterance, error)
}

// CallSession is the wire shape of one durable call-session record.
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

// CallUtterance is the wire shape of committed speech intent and its known
// disposition. 'emitted' means the bridge finished rendering the audio —
// never that a listener heard it.
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

// CallTicket is a short-lived LiveKit credential for one session epoch.
type CallTicket struct {
	URL      string `json:"url"`
	Token    string `json:"token"`
	Room     string `json:"room"`
	Identity string `json:"identity"`
}

// SetCallBridge wires the call-session backend used by the persona-scoped
// /calls bridge routes. Without it those routes report not configured.
func (s *Server) SetCallBridge(bridge CallBridge) {
	s.callBridge = bridge
}
