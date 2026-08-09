package messaging

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
)

// 通話（RTC）の口。ADR 0012: メディアは self-hosted LiveKit（SFU）が運び、
// api は「誰がどの部屋へ入ってよいか」だけを決める。音声・映像はこのプロセスを
// 通らない。
//
// 通話状態（誰が今どの place で話しているか）はテキストと違って durable では
// ない。正本は api のメモリに置き、LiveKit の webhook で更新して WS の
// `call_state` イベントで配る。プロセスが落ちれば webhook から再構成され、
// クライアントは再接続時に GET /messaging/calls で読み直す。

const (
	// CallTokenTTL bounds one issued access token. The token is only the ticket
	// into the room; an active call is not cut off when it expires.
	CallTokenTTL = 6 * time.Hour
	// callWebhookLeeway tolerates clock skew between LiveKit and this process
	// when checking the webhook token's expiry.
	callWebhookLeeway = 5 * time.Minute
	// maxCallWebhookBytes bounds one webhook body. LiveKit events are small
	// JSON objects; anything larger is not one of ours.
	maxCallWebhookBytes = 1 << 20
)

// LiveKitConfig is the deployment's media transport. An empty APIKey or
// APISecret disables the call routes (503): a deployment with no configured
// SFU should say so rather than mint tokens nothing will accept.
type LiveKitConfig struct {
	// URL is the browser-facing signalling endpoint (ws:// or wss://).
	URL string
	// APIKey identifies this deployment to the SFU; it is the JWT issuer.
	APIKey string
	// APISecret signs access tokens and verifies webhook tokens.
	APISecret string
}

func (c LiveKitConfig) configured() bool {
	return c.APIKey != "" && c.APISecret != ""
}

// CallParticipant is one participant currently in a place's call.
type CallParticipant struct {
	Participant ParticipantRef
	JoinedAt    time.Time
	// ScreenShare reports whether this participant is currently publishing a
	// screen. It is a property of the call, not of the person.
	ScreenShare bool
}

// CallState is one place's live call. Active with no participants is the
// moment between the room opening and the first person arriving; the state is
// dropped entirely when the room finishes.
type CallState struct {
	PlaceID      string
	Active       bool
	StartedAt    time.Time
	Participants []CallParticipant
}

// CallRegistry holds the live call state of every place with an open room.
// It is deliberately in-memory and volatile (ADR 0012).
type CallRegistry struct {
	mu    sync.Mutex
	rooms map[string]*CallState
}

// NewCallRegistry returns an empty registry.
func NewCallRegistry() *CallRegistry {
	return &CallRegistry{rooms: map[string]*CallState{}}
}

// snapshot returns a copy of one place's state, or ok=false when no call is
// open there.
func (r *CallRegistry) snapshot(placeID string) (CallState, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, ok := r.rooms[placeID]
	if !ok {
		return CallState{}, false
	}
	return cloneCallState(state), true
}

// active returns every open call, ordered by place so the wire is stable.
func (r *CallRegistry) active() []CallState {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]CallState, 0, len(r.rooms))
	for _, state := range r.rooms {
		out = append(out, cloneCallState(state))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PlaceID < out[j].PlaceID })
	return out
}

func cloneCallState(state *CallState) CallState {
	participants := make([]CallParticipant, len(state.Participants))
	copy(participants, state.Participants)
	return CallState{
		PlaceID:      state.PlaceID,
		Active:       state.Active,
		StartedAt:    state.StartedAt,
		Participants: participants,
	}
}

// open marks a place's room as started. Idempotent.
func (r *CallRegistry) open(placeID string, at time.Time) CallState {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, ok := r.rooms[placeID]
	if !ok {
		state = &CallState{PlaceID: placeID, Active: true, StartedAt: at}
		r.rooms[placeID] = state
	}
	state.Active = true
	return cloneCallState(state)
}

// close drops a place's call entirely. The returned state is the empty call
// clients should render as "no one is talking here".
func (r *CallRegistry) close(placeID string) CallState {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.rooms, placeID)
	return CallState{PlaceID: placeID, Active: false}
}

// join adds or updates a participant. Re-joining the same identity replaces
// the earlier entry rather than duplicating it (LiveKit may resend the event
// after a reconnect).
func (r *CallRegistry) join(placeID string, participant ParticipantRef, at time.Time) CallState {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, ok := r.rooms[placeID]
	if !ok {
		state = &CallState{PlaceID: placeID, Active: true, StartedAt: at}
		r.rooms[placeID] = state
	}
	state.Active = true
	for i, existing := range state.Participants {
		if existing.Participant == participant {
			state.Participants[i].JoinedAt = existing.JoinedAt
			return cloneCallState(state)
		}
	}
	state.Participants = append(state.Participants, CallParticipant{
		Participant: participant, JoinedAt: at,
	})
	sortCallParticipants(state.Participants)
	return cloneCallState(state)
}

// leave removes a participant. The room itself stays open until LiveKit says
// it finished, so a place a person just left still reads as "in call" for the
// people still in it.
func (r *CallRegistry) leave(placeID string, participant ParticipantRef) (CallState, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, ok := r.rooms[placeID]
	if !ok {
		return CallState{}, false
	}
	for i, existing := range state.Participants {
		if existing.Participant == participant {
			state.Participants = append(state.Participants[:i], state.Participants[i+1:]...)
			return cloneCallState(state), true
		}
	}
	return cloneCallState(state), true
}

// setScreenShare records whether a participant is publishing a screen.
func (r *CallRegistry) setScreenShare(placeID string, participant ParticipantRef, sharing bool) (CallState, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, ok := r.rooms[placeID]
	if !ok {
		return CallState{}, false
	}
	for i, existing := range state.Participants {
		if existing.Participant == participant {
			if state.Participants[i].ScreenShare == sharing {
				return CallState{}, false
			}
			state.Participants[i].ScreenShare = sharing
			return cloneCallState(state), true
		}
	}
	return CallState{}, false
}

func sortCallParticipants(participants []CallParticipant) {
	sort.SliceStable(participants, func(i, j int) bool {
		if participants[i].JoinedAt.Equal(participants[j].JoinedAt) {
			return participants[i].Participant.Key() < participants[j].Participant.Key()
		}
		return participants[i].JoinedAt.Before(participants[j].JoinedAt)
	})
}

// --- wire shapes ---

type callParticipantWire struct {
	Participant participantWire `json:"participant"`
	JoinedAt    time.Time       `json:"joined_at"`
	ScreenShare bool            `json:"screen_share"`
}

type callStateWire struct {
	Place        placeWire             `json:"place"`
	Active       bool                  `json:"active"`
	StartedAt    *time.Time            `json:"started_at"`
	Participants []callParticipantWire `json:"participants"`
}

func callStateToWire(place Place, state CallState) callStateWire {
	participants := make([]callParticipantWire, len(state.Participants))
	for i, entry := range state.Participants {
		participants[i] = callParticipantWire{
			Participant: participantToWire(entry.Participant),
			JoinedAt:    entry.JoinedAt,
			ScreenShare: entry.ScreenShare,
		}
	}
	wire := callStateWire{
		Place:        placeToWire(place),
		Active:       state.Active,
		Participants: participants,
	}
	if !state.StartedAt.IsZero() {
		startedAt := state.StartedAt
		wire.StartedAt = &startedAt
	}
	return wire
}

// --- service ---

// CallService is the /messaging call surface. It borrows the messaging
// Server's authentication and store so a call cannot reach a place the same
// session could not open in text (凍結契約 v1 §4: 同じ経路・同じ権限モデル).
type CallService struct {
	Server   *Server
	LiveKit  LiveKitConfig
	Registry *CallRegistry
	// Now is injectable for tests; nil means time.Now.
	Now func() time.Time
}

// NewCallService returns a call service backed by the messaging server.
func NewCallService(server *Server, livekit LiveKitConfig) *CallService {
	return &CallService{Server: server, LiveKit: livekit, Registry: NewCallRegistry()}
}

func (c *CallService) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

// RegisterRoutes mounts the call routes on the public mux. The webhook is a
// server-to-server route: it carries no browser session and is authenticated
// by the LiveKit API secret instead.
func (c *CallService) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /messaging/places/{place_id}/call/token", c.serveCallToken)
	mux.HandleFunc("GET /messaging/calls", c.serveCalls)
	mux.HandleFunc("POST /messaging/livekit/webhook", c.serveWebhook)
}

// serveCallToken hands the session's participant a ticket into the place's
// room. Membership is the store's decision, exactly as it is for reading the
// place's messages.
func (c *CallService) serveCallToken(w http.ResponseWriter, r *http.Request) {
	viewer, claims, ok := c.Server.viewer(w, r)
	if !ok {
		return
	}
	if !c.LiveKit.configured() {
		writeError(w, http.StatusServiceUnavailable, "calls_unavailable")
		return
	}
	tokenFailed := false
	done, err := c.Server.mutate(w, r, claims, func() error {
		placeID := r.PathValue("place_id")
		place, placeErr := c.Server.Store.PlaceFor(r.Context(), placeID, viewer)
		if placeErr != nil {
			return placeErr
		}
		name := c.displayName(r, place, viewer)
		token, tokenErr := c.LiveKit.accessToken(
			place.PlaceID,
			viewer.Key(),
			name,
			c.now(),
			CallTokenTTL,
		)
		if tokenErr != nil {
			tokenFailed = true
			return tokenErr
		}
		// The response commit is part of credential issuance. Keeping it inside
		// the lease makes a successful logout a barrier to later token delivery.
		writeJSON(w, http.StatusOK, struct {
			URL      string `json:"url"`
			Token    string `json:"token"`
			Room     string `json:"room"`
			Identity string `json:"identity"`
		}{c.LiveKit.URL, token, place.PlaceID, viewer.Key()})
		return nil
	})
	if !done {
		return
	}
	if err != nil {
		if tokenFailed {
			writeError(w, http.StatusInternalServerError, "call_token_failed")
		} else {
			writeStoreError(w, err)
		}
		return
	}
}

// displayName resolves the participant's presentation name for the call tile.
// A lookup failure is not fatal: the tile falls back to the client's own copy
// of the member list.
func (c *CallService) displayName(r *http.Request, place Place, viewer ParticipantRef) string {
	profiles, err := c.Server.Store.ActiveMembers(r.Context(), place.PlaceID, viewer)
	if err != nil {
		return ""
	}
	for _, profile := range profiles {
		if profile.Participant == viewer {
			return profile.ProjectedDisplayName()
		}
	}
	return ""
}

// serveCalls reports every open call the viewer may see. A client that
// connects mid-call learns the current state here; `call_state` events carry
// it from then on.
func (c *CallService) serveCalls(w http.ResponseWriter, r *http.Request) {
	viewer, _, ok := c.Server.viewer(w, r)
	if !ok {
		return
	}
	states := c.visibleCalls(r, viewer)
	writeJSON(w, http.StatusOK, struct {
		Calls []callStateWire `json:"calls"`
	}{states})
}

func (c *CallService) visibleCalls(r *http.Request, viewer ParticipantRef) []callStateWire {
	out := []callStateWire{}
	for _, state := range c.Registry.active() {
		place, err := c.Server.Store.PlaceFor(r.Context(), state.PlaceID, viewer)
		if err != nil {
			continue
		}
		out = append(out, callStateToWire(place, state))
	}
	return out
}

// serveWebhook consumes LiveKit's room lifecycle events. The body is
// authenticated by a JWT signed with the same API secret whose `sha256` claim
// must match the body — so neither the events nor the identities in them can
// be forged by anything that lacks the secret.
func (c *CallService) serveWebhook(w http.ResponseWriter, r *http.Request) {
	if !c.LiveKit.configured() {
		writeError(w, http.StatusServiceUnavailable, "calls_unavailable")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxCallWebhookBytes+1))
	if err != nil || len(body) > maxCallWebhookBytes {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	authorization := strings.TrimSpace(r.Header.Get("Authorization"))
	authorization = strings.TrimPrefix(authorization, "Bearer ")
	if err := c.LiveKit.verifyWebhookToken(authorization, body, c.now()); err != nil {
		writeError(w, http.StatusUnauthorized, "invalid_webhook_token")
		return
	}
	var event livekitWebhookEvent
	if err := json.Unmarshal(body, &event); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	c.applyWebhook(r, event)
	w.WriteHeader(http.StatusNoContent)
}

type livekitWebhookEvent struct {
	Event string `json:"event"`
	Room  struct {
		Name string `json:"name"`
	} `json:"room"`
	Participant struct {
		Identity string `json:"identity"`
	} `json:"participant"`
	Track struct {
		Source string `json:"source"`
	} `json:"track"`
}

// applyWebhook folds one LiveKit event into the registry and announces the
// resulting state. Unknown events are ignored: LiveKit's event set grows, and
// an event we do not model changes nothing about who is in the room.
func (c *CallService) applyWebhook(r *http.Request, event livekitWebhookEvent) {
	placeID := event.Room.Name
	if placeID == "" {
		return
	}
	switch event.Event {
	case "room_started":
		c.publishCallState(r, c.Registry.open(placeID, c.now()))
	case "room_finished":
		c.publishCallState(r, c.Registry.close(placeID))
	case "participant_joined":
		participant, err := participantFromIdentity(event.Participant.Identity)
		if err != nil {
			return
		}
		c.publishCallState(r, c.Registry.join(placeID, participant, c.now()))
	case "participant_left":
		participant, err := participantFromIdentity(event.Participant.Identity)
		if err != nil {
			return
		}
		state, ok := c.Registry.leave(placeID, participant)
		if ok {
			c.publishCallState(r, state)
		}
	case "track_published", "track_unpublished":
		if event.Track.Source != "SCREEN_SHARE" {
			return
		}
		participant, err := participantFromIdentity(event.Participant.Identity)
		if err != nil {
			return
		}
		state, changed := c.Registry.setScreenShare(
			placeID, participant, event.Event == "track_published")
		if changed {
			c.publishCallState(r, state)
		}
	}
}

// publishCallState fans a place's current call out to everyone who can see the
// place. Like typing and status it is volatile: a missed frame is repaired by
// the next event or by GET /messaging/calls on reconnect.
func (c *CallService) publishCallState(r *http.Request, state CallState) {
	if c.Server.Hub == nil {
		return
	}
	place, err := c.Server.Store.PlaceByID(r.Context(), state.PlaceID)
	if err != nil {
		return
	}
	wire := callStateToWire(place, state)
	c.Server.Hub.Publish(r.Context(), Event{
		Type: EventCallState, PlaceID: state.PlaceID, Call: &wire,
	})
}

// participantFromIdentity parses the LiveKit identity back into the shared
// participant shape. Humans and PersonalityAgents use the identical key here
// too — a call has participants, not users and bots.
func participantFromIdentity(identity string) (ParticipantRef, error) {
	separator := strings.Index(identity, ":")
	if separator < 0 {
		return ParticipantRef{}, fmt.Errorf("identity is not a participant key")
	}
	ref := ParticipantRef{
		Kind: ParticipantKind(identity[:separator]),
		ID:   identity[separator+1:],
	}
	if err := ref.Validate(); err != nil {
		return ParticipantRef{}, err
	}
	return ref, nil
}

// --- local control (AX) ---

// LocalCallStatePath lets a PersonalityAgent read who is currently in a call,
// through the identical registry the human UI renders. There is deliberately
// no op for joining a call: ADR 0012 records that as an explicit future
// question, not an oversight.
const LocalCallStatePath = "/local-control/v1/messaging:call-state"

func (c *CallService) localCallState(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		PlaceID string `json:"place_id,omitempty"`
	}
	if !decodeJSON(w, r, &request) {
		return
	}
	viewer := localViewer(authorization)
	if err := c.Server.Store.EnsureDefaultWorkspaceMembership(r.Context(), viewer); err != nil {
		writeStoreError(w, err)
		return
	}
	if request.PlaceID != "" {
		place, err := c.Server.Store.PlaceFor(r.Context(), request.PlaceID, viewer)
		if err != nil {
			writeStoreError(w, err)
			return
		}
		state, ok := c.Registry.snapshot(request.PlaceID)
		if !ok {
			state = CallState{PlaceID: request.PlaceID}
		}
		writeJSON(w, http.StatusOK, struct {
			Calls []callStateWire `json:"calls"`
		}{[]callStateWire{callStateToWire(place, state)}})
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Calls []callStateWire `json:"calls"`
	}{c.visibleCalls(r, viewer)})
}

// --- LiveKit access tokens (ADR 0012: 標準ライブラリで組み立てる) ---

type livekitVideoGrant struct {
	Room           string `json:"room"`
	RoomJoin       bool   `json:"roomJoin"`
	CanPublish     bool   `json:"canPublish"`
	CanSubscribe   bool   `json:"canSubscribe"`
	CanPublishData bool   `json:"canPublishData"`
}

type livekitClaims struct {
	Issuer    string            `json:"iss"`
	Subject   string            `json:"sub"`
	Name      string            `json:"name,omitempty"`
	NotBefore int64             `json:"nbf"`
	Expiry    int64             `json:"exp"`
	Video     livekitVideoGrant `json:"video"`
	// SHA256 rides on webhook tokens only: it binds the token to one body.
	SHA256 string `json:"sha256,omitempty"`
}

// accessToken mints one HS256 JWT admitting `identity` to `room`. The grant is
// deliberately uniform: everyone admitted to a place's call may speak, see and
// share — the messaging surface has no listener-only role.
func (c LiveKitConfig) accessToken(room, identity, name string, now time.Time, ttl time.Duration) (string, error) {
	if !c.configured() {
		return "", errors.New("livekit is not configured")
	}
	if room == "" || identity == "" {
		return "", errors.New("room and identity are required")
	}
	claims := livekitClaims{
		Issuer:    c.APIKey,
		Subject:   identity,
		Name:      name,
		NotBefore: now.Add(-callWebhookLeeway).Unix(),
		Expiry:    now.Add(ttl).Unix(),
		Video: livekitVideoGrant{
			Room: room, RoomJoin: true,
			CanPublish: true, CanSubscribe: true, CanPublishData: true,
		},
	}
	return signJWT(claims, c.APISecret)
}

// verifyWebhookToken authenticates one LiveKit webhook delivery: the token is
// signed with our API secret, was issued by our API key, has not expired, and
// its sha256 claim matches this exact body.
func (c LiveKitConfig) verifyWebhookToken(token string, body []byte, now time.Time) error {
	if !c.configured() {
		return errors.New("livekit is not configured")
	}
	payload, err := verifyJWT(token, c.APISecret)
	if err != nil {
		return err
	}
	var claims livekitClaims
	if err := json.Unmarshal(payload, &claims); err != nil {
		return err
	}
	if claims.Issuer != c.APIKey {
		return errors.New("webhook token issuer mismatch")
	}
	if claims.Expiry != 0 && now.Add(-callWebhookLeeway).After(time.Unix(claims.Expiry, 0)) {
		return errors.New("webhook token expired")
	}
	digest := sha256.Sum256(body)
	expected := base64.StdEncoding.EncodeToString(digest[:])
	if subtle.ConstantTimeCompare([]byte(expected), []byte(claims.SHA256)) != 1 {
		return errors.New("webhook body digest mismatch")
	}
	return nil
}

func signJWT(claims livekitClaims, secret string) (string, error) {
	header := base64URL([]byte(`{"alg":"HS256","typ":"JWT"}`))
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	signing := header + "." + base64URL(payload)
	return signing + "." + base64URL(hmacSHA256(signing, secret)), nil
}

// verifyJWT checks an HS256 JWT's signature and returns its payload. It is
// intentionally narrow: only the one algorithm LiveKit uses is accepted, so a
// token claiming `alg: none` cannot slip through.
func verifyJWT(token, secret string) ([]byte, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, errors.New("malformed token")
	}
	header, err := decodeBase64URL(parts[0])
	if err != nil {
		return nil, err
	}
	var head struct {
		Algorithm string `json:"alg"`
	}
	if err := json.Unmarshal(header, &head); err != nil {
		return nil, err
	}
	if head.Algorithm != "HS256" {
		return nil, errors.New("unsupported token algorithm")
	}
	signature, err := decodeBase64URL(parts[2])
	if err != nil {
		return nil, err
	}
	if !hmac.Equal(signature, hmacSHA256(parts[0]+"."+parts[1], secret)) {
		return nil, errors.New("token signature mismatch")
	}
	return decodeBase64URL(parts[1])
}

func hmacSHA256(signing, secret string) []byte {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(signing))
	return mac.Sum(nil)
}

func base64URL(value []byte) string {
	return base64.RawURLEncoding.EncodeToString(value)
}

func decodeBase64URL(value string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(strings.TrimRight(value, "="))
}
