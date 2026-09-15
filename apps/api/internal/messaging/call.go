package messaging

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	// Call tokens are minted for one join attempt. LiveKit validates them
	// locally, so a membership change cannot revoke a credential already issued.
	// Keep the residual reconnect window deliberately short.
	CallTokenTTL        = 5 * time.Minute
	callWebhookLeeway   = 5 * time.Minute
	maxCallWebhookBytes = 1 << 20
)

// LiveKitConfig describes the browser-facing SFU endpoint and the shared
// signing credentials. Media never passes through this service (ADR 0012).
type LiveKitConfig struct {
	URL       string // Browser-facing ws:// or wss:// signalling endpoint.
	APIURL    string // Optional API endpoint; defaults to URL with an HTTP scheme.
	APIKey    string
	APISecret string
}

func (c LiveKitConfig) configured() bool {
	return c.URL != "" && c.APIKey != "" && c.APISecret != ""
}

type CallParticipant struct {
	Participant ParticipantRef
	JoinedAt    time.Time
	ScreenShare bool
	// Identity is the raw LiveKit identity of the most recently confirmed
	// live connection, including any claim-epoch tag.
	Identity string
	// Connections tracks each live connection under this participant ref,
	// keyed by the LiveKit participant SID (falling back to identity when a
	// caller supplies none). A secretary's reclaimed actor (#e1 then #e2)
	// and a human's reconnect both hold distinct connections; the roster
	// entry is present exactly while at least one is live, so a stale
	// connection's departure cannot drop the current generation's entry.
	Connections map[string]string
}

type CallState struct {
	PlaceID      string
	Active       bool
	StartedAt    time.Time
	Participants []CallParticipant
}

// CallRegistry is deliberately volatile. On its first call-state read after an
// API restart, the service reconciles it from LiveKit's RoomService; webhooks
// keep that projection current afterwards.
type CallRegistry struct {
	mu           sync.Mutex
	rooms        map[string]*CallState
	sequence     uint64
	roomSequence map[string]uint64
	roomSID      map[string]string
	finishedSIDs map[string]map[string]struct{}
	pendingShare map[string]map[ParticipantRef]bool
	// leftConns tombstones participant SIDs whose participant_left was
	// processed, so a reordered delayed participant_joined for that same
	// dead connection cannot re-add it to the roster. Cleared per room
	// generation.
	leftConns map[string]map[string]bool
}

func NewCallRegistry() *CallRegistry {
	return &CallRegistry{
		rooms: map[string]*CallState{}, roomSequence: map[string]uint64{},
		roomSID:      map[string]string{},
		finishedSIDs: map[string]map[string]struct{}{}, pendingShare: map[string]map[ParticipantRef]bool{},
		leftConns: map[string]map[string]bool{},
	}
}

func (r *CallRegistry) snapshot(placeID string) (CallState, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, ok := r.rooms[placeID]
	if !ok {
		return CallState{}, false
	}
	return cloneCallState(state), true
}

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

type callRoomSnapshot struct {
	state    CallState
	sid      string
	sequence uint64
}

// replaceSnapshot applies a RoomService snapshot. A room snapshot is accepted
// only when no webhook changed that room while its participants were listed.
// A finished room SID is a tombstone, so its stale listing can never recreate it.
func (r *CallRegistry) replaceSnapshot(states []callRoomSnapshot, snapshotSequence uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	next := make(map[string]*CallState, len(states))
	for _, snapshot := range states {
		placeID := snapshot.state.PlaceID
		if r.finished(placeID, snapshot.sid) {
			continue
		}
		if currentSID := r.roomSID[placeID]; currentSID != "" && currentSID != snapshot.sid {
			if current, ok := r.rooms[placeID]; ok {
				copy := cloneCallState(current)
				next[placeID] = &copy
				continue
			}
		}
		if r.roomSequence[placeID] > snapshot.sequence {
			if current, ok := r.rooms[placeID]; ok {
				copy := cloneCallState(current)
				next[placeID] = &copy
			}
			continue
		}
		copy := cloneCallState(&snapshot.state)
		next[placeID] = &copy
		r.roomSID[placeID] = snapshot.sid
	}
	for placeID, state := range r.rooms {
		if r.roomSequence[placeID] > snapshotSequence {
			if _, alreadyIncluded := next[placeID]; !alreadyIncluded {
				copy := cloneCallState(state)
				next[placeID] = &copy
			}
		}
	}
	r.rooms = next
}

func (r *CallRegistry) snapshotSequence() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.sequence
}

// roomSIDFor returns the active room SID for a place, or "" when the
// projection holds none — used to scope a removal to the live generation.
func (r *CallRegistry) roomSIDFor(placeID string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.roomSID[placeID]
}

func (r *CallRegistry) roomSnapshotSequence(placeID string) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.roomSequence[placeID]
}

func (r *CallRegistry) changed(placeID string) {
	r.sequence++
	r.roomSequence[placeID] = r.sequence
}

func cloneCallState(state *CallState) CallState {
	participants := make([]CallParticipant, len(state.Participants))
	for i, p := range state.Participants {
		participants[i] = p
		if p.Connections != nil {
			participants[i].Connections = make(map[string]string, len(p.Connections))
			for k, v := range p.Connections {
				participants[i].Connections[k] = v
			}
		}
	}
	return CallState{
		PlaceID: state.PlaceID, Active: state.Active, StartedAt: state.StartedAt,
		Participants: participants,
	}
}

func (r *CallRegistry) finished(placeID, sid string) bool {
	_, ok := r.finishedSIDs[placeID][sid]
	return ok
}

func (r *CallRegistry) retire(placeID, sid string) {
	if r.finishedSIDs[placeID] == nil {
		r.finishedSIDs[placeID] = map[string]struct{}{}
	}
	r.finishedSIDs[placeID][sid] = struct{}{}
}

func (r *CallRegistry) activeSID(placeID, sid string) (*CallState, bool) {
	state, ok := r.rooms[placeID]
	return state, ok && r.roomSID[placeID] == sid
}

func (r *CallRegistry) open(placeID, sid string, at time.Time) (CallState, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if sid == "" || r.finished(placeID, sid) {
		return CallState{}, false
	}
	if state, ok := r.activeSID(placeID, sid); ok {
		return cloneCallState(state), false
	}
	if previousSID := r.roomSID[placeID]; previousSID != "" && previousSID != sid {
		// A room name cannot have two live generations. Remember the displaced
		// one so a delayed room_started for it cannot replace this generation.
		r.retire(placeID, previousSID)
	}
	state := &CallState{PlaceID: placeID, Active: true, StartedAt: at}
	r.rooms[placeID] = state
	r.roomSID[placeID] = sid
	delete(r.pendingShare, placeID)
	delete(r.leftConns, placeID)
	r.changed(placeID)
	return cloneCallState(state), true
}

func (r *CallRegistry) close(placeID, sid string) (CallState, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if sid == "" || (r.roomSID[placeID] != "" && r.roomSID[placeID] != sid) || r.finished(placeID, sid) {
		return CallState{}, false
	}
	delete(r.rooms, placeID)
	delete(r.pendingShare, placeID)
	delete(r.leftConns, placeID)
	r.roomSID[placeID] = sid
	r.retire(placeID, sid)
	r.changed(placeID)
	return CallState{PlaceID: placeID}, true
}

// connKey identifies one LiveKit connection within a participant entry. The
// participant SID is unique per connection; an identity is reused by a
// reconnecting participant, so it is only the fallback when no SID is
// available (synthetic callers).
func connKey(participantSID, identity string) string {
	if participantSID != "" {
		return participantSID
	}
	return identity
}

// join records one connection under the participant ref. The roster entry
// stays present while any of its connections is live — a stale-generation
// secretary connection (#e1) departing must not hide the still-connected
// current one (#e2). A join for a connection whose leave was already
// processed is a reordered stale event and is ignored.
func (r *CallRegistry) join(placeID, sid string, participant ParticipantRef, identity, participantSID string, at time.Time) (CallState, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, ok := r.activeSID(placeID, sid)
	if !ok {
		return CallState{}, false
	}
	key := connKey(participantSID, identity)
	if participantSID != "" && r.leftConns[placeID][participantSID] {
		return cloneCallState(state), false
	}
	for i, existing := range state.Participants {
		if existing.Participant != participant {
			continue
		}
		if existing.Connections == nil {
			existing.Connections = map[string]string{}
		}
		if _, dup := existing.Connections[key]; dup {
			return cloneCallState(state), false
		}
		existing.Connections[key] = identity
		existing.Identity = identity
		state.Participants[i] = existing
		r.changed(placeID)
		return cloneCallState(state), true
	}
	sharing, pending := r.pendingShare[placeID][participant]
	if pending {
		delete(r.pendingShare[placeID], participant)
	}
	state.Participants = append(state.Participants, CallParticipant{
		Participant: participant, JoinedAt: at, ScreenShare: sharing,
		Identity:    identity,
		Connections: map[string]string{key: identity},
	})
	sort.SliceStable(state.Participants, func(i, j int) bool {
		if state.Participants[i].JoinedAt.Equal(state.Participants[j].JoinedAt) {
			return state.Participants[i].Participant.Key() < state.Participants[j].Participant.Key()
		}
		return state.Participants[i].JoinedAt.Before(state.Participants[j].JoinedAt)
	})
	r.changed(placeID)
	return cloneCallState(state), true
}

// leave drops exactly the connection the event names. The entry is removed
// only when no live connection remains; when the event carries no
// participant SID, the connection is matched by identity, and a participant
// with an untracked connection (e.g. joined before this process started)
// still falls back to an identity match so it can depart.
func (r *CallRegistry) leave(placeID, sid string, participant ParticipantRef, identity, participantSID string) (CallState, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, ok := r.activeSID(placeID, sid)
	if !ok {
		return CallState{}, false
	}
	if participantSID != "" {
		if r.leftConns[placeID] == nil {
			r.leftConns[placeID] = map[string]bool{}
		}
		r.leftConns[placeID][participantSID] = true
	}
	for i, existing := range state.Participants {
		if existing.Participant != participant {
			continue
		}
		if len(existing.Connections) == 0 {
			// Entry recorded without connection detail (a rebuild or a
			// snapshot older than connection tracking): keep the legacy
			// identity match so it can still depart.
			if existing.Identity != "" && identity != "" && existing.Identity != identity {
				r.changed(placeID)
				return cloneCallState(state), false
			}
			state.Participants = append(state.Participants[:i], state.Participants[i+1:]...)
			r.changed(placeID)
			return cloneCallState(state), true
		}
		key := connKey(participantSID, identity)
		_, tracked := existing.Connections[key]
		if !tracked {
			// The leaving connection is not in the set (unknown SID, or a
			// join this process never saw). Drop the tracked connection
			// carrying the same identity — the media for it is gone.
			for k, id := range existing.Connections {
				if id == identity {
					key = k
					tracked = true
					break
				}
			}
		}
		if !tracked {
			r.changed(placeID)
			return cloneCallState(state), false
		}
		delete(existing.Connections, key)
		if len(existing.Connections) == 0 {
			state.Participants = append(state.Participants[:i], state.Participants[i+1:]...)
		} else {
			keys := make([]string, 0, len(existing.Connections))
			for k := range existing.Connections {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			existing.Identity = existing.Connections[keys[0]]
			state.Participants[i] = existing
		}
		r.changed(placeID)
		return cloneCallState(state), true
	}
	r.changed(placeID)
	return cloneCallState(state), false
}

func (r *CallRegistry) setScreenShare(placeID, sid string, participant ParticipantRef, sharing bool) (CallState, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, ok := r.activeSID(placeID, sid)
	if !ok {
		return CallState{}, false
	}
	for i := range state.Participants {
		if state.Participants[i].Participant != participant {
			continue
		}
		if state.Participants[i].ScreenShare == sharing {
			return CallState{}, false
		}
		state.Participants[i].ScreenShare = sharing
		r.changed(placeID)
		return cloneCallState(state), true
	}
	if r.pendingShare[placeID] == nil {
		r.pendingShare[placeID] = map[ParticipantRef]bool{}
	}
	if pending, ok := r.pendingShare[placeID][participant]; !ok || pending != sharing {
		r.pendingShare[placeID][participant] = sharing
		// Force a concurrent RoomService read to retry; the track may have
		// appeared after its participant listing.
		r.changed(placeID)
	}
	return CallState{}, false
}

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
			Participant: participantToWire(entry.Participant), JoinedAt: entry.JoinedAt,
			ScreenShare: entry.ScreenShare,
		}
	}
	wire := callStateWire{
		Place: placeToWire(place), Active: state.Active, Participants: participants,
	}
	if !state.StartedAt.IsZero() {
		startedAt := state.StartedAt
		wire.StartedAt = &startedAt
	}
	return wire
}

type CallService struct {
	Server      *Server
	LiveKit     LiveKitConfig
	Registry    *CallRegistry
	RoomService liveKitRoomService
	Now         func() time.Time
	// Hooks reach the durable core: call_started admission on room_started
	// and call_event records on terminal sessions. Nil without a core store.
	Hooks *CallHooks

	rebuildMu   sync.Mutex
	rebuiltOnce bool
}

func NewCallService(server *Server, livekit LiveKitConfig) *CallService {
	return &CallService{
		Server: server, LiveKit: livekit, Registry: NewCallRegistry(),
		RoomService: newLiveKitRoomService(livekit),
	}
}

func (c *CallService) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

func (c *CallService) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /messaging/places/{place_id}/call/token", c.serveCallToken)
	mux.HandleFunc("GET /messaging/calls", c.serveCalls)
	mux.HandleFunc("POST /messaging/livekit/webhook", c.serveWebhook)
	mux.HandleFunc("POST /messaging/places/{place_id}/call/participants/remove", c.serveRemoveCallParticipant)
}

// withCallAdmission holds Workspace membership, exact installation epoch, and
// place tenure through token construction and transaction commit.
func (s *ScopedStore) withCallAdmission(
	ctx context.Context,
	placeID string,
	effect func(Place, string) error,
) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin call admission: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.authorizeMutationInTx(ctx, tx); err != nil {
		return err
	}
	place, err := s.lockScopedPlace(ctx, tx, placeID)
	if err != nil {
		return err
	}
	if _, err := s.placeAccessAfterAuthorization(ctx, tx, place, s.Scope.Actor); err != nil {
		return err
	}
	// Threads are workspace-visible discussion places, but never voice rooms.
	// Keep that boundary at admission so no caller can mint a LiveKit credential
	// for a thread merely because the general place access contract admits it.
	if place.Kind == PlaceThread {
		return ErrForbidden
	}
	members, err := s.activeMembersScoped(ctx, tx, place)
	if err != nil {
		return err
	}
	displayName := ""
	for _, member := range members {
		if member.Participant == s.Scope.Actor {
			displayName = member.ProjectedDisplayName()
			break
		}
	}
	if err := effect(place, displayName); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit call admission: %w", err)
	}
	return nil
}

func (c *CallService) serveCallToken(w http.ResponseWriter, r *http.Request) {
	_, claims, ok := c.Server.viewer(w, r)
	if !ok {
		return
	}
	store := scopedStoreForRequest(r)
	tokenFailed := false
	var response struct {
		URL      string `json:"url"`
		Token    string `json:"token"`
		Room     string `json:"room"`
		Identity string `json:"identity"`
	}
	done, err := c.Server.mutate(w, r, claims, func() error {
		return store.withCallAdmission(r.Context(), r.PathValue("place_id"), func(place Place, displayName string) error {
			if place.Kind == PlaceChannel && !place.Voice {
				return ErrForbidden
			}
			token, err := c.LiveKit.accessToken(place.PlaceID, store.Scope.Actor.Key(), displayName, c.now(), CallTokenTTL)
			if err != nil {
				tokenFailed = true
				return err
			}
			response = struct {
				URL      string `json:"url"`
				Token    string `json:"token"`
				Room     string `json:"room"`
				Identity string `json:"identity"`
			}{c.LiveKit.URL, token, place.PlaceID, store.Scope.Actor.Key()}
			return nil
		})
	})
	if !done {
		return
	}
	if err != nil {
		if tokenFailed {
			writeError(w, http.StatusInternalServerError, "call_token_failed")
			return
		}
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (c *CallService) serveCalls(w http.ResponseWriter, r *http.Request) {
	_, _, ok := c.Server.viewer(w, r)
	if !ok {
		return
	}
	calls, err := c.visibleCalls(r.Context(), scopedStoreForRequest(r))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Calls []callStateWire `json:"calls"`
	}{calls})
}

func (c *CallService) visibleCalls(ctx context.Context, store *ScopedStore) ([]callStateWire, error) {
	if err := store.authorize(ctx); err != nil {
		return nil, err
	}
	if err := c.rebuildRegistry(ctx); err != nil {
		return nil, fmt.Errorf("reconcile livekit call state: %w", err)
	}
	out := []callStateWire{}
	for _, state := range c.Registry.active() {
		place, err := store.PlaceFor(ctx, state.PlaceID)
		if errors.Is(err, ErrPlaceNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		out = append(out, callStateToWire(place, state))
	}
	return out, nil
}

func (c *CallService) serveWebhook(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxCallWebhookBytes+1))
	if err != nil || len(body) > maxCallWebhookBytes {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	token := strings.TrimSpace(r.Header.Get("Authorization"))
	if bearer, found := strings.CutPrefix(token, "Bearer "); found {
		token = strings.TrimSpace(bearer)
	}
	if token == "" || c.LiveKit.verifyWebhookToken(token, body, c.now()) != nil {
		writeError(w, http.StatusUnauthorized, "invalid_webhook_token")
		return
	}
	var event livekitWebhookEvent
	if err := json.Unmarshal(body, &event); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	c.applyWebhook(r.Context(), event)
	w.WriteHeader(http.StatusNoContent)
}

type livekitWebhookEvent struct {
	Event string `json:"event"`
	Room  struct {
		Name string `json:"name"`
		SID  string `json:"sid"`
	} `json:"room"`
	Participant struct {
		Identity string `json:"identity"`
		SID      string `json:"sid"`
	} `json:"participant"`
	Track struct {
		Source string `json:"source"`
	} `json:"track"`
}

func (c *CallService) applyWebhook(ctx context.Context, event livekitWebhookEvent) {
	placeID := event.Room.Name
	if placeID == "" || event.Room.SID == "" {
		return
	}
	var state CallState
	changed := true
	switch event.Event {
	case "room_started":
		state, changed = c.Registry.open(placeID, event.Room.SID, c.now())
		if changed {
			go c.notifyCallStarted(placeID, event.Room.SID)
		}
	case "room_finished":
		state, changed = c.Registry.close(placeID, event.Room.SID)
		if changed {
			c.endCallSessionsForRoom(placeID, event.Room.SID)
		}
	case "participant_joined", "participant_left":
		participant, err := participantFromIdentity(event.Participant.Identity)
		if err != nil {
			return
		}
		if event.Event == "participant_joined" {
			state, changed = c.Registry.join(placeID, event.Room.SID, participant, event.Participant.Identity, event.Participant.SID, c.now())
			c.removeStaleEpochParticipant(ctx, placeID, event.Participant.Identity, participant)
		} else {
			state, changed = c.Registry.leave(placeID, event.Room.SID, participant, event.Participant.Identity, event.Participant.SID)
		}
	case "track_published", "track_unpublished":
		if event.Track.Source != "SCREEN_SHARE" {
			return
		}
		participant, err := participantFromIdentity(event.Participant.Identity)
		if err != nil {
			return
		}
		state, changed = c.Registry.setScreenShare(placeID, event.Room.SID, participant, event.Event == "track_published")
	default:
		return
	}
	if changed {
		c.publishCallState(ctx, state)
	}
}

func (c *CallService) publishCallState(ctx context.Context, state CallState) {
	if c.Server == nil || c.Server.Hub == nil || c.Server.Store == nil {
		return
	}
	place, scope, err := c.Server.Store.callDeliveryScope(ctx, state.PlaceID)
	if err != nil {
		return
	}
	wire := callStateToWire(place, state)
	_ = c.Server.Hub.PublishSystemScoped(ctx, scope, Event{
		Type: EventCallState, PlaceID: state.PlaceID, Call: &wire,
	})
}

// callDeliveryScope resolves only the current app address. The Hub re-locks
// that exact epoch with the place audience before it exposes a webhook update.
func (s *Store) callDeliveryScope(ctx context.Context, placeID string) (Place, Scope, error) {
	var place Place
	var installationID string
	var authorityEpoch int64
	var name *string
	err := s.pool.QueryRow(ctx, `
		SELECT p.place_id, p.kind, p.workspace_id, p.revision, p.name, p.topic, p.visibility,
		       p.last_seq, p.voice, ai.installation_id, ai.authority_epoch
		FROM places p
		JOIN app_installations ai
		  ON ai.owner_kind='workspace' AND ai.owner_id=p.workspace_id
		 AND ai.app_id=$2 AND ai.enabled
		WHERE p.place_id=$1`, placeID, MessagingAppID).Scan(
		&place.PlaceID, &place.Kind, &place.WorkspaceID, &place.Revision, &name, &place.Topic,
		&place.Visibility, &place.LastSeq, &place.Voice, &installationID, &authorityEpoch,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Place{}, Scope{}, ErrPlaceNotFound
	}
	if err != nil {
		return Place{}, Scope{}, fmt.Errorf("resolve call delivery scope: %w", err)
	}
	if name != nil {
		place.Name = *name
	}
	return place, Scope{
		WorkspaceID: place.WorkspaceID, InstallationID: installationID,
		AuthorityEpoch: authorityEpoch,
	}, nil
}

// participantFromIdentity parses a LiveKit participant identity into its
// participant ref. Bridge-joined secretaries carry a claim-epoch tag
// (personality_agent:<id>#e<epoch>) so a stale-generation actor is
// distinguishable and precisely removable; the tag is not part of the ref.
func participantFromIdentity(identity string) (ParticipantRef, error) {
	kind, id, found := strings.Cut(identity, ":")
	if !found {
		return ParticipantRef{}, errors.New("identity is not a participant key")
	}
	if cut := strings.Index(id, "#"); cut >= 0 {
		id = id[:cut]
	}
	ref := ParticipantRef{Kind: ParticipantKind(kind), ID: id}
	if err := ref.Validate(); err != nil {
		return ParticipantRef{}, err
	}
	return ref, nil
}

// callIdentityEpoch returns the claim-epoch tag of a bridge identity, or -1
// when the identity carries none (human participants and foreign clients).
func callIdentityEpoch(identity string) int64 {
	cut := strings.Index(identity, "#e")
	if cut < 0 {
		return -1
	}
	var epoch int64
	if _, err := fmt.Sscanf(identity[cut+2:], "%d", &epoch); err != nil {
		return -1
	}
	return epoch
}

// liveKitRoomService is intentionally small: room reconciliation is the only
// server-control API this boundary needs, so keeping it on net/http avoids
// pulling the LiveKit protobuf/gRPC dependency graph into the API.
type liveKitRoomService interface {
	ListRooms(context.Context) ([]liveKitRoom, error)
	ListParticipants(context.Context, string) ([]liveKitParticipant, error)
	RemoveParticipant(context.Context, string, string) error
}

type liveKitRoom struct {
	Name      string `json:"name"`
	SID       string `json:"sid"`
	CreatedAt int64  `json:"creation_time,string"`
}

type liveKitParticipant struct {
	Identity string `json:"identity"`
	SID      string `json:"sid"`
	JoinedAt int64  `json:"joined_at,string"`
	Tracks   []struct {
		Source string `json:"source"`
	} `json:"tracks"`
}

type liveKitRoomServiceClient struct {
	baseURL string
	config  LiveKitConfig
	client  *http.Client
}

func newLiveKitRoomService(config LiveKitConfig) liveKitRoomService {
	baseURL, err := config.roomServiceURL()
	if err != nil {
		return unavailableLiveKitRoomService{err: err}
	}
	return &liveKitRoomServiceClient{
		baseURL: baseURL, config: config, client: &http.Client{Timeout: 5 * time.Second},
	}
}

type unavailableLiveKitRoomService struct{ err error }

func (s unavailableLiveKitRoomService) ListRooms(context.Context) ([]liveKitRoom, error) {
	return nil, s.err
}

func (s unavailableLiveKitRoomService) ListParticipants(context.Context, string) ([]liveKitParticipant, error) {
	return nil, s.err
}

func (s unavailableLiveKitRoomService) RemoveParticipant(context.Context, string, string) error {
	return s.err
}

func (c LiveKitConfig) roomServiceURL() (string, error) {
	endpoint := c.APIURL
	if endpoint == "" {
		endpoint = c.URL
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" {
		return "", errors.New("livekit room service URL is invalid")
	}
	switch parsed.Scheme {
	case "ws":
		parsed.Scheme = "http"
	case "wss":
		parsed.Scheme = "https"
	case "http", "https":
	default:
		return "", errors.New("livekit room service URL must use HTTP or WebSocket")
	}
	return strings.TrimRight(parsed.String(), "/"), nil
}

func (c *liveKitRoomServiceClient) ListRooms(ctx context.Context) ([]liveKitRoom, error) {
	var response struct {
		Rooms []liveKitRoom `json:"rooms"`
	}
	token, err := c.config.roomServiceToken(time.Now(), livekitVideoGrant{RoomList: true})
	if err != nil {
		return nil, err
	}
	if err := c.call(ctx, "ListRooms", struct{}{}, &response, token); err != nil {
		return nil, err
	}
	return response.Rooms, nil
}

func (c *liveKitRoomServiceClient) ListParticipants(ctx context.Context, room string) ([]liveKitParticipant, error) {
	var response struct {
		Participants []liveKitParticipant `json:"participants"`
	}
	token, err := c.config.roomServiceToken(time.Now(), livekitVideoGrant{Room: room, RoomAdmin: true})
	if err != nil {
		return nil, err
	}
	if err := c.call(ctx, "ListParticipants", struct {
		Room string `json:"room"`
	}{Room: room}, &response, token); err != nil {
		return nil, err
	}
	return response.Participants, nil
}

func (c *liveKitRoomServiceClient) RemoveParticipant(ctx context.Context, room, identity string) error {
	token, err := c.config.roomServiceToken(time.Now(), livekitVideoGrant{Room: room, RoomAdmin: true})
	if err != nil {
		return err
	}
	return c.call(ctx, "RemoveParticipant", struct {
		Room     string `json:"room"`
		Identity string `json:"identity"`
	}{Room: room, Identity: identity}, &struct{}{}, token)
}

func (c *liveKitRoomServiceClient) call(ctx context.Context, method string, request, response any, token string) error {
	payload, err := json.Marshal(request)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/twirp/livekit.RoomService/"+method, strings.NewReader(string(payload)))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("livekit RoomService %s returned %s", method, resp.Status)
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxCallWebhookBytes)).Decode(response); err != nil {
		return fmt.Errorf("decode LiveKit RoomService %s response: %w", method, err)
	}
	return nil
}

func (c *CallService) rebuildRegistry(ctx context.Context) error {
	c.rebuildMu.Lock()
	defer c.rebuildMu.Unlock()
	if c.rebuiltOnce {
		return nil
	}
	if c.RoomService == nil {
		return errors.New("LiveKit RoomService is unavailable")
	}
	// Snapshot sequence is captured before the first RoomService read. A room's
	// own sequence is then sampled around ListParticipants and retried so an
	// event during that narrower read cannot be lost to the rebuild snapshot.
	snapshotSequence := c.Registry.snapshotSequence()
	rooms, err := c.RoomService.ListRooms(ctx)
	if err != nil {
		return err
	}
	states := make([]callRoomSnapshot, 0, len(rooms))
	for _, room := range rooms {
		if room.Name == "" || room.SID == "" {
			continue
		}
		snapshot, err := c.snapshotRoom(ctx, room)
		if err != nil {
			return err
		}
		states = append(states, snapshot)
	}
	c.Registry.replaceSnapshot(states, snapshotSequence)
	c.rebuiltOnce = true
	return nil
}

const callSnapshotAttempts = 3

func (c *CallService) snapshotRoom(ctx context.Context, room liveKitRoom) (callRoomSnapshot, error) {
	startedAt := time.Unix(room.CreatedAt, 0)
	if room.CreatedAt == 0 {
		startedAt = c.now()
	}
	var snapshot callRoomSnapshot
	for attempt := 0; attempt < callSnapshotAttempts; attempt++ {
		sequence := c.Registry.roomSnapshotSequence(room.Name)
		participants, err := c.RoomService.ListParticipants(ctx, room.Name)
		if err != nil {
			return callRoomSnapshot{}, err
		}
		snapshot = callRoomSnapshot{
			state:    CallState{PlaceID: room.Name, Active: true, StartedAt: startedAt},
			sid:      room.SID,
			sequence: sequence,
		}
		for _, participant := range participants {
			ref, err := participantFromIdentity(participant.Identity)
			if err != nil {
				continue
			}
			joinedAt := time.Unix(participant.JoinedAt, 0)
			if participant.JoinedAt == 0 {
				joinedAt = startedAt
			}
			merged := false
			for i, existing := range snapshot.state.Participants {
				if existing.Participant != ref {
					continue
				}
				// One roster entry per ref: a second live connection (e.g.
				// a reclaimed secretary actor alongside its dying
				// predecessor) joins the same entry.
				if existing.Connections == nil {
					existing.Connections = map[string]string{}
				}
				existing.Connections[connKey(participant.SID, participant.Identity)] = participant.Identity
				existing.Identity = participant.Identity
				for _, track := range participant.Tracks {
					if track.Source == "SCREEN_SHARE" {
						existing.ScreenShare = true
					}
				}
				snapshot.state.Participants[i] = existing
				merged = true
				break
			}
			if merged {
				continue
			}
			entry := CallParticipant{Participant: ref, JoinedAt: joinedAt, Identity: participant.Identity,
				Connections: map[string]string{connKey(participant.SID, participant.Identity): participant.Identity}}
			for _, track := range participant.Tracks {
				if track.Source == "SCREEN_SHARE" {
					entry.ScreenShare = true
					break
				}
			}
			snapshot.state.Participants = append(snapshot.state.Participants, entry)
		}
		sort.SliceStable(snapshot.state.Participants, func(i, j int) bool {
			if snapshot.state.Participants[i].JoinedAt.Equal(snapshot.state.Participants[j].JoinedAt) {
				return snapshot.state.Participants[i].Participant.Key() < snapshot.state.Participants[j].Participant.Key()
			}
			return snapshot.state.Participants[i].JoinedAt.Before(snapshot.state.Participants[j].JoinedAt)
		})
		after := c.Registry.roomSnapshotSequence(room.Name)
		if after == sequence {
			return snapshot, nil
		}
		if attempt == callSnapshotAttempts-1 {
			// The final listing raced a webhook, so retain its pre-read sequence.
			// replaceSnapshot will discard it in favour of the registry state.
			log.Printf("livekit call snapshot for room %q raced webhooks %d times; discarding stale listing", room.Name, callSnapshotAttempts)
			return snapshot, nil
		}
	}
	return snapshot, nil
}

// RemoveWorkspaceParticipant asks LiveKit to end active media sessions for a
// participant whose Workspace membership has just closed. The Workspace
// transaction is already committed when this runs: RoomService failure must
// never resurrect its authorization.
func (c *CallService) RemoveWorkspaceParticipant(ctx context.Context, workspaceID string, participant ParticipantRef) error {
	if c == nil || c.Server == nil || c.Server.Store == nil || c.RoomService == nil {
		return errors.New("LiveKit RoomService is unavailable")
	}
	rooms, err := c.RoomService.ListRooms(ctx)
	if err != nil {
		return err
	}
	for _, room := range rooms {
		if room.Name == "" {
			continue
		}
		var belongs bool
		if err := c.Server.Store.pool.QueryRow(ctx, `
			SELECT EXISTS(
				SELECT 1 FROM places WHERE workspace_id=$1 AND place_id=$2
			)`, workspaceID, room.Name,
		).Scan(&belongs); err != nil {
			return fmt.Errorf("check LiveKit room Workspace: %w", err)
		}
		if !belongs {
			continue
		}
		participants, err := c.RoomService.ListParticipants(ctx, room.Name)
		if err != nil {
			return fmt.Errorf("list LiveKit participants in room %s: %w", room.Name, err)
		}
		for _, entry := range participants {
			ref, err := participantFromIdentity(entry.Identity)
			if err != nil || ref != participant {
				continue
			}
			// Remove by raw identity so an epoch-tagged secretary actor is
			// matched by exactly the connection LiveKit reported.
			if err := c.RoomService.RemoveParticipant(ctx, room.Name, entry.Identity); err != nil {
				return fmt.Errorf("remove LiveKit participant from room %s: %w", room.Name, err)
			}
			// Publish the local projection immediately; participant_left
			// delivery is idempotent and does not emit a second change.
			if state, changed := c.Registry.leave(room.Name, room.SID, participant, entry.Identity, entry.SID); changed {
				c.publishCallState(ctx, state)
			}
		}
	}
	if participant.Kind == KindPersonalityAgent {
		// The authority record closes with the media: any live session this
		// secretary holds in the workspace's rooms is revoked so a claim
		// cannot outlive the membership that authorized it.
		if err := c.revokeCallSessionsForWorkspace(ctx, workspaceID, participant.ID, "membership_closed"); err != nil {
			log.Printf("call: revoke sessions for closed member %s: %v", participant.Key(), err)
		}
	}
	return nil
}

// snapshotCallsFor returns the volatile registry projection for one place.
func (c *CallService) snapshotCallsFor(placeID string) *CallState {
	if c == nil || c.Registry == nil {
		return nil
	}
	state, ok := c.Registry.snapshot(placeID)
	if !ok {
		return nil
	}
	return &state
}

// rebuildRegistryOnce populates the volatile projection from RoomService on
// first use; webhook deltas keep it current afterwards.
func (c *CallService) rebuildRegistryOnce(ctx context.Context) {
	if c == nil || c.RoomService == nil {
		return
	}
	if err := c.rebuildRegistry(ctx); err != nil {
		log.Printf("call: registry rebuild: %v", err)
	}
}

// endCallSessionsForRoom terminally ends every live session bound to a room
// that LiveKit reports finished. The room is already gone, so sessions end
// directly rather than waiting on a claim holder.
func (c *CallService) endCallSessionsForRoom(placeID, roomSID string) {
	if c == nil || c.Server == nil || c.Server.Store == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var ended []CallSession
	err := withCallTx(ctx, c.Server.Store.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			UPDATE call_sessions
			SET status='ended', ended_at=now(), end_reason='room_finished',
			    updated_at=now(), claimed_by=NULL, claim_expires_at=NULL
			WHERE place_id = $1 AND (room_sid IS NULL OR room_sid = '' OR room_sid = $2)
			  AND status IN ('requested','claimed','active','ending','interrupted')
			RETURNING `+callSessionCols, placeID, roomSID)
		if err != nil {
			return err
		}
		for rows.Next() {
			var session CallSession
			if err := rows.Scan(callSessionScan(&session)...); err != nil {
				rows.Close()
				return err
			}
			ended = append(ended, session)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, session := range ended {
			if err := sweepCallUtterancesInTx(ctx, tx, session.SessionID, "room_finished"); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		log.Printf("call: end sessions for finished room %s: %v", placeID, err)
		return
	}
	for _, session := range ended {
		c.notifyCallEnded(ctx, session)
	}
}

// removeStaleEpochParticipant evicts an epoch-tagged secretary join whose tag
// does not match the session's current epoch. This is the recovery path for
// a stale ticket or zombie runner: it is observational, not instantaneous —
// the actor may have published briefly before this removal lands.
func (c *CallService) removeStaleEpochParticipant(ctx context.Context, placeID, identity string, ref ParticipantRef) {
	if ref.Kind != KindPersonalityAgent || c == nil || c.Server == nil || c.Server.Store == nil {
		return
	}
	epoch := callIdentityEpoch(identity)
	if epoch < 0 {
		// An untagged secretary identity did not come through a claim; there
		// is no durable session authorizing it, so it is always stale.
		if c.RoomService == nil {
			return
		}
		var live bool
		err := c.Server.Store.pool.QueryRow(ctx, `
			SELECT EXISTS(
				SELECT 1 FROM call_sessions
				WHERE personality_agent_id = $1 AND place_id = $2
				  AND status IN ('requested','claimed','active','ending','interrupted'))`,
			ref.ID, placeID).Scan(&live)
		if err != nil || live {
			return
		}
		c.RemoveStaleCallParticipant(ctx, placeID, identity)
		return
	}
	var current int64
	err := c.Server.Store.pool.QueryRow(ctx, `
		SELECT epoch FROM call_sessions
		WHERE personality_agent_id = $1 AND place_id = $2
		  AND status IN ('requested','claimed','active','ending','interrupted')
		LIMIT 1`, ref.ID, placeID).Scan(&current)
	if errors.Is(err, pgx.ErrNoRows) {
		// No live session authorizes any epoch of this secretary here.
		c.RemoveStaleCallParticipant(ctx, placeID, identity)
		return
	}
	if err != nil {
		log.Printf("call: stale-epoch check %s in %s: %v", identity, placeID, err)
		return
	}
	if epoch != current {
		c.RemoveStaleCallParticipant(ctx, placeID, identity)
	}
}

// revokeCallSessionsForWorkspace revokes a secretary's live sessions in the
// workspace whose membership closed.
func (c *CallService) revokeCallSessionsForWorkspace(ctx context.Context, workspaceID, personaID, reason string) error {
	rows, err := c.Server.Store.pool.Query(ctx, `
		SELECT place_id::text FROM call_sessions
		WHERE personality_agent_id = $1 AND workspace_id = $2
		  AND status IN ('requested','claimed','active','ending','interrupted')`,
		personaID, workspaceID)
	if err != nil {
		return err
	}
	placeIDs := []string{}
	for rows.Next() {
		var placeID string
		if err := rows.Scan(&placeID); err != nil {
			rows.Close()
			return err
		}
		placeIDs = append(placeIDs, placeID)
	}
	rows.Close()
	for _, placeID := range placeIDs {
		if err := c.RevokePlaceCallSessions(ctx, personaID, placeID, reason); err != nil {
			return err
		}
	}
	return nil
}

// serveRemoveCallParticipant lets any active place member remove a call
// participant — the same trust a member holds to end their own call. For a
// secretary target the durable session is revoked as well as the media.
func (c *CallService) serveRemoveCallParticipant(w http.ResponseWriter, r *http.Request) {
	if c.RoomService == nil {
		writeError(w, http.StatusServiceUnavailable, "call_request_failed")
		return
	}
	_, claims, ok := c.Server.viewer(w, r)
	if !ok {
		return
	}
	store := scopedStoreForRequest(r)
	var request struct {
		Participant struct {
			Kind string `json:"kind"`
			ID   string `json:"id"`
		} `json:"participant"`
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxCallWebhookBytes))
	if err != nil || len(body) == 0 || json.Unmarshal(body, &request) != nil {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	target := ParticipantRef{Kind: ParticipantKind(request.Participant.Kind), ID: request.Participant.ID}
	if err := target.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	done, err := c.Server.mutate(w, r, claims, func() error {
		return store.withCallAdmission(r.Context(), r.PathValue("place_id"), func(place Place, _ string) error {
			return nil
		})
	})
	if !done {
		return
	}
	if err != nil {
		writeStoreError(w, err)
		return
	}
	placeID := r.PathValue("place_id")
	participants, err := c.RoomService.ListParticipants(r.Context(), placeID)
	if err != nil {
		writeError(w, http.StatusBadGateway, "call_request_failed")
		return
	}
	if target.Kind == KindPersonalityAgent {
		// Revoke the durable session BEFORE removing media: the kicked
		// runner races to report 'failed'/'evicted', and whichever durable
		// record commits first is the cause of record. Revoking first
		// clears the claim, so the runner's report hits ErrCallClaimLost
		// and 'removed_by_member' survives as the observable reason.
		if err := c.RevokePlaceCallSessions(r.Context(), target.ID, placeID, "removed_by_member"); err != nil {
			writeStoreError(w, err)
			return
		}
	}
	removed := false
	sid := c.Registry.roomSIDFor(placeID)
	for _, entry := range participants {
		ref, err := participantFromIdentity(entry.Identity)
		if err != nil || ref != target {
			continue
		}
		if err := c.RoomService.RemoveParticipant(r.Context(), placeID, entry.Identity); err != nil {
			writeError(w, http.StatusBadGateway, "call_request_failed")
			return
		}
		removed = true
		if state, changed := c.Registry.leave(placeID, sid, target, entry.Identity, entry.SID); changed {
			c.publishCallState(r.Context(), state)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"removed": removed})
}

type livekitVideoGrant struct {
	Room           string `json:"room"`
	RoomJoin       bool   `json:"roomJoin"`
	RoomList       bool   `json:"roomList"`
	RoomAdmin      bool   `json:"roomAdmin"`
	CanPublish     bool   `json:"canPublish"`
	CanSubscribe   bool   `json:"canSubscribe"`
	CanPublishData bool   `json:"canPublishData"`
}

type livekitClaims struct {
	Issuer    string            `json:"iss"`
	Subject   string            `json:"sub,omitempty"`
	Name      string            `json:"name,omitempty"`
	NotBefore int64             `json:"nbf,omitempty"`
	Expiry    int64             `json:"exp,omitempty"`
	Video     livekitVideoGrant `json:"video,omitempty"`
	SHA256    string            `json:"sha256,omitempty"`
}

func (c LiveKitConfig) accessToken(room, identity, name string, now time.Time, ttl time.Duration) (string, error) {
	if !c.configured() || room == "" || identity == "" {
		return "", errors.New("livekit call token configuration is incomplete")
	}
	return signJWT(livekitClaims{
		Issuer: c.APIKey, Subject: identity, Name: name,
		NotBefore: now.Add(-callWebhookLeeway).Unix(), Expiry: now.Add(ttl).Unix(),
		Video: livekitVideoGrant{
			Room: room, RoomJoin: true, CanPublish: true,
			CanSubscribe: true, CanPublishData: true,
		},
	}, c.APISecret)
}

func (c LiveKitConfig) roomServiceToken(now time.Time, grant livekitVideoGrant) (string, error) {
	if !c.configured() {
		return "", errors.New("livekit room service configuration is incomplete")
	}
	return signJWT(livekitClaims{
		Issuer: c.APIKey, NotBefore: now.Add(-callWebhookLeeway).Unix(),
		Expiry: now.Add(callWebhookLeeway).Unix(),
		Video:  grant,
	}, c.APISecret)
}

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
	if claims.NotBefore != 0 && now.Add(callWebhookLeeway).Before(time.Unix(claims.NotBefore, 0)) {
		return errors.New("webhook token is not active")
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

func verifyJWT(token, secret string) ([]byte, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, errors.New("malformed token")
	}
	header, err := decodeBase64URL(parts[0])
	if err != nil {
		return nil, err
	}
	var decoded struct {
		Algorithm string `json:"alg"`
		Type      string `json:"typ"`
	}
	if err := json.Unmarshal(header, &decoded); err != nil {
		return nil, err
	}
	if decoded.Algorithm != "HS256" || decoded.Type != "JWT" {
		return nil, errors.New("unsupported token header")
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
	_, _ = mac.Write([]byte(signing))
	return mac.Sum(nil)
}

func base64URL(value []byte) string {
	return base64.RawURLEncoding.EncodeToString(value)
}

func decodeBase64URL(value string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(strings.TrimRight(value, "="))
}
