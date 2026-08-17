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
}

func NewCallRegistry() *CallRegistry {
	return &CallRegistry{
		rooms: map[string]*CallState{}, roomSequence: map[string]uint64{},
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

// replaceSnapshot applies a RoomService snapshot without losing any room that
// received a newer webhook while the snapshot was being read. roomSequence
// retains tombstones too, so a room_finished webhook cannot be resurrected by
// a stale snapshot.
func (r *CallRegistry) replaceSnapshot(states []CallState, snapshotSequence uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	next := make(map[string]*CallState, len(states))
	for _, state := range states {
		if r.roomSequence[state.PlaceID] > snapshotSequence {
			continue
		}
		copy := cloneCallState(&state)
		next[state.PlaceID] = &copy
	}
	for placeID, state := range r.rooms {
		if r.roomSequence[placeID] > snapshotSequence {
			next[placeID] = state
		}
	}
	r.rooms = next
}

func (r *CallRegistry) snapshotSequence() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.sequence
}

func (r *CallRegistry) changed(placeID string) {
	r.sequence++
	r.roomSequence[placeID] = r.sequence
}

func cloneCallState(state *CallState) CallState {
	participants := append([]CallParticipant(nil), state.Participants...)
	return CallState{
		PlaceID: state.PlaceID, Active: state.Active, StartedAt: state.StartedAt,
		Participants: participants,
	}
}

func (r *CallRegistry) open(placeID string, at time.Time) CallState {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, ok := r.rooms[placeID]
	if !ok {
		state = &CallState{PlaceID: placeID, StartedAt: at}
		r.rooms[placeID] = state
	}
	state.Active = true
	r.changed(placeID)
	return cloneCallState(state)
}

func (r *CallRegistry) close(placeID string) CallState {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.rooms, placeID)
	r.changed(placeID)
	return CallState{PlaceID: placeID}
}

func (r *CallRegistry) join(placeID string, participant ParticipantRef, at time.Time) CallState {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, ok := r.rooms[placeID]
	if !ok {
		state = &CallState{PlaceID: placeID, Active: true, StartedAt: at}
		r.rooms[placeID] = state
	}
	state.Active = true
	r.changed(placeID)
	for _, existing := range state.Participants {
		if existing.Participant == participant {
			return cloneCallState(state)
		}
	}
	state.Participants = append(state.Participants, CallParticipant{
		Participant: participant, JoinedAt: at,
	})
	sort.SliceStable(state.Participants, func(i, j int) bool {
		if state.Participants[i].JoinedAt.Equal(state.Participants[j].JoinedAt) {
			return state.Participants[i].Participant.Key() < state.Participants[j].Participant.Key()
		}
		return state.Participants[i].JoinedAt.Before(state.Participants[j].JoinedAt)
	})
	return cloneCallState(state)
}

func (r *CallRegistry) leave(placeID string, participant ParticipantRef) (CallState, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, ok := r.rooms[placeID]
	if !ok {
		return CallState{}, false
	}
	r.changed(placeID)
	for i, existing := range state.Participants {
		if existing.Participant == participant {
			state.Participants = append(state.Participants[:i], state.Participants[i+1:]...)
			return cloneCallState(state), true
		}
	}
	return cloneCallState(state), true
}

func (r *CallRegistry) setScreenShare(placeID string, participant ParticipantRef, sharing bool) (CallState, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, ok := r.rooms[placeID]
	if !ok {
		return CallState{}, false
	}
	r.changed(placeID)
	for i := range state.Participants {
		if state.Participants[i].Participant != participant {
			continue
		}
		if state.Participants[i].ScreenShare == sharing {
			return CallState{}, false
		}
		state.Participants[i].ScreenShare = sharing
		return cloneCallState(state), true
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
}

// withCallAdmission holds Workspace membership, exact installation epoch, and
// place tenure through token construction and the server-side response write.
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
			writeJSON(w, http.StatusOK, struct {
				URL      string `json:"url"`
				Token    string `json:"token"`
				Room     string `json:"room"`
				Identity string `json:"identity"`
			}{c.LiveKit.URL, token, place.PlaceID, store.Scope.Actor.Key()})
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
	}
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
	} `json:"room"`
	Participant struct {
		Identity string `json:"identity"`
	} `json:"participant"`
	Track struct {
		Source string `json:"source"`
	} `json:"track"`
}

func (c *CallService) applyWebhook(ctx context.Context, event livekitWebhookEvent) {
	placeID := event.Room.Name
	if placeID == "" {
		return
	}
	var state CallState
	changed := true
	switch event.Event {
	case "room_started":
		state = c.Registry.open(placeID, c.now())
	case "room_finished":
		state = c.Registry.close(placeID)
	case "participant_joined", "participant_left":
		participant, err := participantFromIdentity(event.Participant.Identity)
		if err != nil {
			return
		}
		if event.Event == "participant_joined" {
			state = c.Registry.join(placeID, participant, c.now())
		} else {
			state, changed = c.Registry.leave(placeID, participant)
		}
	case "track_published", "track_unpublished":
		if event.Track.Source != "SCREEN_SHARE" {
			return
		}
		participant, err := participantFromIdentity(event.Participant.Identity)
		if err != nil {
			return
		}
		state, changed = c.Registry.setScreenShare(placeID, participant, event.Event == "track_published")
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
		SELECT p.place_id, p.kind, p.workspace_id, p.name, p.topic, p.visibility,
		       p.last_seq, p.voice, ai.installation_id, ai.authority_epoch
		FROM places p
		JOIN app_installations ai
		  ON ai.owner_kind='workspace' AND ai.owner_id=p.workspace_id
		 AND ai.app_id=$2 AND ai.enabled
		WHERE p.place_id=$1`, placeID, MessagingAppID).Scan(
		&place.PlaceID, &place.Kind, &place.WorkspaceID, &name, &place.Topic,
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

func participantFromIdentity(identity string) (ParticipantRef, error) {
	kind, id, found := strings.Cut(identity, ":")
	if !found {
		return ParticipantRef{}, errors.New("identity is not a participant key")
	}
	ref := ParticipantRef{Kind: ParticipantKind(kind), ID: id}
	if err := ref.Validate(); err != nil {
		return ParticipantRef{}, err
	}
	return ref, nil
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
	CreatedAt int64  `json:"creation_time,string"`
}

type liveKitParticipant struct {
	Identity string `json:"identity"`
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
	// Snapshot sequence is captured before the first RoomService read. Webhook
	// application takes the registry mutex and records a per-room sequence, so
	// replaceSnapshot keeps any newer webhook projection instead of clobbering
	// it with this necessarily non-atomic RoomService snapshot.
	snapshotSequence := c.Registry.snapshotSequence()
	rooms, err := c.RoomService.ListRooms(ctx)
	if err != nil {
		return err
	}
	states := make([]CallState, 0, len(rooms))
	for _, room := range rooms {
		if room.Name == "" {
			continue
		}
		participants, err := c.RoomService.ListParticipants(ctx, room.Name)
		if err != nil {
			return err
		}
		startedAt := time.Unix(room.CreatedAt, 0)
		if room.CreatedAt == 0 {
			startedAt = c.now()
		}
		state := CallState{PlaceID: room.Name, Active: true, StartedAt: startedAt}
		for _, participant := range participants {
			ref, err := participantFromIdentity(participant.Identity)
			if err != nil {
				continue
			}
			joinedAt := time.Unix(participant.JoinedAt, 0)
			if participant.JoinedAt == 0 {
				joinedAt = startedAt
			}
			entry := CallParticipant{Participant: ref, JoinedAt: joinedAt}
			for _, track := range participant.Tracks {
				if track.Source == "SCREEN_SHARE" {
					entry.ScreenShare = true
					break
				}
			}
			state.Participants = append(state.Participants, entry)
		}
		sort.SliceStable(state.Participants, func(i, j int) bool {
			if state.Participants[i].JoinedAt.Equal(state.Participants[j].JoinedAt) {
				return state.Participants[i].Participant.Key() < state.Participants[j].Participant.Key()
			}
			return state.Participants[i].JoinedAt.Before(state.Participants[j].JoinedAt)
		})
		states = append(states, state)
	}
	c.Registry.replaceSnapshot(states, snapshotSequence)
	c.rebuiltOnce = true
	return nil
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
		found := false
		for _, entry := range participants {
			if entry.Identity == participant.Key() {
				found = true
				break
			}
		}
		if !found {
			continue
		}
		if err := c.RoomService.RemoveParticipant(ctx, room.Name, participant.Key()); err != nil {
			return fmt.Errorf("remove LiveKit participant from room %s: %w", room.Name, err)
		}
		// The corresponding webhook normally publishes the projection update.
		// Update local reads immediately as well, and leave duplicate webhook
		// delivery idempotent.
		c.Registry.leave(room.Name, participant)
	}
	return nil
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
