package messaging

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
)

const (
	LocalOverviewPath          = "/local-control/v1/messaging:overview"
	LocalOpenPath              = "/local-control/v1/messaging:open"
	LocalWritePath             = "/local-control/v1/messaging:write"
	LocalReactPath             = "/local-control/v1/messaging:react"
	LocalStatusPath            = "/local-control/v1/messaging:status"
	LocalReplyLaterPath        = "/local-control/v1/messaging:reply-later"
	LocalReplyLaterResolvePath = "/local-control/v1/messaging:reply-later-resolve"
	LocalReadThroughPath       = "/local-control/v1/messaging:read-through"
	LocalCallStatePath         = "/local-control/v1/messaging:call-state"
	// LocalNotificationSettingsPath reads and writes the same app-owned
	// notification-setting resource for a PersonalityAgent that the Human UI
	// uses for a Human. This adapter route does not itself define which agent
	// tool action invokes the operation.
	LocalNotificationSettingsPath = "/local-control/v1/messaging:notification-settings"
	// LocalSearchPath is the agent's copy of the human search box, through the
	// identical store path. 探すことが UI にしか無いと、agent は「見えている
	// ものしか思い出せない」人になる。
	LocalSearchPath = "/local-control/v1/messaging:search"
	// LocalAttentionPath hands the agent its own unconsumed AttentionCandidates
	// and, in the same call, lets it ack what it has already taken in.
	LocalAttentionPath = "/local-control/v1/messaging:attention"
	// LocalUploadAttachmentPattern is the PAID-local raw-body upload route. The
	// exact Messaging scope travels in headers because the body is the file.
	LocalUploadAttachmentPattern = "/local-control/v1/messaging/places/{place_id}/attachments"
	// LocalAttachmentPath returns the bytes of one attachment the agent can
	// currently see, bounded by MaxLocalAttachmentFetchBytes.
	LocalAttachmentPath = "/local-control/v1/messaging:attachment"

	LocalScopeWorkspaceHeader      = "X-Sumi-Workspace-Id"
	LocalScopeInstallationHeader   = "X-Sumi-Installation-Id"
	LocalScopeAuthorityEpochHeader = "X-Sumi-Authority-Epoch"
)

// MaxLocalAttachmentFetchBytes bounds what the local control lane returns for
// one attachment read. The agent reads an attachment to look at it, not to
// archive it, so the useful bound is much smaller than the upload limit. A
// larger attachment is refused by size, never truncated.
const MaxLocalAttachmentFetchBytes int64 = 2 << 20

// LocalUploadAttachmentPath returns the concrete PAID-local upload endpoint
// for one place.
func LocalUploadAttachmentPath(placeID string) string {
	return "/local-control/v1/messaging/places/" + url.PathEscape(placeID) + "/attachments"
}

// maxRelativeMinutes bounds every relative duration the agent lane accepts.
// The agent names durations ("30分後に"), not wall-clock instants, so the
// server's clock decides the moment and a drifting workspace clock cannot
// place a promise in the past or the far future.
const maxRelativeMinutes = uint32(MaxReplyLaterDelay / time.Minute)

// RegisterLocalControlRoutes exposes the same Store capabilities to a
// PersonalityAgent through its PAID-bound Unix control socket. Identity is
// supplied only by the existing generation-fenced authorization lease.
func (s *Server) RegisterLocalControlRoutes(control *agentevents.LocalControlServer) error {
	routes := []struct {
		pattern string
		handler agentevents.LocalAuthorizedHandler
	}{
		{"POST " + LocalOverviewPath, s.localOverview},
		{"POST " + LocalOpenPath, s.localOpen},
		{"POST " + LocalWritePath, s.localWrite},
		{"POST " + LocalReactPath, s.localReact},
		{"POST " + LocalStatusPath, s.localStatus},
		{"POST " + LocalReplyLaterPath, s.localReplyLater},
		{"POST " + LocalReplyLaterResolvePath, s.localReplyLaterResolve},
		{"POST " + LocalReadThroughPath, s.localReadThrough},
		{"POST " + LocalNotificationSettingsPath, s.localNotificationSettings},
		{"POST " + LocalSearchPath, s.localSearch},
		{"POST " + LocalAttentionPath, s.localAttention},
		{"POST " + LocalAttachmentPath, s.localAttachment},
	}
	if s.Calls != nil {
		routes = append(routes, struct {
			pattern string
			handler agentevents.LocalAuthorizedHandler
		}{"POST " + LocalCallStatePath, s.Calls.localCallState})
	}
	for _, route := range routes {
		if err := control.RegisterAuthorizedRoute(route.pattern, route.handler); err != nil {
			return err
		}
	}
	return control.RegisterStagedAuthorizedRoute("POST "+LocalUploadAttachmentPattern, s.localUploadAttachment)
}

func (c *CallService) localCallState(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		localScopeWire
		PlaceID string `json:"place_id,omitempty"`
	}
	if !decodeJSON(w, r, &request) {
		return
	}
	store, ok := c.Server.localScopedStore(w, r, authorization, request.localScopeWire)
	if !ok {
		return
	}
	if request.PlaceID == "" {
		calls, err := c.visibleCalls(r.Context(), store)
		if err != nil {
			writeStoreError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, struct {
			Calls []callStateWire `json:"calls"`
		}{calls})
		return
	}
	// An exact agent read is as authoritative as the list form. Reconcile the
	// volatile projection before taking its per-place snapshot after restart.
	if err := c.rebuildRegistry(r.Context()); err != nil {
		writeStoreError(w, fmt.Errorf("reconcile livekit call state: %w", err))
		return
	}
	place, err := store.PlaceFor(r.Context(), request.PlaceID)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	state, found := c.Registry.snapshot(request.PlaceID)
	if !found {
		state = CallState{PlaceID: request.PlaceID}
	}
	writeJSON(w, http.StatusOK, struct {
		Calls []callStateWire `json:"calls"`
	}{[]callStateWire{callStateToWire(place, state)}})
}

func localViewer(authorization agentevents.LocalRuntimeAuthorization) ParticipantRef {
	return PersonalityAgent(authorization.PersonalityAgentID)
}

type localScopeWire struct {
	WorkspaceID    localRequiredString `json:"workspace_id"`
	InstallationID localRequiredString `json:"installation_id"`
	AuthorityEpoch localAuthorityEpoch `json:"authority_epoch"`
}

// localRequiredString retains JSON field presence so the shared local-control
// decoder cannot silently accept duplicate Workspace or installation keys with
// last-wins semantics. This is scoped to Messaging's sealed address rather
// than changing decoding rules for unrelated local-control contracts.
type localRequiredString struct {
	value string
	seen  bool
}

func (value *localRequiredString) UnmarshalJSON(data []byte) error {
	if value.seen || len(data) < 2 || data[0] != '"' {
		return errInvalidAuthorityEpoch
	}
	var decoded string
	if err := json.Unmarshal(data, &decoded); err != nil {
		return errInvalidAuthorityEpoch
	}
	value.seen = true
	value.value = decoded
	return nil
}

func (scope localScopeWire) valid() bool {
	return scope.WorkspaceID.seen && scope.InstallationID.seen
}

func (s *Server) localScopedStore(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization, scope localScopeWire) (*ScopedStore, bool) {
	store, ok := s.localBoundStore(w, authorization, scope)
	if !ok {
		return nil, false
	}
	if err := store.authorize(r.Context()); err != nil {
		writeStoreError(w, err)
		return nil, false
	}
	return store, true
}

// localBoundStore only constructs the exact immutable app address. Read
// operations that promise one coherent snapshot must perform lifecycle and
// membership authorization inside that same snapshot, not through the pool
// here and then again while projecting the response.
func (s *Server) localBoundStore(w http.ResponseWriter, authorization agentevents.LocalRuntimeAuthorization, scope localScopeWire) (*ScopedStore, bool) {
	if !scope.valid() {
		writeError(w, http.StatusBadRequest, "invalid_scope")
		return nil, false
	}
	if s.Store == nil {
		writeError(w, http.StatusServiceUnavailable, "messaging_unavailable")
		return nil, false
	}
	store, err := s.Store.Scoped(Scope{
		WorkspaceID: scope.WorkspaceID.value, InstallationID: scope.InstallationID.value,
		AuthorityEpoch: int64(scope.AuthorityEpoch), Actor: localViewer(authorization),
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_scope")
		return nil, false
	}
	return store, true
}

func (s *Server) localOverview(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct{ localScopeWire }
	if !decodeJSON(w, r, &request) {
		return
	}
	store, ok := s.localScopedStore(w, r, authorization, request.localScopeWire)
	if !ok {
		return
	}
	overview, err := s.buildOverview(r.Context(), store)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, overview)
}

func (s *Server) localOpen(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		localScopeWire
		PlaceID   string `json:"place_id"`
		BeforeSeq int64  `json:"before_seq,omitempty"`
		Limit     int    `json:"limit,omitempty"`
	}
	if !decodeJSON(w, r, &request) {
		return
	}
	if request.PlaceID == "" || request.BeforeSeq < 0 || request.Limit < 0 {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	if request.Limit == 0 {
		request.Limit = 20
	}
	if request.Limit > 50 {
		request.Limit = 50
	}
	store, ok := s.localBoundStore(w, authorization, request.localScopeWire)
	if !ok {
		return
	}
	snapshot, err := store.OpenSnapshot(r.Context(), request.PlaceID,
		HistoryOptions{BeforeSeq: request.BeforeSeq, Limit: request.Limit})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	wires := make([]messageWire, len(snapshot.Messages))
	for i, message := range snapshot.Messages {
		wires[i] = messageToWire(snapshot.Place, message)
	}
	members := make([]memberWire, len(snapshot.Members))
	for i, profile := range snapshot.Members {
		members[i] = memberWire{Participant: participantToWire(profile.Participant), DisplayName: profile.ProjectedDisplayName()}
	}
	writeJSON(w, http.StatusOK, struct {
		Place       placeWire     `json:"place"`
		LatestSeq   int64         `json:"latest_seq"`
		LastReadSeq int64         `json:"last_read_seq"`
		Members     []memberWire  `json:"members"`
		Messages    []messageWire `json:"messages"`
	}{placeToWire(snapshot.Place), snapshot.Place.LastSeq, snapshot.LastReadSeq, members, wires})
}

func (s *Server) localWrite(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		localScopeWire
		PlaceID     string   `json:"place_id"`
		Content     string   `json:"content"`
		Urgency     string   `json:"urgency"`
		ReplyTo     *string  `json:"reply_to,omitempty"`
		ClientNonce string   `json:"client_nonce"`
		Attachments []string `json:"attachments,omitempty"`
	}
	if !decodeJSON(w, r, &request) {
		return
	}
	// Reject an invalid mutation before scope authorization or storage. A
	// rejected write must not allocate a sequence or create a durable row.
	if request.PlaceID == "" {
		writeError(w, http.StatusBadRequest, "invalid_content")
		return
	}
	if code := validateSendRequest(request.Content, request.Urgency, request.ClientNonce, request.Attachments); code != "" {
		writeError(w, http.StatusBadRequest, code)
		return
	}
	store, ok := s.localScopedStore(w, r, authorization, request.localScopeWire)
	if !ok {
		return
	}
	place, err := store.PlaceFor(r.Context(), request.PlaceID)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	replyTo := ""
	if request.ReplyTo != nil {
		replyTo = *request.ReplyTo
	}
	message, created, err := store.AppendMessage(r.Context(), AppendInput{
		PlaceID: request.PlaceID, Content: request.Content,
		Urgency: request.Urgency, ReplyTo: replyTo, ClientNonce: request.ClientNonce,
		AttachmentIDs: request.Attachments,
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if created {
		publishMessageCreated(r.Context(), store, s.Hub, place, message)
	}
	status := http.StatusCreated
	if !created {
		status = http.StatusOK
	}
	writeJSON(w, status, messageReceiptToWire(message, created))
}

// localReact toggles the agent's emoji on a message through the identical
// store path the human UI uses. The tool layer scopes it to messages visible
// in the currently open view (ADR 0011 §3: 見えていないものは操作できない);
// the server enforces the shared permission model.
func (s *Server) localReact(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		localScopeWire
		PlaceID     string `json:"place_id"`
		MessageID   string `json:"message_id"`
		Emoji       string `json:"emoji"`
		ClientNonce string `json:"client_nonce"`
	}
	if !decodeJSON(w, r, &request) {
		return
	}
	if request.PlaceID == "" || request.MessageID == "" || request.ClientNonce == "" || len(request.ClientNonce) > 128 || validateReactionEmoji(request.Emoji) != nil {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	store, ok := s.localScopedStore(w, r, authorization, request.localScopeWire)
	if !ok {
		return
	}
	place, err := store.PlaceFor(r.Context(), request.PlaceID)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	message, reacted, err := s.toggleScopedReaction(r.Context(), store, request.PlaceID, request.MessageID, request.Emoji, request.ClientNonce)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	wire := messageToWire(place, message)
	writeJSON(w, http.StatusOK, struct {
		Message messageWire `json:"message"`
		Reacted bool        `json:"reacted"`
	}{wire, reacted})
}

// localStatus sets the agent's own status through the identical store path the
// human status menu uses. Unlike react and reply-later it is not scoped to an
// open place: a person's attention state is about the person, not a screen.
func (s *Server) localStatus(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		localScopeWire
		Status string `json:"status"`
		Note   string `json:"note,omitempty"`
		// 0 (or omitted) means the status holds until it is replaced.
		ExpiresInMinutes uint32 `json:"expires_in_minutes,omitempty"`
	}
	if !decodeJSON(w, r, &request) {
		return
	}
	switch request.Status {
	case StatusAvailable, StatusBusy, StatusAway:
	default:
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	if utf8.RuneCountInString(request.Note) > MaxStatusNoteChars ||
		request.ExpiresInMinutes > maxRelativeMinutes {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	var expiresAt *time.Time
	if request.ExpiresInMinutes > 0 {
		moment := time.Now().Add(time.Duration(request.ExpiresInMinutes) * time.Minute)
		expiresAt = &moment
	}
	store, ok := s.localScopedStore(w, r, authorization, request.localScopeWire)
	if !ok {
		return
	}
	status, err := store.SetStatus(r.Context(), request.Status, request.Note, expiresAt)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	s.publishStatus(r.Context(), store, status)
	writeJSON(w, http.StatusOK, struct {
		Status statusWire `json:"status"`
	}{statusToWire(status)})
}

// localReplyLater places the agent's own「後で返信します」marker. The tool
// layer scopes it to messages visible in the currently open view, the same
// rule as react (ADR 0011 §3); the server enforces the shared permission
// model. The marker's own copy carries remind_at because the agent is its
// owner — other participants' wires never do.
func (s *Server) localReplyLater(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		localScopeWire
		PlaceID   string `json:"place_id"`
		MessageID string `json:"message_id"`
		Note      string `json:"note,omitempty"`
		// Relative so the server's clock, not the workspace's, fixes the moment.
		RemindInMinutes uint32 `json:"remind_in_minutes,omitempty"`
	}
	if !decodeJSON(w, r, &request) {
		return
	}
	if request.PlaceID == "" || request.MessageID == "" ||
		utf8.RuneCountInString(request.Note) > MaxReplyLaterNoteChars ||
		request.RemindInMinutes > maxRelativeMinutes {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	remindAt := time.Now().Add(DefaultReplyLaterDelay)
	if request.RemindInMinutes > 0 {
		remindAt = time.Now().Add(time.Duration(request.RemindInMinutes) * time.Minute)
	}
	store, ok := s.localScopedStore(w, r, authorization, request.localScopeWire)
	if !ok {
		return
	}
	viewer := store.Scope.Actor
	marker, created, err := store.CreateReplyLater(
		r.Context(), request.PlaceID, request.MessageID, request.Note, remindAt)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if created {
		s.publishReplyLaterCreated(r.Context(), store, marker)
	}
	// TODO(#128): the agent's own reminder rides the「予定された出来事」覚醒
	// トリガ from here once that trigger exists; the marker is already durable.
	status := http.StatusCreated
	if !created {
		status = http.StatusOK
	}
	writeJSON(w, status, struct {
		Marker  replyLaterWire `json:"marker"`
		Created bool           `json:"created"`
	}{replyLaterToWire(marker, viewer), created})
}

// localReplyLaterResolve marks the agent's own promise as kept. Someone else's
// marker is reported as missing, never as forbidden.
func (s *Server) localReplyLaterResolve(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		localScopeWire
		MarkerID string `json:"marker_id"`
	}
	if !decodeJSON(w, r, &request) {
		return
	}
	if request.MarkerID == "" {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	store, ok := s.localScopedStore(w, r, authorization, request.localScopeWire)
	if !ok {
		return
	}
	viewer := store.Scope.Actor
	marker, err := store.ResolveReplyLater(r.Context(), request.MarkerID)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	s.publishReplyLaterResolved(r.Context(), store, marker)
	writeJSON(w, http.StatusOK, struct {
		Marker replyLaterWire `json:"marker"`
	}{replyLaterToWire(marker, viewer)})
}

// localNotificationSettings reads or updates the agent's own notification
// setting through the identical store path the human UI uses. A request with
// no field set is a read; any field present is a change to that field only,
// because an agent naming one preference ("この place は mute にして") should not
// silently discard the rest of its setting the way a full PUT would.
func (s *Server) localNotificationSettings(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		localScopeWire
		DefaultsLevel *string `json:"defaults_level,omitempty"`
		PerPlace      *[]struct {
			PlaceID string `json:"place_id"`
			Level   string `json:"level"`
		} `json:"per_place,omitempty"`
		Keywords *[]string `json:"keywords,omitempty"`
	}
	if !decodeJSON(w, r, &request) {
		return
	}
	store, ok := s.localScopedStore(w, r, authorization, request.localScopeWire)
	if !ok {
		return
	}
	current, err := store.NotificationSettingFor(r.Context())
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if request.DefaultsLevel == nil && request.PerPlace == nil && request.Keywords == nil {
		writeJSON(w, http.StatusOK, struct {
			Setting notificationSettingWire `json:"setting"`
		}{notificationSettingToWire(current)})
		return
	}
	defaultLevel := current.Default()
	if request.DefaultsLevel != nil {
		defaultLevel = *request.DefaultsLevel
	}
	perPlace := current.PerPlace
	if request.PerPlace != nil {
		perPlace = make([]PlaceNotifyLevel, 0, len(*request.PerPlace))
		for _, entry := range *request.PerPlace {
			if entry.PlaceID == "" {
				writeError(w, http.StatusBadRequest, "invalid_request")
				return
			}
			perPlace = append(perPlace, PlaceNotifyLevel{PlaceID: entry.PlaceID, Level: entry.Level})
		}
	}
	keywords := current.Keywords
	if request.Keywords != nil {
		keywords = *request.Keywords
	}
	stored, err := store.SetNotificationSetting(r.Context(), defaultLevel, perPlace, keywords)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Setting notificationSettingWire `json:"setting"`
	}{notificationSettingToWire(stored)})
}

// localSearch is the agent's copy of the human search box, through the
// identical store path (SearchMessages)。可視性は store が決めるので、agent が
// 見られない place の発言は結果に現れず、見られない place を名指しした検索は
// 「無い」と答える——人間の UI と同じ答え方である。
func (s *Server) localSearch(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		localScopeWire
		Query   string `json:"query"`
		PlaceID string `json:"place_id,omitempty"`
		Limit   int    `json:"limit,omitempty"`
	}
	if !decodeJSON(w, r, &request) {
		return
	}
	query := strings.TrimSpace(request.Query)
	if query == "" || len(query) > MaxSearchQueryBytes || request.Limit < 0 {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	store, ok := s.localScopedStore(w, r, authorization, request.localScopeWire)
	if !ok {
		return
	}
	results, err := store.SearchMessages(r.Context(), query,
		SearchOptions{PlaceID: request.PlaceID, Limit: request.Limit})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	wires := make([]searchResultWire, len(results))
	for i, result := range results {
		wires[i] = searchResultWire{
			MessageID: result.Message.MessageID,
			Place:     placeToWire(result.Place),
			Seq:       result.Message.Seq,
			Author:    participantToWire(result.Message.Author),
			Snippet:   result.Snippet,
			CreatedAt: result.Message.CreatedAt,
		}
	}
	writeJSON(w, http.StatusOK, struct {
		Results []searchResultWire `json:"results"`
	}{wires})
}

// attentionCandidateWire is one queued「呼ばれた」. The frozen contract keeps
// actor, place, message reference, reason, urgency, and authority together in
// provenance; it only keeps the message body behind the later place-open
// authorization boundary. Candidate-local fields must not duplicate provenance.
type attentionCandidateWire struct {
	Kind         string                          `json:"kind"`
	CandidateID  string                          `json:"candidate_id"`
	CandidateSeq int64                           `json:"candidate_seq"`
	Provenance   agentevents.InboundProvenanceV1 `json:"provenance"`
	UnreadRange  attentionUnreadRangeWire        `json:"unread_range"`
	ArrivalTime  time.Time                       `json:"arrival_time"`
	Attachments  map[string]any                  `json:"attachments"`
}

type attentionUnreadRangeWire struct {
	PlaceSeqFrom int64 `json:"place_seq_from"`
	PlaceSeqTo   int64 `json:"place_seq_to"`
}

func attentionCandidateToWire(candidate AttentionCandidate, tenantID, workspaceID, agentID string) (attentionCandidateWire, error) {
	// The PAID-local authorization normally supplies the tenant. Messaging is a
	// Workspace-owned surface, so older local-control leases without that claim
	// still have one canonical tenancy boundary to record.
	if tenantID == "" {
		tenantID = workspaceID
	}
	actor := inboundActor(candidate.MessageAuthor)
	addressee := inboundActor(PersonalityAgent(agentID))
	workspace := workspaceID
	provenance := agentevents.InboundProvenanceV1{
		Version:            1,
		TenantID:           tenantID,
		PersonalityAgentID: agentID,
		Actor:              actor,
		Source: agentevents.InboundSource{
			Surface:     "messaging",
			WorkspaceID: &workspace,
			Place:       inboundPlace(candidate.PlaceID, candidate.PlaceKind),
			Delivery: &agentevents.InboundDeliveryProvenance{
				MessageID:     candidate.MessageID,
				Seq:           candidate.MessageSeq,
				Addressees:    []agentevents.InboundActorRef{addressee},
				TriggerReason: attentionTriggerReason(candidate.Reason),
				Urgency:       candidate.MessageUrgency,
				CorrelationID: nil,
				CausationID:   nil,
			},
		},
		Authority: agentevents.AdmissionAuthority{Basis: "place_membership", DecisionID: nil},
	}
	if err := provenance.Validate(); err != nil {
		return attentionCandidateWire{}, err
	}
	return attentionCandidateWire{
		Kind:         "attention_candidate",
		CandidateID:  candidate.CandidateID,
		CandidateSeq: candidate.CandidateSeq,
		Provenance:   provenance,
		UnreadRange: attentionUnreadRangeWire{
			PlaceSeqFrom: candidate.UnreadFrom,
			PlaceSeqTo:   candidate.MessageSeq,
		},
		ArrivalTime: candidate.CreatedAt,
		Attachments: map[string]any{},
	}, nil
}

func inboundActor(participant ParticipantRef) agentevents.InboundActorRef {
	if participant.Kind == KindHuman {
		return agentevents.InboundActorRef{Kind: string(KindHuman), HumanID: participant.ID}
	}
	return agentevents.InboundActorRef{Kind: string(KindPersonalityAgent), PersonalityAgentID: participant.ID}
}

func inboundPlace(placeID, kind string) *agentevents.InboundPlaceRef {
	if kind == PlaceChannel {
		return &agentevents.InboundPlaceRef{Kind: kind, ChannelID: placeID}
	}
	return &agentevents.InboundPlaceRef{Kind: kind, DMID: placeID}
}

func attentionTriggerReason(reason string) string {
	switch reason {
	case NotifyReasonDM:
		return "direct_message"
	case NotifyReasonMention:
		return "mention"
	default:
		// keyword and all are delivery-eligibility facts, but neither asserts a
		// resolved address in this candidate; both are place activity.
		return "place_activity"
	}
}

// localAttention hands the agent its own unconsumed AttentionCandidates and, in
// the same call, acks what a previous call already took in.
//
// **暫定配線である（ADR 0010 覚醒トリガ / issue #173）。** 本設計では候補の
// 到着そのものが本人を起こす。ここには自動覚醒が無く、起きている本人が自分で
// 取りに来る形にしてある。それでも「runtime が止まっていた間に呼ばれたこと」は
// message と同じ transaction で shared 側に確定しているので、次に動いたときに
// 必ず見つかる。
//
// consume_through は先に適用する：本人が「ここまで取り込んだ」と言ってから
// 「次は何か」を聞く順である。
//
// latest_seq はこの Workspace の poll が「実際に配った範囲」の高水位で、そのまま
// 次の consume_through に渡してよい上限である。candidate_seq は agent ごとに
// 単調だが、別 Workspace の配布は含まない。limit で切られた残りも含まれない。
// 任意に大きい cursor でも server がこの Workspace の高水位へ clamp するので、
// 未配布の呼びかけが消えることはなく、次の poll で再配送される。
func (s *Server) localAttention(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		localScopeWire
		ConsumeThrough int64 `json:"consume_through,omitempty"`
		Limit          int   `json:"limit,omitempty"`
	}
	if !decodeJSON(w, r, &request) {
		return
	}
	if request.ConsumeThrough < 0 || request.Limit < 0 {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	store, ok := s.localScopedStore(w, r, authorization, request.localScopeWire)
	if !ok {
		return
	}
	inbox, err := store.PollAttentionCandidates(r.Context(), request.ConsumeThrough, request.Limit)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	wires := make([]attentionCandidateWire, len(inbox.Candidates))
	for i, candidate := range inbox.Candidates {
		wire, err := attentionCandidateToWire(candidate, authorization.TenantID,
			store.Scope.WorkspaceID, store.Scope.Actor.ID)
		if err != nil {
			writeStoreError(w, err)
			return
		}
		wires[i] = wire
	}
	writeJSON(w, http.StatusOK, struct {
		Candidates []attentionCandidateWire `json:"candidates"`
		Consumed   int64                    `json:"consumed"`
		LatestSeq  int64                    `json:"latest_seq"`
	}{wires, inbox.Consumed, inbox.LatestSeq})
}

func (s *Server) localReadThrough(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		localScopeWire
		PlaceID string `json:"place_id"`
		Seq     int64  `json:"seq"`
	}
	if !decodeJSON(w, r, &request) {
		return
	}
	store, ok := s.localScopedStore(w, r, authorization, request.localScopeWire)
	if !ok {
		return
	}
	if err := store.ReadThrough(r.Context(), request.PlaceID, request.Seq); err != nil {
		writeStoreError(w, err)
		return
	}
	place, err := store.PlaceFor(r.Context(), request.PlaceID)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	lastRead, err := store.ReadMarker(r.Context(), request.PlaceID)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Place       placeWire `json:"place"`
		LastReadSeq int64     `json:"last_read_seq"`
	}{placeToWire(place), lastRead})
}

// localScopeFromHeaders reads the exact Messaging scope for a raw-body route.
// Every header must be present exactly once; the epoch must be a canonical
// positive int64.
func localScopeFromHeaders(r *http.Request) (localScopeWire, bool) {
	var scope localScopeWire
	for header, target := range map[string]*localRequiredString{
		LocalScopeWorkspaceHeader:    &scope.WorkspaceID,
		LocalScopeInstallationHeader: &scope.InstallationID,
	} {
		values := r.Header.Values(header)
		if len(values) != 1 || values[0] == "" {
			return localScopeWire{}, false
		}
		target.value, target.seen = values[0], true
	}
	epochValues := r.Header.Values(LocalScopeAuthorityEpochHeader)
	if len(epochValues) != 1 {
		return localScopeWire{}, false
	}
	epoch, ok := parseCanonicalAuthorityEpoch(epochValues[0])
	if !ok {
		return localScopeWire{}, false
	}
	scope.AuthorityEpoch = localAuthorityEpoch(epoch)
	return scope, true
}

// localUploadAttachment accepts bytes the runtime already obtained through
// its signed executor source operation. Messaging owns upload persistence
// and visibility from this application boundary; the runtime supplies only
// the exact scope, the per-file nonce, and the body. The shared upload state
// machine reacquires the exact runtime authorization epoch for both the
// reservation and the finalization, while the body streams without any lease.
func (s *Server) localUploadAttachment(
	w http.ResponseWriter,
	r *http.Request,
	authorization agentevents.LocalRuntimeAuthorization,
	release func(),
	admit agentevents.LocalAuthorizationAdmission,
) {
	scope, ok := localScopeFromHeaders(r)
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid_scope")
		return
	}
	store, ok := s.localBoundStore(w, authorization, scope)
	if !ok {
		return
	}
	if !store.Store.AttachmentsEnabled() {
		writeError(w, http.StatusServiceUnavailable, "attachments_unavailable")
		return
	}
	req, err := parseAttachmentUploadRequest(r, r.PathValue("place_id"))
	if err != nil {
		writeAttachmentUploadError(w, err)
		return
	}
	// The initial lease is released before any body byte is read; every
	// durable phase below readmits the exact epoch that authenticated us.
	release()
	body := http.MaxBytesReader(w, r.Body, MaxAttachmentBytes)
	att, created, admitted, err := uploadAttachment(
		r.Context(), store, req,
		attachmentUploadAdmission(admit),
		func() error { return setAttachmentUploadDeadlines(w) },
		body,
	)
	if err != nil {
		writeAttachmentUploadError(w, err)
		return
	}
	if !admitted {
		writeLocalUnauthorized(w)
		return
	}
	status := http.StatusCreated
	if !created {
		status = http.StatusOK
	}
	writeJSON(w, status, attachmentUploadWire{attachmentToWire(att), created})
}

func writeLocalUnauthorized(w http.ResponseWriter) {
	writeError(w, http.StatusUnauthorized, "invalid_authorization")
}

// localAttachment gives a PersonalityAgent the bytes of an attachment it can
// currently see. The request binds the exact place and message the agent's
// view showed the attachment on; a mismatch is not-found, never a hint. The
// read goes through the same AttachmentForViewer rule the browser download
// uses and is bounded by MaxLocalAttachmentFetchBytes.
func (s *Server) localAttachment(w http.ResponseWriter, r *http.Request, authorization agentevents.LocalRuntimeAuthorization) {
	var request struct {
		localScopeWire
		PlaceID      string `json:"place_id"`
		MessageID    string `json:"message_id"`
		AttachmentID string `json:"attachment_id"`
	}
	if !decodeJSON(w, r, &request) {
		return
	}
	if request.PlaceID == "" || request.MessageID == "" || !validAttachmentID(request.AttachmentID) {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	store, ok := s.localScopedStore(w, r, authorization, request.localScopeWire)
	if !ok {
		return
	}
	if !store.Store.AttachmentsEnabled() {
		writeError(w, http.StatusServiceUnavailable, "attachments_unavailable")
		return
	}
	att, err := store.AttachmentForViewer(r.Context(), request.AttachmentID)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if att.PlaceID != request.PlaceID || att.MessageID == "" || att.MessageID != request.MessageID {
		writeError(w, http.StatusNotFound, "not_found")
		return
	}
	if att.SizeBytes > MaxLocalAttachmentFetchBytes {
		writeError(w, http.StatusRequestEntityTooLarge, "attachment_too_large")
		return
	}
	blob, err := store.Store.blobs.Open(att.AttachmentID)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	defer blob.Close()
	writeAttachmentHeaders(w.Header(), att)
	// The bytes are delivered as an opaque body regardless of MIME; the agent
	// renders from the declared type, never by sniffing.
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(att.SizeBytes, 10))
	w.WriteHeader(http.StatusOK)
	if _, err := io.CopyN(w, blob, att.SizeBytes); err != nil && !errors.Is(err, io.EOF) {
		return
	}
}
