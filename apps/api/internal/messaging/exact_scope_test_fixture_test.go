package messaging

// This file is intentionally test-only. It creates exact fixture scopes for
// tests, then returns the real ScopedStore. It must never grow operation
// wrappers that recreate the removed actor-only Store API.

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	applicationapps "github.com/sumi-studio/sumi/apps/api/internal/apps"
	workspacecontrol "github.com/sumi-studio/sumi/apps/api/internal/workspace"
)

const (
	DefaultWorkspaceID      = "01900000-0000-7000-8000-000000000001"
	DefaultGeneralChannelID = "01900000-0000-7000-8000-000000000002"
	RoleOwner               = "owner"
	RoleAdmin               = "admin"
	RoleMember              = "member"
)

type testMessagingStore struct {
	*Store
	core       *Store
	workspaces *workspacecontrol.Store
	apps       *applicationapps.Store
}

func mustScopedStore(t testing.TB, store *ScopedStore, err error) *ScopedStore {
	t.Helper()
	if err != nil {
		t.Fatalf("create exact Messaging fixture scope: %v", err)
	}
	return store
}

func (s *testMessagingStore) mustScope(
	t testing.TB,
	ctx context.Context,
	workspaceID string,
	actor ParticipantRef,
) *ScopedStore {
	t.Helper()
	store, err := s.scope(ctx, workspaceID, actor)
	return mustScopedStore(t, store, err)
}

func (s *testMessagingStore) mustScopeForPlace(
	t testing.TB,
	ctx context.Context,
	placeID string,
	actor ParticipantRef,
) *ScopedStore {
	t.Helper()
	store, err := s.scopeForPlace(ctx, placeID, actor)
	return mustScopedStore(t, store, err)
}

func (s *testMessagingStore) mustScopeForActor(
	t testing.TB,
	ctx context.Context,
	actor ParticipantRef,
) *ScopedStore {
	t.Helper()
	store, err := s.scopeForActor(ctx, actor)
	return mustScopedStore(t, store, err)
}

func (s *testMessagingStore) mustScopeForMessage(
	t testing.TB,
	ctx context.Context,
	in AppendInput,
) *ScopedStore {
	t.Helper()
	return s.mustScopeForPlace(t, ctx, in.PlaceID, in.Author)
}

func (s *testMessagingStore) mustCommonScope(
	t testing.TB,
	ctx context.Context,
	actor ParticipantRef,
	others ...ParticipantRef,
) *ScopedStore {
	t.Helper()
	store, err := s.commonScope(ctx, actor, others...)
	return mustScopedStore(t, store, err)
}

var testStoresByParticipant sync.Map
var testStoresByServer sync.Map
var testActorsByServer sync.Map

func registerTestStore(store *testMessagingStore, participants ...ParticipantRef) {
	for _, participant := range participants {
		testStoresByParticipant.Store(participant.ID, store)
	}
}

func testStoreForParticipant(id string) (*testMessagingStore, bool) {
	value, ok := testStoresByParticipant.Load(id)
	if !ok {
		return nil, false
	}
	store, ok := value.(*testMessagingStore)
	return store, ok
}

func testStoreForServer(serverURL string) (*testMessagingStore, bool) {
	value, ok := testStoresByServer.Load(serverURL)
	if !ok {
		return nil, false
	}
	store, ok := value.(*testMessagingStore)
	return store, ok
}

func testActorForServer(serverURL string) (ParticipantRef, bool) {
	value, ok := testActorsByServer.Load(serverURL)
	if !ok {
		return ParticipantRef{}, false
	}
	actor, ok := value.(ParticipantRef)
	return actor, ok
}

// fixtureScopeForRequest injects an exact scope into transport test helpers.
// It resolves place-addressed requests to that fixture Workspace and otherwise
// chooses the actor's latest fixture Workspace. Production transports never
// infer scope or call this helper.
func (s *testMessagingStore) fixtureScopeForRequest(
	ctx context.Context,
	actor ParticipantRef,
	requestPath string,
	body map[string]any,
) (*ScopedStore, error) {
	if raw, ok := body["workspace_id"].(string); ok && raw != "" {
		delete(body, "workspace_id")
		return s.scope(ctx, raw, actor)
	}
	for _, key := range []string{"place_id", "parent_place_id"} {
		if placeID, ok := body[key].(string); ok && placeID != "" {
			if scoped, err := s.scopeForPlace(ctx, placeID, actor); err == nil {
				return scoped, nil
			}
		}
	}
	parsed, _ := url.Parse(requestPath)
	for _, segment := range strings.Split(parsed.Path, "/") {
		if scoped, err := s.scopeForPlace(ctx, segment, actor); err == nil {
			return scoped, nil
		}
	}
	if scoped, err := s.scopeForActor(ctx, actor); err == nil {
		return scoped, nil
	}
	var workspaceID, installationID string
	var authorityEpoch int64
	err := s.core.pool.QueryRow(ctx, `
		SELECT ai.owner_id, ai.installation_id, ai.authority_epoch
		FROM app_installations ai
		JOIN workspaces w ON w.workspace_id = ai.owner_id
		WHERE ai.owner_kind='workspace' AND ai.app_id=$1 AND ai.enabled
		ORDER BY w.created_at DESC, ai.installation_id DESC
		LIMIT 1`, MessagingAppID).Scan(&workspaceID, &installationID, &authorityEpoch)
	if err != nil {
		return nil, err
	}
	return s.core.Scoped(Scope{
		WorkspaceID: workspaceID, InstallationID: installationID,
		AuthorityEpoch: authorityEpoch, Actor: actor,
	})
}

func (s *testMessagingStore) scope(
	ctx context.Context,
	workspaceID string,
	actor ParticipantRef,
) (*ScopedStore, error) {
	var installationID string
	var authorityEpoch int64
	err := s.core.pool.QueryRow(ctx, `
		SELECT installation_id, authority_epoch
		FROM app_installations
		WHERE owner_kind = 'workspace' AND owner_id = $1
		  AND app_id = $2 AND enabled
		ORDER BY installed_at, installation_id
		LIMIT 1`, workspaceID, MessagingAppID).Scan(&installationID, &authorityEpoch)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, applicationapps.ErrInstallationNotFound
	}
	if err != nil {
		return nil, err
	}
	return s.core.Scoped(Scope{
		WorkspaceID: workspaceID, InstallationID: installationID,
		AuthorityEpoch: authorityEpoch, Actor: actor,
	})
}

func (s *testMessagingStore) scopeForPlace(
	ctx context.Context,
	placeID string,
	actor ParticipantRef,
) (*ScopedStore, error) {
	var workspaceID string
	if err := s.core.pool.QueryRow(ctx,
		"SELECT workspace_id FROM places WHERE place_id = $1", placeID,
	).Scan(&workspaceID); errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrPlaceNotFound
	} else if err != nil {
		return nil, err
	}
	return s.scope(ctx, workspaceID, actor)
}

func (s *testMessagingStore) scopeForActor(
	ctx context.Context,
	actor ParticipantRef,
) (*ScopedStore, error) {
	var workspaceID string
	err := s.core.pool.QueryRow(ctx, `
		SELECT wm.workspace_id
		FROM workspace_members wm
		JOIN app_installations ai
		  ON ai.owner_kind = 'workspace' AND ai.owner_id = wm.workspace_id
		 AND ai.app_id = $3 AND ai.enabled
		WHERE wm.member_kind = $1 AND wm.member_id = $2 AND wm.left_at IS NULL
		ORDER BY wm.joined_at DESC, wm.workspace_id DESC
		LIMIT 1`, actor.Kind, actor.ID, MessagingAppID).Scan(&workspaceID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrPlaceNotFound
	}
	if err != nil {
		return nil, err
	}
	return s.scope(ctx, workspaceID, actor)
}

func (s *testMessagingStore) commonScope(
	ctx context.Context,
	actor ParticipantRef,
	others ...ParticipantRef,
) (*ScopedStore, error) {
	rows, err := s.core.pool.Query(ctx, `
		SELECT wm.workspace_id
		FROM workspace_members wm
		JOIN app_installations ai
		  ON ai.owner_kind = 'workspace' AND ai.owner_id = wm.workspace_id
		 AND ai.app_id = $3 AND ai.enabled
		JOIN workspaces w ON w.workspace_id = wm.workspace_id
		WHERE wm.member_kind = $1 AND wm.member_id = $2 AND wm.left_at IS NULL
		ORDER BY w.created_at DESC, wm.workspace_id DESC`, actor.Kind, actor.ID, MessagingAppID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var candidates []string
	for rows.Next() {
		var workspaceID string
		if err := rows.Scan(&workspaceID); err != nil {
			return nil, err
		}
		candidates = append(candidates, workspaceID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, workspaceID := range candidates {
		all := true
		for _, other := range others {
			var active bool
			if err := s.core.pool.QueryRow(ctx, `
				SELECT EXISTS (
				  SELECT 1 FROM workspace_members
				  WHERE workspace_id = $1 AND member_kind = $2 AND member_id = $3
				    AND left_at IS NULL
				)`, workspaceID, other.Kind, other.ID).Scan(&active); err != nil {
				return nil, err
			}
			if !active {
				all = false
				break
			}
		}
		if all {
			return s.scope(ctx, workspaceID, actor)
		}
	}
	return nil, ErrNotReachable
}

func (s *testMessagingStore) workspaceOwner(
	ctx context.Context,
	workspaceID string,
) (ParticipantRef, error) {
	var owner ParticipantRef
	var kind string
	err := s.core.pool.QueryRow(ctx, `
		SELECT wm.member_kind, wm.member_id
		FROM workspaces w
		JOIN workspace_members wm
		  ON wm.workspace_id = w.workspace_id
		 AND wm.workspace_member_id = w.owner_workspace_member_id
		WHERE w.workspace_id = $1`, workspaceID).Scan(&kind, &owner.ID)
	owner.Kind = ParticipantKind(kind)
	return owner, err
}

func (s *testMessagingStore) createWorkspace(
	ctx context.Context,
	name string,
	creator ParticipantRef,
) (Workspace, error) {
	created, err := s.workspaces.CreateWorkspace(ctx, name, creator)
	if err != nil {
		return Workspace{}, err
	}
	if _, err := s.apps.InstallAtOperation(
		ctx, applicationapps.WorkspaceOwner(created.WorkspaceID), creator, MessagingAppID, uuid.NewString(),
	); err != nil {
		return Workspace{}, err
	}
	return Workspace{WorkspaceID: created.WorkspaceID, Name: created.Name}, nil
}

func (s *testMessagingStore) addWorkspaceMember(
	ctx context.Context,
	workspaceID string,
	member ParticipantRef,
) error {
	var active bool
	if err := s.core.pool.QueryRow(ctx, `
		SELECT EXISTS (
		  SELECT 1 FROM workspace_members
		  WHERE workspace_id=$1 AND member_kind=$2 AND member_id=$3 AND left_at IS NULL
		)`, workspaceID, member.Kind, member.ID).Scan(&active); err != nil {
		return err
	}
	if active {
		return nil
	}
	owner, err := s.workspaceOwner(ctx, workspaceID)
	if err != nil {
		return err
	}
	invite, err := s.workspaces.CreateInvite(ctx, workspaceID, owner)
	if err != nil {
		return err
	}
	_, err = s.workspaces.RedeemInvite(ctx, invite.Code, member)
	return err
}

func (s *testMessagingStore) removeWorkspaceMember(
	ctx context.Context,
	workspaceID string,
	member ParticipantRef,
) error {
	var membershipID string
	err := s.core.pool.QueryRow(ctx, `
		SELECT workspace_member_id FROM workspace_members
		WHERE workspace_id=$1 AND member_kind=$2 AND member_id=$3 AND left_at IS NULL`,
		workspaceID, member.Kind, member.ID).Scan(&membershipID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	owner, err := s.workspaceOwner(ctx, workspaceID)
	if err != nil {
		return err
	}
	return s.workspaces.RemoveMember(ctx, workspaceID, membershipID, owner)
}

func (s *testMessagingStore) seedDefaultWorkspaceFixture(
	ctx context.Context,
	member ParticipantRef,
) error {
	var exists bool
	if err := s.core.pool.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM workspaces WHERE workspace_id=$1)",
		DefaultWorkspaceID).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		membershipID := newUUIDv7()
		tx, err := s.core.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(context.Background()) }()
		if _, err := tx.Exec(ctx, `
			INSERT INTO workspaces (workspace_id, name, owner_workspace_member_id)
			VALUES ($1, 'Sumi', $2)`, DefaultWorkspaceID, membershipID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO workspace_members
			  (workspace_member_id, workspace_id, member_kind, member_id)
			VALUES ($1, $2, $3, $4)`, membershipID, DefaultWorkspaceID,
			member.Kind, member.ID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO places (place_id, kind, workspace_id, name, topic)
			VALUES ($1, 'channel', $2, 'general', 'みんなの場所')`,
			DefaultGeneralChannelID, DefaultWorkspaceID); err != nil {
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		if _, err := s.apps.InstallAtOperation(ctx,
			applicationapps.WorkspaceOwner(DefaultWorkspaceID), member, MessagingAppID, uuid.NewString(),
		); err != nil {
			return err
		}
	} else if err := s.addWorkspaceMember(ctx, DefaultWorkspaceID, member); err != nil {
		return err
	}
	if member.Kind == KindHuman {
		rows, err := s.core.pool.Query(ctx,
			"SELECT personality_agent_id FROM agents WHERE human_id=$1", member.ID)
		if err != nil {
			return err
		}
		var agents []ParticipantRef
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			agents = append(agents, PersonalityAgent(id))
		}
		rows.Close()
		for _, agent := range agents {
			if err := s.addWorkspaceMember(ctx, DefaultWorkspaceID, agent); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *testMessagingStore) workspacesFor(
	ctx context.Context,
	viewer ParticipantRef,
) ([]Workspace, error) {
	items, err := s.workspaces.WorkspacesFor(ctx, viewer)
	if err != nil {
		return nil, err
	}
	out := make([]Workspace, len(items))
	for i, item := range items {
		out[i] = Workspace{WorkspaceID: item.WorkspaceID, Name: item.Name}
	}
	return out, nil
}

// The remaining methods are test-only narrative adapters for pre-cutover
// feature tests. Every operation resolves an exact fixture scope and delegates
// to the production ScopedStore; clients never receive these entry points.
func (s *testMessagingStore) CreateWorkspace(ctx context.Context, name string, creator ParticipantRef) (Workspace, error) {
	return s.createWorkspace(ctx, name, creator)
}

func (s *testMessagingStore) AddWorkspaceMember(ctx context.Context, workspaceID string, member ParticipantRef, _ string) error {
	return s.addWorkspaceMember(ctx, workspaceID, member)
}

func (s *testMessagingStore) RemoveWorkspaceMember(ctx context.Context, workspaceID string, member ParticipantRef) error {
	return s.removeWorkspaceMember(ctx, workspaceID, member)
}

func (s *testMessagingStore) CreateChannel(ctx context.Context, workspaceID, name, topic string, creator ParticipantRef) (Place, error) {
	scoped, err := s.scope(ctx, workspaceID, creator)
	if err != nil {
		return Place{}, err
	}
	return scoped.CreateChannel(ctx, name, topic, false)
}

func (s *testMessagingStore) UpdateChannelTopic(ctx context.Context, placeID, topic string, actor ParticipantRef) (Place, error) {
	scoped, err := s.scopeForPlace(ctx, placeID, actor)
	if err != nil {
		return Place{}, err
	}
	return scoped.UpdateChannelTopic(ctx, placeID, topic)
}

func (s *testMessagingStore) EnsureDM(ctx context.Context, actor, other ParticipantRef) (Place, bool, error) {
	scoped, err := s.commonScope(ctx, actor, other)
	if err != nil {
		return Place{}, false, err
	}
	return scoped.EnsureDM(ctx, other)
}

func (s *testMessagingStore) CreateGroupDM(ctx context.Context, actor ParticipantRef, others []ParticipantRef) (Place, error) {
	scoped, err := s.commonScope(ctx, actor, others...)
	if err != nil {
		return Place{}, err
	}
	return scoped.CreateGroupDM(ctx, others)
}

func (s *testMessagingStore) PlaceFor(ctx context.Context, placeID string, actor ParticipantRef) (Place, error) {
	scoped, err := s.scopeForPlace(ctx, placeID, actor)
	if err != nil {
		return Place{}, err
	}
	return scoped.PlaceFor(ctx, placeID)
}

func (s *testMessagingStore) ActiveMembers(ctx context.Context, placeID string, actor ParticipantRef) ([]MemberProfile, error) {
	scoped, err := s.scopeForPlace(ctx, placeID, actor)
	if err != nil {
		return nil, err
	}
	return scoped.ActiveMembers(ctx, placeID)
}

func (s *testMessagingStore) AppendMessage(ctx context.Context, in AppendInput) (Message, bool, error) {
	scoped, err := s.scopeForPlace(ctx, in.PlaceID, in.Author)
	if err != nil {
		return Message{}, false, err
	}
	return scoped.AppendMessage(ctx, in)
}

func (s *testMessagingStore) History(ctx context.Context, placeID string, actor ParticipantRef, opt HistoryOptions) ([]Message, error) {
	scoped, err := s.scopeForPlace(ctx, placeID, actor)
	if err != nil {
		return nil, err
	}
	return scoped.History(ctx, placeID, opt)
}

func (s *testMessagingStore) MessagesSince(ctx context.Context, placeID string, actor ParticipantRef, since int64, limit int) ([]Message, error) {
	scoped, err := s.scopeForPlace(ctx, placeID, actor)
	if err != nil {
		return nil, err
	}
	return scoped.MessagesSince(ctx, placeID, since, limit)
}

func (s *testMessagingStore) EditMessage(ctx context.Context, placeID, messageID string, actor ParticipantRef, content string) (Message, error) {
	scoped, err := s.scopeForPlace(ctx, placeID, actor)
	if err != nil {
		return Message{}, err
	}
	return scoped.EditMessage(ctx, placeID, messageID, content)
}

func (s *testMessagingStore) DeleteMessage(ctx context.Context, placeID, messageID string, actor ParticipantRef) (Message, error) {
	scoped, err := s.scopeForPlace(ctx, placeID, actor)
	if err != nil {
		return Message{}, err
	}
	return scoped.DeleteMessage(ctx, placeID, messageID)
}

func (s *testMessagingStore) ReadThrough(ctx context.Context, placeID string, actor ParticipantRef, seq int64) error {
	scoped, err := s.scopeForPlace(ctx, placeID, actor)
	if err != nil {
		return err
	}
	return scoped.ReadThrough(ctx, placeID, seq)
}

func (s *testMessagingStore) ReadMarker(ctx context.Context, placeID string, actor ParticipantRef) (int64, error) {
	scoped, err := s.scopeForPlace(ctx, placeID, actor)
	if err != nil {
		return 0, err
	}
	return scoped.ReadMarker(ctx, placeID)
}

func (s *testMessagingStore) UnreadSummaries(ctx context.Context, actor ParticipantRef) ([]UnreadSummary, error) {
	scoped, err := s.scopeForActor(ctx, actor)
	if err != nil {
		return nil, err
	}
	return scoped.UnreadSummaries(ctx)
}

func (s *testMessagingStore) ToggleReaction(ctx context.Context, placeID, messageID string, actor ParticipantRef, emoji string) (Message, bool, error) {
	scoped, err := s.scopeForPlace(ctx, placeID, actor)
	if err != nil {
		return Message{}, false, err
	}
	return scoped.ToggleReactionIdempotent(ctx, placeID, messageID, emoji, newUUIDv7())
}

func (s *testMessagingStore) ToggleReactionIdempotent(ctx context.Context, placeID, messageID string, actor ParticipantRef, emoji, nonce string) (Message, bool, error) {
	scoped, err := s.scopeForPlace(ctx, placeID, actor)
	if err != nil {
		return Message{}, false, err
	}
	return scoped.ToggleReactionIdempotent(ctx, placeID, messageID, emoji, nonce)
}

func (s *testMessagingStore) NotificationSettingFor(ctx context.Context, actor ParticipantRef) (NotificationSetting, error) {
	scoped, err := s.scopeForActor(ctx, actor)
	if err != nil {
		return NotificationSetting{}, err
	}
	return scoped.NotificationSettingFor(ctx)
}

func (s *testMessagingStore) NotificationDecisionsFor(ctx context.Context, place Place, message Message) ([]NotificationDecision, error) {
	scoped, err := s.scope(ctx, place.WorkspaceID, message.Author)
	if err != nil {
		return nil, err
	}
	return scoped.NotificationDecisionsFor(ctx, place, message)
}

func (s *testMessagingStore) NotificationIntentsForMessage(ctx context.Context, messageID string) ([]NotificationDecision, error) {
	var workspaceID, authorKind, authorID string
	err := s.core.pool.QueryRow(ctx, `
		SELECT workspace_id, author_kind, author_id FROM messages WHERE message_id=$1`,
		messageID).Scan(&workspaceID, &authorKind, &authorID)
	if err != nil {
		return nil, err
	}
	scoped, err := s.scope(ctx, workspaceID, ParticipantRef{Kind: ParticipantKind(authorKind), ID: authorID})
	if err != nil {
		return nil, err
	}
	return scoped.NotificationIntentsForMessage(ctx, messageID)
}

func (s *testMessagingStore) SetNotificationSetting(ctx context.Context, actor ParticipantRef, level string, perPlace []PlaceNotifyLevel, keywords []string) (NotificationSetting, error) {
	scoped, err := s.scopeForActor(ctx, actor)
	if err != nil {
		return NotificationSetting{}, err
	}
	return scoped.SetNotificationSetting(ctx, level, perPlace, keywords)
}

func (s *testMessagingStore) CreateReplyLater(ctx context.Context, placeID, messageID string, actor ParticipantRef, note string, remindAt time.Time) (ReplyLaterMarker, bool, error) {
	scoped, err := s.scopeForPlace(ctx, placeID, actor)
	if err != nil {
		return ReplyLaterMarker{}, false, err
	}
	return scoped.CreateReplyLater(ctx, placeID, messageID, note, remindAt)
}

func (s *testMessagingStore) ResolveReplyLater(ctx context.Context, markerID string, actor ParticipantRef) (ReplyLaterMarker, error) {
	scoped, err := s.scopeForActor(ctx, actor)
	if err != nil {
		return ReplyLaterMarker{}, err
	}
	return scoped.ResolveReplyLater(ctx, markerID)
}

func (s *testMessagingStore) ReplyLaterMarkersFor(ctx context.Context, actor ParticipantRef) ([]ReplyLaterMarker, error) {
	scoped, err := s.scopeForActor(ctx, actor)
	if err != nil {
		return nil, err
	}
	return scoped.ReplyLaterMarkersFor(ctx)
}

func (s *testMessagingStore) SetStatus(ctx context.Context, actor ParticipantRef, status, note string, expiresAt *time.Time) (ParticipantStatus, error) {
	scoped, err := s.scopeForActor(ctx, actor)
	if err != nil {
		return ParticipantStatus{}, err
	}
	return scoped.SetStatus(ctx, status, note, expiresAt)
}

func (s *testMessagingStore) StatusesVisibleTo(ctx context.Context, actor ParticipantRef) ([]ParticipantStatus, error) {
	scoped, err := s.scopeForActor(ctx, actor)
	if err != nil {
		return nil, err
	}
	return scoped.StatusesVisibleTo(ctx)
}

func (s *testMessagingStore) Profile(ctx context.Context, actor ParticipantRef) (MemberProfile, error) {
	scoped, err := s.scopeForActor(ctx, actor)
	if err != nil {
		return MemberProfile{}, err
	}
	return scoped.Profile(ctx)
}

func (s *testMessagingStore) SetProfile(ctx context.Context, actor ParticipantRef, displayName, tagline *string) (MemberProfile, error) {
	scoped, err := s.scopeForActor(ctx, actor)
	if err != nil {
		return MemberProfile{}, err
	}
	return scoped.SetProfile(ctx, displayName, tagline)
}

func (s *testMessagingStore) ParticipantVisible(ctx context.Context, actor, target ParticipantRef) (bool, error) {
	scoped, err := s.scopeForActor(ctx, actor)
	if err != nil {
		return false, err
	}
	return scoped.ParticipantVisible(ctx, target)
}

func (s *testMessagingStore) WorkspacesFor(ctx context.Context, actor ParticipantRef) ([]Workspace, error) {
	return s.workspacesFor(ctx, actor)
}

func (s *testMessagingStore) WorkspaceMemberProfiles(ctx context.Context, workspaceID string, actor ParticipantRef) ([]MemberProfile, error) {
	scoped, err := s.scope(ctx, workspaceID, actor)
	if err != nil {
		return nil, err
	}
	return scoped.WorkspaceMembers(ctx)
}

func (s *testMessagingStore) ActiveParticipantsForPlace(ctx context.Context, placeID string) (map[ParticipantRef]struct{}, error) {
	var workspaceID string
	if err := s.core.pool.QueryRow(ctx, "SELECT workspace_id FROM places WHERE place_id=$1", placeID).Scan(&workspaceID); err != nil {
		return nil, err
	}
	owner, err := s.workspaceOwner(ctx, workspaceID)
	if err != nil {
		return nil, err
	}
	members, err := s.mustScopeForPlaceNoTest(ctx, placeID, owner)
	if err != nil {
		return nil, err
	}
	profiles, err := members.ActiveMembers(ctx, placeID)
	if err != nil {
		return nil, err
	}
	out := make(map[ParticipantRef]struct{}, len(profiles))
	for _, profile := range profiles {
		out[profile.Participant] = struct{}{}
	}
	return out, nil
}

func (s *testMessagingStore) mustScopeForPlaceNoTest(ctx context.Context, placeID string, actor ParticipantRef) (*ScopedStore, error) {
	return s.scopeForPlace(ctx, placeID, actor)
}

func (s *testMessagingStore) ParticipantsVisibleTo(ctx context.Context, target ParticipantRef) (map[ParticipantRef]struct{}, error) {
	scoped, err := s.scopeForActor(ctx, target)
	if err != nil {
		return nil, err
	}
	profiles, err := scoped.WorkspaceMembers(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[ParticipantRef]struct{}, len(profiles))
	for _, profile := range profiles {
		out[profile.Participant] = struct{}{}
	}
	return out, nil
}
