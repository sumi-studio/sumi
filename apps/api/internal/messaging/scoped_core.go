package messaging

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	workspacecontrol "github.com/sumi-studio/sumi/apps/api/internal/workspace"
)

type PlaceAccess struct {
	WorkspaceMemberID string
	PlaceMemberID     string
	VisibleFromSeq    int64
}

func (s *ScopedStore) Workspace(ctx context.Context) (Workspace, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Workspace{}, fmt.Errorf("begin scoped Workspace read: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.authorizeInTx(ctx, tx); err != nil {
		return Workspace{}, err
	}
	var workspace Workspace
	if err := tx.QueryRow(ctx, `
		SELECT workspace_id, name FROM workspaces WHERE workspace_id = $1`,
		s.Scope.WorkspaceID).Scan(&workspace.WorkspaceID, &workspace.Name); err != nil {
		return Workspace{}, fmt.Errorf("load scoped Workspace: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Workspace{}, fmt.Errorf("commit scoped Workspace read: %w", err)
	}
	return workspace, nil
}

func (s *ScopedStore) WorkspaceMembers(ctx context.Context) ([]MemberProfile, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin scoped Workspace-members read: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.authorizeInTx(ctx, tx); err != nil {
		return nil, err
	}
	members, err := s.activeMembersScoped(ctx, tx, Place{
		Kind: PlaceChannel, WorkspaceID: s.Scope.WorkspaceID,
	})
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit scoped Workspace-members read: %w", err)
	}
	return members, nil
}

func (s *ScopedStore) CreateChannel(ctx context.Context, name, topic string, voice bool) (Place, error) {
	if name == "" || len(name) > 200 {
		return Place{}, ErrInvalidChannelName
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Place{}, fmt.Errorf("begin create channel: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.authorizeManageChannelsInTx(ctx, tx); err != nil {
		return Place{}, err
	}
	place := Place{
		PlaceID: newUUIDv7(), Kind: PlaceChannel, WorkspaceID: s.Scope.WorkspaceID,
		Name: name, Topic: topic, Visibility: "public", Voice: voice,
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO places (place_id, kind, workspace_id, name, topic, voice)
		VALUES ($1, 'channel', $2, $3, $4, $5)`,
		place.PlaceID, place.WorkspaceID, place.Name, place.Topic, place.Voice); err != nil {
		return Place{}, fmt.Errorf("insert channel: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Place{}, fmt.Errorf("commit create channel: %w", err)
	}
	return place, nil
}

func (s *ScopedStore) UpdateChannelTopic(ctx context.Context, placeID, topic string) (Place, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Place{}, fmt.Errorf("begin update channel: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.authorizeManageChannelsInTx(ctx, tx); err != nil {
		return Place{}, err
	}
	place, err := s.loadScopedPlace(ctx, tx, placeID)
	if err != nil {
		return Place{}, err
	}
	if _, err := s.placeAccessAfterAuthorization(ctx, tx, place, s.Scope.Actor); err != nil {
		return Place{}, err
	}
	if place.Kind != PlaceChannel {
		return Place{}, ErrNotAChannel
	}
	if _, err := tx.Exec(ctx, `
		UPDATE places SET topic = $1 WHERE workspace_id = $2 AND place_id = $3`,
		topic, s.Scope.WorkspaceID, placeID); err != nil {
		return Place{}, fmt.Errorf("update channel topic: %w", err)
	}
	place.Topic = topic
	if err := tx.Commit(ctx); err != nil {
		return Place{}, fmt.Errorf("commit update channel: %w", err)
	}
	return place, nil
}

func (s *ScopedStore) EnsureDM(ctx context.Context, other ParticipantRef) (Place, bool, error) {
	if err := other.Validate(); err != nil {
		return Place{}, false, err
	}
	if other == s.Scope.Actor {
		return Place{}, false, errors.New("a dm needs two distinct participants")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Place{}, false, fmt.Errorf("begin ensure dm: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	actorMembership, err := s.authorizeMutationInTx(ctx, tx)
	if err != nil {
		return Place{}, false, err
	}
	otherMembership, err := s.workspaces.ActiveMembershipInTx(ctx, tx, s.Scope.WorkspaceID, other)
	if err != nil {
		return Place{}, false, ErrNotReachable
	}
	dmKey := dmPairKey(s.Scope.Actor, other)
	placeID := newUUIDv7()
	var inserted string
	err = tx.QueryRow(ctx, `
		INSERT INTO places (place_id, kind, workspace_id, dm_key)
		VALUES ($1, 'dm', $2, $3)
		ON CONFLICT (workspace_id, dm_key) DO NOTHING
		RETURNING place_id`, placeID, s.Scope.WorkspaceID, dmKey).Scan(&inserted)
	created := true
	if errors.Is(err, pgx.ErrNoRows) {
		created = false
		// Existing private-place tenure changes use the place row as their
		// audience fence. Append locks the same row while allocating seq before
		// it snapshots recipients, so re-admission either commits first and is
		// included or waits until the message transaction commits.
		if err := tx.QueryRow(ctx, `
			SELECT place_id FROM places
			WHERE workspace_id = $1 AND dm_key = $2 FOR UPDATE`,
			s.Scope.WorkspaceID, dmKey).Scan(&placeID); err != nil {
			return Place{}, false, fmt.Errorf("load existing dm: %w", err)
		}
	} else if err != nil {
		return Place{}, false, fmt.Errorf("insert dm place: %w", err)
	}
	for _, membership := range []workspacecontrol.Membership{actorMembership, otherMembership} {
		if err := admitPlaceTenure(ctx, tx, placeID, membership, 1); err != nil {
			return Place{}, false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return Place{}, false, fmt.Errorf("commit ensure dm: %w", err)
	}
	return Place{PlaceID: placeID, Kind: PlaceDM, WorkspaceID: s.Scope.WorkspaceID, Visibility: "private"}, created, nil
}

func (s *ScopedStore) CreateGroupDM(ctx context.Context, others []ParticipantRef) (Place, error) {
	seen := map[string]bool{s.Scope.Actor.Key(): true}
	members := []ParticipantRef{s.Scope.Actor}
	for _, ref := range others {
		if err := ref.Validate(); err != nil {
			return Place{}, err
		}
		if !seen[ref.Key()] {
			seen[ref.Key()] = true
			members = append(members, ref)
		}
	}
	if len(members) < 3 {
		return Place{}, errors.New("a group dm needs at least three distinct participants")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Place{}, fmt.Errorf("begin create group dm: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	actorMembership, err := s.authorizeMutationInTx(ctx, tx)
	if err != nil {
		return Place{}, err
	}
	memberships := []workspacecontrol.Membership{actorMembership}
	for _, ref := range members[1:] {
		membership, err := s.workspaces.ActiveMembershipInTx(ctx, tx, s.Scope.WorkspaceID, ref)
		if err != nil {
			return Place{}, ErrNotReachable
		}
		memberships = append(memberships, membership)
	}
	place := Place{PlaceID: newUUIDv7(), Kind: PlaceGroupDM, WorkspaceID: s.Scope.WorkspaceID, Visibility: "private"}
	if _, err := tx.Exec(ctx, `
		INSERT INTO places (place_id, kind, workspace_id)
		VALUES ($1, 'group_dm', $2)`, place.PlaceID, place.WorkspaceID); err != nil {
		return Place{}, fmt.Errorf("insert group dm: %w", err)
	}
	for _, membership := range memberships {
		if err := admitPlaceTenure(ctx, tx, place.PlaceID, membership, 1); err != nil {
			return Place{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return Place{}, fmt.Errorf("commit create group dm: %w", err)
	}
	return place, nil
}

func admitPlaceTenure(ctx context.Context, tx pgx.Tx, placeID string, membership workspacecontrol.Membership, visibleFrom int64) error {
	if visibleFrom < 1 {
		visibleFrom = 1
	}
	var currentWorkspaceMemberID string
	err := tx.QueryRow(ctx, `
		SELECT workspace_member_id FROM place_members
		WHERE place_id = $1 AND member_kind = $2 AND member_id = $3 AND left_at IS NULL`,
		placeID, membership.Participant.Kind, membership.Participant.ID).Scan(&currentWorkspaceMemberID)
	if err == nil {
		if currentWorkspaceMemberID != membership.WorkspaceMemberID {
			return errors.New("active place tenure is bound to a stale Workspace tenure")
		}
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("load active place tenure: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO place_members
			(place_member_id, workspace_id, place_id, workspace_member_id,
			 member_kind, member_id, visible_from_seq)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		newUUIDv7(), membership.WorkspaceID, placeID, membership.WorkspaceMemberID,
		membership.Participant.Kind, membership.Participant.ID, visibleFrom); err != nil {
		return fmt.Errorf("admit place tenure: %w", err)
	}
	return nil
}

func (s *ScopedStore) PlaceFor(ctx context.Context, placeID string) (Place, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Place{}, fmt.Errorf("begin scoped place read: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.authorizeInTx(ctx, tx); err != nil {
		return Place{}, err
	}
	place, err := s.loadScopedPlace(ctx, tx, placeID)
	if err != nil {
		return Place{}, err
	}
	if _, err := s.placeAccessAfterAuthorization(ctx, tx, place, s.Scope.Actor); err != nil {
		return Place{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Place{}, fmt.Errorf("commit scoped place read: %w", err)
	}
	return place, nil
}

func (s *ScopedStore) loadScopedPlace(ctx context.Context, q querier, placeID string) (Place, error) {
	return s.loadScopedPlaceWithClause(ctx, q, placeID, "")
}

// lockScopedPlace is the place-level half of the live authority fence. The
// Workspace fence is always acquired first. Place admission/closure takes the
// conflicting row lock, so audience and exact access cannot change during the
// protected effect.
func (s *ScopedStore) lockScopedPlace(ctx context.Context, q querier, placeID string) (Place, error) {
	return s.loadScopedPlaceWithClause(ctx, q, placeID, " FOR SHARE")
}

func (s *ScopedStore) loadScopedPlaceWithClause(
	ctx context.Context,
	q querier,
	placeID string,
	lockClause string,
) (Place, error) {
	if lockClause != "" && lockClause != " FOR SHARE" {
		return Place{}, errors.New("invalid place lock mode")
	}
	var place Place
	var name *string
	err := q.QueryRow(ctx, `
		SELECT place_id, kind, workspace_id, name, topic, visibility, last_seq, voice
		FROM places WHERE workspace_id = $1 AND place_id = $2`+lockClause,
		s.Scope.WorkspaceID, placeID).Scan(&place.PlaceID, &place.Kind, &place.WorkspaceID,
		&name, &place.Topic, &place.Visibility, &place.LastSeq, &place.Voice)
	if errors.Is(err, pgx.ErrNoRows) {
		return Place{}, ErrPlaceNotFound
	}
	if err != nil {
		return Place{}, fmt.Errorf("load scoped place: %w", err)
	}
	if name != nil {
		place.Name = *name
	}
	return place, nil
}

func (s *ScopedStore) placeAccessAfterAuthorization(ctx context.Context, q querier, place Place, actor ParticipantRef) (PlaceAccess, error) {
	var access PlaceAccess
	if place.Kind == PlaceChannel {
		err := q.QueryRow(ctx, `
			SELECT wm.workspace_member_id,
			       COALESCE(pm.place_member_id::text, ''), COALESCE(pm.visible_from_seq, 1)
			FROM workspace_members wm
			LEFT JOIN place_members pm
			  ON pm.workspace_id = wm.workspace_id
			 AND pm.workspace_member_id = wm.workspace_member_id
			 AND pm.place_id = $4 AND pm.left_at IS NULL
			WHERE wm.workspace_id = $1 AND wm.member_kind = $2
			  AND wm.member_id = $3 AND wm.left_at IS NULL`,
			s.Scope.WorkspaceID, actor.Kind, actor.ID, place.PlaceID).Scan(
			&access.WorkspaceMemberID, &access.PlaceMemberID, &access.VisibleFromSeq)
		if errors.Is(err, pgx.ErrNoRows) {
			return PlaceAccess{}, ErrPlaceNotFound
		}
		if err != nil {
			return PlaceAccess{}, fmt.Errorf("load channel access: %w", err)
		}
		return access, nil
	}
	err := q.QueryRow(ctx, `
		SELECT pm.workspace_member_id, pm.place_member_id, pm.visible_from_seq
		FROM place_members pm
		JOIN workspace_members wm
		  ON wm.workspace_id = pm.workspace_id
		 AND wm.workspace_member_id = pm.workspace_member_id
		 AND wm.member_kind = pm.member_kind AND wm.member_id = pm.member_id
		WHERE pm.workspace_id = $1 AND pm.place_id = $2
		  AND pm.member_kind = $3 AND pm.member_id = $4
		  AND pm.left_at IS NULL AND wm.left_at IS NULL`,
		s.Scope.WorkspaceID, place.PlaceID, actor.Kind, actor.ID).Scan(
		&access.WorkspaceMemberID, &access.PlaceMemberID, &access.VisibleFromSeq)
	if errors.Is(err, pgx.ErrNoRows) {
		return PlaceAccess{}, ErrPlaceNotFound
	}
	if err != nil {
		return PlaceAccess{}, fmt.Errorf("load private-place access: %w", err)
	}
	return access, nil
}

func (s *ScopedStore) ActiveMembers(ctx context.Context, placeID string) ([]MemberProfile, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin active-members read: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.authorizeInTx(ctx, tx); err != nil {
		return nil, err
	}
	place, err := s.loadScopedPlace(ctx, tx, placeID)
	if err != nil {
		return nil, err
	}
	if _, err := s.placeAccessAfterAuthorization(ctx, tx, place, s.Scope.Actor); err != nil {
		return nil, err
	}
	members, err := s.activeMembersScoped(ctx, tx, place)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit active-members read: %w", err)
	}
	return members, nil
}

func (s *ScopedStore) activeMembersScoped(ctx context.Context, q querier, place Place) ([]MemberProfile, error) {
	condition := `pm.workspace_id = $1 AND pm.place_id = $2
		AND pm.left_at IS NULL AND wm.left_at IS NULL`
	args := []any{s.Scope.WorkspaceID, place.PlaceID}
	if place.Kind == PlaceChannel {
		// A channel admits every Workspace member; the place-scoped join above
		// still needs its place argument, so both branches pass the same args.
		condition = `wm.workspace_id = $1 AND wm.left_at IS NULL`
	}
	rows, err := q.Query(ctx, `
		SELECT wm.member_kind, wm.member_id,
		       COALESCE(h.display_name, a.display_name, '') AS display_name,
		       COALESCE(pp.tagline, '') AS tagline,
		       COALESCE(pp.revision, 0) AS revision
		FROM workspace_members wm
		-- Bound to the exact place: without pm.place_id the join multiplies a
		-- member by every other place they are in, which for a channel (whose
		-- condition does not constrain pm) returns the same participant once
		-- per place and made notification-intent issuance insert duplicates.
		LEFT JOIN place_members pm
		  ON pm.workspace_id = wm.workspace_id
		 AND pm.place_id = $2
		 AND pm.workspace_member_id = wm.workspace_member_id AND pm.left_at IS NULL
		LEFT JOIN humans h ON wm.member_kind = 'human' AND h.human_id = wm.member_id
		LEFT JOIN agents a ON wm.member_kind = 'personality_agent'
		                  AND a.personality_agent_id = wm.member_id
		-- The profile is Participant-global, so it joins on the participant
		-- identity alone: the same tagline follows the member into every place.
		LEFT JOIN participant_profiles pp ON pp.member_kind = wm.member_kind
		                                AND pp.member_id = wm.member_id
		WHERE `+condition+`
		ORDER BY wm.workspace_member_id`, args...)
	if err != nil {
		return nil, fmt.Errorf("query scoped active members: %w", err)
	}
	defer rows.Close()
	var members []MemberProfile
	for rows.Next() {
		var member MemberProfile
		if err := rows.Scan(&member.Participant.Kind, &member.Participant.ID,
			&member.DisplayName, &member.Tagline, &member.Revision); err != nil {
			return nil, fmt.Errorf("scan scoped active member: %w", err)
		}
		members = append(members, member)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate scoped active members: %w", err)
	}
	return members, nil
}
