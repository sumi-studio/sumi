package messaging

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	workspacecontrol "github.com/sumi-studio/sumi/apps/api/internal/workspace"
)

const MaxThreadNameChars = 100
const ThreadPreviewChars = 120

var (
	ErrNotThreadable = errors.New("threads can only be created inside a channel")
	ErrThreadExists  = errors.New("this message already has a thread")
)

type Thread struct {
	Place              Place
	ParentPlaceID      string
	ParentMessageID    string
	MessageCount       int64
	LastMessageAt      *time.Time
	LastMessagePreview string
	Participants       []ParticipantRef
}

func (s *ScopedStore) CreateThread(ctx context.Context, parentPlaceID, name, originMessageID string) (Thread, error) {
	name = strings.TrimSpace(name)
	if name == "" || utf8.RuneCountInString(name) > MaxThreadNameChars {
		return Thread{}, fmt.Errorf("thread name must be 1..%d characters", MaxThreadNameChars)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Thread{}, fmt.Errorf("begin create thread: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	membership, err := s.authorizeMutationInTx(ctx, tx)
	if err != nil {
		return Thread{}, err
	}
	parent, err := s.loadScopedPlace(ctx, tx, parentPlaceID)
	if err != nil {
		return Thread{}, err
	}
	if _, err := s.placeAccessAfterAuthorization(ctx, tx, parent, s.Scope.Actor); err != nil {
		return Thread{}, err
	}
	if parent.Kind != PlaceChannel {
		return Thread{}, ErrNotThreadable
	}
	var origin *string
	if originMessageID != "" {
		var exists bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM messages
			 WHERE workspace_id=$1 AND place_id=$2 AND message_id=$3 AND deleted_at IS NULL)`,
			s.Scope.WorkspaceID, parentPlaceID, originMessageID).Scan(&exists); err != nil {
			return Thread{}, fmt.Errorf("check thread origin: %w", err)
		}
		if !exists {
			return Thread{}, ErrMessageNotFound
		}
		origin = &originMessageID
	}
	thread := Thread{
		Place: Place{PlaceID: newUUIDv7(), Kind: PlaceThread, WorkspaceID: s.Scope.WorkspaceID,
			Name: name, Visibility: parent.Visibility},
		ParentPlaceID: parentPlaceID, ParentMessageID: originMessageID,
		Participants: []ParticipantRef{s.Scope.Actor},
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO places (place_id, kind, workspace_id, name, parent_place_id, parent_message_id)
		VALUES ($1, 'thread', $2, $3, $4, $5)`,
		thread.Place.PlaceID, s.Scope.WorkspaceID, name, parentPlaceID, origin)
	if err != nil {
		if isUniqueViolation(err) {
			return Thread{}, ErrThreadExists
		}
		return Thread{}, fmt.Errorf("insert thread: %w", err)
	}
	if err := joinThread(ctx, tx, thread.Place.PlaceID, membership); err != nil {
		return Thread{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Thread{}, fmt.Errorf("commit create thread: %w", err)
	}
	return thread, nil
}

func joinThread(ctx context.Context, tx pgx.Tx, placeID string, membership workspacecontrol.Membership) error {
	return admitPlaceTenure(ctx, tx, placeID, membership, 1)
}

func (s *ScopedStore) ThreadsIn(ctx context.Context, parentPlaceID string) ([]Thread, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	membership, err := s.authorizeInTx(ctx, tx)
	if err != nil {
		return nil, err
	}
	parent, err := s.loadScopedPlace(ctx, tx, parentPlaceID)
	if err != nil {
		return nil, err
	}
	if _, err := s.placeAccessAfterAuthorization(ctx, tx, parent, s.Scope.Actor); err != nil {
		return nil, err
	}
	if parent.Kind != PlaceChannel {
		return nil, ErrNotThreadable
	}
	threads, err := s.threadsWhere(ctx, tx, membership.WorkspaceMemberID, "t.parent_place_id = $3", parentPlaceID)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return threads, nil
}

func (s *ScopedStore) ThreadsFor(ctx context.Context) ([]Thread, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	membership, err := s.authorizeInTx(ctx, tx)
	if err != nil {
		return nil, err
	}
	threads, err := s.threadsWhere(ctx, tx, membership.WorkspaceMemberID,
		"EXISTS (SELECT 1 FROM place_members pm WHERE pm.workspace_id=$1 AND pm.place_id=t.place_id AND pm.workspace_member_id=$2 AND pm.left_at IS NULL)",
	)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return threads, nil
}

func (s *ScopedStore) ThreadFor(ctx context.Context, threadID string) (Thread, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Thread{}, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	membership, err := s.authorizeInTx(ctx, tx)
	if err != nil {
		return Thread{}, err
	}
	place, err := s.loadScopedPlace(ctx, tx, threadID)
	if err != nil || place.Kind != PlaceThread {
		return Thread{}, ErrPlaceNotFound
	}
	if _, err := s.placeAccessAfterAuthorization(ctx, tx, place, s.Scope.Actor); err != nil {
		return Thread{}, err
	}
	threads, err := s.threadsWhere(ctx, tx, membership.WorkspaceMemberID, "t.place_id = $3", threadID)
	if err != nil {
		return Thread{}, err
	}
	if len(threads) == 0 {
		return Thread{}, ErrPlaceNotFound
	}
	if err := tx.Commit(ctx); err != nil {
		return Thread{}, err
	}
	return threads[0], nil
}

// conditions use $1=workspace and $2=viewer Workspace tenure; extra args start at $3.
func (s *ScopedStore) threadsWhere(ctx context.Context, q querier, workspaceMemberID, condition string, args ...any) ([]Thread, error) {
	queryArgs := []any{s.Scope.WorkspaceID, workspaceMemberID}
	queryArgs = append(queryArgs, args...)
	rows, err := q.Query(ctx, fmt.Sprintf(`
		SELECT t.place_id, t.workspace_id, t.name, t.topic, t.visibility, t.last_seq,
		       t.parent_place_id, t.parent_message_id,
		       (SELECT count(*) FROM messages m WHERE m.workspace_id=$1 AND m.place_id=t.place_id AND m.deleted_at IS NULL),
		       (SELECT m.created_at FROM messages m WHERE m.workspace_id=$1 AND m.place_id=t.place_id AND m.deleted_at IS NULL ORDER BY m.seq DESC LIMIT 1),
		       (SELECT m.content FROM messages m WHERE m.workspace_id=$1 AND m.place_id=t.place_id AND m.deleted_at IS NULL ORDER BY m.seq DESC LIMIT 1)
		FROM places t WHERE t.workspace_id=$1 AND $2::text IS NOT NULL AND t.kind='thread' AND (%s)
		ORDER BY COALESCE((SELECT max(m.created_at) FROM messages m WHERE m.place_id=t.place_id), t.created_at) DESC, t.place_id DESC`, condition), queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("query threads: %w", err)
	}
	defer rows.Close()
	var out []Thread
	var ids []string
	for rows.Next() {
		var t Thread
		var name string
		var origin, preview *string
		if err := rows.Scan(&t.Place.PlaceID, &t.Place.WorkspaceID, &name, &t.Place.Topic,
			&t.Place.Visibility, &t.Place.LastSeq, &t.ParentPlaceID, &origin,
			&t.MessageCount, &t.LastMessageAt, &preview); err != nil {
			return nil, fmt.Errorf("scan thread: %w", err)
		}
		t.Place.Kind, t.Place.Name = PlaceThread, name
		if origin != nil {
			t.ParentMessageID = *origin
		}
		if preview != nil {
			t.LastMessagePreview = truncateRunes(*preview, ThreadPreviewChars)
		}
		out, ids = append(out, t), append(ids, t.Place.PlaceID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	participants, err := s.threadParticipants(ctx, q, ids)
	if err != nil {
		return nil, err
	}
	for i := range out {
		out[i].Participants = participants[out[i].Place.PlaceID]
	}
	return out, nil
}

func (s *ScopedStore) joinThreadParticipants(ctx context.Context, tx pgx.Tx, placeID string, actorMembership workspacecontrol.Membership, mentions []ParticipantRef) error {
	if err := joinThread(ctx, tx, placeID, actorMembership); err != nil {
		return err
	}
	for _, mention := range mentions {
		membership, err := s.workspaces.ActiveMembershipInTx(ctx, tx, s.Scope.WorkspaceID, mention)
		if err != nil {
			return fmt.Errorf("load mentioned thread participant: %w", err)
		}
		if err := joinThread(ctx, tx, placeID, membership); err != nil {
			return err
		}
	}
	return nil
}

func (s *ScopedStore) threadNotificationMembers(ctx context.Context, q querier, placeID string, profiles []MemberProfile) ([]MemberProfile, error) {
	joined, err := s.threadParticipants(ctx, q, []string{placeID})
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for _, ref := range joined[placeID] {
		seen[ref.Key()] = true
	}
	out := make([]MemberProfile, 0, len(seen))
	for _, profile := range profiles {
		if seen[profile.Participant.Key()] {
			out = append(out, profile)
		}
	}
	return out, nil
}

func (s *ScopedStore) threadParticipants(ctx context.Context, q querier, ids []string) (map[string][]ParticipantRef, error) {
	out := map[string][]ParticipantRef{}
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := q.Query(ctx, `
		SELECT pm.place_id, pm.member_kind, pm.member_id FROM place_members pm
		JOIN workspace_members wm ON wm.workspace_id=pm.workspace_id AND wm.workspace_member_id=pm.workspace_member_id AND wm.left_at IS NULL
		WHERE pm.workspace_id=$1 AND pm.place_id=ANY($2) AND pm.left_at IS NULL
		ORDER BY pm.place_id, pm.joined_at, pm.place_member_id`, s.Scope.WorkspaceID, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var placeID string
		var ref ParticipantRef
		if err := rows.Scan(&placeID, &ref.Kind, &ref.ID); err != nil {
			return nil, err
		}
		out[placeID] = append(out[placeID], ref)
	}
	return out, rows.Err()
}

func truncateRunes(value string, max int) string {
	runes := []rune(value)
	if len(runes) <= max {
		return value
	}
	return string(runes[:max]) + "…"
}
