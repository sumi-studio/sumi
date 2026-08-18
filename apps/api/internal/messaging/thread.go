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
const MaxClientNonceBytes = 128

// clientNonceValid is the one ingress/storage rule for idempotency keys. A
// PostgreSQL text value cannot contain NUL, so reject it before any mutation
// path can turn malformed client input into a driver error.
func clientNonceValid(nonce string) bool {
	return nonce != "" && len(nonce) <= MaxClientNonceBytes &&
		utf8.ValidString(nonce) && !strings.ContainsRune(nonce, '\x00')
}

func threadNameValid(name string) bool {
	name = strings.TrimSpace(name)
	return name != "" &&
		utf8.RuneCountInString(name) <= MaxThreadNameChars &&
		!strings.ContainsRune(name, '\x00')
}

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

func (s *ScopedStore) CreateThread(ctx context.Context, parentPlaceID, name, originMessageID, clientNonce string) (Thread, bool, error) {
	name = strings.TrimSpace(name)
	if !threadNameValid(name) {
		return Thread{}, false, fmt.Errorf("thread name must be 1..%d characters", MaxThreadNameChars)
	}
	if !clientNonceValid(clientNonce) {
		return Thread{}, false, errors.New("client nonce must be 1..128 bytes")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Thread{}, false, fmt.Errorf("begin create thread: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	membership, err := s.authorizeMutationInTx(ctx, tx)
	if err != nil {
		return Thread{}, false, err
	}
	parent, err := s.loadScopedPlace(ctx, tx, parentPlaceID)
	if err != nil {
		return Thread{}, false, err
	}
	if _, err := s.placeAccessAfterAuthorization(ctx, tx, parent, s.Scope.Actor); err != nil {
		return Thread{}, false, err
	}
	if parent.Kind != PlaceChannel {
		return Thread{}, false, ErrNotThreadable
	}
	// Serialize an operation identity before looking up its receipt. The lock
	// makes a concurrent retry observe the first committed receipt instead of
	// racing into a second empty-origin thread.
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)",
		reactionMutationLockKey(s.Scope.Actor, "thread-create:"+s.Scope.WorkspaceID+":"+clientNonce)); err != nil {
		return Thread{}, false, fmt.Errorf("lock thread creation: %w", err)
	}
	if existing, found, err := s.threadCreationReplay(ctx, tx, membership.WorkspaceMemberID, parentPlaceID, name, originMessageID, clientNonce); err != nil {
		return Thread{}, false, err
	} else if found {
		if err := tx.Commit(ctx); err != nil {
			return Thread{}, false, fmt.Errorf("commit replayed thread creation: %w", err)
		}
		return existing, false, nil
	}
	var origin *string
	if originMessageID != "" {
		// Take the same row lock as DeleteMessage before deciding that an
		// origin is usable. Without it, a delete can commit after an unlocked
		// existence check and leave this transaction creating a thread rooted at
		// a tombstone.
		var active bool
		if err := tx.QueryRow(ctx, `
			SELECT deleted_at IS NULL FROM messages
			WHERE workspace_id=$1 AND place_id=$2 AND message_id=$3
			FOR UPDATE`, s.Scope.WorkspaceID, parentPlaceID, originMessageID).Scan(&active); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return Thread{}, false, ErrMessageNotFound
			}
			return Thread{}, false, fmt.Errorf("lock thread origin: %w", err)
		}
		if !active {
			return Thread{}, false, ErrMessageNotFound
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
			return Thread{}, false, ErrThreadExists
		}
		return Thread{}, false, fmt.Errorf("insert thread: %w", err)
	}
	if err := joinThread(ctx, tx, thread.Place.PlaceID, membership); err != nil {
		return Thread{}, false, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO thread_creation_receipts
			(workspace_id, creator_kind, creator_id, client_nonce, thread_id, parent_place_id, parent_message_id, name)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		s.Scope.WorkspaceID, s.Scope.Actor.Kind, s.Scope.Actor.ID, clientNonce,
		thread.Place.PlaceID, parentPlaceID, origin, name); err != nil {
		return Thread{}, false, fmt.Errorf("record thread creation receipt: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Thread{}, false, fmt.Errorf("commit create thread: %w", err)
	}
	return thread, true, nil
}

// threadCreationReplay returns the durable result of this caller's creation
// nonce. A nonce is an operation identity, so changing any creation input is a
// conflict rather than a replay of a different request.
func (s *ScopedStore) threadCreationReplay(ctx context.Context, q querier, workspaceMemberID, parentPlaceID, name, originMessageID, clientNonce string) (Thread, bool, error) {
	var threadID, storedParent, storedName string
	var storedOrigin *string
	err := q.QueryRow(ctx, `
		SELECT thread_id, parent_place_id, parent_message_id, name
		FROM thread_creation_receipts
		WHERE workspace_id=$1 AND creator_kind=$2 AND creator_id=$3 AND client_nonce=$4`,
		s.Scope.WorkspaceID, s.Scope.Actor.Kind, s.Scope.Actor.ID, clientNonce,
	).Scan(&threadID, &storedParent, &storedOrigin, &storedName)
	if errors.Is(err, pgx.ErrNoRows) {
		return Thread{}, false, nil
	}
	if err != nil {
		return Thread{}, false, fmt.Errorf("load thread creation receipt: %w", err)
	}
	storedOriginID := ""
	if storedOrigin != nil {
		storedOriginID = *storedOrigin
	}
	if storedParent != parentPlaceID || storedName != name || storedOriginID != originMessageID {
		return Thread{}, false, ErrIdempotencyConflict
	}
	threads, err := s.threadsWhere(ctx, q, workspaceMemberID, "t.place_id = $3", threadID)
	if err != nil {
		return Thread{}, false, err
	}
	if len(threads) != 1 {
		return Thread{}, false, fmt.Errorf("thread creation receipt %q has no thread", clientNonce)
	}
	return threads[0], true, nil
}

func joinThread(ctx context.Context, tx pgx.Tx, placeID string, membership workspacecontrol.Membership) error {
	return admitPlaceTenure(ctx, tx, placeID, membership, 1)
}

// ThreadsIn lists the threads under one channel. Like every other thread
// projection it reads at REPEATABLE READ: counts, the latest message, and the
// participant list are three statements, and at READ COMMITTED a commit
// between them would produce a summary that existed at no single moment.
func (s *ScopedStore) ThreadsIn(ctx context.Context, parentPlaceID string) ([]Thread, error) {
	tx, err := s.Store.beginOpenSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	membership, err := s.authorizeSnapshotInTx(ctx, tx)
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

// ThreadsFor lists the threads this viewer participates in, from one snapshot.
func (s *ScopedStore) ThreadsFor(ctx context.Context) ([]Thread, error) {
	tx, err := s.Store.beginOpenSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	membership, err := s.authorizeSnapshotInTx(ctx, tx)
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

// ThreadFor projects one thread from one snapshot.
func (s *ScopedStore) ThreadFor(ctx context.Context, threadID string) (Thread, error) {
	tx, err := s.Store.beginOpenSnapshot(ctx)
	if err != nil {
		return Thread{}, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	membership, err := s.authorizeSnapshotInTx(ctx, tx)
	if err != nil {
		return Thread{}, err
	}
	place, err := s.loadScopedPlace(ctx, tx, threadID)
	// A store failure is not an answer about existence. Reporting it as
	// not-found would tell the caller that a thread it can see is gone, and a
	// client that believes that stops asking for it.
	if err != nil {
		return Thread{}, err
	}
	if place.Kind != PlaceThread {
		return Thread{}, ErrPlaceNotFound
	}
	if _, err := s.placeAccessAfterAuthorization(ctx, tx, place, s.Scope.Actor); err != nil {
		return Thread{}, err
	}
	thread, err := s.threadForAuthorizedPlace(ctx, tx, membership.WorkspaceMemberID, place)
	if err != nil {
		return Thread{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Thread{}, err
	}
	return thread, nil
}

// threadForAuthorizedPlace projects one thread through the caller's existing
// authorization and database snapshot. OpenSnapshot uses this helper so its
// top-level place/history and nested thread aggregate cannot observe different
// commits.
func (s *ScopedStore) threadForAuthorizedPlace(ctx context.Context, q querier, workspaceMemberID string, place Place) (Thread, error) {
	if place.Kind != PlaceThread {
		return Thread{}, ErrPlaceNotFound
	}
	threads, err := s.threadsWhere(ctx, q, workspaceMemberID, "t.place_id = $3", place.PlaceID)
	if err != nil {
		return Thread{}, err
	}
	if len(threads) == 0 {
		return Thread{}, ErrPlaceNotFound
	}
	return threads[0], nil
}

// threadsWhere projects thread summaries from one statement, so the counts,
// the latest message, and the participant list of a thread are always the same
// moment. Reading the participants separately produced summaries that never
// existed: a message committed between the two reads showed up in the count
// with its author still missing from the participants.
//
// conditions use $1=workspace and $2=viewer Workspace tenure; extra args start at $3.
func (s *ScopedStore) threadsWhere(ctx context.Context, q querier, workspaceMemberID, condition string, args ...any) ([]Thread, error) {
	queryArgs := []any{s.Scope.WorkspaceID, workspaceMemberID}
	queryArgs = append(queryArgs, args...)
	rows, err := q.Query(ctx, fmt.Sprintf(`
		SELECT t.place_id, t.workspace_id, t.name, t.topic, t.visibility, t.last_seq,
		       t.parent_place_id, t.parent_message_id,
		       (SELECT count(*) FROM messages m WHERE m.workspace_id=$1 AND m.place_id=t.place_id AND m.deleted_at IS NULL),
		       (SELECT m.created_at FROM messages m WHERE m.workspace_id=$1 AND m.place_id=t.place_id AND m.deleted_at IS NULL ORDER BY m.seq DESC LIMIT 1),
		       (SELECT m.content FROM messages m WHERE m.workspace_id=$1 AND m.place_id=t.place_id AND m.deleted_at IS NULL ORDER BY m.seq DESC LIMIT 1),
		       ARRAY(SELECT pm.member_kind FROM place_members pm
		             JOIN workspace_members wm ON wm.workspace_id=pm.workspace_id
		               AND wm.workspace_member_id=pm.workspace_member_id AND wm.left_at IS NULL
		             WHERE pm.workspace_id=$1 AND pm.place_id=t.place_id AND pm.left_at IS NULL
		             ORDER BY pm.joined_at, pm.place_member_id),
		       ARRAY(SELECT pm.member_id FROM place_members pm
		             JOIN workspace_members wm ON wm.workspace_id=pm.workspace_id
		               AND wm.workspace_member_id=pm.workspace_member_id AND wm.left_at IS NULL
		             WHERE pm.workspace_id=$1 AND pm.place_id=t.place_id AND pm.left_at IS NULL
		             ORDER BY pm.joined_at, pm.place_member_id)
		FROM places t WHERE t.workspace_id=$1 AND $2::text IS NOT NULL AND t.kind='thread' AND (%s)
		ORDER BY COALESCE((SELECT max(m.created_at) FROM messages m WHERE m.place_id=t.place_id), t.created_at) DESC, t.place_id DESC`, condition), queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("query threads: %w", err)
	}
	defer rows.Close()
	var out []Thread
	for rows.Next() {
		var t Thread
		var name string
		var origin, preview *string
		var kinds, ids []string
		if err := rows.Scan(&t.Place.PlaceID, &t.Place.WorkspaceID, &name, &t.Place.Topic,
			&t.Place.Visibility, &t.Place.LastSeq, &t.ParentPlaceID, &origin,
			&t.MessageCount, &t.LastMessageAt, &preview, &kinds, &ids); err != nil {
			return nil, fmt.Errorf("scan thread: %w", err)
		}
		if len(kinds) != len(ids) {
			return nil, fmt.Errorf("thread %q participant projection is inconsistent", t.Place.PlaceID)
		}
		t.Place.Kind, t.Place.Name = PlaceThread, name
		if origin != nil {
			t.ParentMessageID = *origin
		}
		if preview != nil {
			t.LastMessagePreview = truncateRunes(*preview, ThreadPreviewChars)
		}
		for i := range kinds {
			t.Participants = append(t.Participants, ParticipantRef{
				Kind: ParticipantKind(kinds[i]), ID: ids[i],
			})
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
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
