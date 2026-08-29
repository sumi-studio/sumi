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

// ThreadExistsError keeps the existing thread on a conflict so transport
// callers can navigate to the resource that won the one-thread-per-message
// race. It still unwraps to ErrThreadExists for store callers.
type ThreadExistsError struct{ Thread Thread }

func (e *ThreadExistsError) Error() string { return ErrThreadExists.Error() }
func (e *ThreadExistsError) Unwrap() error { return ErrThreadExists }

func (s *ScopedStore) CreateThread(ctx context.Context, parentPlaceID, name, originMessageID, clientNonce string) (Thread, bool, error) {
	name = strings.TrimSpace(name)
	if !threadNameValid(name) {
		return Thread{}, false, fmt.Errorf("thread name must be 1..%d characters", MaxThreadNameChars)
	}
	if !clientNonceValid(clientNonce) {
		return Thread{}, false, errors.New("client nonce must be 1..128 bytes")
	}
	digest := placeCreationDigest(placeCreationThread, struct {
		ParentPlaceID   string `json:"parent_place_id"`
		Name            string `json:"name"`
		OriginMessageID string `json:"origin_message_id"`
	}{parentPlaceID, name, originMessageID})
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
	if replayed, found, err := s.replayPlaceCreation(ctx, tx, placeCreationThread,
		clientNonce, digest, membership.WorkspaceMemberID); err != nil {
		return Thread{}, false, err
	} else if found {
		threads, err := s.threadsWhere(ctx, tx, membership.WorkspaceMemberID,
			"t.place_id = $3", replayed.PlaceID)
		if err != nil {
			return Thread{}, false, err
		}
		if len(threads) != 1 {
			return Thread{}, false, fmt.Errorf("thread creation receipt %q has no thread", clientNonce)
		}
		if err := tx.Commit(ctx); err != nil {
			return Thread{}, false, fmt.Errorf("commit replayed thread creation: %w", err)
		}
		return threads[0], false, nil
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
		// The origin row lock above serializes all normal creators for this
		// message. Check after taking it, so a retried request with a new nonce
		// can be told which durable thread already owns this origin.
		existing, err := s.threadsWhere(ctx, tx, membership.WorkspaceMemberID,
			"t.parent_place_id = $3 AND t.parent_message_id = $4", parentPlaceID, originMessageID)
		if err != nil {
			return Thread{}, false, err
		}
		if len(existing) > 1 {
			return Thread{}, false, fmt.Errorf("message %q has multiple threads", originMessageID)
		}
		if len(existing) == 1 {
			if err := tx.Commit(ctx); err != nil {
				return Thread{}, false, fmt.Errorf("commit existing thread lookup: %w", err)
			}
			return existing[0], false, &ThreadExistsError{Thread: existing[0]}
		}
	}
	thread := Thread{
		Place: Place{PlaceID: newUUIDv7(), Kind: PlaceThread, WorkspaceID: s.Scope.WorkspaceID,
			Name: name, Visibility: parent.Visibility},
		ParentPlaceID: parentPlaceID, ParentMessageID: originMessageID,
		Participants: []ParticipantRef{s.Scope.Actor},
	}
	err = tx.QueryRow(ctx, `
		INSERT INTO places (place_id, kind, workspace_id, name, parent_place_id, parent_message_id)
		VALUES ($1, 'thread', $2, $3, $4, $5)
		RETURNING revision`,
		thread.Place.PlaceID, s.Scope.WorkspaceID, name, parentPlaceID, origin).Scan(&thread.Place.Revision)
	if err != nil {
		if isUniqueViolation(err) {
			return Thread{}, false, ErrThreadExists
		}
		return Thread{}, false, fmt.Errorf("insert thread: %w", err)
	}
	if err := joinThread(ctx, tx, thread.Place.PlaceID, membership); err != nil {
		return Thread{}, false, err
	}
	if err := s.recordPlaceCreation(ctx, tx, placeCreationThread, clientNonce, digest,
		thread.Place.PlaceID, membership.WorkspaceMemberID); err != nil {
		return Thread{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Thread{}, false, fmt.Errorf("commit create thread: %w", err)
	}
	return thread, true, nil
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
		SELECT t.place_id, t.workspace_id, t.revision, t.name, t.topic, t.visibility, t.last_seq,
		       t.parent_place_id, t.parent_message_id,
		       (SELECT count(*) FROM messages m WHERE m.workspace_id=$1 AND m.place_id=t.place_id AND m.deleted_at IS NULL),
		       (SELECT m.created_at FROM messages m WHERE m.workspace_id=$1 AND m.place_id=t.place_id AND m.deleted_at IS NULL ORDER BY m.seq DESC LIMIT 1),
		       (SELECT COALESCE(NULLIF(m.content, ''), mp.question)
		        FROM messages m
		        LEFT JOIN message_polls mp ON mp.message_id=m.message_id
		        WHERE m.workspace_id=$1 AND m.place_id=t.place_id AND m.deleted_at IS NULL
		        ORDER BY m.seq DESC LIMIT 1),
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
		if err := rows.Scan(&t.Place.PlaceID, &t.Place.WorkspaceID, &t.Place.Revision, &name, &t.Place.Topic,
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
