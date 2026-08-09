package messaging

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
)

// MaxThreadNameChars bounds a thread's name. It is a heading for a side
// conversation, not a document title.
const MaxThreadNameChars = 100

// ThreadPreviewChars bounds the latest-message excerpt carried by a thread
// summary. The full text stays in the place; the list only needs a glance.
const ThreadPreviewChars = 120

// A place_members row records that someone joined a thread, but it does not
// grant access: the parent workspace's active membership remains the authority
// boundary. Keep that join in one fragment so every projection agrees.
const activeThreadWorkspaceMemberJoinSQL = `
		 JOIN workspace_members twm
		   ON twm.workspace_id = t.workspace_id
		  AND twm.member_kind = pm.member_kind AND twm.member_id = pm.member_id
		  AND twm.left_at IS NULL`

const activeJoinedThreadMemberExistsSQL = `EXISTS (
		 SELECT 1 FROM place_members pm` + activeThreadWorkspaceMemberJoinSQL + `
		 WHERE pm.place_id = t.place_id AND pm.left_at IS NULL
		   AND pm.member_kind = $1 AND pm.member_id = $2)`

// participantVisiblePlacesCTE is the shared global-list visibility basis for
// unread, search, and reply-later. Channels inherit active workspace
// membership; DMs inherit active place membership; threads require both an
// active joined row and active membership in their parent workspace.
const participantVisiblePlacesCTE = `WITH my_places AS (
		 SELECT p.* FROM places p
		 JOIN workspace_members wm ON wm.workspace_id = p.workspace_id
		  AND wm.member_kind = $1 AND wm.member_id = $2 AND wm.left_at IS NULL
		 WHERE p.kind = 'channel'
		 UNION
		 SELECT p.* FROM places p
		 JOIN place_members pm ON pm.place_id = p.place_id
		  AND pm.member_kind = $1 AND pm.member_id = $2 AND pm.left_at IS NULL
		 WHERE p.kind IN ('dm', 'group_dm')
		 UNION
		 SELECT t.* FROM places t
		 JOIN place_members pm ON pm.place_id = t.place_id
		  AND pm.member_kind = $1 AND pm.member_id = $2 AND pm.left_at IS NULL` +
	activeThreadWorkspaceMemberJoinSQL + `
		 WHERE t.kind = 'thread'
		)`

var (
	// ErrNotThreadable is returned when a thread is asked for somewhere a side
	// conversation has no parent to belong to (v0: channels only).
	ErrNotThreadable = errors.New("threads can only be created inside a channel")
	// ErrThreadExists is returned when a message already has its thread. One
	// message grows at most one side conversation, so the reply-count chip on
	// the origin message always points at a single place.
	ErrThreadExists = errors.New("this message already has a thread")
)

// Thread is a thread place plus what a list of threads needs to render: how
// much was said, when, and by whom. The Place itself is an ordinary place —
// seq, idempotent send, tombstones, read markers and notifications all work
// through the existing machinery.
type Thread struct {
	Place Place
	// ParentPlaceID is the channel the thread hangs under.
	ParentPlaceID string
	// ParentMessageID is the message the thread grew from; empty when the
	// thread was started from scratch rather than from something said.
	ParentMessageID    string
	MessageCount       int64
	LastMessageAt      *time.Time
	LastMessagePreview string
	// Participants are the people who joined by writing (plus the creator) —
	// the ones unread counts and notifications are about.
	Participants []ParticipantRef
}

// CreateThread opens a side conversation under a channel. Any participant who
// can see the parent may start one, matching the v0 rule that reading and
// posting are the same capability. When originMessageID is non-empty the
// thread is anchored to that message, and the origin must live in the parent.
func (s *Store) CreateThread(ctx context.Context, parentPlaceID, name, originMessageID string, creator ParticipantRef) (Thread, error) {
	if err := creator.Validate(); err != nil {
		return Thread{}, err
	}
	name = strings.TrimSpace(name)
	if name == "" || utf8.RuneCountInString(name) > MaxThreadNameChars {
		return Thread{}, fmt.Errorf("thread name must be 1..%d characters", MaxThreadNameChars)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Thread{}, fmt.Errorf("begin create thread: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	parent, err := s.loadPlace(ctx, tx, parentPlaceID)
	if err != nil {
		return Thread{}, err
	}
	visible, err := s.canAccess(ctx, tx, parent, creator)
	if err != nil {
		return Thread{}, err
	}
	if !visible {
		return Thread{}, ErrPlaceNotFound
	}
	if parent.Kind != PlaceChannel {
		return Thread{}, ErrNotThreadable
	}
	var origin *string
	if originMessageID != "" {
		var inParent bool
		if err := tx.QueryRow(ctx,
			"SELECT EXISTS (SELECT 1 FROM messages WHERE message_id = $1 AND place_id = $2 AND deleted_at IS NULL)",
			originMessageID, parentPlaceID).Scan(&inParent); err != nil {
			return Thread{}, fmt.Errorf("check thread origin: %w", err)
		}
		if !inParent {
			return Thread{}, ErrMessageNotFound
		}
		var taken bool
		if err := tx.QueryRow(ctx,
			"SELECT EXISTS (SELECT 1 FROM places WHERE parent_message_id = $1)",
			originMessageID).Scan(&taken); err != nil {
			return Thread{}, fmt.Errorf("check existing thread: %w", err)
		}
		if taken {
			return Thread{}, ErrThreadExists
		}
		origin = &originMessageID
	}
	placeID := newUUIDv7()
	if _, err := tx.Exec(ctx,
		`INSERT INTO places (place_id, kind, workspace_id, name, parent_place_id, parent_message_id)
		 VALUES ($1, 'thread', $2, $3, $4, $5)`,
		placeID, parent.WorkspaceID, name, parentPlaceID, origin); err != nil {
		if isUniqueViolation(err) {
			return Thread{}, ErrThreadExists
		}
		return Thread{}, fmt.Errorf("insert thread place: %w", err)
	}
	// The person who opened the thread is in it from the start, exactly as if
	// they had written the first line.
	if err := joinThread(ctx, tx, placeID, creator); err != nil {
		return Thread{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Thread{}, fmt.Errorf("commit create thread: %w", err)
	}
	return Thread{
		Place: Place{
			PlaceID: placeID, Kind: PlaceThread, WorkspaceID: parent.WorkspaceID,
			Name: name, Visibility: parent.Visibility,
		},
		ParentPlaceID:   parentPlaceID,
		ParentMessageID: originMessageID,
		Participants:    []ParticipantRef{creator},
	}, nil
}

// joinThread records that a participant is now in the thread. Idempotent: a
// second message from the same person does not re-join them.
func joinThread(ctx context.Context, q querier, placeID string, participant ParticipantRef) error {
	if _, err := q.Exec(ctx,
		`INSERT INTO place_members (place_id, member_kind, member_id)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (place_id, member_kind, member_id) WHERE left_at IS NULL
		 DO NOTHING`,
		placeID, participant.Kind, participant.ID); err != nil {
		return fmt.Errorf("join thread: %w", err)
	}
	return nil
}

// ThreadsIn lists the threads under a place the viewer can see, newest activity
// first. Every member of the parent sees every thread: a side conversation is
// not a private room, it is a place to put a tangent so the channel stays
// readable.
func (s *Store) ThreadsIn(ctx context.Context, parentPlaceID string, viewer ParticipantRef) ([]Thread, error) {
	parent, err := s.PlaceFor(ctx, parentPlaceID, viewer)
	if err != nil {
		return nil, err
	}
	if parent.Kind != PlaceChannel {
		return nil, ErrNotThreadable
	}
	return s.threadsWhere(ctx, "t.parent_place_id = $1", parentPlaceID)
}

// ThreadsFor lists the threads the viewer has joined, across every visible
// place. Bootstrap uses it so a thread with unread messages can be shown
// without opening its parent first.
func (s *Store) ThreadsFor(ctx context.Context, viewer ParticipantRef) ([]Thread, error) {
	if err := viewer.Validate(); err != nil {
		return nil, err
	}
	return s.threadsWhere(ctx, activeJoinedThreadMemberExistsSQL,
		viewer.Kind, viewer.ID)
}

// ThreadFor loads one thread the viewer can see, as a summary.
func (s *Store) ThreadFor(ctx context.Context, threadID string, viewer ParticipantRef) (Thread, error) {
	place, err := s.PlaceFor(ctx, threadID, viewer)
	if err != nil {
		return Thread{}, err
	}
	if place.Kind != PlaceThread {
		return Thread{}, ErrPlaceNotFound
	}
	threads, err := s.threadsWhere(ctx, "t.place_id = $1", threadID)
	if err != nil {
		return Thread{}, err
	}
	if len(threads) == 0 {
		return Thread{}, ErrPlaceNotFound
	}
	return threads[0], nil
}

// threadsWhere is the one projection every thread list goes through, so the
// summary shape (count, latest line, participants) cannot drift between the
// panel, bootstrap, and the agent's overview.
func (s *Store) threadsWhere(ctx context.Context, condition string, args ...any) ([]Thread, error) {
	rows, err := s.pool.Query(ctx, fmt.Sprintf(
		`SELECT t.place_id, t.workspace_id, t.name, t.topic, t.visibility, t.last_seq,
		        t.parent_place_id, t.parent_message_id,
		        (SELECT count(*) FROM messages m
		          WHERE m.place_id = t.place_id AND m.deleted_at IS NULL),
		        (SELECT m.created_at FROM messages m
		          WHERE m.place_id = t.place_id AND m.deleted_at IS NULL
		          ORDER BY m.seq DESC LIMIT 1),
		        (SELECT m.content FROM messages m
		          WHERE m.place_id = t.place_id AND m.deleted_at IS NULL
		          ORDER BY m.seq DESC LIMIT 1)
		 FROM places t
		 WHERE t.kind = 'thread' AND (%s)
		 ORDER BY t.created_at DESC, t.place_id DESC`, condition), args...)
	if err != nil {
		return nil, fmt.Errorf("query threads: %w", err)
	}
	var (
		threads []Thread
		ids     []string
	)
	for rows.Next() {
		var (
			thread  Thread
			origin  *string
			lastAt  *time.Time
			preview *string
		)
		if err := rows.Scan(&thread.Place.PlaceID, &thread.Place.WorkspaceID, &thread.Place.Name,
			&thread.Place.Topic, &thread.Place.Visibility, &thread.Place.LastSeq,
			&thread.ParentPlaceID, &origin,
			&thread.MessageCount, &lastAt, &preview); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan thread: %w", err)
		}
		thread.Place.Kind = PlaceThread
		if origin != nil {
			thread.ParentMessageID = *origin
		}
		thread.LastMessageAt = lastAt
		if preview != nil {
			thread.LastMessagePreview = truncateRunes(*preview, ThreadPreviewChars)
		}
		threads = append(threads, thread)
		ids = append(ids, thread.Place.PlaceID)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate threads: %w", err)
	}
	rows.Close()
	if len(threads) == 0 {
		return nil, nil
	}
	participants, err := s.threadParticipants(ctx, ids)
	if err != nil {
		return nil, err
	}
	for i := range threads {
		threads[i].Participants = participants[threads[i].Place.PlaceID]
	}
	return threads, nil
}

// threadParticipants loads the joined participants of many threads at once.
func (s *Store) threadParticipants(ctx context.Context, placeIDs []string) (map[string][]ParticipantRef, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT pm.place_id, pm.member_kind, pm.member_id
		 FROM place_members pm
		 JOIN places t ON t.place_id = pm.place_id AND t.kind = 'thread'`+
			activeThreadWorkspaceMemberJoinSQL+`
		 WHERE pm.place_id = ANY($1) AND pm.left_at IS NULL
		 ORDER BY pm.place_id, pm.place_member_id`, placeIDs)
	if err != nil {
		return nil, fmt.Errorf("query thread participants: %w", err)
	}
	defer rows.Close()
	out := map[string][]ParticipantRef{}
	for rows.Next() {
		var placeID, kind, id string
		if err := rows.Scan(&placeID, &kind, &id); err != nil {
			return nil, fmt.Errorf("scan thread participant: %w", err)
		}
		out[placeID] = append(out[placeID], ParticipantRef{Kind: ParticipantKind(kind), ID: id})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate thread participants: %w", err)
	}
	return out, nil
}

// threadMembers returns the joined participants of one thread inside a
// transaction. Notification candidates for a thread are these people (plus
// whoever the message names), not every member of the parent channel: joining
// a side conversation is what asks to be called about it.
func (s *Store) threadMembers(ctx context.Context, q querier, placeID string) ([]ParticipantRef, error) {
	rows, err := q.Query(ctx,
		`SELECT pm.member_kind, pm.member_id
		 FROM place_members pm
		 JOIN places t ON t.place_id = pm.place_id AND t.kind = 'thread'`+
			activeThreadWorkspaceMemberJoinSQL+`
		 WHERE pm.place_id = $1 AND pm.left_at IS NULL
		 ORDER BY pm.place_member_id`, placeID)
	if err != nil {
		return nil, fmt.Errorf("query thread members: %w", err)
	}
	defer rows.Close()
	var out []ParticipantRef
	for rows.Next() {
		var kind, id string
		if err := rows.Scan(&kind, &id); err != nil {
			return nil, fmt.Errorf("scan thread member: %w", err)
		}
		out = append(out, ParticipantRef{Kind: ParticipantKind(kind), ID: id})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate thread members: %w", err)
	}
	return out, nil
}

// parentOf reports the parent place of a thread; ok is false for other kinds.
func (s *Store) parentOf(ctx context.Context, q querier, placeID string) (Place, bool, error) {
	var parentID *string
	err := q.QueryRow(ctx, "SELECT parent_place_id FROM places WHERE place_id = $1", placeID).Scan(&parentID)
	if errors.Is(err, pgx.ErrNoRows) {
		return Place{}, false, ErrPlaceNotFound
	}
	if err != nil {
		return Place{}, false, fmt.Errorf("load thread parent: %w", err)
	}
	if parentID == nil {
		return Place{}, false, nil
	}
	parent, err := s.loadPlace(ctx, q, *parentID)
	if err != nil {
		return Place{}, false, err
	}
	return parent, true, nil
}

func truncateRunes(value string, max int) string {
	if utf8.RuneCountInString(value) <= max {
		return value
	}
	count := 0
	for index := range value {
		if count == max {
			return value[:index] + "…"
		}
		count++
	}
	return value
}
