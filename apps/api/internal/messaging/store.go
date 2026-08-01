package messaging

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Workspace roles (契約ドラフト: 最小構成。人間にもagentにも同じ形で付く).
const (
	RoleOwner  = "owner"
	RoleAdmin  = "admin"
	RoleMember = "member"
)

// Place kinds.
const (
	PlaceChannel = "channel"
	PlaceDM      = "dm"
	PlaceGroupDM = "group_dm"
)

// Sentinel errors. The transport layer maps these to status codes; the store
// never reveals whether a place exists to a caller who cannot see it
// (ErrPlaceNotFound doubles as the authorization failure for reads).
var (
	ErrWorkspaceNotFound   = errors.New("workspace not found")
	ErrPlaceNotFound       = errors.New("place not found")
	ErrParticipantNotFound = errors.New("participant not found in the 戸籍")
	ErrNotAMember          = errors.New("participant is not an active member of the place")
	ErrNotReachable        = errors.New("participants share no active workspace membership")
	ErrMessageNotFound     = errors.New("message not found")
	ErrNotAuthor           = errors.New("only the author may do this")
	ErrForbidden           = errors.New("participant lacks the required role")
	ErrMessageDeleted      = errors.New("message is deleted")
	ErrSeqBeyondLatest     = errors.New("seq is beyond the place's latest seq")
)

// Store persists the messaging surface. All authorization decisions the
// contract assigns to the service — membership, roles, reachability — are made
// here so REST, WS, and the agent tool path cannot diverge (凍結契約 v1 §4:
// 人間がUIから行うのと同じ経路・同じ権限モデル).
type Store struct {
	pool *pgxpool.Pool
}

// New returns a Store backed by the given pool. The pool must be connected to
// a database with migrations applied (0002 for the 戸籍, 0005 for messaging).
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Workspace is the Discord-shaped server: channels live directly under it.
type Workspace struct {
	WorkspaceID string
	Name        string
}

// Place is where messages flow. WorkspaceID and Name are empty for dm and
// group_dm places.
type Place struct {
	PlaceID     string
	Kind        string
	WorkspaceID string
	Name        string
	Topic       string
	Visibility  string
	LastSeq     int64
}

// MemberProfile is a participant with their scope-resolved display name.
// IDs are never used as display names (ADR 0008 §1).
type MemberProfile struct {
	Participant ParticipantRef
	DisplayName string
	Role        string // workspace role; empty for dm/group_dm members
}

// CreateWorkspace mints a workspace and enrolls the creator as owner.
func (s *Store) CreateWorkspace(ctx context.Context, name string, creator ParticipantRef) (Workspace, error) {
	if err := creator.Validate(); err != nil {
		return Workspace{}, err
	}
	if err := s.participantExists(ctx, creator); err != nil {
		return Workspace{}, err
	}
	workspaceID := newUUIDv7()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Workspace{}, fmt.Errorf("begin create workspace: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx,
		"INSERT INTO workspaces (workspace_id, name) VALUES ($1, $2)",
		workspaceID, name); err != nil {
		return Workspace{}, fmt.Errorf("insert workspace: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO workspace_members (workspace_id, member_kind, member_id, role)
		 VALUES ($1, $2, $3, $4)`,
		workspaceID, creator.Kind, creator.ID, RoleOwner); err != nil {
		return Workspace{}, fmt.Errorf("insert workspace owner: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Workspace{}, fmt.Errorf("commit create workspace: %w", err)
	}
	return Workspace{WorkspaceID: workspaceID, Name: name}, nil
}

// AddWorkspaceMember enrolls a participant. Idempotent: adding an already
// active member leaves their existing row (and role) untouched. Humans and
// PersonalityAgents enroll through the identical path.
func (s *Store) AddWorkspaceMember(ctx context.Context, workspaceID string, member ParticipantRef, role string) error {
	if err := member.Validate(); err != nil {
		return err
	}
	switch role {
	case RoleOwner, RoleAdmin, RoleMember:
	default:
		return fmt.Errorf("unknown workspace role %q", role)
	}
	if err := s.workspaceExists(ctx, workspaceID); err != nil {
		return err
	}
	if err := s.participantExists(ctx, member); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx,
		`INSERT INTO workspace_members (workspace_id, member_kind, member_id, role)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (workspace_id, member_kind, member_id) WHERE left_at IS NULL
		 DO NOTHING`,
		workspaceID, member.Kind, member.ID, role)
	if err != nil {
		return fmt.Errorf("add workspace member: %w", err)
	}
	return nil
}

// RemoveWorkspaceMember closes the active membership row (left_at) so
// authorship history stays explainable. Removing a non-member is a no-op.
func (s *Store) RemoveWorkspaceMember(ctx context.Context, workspaceID string, member ParticipantRef) error {
	if err := member.Validate(); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx,
		`UPDATE workspace_members SET left_at = now()
		 WHERE workspace_id = $1 AND member_kind = $2 AND member_id = $3 AND left_at IS NULL`,
		workspaceID, member.Kind, member.ID)
	if err != nil {
		return fmt.Errorf("remove workspace member: %w", err)
	}
	return nil
}

// CreateChannel creates a public channel in the workspace. v0: any active
// member may create channels (契約ドラフト: 権限は最小構成).
func (s *Store) CreateChannel(ctx context.Context, workspaceID, name, topic string, creator ParticipantRef) (Place, error) {
	if err := creator.Validate(); err != nil {
		return Place{}, err
	}
	active, _, err := s.workspaceMembership(ctx, s.pool, workspaceID, creator)
	if err != nil {
		return Place{}, err
	}
	if !active {
		return Place{}, ErrNotAMember
	}
	placeID := newUUIDv7()
	_, err = s.pool.Exec(ctx,
		`INSERT INTO places (place_id, kind, workspace_id, name, topic)
		 VALUES ($1, 'channel', $2, $3, $4)`,
		placeID, workspaceID, name, topic)
	if err != nil {
		return Place{}, fmt.Errorf("insert channel: %w", err)
	}
	return Place{
		PlaceID: placeID, Kind: PlaceChannel, WorkspaceID: workspaceID,
		Name: name, Topic: topic, Visibility: "public",
	}, nil
}

// EnsureDM returns the one dm place between two participants, creating it on
// first use. Reachability (契約ドラフト: 到達できれば誰とでも会話できる) v0
// requires an active shared workspace membership; the accepted-Connection
// basis lands with the Connection domain (Codex合意 4) and widens this check
// without changing callers.
func (s *Store) EnsureDM(ctx context.Context, a, b ParticipantRef) (Place, error) {
	for _, p := range []ParticipantRef{a, b} {
		if err := p.Validate(); err != nil {
			return Place{}, err
		}
	}
	if a.Key() == b.Key() {
		return Place{}, fmt.Errorf("a dm needs two distinct participants")
	}
	reachable, err := s.shareActiveWorkspace(ctx, a, b)
	if err != nil {
		return Place{}, err
	}
	if !reachable {
		return Place{}, ErrNotReachable
	}
	dmKey := dmPairKey(a, b)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Place{}, fmt.Errorf("begin ensure dm: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	placeID := newUUIDv7()
	var inserted string
	err = tx.QueryRow(ctx,
		`INSERT INTO places (place_id, kind, dm_key) VALUES ($1, 'dm', $2)
		 ON CONFLICT (dm_key) DO NOTHING
		 RETURNING place_id`,
		placeID, dmKey).Scan(&inserted)
	if errors.Is(err, pgx.ErrNoRows) {
		// The pair already has a dm; return it.
		var existing Place
		err = tx.QueryRow(ctx,
			"SELECT place_id, last_seq FROM places WHERE dm_key = $1",
			dmKey).Scan(&existing.PlaceID, &existing.LastSeq)
		if err != nil {
			return Place{}, fmt.Errorf("load existing dm: %w", err)
		}
		existing.Kind = PlaceDM
		existing.Visibility = "public"
		if err := tx.Commit(ctx); err != nil {
			return Place{}, fmt.Errorf("commit ensure dm: %w", err)
		}
		return existing, nil
	}
	if err != nil {
		return Place{}, fmt.Errorf("insert dm place: %w", err)
	}
	for _, p := range []ParticipantRef{a, b} {
		if _, err := tx.Exec(ctx,
			"INSERT INTO place_members (place_id, member_kind, member_id) VALUES ($1, $2, $3)",
			placeID, p.Kind, p.ID); err != nil {
			return Place{}, fmt.Errorf("insert dm member: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return Place{}, fmt.Errorf("commit ensure dm: %w", err)
	}
	return Place{PlaceID: placeID, Kind: PlaceDM, Visibility: "public"}, nil
}

// CreateGroupDM creates a group dm with the creator and at least two others.
// The creator must be able to reach every invitee (same v0 basis as EnsureDM).
func (s *Store) CreateGroupDM(ctx context.Context, creator ParticipantRef, others []ParticipantRef) (Place, error) {
	if err := creator.Validate(); err != nil {
		return Place{}, err
	}
	seen := map[string]bool{creator.Key(): true}
	members := []ParticipantRef{creator}
	for _, p := range others {
		if err := p.Validate(); err != nil {
			return Place{}, err
		}
		if seen[p.Key()] {
			continue
		}
		seen[p.Key()] = true
		members = append(members, p)
	}
	if len(members) < 3 {
		return Place{}, fmt.Errorf("a group dm needs at least three distinct participants")
	}
	for _, p := range members[1:] {
		reachable, err := s.shareActiveWorkspace(ctx, creator, p)
		if err != nil {
			return Place{}, err
		}
		if !reachable {
			return Place{}, ErrNotReachable
		}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Place{}, fmt.Errorf("begin create group dm: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	placeID := newUUIDv7()
	if _, err := tx.Exec(ctx,
		"INSERT INTO places (place_id, kind) VALUES ($1, 'group_dm')", placeID); err != nil {
		return Place{}, fmt.Errorf("insert group dm place: %w", err)
	}
	for _, p := range members {
		if _, err := tx.Exec(ctx,
			"INSERT INTO place_members (place_id, member_kind, member_id) VALUES ($1, $2, $3)",
			placeID, p.Kind, p.ID); err != nil {
			return Place{}, fmt.Errorf("insert group dm member: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return Place{}, fmt.Errorf("commit create group dm: %w", err)
	}
	return Place{PlaceID: placeID, Kind: PlaceGroupDM, Visibility: "public"}, nil
}

// PlaceFor loads a place the viewer can see. A place the viewer cannot read is
// reported as ErrPlaceNotFound — existence is not revealed across the
// membership boundary.
func (s *Store) PlaceFor(ctx context.Context, placeID string, viewer ParticipantRef) (Place, error) {
	if err := viewer.Validate(); err != nil {
		return Place{}, err
	}
	place, err := s.loadPlace(ctx, s.pool, placeID)
	if err != nil {
		return Place{}, err
	}
	canRead, err := s.canAccess(ctx, s.pool, place, viewer)
	if err != nil {
		return Place{}, err
	}
	if !canRead {
		return Place{}, ErrPlaceNotFound
	}
	return place, nil
}

// ActiveMembers returns the active members of a place with display names
// resolved from the 戸籍 (workspace nicknames land later). The viewer must be
// able to see the place.
func (s *Store) ActiveMembers(ctx context.Context, placeID string, viewer ParticipantRef) ([]MemberProfile, error) {
	place, err := s.PlaceFor(ctx, placeID, viewer)
	if err != nil {
		return nil, err
	}
	return s.activeMembers(ctx, s.pool, place)
}

// --- internals ---

// querier lets the same helpers run on the pool or inside a transaction.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func (s *Store) workspaceExists(ctx context.Context, workspaceID string) error {
	var exists bool
	err := s.pool.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM workspaces WHERE workspace_id = $1)", workspaceID).Scan(&exists)
	if err != nil {
		return fmt.Errorf("check workspace exists: %w", err)
	}
	if !exists {
		return ErrWorkspaceNotFound
	}
	return nil
}

// participantExists checks the 戸籍 for the referenced Human or agent so
// membership rows can never point at nobody.
func (s *Store) participantExists(ctx context.Context, p ParticipantRef) error {
	var query string
	switch p.Kind {
	case KindHuman:
		query = "SELECT EXISTS (SELECT 1 FROM humans WHERE human_id = $1)"
	case KindPersonalityAgent:
		query = "SELECT EXISTS (SELECT 1 FROM agents WHERE personality_agent_id = $1)"
	default:
		return fmt.Errorf("unknown participant kind %q", p.Kind)
	}
	var exists bool
	if err := s.pool.QueryRow(ctx, query, p.ID).Scan(&exists); err != nil {
		return fmt.Errorf("check participant exists: %w", err)
	}
	if !exists {
		return ErrParticipantNotFound
	}
	return nil
}

// workspaceMembership reports whether the participant is an active member and,
// if so, their role.
func (s *Store) workspaceMembership(ctx context.Context, q querier, workspaceID string, p ParticipantRef) (bool, string, error) {
	var role string
	err := q.QueryRow(ctx,
		`SELECT role FROM workspace_members
		 WHERE workspace_id = $1 AND member_kind = $2 AND member_id = $3 AND left_at IS NULL`,
		workspaceID, p.Kind, p.ID).Scan(&role)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := s.workspaceExists(ctx, workspaceID); err != nil {
			return false, "", err
		}
		return false, "", nil
	}
	if err != nil {
		return false, "", fmt.Errorf("check workspace membership: %w", err)
	}
	return true, role, nil
}

func (s *Store) placeMembership(ctx context.Context, q querier, placeID string, p ParticipantRef) (bool, error) {
	var exists bool
	err := q.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM place_members
		  WHERE place_id = $1 AND member_kind = $2 AND member_id = $3 AND left_at IS NULL)`,
		placeID, p.Kind, p.ID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check place membership: %w", err)
	}
	return exists, nil
}

// canAccess implements the v0 permission rule: channels are readable and
// postable by every active workspace member; dm/group_dm by their active place
// members. Reading and posting are the same capability in v0.
func (s *Store) canAccess(ctx context.Context, q querier, place Place, p ParticipantRef) (bool, error) {
	if place.Kind == PlaceChannel {
		active, _, err := s.workspaceMembership(ctx, q, place.WorkspaceID, p)
		return active, err
	}
	return s.placeMembership(ctx, q, place.PlaceID, p)
}

func (s *Store) loadPlace(ctx context.Context, q querier, placeID string) (Place, error) {
	var (
		place       Place
		workspaceID *string
		name        *string
	)
	err := q.QueryRow(ctx,
		`SELECT place_id, kind, workspace_id, name, topic, visibility, last_seq
		 FROM places WHERE place_id = $1`, placeID).
		Scan(&place.PlaceID, &place.Kind, &workspaceID, &name,
			&place.Topic, &place.Visibility, &place.LastSeq)
	if errors.Is(err, pgx.ErrNoRows) {
		return Place{}, ErrPlaceNotFound
	}
	if err != nil {
		return Place{}, fmt.Errorf("load place: %w", err)
	}
	if workspaceID != nil {
		place.WorkspaceID = *workspaceID
	}
	if name != nil {
		place.Name = *name
	}
	return place, nil
}

// shareActiveWorkspace is the v0 reachability basis for dm creation.
func (s *Store) shareActiveWorkspace(ctx context.Context, a, b ParticipantRef) (bool, error) {
	var shared bool
	err := s.pool.QueryRow(ctx,
		`SELECT EXISTS (
		   SELECT 1 FROM workspace_members wa
		   JOIN workspace_members wb ON wa.workspace_id = wb.workspace_id
		   WHERE wa.member_kind = $1 AND wa.member_id = $2 AND wa.left_at IS NULL
		     AND wb.member_kind = $3 AND wb.member_id = $4 AND wb.left_at IS NULL)`,
		a.Kind, a.ID, b.Kind, b.ID).Scan(&shared)
	if err != nil {
		return false, fmt.Errorf("check shared workspace: %w", err)
	}
	return shared, nil
}

// activeMembers lists a place's active members with 戸籍 display names.
func (s *Store) activeMembers(ctx context.Context, q querier, place Place) ([]MemberProfile, error) {
	var (
		rows pgx.Rows
		err  error
	)
	if place.Kind == PlaceChannel {
		rows, err = q.Query(ctx,
			`SELECT wm.member_kind, wm.member_id, wm.role,
			        COALESCE(h.display_name, a.display_name, '') AS display_name
			 FROM workspace_members wm
			 LEFT JOIN humans h ON wm.member_kind = 'human' AND h.human_id = wm.member_id
			 LEFT JOIN agents a ON wm.member_kind = 'personality_agent' AND a.personality_agent_id = wm.member_id
			 WHERE wm.workspace_id = $1 AND wm.left_at IS NULL
			 ORDER BY wm.workspace_member_id`, place.WorkspaceID)
	} else {
		rows, err = q.Query(ctx,
			`SELECT pm.member_kind, pm.member_id, '' AS role,
			        COALESCE(h.display_name, a.display_name, '') AS display_name
			 FROM place_members pm
			 LEFT JOIN humans h ON pm.member_kind = 'human' AND h.human_id = pm.member_id
			 LEFT JOIN agents a ON pm.member_kind = 'personality_agent' AND a.personality_agent_id = pm.member_id
			 WHERE pm.place_id = $1 AND pm.left_at IS NULL
			 ORDER BY pm.place_member_id`, place.PlaceID)
	}
	if err != nil {
		return nil, fmt.Errorf("query active members: %w", err)
	}
	defer rows.Close()
	var members []MemberProfile
	for rows.Next() {
		var m MemberProfile
		var kind string
		if err := rows.Scan(&kind, &m.Participant.ID, &m.Role, &m.DisplayName); err != nil {
			return nil, fmt.Errorf("scan member: %w", err)
		}
		m.Participant.Kind = ParticipantKind(kind)
		members = append(members, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate members: %w", err)
	}
	return members, nil
}

// dmPairKey builds the canonical sorted participant key for a dm pair, the
// database-level guarantee that one pair has exactly one dm.
func dmPairKey(a, b ParticipantRef) string {
	keys := []string{a.Key(), b.Key()}
	sort.Strings(keys)
	return strings.Join(keys, "|")
}

// newUUIDv7 returns a canonical lowercase hyphenated UUIDv7 string.
func newUUIDv7() string {
	id, err := uuid.NewV7()
	if err != nil {
		// uuid.NewV7 only fails when the crypto/rand source fails, which is a
		// fatal process condition. Panic so the caller surfaces it immediately.
		panic(fmt.Sprintf("generate uuidv7: %v", err))
	}
	return id.String()
}
