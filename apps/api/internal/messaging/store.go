package messaging

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"

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

	// DefaultWorkspaceID and DefaultGeneralChannelID are stable product
	// identities for the shared MVP Workspace. They are intentionally not
	// tenant-derived: during the hackathon every authenticated Sumi Human and
	// their PersonalityAgent Secretary inhabit the same shared world.
	DefaultWorkspaceID      = "01900000-0000-7000-8000-000000000001"
	DefaultGeneralChannelID = "01900000-0000-7000-8000-000000000002"
)

// Place kinds. thread is a channel-scoped side conversation: an ordinary place
// so seq, idempotent send, tombstones, read markers and notifications all keep
// working without a second implementation (migration 0018).
const (
	PlaceChannel = "channel"
	PlaceDM      = "dm"
	PlaceGroupDM = "group_dm"
	PlaceThread  = "thread"
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
	ErrNotAChannel         = errors.New("place is not a channel")
	ErrInvalidChannelName  = errors.New("channel name must be 1..200 characters")
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
	// push, when attached (UsePush), carries the notification decision out to
	// a Human's registered browsers. Nil means「タブを閉じたら届かない」だけで、
	// 判定も配信も変わらない。
	push *PushDispatcher
}

// New returns a Store backed by the given pool. The pool must be connected to
// a database with migrations applied (0002 for the 戸籍, 0008 for messaging).
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// EnsureDefaultWorkspaceMembership idempotently admits a participant to the
// shared MVP Workspace. When a Human arrives, all of that Human's durable
// PersonalityAgents are admitted through the identical membership shape. The
// Human remains the caller; this does not act or speak on an agent's behalf.
func (s *Store) EnsureDefaultWorkspaceMembership(ctx context.Context, participant ParticipantRef) error {
	if err := participant.Validate(); err != nil {
		return err
	}
	if err := s.participantExists(ctx, participant); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin default workspace admission: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx,
		`INSERT INTO workspaces (workspace_id, name) VALUES ($1, 'Sumi')
		 ON CONFLICT (workspace_id) DO NOTHING`, DefaultWorkspaceID); err != nil {
		return fmt.Errorf("ensure default workspace: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO places (place_id, kind, workspace_id, name, topic)
		 VALUES ($1, 'channel', $2, 'general', 'みんなの場所')
		 ON CONFLICT (place_id) DO NOTHING`, DefaultGeneralChannelID, DefaultWorkspaceID); err != nil {
		return fmt.Errorf("ensure default general channel: %w", err)
	}
	if err := addDefaultMember(ctx, tx, participant); err != nil {
		return err
	}
	if participant.Kind == KindHuman {
		rows, err := tx.Query(ctx,
			`SELECT personality_agent_id FROM agents WHERE human_id = $1 ORDER BY personality_agent_id`,
			participant.ID)
		if err != nil {
			return fmt.Errorf("list Human agents for default workspace: %w", err)
		}
		var agents []ParticipantRef
		for rows.Next() {
			var agentID string
			if err := rows.Scan(&agentID); err != nil {
				rows.Close()
				return fmt.Errorf("scan Human agent for default workspace: %w", err)
			}
			agents = append(agents, PersonalityAgent(agentID))
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("iterate Human agents for default workspace: %w", err)
		}
		rows.Close()
		for _, agent := range agents {
			if err := addDefaultMember(ctx, tx, agent); err != nil {
				return err
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit default workspace admission: %w", err)
	}
	return s.EnsureFoundingAdmin(ctx, participant)
}

func addDefaultMember(ctx context.Context, tx pgx.Tx, participant ParticipantRef) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO workspace_members (workspace_id, member_kind, member_id, role)
		 VALUES ($1, $2, $3, 'member')
		 ON CONFLICT (workspace_id, member_kind, member_id) WHERE left_at IS NULL
		 DO NOTHING`, DefaultWorkspaceID, participant.Kind, participant.ID)
	if err != nil {
		return fmt.Errorf("admit participant to default workspace: %w", err)
	}
	return nil
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
	// Voice marks a channel as a place people are meant to talk in (ADR 0012).
	// It is an attribute of a channel, not a separate kind: the timeline,
	// unread counts and notification settings stay identical, because a voice
	// channel still carries text.
	Voice bool
}

// MemberProfile is a participant with their scope-resolved display name and
// the presentation profile they chose for themselves (participant_profiles).
// IDs are never used as display names (ADR 0008 §1).
type MemberProfile struct {
	Participant             ParticipantRef
	DisplayName             string
	SecretaryForDisplayName string
	Role                    string // workspace role; empty for dm/group_dm members
	// Tagline は本人が名乗る職務の説明。空でよい。
	Tagline string
	// 画像は message_attachments の添付id。空は「未設定」。
	AvatarAttachmentID string
	BannerAttachmentID string
}

// ProjectedDisplayName is the temporary v1 wire compromise for multiple
// Secretaries canonically named Sumi. The composite is presentation only: the
// agent registry continues to store "Sumi", while its stable Human relation
// supplies the qualifier.
func (m MemberProfile) ProjectedDisplayName() string {
	if m.Participant.Kind == KindPersonalityAgent && m.SecretaryForDisplayName != "" {
		return m.DisplayName + "（" + m.SecretaryForDisplayName + "）"
	}
	return m.DisplayName
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
	// Every workspace is born with the same two roles the shared one has, and
	// its creator holds Admin. A workspace nobody can administer is not a
	// state the product should be able to reach.
	adminRoleID := newUUIDv7()
	if _, err := tx.Exec(ctx,
		`INSERT INTO workspace_roles (role_id, workspace_id, name, position, permissions)
		 VALUES ($1, $2, 'Admin', 100, $3), ($4, $2, 'Member', 0, '{}'::jsonb)`,
		adminRoleID, workspaceID, map[string]bool{
			PermManageChannels: true, PermManageRoles: true,
			PermManageMembers: true, PermMentionAll: true,
		}, newUUIDv7()); err != nil {
		return Workspace{}, fmt.Errorf("seed workspace roles: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO participant_roles (role_id, member_kind, member_id) VALUES ($1, $2, $3)`,
		adminRoleID, creator.Kind, creator.ID); err != nil {
		return Workspace{}, fmt.Errorf("grant workspace admin: %w", err)
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

// CreateChannel creates a public channel in the workspace. Creating a channel
// is workspace administration, so it requires manage_channels — the same check
// on the REST lane and the agent lane (AX 同型).
// voice marks the channel as a place people are meant to talk in (ADR 0012).
// It changes nothing else: the channel still carries a timeline, unread counts
// and notification settings, because a voice channel is still a channel.
func (s *Store) CreateChannel(ctx context.Context, workspaceID, name, topic string, creator ParticipantRef, voice bool) (Place, error) {
	if err := creator.Validate(); err != nil {
		return Place{}, err
	}
	if !validChannelName(name) {
		return Place{}, ErrInvalidChannelName
	}
	active, _, err := s.workspaceMembership(ctx, s.pool, workspaceID, creator)
	if err != nil {
		return Place{}, err
	}
	if !active {
		return Place{}, ErrNotAMember
	}
	if err := s.RequirePermission(ctx, workspaceID, creator, PermManageChannels); err != nil {
		return Place{}, err
	}
	placeID := newUUIDv7()
	_, err = s.pool.Exec(ctx,
		`INSERT INTO places (place_id, kind, workspace_id, name, topic, voice)
		 VALUES ($1, 'channel', $2, $3, $4, $5)`,
		placeID, workspaceID, name, topic, voice)
	if err != nil {
		return Place{}, fmt.Errorf("insert channel: %w", err)
	}
	return Place{
		PlaceID: placeID, Kind: PlaceChannel, WorkspaceID: workspaceID,
		Name: name, Topic: topic, Visibility: "public", Voice: voice,
	}, nil
}

// MaxChannelNameChars matches the schema CHECK on places.name.
const MaxChannelNameChars = 200

// UpdateChannel edits a channel's mutable identity: its name, its topic, or
// both. A nil field is left alone, so naming one thing never silently discards
// the other. Editing a channel needs manage_channels, mirroring CreateChannel.
// Visibility is checked first so a place the actor cannot see stays
// unrevealed — a member without the permission is refused, a stranger is told
// nothing.
func (s *Store) UpdateChannel(ctx context.Context, placeID string, name, topic *string, actor ParticipantRef) (Place, error) {
	place, err := s.PlaceFor(ctx, placeID, actor)
	if err != nil {
		return Place{}, err
	}
	if place.Kind != PlaceChannel {
		return Place{}, ErrNotAChannel
	}
	if name != nil && !validChannelName(*name) {
		return Place{}, ErrInvalidChannelName
	}
	if err := s.RequirePermission(ctx, place.WorkspaceID, actor, PermManageChannels); err != nil {
		return Place{}, err
	}
	if _, err := s.pool.Exec(ctx,
		`UPDATE places
		 SET name = COALESCE($1, name), topic = COALESCE($2, topic)
		 WHERE place_id = $3`, name, topic, placeID); err != nil {
		return Place{}, fmt.Errorf("update channel: %w", err)
	}
	if name != nil {
		place.Name = *name
	}
	if topic != nil {
		place.Topic = *topic
	}
	return place, nil
}

// UpdateChannelTopic sets a channel's topic and leaves its name alone.
func (s *Store) UpdateChannelTopic(ctx context.Context, placeID, topic string, actor ParticipantRef) (Place, error) {
	return s.UpdateChannel(ctx, placeID, nil, &topic, actor)
}

// DuplicateChannel creates a new channel beside an existing one, carrying its
// topic over. An empty name derives one from the source ("dev" → "dev のコピー")
// so both the human menu and the agent tool land on the same default instead of
// each inventing their own. The copy is a new, empty place: messages, read
// markers and per-place notification settings belong to the original.
func (s *Store) DuplicateChannel(ctx context.Context, placeID, name string, actor ParticipantRef) (Place, error) {
	source, err := s.PlaceFor(ctx, placeID, actor)
	if err != nil {
		return Place{}, err
	}
	if source.Kind != PlaceChannel {
		return Place{}, ErrNotAChannel
	}
	if name == "" {
		name = duplicateChannelName(source.Name)
	}
	if !validChannelName(name) {
		return Place{}, ErrInvalidChannelName
	}
	// 複製は形（名前・トピック・ボイスかどうか）を写す。ボイスの複製が
	// テキストになったら、それは同じ形の場所ではない。
	return s.CreateChannel(ctx, source.WorkspaceID, name, source.Topic, actor, source.Voice)
}

// duplicateChannelName names the copy, keeping the result inside the schema's
// 200-character bound by trimming the source rather than failing.
func duplicateChannelName(source string) string {
	const suffix = " のコピー"
	room := MaxChannelNameChars - utf8.RuneCountInString(suffix)
	runes := []rune(source)
	if len(runes) > room {
		runes = runes[:room]
	}
	return string(runes) + suffix
}

func validChannelName(name string) bool {
	length := utf8.RuneCountInString(name)
	return length >= 1 && length <= MaxChannelNameChars
}

// EnsureDM returns the one dm place between two participants, creating it on
// first use; created reports whether this call minted the place. Reachability
// (契約ドラフト: 到達できれば誰とでも会話できる) v0 requires an active shared
// workspace membership; the accepted-Connection basis lands with the
// Connection domain (Codex合意 4) and widens this check without changing
// callers.
func (s *Store) EnsureDM(ctx context.Context, a, b ParticipantRef) (place Place, created bool, err error) {
	for _, p := range []ParticipantRef{a, b} {
		if err := p.Validate(); err != nil {
			return Place{}, false, err
		}
	}
	if a.Key() == b.Key() {
		return Place{}, false, fmt.Errorf("a dm needs two distinct participants")
	}
	reachable, err := s.shareActiveWorkspace(ctx, a, b)
	if err != nil {
		return Place{}, false, err
	}
	if !reachable {
		return Place{}, false, ErrNotReachable
	}
	dmKey := dmPairKey(a, b)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Place{}, false, fmt.Errorf("begin ensure dm: %w", err)
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
			return Place{}, false, fmt.Errorf("load existing dm: %w", err)
		}
		existing.Kind = PlaceDM
		existing.Visibility = "public"
		if err := tx.Commit(ctx); err != nil {
			return Place{}, false, fmt.Errorf("commit ensure dm: %w", err)
		}
		return existing, false, nil
	}
	if err != nil {
		return Place{}, false, fmt.Errorf("insert dm place: %w", err)
	}
	for _, p := range []ParticipantRef{a, b} {
		if _, err := tx.Exec(ctx,
			"INSERT INTO place_members (place_id, member_kind, member_id) VALUES ($1, $2, $3)",
			placeID, p.Kind, p.ID); err != nil {
			return Place{}, false, fmt.Errorf("insert dm member: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return Place{}, false, fmt.Errorf("commit ensure dm: %w", err)
	}
	return Place{PlaceID: placeID, Kind: PlaceDM, Visibility: "public"}, true, nil
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

// PlaceByID loads a place without a viewer. It exists for server-to-server
// paths that must name a place before any recipient is known — the LiveKit
// call webhook, which learns only a room name and needs the place shape to
// publish. It performs no visibility check, so it must never answer a request
// that carries a participant: the Hub still decides who is told (call.go).
func (s *Store) PlaceByID(ctx context.Context, placeID string) (Place, error) {
	return s.loadPlace(ctx, s.pool, placeID)
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
	return s.workspaceExistsWith(ctx, s.pool, workspaceID)
}

func (s *Store) workspaceExistsWith(ctx context.Context, q querier, workspaceID string) error {
	var exists bool
	err := q.QueryRow(ctx,
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
		if err := s.workspaceExistsWith(ctx, q, workspaceID); err != nil {
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
//
// A thread inherits its parent channel's Workspace, so every member of the
// parent may read and write in it. place_members on a thread records who
// joined (unread and notification audience), never who is allowed in.
func (s *Store) canAccess(ctx context.Context, q querier, place Place, p ParticipantRef) (bool, error) {
	if place.Kind == PlaceChannel || place.Kind == PlaceThread {
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
		`SELECT place_id, kind, workspace_id, name, topic, visibility, last_seq, voice
		 FROM places WHERE place_id = $1`, placeID).
		Scan(&place.PlaceID, &place.Kind, &workspaceID, &name,
			&place.Topic, &place.Visibility, &place.LastSeq, &place.Voice)
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
// A thread answers with its parent Workspace's members: everyone who can see
// the parent can be named in the thread, so mention resolution must not be
// narrowed to the people who happen to have written in it already.
func (s *Store) activeMembers(ctx context.Context, q querier, place Place) ([]MemberProfile, error) {
	if place.Kind == PlaceChannel || place.Kind == PlaceThread {
		return s.workspaceMemberProfiles(ctx, q, place.WorkspaceID)
	}
	rows, err := q.Query(ctx,
		`SELECT pm.member_kind, pm.member_id, '' AS role,
		        COALESCE(h.display_name, a.display_name, '') AS display_name,
		        CASE WHEN pm.member_kind='personality_agent'
		             THEN COALESCE(owner.display_name, '') ELSE '' END,
		        COALESCE(pp.tagline, ''),
		        pp.avatar_attachment_id, pp.banner_attachment_id
		 FROM place_members pm
		 LEFT JOIN humans h ON pm.member_kind = 'human' AND h.human_id = pm.member_id
		 LEFT JOIN agents a ON pm.member_kind = 'personality_agent' AND a.personality_agent_id = pm.member_id
		 LEFT JOIN humans owner ON owner.human_id = a.human_id
		 LEFT JOIN participant_profiles pp ON pp.member_kind = pm.member_kind
		                                 AND pp.member_id = pm.member_id
		 WHERE pm.place_id = $1 AND pm.left_at IS NULL
		 ORDER BY pm.place_member_id`, place.PlaceID)
	if err != nil {
		return nil, fmt.Errorf("query active members: %w", err)
	}
	return scanMemberProfiles(rows)
}

func (s *Store) workspaceMemberProfiles(ctx context.Context, q querier, workspaceID string) ([]MemberProfile, error) {
	rows, err := q.Query(ctx,
		`SELECT wm.member_kind, wm.member_id, wm.role,
		        COALESCE(h.display_name, a.display_name, '') AS display_name,
		        CASE WHEN wm.member_kind='personality_agent'
		             THEN COALESCE(owner.display_name, '') ELSE '' END,
		        COALESCE(pp.tagline, ''),
		        pp.avatar_attachment_id, pp.banner_attachment_id
		 FROM workspace_members wm
		 LEFT JOIN humans h ON wm.member_kind = 'human' AND h.human_id = wm.member_id
		 LEFT JOIN agents a ON wm.member_kind = 'personality_agent' AND a.personality_agent_id = wm.member_id
		 LEFT JOIN humans owner ON owner.human_id = a.human_id
		 LEFT JOIN participant_profiles pp ON pp.member_kind = wm.member_kind
		                                 AND pp.member_id = wm.member_id
		 WHERE wm.workspace_id = $1 AND wm.left_at IS NULL
		 ORDER BY wm.workspace_member_id`, workspaceID)
	if err != nil {
		return nil, fmt.Errorf("query workspace members: %w", err)
	}
	return scanMemberProfiles(rows)
}

func scanMemberProfiles(rows pgx.Rows) ([]MemberProfile, error) {
	defer rows.Close()
	var members []MemberProfile
	for rows.Next() {
		var m MemberProfile
		var kind string
		var avatar, banner *string
		if err := rows.Scan(&kind, &m.Participant.ID, &m.Role, &m.DisplayName,
			&m.SecretaryForDisplayName, &m.Tagline, &avatar, &banner); err != nil {
			return nil, fmt.Errorf("scan member: %w", err)
		}
		m.Participant.Kind = ParticipantKind(kind)
		if avatar != nil {
			m.AvatarAttachmentID = *avatar
		}
		if banner != nil {
			m.BannerAttachmentID = *banner
		}
		members = append(members, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate members: %w", err)
	}
	return members, nil
}

// WorkspacesFor lists the workspaces where the viewer is an active member.
func (s *Store) WorkspacesFor(ctx context.Context, viewer ParticipantRef) ([]Workspace, error) {
	if err := viewer.Validate(); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx,
		`SELECT w.workspace_id, w.name
		 FROM workspaces w
		 JOIN workspace_members wm ON wm.workspace_id = w.workspace_id
		 WHERE wm.member_kind = $1 AND wm.member_id = $2 AND wm.left_at IS NULL
		 ORDER BY w.created_at, w.workspace_id`,
		viewer.Kind, viewer.ID)
	if err != nil {
		return nil, fmt.Errorf("query workspaces: %w", err)
	}
	defer rows.Close()
	var out []Workspace
	for rows.Next() {
		var w Workspace
		if err := rows.Scan(&w.WorkspaceID, &w.Name); err != nil {
			return nil, fmt.Errorf("scan workspace: %w", err)
		}
		out = append(out, w)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workspaces: %w", err)
	}
	return out, nil
}

// WorkspaceMemberProfiles lists a workspace's active members for a viewer who
// is one of them. Non-members are not told the workspace exists.
func (s *Store) WorkspaceMemberProfiles(ctx context.Context, workspaceID string, viewer ParticipantRef) ([]MemberProfile, error) {
	if err := viewer.Validate(); err != nil {
		return nil, err
	}
	active, _, err := s.workspaceMembership(ctx, s.pool, workspaceID, viewer)
	if err != nil {
		if errors.Is(err, ErrWorkspaceNotFound) {
			return nil, ErrWorkspaceNotFound
		}
		return nil, err
	}
	if !active {
		return nil, ErrWorkspaceNotFound
	}
	return s.workspaceMemberProfiles(ctx, s.pool, workspaceID)
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
