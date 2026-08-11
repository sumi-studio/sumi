package workspace

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/participant"
)

const (
	maxWorkspaceNameChars = 200
	defaultInviteTTL      = 24 * time.Hour
	inviteEntropyBytes    = 32
)

type Store struct {
	pool      *pgxpool.Pool
	now       func() time.Time
	random    io.Reader
	inviteTTL time.Duration
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool, now: time.Now, random: rand.Reader, inviteTTL: defaultInviteTTL}
}

type querier interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

func (s *Store) CreateWorkspace(ctx context.Context, name string, creator participant.Ref) (Workspace, error) {
	name = strings.TrimSpace(name)
	if utf8.RuneCountInString(name) < 1 || utf8.RuneCountInString(name) > maxWorkspaceNameChars {
		return Workspace{}, ErrInvalidName
	}
	exists, err := participant.Exists(ctx, s.pool, creator)
	if err != nil {
		return Workspace{}, err
	}
	if !exists {
		return Workspace{}, ErrForbidden
	}

	workspaceID := newUUIDv7()
	membershipID := newUUIDv7()
	createdAt := s.now().UTC()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Workspace{}, fmt.Errorf("begin create workspace: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `
		INSERT INTO workspaces
			(workspace_id, name, owner_workspace_member_id, created_at)
		VALUES ($1, $2, $3, $4)`,
		workspaceID, name, membershipID, createdAt,
	); err != nil {
		return Workspace{}, fmt.Errorf("insert workspace: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO workspace_members
			(workspace_member_id, workspace_id, member_kind, member_id, joined_at)
		VALUES ($1, $2, $3, $4, $5)`,
		membershipID, workspaceID, creator.Kind, creator.ID, createdAt,
	); err != nil {
		return Workspace{}, fmt.Errorf("insert owner membership: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Workspace{}, fmt.Errorf("commit create workspace: %w", err)
	}
	return Workspace{
		WorkspaceID:            workspaceID,
		Name:                   name,
		OwnerWorkspaceMemberID: membershipID,
		CreatedAt:              createdAt,
	}, nil
}

// WorkspacesFor is a side-effect-free membership projection. An identity with
// no memberships receives an empty slice; no default Workspace is synthesized.
func (s *Store) WorkspacesFor(ctx context.Context, actor participant.Ref) ([]Workspace, error) {
	if err := actor.Validate(); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT w.workspace_id, w.name, w.owner_workspace_member_id, w.created_at
		FROM workspace_members wm
		JOIN workspaces w ON w.workspace_id = wm.workspace_id
		WHERE wm.member_kind = $1 AND wm.member_id = $2 AND wm.left_at IS NULL
		ORDER BY w.created_at, w.workspace_id`, actor.Kind, actor.ID)
	if err != nil {
		return nil, fmt.Errorf("list workspaces: %w", err)
	}
	defer rows.Close()
	workspaces := []Workspace{}
	for rows.Next() {
		var item Workspace
		if err := rows.Scan(&item.WorkspaceID, &item.Name,
			&item.OwnerWorkspaceMemberID, &item.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan workspace: %w", err)
		}
		workspaces = append(workspaces, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workspaces: %w", err)
	}
	return workspaces, nil
}

// WorkspaceFor intentionally uses the same ErrNotFound for a nonexistent
// Workspace and for one the actor cannot see.
func (s *Store) WorkspaceFor(ctx context.Context, workspaceID string, actor participant.Ref) (Workspace, error) {
	if err := actor.Validate(); err != nil {
		return Workspace{}, err
	}
	if !isCanonicalUUIDv7(workspaceID) {
		return Workspace{}, ErrNotFound
	}
	var item Workspace
	err := s.pool.QueryRow(ctx, `
		SELECT w.workspace_id, w.name, w.owner_workspace_member_id, w.created_at
		FROM workspaces w
		JOIN workspace_members wm ON wm.workspace_id = w.workspace_id
		WHERE w.workspace_id = $1
		  AND wm.member_kind = $2 AND wm.member_id = $3 AND wm.left_at IS NULL`,
		workspaceID, actor.Kind, actor.ID,
	).Scan(&item.WorkspaceID, &item.Name, &item.OwnerWorkspaceMemberID, &item.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Workspace{}, ErrNotFound
	}
	if err != nil {
		return Workspace{}, fmt.Errorf("load workspace: %w", err)
	}
	return item, nil
}

func (s *Store) Members(ctx context.Context, workspaceID string, actor participant.Ref) ([]Membership, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return nil, fmt.Errorf("begin workspace-member read: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := activeMembership(ctx, tx, workspaceID, actor); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `
		SELECT wm.workspace_member_id, wm.workspace_id, wm.member_kind, wm.member_id,
		       CASE WHEN wm.member_kind = 'human' THEN h.display_name ELSE a.display_name END,
		       wm.workspace_member_id = w.owner_workspace_member_id,
		       COALESCE(array_agg(wra.role_id ORDER BY wr.position DESC, wr.name)
		           FILTER (WHERE wra.role_id IS NOT NULL), ARRAY[]::text[]),
		       wm.joined_at, wm.left_at
		FROM workspace_members wm
		JOIN workspaces w ON w.workspace_id = wm.workspace_id
		LEFT JOIN humans h ON wm.member_kind = 'human' AND h.human_id = wm.member_id
		LEFT JOIN agents a ON wm.member_kind = 'personality_agent'
		  AND a.personality_agent_id = wm.member_id
		LEFT JOIN workspace_role_assignments wra
		  ON wra.workspace_id = wm.workspace_id
		 AND wra.workspace_member_id = wm.workspace_member_id
		LEFT JOIN workspace_roles wr
		  ON wr.workspace_id = wra.workspace_id AND wr.role_id = wra.role_id
		WHERE wm.workspace_id = $1 AND wm.left_at IS NULL
		GROUP BY wm.workspace_member_id, wm.workspace_id, wm.member_kind, wm.member_id,
		         h.display_name, a.display_name,
		         w.owner_workspace_member_id, wm.joined_at
		ORDER BY wm.joined_at, wm.workspace_member_id`, workspaceID)
	if err != nil {
		return nil, fmt.Errorf("list workspace members: %w", err)
	}
	members := []Membership{}
	for rows.Next() {
		var member Membership
		var kind string
		if err := rows.Scan(&member.WorkspaceMemberID, &member.WorkspaceID,
			&kind, &member.Participant.ID, &member.DisplayName, &member.Owner, &member.RoleIDs,
			&member.JoinedAt, &member.LeftAt); err != nil {
			return nil, fmt.Errorf("scan workspace member: %w", err)
		}
		member.Participant.Kind = participant.Kind(kind)
		if strings.TrimSpace(member.DisplayName) == "" {
			member.DisplayName = participantDisplayNameFallback(member.Participant)
		}
		members = append(members, member)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate workspace members: %w", err)
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit workspace-member read: %w", err)
	}
	return members, nil
}

func participantDisplayNameFallback(ref participant.Ref) string {
	kind := "Human"
	if ref.Kind == participant.KindPersonalityAgent {
		kind = "PersonalityAgent"
	}
	suffix := ref.ID
	if len(suffix) > 8 {
		suffix = suffix[len(suffix)-8:]
	}
	return kind + " · " + suffix
}

func (s *Store) UpdateName(ctx context.Context, workspaceID string, actor participant.Ref, name string) (Workspace, error) {
	name = strings.TrimSpace(name)
	if utf8.RuneCountInString(name) < 1 || utf8.RuneCountInString(name) > maxWorkspaceNameChars {
		return Workspace{}, ErrInvalidName
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Workspace{}, fmt.Errorf("begin update workspace: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.lockAndRequirePermission(ctx, tx, workspaceID, actor, PermissionManageWorkspace); err != nil {
		return Workspace{}, err
	}
	var item Workspace
	err = tx.QueryRow(ctx, `
		UPDATE workspaces SET name = $2 WHERE workspace_id = $1
		RETURNING workspace_id, name, owner_workspace_member_id, created_at`,
		workspaceID, name,
	).Scan(&item.WorkspaceID, &item.Name, &item.OwnerWorkspaceMemberID, &item.CreatedAt)
	if err != nil {
		return Workspace{}, fmt.Errorf("update workspace: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Workspace{}, fmt.Errorf("commit update workspace: %w", err)
	}
	return item, nil
}

// TransferOwnership moves the distinguished Workspace ownership to an exact,
// active membership tenure. Ownership is not a role and cannot be delegated by
// a manage permission: only the current owner may choose their successor.
func (s *Store) TransferOwnership(
	ctx context.Context,
	workspaceID string,
	targetWorkspaceMemberID string,
	actor participant.Ref,
) (Workspace, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Workspace{}, fmt.Errorf("begin transfer workspace ownership: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockWorkspace(ctx, tx, workspaceID); err != nil {
		return Workspace{}, err
	}
	currentOwner, err := activeMembershipForUpdate(ctx, tx, workspaceID, actor)
	if err != nil {
		return Workspace{}, err
	}
	if !currentOwner.Owner {
		return Workspace{}, ErrForbidden
	}
	target, err := membershipByIDForUpdate(ctx, tx, workspaceID, targetWorkspaceMemberID, true)
	if err != nil {
		return Workspace{}, err
	}

	var item Workspace
	err = tx.QueryRow(ctx, `
		UPDATE workspaces
		SET owner_workspace_member_id = $2
		WHERE workspace_id = $1
		RETURNING workspace_id, name, owner_workspace_member_id, created_at`,
		workspaceID, target.WorkspaceMemberID,
	).Scan(&item.WorkspaceID, &item.Name, &item.OwnerWorkspaceMemberID, &item.CreatedAt)
	if err != nil {
		return Workspace{}, fmt.Errorf("transfer workspace ownership: %w", err)
	}
	if err := ensureEffectiveAdministrator(ctx, tx, workspaceID); err != nil {
		return Workspace{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Workspace{}, fmt.Errorf("commit workspace ownership transfer: %w", err)
	}
	return item, nil
}

func (s *Store) CreateInvite(ctx context.Context, workspaceID string, actor participant.Ref) (Invite, error) {
	raw := make([]byte, inviteEntropyBytes)
	if _, err := io.ReadFull(s.random, raw); err != nil {
		return Invite{}, fmt.Errorf("generate invite code: %w", err)
	}
	code := base64.RawURLEncoding.EncodeToString(raw)
	hash := sha256.Sum256([]byte(code))
	createdAt := s.now().UTC()
	invite := Invite{
		InviteID: newUUIDv7(), WorkspaceID: workspaceID, Code: code,
		ExpiresAt: createdAt.Add(s.inviteTTL), CreatedAt: createdAt,
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Invite{}, fmt.Errorf("begin create invite: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	actorMembershipID, err := s.lockAndRequirePermission(ctx, tx, workspaceID, actor, PermissionManageMembers)
	if err != nil {
		return Invite{}, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO workspace_invites
			(invite_id, workspace_id, created_by_workspace_member_id, code_hash,
			 expires_at, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		invite.InviteID, workspaceID, actorMembershipID, hash[:],
		invite.ExpiresAt, createdAt,
	); err != nil {
		return Invite{}, fmt.Errorf("insert workspace invite: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Invite{}, fmt.Errorf("commit workspace invite: %w", err)
	}
	return invite, nil
}

// Invites returns only currently redeemable, non-secret invite records. The
// Workspace lock keeps the caller's exact tenure and manage_members authority,
// issuer authority, redemption, and revocation stable through this read.
func (s *Store) Invites(ctx context.Context, workspaceID string, actor participant.Ref) ([]InviteRecord, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin list workspace invites: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.lockAndRequirePermission(ctx, tx, workspaceID, actor, PermissionManageMembers); err != nil {
		return nil, err
	}
	type candidate struct {
		record             InviteRecord
		issuerMembershipID string
	}
	rows, err := tx.Query(ctx, `
		SELECT invite_id, workspace_id, created_by_workspace_member_id, expires_at, created_at
		FROM workspace_invites
		WHERE workspace_id = $1
		  AND revoked_at IS NULL
		  AND redeemed_at IS NULL
		  AND expires_at > $2
		ORDER BY created_at, invite_id`, workspaceID, s.now().UTC())
	if err != nil {
		return nil, fmt.Errorf("query workspace invites: %w", err)
	}
	var candidates []candidate
	for rows.Next() {
		var item candidate
		if err := rows.Scan(&item.record.InviteID, &item.record.WorkspaceID,
			&item.issuerMembershipID, &item.record.ExpiresAt, &item.record.CreatedAt); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan workspace invite: %w", err)
		}
		candidates = append(candidates, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate workspace invites: %w", err)
	}
	rows.Close()

	items := make([]InviteRecord, 0, len(candidates))
	for _, candidate := range candidates {
		err := requireInviteIssuerAuthority(ctx, tx, workspaceID, candidate.issuerMembershipID)
		if errors.Is(err, ErrInviteUnavailable) {
			continue
		}
		if err != nil {
			return nil, err
		}
		items = append(items, candidate.record)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit list workspace invites: %w", err)
	}
	return items, nil
}

// PreviewInvite resolves only the minimal, non-consuming link preview. It
// deliberately has no actor parameter: possession of the opaque code is the
// sole preview capability, while redemption separately authenticates and
// derives its participant from the transport.
func (s *Store) PreviewInvite(ctx context.Context, code string) (InvitePreview, error) {
	if code == "" || len(code) > 128 {
		return InvitePreview{}, ErrInviteUnavailable
	}
	hash := sha256.Sum256([]byte(code))
	var preview InvitePreview
	err := s.pool.QueryRow(ctx, `
		SELECT w.workspace_id, w.name, wi.expires_at
		FROM workspace_invites wi
		JOIN workspaces w ON w.workspace_id = wi.workspace_id
		WHERE wi.code_hash = $1
		  AND wi.revoked_at IS NULL
		  AND wi.redeemed_at IS NULL
		  AND wi.expires_at > $2`, hash[:], s.now().UTC(),
	).Scan(&preview.WorkspaceID, &preview.WorkspaceName, &preview.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return InvitePreview{}, ErrInviteUnavailable
	}
	if err != nil {
		return InvitePreview{}, fmt.Errorf("preview workspace invite: %w", err)
	}
	return preview, nil
}

func (s *Store) RedeemInvite(ctx context.Context, code string, actor participant.Ref) (Membership, error) {
	if code == "" || len(code) > 128 {
		return Membership{}, ErrInviteUnavailable
	}
	exists, err := participant.Exists(ctx, s.pool, actor)
	if err != nil {
		return Membership{}, err
	}
	if !exists {
		return Membership{}, ErrInviteUnavailable
	}
	hash := sha256.Sum256([]byte(code))
	now := s.now().UTC()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Membership{}, fmt.Errorf("begin redeem invite: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	// Resolve the lock root without trusting or revealing it, then take locks in
	// the same Workspace -> invite order used by invite revocation. Role changes
	// and membership closure also lock the Workspace, so the issuer authority
	// checked below remains true through commit.
	var workspaceID string
	err = tx.QueryRow(ctx, `
		SELECT workspace_id FROM workspace_invites WHERE code_hash = $1`, hash[:],
	).Scan(&workspaceID)
	if errors.Is(err, pgx.ErrNoRows) {
		return Membership{}, ErrInviteUnavailable
	}
	if err != nil {
		return Membership{}, fmt.Errorf("resolve workspace invite: %w", err)
	}
	if err := lockWorkspace(ctx, tx, workspaceID); err != nil {
		if errors.Is(err, ErrNotFound) {
			return Membership{}, ErrInviteUnavailable
		}
		return Membership{}, err
	}
	var expiresAt time.Time
	var revokedAt *time.Time
	var redeemedKind, redeemedID, redeemedMembershipID *string
	var issuerMembershipID string
	err = tx.QueryRow(ctx, `
		SELECT created_by_workspace_member_id, expires_at, revoked_at, redeemed_by_kind,
		       redeemed_by_id, redeemed_workspace_member_id
		FROM workspace_invites WHERE code_hash = $1 FOR UPDATE`, hash[:],
	).Scan(&issuerMembershipID, &expiresAt, &revokedAt, &redeemedKind,
		&redeemedID, &redeemedMembershipID)
	if errors.Is(err, pgx.ErrNoRows) {
		return Membership{}, ErrInviteUnavailable
	}
	if err != nil {
		return Membership{}, fmt.Errorf("lock workspace invite: %w", err)
	}
	if redeemedKind != nil {
		if redeemedID == nil || redeemedMembershipID == nil ||
			*redeemedKind != string(actor.Kind) || *redeemedID != actor.ID {
			return Membership{}, ErrInviteUnavailable
		}
		membership, err := membershipByID(ctx, tx, workspaceID, *redeemedMembershipID, false)
		if err != nil {
			return Membership{}, fmt.Errorf("load prior invite redemption: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return Membership{}, fmt.Errorf("commit invite redemption replay: %w", err)
		}
		return membership, nil
	}
	if revokedAt != nil || !expiresAt.After(now) {
		return Membership{}, ErrInviteUnavailable
	}
	if err := requireInviteIssuerAuthority(ctx, tx, workspaceID, issuerMembershipID); err != nil {
		return Membership{}, err
	}
	var already bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM workspace_members
			WHERE workspace_id = $1 AND member_kind = $2 AND member_id = $3
			  AND left_at IS NULL
		)`, workspaceID, actor.Kind, actor.ID).Scan(&already); err != nil {
		return Membership{}, fmt.Errorf("check existing workspace membership: %w", err)
	}
	if already {
		return Membership{}, ErrAlreadyMember
	}
	membership := Membership{
		WorkspaceMemberID: newUUIDv7(), WorkspaceID: workspaceID,
		Participant: actor, JoinedAt: now, RoleIDs: []string{},
	}
	membership.DisplayName, err = resolveParticipantDisplayName(ctx, tx, actor)
	if err != nil {
		return Membership{}, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO workspace_members
			(workspace_member_id, workspace_id, member_kind, member_id, joined_at)
		VALUES ($1, $2, $3, $4, $5)`, membership.WorkspaceMemberID,
		workspaceID, actor.Kind, actor.ID, now,
	); err != nil {
		if isUniqueViolation(err) {
			return Membership{}, ErrAlreadyMember
		}
		return Membership{}, fmt.Errorf("insert workspace membership: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE workspace_invites
		SET redeemed_by_kind = $2, redeemed_by_id = $3,
		    redeemed_workspace_member_id = $4, redeemed_at = $5
		WHERE code_hash = $1`, hash[:], actor.Kind, actor.ID,
		membership.WorkspaceMemberID, now); err != nil {
		return Membership{}, fmt.Errorf("consume workspace invite: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Membership{}, fmt.Errorf("commit invite redemption: %w", err)
	}
	return membership, nil
}

// requireInviteIssuerAuthority revalidates the exact membership tenure that
// issued an unconsumed invite. A later tenure for the same participant cannot
// revive the invite, and losing manage_members makes it unusable immediately.
// Completed same-actor redemption retries bypass this check because they are
// reads of an already committed result, not a second admission decision.
func requireInviteIssuerAuthority(ctx context.Context, tx pgx.Tx, workspaceID, membershipID string) error {
	issuer, err := membershipByID(ctx, tx, workspaceID, membershipID, true)
	if errors.Is(err, ErrMemberNotFound) {
		return ErrInviteUnavailable
	}
	if err != nil {
		return err
	}
	permissions, err := permissionsForMembership(ctx, tx, issuer)
	if err != nil {
		return err
	}
	if !permissions.Can(PermissionManageMembers) {
		return ErrInviteUnavailable
	}
	return nil
}

func (s *Store) RevokeInvite(ctx context.Context, workspaceID, inviteID string, actor participant.Ref) error {
	if !isCanonicalUUIDv7(inviteID) {
		return ErrInviteUnavailable
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin revoke invite: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.lockAndRequirePermission(ctx, tx, workspaceID, actor, PermissionManageMembers); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
		UPDATE workspace_invites SET revoked_at = COALESCE(revoked_at, $3)
		WHERE workspace_id = $1 AND invite_id = $2`, workspaceID, inviteID, s.now().UTC())
	if err != nil {
		return fmt.Errorf("revoke workspace invite: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrInviteUnavailable
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit invite revocation: %w", err)
	}
	return nil
}

func (s *Store) RemoveMember(ctx context.Context, workspaceID, membershipID string, actor participant.Ref) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin remove workspace member: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.lockAndRequirePermission(ctx, tx, workspaceID, actor, PermissionManageMembers); err != nil {
		return err
	}
	// Lock the exact parent tenure before closing any child tenure. The
	// place_members admission trigger takes a conflicting SHARE lock, so a
	// racing admission either commits before this point and is included below,
	// or waits and observes the closed parent.
	target, err := membershipByIDForUpdate(ctx, tx, workspaceID, membershipID, true)
	if err != nil {
		return err
	}
	if target.Owner {
		return ErrOwnerProtected
	}
	actorPermissions, err := s.permissionsFor(ctx, tx, workspaceID, actor)
	if err != nil {
		return err
	}
	targetPermissions, err := permissionsForMembership(ctx, tx, target)
	if err != nil {
		return err
	}
	if !permissionsWithin(actorPermissions, targetPermissions) {
		return ErrForbidden
	}
	leftAt, err := closeBoundPlaceTenures(ctx, tx, workspaceID, membershipID)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE workspace_members SET left_at = $3
		WHERE workspace_id = $1 AND workspace_member_id = $2 AND left_at IS NULL`,
		workspaceID, membershipID, leftAt,
	); err != nil {
		return fmt.Errorf("close workspace membership: %w", err)
	}
	if err := ensureEffectiveAdministrator(ctx, tx, workspaceID); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit member removal: %w", err)
	}
	return nil
}

func (s *Store) Leave(ctx context.Context, workspaceID string, actor participant.Ref) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin leave workspace: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := lockWorkspace(ctx, tx, workspaceID); err != nil {
		return err
	}
	membership, err := activeMembershipForUpdate(ctx, tx, workspaceID, actor)
	if err != nil {
		return err
	}
	if membership.Owner {
		return ErrOwnerProtected
	}
	leftAt, err := closeBoundPlaceTenures(ctx, tx, workspaceID,
		membership.WorkspaceMemberID)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE workspace_members SET left_at = $3
		WHERE workspace_id = $1 AND workspace_member_id = $2 AND left_at IS NULL`,
		workspaceID, membership.WorkspaceMemberID, leftAt,
	); err != nil {
		return fmt.Errorf("leave workspace: %w", err)
	}
	if err := ensureEffectiveAdministrator(ctx, tx, workspaceID); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit leave workspace: %w", err)
	}
	return nil
}

// closeBoundPlaceTenures derives one effective closure time that is no earlier
// than either the parent or any active child tenure. The parent row is already
// locked by the caller, so the place-admission trigger cannot add a new child
// between this snapshot and the updates. Returning one timestamp keeps every
// child and its parent on the same closure boundary even when application and
// database clocks differ. The database clock is materialized once inside the
// transaction; an application clock cannot choose or inflate the boundary.
func closeBoundPlaceTenures(ctx context.Context, tx pgx.Tx, workspaceID, workspaceMemberID string) (time.Time, error) {
	var leftAt time.Time
	if err := tx.QueryRow(ctx, `
		WITH closure_clock AS MATERIALIZED (
			SELECT clock_timestamp() AS closed_at
		)
		SELECT GREATEST(
			closure_clock.closed_at,
			membership.joined_at,
			COALESCE(MAX(place_tenure.joined_at), closure_clock.closed_at)
		)
		FROM workspace_members membership
		CROSS JOIN closure_clock
		LEFT JOIN place_members place_tenure
		  ON place_tenure.workspace_id = membership.workspace_id
		 AND place_tenure.workspace_member_id = membership.workspace_member_id
		 AND place_tenure.left_at IS NULL
		WHERE membership.workspace_id = $1
		  AND membership.workspace_member_id = $2
		  AND membership.left_at IS NULL
		GROUP BY membership.joined_at, closure_clock.closed_at`, workspaceID, workspaceMemberID,
	).Scan(&leftAt); err != nil {
		return time.Time{}, fmt.Errorf("derive membership-tenure closure time: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE place_members SET left_at = $3
		WHERE workspace_id = $1 AND workspace_member_id = $2 AND left_at IS NULL`,
		workspaceID, workspaceMemberID, leftAt); err != nil {
		return time.Time{}, fmt.Errorf("close place membership tenures: %w", err)
	}
	return leftAt, nil
}

// LockAndRequirePermission is the platform-permission seam used by Workspace
// consumers such as app-installation lifecycle. Its vocabulary is exactly the
// four permissions Workspace owns. The caller owns tx and commits the
// authorized mutation in that same transaction.
func (s *Store) LockAndRequirePermission(ctx context.Context, tx pgx.Tx, workspaceID string, actor participant.Ref, permission string) error {
	_, err := s.lockAndRequirePermission(ctx, tx, workspaceID, actor, permission)
	return err
}

func (s *Store) lockAndRequirePermission(ctx context.Context, tx pgx.Tx, workspaceID string, actor participant.Ref, permission string) (string, error) {
	if !isKnownPermission(permission) {
		return "", ErrInvalidPermission
	}
	return s.lockAndRequireEffectiveCapability(ctx, tx, workspaceID, actor, permission)
}

// LockAndRequireAppCapability evaluates one app-owned, catalog-backed role
// capability. It does not authorize installation state, place tenure, privacy,
// or the app's domain operation; the app enforces those around this check in
// the same transaction.
func (s *Store) LockAndRequireAppCapability(ctx context.Context, tx pgx.Tx, workspaceID string, actor participant.Ref, capabilityRef string) error {
	if !appCapabilityRefPattern.MatchString(capabilityRef) {
		return ErrInvalidPermission
	}
	// Workspace is the lock root for every Workspace-owned application
	// mutation. Resolve the app catalog only after it so role changes,
	// installation lifecycle, and app-domain writes cannot form a
	// catalog -> Workspace / Workspace -> catalog lock cycle.
	if err := lockWorkspace(ctx, tx, workspaceID); err != nil {
		return err
	}
	var capabilityID string
	err := tx.QueryRow(ctx, `
		SELECT capability_id
		FROM app_workspace_role_capabilities
		WHERE capability_ref = $1 AND retired_at IS NULL
		FOR SHARE`, capabilityRef).Scan(&capabilityID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrInvalidPermission
	}
	if err != nil {
		return fmt.Errorf("validate app Workspace-role capability: %w", err)
	}
	_, err = s.requireEffectiveCapabilityAfterWorkspaceLock(ctx, tx, workspaceID, actor, capabilityRef)
	return err
}

func (s *Store) lockAndRequireEffectiveCapability(ctx context.Context, tx pgx.Tx, workspaceID string, actor participant.Ref, capabilityRef string) (string, error) {
	if err := lockWorkspace(ctx, tx, workspaceID); err != nil {
		return "", err
	}
	return s.requireEffectiveCapabilityAfterWorkspaceLock(ctx, tx, workspaceID, actor, capabilityRef)
}

func (s *Store) requireEffectiveCapabilityAfterWorkspaceLock(ctx context.Context, tx pgx.Tx, workspaceID string, actor participant.Ref, capabilityRef string) (string, error) {
	membership, err := activeMembership(ctx, tx, workspaceID, actor)
	if err != nil {
		return "", err
	}
	// Owner bypasses only the role-capability result. Requiring this active
	// membership first prevents the bypass from becoming a Workspace admission.
	if membership.Owner {
		return membership.WorkspaceMemberID, nil
	}
	permissions, err := permissionsForMembership(ctx, tx, membership)
	if err != nil {
		return "", err
	}
	if !permissions.Can(capabilityRef) {
		return "", ErrForbidden
	}
	return membership.WorkspaceMemberID, nil
}

func (s *Store) PermissionsFor(ctx context.Context, workspaceID string, actor participant.Ref) (PermissionSet, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return nil, fmt.Errorf("begin workspace-permission read: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	permissions, err := s.permissionsFor(ctx, tx, workspaceID, actor)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit workspace-permission read: %w", err)
	}
	return permissions, nil
}

// RequireMembership is the read-side application-consumer seam. It reveals no
// more than WorkspaceFor: nonexistent and unauthorized Workspaces both return
// ErrNotFound.
func (s *Store) RequireMembership(ctx context.Context, workspaceID string, actor participant.Ref) error {
	_, err := activeMembership(ctx, s.pool, workspaceID, actor)
	return err
}

// RequireMembershipInTx lets application consumers bind an authorization read
// and its result query to one database snapshot.
func (s *Store) RequireMembershipInTx(ctx context.Context, tx pgx.Tx, workspaceID string, actor participant.Ref) error {
	_, err := activeMembership(ctx, tx, workspaceID, actor)
	return err
}

// LockSharedAndRequireMembership acquires the Workspace-wide shared authority
// fence used by application mutations, then resolves the actor's exact active
// tenure. Membership joins, closures, role changes, and app lifecycle changes
// acquire the same Workspace row FOR UPDATE, so whichever transaction commits
// first defines the authority/audience snapshot. Shared holders do not
// serialize independent application mutations with each other.
func (s *Store) LockSharedAndRequireMembership(
	ctx context.Context,
	tx pgx.Tx,
	workspaceID string,
	actor participant.Ref,
) (Membership, error) {
	if err := lockWorkspaceShared(ctx, tx, workspaceID); err != nil {
		return Membership{}, err
	}
	return activeMembership(ctx, tx, workspaceID, actor)
}

// ActiveMembershipInTx returns the exact active tenure an application binds
// its own child resource to. Returning only a boolean would force consumers to
// rediscover workspace_member_id and reopen a race with removal/rejoin.
func (s *Store) ActiveMembershipInTx(
	ctx context.Context,
	tx pgx.Tx,
	workspaceID string,
	actor participant.Ref,
) (Membership, error) {
	return activeMembership(ctx, tx, workspaceID, actor)
}

// ActiveMembershipsInTx is the narrow audience projection for applications
// that have already admitted an operation in this transaction. It exposes
// exact active tenure identities; it does not authorize the caller by itself.
func (s *Store) ActiveMembershipsInTx(
	ctx context.Context,
	tx pgx.Tx,
	workspaceID string,
) ([]Membership, error) {
	if !isCanonicalUUIDv7(workspaceID) {
		return nil, ErrNotFound
	}
	rows, err := tx.Query(ctx, `
		SELECT wm.workspace_member_id, wm.workspace_id, wm.member_kind, wm.member_id,
		       wm.workspace_member_id = w.owner_workspace_member_id,
		       wm.joined_at
		FROM workspace_members wm
		JOIN workspaces w ON w.workspace_id = wm.workspace_id
		WHERE wm.workspace_id = $1 AND wm.left_at IS NULL
		ORDER BY wm.joined_at, wm.workspace_member_id`, workspaceID)
	if err != nil {
		return nil, fmt.Errorf("list active workspace membership tenures: %w", err)
	}
	defer rows.Close()
	memberships := []Membership{}
	for rows.Next() {
		var membership Membership
		var kind string
		if err := rows.Scan(&membership.WorkspaceMemberID, &membership.WorkspaceID,
			&kind, &membership.Participant.ID, &membership.Owner,
			&membership.JoinedAt); err != nil {
			return nil, fmt.Errorf("scan active workspace membership tenure: %w", err)
		}
		membership.Participant.Kind = participant.Kind(kind)
		memberships = append(memberships, membership)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active workspace membership tenures: %w", err)
	}
	return memberships, nil
}

func (s *Store) permissionsFor(ctx context.Context, q querier, workspaceID string, actor participant.Ref) (PermissionSet, error) {
	membership, err := activeMembership(ctx, q, workspaceID, actor)
	if err != nil {
		return nil, err
	}
	return permissionsForMembership(ctx, q, membership)
}

func activeMembership(ctx context.Context, q querier, workspaceID string, actor participant.Ref) (Membership, error) {
	return activeMembershipWithLock(ctx, q, workspaceID, actor, false)
}

func activeMembershipForUpdate(ctx context.Context, q querier, workspaceID string, actor participant.Ref) (Membership, error) {
	return activeMembershipWithLock(ctx, q, workspaceID, actor, true)
}

func activeMembershipWithLock(ctx context.Context, q querier, workspaceID string, actor participant.Ref, forUpdate bool) (Membership, error) {
	if err := actor.Validate(); err != nil {
		return Membership{}, err
	}
	if !isCanonicalUUIDv7(workspaceID) {
		return Membership{}, ErrNotFound
	}
	var membership Membership
	var kind string
	lockClause := ""
	if forUpdate {
		lockClause = " FOR UPDATE OF wm"
	}
	err := q.QueryRow(ctx, `
		SELECT wm.workspace_member_id, wm.workspace_id, wm.member_kind, wm.member_id,
		       wm.workspace_member_id = w.owner_workspace_member_id, wm.joined_at
		FROM workspace_members wm
		JOIN workspaces w ON w.workspace_id = wm.workspace_id
		WHERE wm.workspace_id = $1 AND wm.member_kind = $2 AND wm.member_id = $3
		  AND wm.left_at IS NULL`+lockClause, workspaceID, actor.Kind, actor.ID,
	).Scan(&membership.WorkspaceMemberID, &membership.WorkspaceID, &kind,
		&membership.Participant.ID, &membership.Owner, &membership.JoinedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Membership{}, ErrNotFound
	}
	if err != nil {
		return Membership{}, fmt.Errorf("load active workspace membership: %w", err)
	}
	membership.Participant.Kind = participant.Kind(kind)
	membership.DisplayName, err = resolveParticipantDisplayName(ctx, q, membership.Participant)
	if err != nil {
		return Membership{}, err
	}
	return membership, nil
}

func resolveParticipantDisplayName(ctx context.Context, q querier, ref participant.Ref) (string, error) {
	var displayName string
	switch ref.Kind {
	case participant.KindHuman:
		err := q.QueryRow(ctx, `SELECT display_name FROM humans WHERE human_id = $1`, ref.ID).Scan(&displayName)
		if errors.Is(err, pgx.ErrNoRows) {
			return participantDisplayNameFallback(ref), nil
		}
		if err != nil {
			return "", fmt.Errorf("resolve Human display name: %w", err)
		}
	case participant.KindPersonalityAgent:
		err := q.QueryRow(ctx,
			`SELECT display_name FROM agents WHERE personality_agent_id = $1`, ref.ID,
		).Scan(&displayName)
		if errors.Is(err, pgx.ErrNoRows) {
			return participantDisplayNameFallback(ref), nil
		}
		if err != nil {
			return "", fmt.Errorf("resolve PersonalityAgent display name: %w", err)
		}
	default:
		return "", fmt.Errorf("unknown participant kind %q", ref.Kind)
	}
	if strings.TrimSpace(displayName) == "" {
		return participantDisplayNameFallback(ref), nil
	}
	return displayName, nil
}

func membershipByID(ctx context.Context, q querier, workspaceID, membershipID string, activeOnly bool) (Membership, error) {
	return membershipByIDWithLock(ctx, q, workspaceID, membershipID, activeOnly, false)
}

func membershipByIDForUpdate(ctx context.Context, q querier, workspaceID, membershipID string, activeOnly bool) (Membership, error) {
	return membershipByIDWithLock(ctx, q, workspaceID, membershipID, activeOnly, true)
}

func membershipByIDWithLock(ctx context.Context, q querier, workspaceID, membershipID string, activeOnly, forUpdate bool) (Membership, error) {
	if !isCanonicalUUIDv7(workspaceID) || !isCanonicalUUIDv7(membershipID) {
		return Membership{}, ErrMemberNotFound
	}
	activeClause := ""
	if activeOnly {
		activeClause = " AND wm.left_at IS NULL"
	}
	lockClause := ""
	if forUpdate {
		lockClause = " FOR UPDATE OF wm"
	}
	var membership Membership
	var kind string
	err := q.QueryRow(ctx, `
		SELECT wm.workspace_member_id, wm.workspace_id, wm.member_kind, wm.member_id,
		       wm.workspace_member_id = w.owner_workspace_member_id,
		       COALESCE(ARRAY(
		           SELECT wra.role_id
		           FROM workspace_role_assignments wra
		           JOIN workspace_roles wr
		             ON wr.workspace_id = wra.workspace_id AND wr.role_id = wra.role_id
		           WHERE wra.workspace_id = wm.workspace_id
		             AND wra.workspace_member_id = wm.workspace_member_id
		           ORDER BY wr.position DESC, wr.name, wr.role_id
		       ), ARRAY[]::text[]),
		       wm.joined_at, wm.left_at
		FROM workspace_members wm
		JOIN workspaces w ON w.workspace_id = wm.workspace_id
		WHERE wm.workspace_id = $1 AND wm.workspace_member_id = $2`+activeClause+lockClause,
		workspaceID, membershipID,
	).Scan(&membership.WorkspaceMemberID, &membership.WorkspaceID, &kind,
		&membership.Participant.ID, &membership.Owner, &membership.RoleIDs,
		&membership.JoinedAt, &membership.LeftAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Membership{}, ErrMemberNotFound
	}
	if err != nil {
		return Membership{}, fmt.Errorf("load workspace membership: %w", err)
	}
	membership.Participant.Kind = participant.Kind(kind)
	return membership, nil
}

func permissionsForMembership(ctx context.Context, q querier, membership Membership) (PermissionSet, error) {
	if membership.Owner {
		permissions := allPermissions()
		rows, err := q.Query(ctx, `
			SELECT capability_ref
			FROM app_workspace_role_capabilities
			WHERE retired_at IS NULL
			ORDER BY capability_ref`)
		if err != nil {
			return nil, fmt.Errorf("query active app capabilities for Workspace owner: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var ref string
			if err := rows.Scan(&ref); err != nil {
				return nil, fmt.Errorf("scan active app capability for Workspace owner: %w", err)
			}
			permissions[ref] = true
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("iterate active app capabilities for Workspace owner: %w", err)
		}
		return permissions, nil
	}
	rows, err := q.Query(ctx, `
		SELECT wr.permissions
		FROM workspace_role_assignments wra
		JOIN workspace_roles wr
		  ON wr.workspace_id = wra.workspace_id AND wr.role_id = wra.role_id
		WHERE wra.workspace_id = $1 AND wra.workspace_member_id = $2`,
		membership.WorkspaceID, membership.WorkspaceMemberID)
	if err != nil {
		return nil, fmt.Errorf("query workspace permissions: %w", err)
	}
	defer rows.Close()
	permissions := PermissionSet{}
	for rows.Next() {
		var rolePermissions map[string]bool
		if err := rows.Scan(&rolePermissions); err != nil {
			return nil, fmt.Errorf("scan workspace permissions: %w", err)
		}
		for permission, allowed := range normalizePermissions(rolePermissions) {
			if allowed {
				permissions[permission] = true
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workspace permissions: %w", err)
	}
	rows.Close()
	appRows, err := q.Query(ctx, `
		SELECT DISTINCT role_grant.capability_ref_snapshot
		FROM workspace_role_assignments assignment
		JOIN workspace_role_app_capability_grants role_grant
		  ON role_grant.workspace_id = assignment.workspace_id
		 AND role_grant.role_id = assignment.role_id
		JOIN app_workspace_role_capabilities capability
		  ON capability.capability_id = role_grant.capability_id
		 AND capability.capability_ref = role_grant.capability_ref_snapshot
		 AND capability.retired_at IS NULL
		WHERE assignment.workspace_id = $1
		  AND assignment.workspace_member_id = $2
		ORDER BY role_grant.capability_ref_snapshot`,
		membership.WorkspaceID, membership.WorkspaceMemberID)
	if err != nil {
		return nil, fmt.Errorf("query effective app capabilities: %w", err)
	}
	defer appRows.Close()
	for appRows.Next() {
		var ref string
		if err := appRows.Scan(&ref); err != nil {
			return nil, fmt.Errorf("scan effective app capability: %w", err)
		}
		permissions[ref] = true
	}
	if err := appRows.Err(); err != nil {
		return nil, fmt.Errorf("iterate effective app capabilities: %w", err)
	}
	return permissions, nil
}

func lockWorkspace(ctx context.Context, q querier, workspaceID string) error {
	return lockWorkspaceWithClause(ctx, q, workspaceID, "FOR UPDATE")
}

func lockWorkspaceShared(ctx context.Context, q querier, workspaceID string) error {
	return lockWorkspaceWithClause(ctx, q, workspaceID, "FOR SHARE")
}

func lockWorkspaceWithClause(ctx context.Context, q querier, workspaceID, clause string) error {
	if !isCanonicalUUIDv7(workspaceID) {
		return ErrNotFound
	}
	if clause != "FOR UPDATE" && clause != "FOR SHARE" {
		return errors.New("invalid Workspace lock mode")
	}
	var locked string
	err := q.QueryRow(ctx, fmt.Sprintf(
		"SELECT workspace_id FROM workspaces WHERE workspace_id = $1 %s", clause), workspaceID,
	).Scan(&locked)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("lock workspace: %w", err)
	}
	return nil
}

func ensureEffectiveAdministrator(ctx context.Context, q querier, workspaceID string) error {
	// The immutable distinguished owner makes this guard redundant today, but
	// keep it as the mutation-level defense: a future audited owner-transfer
	// operation must preserve (or deliberately replace) this invariant.
	var administered bool
	err := q.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM workspace_members wm
			JOIN workspaces w ON w.workspace_id = wm.workspace_id
			WHERE wm.workspace_id = $1 AND wm.left_at IS NULL
			  AND (
				wm.workspace_member_id = w.owner_workspace_member_id
				OR NOT EXISTS (
					SELECT 1 FROM unnest($2::text[]) required(permission)
					WHERE NOT EXISTS (
						SELECT 1
						FROM workspace_role_assignments wra
						JOIN workspace_roles wr
						  ON wr.workspace_id = wra.workspace_id AND wr.role_id = wra.role_id
						WHERE wra.workspace_id = wm.workspace_id
						  AND wra.workspace_member_id = wm.workspace_member_id
						  AND COALESCE((wr.permissions ->> required.permission)::boolean, false)
					)
				)
			  )
		)`, workspaceID, administrativePermissions).Scan(&administered)
	if err != nil {
		return fmt.Errorf("check effective workspace administrator: %w", err)
	}
	if !administered {
		return ErrLastAdministrator
	}
	return nil
}

func normalizePermissions(input map[string]bool) PermissionSet {
	out := PermissionSet{}
	for _, permission := range knownPermissions {
		if input[permission] {
			out[permission] = true
		}
	}
	return out
}

func isKnownPermission(permission string) bool {
	for _, known := range knownPermissions {
		if permission == known {
			return true
		}
	}
	return false
}

func permissionsWithin(actor, target PermissionSet) bool {
	for permission, allowed := range target {
		if allowed && !actor.Can(permission) {
			return false
		}
	}
	return true
}

func newUUIDv7() string {
	id, err := uuid.NewV7()
	if err != nil {
		panic(fmt.Sprintf("generate UUIDv7: %v", err))
	}
	return id.String()
}

func isCanonicalUUIDv7(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id.String() == value && id.Version() == 7 && id.Variant() == uuid.RFC4122
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
