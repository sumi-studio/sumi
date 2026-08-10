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
		       wm.workspace_member_id = w.owner_workspace_member_id,
		       COALESCE(array_agg(wra.role_id ORDER BY wr.position DESC, wr.name)
		           FILTER (WHERE wra.role_id IS NOT NULL), ARRAY[]::text[]),
		       wm.joined_at
		FROM workspace_members wm
		JOIN workspaces w ON w.workspace_id = wm.workspace_id
		LEFT JOIN workspace_role_assignments wra
		  ON wra.workspace_id = wm.workspace_id
		 AND wra.workspace_member_id = wm.workspace_member_id
		LEFT JOIN workspace_roles wr
		  ON wr.workspace_id = wra.workspace_id AND wr.role_id = wra.role_id
		WHERE wm.workspace_id = $1 AND wm.left_at IS NULL
		GROUP BY wm.workspace_member_id, wm.workspace_id, wm.member_kind, wm.member_id,
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
			&kind, &member.Participant.ID, &member.Owner, &member.RoleIDs,
			&member.JoinedAt); err != nil {
			return nil, fmt.Errorf("scan workspace member: %w", err)
		}
		member.Participant.Kind = participant.Kind(kind)
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
	target, err := membershipByID(ctx, tx, workspaceID, membershipID, true)
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
	leftAt := s.now().UTC()
	if _, err := tx.Exec(ctx, `
		UPDATE workspace_members SET left_at = $3
		WHERE workspace_id = $1 AND workspace_member_id = $2 AND left_at IS NULL`,
		workspaceID, membershipID, leftAt,
	); err != nil {
		return fmt.Errorf("close workspace membership: %w", err)
	}
	if err := closeBoundPlaceTenures(ctx, tx, workspaceID, membershipID, leftAt); err != nil {
		return err
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
	membership, err := activeMembership(ctx, tx, workspaceID, actor)
	if err != nil {
		return err
	}
	if membership.Owner {
		return ErrOwnerProtected
	}
	leftAt := s.now().UTC()
	if _, err := tx.Exec(ctx, `
		UPDATE workspace_members SET left_at = $3
		WHERE workspace_id = $1 AND workspace_member_id = $2 AND left_at IS NULL`,
		workspaceID, membership.WorkspaceMemberID, leftAt,
	); err != nil {
		return fmt.Errorf("leave workspace: %w", err)
	}
	if err := closeBoundPlaceTenures(ctx, tx, workspaceID,
		membership.WorkspaceMemberID, leftAt); err != nil {
		return err
	}
	if err := ensureEffectiveAdministrator(ctx, tx, workspaceID); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit leave workspace: %w", err)
	}
	return nil
}

func closeBoundPlaceTenures(ctx context.Context, tx pgx.Tx, workspaceID, workspaceMemberID string, leftAt time.Time) error {
	if _, err := tx.Exec(ctx, `
		UPDATE place_members SET left_at = $3
		WHERE workspace_id = $1 AND workspace_member_id = $2 AND left_at IS NULL`,
		workspaceID, workspaceMemberID, leftAt); err != nil {
		return fmt.Errorf("close place membership tenures: %w", err)
	}
	return nil
}

// LockAndRequirePermission is the application-consumer seam used by app
// lifecycle and, after cutover, Messaging. The caller owns tx and commits the
// authorized mutation in that same transaction.
func (s *Store) LockAndRequirePermission(ctx context.Context, tx pgx.Tx, workspaceID string, actor participant.Ref, permission string) error {
	_, err := s.lockAndRequirePermission(ctx, tx, workspaceID, actor, permission)
	return err
}

func (s *Store) lockAndRequirePermission(ctx context.Context, tx pgx.Tx, workspaceID string, actor participant.Ref, permission string) (string, error) {
	if !isKnownPermission(permission) {
		return "", ErrInvalidPermission
	}
	if err := lockWorkspace(ctx, tx, workspaceID); err != nil {
		return "", err
	}
	membership, err := activeMembership(ctx, tx, workspaceID, actor)
	if err != nil {
		return "", err
	}
	permissions, err := permissionsForMembership(ctx, tx, membership)
	if err != nil {
		return "", err
	}
	if !permissions.Can(permission) {
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

func (s *Store) permissionsFor(ctx context.Context, q querier, workspaceID string, actor participant.Ref) (PermissionSet, error) {
	membership, err := activeMembership(ctx, q, workspaceID, actor)
	if err != nil {
		return nil, err
	}
	return permissionsForMembership(ctx, q, membership)
}

func activeMembership(ctx context.Context, q querier, workspaceID string, actor participant.Ref) (Membership, error) {
	if err := actor.Validate(); err != nil {
		return Membership{}, err
	}
	if !isCanonicalUUIDv7(workspaceID) {
		return Membership{}, ErrNotFound
	}
	var membership Membership
	var kind string
	err := q.QueryRow(ctx, `
		SELECT wm.workspace_member_id, wm.workspace_id, wm.member_kind, wm.member_id,
		       wm.workspace_member_id = w.owner_workspace_member_id, wm.joined_at
		FROM workspace_members wm
		JOIN workspaces w ON w.workspace_id = wm.workspace_id
		WHERE wm.workspace_id = $1 AND wm.member_kind = $2 AND wm.member_id = $3
		  AND wm.left_at IS NULL`, workspaceID, actor.Kind, actor.ID,
	).Scan(&membership.WorkspaceMemberID, &membership.WorkspaceID, &kind,
		&membership.Participant.ID, &membership.Owner, &membership.JoinedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Membership{}, ErrNotFound
	}
	if err != nil {
		return Membership{}, fmt.Errorf("load active workspace membership: %w", err)
	}
	membership.Participant.Kind = participant.Kind(kind)
	return membership, nil
}

func membershipByID(ctx context.Context, q querier, workspaceID, membershipID string, activeOnly bool) (Membership, error) {
	if !isCanonicalUUIDv7(workspaceID) || !isCanonicalUUIDv7(membershipID) {
		return Membership{}, ErrMemberNotFound
	}
	activeClause := ""
	if activeOnly {
		activeClause = " AND wm.left_at IS NULL"
	}
	var membership Membership
	var kind string
	err := q.QueryRow(ctx, `
		SELECT wm.workspace_member_id, wm.workspace_id, wm.member_kind, wm.member_id,
		       wm.workspace_member_id = w.owner_workspace_member_id, wm.joined_at
		FROM workspace_members wm
		JOIN workspaces w ON w.workspace_id = wm.workspace_id
		WHERE wm.workspace_id = $1 AND wm.workspace_member_id = $2`+activeClause,
		workspaceID, membershipID,
	).Scan(&membership.WorkspaceMemberID, &membership.WorkspaceID, &kind,
		&membership.Participant.ID, &membership.Owner, &membership.JoinedAt)
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
		return allPermissions(), nil
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
	return permissions, nil
}

func lockWorkspace(ctx context.Context, q querier, workspaceID string) error {
	if !isCanonicalUUIDv7(workspaceID) {
		return ErrNotFound
	}
	var locked string
	err := q.QueryRow(ctx,
		"SELECT workspace_id FROM workspaces WHERE workspace_id = $1 FOR UPDATE", workspaceID,
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
		return ErrForbidden
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

func validatePermissions(input map[string]bool) (PermissionSet, error) {
	for permission, allowed := range input {
		if allowed && !isKnownPermission(permission) {
			return nil, ErrInvalidPermission
		}
	}
	return normalizePermissions(input), nil
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
