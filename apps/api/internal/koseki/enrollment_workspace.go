package koseki

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
)

// Workspace owns its membership/issuer authority and canonical lock order.
// This seam joins two specific invitation grants, not their consumption steps.
type EnrollmentWorkspaceAuthority interface {
	LockEnrollmentWorkspaceInvite(context.Context, pgx.Tx, string) error
	ValidateEnrollmentWorkspaceInvite(context.Context, pgx.Tx, string) error
}

func (s *Store) lockEnrollmentWorkspace(ctx context.Context, tx pgx.Tx, enrollmentID string, validate bool) (string, error) {
	var workspaceInvite string
	err := tx.QueryRow(ctx, `SELECT COALESCE(workspace_invite_id::text,'') FROM enrollment_invites WHERE invite_id=$1`, enrollmentID).Scan(&workspaceInvite)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrEnrollmentInvite
	}
	if err != nil {
		return "", err
	}
	if workspaceInvite == "" {
		return "", nil
	}
	if s.EnrollmentWorkspaceAuthority == nil {
		return "", ErrEnrollmentInvite
	}
	if err = s.EnrollmentWorkspaceAuthority.LockEnrollmentWorkspaceInvite(ctx, tx, workspaceInvite); err != nil {
		return "", ErrEnrollmentInvite
	}
	if validate {
		if err = s.EnrollmentWorkspaceAuthority.ValidateEnrollmentWorkspaceInvite(ctx, tx, workspaceInvite); err != nil {
			return "", ErrEnrollmentInvite
		}
	}
	return workspaceInvite, nil
}
