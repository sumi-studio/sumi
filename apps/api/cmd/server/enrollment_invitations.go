package main

import (
	"context"
	"errors"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
	"time"
)

type enrollmentInvitationAdapter struct{ store *koseki.Store }

func invitationWire(v koseki.EnrollmentInvite) agentevents.EnrollmentInvitation {
	return agentevents.EnrollmentInvitation{ID: v.ID, IssuedBy: v.IssuedBy, Email: v.Email, CreatedAt: v.CreatedAt, ExpiresAt: v.ExpiresAt, RevokedAt: v.RevokedAt, ConsumedAt: v.ConsumedAt, ConsumedBy: v.ConsumedBy}
}
func invitationError(err error) error {
	if errors.Is(err, koseki.ErrEnrollmentInvite) {
		return agentevents.ErrBrowserEnrollmentInvite
	}
	return err
}
func (a enrollmentInvitationAdapter) IssueEnrollmentInvite(ctx context.Context, issuer, email string, ttl time.Duration) (agentevents.EnrollmentInvitation, string, error) {
	v, token, err := a.store.IssueEnrollmentInvite(ctx, issuer, email, ttl)
	return invitationWire(v), token, invitationError(err)
}
func (a enrollmentInvitationAdapter) ListEnrollmentInvites(ctx context.Context) ([]agentevents.EnrollmentInvitation, error) {
	items, err := a.store.ListEnrollmentInvites(ctx)
	out := make([]agentevents.EnrollmentInvitation, 0, len(items))
	for _, v := range items {
		out = append(out, invitationWire(v))
	}
	return out, err
}
func (a enrollmentInvitationAdapter) RevokeEnrollmentInvite(ctx context.Context, id string) error {
	return invitationError(a.store.RevokeEnrollmentInvite(ctx, id))
}
func (a enrollmentInvitationAdapter) InspectEnrollmentInvite(ctx context.Context, token string) (agentevents.EnrollmentInvitation, error) {
	v, err := a.store.InspectEnrollmentInvite(ctx, token)
	return invitationWire(v), invitationError(err)
}
