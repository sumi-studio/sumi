package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	applicationapps "github.com/sumi-studio/sumi/apps/api/internal/apps"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
	"github.com/sumi-studio/sumi/apps/api/internal/participant"
)

// kosekiIdentityBindingResolver replaces StaticIdentityBindingResolver with a
// 戸籍-backed implementation (ADR 0009 §3). A verified Firebase identity is
// mapped to Sumi claims via the credential registry: known credentials resolve
// to their existing HumanId and Secretary; unbound credentials trigger first-
// login auto-registration (mint HumanId + Secretary + per-agent secrets + bind
// the credential).
type kosekiIdentityBindingResolver struct {
	store    *koseki.Store
	tenantID string // deployment provenance tenant, e.g. "local"
	provider string // credential provider, e.g. "firebase"
}

func newKosekiIdentityBindingResolver(store *koseki.Store, tenantID, provider string) *kosekiIdentityBindingResolver {
	if provider == "" {
		provider = "firebase"
	}
	return &kosekiIdentityBindingResolver{store: store, tenantID: tenantID, provider: provider}
}

func (r *kosekiIdentityBindingResolver) ResolveIdentity(
	ctx context.Context,
	identity agentevents.FirebaseIdentity,
) (agentevents.UserSessionClaims, error) {
	select {
	case <-ctx.Done():
		return agentevents.UserSessionClaims{}, ctx.Err()
	default:
	}
	humanID, err := r.store.ResolveCredential(ctx, r.provider, identity.UID)
	if err == nil {
		if err := r.store.SeedHumanDisplayName(ctx, humanID, identity.DisplayName); err != nil {
			return agentevents.UserSessionClaims{}, fmt.Errorf("seed Human display name: %w", err)
		}
		agentID, aerr := r.store.AgentForHuman(ctx, humanID)
		if aerr != nil {
			return agentevents.UserSessionClaims{}, fmt.Errorf("resolve secretary for human %s: %w", humanID, aerr)
		}
		return r.claims(humanID, agentID), nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return agentevents.UserSessionClaims{}, fmt.Errorf("lookup credential: %w", err)
	}
	// Unbound credential: auto-register (first-login self-serve signup).
	reg, err := r.store.AutoRegisterWithDisplayName(ctx, r.provider, identity.UID, identity.DisplayName)
	if err != nil {
		return agentevents.UserSessionClaims{}, fmt.Errorf("auto-register credential: %w", err)
	}
	return r.claims(reg.HumanID, reg.AgentID), nil
}

func (r *kosekiIdentityBindingResolver) claims(humanID, agentID string) agentevents.UserSessionClaims {
	return agentevents.UserSessionClaims{
		TenantID:           r.tenantID,
		UserID:             humanID,
		PersonalityAgentID: agentID,
	}
}

// directChatAuthorizer composes the 私信 Surface's two independent authority
// sources under one transaction: Current Human Employer first, then the exact
// enabled Human-owned direct-chat AppInstallation. Durable/private operations
// run while both shared leases are held.
type directChatAuthorizer struct {
	pool   *pgxpool.Pool
	koseki *koseki.Store
	apps   *applicationapps.Store
}

func newDirectChatAuthorizer(
	pool *pgxpool.Pool,
	kosekiStore *koseki.Store,
	appStore *applicationapps.Store,
) *directChatAuthorizer {
	return &directChatAuthorizer{pool: pool, koseki: kosekiStore, apps: appStore}
}

func (a *directChatAuthorizer) AuthorizeDirectChat(
	ctx context.Context,
	humanID,
	personalityAgentID,
	installationID string,
	authorityEpoch int64,
) error {
	if a == nil || a.pool == nil || a.koseki == nil || a.apps == nil {
		return agentevents.ErrDirectChatAuthorizationUnavailable
	}
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("%w: begin composite authority: %v", agentevents.ErrDirectChatAuthorizationUnavailable, err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := a.koseki.RequireCurrentHumanEmployerInTx(ctx, tx, humanID, personalityAgentID); err != nil {
		if errors.Is(err, koseki.ErrNotCurrentEmployer) {
			return agentevents.ErrDirectChatAuthorizationDenied
		}
		return fmt.Errorf("%w: require current Employer: %v", agentevents.ErrDirectChatAuthorizationUnavailable, err)
	}
	if _, err := a.apps.RequireEnabledInstallationEpochInTx(
		ctx,
		tx,
		installationID,
		authorityEpoch,
		applicationapps.ParticipantOwner(participant.Human(humanID)),
		"direct-chat",
	); err != nil {
		if errors.Is(err, applicationapps.ErrInstallationNotFound) ||
			errors.Is(err, applicationapps.ErrAppDisabled) {
			return agentevents.ErrDirectChatAuthorizationDenied
		}
		return fmt.Errorf("%w: require direct-chat installation: %v", agentevents.ErrDirectChatAuthorizationUnavailable, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: commit composite authority: %v", agentevents.ErrDirectChatAuthorizationUnavailable, err)
	}
	return nil
}
