package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
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
	reg, err := r.store.AutoRegister(ctx, r.provider, identity.UID)
	if err != nil {
		return agentevents.UserSessionClaims{}, fmt.Errorf("auto-register credential: %w", err)
	}
	return r.claims(reg.HumanID, reg.AgentID), nil
}

func (r *kosekiIdentityBindingResolver) claims(humanID, agentID string) agentevents.UserSessionClaims {
	return agentevents.UserSessionClaims{
		TenantID:           r.tenantID,
		HumanID:            humanID,
		PersonalityAgentID: agentID,
	}
}

// kosekiDirectChatAuthorizer enforces the 私信 Surface contract (ADR 0009 §5):
// raw direct chat is restricted to the agent's current Employer. A Human who is
// not the active Employer (e.g. after 異動 to a Workspace) cannot direct-chat
// with the agent.
type kosekiDirectChatAuthorizer struct {
	store *koseki.Store
}

func newKosekiDirectChatAuthorizer(store *koseki.Store) *kosekiDirectChatAuthorizer {
	return &kosekiDirectChatAuthorizer{store: store}
}

func (a *kosekiDirectChatAuthorizer) AuthorizeDirectChat(
	ctx context.Context,
	humanID,
	personalityAgentID string,
	operation func() error,
) error {
	if err := a.store.AuthorizeCurrentHumanEmployer(
		ctx,
		humanID,
		personalityAgentID,
		operation,
	); err != nil {
		return fmt.Errorf("authorize current Employer operation: %w", err)
	}
	return nil
}
