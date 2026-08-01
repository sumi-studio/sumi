package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

func kosekiResolverTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool
}

func TestKosekiResolverAutoRegistersAndResolves(t *testing.T) {
	pool := kosekiResolverTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	resolver := newKosekiIdentityBindingResolver(koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1"), "local", "firebase")
	store := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1")

	// First account: auto-registration mints a Human + Secretary.
	first, err := resolver.ResolveIdentity(ctx, agentevents.FirebaseIdentity{
		UID: "firebase-uid-aaa", DisplayName: "  First\nHuman  ",
	})
	if err != nil {
		t.Fatalf("resolve first identity: %v", err)
	}
	if first.TenantID != "local" {
		t.Fatalf("tenant id: got %q want local", first.TenantID)
	}
	if first.UserID == "" || first.PersonalityAgentID == "" {
		t.Fatal("auto-registration must produce a HumanId and PersonalityAgentID")
	}
	if first.UserID == first.PersonalityAgentID {
		t.Fatal("HumanId and PersonalityAgentID must differ")
	}
	if got, err := store.HumanDisplayName(ctx, first.UserID); err != nil || got != "First Human" {
		t.Fatalf("verified initial display name = %q, %v", got, err)
	}
	// Per-agent wrapping key is generated at registration.
	firstKey, err := store.AgentWrappingKey(ctx, first.PersonalityAgentID)
	if err != nil {
		t.Fatalf("wrapping key for first agent: %v", err)
	}
	if firstKey.ID != "test-wrapping/v1" || len(firstKey.Bytes) != 64 {
		t.Fatalf("wrapping key pair mismatch: id=%q bytes=%d", firstKey.ID, len(firstKey.Bytes))
	}

	// Known credential resolves to the same HumanId and agent (no re-registration).
	firstAgain, err := resolver.ResolveIdentity(ctx, agentevents.FirebaseIdentity{UID: "firebase-uid-aaa", DisplayName: "Later Provider Name"})
	if err != nil {
		t.Fatalf("resolve known identity: %v", err)
	}
	if firstAgain.UserID != first.UserID || firstAgain.PersonalityAgentID != first.PersonalityAgentID {
		t.Fatalf("known credential resolved differently: first=%+v again=%+v", first, firstAgain)
	}
	if got, _ := store.HumanDisplayName(ctx, first.UserID); got != "First Human" {
		t.Fatalf("later provider name overwrote initial label: %q", got)
	}

	// Second account: a distinct Human + Secretary, auto-registered.
	second, err := resolver.ResolveIdentity(ctx, agentevents.FirebaseIdentity{UID: "firebase-uid-bbb"})
	if err != nil {
		t.Fatalf("resolve second identity: %v", err)
	}
	if second.UserID == first.UserID || second.PersonalityAgentID == first.PersonalityAgentID {
		t.Fatal("second account must get a distinct HumanId and PersonalityAgentID")
	}
	secondKey, err := store.AgentWrappingKey(ctx, second.PersonalityAgentID)
	if err != nil {
		t.Fatalf("wrapping key for second agent: %v", err)
	}
	if secondKey.ID != "test-wrapping/v1" || len(secondKey.Bytes) != 64 {
		t.Fatalf("second wrapping key pair mismatch: id=%q bytes=%d", secondKey.ID, len(secondKey.Bytes))
	}

	// Each Human has exactly one Secretary that round-trips through the store.
	firstAgent, err := store.AgentForHuman(ctx, first.UserID)
	if err != nil {
		t.Fatalf("agent for first human: %v", err)
	}
	if firstAgent != first.PersonalityAgentID {
		t.Fatalf("agent mismatch: got %q want %q", firstAgent, first.PersonalityAgentID)
	}

	// An unbound credential that is not in the registry returns ErrNoRows from the
	// store (the resolver auto-registers instead, so this checks the lookup path).
	if _, err := store.ResolveCredential(ctx, "firebase", "never-bound"); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("expected ErrNoRows for unbound credential, got %v", err)
	}
}

func TestKosekiDirectChatAuthorizerEnforcesEmployer(t *testing.T) {
	pool := kosekiResolverTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	store := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1")
	authorizer := newKosekiDirectChatAuthorizer(store)

	// Two Humans, each with their own Secretary.
	first, err := store.AutoRegister(ctx, "firebase", "uid-employer-1")
	if err != nil {
		t.Fatalf("auto-register first: %v", err)
	}
	second, err := store.AutoRegister(ctx, "firebase", "uid-employer-2")
	if err != nil {
		t.Fatalf("auto-register second: %v", err)
	}

	// Each Human is the Employer of their own Secretary: direct chat allowed.
	if err := authorizer.AuthorizeDirectChat(ctx, first.HumanID, first.AgentID, func() error { return nil }); err != nil {
		t.Fatalf("owner should be authorized for own secretary: %v", err)
	}
	// A Human is NOT the Employer of another Human's Secretary: rejected.
	if err := authorizer.AuthorizeDirectChat(ctx, second.HumanID, first.AgentID, func() error { return nil }); err == nil {
		t.Fatal("non-employer human must not direct-chat with another's secretary")
	}

	// 異動: transfer the first agent's employment to the second Human. The first
	// Human is no longer the Employer and loses direct-chat access.
	if err := store.TransferEmployment(
		ctx,
		first.AgentID,
		koseki.EmployerHuman,
		second.HumanID,
	); err != nil {
		t.Fatalf("transfer employment: %v", err)
	}
	if err := authorizer.AuthorizeDirectChat(ctx, first.HumanID, first.AgentID, func() error { return nil }); err == nil {
		t.Fatal("former employer must lose direct-chat access after 異動")
	}
	if err := authorizer.AuthorizeDirectChat(ctx, second.HumanID, first.AgentID, func() error { return nil }); err != nil {
		t.Fatalf("new employer should be authorized after 異動: %v", err)
	}
}
