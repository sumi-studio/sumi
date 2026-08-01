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
	resolver := newKosekiIdentityBindingResolver(koseki.New(pool), "local", "firebase")
	store := koseki.New(pool)

	// First account: auto-registration mints a Human + Secretary.
	first, err := resolver.ResolveIdentity(ctx, agentevents.FirebaseIdentity{UID: "firebase-uid-aaa"})
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
	// Per-agent wrapping key is generated at registration.
	if _, err := store.AgentWrappingKey(ctx, first.PersonalityAgentID); err != nil {
		t.Fatalf("wrapping key for first agent: %v", err)
	}

	// Known credential resolves to the same HumanId and agent (no re-registration).
	firstAgain, err := resolver.ResolveIdentity(ctx, agentevents.FirebaseIdentity{UID: "firebase-uid-aaa"})
	if err != nil {
		t.Fatalf("resolve known identity: %v", err)
	}
	if firstAgain.UserID != first.UserID || firstAgain.PersonalityAgentID != first.PersonalityAgentID {
		t.Fatalf("known credential resolved differently: first=%+v again=%+v", first, firstAgain)
	}

	// Second account: a distinct Human + Secretary, auto-registered.
	second, err := resolver.ResolveIdentity(ctx, agentevents.FirebaseIdentity{UID: "firebase-uid-bbb"})
	if err != nil {
		t.Fatalf("resolve second identity: %v", err)
	}
	if second.UserID == first.UserID || second.PersonalityAgentID == first.PersonalityAgentID {
		t.Fatal("second account must get a distinct HumanId and PersonalityAgentID")
	}
	if _, err := store.AgentWrappingKey(ctx, second.PersonalityAgentID); err != nil {
		t.Fatalf("wrapping key for second agent: %v", err)
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
