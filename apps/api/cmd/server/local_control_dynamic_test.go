package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

func TestBuildLocalControlAuthorizersFromKoseki(t *testing.T) {
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	store := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1")

	// Register two agents via auto-registration.
	first, err := store.AutoRegister(ctx, "firebase", "uid-dyn-1")
	if err != nil {
		t.Fatalf("auto-register first: %v", err)
	}
	second, err := store.AutoRegister(ctx, "firebase", "uid-dyn-2")
	if err != nil {
		t.Fatalf("auto-register second: %v", err)
	}

	t.Setenv("SUMI_LOCAL_CONTROL_PERSONALITY_AGENT_ID", first.AgentID)
	authorizations, err := buildLocalControlAuthorizations(
		"shared-bearer", "local", "shared-nonce", 0,
		agentevents.DefaultAgentAudience(), agentevents.LocalDeliveryRaw, pool,
	)
	if err != nil {
		t.Fatalf("build authorizations: %v", err)
	}

	byAgent := make(map[string]agentevents.LocalRuntimeAuthorization, len(authorizations))
	for _, auth := range authorizations {
		byAgent[auth.PersonalityAgentID] = auth
	}
	if len(authorizations) < 2 {
		t.Fatalf("expected at least 2 authorizations, got %d", len(authorizations))
	}
	// The env agent keeps the shared bearer.
	if auth, ok := byAgent[first.AgentID]; !ok || auth.BearerToken != "shared-bearer" {
		t.Fatalf("env agent should keep shared bearer, got %+v", auth)
	}
	// The second agent gets a derived bearer distinct from the shared one.
	secondAuth, ok := byAgent[second.AgentID]
	if !ok {
		t.Fatalf("second 戸籍 agent not registered: %v", byAgent)
	}
	if secondAuth.BearerToken == "shared-bearer" {
		t.Fatal("second agent must not reuse the shared bearer")
	}
	if secondAuth.BearerToken != deriveAgentCredential("shared-bearer", second.AgentID) {
		t.Fatalf("second agent bearer should be derived: got %q", secondAuth.BearerToken)
	}
	if secondAuth.RPCBootNonce != deriveAgentCredential("shared-nonce", second.AgentID) {
		t.Fatalf("second agent nonce should be derived: got %q", secondAuth.RPCBootNonce)
	}
	// Bearers are unique across agents.
	seen := make(map[string]bool, len(authorizations))
	for _, auth := range authorizations {
		if seen[auth.BearerToken] {
			t.Fatalf("duplicate bearer: %q", auth.BearerToken)
		}
		seen[auth.BearerToken] = true
	}
}

func TestBuildLocalControlAuthorizersWithoutEnvAgent(t *testing.T) {
	pool := testdb.Create(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	store := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1")
	if _, err := store.AutoRegister(ctx, "firebase", "uid-noenv-1"); err != nil {
		t.Fatalf("auto-register: %v", err)
	}
	// No env agent ID set: the stack starts from the 戸籍 alone.
	os.Unsetenv("SUMI_LOCAL_CONTROL_PERSONALITY_AGENT_ID")
	authorizations, err := buildLocalControlAuthorizations(
		"shared-bearer", "local", "shared-nonce", 0,
		agentevents.DefaultAgentAudience(), agentevents.LocalDeliveryRaw, pool,
	)
	if err != nil {
		t.Fatalf("build without env agent: %v", err)
	}
	if len(authorizations) != 1 {
		t.Fatalf("expected 1 authorization from 戸籍, got %d", len(authorizations))
	}
	if authorizations[0].BearerToken == "shared-bearer" {
		t.Fatal("non-env agent must use a derived bearer")
	}
}
