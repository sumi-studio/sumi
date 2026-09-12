package main

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

// The registry is real Postgres and command/ACK/event logs are real fsynced
// files. Only process launch is replaced; no provider or user effect is run.
func TestPendingWorkColdRestartUsesDurableOriginalCommandAndStopsAfterCompletion(t *testing.T) {
	ctx := context.Background()
	pool := testdb.Create(t)
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	registry := koseki.NewWithWrappingKeyID(pool, "test-wrapping/v1")
	human, err := registry.MintHuman(ctx)
	if err != nil {
		t.Fatal(err)
	}
	id, err := registry.MintSecretary(ctx, human)
	if err != nil {
		t.Fatal(err)
	}
	commandsDir, runtimeDir := t.TempDir(), t.TempDir()
	if err := os.Chmod(runtimeDir, 0700); err != nil {
		t.Fatal(err)
	}
	var store *agentevents.CommandStore
	var gateway *agentevents.DurableGateway
	reopen := func() {
		t.Helper()
		if store != nil {
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
		}
		var err error
		store, err = agentevents.OpenCommandStore(commandsDir)
		if err != nil {
			t.Fatal(err)
		}
		gateway, err = agentevents.OpenDurableGateway(runtimeDir, store)
		if err != nil {
			t.Fatal(err)
		}
	}
	reopen()
	defer func() { _ = store.Close() }()
	claims := agentevents.TokenClaims{PersonalityAgentID: id, Generation: 1}
	if err := gateway.PublishRuntimeState(id, 1, nil); err != nil {
		t.Fatal(err)
	}
	source := agentevents.DirectChatProvenance{Version: 1, TenantID: "tenant-test", PersonalityAgentID: id, Actor: agentevents.ProvenanceActor{Kind: "human", PrincipalID: human}, Source: agentevents.ProvenanceSource{Surface: "direct_chat"}}
	original, err := store.Append(ctx, source, "", json.RawMessage(`{"type":"user_message","text":"accepted before reboot","attachments":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := gateway.ApplyAck(ctx, claims, agentevents.CommandAck{PersonalityAgentID: id, Seq: original.Seq, CommandID: original.CommandID, Status: "received"}); err != nil {
		t.Fatal(err)
	}
	reopen() // API/WSL lost all process-local state, browser never reconnects.
	manager := &pendingTestManager{running: map[string]bool{}, calls: map[string]int{}, denied: map[string]bool{}}
	now := time.Now()
	worker := pendingWorkReconciler{registry: koseki.New(pool), gateway: gateway, manager: manager, now: func() time.Time { return now }, timeout: time.Second, retryBase: time.Second, retryMax: 8 * time.Second, stableAfter: 10 * time.Second}
	if err := worker.reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if manager.calls[id] != 1 {
		t.Fatal("cold accepted work did not launch")
	}
	from, err := gateway.NextCommandSeq(ctx, claims)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := store.CatchUp(ctx, id, from)
	if err != nil {
		t.Fatal(err)
	}
	if len(replay) != 1 || !reflect.DeepEqual(replay[0], original) {
		t.Fatalf("original command identity or source changed: %v", replay)
	}
	// Reconciliation cannot turn the original durable admission into a new one.
	if err := worker.reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if manager.calls[id] != 1 {
		t.Fatal("live process repeatedly touched")
	}
	seq := uint64(1)
	if err := gateway.Receive(ctx, claims, agentevents.Envelope{Audience: agentevents.AudienceDirectChat, PersonalityAgentID: id, Seq: &seq, Event: json.RawMessage(`{"type":"agent_start"}`)}); err != nil {
		t.Fatal(err)
	}
	if err := gateway.ApplyAck(ctx, claims, agentevents.CommandAck{PersonalityAgentID: id, Seq: original.Seq, CommandID: original.CommandID, Status: "applied"}); err != nil {
		t.Fatal(err)
	}
	reopen() // Terminal input ACK alone must not discard an unfinished run.
	worker.gateway = gateway
	worker.attempts = nil
	manager.running[id] = false
	if err := worker.reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if manager.calls[id] != 2 {
		t.Fatal("unclosed run was lost after reboot")
	}
	seq = 2
	if err := gateway.Receive(ctx, claims, agentevents.Envelope{Audience: agentevents.AudienceDirectChat, PersonalityAgentID: id, Seq: &seq, Event: json.RawMessage(`{"type":"agent_end"}`)}); err != nil {
		t.Fatal(err)
	}
	reopen()
	worker.gateway = gateway
	worker.attempts = nil
	manager.running[id] = false
	for i := 0; i < 3; i++ {
		now = now.Add(time.Hour)
		if err := worker.reconcile(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if manager.calls[id] != 2 || len(worker.attempts) != 0 {
		t.Fatal("finished work repeatedly restarted")
	}
	all, err := store.CatchUp(ctx, id, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 || !reflect.DeepEqual(all[0], original) {
		t.Fatal("reconciler duplicated or changed accepted work")
	}
}
