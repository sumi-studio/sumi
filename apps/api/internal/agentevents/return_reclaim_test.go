package agentevents

// Reclaim integration: a secretary that left this placement and returned
// must keep every durable command receipt exactly as committed — a replayed
// accepted command keeps its identity, a replayed moved-rejected command
// keeps its terminal rejection and is never admitted as fresh work just
// because the persona is active again.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/portable"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

// remotePlacement is a second isolated database standing in for the Cloud
// side of the move.
type remotePlacement struct {
	state *agentstate.Store
	svc   *portable.Service
}

func newRemotePlacement(t *testing.T) remotePlacement {
	t.Helper()
	pool := testdb.Create(t)
	if err := db.Migrate(context.Background(), pool); err != nil {
		t.Fatalf("migrate remote: %v", err)
	}
	return remotePlacement{state: agentstate.NewStore(pool), svc: portable.NewService(pool)}
}

func TestCoreDirectChatReplayAfterReturnKeepsDispositions(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	ctx := f.ctx

	// The local placement's own portable service for the same database.
	local := portable.NewService(f.pool)
	cloud := newRemotePlacement(t)

	// A command admitted while the secretary lives here.
	env := f.sendMessage(t, "key-before", "sent before the move")
	f.sweep(t)
	before := commandDispositions(t, f.gateway, f.pa)
	if len(before) != 1 || before[0]["status"] != "applied" {
		t.Fatalf("pre-move dispositions: %v", before)
	}

	// Forward move to completion: local sealed → cloud staged → activated →
	// local transferred.
	localID, err := local.PlacementID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cloudID, err := cloud.svc.PlacementID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := local.Seal(ctx, f.pa, "out-dc-0001", cloudID); err != nil {
		t.Fatalf("seal: %v", err)
	}
	var bundle bytes.Buffer
	if _, err := local.Export(ctx, f.pa, "out-dc-0001", &bundle); err != nil {
		t.Fatalf("export: %v", err)
	}
	if _, created, err := cloud.svc.Import(ctx, &bundle, nil, false); err != nil || !created {
		t.Fatalf("cloud import: created=%v err=%v", created, err)
	}
	act, err := cloud.svc.Activate(ctx, f.pa, "out-dc-0001")
	if err != nil {
		t.Fatalf("cloud activate: %v", err)
	}
	if _, err := local.Complete(ctx, f.pa, "out-dc-0001", act.ActivateProof); err != nil {
		t.Fatalf("local complete: %v", err)
	}

	// A message sent while the secretary lives on Cloud is durable and
	// terminally rejected.
	movedEnv, err := f.adapter.Append(f.ctx, f.provenance(), "key-moved",
		json.RawMessage(`{"type":"user_message","text":"while you were away","attachments":[]}`))
	if !errors.Is(err, agentstate.ErrPersonaTransferred) {
		t.Fatalf("moved append: %v", err)
	}
	f.sweep(t)
	dispositions := commandDispositions(t, f.gateway, f.pa)
	if len(dispositions) != 2 ||
		dispositions[1]["command_id"] != movedEnv.CommandID ||
		dispositions[1]["status"] != "rejected" ||
		dispositions[1]["reject_reason"] != string(RejectSecretaryMoved) {
		t.Fatalf("post-move dispositions: %v", dispositions)
	}

	// Return: cloud seals the continuation back to this placement; the
	// surrendered copy is reclaimed and reactivated.
	if _, err := cloud.svc.Seal(ctx, f.pa, "home-dc-0001", localID); err != nil {
		t.Fatalf("return seal: %v", err)
	}
	var home bytes.Buffer
	if _, err := cloud.svc.Export(ctx, f.pa, "home-dc-0001", &home); err != nil {
		t.Fatalf("return export: %v", err)
	}
	if _, created, err := local.ImportReturning(ctx, &home, nil, false, "out-dc-0001"); err != nil || !created {
		t.Fatalf("reclaim: created=%v err=%v", created, err)
	}
	ret, err := local.Activate(ctx, f.pa, "home-dc-0001")
	if err != nil {
		t.Fatalf("return activate: %v", err)
	}
	if _, err := cloud.svc.Complete(ctx, f.pa, "home-dc-0001", ret.ActivateProof); err != nil {
		t.Fatalf("cloud complete: %v", err)
	}
	if st, err := f.core.PersonaState(ctx, f.pa); err != nil || st.Persona.Authority != "active" {
		t.Fatalf("persona after return: %v %+v", err, st.Persona)
	}

	// Replay of the pre-move accepted command keeps its durable identity.
	replay, err := f.adapter.Append(f.ctx, f.provenance(), "key-before",
		json.RawMessage(`{"type":"user_message","text":"sent before the move","attachments":[]}`))
	if err != nil || replay.CommandID != env.CommandID || replay.Seq != env.Seq {
		t.Fatalf("accepted replay: env=%+v err=%v", replay, err)
	}

	// Replay of the moved-rejected command must remain the same terminal
	// result — not a fresh admission now that the persona is active again.
	replayMoved, err := f.adapter.Append(f.ctx, f.provenance(), "key-moved",
		json.RawMessage(`{"type":"user_message","text":"while you were away","attachments":[]}`))
	if !errors.Is(err, agentstate.ErrPersonaTransferred) {
		t.Fatalf("moved replay must stay terminal: env=%+v err=%v", replayMoved, err)
	}
	if replayMoved.CommandID != movedEnv.CommandID || replayMoved.Seq != movedEnv.Seq {
		t.Fatalf("moved replay lost identity: %+v vs %+v", replayMoved, movedEnv)
	}
	if _, _, err := f.core.GetInput(ctx, f.pa, "direct-chat:"+movedEnv.CommandID); !errors.Is(err, agentstate.ErrInputNotFound) {
		t.Fatalf("rejected command admitted a core input after return: %v", err)
	}

	// A restarted projector rescanning every command emits nothing new and
	// never rewrites the moved rejection.
	restarted := &CoreDirectChat{Core: f.core, Gateway: f.gateway}
	if err := restarted.syncPersona(ctx, f.pa); err != nil {
		t.Fatalf("resync after restart: %v", err)
	}
	after := commandDispositions(t, f.gateway, f.pa)
	if len(after) != 2 || after[1]["reject_reason"] != string(RejectSecretaryMoved) {
		t.Fatalf("dispositions after return+restart: %v", after)
	}

	// And the returned secretary takes new work here again.
	newEnv, err := f.adapter.Append(f.ctx, f.provenance(), "key-home",
		json.RawMessage(`{"type":"user_message","text":"back home","attachments":[]}`))
	if err != nil || newEnv.CommandID == "" {
		t.Fatalf("new command after return: env=%+v err=%v", newEnv, err)
	}
	if _, _, err := f.core.GetInput(ctx, f.pa, "direct-chat:"+newEnv.CommandID); err != nil {
		t.Fatalf("new command did not land a core input: %v", err)
	}
}
