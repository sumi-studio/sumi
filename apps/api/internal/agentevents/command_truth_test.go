package agentevents

// Command-truth repair coverage (F352/F356): a caller-visible terminal
// rejection is committed before it is answered, so a command told
// "rejected" can never become work after a crash, a stopped reconciler,
// or a placement return — and a replayed committed rejection reports the
// same outcome on every surface instead of a bare acceptance.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/directchat"
	"github.com/sumi-studio/sumi/apps/api/internal/portable"
)

func truthUserMessage(text string) json.RawMessage {
	raw, _ := json.Marshal(map[string]any{
		"type":        "user_message",
		"text":        text,
		"attachments": []any{},
	})
	return raw
}

// movePersonaAway completes a forward transfer so the local persona is
// authority=transferred — the state every moved rejection is committed
// under.
func movePersonaAway(t *testing.T, f *coreDirectChatFixture, transferID string) (*portable.Service, remotePlacement) {
	t.Helper()
	local := portable.NewService(f.pool)
	cloud := newRemotePlacement(t)
	cloudID, err := cloud.svc.PlacementID(f.ctx)
	if err != nil {
		t.Fatalf("cloud placement id: %v", err)
	}
	if _, err := local.Seal(f.ctx, f.pa, transferID, cloudID); err != nil {
		t.Fatalf("seal: %v", err)
	}
	var bundle bytes.Buffer
	if _, err := local.Export(f.ctx, f.pa, transferID, &bundle); err != nil {
		t.Fatalf("export: %v", err)
	}
	if _, created, err := cloud.svc.Import(f.ctx, &bundle, nil, false); err != nil || !created {
		t.Fatalf("cloud import: created=%v err=%v", created, err)
	}
	act, err := cloud.svc.Activate(f.ctx, f.pa, transferID)
	if err != nil {
		t.Fatalf("cloud activate: %v", err)
	}
	if _, err := local.Complete(f.ctx, f.pa, transferID, act.ActivateProof); err != nil {
		t.Fatalf("local complete: %v", err)
	}
	return local, cloud
}

// returnPersona reclaims the surrendered copy and reactivates the persona
// on this placement.
func returnPersona(t *testing.T, f *coreDirectChatFixture, local *portable.Service, cloud remotePlacement, outID, homeID string) {
	t.Helper()
	localID, err := local.PlacementID(f.ctx)
	if err != nil {
		t.Fatalf("local placement id: %v", err)
	}
	if _, err := cloud.svc.Seal(f.ctx, f.pa, homeID, localID); err != nil {
		t.Fatalf("return seal: %v", err)
	}
	var home bytes.Buffer
	if _, err := cloud.svc.Export(f.ctx, f.pa, homeID, &home); err != nil {
		t.Fatalf("return export: %v", err)
	}
	if _, created, err := local.ImportReturning(f.ctx, &home, nil, false, outID); err != nil || !created {
		t.Fatalf("reclaim: created=%v err=%v", created, err)
	}
	ret, err := local.Activate(f.ctx, f.pa, homeID)
	if err != nil {
		t.Fatalf("return activate: %v", err)
	}
	if _, err := cloud.svc.Complete(f.ctx, f.pa, homeID, ret.ActivateProof); err != nil {
		t.Fatalf("cloud complete: %v", err)
	}
	if st, err := f.core.PersonaState(f.ctx, f.pa); err != nil || st.Persona.Authority != "active" {
		t.Fatalf("persona after return: %v %+v", err, st.Persona)
	}
}

func requireDisposition(t *testing.T, g *DurableGateway, env CommandEnvelope, wantStatus, wantReason string) {
	t.Helper()
	disposition, found, err := g.CommandDispositionFor(context.Background(), env)
	if err != nil || !found {
		t.Fatalf("command has no committed disposition: found=%v err=%v", found, err)
	}
	var d struct {
		Status       string `json:"status"`
		RejectReason string `json:"reject_reason"`
	}
	if err := json.Unmarshal(disposition, &d); err != nil {
		t.Fatalf("decode disposition: %v", err)
	}
	if d.Status != wantStatus || d.RejectReason != wantReason {
		t.Fatalf("disposition = %s/%s, want %s/%s", d.Status, d.RejectReason, wantStatus, wantReason)
	}
}

func requireNoInput(t *testing.T, f *coreDirectChatFixture, commandID string) {
	t.Helper()
	if _, _, err := f.core.GetInput(f.ctx, f.pa, "direct-chat:"+commandID); !errors.Is(err, agentstate.ErrInputNotFound) {
		t.Fatalf("rejected command minted core input %s: %v", commandID, err)
	}
}

// The terminal moved answer itself commits the rejection: with no
// reconciler pass between the answer and a full return/reactivation, a
// replay from a fresh process still answers moved and the denied input is
// never minted. (F352)
func TestCoreDirectChatMovedRejectionCommittedBeforeAnswer(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	ctx := f.ctx

	// Load the projector's disposition cache before the move so the
	// committed receipt lands strictly out-of-band for it.
	f.sweep(t)

	local, cloud := movePersonaAway(t, f, "out-tr-0001")

	movedEnv, err := f.adapter.Append(ctx, f.provenance(), "key-moved",
		truthUserMessage("while you were away"))
	if !errors.Is(err, agentstate.ErrPersonaTransferred) {
		t.Fatalf("moved append: %v", err)
	}
	// No sweep ran: the synchronous answer itself had to commit the
	// rejection for the guarantee to hold across a crash here.
	requireDisposition(t, f.gateway, movedEnv, "rejected", string(RejectSecretaryMoved))

	returnPersona(t, f, local, cloud, "out-tr-0001", "home-tr-0001")

	// Replay from a new process — a second gateway over the same durable
	// dir whose only shared truth is the committed log — keeps the same
	// terminal answer and never dispatches.
	restarted, restartedGW := f.newProjector(t)
	replay, err := restarted.Append(ctx, f.provenance(), "key-moved",
		truthUserMessage("while you were away"))
	if !errors.Is(err, agentstate.ErrPersonaTransferred) {
		t.Fatalf("moved replay must stay terminal: env=%+v err=%v", replay, err)
	}
	if replay.CommandID != movedEnv.CommandID || replay.Seq != movedEnv.Seq {
		t.Fatalf("moved replay lost identity: %+v vs %+v", replay, movedEnv)
	}
	requireDisposition(t, restartedGW, movedEnv, "rejected", string(RejectSecretaryMoved))
	requireNoInput(t, f, movedEnv.CommandID)

	// The reconciler — whose cache predates the out-of-band receipt — must
	// consult committed truth instead of reviving the command now that the
	// persona accepts work again.
	f.sweep(t)
	requireNoInput(t, f, movedEnv.CommandID)
	dispositions := commandDispositions(t, f.gateway, f.pa)
	if len(dispositions) != 1 ||
		dispositions[0]["status"] != "rejected" ||
		dispositions[0]["reject_reason"] != string(RejectSecretaryMoved) {
		t.Fatalf("dispositions after return+sweep: %v", dispositions)
	}

	// And the returned secretary takes new work here again.
	newEnv := f.sendMessage(t, "key-home", "back home")
	if _, _, err := f.core.GetInput(ctx, f.pa, "direct-chat:"+newEnv.CommandID); err != nil {
		t.Fatalf("new command did not land a core input: %v", err)
	}
}

// A committed non-moved rejection is answered as the rejection it is — on
// first admission, on replay, and after the persona accepts work again —
// never a bare acceptance. (F352 not_allowed + F356 + stale-cache
// reconciler check)
func TestCoreDirectChatSealedRejectionCommittedAndSurvivesReactivation(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	ctx := f.ctx

	// Load the projector's cache first: the committed receipt below must
	// be found via the durable check, not the in-memory map.
	f.sweep(t)

	local := portable.NewService(f.pool)
	cloud := newRemotePlacement(t)
	cloudID, err := cloud.svc.PlacementID(ctx)
	if err != nil {
		t.Fatalf("cloud placement id: %v", err)
	}
	if _, err := local.Seal(ctx, f.pa, "out-tr-0002", cloudID); err != nil {
		t.Fatalf("seal: %v", err)
	}

	env, err := f.adapter.Append(ctx, f.provenance(), "key-sealed",
		truthUserMessage("mid move"))
	var rejection *CommandRejectionError
	if !errors.As(err, &rejection) || rejection.Reason != RejectNotAllowed {
		t.Fatalf("sealed append: env=%+v err=%v", env, err)
	}
	// Committed before the answer returned, no reconciler involved.
	requireDisposition(t, f.gateway, env, "rejected", string(RejectNotAllowed))

	// Replay answers the same committed outcome — not a bare receipt.
	replay, err := f.adapter.Append(ctx, f.provenance(), "key-sealed",
		truthUserMessage("mid move"))
	rejection = nil
	if !errors.As(err, &rejection) || rejection.Reason != RejectNotAllowed {
		t.Fatalf("sealed replay: env=%+v err=%v", replay, err)
	}
	if replay.CommandID != env.CommandID || replay.Seq != env.Seq {
		t.Fatalf("rejected replay lost identity: %+v vs %+v", replay, env)
	}

	// Reactivate the persona with no reconciler pass in between: the
	// durable receipt alone must prevent the denied input now that the
	// in-memory cache cannot know about it.
	if _, err := f.pool.Exec(ctx,
		`UPDATE core_personas SET authority = 'active', transfer_id = NULL WHERE persona_id = $1`, f.pa); err != nil {
		t.Fatalf("reactivate persona: %v", err)
	}
	f.sweep(t)
	requireNoInput(t, f, env.CommandID)
	dispositions := commandDispositions(t, f.gateway, f.pa)
	if len(dispositions) != 1 ||
		dispositions[0]["status"] != "rejected" ||
		dispositions[0]["reject_reason"] != string(RejectNotAllowed) {
		t.Fatalf("dispositions after reactivation+sweep: %v", dispositions)
	}

	// The rejection is terminal for that command only — new work lands.
	newEnv := f.sendMessage(t, "key-after", "hello again")
	if _, _, err := f.core.GetInput(ctx, f.pa, "direct-chat:"+newEnv.CommandID); err != nil {
		t.Fatalf("new command did not land a core input: %v", err)
	}
}

// A receipt that cannot be persisted is answered as its own failure —
// never as the terminal rejection the log cannot back — and the undecided
// command stays recoverable by the reconciler. (F352 failed persistence)
func TestCoreDirectChatFailedReceiptPersistenceStaysUndecided(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	ctx := f.ctx
	movePersonaAway(t, f, "out-tr-0003")

	// A directory at the event path fails every read/write of the log.
	eventPath := f.gateway.eventPath(f.pa)
	if err := os.Mkdir(eventPath, 0o700); err != nil {
		t.Fatalf("break event log: %v", err)
	}
	env, err := f.adapter.Append(ctx, f.provenance(), "key-broken",
		truthUserMessage("unrecordable"))
	if err == nil ||
		errors.Is(err, agentstate.ErrPersonaTransferred) ||
		errors.Is(err, agentstate.ErrPersonaInactive) {
		t.Fatalf("persistence failure must not answer a terminal rejection: %v", err)
	}
	var rejection *CommandRejectionError
	if errors.As(err, &rejection) {
		t.Fatalf("persistence failure must not answer a committed rejection: %v", err)
	}
	if err := os.Remove(eventPath); err != nil {
		t.Fatalf("restore event path: %v", err)
	}

	// The command was never answered terminally, so it remains the
	// reconciler's undecided work — and is closed now.
	f.sweep(t)
	requireDisposition(t, f.gateway, env, "rejected", string(RejectSecretaryMoved))
}

// The durable-command-before-core-write crash window is still recoverable
// undecided work: an admitted command with no disposition dispatches on
// replay and lands its input. (Preserve the recovery claim)
func TestCoreDirectChatUndecidedCommandStillRecovers(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	ctx := f.ctx

	// The durable command append committed; the process died before the
	// dispatch ever ran — no input, no disposition.
	raw := truthUserMessage("crash between append and dispatch")
	env, existing, err := f.gateway.commands.appendWithIdempotencyStatus(
		ctx, f.provenance(), "key-crash", raw)
	if err != nil || existing {
		t.Fatalf("seed durable command: existing=%v err=%v", existing, err)
	}
	if _, found, err := f.gateway.CommandDispositionFor(ctx, env); err != nil || found {
		t.Fatalf("seeded command unexpectedly disposed: found=%v err=%v", found, err)
	}

	replay, err := f.adapter.Append(ctx, f.provenance(), "key-crash", raw)
	if err != nil {
		t.Fatalf("undecided replay must dispatch, not terminalize: %v", err)
	}
	if replay.CommandID != env.CommandID || replay.Seq != env.Seq {
		t.Fatalf("replay lost identity: %+v vs %+v", replay, env)
	}
	if _, _, err := f.core.GetInput(ctx, f.pa, "direct-chat:"+env.CommandID); err != nil {
		t.Fatalf("undecided command did not land its input: %v", err)
	}
	f.sweep(t)
	requireDisposition(t, f.gateway, env, "applied", "")
}

// The synchronous receipt and the reconciler race for the same dedup
// identity: exactly one command_disposition may ever commit per command,
// and no denied input may appear. (Immediate append vs projector race)
func TestCoreDirectChatConcurrentReceiptAndReconcilerCommitOnce(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	ctx := f.ctx
	movePersonaAway(t, f, "out-tr-0004")
	f.sweep(t)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 16; i++ {
			_ = f.adapter.syncPersona(ctx, f.pa)
		}
	}()
	envs := make([]CommandEnvelope, 0, 8)
	for i := 0; i < 8; i++ {
		env, err := f.adapter.Append(ctx, f.provenance(),
			fmt.Sprintf("key-race-%d", i), truthUserMessage("raced rejection"))
		if !errors.Is(err, agentstate.ErrPersonaTransferred) {
			t.Fatalf("raced append %d: env=%+v err=%v", i, env, err)
		}
		envs = append(envs, env)
	}
	wg.Wait()
	f.sweep(t)

	dispositions := commandDispositions(t, f.gateway, f.pa)
	if len(dispositions) != len(envs) {
		t.Fatalf("duplicate or missing dispositions: %d commands, %d dispositions: %v",
			len(envs), len(dispositions), dispositions)
	}
	for _, env := range envs {
		requireDisposition(t, f.gateway, env, "rejected", string(RejectSecretaryMoved))
		requireNoInput(t, f, env.CommandID)
	}
}

// A dedup index lost wholesale is rebuilt from the committed log — the
// reconstructed command-scoped key still refuses a second receipt for the
// already-rejected command. (Missing-index recovery)
func TestCoreDirectChatMissingDedupIndexRecovers(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	ctx := f.ctx
	local := portable.NewService(f.pool)
	cloud := newRemotePlacement(t)
	cloudID, err := cloud.svc.PlacementID(ctx)
	if err != nil {
		t.Fatalf("cloud placement id: %v", err)
	}
	if _, err := local.Seal(ctx, f.pa, "out-tr-0005", cloudID); err != nil {
		t.Fatalf("seal: %v", err)
	}
	env, err := f.adapter.Append(ctx, f.provenance(), "key-index",
		truthUserMessage("index will be lost"))
	var rejection *CommandRejectionError
	if !errors.As(err, &rejection) || rejection.Reason != RejectNotAllowed {
		t.Fatalf("sealed append: %v", err)
	}
	requireDisposition(t, f.gateway, env, "rejected", string(RejectNotAllowed))

	if err := os.Remove(f.gateway.dedupIndexPath(f.pa)); err != nil {
		t.Fatalf("drop dedup index: %v", err)
	}
	// Replay still answers committed truth — the lookup scans the log,
	// not the lost index.
	if _, err := f.adapter.Append(ctx, f.provenance(), "key-index",
		truthUserMessage("index will be lost")); !errors.As(err, &rejection) {
		t.Fatalf("replay after index loss: %v", err)
	}

	// Once the persona accepts work again, ordinary projected writes
	// rebuild the index — and the rebuilt command-scoped key still refuses
	// a second receipt for the rejected command.
	if _, err := f.pool.Exec(ctx,
		`UPDATE core_personas SET authority = 'active', transfer_id = NULL WHERE persona_id = $1`, f.pa); err != nil {
		t.Fatalf("reactivate persona: %v", err)
	}
	newEnv := f.sendMessage(t, "key-index-new", "after index loss")
	f.sweep(t)
	requireDisposition(t, f.gateway, newEnv, "applied", "")
	requireNoInput(t, f, env.CommandID)
	committed, err := f.gateway.CommitCommandDisposition(ctx, env,
		dispositionEvent(env, "rejected", string(RejectNotAllowed)))
	if err != nil || committed {
		t.Fatalf("rebuilt index must refuse a second receipt: committed=%v err=%v", committed, err)
	}
}

// A phantom preimage — a dedup record for a receipt that never committed —
// is truncated at load and never suppresses the real commit. (Preimage
// ordering)
func TestCoreDirectChatPhantomIndexRecordDoesNotSuppressReceipt(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	ctx := f.ctx
	raw := truthUserMessage("phantom preimage")
	env, _, err := f.gateway.commands.appendWithIdempotencyStatus(
		ctx, f.provenance(), "key-phantom", raw)
	if err != nil {
		t.Fatalf("seed durable command: %v", err)
	}
	index, err := os.OpenFile(f.gateway.dedupIndexPath(f.pa), os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open dedup index: %v", err)
	}
	key := commandDispositionKey(env.CommandID)
	if _, err := index.Write(key[:]); err != nil {
		t.Fatalf("write phantom key: %v", err)
	}
	if err := index.Sync(); err != nil {
		t.Fatalf("sync phantom key: %v", err)
	}
	if err := index.Close(); err != nil {
		t.Fatalf("close index: %v", err)
	}

	committed, err := f.gateway.CommitCommandDisposition(ctx, env,
		dispositionEvent(env, "rejected", string(RejectNotAllowed)))
	if err != nil || !committed {
		t.Fatalf("phantom preimage suppressed the real receipt: committed=%v err=%v", committed, err)
	}
	requireDisposition(t, f.gateway, env, "rejected", string(RejectNotAllowed))
}

// A torn tail left by a crashed writer is repaired under the lock and
// disposition truth still answers — then ordinary admission proceeds.
// (Torn-tail recovery)
func TestCoreDirectChatTornTailRepairsDispositionTruth(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	ctx := f.ctx
	raw := truthUserMessage("commit me")
	env, err := f.adapter.Append(ctx, f.provenance(), "key-torn", raw)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	f.sweep(t)
	requireDisposition(t, f.gateway, env, "applied", "")

	file, err := os.OpenFile(f.gateway.eventPath(f.pa), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open event log: %v", err)
	}
	if _, err := file.WriteString(`{"seq":99,"event":{"bogus`); err != nil {
		t.Fatalf("tear tail: %v", err)
	}
	if err := file.Sync(); err != nil {
		t.Fatalf("sync torn tail: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close event log: %v", err)
	}

	// The lookup repairs the torn tail then answers committed truth.
	requireDisposition(t, f.gateway, env, "applied", "")
	replay, err := f.adapter.Append(ctx, f.provenance(), "key-torn", raw)
	if err != nil || replay.CommandID != env.CommandID {
		t.Fatalf("accepted replay after torn tail: env=%+v err=%v", replay, err)
	}

	// Post-repair admission and reconciliation proceed normally.
	newEnv := f.sendMessage(t, "key-torn-new", "after the tear")
	f.sweep(t)
	requireDisposition(t, f.gateway, newEnv, "applied", "")
}

// The committed rejection reaches the HTTP caller as the rejection it is:
// a 409 carrying the committed reason and the durable command identity —
// never a 201 masquerading acceptance. (F356 HTTP mapping)
func TestUserCommandIngressCommittedRejectionMapsReason(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	if _, err := f.pool.Exec(f.ctx,
		`UPDATE core_personas SET authority = 'sealed', transfer_id = 'out-tr-0006' WHERE persona_id = $1`, f.pa); err != nil {
		t.Fatalf("seal persona: %v", err)
	}
	verifier := &fakeSessionVerifier{personalityAgentID: f.pa}
	ingress, err := NewUserCommandIngress(f.adapter, verifier)
	if err != nil {
		t.Fatalf("new ingress: %v", err)
	}
	ingress.AllowedOrigins = []string{testBrowserOrigin}
	ingress.Authorizer = allowDirectChatAuthorizer{}
	ingress.LifecycleFence = directchat.NewLifecycleFence()
	server := httptest.NewServer(newCommandMux(ingress))
	defer server.Close()

	post := func() (int, map[string]any) {
		resp := postWithSessionCookie(t, server.URL+"/direct-chat/commands",
			[]byte(`{"type":"user_message","text":"mid move","attachments":[]}`), f.pa)
		defer resp.Body.Close()
		var body map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		return resp.StatusCode, body
	}

	code, first := post()
	if code != http.StatusConflict ||
		first["error"] != string(RejectNotAllowed) ||
		first["reject_reason"] != string(RejectNotAllowed) ||
		first["command_id"] == "" {
		t.Fatalf("committed rejection mapped as %d %v, want 409 not_allowed with identity", code, first)
	}

	// The rejection is durable: replaying the same request answers the
	// same committed outcome with the same identity.
	code, replay := post()
	if code != http.StatusConflict ||
		replay["reject_reason"] != string(RejectNotAllowed) ||
		replay["command_id"] != first["command_id"] {
		t.Fatalf("rejection replay mapped as %d %v, want 409 same outcome", code, replay)
	}
}

// The same committed outcome over the live socket: the committed rejection
// arrives as command_rejected carrying the committed reason — and it
// committed durably inside the live session. (F352/F356 WS mapping)
func TestBrowserWebSocketCommittedRejectionMapsReason(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	if _, err := f.pool.Exec(f.ctx,
		`UPDATE core_personas SET authority = 'sealed', transfer_id = 'out-tr-0007' WHERE persona_id = $1`, f.pa); err != nil {
		t.Fatalf("seal persona: %v", err)
	}
	sessions, err := NewHMACUserSessionVerifier(testSecret, "", newTestBrowserSessionRevocationStore())
	if err != nil {
		t.Fatal(err)
	}
	server := newAuthorizedBrowserServer(sessions, f.adapter, f.gateway)
	server.AllowedOrigins = []string{browserAuthTestOrigin}
	mux := http.NewServeMux()
	mux.Handle("GET /direct-chat/ws", server)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	claims := userSessionWireClaims{
		TenantID:           "tenant-1",
		UserID:             "user-1",
		PersonalityAgentID: f.pa,
		Exp:                time.Now().Add(time.Hour).Unix(),
		Aud:                defaultBrowserAudience,
	}
	conn := dialBrowserWS(t, httpServer, signBrowserSession(t, testSecret, claims), f.pa)
	defer conn.Close()
	if err := conn.WriteJSON(browserHello{Type: "hello", LastEventSeq: 0}); err != nil {
		t.Fatal(err)
	}
	assertDirectChatStatus(t, conn, "ready")

	send := func(key string) browserCommandRejectedFrame {
		t.Helper()
		if err := conn.WriteJSON(browserCommandFrame{
			Type:           "command",
			IdempotencyKey: key,
			Command:        truthUserMessage("mid move"),
		}); err != nil {
			t.Fatal(err)
		}
		var rejected browserCommandRejectedFrame
		if err := conn.ReadJSON(&rejected); err != nil {
			t.Fatal(err)
		}
		return rejected
	}

	first := send("ws-sealed-1")
	if first.Type != "command_rejected" ||
		first.IdempotencyKey != "ws-sealed-1" ||
		first.RejectReason != RejectNotAllowed {
		t.Fatalf("unexpected committed rejection frame: %+v", first)
	}
	// The terminal answer is durable: replaying the same command over the
	// socket reports the same committed outcome.
	second := send("ws-sealed-1")
	if second.RejectReason != RejectNotAllowed {
		t.Fatalf("replayed committed rejection: %+v", second)
	}
}
